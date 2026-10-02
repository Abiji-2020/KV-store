#include "kvstore/client.hpp"

#include <arpa/inet.h>
#include <cstring>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <thread>
#include <unistd.h>
#include <algorithm>

namespace kvstore {

Client::Client(const std::string& host, int port, double timeout_sec)
    : host_(host), port_(port), timeout_sec_(timeout_sec) {}

Client::~Client() {
    disconnect();
}

Client::Client(Client&& other) noexcept
    : host_(std::move(other.host_)), port_(other.port_),
      timeout_sec_(other.timeout_sec_), sock_fd_(other.sock_fd_),
      last_error_(std::move(other.last_error_)),
      stream_buffer_(std::move(other.stream_buffer_)) {
    other.sock_fd_ = -1;
}

Client& Client::operator=(Client&& other) noexcept {
    if (this != &other) {
        disconnect();
        host_ = std::move(other.host_);
        port_ = other.port_;
        timeout_sec_ = other.timeout_sec_;
        sock_fd_ = other.sock_fd_;
        last_error_ = std::move(other.last_error_);
        stream_buffer_ = std::move(other.stream_buffer_);
        other.sock_fd_ = -1;
    }
    return *this;
}

bool Client::connect(int retries, int retry_delay_ms) {
    disconnect();

    for (int i = 0; i < retries; ++i) {
        sock_fd_ = ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
        if (sock_fd_ < 0) {
            last_error_ = "Failed to create socket: " + std::string(strerror(errno));
            return false;
        }

        // Set socket timeout
        struct timeval tv{};
        tv.tv_sec = static_cast<time_t>(timeout_sec_);
        tv.tv_usec = static_cast<suseconds_t>((timeout_sec_ - static_cast<double>(tv.tv_sec)) * 1e6);
        ::setsockopt(sock_fd_, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
        ::setsockopt(sock_fd_, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));

        // Enable TCP_NODELAY
        int nodelay = 1;
        ::setsockopt(sock_fd_, IPPROTO_TCP, TCP_NODELAY, &nodelay, sizeof(nodelay));

        sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_port = htons(static_cast<uint16_t>(port_));
        if (::inet_pton(AF_INET, host_.c_str(), &addr.sin_addr) <= 0) {
            last_error_ = "Invalid host/IP: " + host_;
            ::close(sock_fd_);
            sock_fd_ = -1;
            return false;
        }

        if (::connect(sock_fd_, reinterpret_cast<struct sockaddr*>(&addr), sizeof(addr)) == 0) {
            last_error_.clear();
            return true;
        }

        last_error_ = "Connection refused to " + host_ + ":" + std::to_string(port_) + ": " + strerror(errno);
        ::close(sock_fd_);
        sock_fd_ = -1;

        if (i + 1 < retries && retry_delay_ms > 0) {
            std::this_thread::sleep_for(std::chrono::milliseconds(retry_delay_ms));
        }
    }

    return false;
}

void Client::disconnect() {
    if (sock_fd_ != -1) {
        ::shutdown(sock_fd_, SHUT_RDWR);
        ::close(sock_fd_);
        sock_fd_ = -1;
    }
    stream_buffer_.clear();
}

bool Client::is_connected() const noexcept {
    return sock_fd_ != -1;
}

std::string Client::send_command(const std::string& command_line) {
    if (!is_connected()) {
        last_error_ = "Socket not connected";
        return "-ERR connection closed\r\n";
    }

    std::string formatted = command_line;
    if (formatted.size() < 2 || formatted.substr(formatted.size() - 2) != "\r\n") {
        if (!formatted.empty() && formatted.back() == '\n') {
            formatted.pop_back();
        }
        formatted += "\r\n";
    }

    if (!send_all(formatted)) {
        last_error_ = "Connection lost during send";
        disconnect();
        return "-ERR connection lost\r\n";
    }

    std::string first_line = read_line();
    if (first_line.empty() && !is_connected()) {
        return "-ERR connection lost\r\n";
    }

    // If RESP2 bulk string ($<len>\r\n), read trailing payload bytes
    if (!first_line.empty() && first_line.front() == '$') {
        size_t len_end = first_line.find("\r\n");
        if (len_end != std::string::npos) {
            std::string len_str = first_line.substr(1, len_end - 1);
            try {
                long long bulk_len = std::stoll(len_str);
                if (bulk_len >= 0) {
                    std::string payload_and_crlf = read_exact(static_cast<size_t>(bulk_len) + 2);
                    return first_line + payload_and_crlf;
                }
            } catch (...) {
                // fall through
            }
        }
    }

    return first_line;
}

std::string Client::set(const std::string& key, const std::string& value) {
    return send_command("SET " + key + " " + value);
}

std::string Client::get(const std::string& key) {
    return send_command("GET " + key);
}

std::string Client::del(const std::string& key) {
    return send_command("DEL " + key);
}

std::string Client::ping(const std::string& message) {
    return message.empty() ? send_command("PING") : send_command("PING " + message);
}

std::string Client::stats() {
    return send_command("STATS");
}

std::string Client::quit() {
    return send_command("QUIT");
}

const std::string& Client::last_error() const noexcept {
    return last_error_;
}

bool Client::send_all(const std::string& data) {
    size_t total_sent = 0;
    while (total_sent < data.size()) {
        ssize_t sent = ::send(sock_fd_, data.data() + total_sent, data.size() - total_sent, MSG_NOSIGNAL);
        if (sent <= 0) {
            if (errno == EINTR) continue;
            return false;
        }
        total_sent += static_cast<size_t>(sent);
    }
    return true;
}

std::string Client::read_line() {
    while (true) {
        size_t crlf = stream_buffer_.find("\r\n");
        if (crlf != std::string::npos) {
            std::string line = stream_buffer_.substr(0, crlf + 2);
            stream_buffer_.erase(0, crlf + 2);
            return line;
        }
        size_t lf = stream_buffer_.find('\n');
        if (lf != std::string::npos) {
            std::string line = stream_buffer_.substr(0, lf + 1);
            stream_buffer_.erase(0, lf + 1);
            return line;
        }

        char buf[2048];
        ssize_t n = ::recv(sock_fd_, buf, sizeof(buf), 0);
        if (n <= 0) {
            disconnect();
            if (!stream_buffer_.empty()) {
                std::string rem = stream_buffer_;
                stream_buffer_.clear();
                return rem;
            }
            return "";
        }
        stream_buffer_.append(buf, static_cast<size_t>(n));
    }
}

std::string Client::read_exact(size_t num_bytes) {
    while (stream_buffer_.size() < num_bytes) {
        char buf[2048];
        ssize_t n = ::recv(sock_fd_, buf, sizeof(buf), 0);
        if (n <= 0) {
            disconnect();
            break;
        }
        stream_buffer_.append(buf, static_cast<size_t>(n));
    }

    size_t to_extract = std::min(num_bytes, stream_buffer_.size());
    std::string result = stream_buffer_.substr(0, to_extract);
    stream_buffer_.erase(0, to_extract);
    return result;
}

} // namespace kvstore
