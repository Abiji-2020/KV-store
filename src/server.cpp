#include "kvstore/server.hpp"

#include <arpa/inet.h>
#include <cstring>
#include <fcntl.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <poll.h>
#include <signal.h>
#include <sys/socket.h>
#include <unistd.h>
#include <algorithm>
#include <stdexcept>
#include <iostream>

namespace kvstore {

Server::Server(KVStore& store, WalManager* wal, const std::string& host, int port,
               size_t worker_threads)
    : store_(store), wal_(wal), host_(host), configured_port_(port),
      bound_port_(port), worker_threads_count_(worker_threads) {
    // Ignore SIGPIPE so abrupt client disconnections do not terminate the process
    ::signal(SIGPIPE, SIG_IGN);
}

Server::~Server() {
    stop();
}

void Server::start() {
    if (running_.load(std::memory_order_acquire)) {
        return;
    }

    // 1. Create stop self-pipe for clean wake-up of the poll loop
    if (::pipe2(stop_pipe_, O_NONBLOCK | O_CLOEXEC) < 0) {
        throw std::runtime_error("Failed to create stop self-pipe: " + std::string(strerror(errno)));
    }

    // 2. Create listening socket
    listen_fd_ = ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
    if (listen_fd_ < 0) {
        ::close(stop_pipe_[0]);
        ::close(stop_pipe_[1]);
        stop_pipe_[0] = stop_pipe_[1] = -1;
        throw std::runtime_error("Failed to create socket: " + std::string(strerror(errno)));
    }

    // 3. Set SO_REUSEADDR for immediate restart capability
    int reuse = 1;
    if (::setsockopt(listen_fd_, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse)) < 0) {
        ::close(listen_fd_);
        listen_fd_ = -1;
        ::close(stop_pipe_[0]);
        ::close(stop_pipe_[1]);
        stop_pipe_[0] = stop_pipe_[1] = -1;
        throw std::runtime_error("Failed to set SO_REUSEADDR: " + std::string(strerror(errno)));
    }

    // 4. Bind to host and port
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(static_cast<uint16_t>(configured_port_));
    if (::inet_pton(AF_INET, host_.c_str(), &addr.sin_addr) <= 0) {
        ::close(listen_fd_);
        listen_fd_ = -1;
        ::close(stop_pipe_[0]);
        ::close(stop_pipe_[1]);
        stop_pipe_[0] = stop_pipe_[1] = -1;
        throw std::runtime_error("Invalid IP/host address: " + host_);
    }

    if (::bind(listen_fd_, reinterpret_cast<struct sockaddr*>(&addr), sizeof(addr)) < 0) {
        std::string err = strerror(errno);
        ::close(listen_fd_);
        listen_fd_ = -1;
        ::close(stop_pipe_[0]);
        ::close(stop_pipe_[1]);
        stop_pipe_[0] = stop_pipe_[1] = -1;
        throw std::runtime_error("Failed to bind " + host_ + ":" + std::to_string(configured_port_) + ": " + err);
    }

    // 5. Query assigned dynamic port via getsockname()
    sockaddr_in bound_addr{};
    socklen_t addr_len = sizeof(bound_addr);
    if (::getsockname(listen_fd_, reinterpret_cast<struct sockaddr*>(&bound_addr), &addr_len) == 0) {
        bound_port_ = ntohs(bound_addr.sin_port);
    } else {
        bound_port_ = configured_port_;
    }

    // 6. Listen for incoming connections
    if (::listen(listen_fd_, SOMAXCONN) < 0) {
        std::string err = strerror(errno);
        ::close(listen_fd_);
        listen_fd_ = -1;
        ::close(stop_pipe_[0]);
        ::close(stop_pipe_[1]);
        stop_pipe_[0] = stop_pipe_[1] = -1;
        throw std::runtime_error("Failed to listen: " + err);
    }

    running_.store(true, std::memory_order_release);

    // 7. Spawn listener thread
    listener_thread_ = std::thread(&Server::accept_loop, this);
}

void Server::stop() {
    bool expected = true;
    if (!running_.compare_exchange_strong(expected, false, std::memory_order_acq_rel)) {
        return;
    }

    // 1. Wake up accept loop via self-pipe
    if (stop_pipe_[1] != -1) {
        char wake = 'q';
        (void)::write(stop_pipe_[1], &wake, 1);
    }

    // 2. Close listening socket to unblock accept4
    if (listen_fd_ != -1) {
        ::shutdown(listen_fd_, SHUT_RDWR);
        ::close(listen_fd_);
        listen_fd_ = -1;
    }

    // 3. Signal shutdown to all active client sockets so blocking recv() unblocks
    {
        std::lock_guard<std::mutex> lock(sessions_mtx_);
        for (auto& session : sessions_) {
            if (session) {
                int client_sock = session->fd.load(std::memory_order_acquire);
                if (client_sock != -1) {
                    ::shutdown(client_sock, SHUT_RDWR);
                }
            }
        }
    }

    // 4. Join listener thread
    if (listener_thread_.joinable()) {
        listener_thread_.join();
    }

    // 5. Join all client threads and clean up
    {
        std::lock_guard<std::mutex> lock(sessions_mtx_);
        for (auto& session : sessions_) {
            if (session && session->th.joinable()) {
                session->th.join();
            }
            if (session) {
                int client_sock = session->fd.exchange(-1, std::memory_order_acq_rel);
                if (client_sock != -1) {
                    ::close(client_sock);
                }
            }
        }
        sessions_.clear();
    }

    // 6. Close self-pipe
    if (stop_pipe_[0] != -1) {
        ::close(stop_pipe_[0]);
        stop_pipe_[0] = -1;
    }
    if (stop_pipe_[1] != -1) {
        ::close(stop_pipe_[1]);
        stop_pipe_[1] = -1;
    }

    // 7. Flush WAL to disk
    if (wal_) {
        wal_->sync();
    }
}

int Server::port() const noexcept {
    return bound_port_;
}

bool Server::is_running() const noexcept {
    return running_.load(std::memory_order_acquire);
}

void Server::reap_sessions_unlocked() {
    auto it = sessions_.begin();
    while (it != sessions_.end()) {
        if ((*it)->done.load(std::memory_order_acquire)) {
            if ((*it)->th.joinable()) {
                (*it)->th.join();
            }
            it = sessions_.erase(it);
        } else {
            ++it;
        }
    }
}

void Server::accept_loop() {
    struct pollfd pfd[2];
    pfd[0].fd = listen_fd_;
    pfd[0].events = POLLIN;
    pfd[1].fd = stop_pipe_[0];
    pfd[1].events = POLLIN;

    while (running_.load(std::memory_order_acquire)) {
        int ret = ::poll(pfd, 2, -1);
        if (ret < 0) {
            if (errno == EINTR) continue;
            break;
        }

        // Check if stop requested
        if (pfd[1].revents & POLLIN) {
            break;
        }

        if (pfd[0].revents & POLLIN) {
            sockaddr_in client_addr{};
            socklen_t client_len = sizeof(client_addr);
            int client_fd = ::accept4(listen_fd_, reinterpret_cast<struct sockaddr*>(&client_addr),
                                      &client_len, SOCK_CLOEXEC);
            if (client_fd < 0) {
                if (errno == EINTR || errno == EAGAIN || errno == EWOULDBLOCK) continue;
                if (!running_.load(std::memory_order_acquire)) break;
                continue;
            }

            // Enable TCP_NODELAY for minimum latency
            int nodelay = 1;
            ::setsockopt(client_fd, IPPROTO_TCP, TCP_NODELAY, &nodelay, sizeof(nodelay));

            // Launch client session thread
            std::lock_guard<std::mutex> lock(sessions_mtx_);
            reap_sessions_unlocked();

            auto sess = std::make_unique<ClientSession>();
            sess->fd.store(client_fd, std::memory_order_release);
            sess->done.store(false, std::memory_order_relaxed);
            ClientSession* s_ptr = sess.get();
            sess->th = std::thread(&Server::handle_client_connection, this, s_ptr);
            sessions_.push_back(std::move(sess));
        }
    }
}

void Server::handle_client_connection(ClientSession* session) {
    char buf[READ_BUFFER_SIZE];
    std::string stream_buf;
    int fd = session->fd.load(std::memory_order_acquire);
    if (fd < 0) {
        session->done.store(true, std::memory_order_release);
        return;
    }

    while (running_.load(std::memory_order_acquire)) {
        ssize_t bytes_read = ::recv(fd, buf, sizeof(buf), 0);
        if (bytes_read <= 0) {
            break; // EOF or connection dropped
        }

        stream_buf.append(buf, static_cast<size_t>(bytes_read));

        Command cmd;
        size_t consumed = 0;
        while (running_.load(std::memory_order_acquire) &&
               Protocol::parse(stream_buf, cmd, consumed)) {
            stream_buf.erase(0, consumed);

            // Ignore bare empty lines
            if (cmd.type == CommandType::UNKNOWN && cmd.raw_line.empty()) {
                continue;
            }

            Response resp = execute_command(cmd);
            resp.is_resp = cmd.is_resp;
            std::string serialized = Protocol::format_response(resp, cmd.is_resp);

            if (!send_all(fd, serialized)) {
                goto connection_done;
            }

            if (cmd.type == CommandType::QUIT) {
                goto connection_done;
            }
        }
    }

connection_done:
    int client_sock = session->fd.exchange(-1, std::memory_order_acq_rel);
    if (client_sock != -1) {
        ::shutdown(client_sock, SHUT_RDWR);
        ::close(client_sock);
    }
    session->done.store(true, std::memory_order_release);
}

Response Server::execute_command(const Command& cmd) {
    switch (cmd.type) {
        case CommandType::SET: {
            if (cmd.key.empty()) {
                return Protocol::make_err("wrong number of arguments for 'SET'");
            }
            if (wal_ && !wal_->append_set(cmd.key, cmd.value)) {
                return Protocol::make_err("WAL write failed");
            }
            store_.set(cmd.key, cmd.value);
            return Protocol::make_ok();
        }
        case CommandType::GET: {
            if (cmd.key.empty()) {
                return Protocol::make_err("wrong number of arguments for 'GET'");
            }
            auto val = store_.get(cmd.key);
            if (val.has_value()) {
                return Protocol::make_value(std::move(*val));
            }
            return Protocol::make_not_found("NOT_FOUND");
        }
        case CommandType::DEL: {
            if (cmd.key.empty()) {
                return Protocol::make_err("wrong number of arguments for 'DEL'");
            }
            if (wal_ && !wal_->append_del(cmd.key)) {
                return Protocol::make_err("WAL write failed");
            }
            DelResult res = store_.del(cmd.key);
            if (res == DelResult::DELETED) {
                return Protocol::make_deleted();
            }
            Response r = Protocol::make_not_found("NOT_FOUND");
            r.is_del = true;
            return r;
        }
        case CommandType::PING: {
            return Protocol::make_pong(cmd.key);
        }
        case CommandType::STATS: {
            Stats s = store_.get_stats();
            return Protocol::make_stats("keys=" + std::to_string(s.key_count) +
                                       " capacity=" + std::to_string(s.capacity) +
                                       " hits=" + std::to_string(s.hits) +
                                       " misses=" + std::to_string(s.misses) +
                                       " evictions=" + std::to_string(s.evictions));
        }
        case CommandType::QUIT: {
            return Protocol::make_quit();
        }
        case CommandType::CLEAR: {
            if (wal_ && !wal_->append_clear()) {
                return Protocol::make_err("WAL write failed");
            }
            store_.clear();
            return Protocol::make_ok();
        }
        case CommandType::SYNC: {
            if (wal_) {
                wal_->sync();
            }
            return Protocol::make_ok();
        }
        case CommandType::UNKNOWN:
        default: {
            return Protocol::make_err(cmd.raw_line.empty() ? "unknown command" : cmd.raw_line);
        }
    }
}

bool Server::send_all(int fd, const std::string& data) {
    size_t total_sent = 0;
    while (total_sent < data.size()) {
        ssize_t sent = ::send(fd, data.data() + total_sent, data.size() - total_sent, MSG_NOSIGNAL);
        if (sent <= 0) {
            if (errno == EINTR) continue;
            return false;
        }
        total_sent += static_cast<size_t>(sent);
    }
    return true;
}

} // namespace kvstore
