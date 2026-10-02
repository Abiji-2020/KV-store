#include "kvstore/client.hpp"

#include <cctype>
#include <iostream>
#include <string>
#include <vector>

static void print_usage(const char* prog) {
    std::cout << "Usage:\n"
              << "  Single-shot mode:   " << prog << " [OPTIONS] COMMAND [ARGS...]\n"
              << "  Interactive mode:   " << prog << " [OPTIONS]\n\n"
              << "Options:\n"
              << "  -h, --host <ip>      Server IP address (default: 127.0.0.1)\n"
              << "  -p, --port <port>    Server port (default: 7379)\n"
              << "      --help           Display this help message and exit\n";
}

static std::vector<std::string> tokenize_line(const std::string& line) {
    std::vector<std::string> tokens;
    size_t i = 0;
    const size_t len = line.size();

    while (i < len) {
        while (i < len && (line[i] == ' ' || line[i] == '\t')) {
            ++i;
        }
        if (i >= len) break;

        if (line[i] == '"' || line[i] == '\'') {
            char quote = line[i++];
            size_t start = i;
            while (i < len && line[i] != quote) {
                ++i;
            }
            tokens.push_back(line.substr(start, i - start));
            if (i < len && line[i] == quote) {
                ++i;
            }
        } else {
            size_t start = i;
            while (i < len && line[i] != ' ' && line[i] != '\t') {
                ++i;
            }
            tokens.push_back(line.substr(start, i - start));
        }
    }
    return tokens;
}

int main(int argc, char* argv[]) {
    std::string host = "127.0.0.1";
    int port = 7379;
    std::vector<std::string> cmd_args;

    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        if (arg == "--help") {
            print_usage(argv[0]);
            return 0;
        } else if ((arg == "-h" || arg == "--host") && i + 1 < argc) {
            host = argv[++i];
        } else if ((arg == "-p" || arg == "--port") && i + 1 < argc) {
            try {
                port = std::stoi(argv[++i]);
            } catch (...) {
                std::cerr << "Error: Invalid port number: " << argv[i] << std::endl;
                return 1;
            }
        } else if (!arg.empty() && arg[0] == '-') {
            std::cerr << "Error: Unknown option flag '" << arg << "'" << std::endl;
            print_usage(argv[0]);
            return 1;
        } else {
            cmd_args.push_back(std::move(arg));
        }
    }

    kvstore::Client client(host, port);

    // ==========================================
    // Single-Shot Mode (command arguments given)
    // ==========================================
    if (!cmd_args.empty()) {
        std::string verb = cmd_args[0];
        for (char& c : verb) {
            c = static_cast<char>(std::toupper(static_cast<unsigned char>(c)));
        }

        if (verb == "SET" && cmd_args.size() < 3) {
            std::cerr << "Usage Error: SET requires KEY and VALUE arguments" << std::endl;
            return 1;
        }
        if ((verb == "GET" || verb == "DEL") && cmd_args.size() < 2) {
            std::cerr << "Usage Error: " << verb << " requires KEY argument" << std::endl;
            return 1;
        }

        if (!client.connect(1, 0)) {
            std::cerr << "Error: Connection refused to " << host << ":" << port << std::endl;
            return 1;
        }

        std::string line;
        for (size_t i = 0; i < cmd_args.size(); ++i) {
            if (i > 0) line += " ";
            if (cmd_args[i].find(' ') != std::string::npos) {
                line += "\"" + cmd_args[i] + "\"";
            } else {
                line += cmd_args[i];
            }
        }

        std::string response = client.send_command(line);
        std::cout << response;
        std::cout.flush();

        if (response.find("-ERR") != std::string::npos) {
            if (response.find("NOT_FOUND") != std::string::npos) {
                return 0;
            }
            return 1;
        }
        return 0;
    }

    // ==========================================
    // Interactive REPL Mode (no command args)
    // ==========================================
    if (!client.connect(1, 0)) {
        std::cerr << "Error: Connection refused to " << host << ":" << port << std::endl;
        return 1;
    }

    std::string line;
    while (true) {
        std::cout << "kv> " << std::flush;
        if (!std::getline(std::cin, line)) {
            // EOF reached (Ctrl+D) -> clean exit
            break;
        }

        if (!line.empty() && line.back() == '\r') {
            line.pop_back();
        }

        auto tokens = tokenize_line(line);
        if (tokens.empty()) {
            continue; // Ignore blank lines
        }

        std::string verb = tokens[0];
        for (char& c : verb) {
            c = static_cast<char>(std::toupper(static_cast<unsigned char>(c)));
        }

        if (verb == "QUIT") {
            client.send_command("QUIT");
            break;
        }

        std::string resp = client.send_command(line);
        if (!client.is_connected()) {
            std::cerr << "Error: Connection refused / lost mid-session" << std::endl;
            return 1;
        }

        std::cout << resp;
        std::cout.flush();
    }

    return 0;
}
