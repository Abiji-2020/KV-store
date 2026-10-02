#include "kvstore/benchmark.hpp"

#include <iostream>
#include <string>

static void print_usage(const char* prog) {
    std::cout << "Usage: " << prog << " [OPTIONS]\n\n"
              << "High-Performance Key-Value Store Benchmark Harness (cpp_kv_store)\n\n"
              << "Options:\n"
              << "  -h, --host <ip>                  Server IP address (default: 127.0.0.1)\n"
              << "  -p, --port <port>                Server port (default: 7379)\n"
              << "  -c, --threads, --clients <n>     Worker threads / concurrency level (default: 4)\n"
              << "  -n, --requests, --ops <n>        Total operations to execute (default: 1000)\n"
              << "  -r, --ratio, --read-ratio <f>    Read fraction between 0.0 and 1.0 (default: 0.8)\n"
              << "  -k, --keys, --keyspace <n>       Keyspace cardinality (default: 100)\n"
              << "  -s, --valsize, --val-size <n>    Value payload size in bytes (default: 64)\n"
              << "      --help                       Display this help message and exit (0)\n";
}

int main(int argc, char* argv[]) {
    kvstore::BenchmarkConfig config;
    config.requests = 1000;
    config.threads = 4;
    config.ratio = 0.8;
    config.keyspace = 100;
    config.val_size = 64;

    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        std::string val;
        bool has_inline_val = false;

        if (arg == "--help" || arg == "-help" || arg == "-?") {
            print_usage(argv[0]);
            return 0;
        }

        if (arg.rfind("--", 0) == 0 && arg.find('=') != std::string::npos) {
            size_t eq_pos = arg.find('=');
            val = arg.substr(eq_pos + 1);
            arg = arg.substr(0, eq_pos);
            has_inline_val = true;
        }

        auto get_value = [&](std::string& out_val) -> bool {
            if (has_inline_val) {
                out_val = val;
                return true;
            }
            if (i + 1 < argc) {
                out_val = argv[++i];
                return true;
            }
            return false;
        };

        if (arg == "-h" || arg == "--host") {
            std::string v;
            if (!get_value(v)) {
                std::cerr << "Error: Missing value for " << arg << std::endl;
                return 1;
            }
            config.host = v;
        } else if (arg == "-p" || arg == "--port") {
            std::string v;
            if (!get_value(v)) {
                std::cerr << "Error: Missing value for " << arg << std::endl;
                return 1;
            }
            try {
                int p = std::stoi(v);
                if (p <= 0 || p > 65535) {
                    std::cerr << "Error: Port must be between 1 and 65535" << std::endl;
                    return 1;
                }
                config.port = p;
            } catch (...) {
                std::cerr << "Error: Invalid port number: " << v << std::endl;
                return 1;
            }
        } else if (arg == "-c" || arg == "-t" || arg == "--threads" || arg == "--clients") {
            std::string v;
            if (!get_value(v)) {
                std::cerr << "Error: Missing value for " << arg << std::endl;
                return 1;
            }
            try {
                long long t = std::stoll(v);
                if (t <= 0) {
                    std::cerr << "Error: Threads must be greater than 0" << std::endl;
                    return 1;
                }
                config.threads = static_cast<size_t>(t);
            } catch (...) {
                std::cerr << "Error: Invalid threads value: " << v << std::endl;
                return 1;
            }
        } else if (arg == "-n" || arg == "--requests" || arg == "--ops") {
            std::string v;
            if (!get_value(v)) {
                std::cerr << "Error: Missing value for " << arg << std::endl;
                return 1;
            }
            try {
                long long n = std::stoll(v);
                if (n <= 0) {
                    std::cerr << "Error: Requests must be greater than 0" << std::endl;
                    return 1;
                }
                config.requests = static_cast<size_t>(n);
            } catch (...) {
                std::cerr << "Error: Invalid requests value: " << v << std::endl;
                return 1;
            }
        } else if (arg == "-r" || arg == "--ratio" || arg == "--read-ratio") {
            std::string v;
            if (!get_value(v)) {
                std::cerr << "Error: Missing value for " << arg << std::endl;
                return 1;
            }
            try {
                double r = std::stod(v);
                if (r < 0.0 || r > 1.0) {
                    std::cerr << "Error: Read ratio must be between 0.0 and 1.0" << std::endl;
                    return 1;
                }
                config.ratio = r;
            } catch (...) {
                std::cerr << "Error: Invalid ratio value: " << v << std::endl;
                return 1;
            }
        } else if (arg == "-k" || arg == "--keys" || arg == "--keyspace") {
            std::string v;
            if (!get_value(v)) {
                std::cerr << "Error: Missing value for " << arg << std::endl;
                return 1;
            }
            try {
                long long k = std::stoll(v);
                if (k <= 0) {
                    std::cerr << "Error: Keyspace must be greater than 0" << std::endl;
                    return 1;
                }
                config.keyspace = static_cast<size_t>(k);
            } catch (...) {
                std::cerr << "Error: Invalid keyspace value: " << v << std::endl;
                return 1;
            }
        } else if (arg == "-s" || arg == "--valsize" || arg == "--val-size" || arg == "--val_size") {
            std::string v;
            if (!get_value(v)) {
                std::cerr << "Error: Missing value for " << arg << std::endl;
                return 1;
            }
            try {
                long long s = std::stoll(v);
                if (s < 0) {
                    std::cerr << "Error: Value size must be non-negative" << std::endl;
                    return 1;
                }
                config.val_size = static_cast<size_t>(s);
            } catch (...) {
                std::cerr << "Error: Invalid valsize value: " << v << std::endl;
                return 1;
            }
        } else {
            std::cerr << "Error: Unknown option flag '" << arg << "'" << std::endl;
            print_usage(argv[0]);
            return 1;
        }
    }

    std::string err_msg;
    if (!config.validate(err_msg)) {
        std::cerr << "Configuration Error: " << err_msg << std::endl;
        return 1;
    }

    // Pre-flight check: ensure target server is reachable
    {
        kvstore::Client probe(config.host, config.port, 1.0);
        if (!probe.connect(2, 20)) {
            std::cerr << "Error: Target server unreachable at " << config.host << ":" << config.port << std::endl;
            return 1;
        }
        probe.disconnect();
    }

    try {
        kvstore::BenchmarkRunner runner(config);
        auto result = runner.run_network();

        if (result.completed_requests == 0 && (result.failed_requests > 0 || config.requests > 0)) {
            std::cerr << "Error: Benchmark failed to complete any requests" << std::endl;
            return 1;
        }

        result.print_summary(std::cout);
        return 0;
    } catch (const std::exception& ex) {
        std::cerr << "Benchmark Error: " << ex.what() << std::endl;
        return 1;
    }
}
