#include <iostream>
#include <cstdlib>
#include <string>
#include <cstring>
#include <vector>
#include <map>
#include <chrono>
#include <unistd.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <sys/select.h>
#include "redis_parser.hpp"
#include "redis_commands.hpp"
#include "rdb_parser.hpp"

struct ValueEntry {
    std::string value;
    std::chrono::steady_clock::time_point expiry;
    bool has_expiry;

    ValueEntry() : value(""), has_expiry(false) {}

    ValueEntry(const std::string& val)
        : value(val), has_expiry(false) {}

    ValueEntry(const std::string& val, std::chrono::steady_clock::time_point exp)
        : value(val), expiry(exp), has_expiry(true) {}
};

std::map<std::string, ValueEntry> kv_store;
ServerConfig config;
int server_port = 6379; // Default port
bool is_replica = false;
std::string master_host;
int master_port;

std::chrono::steady_clock::time_point get_current_time() {
    return std::chrono::steady_clock::now();
}

void load_rdb_file() {
    std::string filepath = config.dir + "/" + config.dbfilename;
    std::cout << "Attempting to load RDB file from: " << filepath << std::endl;
    
    RDBParser parser;
    
    try {
        auto rdb_data = parser.parse_rdb_file(filepath);
        
        // Convert RDBValueEntry to ValueEntry and load into kv_store
        for (const auto& pair : rdb_data) {
            const std::string& key = pair.first;
            const RDBValueEntry& rdb_entry = pair.second;
            
            if (rdb_entry.has_expiry) {
                kv_store[key] = ValueEntry(rdb_entry.value, rdb_entry.expiry);
            } else {
                kv_store[key] = ValueEntry(rdb_entry.value);
            }
        }
        
        if (!rdb_data.empty()) {
            std::cout << "Successfully loaded " << rdb_data.size() << " keys from RDB file" << std::endl;
            // Print all loaded keys for debugging
            for (const auto& pair : kv_store) {
                std::cout << "  Key: '" << pair.first << "' -> Value: '" << pair.second.value << "'" << std::endl;
            }
        } else {
            std::cout << "No keys found in RDB file or file doesn't exist" << std::endl;
        }
    } catch (const std::exception& e) {
        std::cerr << "Error loading RDB file: " << e.what() << std::endl;
    }
}

void execute_redis_command(int client_fd, const std::vector<std::string>& parsed_command) {
    if (parsed_command.empty()) {
        return;
    }

    std::string command = parsed_command[0];

    if (command == "PING") {
        std::string response = "+PONG\r\n";
        send(client_fd, response.c_str(), response.length(), 0);
    } else if (command == "ECHO" && parsed_command.size() == 2) {
        std::string arg = parsed_command[1];
        std::string response = "$" + std::to_string(arg.length()) + "\r\n" + arg + "\r\n";
        send(client_fd, response.c_str(), response.length(), 0);
    } else if (command == "SET" && (parsed_command.size() == 3 || parsed_command.size() == 5)) {
        std::string key = parsed_command[1];
        std::string value = parsed_command[2];

        if (parsed_command.size() == 3) {
            kv_store[key] = ValueEntry(value);
        } else if (parsed_command.size() == 5) {
            std::string px_arg = parsed_command[3];
            for (char& c : px_arg) {
                c = std::toupper(c);
            }
            if (px_arg == "PX") {
                try {
                    long expiry_ms = std::stol(parsed_command[4]);
                    if (expiry_ms <= 0) {
                        std::string response = "-ERR invalid expire time\r\n";
                        send(client_fd, response.c_str(), response.length(), 0);
                        return;
                    }
                    auto expiry_time = get_current_time() + std::chrono::milliseconds(expiry_ms);
                    kv_store[key] = ValueEntry(value, expiry_time);
                } catch (const std::exception& e) {
                    std::string response = "-ERR invalid expire time\r\n";
                    send(client_fd, response.c_str(), response.length(), 0);
                    return;
                }
            } else {
                std::string response = "-ERR syntax error\r\n";
                send(client_fd, response.c_str(), response.length(), 0);
                return;
            }
        }
        std::string response = "+OK\r\n";
        send(client_fd, response.c_str(), response.length(), 0);
    } else if (command == "GET" && parsed_command.size() == 2) {
        std::string key = parsed_command[1];
        std::cout << "GET request for key: '" << key << "'" << std::endl;
        
        auto it = kv_store.find(key);
        std::string response;
        if (it != kv_store.end()) {
            // Check if key has expired
            if (it->second.has_expiry && get_current_time() > it->second.expiry) {
                std::cout << "Key '" << key << "' has expired, removing from store" << std::endl;
                kv_store.erase(it);
                response = "$-1\r\n"; // Null bulk string (key not found)
            } else {
                std::string value = it->second.value;
                std::cout << "Found key '" << key << "' with value: '" << value << "'" << std::endl;
                response = "$" + std::to_string(value.length()) + "\r\n" + value + "\r\n";
            }
        } else {
            std::cout << "Key '" << key << "' not found in store" << std::endl;
            response = "$-1\r\n"; // Null bulk string (key not found)
        }
        std::cout << "Sending response: " << response << std::endl;
        send(client_fd, response.c_str(), response.length(), 0);
    } else if (command == "KEYS" && parsed_command.size() == 2) {
        std::string pattern = parsed_command[1];
        
        if (pattern == "*") {
            // Remove expired keys first
            auto current_time = get_current_time();
            for (auto it = kv_store.begin(); it != kv_store.end();) {
                if (it->second.has_expiry && current_time > it->second.expiry) {
                    it = kv_store.erase(it);
                } else {
                    ++it;
                }
            }
            
            // Build response array
            std::string response = "*" + std::to_string(kv_store.size()) + "\r\n";
            for (const auto& pair : kv_store) {
                const std::string& key = pair.first;
                response += "$" + std::to_string(key.length()) + "\r\n" + key + "\r\n";
            }
            send(client_fd, response.c_str(), response.length(), 0);
        } else {
            // For now, only support "*" pattern
            std::string response = "-ERR pattern not supported\r\n";
            send(client_fd, response.c_str(), response.length(), 0);
        }
    } else if (command == "CONFIG" && parsed_command.size() == 3 && parsed_command[1] == "GET") {
        std::string param = parsed_command[2];
        std::string param_value;
        if (param == "dir") {
            param_value = config.dir;
        } else if (param == "dbfilename") {
            param_value = config.dbfilename;
        } else {
            std::string response = "-ERR unknown parameter\r\n";
            send(client_fd, response.c_str(), response.length(), 0);
            return;
        }
        std::string response = "*2\r\n$" + std::to_string(param.length()) + "\r\n" + param + "\r\n$" + 
                              std::to_string(param_value.length()) + "\r\n" + param_value + "\r\n";
        send(client_fd, response.c_str(), response.length(), 0);
    } else if (command == "INFO" && parsed_command.size() == 2) {
        std::string section = parsed_command[1];
        
        // Convert section to lowercase for case-insensitive comparison
        for (char& c : section) {
            c = std::tolower(c);
        }
        
        if (section == "replication") {
            // For now, we're always a master with basic replication info
            std::string info_content = "role:master";
            std::string response = "$" + std::to_string(info_content.length()) + "\r\n" + info_content + "\r\n";
            send(client_fd, response.c_str(), response.length(), 0);
        } else {
            // Only support replication section for now
            std::string response = "-ERR unsupported INFO section\r\n";
            send(client_fd, response.c_str(), response.length(), 0);
        }
    } else {
        std::string response = "-ERR unknown command or wrong number of arguments\r\n";
        send(client_fd, response.c_str(), response.length(), 0);
    }
}

int main(int argc, char **argv) {
    std::cout << std::unitbuf;
    std::cerr << std::unitbuf;

    // Parse command-line arguments
    for (int i = 1; i < argc; i += 2) {
        std::string arg = argv[i];
        if (i + 1 < argc) {
            if (arg == "--dir") {
                config.dir = argv[i + 1];
            } else if (arg == "--dbfilename") {
                config.dbfilename = argv[i + 1];
            } else if (arg == "--port") {
                try {
                    server_port = std::stoi(argv[i + 1]);
                    if (server_port <= 0 || server_port > 65535) {
                        std::cerr << "Invalid port number: " << argv[i + 1] << std::endl;
                        return 1;
                    }
                } catch (const std::exception& e) {
                    std::cerr << "Invalid port number: " << argv[i + 1] << std::endl;
                    return 1;
                }
            }
        }
    }

    std::cout << "Starting Redis server on port " << server_port << std::endl;

    // Load RDB file at startup
    load_rdb_file();

    int server_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd < 0) {
        std::cerr << "Failed to create server socket\n";
        return 1;
    }

    int reuse = 1;
    if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse)) < 0) {
        std::cerr << "setsockopt failed\n";
        return 1;
    }

    struct sockaddr_in server_addr;
    server_addr.sin_family = AF_INET;
    server_addr.sin_addr.s_addr = INADDR_ANY;
    server_addr.sin_port = htons(server_port); // Use configurable port

    if (bind(server_fd, (struct sockaddr *) &server_addr, sizeof(server_addr)) != 0) {
        std::cerr << "Failed to bind to port " << server_port << "\n";
        return 1;
    }

    int connection_backlog = 5;
    if (listen(server_fd, connection_backlog) != 0) {
        std::cerr << "listen failed\n";
        return 1;
    }

    std::cout << "Waiting for a client to connect...\n";
    std::cout << "Logs from your program will appear here!\n";

    fd_set read_fds;
    std::vector<int> client_fds;

    while (true) {
        FD_ZERO(&read_fds);
        FD_SET(server_fd, &read_fds);
        int max_fd = server_fd;

        for (int fd : client_fds) {
            FD_SET(fd, &read_fds);
            if (fd > max_fd) {
                max_fd = fd;
            }
        }

        if (select(max_fd + 1, &read_fds, nullptr, nullptr, nullptr) < 0) {
            std::cerr << "select failed\n";
            close(server_fd);
            return 1;
        }

        if (FD_ISSET(server_fd, &read_fds)) {
            struct sockaddr_in client_addr;
            socklen_t client_addr_len = sizeof(client_addr);
            int client_fd = accept(server_fd, (struct sockaddr *) &client_addr, &client_addr_len);
            if (client_fd < 0) {
                std::cerr << "accept failed\n";
                continue;
            }
            std::cout << "Client connected (fd: " << client_fd << ")\n";
            client_fds.push_back(client_fd);
        }

        for (auto it = client_fds.begin(); it != client_fds.end();) {
            int client_fd = *it;
            if (FD_ISSET(client_fd, &read_fds)) {
                char buffer[4096];
                int bytes_received = recv(client_fd, buffer, sizeof(buffer) - 1, 0);

                if (bytes_received <= 0) {
                    std::cout << "Client disconnected (fd: " << client_fd << ")\n";
                    close(client_fd);
                    it = client_fds.erase(it);
                    continue;
                }

                buffer[bytes_received] = '\0';
                std::cout << "Received: " << buffer << std::endl;

                std::vector<std::string> parsed_command;
                try {
                    parse_redis_command(buffer, parsed_command);
                    execute_redis_command(client_fd, parsed_command);
                } catch (const std::exception& e) {
                    std::string response = "-ERR protocol error: " + std::string(e.what()) + "\r\n";
                    send(client_fd, response.c_str(), response.length(), 0);
                }
            }
            ++it;
        }
    }

    for (int client_fd : client_fds) {
        close(client_fd);
    }
    close(server_fd);

    return 0;
}