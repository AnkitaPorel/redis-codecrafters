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
#include <fstream>
#include "redis_parser.hpp"
#include "redis_commands.hpp"

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

std::chrono::steady_clock::time_point get_current_time() {
    return std::chrono::steady_clock::now();
}

uint64_t read_size_encoded(std::ifstream& file) {
    char first_byte;
    file.get(first_byte);
    uint8_t first = static_cast<uint8_t>(first_byte);
    uint8_t type = (first >> 6) & 0x03;

    if (type == 0) { // 6-bit size
        return first & 0x3F;
    } else if (type == 1) { // 14-bit size
        char next_byte;
        file.get(next_byte);
        return ((first & 0x3F) << 8) | static_cast<uint8_t>(next_byte);
    } else if (type == 2) { // 32-bit size
        uint32_t size = 0;
        char bytes[4];
        file.read(bytes, 4);
        for (int i = 0; i < 4; ++i) {
            size = (size << 8) | static_cast<uint8_t>(bytes[i]);
        }
        return size;
    } else { // type == 3, special string encoding
        throw std::runtime_error("Special string encoding not supported");
    }
}

std::string read_string_encoded(std::ifstream& file) {
    char first_byte;
    file.get(first_byte);
    uint8_t first = static_cast<uint8_t>(first_byte);
    uint8_t type = (first >> 6) & 0x03;

    if (type == 3) {
        uint8_t encoding = first & 0x3F;
        if (encoding == 0) { // 8-bit integer
            char byte;
            file.get(byte);
            return std::to_string(static_cast<uint8_t>(byte));
        } else if (encoding == 1) { // 16-bit integer
            char bytes[2];
            file.read(bytes, 2);
            uint16_t value = (static_cast<uint8_t>(bytes[1]) << 8) | static_cast<uint8_t>(bytes[0]);
            return std::to_string(value);
        } else if (encoding == 2) { // 32-bit integer
            char bytes[4];
            file.read(bytes, 4);
            uint32_t value = 0;
            for (int i = 3; i >= 0; --i) {
                value = (value << 8) | static_cast<uint8_t>(bytes[i]);
            }
            return std::to_string(value);
        } else {
            throw std::runtime_error("Unsupported string encoding");
        }
    } else {
        file.putback(first_byte);
        uint64_t length = read_size_encoded(file);
        std::string str(length, '\0');
        file.read(&str[0], length);
        return str;
    }
}

void load_rdb_file(const std::string& dir, const std::string& dbfilename) {
    std::string filepath = dir + "/" + dbfilename;
    std::ifstream file(filepath, std::ios::binary);
    if (!file.is_open()) {
        std::cout << "RDB file not found, starting with empty database\n";
        return;
    }

    // Read header
    char header[9];
    file.read(header, 9);
    if (std::strncmp(header, "REDIS0011", 9) != 0) {
        file.close();
        throw std::runtime_error("Invalid RDB file header");
    }

    // Read metadata and database sections
    while (file.peek() != EOF && file.peek() != 0xFF) { // Stop at EOF or checksum
        char byte;
        file.get(byte);
        if (byte == (char)0xFA) { // Metadata subsection
            std::string meta_name = read_string_encoded(file);
            std::string meta_value = read_string_encoded(file);
        } else if (byte == (char)0xFE) { // Database subsection
            uint64_t db_index = read_size_encoded(file);
            if (file.peek() == (char)0xFB) {
                file.get(); // Consume FB
                uint64_t hash_table_size = read_size_encoded(file);
                uint64_t expire_table_size = read_size_encoded(file);

                for (uint64_t i = 0; i < hash_table_size; ++i) {
                    bool has_expiry = false;
                    std::chrono::steady_clock::time_point expiry_time;

                    if (file.peek() == (char)0xFD) { // Expire in seconds
                        file.get();
                        char bytes[4];
                        file.read(bytes, 4);
                        uint32_t seconds = 0;
                        for (int j = 3; j >= 0; --j) {
                            seconds = (seconds << 8) | static_cast<uint8_t>(bytes[j]);
                        }
                        // Convert Unix timestamp (seconds) to steady_clock
                        auto duration = std::chrono::seconds(seconds);
                        expiry_time = std::chrono::steady_clock::time_point(duration);
                        has_expiry = true;
                    } else if (file.peek() == (char)0xFC) { // Expire in milliseconds
                        file.get();
                        char bytes[8];
                        file.read(bytes, 8);
                        uint64_t millis = 0;
                        for (int j = 7; j >= 0; --j) {
                            millis = (millis << 8) | static_cast<uint8_t>(bytes[j]);
                        }
                        // Convert Unix timestamp (milliseconds) to steady_clock
                        auto duration = std::chrono::milliseconds(millis);
                        expiry_time = std::chrono::steady_clock::time_point(duration);
                        has_expiry = true;
                    }

                    char value_type;
                    file.get(value_type);
                    if (value_type != 0) {
                        throw std::runtime_error("Only string type supported");
                    }

                    std::string key = read_string_encoded(file);
                    std::string value = read_string_encoded(file);

                    if (has_expiry) {
                        kv_store[key] = ValueEntry(value, expiry_time);
                    } else {
                        kv_store[key] = ValueEntry(value);
                    }
                }
            }
        }
    }

    file.close();
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
        auto it = kv_store.find(key);
        std::string response;
        if (it != kv_store.end()) {
            if (it->second.has_expiry && get_current_time() > it->second.expiry) {
                kv_store.erase(it);
                response = "$-1\r\n";
            } else {
                std::string value = it->second.value;
                response = "$" + std::to_string( value.length()) + "\r\n" + value + "\r\n";
            }
        } else {
            response = "$-1\r\n";
        }
        send(client_fd, response.c_str(), response.length(), 0);
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
    } else if (command == "KEYS" && parsed_command.size() == 2 && parsed_command[1] == "*") {
        std::vector<std::string> valid_keys;
        for (auto it = kv_store.begin(); it != kv_store.end();) {
            if (it->second.has_expiry && get_current_time() > it->second.expiry) {
                it = kv_store.erase(it);
            } else {
                valid_keys.push_back(it->first);
                ++it;
            }
        }
        std::string response = "*" + std::to_string(valid_keys.size()) + "\r\n";
        for (const auto& key : valid_keys) {
            response += "$" + std::to_string(key.length()) + "\r\n" + key + "\r\n";
        }
        send(client_fd, response.c_str(), response.length(), 0);
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
            }
        }
    }

    // Load RDB file
    try {
        load_rdb_file(config.dir, config.dbfilename);
    } catch (const std::exception& e) {
        std::cerr << "Error loading RDB file: " << e.what() << "\n";
    }

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
    server_addr.sin_port = htons(6379);

    if (bind(server_fd, (struct sockaddr *) &server_addr, sizeof(server_addr)) != 0) {
        std::cerr << "Failed to bind to port 6379\n";
        return 1;
    }

    int connection_backDown = 5;
    if (listen(server_fd, connection_backDown) != 0) {
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
                int bytes_received = recv(client_fd, buffer, sizeof(buffer - 1), 0);

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