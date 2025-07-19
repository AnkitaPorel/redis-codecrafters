#include <iostream>
#include <cstdlib>
#include <string>
#include <cstring>
#include <vector>
#include <map>
#include <chrono>
#include <fstream>
#include <unistd.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <sys/select.h>
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

// Function to read size-encoded value
uint64_t read_size_encoded(std::ifstream& file, size_t& pos, const std::string& data) {
    if (pos >= data.length()) {
        throw std::runtime_error("Invalid RDB: incomplete size encoding");
    }
    unsigned char byte = data[pos++];
    unsigned char first_two_bits = (byte >> 6) & 0x03;

    if (first_two_bits == 0x00) {
        return byte & 0x3F;
    } else if (first_two_bits == 0x01) {
        if (pos >= data.length()) {
            throw std::runtime_error("Invalid RDB: incomplete 14-bit size encoding");
        }
        unsigned char next_byte = data[pos++];
        return ((byte & 0x3F) << 8) | next_byte;
    } else if (first_two_bits == 0x10) {
        if (pos + 3 >= data.length()) {
            throw std::runtime_error("Invalid RDB: incomplete 32-bit size encoding");
        }
        uint32_t size = 0;
        for (int i = 0; i < 4; ++i) {
            size = (size << 8) | (unsigned char)data[pos++];
        }
        return size;
    } else {
        throw std::runtime_error("Invalid RDB: unsupported size encoding");
    }
}

// Function to read string-encoded value
std::string read_string_encoded(std::ifstream& file, size_t& pos, const std::string& data) {
    uint64_t len = read_size_encoded(file, pos, data);
    if (pos + len > data.length()) {
        throw std::runtime_error("Invalid RDB: incomplete string data");
    }
    std::string str = data.substr(pos, len);
    pos += len;
    return str;
}

// Function to parse RDB file and populate kv_store
void parse_rdb_file(const std::string& dir, const std::string& dbfilename) {
    std::string filepath = dir + "/" + dbfilename;
    std::ifstream file(filepath, std::ios::binary);
    if (!file.is_open()) {
        std::cout << "RDB file not found, starting with empty database\n";
        return;
    }

    // Read entire file into string
    file.seekg(0, std::ios::end);
    size_t size = file.tellg();
    std::string data(size, ' ');
    file.seekg(0);
    file.read(&data[0], size);
    file.close();

    size_t pos = 0;

    // Parse header
    if (pos + 9 > data.length() || data.substr(pos, 9) != "REDIS0011") {
        throw std::runtime_error("Invalid RDB: incorrect header");
    }
    pos += 9;

    // Skip metadata section
    while (pos < data.length() && data[pos] == 0xFA) {
        pos++; // Skip FA
        read_string_encoded(file, pos, data); // Skip metadata name
        read_string_encoded(file, pos, data); // Skip metadata value
    }

    // Parse database section
    while (pos < data.length() && data[pos] == 0xFE) {
        pos++; // Skip FE
        uint64_t db_index = read_size_encoded(file, pos, data);
        if (db_index != 0) {
            throw std::runtime_error("Only database 0 is supported");
        }

        if (pos >= data.length() || data[pos] != 0xFB) {
            throw std::runtime_error("Invalid RDB: missing hash table sizes");
        }
        pos++; // Skip FB
        read_size_encoded(file, pos, data); // Skip key-value hash table size
        read_size_encoded(file, pos, data); // Skip expires hash table size

        // Parse key-value pairs
        while (pos < data.length() && data[pos] != 0xFF && data[pos] != 0xFE) {
            bool has_expiry = false;
            std::chrono::steady_clock::time_point expiry_time;

            // Check for expiry information
            if (data[pos] == 0xFC) {
                pos++; // Skip FC
                if (pos + 8 > data.length()) {
                    throw std::runtime_error("Invalid RDB: incomplete millisecond expiry");
                }
                uint64_t expiry_ms = 0;
                for (int i = 7; i >= 0; --i) {
                    expiry_ms = (expiry_ms << 8) | (unsigned char)data[pos + i];
                }
                pos += 8;
                has_expiry = true;
                expiry_time = std::chrono::steady_clock::time_point() + std::chrono::milliseconds(expiry_ms);
            } else if (data[pos] == 0xFD) {
                pos++; // Skip FD
                if (pos + 4 > data.length()) {
                    throw std::runtime_error("Invalid RDB: incomplete second expiry");
                }
                uint32_t expiry_sec = 0;
                for (int i = 3; i >= 0; --i) {
                    expiry_sec = (expiry_sec << 8) | (unsigned char)data[pos + i];
                }
                pos += 4;
                has_expiry = true;
                expiry_time = std::chrono::steady_clock::time_point() + 
                              std::chrono::seconds(expiry_sec);
            }

            // Read value type
            if (pos >= data.length()) {
                throw std::runtime_error("Invalid RDB: missing value type");
            }
            unsigned char value_type = data[pos++];
            if (value_type != 0x00) {
                throw std::runtime_error("Only string values are supported");
            }

            // Read key and value
            std::string key = read_string_encoded(file, pos, data);
            std::string value = read_string_encoded(file, pos, data);

            // Store in kv_store
            if (has_expiry) {
                kv_store[key] = ValueEntry(value, expiry_time);
            } else {
                kv_store[key] = ValueEntry(value);
            }
        }
    }

    // End of file section (FF followed by 8-byte checksum)
    if (pos < data.length() && data[pos] == 0xFF) {
        pos++; // Skip FF
        if (pos + 8 > data.length()) {
            throw std::runtime_error("Invalid RDB: incomplete checksum");
        }
        pos += 8; // Skip checksum
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
        auto it = kv_store.find(key);
        std::string response;
        if (it != kv_store.end()) {
            if (it->second.has_expiry && get_current_time() > it->second.expiry) {
                kv_store.erase(it);
                response = "$-1\r\n";
            } else {
                std::string value = it->second.value;
                response = "$" + std::to_string(value.length()) + "\r\n" + value + "\r\n";
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
        auto current_time = get_current_time();
        for (auto it = kv_store.begin(); it != kv_store.end();) {
            if (it->second.has_expiry && current_time > it->second.expiry) {
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
        parse_rdb_file(config.dir, config.dbfilename);
    } catch (const std::exception& e) {
        std::cerr << "Error parsing RDB file: " << e.what() << "\n";
        // Continue with empty database if RDB parsing fails
        kv_store.clear();
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