#include <iostream>
#include <cstdlib>
#include <string>
#include <cstring>
#include <vector>
#include <map>
#include <set>
#include <chrono>
#include <unistd.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <sys/select.h>
#include <thread>
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
int server_port = 6379;

bool is_replica = false;
std::string master_host;
int master_port;

std::string master_replid = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb";
int master_repl_offset = 0;

std::map<int, std::map<std::string, std::string>> replica_info;
std::set<int> connected_replicas;

int replica_offset = 0;

std::chrono::steady_clock::time_point get_current_time() {
    return std::chrono::steady_clock::now();
}

bool is_write_command(const std::string& command) {
    return (command == "SET" || command == "DEL" || command == "INCR" || command == "DECR" || 
            command == "LPUSH" || command == "RPUSH" || command == "LPOP" || command == "RPOP" ||
            command == "SADD" || command == "SREM" || command == "HSET" || command == "HDEL");
}

std::string command_to_resp_array(const std::vector<std::string>& command) {
    std::string resp = "*" + std::to_string(command.size()) + "\r\n";
    for (const std::string& arg : command) {
        resp += "$" + std::to_string(arg.length()) + "\r\n" + arg + "\r\n";
    }
    return resp;
}

void propagate_to_replicas(const std::vector<std::string>& command) {
    if (is_replica || connected_replicas.empty()) {
        return;
    }
    
    if (!is_write_command(command[0])) {
        return;
    }
    
    std::string resp_command = command_to_resp_array(command);
    std::cout << "Propagating command to " << connected_replicas.size() << " replicas: " << resp_command;
    
    // Send to all connected replicas
    for (auto it = connected_replicas.begin(); it != connected_replicas.end();) {
        int replica_fd = *it;
        ssize_t bytes_sent = send(replica_fd, resp_command.c_str(), resp_command.length(), MSG_NOSIGNAL);
        
        if (bytes_sent < 0) {
            // Replica connection failed, remove from set
            std::cout << "Failed to send to replica (fd: " << replica_fd << "), removing from replica list" << std::endl;
            replica_info.erase(replica_fd);
            it = connected_replicas.erase(it);
        } else {
            std::cout << "Successfully sent command to replica (fd: " << replica_fd << ")" << std::endl;
            ++it;
        }
    }
}

void load_rdb_file() {
    std::string filepath = config.dir + "/" + config.dbfilename;
    std::cout << "Attempting to load RDB file from: " << filepath << std::endl;
    
    RDBParser parser;
    
    try {
        auto rdb_data = parser.parse_rdb_file(filepath);
        
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

std::string execute_replica_command(const std::vector<std::string>& parsed_command, int bytes_processed) {
    // Store the offset before updating it
    int offset_to_report = replica_offset;
    
    // Only increment offset for actual commands that modify the replica state
    // Don't increment for REPLCONF GETACK since it's just a query
    bool should_increment_offset = true;
    
    if (!parsed_command.empty()) {
        std::string command = parsed_command[0];
        
        // Convert command to uppercase for comparison
        for (char& c : command) {
            c = std::toupper(c);
        }
        
        if (command == "REPLCONF" && parsed_command.size() >= 2) {
            std::string subcommand = parsed_command[1];
            
            for (char& c : subcommand) {
                c = std::toupper(c);
            }
            
            if (subcommand == "GETACK") {
                std::cout << "Replica: Responding to GETACK with ACK " << offset_to_report << std::endl;
                
                // Don't increment offset for GETACK - it's just a query
                should_increment_offset = false;
                
                std::string offset_str = std::to_string(offset_to_report);
                std::string ack_response = "*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n$" + 
                                           std::to_string(offset_str.length()) + "\r\n" + offset_str + "\r\n";
                return ack_response;
            }
        }
    }
    
    // Increment offset for all other commands
    if (should_increment_offset) {
        replica_offset += bytes_processed;
    }

    if (parsed_command.empty()) {
        return "";
    }

    std::string command = parsed_command[0];
    
    // Convert to uppercase
    for (char& c : command) {
        c = std::toupper(c);
    }

    if (command == "SET" && (parsed_command.size() == 3 || parsed_command.size() == 5)) {
        std::string key = parsed_command[1];
        std::string value = parsed_command[2];

        if (parsed_command.size() == 3) {
            kv_store[key] = ValueEntry(value);
            std::cout << "Replica: SET '" << key << "' = '" << value << "'" << std::endl;
        } else if (parsed_command.size() == 5) {
            std::string px_arg = parsed_command[3];
            for (char& c : px_arg) {
                c = std::toupper(c);
            }
            if (px_arg == "PX") {
                try {
                    long expiry_ms = std::stol(parsed_command[4]);
                    if (expiry_ms > 0) {
                        auto expiry_time = get_current_time() + std::chrono::milliseconds(expiry_ms);
                        kv_store[key] = ValueEntry(value, expiry_time);
                        std::cout << "Replica: SET '" << key << "' = '" << value << "' with expiry " << expiry_ms << "ms" << std::endl;
                    }
                } catch (const std::exception& e) {
                    std::cerr << "Replica: Invalid expiry time in SET command" << std::endl;
                }
            }
        }
    } else if (command == "DEL" && parsed_command.size() >= 2) {
        for (size_t i = 1; i < parsed_command.size(); i++) {
            std::string key = parsed_command[i];
            kv_store.erase(key);
            std::cout << "Replica: DEL '" << key << "'" << std::endl;
        }
    } else if (command == "PING") {
        std::cout << "Replica: Received PING" << std::endl;
    }
    
    return "";
}

void handle_master_connection() {
    if (!is_replica) {
        return;
    }
    
    std::cout << "Connecting to master at " << master_host << ":" << master_port << std::endl;
    
    int master_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (master_fd < 0) {
        std::cerr << "Failed to create socket for master connection" << std::endl;
        return;
    }
    
    struct sockaddr_in master_addr;
    memset(&master_addr, 0, sizeof(master_addr));
    master_addr.sin_family = AF_INET;
    master_addr.sin_port = htons(master_port);
    
    if (master_host == "localhost") {
        master_addr.sin_addr.s_addr = inet_addr("127.0.0.1");
    } else if (inet_aton(master_host.c_str(), &master_addr.sin_addr) == 0) {
        struct hostent *host_entry = gethostbyname(master_host.c_str());
        if (host_entry == nullptr) {
            std::cerr << "Failed to resolve master hostname: " << master_host << std::endl;
            close(master_fd);
            return;
        }
        memcpy(&master_addr.sin_addr, host_entry->h_addr_list[0], host_entry->h_length);
    }
    
    if (connect(master_fd, (struct sockaddr*)&master_addr, sizeof(master_addr)) < 0) {
        std::cerr << "Failed to connect to master at " << master_host << ":" << master_port << std::endl;
        close(master_fd);
        return;
    }
    
    std::cout << "Connected to master successfully" << std::endl;
    
    std::string ping_command = "*1\r\n$4\r\nPING\r\n";
    if (send(master_fd, ping_command.c_str(), ping_command.length(), 0) < 0) {
        std::cerr << "Failed to send PING to master" << std::endl;
        close(master_fd);
        return;
    }
    
    std::cout << "Sent PING to master" << std::endl;
    
    char response_buffer[4096];
    int bytes_received = recv(master_fd, response_buffer, sizeof(response_buffer) - 1, 0);
    if (bytes_received <= 0) {
        std::cerr << "Failed to receive PONG from master" << std::endl;
        close(master_fd);
        return;
    }
    response_buffer[bytes_received] = '\0';
    std::cout << "Received response from master: " << response_buffer << std::endl;
    
    std::string port_str = std::to_string(server_port);
    std::string replconf_port_command = "*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$" + 
                                       std::to_string(port_str.length()) + "\r\n" + port_str + "\r\n";
    
    if (send(master_fd, replconf_port_command.c_str(), replconf_port_command.length(), 0) < 0) {
        std::cerr << "Failed to send REPLCONF listening-port to master" << std::endl;
        close(master_fd);
        return;
    }
    
    std::cout << "Sent REPLCONF listening-port " << server_port << " to master" << std::endl;
    
    bytes_received = recv(master_fd, response_buffer, sizeof(response_buffer) - 1, 0);
    if (bytes_received <= 0) {
        std::cerr << "Failed to receive OK from master for REPLCONF listening-port" << std::endl;
        close(master_fd);
        return;
    }
    response_buffer[bytes_received] = '\0';
    std::cout << "Received response to REPLCONF listening-port: " << response_buffer << std::endl;
    
    std::string replconf_capa_command = "*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n";
    
    if (send(master_fd, replconf_capa_command.c_str(), replconf_capa_command.length(), 0) < 0) {
        std::cerr << "Failed to send REPLCONF capa psync2 to master" << std::endl;
        close(master_fd);
        return;
    }
    
    std::cout << "Sent REPLCONF capa psync2 to master" << std::endl;
    
    bytes_received = recv(master_fd, response_buffer, sizeof(response_buffer) - 1, 0);
    if (bytes_received <= 0) {
        std::cerr << "Failed to receive OK from master for REPLCONF capa" << std::endl;
        close(master_fd);
        return;
    }
    response_buffer[bytes_received] = '\0';
    std::cout << "Received response to REPLCONF capa: " << response_buffer << std::endl;
    
    std::string psync_command = "*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n";
    
    if (send(master_fd, psync_command.c_str(), psync_command.length(), 0) < 0) {
        std::cerr << "Failed to send PSYNC to master" << std::endl;
        close(master_fd);
        return;
    }
    
    std::cout << "Sent PSYNC ? -1 to master" << std::endl;
    
    bytes_received = recv(master_fd, response_buffer, sizeof(response_buffer) - 1, 0);
    if (bytes_received <= 0) {
        std::cerr << "Failed to receive FULLRESYNC from master" << std::endl;
        close(master_fd);
        return;
    }
    response_buffer[bytes_received] = '\0';
    std::cout << "Received response to PSYNC: " << response_buffer << std::endl;
    
    std::string current_data(response_buffer, bytes_received);
    
    size_t rdb_start = current_data.find("$");
    if (rdb_start != std::string::npos) {
        size_t crlf_pos = current_data.find("\r\n", rdb_start);
        if (crlf_pos != std::string::npos) {
            std::string size_str = current_data.substr(rdb_start + 1, crlf_pos - rdb_start - 1);
            int rdb_size = std::stoi(size_str);
            std::cout << "Expecting RDB file of size: " << rdb_size << " bytes" << std::endl;
            
            size_t data_start = crlf_pos + 2;
            int remaining_rdb_bytes = rdb_size - (bytes_received - data_start);
            
            while (remaining_rdb_bytes > 0) {
                bytes_received = recv(master_fd, response_buffer, sizeof(response_buffer), 0);
                if (bytes_received <= 0) {
                    std::cerr << "Failed to read RDB data from master" << std::endl;
                    close(master_fd);
                    return;
                }
                remaining_rdb_bytes -= bytes_received;
            }
            std::cout << "Successfully received and discarded RDB file" << std::endl;
        }
    }
    
    std::cout << "Handshake completed successfully. Now listening for propagated commands..." << std::endl;
    
    std::string command_buffer;
    
    while (true) {
    bytes_received = recv(master_fd, response_buffer, sizeof(response_buffer), 0);
    if (bytes_received <= 0) {
        std::cout << "Master connection closed or error occurred" << std::endl;
        break;
    }
    
    // Add received data to buffer
    command_buffer.append(response_buffer, bytes_received);
    
    // Process all complete commands in the buffer
    size_t pos = 0;
    while (pos < command_buffer.length()) {
        // Skip any whitespace or non-command characters
        while (pos < command_buffer.length() && command_buffer[pos] != '*') {
            pos++;
        }
        
        if (pos >= command_buffer.length()) {
            break; // No more commands
        }
        
        try {
            // Parse RESP command starting at pos
            size_t command_start = pos;
            std::vector<std::string> parsed_command;
            
            // Parse array length
            size_t crlf_pos = command_buffer.find("\r\n", pos);
            if (crlf_pos == std::string::npos) {
                break; // Incomplete command
            }
            
            int num_elements = std::stoi(command_buffer.substr(pos + 1, crlf_pos - pos - 1));
            pos = crlf_pos + 2;
            
            bool complete_command = true;
            
            // Parse each element
            for (int i = 0; i < num_elements; i++) {
                if (pos >= command_buffer.length() || command_buffer[pos] != '$') {
                    complete_command = false;
                    break;
                }
                
                size_t len_crlf = command_buffer.find("\r\n", pos);
                if (len_crlf == std::string::npos) {
                    complete_command = false;
                    break;
                }
                
                int str_len = std::stoi(command_buffer.substr(pos + 1, len_crlf - pos - 1));
                pos = len_crlf + 2;
                
                if (pos + str_len + 2 > command_buffer.length()) {
                    complete_command = false;
                    break;
                }
                
                parsed_command.push_back(command_buffer.substr(pos, str_len));
                pos += str_len + 2; // Skip the string and \r\n
            }
            
            if (!complete_command) {
                break; // Wait for more data
            }
            
            // Calculate the exact number of bytes for this command
            int command_bytes = pos - command_start;
            
            std::cout << "Received propagated command from master (" << command_bytes << " bytes): ";
            for (const auto& arg : parsed_command) {
                std::cout << "'" << arg << "' ";
            }
            std::cout << std::endl;
            
            // Execute the command and get response
            std::string response = execute_replica_command(parsed_command, command_bytes);
            
            if (!response.empty()) {
                std::cout << "Sending response to master: " << response;
                if (send(master_fd, response.c_str(), response.length(), 0) < 0) {
                    std::cerr << "Failed to send response to master" << std::endl;
                    close(master_fd);
                    return;
                }
            }
            
        } catch (const std::exception& e) {
            std::cout << "Parse error: " << e.what() << ", skipping to next position" << std::endl;
            pos++;
        }
    }
    
    // Remove processed commands from buffer
    if (pos > 0) {
        command_buffer = command_buffer.substr(pos);
    }
}
    
    close(master_fd);
}

int main(int argc, char **argv) {
    std::cout << std::unitbuf;
    std::cerr << std::unitbuf;

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
            } else if (arg == "--replicaof") {
                std::string replicaof_arg = argv[i + 1];
                size_t space_pos = replicaof_arg.find(' ');
                if (space_pos == std::string::npos) {
                    std::cerr << "Invalid --replicaof format. Expected: --replicaof \"<host> <port>\"" << std::endl;
                    return 1;
                }
                
                master_host = replicaof_arg.substr(0, space_pos);
                std::string master_port_str = replicaof_arg.substr(space_pos + 1);
                
                try {
                    master_port = std::stoi(master_port_str);
                    if (master_port <= 0 || master_port > 65535) {
                        std::cerr << "Invalid master port number: " << master_port_str << std::endl;
                        return 1;
                    }
                    is_replica = true;
                    std::cout << "Running in replica mode. Master: " << master_host << ":" << master_port << std::endl;
                } catch (const std::exception& e) {
                    std::cerr << "Invalid master port number: " << master_port_str << std::endl;
                    return 1;
                }
            }
        }
    }

    if (is_replica) {
        std::cout << "Starting Redis replica server on port " << server_port 
                  << " (master: " << master_host << ":" << master_port << ")" << std::endl;
    } else {
        std::cout << "Starting Redis master server on port " << server_port << std::endl;
    }

    load_rdb_file();

    if (is_replica) {
    std::thread master_connection_thread(handle_master_connection);
    master_connection_thread.detach();
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
    server_addr.sin_port = htons(server_port);

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
                    replica_info.erase(client_fd);
                    connected_replicas.erase(client_fd);
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