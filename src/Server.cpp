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
#include <mutex>
#include <condition_variable>
#include <future>
#include <atomic>

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

std::map<int, int> replica_offsets;
std::mutex offset_mutex;
int master_offset = 0;

std::mutex wait_mutex;
std::condition_variable wait_cv;
std::map<int, int> replica_ack_offsets;
std::atomic<int> pending_wait_offset{0};
std::atomic<int> expected_replicas{0};
std::atomic<int> acked_replicas{0};

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
    int cmd_size = resp_command.size();
    
    {
        std::lock_guard<std::mutex> lock(offset_mutex);
        master_offset += cmd_size;
    }
    
    std::cout << "Propagating command (" << cmd_size << " bytes) to " 
              << connected_replicas.size() << " replicas" << std::endl;
    
    for (auto it = connected_replicas.begin(); it != connected_replicas.end();) {
        int replica_fd = *it;
        ssize_t bytes_sent = send(replica_fd, resp_command.c_str(), resp_command.length(), MSG_NOSIGNAL);
        
        if (bytes_sent < 0) {
            std::lock_guard<std::mutex> lock(offset_mutex);
            replica_offsets.erase(replica_fd);
            replica_info.erase(replica_fd);
            replica_ack_offsets.erase(replica_fd);
            it = connected_replicas.erase(it);
            std::cout << "Replica (fd: " << replica_fd << ") disconnected" << std::endl;
        } else {
            // Initialize replica ACK offset if not exists
            {
                std::lock_guard<std::mutex> lock(wait_mutex);
                if (replica_ack_offsets.find(replica_fd) == replica_ack_offsets.end()) {
                    replica_ack_offsets[replica_fd] = 0;
                }
            }
            ++it;
        }
    }
}

void send_getack_to_replicas() {
    if (connected_replicas.empty()) {
        return;
    }
    
    std::string getack_command = "*3\r\n$8\r\nREPLCONF\r\n$6\r\nGETACK\r\n$1\r\n*\r\n";
    
    std::cout << "Sending GETACK to " << connected_replicas.size() << " replicas" << std::endl;
    
    for (auto it = connected_replicas.begin(); it != connected_replicas.end();) {
        int replica_fd = *it;
        ssize_t bytes_sent = send(replica_fd, getack_command.c_str(), getack_command.length(), MSG_NOSIGNAL);
        
        if (bytes_sent < 0) {
            std::lock_guard<std::mutex> lock(offset_mutex);
            replica_offsets.erase(replica_fd);
            replica_info.erase(replica_fd);
            {
                std::lock_guard<std::mutex> wait_lock(wait_mutex);
                replica_ack_offsets.erase(replica_fd);
            }
            it = connected_replicas.erase(it);
            std::cout << "Replica (fd: " << replica_fd << ") disconnected while sending GETACK" << std::endl;
        } else {
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

void connect_to_master() {
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
    
    char response_buffer[1024];
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
    
    std::cout << "Handshake completed successfully" << std::endl;
    
    close(master_fd);
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
        
        if (connected_replicas.find(client_fd) == connected_replicas.end()) {
            propagate_to_replicas(parsed_command);
        }
        
    } else if (command == "GET" && parsed_command.size() == 2) {
        std::string key = parsed_command[1];
        std::cout << "GET request for key: '" << key << "'" << std::endl;
        
        auto it = kv_store.find(key);
        std::string response;
        if (it != kv_store.end()) {
            if (it->second.has_expiry && get_current_time() > it->second.expiry) {
                std::cout << "Key '" << key << "' has expired, removing from store" << std::endl;
                kv_store.erase(it);
                response = "$-1\r\n";
            } else {
                std::string value = it->second.value;
                std::cout << "Found key '" << key << "' with value: '" << value << "'" << std::endl;
                response = "$" + std::to_string(value.length()) + "\r\n" + value + "\r\n";
            }
        } else {
            std::cout << "Key '" << key << "' not found in store" << std::endl;
            response = "$-1\r\n";
        }
        std::cout << "Sending response: " << response << std::endl;
        send(client_fd, response.c_str(), response.length(), 0);
    } else if (command == "KEYS" && parsed_command.size() == 2) {
        std::string pattern = parsed_command[1];
        
        if (pattern == "*") {
            auto current_time = get_current_time();
            for (auto it = kv_store.begin(); it != kv_store.end();) {
                if (it->second.has_expiry && current_time > it->second.expiry) {
                    it = kv_store.erase(it);
                } else {
                    ++it;
                }
            }
            
            std::string response = "*" + std::to_string(kv_store.size()) + "\r\n";
            for (const auto& pair : kv_store) {
                const std::string& key = pair.first;
                response += "$" + std::to_string(key.length()) + "\r\n" + key + "\r\n";
            }
            send(client_fd, response.c_str(), response.length(), 0);
        } else {
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
        
        for (char& c : section) {
            c = std::tolower(c);
        }
        
        if (section == "replication") {
            std::string info_content;
            if (is_replica) {
                info_content = "role:slave";
            } else {
                info_content = "role:master\r\nmaster_replid:" + master_replid + "\r\nmaster_repl_offset:" + std::to_string(master_repl_offset);
            }
            std::string response = "$" + std::to_string(info_content.length()) + "\r\n" + info_content + "\r\n";
            send(client_fd, response.c_str(), response.length(), 0);
        } else {
            std::string response = "-ERR unsupported INFO section\r\n";
            send(client_fd, response.c_str(), response.length(), 0);
        }
    } else if (command == "REPLCONF" && parsed_command.size() >= 3) {
        std::string subcommand = parsed_command[1];
    
    for (char& c : subcommand) {
        c = std::tolower(c);
    }
    
    if (subcommand == "listening-port" && parsed_command.size() == 3) {
        std::string port = parsed_command[2];
        replica_info[client_fd]["listening-port"] = port;
        std::cout << "Replica (fd: " << client_fd << ") listening on port: " << port << std::endl;
        
        std::string response = "+OK\r\n";
        send(client_fd, response.c_str(), response.length(), 0);
    } else if (subcommand == "capa" && parsed_command.size() == 3) {
        std::string capability = parsed_command[2];
        replica_info[client_fd]["capa"] = capability;
        std::cout << "Replica (fd: " << client_fd << ") capability: " << capability << std::endl;
        
        std::string response = "+OK\r\n";
        send(client_fd, response.c_str(), response.length(), 0);
    } else if (subcommand == "ack" && parsed_command.size() == 3) {
        try {
        int ack_offset = std::stoi(parsed_command[2]);
        std::cout << "Received ACK from replica (fd: " << client_fd << ") with offset: " << ack_offset << std::endl;
        
        {
            std::lock_guard<std::mutex> wait_lock(wait_mutex);
            replica_ack_offsets[client_fd] = ack_offset;
            
            // Check if this replica has processed all pending commands
            if (ack_offset >= pending_wait_offset && pending_wait_offset > 0) {
                acked_replicas++;
                std::cout << "Replica " << client_fd << " acknowledged. Total acked: " << acked_replicas.load() << std::endl;
                wait_cv.notify_all();
            }
        }
    } catch (const std::exception& e) {
        std::cerr << "Invalid ACK offset: " << parsed_command[2] << std::endl;
    }
    } else {
        std::string response = "-ERR unsupported REPLCONF subcommand\r\n";
        send(client_fd, response.c_str(), response.length(), 0);
    }
    } else if (command == "PSYNC" && parsed_command.size() == 3) {
        std::string repl_id = parsed_command[1];
        std::string offset = parsed_command[2];
        
        std::cout << "Received PSYNC from replica (fd: " << client_fd << ") with repl_id: " 
                  << repl_id << " and offset: " << offset << std::endl;
        
        if (repl_id == "?" && offset == "-1") {
            std::string response = "+FULLRESYNC " + master_replid + " " + std::to_string(master_repl_offset) + "\r\n";
            std::cout << "Sending FULLRESYNC response: " << response;
            send(client_fd, response.c_str(), response.length(), 0);
            
            std::string rdb_hex = "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2";
            
            std::vector<uint8_t> rdb_data;
            for (size_t i = 0; i < rdb_hex.length(); i += 2) {
                std::string hex_byte = rdb_hex.substr(i, 2);
                uint8_t byte = static_cast<uint8_t>(std::stoul(hex_byte, nullptr, 16));
                rdb_data.push_back(byte);
            }
            
            std::string rdb_header = "$" + std::to_string(rdb_data.size()) + "\r\n";
            std::cout << "Sending RDB file header: " << rdb_header;
            send(client_fd, rdb_header.c_str(), rdb_header.length(), 0);
            
            std::cout << "Sending RDB file data (" << rdb_data.size() << " bytes)" << std::endl;
            send(client_fd, rdb_data.data(), rdb_data.size(), 0);
            
            connected_replicas.insert(client_fd);
            
            {
                std::lock_guard<std::mutex> wait_lock(wait_mutex);
                replica_ack_offsets[client_fd] = 0;
            }
    
            std::cout << "Replica (fd: " << client_fd << ") handshake completed. Now tracking for command propagation." << std::endl;
        } else {
            std::string response = "-ERR partial sync not supported\r\n";
            send(client_fd, response.c_str(), response.length(), 0);
        }
    } else if (command == "WAIT" && parsed_command.size() == 3) {
        try {
        int num_replicas_expected = std::stoi(parsed_command[1]);
        int timeout_ms = std::stoi(parsed_command[2]);
        
        // ... [initial checks] ...

        // Reset counters and set expectations
        {
            std::lock_guard<std::mutex> wait_lock(wait_mutex);
            pending_wait_offset = master_offset;
            expected_replicas = num_replicas_expected;
            acked_replicas = 0;
            
            // Initialize ack offsets
            for (int replica_fd : connected_replicas) {
                if (replica_ack_offsets.find(replica_fd) == replica_ack_offsets.end()) {
                    replica_ack_offsets[replica_fd] = 0;
                }
            }
        }
        
        send_getack_to_replicas();
        
        // Non-blocking wait with periodic checks
        int final_acked = 0;
        auto start_time = std::chrono::steady_clock::now();
        auto timeout_time = start_time + std::chrono::milliseconds(timeout_ms);
        
        while (true) {
            // Check current ACK count
            {
                std::lock_guard<std::mutex> lock(wait_mutex);
                final_acked = acked_replicas;
            }
            
            if (final_acked >= num_replicas_expected) {
                std::cout << "WAIT completed: " << final_acked << std::endl;
                break;
            }
            
            if (std::chrono::steady_clock::now() >= timeout_time) {
                std::cout << "WAIT timed out: " << final_acked << std::endl;
                break;
            }
            
            // Process network events while waiting
            struct timeval tv {
                .tv_sec = 0,
                .tv_usec = 10000  // 10ms
            };
            
            fd_set read_fds;
            FD_ZERO(&read_fds);
            int max_fd = -1;
            
            // Add replica sockets
            for (int fd : connected_replicas) {
                FD_SET(fd, &read_fds);
                if (fd > max_fd) max_fd = fd;
            }
            
            select(max_fd + 1, &read_fds, nullptr, nullptr, &tv);
            
            // Process incoming data from replicas
            for (int fd : connected_replicas) {
                if (FD_ISSET(fd, &read_fds)) {
                    char buffer[4096];
                    int bytes = recv(fd, buffer, sizeof(buffer), 0);
                    if (bytes > 0) {
                        std::vector<std::string> cmd;
                        parse_redis_command(std::string(buffer, bytes), cmd);
                        execute_redis_command(fd, cmd);
                    }
                }
            }
        }
        
        std::string response = ":" + std::to_string(final_acked) + "\r\n";
        send(client_fd, response.c_str(), response.length(), 0);
    } 
    } catch (const std::exception& e) {
        std::cerr << "Invalid WAIT command arguments" << std::endl;
        std::string response = "-ERR invalid arguments\r\n";
        send(client_fd, response.c_str(), response.length(), 0);
    }
     else {
        std::string response = "-ERR unknown command or wrong number of arguments\r\n";
        send(client_fd, response.c_str(), response.length(), 0);
    }
}

std::string execute_replica_command(const std::vector<std::string>& parsed_command, int bytes_processed) {
    std::string response;

    if (!parsed_command.empty() && parsed_command[0] == "REPLCONF" && 
        parsed_command.size() >= 2 && parsed_command[1] == "GETACK") {
        {
            std::lock_guard<std::mutex> lock(offset_mutex);
            replica_offset += bytes_processed;
            
            std::string offset_str = std::to_string(replica_offset);
            response = "*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n$" + 
                      std::to_string(offset_str.length()) + "\r\n" + offset_str + "\r\n";
            
            std::cout << "Replica: Sending ACK with offset " 
                      << replica_offset << std::endl;
        }
        return response;
    }

    if (parsed_command.empty()) {
        return "";
    }

    std::string command = parsed_command[0];
    
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
                        std::cout << "Replica: SET '" << key << "' = '" << value 
                                  << "' with expiry " << expiry_ms << "ms" << std::endl;
                    }
                } catch (const std::exception& e) {
                    std::cerr << "Replica: Invalid expiry time in SET command" << std::endl;
                }
            }
        }
    } 
    else if (command == "DEL" && parsed_command.size() >= 2) {
        for (size_t i = 1; i < parsed_command.size(); i++) {
            std::string key = parsed_command[i];
            kv_store.erase(key);
            std::cout << "Replica: DEL '" << key << "'" << std::endl;
        }
    } 
    else if (command == "PING") {
        std::cout << "Replica: Received PING" << std::endl;
    }
    
    {
        std::lock_guard<std::mutex> lock(offset_mutex);
        replica_offset += bytes_processed;
        std::cout << "Replica: Updated offset to " 
                  << replica_offset << std::endl;
    }
    
    return "";
}

void handle_master_connection() {
    if (!is_replica) {
        return;
    }

    replica_offset = 0;

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

    std::string command_buffer;
    size_t rdb_start = std::string(response_buffer).find("$");
    if (rdb_start != std::string::npos) {
        size_t crlf_pos = std::string(response_buffer).find("\r\n", rdb_start);
        if (crlf_pos != std::string::npos) {
            std::string size_str = std::string(response_buffer).substr(rdb_start + 1, crlf_pos - rdb_start - 1);
            int rdb_size = std::stoi(size_str);
            std::cout << "Expecting RDB file of size: " << rdb_size << " bytes" << std::endl;

            size_t data_start = crlf_pos + 2;
            int rdb_data_bytes_in_buffer = bytes_received - data_start;
            if (rdb_data_bytes_in_buffer > rdb_size) {
                rdb_data_bytes_in_buffer = rdb_size;
            }

            if (bytes_received > static_cast<int>(data_start + rdb_size)) {
                size_t extra_start = data_start + rdb_size;
                size_t extra_length = bytes_received - extra_start;
                command_buffer.append(response_buffer + extra_start, extra_length);
                std::cout << "Saved " << extra_length << " bytes after RDB file for command processing" << std::endl;
            }

            int remaining_rdb_bytes = rdb_size - rdb_data_bytes_in_buffer;
            while (remaining_rdb_bytes > 0) {
                bytes_received = recv(master_fd, response_buffer, 
                                    std::min(static_cast<int>(sizeof(response_buffer)), remaining_rdb_bytes), 0);
                if (bytes_received <= 0) {
                    std::cerr << "Failed to read RDB data from master" << std::endl;
                    close(master_fd);
                    return;
                }
                
                if (bytes_received > remaining_rdb_bytes) {
                    command_buffer.append(response_buffer + remaining_rdb_bytes, bytes_received - remaining_rdb_bytes);
                    std::cout << "Saved " << (bytes_received - remaining_rdb_bytes) 
                              << " extra bytes from RDB read" << std::endl;
                }
                
                remaining_rdb_bytes -= bytes_received;
            }
            std::cout << "Successfully received and discarded RDB file" << std::endl;
        }
    }

    replica_offset = 0;
    std::cout << "Handshake completed successfully. Now listening for propagated commands..." << std::endl;

    if (!command_buffer.empty()) {
        std::cout << "Processing " << command_buffer.length() 
                  << " bytes of saved commands" << std::endl;
    }

    while (true) {
        size_t pos = 0;
        while (pos < command_buffer.length()) {
            while (pos < command_buffer.length() && command_buffer[pos] != '*') {
                pos++;
            }

            if (pos >= command_buffer.length()) {
                break;
            }

            try {
                size_t command_start = pos;
                std::vector<std::string> parsed_command;

                // Parse array length
                size_t crlf_pos = command_buffer.find("\r\n", pos);
                if (crlf_pos == std::string::npos) {
                    break;
                }

                int num_elements = std::stoi(command_buffer.substr(pos + 1, crlf_pos - pos - 1));
                pos = crlf_pos + 2;

                bool complete_command = true;

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
                    pos += str_len + 2;
                }

                if (!complete_command) {
                    break;
                }

                int command_bytes = pos - command_start;

                std::cout << "Received propagated command from master (" << command_bytes << " bytes): ";
                for (const auto& arg : parsed_command) {
                    std::cout << "'" << arg << "' ";
                }
                std::cout << std::endl;

                if (!parsed_command.empty() && parsed_command[0] == "REPLCONF" && 
                    parsed_command.size() >= 2 && parsed_command[1] == "GETACK") {
                    
                    std::string offset_str = std::to_string(replica_offset);
                    std::string response = "*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n$" + 
                                         std::to_string(offset_str.length()) + "\r\n" + offset_str + "\r\n";

                    std::cout << "Sending response to master: " << response;
                    if (send(master_fd, response.c_str(), response.length(), 0) < 0) {
                        std::cerr << "Failed to send response to master" << std::endl;
                        close(master_fd);
                        return;
                    }

                    replica_offset += command_bytes;
                    std::cout << "Updated offset to " << replica_offset << " after processing " << command_bytes << " bytes" << std::endl;
                } else {
                    execute_replica_command(parsed_command, command_bytes);
                }
            } catch (const std::exception& e) {
                std::cout << "Parse error: " << e.what() << ", skipping to next position" << std::endl;
                pos++;
            }
        }

        if (pos > 0) {
            command_buffer = command_buffer.substr(pos);
        }

        bytes_received = recv(master_fd, response_buffer, sizeof(response_buffer), 0);
        if (bytes_received <= 0) {
            std::cout << "Master connection closed or error occurred" << std::endl;
            break;
        }

        command_buffer.append(response_buffer, bytes_received);
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
                    {
                        std::lock_guard<std::mutex> wait_lock(wait_mutex);
                        replica_ack_offsets.erase(client_fd);
                    }
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