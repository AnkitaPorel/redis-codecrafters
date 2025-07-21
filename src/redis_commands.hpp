#ifndef REDIS_COMMANDS_HPP
#define REDIS_COMMANDS_HPP

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
#include <unordered_set>
#include <netdb.h>
#include <sys/select.h>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <future>
#include <fcntl.h>
#include <atomic>
#include <signal.h>

#include "redis_parser.hpp"
#include "rdb_parser.hpp"

struct ServerConfig {
    std::string dir;
    std::string dbfilename;

    ServerConfig() : dir("/tmp/redis-files"), dbfilename("dump.rdb") {}
};

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

struct StreamEntry {
    std::string id;
    std::map<std::string, std::string> fields;
    
    StreamEntry(const std::string& entry_id) : id(entry_id) {}
};

struct StreamData {
    std::vector<StreamEntry> entries;
    std::chrono::steady_clock::time_point expiry;
    bool has_expiry;
    
    StreamData() : has_expiry(false) {}
    StreamData(std::chrono::steady_clock::time_point exp) : expiry(exp), has_expiry(true) {}
};

struct BlockedClient {
    int fd;
    std::string stream_key;
    std::string last_id;
    std::chrono::steady_clock::time_point expiry;
};

std::unordered_set<int> clients_in_multi;
std::mutex multi_mutex;

std::map<int, std::vector<std::vector<std::string>>> queued_commands;
std::mutex queue_mutex;

std::atomic<bool> shutdown_server(false);

std::mutex blocked_clients_mutex;

std::vector<BlockedClient> blocked_clients;

std::map<std::string, StreamData> stream_store;

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
            command == "SADD" || command == "SREM" || command == "HSET" || command == "HDEL" || command == "XADD");
}

std::string command_to_resp_array(const std::vector<std::string>& command) {
    std::string resp = "*" + std::to_string(command.size()) + "\r\n";
    for (const std::string& arg : command) {
        resp += "$" + std::to_string(arg.length()) + "\r\n" + arg + "\r\n";
    }
    return resp;
}

std::string generate_stream_id() {
    auto now = std::chrono::system_clock::now();
    auto duration = now.time_since_epoch();
    auto millis = std::chrono::duration_cast<std::chrono::milliseconds>(duration).count();
    
    static long long last_ms = 0;
    static long long seq = 0;
    
    if (millis == last_ms) {
        seq++;
    } else {
        last_ms = millis;
        seq = 0;
    }
    
    return std::to_string(last_ms) + "-" + std::to_string(seq);
}

void check_blocked_clients_timeout() {
    while (!shutdown_server) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        
        auto now = std::chrono::steady_clock::now();
        std::vector<BlockedClient> timed_out;
        
        {
            std::lock_guard<std::mutex> lock(blocked_clients_mutex);
            for (auto it = blocked_clients.begin(); it != blocked_clients.end();) {
                if (now >= it->expiry) {
                    timed_out.push_back(*it);
                    it = blocked_clients.erase(it);
                } else {
                    ++it;
                }
            }
        }
        
        for (const auto& client : timed_out) {
            if (!shutdown_server) {
                std::string response = "*-1\r\n";
                send(client.fd, response.c_str(), response.length(), 0);
            }
        }
    }
}

void handle_signal(int signum) {
    shutdown_server = true;
}

bool parse_stream_id(const std::string& id, long long& ms, long long& seq) {
    size_t dash_pos = id.find('-');
    if (dash_pos == std::string::npos || dash_pos == 0 || dash_pos == id.length() - 1) {
        return false;
    }
    
    try {
        ms = std::stoll(id.substr(0, dash_pos));
        seq = std::stoll(id.substr(dash_pos + 1));
    } catch (const std::exception& e) {
        return false;
    }
    
    return true;
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

long long get_current_timestamp_ms() {
    auto now = std::chrono::system_clock::now();
    auto duration = now.time_since_epoch();
    return std::chrono::duration_cast<std::chrono::milliseconds>(duration).count();
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

long long generate_sequence_number(long long ms, const StreamData& stream) {
    if (ms == 0) {
        if (stream.entries.empty()) {
            return 1;
        }
    } else {
        if (stream.entries.empty()) {
            return 0;
        }
    }

    long long max_seq = (ms == 0) ? 0 : -1;
    
    for (const auto& entry : stream.entries) {
        long long entry_ms, entry_seq;
        if (parse_stream_id(entry.id, entry_ms, entry_seq)) {
            if (entry_ms == ms && entry_seq > max_seq) {
                max_seq = entry_seq;
            }
        }
    }
    
    return max_seq + 1;
}

std::pair<bool, std::string> validate_or_generate_id(const std::string& id_spec, StreamData& stream) {
    size_t dash_pos = id_spec.find('-');
    if (dash_pos == std::string::npos || dash_pos == 0 || dash_pos == id_spec.length() - 1) {
        return {false, ""};
    }
    
    std::string ms_part = id_spec.substr(0, dash_pos);
    std::string seq_part = id_spec.substr(dash_pos + 1);
    
    long long ms;
    try {
        ms = std::stoll(ms_part);
    } catch (const std::exception& e) {
        return {false, ""};
    }
    
    if (seq_part == "*") {
        long long seq = generate_sequence_number(ms, stream);
        return {true, ms_part + "-" + std::to_string(seq)};
    }
    
    try {
        long long seq = std::stoll(seq_part);
        if (ms < 0 || seq <= 0) {
            return {false, ""};
        }
        
        if (stream.entries.empty()) {
            if (ms > 0 || (ms == 0 && seq > 0)) {
                return {true, id_spec};
            }
            return {false, ""};
        }
        
        const std::string& last_id = stream.entries.back().id;
        long long last_ms, last_seq;
        if (!parse_stream_id(last_id, last_ms, last_seq)) {
            return {false, ""};
        }
        
        if (ms > last_ms || (ms == last_ms && seq > last_seq)) {
            return {true, id_spec};
        }
        return {false, ""};
    } catch (const std::exception& e) {
        return {false, ""};
    }
}

#endif