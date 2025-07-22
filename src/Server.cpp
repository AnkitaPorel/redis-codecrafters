#include "redis_commands.hpp"

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

    if (parsed_command.size() > 0 && parsed_command[0] == "FLUSHALL") {
        kv_store.clear();
        list_store.clear();
        stream_store.clear();
        std::string response = "+OK\r\n";
        send(client_fd, response.c_str(), response.length(), 0);
        return;
    }

    if (parsed_command[0] == "DISCARD" && parsed_command.size() == 1) {
        std::lock_guard<std::mutex> lock(multi_mutex);
        if (clients_in_multi.find(client_fd) == clients_in_multi.end()) {
            std::string response = "-ERR DISCARD without MULTI\r\n";
            send(client_fd, response.c_str(), response.length(), 0);
            return;
        }
        clients_in_multi.erase(client_fd);
        {
            std::lock_guard<std::mutex> qlock(queue_mutex);
            queued_commands[client_fd].clear();
        }
        std::string response = "+OK\r\n";
        send(client_fd, response.c_str(), response.length(), 0);
        return;
    }

    bool in_multi = false;
    {
        std::lock_guard<std::mutex> lock(multi_mutex);
        in_multi = clients_in_multi.find(client_fd) != clients_in_multi.end();
    }

    if (in_multi && parsed_command[0] != "MULTI" && parsed_command[0] != "EXEC") {
        std::lock_guard<std::mutex> qlock(queue_mutex);
        queued_commands[client_fd].push_back(parsed_command);
        std::string response = "+QUEUED\r\n";
        send(client_fd, response.c_str(), response.length(), 0);
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
            
                std::cout << "WAIT command: expecting " << num_replicas_expected 
                    << " replicas, timeout " << timeout_ms << "ms" << std::endl;
            
                int current_connected = connected_replicas.size();
            
                if (current_connected == 0) {
                    std::string response = ":0\r\n";
                    send(client_fd, response.c_str(), response.length(), 0);
                    std::cout << "No replicas connected, returning 0" << std::endl;
                    return;
                }
            
                if (master_offset == 0) {
                    std::string response = ":" + std::to_string(current_connected) + "\r\n";
                    send(client_fd, response.c_str(), response.length(), 0);
                    std::cout << "No writes to replicate, returning " << current_connected << std::endl;
                    return;
                }
            
                {
                    std::lock_guard<std::mutex> wait_lock(wait_mutex);
                    pending_wait_offset = master_offset;
                    expected_replicas = num_replicas_expected;
                    acked_replicas = 0;
                
                    for (int replica_fd : connected_replicas) {
                        if (replica_ack_offsets.find(replica_fd) == replica_ack_offsets.end()) {
                            replica_ack_offsets[replica_fd] = 0;
                        }
                    }
                }
            
                send_getack_to_replicas();
            
                int final_acked = 0;
                auto start_time = std::chrono::steady_clock::now();
                auto timeout_time = start_time + std::chrono::milliseconds(timeout_ms);
                std::set<int> replicas_to_remove;
            
                while (true) {
                    {
                        std::lock_guard<std::mutex> lock(wait_mutex);
                        final_acked = acked_replicas;
                    }
                
                    if (final_acked >= num_replicas_expected) {
                        std::cout << "WAIT completed: " << final_acked << std::endl;
                        break;
                    }
                
                    auto now = std::chrono::steady_clock::now();
                    if (now >= timeout_time) {
                        std::cout << "WAIT timed out: " << final_acked << std::endl;
                        break;
                    }
                
                    auto remaining_time = timeout_time - now;
                    auto remaining_us = std::chrono::duration_cast<std::chrono::microseconds>(remaining_time).count();
                    struct timeval tv;
                    tv.tv_sec = remaining_us / 1000000;
                    tv.tv_usec = remaining_us % 1000000;

                    if (tv.tv_sec > 0 || tv.tv_usec > 10000) {
                        tv.tv_sec = 0;
                        tv.tv_usec = 10000;
                    }
                
                    fd_set read_fds;
                    FD_ZERO(&read_fds);
                    int max_fd = -1;
                
                    for (int fd : connected_replicas) {
                        FD_SET(fd, &read_fds);
                        if (fd > max_fd) max_fd = fd;
                    }
                
                    int n = select(max_fd + 1, &read_fds, nullptr, nullptr, &tv);
                    if (n < 0) {
                        std::cerr << "select error in WAIT: " << strerror(errno) << std::endl;
                        continue;
                    }
                
                    replicas_to_remove.clear();
                    for (int fd : connected_replicas) {
                        if (FD_ISSET(fd, &read_fds)) {
                            char buffer[4096];
                            int bytes = recv(fd, buffer, sizeof(buffer), 0);
                            if (bytes > 0) {
                                std::vector<std::string> cmd;
                                parse_redis_command(std::string(buffer, bytes), cmd);
                                execute_redis_command(fd, cmd);
                            } else if (bytes <= 0) {
                                std::cout << "Replica (fd: " << fd << ") disconnected during WAIT" << std::endl;
                                replicas_to_remove.insert(fd);
                            
                                {
                                    std::lock_guard<std::mutex> lock(wait_mutex);
                                    if (replica_ack_offsets[fd] >= pending_wait_offset) {
                                        acked_replicas--;
                                    }
                                }
                            }
                        }
                    }
                
                    for (int fd : replicas_to_remove) {
                        connected_replicas.erase(fd);
                        replica_info.erase(fd);
                        {
                            std::lock_guard<std::mutex> lock(wait_mutex);
                            replica_ack_offsets.erase(fd);
                        }
                    }
                }
            
                {
                    std::lock_guard<std::mutex> lock(wait_mutex);
                    final_acked = acked_replicas;
                }
            
                std::string response = ":" + std::to_string(final_acked) + "\r\n";
                send(client_fd, response.c_str(), response.length(), 0); 
            } catch (const std::exception& e) {
                std::cerr << "Invalid WAIT command arguments" << std::endl;
                std::string response = "-ERR invalid arguments\r\n";
                send(client_fd, response.c_str(), response.length(), 0);
            }
    } else if (command == "XADD") {
        if (parsed_command.size() < 5 || (parsed_command.size() % 2 == 0)) {
            std::string response = "-ERR wrong number of arguments for XADD\r\n";
            send(client_fd, response.c_str(), response.length(), 0);
            return;
        }

        std::string stream_key = parsed_command[1];
        std::string entry_id = parsed_command[2];

        if (entry_id == "*") {
            entry_id = generate_stream_id();
        } else {
            size_t dash_pos = entry_id.find('-');
            if (dash_pos != std::string::npos && entry_id.substr(dash_pos+1) == "*") {
                std::string ms_str = entry_id.substr(0, dash_pos);
                try {
                    long long ms_val = std::stoll(ms_str);
                    long long seq_val = 0;

                    if (ms_val == 0) {
                        seq_val = 1;
                    }

                    if (stream_store.find(stream_key) != stream_store.end()) {
                        auto& entries = stream_store[stream_key].entries;
                        if (!entries.empty()) {
                            const auto& last_entry = entries.back();
                            long long last_ms, last_seq;
                            if (parse_stream_id(last_entry.id, last_ms, last_seq) && last_ms == ms_val) {
                                seq_val = std::max(seq_val, last_seq + 1);
                            }
                        }
                    }

                    entry_id = ms_str + "-" + std::to_string(seq_val);
                } catch (...) {
                    std::string response = "-ERR Invalid ID format for milliseconds part\r\n";
                    send(client_fd, response.c_str(), response.length(), 0);
                    return;
                }
            }
        }

        long long ms, seq;
        if (!parse_stream_id(entry_id, ms, seq) || ms < 0 || seq < 0) {
            std::string response = "-ERR The ID specified in XADD must be greater than 0-0\r\n";
            send(client_fd, response.c_str(), response.length(), 0);
            return;
        }

        if (ms == 0 && seq == 0) {
            std::string response = "-ERR The ID specified in XADD must be greater than 0-0\r\n";
            send(client_fd, response.c_str(), response.length(), 0);
            return;
        }

        if (!stream_store[stream_key].entries.empty()) {
            const auto& last_entry = stream_store[stream_key].entries.back();
            long long last_ms, last_seq;
            if (parse_stream_id(last_entry.id, last_ms, last_seq)) {
                if (ms < last_ms || (ms == last_ms && seq <= last_seq)) {
                    std::string response = "-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n";
                    send(client_fd, response.c_str(), response.length(), 0);
                    return;
                }
            }
        }

        if (stream_store.find(stream_key) == stream_store.end()) {
            stream_store[stream_key] = StreamData();
        }

        StreamEntry new_entry(entry_id);
        for (size_t i = 3; i < parsed_command.size(); i += 2) {
            if (i + 1 >= parsed_command.size()) break;
            new_entry.fields[parsed_command[i]] = parsed_command[i + 1];
        }

        stream_store[stream_key].entries.push_back(new_entry);

        std::string response = "$" + std::to_string(entry_id.length()) + "\r\n" + entry_id + "\r\n";
        send(client_fd, response.c_str(), response.length(), 0);

        std::vector<BlockedClient> to_unblock;
        {
            std::lock_guard<std::mutex> lock(blocked_clients_mutex);
            std::cout << "XADD: Added entry " << entry_id << " to stream " << stream_key << std::endl;
            std::cout << "XADD: Checking " << blocked_clients.size() << " blocked clients" << std::endl;

            for (auto it = blocked_clients.begin(); it != blocked_clients.end();) {
                if (it->stream_key == stream_key) {
                    long long entry_ms, entry_seq;
                    long long last_ms, last_seq;

                    bool entry_parsed = parse_stream_id(entry_id, entry_ms, entry_seq);
                    bool last_parsed = parse_stream_id(it->last_id, last_ms, last_seq);

                    bool should_unblock = false;
                    if (entry_parsed && last_parsed) {
                        should_unblock = (entry_ms > last_ms || (entry_ms == last_ms && entry_seq > last_seq));
                    } else if (it->last_id == "$") {
                        should_unblock = true;
                    }

                    if (should_unblock) {
                        to_unblock.push_back(*it);
                        it = blocked_clients.erase(it);
                        continue;
                    }
                }
                ++it;
            }
        }

        std::cout << "XADD: Will unblock " << to_unblock.size() << " clients" << std::endl;

        for (const auto& client : to_unblock) {
            std::string unblock_response = "*1\r\n*2\r\n";
            unblock_response += "$" + std::to_string(stream_key.length()) + "\r\n" + stream_key + "\r\n";
            unblock_response += "*1\r\n*2\r\n";
            unblock_response += "$" + std::to_string(new_entry.id.length()) + "\r\n" + new_entry.id + "\r\n";
            unblock_response += "*" + std::to_string(new_entry.fields.size() * 2) + "\r\n";
            for (const auto& field : new_entry.fields) {
                unblock_response += "$" + std::to_string(field.first.length()) + "\r\n" + field.first + "\r\n";
                unblock_response += "$" + std::to_string(field.second.length()) + "\r\n" + field.second + "\r\n";
            }

            std::cout << "XADD: Sending unblock response to fd=" << client.fd 
                  << ": " << unblock_response << std::endl;
            send(client.fd, unblock_response.c_str(), unblock_response.length(), 0);
        }

        if (connected_replicas.find(client_fd) == connected_replicas.end()) {
            propagate_to_replicas(parsed_command);
        }
    } else if (command == "XRANGE" && parsed_command.size() == 4) {
        std::string stream_key = parsed_command[1];
        std::string start_id = parsed_command[2];
        std::string end_id = parsed_command[3];
    
        if (stream_store.find(stream_key) == stream_store.end()) {
            send(client_fd, "*0\r\n", 4, 0);
            return;
        }

        const StreamData& stream = stream_store[stream_key];
        std::vector<const StreamEntry*> matched_entries;

        long long start_ms = 0, start_seq = 0;
        if (start_id == "-") {
            start_ms = 0;
            start_seq = 0;
        } else {
            size_t dash_pos = start_id.find('-');
            if (dash_pos != std::string::npos) {
                try {
                    start_ms = std::stoll(start_id.substr(0, dash_pos));
                    if (dash_pos + 1 < start_id.length()) {
                        start_seq = std::stoll(start_id.substr(dash_pos + 1));
                    }
                } catch (...) {
                    send(client_fd, "-ERR Invalid start ID\r\n", 22, 0);
                    return;
                }
            } else {
                try {
                    start_ms = std::stoll(start_id);
                } catch (...) {
                    send(client_fd, "-ERR Invalid start ID\r\n", 22, 0);
                    return;
                }
            }
        }

        long long end_ms = LLONG_MAX, end_seq = LLONG_MAX;
        if (end_id != "+") {
            size_t dash_pos = end_id.find('-');
            if (dash_pos != std::string::npos) {
                try {
                    end_ms = std::stoll(end_id.substr(0, dash_pos));
                    if (dash_pos + 1 < end_id.length()) {
                        end_seq = std::stoll(end_id.substr(dash_pos + 1));
                    }
                } catch (...) {
                    send(client_fd, "-ERR Invalid end ID\r\n", 20, 0);
                    return;
                }
            } else {
                try {
                    end_ms = std::stoll(end_id);
                } catch (...) {
                    send(client_fd, "-ERR Invalid end ID\r\n", 20, 0);
                    return;
                }
            }
        }

        for (const auto& entry : stream.entries) {
            long long entry_ms, entry_seq;
            if (!parse_stream_id(entry.id, entry_ms, entry_seq)) {
                continue;
            }

            if (entry_ms < start_ms || (entry_ms == start_ms && entry_seq < start_seq)) {
                continue;
            }

            if (end_id != "+" && (entry_ms > end_ms || (entry_ms == end_ms && entry_seq > end_seq))) {
                continue;
            }

            matched_entries.push_back(&entry);
        }

        std::string response = "*" + std::to_string(matched_entries.size()) + "\r\n";
    
        for (const auto entry : matched_entries) {
            response += "*2\r\n";
            
            response += "$" + std::to_string(entry->id.length()) + "\r\n";
            response += entry->id + "\r\n";
            
            response += "*" + std::to_string(entry->fields.size() * 2) + "\r\n";
            for (const auto& field : entry->fields) {
                response += "$" + std::to_string(field.first.length()) + "\r\n";
                response += field.first + "\r\n";
                response += "$" + std::to_string(field.second.length()) + "\r\n";
                response += field.second + "\r\n";
            }
        }

        send(client_fd, response.c_str(), response.length(), 0);
    } else if (command == "XREAD") {
        int block_time = -1;
        size_t streams_pos = 1;
        std::vector<std::pair<std::string, std::string>> streams;
        std::vector<std::string> responses;
        bool has_data = false;

        if (parsed_command.size() > 2) {
            std::string second_arg = parsed_command[1];
            std::transform(second_arg.begin(), second_arg.end(), second_arg.begin(), ::toupper);
            if (second_arg == "BLOCK") {
                try {
                    block_time = std::stoi(parsed_command[2]);
                    if (block_time < 0) {
                        send(client_fd, "-ERR timeout is negative\r\n", 24, 0);
                        return;
                    }
                    streams_pos = 3;
                } catch (...) {
                    send(client_fd, "-ERR invalid timeout value\r\n", 27, 0);
                    return;
                }
            }
        }

        if (parsed_command.size() <= streams_pos) {
            send(client_fd, "-ERR syntax error, STREAMS keyword expected\r\n", 44, 0);
            return;
        }
    
        std::string streams_keyword = parsed_command[streams_pos];
        std::transform(streams_keyword.begin(), streams_keyword.end(), streams_keyword.begin(), ::toupper);
        if (streams_keyword != "STREAMS") {
            send(client_fd, "-ERR syntax error, STREAMS keyword expected\r\n", 44, 0);
            return;
        }

        size_t keys_start = streams_pos + 1;
        size_t ids_start = keys_start + (parsed_command.size() - keys_start) / 2;

        if ((parsed_command.size() - keys_start) % 2 != 0 || ids_start == keys_start) {
            send(client_fd, "-ERR wrong number of arguments for XREAD\r\n", 41, 0);
            return;
        }

        for (size_t i = keys_start; i < ids_start; i++) {
            std::string stream_key = parsed_command[i];
            std::string start_id = parsed_command[i + (ids_start - keys_start)];
            
            if (start_id == "$") {
                auto stream_it = stream_store.find(stream_key);
                if (stream_it != stream_store.end() && !stream_it->second.entries.empty()) {
                    start_id = stream_it->second.entries.back().id;
                } else {
                    start_id = "0-0";
                }
            }
            
            streams.emplace_back(stream_key, start_id);
        }

        for (auto& stream : streams) {
            auto& stream_key = stream.first;
            auto& start_id = stream.second;
        
            auto stream_it = stream_store.find(stream_key);
            if (stream_it == stream_store.end()) continue;

            long long start_ms = 0, start_seq = 0;
            size_t dash_pos = start_id.find('-');
            if (dash_pos != std::string::npos) {
                try {
                    start_ms = std::stoll(start_id.substr(0, dash_pos));
                    start_seq = std::stoll(start_id.substr(dash_pos + 1));
                } catch (...) {
                    send(client_fd, "-ERR Invalid ID\r\n", 17, 0);
                    return;
                }
            } else {
                try {
                    start_ms = std::stoll(start_id);
                    start_seq = 0;
                } catch (...) {
                    send(client_fd, "-ERR Invalid ID\r\n", 17, 0);
                    return;
                }
            }

            std::vector<const StreamEntry*> matches;
            for (const auto& entry : stream_it->second.entries) {
                long long entry_ms, entry_seq;
                if (!parse_stream_id(entry.id, entry_ms, entry_seq)) continue;

                if (entry_ms > start_ms || (entry_ms == start_ms && entry_seq > start_seq)) {
                    matches.push_back(&entry);
                }
            }

            if (!matches.empty()) {
                has_data = true;
                std::string response = "*2\r\n";
                response += "$" + std::to_string(stream_key.length()) + "\r\n" + stream_key + "\r\n";
                response += "*" + std::to_string(matches.size()) + "\r\n";

                for (const auto entry : matches) {
                    response += "*2\r\n";
                    response += "$" + std::to_string(entry->id.length()) + "\r\n" + entry->id + "\r\n";
                    response += "*" + std::to_string(entry->fields.size() * 2) + "\r\n";
                    for (const auto& [field, value] : entry->fields) {
                        response += "$" + std::to_string(field.length()) + "\r\n" + field + "\r\n";
                        response += "$" + std::to_string(value.length()) + "\r\n" + value + "\r\n";
                    }
                }

                responses.push_back(response);
            }
        }

        if (has_data || block_time < 0) {
            if (has_data) {
                std::string final_response = "*" + std::to_string(responses.size()) + "\r\n";
                for (const auto& resp : responses) {
                    final_response += resp;
                }
                send(client_fd, final_response.c_str(), final_response.length(), 0);
            } else {
                send(client_fd, "*0\r\n", 4, 0);
            }
            return;
        }

        if (streams.size() != 1) {
            send(client_fd, "-ERR BLOCK option is only supported for a single stream\r\n", 56, 0);
            return;
        }

        auto& stream = streams[0];
        auto& stream_key = stream.first;
        auto& last_id = stream.second;
    
        BlockedClient client;
        client.fd = client_fd;
        client.stream_key = stream_key;
        client.last_id = last_id;
        if (block_time == 0) {
            client.expiry = std::chrono::steady_clock::time_point::max();
        } else {
            client.expiry = std::chrono::steady_clock::now() + std::chrono::milliseconds(block_time);
        }

        {
            std::lock_guard<std::mutex> lock(blocked_clients_mutex);
            blocked_clients.push_back(client);
            std::cout << "XREAD: Blocking client fd=" << client_fd 
                  << " on stream '" << stream_key 
                  << "' with last_id '" << last_id << "'" << std::endl;
        }
    } else if (command == "TYPE" && parsed_command.size() == 2) {
        std::string key = parsed_command[1];
        std::string response;
    
        auto stream_it = stream_store.find(key);
        if (stream_it != stream_store.end()) {
            if (stream_it->second.has_expiry && get_current_time() > stream_it->second.expiry) {
                stream_store.erase(stream_it);
                response = "+none\r\n";
            } else {
                response = "+stream\r\n";
            }
        } else {
            auto kv_it = kv_store.find(key);
            if (kv_it != kv_store.end()) {
                if (kv_it->second.has_expiry && get_current_time() > kv_it->second.expiry) {
                    kv_store.erase(kv_it);
                    response = "+none\r\n";
                } else {
                    response = "+string\r\n";
                }
            } else {
                auto list_it = list_store.find(key);
                if (list_it != list_store.end()) {
                    response = "+list\r\n";
                } else {
                    response = "+none\r\n";
                }
            }
        }
    
        send(client_fd, response.c_str(), response.length(), 0);
    } else if (command == "INCR" && parsed_command.size() == 2) {
        std::string key = parsed_command[1];
        auto it = kv_store.find(key);
    
        if (it != kv_store.end()) {
            try {
                long long value = std::stoll(it->second.value);
                value++;
                it->second.value = std::to_string(value);
            
                std::string response = ":" + std::to_string(value) + "\r\n";
                send(client_fd, response.c_str(), response.length(), 0);
            
                if (connected_replicas.find(client_fd) == connected_replicas.end()) {
                    propagate_to_replicas(parsed_command);
                }
            } catch (const std::exception& e) {
                std::string response = "-ERR value is not an integer or out of range\r\n";
                send(client_fd, response.c_str(), response.length(), 0);
            }
        } else {
            kv_store[key] = ValueEntry("1");
        
            std::string response = ":1\r\n";
            send(client_fd, response.c_str(), response.length(), 0);
        
            if (connected_replicas.find(client_fd) == connected_replicas.end()) {
                propagate_to_replicas(parsed_command);
            }
        }
    } else if (command == "EXEC" && parsed_command.size() == 1) {
        {
            std::lock_guard<std::mutex> lock(multi_mutex);
            if (clients_in_multi.find(client_fd) == clients_in_multi.end()) {
                std::string response = "-ERR EXEC without MULTI\r\n";
                send(client_fd, response.c_str(), response.length(), 0);
                return;
            }
            clients_in_multi.erase(client_fd);
        }
    
        std::vector<std::vector<std::string>> commands;
        {
            std::lock_guard<std::mutex> qlock(queue_mutex);
            commands = queued_commands[client_fd];
            queued_commands.erase(client_fd);
        }
    
        std::string response = "*" + std::to_string(commands.size()) + "\r\n";
    
        for (const auto& cmd : commands) {
            if (cmd.empty()) {
                response += "+OK\r\n";
                continue;
            }
        
            std::string command_name = cmd[0];
        
            if (command_name == "SET" && (cmd.size() == 3 || cmd.size() == 5)) {
                std::string key = cmd[1];
                std::string value = cmd[2];
            
                if (cmd.size() == 3) {
                    kv_store[key] = ValueEntry(value);
                    response += "+OK\r\n";
                } else if (cmd.size() == 5) {
                    std::string px_arg = cmd[3];
                    for (char& c : px_arg) {
                        c = std::toupper(c);
                    }
                    if (px_arg == "PX") {
                        try {
                            long expiry_ms = std::stol(cmd[4]);
                            if (expiry_ms <= 0) {
                                response += "-ERR invalid expire time\r\n";
                            } else {
                                auto expiry_time = get_current_time() + std::chrono::milliseconds(expiry_ms);
                                kv_store[key] = ValueEntry(value, expiry_time);
                                response += "+OK\r\n";
                            }
                        } catch (const std::exception& e) {
                            response += "-ERR invalid expire time\r\n";
                        }
                    } else {
                        response += "-ERR syntax error\r\n";
                    }
                }
            
                if (connected_replicas.find(client_fd) == connected_replicas.end()) {
                    propagate_to_replicas(cmd);
                }
            
            } else if (command_name == "GET" && cmd.size() == 2) {
                std::string key = cmd[1];
                auto it = kv_store.find(key);
                if (it != kv_store.end()) {
                    if (it->second.has_expiry && get_current_time() > it->second.expiry) {
                        kv_store.erase(it);
                        response += "$-1\r\n";
                    } else {
                        std::string value = it->second.value;
                        response += "$" + std::to_string(value.length()) + "\r\n" + value + "\r\n";
                    }
                } else {
                    response += "$-1\r\n";
                }
            
            } else if (command_name == "INCR" && cmd.size() == 2) {
                std::string key = cmd[1];
                auto it = kv_store.find(key);
            
                if (it != kv_store.end()) {
                    try {
                        long long value = std::stoll(it->second.value);
                        value++;
                        it->second.value = std::to_string(value);
                        response += ":" + std::to_string(value) + "\r\n";
                    } catch (const std::exception& e) {
                        response += "-ERR value is not an integer or out of range\r\n";
                    }
                } else {
                    kv_store[key] = ValueEntry("1");
                    response += ":1\r\n";
                }
            
                if (connected_replicas.find(client_fd) == connected_replicas.end()) {
                    propagate_to_replicas(cmd);
                }
            
            } else {
                response += "+OK\r\n";
            }
        }
    
        send(client_fd, response.c_str(), response.length(), 0);
    } else if (command == "MULTI" && parsed_command.size() == 1) {
        {
            std::lock_guard<std::mutex> lock(multi_mutex);
            clients_in_multi.insert(client_fd);
            {
                std::lock_guard<std::mutex> qlock(queue_mutex);
                queued_commands[client_fd].clear();
            }
        }
        std::string response = "+OK\r\n";
        send(client_fd, response.c_str(), response.length(), 0);
    } else if (command == "DISCARD" && parsed_command.size() == 1) {
        {
            std::lock_guard<std::mutex> lock(multi_mutex);
            if (clients_in_multi.find(client_fd) == clients_in_multi.end()) {
                std::string response = "-ERR DISCARD without MULTI\r\n";
                send(client_fd, response.c_str(), response.length(), 0);
                return;
            }
            clients_in_multi.erase(client_fd);
            {
                std::lock_guard<std::mutex> qlock(queue_mutex);
                queued_commands[client_fd].clear();
            }
        }
        std::string response = "+OK\r\n";
        send(client_fd, response.c_str(), response.length(), 0);
    } else if (command == "RPUSH" && parsed_command.size() >= 3) {
        std::string key = parsed_command[1];
        int elements_to_add = parsed_command.size() - 2;
        int list_length;
        
        {
            auto it = list_store.find(key);
            if (it != list_store.end()) {
                for (int i = 2; i < parsed_command.size(); i++) {
                    it->second.push_back(parsed_command[i]);
                }
                list_length = it->second.size();
            } else {
                std::vector<std::string> new_list;
                for (int i = 2; i < parsed_command.size(); i++) {
                    new_list.push_back(parsed_command[i]);
                }
                list_store[key] = new_list;
                list_length = new_list.size();
            }
        }
        
        std::vector<BlockedClient> to_unblock;
        {
            std::lock_guard<std::mutex> lock(blocked_clients_mutex);
            for (auto it = blocked_clients.begin(); it != blocked_clients.end();) {
                if (it->is_list_block && it->key == key) {
                    to_unblock.push_back(*it);
                    it = blocked_clients.erase(it);
                } else {
                    ++it;
                }
            }
        }
        
        if (!to_unblock.empty()) {
            std::sort(to_unblock.begin(), to_unblock.end(), 
                [](const BlockedClient& a, const BlockedClient& b) {
                    return a.expiry < b.expiry;
                });
            
            auto& client = to_unblock[0];
            auto list_it = list_store.find(key);
            if (list_it != list_store.end() && !list_it->second.empty()) {
                std::string popped_value = list_it->second.front();
                list_it->second.erase(list_it->second.begin());
                
                std::string response = "*2\r\n";
                response += "$" + std::to_string(key.length()) + "\r\n" + key + "\r\n";
                response += "$" + std::to_string(popped_value.length()) + "\r\n" + popped_value + "\r\n";
                
                send(client.fd, response.c_str(), response.length(), 0);
                
                if (connected_replicas.find(client.fd) == connected_replicas.end()) {
                    std::vector<std::string> lpop_cmd = {"LPOP", key};
                    propagate_to_replicas(lpop_cmd);
                }
                
                list_length = list_it->second.size();
                if (list_length == 0) {
                    list_store.erase(list_it);
                }
            }
        }
        
        std::string response = ":" + std::to_string(list_length) + "\r\n";
        send(client_fd, response.c_str(), response.length(), 0);
        
        if (connected_replicas.find(client_fd) == connected_replicas.end()) {
            propagate_to_replicas(parsed_command);
        }
    } else if (command == "LRANGE" && parsed_command.size() == 4) {
        std::string key = parsed_command[1];
    
        try {
            int start = std::stoi(parsed_command[2]);
            int end = std::stoi(parsed_command[3]);
        
            auto it = list_store.find(key);
            if (it == list_store.end()) {
                send(client_fd, "*0\r\n", 4, 0);
                return;
            }
        
            const auto& list = it->second;
            int list_size = list.size();
        
            if (start < 0) {
                start = list_size + start;
                if (start < 0) start = 0;
            }
        
            if (end < 0) {
                end = list_size + end;
                if (end < 0) end = 0;
            }
        
            if (start >= list_size) {
                send(client_fd, "*0\r\n", 4, 0);
                return;
            }
        
            if (end >= list_size) {
                end = list_size - 1;
            }
        
            if (start > end) {
                send(client_fd, "*0\r\n", 4, 0);
                return;
            }
        
            int count = end - start + 1;
            std::string response = "*" + std::to_string(count) + "\r\n";
        
            for (int i = start; i <= end; i++) {
                const std::string& element = list[i];
                response += "$" + std::to_string(element.length()) + "\r\n" + element + "\r\n";
            }
        
            send(client_fd, response.c_str(), response.length(), 0);
        } catch (const std::exception& e) {
            std::string response = "-ERR value is not an integer or out of range\r\n";
            send(client_fd, response.c_str(), response.length(), 0);
        }
    } else if (command == "LPUSH" && parsed_command.size() >= 3) {
        std::string key = parsed_command[1];
        std::vector<std::string> values(parsed_command.begin() + 2, parsed_command.end());
        std::reverse(values.begin(), values.end());
        int list_length;

        {
            auto it = list_store.find(key);
            if (it != list_store.end()) {
                it->second.insert(it->second.begin(), values.begin(), values.end());
                list_length = it->second.size();
            } else {
                list_store[key] = values;
                list_length = values.size();
            }
        }

        std::string response = ":" + std::to_string(list_length) + "\r\n";
        send(client_fd, response.c_str(), response.length(), 0);

        if (connected_replicas.find(client_fd) == connected_replicas.end()) {
            propagate_to_replicas(parsed_command);
        }
    } else if (command == "LLEN" && parsed_command.size() == 2) {
        std::string key = parsed_command[1];
        auto it = list_store.find(key);
        int length = (it != list_store.end()) ? it->second.size() : 0;
        std::string response = ":" + std::to_string(length) + "\r\n";
        send(client_fd, response.c_str(), response.length(), 0);
    } else if (command == "LPOP" && (parsed_command.size() == 2 || parsed_command.size() == 3)) {
        std::string key = parsed_command[1];
        int count = 1;
        
        if (parsed_command.size() == 3) {
            try {
                count = std::stoi(parsed_command[2]);
                if (count <= 0) {
                    std::string response = "-ERR count must be greater than 0\r\n";
                    send(client_fd, response.c_str(), response.length(), 0);
                    return;
                }
            } catch (const std::exception& e) {
                std::string response = "-ERR value is not an integer or out of range\r\n";
                send(client_fd, response.c_str(), response.length(), 0);
                return;
            }
        }
        
        auto it = list_store.find(key);
        
        if (it == list_store.end() || it->second.empty()) {
            if (count == 1) {
                std::string response = "$-1\r\n";
                send(client_fd, response.c_str(), response.length(), 0);
            } else {
                std::string response = "*0\r\n";
                send(client_fd, response.c_str(), response.length(), 0);
            }
            return;
        }
        
        std::vector<std::string> popped_values;
        int actual_count = std::min(count, static_cast<int>(it->second.size()));
        
        for (int i = 0; i < actual_count; i++) {
            popped_values.push_back(it->second.front());
            it->second.erase(it->second.begin());
        }
        
        if (it->second.empty()) {
            list_store.erase(it);
        }
        
        std::string response;
        if (count == 1) {
            response = "$" + std::to_string(popped_values[0].length()) + "\r\n" + popped_values[0] + "\r\n";
        } else {
            response = "*" + std::to_string(popped_values.size()) + "\r\n";
            for (const auto& value : popped_values) {
                response += "$" + std::to_string(value.length()) + "\r\n" + value + "\r\n";
            }
        }
        
        send(client_fd, response.c_str(), response.length(), 0);
        
        if (connected_replicas.find(client_fd) == connected_replicas.end()) {
            propagate_to_replicas(parsed_command);
        }
    } else if (command == "BLPOP" && parsed_command.size() >= 3) {
        double timeout;
        try {
            timeout = std::stod(parsed_command.back());
        } catch (const std::exception& e) {
            std::string response = "-ERR timeout is not a float\r\n";
            send(client_fd, response.c_str(), response.length(), 0);
            return;
        }

        for (size_t i = 1; i < parsed_command.size() - 1; i++) {
            const std::string& list_key = parsed_command[i];
            auto it = list_store.find(list_key);
            
            if (it != list_store.end() && !it->second.empty()) {
                std::string popped_value = it->second.front();
                it->second.erase(it->second.begin());
                
                if (it->second.empty()) {
                    list_store.erase(it);
                }
                
                std::string response = "*2\r\n";
                response += "$" + std::to_string(list_key.length()) + "\r\n" + list_key + "\r\n";
                response += "$" + std::to_string(popped_value.length()) + "\r\n" + popped_value + "\r\n";
                
                send(client_fd, response.c_str(), response.length(), 0);
                
                if (connected_replicas.find(client_fd) == connected_replicas.end()) {
                    std::vector<std::string> lpop_cmd = {"LPOP", list_key};
                    propagate_to_replicas(lpop_cmd);
                }
                return;
            }
        }
        
        BlockedClient client;
        client.fd = client_fd;
        client.key = parsed_command[1];
        client.is_list_block = true;
        
        if (timeout == 0) {
            client.expiry = std::chrono::steady_clock::time_point::max();
        } else {
            client.expiry = std::chrono::steady_clock::now() + 
                        std::chrono::milliseconds(static_cast<long>(timeout * 1000));
        }
        
        {
            std::lock_guard<std::mutex> lock(blocked_clients_mutex);
            blocked_clients.push_back(client);
            std::cout << "BLPOP: Blocking client fd=" << client_fd 
                    << " on list '" << client.key << "'" << std::endl;
        }
    } else {
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
    } else if (command == "PING") {
        std::cout << "Replica: Received PING" << std::endl;
    } else if (command == "INCR" && parsed_command.size() == 2) {
        std::string key = parsed_command[1];
        auto it = kv_store.find(key);
    
        if (it != kv_store.end()) {
            try {
                long long value = std::stoll(it->second.value);
                value++;
                it->second.value = std::to_string(value);
                std::cout << "Replica: INCR '" << key << "' = " << value << std::endl;
            } catch (...) {
                std::cerr << "Replica: INCR failed - value is not an integer" << std::endl;
            }
        }
    } else if (command == "XADD" && parsed_command.size() >= 5 && (parsed_command.size() % 2 == 1)) {
        std::string stream_key = parsed_command[1];
        std::string entry_id = parsed_command[2];
        
        std::cout << "Replica: XADD '" << stream_key << "' " << entry_id;
        
        if (stream_store.find(stream_key) == stream_store.end()) {
            stream_store[stream_key] = StreamData();
        }
        
        StreamEntry new_entry(entry_id);
        
        for (size_t i = 3; i < parsed_command.size(); i += 2) {
            std::string field = parsed_command[i];
            std::string value = parsed_command[i + 1];
            new_entry.fields[field] = value;
            std::cout << " " << field << " " << value;
        }
        std::cout << std::endl;
        
        stream_store[stream_key].entries.push_back(new_entry);
    } else if (command == "RPUSH" && parsed_command.size() >= 3) {
        std::string key = parsed_command[1];
    
        auto it = list_store.find(key);
        if (it != list_store.end()) {
            for (size_t i = 2; i < parsed_command.size(); i++) {
                it->second.push_back(parsed_command[i]);
            }
        } else {
            std::vector<std::string> new_list;
            for (size_t i = 2; i < parsed_command.size(); i++) {
                new_list.push_back(parsed_command[i]);
            }
            list_store[key] = new_list;
        }
    
        std::cout << "Replica: RPUSH '" << key << "' with " << (parsed_command.size() - 2) << " elements" << std::endl;
    } else if (command == "LPUSH" && parsed_command.size() >= 3) {
        std::string key = parsed_command[1];
    
        auto it = list_store.find(key);
        if (it != list_store.end()) {
            for (int i = parsed_command.size() - 1; i >= 2; i--) {
                it->second.insert(it->second.begin(), parsed_command[i]);
            }
        } else {
            std::vector<std::string> new_list;
            for (size_t i = 2; i < parsed_command.size(); i++) {
                new_list.push_back(parsed_command[i]);
            }
            list_store[key] = new_list;
        }
    
        std::cout << "Replica: LPUSH '" << key << "' with " << (parsed_command.size() - 2) << " elements" << std::endl;
    } else if (command == "LPOP" && (parsed_command.size() == 2 || parsed_command.size() == 3)) {
        std::string key = parsed_command[1];
        int count = 1;
        
        if (parsed_command.size() == 3) {
            try {
                count = std::stoi(parsed_command[2]);
            } catch (...) {
                std::cerr << "Replica: Invalid count in LPOP command" << std::endl;
                return "";
            }
        }
        
        auto it = list_store.find(key);
        
        if (it != list_store.end() && !it->second.empty()) {
            int actual_count = std::min(count, static_cast<int>(it->second.size()));
            for (int i = 0; i < actual_count; i++) {
                it->second.erase(it->second.begin());
            }
            if (it->second.empty()) {
                list_store.erase(it);
            }
        }
        
        std::cout << "Replica: LPOP '" << key << "' count=" << count << std::endl;
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

    signal(SIGINT, handle_signal);
    signal(SIGTERM, handle_signal);

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

    std::thread timeout_thread(check_blocked_clients_timeout);

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
        std::cerr << "Failed to create server socket: " << strerror(errno) << std::endl;
        shutdown_server = true;
        timeout_thread.join();
        return 1;
    }

    int reuse = 1;
    if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse)) < 0) {
        std::cerr << "setsockopt failed: " << strerror(errno) << std::endl;
        close(server_fd);
        shutdown_server = true;
        timeout_thread.join();
        return 1;
    }

    struct sockaddr_in server_addr;
    server_addr.sin_family = AF_INET;
    server_addr.sin_addr.s_addr = INADDR_ANY;
    server_addr.sin_port = htons(server_port);

    if (bind(server_fd, (struct sockaddr *) &server_addr, sizeof(server_addr)) < 0) {
        std::cerr << "Failed to bind to port " << server_port << ": " << strerror(errno) << std::endl;
        close(server_fd);
        shutdown_server = true;
        timeout_thread.join();
        return 1;
    }

    int connection_backlog = 128;
    if (listen(server_fd, connection_backlog) < 0) {
        std::cerr << "listen failed: " << strerror(errno) << std::endl;
        close(server_fd);
        shutdown_server = true;
        timeout_thread.join();
        return 1;
    }

    std::cout << "Server started successfully. Waiting for connections on port " << server_port << std::endl;

    fd_set read_fds;
    std::vector<int> client_fds;
    struct timeval tv;

    while (!shutdown_server) {
        tv.tv_sec = 1;
        tv.tv_usec = 0;

        FD_ZERO(&read_fds);
        FD_SET(server_fd, &read_fds);
        int max_fd = server_fd;

        for (int fd : client_fds) {
            FD_SET(fd, &read_fds);
            if (fd > max_fd) {
                max_fd = fd;
            }
        }

        int activity = select(max_fd + 1, &read_fds, nullptr, nullptr, &tv);
        
        if (activity < 0 && errno != EINTR) {
            std::cerr << "select error: " << strerror(errno) << std::endl;
            break;
        }

        if (shutdown_server) {
            break;
        }

        if (FD_ISSET(server_fd, &read_fds)) {
            struct sockaddr_in client_addr;
            socklen_t client_len = sizeof(client_addr);
            int client_fd = accept(server_fd, (struct sockaddr *)&client_addr, &client_len);
    
            if (client_fd < 0) {
                if (errno != EWOULDBLOCK && errno != EAGAIN) {
                    std::cerr << "accept failed: " << strerror(errno) << std::endl;
                }
                continue;
            }

            int flags = fcntl(client_fd, F_GETFL, 0);
            fcntl(client_fd, F_SETFL, flags | O_NONBLOCK);

            std::cout << "New client connected (fd: " << client_fd << ")" << std::endl;
            client_fds.push_back(client_fd);
        }

        for (auto it = client_fds.begin(); it != client_fds.end(); ) {
            int client_fd = *it;
            
            if (FD_ISSET(client_fd, &read_fds)) {
                char buffer[4096];
                ssize_t bytes_received = recv(client_fd, buffer, sizeof(buffer) - 1, 0);

                if (bytes_received <= 0) {
                    if (bytes_received == 0) {
                        std::cout << "Client (fd: " << client_fd << ") disconnected" << std::endl;
                    } else {
                        std::cerr << "recv error from client (fd: " << client_fd << "): " << strerror(errno) << std::endl;
                    }

                    {
                        std::lock_guard<std::mutex> lock(multi_mutex);
                        clients_in_multi.erase(client_fd);
                    }

                    {
                        std::lock_guard<std::mutex> lock(blocked_clients_mutex);
                        blocked_clients.erase(
                            std::remove_if(blocked_clients.begin(), blocked_clients.end(),
                                [client_fd](const BlockedClient& bc) { return bc.fd == client_fd; }),
                            blocked_clients.end());
                    }

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
                std::cout << "Received from client (fd: " << client_fd << ") [" << bytes_received << " bytes]: ";
                for (ssize_t i = 0; i < bytes_received; ++i) {
                    if (buffer[i] == '\r') {
                        std::cout << "\\r";
                    } else if (buffer[i] == '\n') {
                        std::cout << "\\n";
                    } else {
                        std::cout << buffer[i];
                    }
                }
                std::cout << std::endl;

                try {
                    std::vector<std::string> parsed_command;
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

    std::cout << "Shutting down server..." << std::endl;

    {
        std::lock_guard<std::mutex> lock(blocked_clients_mutex);
        for (const auto& client : blocked_clients) {
            close(client.fd);
        }
        blocked_clients.clear();
    }

    for (int fd : client_fds) {
        close(fd);
    }

    if (server_fd >= 0) {
        close(server_fd);
    }

    timeout_thread.join();

    std::cout << "Server shutdown complete" << std::endl;
    return 0;
}