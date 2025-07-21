#ifndef REDIS_COMMANDS_HPP
#define REDIS_COMMANDS_HPP

#include <string>
#include <utility>
#include <iostream>
#include <cstdlib>
#include <string>
#include <cstring>
#include <vector>
#include <map>
#include <set>
#include <unistd.h>

#include "rdb_parser.hpp"
#include "redis_parser.hpp"

struct ServerConfig {
    std::string dir;
    std::string dbfilename;

    ServerConfig() : dir("/tmp/redis-files"), dbfilename("dump.rdb") {}
};

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
         

void propagate_to_replicas(const std::vector<std::string>& command) {
    if (is_replica || connected_replicas.empty()) {
        return;
    }
    
    if (!is_write_command(command[0])) {
        return;
    }
    
    std::string resp_command = command_to_resp_array(command);
    int cmd_size = resp_command.size();
    
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

#endif