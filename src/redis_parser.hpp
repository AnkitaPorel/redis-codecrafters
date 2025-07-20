#ifndef REDIS_PARSER_HPP
#define REDIS_PARSER_HPP

#include <string>
#include <vector>
#include <stdexcept>

void parse_redis_command(const std::string& input, std::vector<std::string>& output) {
    output.clear();
    
    std::cout << "Raw buffer: " << input << std::endl;

    if (input.empty() || input[0] != '*') {
        throw std::runtime_error("Invalid RESP array format");
    }

    size_t pos = 1;
    size_t end = input.find("\r\n", pos);
    if (end == std::string::npos) {
        throw std::runtime_error("Invalid RESP array length");
    }

    int num_elements = std::stoi(input.substr(pos, end - pos));
    pos = end + 2;

    for (int i = 0; i < num_elements; ++i) {
        if (pos >= input.length() || input[pos] != '$') {
            throw std::runtime_error("Invalid RESP bulk string format");
        }
        pos++;

        end = input.find("\r\n", pos);
        if (end == std::string::npos) {
            throw std::runtime_error("Invalid RESP bulk string length");
        }
        
        int str_length = std::stoi(input.substr(pos, end - pos));
        pos = end + 2;

        if (pos + str_length + 2 > input.length()) {
            throw std::runtime_error("Incomplete RESP bulk string");
        }
        
        std::string content = input.substr(pos, str_length);
        output.push_back(content);
        pos += str_length + 2;
    }

    if (!output.empty()) {
        for (char& c : output[0]) {
            c = std::toupper(c);
        }
    }

    std::cout << "Parsed command (" << output.size() << " elements):" << std::endl;
    for (const auto& arg : output) {
        std::cout << "  '" << arg << "'" << std::endl;
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

// Helper function to generate a new sequence number for a given timestamp
long long generate_sequence_number(long long ms, const StreamData& stream) {
    // Default sequence number is 0, except when ms is 0 (then it's 1)
    long long default_seq = (ms == 0) ? 1 : 0;
    
    if (stream.entries.empty()) {
        return default_seq;
    }
    
    // Find the last entry with the same timestamp
    for (auto it = stream.entries.rbegin(); it != stream.entries.rend(); ++it) {
        long long entry_ms, entry_seq;
        if (parse_stream_id(it->id, entry_ms, entry_seq)) {
            if (entry_ms == ms) {
                return entry_seq + 1;
            } else if (entry_ms < ms) {
                break;
            }
        }
    }
    
    return default_seq;
}

// Helper function to validate and potentially generate a new ID
std::pair<bool, std::string> validate_or_generate_id(const std::string& id_spec, StreamData& stream) {
    size_t dash_pos = id_spec.find('-');
    if (dash_pos == std::string::npos || dash_pos == 0 || dash_pos == id_spec.length() - 1) {
        return {false, ""};
    }
    
    std::string ms_part = id_spec.substr(0, dash_pos);
    std::string seq_part = id_spec.substr(dash_pos + 1);
    
    // Parse milliseconds part
    long long ms;
    try {
        ms = std::stoll(ms_part);
    } catch (const std::exception& e) {
        return {false, ""};
    }
    
    // Handle auto-sequence case
    if (seq_part == "*") {
        long long seq = generate_sequence_number(ms, stream);
        return {true, ms_part + "-" + std::to_string(seq)};
    }
    
    // Handle explicit sequence number case
    try {
        long long seq = std::stoll(seq_part);
        if (ms < 0 || seq <= 0) {
            return {false, ""};
        }
        
        // For empty stream, just check it's greater than 0-0
        if (stream.entries.empty()) {
            if (ms > 0 || (ms == 0 && seq > 0)) {
                return {true, id_spec};
            }
            return {false, ""};
        }
        
        // Compare with last entry
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