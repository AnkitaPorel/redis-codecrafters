#ifndef REDIS_PARSER_HPP
#define REDIS_PARSER_HPP

#include <string>
#include <vector>
#include <stdexcept>

void parse_redis_command(const std::string& input, std::vector<std::string>& output) {
    output.clear();
    
    // Check if input is a valid RESP array
    if (input.empty() || input[0] != '*') {
        throw std::runtime_error("Invalid RESP array format");
    }

    // Find number of elements
    size_t pos = 1;
    size_t end = input.find("\r\n", pos);
    if (end == std::string::npos) {
        throw std::runtime_error("Invalid RESP array length");
    }

    int num_elements = std::stoi(input.substr(pos, end - pos));
    pos = end + 2; // Skip \r\n

    // Parse each bulk string
    for (int i = 0; i < num_elements; ++i) {
        // Check for bulk string marker
        if (pos >= input.length() || input[pos] != '$') {
            throw std::runtime_error("Invalid RESP bulk string format");
        }
        pos++;

        // Get string length
        end = input.find("\r\n", pos);
        if (end == std::string::npos) {
            throw std::runtime_error("Invalid RESP bulk string length");
        }
        
        int str_length = std::stoi(input.substr(pos, end - pos));
        pos = end + 2; // Skip \r\n

        // Get string content
        if (pos + str_length + 2 > input.length()) {
            throw std::runtime_error("Incomplete RESP bulk string");
        }
        
        std::string content = input.substr(pos, str_length);
        output.push_back(content);
        pos += str_length + 2; // Skip string content and \r\n
    }

    // Convert command to uppercase for case-insensitive comparison
    if (!output.empty()) {
        for (char& c : output[0]) {
            c = std::toupper(c);
        }
    }
}

#endif