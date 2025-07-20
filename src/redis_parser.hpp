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

#endif