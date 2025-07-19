#ifndef RDB_PARSER_HPP
#define RDB_PARSER_HPP

#include <string>
#include <vector>
#include <fstream>
#include <stdexcept>
#include "redis_commands.hpp"

class RDBParser {
public:
    static std::vector<std::string> load_keys(const ServerConfig& config) {
        std::vector<std::string> keys;
        std::string filepath = config.dir + "/" + config.dbfilename;
        
        std::ifstream file(filepath, std::ios::binary);
        if (!file.is_open()) {
            return keys;
        }

        char buffer[9];
        if (!file.read(buffer, 9)) {
            file.close();
            throw std::runtime_error("Invalid RDB file: failed to read header");
        }
        if (std::string(buffer, 5) != "REDIS") {
            file.close();
            throw std::runtime_error("Invalid RDB file: incorrect header");
        }

        file.seekg(4, std::ios::cur);

        while (file.good()) {
            char opcode;
            if (!file.read(&opcode, 1)) {
                break;
            }

            if (opcode == 0xFE) {
                char len;
                file.read(&len, 1);
                if (len & 0xC0) {
                    file.seekg(1, std::ios::cur);
                }
            } else if (opcode == 0x00) {
                uint32_t length = read_length(file);
                if (length == 0xFFFFFFFF) {
                    file.close();
                    throw std::runtime_error("Invalid RDB string length");
                }

                std::string key(length, '\0');
                if (!file.read(&key[0], length)) {
                    file.close();
                    throw std::runtime_error("Failed to read key");
                }
                keys.push_back(key);

                length = read_length(file);
                if (length == 0xFFFFFFFF) {
                    file.close();
                    throw std::runtime_error("Invalid RDB value length");
                }
                file.seekg(length, std::ios::cur);

                break;
            } else if (opcode == 0xFF) {
                break;
            } else {
                file.seekg(1, std::ios::cur);
            }
        }

        file.close();
        return keys;
    }

private:
    static uint32_t read_length(std::ifstream& file) {
        char first_byte;
        if (!file.read(&first_byte, 1)) {
            return 0xFFFFFFFF;
        }
        uint8_t format = (first_byte >> 6) & 0x03;
        uint32_t length = first_byte & 0x3F;

        if (format == 0) {
            return length;
        } else if (format == 1) {
            char next_byte;
            if (!file.read(&next_byte, 1)) {
                return 0xFFFFFFFF;
            }
            length = (length << 8) | (uint8_t)next_byte;
            return length;
        } else if (format == 2) {
            char buffer[4];
            if (!file.read(buffer, 4)) {
                return 0xFFFFFFFF;
            }
            length = ((uint8_t)buffer[0] << 24) | ((uint8_t)buffer[1] << 16) |
                     ((uint8_t)buffer[2] << 8) | (uint8_t)buffer[3];
            return length;
        } else {
            return 0xFFFFFFFF;
        }
    }
};

#endif