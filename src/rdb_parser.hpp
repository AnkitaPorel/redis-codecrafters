#ifndef RDB_PARSER_HPP
#define RDB_PARSER_HPP

#include <string>
#include <vector>
#include <fstream>
#include <stdexcept>
#include <iostream>

#include "redis_commands.hpp"

class RDBParser {
public:
    static std::vector<std::string> load_keys(const ServerConfig& config) {
        std::vector<std::string> keys;
        std::string filepath = config.dir + "/" + config.dbfilename;
        
        std::ifstream file(filepath, std::ios::binary);
        if (!file.is_open()) {
            std::cerr << "RDB file not found: " << filepath << "\n";
            return keys; // File doesn't exist, return empty
        }

        // Read header (REDISxxxx)
        char buffer[9];
        if (!file.read(buffer, 9)) {
            file.close();
            std::cerr << "Failed to read RDB header at position " << file.tellg() << "\n";
            throw std::runtime_error("Invalid RDB file: failed to read header");
        }
        if (std::string(buffer, 5) != "REDIS") {
            file.close();
            std::cerr << "Invalid RDB header: " << std::string(buffer, 5) << "\n";
            throw std::runtime_error("Invalid RDB file: incorrect header");
        }
        std::cerr << "Read RDB header: REDIS" << std::string(buffer + 5, 4) << "\n";

        // Parse until we hit a key-value pair or EOF
        while (file.good()) {
            std::streampos pos = file.tellg();
            char opcode;
            if (!file.read(&opcode, 1)) {
                std::cerr << "Failed to read opcode at position " << pos << "\n";
                break;
            }
            std::cerr << "Read opcode: 0x" << std::hex << (int)(unsigned char)opcode << std::dec 
                      << " at position " << pos << "\n";

            if (opcode == 0xFE) { // Database selector
                uint32_t db_number = read_length(file);
                if (db_number == 0xFFFFFFFF) {
                    file.close();
                    std::cerr << "Failed to read database number at position " << file.tellg() << "\n";
                    throw std::runtime_error("Invalid RDB database selector");
                }
                std::cerr << "Database selector: " << db_number << "\n";
            } else if (opcode == 0x00) { // Value type: string
                // Read key length
                uint32_t key_length = read_length(file);
                if (key_length == 0xFFFFFFFF) {
                    file.close();
                    std::cerr << "Invalid key length at position " << file.tellg() << "\n";
                    throw std::runtime_error("Invalid RDB key length");
                }
                std::cerr << "Key length: " << key_length << "\n";

                // Read key
                std::string key(key_length, '\0');
                if (!file.read(&key[0], key_length)) {
                    file.close();
                    std::cerr << "Failed to read key of length " << key_length 
                              << " at position " << file.tellg() << "\n";
                    throw std::runtime_error("Failed to read key");
                }
                keys.push_back(key);
                std::cerr << "Read key: " << key << "\n";

                // Read value length and skip value
                uint32_t value_length = read_length(file);
                if (value_length == 0xFFFFFFFF) {
                    file.close();
                    std::cerr << "Invalid value length at position " << file.tellg() << "\n";
                    throw std::runtime_error("Invalid RDB value length");
                }
                std::cerr << "Value length: " << value_length << "\n";
                file.seekg(value_length, std::ios::cur);
                std::cerr << "Skipped value, new position: " << file.tellg() << "\n";

                // For this stage, assume one key
                break;
            } else if (opcode == 0xFF) { // EOF
                std::cerr << "Reached EOF at position " << file.tellg() << "\n";
                break;
            } else if (opcode == (char)0xFA) { // AUX field
                // Read AUX key length and key
                uint32_t aux_key_length = read_length(file);
                if (aux_key_length == 0xFFFFFFFF) {
                    file.close();
                    std::cerr << "Invalid AUX key length at position " << file.tellg() << "\n";
                    throw std::runtime_error("Invalid RDB AUX key length");
                }
                std::string aux_key(aux_key_length, '\0');
                if (!file.read(&aux_key[0], aux_key_length)) {
                    file.close();
                    std::cerr << "Failed to read AUX key of length " << aux_key_length 
                              << " at position " << file.tellg() << "\n";
                    throw std::runtime_error("Failed to read AUX key");
                }
                std::cerr << "Read AUX key: " << aux_key << "\n";

                // Read AUX value length and value
                uint32_t aux_value_length = read_length(file);
                if (aux_value_length == 0xFFFFFFFF) {
                    file.close();
                    std::cerr << "Invalid AUX value length at position " << file.tellg() << "\n";
                    throw std::runtime_error("Invalid RDB AUX value length");
                }
                std::string aux_value(aux_value_length, '\0');
                if (!file.read(&aux_value[0], aux_value_length)) {
                    file.close();
                    std::cerr << "Failed to read AUX value of length " << aux_value_length 
                              << " at position " << file.tellg() << "\n";
                    throw std::runtime_error("Failed to read AUX value");
                }
                std::cerr << "Read AUX value: " << aux_value << "\n";
            } else {
                file.close();
                std::cerr << "Unknown opcode: 0x" << std::hex << (int)(unsigned char)opcode 
                          << std::dec << " at position " << pos << "\n";
                throw std::runtime_error("Unknown RDB opcode: " + std::to_string((int)(unsigned char)opcode));
            }
        }

        file.close();
        std::cerr << "Finished parsing RDB file, found " << keys.size() << " keys\n";
        return keys;
    }

private:
    static uint32_t read_length(std::ifstream& file) {
        std::streampos pos = file.tellg();
        char first_byte;
        if (!file.read(&first_byte, 1)) {
            std::cerr << "Failed to read length byte at position " << pos << "\n";
            return 0xFFFFFFFF;
        }
        uint8_t format = (first_byte >> 6) & 0x03;
        uint32_t length = first_byte & 0x3F;
        std::cerr << "Reading length, format: " << (int)format << ", initial length: " << length 
                  << " at position " << pos << "\n";

        if (format == 0) { // 6-bit length
            std::cerr << "6-bit length: " << length << "\n";
            return length;
        } else if (format == 1) { // 14-bit length
            char next_byte;
            if (!file.read(&next_byte, 1)) {
                std::cerr << "Failed to read second length byte at position " << file.tellg() << "\n";
                return 0xFFFFFFFF;
            }
            length = (length << 8) | (uint8_t)next_byte;
            std::cerr << "14-bit length: " << length << "\n";
            return length;
        } else if (format == 2) { // 32-bit length
            char buffer[4];
            if (!file.read(buffer, 4)) {
                std::cerr << "Failed to read 4-byte length at position " << file.tellg() << "\n";
                return 0xFFFFFFFF;
            }
            length = ((uint8_t)buffer[0] << 24) | ((uint8_t)buffer[1] << 16) |
                     ((uint8_t)buffer[2] << 8) | (uint8_t)buffer[3];
            std::cerr << "32-bit length: " << length << "\n";
            return length;
        } else { // format == 3, special encoding
            // For this stage, assume it's an 8-bit integer
            length = (uint8_t)first_byte;
            std::cerr << "Special encoding (format 3), length: " << length << "\n";
            return length;
        }
    }
};

#endif