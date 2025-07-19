#ifndef RDB_PARSER_HPP
#define RDB_PARSER_HPP

#include <string>
#include <vector>
#include <fstream>
#include <stdexcept>
#include <iostream>

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

        char buffer[9];
        // Read header (REDISxxxx)
        if (!file.read(buffer, 9)) {
            file.close();
            std::cerr << "Failed to read RDB header\n";
            throw std::runtime_error("Invalid RDB file: failed to read header");
        }
        if (std::string(buffer, 5) != "REDIS") {
            file.close();
            std::cerr << "Invalid RDB header: " << std::string(buffer, 5) << "\n";
            throw std::runtime_error("Invalid RDB file: incorrect header");
        }

        // Skip metadata (AUX fields, etc.) until database selector (0xFE) or key-value pair (0x00)
        while (file.good()) {
            char opcode;
            if (!file.read(&opcode, 1)) {
                std::cerr << "Failed to read opcode\n";
                break;
            }

            if (opcode == 0xFE) { // Database selector
                uint32_t db_number = read_length(file);
                if (db_number == 0xFFFFFFFF) {
                    file.close();
                    std::cerr << "Failed to read database number\n";
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

                // Read key
                std::string key(key_length, '\0');
                if (!file.read(&key[0], key_length)) {
                    file.close();
                    std::cerr << "Failed to read key of length " << key_length << "\n";
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
                file.seekg(value_length, std::ios::cur); // Skip value
                std::cerr << "Skipped value of length " << value_length << "\n";

                // For this stage, assume one key
                break;
            } else if (opcode == 0xFF) { // EOF
                std::cerr << "Reached EOF\n";
                break;
            } else if (opcode == 0xFA) { // AUX field
                uint32_t key_length = read_length(file);
                if (key_length == 0xFFFFFFFF) {
                    file.close();
                    std::cerr << "Invalid AUX key length\n";
                    throw std::runtime_error("Invalid RDB AUX key length");
                }
                file.seekg(key_length, std::ios::cur); // Skip AUX key
                uint32_t value_length = read_length(file);
                if (value_length == 0xFFFFFFFF) {
                    file.close();
                    std::cerr << "Invalid AUX value length\n";
                    throw std::runtime_error("Invalid RDB AUX value length");
                }
                file.seekg(value_length, std::ios::cur); // Skip AUX value
                std::cerr << "Skipped AUX field\n";
            } else {
                std::cerr << "Unknown opcode: " << std::hex << (int)(unsigned char)opcode << std::dec << "\n";
                // Skip unknown opcodes cautiously
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
            std::cerr << "Failed to read length byte\n";
            return 0xFFFFFFFF;
        }
        uint8_t format = (first_byte >> 6) & 0x03;
        uint32_t length = first_byte & 0x3F;

        if (format == 0) { // 6-bit length
            return length;
        } else if (format == 1) { // 14-bit length
            char next_byte;
            if (!file.read(&next_byte, 1)) {
                std::cerr << "Failed to read second length byte\n";
                return 0xFFFFFFFF;
            }
            length = (length << 8) | (uint8_t)next_byte;
            return length;
        } else if (format == 2) { // 32-bit length
            char buffer[4];
            if (!file.read(buffer, 4)) {
                std::cerr << "Failed to read 4-byte length\n";
                return 0xFFFFFFFF;
            }
            length = ((uint8_t)buffer[0] << 24) | ((uint8_t)buffer[1] << 16) |
                     ((uint8_t)buffer[2] << 8) | (uint8_t)buffer[3];
            return length;
        } else if (format == 3) { // Special encoding
            // For this stage, we don't expect special encodings like compressed strings
            std::cerr << "Unsupported special length encoding\n";
            return 0xFFFFFFFF;
        }
        return 0xFFFFFFFF; // Should not reach here
    }
};

#endif