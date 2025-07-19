#ifndef RDB_PARSER_HPP
#define RDB_PARSER_HPP

#include <string>
#include <vector>
#include <map>
#include <fstream>
#include <iostream>
#include <chrono>
#include <cstring>

struct RDBValueEntry {
    std::string value;
    std::chrono::steady_clock::time_point expiry;
    bool has_expiry;

    RDBValueEntry() : value(""), has_expiry(false) {}
    RDBValueEntry(const std::string& val) : value(val), has_expiry(false) {}
    RDBValueEntry(const std::string& val, std::chrono::steady_clock::time_point exp) 
        : value(val), expiry(exp), has_expiry(true) {}
};

class RDBParser {
private:
    std::vector<uint8_t> data;
    size_t pos;

    uint8_t read_byte() {
        if (pos >= data.size()) {
            throw std::runtime_error("Unexpected end of RDB file");
        }
        return data[pos++];
    }

    uint32_t read_uint32_le() {
        if (pos + 4 > data.size()) {
            throw std::runtime_error("Unexpected end of RDB file");
        }
        uint32_t val = 0;
        for (int i = 0; i < 4; i++) {
            val |= (static_cast<uint32_t>(data[pos + i]) << (i * 8));
        }
        pos += 4;
        return val;
    }

    uint64_t read_uint64_le() {
        if (pos + 8 > data.size()) {
            throw std::runtime_error("Unexpected end of RDB file");
        }
        uint64_t val = 0;
        for (int i = 0; i < 8; i++) {
            val |= (static_cast<uint64_t>(data[pos + i]) << (i * 8));
        }
        pos += 8;
        return val;
    }

    uint32_t read_size_encoding() {
        uint8_t first_byte = read_byte();
        uint8_t type = (first_byte & 0xC0) >> 6;

        switch (type) {
            case 0b00:
                return first_byte & 0x3F;
            
            case 0b01: {
                uint8_t second_byte = read_byte();
                return ((first_byte & 0x3F) << 8) | second_byte;
            }
            
            case 0b10: {
                return read_uint32_le();
            }
            
            default:
                pos--;
                return 0;
        }
    }

    std::string read_string_encoding() {
        uint8_t first_byte = read_byte();
        uint8_t type = (first_byte & 0xC0) >> 6;

        if (type != 0b11) {
            pos--;
            uint32_t size = read_size_encoding();
            
            if (pos + size > data.size()) {
                throw std::runtime_error("String extends beyond file");
            }
            
            std::string result(data.begin() + pos, data.begin() + pos + size);
            pos += size;
            return result;
        }

        uint8_t encoding_type = first_byte & 0x3F;
        
        switch (encoding_type) {
            case 0x00: {
                uint8_t val = read_byte();
                return std::to_string(static_cast<int8_t>(val));
            }
            
            case 0x01: {
                uint16_t val = 0;
                for (int i = 0; i < 2; i++) {
                    val |= (static_cast<uint16_t>(read_byte()) << (i * 8));
                }
                return std::to_string(static_cast<int16_t>(val));
            }
            
            case 0x02: {
                uint32_t val = read_uint32_le();
                return std::to_string(static_cast<int32_t>(val));
            }
            
            case 0x03: {
                throw std::runtime_error("LZF compressed strings not supported in this stage");
            }
            
            default:
                throw std::runtime_error("Unsupported string encoding type: " + std::to_string(encoding_type));
        }
    }

    void skip_metadata_section() {
        while (pos < data.size()) {
            uint8_t byte = read_byte();
            if (byte == 0xFA) {
                read_string_encoding();
                read_string_encoding();
            } else {
                pos--;
                break;
            }
        }
    }

public:
    std::map<std::string, RDBValueEntry> parse_rdb_file(const std::string& filepath) {
        std::map<std::string, RDBValueEntry> result;
        
        std::ifstream file(filepath, std::ios::binary);
        if (!file) {
            std::cout << "RDB file not found: " << filepath << " (treating database as empty)" << std::endl;
            return result;
        }

        file.seekg(0, std::ios::end);
        size_t file_size = file.tellg();
        file.seekg(0, std::ios::beg);
        
        data.resize(file_size);
        file.read(reinterpret_cast<char*>(data.data()), file_size);
        file.close();

        if (data.empty()) {
            std::cout << "RDB file is empty: " << filepath << std::endl;
            return result;
        }

        pos = 0;

        try {
            std::cout << "Parsing RDB file: " << filepath << std::endl;
            
            if (pos + 9 > data.size()) {
                throw std::runtime_error("File too small for header");
            }
            
            std::string magic(data.begin() + pos, data.begin() + pos + 5);
            pos += 5;
            
            if (magic != "REDIS") {
                throw std::runtime_error("Invalid RDB magic string: " + magic);
            }
            
            std::string version(data.begin() + pos, data.begin() + pos + 4);
            pos += 4;
            std::cout << "RDB version: " << version << std::endl;
            
            skip_metadata_section();
            
            while (pos < data.size()) {
                uint8_t marker = read_byte();
                
                if (marker == 0xFF) {
                    std::cout << "Reached end of RDB file" << std::endl;
                    break;
                }
                
                if (marker == 0xFE) {
                    uint32_t db_index = read_size_encoding();
                    std::cout << "Processing database index: " << db_index << std::endl;
                    
                    if (pos < data.size() && data[pos] == 0xFB) {
                        read_byte();
                        uint32_t hash_table_size = read_size_encoding();
                        uint32_t expire_hash_table_size = read_size_encoding();
                        std::cout << "Hash table size: " << hash_table_size 
                                  << ", Expire hash table size: " << expire_hash_table_size << std::endl;
                    }
                    
                    while (pos < data.size()) {
                        uint8_t next_byte = data[pos];
                        
                        if (next_byte == 0xFE || next_byte == 0xFF) {
                            break;
                        }
                        
                        std::chrono::steady_clock::time_point expiry_time;
                        bool has_expiry = false;
                        
                        if (next_byte == 0xFC) {
                            read_byte();
                            uint64_t expire_ms = read_uint64_le();
                            std::cout << "Key has expiry in milliseconds: " << expire_ms << std::endl;
                            
                            auto unix_epoch = std::chrono::system_clock::from_time_t(0);
                            auto expire_time_point = unix_epoch + std::chrono::milliseconds(expire_ms);
                            auto now_system = std::chrono::system_clock::now();
                            auto now_steady = std::chrono::steady_clock::now();
                            
                            expiry_time = now_steady + (expire_time_point - now_system);
                            has_expiry = true;
                        } else if (next_byte == 0xFD) {
                            read_byte();
                            uint32_t expire_sec = read_uint32_le();
                            std::cout << "Key has expiry in seconds: " << expire_sec << std::endl;
                            
                            auto unix_epoch = std::chrono::system_clock::from_time_t(0);
                            auto expire_time_point = unix_epoch + std::chrono::seconds(expire_sec);
                            auto now_system = std::chrono::system_clock::now();
                            auto now_steady = std::chrono::steady_clock::now();
                            
                            expiry_time = now_steady + (expire_time_point - now_system);
                            has_expiry = true;
                        }
                        
                        uint8_t value_type = read_byte();
                        
                        if (value_type != 0x00) {
                            throw std::runtime_error("Unsupported value type: " + std::to_string(value_type));
                        }
                        
                        std::string key = read_string_encoding();
                        std::string value = read_string_encoding();
                        
                        std::cout << "Loaded key: '" << key << "' -> value: '" << value << "'" 
                                  << (has_expiry ? " (with expiry)" : " (no expiry)") << std::endl;
                        
                        if (has_expiry) {
                            result[key] = RDBValueEntry(value, expiry_time);
                        } else {
                            result[key] = RDBValueEntry(value);
                        }
                    }
                }
            }
            
            std::cout << "Successfully loaded " << result.size() << " keys from RDB file" << std::endl;
            
        } catch (const std::exception& e) {
            std::cerr << "Error parsing RDB file: " << e.what() << std::endl;
            std::cerr << "Position in file: " << pos << "/" << data.size() << std::endl;
        }
        
        return result;
    }
};

#endif