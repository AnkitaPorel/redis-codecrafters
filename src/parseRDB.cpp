#include "parseRDB.h"
#include <iostream>
#include <cstdint>
#include <stdexcept>
#include <iomanip>

uint32_t read_size_encoded(std::ifstream &file)
{
  uint8_t first = file.get();
  uint8_t type = (first & 0xC0) >> 6;

  if (type == 0b00)
  {
    // 6-bit integer (lower 6 bits)
    return first & 0x3F;
  }
  else if (type == 0b01)
  {
    uint8_t second = file.get();
    return ((first & 0x3F) << 8) | second;
  }
  else if (type == 0b10)
  {
    uint8_t bytes[4];
    file.read(reinterpret_cast<char *>(bytes), 4);
    return (bytes[0] << 24) | (bytes[1] << 16) | (bytes[2] << 8) | bytes[3];
  }
  else
  {
    throw std::runtime_error("Invalid length encoding (special encoding found)");
  }
}

std::string read_encoded_string(std::ifstream &file)
{
  uint8_t prefix = file.get();
  uint8_t first2 = (prefix & 0xC0) >> 6;
  if (first2 == 0b11)
  {
    if (prefix == 0xC0)
    {
      uint8_t value = file.get();
      return std::to_string(value);
    }
    else if (prefix == 0xC1)
    {
      uint8_t bytes[2];
      file.read(reinterpret_cast<char *>(bytes), 2);
      uint16_t value = bytes[0] | (bytes[1] << 8);
      return std::to_string(value);
    }
    else if (prefix == 0xC2)
    {
      uint8_t bytes[4];
      file.read(reinterpret_cast<char *>(bytes), 4);
      uint16_t value = bytes[0] | (bytes[1] << 8) | (bytes[2] << 16) | (bytes[3] << 24);
      return std::to_string(value);
    }
    else if (prefix == 0xC3)
    {
      std::cerr << "LZF compressed string encountered (unsupported).\n";
    }
  }
  else
  {
    int len = 0;
    if (first2 == 0b00)
    {
      len = prefix & 0x3F;
    }
    else if (first2 == 0b01)
    {
      uint8_t next = file.get();
      len = ((prefix & 0x3F) << 8) | next;
    }
    else if (first2 == 0b10)
    {
      uint8_t bytes[4];
      file.read(reinterpret_cast<char *>(bytes), 4);
      len = bytes[0] | (bytes[1] << 8) | (bytes[2] << 16) | (bytes[3] << 24);
    }
    std::string str(len, '\0');
    file.read(&str[0], len);
    return str;
  }
  return "error";
}

int parse_rdbFile(std::map<std::string, std::tuple<std::string, std::chrono::system_clock::time_point>> &dict, std::string filepath)
{
  std::ifstream file(filepath, std::ios::binary);

  if (!file)
  {
    //std::cout << "Did not find file " << filepath << std::endl;
    return -1;
  }

  //std::cout << "FOUND file at " << filepath << std::endl;

  // Read header (first 9 bytes)
  char header[9];
  file.read(header, 9);
  std::string headerStr(header, 9);
  //std::cout << "Header: " << headerStr << std::endl;

  while (true)
  {
    int type = file.get();
    if (type == EOF)
    {
      //std::cout << "EOF FOUND" << std::endl;
      break;
    }
    if (type == 0xFF)
    {
      //std::cout << "FF FOUND" << std::endl;
      break;
    }

    std::chrono::system_clock::time_point expiry;

    // Start of Database
    if (type == 0xFE)
    {
      //std::cout << "Database Start found" << std::endl;
      uint32_t index = read_size_encoded(file);
      //std::cout << "index of: " << std::to_string(index) << std::endl;
      continue;
    }
    else if (type == 0xFA)
    {
      //std::cout << "0xFA (AUX FIELD) FOUND — skipping\n";
      std::string auxKey = read_encoded_string(file);
      std::string auxVal = read_encoded_string(file);
      //std::cout << "  Aux Key: " << auxKey << " | Aux Val: " << auxVal << std::endl;
      continue;
    }
    // Hash Information
    else if (type == 0xFB)
    {
      int mainSize = file.get();
      int expSize = file.get();
      //std::cout << "Hash information found:  mainsize = " << std::to_string(mainSize) << " expsize = " << std::to_string(expSize) << std::endl;
      continue;
    }

    // Expire in miliseconds FC
    if (type == 0xFC)
    {
      //std::cout << "FC (miliseconds expiry) FOUND" << std::endl;
      uint8_t bytes[8];
      file.read(reinterpret_cast<char *>(bytes), 8);
      if (file.gcount() != 8)
      {
        throw std::runtime_error("Failed to read 8 bytes for uint64_t");
      }
      uint64_t value = 0;
      for (int i = 0; i < 8; ++i)
      {
        value |= (static_cast<uint64_t>(bytes[i]) << (8 * i));  // Little-endian
      }
      expiry = std::chrono::system_clock::time_point{std::chrono::milliseconds{value}};
      std::time_t expiry_time = std::chrono::system_clock::to_time_t(expiry);
      //std::cout << "Expiry: " << std::put_time(std::localtime(&expiry_time), "%Y-%m-%d %H:%M:%S") << '\n';
      type = file.get();
    } else if (type == 0xFD) { // Expire in seconds FD
      //std::cout << "FD (Seconds expiry) FOUND" << std::endl;
      uint8_t bytes[4];
      file.read(reinterpret_cast<char *>(bytes), 4);
      if (file.gcount() != 4)
      {
        throw std::runtime_error("Failed to read 4 bytes for uint32_t");
      }
      uint32_t value = 0;
      for (int i = 0; i < 4; ++i)
      {
        value |= (static_cast<uint32_t>(bytes[i]) << (8 * i));  // Little-endian
      }
      expiry = std::chrono::system_clock::time_point{std::chrono::seconds{value}};
      std::time_t expiry_time = std::chrono::system_clock::to_time_t(expiry);
      //std::cout << "Expiry: " << std::put_time(std::localtime(&expiry_time), "%Y-%m-%d %H:%M:%S") << '\n';
      type = file.get();
    }

    if (type == 0x00)
    {
      std::string key = read_encoded_string(file);
      std::string value = read_encoded_string(file);
      //std::cout << "Found key:" << key << " With value of: " << value << std::endl;
      dict[key] = make_tuple(value, expiry);
    }
  }
  return 0;
}