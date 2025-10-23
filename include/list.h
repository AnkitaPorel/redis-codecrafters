#ifndef LIST_H
#define LIST_H

#include <string>
#include <vector>
#include <map>

std::string rpush_command(int& items, int client_fd, std::string& read_buffer, std::map<std::string, std::vector<std::string>>&lDict);

std::string lrange_command(int& items, int client_fd, std::string& read_buffer, std::map<std::string, std::vector<std::string>>&lDict);

std::string lpush_command(int& items, int client_fd, std::string& read_buffer, std::map<std::string, std::vector<std::string>>&lDict);

std::string llen_command(int& items, int client_fd, std::string& read_buffer, std::map<std::string, std::vector<std::string>>&lDict);

std::string lpop_command(int& items, int client_fd, std::string& read_buffer, std::map<std::string, std::vector<std::string>>&lDict);

std::string blpop_command(int& items, int client_fd, std::string& read_buffer, std::map<std::string, std::vector<std::string>>&lDict);

#endif