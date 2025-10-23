#ifndef KEYS_H
#define KEYS_H

#include <string>
#include <map>
#include <tuple>
#include <chrono>

std::string key_command(int& items, int client_fd, std::string& read_buffer, std::map<std::string, std::tuple<std::string,std::chrono::system_clock::time_point>>& dict);

#endif