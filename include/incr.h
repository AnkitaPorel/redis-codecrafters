#ifndef INCR_H
#define INCR_H

#include <string>
#include <map>
#include <tuple>
#include <chrono>

std::string incr_command(int& items, int client_fd, std::string& read_buffer, 
    std::map<std::string, std::tuple<std::string,std::chrono::system_clock::time_point>>& dict);

#endif