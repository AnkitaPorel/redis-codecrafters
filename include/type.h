#ifndef TYPE_H
#define TYPE_H

#include <string>
#include "stream.h"
#include <map>
#include <tuple>
#include <chrono>

std::string type_command(int& items, int client_fd, std::string& read_buffer, 
    std::map<std::string, std::tuple<std::string,std::chrono::system_clock::time_point>>& dict,
    std::map<std::string, Stream>& sDict);

#endif