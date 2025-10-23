#ifndef STREAM_H
#define STREAM_H

#include <string>
#include <vector>
#include <map>
#include <tuple>
#include <chrono>

struct Stream {
    std::map<std::string, std::vector<std::pair<std::string, std::string>>> entries;
    std::string lastID;
};

std::string xadd_command(int& items, int client_fd, std::string& read_buffer, 
    std::map<std::string, std::tuple<std::string,std::chrono::system_clock::time_point>>& dict,
    std::map<std::string, Stream>& sDict);

std::string xrange_command(int& items, int client_fd, std::string& read_buffer, 
    std::map<std::string, Stream>& sDict);

std::string xread_command(int& items, int client_fd, std::string& read_buffer, 
    std::map<std::string, Stream>& sDict);

#endif 