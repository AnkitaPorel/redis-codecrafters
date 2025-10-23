#ifndef GEO_H
#define GEO_H

#include "set.h"
#include <string>
#include <set>
#include <vector>
#include <map>

std::string geoadd_command(int& items, int client_fd, std::string& read_buffer, 
    std::map<std::string, SkipList>& sets);

std::string geopos_command(int& items, int client_fd, std::string& read_buffer, 
    std::map<std::string, SkipList>& sets);

std::string geodist_command(int& items, int client_fd, std::string& read_buffer, 
    std::map<std::string, SkipList>& sets);

std::string geosearch_command(int& items, int client_fd, std::string& read_buffer, 
    std::map<std::string, SkipList>& sets);

#endif