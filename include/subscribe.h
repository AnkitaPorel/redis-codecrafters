#ifndef SUBSCRIBE_H
#define SUBSCRIBE_H

#include <string>
#include <set>
#include <vector>
#include <chrono>
#include <map>

std::string subscribe_command(int& items, int client_fd, std::string& read_buffer, 
    std::map<std::string, std::set<int>>& channels, std::set<std::string>& subbed);

std::string unsubscribe_command(int& items, int client_fd, std::string& read_buffer, 
    std::map<std::string, std::set<int>>& channels, std::set<std::string>& subbed);
    
std::string publish_command(int& items, int client_fd, std::string& read_buffer, 
    std::map<std::string, std::set<int>>& channels);

#endif