#ifndef SETGET_H
#define SETGET_H

#include <string>
#include <map>
#include <tuple>
#include <chrono>

std::string set_command(int& items, int client_fd, std::string& read_buffer, std::map<std::string, std::tuple<std::string,std::chrono::system_clock::time_point>>& dict);

std::string get_command(int& items, int client_fd, std::string& read_buffer, std::map<std::string, std::tuple<std::string,std::chrono::system_clock::time_point>>& dict);

void set_propogated(int& items, int client_fd, std::string& read_buffer, std::map<std::string, std::tuple<std::string,std::chrono::system_clock::time_point>>& dict);
#endif