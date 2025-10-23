#ifndef ECHO_H
#define ECHO_H

#include <string>

std::string echo_command(int& items, int client_fd, std::string& read_buffer);

#endif