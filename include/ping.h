#ifndef PING_H
#define PING_H

#include <string>

std::string ping_command(int& items, int client_fd, std::string& read_buffer);

#endif