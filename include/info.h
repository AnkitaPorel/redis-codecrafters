#ifndef INFO_H
#define INFO_H

#include <string>
#include "config.h"

std::string info_command(int& items, int client_fd, std::string& read_buffer, Config config);

#endif