#ifndef BULKSTRING_H
#define BULKSTRING_H

#include <fstream>
#include <string>

std::string parsebulkString(int& items, int client_fd, std::string& read_buffer);

#endif // BULKSTRING_H