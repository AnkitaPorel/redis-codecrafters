#include "bulkString.h"
#include "clear.h"
#include <iostream>
#include <sys/types.h>
#include <sys/socket.h>
#include <unistd.h>


std::string parsebulkString(int& items, int client_fd, std::string& read_buffer){
  if (read_buffer[0] != '$'){
    std::string response = "-ERR could not read bulkString or wrong argument given \r\n";
    send(client_fd, response.c_str(), response.size(), 0);
    clear_array(items, read_buffer);
  }
  
  ssize_t pos = read_buffer.find("\r\n"); // Get length of string
  int strLen = std::stoi(read_buffer.substr(1,pos));
  read_buffer.erase(0, pos + 2);

  std::string bulkString = read_buffer.substr(0, strLen); // Get string
  read_buffer.erase(0, strLen + 2);
  items -= 1;
  
  return bulkString;
}