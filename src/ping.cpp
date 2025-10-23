#include "ping.h"
#include "bulkString.h"
#include "clear.h"
#include <iostream>
#include <sys/types.h>
#include <sys/socket.h>
#include <unistd.h>

std::string ping_command(int& items, int client_fd, std::string& read_buffer){
  if (items == 0){
    std::string response = "+PONG\r\n";
    return response;
  } 
  else if (items == 1){
    std::string bulkString = parsebulkString(items, client_fd, read_buffer);
    std::string response = "+";
    response.append(bulkString);
    response.append("\r\n");
    return response;
  }
  else{
    std::string response = "-ERR wrong number of arguments for ping command\r\n";
    return response;
  }
}