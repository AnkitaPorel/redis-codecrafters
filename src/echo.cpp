#include "echo.h"
#include "bulkString.h"
#include "clear.h"

std::string echo_command(int& items, int client_fd, std::string& read_buffer){

  if (items == 1){
    if (read_buffer[0] != '$'){
      std::string response = "-ERR argument must be bulk string for echo command\r\n";
      return response;
    }
    std::string bulkString = parsebulkString(items, client_fd, read_buffer);
    std::string response = "+";
    response.append(bulkString);
    response.append("\r\n");
    return response;
  }
  else{
    std::string response = "-ERR wrong number of arguments for echo command\r\n";
    return response;
  }
}