#include "command.h"
#include "bulkString.h"
#include "clear.h"
#include "lowerCMD.h"
#include <sys/types.h>
#include <sys/socket.h>
#include <unistd.h>

std::string command_command(int& items, int client_fd, std::string& read_buffer){
  if (items == 1){
    std::string bulkString = parsebulkString(items, client_fd, read_buffer);
    bulkString = lowercase_command(bulkString);
    std::string response = "-ERR command for command ";
    response.append(bulkString);
    response.append(" not yet implemented ");
    response.append("\r\n");
    return response;
  }
  else{
    std::string response = "-ERR wrong number of arguments for command command\r\n";
    return response;
  }
}