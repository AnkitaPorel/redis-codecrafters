#include "config.h"
#include "clear.h"
#include "lowerCMD.h"
#include "bulkString.h"
#include <sys/types.h>
#include <sys/socket.h>
#include <unistd.h>


std::string config_command(int& items, int client_fd, std::string& read_buffer, Config config){
  //error if 1 or 0 items left, like "config get" or "config"
  if (items <= 1){
    std::string response = "-ERR wrong number of arguments for config command\r\n";
    return response;
  }
  // only work with config get not other config command
  std::string subCmd = parsebulkString(items, client_fd, read_buffer);
  subCmd = lowercase_command(subCmd);
  if ( subCmd != "get"){
    std::string response = "-ERR wrong arguments for config command\r\n";
    return response;
  }
  std::string response = "*" + std::to_string(items*2) + "\r\n";
  while (items != 0){
    std::string key  = parsebulkString(items, client_fd, read_buffer);
    std::string val;
    if (key == "dir") val = config.dir;
    else if (key == "dbfilename") val = config.dbfilename; 
    else{
      std::string response = "-ERR config parameter not found \r\n";
      return response;
    }
    int keyLen = key.length();
    int valLen = val.length();
    response += "$" + std::to_string(keyLen) + "\r\n";
    response += key + "\r\n";
    response += "$" + std::to_string(valLen) + "\r\n";
    response += val + "\r\n";
  }
  return response;
}