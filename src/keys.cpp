#include "keys.h"
#include "clear.h"
#include "bulkString.h"
#include <sys/types.h>
#include <sys/socket.h>
#include <unistd.h>

std::string key_command(int& items, int client_fd, std::string& read_buffer, std::map<std::string, std::tuple<std::string,std::chrono::system_clock::time_point>>& dict){
    if (items == 1){
        std::string pattern = parsebulkString(items, client_fd, read_buffer);
        if (pattern == "*"){
            std::string response = "*" + std::to_string(dict.size()) + "\r\n";
            for (const auto& pair : dict) {
                std::string key = pair.first;
                int keyLen = key.length();
                response += "$" + std::to_string(keyLen) + "\r\n";
                response += key + "\r\n";
            }
            return response;
        }
        else {
            std::string response = "-ERR wrong format for keys command, only * supported\r\n";
            return response;
        }
    }
    else{
        std::string response = "-ERR wrong number of arguments for keys command\r\n";
        return response;
    }
    std::string response = "-ERR error\r\n";
    return response;
}