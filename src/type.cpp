#include "type.h"
#include "bulkString.h"
#include "lowerCMD.h"
#include <iostream>
#include "clear.h"
#include <sys/types.h>
#include <sys/socket.h>
#include <unistd.h>


std::string type_command(int& items, int client_fd, std::string& read_buffer, 
    std::map<std::string, std::tuple<std::string,std::chrono::system_clock::time_point>>& dict,
    std::map<std::string, Stream>& sDict){
    if (items == 1){
        std::string key = parsebulkString(items, client_fd, read_buffer);
        //tries to find val
        auto tuple = dict.find(key);
        // val found
        if (tuple != dict.end()){
            auto [val, ttl] = tuple->second;
            // check if an expiry was given 
            if (ttl != std::chrono::system_clock::time_point{}){
                //time expired
                if (ttl <= std::chrono::system_clock::now()) {
                    dict.erase(key);
                    std::string response = "+none\r\n";
                    return response;
                }
            }
            int valSize = val.length();
            std::string response = "+string\r\n";
            return response;
        }
        // val not found
        else if (sDict.find(key) != sDict.end()){
            std::string response = "+stream\r\n";
            return response;
        }
        else{
            std::string response = "+none\r\n";
            return response;
        }
    }
    else{
        std::string response = "-ERR wrong number of arguments for type command\r\n";
        return response;
    }
}