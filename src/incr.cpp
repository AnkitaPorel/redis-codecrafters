#include "incr.h"
#include "clear.h"
#include "bulkString.h"
#include <sys/types.h>
#include <sys/socket.h>
#include <unistd.h>

std::string incr_command(int& items, int client_fd, std::string& read_buffer, 
    std::map<std::string, std::tuple<std::string,std::chrono::system_clock::time_point>>& dict){
    if (items == 1){
        std::string item = parsebulkString(items,client_fd, read_buffer);
        //tries to find key in  dictionary
        auto tuple = dict.find(item);
        // val not found
        if (tuple == dict.end()){
            std::chrono::system_clock::time_point ttl;
            dict[item] = make_tuple("1",ttl);
            std::string response = ":1\r\n";
            return response;
        }
        //val is found
        auto [val, ttl] = tuple->second;
        try{
            long long itemINT = std::stoi(val);
            dict[item] = make_tuple(std::to_string(itemINT+1), ttl);
            std::string response = ":"+ std::to_string(itemINT+1) +"\r\n";
            return response;
        } catch(...){
            std::string response = "-ERR value is not an integer or out of range\r\n";
            return response;
        }
        
    }
    else{
        std::string response = "-ERR wrong number of arguments for incr command\r\n";
        return response;
    }
}