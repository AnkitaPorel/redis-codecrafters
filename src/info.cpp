#include "info.h"
#include "bulkString.h"
#include "clear.h"
#include <sys/types.h>    
#include <sys/socket.h>
#include <unistd.h> 

std::string info_command(int& items, int client_fd, std::string& read_buffer, Config config){
    if (items == 0){

    }
    if (items == 1){
        if (read_buffer[0] != '$'){
            std::string response = "-ERR argument must be bulk string for info command\r\n";
            return response;
        }
        std::string bulkString = parsebulkString(items, client_fd, read_buffer);
        if (bulkString != "replication"){
            std::string response = "-ERR invalid argument for info command\r\n";
            return response;
        }
        std::string roles = "role:" + config.replica + "\n";
        roles += "master_replid:8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb\n";
        roles += "master_repl_offset:0\n";
        std::string response = "$" + std::to_string(roles.length()) + "\r\n" + roles + "\r\n";
        return response;
    }
    else{
        std::string response = "-ERR wrong number of arguments for info command\r\n";
        return response;
    }
}