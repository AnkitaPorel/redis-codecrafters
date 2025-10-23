#include "subscribe.h"
#include "bulkString.h"

#include <iostream>
#include <sys/types.h>    
#include <sys/socket.h>
#include <unistd.h> 

std::string subscribe_command(int& items, int client_fd, std::string& read_buffer,
    std::map<std::string, std::set<int>>& channels, std::set<std::string>& subbed){

        if (items == 1){
            
            std::string key = parsebulkString(items, client_fd, read_buffer);
            channels[key].insert(client_fd);
            subbed.insert(key);

            std::string response = "*3\r\n$9\r\nsubscribe\r\n";
            response += "$" + std::to_string(key.size()) + "\r\n";
            response += key + "\r\n";
            response += ":" + std::to_string(subbed.size()) + "\r\n";

            std::cout << "client_fd: " << std::to_string(client_fd)<< std::endl;
            for (const auto& [key, s] : channels) {
                std::cout << key << ": { ";
                for (const int val : s) {
                    std::cout << val << " ";
                }
                std::cout << "}" << std::endl;
            }

            return response;

        }
        else{
            std::string response = "-ERR wrong number of arguments for subscribe command\r\n";
            return response;
        }
}

std::string unsubscribe_command(int& items, int client_fd, std::string& read_buffer,
    std::map<std::string, std::set<int>>& channels, std::set<std::string>& subbed){
        if (items == 1){
            
            std::string key = parsebulkString(items, client_fd, read_buffer);
            channels[key].erase(client_fd);
            subbed.erase(key);

            std::string response = "*3\r\n$11\r\nunsubscribe\r\n";
            response += "$" + std::to_string(key.size()) + "\r\n";
            response += key + "\r\n";
            response += ":" + std::to_string(subbed.size()) + "\r\n";
            return response;

        }
        else{
            std::string response = "-ERR wrong number of arguments for unsubscribe command\r\n";
            return response;
        }
    }

std::string publish_command(int& items, int client_fd, std::string& read_buffer, 
    std::map<std::string, std::set<int>>& channels){
        if (items == 2){
            for (const auto& [key, s] : channels) {
                std::cout << key << ": { ";
                for (const int val : s) {
                    std::cout << val << " ";
                }
                std::cout << "}" << std::endl;
            }
            std::string channel = parsebulkString(items, client_fd, read_buffer);
            std::string message = parsebulkString(items, client_fd, read_buffer);

            for (int client: channels[channel]){
                std::string clientMessage = "*3\r\n";
                clientMessage += "$7\r\nmessage\r\n";
                clientMessage += "$" + std::to_string(channel.size()) + "\r\n";
                clientMessage += channel + "\r\n";
                clientMessage += "$" + std::to_string(message.size()) + "\r\n";
                clientMessage += message + "\r\n";
                send(client, clientMessage.c_str(), clientMessage.size(), 0);
            }

            std::string response = ":" + std::to_string(channels[channel].size()) + "\r\n";
            return response;
        }
        else{
            std::string response = "-ERR wrong number of arguments for publish command\r\n";
            return response;
        }
    }