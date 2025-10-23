#include "list.h"
#include <chrono>
#include <thread>
#include "bulkString.h"
#include <iostream>

std::string rpush_command(int& items, int client_fd, std::string& read_buffer, std::map<std::string, std::vector<std::string>>&lDict){
    if (items >= 2){
        std::string key = parsebulkString(items, client_fd, read_buffer);
        std::cout << key << std::endl;
        auto tuple = lDict.find(key);
        int itemsCopy = items;
        for (int i = 0; i < itemsCopy; i++){
            std::string element = parsebulkString(items, client_fd, read_buffer);
            std::cout << element << std::endl;
            lDict[key].push_back(element);
        }
        size_t vecSize = lDict[key].size();
        std::string response = ":"+std::to_string(vecSize)+"\r\n";
        return response;
    }
    else{
        std::string response = "-ERR wrong number of arguments for rpush command\r\n";
        return response;
    }
}

std::string lrange_command(int& items, int client_fd, std::string& read_buffer, std::map<std::string, std::vector<std::string>>&lDict){
    if (items == 3){
        int len = 0; 
        std::string response = "";
        std::string key = parsebulkString(items, client_fd, read_buffer);
        int startIndex = std::stoi(parsebulkString(items, client_fd, read_buffer));
        int endIndex = std::stoi(parsebulkString(items, client_fd, read_buffer));
        auto tuple = lDict.find(key);

        // List doesnt exist
        if (tuple == lDict.end()){ 
            response = "*0\r\n";
            return response;
        }

        len = lDict[key].size();

        //convert negative indexes;
        if (startIndex < 0){
            startIndex = len + startIndex;
            startIndex = (startIndex > 0) ? startIndex : 0;
        }
        if (endIndex < 0){
            endIndex = len + endIndex;
            endIndex = (endIndex > 0) ? endIndex : 0;
        }
        // Start greater than end /  start greater than size
        if ((startIndex > endIndex) || (startIndex >= len)){ 
            response = "*0\r\n";
            return response;
        }

        // default end to last index if too big
        endIndex = (endIndex < len-1) ? endIndex : len-1;

        response = "*" + std::to_string(endIndex-startIndex+1) + "\r\n";

        for (int i = startIndex; i <= endIndex; i++){
            std::string element = lDict[key][i];
            response += "$"+ std::to_string(element.size()) + "\r\n";
            response += element + "\r\n";
        }
        return response;
    }
    else{
        std::string response = "-ERR wrong number of arguments for lrange command\r\n";
        return response;
    }
}

std::string lpush_command(int& items, int client_fd, std::string& read_buffer, std::map<std::string, std::vector<std::string>>&lDict){
    if (items >= 2){
        std::string key = parsebulkString(items, client_fd, read_buffer);
        auto tuple = lDict.find(key);
        int itemsCopy = items;
        for (int i = 0; i < itemsCopy; i++){
            std::string element = parsebulkString(items, client_fd, read_buffer);
            std::cout << element << std::endl;
            lDict[key].insert(lDict[key].begin(),element);
        }
        size_t vecSize = lDict[key].size();
        std::string response = ":"+std::to_string(vecSize)+"\r\n";
        return response;
    }
    else{
        std::string response = "-ERR wrong number of arguments for lpush command\r\n";
        return response;
    }
}

std::string llen_command(int& items, int client_fd, std::string& read_buffer, std::map<std::string, std::vector<std::string>>&lDict){
    if (items == 1){
        std::string key = parsebulkString(items, client_fd, read_buffer);
        int len = 0; 
        std::string response = "";
        auto tuple = lDict.find(key);
        if (tuple == lDict.end()){  // If doesnt exist return 0
            response = ":" + std::to_string(len) + "\r\n";
            return response;
        }
        len = lDict[key].size();
        response = ":" + std::to_string(len) + "\r\n";
        return response;
    }
    else{
        std::string response = "-ERR wrong number of arguments for llen command\r\n";
        return response;
    }
}

std::string lpop_command(int& items, int client_fd, std::string& read_buffer, std::map<std::string, std::vector<std::string>>&lDict){
    if (items == 1 || items == 2){
        std::string key = parsebulkString(items, client_fd, read_buffer);
        int len = 0; 
        std::string response = "";
        auto tuple = lDict.find(key);
        if (tuple == lDict.end() || lDict[key].size()==0){  // If doesnt exist or empty 
            response = "$-1\r\n";
            return response;
        }
        if(items == 0){
            std::string erased = lDict[key].front();
            lDict[key].erase(lDict[key].begin());
            response += "$"+ std::to_string(erased.size()) + "\r\n";
            response += erased + "\r\n";
            return response;
        }
        int num = std::stoi(parsebulkString(items, client_fd, read_buffer));
        if (num > lDict[key].size() ) num = lDict[key].size();
        response += "*" + std::to_string(num) + "\r\n";
        for (int i = 0; i<num;i++) {
            std::string erased = lDict[key].front();
            lDict[key].erase(lDict[key].begin());
            response += "$"+ std::to_string(erased.size()) + "\r\n";
            response += erased + "\r\n";
        }
        return response;
    }
    else{
        std::string response = "-ERR wrong number of arguments for lpop command\r\n";
        return response;
    }
}

std::string blpop_command(int& items, int client_fd, std::string& read_buffer, std::map<std::string, std::vector<std::string>>&lDict){
    if (items == 2){
        std::string key = parsebulkString(items, client_fd, read_buffer);
        float waitTime = std::stof(parsebulkString(items, client_fd, read_buffer));
        std::string response = "";
        bool infiniteTime = false;

        if(waitTime == 0){
            infiniteTime = true;
        }
        auto end = std::chrono::steady_clock::now() + std::chrono::duration<float>(waitTime); 
        do{
            if(lDict[key].size() != 0){
                std::string erased = lDict[key].front();
                lDict[key].erase(lDict[key].begin());
                response += "*2\r\n";
                response += "$"+ std::to_string(key.size()) + "\r\n";
                response += key + "\r\n";
                response += "$"+ std::to_string(erased.size()) + "\r\n";
                response += erased + "\r\n";
                return response;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(5));
        } while( response == "" && (std::chrono::steady_clock::now() < end || infiniteTime));
        response = "*-1\r\n";
        return response;
    }
    else{
        std::string response = "-ERR wrong number of arguments for blpop command\r\n";
        return response;
    }
}