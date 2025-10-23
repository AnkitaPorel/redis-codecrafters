#include "setGet.h"
#include "bulkString.h"
#include "lowerCMD.h"
#include <iostream>
#include "clear.h"
#include <sys/types.h>
#include <sys/socket.h>
#include <unistd.h>

std::string set_command(int& items, int client_fd, std::string& read_buffer, std::map<std::string, std::tuple<std::string,std::chrono::system_clock::time_point>>& dict){
  if (items == 2 || items == 4){
    // read key
    std::string key = parsebulkString(items, client_fd, read_buffer);
    std::string val = parsebulkString(items, client_fd, read_buffer);
    // sets to epoch if no expiry given
    std::chrono::system_clock::time_point ttl;
    // if expiry given
    if (items == 2){
      //check if ex after set key val
      std::string ttlCaller = parsebulkString(items, client_fd, read_buffer);
      ttlCaller = lowercase_command(ttlCaller);
      if (ttlCaller != "ex" && ttlCaller != "px"){
        std::string response = "-ERR incorrect arguments for set command\r\n";
        return response;
      }
      // read ttl blkstring
      int ttlINT = std::stoi(parsebulkString(items, client_fd, read_buffer));
      if (ttlCaller == "ex"){
         ttlINT *= 1000;
      }
      ttl = std::chrono::system_clock::now() + std::chrono::milliseconds(ttlINT);
    }
    dict[key] = make_tuple(val, ttl);
    std::string response = "+OK\r\n";
    return response;
  }
  else{
    std::string response = "-ERR wrong number of arguments for set command\r\n";
    return response;
  }
}

std::string get_command(int& items, int client_fd, std::string& read_buffer, std::map<std::string, std::tuple<std::string,std::chrono::system_clock::time_point>>& dict){
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
          std::string response = "$-1\r\n";
          return response;
        }
      }
      int valSize = val.length();
      std::string response = "$"+ std::to_string(valSize) + "\r\n";
      response += val;
      response += "\r\n";
      return response;
    }
    // val not found
    else{
      std::string response = "$-1\r\n";
      return response;
    }
  }
  else{
    std::string response = "-ERR wrong number of arguments for get command\r\n";
    return response;
  }
}

void set_propogated(int& items, int client_fd, std::string& read_buffer, std::map<std::string, std::tuple<std::string,std::chrono::system_clock::time_point>>& dict){
  if (items == 2 || items == 4){
    // read key
    std::string key = parsebulkString(items, client_fd, read_buffer);
    std::string val = parsebulkString(items, client_fd, read_buffer);
    // sets to epoch if no expiry given
    std::chrono::system_clock::time_point ttl;
    // if expiry given
    if (items == 2){
      //check if ex after set key val
      std::string ttlCaller = parsebulkString(items, client_fd, read_buffer);
      ttlCaller = lowercase_command(ttlCaller);
      if (ttlCaller != "ex" && ttlCaller != "px"){
        clear_array(items, read_buffer);
        return;
      }
      // read ttl blkstring
      int ttlINT = std::stoi(parsebulkString(items, client_fd, read_buffer));
      if (ttlCaller == "ex"){
         ttlINT *= 1000;
      }
      ttl = std::chrono::system_clock::now() + std::chrono::milliseconds(ttlINT);
    }
    dict[key] = make_tuple(val, ttl);
  }
  else{
    std::string response = "-ERR wrong number of arguments for set command\r\n";
    clear_array(items, read_buffer);
  }
}