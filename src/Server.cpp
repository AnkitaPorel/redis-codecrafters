#include "bulkString.h"
#include "clear.h"
#include "command.h"
#include "config.h"
#include "echo.h"
#include "lowerCMD.h"
#include "parseRDB.h"
#include "ping.h"
#include "setGet.h"
#include "keys.h"
#include "info.h"
#include "type.h"
#include "stream.h"
#include "incr.h"
#include "list.h"
#include "subscribe.h"
#include "set.h"
#include "geo.h"

#include <mutex>
#include <iostream>
#include <set>
#include <cstdlib>
#include <vector>
#include <thread>
#include <string>
#include <cstring>
#include <cctype>
#include <tuple>
#include <map>
#include <chrono>
#include <fstream>
#include <unistd.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <netdb.h>

struct HandshakeResult {
    int fd;
    std::string leftover;
};

const size_t BUFFER_SIZE = 1024;
std::vector<int> slaves;
std::map<int,int> replicaOffsets;
std::mutex replicaMutex;

using RedisDict = std::map<std::string, std::tuple<std::string, std::chrono::system_clock::time_point>>;

std::string extractArray(std::string& buffer){
  if (buffer.empty() || buffer[0] != '*') {
      throw std::runtime_error("Invalid RESP: expected array start '*'");
  }
  size_t end = buffer.find("\r\n");
  if (end == std::string::npos) throw std::runtime_error("Invalid RESP array");
  int count = std::stoi(buffer.substr(1, end - 1));

  int arrayEnd = end + 2;

  for (int i = 0; i < count; i++) {
    if (buffer[arrayEnd] != '$') {
      throw std::runtime_error("Invalid RESP: expected bulk string '$'");
    }

    end = buffer.find("\r\n", arrayEnd);
    if (end == std::string::npos) throw std::runtime_error("Invalid RESP: missing CRLF for length");
    int bulkLen = std::stoi(buffer.substr(arrayEnd + 1, end - (arrayEnd + 1)));
    arrayEnd = end + 2;

    arrayEnd += bulkLen;

    if (arrayEnd + 2 > buffer.size() || buffer.substr(arrayEnd, 2) != "\r\n") {
        throw std::runtime_error("Invalid RESP: missing CRLF after bulk string");
    }
    arrayEnd += 2;
  }

  // Extract array
  std::string firstArray = buffer.substr(0, arrayEnd);

  // Remove it from the buffer
  buffer.erase(0, arrayEnd);

  return firstArray;
}

void handle_master(int client_fd, Config config, std::string filepath, RedisDict& dict, std::string initial_buffer = ""){
  std::string read_buffer = initial_buffer;
  char buffer[BUFFER_SIZE] = {0};
  int offset = 0;

  while(true){
    size_t pos = read_buffer.find("\r\n");
    if (pos == std::string::npos){
      ssize_t recieved = recv(client_fd, buffer, sizeof(buffer)-1, 0);
      if (recieved < 0) {
        std::cerr << "error\n";
        break;
      } else if (recieved == 0) {
        std::cerr << "master disconnected \n";
        close(client_fd);
        break;
      }   
      read_buffer.append(buffer, recieved); // Array gonna be like *2\r\n$4\r\ECHO\r\n$5\r\nworld\r\n
    }
    
    std::cout << "handle_master: " << read_buffer << std::endl;
    while (true) {
      if (read_buffer.empty()) break;

      size_t buffer_start_len = read_buffer.size(); 

      size_t pos = read_buffer.find("\r\n");
      if (pos == std::string::npos) break;
      
      std::string start = read_buffer.substr(0,pos);
      read_buffer.erase(0, pos + 2);
      if (start[0]== '*'){  // Array gonna be like $4\r\nECHO\r\n$5\r\nworld\r\n
        int items = std::stoi(start.substr(1));
        pos = read_buffer.find("\r\n");
        std::string indicator = read_buffer.substr(0,pos);
        read_buffer.erase(0, pos + 2);
        if (indicator[0] == '$'){ // Array gonna be like ECHO\r\n$5\r\nworld\r\n
          int strLen = std::stoi(indicator.substr(1,pos-1));
          std::string bulkString = read_buffer.substr(0, strLen);
          bulkString = lowercase_command(bulkString);
          read_buffer.erase(0, strLen + 2);
          items -= 1;
          if ( bulkString == "set" ){
            set_propogated(items, client_fd, read_buffer, dict);
            offset += buffer_start_len - read_buffer.size();
          }
          else if ( bulkString == "ping"){
            offset += buffer_start_len - read_buffer.size();
          }
          else if (bulkString == "replconf"){
            std::string acks = parsebulkString(items, client_fd, read_buffer);
            acks = lowercase_command(acks);
            std::string filler = parsebulkString(items, client_fd, read_buffer);
            if (acks == "getack" && filler == "*"){
              std::cout << "Called GetAck " << std::endl;
              std::string string_offset = std::to_string(offset);
              std::string response = "*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n$";
              response += std::to_string(string_offset.length()) + "\r\n";
              response += std::to_string(offset) + "\r\n";
              send(client_fd, response.c_str(), response.size(), 0);
              clear_array(items,read_buffer);
              offset += buffer_start_len - read_buffer.size();
            }
          }
          else{
            std::string response = "-ERR unrecognized command\r\n";
            send(client_fd, response.c_str(), response.size(), 0);
            clear_array(items,read_buffer);
          }
        }
      }
      else if (start[0]== '$'){
          int strLen = std::stoi(start.substr(1,pos));
          std::string bulkString = read_buffer.substr(0, strLen);
          read_buffer.erase(0, strLen + 2);
      }
    }
  }
}

void handle_client(int client_fd, Config config, std::string filepath, RedisDict& dict, 
  std::map<std::string, Stream>& sDict, std::map<std::string, std::vector<std::string>>&lDict,
  std::map<std::string, std::set<int>>& channels, std::map<std::string, SkipList>& sets ) {

  int replOffset = 0;
  std::string read_buffer;
  char buffer[BUFFER_SIZE] = {0};

  std::set<std::string> subbed;
  bool subMode = false;

  std::string response = "";
  std::string tempResponse = "";
  std::string commandQueue = "";
  int queuedUp = 0;
  bool multi = false;

  while(true){
    ssize_t recieved = recv(client_fd, buffer, sizeof(buffer)-1, 0); // Waiting for client input
    if (recieved < 0) {
      std::cerr << "error\n";
      break;
    } else if (recieved == 0) {
      std::cerr << "client disconnected \n";
      close(client_fd);
      break;
    }
    
    read_buffer.append(buffer, recieved); // Read clients into buffer
    while (true) {
      if (read_buffer.empty()) break;
      size_t pos = read_buffer.find("\r\n");
      if (pos == std::string::npos) break; // Check there is something to process / not half a command

      std::string prefix = "";
      if(multi){
        // If multi, first check if being given discard
        std::string discardPattern = "*1\r\n$7\r\ndiscard\r\n";
        if (read_buffer.size() >= discardPattern.size()) {
          prefix = read_buffer.substr(0,discardPattern.size());
          prefix = lowercase_command(prefix);
          if (prefix.compare(0, discardPattern.size(), discardPattern) == 0){ // Discard
            int indicator = read_buffer.find("\r\n");
            read_buffer.erase(0, pos + 2);
            int throwawayItems = 1;
            clear_array(throwawayItems, read_buffer);
            multi = false;
            queuedUp = 0;
            commandQueue = "";
            response = "+OK\r\n";
            send(client_fd, response.c_str(), response.size(), 0);
            response = "";
            continue;
          }
        }
      }
      
      if(multi){
        std::string execPattern = "*1\r\n$4\r\nexec\r\n";
        if (read_buffer.size() >= execPattern.size()) {
          prefix = read_buffer.substr(0,execPattern.size());
          prefix = lowercase_command(prefix);
          if (prefix.compare(0, execPattern.size(), execPattern) != 0){ // Not a exec command so just queue it
            tempResponse = "+QUEUED\r\n";
            send(client_fd, tempResponse.c_str(), tempResponse.size(), 0);

            commandQueue += extractArray(read_buffer);
            queuedUp +=1;
            continue;
          }
        }
      }
      
      std::string start = read_buffer.substr(0,pos); // Get starting identifier from buffer
      read_buffer.erase(0, pos + 2); 
      if (start[0]== '*'){  // If input is an array

        int items = std::stoi(start.substr(1)); // How many items are in the array
        pos = read_buffer.find("\r\n");
        std::string indicator = read_buffer.substr(0,pos); // What the first item in the array type is
        read_buffer.erase(0, pos + 2);

        if (indicator[0] == '$'){ // First item is bulk String (only supported type at this time)

          pos = read_buffer.find("\r\n");
          std::string bulkString = read_buffer.substr(0,pos);
          bulkString = lowercase_command(bulkString);
          read_buffer.erase(0, pos + 2);
          items -=1;

          if(subMode){ // when in subscribed mode, only take subscribe, unsubscribe, and special ping, give error for the rest
            if (bulkString == "subscribe"){
              response += subscribe_command(items, client_fd, read_buffer, channels, subbed);
            }
            else if (bulkString == "unsubscribe"){
              response += unsubscribe_command(items, client_fd, read_buffer, channels, subbed);
            }
            else if (bulkString == "ping"){
              response += "*2\r\n$4\r\npong\r\n$0\r\n\r\n";
            }
            else{
              response += "-ERR Can't execute '"+bulkString+"' in subscribed mode: only SUBSCRIBE / UNSUBSCRIBE / PING are allowed \r\n";
            }
            send(client_fd, response.c_str(), response.size(), 0);
            clear_array(items, read_buffer);
            response = "";
            continue;
          }

          if (bulkString == "multi"){
            if (multi){
              tempResponse = "-ERR MULTI calls can not be nested\r\n";
              send(client_fd, tempResponse.c_str(), tempResponse.size(), 0);
              clear_array(items, read_buffer);
              continue;
            }
            else{
              multi = true;
              tempResponse = "+OK\r\n";
              send(client_fd, tempResponse.c_str(), tempResponse.size(), 0);
              clear_array(items, read_buffer);
              continue;
            }
          }
          else if (bulkString == "exec"){
            if (!multi){
              response = "-ERR EXEC without MULTI\r\n";
              send(client_fd, response.c_str(), response.size(), 0);
              clear_array(items, read_buffer);
              continue;
            }
            else{
              multi = false;
              read_buffer = commandQueue + read_buffer;
              commandQueue = "";
              response = "*" + std::to_string(queuedUp) + "\r\n";
            }
          }
          else if (bulkString == "discard"){
            if (!multi){
              response = "-ERR DISCARD without MULTI\r\n";
            }
          }
          else if ( bulkString == "ping" ){ // Array gonna be like $5\r\nworld\r\n
            response += ping_command(items, client_fd, read_buffer);
          }
          else if ( bulkString == "echo" ){
            response += echo_command(items, client_fd, read_buffer);
          }
          else if ( bulkString == "command" ){
            response += command_command(items, client_fd, read_buffer);
          }
          else if ( bulkString == "set" ){
            if (true){  // Pass on to replicas before processing
              std::string sMessage = "*" + std::to_string(items+1) + "\r\n$3\r\nSET\r\n";
              sMessage += read_buffer;
              for (int s : slaves){ 
                send(s, sMessage.c_str(), sMessage.length(), 0);
              }
              replOffset += sMessage.size();
            }
           
            response += set_command(items, client_fd, read_buffer, dict);
          }
          else if ( bulkString == "get" ){
            response += get_command(items, client_fd, read_buffer, dict);
          }
          else if (bulkString == "config"){
            response += config_command(items, client_fd, read_buffer, config);
          }
          else if (bulkString == "keys"){
            response += key_command(items, client_fd, read_buffer, dict);
          }
          else if (bulkString == "info"){
            response += info_command(items, client_fd, read_buffer, config);
          }
          else if (bulkString == "type"){
            response += type_command(items, client_fd, read_buffer, dict, sDict);
          }
          else if (bulkString == "xadd"){
            response += xadd_command(items, client_fd, read_buffer, dict, sDict);
          }
          else if (bulkString == "xrange"){
            response += xrange_command(items, client_fd, read_buffer, sDict);
          }
          else if (bulkString == "xread"){
            response += xread_command(items, client_fd, read_buffer, sDict);
          }
          else if (bulkString == "incr"){
            response += incr_command(items, client_fd, read_buffer, dict);
          }
          else if (bulkString == "rpush"){
            response += rpush_command(items, client_fd, read_buffer, lDict);
          }
          else if (bulkString == "lrange"){
            response += lrange_command(items, client_fd, read_buffer, lDict);
          }
          else if (bulkString == "lpush"){
            response += lpush_command(items, client_fd, read_buffer, lDict);
          }
          else if (bulkString == "llen"){
            response += llen_command(items, client_fd, read_buffer, lDict);
          }
          else if (bulkString == "lpop"){
            response += lpop_command(items, client_fd, read_buffer, lDict);
          }
          else if (bulkString == "blpop"){
            response += blpop_command(items, client_fd, read_buffer, lDict);
          }
          else if (bulkString == "subscribe"){
            response += subscribe_command(items, client_fd, read_buffer, channels, subbed);
            subMode = true;
          }
          else if (bulkString == "unsubscribe"){
            response += unsubscribe_command(items, client_fd, read_buffer, channels, subbed);
          }
          else if (bulkString == "publish"){
            response += publish_command(items, client_fd, read_buffer, channels);
          }
          else if (bulkString == "zadd"){
            response += zadd_command(items, client_fd, read_buffer, sets);
          }
          else if (bulkString == "zrank"){
            response += zrank_command(items, client_fd, read_buffer, sets);
          }
          else if (bulkString == "zrange"){
            response += zrange_command(items, client_fd, read_buffer, sets);
          }
          else if (bulkString == "zcard"){
            response += zcard_command(items, client_fd, read_buffer, sets);
          }
          else if (bulkString == "zscore"){
            response += zscore_command(items, client_fd, read_buffer, sets);
          }
          else if (bulkString == "zrem"){
            response += zrem_command(items, client_fd, read_buffer, sets);
          }
          else if (bulkString == "geoadd"){
            response += geoadd_command(items, client_fd, read_buffer, sets);
          }
          else if (bulkString == "geopos"){
            response += geopos_command(items, client_fd, read_buffer, sets);
          }
          else if (bulkString == "geodist"){
            response += geodist_command(items, client_fd, read_buffer, sets);
          }
          else if (bulkString == "geosearch"){
            response += geosearch_command(items, client_fd, read_buffer, sets);
          }
          else if (bulkString == "replconf"){
            std::string next = parsebulkString(items, client_fd, read_buffer);
            next = lowercase_command(next);
            if (next == "getack"){
              std::cout << "getack called from client" << std::endl;
              std::string sMessage = "*3\r\n$8\r\nreplconf\r\n$6\r\ngetack\r\n$1\r\n*\r\n";
              for (int s : slaves){
                send(s, sMessage.c_str(), sMessage.length(), 0);
              }
              std::string response = "+\r\n";
              send(client_fd, response.c_str(), response.size(), 0);
              clear_array(items,read_buffer);
            }
            else if (next == "ack"){
              int offset = std::stoi(parsebulkString(items, client_fd, read_buffer));
              {
                  std::lock_guard<std::mutex> lock(replicaMutex); 
                  replicaOffsets[client_fd] = offset;             
              } 
            }
            else{
              std::string response = "+OK\r\n";
              send(client_fd, response.c_str(), response.size(), 0);
              clear_array(items,read_buffer);
            }
          }
          else if (bulkString == "psync"){
            std::string response = "+FULLRESYNC 8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb 0\r\n";
            send(client_fd, response.c_str(), response.size(), 0);
            clear_array(items,read_buffer);

            std::string emptyRDB = "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2";
            int length = emptyRDB.length() / 2;
            response = "$" + std::to_string(length) + "\r\n";
            std::vector<uint8_t> bytes;
            bytes.reserve(length);
            for (size_t i = 0; i < emptyRDB.length(); i += 2) {
              std::string byteStr = emptyRDB.substr(i, 2);
              uint8_t byte = static_cast<uint8_t>(std::stoul(byteStr, nullptr, 16));
              bytes.push_back(byte);
            }
            response.append(reinterpret_cast<const char*>(bytes.data()), bytes.size());
            send(client_fd, response.c_str(), response.size(), 0);
            clear_array(items,read_buffer);
            {
                std::lock_guard<std::mutex> lock(replicaMutex); 
                replicaOffsets[client_fd] = 0;             
            } 
            slaves.push_back(client_fd);
          }
          else if (bulkString == "wait"){
            int replicaCount = std::stoi(parsebulkString(items, client_fd, read_buffer));
            int timeout = std::stoi(parsebulkString(items, client_fd, read_buffer));
            int connectedReplicas = 0;
            auto start = std::chrono::steady_clock::now();

            std::string sMessage = "*3\r\n$8\r\nREPLCONF\r\n$6\r\nGETACK\r\n$1\r\n*\r\n";
            for (int s : slaves){
              send(s, sMessage.c_str(), sMessage.length(), 0);
            }

            while(true){

              connectedReplicas = 0;

              {
                std::lock_guard<std::mutex> lock(replicaMutex);
                for (int s : slaves) {
                    if (replicaOffsets[s] >= replOffset) connectedReplicas++;
                }
              }

              if (timeout > 0){
                auto now = std::chrono::steady_clock::now();
                auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(now - start).count();
                if (elapsed >= timeout) {
                  break; // timeout
                }
              }
              if (connectedReplicas >= replicaCount || connectedReplicas == slaves.size()){
                break;
              }
              std::this_thread::sleep_for(std::chrono::milliseconds(1));
            }

            std::string response = ":" + std::to_string(connectedReplicas) + "\r\n";
            send(client_fd, response.c_str(), response.size(), 0);
            clear_array(items,read_buffer);
          }
          else{
            response += "-ERR unrecognized command\r\n";
          }

          if (queuedUp == 0){
            send(client_fd, response.c_str(), response.size(), 0);
            clear_array(items, read_buffer);
            response = "";
          }
          else{
            queuedUp -=1;
          }
        }
      }
      else if (start[0]== '$'){ // If input is a bulkString, just scrap it 
          int strLen = std::stoi(start.substr(1,pos));
          std::string bulkString = read_buffer.substr(0, strLen);
          read_buffer.erase(0, strLen + 2);
      }
      else if (start == "PING\r\n"){ // If input is a simple PING command
        std::string response = "+PONG\r\n";
        send(client_fd, response.c_str(), response.size(), 0);
      }
      else{ // If input is not supported
        std::string response = "+ERROR\r\n";
        send(client_fd, response.c_str(), response.size(), 0);
      }
    }
  }
}

int connect_to_master(const std::string& host, int port){

  int client_fd = socket(AF_INET, SOCK_STREAM, 0);
  if (client_fd < 0) {
   std::cerr << "Failed to create client socket\n";
   return 1;
  }

  struct sockaddr_in server_addr;
  server_addr.sin_family = AF_INET;
  server_addr.sin_port = htons(port);

  if (inet_addr(host.c_str()) != INADDR_NONE) {
    server_addr.sin_addr.s_addr = inet_addr(host.c_str());
  } else {
    struct hostent* server = gethostbyname(host.c_str());
    if (!server) {
      std::cerr << "DNS resolution failed for host: " << host << "\n";
      close(client_fd);
      return -1;
    }
    std::memcpy(&server_addr.sin_addr, server->h_addr, server->h_length);
  }

  if (connect(client_fd, (sockaddr*)&server_addr, sizeof(server_addr)) < 0) {
      perror("connect");
      close(client_fd); // cleanup
      return -1;   // signal failure
  }

  return client_fd;

}

std::string simple_rcv(int client_fd){
  std::string read_buffer;
  char buffer[BUFFER_SIZE] = {0};
  ssize_t recieved = recv(client_fd, buffer, sizeof(buffer)-1, 0);
  if (recieved < 0) {
    std::cerr << "error\n";
    return "err";
  } else if (recieved == 0) {
    std::cerr << "master disconnected \n";
    close(client_fd);
    return "err";
  } 
  read_buffer.append(buffer, recieved);
  return read_buffer;
}

HandshakeResult handshake(std::string masterport, Config params){
  int space = masterport.find(" ");
  if (space == std::string::npos) throw std::invalid_argument("Expected '<HOST> <PORT>' format");
  std::string masterhost = masterport.substr(0, space);
  masterport = masterport.substr(space+1);
  int client_fd = connect_to_master(masterhost,std::stoi(masterport)); //Connect to master
  if (client_fd == -1) throw std::invalid_argument("Could not connect to master");

  std::string message = "*1\r\n$4\r\nPING\r\n";
  send(client_fd, message.c_str(), message.size(), 0);

  std::string read_buffer = simple_rcv(client_fd);
  if (read_buffer != "+PONG\r\n"){
    std::cerr << "Handshake failed at ping\n";
    close(client_fd);
    return {-1,""};
  }

  message = "*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$4\r\n";
  message += params.port + "\r\n";
  send(client_fd, message.c_str(), message.size(), 0);

  read_buffer = simple_rcv(client_fd);
  if (read_buffer != "+OK\r\n"){
    std::cerr << "Handshake failed at replconf port\n";
    close(client_fd);
    return {-1,""};
  }

  message = "*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n";
  send(client_fd, message.c_str(), message.size(), 0);

  read_buffer = simple_rcv(client_fd);
  if (read_buffer != "+OK\r\n"){
    std::cerr << "Handshake failed at replconf capa\n";
    close(client_fd);
    return {-1,""};
  }

  message = "*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n";
  send(client_fd, message.c_str(), message.size(), 0);

  read_buffer = simple_rcv(client_fd);
  if (read_buffer.substr(0,11) != "+FULLRESYNC"){
    std::cerr << "Handshake failed at replconf capa\n";
    close(client_fd);
    return {-1,""};
  }
  ssize_t syncPos = read_buffer.find("\r\n");
  read_buffer.erase(0, syncPos + 2);
  std::cout << "parsing sync: " << read_buffer << std::endl;
  if (read_buffer == ""){
    read_buffer = simple_rcv(client_fd); // Recieve RDB File
  }
  ssize_t pos = read_buffer.find("\r\n");
  int rbdLen = std::stoi(read_buffer.substr(1,pos));
  read_buffer.erase(0, pos + 2);
  std::string rbdFile = read_buffer.substr(0, rbdLen);
  read_buffer.erase(0, rbdLen);
  std::cout << "leftover bufffer: " << read_buffer << std::endl;
  return {client_fd, read_buffer};
}

int main(int argc, char **argv) {
  Config params;
  std::vector<std::thread> threads;
  RedisDict dict;
  std::map<std::string, Stream> sDict; 
  std::map<std::string, std::vector<std::string>> lDict;
  std::map<std::string, std::set<int>> channels;
  std::map<std::string, SkipList> sets;
  std::string masterport;

  for (int i = 1; i < argc; i++){
    std::string arg = argv[i];
    if( arg == "--dir" && i+1 < argc){
      params.dir = argv[++i];
    }
    else if(arg == "--dbfilename" && i+1 < argc){
      params.dbfilename = argv[++i];
    }
    else if(arg == "--port" && i+1 < argc){
      params.port = argv[++i];
    }
    else if(arg == "--replicaof" && i+1 < argc){
      params.replica = "slave";
      
      masterport = argv[++i];
    }
  }

  std::string filepath = params.dir + "/" + params.dbfilename;
  if (params.dir !="" || params.dbfilename != ""){
    std::cout << filepath << std::endl;
  }
  parse_rdbFile(dict, filepath);

  if (params.replica == "slave"){
    HandshakeResult hr = handshake(masterport, params);
    if( hr.fd == -1){
      return 1;
    };
    std::cout << "Connected to Master \n";
    threads.emplace_back(std::thread(handle_master, hr.fd, params, filepath, std::ref(dict), hr.leftover));
    threads.back().detach();
  }

  // Flush after every std::cout / std::cerr
  std::cout << std::unitbuf;
  std::cerr << std::unitbuf;
  
  int server_fd = socket(AF_INET, SOCK_STREAM, 0);
  if (server_fd < 0) {
   std::cerr << "Failed to create server socket\n";
   return 1;
  }

  // Since the tester restarts your program quite often, setting SO_REUSEADDR
  // ensures that we don't run into 'Address already in use' errors
  int reuse = 1;
  if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse)) < 0) {
    std::cerr << "setsockopt failed\n";
    return 1;
  }

  struct sockaddr_in server_addr;
  server_addr.sin_family = AF_INET;
  server_addr.sin_addr.s_addr = INADDR_ANY;
  server_addr.sin_port = htons(std::stoi(params.port));
  
  if (bind(server_fd, (struct sockaddr *) &server_addr, sizeof(server_addr)) != 0) {
    std::cerr << "Failed to bind to port "<< std::stoi(params.port) <<"\n";
    return 1;
  }
  
  int connection_backlog = 5;
  if (listen(server_fd, connection_backlog) != 0) {
    std::cerr << "listen failed\n";
    return 1;
  }
  
  struct sockaddr_in client_addr;
  int client_addr_len = sizeof(client_addr);
  std::cout << "Waiting for a client to connect...\n";

  // You can use print statements as follows for debugging, they'll be visible when running tests.
  std::cout << "Logs from your program will appear here!\n";

  while(true){
    int client_fd = accept(server_fd, (struct sockaddr *) &client_addr, (socklen_t *) &client_addr_len);
    if (client_fd < 0) {
      perror("accept");
      continue;
    }
    std::cout << "Client connected\n";
    threads.emplace_back(std::thread(handle_client, client_fd, params, filepath, 
      std::ref(dict), std::ref(sDict), std::ref(lDict), std::ref(channels), std::ref(sets)));
    threads.back().detach();
  }

  close(server_fd);

  return 0;
}