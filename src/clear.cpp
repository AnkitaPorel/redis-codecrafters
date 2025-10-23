#include "clear.h"
#include <iostream>
#include <sys/types.h>
#include <unistd.h>

void clear_array(int& items, std::string& read_buffer){
  ssize_t pos;
  for (int i = 0; i < items; i++){
    pos = read_buffer.find("\r\n");
    if (pos == std::string::npos) return;
    std::string strLen = (read_buffer.substr(0,pos));
    read_buffer.erase(0,pos+2);
    if (strLen[0] == '$'){
      int stringLen = std::stoi(strLen.substr(1));
      if (read_buffer.size() < (size_t)(stringLen + 2)) return;
      read_buffer.erase(0,stringLen+2);
    }
  }
}