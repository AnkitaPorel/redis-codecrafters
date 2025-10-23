#include "stream.h"
#include "bulkString.h"
#include "lowerCMD.h"
#include <iostream>
#include <chrono>
#include "clear.h"
#include <sys/types.h>
#include <thread>
#include <sys/socket.h>
#include <unistd.h>
#include <cstdint>

std::string xadd_command(int& items, int client_fd, std::string& read_buffer, 
    std::map<std::string, std::tuple<std::string,std::chrono::system_clock::time_point>>& dict,
    std::map<std::string, Stream>& sDict){
    
        if (items >= 4 && (items % 2 == 0)){
            std::string key = parsebulkString(items, client_fd, read_buffer);
            //tries to find val dictionary
            auto tuple = dict.find(key);
            // val found... error
            if (tuple != dict.end()){
                std::string response = "-ERR key already in use\r\n";
                return response;
            }   

            long long lastMili = 0;
            long long lastSequence = 0;
            // val found inside stream dictionary
            if (sDict.find(key) != sDict.end()){
                std::string lastKey = sDict[key].lastID;
                size_t div = lastKey.find("-");
                lastMili = std::stoll(lastKey.substr(0,div));
                lastSequence = std::stoll(lastKey.substr(div+1)); // Save the last key for comparison purposes
            }

            std::string ID = parsebulkString(items, client_fd, read_buffer);
            if (ID == "*"){
                auto now = std::chrono::system_clock::now();
                auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                            now.time_since_epoch()
                        ).count();
                ID = std::to_string(ms)+ "-0";
            }
            else{
                int div = ID.find("-");
                long long miliseconds = std::stoll(ID.substr(0,div));
                std::string sequence = ID.substr(div+1);
                if (sequence == "*"){
                    if (miliseconds == 0){
                        sequence = "1";
                    }
                    else if (miliseconds < lastMili){
                        std::string response = "-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n";
                        return response;
                    }
                    else if (lastMili == miliseconds){
                        sequence = std::to_string(lastSequence + 1);
                    }
                    else{
                        sequence = "0";
                    }
                }
                else{
                    int isequence = std::stoll(sequence);
                    if( miliseconds == 0 && isequence == 0){
                        std::string response = "-ERR The ID specified in XADD must be greater than 0-0\r\n";
                        return response;
                    }
                    if (miliseconds < lastMili){
                        std::string response = "-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n";
                        return response;
                    }
                    if ((miliseconds == lastMili) && (isequence < lastSequence)){
                        std::string response = "-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n";
                        return response;
                    }
                    if ((miliseconds == lastMili) && (isequence == lastSequence)){
                        std::string response = "-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n";
                        return response;
                    }
                    

                }
                ID =  std::to_string(miliseconds) + "-" + sequence;
            }
            while (items > 0){
                std::string field = parsebulkString(items, client_fd, read_buffer);
                std::string val = parsebulkString(items, client_fd, read_buffer);
                sDict[key].entries[ID].push_back({field,val});
            }
            sDict[key].lastID = ID;
            int idLen = ID.length();
            std::string response = "$" + std::to_string(idLen) + "\r\n";
            response += ID + "\r\n";
            return response;
        }
        else{
            std::cout << "given " << std::to_string(items) << " items" << std::endl;
            std::string response = "-ERR wrong number of arguments for xadd command\r\n";
            return response;
        }

}

std::string xrange_command(int& items, int client_fd, std::string& read_buffer, 
    std::map<std::string, Stream>& sDict){
        std::string response = "";
        if (items == 3){
            std::string key = parsebulkString(items, client_fd, read_buffer);
            std::string start = parsebulkString(items, client_fd, read_buffer);
            uint64_t startmili = 0;
            uint64_t startSeq = 0;
            int div = 0;
            if( start != "-"){
                div = start.find("-");
                if (div == -1){
                    startmili = std::stoll(start);
                    startSeq = 0;
                }
                else{
                    startmili = std::stoll(start.substr(0,div));
                    startSeq = std::stoll(start.substr(div+1));
                }
            }
            std::string end = parsebulkString(items, client_fd, read_buffer);
            uint64_t endmili = 0;
            uint64_t endSeq = 0;
            if( end == "+"){
                endmili = UINT64_MAX;
                endSeq = UINT64_MAX;
            }
            else{
                div = end.find("-");
                if (div == -1){
                    endmili = std::stoll(start);
                    endSeq = UINT64_MAX;
                }
                else{
                    endmili = std::stoll(end.substr(0,div));
                    endSeq = std::stoll(end.substr(div+1));
                }
            }

            int count = 0;
            auto it = sDict.find(key);
            if (it != sDict.end()) {
                for (const auto& [id, fields] : it->second.entries) {
                    int div = id.find("-");
                    int miliID = std::stoi(id.substr(0,div));
                    int seqID = std::stoi(id.substr(div+1));
                    if ((miliID > startmili && miliID < endmili) || ((miliID == startmili || miliID == endmili) && ((seqID >= startSeq) && (seqID <= endSeq))) ){
                        count+=1;
                        response += "*2\r\n";
                        response += "$" + std::to_string(id.length()) + "\r\n" + id + "\r\n";
                        response += "*" + std::to_string(fields.size()*2) + "\r\n";
                        for (const auto& [field, value] : fields) {
                            response += "$" + std::to_string(field.length()) + "\r\n" + field + "\r\n";
                            response += "$" + std::to_string(value.length()) + "\r\n" + value + "\r\n";
                        }
                    }
                }
                response = "*" + std::to_string(count) + "\r\n" + response;
                return response;
            }
            else{
                std::string response = "-ERR invalid keyname for xrange command\r\n";
                return response;
            }
        }
        else if (items == 1){
            std::string key = parsebulkString(items, client_fd, read_buffer);
            int count = 0;
            auto it = sDict.find(key);
            // if key found in stream dict
            if (it != sDict.end()) {
                for (const auto& [id, fields] : it->second.entries) {
                    count+=1;
                    response += "*2\r\n";
                    response += "$" + std::to_string(id.length()) + "\r\n" + id + "\r\n";
                    response += "*" + std::to_string(fields.size()) + "\r\n";
                    for (const auto& [field, value] : fields) {
                        response += "$" + std::to_string(field.length()) + "\r\n" + field + "\r\n";
                        response += "$" + std::to_string(value.length()) + "\r\n" + value + "\r\n";
                    }
                }
                response = "*" + std::to_string(count) + "\r\n" + response;
                return response;
            }
            else{ // if key not found in stream dict
                std::string response = "-ERR invalid keyname for xrange command\r\n";
                return response;
            }
        }
        else {
            std::string response = "-ERR wrong number of arguments for xrange command\r\n";
            return response;
        }
}

std::string xread_command(int& items, int client_fd, std::string& read_buffer, 
    std::map<std::string, Stream>& sDict){
        if (items >= 3 && ( (items % 2) == 1)){
            std::string firstWord = parsebulkString(items, client_fd, read_buffer);
            firstWord = lowercase_command(firstWord);
            bool infiniteTime = false;
            uint64_t waitTime = 0;
            if (firstWord == "block"){
                std::string timeGiven = parsebulkString(items, client_fd, read_buffer);
                uint64_t timeTemp = std::stoll(timeGiven);
                
                if (timeTemp == 0){
                    infiniteTime = true;
                }
                else{
                    waitTime = timeTemp;
                }
                parsebulkString(items,client_fd, read_buffer);
            }
            else if ( firstWord != "streams"){
                std::string response = "-ERR wrong arguments for xread command\r\n";
                return response;
            }

            std::vector<std::string> streams;
            std::vector<std::string> ids;
            int givenStreams = items / 2;
            for (int i = 0; i < givenStreams; i++){
                streams.push_back(parsebulkString(items, client_fd, read_buffer));
            }
            for (int i = 0; i < givenStreams; i++){
                std::string givenID = parsebulkString(items, client_fd, read_buffer);
                if (givenID == "$"){
                    givenID = sDict[streams[i]].lastID;
                }
                ids.push_back(givenID);
            }
            
            auto end = std::chrono::steady_clock::now() + std::chrono::milliseconds(waitTime); 

            std::string response = ""; 
            do{
                for (int i = 0; i < givenStreams; i++){
                    std::string tempResponse = "";
                    tempResponse += "*2\r\n";
                    tempResponse += "$" + std::to_string(streams[i].length()) + "\r\n" + streams[i] + "\r\n";

                    std::string entryID = ids[i];
                    int div = entryID.find("-");
                    long long startmili = std::stoi(entryID.substr(0,div));
                    long long startSeq = std::stoi(entryID.substr(div+1));

                    int count = 0;
                    auto it = sDict.find(streams[i]);
                    std::string innerArray = "";
                    if (it != sDict.end()) {
                        for (const auto& [id, fields] : it->second.entries) {
                            div = id.find("-");
                            long long miliID = std::stoi(id.substr(0,div));
                            long long seqID = std::stoi(id.substr(div+1));
                            if ( (miliID > startmili) || ( (miliID == startmili) && (seqID > startSeq) ) ){
                                count+=1;
                                innerArray += "*2\r\n";
                                innerArray += "$" + std::to_string(id.length()) + "\r\n" + id + "\r\n";
                                innerArray += "*" + std::to_string(fields.size()*2) + "\r\n";
                                for (const auto& [field, value] : fields) {
                                    innerArray += "$" + std::to_string(field.length()) + "\r\n" + field + "\r\n";
                                    innerArray += "$" + std::to_string(value.length()) + "\r\n" + value + "\r\n";
                                }
                            }
                        }
                    }
                    tempResponse += "*" + std::to_string(count) + "\r\n" + innerArray;
                    if(innerArray != ""){
                        response += tempResponse;
                    }
                    std::this_thread::sleep_for(std::chrono::milliseconds(5));
            }
            } while( response == "" && (std::chrono::steady_clock::now() < end || infiniteTime));
            if (response == ""){
                response = "*-1\r\n";
            }
            else{
                response = "*" + std::to_string(givenStreams) + "\r\n" + response;
            }
            return response;
        }
        else{
            std::string response = "-ERR ewrong number of arguments for xread command\r\n";
            return response;
        }
}