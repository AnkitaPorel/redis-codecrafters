#include "geo.h"
#include "bulkString.h"
#include <cstdint>
#include <iostream>
#include <cmath>
#include <utility>
#include <math.h>

constexpr double MIN_LATITUDE = -85.05112878;
constexpr double MAX_LATITUDE = 85.05112878;
constexpr double MIN_LONGITUDE = -180.0;
constexpr double MAX_LONGITUDE = 180.0;

const double LATITUDE_RANGE = MAX_LATITUDE - MIN_LATITUDE;
const double LONGITUDE_RANGE = MAX_LONGITUDE - MIN_LONGITUDE;

const static double EarthRadiusKm = 6372797.560856;
const double PI = 3.14159265358979323846;

uint64_t spread_int32_to_int64(uint32_t v){
    // Ensure only lower 32 bits are non-zero.
    uint64_t result = v;

    // Bitwise operations to spread 32 bits into 64 bits with zeros in-between
    result = (result | (result << 16)) & 0x0000FFFF0000FFFF;
    result = (result | (result << 8))  & 0x00FF00FF00FF00FF;
    result = (result | (result << 4))  & 0x0F0F0F0F0F0F0F0F;
    result = (result | (result << 2))  & 0x3333333333333333;
    result = (result | (result << 1))  & 0x5555555555555555;

    return static_cast<uint64_t>(result);
}

uint32_t compact_int64_to_int32(uint64_t v){
    // Keep only the bits in even positions
    v = v & 0x5555555555555555;

    v = (v | (v >> 1)) & 0x3333333333333333;
    v = (v | (v >> 2)) & 0x0F0F0F0F0F0F0F0F;
    v = (v | (v >> 4)) & 0x00FF00FF00FF00FF;
    v = (v | (v >> 8)) & 0x0000FFFF0000FFFF;
    v = (v | (v >> 16)) & 0x00000000FFFFFFFF;
    
    return v;
}

std::pair<double, double> decodeCoords(uint64_t score){
    uint64_t longitude = score >> 1;
    uint64_t lattitude = score;
    uint32_t compactedLong = compact_int64_to_int32(longitude);
    uint32_t compactedLat = compact_int64_to_int32(lattitude);
    double grid_latitude_min = MIN_LATITUDE + LATITUDE_RANGE * (compactedLat / pow(2, 26));
    double grid_latitude_max = MIN_LATITUDE + LATITUDE_RANGE * ((compactedLat + 1) / pow(2, 26));
    double grid_longitude_min = MIN_LONGITUDE + LONGITUDE_RANGE * (compactedLong / pow(2, 26));
    double grid_longitude_max = MIN_LONGITUDE + LONGITUDE_RANGE * ((compactedLong + 1) / pow(2, 26));
    double lat = (grid_latitude_min + grid_latitude_max) / 2.0;
    double lon = (grid_longitude_min + grid_longitude_max) / 2.0;
    return std::make_pair(lat, lon);
}

uint64_t encodeCoords(double lat, double lon){
    int x = 1 << 26;
    double normalized_latitude = x * (lat - MIN_LATITUDE) / LATITUDE_RANGE;
    double normalized_longitude = x * (lon - MIN_LONGITUDE) / LONGITUDE_RANGE;
    uint32_t normalLatitude = static_cast<uint32_t>(normalized_latitude);
    uint32_t normalLongitude = static_cast<uint32_t>(normalized_longitude);
    uint64_t interleavedLat = spread_int32_to_int64(normalLatitude);
    uint64_t interleavedLong = spread_int32_to_int64(normalLongitude);
    uint64_t shiftedLong = interleavedLong << 1;
    uint64_t score = interleavedLat | shiftedLong;
    return score;
}

double haversine(uint64_t score1, uint64_t score2){
    auto coords1 = decodeCoords(score1);
    double lat1 = coords1.first;
    double lon1 = coords1.second;
    auto coords2 = decodeCoords(score2);
    double lat2 = coords2.first;
    double lon2 = coords2.second;

    double latRad1 = (PI*lat1) / 180;
	double latRad2 = (PI*lat2) / 180;
	double lonRad1 = (PI*lon1) / 180;
	double lonRad2 = (PI*lon2) / 180;

    double diffLa = latRad2 - latRad1;
	double diffLo = lonRad2 - lonRad1;

    double computation = asin(sqrt(sin(diffLa / 2) * sin(diffLa / 2) + cos(latRad1) * cos(latRad2) * sin(diffLo / 2) * sin(diffLo / 2)));
    double distance  = 2 * EarthRadiusKm * computation;
    return distance;
}

std::string geoadd_command(int& items, int client_fd, std::string& read_buffer, 
    std::map<std::string, SkipList>& sets){
        if (items == 4){
            std::string response = "";
            std::string listName = parsebulkString(items, client_fd, read_buffer);
            double longitude = std::stod(parsebulkString(items, client_fd, read_buffer));
            double latitude = std::stod(parsebulkString(items, client_fd, read_buffer));
            std::string name = parsebulkString(items, client_fd, read_buffer);

            if(longitude > MAX_LONGITUDE || longitude < MIN_LONGITUDE ||
            latitude > MAX_LATITUDE || latitude < MIN_LATITUDE)
            {
                std::string response = "-ERR invalid longitude,latitude pair " + std::to_string(longitude) + "," +std::to_string(latitude) +"\r\n";
                return response;
            }

            uint64_t score = encodeCoords(latitude, longitude);

            SkipList& sl = sets[listName];

            int tempItems = 3;
            std::string message = "$" + std::to_string(listName.size())+ "\r\n" + listName + "\r\n";
            message += "$" + std::to_string(std::to_string(score).size())+ "\r\n" + std::to_string(score) + "\r\n";
            message += "$" + std::to_string(name.size())+ "\r\n" + name + "\r\n";
            response = zadd_command(tempItems,client_fd,message, sets);
            return response;
        }
        else{
            std::string response = "-ERR wrong number of arguments for geoadd command\r\n";
            return response;
        }
    }

std::string geopos_command(int& items, int client_fd, std::string& read_buffer, 
    std::map<std::string, SkipList>& sets){
        if (items >= 2){

            std::string listName = parsebulkString(items, client_fd, read_buffer);
            std::string response = "";
            int itemsCopy = items;
            response += "*"+std::to_string(itemsCopy)+"\r\n";

            auto it = sets.find(listName);
            if(it == sets.end()){
                std::cout << "not found" << std::endl;
                for(int i = 0; i < items; i++){
                    response += "*-1\r\n";
                }
                return response;
            }

            SkipList& sl = it->second;

            for(int i = 0; i < itemsCopy; i++){
                uint64_t score = 0;
                std::string name = parsebulkString(items, client_fd, read_buffer);
                Node* prev = sl.head;
                for (Node* n = sl.head->forward[0]; n != nullptr; n = n->forward[0]) {
                    if (n->key == name) {
                        score = n->score;
                        break;
                    }
                }
                if (score == 0){
                    response += "*-1\r\n";
                    continue;
                }
                auto coords = decodeCoords(score);
                double lat = coords.first;
                double lon = coords.second;
                response += "*2\r\n";
                response += "$"+ std::to_string(std::to_string(lon).size()) + "\r\n" + std::to_string(lon) + "\r\n";
                response += "$"+ std::to_string(std::to_string(lat).size()) + "\r\n" + std::to_string(lat) + "\r\n";
                std::cout << response << std::endl;
            }
            return response;

        }
        else{
            std::string response = "-ERR wrong number of arguments for geopos command\r\n";
            return response;
        }
    }

std::string geodist_command(int& items, int client_fd, std::string& read_buffer, 
    std::map<std::string, SkipList>& sets){
        if(items == 3){
            std::string response = "";
            std::string listName = parsebulkString(items, client_fd, read_buffer);
            auto it = sets.find(listName);
            if (it == sets.end()){
                response = "-ERR could not find set \r\n";
                return response;
            }
            SkipList& sl = it->second;
            std::string loc1 = parsebulkString(items, client_fd, read_buffer);
            std::string loc2 = parsebulkString(items, client_fd, read_buffer);
            Node* prev = sl.head;
            uint64_t score1 = -1;
            for (Node* n = sl.head->forward[0]; n != nullptr; n = n->forward[0]) {
                if (n->key == loc1) {
                    score1 = n->score;
                    break;
                }
            }
            prev = sl.head;
            uint64_t score2 = -1;
            for (Node* n = sl.head->forward[0]; n != nullptr; n = n->forward[0]) {
                if (n->key == loc2) {
                    score2 = n->score;
                    break;
                }
            }

            double distance  = haversine(score1, score2);

            std::string strDistance = std::to_string(distance);
            response += "$" + std::to_string(strDistance.size()) + "\r\n";
            response += strDistance + "\r\n";
            return response;
        }
        else{
            std::string response = "-ERR wrong number of arguments for geodist command\r\n";
            return response;
        }
    }

std::string geosearch_command(int& items, int client_fd, std::string& read_buffer, 
    std::map<std::string, SkipList>& sets){
        if(items == 7){
            std::string response = "";
            std::string listName = parsebulkString(items, client_fd, read_buffer);
            auto it = sets.find(listName);
            if (it == sets.end()){
                response = "-ERR could not find set \r\n";
                return response;
            }
            SkipList& sl = it->second;
            std::string fromlonlat = parsebulkString(items, client_fd, read_buffer);
            double lon = std::stod(parsebulkString(items, client_fd, read_buffer));
            double lat = std::stod(parsebulkString(items, client_fd, read_buffer));
            uint64_t newscore = encodeCoords(lat,lon);
            std::string byradius = parsebulkString(items, client_fd, read_buffer);
            double distance = std::stod(parsebulkString(items, client_fd, read_buffer));
            std::string units = parsebulkString(items, client_fd, read_buffer);
            if(units == "km"){
                distance *= 1000;
            }
            else if (units == "mi"){
                distance *= 1.60934;
            }

            std::vector<std::string > inRange;
            for (Node* n = sl.head->forward[0]; n != nullptr; n = n->forward[0]) {
                double dist = haversine(newscore, n->score);
                if (distance >= dist){
                    inRange.push_back(n->key);
                }
            }

            response += "*" + std::to_string(inRange.size()) + "\r\n";
            for (std::string s : inRange){
                response += "$" + std::to_string(s.size()) + "\r\n";
                response += s + "\r\n";
            }

            return response;

        }
        else{
            std::string response = "-ERR wrong number of arguments for geosearch command\r\n";
            return response;
        }
    }