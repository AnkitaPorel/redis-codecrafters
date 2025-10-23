#ifndef PARSERDB_H
#define PARSERDB_H

#include <string>
#include <chrono>
#include <map>
#include <fstream>

int parse_rdbFile( std::map<std::string, std::tuple<std::string,std::chrono::system_clock::time_point>>& dict, std::string filepath);

#endif