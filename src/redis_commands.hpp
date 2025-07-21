#ifndef REDIS_COMMANDS_HPP
#define REDIS_COMMANDS_HPP

#include <string>
#include <utility>
#include <iostream>
#include <cstdlib>
#include <string>
#include <cstring>
#include <vector>
#include <map>
#include <set>
#include <unistd.h>

#include "rdb_parser.hpp"
#include "redis_parser.hpp"

struct ServerConfig {
    std::string dir;
    std::string dbfilename;

    ServerConfig() : dir("/tmp/redis-files"), dbfilename("dump.rdb") {}
};

#endif