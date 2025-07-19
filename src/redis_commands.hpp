#ifndef REDIS_COMMANDS_HPP
#define REDIS_COMMANDS_HPP

#include <string>

struct ServerConfig {
    std::string dir;
    std::string dbfilename;

    ServerConfig() : dir("/tmp/redis-files"), dbfilename("dump.rdb") {}
};

#endif