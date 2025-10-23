#ifndef CONFIG_H
#define CONFIG_H

#include <string>

struct Config {
    std::string dir;
    std::string dbfilename;
    std::string port = "6379";
    std::string replica = "master";
};

std::string config_command(int& items, int client_fd, std::string& read_buffer, Config config);

#endif