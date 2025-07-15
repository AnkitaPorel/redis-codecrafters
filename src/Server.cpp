#include <iostream>
#include <cstdlib>
#include <string>
#include <cstring>
#include <vector>
#include <unistd.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <sys/select.h>
#include "redis_parser.hpp"

void execute_redis_command(int client_fd, const std::vector<std::string>& parsed_command) {
    if (parsed_command.empty()) {
        return;
    }

    std::string command = parsed_command[0]; // Command is already uppercase from parser

    if (command == "PING") {
        // Respond with simple string "+PONG\r\n"
        std::string response = "+PONG\r\n";
        send(client_fd, response.c_str(), response.length(), 0);
    } else if (command == "ECHO" && parsed_command.size() == 2) {
        // Respond with the argument as a RESP bulk string
        std::string arg = parsed_command[1];
        std::string response = "$" + std::to_string(arg.length()) + "\r\n" + arg + "\r\n";
        send(client_fd, response.c_str(), response.length(), 0);
    } else {
        // Handle unknown commands or invalid ECHO
        std::string response = "-ERR unknown command or wrong number of arguments\r\n";
        send(client_fd, response.c_str(), response.length(), 0);
    }
}

int main(int argc, char **argv) {
    // Flush after every std::cout / std::cerr
    std::cout << std::unitbuf;
    std::cerr << std::unitbuf;

    int server_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd < 0) {
        std::cerr << "Failed to create server socket\n";
        return 1;
    }

    // Set SO_REUSEADDR to avoid 'Address already in use' errors
    int reuse = 1;
    if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse)) < 0) {
        std::cerr << "setsockopt failed\n";
        return 1;
    }

    struct sockaddr_in server_addr;
    server_addr.sin_family = AF_INET;
    server_addr.sin_addr.s_addr = INADDR_ANY;
    server_addr.sin_port = htons(6379);

    if (bind(server_fd, (struct sockaddr *) &server_addr, sizeof(server_addr)) != 0) {
        std::cerr << "Failed to bind to port 6379\n";
        return 1;
    }

    int connection_backlog = 5;
    if (listen(server_fd, connection_backlog) != 0) {
        std::cerr << "listen failed\n";
        return 1;
    }

    std::cout << "Waiting for a client to connect...\n";
    std::cout << "Logs from your program will appear here!\n";

    fd_set read_fds;
    std::vector<int> client_fds;

    while (true) {
        FD_ZERO(&read_fds);
        FD_SET(server_fd, &read_fds);
        int max_fd = server_fd;

        // Add all client file descriptors to the set
        for (int fd : client_fds) {
            FD_SET(fd, &read_fds);
            if (fd > max_fd) {
                max_fd = fd;
            }
        }

        // Use select() to monitor sockets
        if (select(max_fd + 1, &read_fds, nullptr, nullptr, nullptr) < 0) {
            std::cerr << "select failed\n";
            close(server_fd);
            return 1;
        }

        // Check for new connections
        if (FD_ISSET(server_fd, &read_fds)) {
            struct sockaddr_in client_addr;
            socklen_t client_addr_len = sizeof(client_addr);
            int client_fd = accept(server_fd, (struct sockaddr *) &client_addr, &client_addr_len);
            if (client_fd < 0) {
                std::cerr << "accept failed\n";
                continue;
            }
            std::cout << "Client connected (fd: " << client_fd << ")\n";
            client_fds.push_back(client_fd);
        }

        // Check for data from clients
        for (auto it = client_fds.begin(); it != client_fds.end();) {
            int client_fd = *it;
            if (FD_ISSET(client_fd, &read_fds)) {
                char buffer[4096];
                int bytes_received = recv(client_fd, buffer, sizeof(buffer) - 1, 0);

                if (bytes_received <= 0) {
                    // Client disconnected or error occurred
                    std::cout << "Client disconnected (fd: " << client_fd << ")\n";
                    close(client_fd);
                    it = client_fds.erase(it);
                    continue;
                }

                buffer[bytes_received] = '\0';
                std::cout << "Received: " << buffer << std::endl;

                std::vector<std::string> parsed_command;
                try {
                    parse_redis_command(buffer, parsed_command);
                    execute_redis_command(client_fd, parsed_command);
                } catch (const std::exception& e) {
                    std::string response = "-ERR protocol error: " + std::string(e.what()) + "\r\n";
                    send(client_fd, response.c_str(), response.length(), 0);
                }
            }
            ++it;
        }
    }

    // Cleanup
    for (int client_fd : client_fds) {
        close(client_fd);
    }
    close(server_fd);

    return 0;
}