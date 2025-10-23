#ifndef SET_H
#define SET_H

#include <string>
#include <set>
#include <vector>
#include <chrono>
#include <map>

#include <cstdint>

struct Node {
    double score;
    std::string key;
    std::vector<Node*> forward; // forward[i] points to next node at level i

    Node(double s, std::string k, int level) : score(s), key(k), forward(level, nullptr) {}
};

struct SkipList {
    int maxLevel;
    float p; // probability for promotion (e.g. 0.5)
    int currentLevel;
    Node* head;
    int size;

    SkipList(int maxL, float prob)
        : maxLevel(maxL), p(prob), currentLevel(0),
          head(new Node(-1, "", maxL + 1)), size(0) {} // sentinel head
        
    SkipList()
        : maxLevel(5), p(0.5),  currentLevel(0),
          head(new Node(-1, "", 6)), size(0) {}
};

std::string zadd_command(int& items, int client_fd, std::string& read_buffer, std::map<std::string, SkipList>& sets);

std::string zrank_command(int& items, int client_fd, std::string& read_buffer, std::map<std::string, SkipList>& sets);

std::string zrange_command(int& items, int client_fd, std::string& read_buffer, std::map<std::string, SkipList>& sets);

std::string zcard_command(int& items, int client_fd, std::string& read_buffer, std::map<std::string, SkipList>& sets);

std::string zscore_command(int& items, int client_fd, std::string& read_buffer, std::map<std::string, SkipList>& sets);

std::string zrem_command(int& items, int client_fd, std::string& read_buffer, std::map<std::string, SkipList>& sets);

#endif