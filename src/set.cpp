#include "set.h"
#include "bulkString.h"

#include <iostream>
#include <iomanip>
#include <sstream>

static int randomLevel(float p, int maxLevel) {
    int level = 0;
    while (((float) rand() / RAND_MAX) < p && level < maxLevel) {
        level++;
    }
    return level;
}

std::string zadd_command(int& items, int client_fd, std::string& read_buffer, std::map<std::string, SkipList>& sets){
        if (items == 3){

            std::string listName = parsebulkString(items, client_fd, read_buffer);
            std::string strScore = parsebulkString(items, client_fd, read_buffer);\
            double score = std::stod(strScore);
            std::string key = parsebulkString(items, client_fd, read_buffer);
            std::string response = ":1\r\n";

            SkipList& sl = sets[listName];

            std::vector<Node*> update(sl.maxLevel + 1, nullptr);
            Node* x = sl.head;

            // Now scan level 0 from head to find existing node with the same key
            Node* prev = sl.head;
            Node* nodeToRemove = nullptr;
            for (Node* n = sl.head->forward[0]; n != nullptr; n = n->forward[0]) {
                if (n->key == key) {
                    nodeToRemove = n;
                    break;
                }
            }
            if (nodeToRemove) {
                std::vector<Node*> update(sl.maxLevel + 1, nullptr);
                Node* curr = sl.head;
                for (int i = sl.currentLevel; i >= 0; i--) {
                    Node* node = curr;
                    while (node->forward[i] != nullptr && node->forward[i]->key != key) {
                        node = node->forward[i];
                    }
                    update[i] = node;
                }
                Node* target = update[0]->forward[0];
                for (int i = 0; i <= sl.currentLevel; i++) {
                    if (update[i]->forward[i] != target) break;
                    update[i]->forward[i] = target->forward[i];
                }
                delete target;
                response = ":0\r\n";
            }

            // Step 1: Search top-down to find insertion positions
            for (int i = sl.currentLevel; i >= 0; i--) {
                while (x->forward[i] != nullptr && 
                    (x->forward[i]->score < score || (x->forward[i]->score == score && x->forward[i]->key < key))) 
                {
                    x = x->forward[i];
                }
                update[i] = x; // remember where we dropped down
            }

            // 2 : assign a random level, make new ones as necessary
            int lvl = randomLevel(sl.p, sl.maxLevel);
            if (lvl > sl.currentLevel) {
                for (int i = sl.currentLevel + 1; i <= lvl; i++) {
                    update[i] = sl.head;
                }
                sl.currentLevel = lvl;
            }

            // 3 : Make new node and fill in 
            Node* newNode = new Node(score, key, lvl + 1);
            for (int i = 0; i <= lvl; i++) {
                newNode->forward[i] = update[i]->forward[i];
                update[i]->forward[i] = newNode;
            }

            if (response != ":0\r\n"){
                sl.size +=1;
            }

            return response;
        }
        else{
            std::string response = "-ERR wrong number of arguments for zadd command\r\n";
            return response;
        }
    }

std::string zrank_command(int& items, int client_fd, std::string& read_buffer, std::map<std::string, SkipList>& sets){
        if (items == 2){
            std::string listName = parsebulkString(items, client_fd, read_buffer);
            std::string key = parsebulkString(items, client_fd, read_buffer);

            std::string response = "";
            int rank = -1;

            SkipList& sl = sets[listName];

            Node* prev = sl.head;
            for (Node* n = sl.head->forward[0]; n != nullptr; n = n->forward[0]) {
                rank +=1;
                if (n->key == key) {
                    break;
                }
                if (n->forward[0] == nullptr){
                    rank = -1;
                }
            }

            if (rank == -1){
                response = "$-1\r\n";
            }
            else{
                response += ":" + std::to_string(rank) + "\r\n";
            }

            return response;
        }
        else{
            std::string response = "-ERR wrong number of arguments for zrank command\r\n";
            return response;
        }
    }

std::string zrange_command(int& items, int client_fd, std::string& read_buffer, std::map<std::string, SkipList>& sets){
        if (items == 3){

            std::string response = "";
            std::string listName = parsebulkString(items, client_fd, read_buffer);
            int startIndex = std::stoi(parsebulkString(items, client_fd, read_buffer));
            int endIndex = std::stoi(parsebulkString(items, client_fd, read_buffer));

            auto it = sets.find(listName);

            if (it == sets.end()){ 
                response = "*0\r\n";
                return response;
            }

            SkipList& sl = it->second;
            int len = sl.size;

            if (startIndex < 0){
                startIndex = len + startIndex;
                startIndex = (startIndex > 0) ? startIndex : 0;
            }
            if (endIndex < 0){
                endIndex = len + endIndex;
                endIndex = (endIndex > 0) ? endIndex : 0;
            }

            if ((startIndex > endIndex) || (startIndex >= len)){ 
                response = "*0\r\n";
                return response;
            }

            endIndex = (endIndex < len-1) ? endIndex : len-1;
            
            response = "*" + std::to_string(endIndex-startIndex+1) + "\r\n";
            Node* curr = sl.head;
            for(int i = 0; i < startIndex; i++){
                curr = curr->forward[0];
            }
            for(int i = startIndex; i <= endIndex; i++){
                std::string element = curr->forward[0]->key;
                response += "$"+ std::to_string(element.size()) + "\r\n";
                response += element + "\r\n";
                curr = curr->forward[0];
            }

            return response;
        }
        else{
            std::string response = "-ERR wrong number of arguments for zrange command\r\n";
            return response;
        }
    }

std::string zcard_command(int& items, int client_fd, std::string& read_buffer, std::map<std::string, SkipList>& sets){
        if(items == 1){
            std::string key = parsebulkString(items, client_fd, read_buffer);
            SkipList& sl = sets[key];

            std::string response = ":"+std::to_string(sl.size)+"\r\n";
            return response;
        }
        else{
            std::string response = "-ERR wrong number of arguments for zcard command\r\n";
            return response;
        }
    }

std::string zscore_command(int& items, int client_fd, std::string& read_buffer, std::map<std::string, SkipList>& sets){
        if (items == 2){
            std::string response = "$-1\r\n";
            std::string listName = parsebulkString(items, client_fd, read_buffer);
            std::string key = parsebulkString(items, client_fd, read_buffer);

            SkipList& sl = sets[listName];

            Node* prev = sl.head;
            for (Node* n = sl.head->forward[0]; n != nullptr; n = n->forward[0]) {
                if (n->key == key) {
                    std::ostringstream oss;
                    oss << std::setprecision(17) << n->score;
                    std::string preciseScore = oss.str();
                    std::cout << preciseScore << std::endl;
                    response = "$"+ std::to_string(preciseScore.size())+ "\r\n" + preciseScore + "\r\n";
                }
            }
            return response;
        }
        else{
            std::string response = "-ERR wrong number of arguments for zscore command\r\n";
            return response;
        }
    }

std::string zrem_command(int& items, int client_fd, std::string& read_buffer, std::map<std::string, SkipList>& sets){
        if (items == 2){
            std::string response = ":1\r\n";
            std::string listName = parsebulkString(items, client_fd, read_buffer);
            std::string key = parsebulkString(items, client_fd, read_buffer);

            SkipList& sl = sets[listName];
            std::cout << sl.head->forward[0]->key << std::endl;
            std::vector<Node*> update(sl.maxLevel + 1, nullptr);
            Node* curr = sl.head;
            for (int i = sl.currentLevel; i >= 0; i--) {
                Node* node = curr;
                while (node->forward[i] != nullptr && node->forward[i]->key != key) {
                    node = node->forward[i];
                }
                std::cout << "update" << std::to_string(i) << " is " << node->key << std::endl;
                update[i] = node;
            }

            Node* target = update[0]->forward[0];
            if (target == nullptr || target->key != key) {
                response = ":0\r\n";
                return response;
            }

            for (int i = 0; i <= sl.currentLevel; i++) {
                if (update[i]->forward[i] != target) break;
                update[i]->forward[i] = target->forward[i];
            }

            delete target;
            sl.size-=1;
            while (sl.currentLevel > 0 && sl.head->forward[sl.currentLevel] == nullptr) {
                sl.currentLevel--;
            }

            return response;
        }
        else{
            std::string response = "-ERR wrong number of arguments for zrem command\r\n";
            return response;
        }
    }