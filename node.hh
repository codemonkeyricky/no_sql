
#pragma once

#include <array>
#include <functional>
#include <map>
#include <set>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

class Node {

    std::map<uint64_t, Node*> remote;
    std::set<std::array<uint64_t, 2>> local;

  public:
    Node();
    ~Node();

    std::string read(std::string& key) {

        auto hash = static_cast<uint64_t>(std::hash<std::string>{}(key));
        auto n = remote.lower_bound(hash);
        return n->second->read(key);
    }
    int write(int key, int value) {}

    std::map<uint64_t, Node*> gossip() { return remote; }
};