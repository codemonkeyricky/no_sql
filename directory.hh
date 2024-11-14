
#pragma once

#include <cassert>
#include <cstdint>
#include <string>
#include <unordered_map>

class GlobalDirectory {
    std::unordered_map<uint64_t, void*> dir;

  public:
    static GlobalDirectory& instance() {
        static GlobalDirectory gd;
        return gd;
    }

    void* lookup(uint64_t id) {
        if (dir.count(id))
            return dir[id];
        return nullptr;
    }

    void insert(uint64_t id, void* n) {

        /* id must not exist */
        assert(dir.count(id) == 0);
        dir[id] = n;
    }

    void erase(uint64_t id) {

        /* id must exist */
        assert(dir.count(id));
        dir.erase(id);
    }

    bool is_alive(uint64_t id) { return dir.count(id); }

    auto generate_id() -> int {
        static int id = 0;
        return id++;
    }
};
