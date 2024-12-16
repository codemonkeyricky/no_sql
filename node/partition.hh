#pragma once

#include <stdint.h>

class Partitioner {

    uint64_t range = 1e9 + 7;

  public:
    Partitioner() { srand(0); }
    ~Partitioner() {}

    static Partitioner& instance() {
        static Partitioner p;
        return p;
    }

    uint64_t getToken() const { return rand() % range; };

    uint64_t getRange() const { return range; }
};
