
#include <array>
#include <functional>
#include <iostream>
#include <map>
#include <set>
#include <string>
#include <sys/types.h>
#include <thread>
#include <tuple>
#include <unordered_map>
#include <vector>

using key = std::string;
using value = std::string;

using hash = uint64_t;

constexpr hash get_keyspace_hash(hash i, hash j) {
    if (i + 1 >= j)
        return 1;

    auto m = (i + j) / 2;
    auto l = get_keyspace_hash(i, i + m);
    auto r = get_keyspace_hash(i + m, j);

    // return static_cast<uint64_t>(std::hash<uint64_t>{}(l + r));
    return l + r;
}

int main() { std::cout << get_keyspace_hash(0, 4) << std::endl; }