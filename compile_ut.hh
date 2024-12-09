
#pragma once

#include <algorithm>
#include <array>

#include "node.hh"

using hash = uint64_t;

namespace Compile {

struct UT {

    // Compile-time FNV-1a hash function
    template <typename Type>
    static constexpr size_t cx_hash(const Type& toHash) {
        return toHash;
    }

    // Compile-time map structure
    template <typename Key, typename Value, std::size_t Size> struct cx_map {
        std::array<std::pair<Key, Value>, Size> data;
        [[nodiscard]] constexpr Value at(const Key& key) const {
            const auto it =
                std::find_if(data.begin(), data.end(),
                             [&key](const auto& v) { return v.first == key; });
            if (it != data.end()) {
                return it->second;
            } else {
                throw std::range_error("Key not found");
            }
        }

        [[nodiscard]] constexpr std::array<std::pair<Key, Value>,
                                           Size>::const_iterator
        lower_bound(const Key& key) const {
            return std::find_if(
                data.begin(), data.end(),
                [&key](const auto& v) { return v.first >= key; });
        }
    };

    template <typename Generator, std::size_t... Is>
    constexpr std::array<
        std::pair<hash, std::pair<hash, hash>>,
        sizeof...(Is)> static make_keyspace(Generator generator,
                                            std::index_sequence<Is...>) {
        return std::array<std::pair<hash, std::pair<hash, hash>>,
                          sizeof...(Is)>{generator(Is)...};
    }

    static constexpr std::pair<unsigned long,
                               std::pair<unsigned long, unsigned long>>
    entry(size_t k) noexcept {
        return {k, {k, k}};
    }

    template <size_t N> static void compile_time_keyspace() {
        /* db has N entries */
        constexpr auto db = cx_map<hash, std::pair<hash, hash>, N>{
            make_keyspace([](size_t i) { return entry(i); },
                          std::make_index_sequence<N>{})};

        /* (i, j) are inclusve (eg. (0, 1) should return 2) */
        static_assert(Node::get_keyspace_hash<cx_hash<uint64_t>>(db, 0, N) ==
                      N);
    }

    /* compile time unit test */
    void ctut_keyspace() {

        compile_time_keyspace<1>();
        compile_time_keyspace<2>();
        // compile_time_keyspace<3>();
        compile_time_keyspace<7>();
        compile_time_keyspace<11>();

        // constexpr cx_map<hash, std ::pair<hash, hash>, 1> db = {
        //     std::make_pair<int, std::pair<int, int>>(0, {0, 1}),
        //     // std::make_pair<int, std::pair<int, int>>(1, {0, 1}),
        //     // std::make_pair<int, std::pair<int, int>>(2, {0, 1}),
        //     // std::make_pair<int, std::pair<int, int>>(3, {0, 1}),
        // };

        // static_assert(get_keyspace_hash<cx_hash<uint64_t>>(db, 0, 1) ==
        // 1);
    }
};
}; // namespace Compile