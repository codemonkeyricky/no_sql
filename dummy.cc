
#include <algorithm>
#include <array>
#include <cstddef>
#include <stdexcept>
#include <stdint.h>
#include <utility>
#include <cassert>

// Hash type
using hash = unsigned long;

// Compile-time FNV-1a hash function
template <typename Type> static constexpr size_t cx_hash(const Type& toHash) {
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
        return std::find_if(data.begin(), data.end(),
                            [&key](const auto& v) { return v.first >= key; });
    }
};

// Recursive compile-time hash function for keyspace
template <auto hash_fn, typename DB>
static constexpr hash get_keyspace_hash(DB& db, hash i, hash j) {

    auto i_it = db.lower_bound(i);
    auto j_it = db.lower_bound(j);

    if (i_it + 1 >= j_it) {
        return 1;
    }

    hash m = (i + j) / 2;
    auto l = get_keyspace_hash<hash_fn>(db, i, m);
    auto r = get_keyspace_hash<hash_fn>(db, m, j);

    return static_cast<hash>(hash_fn(l + r));
}

template <typename Generator, std::size_t... Is>
constexpr std::array<std::pair<hash, std::pair<hash, hash>>, sizeof...(Is)>
make_keyspace(Generator generator, std::index_sequence<Is...>) {
    return std::array<std::pair<hash, std::pair<hash, hash>>, sizeof...(Is)>{
        generator(Is)...};
}

constexpr std::pair<unsigned long, std::pair<unsigned long, unsigned long>>
entry(size_t k) noexcept {
    return {k, {k, k}};
}

template <size_t N> void compile_time_keyspace() {
    /* db has N entries */
    constexpr auto db = cx_map<hash, std::pair<hash, hash>, N>{make_keyspace(
        [](size_t i) { return entry(i); }, std::make_index_sequence<N>{})};

    /* (i, j) are inclusve (eg. (0, 1) should return 2) */
    static_assert(get_keyspace_hash<cx_hash<uint64_t>>(db, 0, N) == N);
}

/* compile time unit test */
int main() {

    compile_time_keyspace<1>();
    compile_time_keyspace<2>();
    compile_time_keyspace<3>();
    compile_time_keyspace<4>();
    compile_time_keyspace<128>();
}