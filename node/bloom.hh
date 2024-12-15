#pragma once

#include <bitset>
#include <chrono>
#include <cmath>
#include <ctime>
#include <deque>
#include <fstream>
#include <functional>
#include <iostream>
#include <map>
#include <memory>
#include <optional>
#include <queue>
#include <sstream>
#include <string>
#include <vector>

#include <boost/archive/text_iarchive.hpp>
#include <boost/archive/text_oarchive.hpp>
#include <boost/serialization/vector.hpp>

class Bloom {
  private:
    std::vector<bool>
        bit_array; // Use vector with dynamic size based on expected keys
    size_t hash_count;

    size_t hash(const std::string& key, size_t seed) const {
        std::hash<std::string> hasher;
        return (hasher(key) + seed) % bit_array.size();
    }

  public:
    Bloom() {}
    Bloom(size_t expected_keys, double false_positive_rate) {
        size_t bit_array_size =
            -(expected_keys * std::log(false_positive_rate)) /
            (std::log(2) * std::log(2));
        size_t optimal_hash_count =
            (bit_array_size / expected_keys) * std::log(2);

        bit_array.resize(bit_array_size); // Dynamically set bit array size
        hash_count = std::max(static_cast<size_t>(1), optimal_hash_count);
    }

    void add(const std::string& key) {
        for (size_t i = 0; i < hash_count; ++i) {
            bit_array[hash(key, i)] = true;
        }
    }

    bool likely_contain(const std::string& key) const {
        for (size_t i = 0; i < hash_count; ++i) {
            if (!bit_array[hash(key, i)]) {
                return false;
            }
        }
        return true;
    }

    template <class Archive>
    void serialize(Archive& ar, const unsigned int version) {
        ar & bit_array;
        ar & hash_count;
    }
};
