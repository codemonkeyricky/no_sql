#include <chrono>
#include <cmath>
#include <ctime>
#include <fstream>
#include <iostream>
#include <map>
#include <optional>
#include <sstream>
#include <string>
#include <vector>

#include <boost/archive/text_iarchive.hpp>
#include <boost/archive/text_oarchive.hpp>
#include <boost/serialization/vector.hpp>

#include "node/bloom.hh"
#include "node/log.hh"

class Sstable {
    std::string filename;
    Bloom bloom;

    std::string serialize(Bloom& data) {
        std::ostringstream oss;
        boost::archive::text_oarchive oa(oss);
        oa << data;
        return oss.str();
    }

    Bloom deserialize(std::string& data) {
        Bloom rv;
        std::istringstream iss(data);
        boost::archive::text_iarchive ia(iss);
        ia >> rv;
        return std::move(rv);
    }

    void create_bloom_db(std::map<std::string, std::string>& db) {

        // Write Bloom filter
        for (const auto& [key, value] : db) {
            bloom.add(key);
        }

        std::ofstream file_bloom(filename + "_bloom.db",
                                 std::ios::binary | std::ofstream::trunc);
        assert(file_bloom.is_open());

        auto i = std::chrono::high_resolution_clock::now();
        std::string serialized_bloom = serialize(bloom);
        auto j = std::chrono::high_resolution_clock::now();

        std::chrono::duration<double> duration = j - i;
        std::cout << "bloom filter serialize time: " << duration.count()
                  << " seconds\n";

        file_bloom.write(serialized_bloom.c_str(), serialized_bloom.size());
        file_bloom.close();
    }

    std::vector<std::pair<std::string, std::streampos>>
    create_data_db(std::map<std::string, std::string>& db) {

        std::ofstream file_data(filename + "_data.db",
                                std::ios::binary | std::ofstream::trunc);

        std::vector<std::pair<std::string, std::streampos>> index;
        for (const auto& [key, value] : db) {
            index.push_back(
                {key, file_data.tellp()}); // Save the offset of the
                                           // current position in the file
            file_data.write(value.c_str(), value.size());
            file_data.put('\0'); // Null-terminate each value
        }

        file_data.close();

        return index;
    }

    void create_index_db(
        std::vector<std::pair<std::string, std::streampos>>& index) {
        std::ofstream file_index(filename + "_index.db",
                                 std::ios::binary | std::ofstream::trunc);

        // Write index section at the end of the file
        for (const auto& [key, pos] : index) {
            file_index.write(key.c_str(), key.size());
            file_index.put('\0'); // Null-terminate the key
            file_index.write(reinterpret_cast<const char*>(&pos), sizeof(pos));
            // file_index.put('\0'); // Null-terminate the key
        }

        file_index.close();
    }

  public:
    /* not copyable*/
    Sstable(const Sstable&) = delete;
    /* not movable */
    Sstable(const Sstable&&) = delete;

    /* create sstable based on memtable */
    explicit Sstable(const std::string& filename,
                     std::map<std::string, std::string>&& db) noexcept
        // Initialize BloomFilter with 1% FPR
        : bloom(db.size(), 0.01), filename(filename) {

        /* create bloom.db */
        create_bloom_db(db);

        /* create data.db */
        auto index = create_data_db(db);

        /* create index.db */
        create_index_db(index);
    }

    /* create sstable based on files */
    explicit Sstable(const std::string& path) noexcept
        : bloom(0, 0.01) { // Placeholder BloomFilter initialization

        /* TODO: */
        assert(0);
    }

    bool likely_contain(const std::string& key) {
        return bloom.likely_contain(key);
    }

    std::optional<std::string> get_value(const std::string& key) {
        // Open the file to read the value directly using the key's offset
        std::ifstream file_index(filename + "_index.db", std::ios::binary);
        assert(file_index.is_open());

        // Locate the index by reading the index section from the end of the
        // file
        std::map<std::string, std::streampos> index;
        {
            file_index.seekg(0);
            while (!file_index.eof()) {
                std::string key;
                std::getline(file_index, key, '\0');
                if (key == "")
                    break;
                std::streampos pos;
                file_index.read(reinterpret_cast<char*>(&pos), sizeof(pos));
                index[key] = pos;
            }
            file_index.close();
        }

        // Look for the key in the index
        auto it = index.find(key);
        if (it == index.end()) {
            return {}; // Key not found
        }

        std::string value;
        {
            std::ifstream file_data(filename + "_data.db", std::ios::binary);

            // Seek to the position of the value in the file

            file_data.seekg(it->second);
            // file_data.seekg(0);
            std::getline(file_data, value, '\0');

            file_data.close();
        }

        return value;
    }
};
