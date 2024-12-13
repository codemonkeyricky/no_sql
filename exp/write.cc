
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

using namespace std;

constexpr int KB = 1024;
constexpr int MB = 1024 * KB;
constexpr int GB = 1024 * MB;
constexpr int MEMTABLE_FLUSH_SIZE = (128 * MB);

class CommitLog {
  public:
    CommitLog(const std::string& filename) : filename(filename) {}

    // Function to append key-value pair to the log file
    void append(const std::string& key, const std::string& value) {
        std::ofstream logFile(filename, std::ios::app);
        if (!logFile.is_open()) {
            std::cerr << "Failed to open log file for appending.\n";
            return;
        }

        // Create a log entry string (timestamp, key-value pair)
        logFile << key << ":" << value << ";";

        logFile.close();
    }

    void clear() {
        /* clear file */
        std::ofstream log(filename, std::ofstream::out | std::ofstream::trunc);
        log.close();
    }

  private:
    std::string filename;
};

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

    bool might_contain(const std::string& key) const {
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

  public:
    /* create sstable based on memtable */
    explicit Sstable(const std::string& filename,
                     std::map<std::string, std::string>&& db) noexcept
        // Initialize BloomFilter with 1% FPR
        : bloom(db.size(), 0.01), filename(filename) {
        try {
            // Create a new file
            std::ofstream file(filename, std::ios::binary);
            if (!file.is_open()) {
                throw std::runtime_error("Failed to create SSTable file.");
            }

            // Serialize and write Bloom filter
            std::string serialized_bloom = serialize(bloom);
            file.write(serialized_bloom.c_str(), serialized_bloom.size());

            // Write data and store the position for the index
            std::vector<std::pair<std::string, std::streampos>> index;
            for (const auto& [key, value] : db) {
                index.push_back(
                    {key, file.tellp()}); // Save the offset of the current
                                          // position in the file
                file.write(value.c_str(), value.size());
                file.put('\0'); // Null-terminate each value
            }

            // Write index section at the end of the file
            for (const auto& [key, pos] : index) {
                file.write(key.c_str(), key.size());
                file.put('\0'); // Null-terminate the key
                file.write(reinterpret_cast<const char*>(&pos), sizeof(pos));
            }

            file.close();
        } catch (const std::exception& e) {
            std::cerr << "Error creating SSTable: " << e.what() << std::endl;
        }
    }

    explicit Sstable(const std::string& path) noexcept
        : bloom(0, 0.01) { // Placeholder BloomFilter initialization
        filename = path;
        std::ifstream file(filename, std::ios::binary);
        if (!file.is_open()) {
            assert(0); // Error handling
        }

        // Load and deserialize Bloom filter
        std::string bloom_data;
        std::getline(file, bloom_data,
                     '\0'); // Read the serialized Bloom filter data
        bloom = deserialize(bloom_data);

        // Load index section (assume it's at the end)
        file.seekg(-1, std::ios::end); // Simplified for demo purposes
        std::map<std::string, std::streampos> index;
        while (file.tellg() > 0) {
            std::string key;
            std::getline(file, key, '\0');
            std::streampos pos;
            file.read(reinterpret_cast<char*>(&pos), sizeof(pos));
            index[key] = pos;
        }

        // Store the index map into the class as needed for any future lookup
        // (optional)
        file.close();
    }

    bool likely_contains_key(const std::string& key) {
        return bloom.might_contain(key);
    }

    std::optional<std::string> get_value(const std::string& key) {
        // Open the file to read the value directly using the key's offset
        std::ifstream file(filename, std::ios::binary);
        if (!file.is_open()) {
            throw std::runtime_error(
                "Failed to open SSTable file for reading.");
        }

        // Locate the index by reading the index section from the end of the
        // file
        file.seekg(-1, std::ios::end);
        std::map<std::string, std::streampos> index;
        while (file.tellg() > 0) {
            std::string idx_key;
            std::getline(file, idx_key, '\0');
            std::streampos pos;
            file.read(reinterpret_cast<char*>(&pos), sizeof(pos));
            index[idx_key] = pos;
        }

        // Look for the key in the index
        auto it = index.find(key);
        if (it == index.end()) {
            return {}; // Key not found
        }

        // Seek to the position of the value in the file
        file.seekg(it->second);
        std::string value;
        std::getline(file, value, '\0');

        return value;
    }
};

class Memtable {

    uint64_t size = 0;
    std::map<std::string, std::string> db;

  public:
    void insert(const string& k, const string& v) {
        db[k] = v;
        size += k.size() + v.size();
    }

    optional<string> get(const string& k) {
        if (db.count(k)) {
            return db[k];
        }
        return {};
    }

    uint64_t get_size() const { return size; }

    std::unique_ptr<Sstable> flush(const string& name) {
        auto sstable = std::make_unique<Sstable>(name, std::move(db));
        db.clear();
        return sstable;
    }
};

struct Node {

    CommitLog log;

    // void flush() {}

    deque<unique_ptr<Sstable>> q;

    int sstable_index;

    void memtable_flush() {

        /* flush */
        /* flush memtable */
        q.push_front(
            memtable.flush(string("sstable_") + to_string(sstable_index++)));

        /* trim log */
        log.clear();
    }

    void sstable_compact() {

        /* compact sstable */
        if (q.size() >= 2) {
            auto older = move(q.back());
            q.pop_back();

            auto newer = move(q.back());
            q.pop_back();
        }
    }

  public:
    Memtable memtable;
    Node() : log("clog.txt") {}

    optional<std::string> read(const std::string& k) {
        /* check memtable */
        if (auto v = memtable.get(k)) {
            return v;
        }

        /* check sstable */
        for (auto& ss : q) {
            if (ss->likely_contains_key(k)) {
                if (auto v = ss->get_value(k)) {
                    /* value exists */
                    return v;
                }
            }
        }
        return {};
    }

    void write(const std::string& k, const std::string& v) {

        /* append to log */
        log.append(k, v);

        /* update memtable */
        memtable.insert(k, v);
    }

    void heartbeat() {

        if (memtable.get_size() >= MEMTABLE_FLUSH_SIZE) {
            memtable_flush();
        }

        sstable_compact();
    }
};

int main() {
    CommitLog commitLog("commit_log.txt");

    // Data to append (key-value pair)
    std::string key = "user";
    std::string value = "A"; // Starting with a single character

    // Size of value to append in each entry to eventually reach ~1GB total size
    // size_t targetSize = (16 * MB);
    size_t entrySize = 1024; // Size of each value (1KB)
    // size_t totalAppends =
    //     targetSize / entrySize; // Number of appends to reach 1GB

    // Fill value with repeated 'A' characters to form 1KB data per entry
    value = std::string(entrySize, 'A');

    Node node;

    // Measure the time taken to append 1GB worth of data
    auto startTime = std::chrono::high_resolution_clock::now();

    /*
        int i = 0;
        while (true) {
            node.write(key + std::to_string(i++), value);
            if (node.memtable.get_size() >= 3 * KB) {
                break;
            }
        }
        */

    for (auto i = 0; i < 32; ++i) {
        node.write(to_string(i), to_string(i));
    }

    cout << *node.read("0") << endl;
    node.memtable_flush();
    cout << *node.read("0") << endl;

    auto endTime = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> duration = endTime - startTime;

    std::cout << "append to commit log: " << duration.count() << " seconds\n";

    return 0;
}
