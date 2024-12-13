
#include <bitset>
#include <chrono>
#include <cmath>
#include <ctime>
#include <fstream>
#include <functional>
#include <iostream>
#include <map>
#include <memory>
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
constexpr int MEMTABLE_FLUSH_SIZE_MB = (128 * MB);

class CommitLog {
  public:
    CommitLog(const std::string& logFileName) : logFileName_(logFileName) {}

    // Function to append key-value pair to the log file
    void append(const std::string& key, const std::string& value) {
        std::ofstream logFile(logFileName_, std::ios::app);
        if (!logFile.is_open()) {
            std::cerr << "Failed to open log file for appending.\n";
            return;
        }

        // Create a log entry string (timestamp, key-value pair)
        logFile << key << ":" << value << ";";

        logFile.close();
    }

    // Function to read and print all entries in the commit log
    void printLog() {
        std::ifstream logFile(logFileName_);
        if (!logFile.is_open()) {
            std::cerr << "Failed to open log file for reading.\n";
            return;
        }

        std::string line;
        while (std::getline(logFile, line)) {
            std::cout << line << "\n";
        }

        logFile.close();
    }

  private:
    std::string logFileName_;

    // Helper function to get current timestamp as a string
    std::string getCurrentTimestamp() {
        std::time_t now = std::time(nullptr);
        std::tm* localTime = std::localtime(&now);

        std::ostringstream timestampStream;
        timestampStream << 1900 + localTime->tm_year << "-"
                        << 1 + localTime->tm_mon << "-" << localTime->tm_mday
                        << " " << 1 + localTime->tm_hour << ":"
                        << 1 + localTime->tm_min << ":"
                        << 1 + localTime->tm_sec;

        return timestampStream.str();
    }
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
    }

    template <class Archive>
    void serialize(Archive& ar, const unsigned int version) {
        ar & bit_array;
        ar & hash_count;
    }
};

class Sstable {
  private:
    std::string file_path;
    Bloom bloom;
    std::map<std::string, std::streampos> index;

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
    explicit Sstable(std::map<std::string, std::string>&& db) noexcept
        : bloom(db.size(), 0.01) { // Initialize BloomFilter with 1% FPR
        try {
            // Create a new file
            file_path = "sstable.dat";
            std::ofstream file(file_path, std::ios::binary);
            if (!file.is_open()) {
                throw std::runtime_error("Failed to create SSTable file.");
            }

            // Write Bloom filter
            for (const auto& [key, value] : db) {
                bloom.add(key);
            }
            // Placeholder for Bloom filter serialization (implement if needed)
            file.write("BLOOMFILTERPLACEHOLDER", 24);

            // auto s = serialize(bloom);
            // file.write(s.c_str(), s.size());

            // Write data and index sections directly from db
            for (const auto& [key, value] : db) {
                index[key] = file.tellp();
                file.write(value.c_str(), value.size());
                file.put('\0'); // Null-terminate each value
            }

            // Write index section at the end
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
        file_path = path;
        std::ifstream file(file_path, std::ios::binary);
        if (!file.is_open()) {
            // throw std::runtime_error("Failed to open SSTable file.");
            assert(0);
        }

        // Load Bloom filter (placeholder)
        char bloom_filter_placeholder[24];
        file.read(bloom_filter_placeholder, 24);

        // Load index section (assume it's at the end)
        file.seekg(-1, std::ios::end); // Simplified for demo purposes
        while (file.tellg() > 0) {
            std::string key;
            std::getline(file, key, '\0');
            std::streampos pos;
            file.read(reinterpret_cast<char*>(&pos), sizeof(pos));
            index[key] = pos;
        }

        file.close();
    }

    bool likely_contains_key(const std::string& key) {
        return bloom.might_contain(key);
    }

    std::string get_value(const std::string& key) {
        auto it = index.find(key);
        if (it == index.end()) {
            return ""; // Key not found
        }

        std::ifstream file(file_path, std::ios::binary);
        if (!file.is_open()) {
            throw std::runtime_error(
                "Failed to open SSTable file for reading.");
        }

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

    uint64_t get_size() const { return size; }

    std::unique_ptr<Sstable> flush() {
        return std::make_unique<Sstable>(std::move(db));
    }
};

struct Node {

    CommitLog log;

    void flush() { auto sstable = memtable.flush(); }

  public:
    Memtable memtable;
    Node() : log("clog.txt") {}

    void write(const std::string& k, const std::string& v) {

        /* append to log */
        log.append(k, v);

        /* update memtable */
        memtable.insert(k, v);
    }

    void heartbeat() {
        if (memtable.get_size() >= MEMTABLE_FLUSH_SIZE_MB) {
            flush();
        }
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

    int i = 0;
    while (true) {
        node.write(key + std::to_string(i++), value);
        if (node.memtable.get_size() >= 1 * GB) {
            break;
        }
    }

    auto endTime = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> duration = endTime - startTime;

    std::cout << "append to commit log: " << duration.count() << " seconds\n";

    node.flush();

    return 0;
}
