
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

    int sstable_index = 0;

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
            if (ss->likely_contain(k)) {
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

    Bloom bloom(8, 0.01);
    for (auto i = 0; i < 8; ++i) {
        bloom.add(to_string(i));
    }
    for (auto i = 0; i < 10; ++i) {
        cout << bloom.likely_contain(to_string(i)) << endl;
    }

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

    for (auto i = 0; i < 500000; ++i) {
        node.write(to_string(i), to_string(i));
    }

    cout << *node.read("7") << endl;
    node.memtable_flush();
    auto v = node.read("3");
    cout << *v << endl;

    auto endTime = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> duration = endTime - startTime;

    std::cout << "append to commit log: " << duration.count() << " seconds\n";

    return 0;
}
