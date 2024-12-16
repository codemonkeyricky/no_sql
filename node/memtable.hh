#pragma once

#include <map>
#include <memory>
#include <optional>
#include <string>

#include "node/sstable.hh"

class Memtable {

    uint64_t size = 0;
    std::map<std::string, std::string> db;

  public:
    void insert(const std::string& k, const std::string& v) {
        db[k] = v;
        size += k.size() + v.size();
    }

    std::optional<std::string> get(const std::string& k) {
        if (db.count(k)) {
            return db[k];
        }
        return {};
    }

    std::map<std::string, std::string> get_all() const { return db; }

    uint64_t get_size() const { return size; }

    std::unique_ptr<Sstable> flush(const std::string& name) {
        auto sstable = std::make_unique<Sstable>(name, std::move(db));
        db.clear();
        return sstable;
    }
};
