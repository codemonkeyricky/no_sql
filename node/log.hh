
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

class CommitLog {
    std::string filename;

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
};
