#include "node.hh"
#include <iomanip>
#include <iostream>

using namespace std;

void printStats(const Node::Stats& stats) {}

void printPeerMap(const Lookup& lookup) {

    constexpr int columnWidth = 6;

    std::cout << std::setw(columnWidth) << "Token";
    std::cout << std::setw(columnWidth) << "TS";
    std::cout << std::setw(columnWidth) << "Id";
    std::cout << '\n';
    for (const auto& row : lookup) {
        for (int element : row) {
            std::cout << std::setw(columnWidth) << element;
        }
        std::cout << '\n';
    }

    // for (auto& [token, timestamp, node_id] : lookup) {
    //     cout << token << " " << timestamp << " " << node_id << endl;
    // }

    cout << endl;
}

int main() {
    auto& gd = GlobalDirectory::instance();

    vector<Node*> nodes;

    constexpr int TRANSACTION = 512;

    auto printStats = [&]() {
        for (auto i = 0; i < nodes.size(); ++i) {

            auto stats = nodes[i]->get_stats();

            cout << "id : " << nodes[i]->get_id() << ", ";
            cout << "rd : " << stats.read << ", ";
            cout << "wr : " << stats.write << ", ";
            cout << "rd_fwd : " << stats.read_fwd << ", ";
            cout << "wr_fwd : " << stats.write_fwd << endl;
        }
    };

    auto populate_db = [&]() {
        /* write */
        for (auto i = 0; i < TRANSACTION; ++i) {
            auto k = string("k");
            k += to_string(i);
            auto v = string("v");
            v += to_string(i);
            auto kk = rand() % nodes.size();

            nodes[kk]->write(k, v);
            auto vv = nodes[kk]->read(k);
            assert(v == vv);
        }
    };

    auto verify_db = [&]() {
        /* read back */
        for (auto i = 0; i < TRANSACTION; ++i) {
            auto k = string("k");
            k += to_string(i);
            auto v = string("v");
            v += to_string(i);
            auto kk = rand() % nodes.size();
            auto vv = nodes[kk]->read(k);
            assert(v == vv);
        }
    };

    auto heartbeat = [&]() {
        for (auto& n : nodes) {
            n->heartbeat();
        }
    };

    auto wait_for_gossip = [&]() {
        bool match;
        do {
            heartbeat();

            /* check match */
            match = true;
            for (auto i = 0; i + 1 < nodes.size() && match; ++i) {
                auto a = nodes[i]->get_lookup();
                auto b = nodes[i + 1]->get_lookup();

                auto pa = nodes[i]->peers();
                auto pb = nodes[i + 1]->peers();
                if (a != b)
                    match = false;
            }

        } while (!match);
    };

    auto node_remove = [&](int k) {
        auto id = nodes[k]->get_id();

        /* remove node from global directory */
        gd.erase(id);

        /* erase node */
        nodes.erase(nodes.begin() + k);
    };

    auto node_add = [&](int seed) {
        // nodes.push_back(new Node(nodes.size() == 0 ? -1 : 0, 3));
        nodes.push_back(new Node(seed));
    };

    vector<uint64_t> seeds;

    for (auto i = 0; i < 10; ++i) {
        nodes.push_back(new Node(i == 0 ? -1 : 0, 3));
    }
    // cout << "\033[2J\033[1;1H";

    int round = 0;
    // while (round < 2)
    {
        heartbeat();

        printPeerMap(nodes[0]->get_lookup());

        wait_for_gossip();

        populate_db();

        verify_db();

        printStats();

        node_remove(0);
        // populate_db();
        verify_db();

        /* seed node 2 */
        node_add(2);

        wait_for_gossip();

        verify_db();

        ++round;
    }
}