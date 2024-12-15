
#pragma once

#include "directory.hh"
#include <array>
#include <functional>
#include <iostream>
#include <map>
#include <set>
#include <string>
#include <sys/types.h>
#include <thread>
#include <tuple>
#include <unordered_map>
#include <vector>

#include <boost/variant2/variant.hpp>

#include <boost/asio.hpp>
#include <boost/asio/connect.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/write.hpp>
#include <boost/cobalt.hpp>

#include <boost/archive/text_iarchive.hpp>
#include <boost/archive/text_oarchive.hpp>
#include <boost/exception/diagnostic_information.hpp>
#include <boost/serialization/map.hpp>
#include <boost/serialization/set.hpp>
#include <boost/serialization/vector.hpp>

using key = std::string;
using value = std::string;
using hash = uint64_t;
using node_addr = std::string;
using node_id = uint64_t;
using Tokens = std::set<hash>;
using LookupEntry = std::array<uint64_t, 3>; ///< token / timestamp / node_id
using Lookup = std::set<LookupEntry>;
using HashLookup = std::unordered_map<uint64_t, std::string>;

class Time {

    uint64_t time;

  public:
    static Time& instance() {
        static Time t;
        return t;
    }

    /* increment everytime we look at the time */
    uint64_t get() { return ++time; }
};

struct NodeMap {

    struct Node {
        enum Status {
            Joining,
            Live,
            Down,
        };

        uint64_t timestamp;
        Status status;
        Tokens tokens;

        template <class Archive>
        void serialize(Archive& ar, const unsigned int version) {
            ar & timestamp;
            ar & status;
            ar & tokens;
        }
    };

    std::map<node_addr, Node> nodes;

    template <class Archive>
    void serialize(Archive& ar, const unsigned int version) {
        ar & nodes;
    }

    /* add remote map into local map */
    NodeMap operator+(NodeMap const& remote_map) {
        auto& remote_nodes = remote_map.nodes;
        auto& local_nodes = nodes;
        auto i = local_nodes.begin();
        auto j = remote_nodes.begin();
        while (i != local_nodes.end() || j != remote_nodes.end()) {
            if (i != local_nodes.end() && j != remote_nodes.end()) {
                if (i->first < j->first) {
                    /* remote is missing - whatever */
                    ++i;
                } else if (i->first > j->first) {
                    /* local is missing - add */
                    local_nodes[j->first] = j->second;
                    ++j;
                } else { // if (i->first == j->first)
                    if (j->second.timestamp > i->second.timestamp) {
                        /* remote is more up to date */
                        i->second = j->second;
                    }
                    ++i, ++j;
                }
            } else if (i == local_nodes.end()) {
                /* append all remote */
                local_nodes[j->first] = j->second;
                ++j;
            } else {
                /* remote ran out - don't care */
                break;
            }
        }
        return *this;
    }
};

class Partitioner {

    uint64_t range = 1e9 + 7;

  public:
    Partitioner() { srand(0); }
    ~Partitioner() {}

    static Partitioner& instance() {
        static Partitioner p;
        return p;
    }

    uint64_t getToken() const { return rand() % range; };

    uint64_t getRange() const { return range; }
};

class Node final {

  public:
    struct Stats {
        int read;
        int write;
        int read_fwd;
        int write_fwd;
        int gossip_rx;
        int gossip_tx;
    };
    std::shared_ptr<boost::asio::steady_timer> cancel;
    uint32_t outstanding = 0;
    node_addr self;
    bool anti_entropy_req = false;

  private:
    Partitioner& p = Partitioner::instance();
    Time& t = Time::instance();

    NodeMap local_map;
    Lookup lookup;
    std::map<hash, std::pair<key, value>> db;
    node_id id;
    Stats stats;
    HashLookup nodehash_lookup;
    int replication_factor; /* replication factor */

    auto update_lookup() -> void;
    auto retire_token() -> void;

    /* update lookup table after gossip completes */
    uint64_t current_time_ms() {
        std::chrono::duration<double, std::milli> ms =
            std::chrono::steady_clock::now().time_since_epoch();
        return ms.count();
    }

  public:
    Node(node_addr self, node_addr seed = "", int vnode = 3, int rf = 3)
        : self(self), replication_factor(rf) {

        auto seedhash = static_cast<uint64_t>(std::hash<std::string>{}(seed));
        nodehash_lookup[seedhash] = seed;
        if (seed != "") {
            /* insert seed unknown state, updated during gossip */
            local_map.nodes[seed].timestamp = 0;
        }

        auto selfhash = this->id =
            static_cast<uint64_t>(std::hash<std::string>{}(self));
        nodehash_lookup[selfhash] = self;
        local_map.nodes[self].timestamp = current_time_ms();
        local_map.nodes[self].status = NodeMap::Node::Joining;
        for (auto i = 0; i < vnode; ++i) {
            /* token value may dup... but should fail gracefully */
            local_map.nodes[self].tokens.insert(p.getToken());
        }
    }

    Node(const Node&) = delete; /* cannot be copied */
    Node(Node&&) = delete;

    // ~Node() {}

    /* stream data: [i, j) */
    std::map<hash, std::pair<key, value>> stream(hash i, hash j) {

        std::map<hash, std::pair<key, value>> rv;
        for (auto it = db.lower_bound(i); it != db.end() && it->first < j;
             ++it) {
            rv[it->first] = it->second;
        }

        return rv;
    }

    template <typename T> std::string serialize(T&& data) {
        std::ostringstream oss;
        boost::archive::text_oarchive oa(oss);
        oa << data;
        return oss.str();
    }

    template <typename T, typename StringType>
    T deserialize(StringType&& data) {
        T rv;
        std::istringstream iss(data);
        boost::archive::text_iarchive ia(iss);
        ia >> rv;
        return std::move(rv);
    }

    void gossip_rx(std::string& gossip) {

        // std::cout << self << ":" << "gossip invoked!" << std::endl;

        /* fetch remote_map */
        auto remote_map = deserialize<NodeMap>(gossip);

        /* update local_map */
        local_map = remote_map = remote_map + local_map;
        update_lookup();

        /* communicated retired token in next gossip round */
        retire_token();

        /* serialize local_map */
        gossip = serialize(local_map);

        /* received gossip */
        ++stats.gossip_rx;
    }

    boost::cobalt::task<void> gossip_tx() {

        // std::cout << self << ":" << "gossip_tx() - #1" << std::endl;

        /* collect all peers */
        std::vector<node_addr> peers;
        for (auto& peer : local_map.nodes) {
            /* exclude ourselves */
            if (peer.first != self) {
                if (peer.second.status != NodeMap::Node::Down) {
                    peers.push_back(peer.first);
                }
            }
        }

        /* TODO: pick 3 peers at random to gossip */
        int k = 1;

        auto io = co_await boost::asio::this_coro::executor;
        // std::cout << self << ":" << "gossip_tx() - #2" << std::endl;
        while (k && peers.size()) {
            auto kk = peers[rand() % peers.size()];

            auto peer_addr = kk;
            auto p = peer_addr.find(":");
            auto addr = peer_addr.substr(0, p);
            auto port = peer_addr.substr(p + 1);

            boost::asio::ip::tcp::resolver resolver(io);
            boost::asio::ip::tcp::socket socket(io);
            auto ep = resolver.resolve(addr, port);

            boost::system::error_code err_code;
            boost::asio::async_connect(
                socket, ep,
                [&socket, &err_code](const boost::system::error_code& error,
                                     const boost::asio::ip::tcp::endpoint&) {
                    err_code = error;
                    // std::cout << "error = " << error << std::endl;
                });

            // std::cout << self << ":" << "gossip_tx() - #3" << std::endl;
            try {

                auto payload = "g:" + serialize(local_map);
                co_await boost::asio::async_write(
                    socket, boost::asio::buffer(payload, payload.size()),
                    boost::cobalt::use_task);

                /* read results */

                // std::cout << self << ":" << "gossip_tx() - #4" << std::endl;
                char rx_payload[1024] = {};
                std::size_t n = co_await socket.async_read_some(
                    boost::asio::buffer(rx_payload), boost::cobalt::use_task);

                auto serialized_data = std::string(rx_payload + 3, n - 3);
                auto remote_map = deserialize<NodeMap>(serialized_data);
                local_map = remote_map = local_map + remote_map;
                update_lookup();

                /* TODO: account for peer death */

            } catch (std::exception& e) {
                std::cout << self << ":" << "heartbeat() - failed to connect "
                          << peer_addr << std::endl;
                local_map.nodes[peer_addr].status = NodeMap::Node::Down;
                local_map.nodes[peer_addr].timestamp = current_time_ms();
                update_lookup();
            }
            --k;
        }

        if (local_map.nodes[self].status == NodeMap::Node::Joining) {

            auto& my_node = local_map.nodes[self];

            /* take over token, token-1, token-2, .... ptoken +1*/

            if (lookup.size()) {

                for (auto& my_token : my_node.tokens) {

                    /*
                     * range to stream is from previous token to my_token
                     * from the *next* node.
                     */

                    /* calculate range */

                    std::array<uint64_t, 3> target = {my_token};
                    auto it = lookup.lower_bound(target);
                    if (it == lookup.end()) {
                        it = lookup.begin();
                    }
                    auto [token, timestamp, id] = *it;

                    Lookup::iterator p;
                    if (it == lookup.begin())
                        p = prev(lookup.end());
                    else
                        p = prev(it);

                    auto [ptoken, pts, pid] = *p;

                    auto remote_db = co_await stream_from_remote(
                        nodehash_lookup[id], ptoken + 1, token);

                    /* insert into local db */
                    db.insert(remote_db.begin(), remote_db.end());
                }
            }

            local_map.nodes[self].status = NodeMap::Node::Live;
            local_map.nodes[self].timestamp = current_time_ms();
        }
    }

    boost::cobalt::task<std::pair<boost::system::error_code, std::string>>
    read_remote(std::string& peer, std::string& key) {

        std::cout << self << ":" << "read_remote():" << peer << " - " << key
                  << std::endl;

        auto io = co_await boost::asio::this_coro::executor;

        boost::asio::ip::tcp::resolver resolver(io);
        boost::asio::ip::tcp::socket socket(io);

        auto p = peer.find(":");
        auto addr = peer.substr(0, p);
        auto port = peer.substr(p + 1);

        auto ep = resolver.resolve(addr, port);

        try {
            /* Connect */
            boost::asio::async_connect(
                socket, ep,
                [&socket](const boost::system::error_code& error,
                          const boost::asio::ip::tcp::endpoint&) {});

            std::string req = "r:" + key;
            co_await boost::asio::async_write(
                socket, boost::asio::buffer(req.c_str(), req.size()),
                boost::cobalt::use_task);

            /* read results */
            char payload[1024] = {};
            std::size_t n = co_await socket.async_read_some(
                boost::asio::buffer(payload), boost::cobalt::use_task);

            std::string rv(payload + 3, n - 3);
            co_return {{}, rv};
        } catch (boost::system::system_error const& e) {
            std::cout << "read error: " << e.what() << std::endl;
            co_return {e.code(), ""};
        }
    }

    boost::cobalt::task<std::string> read(std::string& key) {

        auto key_hash =
            static_cast<uint64_t>(std::hash<std::string>{}(key)) % p.getRange();

        LookupEntry target = {key_hash, 0, 0};
        auto it = lookup.lower_bound(target);

        std::vector<uint64_t> ids;
        int rf = replication_factor;
        while (rf-- > 0) {
            if (it == lookup.end())
                it = lookup.begin();
            ids.push_back((*it)[2]);
            ++it;
        }

        /* walk the ring until we find a working node */
        int k = 0;
        while (k < ids.size()) {

            /* parse */
            auto id = ids[k++]; /* note id shadows this->id */
            if (id == this->id) {
                std::cout << "read (local) " << key << std::endl;
                if (key == "k3") {
                    volatile int dummy = 0;
                }
                /* local */
                ++stats.read;
                if (db.count(key_hash)) {
                    /* exists */
                    co_return db[key_hash].second;
                }
                assert(0);
            } else {
                std::cout << "read (remote) " << key << std::endl;
                /* forward to remote if alive */
                ++stats.read_fwd;

                auto [ec, rv] = co_await read_remote(nodehash_lookup[id], key);
                if (ec == boost::system::errc::success) {
                    co_return rv;
                }
            }

            ++it;
        }
        assert(0);
        co_return "";
    }

    /* stream key range (i-j] to peer */
    boost::cobalt::task<size_t> stream_to_remote(const std::string& replica,
                                                 const hash i,
                                                 const hash j) const {

        auto cnt = 0;
        try {

            auto it = db.lower_bound(i);
            while (it != db.end() && it->first < j) {
                co_await write_remote(replica, it->second.first,
                                      it->second.second);
                ++it;
                ++cnt;
            }

            std::cerr << "stream_to_remote(): " << replica
                      << " - records = " << cnt << std::endl;

        } catch (const std::exception& e) {
            std::cerr << "Connection error: " << e.what() << std::endl;
            assert(0);
        }

        co_return cnt;
    }

    boost::cobalt::task<void> write_remote(const std::string& peer_addr,
                                           const std::string& key,
                                           const std::string& value) const {

        // std::cout << self << ":" << "write_remote() invoked!" <<
        // std::endl;

        auto io = co_await boost::asio::this_coro::executor;

        const auto p = peer_addr.find(":");
        const auto addr = peer_addr.substr(0, p);
        const auto port = peer_addr.substr(p + 1);

        boost::asio::ip::tcp::resolver resolver(io);
        boost::asio::ip::tcp::socket socket(io);
        auto ep = resolver.resolve(addr, port);

        try {

            boost::asio::async_connect(
                socket, ep,
                [&socket](const boost::system::error_code& error,
                          const boost::asio::ip::tcp::endpoint&) {});

            // std::cout << self << ":" << "write_remote(): writing to "
            //           << peer_addr << std::endl;

            const auto payload = "wf:" + key + "=" + value;
            co_await boost::asio::async_write(
                socket, boost::asio::buffer(payload, payload.size()),
                boost::cobalt::use_task);

            // std::cout << self << ":" << "write_remote(): waiting for
            // ack... "
            //           << std::endl;

            char rx[1024] = {};
            auto n = co_await socket.async_read_some(boost::asio::buffer(rx),
                                                     boost::cobalt::use_task);

            // std::cout << self << ":" << "write_remote(): ack received! "
            //           << std::endl;
        } catch (const std::exception& e) {
            std::cerr << "Connection error: " << e.what() << std::endl;
            assert(0);
        }

        volatile int dummy = 0;
    }

    boost::cobalt::task<void> write_local(const std::string& key,
                                          const std::string& value) {

        /* append to commit log */

        /* update memtable */

        /* flush to sstable if required */
        if (db.size() >= 128) {
        }
    }

    boost::cobalt::task<void> write(const std::string& key,
                                    const std::string& value,
                                    bool coordinator = true) {

        auto key_hash =
            static_cast<uint64_t>(std::hash<std::string>{}(key)) % p.getRange();

        if (!coordinator) {
            /* force write */
            ++stats.write;
            db[key_hash] = {key, value};
            co_return;
        }

        LookupEntry target = {key_hash, 0, 0};
        auto it = lookup.lower_bound(target);
        auto copy = lookup;
        std::vector<uint64_t> ids;
        int rf = replication_factor;
        while (rf-- > 0) {
            if (it == lookup.end())
                it = lookup.begin();
            ids.push_back((*it)[2]);
            ++it;
        }

        /* walk the ring until we find a working node */
        for (auto id : ids) {

            if (id == this->id) {
                ++stats.write;
                db[key_hash] = {key, value};
            } else {
                ++stats.write_fwd;
                co_await write_remote(nodehash_lookup[id], key, value);
            }
        }

        co_return;

        /* not local - forward request */
    }

    boost::cobalt::task<void> heartbeat() {

        /* gossip during heartbeat */
        co_await gossip_tx();

        if (this->anti_entropy_req) {

            auto start = std::chrono::system_clock::now();
            size_t cnt = co_await anti_entropy();
            auto end = std::chrono::system_clock::now();
            auto elapsed =
                std::chrono::duration_cast<std::chrono::milliseconds>(end -
                                                                      start);
            std::cout << "anti_entropy duration: " << elapsed.count() << '\n';

            this->anti_entropy_req = false;
        }
    }

    template <typename V, typename IT> auto next_it(V& var, IT& it) -> auto {
        if (next(it) == var.end()) {
            return var.begin();
        } else {
            return next(it);
        }
    }

    template <typename V, typename IT> auto prev_it(V& var, IT& it) -> auto {
        if (it != var.begin()) {
            return prev(it);
        } else {
            return prev(var.end());
        }
    }

    boost::cobalt::task<size_t> anti_entropy() {

        const auto tokens = local_map.nodes[self].tokens;
        size_t cnt = 0;
        for (auto& token : tokens) {
            std::array<uint64_t, 3> target = {token};

            /* determine replicas */
            std::vector<std::string> replicas;
            auto it = lookup.lower_bound(target);
            auto rf = replication_factor - 1;
            while (rf-- > 0) {
                it = next_it(lookup, it);
                const auto [skip, skip2, id_hash] = *it;
                replicas.push_back(nodehash_lookup[id_hash]);
            }

            /* find range to stream */
            it = lookup.lower_bound(target);
            it = prev_it(lookup, it);
            auto [ptoken, skip, skip1] = *it;

            /* sync range is inclusive of i and exclusive of j*/
            auto i = ptoken + 1;
            auto j = token + 1;

            for (auto& replica : replicas) {
                if (i < j) {
                    cnt += co_await sync_range(replica, i, j);
                } else {
                    cnt += co_await sync_range(replica, 0, j);
                    cnt += co_await sync_range(
                        replica, i, Partitioner::instance().getRange());
                }
            }

            std::cout << "anti_entropy(): sync = " << cnt << std::endl;
        }

        co_return cnt;
    }

    boost::cobalt::task<std::map<hash, std::pair<key, value>>>
    stream_from_remote(const std::string& peer, const hash i, const hash j) {

        // std::cout << self << ":" << "stream_remote() invoked!" <<
        // std::endl;

        auto io = co_await boost::asio::this_coro::executor;

        boost::asio::ip::tcp::resolver resolver(io);
        boost::asio::ip::tcp::socket socket(io);

        auto p = peer.find(":");
        auto addr = peer.substr(0, p);
        auto port = peer.substr(p + 1);

        auto ep = resolver.resolve(addr, port);

        /* Connect */
        boost::asio::async_connect(
            socket, ep,
            [&socket](const boost::system::error_code& error,
                      const boost::asio::ip::tcp::endpoint&) {});

        std::string req = "s:" + std::to_string(i) + "-" + std::to_string(j);
        co_await boost::asio::async_write(
            socket, boost::asio::buffer(req.c_str(), req.size()),
            boost::cobalt::use_task);

        /* read results */
        char payload[1024] = {};
        auto n = co_await socket.async_read_some(boost::asio::buffer(payload),
                                                 boost::cobalt::use_task);

        co_return deserialize<std::map<hash, std::pair<key, value>>>(
            std::string(payload + 3, n - 3));
    }

    boost::cobalt::task<void> rx_process(boost::asio::ip::tcp::socket socket) {

        // std::cout << self << ":" << "rx_process() spawned!" << std::endl;

        ++outstanding;

        try {
            for (;;) {
                char data[1024] = {};
                std::size_t n = co_await socket.async_read_some(
                    boost::asio::buffer(data), boost::cobalt::use_task);

                std::string payload = std::string(data, n);
                auto p = payload.find(":");

                auto cmd = payload.substr(0, p);
                if (cmd == "r") {
                    /* read */
                    auto key = std::string(data + 2, n - 2);
                    auto value = co_await read(key);
                    auto resp = "ra:" + value;
                    co_await boost::asio::async_write(
                        socket, boost::asio::buffer(resp.c_str(), resp.size()),
                        boost::cobalt::use_task);

                } else if (cmd == "w" || cmd == "wf") {

                    /* write */

                    auto kv =
                        std::string(data + cmd.size() + 1, n - cmd.size() - 1);
                    auto p = kv.find("=");
                    auto k = kv.substr(0, p);
                    auto v = kv.substr(p + 1);

                    co_await write(k, v, cmd == "w");

                    std::string resp;
                    if (cmd == "wf")
                        resp = std::string("wfa:");
                    else
                        resp = std::string("wa:");

                    co_await boost::asio::async_write(
                        socket, boost::asio::buffer(resp.c_str(), resp.size()),
                        boost::cobalt::use_task);
                } else if (cmd == "g") {
                    /* gossip */
                    auto gossip = payload.substr(p + 1);
                    gossip_rx(gossip);
                    auto resp = "ga:" + gossip;
                    co_await boost::asio::async_write(
                        socket, boost::asio::buffer(resp.c_str(), resp.size()),
                        boost::cobalt::use_task);
                } else if (cmd == "s") {
                    auto s = payload.substr(p + 1);
                    auto p = s.find("-");
                    auto i = stoll(s.substr(0, p)), j = stoll(s.substr(p + 1));

                    auto resp = "sa:" + serialize(stream(i, j));
                    co_await boost::asio::async_write(
                        socket, boost::asio::buffer(resp.c_str(), resp.size()),
                        boost::cobalt::use_task);
                } else if (cmd == "st") {
                    auto resp = "sta:" + get_status();
                    co_await boost::asio::async_write(
                        socket, boost::asio::buffer(resp.c_str(), resp.size()),
                        boost::cobalt::use_task);
                } else if (cmd == "ring") {
                    const auto& [lookup, hash_lookup] = get_ring_view();
                    std::vector<std::pair<const std::string, const uint64_t>>
                        bars;
                    for (auto it = lookup.begin(); it != lookup.end(); ++it) {
                        const auto [token, timestamp, id_hash] = *it;
                        auto p = it == lookup.begin() ? prev(lookup.end())
                                                      : prev(it);
                        auto [ptoken, skip, skip1] = *p;
                        auto range =
                            it != lookup.begin()
                                ? (token - ptoken)
                                : (token + Partitioner::instance().getRange() -
                                   ptoken);
                        auto s = hash_lookup.at(id_hash);
                        bars.push_back(make_pair(s, range));

                        /*
                         * format to:
                         * var data = {a: 9, b: 20, c:30, d:8, e:12, f:3,
                         * g:7, h:14}
                         */
                    }

                    std::unordered_map<std::string, int> cnt;

                    std::string replacement = "{";
                    int k = 0;
                    for (auto& bar : bars) {
                        auto p = bar.first.find(":");
                        auto name = bar.first.substr(p + 1);
                        /* <index>-<node>-<instance>*/
                        replacement += "\"" + std::to_string(k++) + "-" + name +
                                       "-" + std::to_string(cnt[name]++) +
                                       "\"" + ":" + std::to_string(bar.second) +
                                       ",";
                    }
                    replacement += "}";

                    // {
                    //     string filename("web/ring_fmt.html");

                    //     // Open the file for reading
                    //     std::ifstream inFile(filename);
                    //     assert(inFile);

                    //     // Read the file content into a string
                    //     std::string fileContent(
                    //         (std::istreambuf_iterator<char>(inFile)),
                    //         std::istreambuf_iterator<char>());
                    //     inFile.close();

                    //     // Find and replace the target line with the
                    //     replacement string targetLine =
                    //     "LINE_TO_REPLACE";
                    //     // string replacement = "blah";
                    //     if (fileContent.find(targetLine) !=
                    //     std::string::npos) {
                    //         boost::algorithm::replace_all(fileContent,
                    //         targetLine,
                    //                                       replacement);
                    //     } else {
                    //         assert(0);
                    //     }

                    //     // Open the file for writing and overwrite the
                    //     content std::ofstream outFile("web/ring.html");
                    //     assert(outFile);

                    //     outFile << fileContent;
                    //     outFile.close();
                    // }

                    auto resp = "ring_ack:" + replacement;
                    co_await boost::asio::async_write(
                        socket, boost::asio::buffer(resp.c_str(), resp.size()),
                        boost::cobalt::use_task);
                } else if (cmd == "aa") {

                    auto resp =
                        std::string("aa_ack:"); //  + std::to_string(cnt);
                    co_await boost::asio::async_write(
                        socket, boost::asio::buffer(resp.c_str(), resp.size()),
                        boost::cobalt::use_task);

                    this->anti_entropy_req = true;

                } else if (cmd == "range_hash") {

                    auto range =
                        std::string(data + cmd.size() + 1, n - cmd.size() - 1);
                    auto p = range.find("-");
                    auto i = range.substr(0, p);
                    auto j = range.substr(p + 1);
                    auto hash = get_range_hash(stoll(i), stoll(j));

                    auto resp =
                        std::string("range_hash_ack:") + std::to_string(hash);
                    co_await boost::asio::async_write(
                        socket, boost::asio::buffer(resp.c_str(), resp.size()),
                        boost::cobalt::use_task);
                }
            }
        } catch (const std::exception& e) {

            // std::string info = boost::diagnostic_information(e);
            // std::cout << info << std::endl;

            // std::cerr << "Connection error: " << e.what() << std::endl;
        }

        // std::cout << self << ":" << "rx_process() end!" << std::endl;

        --outstanding;

        co_return;
    }

    boost::cobalt::task<bool> Cancelled() {
        boost::system::error_code ec;
        co_await cancel->async_wait(
            boost::asio::redirect_error(boost::cobalt::use_task, ec));

        co_return true;
    }

    boost::cobalt::task<void> node_listener() {

        ++outstanding;

        auto executor = co_await boost::cobalt::this_coro::executor;

        auto p = get_addr().find(":");
        auto addr = get_addr().substr(0, p);
        auto port = get_addr().substr(p + 1);

        cancel = std::shared_ptr<boost::asio::steady_timer>(
            new boost::asio::steady_timer{
                executor, std::chrono::steady_clock::duration::max()});

        boost::asio::ip::tcp::acceptor acceptor(
            executor, {boost::asio::ip::tcp::v4(), stoi(port)});
        bool running = true;
        while (running) {

            assert(outstanding < 100);

// auto socket =
#if 0
            auto socket =
                co_await acceptor.async_accept(boost::cobalt::use_task);
            std::cout << "New connection accepted from: "
                      << socket.remote_endpoint() << "\n";

            boost::cobalt::spawn(executor, rx_process(std::move(socket)),
                                 boost::asio::detached);
#else
            boost::variant2::variant<boost::asio::ip::tcp::socket, bool> nx =
                co_await boost::cobalt::race(
                    acceptor.async_accept(boost::cobalt::use_task),
                    Cancelled());
            switch (nx.index()) {
            case 0: {

                auto& socket = boost::variant2::get<0>(nx);
                boost::cobalt::spawn(executor, rx_process(std::move(socket)),
                                     boost::asio::detached);
                break;
            }
            case 1:
                running = false;
                // std::cout << self << ":" << "node_listener() -
                // cancelled!"
                //           << std::endl;
                --outstanding;
                co_return;
            }
#endif
        }
    }

    const NodeMap& peers() const { return local_map; }
    const Lookup& get_lookup() const { return lookup; }
    const node_id get_id() const { return id; }
    const node_addr get_addr() const { return self; }
    const Stats get_stats() const { return stats; }
    const std::string get_status() const {
        return status_to_string(local_map.nodes.at(self).status);
    }

    constexpr std::string
    status_to_string(const NodeMap::Node::Status& status) const {
        switch (status) {
        case NodeMap::Node::Status::Joining:
            return "Joining";
        case NodeMap::Node::Status::Live:
            return "Live";
        case NodeMap::Node::Status::Down:
            return "Down";
        default:
            assert(0);
        }
    }

    std::tuple<const Lookup&, const HashLookup&> get_ring_view() const {
        return std::make_tuple(std::ref(lookup), std::ref(nodehash_lookup));
    }

    boost::cobalt::task<std::string> async_cmd(const std::string& peer_addr,
                                               const std::string& cmd) {

        auto io = co_await boost::asio::this_coro::executor;

        const auto p = peer_addr.find(":");
        const auto addr = peer_addr.substr(0, p);
        const auto port = peer_addr.substr(p + 1);

        boost::asio::ip::tcp::resolver resolver(io);
        boost::asio::ip::tcp::socket socket(io);
        auto ep = resolver.resolve(addr, port);

        try {

            boost::asio::async_connect(
                socket, ep,
                [&socket](const boost::system::error_code& error,
                          const boost::asio::ip::tcp::endpoint&) {});

            const auto tx = cmd;
            co_await boost::asio::async_write(
                socket, boost::asio::buffer(tx, tx.size()),
                boost::cobalt::use_task);

            char rx[1024] = {};
            auto n = co_await socket.async_read_some(boost::asio::buffer(rx),
                                                     boost::cobalt::use_task);
            co_return std::string(rx);

        } catch (const std::exception& e) {
            std::cerr << "Connection error: " << e.what() << std::endl;
        }

        assert(0);
    }

    boost::cobalt::task<hash>
    get_range_hash_remote(const std::string& peer_addr, const hash i,
                          const hash j) {

        const auto cmd =
            "range_hash:" + std::to_string(i) + "-" + std::to_string(j);

        auto hash_str = co_await async_cmd(peer_addr, cmd);

        hash_str = hash_str.substr(sizeof("range_hash_ack"));
        auto hash = std::stoll(hash_str);

        co_return hash;
    }

    boost::cobalt::task<size_t> erase_remote(const std::string& peer_addr,
                                             const hash i, const hash j) {

        auto io = co_await boost::asio::this_coro::executor;

        const auto p = peer_addr.find(":");
        const auto addr = peer_addr.substr(0, p);
        const auto port = peer_addr.substr(p + 1);

        boost::asio::ip::tcp::resolver resolver(io);
        boost::asio::ip::tcp::socket socket(io);
        auto ep = resolver.resolve(addr, port);

        try {

            boost::asio::async_connect(
                socket, ep,
                [&socket](const boost::system::error_code& error,
                          const boost::asio::ip::tcp::endpoint&) {});

            /* TODO */
            // static_assert(0);

            const auto tx =
                "erase_range:" + std::to_string(i) + "-" + std::to_string(j);
            co_await boost::asio::async_write(
                socket, boost::asio::buffer(tx, tx.size()),
                boost::cobalt::use_task);

            char rx[1024] = {};
            auto n = co_await socket.async_read_some(boost::asio::buffer(rx),
                                                     boost::cobalt::use_task);

        } catch (const std::exception& e) {
            std::cerr << "Connection error: " << e.what() << std::endl;
            assert(0);
        }

        /* TODO: records erased? */
        co_return 0;
    }

    // Recursive compile-time hash function for keyspace
    hash get_range_hash(hash i, hash j) {

        auto ii = db.lower_bound(i);
        auto jj = db.lower_bound(j);

        if (ii == db.end() || ii->first >= j) {
            /* nothing in this range */
            return 0;
        }

        if (ii == jj) {
            /* supposedly ii is inclusive and jj is exclusive. If they point to
             * the same thing then range is invalid. */
            return 0;
        }

        if (next(ii) == jj) {
            /* base case - only one entry */
            return ii->first;
        }

        hash m = (i + j) / 2;
        auto l = get_range_hash(i, m);
        auto r = get_range_hash(m, j);

        return static_cast<uint64_t>(std::hash<hash>{}(l + r));
    }

    boost::cobalt::task<size_t> sync_range(const std::string& replica, hash i,
                                           hash j) {
        auto l = get_range_hash(i, j);
        auto r = co_await get_range_hash_remote(replica, i, j);

        size_t cnt = 0;

        if (l != r) {
            if (l == 0) {
                cnt += co_await erase_remote(replica, i, j);
            } else if (r == 0) {
                cnt += co_await stream_to_remote(replica, i, j);
            } else {
                int m = (i + j) / 2;
                cnt += co_await sync_range(replica, i, m);
                cnt += co_await sync_range(replica, m, j);
            }
        }

        co_return cnt;
    }
};