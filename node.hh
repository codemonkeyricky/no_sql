
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
#include <unordered_map>
#include <vector>

#include <boost/asio/co_spawn.hpp>
#include <boost/asio/connect.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/signal_set.hpp>
#include <boost/asio/write.hpp>

#include <boost/archive/text_iarchive.hpp>
#include <boost/archive/text_oarchive.hpp>
#include <boost/serialization/map.hpp>
#include <boost/serialization/set.hpp>
#include <boost/serialization/vector.hpp>

using key = std::string;
using value = std::string;
using hash = uint64_t;
using node_addr = std::string;
using node_id = uint64_t;
using Tokens = std::set<hash>;
using LookupEntry = std::array<uint64_t, 3>;
using Lookup = std::set<LookupEntry>; ///< token / timestamp / node_id

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
    Partitioner() {}
    ~Partitioner() {}

    static Partitioner& instance() {
        static Partitioner p;
        return p;
    }

    uint64_t getToken() const { return rand() % range; };

    uint64_t getRange() const { return range; }
};

class Node {

  public:
    struct Stats {
        int read;
        int write;
        int read_fwd;
        int write_fwd;
        int gossip_rx;
        int gossip_tx;
    };

  private:
    Partitioner& p = Partitioner::instance();
    Time& t = Time::instance();

    NodeMap local_map;
    Lookup lookup;
    std::map<hash, std::pair<key, value>> db;
    node_id id;
    Stats stats;
    std::unordered_map<uint64_t, std::string> nodehash_lookup;
    int replication_factor; /* replication factor */
    node_addr self;

    /* update lookup table after gossip completes */
    auto update_lookup() -> void {

        lookup.clear();
        for (auto& [node_addr, node] : local_map.nodes) {

            auto id =
                static_cast<uint64_t>(std::hash<std::string>{}(node_addr));

            if (node.status == NodeMap::Node::Live) {
                auto timestamp = node.timestamp;
                for (auto token : node.tokens) {
                    std::array<uint64_t, 3> target = {token};
                    auto it = lookup.lower_bound(target);
                    target = {token, timestamp, id};

                    if ((*it)[0] == target[0]) {
                        /* already exists - conflict. pick higher timestamp */
                        if ((*it)[1] < timestamp) {
                            lookup.erase(it);
                            lookup.insert(target);
                        } else {
                            /* already more recent - do nothing */
                        }
                    } else {
                        lookup.insert(target);
                    }
                }
            }
        }
    }
    /* retire local tokens owned by others  */
    auto retire_token() {

        if (local_map.nodes[self].status == NodeMap::Node::Live) {
            /* adjust local map if someone else took over my token recently */
            auto& my_node = local_map.nodes[self];
            for (auto my_token = my_node.tokens.begin();
                 my_token != my_node.tokens.end();) {
                std::array<uint64_t, 3> target = {*my_token};
                auto it = lookup.lower_bound(target);
                /* must exists */
                assert((*it)[0] == *my_token);
                if ((*it)[1] > my_node.timestamp) {
                    /* someone else owns this now - I no longer own this token
                     */
                    my_token = my_node.tokens.erase(my_token);
                    my_node.timestamp = t.get();

                    /* update to my node will be communicated in next round of
                     * gossip */
                } else {
                    ++my_token;
                }
            }
        }
    };

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

    ~Node() {}

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

    void gossip(std::string& gossip) {

        auto remote_map = deserialize<NodeMap>(gossip);

        local_map = remote_map = remote_map + local_map;

        /* update local lookup based on updated local map */
        update_lookup();

        /* communicated retired token in next gossip round */
        retire_token();

        gossip = serialize(local_map);

        /* received gossip */
        ++stats.gossip_rx;
    }

    boost::asio::awaitable<std::string> read_remote(std::string& peer,
                                                    std::string& key) {

        auto io = co_await boost::asio::this_coro::executor;

        boost::asio::ip::tcp::resolver resolver(io);
        boost::asio::ip::tcp::socket socket(io);

        auto p = peer.find(":");
        auto addr = peer.substr(0, p);
        auto port = peer.substr(p + 1);

        auto ep = resolver.resolve(addr, port);

        /* Connect */
        async_connect(socket, ep,
                      [&socket](const boost::system::error_code& error,
                                const boost::asio::ip::tcp::endpoint&) {});

        std::string req = "r:" + key;
        co_await async_write(socket,
                             boost::asio::buffer(req.c_str(), req.size()),
                             boost::asio::use_awaitable);

        /* read results */
        char payload[1024] = {};
        std::size_t n = co_await socket.async_read_some(
            boost::asio::buffer(payload), boost::asio::use_awaitable);

        std::string rv(payload + 3, n - 3);

        co_return rv;
    }

    boost::asio::awaitable<std::string> read(std::string& key) {

        auto key_hash =
            static_cast<uint64_t>(std::hash<std::string>{}(key)) % p.getRange();

        LookupEntry target = {key_hash, 0, 0};
        auto it = lookup.lower_bound(target);

        /* walk the ring until we find a working node */
        while (true) {

            /* wrap around if needed */
            if (it == lookup.end())
                it = lookup.begin();

            /* parse */
            auto [token, timestamp, id] = *it;
            if (id == this->id) {
                /* local */
                ++stats.read;
                if (db.count(key_hash)) {
                    /* exists */
                    co_return db[key_hash].second;
                }
                co_return "";
            } else {
                /* forward to remote if alive */
                ++stats.read_fwd;

                co_return co_await read_remote(nodehash_lookup[id], key);
            }

            ++it;
        }
        assert(0);
        co_return "";
    }

    boost::asio::awaitable<void> write_remote(const std::string& peer,
                                              const std::string& key,
                                              const std::string& value) {
        auto io = co_await boost::asio::this_coro::executor;

        const auto& peer_addr = nodehash_lookup[id];
        const auto p = peer_addr.find(":");
        const auto addr = peer_addr.substr(0, p);
        const auto port = peer_addr.substr(p + 1);

        boost::asio::ip::tcp::resolver resolver(io);
        boost::asio::ip::tcp::socket socket(io);
        auto ep = resolver.resolve(addr, port);

        async_connect(socket, ep,
                      [&socket](const boost::system::error_code& error,
                                const boost::asio::ip::tcp::endpoint&) {});

        const auto payload = "w:" + key + "=" + value;
        co_await async_write(socket,
                             boost::asio::buffer(payload, payload.size()),
                             boost::asio::use_awaitable);
    }

    boost::asio::awaitable<void> write(std::string& key, std::string& value,
                                       bool coordinator = true) {

        auto key_hash =
            static_cast<uint64_t>(std::hash<std::string>{}(key)) % p.getRange();

        if (!coordinator) {
            /* not coordinator mode - commit the write directly */
            ++stats.write;
            db[key_hash] = {key, value};
            co_return;
        }

        LookupEntry target = {key_hash, 0, 0};
        auto it = lookup.lower_bound(target);

        /* walk the ring until we find a working node */
        int rf = replication_factor;
        while (true) {

            /* wrap around if needed */
            if (it == lookup.end())
                it = lookup.begin();

            /* parse */
            auto [token, timestamp, id] = *it;
            if (id == this->id) {
                /* local */
                ++stats.write;
                db[key_hash] = {key, value};
            } else {
                /* forward to remote if alive */

                write_remote(nodehash_lookup[id], key, value);

                ++stats.write_fwd;
            }

            if (--rf <= 0)
                co_return;

            ++it;
        }

        assert(0);

        /* not local - forward request */
    }

    boost::asio::awaitable<void> heartbeat() {

        /* collect all peers */
        std::vector<node_addr> peers;
        for (auto& peer : local_map.nodes) {
            /* exclude ourselves */
            if (peer.first != self) {
                peers.push_back(peer.first);
            }
        }

        /* pick 3 peers at random to gossip */
        int k = 1;

        auto io = co_await boost::asio::this_coro::executor;
        while (k && peers.size()) {
            auto kk = peers[rand() % peers.size()];

            auto peer_addr = kk;
            auto p = peer_addr.find(":");
            auto addr = peer_addr.substr(0, p);
            auto port = peer_addr.substr(p + 1);

            boost::asio::ip::tcp::resolver resolver(io);
            boost::asio::ip::tcp::socket socket(io);
            auto ep = resolver.resolve(addr, port);

            async_connect(socket, ep,
                          [&socket](const boost::system::error_code& error,
                                    const boost::asio::ip::tcp::endpoint&) {});

            auto payload = "g:" + serialize(local_map);
            co_await async_write(socket,
                                 boost::asio::buffer(payload, payload.size()),
                                 boost::asio::use_awaitable);

            /* read results */

            char rx_payload[1024] = {};
            std::size_t n = co_await socket.async_read_some(
                boost::asio::buffer(rx_payload), boost::asio::use_awaitable);

            auto serialized_data = std::string(rx_payload + 3, n - 3);
            local_map = deserialize<NodeMap>(serialized_data);

            /* TODO: account for peer death */

            // if (gd.is_alive(kk)) {
            //     static_cast<Node*>(gd.lookup(kk))->gossip(local_map);
            //     peers.erase(peers.begin() + kk);
            //     --k;
            //     ++stats.gossip_tx;
            // } else {
            //     /* remove dead peer */
            //     peers.erase(peers.begin() + kk);
            // }
            --k;
        }

        if (local_map.nodes[self].status == NodeMap::Node::Joining) {

            auto& my_node = local_map.nodes[self];

            /* take over token, token-1, token-2, .... ptoken +1*/

            if (lookup.size()) {

                for (auto& my_token : my_node.tokens) {

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

                    auto remote_db = co_await stream_remote(nodehash_lookup[id],
                                                            ptoken + 1, token);

                    /* insert into local db */
                    db.insert(remote_db.begin(), remote_db.end());
                }
            }

            local_map.nodes[self].status = NodeMap::Node::Live;
        }
    }

    boost::asio::awaitable<std::map<hash, std::pair<key, value>>>
    stream_remote(const std::string& peer, const hash i, const hash j) {

        auto io = co_await boost::asio::this_coro::executor;

        boost::asio::ip::tcp::resolver resolver(io);
        boost::asio::ip::tcp::socket socket(io);

        auto p = peer.find(":");
        auto addr = peer.substr(0, p);
        auto port = peer.substr(p + 1);

        auto ep = resolver.resolve(addr, port);

        /* Connect */
        async_connect(socket, ep,
                      [&socket](const boost::system::error_code& error,
                                const boost::asio::ip::tcp::endpoint&) {});

        std::string req = "s:" + std::to_string(i) + "-" + std::to_string(j);
        co_await async_write(socket,
                             boost::asio::buffer(req.c_str(), req.size()),
                             boost::asio::use_awaitable);

        /* read results */
        char payload[1024] = {};
        std::size_t n = co_await socket.async_read_some(
            boost::asio::buffer(payload), boost::asio::use_awaitable);
        std::string sa(payload + 3, n - 3);

        co_return std::move(
            deserialize<std::map<hash, std::pair<key, value>>>(sa));
    }

    const NodeMap& peers() const { return local_map; }
    const Lookup& get_lookup() const { return lookup; }
    const node_id get_id() const { return id; }
    const node_addr get_addr() const { return self; }
    const Stats get_stats() const { return stats; }
};