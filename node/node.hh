
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

#include "node/bloom.hh"
#include "node/log.hh"
#include "node/sstable.hh"

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
    Node(node_addr self, node_addr seed = "", int vnode = 3, int rf = 3);

    Node(const Node&) = delete; /* cannot be copied */
    Node(Node&&) = delete;

    // ~Node() {}

    /* stream data: [i, j) */

    std::map<hash, std::pair<key, value>> stream(hash i, hash j);

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

    void gossip_rx(std::string& gossip);

    boost::cobalt::task<void> gossip_tx();

    boost::cobalt::task<std::pair<boost::system::error_code, std::string>>
    read_remote(std::string& peer, std::string& key);

    boost::cobalt::task<std::string> read(std::string& key);

    boost::cobalt::task<size_t> stream_to_remote(const std::string& replica,
                                                 const hash i,
                                                 const hash j) const;

    boost::cobalt::task<void> write_remote(const std::string& peer_addr,
                                           const std::string& key,
                                           const std::string& value) const;

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
                                    bool coordinator = true);

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
    stream_from_remote(const std::string& peer, const hash i, const hash j);

    boost::cobalt::task<void> rx_process(boost::asio::ip::tcp::socket socket);

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