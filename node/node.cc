#include "node/node.hh"

auto Node::update_lookup() -> void {

    lookup.clear();
    nodehash_lookup.clear();
    for (auto& [node_addr, node] : local_map.nodes) {

        auto id = static_cast<uint64_t>(std::hash<std::string>{}(node_addr));

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

            nodehash_lookup[id] = node_addr;
        }
    }
}

/* retire local tokens owned by others  */
auto Node::retire_token() -> void {

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
                my_node.timestamp = current_time_ms();

                /* update to my node will be communicated in next round of
                 * gossip */
            } else {
                ++my_token;
            }
        }
    }
};

Node::Node(node_addr self, node_addr seed, int vnode, int rf)
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

void Node::gossip_rx(std::string& gossip) {

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

std::map<hash, std::pair<key, value>> Node::stream(hash i, hash j) {

    std::map<hash, std::pair<key, value>> rv;
    for (auto it = db.lower_bound(i); it != db.end() && it->first < j; ++it) {
        rv[it->first] = it->second;
    }

    return rv;
}

boost::cobalt::task<void> Node::gossip_tx() {

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

boost::cobalt::task<void> Node::write(const std::string& key,
                                      const std::string& value,
                                      bool coordinator) {

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

boost::cobalt::task<std::map<hash, std::pair<key, value>>>
Node::stream_from_remote(const std::string& peer, const hash i, const hash j) {

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

boost::cobalt::task<std::pair<boost::system::error_code, std::string>>
Node::read_remote(std::string& peer, std::string& key) {

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

boost::cobalt::task<std::string> Node::read(std::string& key) {

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
boost::cobalt::task<size_t> Node::stream_to_remote(const std::string& replica,
                                                   const hash i,
                                                   const hash j) const {

    auto cnt = 0;
    try {

        auto it = db.lower_bound(i);
        while (it != db.end() && it->first < j) {
            co_await write_remote(replica, it->second.first, it->second.second);
            ++it;
            ++cnt;
        }

        std::cerr << "stream_to_remote(): " << replica << " - records = " << cnt
                  << std::endl;

    } catch (const std::exception& e) {
        std::cerr << "Connection error: " << e.what() << std::endl;
        assert(0);
    }

    co_return cnt;
}

boost::cobalt::task<void> Node::write_remote(const std::string& peer_addr,
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

boost::cobalt::task<size_t> Node::anti_entropy() {

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
                cnt += co_await sync_range(replica, i,
                                           Partitioner::instance().getRange());
            }
        }

        std::cout << "anti_entropy(): sync = " << cnt << std::endl;
    }

    co_return cnt;
}

boost::cobalt::task<void> Node::connection_dispatcher() {

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
                acceptor.async_accept(boost::cobalt::use_task), Cancelled());
        switch (nx.index()) {
        case 0: {

            auto& socket = boost::variant2::get<0>(nx);
            boost::cobalt::spawn(executor, rx_process(std::move(socket)),
                                 boost::asio::detached);
            break;
        }
        case 1:
            running = false;
            // std::cout << self << ":" << "connection_dispatcher() -
            // cancelled!"
            //           << std::endl;
            --outstanding;
            co_return;
        }
#endif
    }
}
