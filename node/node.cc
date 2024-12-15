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

template <typename T> std::string Node::serialize(T&& data) {
    std::ostringstream oss;
    boost::archive::text_oarchive oa(oss);
    oa << data;
    return oss.str();
}

template <typename T, typename StringType>
T Node::deserialize(StringType&& data) {
    T rv;
    std::istringstream iss(data);
    boost::archive::text_iarchive ia(iss);
    ia >> rv;
    return std::move(rv);
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
