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