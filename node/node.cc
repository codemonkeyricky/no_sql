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
