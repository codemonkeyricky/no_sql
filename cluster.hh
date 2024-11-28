

#include "node.hh"
#include <boost/asio/cancellation_signal.hpp>
#include <boost/asio/experimental/parallel_group.hpp>
#include <chrono>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <memory>
#include <queue>
#include <unordered_map>

#include <boost/algorithm/string.hpp>
#include <boost/asio.hpp>
#include <boost/cobalt.hpp>

#include <string>
#include <utility>

#include "node.hh"

struct Cluster {

    std::queue<std::array<std::string, 2>> pending_add;
    std::queue<std::string> to_be_removed;
    std::queue<std::string> can_be_removed;
    std::map<std::string, std::unique_ptr<Node>> nodes;
    bool ready = false;

    boost::cobalt::task<void> node_drain(std::string&& server) {
        nodes[server]->cancel->cancel();

        auto io = co_await boost::cobalt::this_coro::executor;
        boost::asio::steady_timer timer(io);

        /* wait until all connections drained */
        while (nodes[server]->outstanding) {
            timer.expires_at(std::chrono::steady_clock::now() +
                             std::chrono::milliseconds(100));
            co_await timer.async_wait(boost::cobalt::use_task);
        }

        can_be_removed.push(server);
    }

    boost::cobalt::task<void> heartbeat() {
        auto io = co_await boost::cobalt::this_coro::executor;
        boost::asio::steady_timer timer(io);

        for (;;) {

            for (auto& n : nodes) {
                co_await n.second->heartbeat();
            }

            while (pending_add.size()) {
                auto [server, seed] = pending_add.front();
                pending_add.pop();

                auto& n = nodes[server] =
                    std::unique_ptr<Node>(new Node(server, seed));

                boost::cobalt::spawn(io, n->node_listener(),
                                     boost::asio::detached);
            }

            while (to_be_removed.size()) {

                auto server = to_be_removed.front();
                to_be_removed.pop();

                boost::cobalt::spawn(io, node_drain(move(server)),
                                     boost::asio::detached);
            }

            while (can_be_removed.size()) {

                /* remove thread */
                nodes.erase(can_be_removed.front());
                can_be_removed.pop();
            }

            ready = true;
            for (auto& n : nodes) {
                if (n.second->get_status() != "Live") {
                    ready = false;
                    break;
                }
            }

            /* heartbeat every second */
            timer.expires_at(std::chrono::steady_clock::now() +
                             std::chrono::milliseconds(100));
            co_await timer.async_wait(boost::cobalt::use_task);
        }
    }
};
