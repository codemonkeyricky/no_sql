

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

using namespace std;

struct Cluster {

    queue<array<string, 2>> pending_add;
    queue<string> pending_remove;
    map<string, unique_ptr<Node>> nodes;

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
                    std::unique_ptr<Node>(new Node(server, seed, 1));

                boost::cobalt::spawn(io, n->node_listener(),
                                     boost::asio::detached);
            }

            while (pending_remove.size()) {
            }

            /* heartbeat every second */
            timer.expires_at(std::chrono::steady_clock::now() +
                             std::chrono::seconds(1));
        }
    }

    // unique_ptr<Node>& operator[](string& name) { return nodes[name]; }
};

/* shared_ptr to cluster */
static shared_ptr<Cluster> cluster;

// boost::cobalt::task<void> Cancelled(boost::asio::steady_timer& cancelTimer) {
//     boost::system::error_code ec;
//     co_await cancelTimer.async_wait(redirect_error(use_awaitable, ec));
// }

static int port = 6000;
boost::cobalt::task<void> cp_process(shared_ptr<Cluster> cluster,
                                     boost::asio::ip::tcp::socket socket) {
    auto executor = co_await boost::cobalt::this_coro::executor;
    for (;;) {

        char data[1024] = {};
        std::size_t n = co_await socket.async_read_some(
            boost::asio::buffer(data), boost::cobalt::use_task);

        string payload = string(data, n);
        auto p = payload.find(":");
        auto cmd = payload.substr(0, p);
        if (cmd == "an") {
            /* TODO: parse address */
            auto seed = payload.substr(p + 1);

            cluster->pending_add.push(
                {"127.0.0.1:" + to_string(port++), "127.0.0.1:5555"});

            auto resp = "ana:" + seed;
            co_await boost::asio::async_write(
                socket, boost::asio::buffer(resp.c_str(), resp.size()),
                boost::cobalt::use_task);
        } else if (cmd == "rn") {
            auto seed = payload.substr(p + 1);
            // auto& target = (*nodes.begin()).second->cancel_signal;
            // target.emit(boost::asio::cancellation_type::all);

            /* TODO: delete heartbeat */
            /* TODO: wait for all current connnections to drain */
        }
    }
    co_return;
}

boost::cobalt::task<void> system_listener(shared_ptr<Cluster> cluster) {
    auto executor = co_await boost::cobalt::this_coro::executor;

    boost::asio::ip::tcp::acceptor acceptor(executor,
                                            {boost::asio::ip::tcp::v4(), 5001});
    for (;;) {
        auto socket = co_await acceptor.async_accept(boost::cobalt::use_task);
        boost::cobalt::spawn(executor, cp_process(cluster, std::move(socket)),
                             boost::asio::detached);
    }

    co_return;
}

int main() {
    constexpr int NODES = 2;

    thread db_instance([] {
        boost::asio::io_context io_context(1);
        boost::asio::signal_set signals(io_context, SIGINT, SIGTERM);
        signals.async_wait([&](auto, auto) { io_context.stop(); });

        cluster = shared_ptr<Cluster>(new Cluster());

        string addr = "127.0.0.1";
        int port = 5555;
        string seed;
        for (auto i = 0; i < NODES; ++i) {

            string addr_port = addr + ":" + to_string(port++);
            auto& n = cluster->nodes[addr_port] =
                std::unique_ptr<Node>(new Node(addr_port, seed, 1));
            if (seed == "") {
                seed = addr_port;
            }
            /* node control plane */
            boost::cobalt::spawn(io_context, n->node_listener(),
                                 boost::asio::detached);
        }

        // /* distributed system heartbeat */
        boost::cobalt::spawn(io_context, cluster->heartbeat(),
                             boost::asio::detached);

        // /* distributed system control plane */
        boost::cobalt::spawn(io_context, system_listener(cluster),
                             boost::asio::detached);

        io_context.run();
    });

    /* test code here */

    /* wait for cluster ready */

    db_instance.join();

    cout << "### " << endl;
}