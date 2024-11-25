

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
    bool ready = false;

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

            ready = true;
            for (auto& n : nodes) {
                if (n.second->get_status() != "Live") {
                    ready = false;
                    break;
                }
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

// boost::cobalt::task<void> Cancelled(boost::asio::steady_timer&
// cancelTimer) {
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
        if (cmd == "add_node") {

            auto v = payload.substr(p + 1);
            p = v.find(",");
            auto addr = v.substr(0, p);
            auto seed = v.substr(p + 1);

            cluster->pending_add.push({addr, seed});

            auto resp = "add_node_ack:" + addr;
            co_await boost::asio::async_write(
                socket, boost::asio::buffer(resp.c_str(), resp.size()),
                boost::cobalt::use_task);
        } else if (cmd == "remove_node") {

            auto to_remove = payload.substr(p + 1);

            // auto& target = (*nodes.begin()).second->cancel_signal;
            // target.emit(boost::asio::cancellation_type::all);

            /* TODO: delete heartbeat */
            /* TODO: wait for all current connnections to drain */
        } else if (cmd == "ready") {
            std::string resp;
            if (cluster->ready) {
                resp = "ready_ack:ready";
            } else {
                resp = "ready_ack:not_ready";
            }
            co_await boost::asio::async_write(
                socket, boost::asio::buffer(resp.c_str(), resp.size()),
                boost::cobalt::use_task);
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

        /* distributed system heartbeat */
        boost::cobalt::spawn(io_context, cluster->heartbeat(),
                             boost::asio::detached);

        /* distributed system control plane */
        boost::cobalt::spawn(io_context, system_listener(cluster),
                             boost::asio::detached);

        io_context.run();
    });

    /* test code here */

    usleep(500 * 1000);

    boost::asio::io_context io(1);

    boost::cobalt::spawn(
        io,
        []() -> boost::cobalt::task<void> {
            auto io = co_await boost::cobalt::this_coro::executor;
            boost::asio::ip::tcp::resolver resolver(io);
            boost::asio::ip::tcp::socket socket(io);
            auto ep = resolver.resolve("127.0.0.1", "5001");

            boost::system::error_code err_code;
            boost::asio::async_connect(
                socket, ep,
                [&socket, &err_code](const boost::system::error_code& error,
                                     const boost::asio::ip::tcp::endpoint&) {
                    err_code = error;
                    // std::cout << "error = " << error << std::endl;
                });

            try {

                std::string tx_payload = "ready:";
                co_await boost::asio::async_write(
                    socket, boost::asio::buffer(tx_payload, tx_payload.size()),
                    boost::cobalt::use_task);

                /* read results */

                // std::cout << self << ":" << "gossip_tx() - #4" << std::endl;
                char rx_payload[1024] = {};
                std::size_t n = co_await socket.async_read_some(
                    boost::asio::buffer(rx_payload), boost::cobalt::use_task);

                /* TODO: account for peer death */
                volatile int dummy = 0;

            } catch (std::exception& e) {
                // std::cout << self << ":" << "heartbeat() - failed to
                // connect!"
                //           << std::endl;
            }

            std::string tx_payload = "add_node:127.0.0.1:6000,127.0.0.1:5555";
            co_await boost::asio::async_write(
                socket, boost::asio::buffer(tx_payload, tx_payload.size()),
                boost::cobalt::use_task);

            char rx_payload[1024] = {};
            std::size_t n = co_await socket.async_read_some(
                boost::asio::buffer(rx_payload), boost::cobalt::use_task);

            /* TODO: wait for ready */
            usleep(500 * 1000);

            tx_payload = "remove_node:127.0.0.1:6000";
            co_await boost::asio::async_write(
                socket, boost::asio::buffer(tx_payload, tx_payload.size()),
                boost::cobalt::use_task);

            rx_payload[1024] = {};
            n = co_await socket.async_read_some(boost::asio::buffer(rx_payload),
                                                boost::cobalt::use_task);

            while (true) {
                usleep(500 * 1000);
            }
        }(),
        boost::asio::detached);

    io.run();

    /* wait for cluster ready */

    db_instance.join();

    cout << "### " << endl;
}