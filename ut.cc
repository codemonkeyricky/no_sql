

#include "node/node.hh"
#include <boost/asio/cancellation_signal.hpp>
#include <boost/asio/experimental/parallel_group.hpp>
#include <boost/system/detail/errc.hpp>
#include <boost/system/detail/error_code.hpp>
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

#include "cluster.hh"

using namespace std;

/* shared_ptr to cluster */
static shared_ptr<Cluster> cluster;

static int port = 6000;
boost::cobalt::task<void> cluster_process(shared_ptr<Cluster> cluster,
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

            std::cout << "cluster_process(): pending_add.push()" << std::endl;

            cluster->pending_add.push({addr, seed});
            cluster->ready = false;

            auto resp = "add_node_ack:" + addr;
            co_await boost::asio::async_write(
                socket, boost::asio::buffer(resp.c_str(), resp.size()),
                boost::cobalt::use_task);
        } else if (cmd == "remove_node") {

            auto to_remove = payload.substr(p + 1);

            cluster->to_be_removed.push(to_remove);
            cluster->ready = false;

            // auto& target = (*nodes.begin()).second->cancel_signal;
            // target.emit(boost::asio::cancellation_type::all);

            auto resp = "remove_node_ack:" + to_remove;

            co_await boost::asio::async_write(
                socket, boost::asio::buffer(resp.c_str(), resp.size()),
                boost::cobalt::use_task);

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
        boost::cobalt::spawn(executor,
                             cluster_process(cluster, std::move(socket)),
                             boost::asio::detached);
    }

    co_return;
}

int main() {
    constexpr int NODES = 5;

    thread cluster_instance([] {
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
                std::unique_ptr<Node>(new Node(addr_port, seed));
            if (seed == "") {
                seed = addr_port;
            }
            /* node control plane */
            boost::cobalt::spawn(io_context, n->connection_dispatcher(),
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
            auto async_connect = [](const string& addr, const string& port)
                -> boost::cobalt::task<
                    unique_ptr<boost::asio::ip::tcp::socket>> {
                auto io = co_await boost::cobalt::this_coro::executor;
                auto socket = unique_ptr<boost::asio::ip::tcp::socket>(
                    new boost::asio::ip::tcp::socket(io));
                boost::asio::ip::tcp::resolver resolver(io);
                auto ep = resolver.resolve(addr, port);

                boost::asio::async_connect(
                    *socket, ep,
                    [](const boost::system::error_code& error,
                       const boost::asio::ip::tcp::endpoint&) {});

                co_return move(socket);
            };

            auto add_node =
                [](unique_ptr<boost::asio::ip::tcp::socket>& socket,
                   const string& payload) -> boost::cobalt::task<void> {
                std::string tx_payload = "add_node:" + payload;
                co_await boost::asio::async_write(
                    *socket, boost::asio::buffer(tx_payload, tx_payload.size()),
                    boost::cobalt::use_task);

                char rx_payload[1024] = {};
                std::size_t n = co_await socket->async_read_some(
                    boost::asio::buffer(rx_payload), boost::cobalt::use_task);

                co_return;
            };

            auto cluster_ready =
                [](unique_ptr<boost::asio::ip::tcp::socket>& socket)
                -> boost::cobalt::task<bool> {
                std::string tx_payload = "ready:";
                co_await boost::asio::async_write(
                    *socket, boost::asio::buffer(tx_payload, tx_payload.size()),
                    boost::cobalt::use_task);

                char rx_payload[1024] = {};
                std::size_t n = co_await socket->async_read_some(
                    boost::asio::buffer(rx_payload), boost::cobalt::use_task);

                string rxs(rx_payload);
                co_return rxs == "ready_ack:ready";
            };

            auto wait_for_cluster_ready =
                [&cluster_ready](
                    unique_ptr<boost::asio::ip::tcp::socket>& socket)
                -> boost::cobalt::task<void> {
                auto io = co_await boost::cobalt::this_coro::executor;
                boost::asio::steady_timer timer(io);

                bool ready;
                while (true) {
                    auto ready = co_await cluster_ready(socket);
                    if (ready)
                        break;
                    timer.expires_at(std::chrono::steady_clock::now() +
                                     std::chrono::milliseconds(100));
                    co_await timer.async_wait(boost::cobalt::use_task);
                }
            };

            auto remove_node =
                [](unique_ptr<boost::asio::ip::tcp::socket>& socket,
                   const string& node)
                -> boost::cobalt::task<boost::system::error_code> {
                try {

                    string tx = "remove_node:" + node;
                    co_await boost::asio::async_write(
                        *socket, boost::asio::buffer(tx, tx.size()),
                        boost::cobalt::use_task);

                    char rx_payload[1024] = {};
                    auto n = co_await socket->async_read_some(
                        boost::asio::buffer(rx_payload),
                        boost::cobalt::use_task);
                } catch (boost::system::system_error const& e) {
                    co_return e.code();
                }

                co_return {};
            };

            auto async_read =
                [](unique_ptr<boost::asio::ip::tcp::socket>& socket,
                   const string& key) -> boost::cobalt::task<std::string> {
                try {

                    std::string tx = "r:" + key;
                    co_await boost::asio::async_write(
                        *socket, boost::asio::buffer(tx, tx.size()),
                        boost::cobalt::use_task);

                    char rx[1024] = {};
                    std::size_t n = co_await socket->async_read_some(
                        boost::asio::buffer(rx), boost::cobalt::use_task);

                    string rxs(rx);
                    cout << "read received: " << rxs << endl;
                    co_return rxs.substr(rxs.find(":") + 1);

                } catch (boost::system::system_error const& e) {
                    cout << "read error: " << e.what() << endl;
                    co_return "";
                }

                co_return "";
            };

            auto async_write =
                [](unique_ptr<boost::asio::ip::tcp::socket>& socket,
                   const string& key,
                   const string& value) -> boost::cobalt::task<void> {
                try {

                    std::string tx = "w:" + key + "=" + value;
                    co_await boost::asio::async_write(
                        *socket, boost::asio::buffer(tx, tx.size()),
                        boost::cobalt::use_task);

                    char rx[1024] = {};
                    std::size_t n = co_await socket->async_read_some(
                        boost::asio::buffer(rx), boost::cobalt::use_task);

                    co_return;

                } catch (boost::system::system_error const& e) {
                    co_return;
                }

                co_return;
            };

            constexpr int COUNT = 8 * 1024;

            auto ctrl = co_await async_connect("127.0.0.1", "5001");
            co_await wait_for_cluster_ready(ctrl);

            /*
             * add node
             */
            co_await add_node(ctrl, "127.0.0.1:6000,127.0.0.1:5555");

            usleep(500 * 1000);
            /* TODO: this API is not yet reliable */
            // co_await wait_for_cluster_ready(ctrl);

            auto node = co_await async_connect("127.0.0.1", "5555");

            /* write */
            for (auto i = 0; i < COUNT; ++i) {
                co_await async_write(node, "k" + to_string(i), to_string(i));
            }

            /* read-back */
            for (auto i = 0; i < COUNT; ++i) {
                auto s = co_await async_read(node, "k" + to_string(i));
                assert(s == to_string(i));
            }

            /*
             * remove node
             */

            co_await remove_node(ctrl, "127.0.0.1:6000");
            usleep(500 * 1000);
            // co_await wait_for_cluster_ready(ctrl);

            for (auto i = 0; i < COUNT; ++i) {
                auto retry = 3;
                while (retry-- > 0) {
                    auto s = co_await async_read(node, "k" + to_string(i));
                    if (s == to_string(i))
                        break;
                    node = co_await async_connect("127.0.0.1", "5555");
                }

                assert(retry > 0);
            }
#if 0

            co_await add_node(ctrl, "127.0.0.1:6000,127.0.0.1:5555");
            co_await wait_for_cluster_ready(ctrl);

            for (auto i = 0; i < COUNT; ++i) {
                auto retry = 3;
                while (retry-- > 0) {
                    auto s = co_await async_read(node, "k" + to_string(i));
                    if (s == to_string(i))
                        break;
                    node = co_await async_connect("127.0.0.1", "5555");
                }

                assert(retry > 0);
            }
#endif

            // co_await add_node(ctrl, "127.0.0.1:6001,127.0.0.1:5555");
            // usleep(1000 * 1000);

            // co_await add_node(ctrl, "127.0.0.1:6002,127.0.0.1:5555");
            // usleep(1000 * 1000);

            // // co_await add_node(ctrl, "127.0.0.1:6003,127.0.0.1:5555");
            // // usleep(1000 * 1000);

            // // co_await add_node(ctrl, "127.0.0.1:6004,127.0.0.1:5555");
            // // usleep(1000 * 1000);

            // cout << "waiting before read...  " << endl;
            // usleep(5000 * 1000);

            // for (auto i = 0; i < COUNT; ++i) {
            //     auto s = co_await async_read(node, "k" + to_string(i));
            //     assert(s == to_string(i));
            // }

            // cout << "wait for tear down...  " << endl;
            // usleep(10000 * 1000);

            exit(0);
        }(),
        boost::asio::detached);

    io.run();

    /* wait for cluster ready */

    cluster_instance.join();

    cout << "### " << endl;
}
