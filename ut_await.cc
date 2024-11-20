

#include "node.hh"
#include <chrono>
#include <iomanip>
#include <iostream>

#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/signal_set.hpp>
#include <boost/asio/write.hpp>
#include <string>

using boost::asio::awaitable;
using boost::asio::co_spawn;
using boost::asio::detached;
using boost::asio::use_awaitable;
using boost::asio::ip::tcp;
namespace this_coro = boost::asio::this_coro;

using namespace std;

awaitable<void> rx_process(Node& node, tcp::socket socket) {

    for (;;) {
        char data[1024] = {};
        std::size_t n = co_await socket.async_read_some(
            boost::asio::buffer(data), boost::asio::use_awaitable);

        string payload = string(data, n);
        auto p = payload.find(":");

        auto cmd = payload.substr(0, p);
        if (cmd == "r") {
            /* read */
            auto key = string(data + 2, n - 2);
            auto value = co_await node.read(key);
            auto resp = "ra:" + value;
            co_await async_write(socket,
                                 boost::asio::buffer(resp.c_str(), resp.size()),
                                 boost::asio::use_awaitable);

        } else if (cmd == "w" || cmd == "wf") {

            /* write */

            auto kv = string(data + cmd.size() + 1, n - cmd.size() - 1);
            auto p = kv.find("=");
            auto k = kv.substr(0, p);
            auto v = kv.substr(p + 1);

            co_await node.write(k, v, cmd == "w");

            string resp;
            if (cmd == "wf")
                resp = string("wfa:");
            else
                resp = string("wa:");

            co_await async_write(socket,
                                 boost::asio::buffer(resp.c_str(), resp.size()),
                                 boost::asio::use_awaitable);
        } else if (cmd == "g") {
            /* gossip */
            auto gossip = payload.substr(p + 1);
            node.gossip(gossip);
            auto resp = "ga:" + gossip;
            co_await async_write(socket,
                                 boost::asio::buffer(resp.c_str(), resp.size()),
                                 boost::asio::use_awaitable);
        } else if (cmd == "s") {
            auto stream = payload.substr(p + 1);
            auto p = stream.find("-");
            auto i = stoll(stream.substr(0, p)),
                 j = stoll(stream.substr(p + 1));

            auto resp = "sa:" + node.serialize(node.stream(i, j));
            co_await async_write(socket,
                                 boost::asio::buffer(resp.c_str(), resp.size()),
                                 boost::asio::use_awaitable);
        }
    }
    co_return;
}

awaitable<void> listener(Node& node) {
    auto executor = co_await this_coro::executor;

    auto p = node.get_addr().find(":");
    auto addr = node.get_addr().substr(0, p);
    auto port = node.get_addr().substr(p + 1);

    tcp::acceptor acceptor(executor, {tcp::v4(), stoi(port)});
    for (;;) {
        auto socket =
            co_await acceptor.async_accept(boost::asio::use_awaitable);
        co_spawn(executor, rx_process(node, std::move(socket)), detached);
    }
}

awaitable<void> heartbeat(Node& node, int start_delay_ms) {
    boost::asio::steady_timer timer(co_await this_coro::executor);

    timer.expires_at(std::chrono::steady_clock::now() +
                     std::chrono::milliseconds(start_delay_ms));
    co_await timer.async_wait(use_awaitable);

    for (;;) {
        co_await node.heartbeat();

        /* heartbeat every second */
        timer.expires_at(std::chrono::steady_clock::now() +
                         std::chrono::seconds(1));
        co_await timer.async_wait(use_awaitable);
    }
}

int main() {

    thread co([] {
        boost::asio::io_context io_context(1);
        boost::asio::signal_set signals(io_context, SIGINT, SIGTERM);
        Node node("127.0.0.1:5555"); // , "127.0.0.1:5557");
        signals.async_wait([&](auto, auto) { io_context.stop(); });
        co_spawn(io_context, listener(node), detached);
        co_spawn(io_context, heartbeat(node, 0), detached);

        Node node2("127.0.0.1:5556", "127.0.0.1:5555");
        co_spawn(io_context, listener(node2), detached);
        co_spawn(io_context, heartbeat(node2, 100), detached);

        // Node node3("127.0.0.1:5557", "127.0.0.1:5559");
        // co_spawn(io_context, listener(node3), detached);
        // co_spawn(io_context, heartbeat(node3, 100), detached);

        io_context.run();
    });

    co.join();

    cout << "### " << endl;
}