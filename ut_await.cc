

#include "node.hh"
#include <iomanip>
#include <iostream>

#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/signal_set.hpp>
#include <boost/asio/write.hpp>

using boost::asio::awaitable;
using boost::asio::co_spawn;
using boost::asio::detached;
using boost::asio::use_awaitable;
using boost::asio::ip::tcp;
namespace this_coro = boost::asio::this_coro;

using namespace std;

awaitable<void> rx_process(Node& node, tcp::socket socket) {

    char payload[1024];
    for (;;) {
        std::size_t n = co_await socket.async_read_some(
            boost::asio::buffer(payload), boost::asio::use_awaitable);

        string cmd(payload, 1);

        // auto cmd = s.substr(0, p);
        // auto payload = s.substr(p);

        string value;
        if (cmd == "r") {
            /* read */
            auto key = string(payload + 2, n - 2);
            value = co_await node.read_await(key);
            auto resp = "ra:" + value;
            co_await async_write(socket,
                                 boost::asio::buffer(resp.c_str(), resp.size()),
                                 boost::asio::use_awaitable);

        } else if (cmd == "w") {
            /* write */
        } else if (cmd == "g") {
            /* gossip */
            auto gossip = string(payload + 2, n - 2);
            node.gossip_await(gossip);
            auto resp = "ga:" + gossip;
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

awaitable<void> heartbeat(Node& node) {
    boost::asio::steady_timer timer(co_await this_coro::executor);

    for (;;) {

        co_await node.heartbeat_await();

        /* heartbeat every second */
        timer.expires_at(std::chrono::steady_clock::now() +
                         std::chrono::seconds(1));
        co_await timer.async_wait(use_awaitable);

        cout << "heartbeat!" << endl;
    }
}

int main() {
    boost::asio::io_context io_context(1);
    boost::asio::signal_set signals(io_context, SIGINT, SIGTERM);

    Node node("127.0.0.1:5555");

    signals.async_wait([&](auto, auto) { io_context.stop(); });

    co_spawn(io_context, listener(node), detached);
    co_spawn(io_context, heartbeat(node), detached);

    Node node2("127.0.0.1:5556", "127.0.0.1:5555");

    co_spawn(io_context, listener(node2), detached);
    co_spawn(io_context, heartbeat(node2), detached);

    io_context.run();

    cout << "### " << endl;
}