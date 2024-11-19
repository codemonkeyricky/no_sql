

#include "node.hh"
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

        } else if (cmd == "w" || cmd == "wc") {

            /* write */

            auto kv = string(data + 2, n - 2);
            auto p = kv.find("=");
            auto k = kv.substr(0, p);
            auto v = kv.substr(p + 1);

            co_await node.write(k, v, cmd == "wc");

            auto resp = string("ra:");
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
            auto si = stream.substr(0, p);
            auto sj = stream.substr(p + 1);
            auto i = stoll(si), j = stoll(sj);
            auto db = node.stream(i, j);

            std::ostringstream oss;
            boost::archive::text_oarchive oa(oss);
            oa << db;
            auto resp = "sa:" + oss.str();
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
        cout << "heartbeat #1" << endl;

        co_await node.heartbeat();

        cout << "heartbeat #2" << endl;

        /* heartbeat every second */
        timer.expires_at(std::chrono::steady_clock::now() +
                         std::chrono::seconds(1));
        co_await timer.async_wait(use_awaitable);

        cout << "heartbeat #3" << endl;
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