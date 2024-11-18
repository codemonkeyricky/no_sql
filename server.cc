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

awaitable<void> rx_process(tcp::socket socket) {

    char payload[1024];
    for (;;) {
        std::size_t n = co_await socket.async_read_some(
            boost::asio::buffer(payload), boost::asio::use_awaitable);

        string cmd(payload, 1);

        // auto cmd = s.substr(0, p);
        // auto payload = s.substr(p);

        if (cmd == "r") {
            /* read */
        } else if (cmd == "w") {
            /* write */
        } else if (cmd == "g") {
            /* gossip */
        }

        co_await async_write(socket, boost::asio::buffer(payload, n),
                             boost::asio::use_awaitable);
    }
}

awaitable<void> listener() {
    auto executor = co_await this_coro::executor;

    tcp::acceptor acceptor(executor, {tcp::v4(), 55555});
    for (;;) {
        auto socket =
            co_await acceptor.async_accept(boost::asio::use_awaitable);
        co_spawn(executor, rx_process(std::move(socket)), detached);
    }
}

int main() {
    boost::asio::io_context io_context(1);
    boost::asio::signal_set signals(io_context, SIGINT, SIGTERM);

    signals.async_wait([&](auto, auto) { io_context.stop(); });

    co_spawn(io_context, listener(), detached);

    io_context.run();
}