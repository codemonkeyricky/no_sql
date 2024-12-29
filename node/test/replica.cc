#include "node/replica.hh"

#include <boost/algorithm/string.hpp>
#include <boost/asio.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/write.hpp>
#include <boost/cobalt.hpp>

#include <iostream>
#include <string>
#include <vector>

using namespace std;

boost::cobalt::promise<int> eager_promise() {

    // cout << "eager_promise" << endl;
    co_return 3;
}

boost::cobalt::task<void> task2();

boost::cobalt::task<void> task1() {
    cout << "task 1" << endl;
    auto io = co_await boost::asio::this_coro::executor;
    boost::cobalt::spawn(io, task2(), boost::asio::detached);
    cout << "detached task 1" << endl;
    co_return;
}

boost::cobalt::task<void> task2() {
    cout << "task 2" << endl;
    auto io = co_await boost::asio::this_coro::executor;
    boost::cobalt::spawn(io, task1(), boost::asio::detached);
    cout << "detached task 2" << endl;
    co_return;
}

auto spawn_task = []() -> boost::cobalt::task<void> {
    cout << "spawn task" << endl;
    auto io = co_await boost::asio::this_coro::executor;
    boost::cobalt::spawn(io, task1(), boost::asio::detached);
    co_return;
};

namespace Command {

auto async_connect = [](const string& addr, const string& port)
    -> boost::cobalt::task<unique_ptr<boost::asio::ip::tcp::socket>> {
    auto io = co_await boost::cobalt::this_coro::executor;
    auto socket = unique_ptr<boost::asio::ip::tcp::socket>(
        new boost::asio::ip::tcp::socket(io));
    boost::asio::ip::tcp::resolver resolver(io);
    auto ep = resolver.resolve(addr, port);

    boost::asio::async_connect(*socket, ep,
                               [](const boost::system::error_code& error,
                                  const boost::asio::ip::tcp::endpoint&) {});

    co_return move(socket);
};

auto async_read = [](unique_ptr<boost::asio::ip::tcp::socket>& socket,
                     const string& key) -> boost::cobalt::task<std::string> {
    try {

        std::string tx = "r:" + key;
        co_await boost::asio::async_write(*socket,
                                          boost::asio::buffer(tx, tx.size()),
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

template <typename T> std::string serialize(T&& data) {
    std::ostringstream oss;
    boost::archive::text_oarchive oa(oss);
    oa << data;
    return oss.str();
}

template <typename T, typename StringType> T deserialize(StringType&& data) {
    T rv;
    std::istringstream iss(data);
    boost::archive::text_iarchive ia(iss);
    ia >> rv;
    return std::move(rv);
}

auto async_write = [](unique_ptr<boost::asio::ip::tcp::socket>& socket,
                      const Replica::ClientReqVariant& req)
    -> boost::cobalt::task<Replica::ClientReplyVariant> {
    try {

        std::string tx = serialize(req);
        co_await boost::asio::async_write(*socket,
                                          boost::asio::buffer(tx, tx.size()),
                                          boost::cobalt::use_task);

        char rx[1024] = {};
        std::size_t n = co_await socket->async_read_some(
            boost::asio::buffer(rx), boost::cobalt::use_task);

        auto reply = deserialize<Replica::ClientReplyVariant>(rx);

        co_return reply;

    } catch (boost::system::system_error const& e) {
    }

    co_return Replica::ClientReplyVariant();
};

} // namespace Command

int main() {

    vector<string> cluster = {"127.0.0.1:5555", "127.0.0.1:5556"};

    Replica r0(cluster[0], "127.0.0.1:6000", cluster);
    Replica r1(cluster[1], "127.0.0.1:6001", cluster);

    boost::asio::io_context io(1);

    r0.spawn(io);
    r1.spawn(io);

    /* TODO: co_await on a use_task that spawns many detached - hopefully this
     * means the use_task only returns when all detached are complete. */

    boost::cobalt::spawn(
        io,
        []() -> boost::cobalt::task<void> {
            usleep(500 * 1000);

            auto socket = co_await Command::async_connect("127.0.0.1", "6000");

            Replica::WriteReq req = {"k", "v"};
            co_await Command::async_write(socket,
                                          Replica::ClientReqVariant(req));
        }(),
        boost::asio::detached);

    // cout << "whatever" << endl;
    // boost::cobalt::spawn(
    //     io,
    //     []() -> boost::cobalt::task<void> {
    //         /* TODO: */

    //         auto io = co_await boost::asio::this_coro::executor;

    //         cout << "coro #1" << endl;

    //         // cout << co_await eager_promise() << endl;

    //         // boost::asio::io_context io2(1);

    //         auto t =
    //             boost::cobalt::spawn(io, spawn_task(),
    //             boost::cobalt::use_task);

    //         // io2.run();
    //         co_await t;

    //         cout << "coro #2" << endl;

    //         co_return;
    //     }(),
    //     boost::asio::detached);

    /* TC: All followers electing one leader */

    /* TC: Single leader multi follower majority */

    /* TC: Split majority  */

    io.run();

    // usleep(1000000);
}
