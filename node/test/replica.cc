#include "node/replica.hh"

#include <boost/algorithm/string.hpp>
#include <boost/asio.hpp>
#include <boost/asio/detached.hpp>
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

int main() {

    vector<string> cluster = {"127.0.0.1:5555", "127.0.0.1:5556"};

    Replica r0(cluster[0], cluster);

    boost::asio::io_context io(1);

    cout << "whatever" << endl;
    boost::cobalt::spawn(
        io,
        []() -> boost::cobalt::task<void> {
            /* TODO: */

            auto io = co_await boost::asio::this_coro::executor;

            cout << "coro #1" << endl;

            // cout << co_await eager_promise() << endl;

            // boost::asio::io_context io2(1);

            auto t =
                boost::cobalt::spawn(io, spawn_task(), boost::cobalt::use_task);

            // io2.run();
            co_await t;

            cout << "coro #2" << endl;

            co_return;
        }(),
        boost::asio::detached);

    /* TC: All followers electing one leader */

    /* TC: Single leader multi follower majority */

    /* TC: Split majority  */

    io.run();

    // usleep(1000000);
}