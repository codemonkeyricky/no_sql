
#pragma once

#include <array>
#include <deque>
#include <functional>
#include <iostream>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <sys/types.h>
#include <thread>
#include <tuple>
#include <unordered_map>
#include <vector>

#include <boost/archive/text_iarchive.hpp>
#include <boost/archive/text_oarchive.hpp>
#include <boost/asio.hpp>
#include <boost/asio/connect.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/write.hpp>
#include <boost/cobalt.hpp>
// #include <boost/exception/diagnostic_information.hpp>
// #include <boost/serialization/map.hpp>
// #include <boost/serialization/set.hpp>
// #include <boost/serialization/vector.hpp>
// #include <boost/variant2/variant.hpp>

class Replica {

    enum State {
        Follower,
        Candidate,
        Leader,
    };

    /* default initialized */

    State state = Follower;
    bool leader_keep_alive = false;
    int currentTerm = 0;
    std::vector<std::string> group;

    boost::cobalt::task<void> heartbeat_candidate() {

        /* TODO */
        co_return;
    }

    boost::cobalt::task<void> candidate_campaign() {

        auto io = co_await boost::asio::this_coro::executor;

        for (auto peer_addr : group) {

            auto p = peer_addr.find(":");
            auto addr = peer_addr.substr(0, p);
            auto port = peer_addr.substr(p + 1);

            boost::asio::ip::tcp::resolver resolver(io);
            boost::asio::ip::tcp::socket socket(io);
            auto ep = resolver.resolve(addr, port);

            boost::system::error_code err_code;

            boost::asio::async_connect(
                socket, ep,
                [&socket, &err_code](const boost::system::error_code& error,
                                     const boost::asio::ip::tcp::endpoint&) {
                    err_code = error;
                    // std::cout << "error = " << error << std::endl;
                });

            std::string req =
                "v:" + std::to_string(i) + "-" + std::to_string(j);
            co_await boost::asio::async_write(
                socket, boost::asio::buffer(req.c_str(), req.size()),
                boost::cobalt::use_task);

            /* read results */
            char payload[1024] = {};
            auto n = co_await socket.async_read_some(
                boost::asio::buffer(payload), boost::cobalt::use_task);
        }
    }

    boost::cobalt::task<void> heartbeat_follower() {

        auto io = co_await boost::asio::this_coro::executor;

        /* TODO */

        if (!leader_keep_alive) {
            state = Candidate;

            /* spawn a new coroutine to campaign for election */
            boost::cobalt::spawn(io, candidate_campaign(),
                                 boost::asio::detached);
        }

        leader_keep_alive = -1;

        co_return;
    }

    boost::cobalt::task<void> heartbeat_leader() {
        /* TODO */
        co_return;
    }

  public:
    Replica() {}

    boost::cobalt::task<void> heartbeat() {

        switch (state) {
        case Leader:
            co_await heartbeat_leader();
            break;
        case Follower:
            co_await heartbeat_follower();
            break;
        case Candidate:
            co_await heartbeat_candidate();
            break;
        }
        co_return;
    }
};