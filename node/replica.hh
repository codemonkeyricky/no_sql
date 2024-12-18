
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

    /* Raft protocol */

    struct PersistentState {
        int currentTerm = 0;
        std::optional<int> votedFor = {};
        std::vector<std::pair<int, std::array<std::string, 2>>>
            logs; /* term / (key, value)*/
    };

    struct VolatileState {
        int commitIndex = 0;
        int lastApplied = 0;
    };

    struct VolatileStateLeader {
        std::vector<int> nextIndex;
        std::vector<int> matchIndex;
    };

    PersistentState pstate;
    VolatileState vstate;
    VolatileStateLeader vstate_leader;

    /* internal */

    struct Implementation {
        State state = Follower;
        bool leader_keep_alive = false;
        int port = 0;
    };

    Implementation impl;

    boost::cobalt::task<void> heartbeat_candidate() {

        /* TODO */
        co_return;
    }

    boost::cobalt::task<void> candidate_campaign() {

        auto io = co_await boost::asio::this_coro::executor;

        // for (auto peer_addr : group) {

        //     auto p = peer_addr.find(":");
        //     auto addr = peer_addr.substr(0, p);
        //     auto port = peer_addr.substr(p + 1);

        //     boost::asio::ip::tcp::resolver resolver(io);
        //     boost::asio::ip::tcp::socket socket(io);
        //     auto ep = resolver.resolve(addr, port);

        //     boost::system::error_code err_code;

        //     boost::asio::async_connect(
        //         socket, ep,
        //         [&socket, &err_code](const boost::system::error_code& error,
        //                              const boost::asio::ip::tcp::endpoint&) {
        //             err_code = error;
        //             // std::cout << "error = " << error << std::endl;
        //         });

        //     std::string req =
        //         "v:" + std::to_string(i) + "-" + std::to_string(j);
        //     co_await boost::asio::async_write(
        //         socket, boost::asio::buffer(req.c_str(), req.size()),
        //         boost::cobalt::use_task);

        //     /* read results */
        //     char payload[1024] = {};
        //     auto n = co_await socket.async_read_some(
        //         boost::asio::buffer(payload), boost::cobalt::use_task);
        // }
    }

    boost::cobalt::task<void> heartbeat_follower() {

        // auto io = co_await boost::asio::this_coro::executor;

        // /* TODO */

        // if (!leader_keep_alive) {
        //     state = Candidate;

        //     /* spawn a new coroutine to campaign for election */
        //     boost::cobalt::spawn(io, candidate_campaign(),
        //                          boost::asio::detached);
        // }

        // leader_keep_alive = -1;

        co_return;
    }

    boost::cobalt::task<void> heartbeat_leader() {

        /* TODO */
        co_return;
    }

  public:
    struct AppendEntryReq {
        int term;
        std::string leaderId;
        int prevLogIndex;
        int prevLogTerm;
        int leaderCommit;
        std::optional<std::array<std::string, 2>> entry; /* key / value*/
    };

    struct AppendEntryReply {
        int term;
        bool success;
    };

    Replica() {}

    boost::cobalt::task<Replica::AppendEntryReply>
    follower_process_addEntryReq(const AppendEntryReq& entry);

    boost::cobalt::task<void> follower_rx_conn();
    boost::cobalt::task<void>
    follower_rx_payload(boost::asio::ip::tcp::socket socket);

    boost::cobalt::task<void> apply_logs() {

        while (vstate.lastApplied < vstate.commitIndex) {
            /* TODO: execute the logs here */
        }
    }

    // boost::cobalt::task<void> heartbeat() {

    //     // switch (state) {
    //     // case Leader:
    //     //     co_await heartbeat_leader();
    //     //     break;
    //     // case Follower:
    //     //     co_await heartbeat_follower();
    //     //     break;
    //     // case Candidate:
    //     //     co_await heartbeat_candidate();
    //     //     break;
    //     // }
    //     co_return;
    // };
};