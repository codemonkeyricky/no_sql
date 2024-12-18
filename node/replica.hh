
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

    /* Raft protocol requirement */

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

    struct FollowerState {
        std::string addr;
        int nextIndex;
        int matchIndex;
    };

    struct VolatileStateLeader {
        std::vector<FollowerState> followers;
    };

    PersistentState pstate;
    VolatileState vstate;
    VolatileStateLeader vstate_leader;

    /* implementation requirements */

    struct Implementation {
        State state = Follower;
        bool leader_keep_alive = false;
        std::string my_addr;
        std::vector<std::string> cluster;
    };

    Implementation impl;

    boost::cobalt::task<void> heartbeat_candidate() {

        /* TODO */
        co_return;
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

    boost::cobalt::task<bool> candidate_campaign();

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

    struct RequestVoteReq {
        int term;
        std::string candidateId;
        int lastLogIndex;
        int lastLogTerm;
    };

    struct RequestVoteReply {
        int term;
        bool voteGranted;
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

        co_return;
    }

    boost::cobalt::task<void> replicate_logs(
        std::optional<std::reference_wrapper<std::array<std::string, 2>>> kv);

    boost::cobalt::task<Replica::AppendEntryReply>
    replicate_log(std::string addr, Replica::AppendEntryReq req);

    boost::cobalt::task<Replica::RequestVoteReply>
    candidate_request_vote(std::string peer_addr);

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