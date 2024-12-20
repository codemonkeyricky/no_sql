
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

    struct LeaderImpl {
        bool step_down = 0;
        // boost::asio::steady_timer cancel_timer;
    };

    struct FollowerImpl {
        bool keep_alive = 0;
    };

    struct Implementation {
        State state = Follower;
        std::string my_addr;
        std::vector<std::string> cluster;
        std::optional<std::string> votedFor;

        LeaderImpl leader;
        FollowerImpl follower;
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

    boost::cobalt::task<void> candidate_fsm();
    boost::cobalt::task<void> leader_fsm(boost::asio::ip::tcp::acceptor);
    boost::cobalt::task<void> follower_fsm();

    boost::cobalt::task<void>
    leader_rx(boost::asio::ip::tcp::acceptor& acceptor,
              boost::asio::steady_timer& cancel);

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

    Replica() { /* spawn as follower */ }

    boost::cobalt::task<void> init() {

        auto io = co_await boost::cobalt::this_coro::executor;

        boost::cobalt::spawn(io, follower_fsm(), boost::asio::detached);

        boost::cobalt::spawn(io, rx_conn_acceptor(), boost::asio::detached);
    }

    using RequestVariant =
        boost::variant2::variant<AppendEntryReq, RequestVoteReq>;

    template <State T>
    boost::cobalt::task<AppendEntryReply>
    add_entries(const AppendEntryReq& req);
    template <State T>
    boost::cobalt::task<RequestVoteReply>
    request_vote(const RequestVoteReq& req);

    // boost::cobalt::task<AppendEntryReply>
    // leader_add_entries(const AppendEntryReq& req);
    // boost::cobalt::task<RequestVoteReply>
    // leader_request_vote(const RequestVoteReq& req);

    // boost::cobalt::task<AppendEntryReply>
    // candidate_add_entries(const AppendEntryReq& req);
    // boost::cobalt::task<RequestVoteReply>
    // candidate_request_vote(const RequestVoteReq& req);

    boost::cobalt::task<void> leader_replicate_logs(
        std::optional<std::reference_wrapper<std::array<std::string, 2>>> kv);

    /* used by everyone */
    boost::cobalt::task<void> timeout(int ms) {
        boost::asio::steady_timer timer{
            co_await boost::cobalt::this_coro::executor};
        timer.expires_after(std::chrono::milliseconds(ms));

        co_await timer.async_wait(boost::cobalt::use_op);

        co_return;
    }

    /* used by leader and follower */
    boost::cobalt::task<void> apply_logs() {
        while (vstate.lastApplied < vstate.commitIndex) {
            /* TODO: execute the logs here */
        }

        co_return;
    }

    boost::cobalt::task<void> rx_conn_acceptor() {

        auto io = co_await boost::cobalt::this_coro::executor;

        /* TODO: extract port from my_addr */
        boost::asio::ip::tcp::acceptor acceptor(
            io, {boost::asio::ip::tcp::v4(), 5555});

        for (;;) {
            auto socket =
                co_await acceptor.async_accept(boost::cobalt::use_task);
            boost::cobalt::spawn(io, rx_conn_handler(std::move(socket)),
                                 boost::asio::detached);
        }
    }

    std::function<boost::cobalt::task<void>(const RequestVariant&)>
        rx_payload_handler;

    // boost::cobalt::task<void>
    // rx_payload_handler(const RequestVariant& variant) {}

    boost::cobalt::task<void>
    rx_conn_handler(boost::asio::ip::tcp::socket socket) {

        auto io = co_await boost::cobalt::this_coro::executor;

        char data[1024] = {};
        std::size_t n = co_await socket.async_read_some(
            boost::asio::buffer(data), boost::cobalt::use_task);

        // AppendEntryReq add_entry_req;
        // RequestVoteReq req_vote_req;

        RequestVariant var;

        co_await rx_payload_handler(var);

        // #define ADD_ENTRIES 0
        // #define REQ_VOTE 1
        //         int payload_type = 0;

        //         switch (impl.state) {
        //         case Candidate:
        //             if (payload_type == ADD_ENTRIES) {

        //             } else {
        //             }
        //             break;
        //         case Follower:
        //         if(payload_type)
        //             break;

        //         case Leader:
        //             break;
        //         }
    }
};