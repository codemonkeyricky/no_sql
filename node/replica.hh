
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
#include <boost/serialization/array.hpp>
#include <boost/serialization/optional.hpp>
#include <boost/serialization/variant.hpp>

class Replica {
  public:
    enum State {
        Follower,
        Candidate,
        Leader,
    };

  private:
    /* Raft protocol requirement */

    struct Log {
        int term;
        std::array<std::string, 2> kv;

        template <class Archive>
        void serialize(Archive& ar, const unsigned int version) {
            ar & term;
            ar & kv;
        }
    };

    struct PersistentState {
        int currentTerm = 0;
        std::optional<std::string> votedFor =
            {}; /* TODO: clear on a new term! */
                /* TODO: update to std::string */
        /* it's possible to have *not* voted for anyone. eg. if a new leader
         * is established while we were offline, we would accept leader as
         * is. */
        std::vector<Log> logs;
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

    boost::cobalt::task<State> candidate_fsm(boost::asio::ip::tcp::acceptor&,
                                             boost::asio::ip::tcp::acceptor&);
    boost::cobalt::task<State> leader_fsm(boost::asio::ip::tcp::acceptor&,
                                          boost::asio::ip::tcp::acceptor&);
    boost::cobalt::task<State> follower_fsm(boost::asio::ip::tcp::acceptor&,
                                            boost::asio::ip::tcp::acceptor&);
    boost::cobalt::task<void> fsm(boost::asio::ip::tcp::acceptor,
                                  boost::asio::ip::tcp::acceptor);

    template <State T>
    boost::cobalt::task<void>
    rx_connection(boost::asio::ip::tcp::acceptor& acceptor,
                  boost::asio::steady_timer& cancel);

  public:
    struct AppendEntryReq {
        int term;
        std::string leaderId;
        int prevLogIndex;
        int prevLogTerm;
        int leaderCommit;
        std::optional<Log> entry; /* key / value*/

        template <class Archive>
        void serialize(Archive& ar, const unsigned int version) {
            ar & term;
            ar & leaderId;
            ar & prevLogIndex;
            ar & prevLogTerm;
            ar & leaderCommit;
            ar & entry;
        }
    };

    struct AppendEntryReply {
        int term;
        bool success;

        template <class Archive>
        void serialize(Archive& ar, const unsigned int version) {
            ar & term;
            ar & success;
        }
    };

    struct RequestVoteReq {
        int term;
        std::string candidateId;
        int lastLogIndex;
        int lastLogTerm;

        template <class Archive>
        void serialize(Archive& ar, const unsigned int version) {
            ar & term;
            ar & candidateId;
            ar & lastLogIndex;
            ar & lastLogTerm;
        }
    };

    struct RequestVoteReply {
        int term;
        bool voteGranted;

        template <class Archive>
        void serialize(Archive& ar, const unsigned int version) {
            ar & term;
            ar & voteGranted;
        }
    };

    Replica(const std::string& addr, const std::vector<std::string>& cluster) {
        impl.my_addr = addr;
        impl.cluster = cluster;
    }

    void spawn(boost::asio::io_context& io) {

        auto p = impl.my_addr.find(":");
        auto addr = impl.my_addr.substr(0, p);
        auto port = impl.my_addr.substr(p + 1);

        boost::asio::ip::tcp::acceptor acceptor(
            io, {boost::asio::ip::tcp::v4(), stoi(port)});

        boost::asio::ip::tcp::acceptor client_acceptor(
            io, {boost::asio::ip::tcp::v4(), 6000});

        boost::cobalt::spawn(
            io, fsm(std::move(acceptor), std::move(client_acceptor)),
            boost::asio::detached);
    }

    using RequestVariant =
        boost::variant2::variant<AppendEntryReq, RequestVoteReq>;

    using ReplyVariant =
        boost::variant2::variant<AppendEntryReply, RequestVoteReply>;

    using RpcVariant = boost::variant2::variant<RequestVariant, ReplyVariant>;

    template <State T>
    std::tuple<State, AppendEntryReply> add_entries(const AppendEntryReq& req);
    template <State T>
    std::tuple<State, RequestVoteReply> request_vote(const RequestVoteReq& req);

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
    boost::cobalt::task<bool> timeout(int ms) {
        boost::asio::steady_timer timer{
            co_await boost::cobalt::this_coro::executor};
        timer.expires_after(std::chrono::milliseconds(ms));

        co_await timer.async_wait(boost::cobalt::use_op);

        co_return true;
    }

    /* used by leader and follower */
    boost::cobalt::task<void> apply_logs() {
        while (vstate.lastApplied < vstate.commitIndex) {
            /* TODO: execute the logs here */
        }

        co_return;
    }

    // boost::cobalt::task<void> rx_conn_acceptor() {

    //     auto io = co_await boost::cobalt::this_coro::executor;

    //     /* TODO: extract port from my_addr */
    //     boost::asio::ip::tcp::acceptor acceptor(
    //         io, {boost::asio::ip::tcp::v4(), 5555});

    //     for (;;) {
    //         auto socket =
    //             co_await acceptor.async_accept(boost::cobalt::use_task);
    //         boost::cobalt::spawn(io, rx_conn_handler(std::move(socket)),
    //                              boost::asio::detached);
    //     }
    // }

    template <State T>
    std::tuple<State, Replica::ReplyVariant>
    rx_payload_handler(const RequestVariant&);

#if 0
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
#endif

    bool at_least_as_up_to_date_as_me(int peer_last_log_index,
                                      int peer_last_log_term);

    template <typename T> std::string serialize(T&& data) {
        std::ostringstream oss;
        boost::archive::text_oarchive oa(oss);
        oa << data;
        return oss.str();
    }

    template <typename T, typename StringType>
    T deserialize(StringType&& data) {
        T rv;
        std::istringstream iss(data);
        boost::archive::text_iarchive ia(iss);
        ia >> rv;
        return std::move(rv);
    }

    boost::cobalt::task<Replica::RequestVoteReply>
    request_vote_from_peer(std::string& peer_addr);

    boost::cobalt::task<Replica::AppendEntryReply>

    replicate_log(std::string& peer_addr, Replica::AppendEntryReq& req);

    boost::cobalt::task<void> follower_handler(
        std::string& peer_addr,
        std::shared_ptr<boost::cobalt::channel<Replica::RequestVariant>> rx,
        std::shared_ptr<boost::cobalt::channel<Replica::ReplyVariant>> tx,
        boost::asio::steady_timer& cancel);

    /* TODO: ClientReq isn't actually RequestVariant. RequestVariant is only
     * used within replica group */
    using ClientReq = std::tuple<
        Replica::RequestVariant,
        std::shared_ptr<boost::cobalt::channel<Replica::ReplyVariant>>>;

    using ReplicaReq = std::tuple<
        Replica::RequestVariant,
        std::shared_ptr<boost::cobalt::channel<Replica::ReplyVariant>>>;

    auto rx_client_conn(boost::asio::ip::tcp::acceptor& acceptor_replica,
                        std::shared_ptr<boost::cobalt::channel<ClientReq>> tx,
                        boost::asio::steady_timer& cancel)
        -> boost::cobalt::task<void>;

    auto rx_replica_conn(boost::asio::ip::tcp::acceptor&,
                         std::shared_ptr<boost::cobalt::channel<ReplicaReq>> tx,
                         boost::asio::steady_timer& cancel)
        -> boost::cobalt::task<void>;

    auto rx_client_payload(
        boost::asio::ip::tcp::socket socket,
        std::shared_ptr<boost::cobalt::channel<ClientReq>> tx,
        boost::asio::steady_timer& cancel) -> boost::cobalt::task<void>;

    auto send_rpc(std::string& peer,
                  RequestVariant& variant) -> boost::cobalt::task<ReplyVariant>;
};