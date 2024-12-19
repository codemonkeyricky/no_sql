#include "node/replica.hh"
#include "replica.hh"
#include <optional>

using namespace std;

boost::cobalt::task<Replica::RequestVoteReply>
Replica::leader_request_vote(const Replica::RequestVoteReq& req) {}

boost::cobalt::task<Replica::AppendEntryReply>
Replica::leader_add_entries(const Replica::AppendEntryReq& req) {}

static boost::cobalt::task<Replica::AppendEntryReply>
replicate_log(std::string peer_addr, Replica::AppendEntryReq req) {

    auto p = peer_addr.find(":");
    auto addr = peer_addr.substr(0, p);
    auto port = peer_addr.substr(p + 1);

    auto io = co_await boost::cobalt::this_coro::executor;

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

#if 0
    std::string req = "v:" + std::to_string(i) + "-" + std::to_string(j);
    co_await boost::asio::async_write(
        socket, boost::asio::buffer(req.c_str(), req.size()),
        boost::cobalt::use_task);

    /* read results */
    char payload[1024] = {};
    auto n = co_await socket.async_read_some(boost::asio::buffer(payload),
                                             boost::cobalt::use_task);
#endif

    Replica::AppendEntryReply empty{};
    co_return empty;
}

boost::cobalt::task<void> Replica::leader_replicate_logs(
    optional<reference_wrapper<array<string, 2>>> kv) {

    int success_cnt = 0;
    int highest_term = 0;
    for (auto& follower : vstate_leader.followers) {
        Replica::AppendEntryReq req{
            pstate.currentTerm, "",
            follower.nextIndex, pstate.logs[follower.nextIndex].first,
            vstate.commitIndex, kv,
        };

        auto [term, success] = co_await replicate_log(follower.addr, req);
        success_cnt += success;
        highest_term = max(highest_term, term);
    }

    if (highest_term > pstate.currentTerm) {
        /* Someone has been elected leader */

        /* TODO: become a follower */
    } else if (success_cnt >=
               (vstate_leader.followers.size() + 1 /* leader */) / 2 + 1) {
        /*
         * If we have consensus from majority the entry is committe
         * Majority _must_ be greater than half. In either cluster size of 4 or
         * 5, 3 is required to be majority.
         */

        co_await apply_logs();
    }
}

boost::cobalt::task<void> Replica::leader_fsm() {

    auto leader_rx_payload_handler =
        [this](const Replica::RequestVariant& variant)
        -> boost::cobalt::task<void> {
        switch (variant.index()) {
        case 0: {
            /* append entries */
            auto reply = co_await leader_add_entries(get<0>(variant));
        } break;
        case 1: {
            // auto req = variant.value();
            auto reply = co_await leader_request_vote(get<1>(variant));
        } break;
        }
    };

    impl.state = Leader;
    impl.leader = {};

    rx_payload_handler = [&](const RequestVariant& variant) {
        /* need to trampoline through a lambda because rx_payload_handler
         * parameters is missing the implicit "this" argument */
        return leader_rx_payload_handler(variant);
    };

    while (true) {
        /* wait for heartbeat timeout */
        co_await timeout(150);

        if (impl.leader.step_down) {
            break;
        }
    }

    /* become a follower after stepping down */

    auto io = co_await boost::cobalt::this_coro::executor;
    boost::cobalt::spawn(io, follower_fsm(), boost::asio::detached);
}