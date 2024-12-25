#include "node/replica.hh"
#include "replica.hh"
#include <array>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/use_future.hpp>
#include <memory>
#include <optional>
#include <set>
#include <vector>

using namespace std;

/*
 * two things to decide:
 *  1. do we need to transition to follower
 *  2. do we need to ask leader to walk backwards in history
 */

boost::cobalt::task<Replica::AppendEntryReply>
Replica::replicate_log(std::string& peer_addr, Replica::AppendEntryReq& req) {

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
         * Majority _must_ be greater than half. In either cluster size of 4
         * or 5, 3 is required to be majority.
         */

        co_await apply_logs();
    }
}

// template <>
// auto Replica::rx_payload_handler<Replica::Leader>(
//     const Replica::RequestVariant& variant)
//     -> tuple<State, Replica::ReplyVariant> {

//     State s = impl.state;
//     ReplyVariant rv;
//     switch (variant.index()) {
//     case 0: {
//         /* append entries */
//         auto [s, reply] = add_entries<Replica::Leader>(get<0>(variant));
//         rv = ReplyVariant(reply);
//     } break;
//     case 1: {
//         auto [s, reply] = request_vote<Replica::Leader>(get<1>(variant));
//         rv = ReplyVariant(reply);
//     } break;
//     }

//     return {s, rv};
// };

template <>
boost::cobalt::task<void> Replica::rx_connection<Replica::Leader>(
    boost::asio::ip::tcp::acceptor& acceptor,
    boost::asio::steady_timer& cancel) {

    auto wait_for_cancel = [&]() -> boost::cobalt::task<void> {
        boost::system::error_code ec;
        co_await cancel.async_wait(
            boost::asio::redirect_error(boost::cobalt::use_task, ec));
    };

    auto io = co_await boost::cobalt::this_coro::executor;

    // auto active_tasks = set<boost::cobalt::promise<void>>;

    set<boost::cobalt::task<void>> active_tasks;

    while (true) {

        bool teardown = false;

        auto nx = co_await boost::cobalt::race(
            acceptor.async_accept(boost::cobalt::use_task), wait_for_cancel());
        switch (nx.index()) {
        case 0: {

            auto& socket = get<0>(nx);

            // auto task = boost::cobalt::spawn(io,
            // rx_process(std::move(socket)),
            //                                  boost::cobalt::use_task);
            // active_tasks.insert(task);

            // task->finally([&]() { active_tasks.erase(task); });

        } break;
        case 1: {
            teardown = true;
        } break;
        }

        if (teardown) {
            break;
        }
    }

    while (active_tasks.size()) {
        /* TODO: */
    }
}

boost::cobalt::task<void> Replica::follower_handler(
    string& peer_addr,
    shared_ptr<boost::cobalt::channel<Replica::RequestVariant>> rx,
    shared_ptr<boost::cobalt::channel<Replica::ReplyVariant>> tx) {

    auto io = co_await boost::cobalt::this_coro::executor;

    auto p = peer_addr.find(":");
    auto addr = peer_addr.substr(0, p);
    auto port = peer_addr.substr(p + 1);

    boost::asio::ip::tcp::resolver resolver(io);
    boost::asio::ip::tcp::socket socket(io);
    auto ep = resolver.resolve(addr, port);

    while (rx->is_open()) {
        auto variant = co_await rx->read();

        volatile int dummy = 0;

        cout << impl.my_addr << " followe_handler(): sending! " << endl;

        boost::system::error_code err_code;
        boost::asio::async_connect(
            socket, ep,
            [&socket, &err_code](const boost::system::error_code& error,
                                 const boost::asio::ip::tcp::endpoint&) {
                err_code = error;
                // std::cout << "error = " << error << std::endl;
            });

        auto req = serialize(Replica::RequestVariant(variant));
        co_await boost::asio::async_write(
            socket, boost::asio::buffer(req.c_str(), req.size()),
            boost::cobalt::use_task);

        cout << impl.my_addr << " followe_handler(): receiving .. " << endl;

        char reply_char[1024] = {};
        auto n = co_await socket.async_read_some(
            boost::asio::buffer(reply_char), boost::cobalt::use_task);
        auto reply = deserialize<Replica::ReplyVariant>(string(reply_char));

        cout << impl.my_addr << " followe_handler(): received! " << endl;

        co_await tx->write(reply);
    }

    co_return;
}

static boost::cobalt::task<Replica::ReplyVariant>
read_proxy(shared_ptr<boost::cobalt::channel<Replica::ReplyVariant>> rx) {
    co_return co_await rx->read();
}

boost::cobalt::task<Replica::State>
Replica::leader_fsm(boost::asio::ip::tcp::acceptor& acceptor) {

    impl.state = Leader;
    impl.leader = {};

    auto io = co_await boost::cobalt::this_coro::executor;

    vector<shared_ptr<boost::cobalt::channel<Replica::RequestVariant>>> tx;
    vector<shared_ptr<boost::cobalt::channel<Replica::ReplyVariant>>> rx;

    AppendEntryReq heartbeat = {};
    heartbeat.term = pstate.currentTerm;
    heartbeat.leaderId = impl.my_addr;
    if (pstate.logs.size()) {
        heartbeat.prevLogIndex = pstate.logs.size() - 1;
        heartbeat.prevLogTerm = pstate.logs.back().first;
    }
    heartbeat.leaderCommit = vstate.commitIndex;

    vector<boost::cobalt::task<Replica::ReplyVariant>> replies;

    for (auto k = 0; k < impl.cluster.size(); ++k) {
        tx.push_back(
            make_shared<boost::cobalt::channel<Replica::RequestVariant>>(8,
                                                                         io));
        rx.push_back(
            make_shared<boost::cobalt::channel<Replica::ReplyVariant>>(8, io));
        auto peer_addr = impl.cluster[k];
        if (peer_addr != impl.my_addr) {
            boost::cobalt::spawn(io, follower_handler(peer_addr, tx[k], rx[k]),
                                 boost::asio::detached);
            co_await tx[k]->write(heartbeat);

            replies.push_back(read_proxy(rx[k]));
        }
    }

    /* block forever */
    boost::asio::steady_timer cancel{io};
    cancel.expires_at(decltype(cancel)::time_point::max());

    while (true) {
        auto rv = co_await boost::cobalt::race(replies);
        /* Note: rv.first is index into replies I think */
        auto variant = rv.second;
        // switch(variant.index())
        // volatile int dumym = 0;
        switch (variant.index()) {
        case 0: {
            auto r = boost::variant2::get<0>(variant);
            volatile int dumym = 0;
        } break;
        case 1: {
            volatile int dumym = 0;
        } break;
        }
    }

    while (true) {

        auto resp = co_await rx[1]->read();

        co_await cancel.async_wait(boost::cobalt::use_task);
    }

    co_return Follower;
}