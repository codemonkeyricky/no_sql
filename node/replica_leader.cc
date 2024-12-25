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
using namespace boost;

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

cobalt::task<void> Replica::follower_handler(
    string& peer_addr,
    std::shared_ptr<cobalt::channel<Replica::RequestVariant>> rx,
    std::shared_ptr<cobalt::channel<Replica::ReplyVariant>> tx) {

    auto io = co_await boost::cobalt::this_coro::executor;

    auto p = peer_addr.find(":");
    auto addr = peer_addr.substr(0, p);
    auto port = peer_addr.substr(p + 1);

    boost::asio::ip::tcp::resolver resolver(io);
    boost::asio::ip::tcp::socket socket(io);
    auto ep = resolver.resolve(addr, port);

    /*
     *  wait on follower_rx for command, or send heartbeat every 150ms
     *      1. Send a heartbeat first
     *      2. Determine where the history aligned
     *      3. Replay history until follower is caught up
     *      4. replicate log at runtime as needed
     */

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
read_proxy(std::shared_ptr<boost::cobalt::channel<Replica::ReplyVariant>> rx) {
    co_return co_await rx->read();
}

boost::cobalt::task<Replica::State>
Replica::leader_fsm(boost::asio::ip::tcp::acceptor& acceptor) {

    impl.state = Leader;
    impl.leader = {};

    auto io = co_await boost::cobalt::this_coro::executor;

    vector<std::shared_ptr<cobalt::channel<Replica::RequestVariant>>>
        follower_req;
    auto follower_reply =
        std::make_shared<cobalt::channel<Replica::ReplyVariant>>(8, io);

    auto client_req = std::make_shared<cobalt::channel<ClientReq>>(8, io);
    auto client_reply =
        std::make_shared<cobalt::channel<Replica::ReplyVariant>>(8, io);

    auto replica_req =
        std::make_shared<cobalt::channel<Replica::RequestVariant>>(8, io);

    AppendEntryReq heartbeat = {};
    heartbeat.term = pstate.currentTerm;
    heartbeat.leaderId = impl.my_addr;
    if (pstate.logs.size()) {
        heartbeat.prevLogIndex = pstate.logs.size() - 1;
        heartbeat.prevLogTerm = pstate.logs.back().first;
    }
    heartbeat.leaderCommit = vstate.commitIndex;

    /* spawn follower_handlers */
    for (auto k = 0; k < impl.cluster.size(); ++k) {
        follower_req.push_back(
            std::make_shared<cobalt::channel<Replica::RequestVariant>>(8, io));
        auto peer_addr = impl.cluster[k];
        if (peer_addr != impl.my_addr) {
            cobalt::spawn(
                io,
                follower_handler(peer_addr, follower_req[k], follower_reply),
                asio::detached);
        }
    }

    /* block forever */
    asio::steady_timer cancel{io};
    cancel.expires_at(decltype(cancel)::time_point::max());

    /* spawn rx_connection handler */
    cobalt::spawn(io, rx_conn_leader(acceptor, client_req, cancel),
                  asio::detached);

    while (true) {

        /* Wait for request from either client or replica group */

        auto nx = co_await race(client_req->read(), replica_req->read());
        if (nx.index() == 0) {

            /* process one client request at a time */

            /* forward request to all followers */
            auto [req, pipe] = get<0>(nx);

            /* forward to all followers */
            int cnt = 0;
            for (auto& f : follower_req) {
                co_await f->write(req);
            }

            /* wait until all followers respond */
            while (cnt > 0) {
                auto reply_var = co_await follower_reply->read();
                if (reply_var.index() == 0) {
                } else if (reply_var.index() == 1) {
                }
            }
        } else if (nx.index() == 1) {
        }
    }

    // auto nx = co_await race(client_req->read(), follower_reply->read());

    /*
     * Requirements:
     *  1. Relay replicate log command to each follower
     *      client -> leader_rx -> follower_tx
     *  2. Become follower if any follower reporting newer term
     *      follower_tx -> leader_rx
     */

#if 0
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
#endif

    co_return Follower;
}

auto Replica::rx_conn_leader(
    asio::ip::tcp::acceptor& acceptor,
    std::shared_ptr<boost::cobalt::channel<ClientReq>> tx,
    asio::steady_timer& cancel) -> cobalt::task<void> {

    auto io = co_await boost::cobalt::this_coro::executor;

    auto wait_for_cancel = [&]() -> boost::cobalt::task<void> {
        boost::system::error_code ec;
        co_await cancel.async_wait(
            boost::asio::redirect_error(boost::cobalt::use_task, ec));
    };

    while (true) {

        bool teardown = false;

        auto nx = co_await boost::cobalt::race(
            acceptor.async_accept(boost::cobalt::use_task), wait_for_cancel());
        switch (nx.index()) {
        case 0: {

            auto& socket = get<0>(nx);
            boost::cobalt::spawn(
                io, rx_payload_leader(std::move(socket), tx, cancel),
                asio::detached);
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
};

auto Replica::rx_payload_leader(
    boost::asio::ip::tcp::socket socket,
    std::shared_ptr<boost::cobalt::channel<ClientReq>> tx,
    boost::asio::steady_timer& cancel) -> boost::cobalt::task<void> {

    auto io = co_await boost::cobalt::this_coro::executor;

    /* connections are dynamic - allocate rx channel on the fly */
    auto rx = std::make_shared<cobalt::channel<Replica::ReplyVariant>>(8, io);

    while (true) {
        /* repeat until socket closure */
        try {
            /* get payload */
            char data[1024] = {};
            std::size_t n = co_await socket.async_read_some(
                boost::asio::buffer(data), boost::cobalt::use_task);
            auto req_var = deserialize<Replica::RequestVariant>(string(data));

            /* write to request and wait for response */
            co_await tx->write(std::make_tuple(req_var, rx));
            auto reply_var = co_await rx->read();

            auto reply_s =
                serialize<Replica::ReplyVariant>(std::move(reply_var));
            co_await asio::async_write(socket, asio::buffer(reply_s),
                                       cobalt::use_task);
        } catch (std::exception& e) {
            cout << "rx_payload_leader(): socket closed?" << endl;
        }
    }

    co_return;
}