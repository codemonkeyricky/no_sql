#include "node/replica.hh"
#include "replica.hh"
#include <array>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/use_future.hpp>
#include <chrono>
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
    std::shared_ptr<cobalt::channel<Replica::ReplyVariant>> tx,
    asio::steady_timer& cancel) {

    ++coro_cnt;

    AppendEntryReq heartbeat = {};
    heartbeat.term = pstate.currentTerm;
    heartbeat.leaderId = impl.replica_addr;
    heartbeat.prevLogIndex = pstate.logs.size() - 1;
    heartbeat.prevLogTerm = pstate.logs.size() ? pstate.logs.back().term : -1;
    heartbeat.leaderCommit = vstate.commitIndex;

    int matchIndex;
    int nextIndex;

    auto getCommonAncestor = [&]() -> cobalt::task<bool> {
        /* catching the replica up to speed */
        while (true) {
            auto reply_variant =
                co_await send_rpc(peer_addr, RequestVariant() = {heartbeat});

            auto [term, success] = get<0>(reply_variant);
            if (term > pstate.currentTerm) {
                co_return false;
            } else if (term < pstate.currentTerm) {
                /* peer is either higher or same - lower is implementation fault
                 */
                assert(0);
            } else {
                if (success) {
                    /* found matching history */
                    co_return true;
                } else {
                    if (heartbeat.prevLogIndex >= 0) {
                        heartbeat.prevLogTerm =
                            pstate.logs[--heartbeat.prevLogIndex].term;
                    } else {
                        /* peer still disagree without any logs - implementation
                         * fault */
                        assert(0);
                    }
                }
            }
        }
    };

    auto replayHistory = [&]() -> cobalt::task<bool> {
        /* replay until catch up */
        AppendEntryReq replay = heartbeat;
        while (nextIndex < pstate.logs.size()) {

            replay.prevLogIndex = nextIndex - 1;
            replay.prevLogTerm = pstate.logs[nextIndex - 1].term;
            replay.entry = pstate.logs[nextIndex];

            auto reply_variant =
                co_await send_rpc(peer_addr, RequestVariant() = {heartbeat});
            auto [term, success] = get<0>(reply_variant);
            if (success) {
                matchIndex = nextIndex++;
            }

            /* TODO: I think the only way this can fail is if follower become
             * new leader? */
            assert(success);
        }

        co_return true;
    };

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

    if (!(co_await getCommonAncestor())) {
        /* TODO: common ancestor must exist - error means the follower became a
         * leader. revert back to follower */
    }

    if (!co_await replayHistory()) {
        /* TODO: error likely means the follower became a
         * leader. revert back to follower */
    }

    matchIndex = heartbeat.prevLogIndex;
    nextIndex = heartbeat.prevLogIndex + 1;

    /* manages the connection, including heartbeat and catching the replica
     * up to speed */

    asio::steady_timer keep_alive{io};

    while (rx->is_open()) {
        ReplyVariant reply_var;

        keep_alive.expires_after(std::chrono::milliseconds(100));
        auto nx = co_await race(rx->read(),
                                keep_alive.async_wait(boost::cobalt::use_task),
                                cancel.async_wait(boost::cobalt::use_task));
        if (nx.index() == 0) {
            /* leader issues command */
            co_await tx->write(co_await send_rpc(peer_addr, get<0>(nx)));
        } else if (nx.index() == 1) {
            /* heartbeat */
            auto var = RequestVariant(heartbeat);
            auto reply_var = co_await send_rpc(peer_addr, var);
            auto reply = get<0>(reply_var);
            if (reply.term > pstate.currentTerm) {
                /* peer has higher term */
                co_await tx->write(reply);
                break;
            }
        } else if (nx.index() == 2) {
            /* tearing down */
            break;
        }
    }

    --coro_cnt;

    co_return;
}

template <typename T>
auto proxy_read(std::shared_ptr<cobalt::channel<T>> rx)
    -> boost::cobalt::task<T> {
    auto rv = co_await rx->read();
    co_return rv;
}

boost::cobalt::task<Replica::State>
Replica::leader_fsm(boost::asio::ip::tcp::acceptor& replica_acceptor,
                    boost::asio::ip::tcp::acceptor& client_acceptor) {

    impl.state = Leader;
    impl.leader = {};

    auto io = co_await boost::cobalt::this_coro::executor;

    /* many to one */
    vector<std::shared_ptr<cobalt::channel<Replica::RequestVariant>>>
        follower_req;
    auto follower_reply =
        std::make_shared<cobalt::channel<Replica::ReplyVariant>>(8, io);

    /* one to many*/
    auto client_req = std::make_shared<cobalt::channel<ClientReq>>(8, io);
    auto client_reply =
        std::make_shared<cobalt::channel<Replica::ReplyVariant>>(8, io);

    /* many to one */
    auto replica_req = std::make_shared<cobalt::channel<ReplicaReq>>(8, io);

    /* cancel interrupt */
    asio::steady_timer cancel{io};
    cancel.expires_at(decltype(cancel)::time_point::max()); /* block forever */

    /* spawn follower_handlers */
    for (auto k = 0; k < impl.cluster.size(); ++k) {
        follower_req.push_back(
            std::make_shared<cobalt::channel<Replica::RequestVariant>>(8, io));
        auto peer_addr = impl.cluster[k];
        if (peer_addr != impl.replica_addr) {
            cobalt::spawn(io,
                          follower_handler(peer_addr, follower_req[k],
                                           follower_reply, cancel),
                          asio::detached);
        }
    }

    /* spawn rx client connection handler */
    cobalt::spawn(io, rx_client_conn(client_acceptor, client_req, cancel),
                  asio::detached);

    /* spawn rx replica connection handler */
    cobalt::spawn(io, rx_replica_conn(replica_acceptor, replica_req, cancel),
                  asio::detached);

    while (true) {

        /* Wait for request from either client or replica group */

        auto nx = co_await cobalt::race(proxy_read(client_req),
                                        proxy_read(replica_req),
                                        proxy_read(follower_reply));

        if (nx.index() == 0) {

            /* process one client request at a time */

            /* forward request to all followers */
            auto [req, tx] = get<0>(nx);

            /* forward to all followers */
            int cnt = 0;
            for (auto& f : follower_req) {
                co_await f->write(req);
            }

            /* wait until majority respond */
            while (cnt + 1 < impl.cluster.size() / 2) {
                auto reply_var = co_await follower_reply->read();
                /* only expect appendEntries for now */
                assert(reply_var.index() == 0);

                // auto reply =
            }
        } else if (nx.index() == 1) {
            /*
             * replica request
             *
             * replica can request appendEntries or requestVote.
             */

            bool become_follower = false;
            auto [req_var, tx] = get<1>(nx);
            if (req_var.index() == 0) {
                /* append entries */
                auto& [term, leaderId, prevLogIndex, prevLogTerm, leaderCommit,
                       entry] = get<0>(req_var);

                /* while we can have more than one leader, they must be on
                 * different term! */
                assert(term != pstate.currentTerm);

                if (term > pstate.currentTerm) {
                    become_follower = true;

                    /* TODO: return the *wrong* term force the new leader to
                     * try again. I thinnk the spec actually wants us to
                     * respond as follower directly */
                    AppendEntryReply reply = {pstate.currentTerm, false};

                    co_await tx->write(ReplyVariant(reply));

                    pstate.currentTerm = term;
                    become_follower = true;

                } else if (term < pstate.currentTerm) {
                    /* ignore */
                }
            } else {
                /* request vote */
                // auto [term, vote] = get<1>(req_var);
            }

            if (become_follower) {
                break;
            }
        } else if (nx.index() == 2) {
            assert(0);
        }
    }

    cancel.cancel();
    while (coro_cnt > 0) {
        timeout(10);
    }

    /*
     * wait for all coroutines to drain. This includes:
     * rx client connection(s), rx replica connection(s)
     */

    co_return Follower;
}

auto Replica::rx_client_conn(
    asio::ip::tcp::acceptor& client_acceptor,
    std::shared_ptr<boost::cobalt::channel<ClientReq>> tx,
    asio::steady_timer& cancel) -> cobalt::task<void> {

    ++coro_cnt;

    auto io = co_await boost::cobalt::this_coro::executor;

    auto wait_for_cancel = [&]() -> boost::cobalt::task<void> {
        boost::system::error_code ec;
        co_await cancel.async_wait(
            boost::asio::redirect_error(boost::cobalt::use_task, ec));
    };

    while (true) {

        bool teardown = false;

        auto nx = co_await boost::cobalt::race(
            client_acceptor.async_accept(boost::cobalt::use_task),
            wait_for_cancel());
        switch (nx.index()) {
        case 0: {

            auto& socket = get<0>(nx);
            boost::cobalt::spawn(
                io, rx_client_payload(std::move(socket), tx, cancel),
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

    --coro_cnt;
};

auto Replica::rx_client_payload(
    boost::asio::ip::tcp::socket socket,
    std::shared_ptr<boost::cobalt::channel<ClientReq>> tx,
    boost::asio::steady_timer& cancel) -> boost::cobalt::task<void> {

    ++coro_cnt;

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
            cout << "rx_client_payload(): socket closed?" << endl;
        }
    }

    --coro_cnt;

    co_return;
}

auto Replica::rx_replica_conn(
    boost::asio::ip::tcp::acceptor&,
    std::shared_ptr<boost::cobalt::channel<ReplicaReq>> tx,
    boost::asio::steady_timer& cancel) -> boost::cobalt::task<void> {
    co_return;
}
