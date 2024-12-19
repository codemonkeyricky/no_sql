#include "node/replica.hh"
#include "replica.hh"
#include <optional>

using namespace std;

static boost::cobalt::task<Replica::RequestVoteReply>
request_vote(std::string peer_addr) {

    auto io = co_await boost::cobalt::this_coro::executor;

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

    //     std::string req =
    //         "v:" + std::to_string(i) + "-" + std::to_string(j);
    //     co_await boost::asio::async_write(
    //         socket, boost::asio::buffer(req.c_str(), req.size()),
    //         boost::cobalt::use_task);

    //     /* read results */
    //     char payload[1024] = {};
    //     auto n = co_await socket.async_read_some(
    //         boost::asio::buffer(payload), boost::cobalt::use_task);

    Replica::RequestVoteReply reply{};
    co_return reply;
}

boost::cobalt::task<void> timeout(int ms) {
    boost::asio::steady_timer timer{
        co_await boost::cobalt::this_coro::executor};
    timer.expires_after(std::chrono::milliseconds(ms));

    co_await timer.async_wait(boost::cobalt::use_op);

    co_return;
}

boost::cobalt::task<void> Replica::candidate_fsm() {

    auto io = co_await boost::asio::this_coro::executor;

    vector<boost::cobalt::task<Replica::RequestVoteReply>> reqs;

    for (auto peer_addr : impl.cluster) {
        reqs.push_back(request_vote(peer_addr));
    }

    /* wait until either all reqs are serviced or timeout */

    auto rv = co_await boost::cobalt::race(
        boost::cobalt::gather(std::move(reqs)), timeout(150));

    bool leader = true;

    switch (rv.index()) {
    case 0: {
        /* all requests finished */
        cout << "candidate_campaign(): All request_vote() completed!" << endl;
        auto replies = get<0>(rv);

        int highest_term = 0;
        int vote_cnt = 0;

        for (auto& reply_variant : replies) {
            auto [term, vote] = reply_variant.value();
            highest_term = max(highest_term, term);
            vote_cnt += vote;
        }

        if (highest_term > pstate.currentTerm) {
            /* someone was elected? */
            leader = false;
        } else if (vote_cnt < impl.cluster.size() / 2 + 1) {
            /* failed to achieve majority vote */
            leader = false;
        }

    } break;
    case 1: {
        cout << "candidate_campaign(): request_vote() timeout!" << endl;
        leader = false;
    } break;
    }

    if (leader) {
        /* transition to be a leader */
        boost::cobalt::spawn(io, leader_fsm(), boost::asio::detached);
    } else {
        /* transition to be a follower */
        boost::cobalt::spawn(io, follower_fsm(), boost::asio::detached);
    }
}

// boost::cobalt::task<bool> Replica::candidate_rx_request_vote() {}
// boost::cobalt::task<bool> Replica::candidate_rx_append_entry() {}