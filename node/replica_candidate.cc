#include "node/replica.hh"
#include "replica.hh"
#include <optional>

using namespace std;

boost::cobalt::task<Replica::RequestVoteReply>
Replica::candidate_request_vote(std::string peer_addr) {

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

    RequestVoteReply reply{};
    co_return reply;
}

boost::cobalt::task<bool> Replica::candidate_campaign() {

    auto io = co_await boost::asio::this_coro::executor;

    int highest_term = 0;
    int vote_cnt = 0;

    for (auto peer_addr : impl.cluster) {
        auto [term, vote_granted] = co_await candidate_request_vote(peer_addr);
        vote_cnt += vote_cnt;
        highest_term = max(highest_term, term);
    }

    if (highest_term > pstate.currentTerm) {
        /* someone was elected? */
        co_return false;
    }

    if (vote_cnt < impl.cluster.size() / 2 + 1) {
        /* failed to achieve majority vote */
        co_return false;
    }

    /* transition to leader */

    co_return true;
}

boost::cobalt::task<bool> Replica::candidate_rx_request_vote() {}
boost::cobalt::task<bool> Replica::candidate_rx_append_entry() {}