#include "node/replica.hh"
#include "replica.hh"
#include <optional>
#include <vector>

using namespace std;

boost::cobalt::task<Replica::RequestVoteReply>
Replica::request_vote_from_peer(std::string& peer_addr) {

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

    Replica::RequestVariant reqv = {};
    auto reqs = serialize(reqv);

    //     std::string req =
    //         "v:" + std::to_string(i) + "-" + std::to_string(j);
    co_await boost::asio::async_write(
        socket, boost::asio::buffer(reqs.c_str(), reqs.size()),
        boost::cobalt::use_task);

    //     /* read results */
    //     char payload[1024] = {};
    //     auto n = co_await socket.async_read_some(
    //         boost::asio::buffer(payload), boost::cobalt::use_task);

    Replica::RequestVoteReply reply{};
    co_return reply;
}

template <>
boost::cobalt::task<void> Replica::rx_connection<Replica::Candidate>(
    boost::asio::ip::tcp::acceptor& acceptor,
    boost::asio::steady_timer& cancel) {

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

            char data[1024] = {};
            std::size_t n = co_await socket.async_read_some(
                boost::asio::buffer(data), boost::cobalt::use_task);

            /* TODO: deserialize the payload here */
            Replica::RequestVariant req_var;
            auto [state, reply_var] = rx_payload_handler<Candidate>(req_var);

            /* TODO: serialize reply_var */
            // co_await boost::asio::async_write(
            //     socket, boost::asio::buffer(reply.c_str(), reply.size()),
            //     boost::cobalt::use_task);

            if (state != Replica::Leader) {
                /* processing the payload is forcing a step down */
                teardown = true;
            }

        } break;
        case 1: {
            teardown = true;
        } break;
        }

        if (teardown) {
            break;
        }
    }
}

boost::cobalt::task<Replica::State>
Replica::candidate_fsm(boost::asio::ip::tcp::acceptor& acceptor) {

#if 0
    auto candidate_rx_payload_handler =
        [this](const Replica::RequestVariant& variant)
        -> boost::cobalt::task<void> {
        switch (variant.index()) {
        case 0: {
            /* append entries */
            auto reply =
                co_await add_entries<Replica::Candidate>(get<0>(variant));
        } break;
        case 1: {
            // auto req = variant.value();
            auto reply =
                co_await request_vote<Replica::Candidate>(get<1>(variant));
        } break;
        }
    };

    impl.state = Candidate;

    rx_payload_handler = [&](const RequestVariant& variant) {
        /* need to trampoline through a lambda because rx_payload_handler
         * parameters is missing the implicit "this" argument */
        return candidate_rx_payload_handler(variant);
    };

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
#endif

    impl.state = Candidate;
    // impl.candidate = {};

    auto io = co_await boost::cobalt::this_coro::executor;

    boost::asio::steady_timer cancel{io};
    cancel.expires_after(
        std::chrono::milliseconds(1000)); /* TODO: block forever */

    auto rx_coro = boost::cobalt::spawn(
        io, rx_connection<Replica::Candidate>(acceptor, cancel),
        boost::cobalt::use_task);

    auto wait_for_cancel = [&]() -> boost::cobalt::task<void> {
        boost::system::error_code ec;
        co_await cancel.async_wait(
            boost::asio::redirect_error(boost::cobalt::use_task, ec));
    };

    vector<boost::cobalt::task<Replica::RequestVoteReply>> reqs;

    for (auto peer_addr : impl.cluster) {
        if (peer_addr != impl.my_addr) {
            reqs.push_back(request_vote_from_peer(peer_addr));
        }
    }

    /* wait until either all reqs are serviced or timeout */

    auto rv = co_await boost::cobalt::race(
        boost::cobalt::gather(std::move(reqs)), timeout(150));

#if 0
    while (true) {
        /* wait for heartbeat timeout */
        /* TODO: randomized timeout? */
        bool stepping_down = false;
        auto nx = co_await boost::cobalt::race(timeout(150), wait_for_cancel());
        switch (nx.index()) {
        case 0: {
            /* TODO: heartbeat - establish authority */
        } break;
        case 1: {
            /* stepping down */
            stepping_down = true;
        } break;
        }

        if (stepping_down) {
            break;
        }
    }

    /* become a follower after stepping down */

    /* wait for rx_connection to complete */
    cancel.cancel();
    co_await rx_coro;
#endif

    co_return Follower;
}

// template auto Replica::rx_payload_handler<Replica::Candidate>(
//     const Replica::RequestVariant& variant)
//     -> tuple<State, Replica::ReplyVariant>;