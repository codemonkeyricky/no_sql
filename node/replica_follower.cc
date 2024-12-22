#include "node/replica.hh"
#include "replica.hh"

/*
 * If we are a follower
 *  Only need to worry about history convergence
 */

using namespace std;

bool Replica::at_least_as_up_to_date_as_me(int peer_last_log_index,
                                           int peer_last_log_term) {

    if (pstate.logs.empty()) {
        /* peer can only be same or more recent */
        return true;
    } else if (pstate.logs.back().first < peer_last_log_term) {
        /* my log isn't recent */
        return true;
    }

    else if (pstate.logs.back().first > peer_last_log_term) {
        return false;
    }

    else /* same */ {
        return peer_last_log_index + 1 >= pstate.logs.size();
    }
}

boost::cobalt::task<Replica::State>
Replica::follower_fsm(boost::asio::ip::tcp::acceptor& acceptor) {

    impl.state = Leader;
    impl.leader = {};

    auto io = co_await boost::cobalt::this_coro::executor;

    boost::asio::steady_timer cancel{io};
    cancel.expires_after(
        std::chrono::milliseconds(1000)); /* TODO: block forever */

    auto rx_coro = boost::cobalt::spawn(
        io, rx_connection<Replica::Leader>(acceptor, cancel),
        boost::cobalt::use_task);

    auto wait_for_cancel = [&]() -> boost::cobalt::task<void> {
        boost::system::error_code ec;
        co_await cancel.async_wait(
            boost::asio::redirect_error(boost::cobalt::use_task, ec));
    };

    while (true) {

        co_await timeout(150);

        if (!impl.follower.keep_alive) {
            break;
        }

        impl.follower.keep_alive = false;
    }

    /* become a follower after stepping down */

    /* wait for rx_connection to complete */
    cancel.cancel();
    co_await rx_coro;

    co_return Replica::Candidate;
}