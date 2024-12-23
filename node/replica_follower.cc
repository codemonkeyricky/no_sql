#include "node/replica.hh"
#include "replica.hh"

#include <iostream>

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

boost::cobalt::task<void> dummy() {
    cout << "dummy1" << endl;

    auto io = co_await boost::cobalt::this_coro::executor;
    boost::asio::steady_timer timer{io};
    timer.expires_after(std::chrono::seconds(1));
    co_await timer.async_wait(boost::cobalt::use_task);

    cout << "dummy1 after timeout" << endl;

    co_return;
}
boost::cobalt::task<void> dummy2() {
    cout << "dummy2" << endl;
    co_return;
}

boost::cobalt::task<Replica::State>
Replica::follower_fsm(boost::asio::ip::tcp::acceptor& acceptor) {

    impl.state = Follower;
    impl.follower = {};

    auto io = co_await boost::cobalt::this_coro::executor;

    boost::asio::steady_timer cancel{io};
    cancel.expires_at(
        decltype(cancel)::time_point::max()); /* TODO: block forever */

    // auto task = boost::cobalt::spawn(io, dummy(), boost::cobalt::use_task);

    // task->finally([&]() { active_tasks.erase(task); });
    // task->finally

    //     co_await dummy();

    // boost::cobalt::spawn(io, dummy(), boost::asio::detached);
    // boost::cobalt::spawn(io, dummy2(), boost::asio::detached);

    boost::cobalt::spawn(io, rx_connection<Replica::Follower>(acceptor, cancel),
                         boost::asio::detached);

    auto wait_for_cancel = [&]() -> boost::cobalt::task<void> {
        boost::system::error_code ec;
        co_await cancel.async_wait(
            boost::asio::redirect_error(boost::cobalt::use_task, ec));
    };

    while (true) {

        if (impl.my_addr == "127.0.0.1:5555") {
            co_await timeout(150);
        } else {
            co_await timeout(100 * 1000);
        }

        if (!impl.follower.keep_alive) {
            break;
        }

        impl.follower.keep_alive = false;
    }

    /* become a follower after stepping down */

    /* wait for rx_connection to complete */
    cancel.cancel();
    // co_await rx_coro;

    cout << "follower fsm after spawns" << endl;

    co_return Replica::Candidate;
}