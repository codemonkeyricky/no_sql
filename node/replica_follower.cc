#include "node/replica.hh"

/*
 * If we are a follower
 *  Only need to worry about history convergence
 */

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

boost::cobalt::task<void>
Replica::follower_fsm(boost::asio::ip::tcp::acceptor) {

#if 0
    auto follower_rx_payload_handler =
        [this](const Replica::RequestVariant& variant)
        -> boost::cobalt::task<void> {
        switch (variant.index()) {
        case 0: {
            /* append entries */
            auto reply =
                co_await add_entries<Replica::Follower>(get<0>(variant));
        } break;
        case 1: {
            // auto req = variant.value();
            auto reply =
                co_await request_vote<Replica::Follower>(get<1>(variant));
        } break;
        }
    };

    impl.state = Follower;

    rx_payload_handler = [&](const RequestVariant& variant) {
        /* need to trampoline through a lambda because rx_payload_handler
         * parameters is missing the implicit "this" argument */
        return follower_rx_payload_handler(variant);
    };

    while (true) {
        /* wait for heartbeat timeout */
        co_await timeout(150);

        if (!impl.follower.keep_alive)
            break;

        impl.follower.keep_alive = false;
    }

    /* failed to receive leader heartbeat - start campaigning */

    auto io = co_await boost::cobalt::this_coro::executor;
    boost::cobalt::spawn(io, candidate_fsm(), boost::asio::detached);
#endif
}