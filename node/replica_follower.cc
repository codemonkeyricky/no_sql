#include "node/replica.hh"

/*
 * If we are a follower
 *  Only need to worry about history convergence
 */

bool Replica::at_least_as_up_to_date_as_me(int peer_last_log_index,
                                           int peer_last_log_term) {

#if 0
    if (pstate.logs.empty()) {
        /* peer can only be same or more recent */
        return true;
    } else if (pstate.logs.back().first < peer_last_log_term) {
        /* my log isn't recent */
        return true;
    } else if (pstate.logs.back().first > peer_last_log_term) {
        return false;
    } else /* same */ {
        return peer_last_log_index + 1 >= pstate.logs.size();
    }
#endif
}

template <>
std::tuple<Replica::State, Replica::RequestVoteReply>
Replica::request_vote<Replica::Follower>(const Replica::RequestVoteReq& req) {
    auto& [term, candidateId, lastLogIndex, lastLogTerm] = req;

    bool grant = false;

#if 0
    if (term < pstate.currentTerm) {
        /* We have seen higher term - reject */
    } else {
        /* compare logs */
        if (pstate.voteFor == nullopt || *pstate.votedFor == candidateId) {
            /* It's possible for the candidate to "lose" the previous reply due
             * to unfavourable network conditions */
            grant = at_least_as_up_to_date_as_me(lastLogIndex, lastLogTerm);
        }
    }

    pstate.currentTerm = std::max(pstate.currentTerm, term);

    return {Replica::Follower, {pstate.currentTerm, grant}};
#endif
}

// template <>
// std::tuple<Replica::State, Replica::AppendEntryReply>
// Replica::add_entries<Replica::Follower>(const Replica::AppendEntryReq& req)
// {}

#if 0
template <>
Replica::AppendEntryReply
Replica::add_entries<Replica::Follower>(const Replica::AppendEntryReq& req) {

    /* Receiver implementation 1 */
if (req.term < pstate.currentTerm) {
    return Replica::AppendEntryReply{pstate.currentTerm, false};
}
#if 0

    /* Receiver implementation 2 */
    if (req.prevLogIndex >= pstate.logs.size()) {
        /* follower is not up to date */
        co_return {pstate.currentTerm, false};
    }

    /* Receiver implementation 3 */
    if (pstate.logs[req.prevLogIndex].first != req.prevLogTerm) {

        /* follower has diverging history */

        /*
         *
         * x, x, x, ...
         *       ^   assume prevLogIndex == 2 and disagrees
         */

        /* drop diverged history */
        pstate.logs.resize(req.prevLogIndex);

        co_return {pstate.currentTerm, false};
    }

    /* history must match at this point */

    /* Receiver implementation 4 */
    if (req.entry) {
        pstate.logs.push_back({pstate.currentTerm, *req.entry});
    }

    /* Receiver implementation 5 */
    if (req.leaderCommit > vstate.commitIndex) {

        /* log ready to be executed */
        vstate.commitIndex =
            std::min(req.leaderCommit, (int)pstate.logs.size() - 1);

        /*
         * Apply all committed logs.
         *
         * Note that re-applying already applied logs is assumed safe - this
         can
         * happen since commitIndex is volatile and server can crash right
         after
         * applying committed logs.
         */
        boost::cobalt::spawn(co_await boost::cobalt::this_coro::executor,
                             apply_logs(), boost::asio::detached);
    }

    co_return {pstate.currentTerm, true};
#endif
}
#endif

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