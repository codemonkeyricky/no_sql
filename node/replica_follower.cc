#include "node/replica.hh"

boost::cobalt::task<Replica::AppendEntryReply>
Replica::follower_add_entries_req(const Replica::AppendEntryReq& req) {

    /* Receiver implementation 1 */
    if (req.term < pstate.currentTerm) {
        co_return Replica::AppendEntryReply{pstate.currentTerm, false};
    }

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
         * Note that re-applying already applied logs is assumed safe - this can
         * happen since commitIndex is volatile and server can crash right after
         * applying committed logs.
         */
        boost::cobalt::spawn(co_await boost::cobalt::this_coro::executor,
                             apply_logs(), boost::asio::detached);
    }

    co_return {pstate.currentTerm, true};
}

boost::cobalt::task<void> Replica::follower_fsm() {

    impl.state = Follower;

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
}