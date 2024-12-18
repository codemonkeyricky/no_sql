#include "node/replica.hh"

Replica::AppendEntryReply
Replica::process_addEntry(const Replica::AppendEntryReq& req) {

    /* Receiver implementation 1 */
    if (req.term < pstate.currentTerm) {
        return Replica::AppendEntryReply{pstate.currentTerm, false};
    }

    /* Receiver implementation 2 */
    if (req.prevLogIndex >= pstate.logs.size()) {
        /* follower is not up to date */
        return {pstate.currentTerm, false};
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

        return {pstate.currentTerm, false};
    }

    /* history must match at this point */

    /* Receiver implementation 4 */
    if (req.entry)
        pstate.logs.push_back({pstate.currentTerm, *req.entry});

    /* Receiver implementation 5 */
    if (req.leaderCommit > vstate.commitIndex) {

        /* log ready to be executed */
        vstate.commitIndex = min(req.leaderCommit, pstate.logs.size() - 1);
    }

    return {pstate.currentTerm, true};
}