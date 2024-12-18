#include "node/replica.hh"

AppendEntryReply Replica::process_addEntry(const AppendEntryReq& entry) {

    /* Receiver implementation 1 */
    if (entry.term < currentTerm) {
        return {currentTerm, false};
    }

    /* Receiver implementation 2 */
    if (entry.prevLogIndex >= pstate.logs.size()) {
        /* follower is not up to date */
        return false;
    }

    /* Receiver implementation 3 */
    if (pstate.logs[entry.prevLogIndex].first != entry.prevLogTerm) {

        /* follower has diverging history */

        /*
         *
         * x, x, x, ...
         *       ^   assume prevLogIndex == 2 and disagrees
         */

        /* drop diverged history */
        pstate.logs.resize(entry.prevLogIndex);

        return false;
    }

    /* matching history */

    /* Receiver implementation 4 */
    pstate.logs.push_back(entry

    /* TODO: Receiver implementation 5 */
    if (entry.leaderCommit > commitIndex) {
    }
}