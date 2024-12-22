
#include "node/replica.hh"
#include "replica.hh"
#include <optional>

using namespace std;

template <Replica::State T>
auto Replica::rx_payload_handler(const Replica::RequestVariant& variant)
    -> tuple<State, Replica::ReplyVariant> {

    State s = impl.state;
    ReplyVariant rv;
    switch (variant.index()) {
    case 0: {
        /* append entries */
        auto [s, reply] = add_entries<T>(get<0>(variant));
        rv = ReplyVariant(reply);
    } break;
    case 1: {
        auto [s, reply] = request_vote<T>(get<1>(variant));
        rv = ReplyVariant(reply);
    } break;
    }

    return {s, rv};
};

/* instantiate specialization */
template auto Replica::rx_payload_handler<Replica::Candidate>(
    const Replica::RequestVariant& variant)
    -> tuple<State, Replica::ReplyVariant>;

template auto Replica::rx_payload_handler<Replica::Leader>(
    const Replica::RequestVariant& variant)
    -> tuple<State, Replica::ReplyVariant>;

template <Replica::State T>
auto Replica::request_vote(const Replica::RequestVoteReq& req)
    -> std::tuple<Replica::State, Replica::RequestVoteReply> {

    auto& [term, candidateId, lastLogIndex, lastLogTerm] = req;

    Replica::RequestVoteReply reply = {};

    /* under no scenario should we cache a vote as leader - we either reject or
     * vote and step down */

    bool grant = false;
    if (term < pstate.currentTerm) {
        /* candidate is out of date */
        return {impl.state, {pstate.currentTerm, false}};
    }

    if (pstate.votedFor) {
        if (*pstate.votedFor == candidateId) {
            /* have previously voted for this candidate - this can happen due to
             * unfavourable network condition */

            assert(impl.state == Replica::Follower);
            return {Replica::Follower, {pstate.currentTerm, true}};
        } else {
            /* already voted for someone else. we can either be a follower or
             * candidate (ie. voted for ourselves )*/
            return {impl.state, {pstate.currentTerm, false}};
        }
    }

    /* we have not yet voted - only grant vote if candidate's logs are as up to
     * date. The following is a series of up to date checks */
    grant = false;
    if (pstate.logs.empty()) {
        /* our log is empty... candidate must be as up to date */
        grant = true;
    } else if (pstate.logs.back().first < lastLogTerm) {
        /* candidate has logs from later terms */
        grant = true;
    } else if (pstate.logs.back().first == lastLogTerm &&
               lastLogIndex + 1 >= pstate.logs.size()) {
        /* candidate have logs from the same term */
        grant = true;
    }

    /* always update the term to latest - but we can still reject request
     * requestVote if candidate's history is not as up to date */
    pstate.currentTerm = max(pstate.currentTerm, term);

    if (grant) {
        /* grant vote and become a follower */
        pstate.votedFor = candidateId;
        return {Replica::Follower, {pstate.currentTerm, true}};
    } else {
        /* candidate doesn't have as up to date logs */
        return {impl.state, {pstate.currentTerm, false}};
    }
}

template auto
Replica::request_vote<Replica::Leader>(const Replica::RequestVoteReq& variant)
    -> tuple<Replica::State, Replica::RequestVoteReply>;

template auto Replica::request_vote<Replica::Candidate>(
    const Replica::RequestVoteReq& variant)
    -> tuple<Replica::State, Replica::RequestVoteReply>;

template auto
Replica::request_vote<Replica::Follower>(const Replica::RequestVoteReq& variant)
    -> tuple<Replica::State, Replica::RequestVoteReply>;

template <Replica::State T>
auto Replica::add_entries(
    const Replica::AppendEntryReq& req)
    -> tuple<Replica::State, Replica::AppendEntryReply> {

    auto& [term, leaderId, prevLogIndex, prevLogTerm, leaderCommit, entry] =
        req;

    /*
     * Raft paper 5.2:
     *
     * While waiting for votes, a candidate may receive an
     * AppendEntries RPC from another server claiming to be
     * leader. If the leader’s term (included in its RPC) is at least
     * as large as the candidate’s current term, then the candidate
     * recognizes the leader as legitimate and returns to follower
     * state. If the term in the RPC is smaller than the candidate’s
     * current term, then the candidate rejects the RPC and con-
     * tinues in candidate state.
     */

    if (term < pstate.currentTerm) {
        /* leader is stale - reject */
        return {impl.state, {pstate.currentTerm, false}};
    }

    /* accept leader - but only accept log if history match */

    if (term >= pstate.currentTerm) {
        /* recognize leader - drop down to follower */

        /* however, may need to force leader to walk history backwards */

        /* find common ancester */
        bool success = false;
        if (pstate.logs.size() - 1 < prevLogIndex) {
            /* Our log is too small. Force leader to find a common ancestor
             */
            success = false;
        } else if (pstate.logs[prevLogIndex].first != prevLogTerm) {
            /* log exist, but term disagrees. ask leader to keep walking
             * backwards to find common history */
            pstate.logs.resize(prevLogIndex);
            success = false;
        }

        pstate.currentTerm = max(pstate.currentTerm, term);

        return {Replica::Follower, {pstate.currentTerm, success}};
    } else {
        /* Current leader is stale - reject */
        return {impl.state, {pstate.currentTerm, false}};
    }
}
