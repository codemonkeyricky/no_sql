#include "node/replica.hh"
#include "replica.hh"
#include <optional>

using namespace std;

template <>
tuple<Replica::State, Replica::RequestVoteReply>
Replica::request_vote<Replica::Candidate>(const Replica::RequestVoteReq& req) {

    /*
     * If we are a candidate, we have already voted for ourselves. However, we
     * can still go back to follower state if new leader or higher term election
     * started.
     *
     * If we receive requestVote RPC as a candidate:
     *  1. The other candidiate is starting a new term - in which cas we drop
     *     back to follower.
     *  2. The other candidate is competing for a stale term - ignore.
     *  3. The other candidate is competing for the same term - don't care as we
     *     already voted ourselves.
     */

    auto& [term, candidateId, lastLogIndex, lastLogTerm] = req;

    bool granted = false;
    if (term > pstate.currentTerm) {
        granted = true;
    }

    pstate.currentTerm = max(pstate.currentTerm, term);

    return {granted ? Replica::Follower : impl.state,
            {pstate.currentTerm, granted}};
}

#if 0
        /* same term - compare log completeness */

        if (pstate.logs.empty()) {
            /* we have no logs */
            grant = true;
        } else if (pstate.logs.back().first < lastLogTerm) {
            /* candidate's log is more recent */
            grant = true;
        } else if (pstate.logs.back().first == lastLogTerm) {
            if (pstate.logs.size() + 1 < lastLogIndex) {
                /* candidate has more logs on the same term */
                grant = true;
            } else if (pstate.logs.size() + 1 == lastLogIndex) {
                /*
                 * TODO: is this possible?
                 * If we suffer a network outage and came back online - the new
                 * campaign will have a newer term. No one should be competing.
                 *
                 * It's possible if two followers both become candidates at the
                 * same time and compete for votes? But in this case the
                 * candidates would have voted for themselves and deny the
                 * request. We shouldn't get to the log comparison part.
                 */
            } else {
                /* we have more log - reject */
            }
        }
#endif

/*
 * If we are a candidate
 *  Must be getting this from a leader. Is it possible for a leader to have
 * same term but less history? Either the leader is stale or we revert.
 */

#if 0
template <>
Replica::RequestVoteReply
Replica::request_vote<Replica::Candidate>(const Replica::RequestVoteReq& req) {

    // struct RequestVoteReq {
    //     int term;
    //     std::string candidateId;
    //     int lastLogIndex;
    //     int lastLogTerm;

    if (req.term < pstate.currentTerm) {
        /* request is stale */
        return {pstate.currentTerm, false};
    }

    if (!impl.votedFor) {
        /* already voted */
        return {pstate.currentTerm, false};
    }

    if (pstate.logs.size() > req.lastLogIndex + 1) {
        /* candidate has less logs than me */
        return {pstate.currentTerm, false};
    }

    if (pstate.logs.size() >= req.lastLogIndex + 1 &&
        pstate.logs[req.lastLogIndex].first != req.lastLogTerm) {
        /* candidate's log disagree with mine */
        return {pstate.currentTerm, false};
    }

    return {pstate.currentTerm, true};
}

template <>
Replica::AppendEntryReply
Replica::add_entries<Replica::Candidate>(const Replica::AppendEntryReq& req) {

    // int term;
    // std::string leaderId;
    // int prevLogIndex;
    // int prevLogTerm;
    // int leaderCommit;
    // std::optional<std::array<std::string, 2>> entry; /* key / value*/

#if 0
    if (req.term < pstate.currentTerm) {
        /* leader is stale - reject */
        co_return {pstate.term, false};
    } else {
        /* we are stale. abandon the campaign and become a follower */
    }
#endif
}
#endif

static boost::cobalt::task<Replica::RequestVoteReply>
request_vote_from_peer(std::string peer_addr) {

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

    Replica::RequestVoteReply reply{};
    co_return reply;
}

boost::cobalt::task<void> Replica::candidate_fsm() {

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

    auto io = co_await boost::asio::this_coro::executor;

    vector<boost::cobalt::task<Replica::RequestVoteReply>> reqs;

    for (auto peer_addr : impl.cluster) {
        reqs.push_back(request_vote_from_peer(peer_addr));
    }

    /* wait until either all reqs are serviced or timeout */

    auto rv = co_await boost::cobalt::(
        boost::cobalt::gather(std::move(reqs)), timeout(150));

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
}
