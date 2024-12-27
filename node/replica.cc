
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
    } else if (pstate.logs.back().term < lastLogTerm) {
        /* candidate has logs from later terms */
        grant = true;
    } else if (pstate.logs.back().term == lastLogTerm &&
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

template auto
Replica::add_entries<Replica::Follower>(const Replica::AppendEntryReq& req)
    -> tuple<Replica::State, Replica::AppendEntryReply>;

template auto
Replica::add_entries<Replica::Leader>(const Replica::AppendEntryReq& req)
    -> tuple<Replica::State, Replica::AppendEntryReply>;

template auto
Replica::add_entries<Replica::Candidate>(const Replica::AppendEntryReq& req)
    -> tuple<Replica::State, Replica::AppendEntryReply>;

template <Replica::State T>
auto Replica::add_entries(const Replica::AppendEntryReq& req)
    -> tuple<Replica::State, Replica::AppendEntryReply> {

    auto& [term, leaderId, prevLogIndex, prevLogTerm, leaderCommit, entry] =
        req;

    if (term < pstate.currentTerm) {
        /* leader is stale - reject */
        return {impl.state, {pstate.currentTerm, false}};
    }

    /* accept leader - but only accept RPC if history match */
    impl.state = Replica::Follower;

    impl.follower.keep_alive = true;

    auto peerLogSize = prevLogIndex + 1;

    bool accept = false;
    if (pstate.logs.size() == peerLogSize) {
        if (pstate.logs.empty()) {
            accept = true;
        } else if (pstate.logs.back().term == prevLogTerm) {
            accept = true;
        }
    } else if (pstate.logs.size() > peerLogSize) {
        /* our log is longer */
        pstate.logs.resize(peerLogSize);
        if (peerLogSize == 0) {
            accept = true;
        } else if (pstate.logs[prevLogIndex].term == prevLogTerm) {
            accept = true;
        } else {
            /* reject and force leader to wallk backwards */
        }
    } else /* our logs is smaller */ {
        /* reject and force leader to wallk backwards */
    }

    return {impl.state, {pstate.currentTerm, accept}};
}

boost::cobalt::task<void>
Replica::fsm(boost::asio::ip::tcp::acceptor acceptor,
             boost::asio::ip::tcp::acceptor client_acceptor) {

    auto io = co_await boost::asio::this_coro::executor;

    while (true) {
        switch (impl.state) {
        case Follower: {
            impl.state = co_await follower_fsm(acceptor, client_acceptor);
        } break;
        case Candidate: {
            impl.state = co_await candidate_fsm(acceptor, client_acceptor);
        } break;
        case Leader: {
            impl.state = co_await leader_fsm(acceptor, client_acceptor);
        } break;
        }
    }
}

template auto Replica::rx_connection<Replica::Follower>(
    boost::asio::ip::tcp::acceptor& acceptor,
    boost::asio::steady_timer& cancel) -> boost::cobalt::task<void>;

template <Replica::State T>
auto Replica::rx_connection(boost::asio::ip::tcp::acceptor& acceptor,
                            boost::asio::steady_timer& cancel)
    -> boost::cobalt::task<void> {

    auto wait_for_cancel = [&]() -> boost::cobalt::task<void> {
        boost::system::error_code ec;
        co_await cancel.async_wait(
            boost::asio::redirect_error(boost::cobalt::use_task, ec));
    };

    while (true) {

        bool teardown = false;

        cout << impl.my_addr << " rx_connection(): waiting ...  " << endl;

        auto nx = co_await boost::cobalt::race(
            acceptor.async_accept(boost::cobalt::use_task), wait_for_cancel());
        switch (nx.index()) {
        case 0: {

            cout << impl.my_addr
                 << " rx_connection():packet received! reading... " << endl;

            auto& socket = get<0>(nx);

            char data[1024] = {};
            std::size_t n = co_await socket.async_read_some(
                boost::asio::buffer(data), boost::cobalt::use_task);
            auto req_var = deserialize<Replica::RequestVariant>(string(data));

            auto [state, reply_var] = rx_payload_handler<T>(req_var);

            cout << impl.my_addr << " rx_connection(): sending reply ...  "
                 << endl;

            auto reply_s = serialize(reply_var);
            co_await boost::asio::async_write(
                socket, boost::asio::buffer(reply_s.c_str(), reply_s.size()),
                boost::cobalt::use_task);

            cout << impl.my_addr << " rx_connection(): reply sent! " << endl;

            if (T != Replica::Follower) {
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
