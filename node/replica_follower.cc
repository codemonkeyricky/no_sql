#include "node/replica.hh"

Replica::AppendEntryReply
Replica::follower_process_addEntryReq(const Replica::AppendEntryReq& req) {

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
        vstate.commitIndex =
            std::min(req.leaderCommit, (int)pstate.logs.size() - 1);
    }

    return {pstate.currentTerm, true};
}

boost::cobalt::task<void> Replica::follower_rx_conn() {

    auto io = co_await boost::cobalt::this_coro::executor;

    boost::asio::ip::tcp::acceptor acceptor(
        io, {boost::asio::ip::tcp::v4(), impl.port});

    for (;;) {
        auto socket = co_await acceptor.async_accept(boost::cobalt::use_task);
        boost::cobalt::spawn(io, follower_rx_payload(std::move(socket)),
                             boost::asio::detached);
    }
}

boost::cobalt::task<void>
Replica::follower_rx_payload(boost::asio::ip::tcp::socket socket) {

    auto io = co_await boost::cobalt::this_coro::executor;

    char data[1024] = {};
    std::size_t n = co_await socket.async_read_some(boost::asio::buffer(data),
                                                    boost::cobalt::use_task);

    // string payload = string(data, n);
    // auto p = payload.find(":");
    // auto cmd = payload.substr(0, p);
    // if (cmd == "add_node") {
    // }
}
