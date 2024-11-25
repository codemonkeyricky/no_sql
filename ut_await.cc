

#include "node.hh"
#include <boost/asio/cancellation_signal.hpp>
#include <boost/asio/experimental/parallel_group.hpp>
#include <chrono>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <queue>
#include <unordered_map>

#include <boost/asio.hpp>
#include <boost/asio/experimental/awaitable_operators.hpp>
#include <boost/cobalt.hpp>

#include <boost/algorithm/string.hpp>
#include <boost/asio/bind_cancellation_slot.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/signal_set.hpp>
#include <boost/asio/write.hpp>
#include <boost/filesystem.hpp>
#include <string>
#include <utility>

using boost::asio::awaitable;
using boost::asio::co_spawn;
using boost::asio::detached;
using boost::asio::use_awaitable;
using boost::asio::ip::tcp;
namespace this_coro = boost::asio::this_coro;

using namespace std;

map<string, shared_ptr<Node>> nodes;

awaitable<void> rx_process(Node& node, tcp::socket socket) {

    for (;;) {
        char data[1024] = {};
        std::size_t n = co_await socket.async_read_some(
            boost::asio::buffer(data), boost::asio::use_awaitable);

        string payload = string(data, n);
        auto p = payload.find(":");

        auto cmd = payload.substr(0, p);
        if (cmd == "r") {
            /* read */
            auto key = string(data + 2, n - 2);
            auto value = co_await node.read(key);
            auto resp = "ra:" + value;
            co_await async_write(socket,
                                 boost::asio::buffer(resp.c_str(), resp.size()),
                                 boost::asio::use_awaitable);

        } else if (cmd == "w" || cmd == "wf") {

            /* write */

            auto kv = string(data + cmd.size() + 1, n - cmd.size() - 1);
            auto p = kv.find("=");
            auto k = kv.substr(0, p);
            auto v = kv.substr(p + 1);

            co_await node.write(k, v, cmd == "w");

            string resp;
            if (cmd == "wf")
                resp = string("wfa:");
            else
                resp = string("wa:");

            co_await async_write(socket,
                                 boost::asio::buffer(resp.c_str(), resp.size()),
                                 boost::asio::use_awaitable);
        } else if (cmd == "g") {
            /* gossip */
            auto gossip = payload.substr(p + 1);
            node.gossip_rx(gossip);
            auto resp = "ga:" + gossip;
            co_await async_write(socket,
                                 boost::asio::buffer(resp.c_str(), resp.size()),
                                 boost::asio::use_awaitable);
        } else if (cmd == "s") {
            auto stream = payload.substr(p + 1);
            auto p = stream.find("-");
            auto i = stoll(stream.substr(0, p)),
                 j = stoll(stream.substr(p + 1));

            auto resp = "sa:" + node.serialize(node.stream(i, j));
            co_await async_write(socket,
                                 boost::asio::buffer(resp.c_str(), resp.size()),
                                 boost::asio::use_awaitable);
        } else if (cmd == "st") {
            auto resp = "sta:" + node.get_status();
            co_await async_write(socket,
                                 boost::asio::buffer(resp.c_str(), resp.size()),
                                 boost::asio::use_awaitable);
        } else if (cmd == "ring") {
            const auto& [lookup, hash_lookup] = node.get_ring_view();
            vector<pair<const string, const uint64_t>> bars;
            for (auto it = lookup.begin(); it != lookup.end(); ++it) {
                const auto [token, timestamp, id_hash] = *it;
                auto p = it == lookup.begin() ? prev(lookup.end()) : prev(it);
                auto [ptoken, skip, skip1] = *p;
                auto range =
                    it != lookup.begin()
                        ? (token - ptoken)
                        : (token + Partitioner::instance().getRange() - ptoken);
                auto s = hash_lookup.at(id_hash);
                bars.push_back(make_pair(s, range));

                /*
                 * format to:
                 * var data = {a: 9, b: 20, c:30, d:8, e:12, f:3, g:7, h:14}
                 */
            }

            unordered_map<string, int> cnt;

            string replacement = "{";
            int k = 0;
            for (auto& bar : bars) {
                auto p = bar.first.find(":");
                auto name = bar.first.substr(p + 1);
                /* <index>-<node>-<instance>*/
                replacement += "\"" + to_string(k++) + "-" + name + "-" +
                               to_string(cnt[name]++) + "\"" + ":" +
                               to_string(bar.second) + ",";
            }
            replacement += "}";

            {
                string filename("web/ring_fmt.html");

                // Open the file for reading
                std::ifstream inFile(filename);
                assert(inFile);

                // Read the file content into a string
                std::string fileContent(
                    (std::istreambuf_iterator<char>(inFile)),
                    std::istreambuf_iterator<char>());
                inFile.close();

                // Find and replace the target line with the replacement
                string targetLine = "LINE_TO_REPLACE";
                // string replacement = "blah";
                if (fileContent.find(targetLine) != std::string::npos) {
                    boost::algorithm::replace_all(fileContent, targetLine,
                                                  replacement);
                } else {
                    assert(0);
                }

                // Open the file for writing and overwrite the content
                std::ofstream outFile("web/ring.html");
                assert(outFile);

                outFile << fileContent;
                outFile.close();
            }

            auto resp = "ring_ack:" + replacement;
            co_await async_write(socket,
                                 boost::asio::buffer(resp.c_str(), resp.size()),
                                 boost::asio::use_awaitable);
        }
    }
    co_return;
}

// boost::asio::awaitable<void> dummy() {
//     auto executor = co_await boost::asio::this_coro::executor;
//     boost::asio::steady_timer timer(executor);

//     while (true) {
//         timer.expires_after(std::chrono::seconds(3));
//         co_await timer.async_wait(boost::asio::use_awaitable);
//     }

//     co_return;
// }

// boost::asio::awaitable<void> cancellable(Node& n) {
//     auto io = co_await boost::asio::this_coro::executor;

//     co_await boost::asio::co_spawn(
//         io, dummy(),
//         boost::asio::bind_cancellation_slot(n.cancel_signal.slot(),
//         detached));

//     co_return;
// }

awaitable<void> Cancelled(boost::asio::steady_timer& cancelTimer) {
    boost::system::error_code ec;
    co_await cancelTimer.async_wait(redirect_error(use_awaitable, ec));
}

awaitable<void> node_listener(Node& node) {
    auto executor = co_await this_coro::executor;
    // auto cancellation = co_await asio::this_coro::cancellation_state;

    auto p = node.get_addr().find(":");
    auto addr = node.get_addr().substr(0, p);
    auto port = node.get_addr().substr(p + 1);

    boost::asio::steady_timer cancelTimer{
        executor, std::chrono::steady_clock::duration::max()};

    tcp::acceptor acceptor(executor, {tcp::v4(), stoi(port)});
    for (;;) {

        using namespace boost::asio::experimental::awaitable_operators;

        auto result{
            co_await (acceptor.async_accept(boost::asio::use_awaitable) ||
                      Cancelled(cancelTimer))};

        // Check which awaitable completed

        if (result.index() == 0) {
            auto socket = std::move(std::get<0>(result));
            co_spawn(executor, rx_process(node, std::move(socket)), detached);
        }
    }
}
static queue<array<string, 2>> pending_add;
static queue<string> pending_remove;

awaitable<void> heartbeat(map<string, shared_ptr<Node>>& nodes) {
    boost::asio::steady_timer timer(co_await this_coro::executor);
    auto io = co_await this_coro::executor;

    for (;;) {

        for (auto& n : nodes) {
            co_await n.second->heartbeat();
        }

        while (pending_add.size()) {
            auto [server, seed] = pending_add.front();
            pending_add.pop();

            auto n = nodes[server] =
                std::shared_ptr<Node>(new Node(server, seed, 1));
            co_spawn(io, node_listener(*n),
                     boost::asio::bind_cancellation_slot(
                         n->cancel_signal.slot(), detached));
        }

        while (pending_remove.size()) {
        }

        /* heartbeat every second */
        timer.expires_at(std::chrono::steady_clock::now() +
                         std::chrono::seconds(1));
    }
}

static int port = 6000;
awaitable<void> cp_process(map<string, shared_ptr<Node>>& nodes,
                           tcp::socket socket) {
    auto executor = co_await this_coro::executor;
    for (;;) {

        char data[1024] = {};
        std::size_t n = co_await socket.async_read_some(
            boost::asio::buffer(data), boost::asio::use_awaitable);

        string payload = string(data, n);
        auto p = payload.find(":");
        auto cmd = payload.substr(0, p);
        if (cmd == "an") {
            /* TODO: parse address */
            auto seed = payload.substr(p + 1);

            pending_add.push(
                {"127.0.0.1:" + to_string(port++), "127.0.0.1:5555"});

            auto resp = "ana:" + seed;
            co_await async_write(socket,
                                 boost::asio::buffer(resp.c_str(), resp.size()),
                                 boost::asio::use_awaitable);
        } else if (cmd == "rn") {
            auto seed = payload.substr(p + 1);
            auto& target = (*nodes.begin()).second->cancel_signal;
            target.emit(boost::asio::cancellation_type::all);

            /* TODO: delete heartbeat */
            /* TODO: wait for all current connnections to drain */
        }
    }
    co_return;
}

awaitable<void> system_listener(map<string, shared_ptr<Node>>& nodes) {
    auto executor = co_await this_coro::executor;

    tcp::acceptor acceptor(executor, {tcp::v4(), 5001});
    for (;;) {
        auto socket =
            co_await acceptor.async_accept(boost::asio::use_awaitable);
        co_spawn(executor, cp_process(nodes, std::move(socket)), detached);
    }
}

int main() {
    constexpr int NODES = 2;

    thread db_instance([] {
        boost::asio::io_context io_context(1);
        boost::asio::signal_set signals(io_context, SIGINT, SIGTERM);
        signals.async_wait([&](auto, auto) { io_context.stop(); });

        string addr = "127.0.0.1";
        int port = 5555;
        string seed;
        for (auto i = 0; i < NODES; ++i) {

            string addr_port = addr + ":" + to_string(port++);
            auto n = nodes[addr_port] =
                std::shared_ptr<Node>(new Node(addr_port, seed, 1));
            if (seed == "") {
                seed = addr_port;
            }
            /* node control plane */
            co_spawn(io_context, node_listener(*n),
                     boost::asio::bind_cancellation_slot(
                         n->cancel_signal.slot(), detached));
        }

        /* distributed system heartbeat */
        co_spawn(io_context, heartbeat(nodes), detached);

        /* distributed system control plane */
        co_spawn(io_context, system_listener(nodes), detached);

        io_context.run();
    });

    /* test code here */

    /* wait for cluster ready */

    db_instance.join();

    cout << "### " << endl;
}