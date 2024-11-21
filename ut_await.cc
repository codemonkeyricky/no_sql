

#include "node.hh"
#include <chrono>
#include <fstream>
#include <iomanip>
#include <iostream>

#include <boost/algorithm/string.hpp>
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
                auto range = it != lookup.begin() ? (token - ptoken)
                                                  : (token + 1e9 + 7 - ptoken);
                auto s = hash_lookup.at(id_hash);
                bars.push_back(make_pair(s, range));
            }

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
                string replacement = "blah";
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

            auto resp = "ring_ack:" + node.get_status();
            co_await async_write(socket,
                                 boost::asio::buffer(resp.c_str(), resp.size()),
                                 boost::asio::use_awaitable);
        }
    }
    co_return;
}

awaitable<void> listener(Node& node) {
    auto executor = co_await this_coro::executor;

    auto p = node.get_addr().find(":");
    auto addr = node.get_addr().substr(0, p);
    auto port = node.get_addr().substr(p + 1);

    tcp::acceptor acceptor(executor, {tcp::v4(), stoi(port)});
    for (;;) {
        auto socket =
            co_await acceptor.async_accept(boost::asio::use_awaitable);
        co_spawn(executor, rx_process(node, std::move(socket)), detached);
    }
}

awaitable<void> heartbeat(vector<shared_ptr<Node>>& nodes) {
    boost::asio::steady_timer timer(co_await this_coro::executor);
    auto io = co_await this_coro::executor;

    for (;;) {

        for (auto& n : nodes) {
            co_await n->heartbeat();
        }

        /* heartbeat every second */
        timer.expires_at(std::chrono::steady_clock::now() +
                         std::chrono::seconds(1));
    }
}

int main() {
    constexpr int NODES = 5;

    thread db_instance([] {
        boost::asio::io_context io_context(1);
        boost::asio::signal_set signals(io_context, SIGINT, SIGTERM);
        signals.async_wait([&](auto, auto) { io_context.stop(); });

        string addr = "127.0.0.1";
        int port = 5555;
        string seed;
        vector<shared_ptr<Node>> nodes;
        for (auto i = 0; i < NODES; ++i) {

            string addr_port = addr + ":" + to_string(port++);
            nodes.push_back(std::shared_ptr<Node>(new Node(addr_port, seed)));
            if (seed == "") {
                seed = addr_port;
            }
            co_spawn(io_context, listener(*nodes.back()), detached);
        }

        co_spawn(io_context, heartbeat(nodes), detached);

        io_context.run();
    });

    /* test code here */

    /* wait for cluster ready */

    db_instance.join();

    cout << "### " << endl;
}