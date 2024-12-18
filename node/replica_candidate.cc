#include "node/replica.hh"
#include "replica.hh"
#include <optional>

using namespace std;

boost::cobalt::task<bool> Replica::candidate_campaign() {

    auto io = co_await boost::asio::this_coro::executor;

    for (auto peer_addr : impl.cluster) {

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
    }

    co_return true;
}
