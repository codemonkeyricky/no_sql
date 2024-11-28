

#include <boost/asio/cancellation_signal.hpp>
#include <boost/asio/experimental/parallel_group.hpp>
#include <boost/system/detail/errc.hpp>
#include <boost/system/detail/error_code.hpp>
#include <chrono>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <memory>
#include <queue>
#include <unordered_map>

#include <boost/algorithm/string.hpp>
#include <boost/asio.hpp>
#include <boost/cobalt.hpp>

#include <string>
#include <utility>

using namespace std;

int main() {

    auto coro1 = []() -> boost::cobalt::task<void> { co_return; };
    auto coro2 = [&]() -> boost::cobalt::task<void> { co_await coro1(); };

    boost::asio::io_context io(1);

    /* case 1 */
    // boost::cobalt::spawn(io, coro2(), boost::asio::detached);

    /* case 2 */
    boost::cobalt::spawn(
        io, [coro1]() -> boost::cobalt::task<void> { co_await coro1(); }(),
        boost::asio::detached);

    io.run();

    volatile int dummy = 0;
}
