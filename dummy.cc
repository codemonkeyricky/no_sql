

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

    // auto coro = []() -> boost::cobalt::task<int> {
    //     int a = 3;
    //     co_return a;
    // };

    boost::asio::io_context io(1);

    int var = 0xdeadbeef;
    cout << "var address = " << &var << endl;

    auto coro = [&]() -> boost::cobalt::task<void> {
        cout << "coro var address = " << &var << endl;
        auto a = var;
        co_return;
    };

    /* case 2 */
    boost::cobalt::spawn(io, coro(), boost::asio::detached);

    io.run();
}
