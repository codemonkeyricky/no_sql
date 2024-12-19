

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

boost::cobalt::task<void> quick_task() {
    cout << "### quick" << endl;
    co_return;
}

boost::cobalt::task<void> micro_task(int k) {
    cout << "### " << k << endl;
    co_return;
}

boost::cobalt::task<void> to_run() {

    std::vector<boost::cobalt::task<void>> tasks;
    for (int i = 0; i < 8; ++i) {
        tasks.push_back(micro_task(i));
    }

    /* The following returns either when tasks are done or quick_task is done */

    co_await boost::cobalt::race(boost::cobalt::gather(std::move(tasks)),
                                 quick_task());

    co_return;
}

int main() {

    boost::asio::io_context io(1);

    boost::cobalt::spawn(io, to_run(), boost::asio::detached);

    io.run();
}