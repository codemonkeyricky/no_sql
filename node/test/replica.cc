#include "node/replica.hh"

#include <boost/algorithm/string.hpp>
#include <boost/asio.hpp>
#include <boost/cobalt.hpp>

#include <string>
#include <vector>

using namespace std;

int main() {

    vector<string> cluster = {"127.0.0.1:5555", "127.0.0.1:5556"};

    Replica r0(cluster[0], cluster);

    boost::asio::io_context io(1);

    /* TC: All followers electing one leader */

    /* TC: Single leader multi follower majority */

    /* TC: Split majority  */
}