#include "node/node.hh"

// boost::cobalt::task<void>
// Node::rx_process(boost::asio::ip::tcp::socket socket) {

//     // std::cout << self << ":" << "rx_process() spawned!" << std::endl;

//     ++outstanding;

//     try {
//         for (;;) {
//             char data[1024] = {};
//             std::size_t n = co_await socket.async_read_some(
//                 boost::asio::buffer(data), boost::cobalt::use_task);

//             std::string payload = std::string(data, n);
//             auto p = payload.find(":");

//             auto cmd = payload.substr(0, p);
//             if (cmd == "r") {
//                 /* read */
//                 auto key = std::string(data + 2, n - 2);
//                 auto value = co_await read(key);
//                 auto resp = "ra:" + value;
//                 co_await boost::asio::async_write(
//                     socket, boost::asio::buffer(resp.c_str(), resp.size()),
//                     boost::cobalt::use_task);

//             } else if (cmd == "w" || cmd == "wf") {

//                 /* write */

//                 auto kv =
//                     std::string(data + cmd.size() + 1, n - cmd.size() - 1);
//                 auto p = kv.find("=");
//                 auto k = kv.substr(0, p);
//                 auto v = kv.substr(p + 1);

//                 co_await write(k, v, cmd == "w");

//                 std::string resp;
//                 if (cmd == "wf")
//                     resp = std::string("wfa:");
//                 else
//                     resp = std::string("wa:");

//                 co_await boost::asio::async_write(
//                     socket, boost::asio::buffer(resp.c_str(), resp.size()),
//                     boost::cobalt::use_task);
//             } else if (cmd == "g") {
//                 /* gossip */
//                 auto gossip = payload.substr(p + 1);
//                 gossip_rx(gossip);
//                 auto resp = "ga:" + gossip;
//                 co_await boost::asio::async_write(
//                     socket, boost::asio::buffer(resp.c_str(), resp.size()),
//                     boost::cobalt::use_task);
//             } else if (cmd == "s") {
//                 auto s = payload.substr(p + 1);
//                 auto p = s.find("-");
//                 auto i = stoll(s.substr(0, p)), j = stoll(s.substr(p + 1));

//                 auto resp = "sa:" + serialize(stream(i, j));
//                 co_await boost::asio::async_write(
//                     socket, boost::asio::buffer(resp.c_str(), resp.size()),
//                     boost::cobalt::use_task);
//             } else if (cmd == "st") {
//                 auto resp = "sta:" + get_status();
//                 co_await boost::asio::async_write(
//                     socket, boost::asio::buffer(resp.c_str(), resp.size()),
//                     boost::cobalt::use_task);
//             } else if (cmd == "ring") {
//                 const auto& [lookup, hash_lookup] = get_ring_view();
//                 std::vector<std::pair<const std::string, const uint64_t>> bars;
//                 for (auto it = lookup.begin(); it != lookup.end(); ++it) {
//                     const auto [token, timestamp, id_hash] = *it;
//                     auto p =
//                         it == lookup.begin() ? prev(lookup.end()) : prev(it);
//                     auto [ptoken, skip, skip1] = *p;
//                     auto range =
//                         it != lookup.begin()
//                             ? (token - ptoken)
//                             : (token + Partitioner::instance().getRange() -
//                                ptoken);
//                     auto s = hash_lookup.at(id_hash);
//                     bars.push_back(make_pair(s, range));

//                     /*
//                      * format to:
//                      * var data = {a: 9, b: 20, c:30, d:8, e:12, f:3,
//                      * g:7, h:14}
//                      */
//                 }

//                 std::unordered_map<std::string, int> cnt;

//                 std::string replacement = "{";
//                 int k = 0;
//                 for (auto& bar : bars) {
//                     auto p = bar.first.find(":");
//                     auto name = bar.first.substr(p + 1);
//                     /* <index>-<node>-<instance>*/
//                     replacement += "\"" + std::to_string(k++) + "-" + name +
//                                    "-" + std::to_string(cnt[name]++) + "\"" +
//                                    ":" + std::to_string(bar.second) + ",";
//                 }
//                 replacement += "}";

//                 // {
//                 //     string filename("web/ring_fmt.html");

//                 //     // Open the file for reading
//                 //     std::ifstream inFile(filename);
//                 //     assert(inFile);

//                 //     // Read the file content into a string
//                 //     std::string fileContent(
//                 //         (std::istreambuf_iterator<char>(inFile)),
//                 //         std::istreambuf_iterator<char>());
//                 //     inFile.close();

//                 //     // Find and replace the target line with the
//                 //     replacement string targetLine =
//                 //     "LINE_TO_REPLACE";
//                 //     // string replacement = "blah";
//                 //     if (fileContent.find(targetLine) !=
//                 //     std::string::npos) {
//                 //         boost::algorithm::replace_all(fileContent,
//                 //         targetLine,
//                 //                                       replacement);
//                 //     } else {
//                 //         assert(0);
//                 //     }

//                 //     // Open the file for writing and overwrite the
//                 //     content std::ofstream outFile("web/ring.html");
//                 //     assert(outFile);

//                 //     outFile << fileContent;
//                 //     outFile.close();
//                 // }

//                 auto resp = "ring_ack:" + replacement;
//                 co_await boost::asio::async_write(
//                     socket, boost::asio::buffer(resp.c_str(), resp.size()),
//                     boost::cobalt::use_task);
//             } else if (cmd == "aa") {

//                 auto resp = std::string("aa_ack:"); //  + std::to_string(cnt);
//                 co_await boost::asio::async_write(
//                     socket, boost::asio::buffer(resp.c_str(), resp.size()),
//                     boost::cobalt::use_task);

//                 this->anti_entropy_req = true;

//             } else if (cmd == "range_hash") {

//                 auto range =
//                     std::string(data + cmd.size() + 1, n - cmd.size() - 1);
//                 auto p = range.find("-");
//                 auto i = range.substr(0, p);
//                 auto j = range.substr(p + 1);
//                 auto hash = get_range_hash(stoll(i), stoll(j));

//                 auto resp =
//                     std::string("range_hash_ack:") + std::to_string(hash);
//                 co_await boost::asio::async_write(
//                     socket, boost::asio::buffer(resp.c_str(), resp.size()),
//                     boost::cobalt::use_task);
//             }
//         }
//     } catch (const std::exception& e) {

//         // std::string info = boost::diagnostic_information(e);
//         // std::cout << info << std::endl;

//         // std::cerr << "Connection error: " << e.what() << std::endl;
//     }

//     // std::cout << self << ":" << "rx_process() end!" << std::endl;

//     --outstanding;

//     co_return;
// }
