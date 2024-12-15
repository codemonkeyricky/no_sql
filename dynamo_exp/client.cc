
#include <boost/asio.hpp>
#include <boost/beast.hpp>
#include <iostream>
#include <string>

namespace beast = boost::beast;
namespace http = beast::http;
namespace net = boost::asio;

std::string signRequest(const std::string& payload,
                        const std::string& aws_access_key,
                        const std::string& aws_secret_key,
                        const std::string& region) {
    // Placeholder for AWS Signature V4 signing logic
    return "AWS4-HMAC-SHA256 SignedHeader=...";
}

void queryDynamoDB(const std::string& table_name, const std::string& key,
                   const std::string& value, const std::string& aws_access_key,
                   const std::string& aws_secret_key,
                   const std::string& region) {
    try {
        net::io_context ioc;

        // Host and port for DynamoDB (HTTP endpoint)
        std::string host =
            "127.0.0.1"; // "dynamodb." + region + ".amazonaws.com";
        std::string port = "8080";

        // Create a TCP stream
        beast::tcp_stream stream(ioc);

        // Resolve the host
        net::ip::tcp::resolver resolver(ioc);
        auto const results = resolver.resolve(host, port);
        stream.connect(results);

        // Construct the payload for a Query operation
        std::string payload = R"({
            "TableName": ")" + table_name +
                              R"(",
            "KeyConditionExpression": "#key = :value",
            "ExpressionAttributeNames": {"#key": ")" +
                              key + R"("},
            "ExpressionAttributeValues": {":value": {"S": ")" +
                              value + R"("}}
        })";

        // Create an HTTP POST request
        http::request<http::string_body> req{http::verb::post, "/", 11};
        req.set(http::field::host, host);
        req.set(http::field::content_type, "application/x-amz-json-1.0");
        req.set("X-Amz-Target", "DynamoDB_20120810.Query");
        req.set(http::field::content_length, std::to_string(payload.size()));

        // Sign the request
        req.set(http::field::authorization,
                signRequest(payload, aws_access_key, aws_secret_key, region));

        // Add the body and send the request
        req.body() = payload;
        req.prepare_payload();
        http::write(stream, req);

        // Receive and parse the response
        beast::flat_buffer buffer;
        http::response<http::string_body> res;
        http::read(stream, buffer, res);

        // Print the response
        std::cout << res.body() << std::endl;

        // Close the connection
        beast::error_code ec;
        stream.socket().shutdown(net::ip::tcp::socket::shutdown_both, ec);
        if (ec && ec != beast::errc::not_connected)
            throw beast::system_error{ec};
    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << std::endl;
    }
}

int main() {
    std::string aws_access_key = "your-access-key";
    std::string aws_secret_key = "your-secret-key";
    std::string region = "us-east-1";

    queryDynamoDB("YourTableName", "YourKey", "YourValue", aws_access_key,
                  aws_secret_key, region);
    return 0;
}
