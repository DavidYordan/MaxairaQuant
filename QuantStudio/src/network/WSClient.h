// class WSClient
#pragma once
#include <string>
#include <string_view>
#include <functional>
#include <atomic>
#include <thread>
#include <memory>

// Boost 头（确保模板成员类型完整）
#include <boost/asio/io_context.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/ssl/context.hpp>
#include <boost/asio/ssl/stream.hpp>
#include <boost/beast/websocket.hpp>

namespace qs {

class WSClient {
public:
    bool start(const std::string& url);
    void stop();
    bool is_connected() const { return connected_.load(); }
    void set_on_message(std::function<void(std::string_view)> cb) { on_message_ = std::move(cb); }

private:
    std::atomic<bool> connected_{false};
    std::jthread io_thread_;
    std::unique_ptr<boost::asio::io_context> io_;
    std::unique_ptr<boost::asio::ssl::context> ssl_ctx_;
    std::unique_ptr<boost::beast::websocket::stream<
        boost::asio::ssl::stream<boost::asio::ip::tcp::socket>>> ws_;
    std::function<void(std::string_view)> on_message_;
};

} // namespace qs