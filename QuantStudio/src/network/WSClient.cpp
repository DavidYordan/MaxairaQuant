#include "WSClient.h"
#include <atomic>
#include <thread>
#include <memory>
#include <string>
#include <spdlog/spdlog.h>
#include <boost/beast/core.hpp>
#include <boost/beast/websocket.hpp>
#include <boost/beast/ssl.hpp>
#include <boost/asio.hpp>
#include <boost/asio/ssl.hpp>
#include <openssl/ssl.h>
#include <stop_token>

namespace qs {

namespace beast = boost::beast;
namespace websocket = beast::websocket;
namespace net = boost::asio;
namespace ssl = net::ssl;
using tcp = net::ip::tcp;

static std::tuple<std::string, std::string, std::string, std::string> parse_url(const std::string& u) {
    std::string scheme, host, port, path;
    auto p = u.find("://");
    scheme = p == std::string::npos ? "" : u.substr(0, p);
    auto rest = p == std::string::npos ? u : u.substr(p + 3);
    auto s = rest.find('/');
    host = s == std::string::npos ? rest : rest.substr(0, s);
    path = s == std::string::npos ? "/" : rest.substr(s);
    auto c = host.find(':');
    if (c != std::string::npos) {
        port = host.substr(c + 1);
        host = host.substr(0, c);
    }
    if (port.empty()) {
        port = (scheme == "wss" || scheme == "https") ? "443" : "80";
    }
    return {scheme, host, port, path};
}

bool WSClient::start(const std::string& url) {
    if (io_thread_.joinable()) return true;
    io_ = std::make_unique<net::io_context>();
    ssl_ctx_ = std::make_unique<ssl::context>(ssl::context::tls_client);
    ssl_ctx_->set_default_verify_paths();
#if defined(QS_SSL_VERIFY)
    ssl_ctx_->set_verify_mode(ssl::verify_peer);
#else
    ssl_ctx_->set_verify_mode(ssl::verify_none);
#endif
    auto [scheme, host, port, path] = parse_url(url);
    io_thread_ = std::jthread([this, host, port, path](std::stop_token st) {
        beast::error_code ec;
        try {
            tcp::resolver resolver(*io_);
            auto results = resolver.resolve(host, port, ec);
            if (ec) { spdlog::error("Resolve {}:{} failed: {}", host, port, ec.message()); return; }

            tcp::socket sock(*io_);
            net::connect(sock, results, ec);
            if (ec) { spdlog::error("Connect failed: {}", ec.message()); return; }

            ssl::stream<tcp::socket> ssl_stream(std::move(sock), *ssl_ctx_);
            SSL_set_tlsext_host_name(ssl_stream.native_handle(), host.c_str());
            ssl_stream.handshake(ssl::stream_base::client, ec);
            if (ec) { spdlog::error("TLS handshake failed: {}", ec.message()); return; }

            ws_ = std::make_unique<websocket::stream<ssl::stream<tcp::socket>>>(std::move(ssl_stream));
            ws_->set_option(websocket::stream_base::timeout::suggested(beast::role_type::client));
            ws_->handshake(host, path, ec);
            if (ec) { spdlog::error("WS handshake failed: {}", ec.message()); return; }

            connected_.store(true);
            spdlog::info("WS connected: {}{}", host, path);
            while (!st.stop_requested()) {
                beast::flat_buffer buffer;
                ws_->read(buffer, ec);
                if (ec) {
                    if (!st.stop_requested()) spdlog::error("WS read error: {}", ec.message());
                    break;
                }
                std::string msg = beast::buffers_to_string(buffer.data());
                spdlog::info("WS msg: {}", msg);
                if (on_message_) on_message_(msg);
            }
        } catch (const std::exception& e) {
            spdlog::error("WS exception: {}", e.what());
        }
        connected_.store(false);
    });
    return true;
}

void WSClient::stop() {
    if (!io_thread_.joinable()) return;
    io_thread_.request_stop();
    if (ws_) {
        beast::error_code ec;
        ws_->close(websocket::close_code::normal, ec);
        ws_.reset();
    }
    io_thread_.join();
    io_.reset();
    ssl_ctx_.reset();
    spdlog::info("WSClient stopped");
}

} // namespace qs