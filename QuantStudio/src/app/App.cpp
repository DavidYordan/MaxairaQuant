#include "App.h"
#include <spdlog/spdlog.h>
#include <yaml-cpp/yaml.h>
#include <nlohmann/json.hpp>
#include <filesystem>
#include <thread>
#include <chrono>
#include "network/WSClient.h"
#include "ui/UIDemoDX11.h"

namespace qs {

void App::start() {
    spdlog::info("Starting QuantStudio...");
    const std::filesystem::path cfg = std::filesystem::path("config") / "app.yaml";
    if (std::filesystem::exists(cfg)) {
        try {
            auto y = YAML::LoadFile(cfg.string());
            auto mode = y["mode"] ? y["mode"].as<std::string>() : "dev";
            auto lvl  = y["log_level"] ? y["log_level"].as<std::string>() : "info";
            spdlog::set_level(
                lvl == "trace" ? spdlog::level::trace :
                lvl == "debug" ? spdlog::level::debug :
                lvl == "warn"  ? spdlog::level::warn  :
                lvl == "error" ? spdlog::level::err   : spdlog::level::info
            );
            spdlog::info("Config loaded: mode={}, log_level={}", mode, lvl);
        } catch (const std::exception& e) {
            spdlog::error("Config load failed: {}", e.what());
        }
    } else {
        spdlog::warn("Config not found: {}. Using defaults.", cfg.string());
    }

    nlohmann::json meta = {
        {"app", "QuantStudio"},
        {"version", "0.1.0"},
        {"date", "2025-10-29"}
    };
    spdlog::info("Meta: {}", meta.dump());

    running_.store(true);

    // 新增：WS 消息回调，解析 Binance markPrice，并写入 DataStore
    ws_.set_on_message([this](std::string_view sv) {
        try {
            nlohmann::json j = nlohmann::json::parse(sv, nullptr, false);
            if (j.is_discarded()) return;

            // 支持单流与合并流两种格式
            auto get_field = [&](const nlohmann::json& root, const char* key) -> const nlohmann::json& {
                if (root.contains(key)) return root.at(key);
                if (root.contains("data") && root["data"].contains(key)) return root["data"].at(key);
                static nlohmann::json null;
                return null;
            };

            const auto& jp = get_field(j, "p");
            if (jp.is_null()) return;
            double price = 0.0;
            if (jp.is_string()) price = std::stod(jp.get<std::string>());
            else if (jp.is_number()) price = jp.get<double>();
            else return;

            const auto& je = get_field(j, "E");
            double tsec = 0.0;
            if (je.is_number()) tsec = je.get<long long>() / 1000.0;
            else {
                tsec = std::chrono::duration<double>(std::chrono::system_clock::now().time_since_epoch()).count();
            }

            store_.append(tsec, price);
        } catch (...) {
            // 忽略解析异常
        }
    });

    ws_.start("wss://fstream.binance.com/ws/btcusdt@markPrice");
}

void App::run() {
    spdlog::info("Run loop started.");
    UIDemoDX11 ui;
    if (!ui.init(L"QuantStudio (DX11 + ImGui + ImPlot)", 1280, 800)) {
        spdlog::error("UI init failed.");
        return;
    }
    // 新增：将数据存储注入到 UI
    ui.setDataStore(&store_);

    ui.main_loop();
    ui.shutdown();
    spdlog::info("Run loop finished.");
}

void App::stop() {
    running_.store(false);
    ws_.stop();
    spdlog::info("Stopping QuantStudio...");
}

} // namespace qs