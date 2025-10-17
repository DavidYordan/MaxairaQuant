#include <spdlog/spdlog.h>
#include "app/App.h"

int main() {
    spdlog::set_pattern("[%H:%M:%S.%e] [%^%l%$] %v");
    spdlog::info("QuantStudio booting...");

    qs::App app;
    app.start();
    app.run();
    app.stop();

    spdlog::info("QuantStudio exit.");
    return 0;
}