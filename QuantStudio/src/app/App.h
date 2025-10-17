#pragma once
#include <atomic>
#include <string>
#include "network/WSClient.h"
#include "data/DataStore.h"

namespace qs {

class App {
public:
    void start();
    void run();
    void stop();

private:
    std::atomic<bool> running_{false};
    WSClient ws_;
    DataStore store_;
};

} // namespace qs