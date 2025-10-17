#pragma once
#include <vector>
#include <mutex>
#include "indicator/IndicatorEngine.h"

namespace qs {

class DataStore {
public:
    void append(double t, double v) {
        std::lock_guard<std::mutex> lk(mtx_);
        series_.push_back({t, v});
        // 限制最大长度，避免无限增长（示例：保留最近 5000 点）
        if (series_.size() > 5000) {
            series_.erase(series_.begin(), series_.begin() + (series_.size() - 5000));
        }
    }
    const std::vector<SeriesPoint>& series() const { return series_; }
    std::vector<SeriesPoint> snapshot() const {
        std::lock_guard<std::mutex> lk(mtx_);
        return series_;
    }

private:
    std::vector<SeriesPoint> series_;
    mutable std::mutex mtx_;
};

} // namespace qs