#pragma once
#include <vector>

namespace qs {

struct SeriesPoint {
    double t;
    double v;
};

class IndicatorEngine {
public:
    std::vector<SeriesPoint> sma(const std::vector<SeriesPoint>& src, int window);
};

} // namespace qs