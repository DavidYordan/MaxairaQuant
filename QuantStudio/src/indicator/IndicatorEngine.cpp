#include "IndicatorEngine.h"

namespace qs {

std::vector<SeriesPoint> IndicatorEngine::sma(const std::vector<SeriesPoint>& src, int window) {
    std::vector<SeriesPoint> out;
    if (window <= 0 || src.size() < static_cast<size_t>(window)) return out;
    double acc = 0.0;
    for (size_t i = 0; i < src.size(); ++i) {
        acc += src[i].v;
        if (i >= static_cast<size_t>(window)) acc -= src[i - window].v;
        if (i + 1 >= static_cast<size_t>(window)) {
            out.push_back({src[i].t, acc / window});
        }
    }
    return out;
}

} // namespace qs