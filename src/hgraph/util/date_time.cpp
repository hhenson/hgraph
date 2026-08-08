#include <hgraph/util/date_time.h>

#include <cstdio>
#include <ostream>

namespace hgraph
{
    std::ostream &operator<<(std::ostream &os, const Time &value)
    {
        const auto total_seconds = value.microseconds / 1'000'000;
        const auto micros        = value.microseconds % 1'000'000;
        const auto hours         = total_seconds / 3'600;
        const auto minutes       = (total_seconds / 60) % 60;
        const auto seconds       = total_seconds % 60;

        char buffer[24];
        std::snprintf(buffer, sizeof buffer, "%02lld:%02lld:%02lld",
                      static_cast<long long>(hours), static_cast<long long>(minutes),
                      static_cast<long long>(seconds));
        os << buffer;
        if (micros != 0)
        {
            std::snprintf(buffer, sizeof buffer, ".%06lld", static_cast<long long>(micros));
            os << buffer;
        }
        return os;
    }
}  // namespace hgraph
