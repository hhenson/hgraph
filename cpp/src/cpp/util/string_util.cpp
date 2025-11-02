#include <hgraph/util/string_utils.h>

namespace hgraph {
    template<>
    std::string to_string(const bool &value) { return value ? "true" : "false"; }

    template<>
    std::string to_string(const int64_t &value) { return std::to_string(value); }

    template<>
    std::string to_string(const double &value) { return std::to_string(value); }

    template<>
    std::string to_string(const engine_time_t &value) {
        auto tt = std::chrono::system_clock::to_time_t(value);
        auto tm = *std::gmtime(&tt);
        char buffer[32];
        std::strftime(buffer, 32, "%Y-%m-%d %H:%M:%S", &tm);
        return {buffer};
    }

    template<>
    std::string to_string(const engine_date_t &value) {
        return std::format("{:04}-{:02}-{:02}", static_cast<int>(value.year()), static_cast<unsigned>(value.month()),
                           static_cast<unsigned>(value.day()));
    }

    template<>
    std::string to_string(const engine_time_delta_t &value) {
        auto hours = std::chrono::duration_cast<std::chrono::hours>(value);
        auto mins = std::chrono::duration_cast<std::chrono::minutes>(value - hours);
        auto secs = std::chrono::duration_cast<std::chrono::seconds>(value - hours - mins);
        return std::to_string(hours.count()) + ":" + std::to_string(mins.count()) + ":" + std::to_string(secs.count());
    }

    template<>
    std::string to_string(const nb::object &value) {
        try {
            return nb::cast<std::string>(nb::str(value));
        } catch (...) { return nb::cast<std::string>(nb::repr(value)); }
    }
} // namespace hgraph