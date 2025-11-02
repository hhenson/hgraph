//
// Created by Howard Henson on 03/04/2021.
//

#ifndef HGRAPH_DATE_TIME_H
#define HGRAPH_DATE_TIME_H

#include <chrono>

namespace std {
    // Specialization for std::chrono::time_point
    template<class Clock, class Duration>
    struct hash<std::chrono::time_point<Clock, Duration> > {
        size_t operator()(const std::chrono::time_point<Clock, Duration> &tp) const noexcept {
            return std::hash<typename Duration::rep>()(tp.time_since_epoch().count());
        }
    };

    // Specialization for std::chrono::duration
    template<class Rep, class Period>
    struct hash<std::chrono::duration<Rep, Period> > {
        size_t operator()(const std::chrono::duration<Rep, Period> &d) const noexcept {
            return std::hash<Rep>()(d.count());
        }
    };


    template<>
    struct hash<std::chrono::year_month_day> {
        size_t operator()(const std::chrono::year_month_day &ymd) const noexcept {
            size_t h1 = std::hash<int>{}(static_cast<int>(ymd.year()));
            size_t h2 = std::hash<unsigned>{}(static_cast<unsigned>(ymd.month()));
            size_t h3 = std::hash<unsigned>{}(static_cast<unsigned>(ymd.day()));
            // Combine hashes (boost::hash_combine-like)
            return h1 ^ (h2 << 1) ^ (h3 << 2);
        }
    };
} // namespace std

namespace hgraph {
    using engine_clock = std::chrono::system_clock;
    // Use microsecond precision to avoid overflow for dates beyond 2262
    using engine_time_t = std::chrono::time_point<engine_clock, std::chrono::microseconds>;
    using engine_time_delta_t = std::chrono::microseconds;

    constexpr engine_time_t min_time() noexcept { return engine_time_t{}; }
    // Note: system_clock::duration may be as fine as nanoseconds on some platforms (e.g., glibc++ on Linux).
    // 64-bit nanoseconds can only represent times up to ~2262-04-11 23:47:16 since the Unix epoch.
    // The previously hardcoded cap of 2300-01-01 could overflow on such platforms, wrapping into a
    // negative timestamp. To keep behavior consistent across platforms, we clamp the maximum engine
    // time to the lesser of:
    //  - the desired logical cap (2300-01-01 00:00:00), and
    //  - the largest whole-day time point representable by engine_clock::time_point.
    inline engine_time_t max_time() noexcept {
        using namespace std::chrono;
        // Desired logical cap
        const sys_days desired_cap = year(2300) / January / day(1);
        // Compute the largest whole-day that can be represented without overflow
        const auto max_whole_day = floor<days>(engine_time_t::max());
        // Pick the earlier of the two
        const sys_days chosen_day = (desired_cap <= max_whole_day) ? desired_cap : max_whole_day;
        // Convert back to the engine_time_t representation at midnight of the chosen day
        return engine_time_t{chosen_day.time_since_epoch()};
    }

    constexpr engine_time_delta_t smallest_time_increment() noexcept { return engine_time_delta_t(1); }
    constexpr engine_time_t min_start_time() noexcept { return min_time() + smallest_time_increment(); }
    constexpr engine_time_t max_end_time() noexcept { return max_time() - smallest_time_increment(); }

    inline auto static MIN_DT = min_time();
    inline auto static MAX_DT = max_time();
    inline auto static MIN_ST = min_start_time();
    inline auto static MAX_ET = max_end_time();

    inline auto static MIN_TD = smallest_time_increment();

    using engine_date_t = std::chrono::year_month_day;
} // namespace hgraph
#endif  // HGRAPH_DATE_TIME_H