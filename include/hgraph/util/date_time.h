//
// Created by Howard Henson on 03/04/2021.
//

#ifndef HGRAPH_DATE_TIME_H
#define HGRAPH_DATE_TIME_H

#include <chrono>
#include <compare>
#include <cstdint>
#include <iosfwd>

namespace std {
    /** ``std::hash`` specialisation for ``std::chrono::time_point``. */
    template<class Clock, class Duration>
    struct hash<std::chrono::time_point<Clock, Duration> > {
        size_t operator()(const std::chrono::time_point<Clock, Duration> &tp) const noexcept {
            return std::hash<typename Duration::rep>()(tp.time_since_epoch().count());
        }
    };

    /** ``std::hash`` specialisation for ``std::chrono::duration``. */
    template<class Rep, class Period>
    struct hash<std::chrono::duration<Rep, Period> > {
        size_t operator()(const std::chrono::duration<Rep, Period> &d) const noexcept {
            return std::hash<Rep>()(d.count());
        }
    };


    /** ``std::hash`` specialisation for ``std::chrono::year_month_day``. */
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
    /** Reference clock used by the runtime; aliases ``std::chrono::system_clock``. */
    using engine_clock = std::chrono::system_clock;

    /**
     * Hgraph datetime scalar with microsecond resolution.
     *
     * Microseconds are used (rather than the platform default for
     * ``system_clock``) so the representable range exceeds 2262 — the limit
     * that nanosecond precision would impose with a 64-bit count.
     */
    using DateTime = std::chrono::time_point<engine_clock, std::chrono::microseconds>;

    /** Hgraph time-delta scalar with the same microsecond resolution as ``DateTime``. */
    using TimeDelta = std::chrono::microseconds;

    /** Calendar date alias used by the runtime. */
    using Date = std::chrono::year_month_day;

    /** The earliest representable hgraph time. Used as the never-modified sentinel. */
    constexpr DateTime min_time() noexcept { return DateTime{}; }

    /**
     * The latest representable hgraph time, clamped to the smaller of:
     *
     * - the desired logical cap (2300-01-01 00:00:00), and
     * - the largest whole-day time point representable by ``engine_clock``.
     *
     * Some platforms (notably libstdc++ on Linux) define
     * ``system_clock::duration`` at nanosecond resolution, which limits 64-bit
     * timestamps to roughly 2262-04-11. The clamp keeps behaviour consistent
     * across platforms and avoids overflow into a negative timestamp.
     */
    inline DateTime max_time() noexcept {
        using namespace std::chrono;
        // Desired logical cap
        const sys_days desired_cap = year(2300) / January / day(1);
        // Compute the largest whole-day that can be represented without overflow
        const auto max_whole_day = floor<days>(DateTime::max());
        // Pick the earlier of the two
        const sys_days chosen_day = (desired_cap <= max_whole_day) ? desired_cap : max_whole_day;
        // Convert back to the DateTime representation at midnight of the chosen day
        return DateTime{chosen_day.time_since_epoch()};
    }

    /** Smallest representable time-delta (1 microsecond). */
    constexpr TimeDelta smallest_time_increment() noexcept { return TimeDelta(1); }

    /** Earliest start time accepted by the runtime (``min_time()`` plus one tick). */
    constexpr DateTime min_start_time() noexcept { return min_time() + smallest_time_increment(); }

    /** Latest end time accepted by the runtime (``max_time()`` minus one tick). */
    inline DateTime max_end_time() noexcept { return max_time() - smallest_time_increment(); }

    /** Sentinel for the never-modified time; equal to ``min_time()``. */
    inline auto static MIN_DT = min_time();
    /** Latest representable time; equal to ``max_time()``. */
    inline auto static MAX_DT = max_time();
    /** Earliest valid start time; equal to ``min_start_time()``. */
    inline auto static MIN_ST = min_start_time();
    /** Latest valid end time; equal to ``max_end_time()``. */
    inline auto static MAX_ET = max_end_time();

    /** Smallest time delta; equal to ``smallest_time_increment()``. */
    inline auto static MIN_TD = smallest_time_increment();

    /**
     * Time-of-day scalar (microseconds since midnight), mirroring Python's
     * ``datetime.time``. A distinct strong type — NOT an alias of ``TimeDelta``
     * — so operator dispatch and schema identity can tell the two apart.
     * The valid range is [0, 86'400'000'000); like Python, validation belongs
     * at construction, not in the value layer.
     */
    struct Time
    {
        std::int64_t microseconds{0};

        friend constexpr bool operator==(const Time &, const Time &) noexcept = default;
        friend constexpr std::strong_ordering operator<=>(const Time &, const Time &) noexcept = default;
    };

    /** Build a ``Time`` from wall-clock components. */
    [[nodiscard]] constexpr Time time_of_day(int hours, int minutes, int seconds = 0,
                                             std::int64_t microseconds = 0) noexcept
    {
        return Time{((static_cast<std::int64_t>(hours) * 60 + minutes) * 60 + seconds) * 1'000'000 +
                    microseconds};
    }

    /** The time-of-day component of a ``DateTime``. */
    [[nodiscard]] inline Time time_of_day(DateTime when) noexcept
    {
        const auto day = std::chrono::floor<std::chrono::days>(when);
        return Time{std::chrono::duration_cast<std::chrono::microseconds>(when - day).count()};
    }

    /** Render as ``HH:MM:SS[.ffffff]`` (the Python ``time`` string form). */
    std::ostream &operator<<(std::ostream &os, const Time &value);
} // namespace hgraph

/** ``std::hash`` for ``Time`` (usable as a TSS element / TSD key). */
template <>
struct std::hash<hgraph::Time>
{
    [[nodiscard]] std::size_t operator()(const hgraph::Time &value) const noexcept
    {
        return std::hash<std::int64_t>{}(value.microseconds);
    }
};
#endif  // HGRAPH_DATE_TIME_H
