#ifndef HGL_SYNTAX_TEMPORAL_H
#define HGL_SYNTAX_TEMPORAL_H

#include <cstdint>
#include <optional>
#include <string>
#include <string_view>

namespace hgl::syntax
{
    /// The eight temporal scalar types of the language (developer guide,
    /// "Temporal scalar types"). The literal's shape selects the kind.
    enum class TemporalKind
    {
        Date,           ///< `@2026-09-03`
        Time,           ///< `@09:30`
        DateTime,       ///< `@2026-09-03T09:30Z`, normalized to UTC
        CivilDateTime,  ///< `@2026-09-03T10:30`
        ZonedDateTime,  ///< `@2026-09-03T10:30+01[Europe/London]`
        ZonedTime,      ///< `@09:30[America/New_York]`
        TimeZone,       ///< `@[Europe/London]`
        Duration,       ///< `5m`, `1h30m`
    };

    [[nodiscard]] std::string_view temporal_kind_name(TemporalKind kind) noexcept;

    /// A validated and normalized temporal literal. Microsecond fields use
    /// hgraph's conventions: instants count from the Unix epoch in UTC, times
    /// from midnight, and dates in whole days from the epoch.
    struct TemporalValue
    {
        TemporalKind kind{TemporalKind::Duration};
        /// Date: days since 1970-01-01. Time/ZonedTime: microseconds since
        /// midnight. DateTime/ZonedDateTime: UTC microseconds since the epoch.
        /// CivilDateTime: local microseconds since the epoch. Duration:
        /// microseconds. TimeZone: unused (0).
        std::int64_t micros{0};
        /// ZonedDateTime only: the literal's offset from UTC in seconds.
        std::int32_t offset_seconds{0};
        /// ZonedDateTime, ZonedTime, TimeZone: the zone name as written.
        std::string zone{};

        friend bool operator==(const TemporalValue &, const TemporalValue &) = default;
    };

    /// Result of validating one temporal literal spelling.
    struct TemporalParseResult
    {
        std::optional<TemporalValue> value{};
        /// The diagnostic message (developer guide wording) when `value` is empty.
        std::string error{};
        /// Non-empty when the diagnostic should carry a hint note.
        std::string hint{};
    };

    /// Parse an `@`-literal (`spelling` includes the `@`) or a duration
    /// literal (`spelling` is the digit-and-unit run) applying every rule of
    /// the "Literals" section: calendar validity, `24:00`, offset and zone
    /// requirements, zone-name syntax, digit counts, whole microseconds, unit
    /// order, and range.
    [[nodiscard]] TemporalParseResult parse_temporal_literal(std::string_view spelling);

    /// The canonical spelling of the "Canonical spelling" section.
    [[nodiscard]] std::string canonical_spelling(const TemporalValue &value);

    /// RFC 0002 zone-name syntax check (letters, digits, `.`, `_`, `-`, `+`,
    /// `/`; no empty, leading, trailing, or repeated `/`; no `.` or `..`
    /// components; at most 255 bytes).
    [[nodiscard]] bool is_valid_zone_name(std::string_view name) noexcept;

    /// Days since the epoch for a proleptic Gregorian date; nullopt when the
    /// date is not in the calendar.
    [[nodiscard]] std::optional<std::int64_t> days_from_civil(int year, int month, int day) noexcept;
    struct CivilDate
    {
        int year;
        int month;
        int day;
    };
    [[nodiscard]] CivilDate civil_from_days(std::int64_t days) noexcept;
}  // namespace hgl::syntax

#endif  // HGL_SYNTAX_TEMPORAL_H
