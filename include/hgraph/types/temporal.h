#ifndef HGRAPH_TYPES_TEMPORAL_H
#define HGRAPH_TYPES_TEMPORAL_H

#include <hgraph/hgraph_export.h>
#include <hgraph/util/date_time.h>

#include <algorithm>
#include <array>
#include <compare>
#include <concepts>
#include <cstddef>
#include <cstdint>
#include <initializer_list>
#include <iosfwd>
#include <limits>
#include <memory>
#include <optional>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>

namespace hgraph
{
    class GlobalStateView;

    /** Absolute POSIX timeline value. DateTime remains the canonical schema. */
    using Instant = DateTime;
    /** Fixed elapsed microseconds. TimeDelta remains the canonical schema. */
    using Duration = TimeDelta;
    using CivilDate = Date;
    using CivilTime = Time;

    // The aliases above intentionally retain std::chrono's raw operator
    // contract. These named functions are the checked C++ and graph surface.
    [[nodiscard]] HGRAPH_EXPORT Instant checked_add(Instant value, Duration delta);
    [[nodiscard]] HGRAPH_EXPORT Instant checked_subtract(Instant value, Duration delta);
    [[nodiscard]] HGRAPH_EXPORT Duration checked_subtract(Instant lhs, Instant rhs);
    [[nodiscard]] HGRAPH_EXPORT Duration checked_add(Duration lhs, Duration rhs);
    [[nodiscard]] HGRAPH_EXPORT Duration checked_subtract(Duration lhs, Duration rhs);
    [[nodiscard]] HGRAPH_EXPORT Duration checked_negate(Duration value);
    [[nodiscard]] HGRAPH_EXPORT Duration checked_multiply(Duration value, std::int64_t factor);
    [[nodiscard]] HGRAPH_EXPORT Duration checked_multiply(Duration value, double factor);
    [[nodiscard]] HGRAPH_EXPORT Duration checked_divide(Duration value, std::int64_t divisor);
    [[nodiscard]] HGRAPH_EXPORT Duration checked_divide(Duration value, double divisor);

    /** Gregorian civil date/time fields stored as local epoch microseconds. */
    class HGRAPH_CLASS_EXPORT CivilDateTime
    {
      public:
        constexpr CivilDateTime() noexcept = default;
        CivilDateTime(CivilDate date, CivilTime time);
        CivilDateTime(CivilDate date, int hour, int minute = 0, int second = 0,
                      int microsecond = 0);

        [[nodiscard]] static CivilDateTime from_epoch_microseconds(std::int64_t value);
        [[nodiscard]] constexpr std::int64_t epoch_microseconds() const noexcept
        {
            return microseconds_;
        }
        [[nodiscard]] CivilDate date() const noexcept;
        [[nodiscard]] CivilTime time() const noexcept;
        [[nodiscard]] int year() const noexcept;
        [[nodiscard]] unsigned month() const noexcept;
        [[nodiscard]] unsigned day() const noexcept;
        [[nodiscard]] int hour() const noexcept;
        [[nodiscard]] int minute() const noexcept;
        [[nodiscard]] int second() const noexcept;
        [[nodiscard]] int microsecond() const noexcept;
        [[nodiscard]] int weekday() const noexcept;
        [[nodiscard]] int isoweekday() const noexcept;
        [[nodiscard]] int day_of_year() const noexcept;

        friend constexpr bool operator==(const CivilDateTime &,
                                         const CivilDateTime &) noexcept = default;
        friend constexpr std::strong_ordering operator<=>(const CivilDateTime &,
                                                           const CivilDateTime &) noexcept = default;

      private:
        explicit constexpr CivilDateTime(std::int64_t value, int) noexcept
            : microseconds_(value)
        {
        }

        std::int64_t microseconds_{0};
    };

    [[nodiscard]] HGRAPH_EXPORT CivilDateTime checked_add(CivilDateTime value, Duration delta);
    [[nodiscard]] HGRAPH_EXPORT CivilDateTime checked_subtract(CivilDateTime value, Duration delta);
    [[nodiscard]] HGRAPH_EXPORT Duration checked_subtract(CivilDateTime lhs, CivilDateTime rhs);
    [[nodiscard]] HGRAPH_EXPORT CivilDate checked_add_days(CivilDate value, std::int64_t days);
    [[nodiscard]] HGRAPH_EXPORT Duration checked_subtract(CivilDate lhs, CivilDate rhs);

    /** Calendar-relative total months plus days. */
    class HGRAPH_CLASS_EXPORT Period
    {
      public:
        constexpr Period() noexcept = default;
        Period(std::int64_t years, std::int64_t months = 0, std::int64_t days = 0);

        [[nodiscard]] static constexpr Period from_total(std::int32_t total_months,
                                                         std::int32_t days) noexcept
        {
            return Period{total_months, days, InternalTag{}};
        }

        [[nodiscard]] constexpr std::int32_t total_months() const noexcept
        {
            return total_months_;
        }
        [[nodiscard]] constexpr std::int32_t years() const noexcept
        {
            return total_months_ / 12;
        }
        [[nodiscard]] constexpr std::int32_t months() const noexcept
        {
            return total_months_ % 12;
        }
        [[nodiscard]] constexpr std::int32_t days() const noexcept { return days_; }

        friend constexpr bool operator==(const Period &, const Period &) noexcept = default;

      private:
        struct InternalTag
        {
        };

        constexpr Period(std::int32_t total_months, std::int32_t days, InternalTag) noexcept
            : total_months_(total_months), days_(days)
        {
        }

        std::int32_t total_months_{0};
        std::int32_t days_{0};
    };

    [[nodiscard]] HGRAPH_EXPORT Period checked_add(Period lhs, Period rhs);
    [[nodiscard]] HGRAPH_EXPORT Period checked_subtract(Period lhs, Period rhs);
    [[nodiscard]] HGRAPH_EXPORT Period checked_negate(Period value);
    [[nodiscard]] HGRAPH_EXPORT Period checked_multiply(Period value, std::int64_t factor);

    enum class MonthEndPolicy : std::uint8_t
    {
        Reject,
        Clamp,
        PreserveEndOfMonth,
    };

    [[nodiscard]] HGRAPH_EXPORT CivilDate apply_period(
        CivilDate value, Period period, MonthEndPolicy policy = MonthEndPolicy::Reject);
    [[nodiscard]] HGRAPH_EXPORT CivilDateTime apply_period(
        CivilDateTime value, Period period, MonthEndPolicy policy = MonthEndPolicy::Reject);

    /**
     * Eight-byte process-local zone-name handle.
     *
     * Raw handles never cross a process or serialization boundary; codecs use
     * the exact name and intern it again in the receiving process.
     */
    struct ZoneIdPayload
    {
        std::uint32_t slot{0};
        std::uint16_t generation{0};
        std::uint16_t name_tag{0};

        friend constexpr bool operator==(const ZoneIdPayload &,
                                         const ZoneIdPayload &) noexcept = default;
        friend constexpr std::strong_ordering operator<=>(const ZoneIdPayload &,
                                                           const ZoneIdPayload &) noexcept = default;
    };

    class HGRAPH_CLASS_EXPORT ZoneId
    {
      public:
        constexpr ZoneId() noexcept = default;
        explicit ZoneId(std::string_view name);

        [[nodiscard]] static bool valid_syntax(std::string_view name) noexcept;
        [[nodiscard]] bool valid() const noexcept;
        [[nodiscard]] std::string_view name() const;
        [[nodiscard]] std::string_view value() const { return name(); }
        [[nodiscard]] constexpr ZoneIdPayload payload() const noexcept { return payload_; }
        [[nodiscard]] constexpr std::uint32_t slot() const noexcept { return payload_.slot; }
        [[nodiscard]] constexpr std::uint16_t generation() const noexcept
        {
            return payload_.generation;
        }
        [[nodiscard]] constexpr std::uint16_t name_tag() const noexcept
        {
            return payload_.name_tag;
        }

        friend constexpr bool operator==(const ZoneId &, const ZoneId &) noexcept = default;
        friend constexpr std::strong_ordering operator<=>(const ZoneId &,
                                                           const ZoneId &) noexcept = default;

        /** Test/diagnostic construction; lookup still validates the payload. */
        [[nodiscard]] static constexpr ZoneId from_payload(ZoneIdPayload payload) noexcept
        {
            return ZoneId{payload, InternalTag{}};
        }

      private:
        struct InternalTag
        {
        };
        constexpr ZoneId(ZoneIdPayload payload, InternalTag) noexcept
            : payload_(payload)
        {
        }

        ZoneIdPayload payload_{};
    };

    enum class AmbiguousTimePolicy : std::uint8_t
    {
        Reject,
        Earliest,
        Latest,
    };

    enum class NonexistentTimePolicy : std::uint8_t
    {
        Reject,
        NextValid,
        PreviousValid,
    };

    struct ResolvedInstant
    {
        Instant instant{};
        std::int32_t offset_seconds{0};

        friend bool operator==(const ResolvedInstant &, const ResolvedInstant &) noexcept = default;
    };

    enum class LocalResolutionKind : std::uint8_t
    {
        Unique,
        Ambiguous,
        Nonexistent,
    };

    struct LocalResolution
    {
        LocalResolutionKind kind{LocalResolutionKind::Unique};
        ResolvedInstant first{};
        ResolvedInstant second{};

        friend bool operator==(const LocalResolution &, const LocalResolution &) noexcept = default;
    };

    struct OffsetInfo
    {
        std::int32_t offset_seconds{0};
        Instant begin{Instant::min()};
        Instant end{Instant::max()};

        friend bool operator==(const OffsetInfo &, const OffsetInfo &) noexcept = default;
    };

    class HGRAPH_CLASS_EXPORT TimeZoneProvider
    {
      public:
        virtual ~TimeZoneProvider() = default;
        [[nodiscard]] virtual std::string_view version() const noexcept = 0;
        [[nodiscard]] virtual bool contains(ZoneId zone) const noexcept = 0;
        [[nodiscard]] virtual OffsetInfo at(Instant instant, ZoneId zone) const = 0;
        [[nodiscard]] virtual LocalResolution resolve(CivilDateTime local, ZoneId zone) const = 0;
    };

    enum class TimeZoneBackend : std::uint8_t
    {
        Standard,
        DateTz,
    };

    [[nodiscard]] HGRAPH_EXPORT TimeZoneBackend configured_time_zone_backend() noexcept;
    [[nodiscard]] HGRAPH_EXPORT std::shared_ptr<const TimeZoneProvider>
    make_time_zone_provider();
    /** Test-only reset of the process-wide interned native-zone bindings. */
    HGRAPH_EXPORT void clear_time_zone_provider_cache() noexcept;
    /** Test-only registry reset; production zone-name storage is append-only. */
    HGRAPH_EXPORT void clear_zone_name_registry() noexcept;
    inline constexpr std::string_view TIME_ZONE_PROVIDER_STATE_KEY =
        "__hgraph_time_zone_provider__";
    HGRAPH_EXPORT void set_time_zone_provider(
        GlobalStateView state,
        std::shared_ptr<const TimeZoneProvider> provider);
    [[nodiscard]] HGRAPH_EXPORT const TimeZoneProvider &
    time_zone_provider(GlobalStateView state);

    /** Resolved instant, named-zone intent, and the offset used to resolve it. */
    class HGRAPH_CLASS_EXPORT ZonedDateTime
    {
      public:
        constexpr ZonedDateTime() noexcept = default;

        [[nodiscard]] static ZonedDateTime from_resolved(Instant instant, ZoneId zone,
                                                         std::int32_t offset_seconds);

        [[nodiscard]] constexpr Instant instant() const noexcept { return instant_; }
        [[nodiscard]] constexpr ZoneId zone() const noexcept { return zone_; }
        [[nodiscard]] constexpr std::int32_t offset_seconds() const noexcept
        {
            return offset_seconds_;
        }
        [[nodiscard]] CivilDateTime civil() const;
        [[nodiscard]] constexpr bool same_instant(const ZonedDateTime &other) const noexcept
        {
            return instant_ == other.instant_;
        }

        friend constexpr bool operator==(const ZonedDateTime &,
                                         const ZonedDateTime &) noexcept = default;

      private:
        Instant instant_{};
        ZoneId zone_{};
        std::int32_t offset_seconds_{0};
    };

    [[nodiscard]] HGRAPH_EXPORT ZonedDateTime at_zone(
        Instant instant, ZoneId zone, const TimeZoneProvider &provider);
    [[nodiscard]] HGRAPH_EXPORT ZonedDateTime resolve(
        CivilDateTime local, ZoneId zone, const TimeZoneProvider &provider,
        AmbiguousTimePolicy ambiguous = AmbiguousTimePolicy::Reject,
        NonexistentTimePolicy nonexistent = NonexistentTimePolicy::Reject);
    [[nodiscard]] HGRAPH_EXPORT ZonedDateTime convert_zone(
        ZonedDateTime value, ZoneId zone, const TimeZoneProvider &provider);
    [[nodiscard]] HGRAPH_EXPORT ZonedDateTime checked_add(
        ZonedDateTime value, Duration delta, const TimeZoneProvider &provider);

    enum class Boundary : std::uint8_t
    {
        Open,
        Closed,
    };

    template <std::totally_ordered T>
    class TimeRange;

    template <std::totally_ordered T, std::size_t Capacity>
    class FixedRangeSet;

    namespace temporal_detail
    {
        inline constexpr std::uint8_t range_empty = 1U << 0U;
        inline constexpr std::uint8_t range_lower_unbounded = 1U << 1U;
        inline constexpr std::uint8_t range_upper_unbounded = 1U << 2U;
        inline constexpr std::uint8_t range_lower_closed = 1U << 3U;
        inline constexpr std::uint8_t range_upper_closed = 1U << 4U;

        [[nodiscard]] constexpr Boundary opposite(Boundary boundary) noexcept
        {
            return boundary == Boundary::Closed ? Boundary::Open : Boundary::Closed;
        }
    }  // namespace temporal_detail

    /** Normalized empty, bounded, half-bounded, or unbounded interval. */
    template <std::totally_ordered T>
    class TimeRange
    {
      public:
        using value_type = T;

        constexpr TimeRange() noexcept = default;

        TimeRange(T start, T end, Boundary lower = Boundary::Closed,
                  Boundary upper = Boundary::Open)
            : TimeRange(bounded(std::move(start), std::move(end), lower, upper))
        {
        }

        [[nodiscard]] static constexpr TimeRange make_empty() noexcept { return {}; }

        [[nodiscard]] static TimeRange all()
        {
            TimeRange value;
            value.flags_ = temporal_detail::range_lower_unbounded |
                           temporal_detail::range_upper_unbounded;
            return value;
        }

        [[nodiscard]] static TimeRange bounded(T start, T end,
                                               Boundary lower = Boundary::Closed,
                                               Boundary upper = Boundary::Open)
        {
            return normalize(std::move(start), std::move(end), false, false, lower, upper);
        }

        [[nodiscard]] static TimeRange from(T start,
                                            Boundary lower = Boundary::Closed)
        {
            return normalize(std::move(start), T{}, false, true, lower, Boundary::Open);
        }

        [[nodiscard]] static TimeRange until(T end,
                                             Boundary upper = Boundary::Open)
        {
            return normalize(T{}, std::move(end), true, false, Boundary::Open, upper);
        }

        [[nodiscard]] constexpr bool empty() const noexcept
        {
            return (flags_ & temporal_detail::range_empty) != 0;
        }
        [[nodiscard]] constexpr bool lower_unbounded() const noexcept
        {
            return !empty() &&
                   (flags_ & temporal_detail::range_lower_unbounded) != 0;
        }
        [[nodiscard]] constexpr bool upper_unbounded() const noexcept
        {
            return !empty() &&
                   (flags_ & temporal_detail::range_upper_unbounded) != 0;
        }
        [[nodiscard]] constexpr bool bounded() const noexcept
        {
            return !empty() && !lower_unbounded() && !upper_unbounded();
        }
        [[nodiscard]] constexpr bool lower_bounded() const noexcept
        {
            return !empty() && !lower_unbounded();
        }
        [[nodiscard]] constexpr bool upper_bounded() const noexcept
        {
            return !empty() && !upper_unbounded();
        }
        [[nodiscard]] constexpr Boundary lower_boundary() const noexcept
        {
            return lower_bounded() &&
                           (flags_ & temporal_detail::range_lower_closed) != 0
                       ? Boundary::Closed
                       : Boundary::Open;
        }
        [[nodiscard]] constexpr Boundary upper_boundary() const noexcept
        {
            return upper_bounded() &&
                           (flags_ & temporal_detail::range_upper_closed) != 0
                       ? Boundary::Closed
                       : Boundary::Open;
        }
        [[nodiscard]] std::optional<T> start() const
        {
            return lower_bounded() ? std::optional<T>{lower_} : std::nullopt;
        }
        [[nodiscard]] std::optional<T> end() const
        {
            return upper_bounded() ? std::optional<T>{upper_} : std::nullopt;
        }

        [[nodiscard]] constexpr const T &lower_value() const noexcept { return lower_; }
        [[nodiscard]] constexpr const T &upper_value() const noexcept { return upper_; }

        [[nodiscard]] bool contains(const T &value) const noexcept
        {
            if (empty()) { return false; }
            if (lower_bounded() &&
                (value < lower_ ||
                 (value == lower_ && lower_boundary() == Boundary::Open)))
            {
                return false;
            }
            if (upper_bounded() &&
                (value > upper_ ||
                 (value == upper_ && upper_boundary() == Boundary::Open)))
            {
                return false;
            }
            return true;
        }

        [[nodiscard]] bool contains(const TimeRange &other) const noexcept
        {
            if (other.empty()) { return true; }
            if (empty()) { return false; }

            if (!lower_unbounded())
            {
                if (other.lower_unbounded() || other.lower_ < lower_) { return false; }
                if (other.lower_ == lower_ && lower_boundary() == Boundary::Open &&
                    other.lower_boundary() == Boundary::Closed)
                {
                    return false;
                }
            }
            if (!upper_unbounded())
            {
                if (other.upper_unbounded() || other.upper_ > upper_) { return false; }
                if (other.upper_ == upper_ && upper_boundary() == Boundary::Open &&
                    other.upper_boundary() == Boundary::Closed)
                {
                    return false;
                }
            }
            return true;
        }

        [[nodiscard]] TimeRange intersection(const TimeRange &other) const
        {
            if (empty() || other.empty()) { return {}; }

            bool lower_unbounded_result = lower_unbounded() && other.lower_unbounded();
            T lower{};
            Boundary lower_boundary_result = Boundary::Open;
            if (!lower_unbounded_result)
            {
                if (lower_unbounded())
                {
                    lower = other.lower_;
                    lower_boundary_result = other.lower_boundary();
                }
                else if (other.lower_unbounded())
                {
                    lower = lower_;
                    lower_boundary_result = lower_boundary();
                }
                else if (lower_ > other.lower_)
                {
                    lower = lower_;
                    lower_boundary_result = lower_boundary();
                }
                else if (other.lower_ > lower_)
                {
                    lower = other.lower_;
                    lower_boundary_result = other.lower_boundary();
                }
                else
                {
                    lower = lower_;
                    lower_boundary_result =
                        lower_boundary() == Boundary::Closed &&
                                other.lower_boundary() == Boundary::Closed
                            ? Boundary::Closed
                            : Boundary::Open;
                }
            }

            bool upper_unbounded_result = upper_unbounded() && other.upper_unbounded();
            T upper{};
            Boundary upper_boundary_result = Boundary::Open;
            if (!upper_unbounded_result)
            {
                if (upper_unbounded())
                {
                    upper = other.upper_;
                    upper_boundary_result = other.upper_boundary();
                }
                else if (other.upper_unbounded())
                {
                    upper = upper_;
                    upper_boundary_result = upper_boundary();
                }
                else if (upper_ < other.upper_)
                {
                    upper = upper_;
                    upper_boundary_result = upper_boundary();
                }
                else if (other.upper_ < upper_)
                {
                    upper = other.upper_;
                    upper_boundary_result = other.upper_boundary();
                }
                else
                {
                    upper = upper_;
                    upper_boundary_result =
                        upper_boundary() == Boundary::Closed &&
                                other.upper_boundary() == Boundary::Closed
                            ? Boundary::Closed
                            : Boundary::Open;
                }
            }

            if (!lower_unbounded_result && !upper_unbounded_result)
            {
                if (upper < lower) { return {}; }
                if (upper == lower &&
                    (lower_boundary_result == Boundary::Open ||
                     upper_boundary_result == Boundary::Open))
                {
                    return {};
                }
            }
            return normalize(std::move(lower), std::move(upper),
                             lower_unbounded_result, upper_unbounded_result,
                             lower_boundary_result, upper_boundary_result);
        }

        [[nodiscard]] bool overlaps(const TimeRange &other) const
        {
            return !intersection(other).empty();
        }

        [[nodiscard]] bool touches(const TimeRange &other) const noexcept
        {
            if (empty() || other.empty()) { return false; }
            return (upper_bounded() && other.lower_bounded() &&
                    upper_ == other.lower_) ||
                   (other.upper_bounded() && lower_bounded() &&
                    other.upper_ == lower_);
        }

        [[nodiscard]] bool adjacent(const TimeRange &other) const
        {
            if (empty() || other.empty() || overlaps(other)) { return false; }
            if (upper_bounded() && other.lower_bounded() &&
                upper_ == other.lower_)
            {
                return upper_boundary() != other.lower_boundary();
            }
            if (other.upper_bounded() && lower_bounded() &&
                other.upper_ == lower_)
            {
                return other.upper_boundary() != lower_boundary();
            }
            return false;
        }

        [[nodiscard]] bool mergeable(const TimeRange &other) const
        {
            return overlaps(other) || adjacent(other);
        }

        [[nodiscard]] std::optional<TimeRange> merge(const TimeRange &other) const
        {
            if (empty()) { return other; }
            if (other.empty()) { return *this; }
            if (!mergeable(other)) { return std::nullopt; }
            return hull(other);
        }

        [[nodiscard]] TimeRange hull(const TimeRange &other) const
        {
            if (empty()) { return other; }
            if (other.empty()) { return *this; }

            bool lower_unbounded_result = lower_unbounded() || other.lower_unbounded();
            T lower{};
            Boundary lower_boundary_result = Boundary::Open;
            if (!lower_unbounded_result)
            {
                if (lower_ < other.lower_)
                {
                    lower = lower_;
                    lower_boundary_result = lower_boundary();
                }
                else if (other.lower_ < lower_)
                {
                    lower = other.lower_;
                    lower_boundary_result = other.lower_boundary();
                }
                else
                {
                    lower = lower_;
                    lower_boundary_result =
                        lower_boundary() == Boundary::Closed ||
                                other.lower_boundary() == Boundary::Closed
                            ? Boundary::Closed
                            : Boundary::Open;
                }
            }

            bool upper_unbounded_result = upper_unbounded() || other.upper_unbounded();
            T upper{};
            Boundary upper_boundary_result = Boundary::Open;
            if (!upper_unbounded_result)
            {
                if (upper_ > other.upper_)
                {
                    upper = upper_;
                    upper_boundary_result = upper_boundary();
                }
                else if (other.upper_ > upper_)
                {
                    upper = other.upper_;
                    upper_boundary_result = other.upper_boundary();
                }
                else
                {
                    upper = upper_;
                    upper_boundary_result =
                        upper_boundary() == Boundary::Closed ||
                                other.upper_boundary() == Boundary::Closed
                            ? Boundary::Closed
                            : Boundary::Open;
                }
            }
            return normalize(std::move(lower), std::move(upper),
                             lower_unbounded_result, upper_unbounded_result,
                             lower_boundary_result, upper_boundary_result);
        }

        [[nodiscard]] FixedRangeSet<T, 2> difference(const TimeRange &other) const;
        [[nodiscard]] FixedRangeSet<T, 2> set_union(const TimeRange &other) const;

        friend bool operator==(const TimeRange &, const TimeRange &) noexcept = default;

        friend std::strong_ordering operator<=>(const TimeRange &lhs,
                                                const TimeRange &rhs) noexcept
        {
            if (lhs.empty() != rhs.empty())
            {
                return lhs.empty() ? std::strong_ordering::less
                                   : std::strong_ordering::greater;
            }
            if (lhs.empty()) { return std::strong_ordering::equal; }
            if (lhs.lower_unbounded() != rhs.lower_unbounded())
            {
                return lhs.lower_unbounded() ? std::strong_ordering::less
                                             : std::strong_ordering::greater;
            }
            if (!lhs.lower_unbounded())
            {
                if (lhs.lower_ < rhs.lower_) { return std::strong_ordering::less; }
                if (lhs.lower_ > rhs.lower_) { return std::strong_ordering::greater; }
                if (lhs.lower_boundary() != rhs.lower_boundary())
                {
                    return lhs.lower_boundary() == Boundary::Closed
                               ? std::strong_ordering::less
                               : std::strong_ordering::greater;
                }
            }
            if (lhs.upper_unbounded() != rhs.upper_unbounded())
            {
                return lhs.upper_unbounded() ? std::strong_ordering::greater
                                             : std::strong_ordering::less;
            }
            if (!lhs.upper_unbounded())
            {
                if (lhs.upper_ < rhs.upper_) { return std::strong_ordering::less; }
                if (lhs.upper_ > rhs.upper_) { return std::strong_ordering::greater; }
                if (lhs.upper_boundary() != rhs.upper_boundary())
                {
                    return lhs.upper_boundary() == Boundary::Open
                               ? std::strong_ordering::less
                               : std::strong_ordering::greater;
                }
            }
            return std::strong_ordering::equal;
        }

      private:
        static TimeRange normalize(T lower, T upper, bool lower_unbounded,
                                   bool upper_unbounded, Boundary lower_boundary,
                                   Boundary upper_boundary)
        {
            if (!lower_unbounded && !upper_unbounded)
            {
                if (upper < lower)
                {
                    throw std::invalid_argument("time range end precedes start");
                }
                if (upper == lower &&
                    (lower_boundary == Boundary::Open ||
                     upper_boundary == Boundary::Open))
                {
                    return {};
                }
            }

            TimeRange value;
            value.lower_ = lower_unbounded ? T{} : std::move(lower);
            value.upper_ = upper_unbounded ? T{} : std::move(upper);
            value.flags_ = 0;
            if (lower_unbounded) { value.flags_ |= temporal_detail::range_lower_unbounded; }
            else if (lower_boundary == Boundary::Closed)
            {
                value.flags_ |= temporal_detail::range_lower_closed;
            }
            if (upper_unbounded) { value.flags_ |= temporal_detail::range_upper_unbounded; }
            else if (upper_boundary == Boundary::Closed)
            {
                value.flags_ |= temporal_detail::range_upper_closed;
            }
            return value;
        }

        T lower_{};
        T upper_{};
        std::uint8_t flags_{temporal_detail::range_empty};
    };

    /** Immutable allocation-free sequence of up to Capacity normalized ranges. */
    template <std::totally_ordered T, std::size_t Capacity>
    class FixedRangeSet
    {
        static_assert(Capacity > 0 && Capacity <= 255);

      public:
        using value_type = TimeRange<T>;
        using const_iterator = const value_type *;

        constexpr FixedRangeSet() noexcept = default;

        explicit FixedRangeSet(std::span<const value_type> ranges)
        {
            for (const value_type &range : ranges)
            {
                if (range.empty()) { continue; }
                if (size_ >= Capacity)
                {
                    throw std::length_error("fixed range set capacity exceeded");
                }
                ranges_[size_++] = range;
            }
            // The active prefix is bounded by Capacity.  A small insertion
            // sort avoids libstdc++'s fixed 16-element final insertion-sort
            // probe, which triggers -Warray-bounds for small std::array
            // capacities even though the std::sort range itself is valid.
            for (std::size_t index = 1; index < size_; ++index)
            {
                value_type value = ranges_[index];
                std::size_t insertion = index;
                while (insertion != 0 && value < ranges_[insertion - 1])
                {
                    ranges_[insertion] = ranges_[insertion - 1];
                    --insertion;
                }
                ranges_[insertion] = std::move(value);
            }
            std::uint8_t out = 0;
            for (std::uint8_t index = 0; index < size_; ++index)
            {
                if (out != 0 && ranges_[out - 1].mergeable(ranges_[index]))
                {
                    ranges_[out - 1] = *ranges_[out - 1].merge(ranges_[index]);
                }
                else
                {
                    ranges_[out++] = ranges_[index];
                }
            }
            for (std::size_t index = out; index < Capacity; ++index)
            {
                ranges_[index] = value_type{};
            }
            size_ = out;
        }

        FixedRangeSet(std::initializer_list<value_type> ranges)
            : FixedRangeSet(std::span<const value_type>{ranges.begin(), ranges.size()})
        {
        }

        [[nodiscard]] static constexpr std::size_t capacity() noexcept
        {
            return Capacity;
        }
        [[nodiscard]] constexpr std::size_t size() const noexcept { return size_; }
        [[nodiscard]] constexpr bool empty() const noexcept { return size_ == 0; }
        [[nodiscard]] constexpr const value_type &operator[](std::size_t index) const
        {
            if (index >= size_) { throw std::out_of_range("fixed range set index"); }
            return ranges_[index];
        }
        [[nodiscard]] constexpr const_iterator begin() const noexcept
        {
            return ranges_.data();
        }
        [[nodiscard]] constexpr const_iterator end() const noexcept
        {
            return ranges_.data() + size_;
        }

        friend bool operator==(const FixedRangeSet &, const FixedRangeSet &) noexcept = default;

      private:
        std::array<value_type, Capacity> ranges_{};
        std::uint8_t size_{0};
    };

    template <std::totally_ordered T>
    FixedRangeSet<T, 2> TimeRange<T>::difference(const TimeRange &other) const
    {
        if (empty()) { return {}; }
        if (other.empty()) { return FixedRangeSet<T, 2>{{*this}}; }
        const TimeRange overlap = intersection(other);
        if (overlap.empty()) { return FixedRangeSet<T, 2>{{*this}}; }
        if (overlap == *this) { return {}; }

        std::array<TimeRange, 2> pieces{};
        std::size_t count = 0;
        if (overlap.lower_bounded())
        {
            TimeRange left = normalize(
                lower_, overlap.lower_, lower_unbounded(), false,
                lower_boundary(), temporal_detail::opposite(overlap.lower_boundary()));
            if (!left.empty()) { pieces[count++] = left; }
        }
        if (overlap.upper_bounded())
        {
            TimeRange right = normalize(
                overlap.upper_, upper_, false, upper_unbounded(),
                temporal_detail::opposite(overlap.upper_boundary()), upper_boundary());
            if (!right.empty()) { pieces[count++] = right; }
        }
        return FixedRangeSet<T, 2>{
            std::span<const TimeRange>{pieces.data(), count}};
    }

    template <std::totally_ordered T>
    FixedRangeSet<T, 2> TimeRange<T>::set_union(const TimeRange &other) const
    {
        if (empty() && other.empty()) { return {}; }
        if (empty()) { return FixedRangeSet<T, 2>{{other}}; }
        if (other.empty()) { return FixedRangeSet<T, 2>{{*this}}; }
        if (const auto combined = merge(other))
        {
            return FixedRangeSet<T, 2>{{*combined}};
        }
        return *this < other ? FixedRangeSet<T, 2>{{*this, other}}
                             : FixedRangeSet<T, 2>{{other, *this}};
    }

    using InstantRange = TimeRange<Instant>;
    using CivilDateRange = TimeRange<CivilDate>;
    using InstantRangeSet = FixedRangeSet<Instant, 2>;
    using CivilDateRangeSet = FixedRangeSet<CivilDate, 2>;

    [[nodiscard]] HGRAPH_EXPORT InstantRange shift(InstantRange range, Duration delta);
    [[nodiscard]] HGRAPH_EXPORT CivilDateRange shift(
        CivilDateRange range, Period period,
        MonthEndPolicy policy = MonthEndPolicy::Reject);
    [[nodiscard]] HGRAPH_EXPORT std::optional<Duration> extent(InstantRange range);

    [[nodiscard]] HGRAPH_EXPORT Duration floor(Duration value, Duration quantum);
    [[nodiscard]] HGRAPH_EXPORT Duration ceil(Duration value, Duration quantum);
    [[nodiscard]] HGRAPH_EXPORT Duration round(Duration value, Duration quantum);
    [[nodiscard]] HGRAPH_EXPORT Instant floor(
        Instant value, Duration quantum, Instant origin = Instant{});
    [[nodiscard]] HGRAPH_EXPORT Instant ceil(
        Instant value, Duration quantum, Instant origin = Instant{});
    [[nodiscard]] HGRAPH_EXPORT Instant round(
        Instant value, Duration quantum, Instant origin = Instant{});
    [[nodiscard]] HGRAPH_EXPORT InstantRange bucket(
        Instant value, Duration width, Instant origin = Instant{});

    template <typename Visit>
    void iterate(InstantRange range, Duration step, Visit &&visit)
    {
        if (!range.bounded()) { throw std::invalid_argument("cannot iterate an unbounded range"); }
        if (step <= Duration::zero()) { throw std::invalid_argument("iteration step must be positive"); }
        Instant current = *range.start();
        if (range.lower_boundary() == Boundary::Open)
        {
            current = checked_add(current, step);
        }
        while (range.contains(current))
        {
            std::forward<Visit>(visit)(current);
            const Duration remaining =
                checked_subtract(range.upper_value(), current);
            if (step > remaining ||
                (step == remaining &&
                 range.upper_boundary() == Boundary::Open))
            {
                break;
            }
            current = checked_add(current, step);
        }
    }

    template <typename Visit>
    void iterate(CivilDateRange range, std::int64_t day_step, Visit &&visit)
    {
        if (!range.bounded()) { throw std::invalid_argument("cannot iterate an unbounded range"); }
        if (day_step <= 0) { throw std::invalid_argument("iteration step must be positive"); }
        CivilDate current = *range.start();
        if (range.lower_boundary() == Boundary::Open)
        {
            current = checked_add_days(current, day_step);
        }
        while (range.contains(current))
        {
            std::forward<Visit>(visit)(current);
            const auto remaining =
                (std::chrono::sys_days{range.upper_value()} -
                 std::chrono::sys_days{current})
                    .count();
            if (day_step > remaining ||
                (day_step == remaining &&
                 range.upper_boundary() == Boundary::Open))
            {
                break;
            }
            current = checked_add_days(current, day_step);
        }
    }

    template <typename Visit>
    void partition(InstantRange range, Duration width, Visit &&visit)
    {
        if (!range.bounded()) { throw std::invalid_argument("cannot partition an unbounded range"); }
        if (width <= Duration::zero()) { throw std::invalid_argument("partition width must be positive"); }

        Instant start = *range.start();
        if (range.lower_boundary() == Boundary::Open)
        {
            start = checked_add(start, Duration{1});
        }
        const Instant finish = *range.end();
        while (start < finish ||
               (start == finish && range.upper_boundary() == Boundary::Closed))
        {
            const Duration remaining = checked_subtract(finish, start);
            if (width < remaining)
            {
                const Instant next = checked_add(start, width);
                std::forward<Visit>(visit)(
                    InstantRange::bounded(start, next, Boundary::Closed, Boundary::Open));
                start = next;
                continue;
            }
            std::forward<Visit>(visit)(
                InstantRange::bounded(start, finish, Boundary::Closed,
                                      range.upper_boundary()));
            break;
        }
    }

    [[nodiscard]] HGRAPH_EXPORT std::string format_duration(Duration value);
    [[nodiscard]] HGRAPH_EXPORT Duration parse_duration(std::string_view value);
    [[nodiscard]] HGRAPH_EXPORT std::string format_instant(Instant value);
    [[nodiscard]] HGRAPH_EXPORT std::string format_civil_date(CivilDate value);
    [[nodiscard]] HGRAPH_EXPORT std::string format_civil_time(CivilTime value);
    [[nodiscard]] HGRAPH_EXPORT std::string format_civil_datetime(CivilDateTime value);

    HGRAPH_EXPORT std::ostream &operator<<(std::ostream &out, const CivilDateTime &value);
    HGRAPH_EXPORT std::ostream &operator<<(std::ostream &out, const Period &value);
    HGRAPH_EXPORT std::ostream &operator<<(std::ostream &out, const ZoneId &value);
    HGRAPH_EXPORT std::ostream &operator<<(std::ostream &out, const ZonedDateTime &value);
    HGRAPH_EXPORT std::ostream &operator<<(std::ostream &out, const InstantRange &value);
    HGRAPH_EXPORT std::ostream &operator<<(std::ostream &out, const CivilDateRange &value);
}  // namespace hgraph

namespace std
{
    template <>
    struct hash<hgraph::CivilDateTime>
    {
        size_t operator()(const hgraph::CivilDateTime &value) const noexcept
        {
            return hash<std::int64_t>{}(value.epoch_microseconds());
        }
    };

    template <>
    struct hash<hgraph::Period>
    {
        size_t operator()(const hgraph::Period &value) const noexcept
        {
            size_t seed = hash<std::int32_t>{}(value.total_months());
            return seed ^ (hash<std::int32_t>{}(value.days()) + 0x9e3779b9U +
                           (seed << 6U) + (seed >> 2U));
        }
    };

    template <>
    struct hash<hgraph::ZoneId>
    {
        size_t operator()(const hgraph::ZoneId &value) const noexcept
        {
            const auto payload = value.payload();
            const std::uint64_t packed =
                static_cast<std::uint64_t>(payload.slot) |
                (static_cast<std::uint64_t>(payload.generation) << 32U) |
                (static_cast<std::uint64_t>(payload.name_tag) << 48U);
            return hash<std::uint64_t>{}(packed);
        }
    };

    template <>
    struct hash<hgraph::ZonedDateTime>
    {
        size_t operator()(const hgraph::ZonedDateTime &value) const noexcept
        {
            size_t seed = hash<hgraph::Instant>{}(value.instant());
            seed ^= hash<hgraph::ZoneId>{}(value.zone()) + 0x9e3779b9U +
                    (seed << 6U) + (seed >> 2U);
            return seed ^ (hash<std::int32_t>{}(value.offset_seconds()) +
                           0x9e3779b9U + (seed << 6U) + (seed >> 2U));
        }
    };

    template <std::totally_ordered T>
    struct hash<hgraph::TimeRange<T>>
    {
        size_t operator()(const hgraph::TimeRange<T> &value) const noexcept
        {
            if (value.empty()) { return 0x54b3a91dU; }
            size_t seed = 0x9e3779b9U;
            if (value.lower_bounded())
            {
                seed ^= hash<T>{}(value.lower_value()) + (seed << 6U) +
                        (seed >> 2U);
                seed ^= hash<unsigned>{}(
                    value.lower_boundary() == hgraph::Boundary::Closed ? 1U : 0U);
            }
            else { seed ^= 0x1172U; }
            if (value.upper_bounded())
            {
                seed ^= hash<T>{}(value.upper_value()) + (seed << 6U) +
                        (seed >> 2U);
                seed ^= hash<unsigned>{}(
                    value.upper_boundary() == hgraph::Boundary::Closed ? 1U : 0U);
            }
            else { seed ^= 0x2274U; }
            return seed;
        }
    };

    template <std::totally_ordered T, std::size_t Capacity>
    struct hash<hgraph::FixedRangeSet<T, Capacity>>
    {
        size_t operator()(const hgraph::FixedRangeSet<T, Capacity> &value) const noexcept
        {
            size_t seed = hash<size_t>{}(value.size());
            for (const auto &range : value)
            {
                seed ^= hash<hgraph::TimeRange<T>>{}(range) + 0x9e3779b9U +
                        (seed << 6U) + (seed >> 2U);
            }
            return seed;
        }
    };
}  // namespace std

static_assert(sizeof(hgraph::CivilDateTime) == 8);
static_assert(sizeof(hgraph::Period) == 8);
static_assert(sizeof(hgraph::ZoneIdPayload) == 8);
static_assert(sizeof(hgraph::ZoneId) == 8);
static_assert(sizeof(hgraph::ZonedDateTime) <= 24);
static_assert(sizeof(hgraph::InstantRange) <= 24);
static_assert(sizeof(hgraph::CivilDateRange) <= 16);
static_assert(sizeof(hgraph::InstantRangeSet) <= 56);
static_assert(sizeof(hgraph::CivilDateRangeSet) <= 40);

#include <hgraph/types/static_schema.h>

namespace hgraph::static_schema_detail
{
    template <> struct scalar_name<hgraph::Period>
    { static constexpr std::string_view value{"period"}; };
    template <> struct scalar_name<hgraph::CivilDateTime>
    { static constexpr std::string_view value{"civil_datetime"}; };
    template <> struct scalar_name<hgraph::ZoneId>
    { static constexpr std::string_view value{"zone_id"}; };
    template <> struct scalar_name<hgraph::ZonedDateTime>
    { static constexpr std::string_view value{"zoned_datetime"}; };
    template <> struct scalar_name<hgraph::InstantRange>
    { static constexpr std::string_view value{"instant_range"}; };
    template <> struct scalar_name<hgraph::CivilDateRange>
    { static constexpr std::string_view value{"civil_date_range"}; };
    template <> struct scalar_name<hgraph::InstantRangeSet>
    { static constexpr std::string_view value{"instant_range_set"}; };
    template <> struct scalar_name<hgraph::CivilDateRangeSet>
    { static constexpr std::string_view value{"civil_date_range_set"}; };
    template <> struct scalar_name<hgraph::MonthEndPolicy>
    { static constexpr std::string_view value{"month_end_policy"}; };
    template <> struct scalar_name<hgraph::AmbiguousTimePolicy>
    { static constexpr std::string_view value{"ambiguous_time_policy"}; };
    template <> struct scalar_name<hgraph::NonexistentTimePolicy>
    { static constexpr std::string_view value{"nonexistent_time_policy"}; };
    template <> struct scalar_name<hgraph::Boundary>
    { static constexpr std::string_view value{"boundary"}; };
}  // namespace hgraph::static_schema_detail

namespace hgraph
{
#define HGRAPH_DECLARE_TEMPORAL_SCALAR_BINDING(Type)                                                \
    extern template HGRAPH_EXPORT const MemoryUtils::StoragePlan &MemoryUtils::plan_for<Type>() noexcept; \
    extern template HGRAPH_EXPORT const ValueOps &ops_for<Type>() noexcept

    HGRAPH_DECLARE_TEMPORAL_SCALAR_BINDING(Period);
    HGRAPH_DECLARE_TEMPORAL_SCALAR_BINDING(CivilDateTime);
    HGRAPH_DECLARE_TEMPORAL_SCALAR_BINDING(ZoneId);
    HGRAPH_DECLARE_TEMPORAL_SCALAR_BINDING(ZonedDateTime);
    HGRAPH_DECLARE_TEMPORAL_SCALAR_BINDING(InstantRange);
    HGRAPH_DECLARE_TEMPORAL_SCALAR_BINDING(CivilDateRange);
    HGRAPH_DECLARE_TEMPORAL_SCALAR_BINDING(InstantRangeSet);
    HGRAPH_DECLARE_TEMPORAL_SCALAR_BINDING(CivilDateRangeSet);
    HGRAPH_DECLARE_TEMPORAL_SCALAR_BINDING(MonthEndPolicy);
    HGRAPH_DECLARE_TEMPORAL_SCALAR_BINDING(AmbiguousTimePolicy);
    HGRAPH_DECLARE_TEMPORAL_SCALAR_BINDING(NonexistentTimePolicy);
    HGRAPH_DECLARE_TEMPORAL_SCALAR_BINDING(Boundary);

#undef HGRAPH_DECLARE_TEMPORAL_SCALAR_BINDING
}  // namespace hgraph

#if HGRAPH_ENABLE_PYTHON_USER_NODES
#include <hgraph/types/value/value_ops.h>

namespace hgraph
{
    template <typename T>
    struct temporal_native_python_conversion
    {
        static nb::object to_python(const T &value) { return nb::cast(value); }
        static T from_python(nb::handle source) { return nb::cast<T>(source); }
    };

    template <> struct python_conversion_traits<Period>
        : temporal_native_python_conversion<Period> {};
    template <> struct python_conversion_traits<CivilDateTime>
        : temporal_native_python_conversion<CivilDateTime> {};
    template <> struct python_conversion_traits<ZoneId>
        : temporal_native_python_conversion<ZoneId> {};
    template <> struct python_conversion_traits<ZonedDateTime>
        : temporal_native_python_conversion<ZonedDateTime> {};
    template <> struct python_conversion_traits<InstantRange>
        : temporal_native_python_conversion<InstantRange> {};
    template <> struct python_conversion_traits<CivilDateRange>
        : temporal_native_python_conversion<CivilDateRange> {};
    template <> struct python_conversion_traits<InstantRangeSet>
        : temporal_native_python_conversion<InstantRangeSet> {};
    template <> struct python_conversion_traits<CivilDateRangeSet>
        : temporal_native_python_conversion<CivilDateRangeSet> {};
    template <> struct python_conversion_traits<MonthEndPolicy>
        : temporal_native_python_conversion<MonthEndPolicy> {};
    template <> struct python_conversion_traits<AmbiguousTimePolicy>
        : temporal_native_python_conversion<AmbiguousTimePolicy> {};
    template <> struct python_conversion_traits<NonexistentTimePolicy>
        : temporal_native_python_conversion<NonexistentTimePolicy> {};
    template <> struct python_conversion_traits<Boundary>
        : temporal_native_python_conversion<Boundary> {};
}  // namespace hgraph
#endif

#endif  // HGRAPH_TYPES_TEMPORAL_H
