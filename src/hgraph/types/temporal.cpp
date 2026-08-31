#include <hgraph/types/temporal.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <charconv>
#include <cmath>
#include <cstdio>
#include <deque>
#include <mutex>
#include <ostream>
#include <shared_mutex>
#include <sstream>
#include <unordered_map>

namespace hgraph
{
    namespace
    {
        constexpr std::int64_t microseconds_per_second = 1'000'000;
        constexpr std::int64_t seconds_per_day = 86'400;
        constexpr std::int64_t microseconds_per_day =
            seconds_per_day * microseconds_per_second;

        [[nodiscard]] std::int64_t add_int64(std::int64_t lhs, std::int64_t rhs)
        {
            if ((rhs > 0 && lhs > std::numeric_limits<std::int64_t>::max() - rhs) ||
                (rhs < 0 && lhs < std::numeric_limits<std::int64_t>::min() - rhs))
            {
                throw std::overflow_error("temporal addition overflow");
            }
            return lhs + rhs;
        }

        [[nodiscard]] std::int64_t subtract_int64(std::int64_t lhs, std::int64_t rhs)
        {
            if ((rhs > 0 && lhs < std::numeric_limits<std::int64_t>::min() + rhs) ||
                (rhs < 0 && lhs > std::numeric_limits<std::int64_t>::max() + rhs))
            {
                throw std::overflow_error("temporal subtraction overflow");
            }
            return lhs - rhs;
        }

        [[nodiscard]] std::int64_t negate_int64(std::int64_t value)
        {
            if (value == std::numeric_limits<std::int64_t>::min())
            {
                throw std::overflow_error("temporal negation overflow");
            }
            return -value;
        }

        [[nodiscard]] std::int64_t multiply_int64(std::int64_t lhs, std::int64_t rhs)
        {
            if (lhs == 0 || rhs == 0) { return 0; }
            constexpr auto minimum = std::numeric_limits<std::int64_t>::min();
            constexpr auto maximum = std::numeric_limits<std::int64_t>::max();
            if ((lhs > 0 &&
                 ((rhs > 0 && lhs > maximum / rhs) ||
                  (rhs < 0 && rhs < minimum / lhs))) ||
                (lhs < 0 &&
                 ((rhs > 0 && lhs < minimum / rhs) ||
                  (rhs < 0 && lhs < maximum / rhs))))
            {
                throw std::overflow_error("temporal multiplication overflow");
            }
            return lhs * rhs;
        }

        [[nodiscard]] std::int32_t narrow_int32(std::int64_t value,
                                                std::string_view operation)
        {
            if (value < std::numeric_limits<std::int32_t>::min() ||
                value > std::numeric_limits<std::int32_t>::max())
            {
                throw std::overflow_error(std::string{operation} + " overflow");
            }
            return static_cast<std::int32_t>(value);
        }

        [[nodiscard]] std::int64_t floor_div(std::int64_t numerator,
                                             std::int64_t denominator)
        {
            const std::int64_t quotient = numerator / denominator;
            const std::int64_t remainder = numerator % denominator;
            return quotient - (remainder < 0 ? 1 : 0);
        }

        [[nodiscard]] bool valid_date(CivilDate value) noexcept
        {
            const int year = static_cast<int>(value.year());
            return value.ok() && year >= 1 && year <= 9999;
        }

        void require_valid_date(CivilDate value)
        {
            if (!valid_date(value))
            {
                throw std::invalid_argument(
                    "civil date must be within 0001-01-01 through 9999-12-31");
            }
        }

        void require_valid_time(CivilTime value)
        {
            if (value.microseconds < 0 ||
                value.microseconds >= microseconds_per_day)
            {
                throw std::invalid_argument(
                    "civil time must be within 00:00:00 through 23:59:59.999999");
            }
        }

        [[nodiscard]] std::int64_t first_civil_microsecond() noexcept
        {
            using namespace std::chrono;
            return duration_cast<microseconds>(
                       sys_days{year{1} / January / day{1}}.time_since_epoch())
                .count();
        }

        [[nodiscard]] std::int64_t after_last_civil_microsecond() noexcept
        {
            using namespace std::chrono;
            return duration_cast<microseconds>(
                       sys_days{year{10000} / January / day{1}}.time_since_epoch())
                .count();
        }

        void require_valid_civil_epoch(std::int64_t value)
        {
            if (value < first_civil_microsecond() ||
                value >= after_last_civil_microsecond())
            {
                throw std::overflow_error(
                    "civil datetime is outside 0001-01-01 through 9999-12-31");
            }
        }

        [[nodiscard]] std::uint16_t zone_name_tag(std::string_view name) noexcept
        {
            std::uint64_t hash = 14695981039346656037ULL;
            for (const unsigned char byte : name)
            {
                hash ^= byte;
                hash *= 1099511628211ULL;
            }
            std::uint16_t tag = static_cast<std::uint16_t>(hash);
            return tag == 0 ? 1 : tag;
        }

        struct ZoneRecord
        {
            std::string name;
            std::uint32_t slot{0};
            std::uint16_t generation{1};
            std::uint16_t tag{0};
            bool active{true};
            bool retired{false};
        };

        struct TransparentStringHash
        {
            using is_transparent = void;

            [[nodiscard]] std::size_t operator()(std::string_view value) const noexcept
            {
                return std::hash<std::string_view>{}(value);
            }

            [[nodiscard]] std::size_t operator()(const std::string &value) const noexcept
            {
                return (*this)(std::string_view{value});
            }
        };

        class ZoneNameRegistry
        {
          public:
            ZoneNameRegistry()
            {
                for (auto &entry : front_)
                {
                    entry.store(nullptr, std::memory_order_relaxed);
                }
            }

            [[nodiscard]] ZoneIdPayload intern(std::string_view name)
            {
                if (!ZoneId::valid_syntax(name))
                {
                    throw std::invalid_argument("invalid time-zone identifier");
                }

                {
                    std::shared_lock lock{mutex_};
                    if (const auto found = by_name_.find(name); found != by_name_.end())
                    {
                        return publish(found->second);
                    }
                }

                std::unique_lock lock{mutex_};
                if (const auto found = by_name_.find(name); found != by_name_.end())
                {
                    return publish(found->second);
                }
                for (std::size_t index = 0; index < records_.size(); ++index)
                {
                    ZoneRecord &record = records_[index];
                    if (!record.active && !record.retired &&
                        record.name == name)
                    {
                        record.active = true;
                        const std::uint32_t slot =
                            static_cast<std::uint32_t>(index + 1);
                        by_name_.emplace(record.name, slot);
                        return publish(slot);
                    }
                }
                if (records_.size() >= std::numeric_limits<std::uint32_t>::max())
                {
                    throw std::overflow_error("time-zone name registry is full");
                }

                const std::uint32_t slot =
                    static_cast<std::uint32_t>(records_.size() + 1);
                records_.push_back(
                    ZoneRecord{
                        std::string{name}, slot, 1, zone_name_tag(name),
                        true, false});
                by_name_.emplace(records_.back().name, slot);
                return publish(slot);
            }

            [[nodiscard]] bool valid(ZoneIdPayload candidate) const noexcept
            {
                if (candidate.slot == 0 || candidate.generation == 0 ||
                    candidate.name_tag == 0)
                {
                    return false;
                }
                return find(candidate) != nullptr;
            }

            [[nodiscard]] std::string_view name(ZoneIdPayload candidate) const
            {
                const ZoneRecord *record = find(candidate);
                if (record == nullptr)
                {
                    throw std::invalid_argument("stale or corrupt time-zone handle");
                }
                return record->name;
            }

            void reset() noexcept
            {
                std::unique_lock lock{mutex_};
                for (auto &entry : front_)
                {
                    entry.store(nullptr, std::memory_order_relaxed);
                }
                by_name_.clear();
                for (ZoneRecord &record : records_)
                {
                    if (!record.active) { continue; }
                    record.active = false;
                    if (record.generation ==
                        std::numeric_limits<std::uint16_t>::max())
                    {
                        record.retired = true;
                    }
                    else
                    {
                        ++record.generation;
                    }
                }
            }

          private:
            static constexpr std::size_t front_size = 4096;

            [[nodiscard]] static constexpr std::size_t cache_index(
                ZoneIdPayload candidate) noexcept
            {
                return static_cast<std::size_t>(candidate.slot) &
                       (front_size - 1U);
            }

            [[nodiscard]] static bool matches(
                const ZoneRecord &record,
                ZoneIdPayload candidate) noexcept
            {
                return record.slot == candidate.slot &&
                       record.active && !record.retired &&
                       record.generation == candidate.generation &&
                       record.tag == candidate.name_tag;
            }

            [[nodiscard]] const ZoneRecord *find(
                ZoneIdPayload candidate) const noexcept
            {
                if (candidate.slot == 0 || candidate.generation == 0 ||
                    candidate.name_tag == 0)
                {
                    return nullptr;
                }
                const std::size_t index = cache_index(candidate);
                if (const ZoneRecord *cached =
                        front_[index].load(std::memory_order_acquire);
                    cached != nullptr && matches(*cached, candidate))
                {
                    return cached;
                }

                std::shared_lock lock{mutex_};
                if (candidate.slot > records_.size()) { return nullptr; }
                const ZoneRecord *record =
                    &records_[candidate.slot - 1];
                if (!matches(*record, candidate)) { return nullptr; }
                front_[index].store(
                    record, std::memory_order_release);
                return record;
            }

            [[nodiscard]] ZoneIdPayload publish(
                std::uint32_t slot) const noexcept
            {
                const ZoneIdPayload result = payload(slot);
                front_[cache_index(result)].store(
                    &records_[slot - 1], std::memory_order_release);
                return result;
            }

            [[nodiscard]] ZoneIdPayload payload(std::uint32_t slot) const noexcept
            {
                const ZoneRecord &record = records_[slot - 1];
                return ZoneIdPayload{slot, record.generation, record.tag};
            }

            static_assert(
                std::atomic<const ZoneRecord *>::is_always_lock_free,
                "supported 64-bit targets require lock-free pointer atomics");

            mutable std::shared_mutex mutex_;
            // deque preserves the backing storage of names returned as
            // string_view while other threads intern additional zones.
            std::deque<ZoneRecord> records_;
            std::unordered_map<std::string, std::uint32_t,
                               TransparentStringHash, std::equal_to<>>
                by_name_;
            mutable std::array<
                std::atomic<const ZoneRecord *>, front_size>
                front_{};
        };

        [[nodiscard]] ZoneNameRegistry &zone_registry()
        {
            static ZoneNameRegistry registry;
            return registry;
        }

        [[nodiscard]] std::int64_t round_to_int64(long double value,
                                                  std::string_view operation)
        {
            if (!std::isfinite(value))
            {
                throw std::overflow_error(std::string{operation} + " overflow");
            }
            if (value < static_cast<long double>(
                            std::numeric_limits<std::int64_t>::min()) -
                            0.5L ||
                value > static_cast<long double>(
                            std::numeric_limits<std::int64_t>::max()) +
                            0.5L)
            {
                throw std::overflow_error(std::string{operation} + " overflow");
            }

            const long double base = std::floor(value);
            const long double fraction = value - base;
            long double rounded = base;
            if (fraction > 0.5L ||
                (fraction == 0.5L &&
                 std::fmod(std::fabs(base), 2.0L) == 1.0L))
            {
                rounded += 1.0L;
            }
            if (rounded < static_cast<long double>(
                              std::numeric_limits<std::int64_t>::min()) ||
                rounded > static_cast<long double>(
                              std::numeric_limits<std::int64_t>::max()))
            {
                throw std::overflow_error(std::string{operation} + " overflow");
            }
            return static_cast<std::int64_t>(rounded);
        }

        [[nodiscard]] std::int64_t rounded_product(std::int64_t value,
                                                   double factor)
        {
            if (!std::isfinite(factor))
            {
                throw std::invalid_argument("temporal factor must be finite");
            }
            const long double product =
                static_cast<long double>(value) * static_cast<long double>(factor);
            return round_to_int64(product, "temporal multiplication");
        }

        [[nodiscard]] std::int64_t rounded_ratio(std::int64_t value,
                                                 double divisor)
        {
            if (divisor == 0.0)
            {
                throw std::invalid_argument("division by zero");
            }
            if (std::isnan(divisor))
            {
                throw std::invalid_argument("temporal divisor must not be NaN");
            }
            if (std::isinf(divisor)) { return 0; }
            const long double quotient =
                static_cast<long double>(value) /
                static_cast<long double>(divisor);
            return round_to_int64(quotient, "temporal division");
        }

        [[nodiscard]] std::int64_t rounded_integer_ratio(
            std::int64_t value, std::int64_t divisor)
        {
            if (divisor == 0) { throw std::invalid_argument("division by zero"); }
            if (value == std::numeric_limits<std::int64_t>::min() &&
                divisor == -1)
            {
                throw std::overflow_error("temporal division overflow");
            }
            std::int64_t quotient = value / divisor;
            const std::int64_t remainder = value % divisor;
            if (remainder == 0) { return quotient; }

            const auto magnitude = [](std::int64_t number) {
                const std::uint64_t bits = static_cast<std::uint64_t>(number);
                return number < 0 ? std::uint64_t{0} - bits : bits;
            };
            const std::uint64_t remainder_magnitude = magnitude(remainder);
            const std::uint64_t divisor_magnitude = magnitude(divisor);
            const bool above_half =
                remainder_magnitude > divisor_magnitude - remainder_magnitude;
            const bool tie =
                remainder_magnitude == divisor_magnitude - remainder_magnitude;
            if (above_half || (tie && (quotient & 1) != 0))
            {
                quotient = add_int64(
                    quotient, (value < 0) == (divisor < 0) ? 1 : -1);
            }
            return quotient;
        }

        [[nodiscard]] std::int64_t quantized_index(std::int64_t value,
                                                   std::int64_t quantum,
                                                   bool ceiling)
        {
            const std::int64_t lower = floor_div(value, quantum);
            if (!ceiling || value % quantum == 0) { return lower; }
            return add_int64(lower, 1);
        }

        [[nodiscard]] std::int64_t quantize_count(std::int64_t value,
                                                  std::int64_t quantum,
                                                  int mode)
        {
            if (quantum <= 0)
            {
                throw std::invalid_argument("temporal quantum must be positive");
            }
            std::int64_t index = 0;
            if (mode == -1) { index = quantized_index(value, quantum, false); }
            else if (mode == 1) { index = quantized_index(value, quantum, true); }
            else
            {
                const std::int64_t lower = floor_div(value, quantum);
                std::int64_t remainder = value % quantum;
                if (remainder < 0) { remainder += quantum; }
                const std::int64_t complement = quantum - remainder;
                index = remainder < complement
                            ? lower
                            : remainder > complement
                                  ? add_int64(lower, 1)
                                  : ((lower & 1) == 0 ? lower
                                                     : add_int64(lower, 1));
            }
            return multiply_int64(index, quantum);
        }

        [[nodiscard]] std::int64_t quantize_instant_count(
            std::int64_t value, std::int64_t quantum,
            std::int64_t origin, int mode)
        {
            if (quantum <= 0)
            {
                throw std::invalid_argument(
                    "temporal quantum must be positive");
            }
            if (value == origin) { return value; }

            const bool below_origin = value < origin;
            const std::uint64_t magnitude =
                below_origin
                    ? static_cast<std::uint64_t>(origin) -
                          static_cast<std::uint64_t>(value)
                    : static_cast<std::uint64_t>(value) -
                          static_cast<std::uint64_t>(origin);
            const auto unsigned_quantum =
                static_cast<std::uint64_t>(quantum);
            const std::uint64_t quotient =
                magnitude / unsigned_quantum;
            const std::uint64_t remainder =
                magnitude % unsigned_quantum;
            if (remainder == 0) { return value; }

            const auto move_down = [&](std::uint64_t amount) {
                return subtract_int64(
                    value, static_cast<std::int64_t>(amount));
            };
            const auto move_up = [&](std::uint64_t amount) {
                return add_int64(
                    value, static_cast<std::int64_t>(amount));
            };
            const std::uint64_t floor_distance =
                below_origin ? unsigned_quantum - remainder
                             : remainder;
            const std::uint64_t ceil_distance =
                below_origin ? remainder
                             : unsigned_quantum - remainder;
            if (mode < 0) { return move_down(floor_distance); }
            if (mode > 0) { return move_up(ceil_distance); }
            if (floor_distance < ceil_distance)
            {
                return move_down(floor_distance);
            }
            if (ceil_distance < floor_distance)
            {
                return move_up(ceil_distance);
            }

            const std::uint64_t floor_index_magnitude =
                below_origin ? quotient + 1 : quotient;
            return (floor_index_magnitude & 1U) == 0
                       ? move_down(floor_distance)
                       : move_up(ceil_distance);
        }

        [[nodiscard]] CivilDate last_day_of_month(std::chrono::year year,
                                                  std::chrono::month month)
        {
            using namespace std::chrono;
            return year_month_day{year / month / last};
        }

        template <typename T>
        void write_range(std::ostream &out, const TimeRange<T> &value)
        {
            if (value.empty())
            {
                out << "empty";
                return;
            }
            out << (value.lower_boundary() == Boundary::Closed ? '[' : '(');
            if (value.lower_bounded()) { out << value.lower_value(); }
            out << ", ";
            if (value.upper_bounded()) { out << value.upper_value(); }
            out << (value.upper_boundary() == Boundary::Closed ? ']' : ')');
        }
    }  // namespace

    Instant checked_add(Instant value, Duration delta)
    {
        return Instant{Duration{add_int64(value.time_since_epoch().count(),
                                          delta.count())}};
    }

    Instant checked_subtract(Instant value, Duration delta)
    {
        return Instant{Duration{subtract_int64(value.time_since_epoch().count(),
                                               delta.count())}};
    }

    Duration checked_subtract(Instant lhs, Instant rhs)
    {
        return Duration{subtract_int64(lhs.time_since_epoch().count(),
                                       rhs.time_since_epoch().count())};
    }

    Duration checked_add(Duration lhs, Duration rhs)
    {
        return Duration{add_int64(lhs.count(), rhs.count())};
    }

    Duration checked_subtract(Duration lhs, Duration rhs)
    {
        return Duration{subtract_int64(lhs.count(), rhs.count())};
    }

    Duration checked_negate(Duration value)
    {
        return Duration{negate_int64(value.count())};
    }

    Duration checked_multiply(Duration value, std::int64_t factor)
    {
        return Duration{multiply_int64(value.count(), factor)};
    }

    Duration checked_multiply(Duration value, double factor)
    {
        return Duration{rounded_product(value.count(), factor)};
    }

    Duration checked_divide(Duration value, std::int64_t divisor)
    {
        return Duration{rounded_integer_ratio(value.count(), divisor)};
    }

    Duration checked_divide(Duration value, double divisor)
    {
        return Duration{rounded_ratio(value.count(), divisor)};
    }

    CivilDateTime::CivilDateTime(CivilDate date, CivilTime time)
    {
        require_valid_date(date);
        require_valid_time(time);
        using namespace std::chrono;
        const auto day_part =
            duration_cast<microseconds>(sys_days{date}.time_since_epoch()).count();
        microseconds_ = add_int64(day_part, time.microseconds);
    }

    CivilDateTime::CivilDateTime(CivilDate date, int hour, int minute,
                                 int second, int microsecond)
        : CivilDateTime(
              date,
              CivilTime{((static_cast<std::int64_t>(hour) * 60 + minute) * 60 +
                         second) *
                            microseconds_per_second +
                        microsecond})
    {
        if (hour < 0 || hour > 23 || minute < 0 || minute > 59 ||
            second < 0 || second > 59 || microsecond < 0 ||
            microsecond >= microseconds_per_second)
        {
            throw std::invalid_argument("invalid civil time fields");
        }
    }

    CivilDateTime CivilDateTime::from_epoch_microseconds(std::int64_t value)
    {
        require_valid_civil_epoch(value);
        return CivilDateTime{value, 0};
    }

    CivilDate CivilDateTime::date() const noexcept
    {
        using namespace std::chrono;
        return year_month_day{sys_days{days{floor_div(microseconds_,
                                                      microseconds_per_day)}}};
    }

    CivilTime CivilDateTime::time() const noexcept
    {
        const std::int64_t day_index =
            floor_div(microseconds_, microseconds_per_day);
        return CivilTime{microseconds_ - day_index * microseconds_per_day};
    }

    int CivilDateTime::year() const noexcept
    {
        return static_cast<int>(date().year());
    }

    unsigned CivilDateTime::month() const noexcept
    {
        return static_cast<unsigned>(date().month());
    }

    unsigned CivilDateTime::day() const noexcept
    {
        return static_cast<unsigned>(date().day());
    }

    int CivilDateTime::hour() const noexcept
    {
        return static_cast<int>(time().microseconds /
                                (3'600 * microseconds_per_second));
    }

    int CivilDateTime::minute() const noexcept
    {
        return static_cast<int>((time().microseconds /
                                 (60 * microseconds_per_second)) %
                                60);
    }

    int CivilDateTime::second() const noexcept
    {
        return static_cast<int>((time().microseconds /
                                 microseconds_per_second) %
                                60);
    }

    int CivilDateTime::microsecond() const noexcept
    {
        return static_cast<int>(time().microseconds % microseconds_per_second);
    }

    int CivilDateTime::weekday() const noexcept
    {
        return static_cast<int>(
                   std::chrono::weekday{std::chrono::sys_days{date()}}
                       .iso_encoding()) -
               1;
    }

    int CivilDateTime::isoweekday() const noexcept
    {
        return static_cast<int>(
            std::chrono::weekday{std::chrono::sys_days{date()}}.iso_encoding());
    }

    int CivilDateTime::day_of_year() const noexcept
    {
        using namespace std::chrono;
        const CivilDate current = date();
        return static_cast<int>(
                   (sys_days{current} -
                    sys_days{current.year() / January / std::chrono::day{1}})
                       .count()) +
               1;
    }

    CivilDateTime checked_add(CivilDateTime value, Duration delta)
    {
        return CivilDateTime::from_epoch_microseconds(
            add_int64(value.epoch_microseconds(), delta.count()));
    }

    CivilDateTime checked_subtract(CivilDateTime value, Duration delta)
    {
        return CivilDateTime::from_epoch_microseconds(
            subtract_int64(value.epoch_microseconds(), delta.count()));
    }

    Duration checked_subtract(CivilDateTime lhs, CivilDateTime rhs)
    {
        return Duration{subtract_int64(lhs.epoch_microseconds(),
                                       rhs.epoch_microseconds())};
    }

    CivilDate checked_add_days(CivilDate value, std::int64_t days)
    {
        require_valid_date(value);
        using namespace std::chrono;
        const auto epoch_day = sys_days{value}.time_since_epoch().count();
        const CivilDate result{
            sys_days{std::chrono::days{add_int64(epoch_day, days)}}};
        require_valid_date(result);
        return result;
    }

    Duration checked_subtract(CivilDate lhs, CivilDate rhs)
    {
        require_valid_date(lhs);
        require_valid_date(rhs);
        const auto days =
            (std::chrono::sys_days{lhs} - std::chrono::sys_days{rhs}).count();
        return Duration{multiply_int64(days, microseconds_per_day)};
    }

    Period::Period(std::int64_t years, std::int64_t months, std::int64_t days)
        : total_months_(
              narrow_int32(add_int64(multiply_int64(years, 12), months),
                           "period month")),
          days_(narrow_int32(days, "period day"))
    {
    }

    Period checked_add(Period lhs, Period rhs)
    {
        return Period::from_total(
            narrow_int32(add_int64(lhs.total_months(), rhs.total_months()),
                         "period month"),
            narrow_int32(add_int64(lhs.days(), rhs.days()), "period day"));
    }

    Period checked_subtract(Period lhs, Period rhs)
    {
        return Period::from_total(
            narrow_int32(subtract_int64(lhs.total_months(), rhs.total_months()),
                         "period month"),
            narrow_int32(subtract_int64(lhs.days(), rhs.days()), "period day"));
    }

    Period checked_negate(Period value)
    {
        return Period::from_total(
            narrow_int32(negate_int64(value.total_months()), "period month"),
            narrow_int32(negate_int64(value.days()), "period day"));
    }

    Period checked_multiply(Period value, std::int64_t factor)
    {
        return Period::from_total(
            narrow_int32(multiply_int64(value.total_months(), factor),
                         "period month"),
            narrow_int32(multiply_int64(value.days(), factor), "period day"));
    }

    CivilDate apply_period(CivilDate value, Period period,
                           MonthEndPolicy policy)
    {
        require_valid_date(value);
        using namespace std::chrono;
        const int source_year = static_cast<int>(value.year());
        const unsigned source_month = static_cast<unsigned>(value.month());
        const unsigned source_day = static_cast<unsigned>(value.day());
        const std::int64_t source_month_index =
            static_cast<std::int64_t>(source_year) * 12 +
            static_cast<std::int64_t>(source_month) - 1;
        const std::int64_t target_month_index =
            add_int64(source_month_index, period.total_months());
        const std::int64_t target_year_number = floor_div(target_month_index, 12);
        if (target_year_number < 1 || target_year_number > 9999)
        {
            throw std::overflow_error("period application exceeds civil date domain");
        }
        const unsigned target_month_number =
            static_cast<unsigned>(target_month_index -
                                  target_year_number * 12 + 1);
        const year target_year{static_cast<int>(target_year_number)};
        const month target_month{target_month_number};
        const CivilDate target_end = last_day_of_month(target_year, target_month);
        const unsigned target_last_day =
            static_cast<unsigned>(target_end.day());
        const CivilDate source_end =
            last_day_of_month(value.year(), value.month());
        const bool source_is_end = source_day ==
                                   static_cast<unsigned>(source_end.day());

        unsigned target_day = source_day;
        if (policy == MonthEndPolicy::PreserveEndOfMonth && source_is_end)
        {
            target_day = target_last_day;
        }
        else if (target_day > target_last_day)
        {
            if (policy == MonthEndPolicy::Reject)
            {
                throw std::invalid_argument(
                    "period produces an invalid target day");
            }
            target_day = target_last_day;
        }
        return checked_add_days(
            CivilDate{target_year / target_month / day{target_day}},
            period.days());
    }

    CivilDateTime apply_period(CivilDateTime value, Period period,
                               MonthEndPolicy policy)
    {
        return CivilDateTime{apply_period(value.date(), period, policy),
                             value.time()};
    }

    ZoneId::ZoneId(std::string_view name)
        : payload_(zone_registry().intern(name))
    {
    }

    bool ZoneId::valid_syntax(std::string_view name) noexcept
    {
        if (name.empty() || name.size() > 255 || name.front() == '/' ||
            name.back() == '/')
        {
            return false;
        }
        std::size_t component_start = 0;
        for (std::size_t index = 0; index < name.size(); ++index)
        {
            const unsigned char character =
                static_cast<unsigned char>(name[index]);
            const bool accepted =
                (character >= 'a' && character <= 'z') ||
                (character >= 'A' && character <= 'Z') ||
                (character >= '0' && character <= '9') ||
                character == '.' || character == '_' || character == '-' ||
                character == '+' || character == '/';
            if (!accepted) { return false; }
            if (character == '/')
            {
                if (index == component_start) { return false; }
                const std::string_view component =
                    name.substr(component_start, index - component_start);
                if (component == "." || component == "..") { return false; }
                component_start = index + 1;
            }
        }
        const std::string_view component = name.substr(component_start);
        return component != "." && component != "..";
    }

    bool ZoneId::valid() const noexcept
    {
        return zone_registry().valid(payload_);
    }

    std::string_view ZoneId::name() const
    {
        return zone_registry().name(payload_);
    }

    void clear_zone_name_registry() noexcept
    {
        zone_registry().reset();
    }

    ZonedDateTime ZonedDateTime::from_resolved(Instant instant, ZoneId zone,
                                                std::int32_t offset_seconds)
    {
        if (!zone.valid())
        {
            throw std::invalid_argument("zoned datetime requires a valid zone");
        }
        if (offset_seconds <= -seconds_per_day ||
            offset_seconds >= seconds_per_day)
        {
            throw std::invalid_argument(
                "zoned datetime offset must be less than one day");
        }
        ZonedDateTime value;
        value.instant_ = instant;
        value.zone_ = zone;
        value.offset_seconds_ = offset_seconds;
        (void)value.civil();
        return value;
    }

    CivilDateTime ZonedDateTime::civil() const
    {
        return CivilDateTime::from_epoch_microseconds(
            add_int64(instant_.time_since_epoch().count(),
                      multiply_int64(offset_seconds_,
                                     microseconds_per_second)));
    }

    ZonedDateTime at_zone(Instant instant, ZoneId zone,
                          const TimeZoneProvider &provider)
    {
        if (!provider.contains(zone))
        {
            throw std::invalid_argument("unknown time-zone identifier");
        }
        const OffsetInfo info = provider.at(instant, zone);
        return ZonedDateTime::from_resolved(instant, zone, info.offset_seconds);
    }

    ZonedDateTime resolve(CivilDateTime local, ZoneId zone,
                          const TimeZoneProvider &provider,
                          AmbiguousTimePolicy ambiguous,
                          NonexistentTimePolicy nonexistent)
    {
        if (!provider.contains(zone))
        {
            throw std::invalid_argument("unknown time-zone identifier");
        }
        const LocalResolution result = provider.resolve(local, zone);
        ResolvedInstant selected{};
        switch (result.kind)
        {
        case LocalResolutionKind::Unique:
            selected = result.first;
            break;
        case LocalResolutionKind::Ambiguous:
            if (ambiguous == AmbiguousTimePolicy::Reject)
            {
                throw std::invalid_argument("ambiguous civil time");
            }
            selected = ambiguous == AmbiguousTimePolicy::Earliest
                           ? std::min(result.first, result.second,
                                      [](const auto &lhs, const auto &rhs) {
                                          return lhs.instant < rhs.instant;
                                      })
                           : std::max(result.first, result.second,
                                      [](const auto &lhs, const auto &rhs) {
                                          return lhs.instant < rhs.instant;
                                      });
            break;
        case LocalResolutionKind::Nonexistent:
            if (nonexistent == NonexistentTimePolicy::Reject)
            {
                throw std::invalid_argument("nonexistent civil time");
            }
            selected = nonexistent == NonexistentTimePolicy::NextValid
                           ? std::max(result.first, result.second,
                                      [](const auto &lhs, const auto &rhs) {
                                          return lhs.instant < rhs.instant;
                                      })
                           : std::min(result.first, result.second,
                                      [](const auto &lhs, const auto &rhs) {
                                          return lhs.instant < rhs.instant;
                                      });
            break;
        }
        return ZonedDateTime::from_resolved(
            selected.instant, zone, selected.offset_seconds);
    }

    ZonedDateTime convert_zone(ZonedDateTime value, ZoneId zone,
                               const TimeZoneProvider &provider)
    {
        return at_zone(value.instant(), zone, provider);
    }

    ZonedDateTime checked_add(ZonedDateTime value, Duration delta,
                              const TimeZoneProvider &provider)
    {
        return at_zone(checked_add(value.instant(), delta), value.zone(),
                       provider);
    }

    InstantRange shift(InstantRange range, Duration delta)
    {
        if (range.empty()) { return {}; }
        if (range.lower_unbounded() && range.upper_unbounded())
        {
            return InstantRange::all();
        }
        if (range.lower_unbounded())
        {
            return InstantRange::until(
                checked_add(range.upper_value(), delta),
                range.upper_boundary());
        }
        if (range.upper_unbounded())
        {
            return InstantRange::from(
                checked_add(range.lower_value(), delta),
                range.lower_boundary());
        }
        return InstantRange::bounded(
            checked_add(range.lower_value(), delta),
            checked_add(range.upper_value(), delta), range.lower_boundary(),
            range.upper_boundary());
    }

    CivilDateRange shift(CivilDateRange range, Period period,
                         MonthEndPolicy policy)
    {
        if (range.empty()) { return {}; }
        if (range.lower_unbounded() && range.upper_unbounded())
        {
            return CivilDateRange::all();
        }
        if (range.lower_unbounded())
        {
            return CivilDateRange::until(
                apply_period(range.upper_value(), period, policy),
                range.upper_boundary());
        }
        if (range.upper_unbounded())
        {
            return CivilDateRange::from(
                apply_period(range.lower_value(), period, policy),
                range.lower_boundary());
        }
        return CivilDateRange::bounded(
            apply_period(range.lower_value(), period, policy),
            apply_period(range.upper_value(), period, policy),
            range.lower_boundary(), range.upper_boundary());
    }

    std::optional<Duration> extent(InstantRange range)
    {
        if (!range.bounded()) { return std::nullopt; }
        return checked_subtract(range.upper_value(), range.lower_value());
    }

    Duration floor(Duration value, Duration quantum)
    {
        return Duration{quantize_count(value.count(), quantum.count(), -1)};
    }

    Duration ceil(Duration value, Duration quantum)
    {
        return Duration{quantize_count(value.count(), quantum.count(), 1)};
    }

    Duration round(Duration value, Duration quantum)
    {
        return Duration{quantize_count(value.count(), quantum.count(), 0)};
    }

    Instant floor(Instant value, Duration quantum, Instant origin)
    {
        return Instant{Duration{quantize_instant_count(
            value.time_since_epoch().count(), quantum.count(),
            origin.time_since_epoch().count(), -1)}};
    }

    Instant ceil(Instant value, Duration quantum, Instant origin)
    {
        return Instant{Duration{quantize_instant_count(
            value.time_since_epoch().count(), quantum.count(),
            origin.time_since_epoch().count(), 1)}};
    }

    Instant round(Instant value, Duration quantum, Instant origin)
    {
        return Instant{Duration{quantize_instant_count(
            value.time_since_epoch().count(), quantum.count(),
            origin.time_since_epoch().count(), 0)}};
    }

    InstantRange bucket(Instant value, Duration width, Instant origin)
    {
        const Instant start = floor(value, width, origin);
        return InstantRange::bounded(start, checked_add(start, width));
    }

    std::string format_duration(Duration value)
    {
        return std::to_string(value.count()) + "us";
    }

    Duration parse_duration(std::string_view value)
    {
        if (value.size() < 3 || !value.ends_with("us"))
        {
            throw std::invalid_argument("invalid canonical duration");
        }
        const std::string_view number = value.substr(0, value.size() - 2);
        if (number.empty() || number.front() == '+' ||
            (number.size() > 1 && number.front() == '0') ||
            (number.starts_with("-0")))
        {
            throw std::invalid_argument("invalid canonical duration");
        }
        std::int64_t result{};
        const auto [end, error] =
            std::from_chars(number.data(), number.data() + number.size(), result);
        if (error != std::errc{} || end != number.data() + number.size())
        {
            throw std::invalid_argument("invalid canonical duration");
        }
        return Duration{result};
    }

    std::string format_civil_date(CivilDate value)
    {
        require_valid_date(value);
        char buffer[16];
        std::snprintf(buffer, sizeof buffer, "%04d-%02u-%02u",
                      static_cast<int>(value.year()),
                      static_cast<unsigned>(value.month()),
                      static_cast<unsigned>(value.day()));
        return buffer;
    }

    std::string format_civil_time(CivilTime value)
    {
        require_valid_time(value);
        std::ostringstream out;
        out << value;
        return out.str();
    }

    std::string format_civil_datetime(CivilDateTime value)
    {
        return format_civil_date(value.date()) + "T" +
               format_civil_time(value.time());
    }

    std::string format_instant(Instant value)
    {
        const auto count = value.time_since_epoch().count();
        const std::int64_t day_index =
            floor_div(count, microseconds_per_day);
        const CivilDate date{
            std::chrono::sys_days{std::chrono::days{day_index}}};
        require_valid_date(date);
        const CivilTime time{
            count - day_index * microseconds_per_day};
        return format_civil_date(date) + "T" + format_civil_time(time) + "Z";
    }

    std::ostream &operator<<(std::ostream &out,
                             const CivilDateTime &value)
    {
        return out << format_civil_datetime(value);
    }

    std::ostream &operator<<(std::ostream &out, const Period &value)
    {
        return out << "Period(months=" << value.total_months()
                   << ", days=" << value.days() << ')';
    }

    std::ostream &operator<<(std::ostream &out, const ZoneId &value)
    {
        return out << value.name();
    }

    std::string format_zoned_datetime(const ZonedDateTime &value)
    {
        const int  offset = value.offset_seconds();
        const char sign = offset < 0 ? '-' : '+';
        const int  magnitude = offset < 0 ? -offset : offset;
        char       suffix[16];
        std::snprintf(suffix, sizeof suffix, "%c%02d:%02d", sign,
                      magnitude / 3600, (magnitude / 60) % 60);
        std::string text = format_civil_datetime(value.civil());
        text += suffix;
        text += '[';
        text += value.zone().name();
        text += ']';
        return text;
    }

    // Streaming delegates to the string form so the layout is stated once.
    std::ostream &operator<<(std::ostream &out,
                             const ZonedDateTime &value)
    {
        return out << format_zoned_datetime(value);
    }

    std::ostream &operator<<(std::ostream &out,
                             const InstantRange &value)
    {
        write_range(out, value);
        return out;
    }

    std::ostream &operator<<(std::ostream &out,
                             const CivilDateRange &value)
    {
        write_range(out, value);
        return out;
    }
}  // namespace hgraph
