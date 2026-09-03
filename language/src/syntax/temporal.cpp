#include "syntax/temporal.h"

#include <array>
#include <cstddef>
#include <limits>
#include <string>
#include <string_view>

namespace hgl::syntax
{
    namespace
    {
        constexpr std::int64_t micros_per_second = 1'000'000;
        constexpr std::int64_t micros_per_minute = 60 * micros_per_second;
        constexpr std::int64_t micros_per_hour = 60 * micros_per_minute;
        constexpr std::int64_t micros_per_day = 24 * micros_per_hour;

        constexpr std::int64_t seconds_per_minute = 60;
        constexpr std::int64_t seconds_per_hour = 60 * seconds_per_minute;

        // The proleptic Gregorian years a four-digit calendar date can spell.
        constexpr int min_year = 1;
        constexpr int max_year = 9999;

        // The instants whose UTC spelling stays within those years: the
        // canonical spelling of an instant is always UTC, so an offset that
        // would carry it into year 0 or 10000 has no spelling to round-trip to.
        constexpr std::int64_t min_instant_micros = -719162 * micros_per_day;
        constexpr std::int64_t max_instant_micros = 2932897 * micros_per_day - 1;

        // The single-character units d/h/m/s and the two-character units ms/us,
        // in descending order. `rank` is the position in that order.
        struct DurationUnit
        {
            std::string_view name;
            std::int64_t micros;
            int rank;
        };

        constexpr std::array<DurationUnit, 6> duration_units{{
            {"d", micros_per_day, 0},
            {"h", micros_per_hour, 1},
            {"m", micros_per_minute, 2},
            {"s", micros_per_second, 3},
            {"ms", 1'000, 4},
            {"us", 1, 5},
        }};

        constexpr std::string_view unit_list = "d h m s ms us";

        [[nodiscard]] constexpr bool is_digit(char c) noexcept
        {
            return c >= '0' && c <= '9';
        }

        [[nodiscard]] constexpr bool is_alpha(char c) noexcept
        {
            return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z');
        }

        [[nodiscard]] std::string quoted(std::string_view text)
        {
            std::string out;
            out.reserve(text.size() + 2);
            out += '\'';
            out += text;
            out += '\'';
            return out;
        }

        // Every diagnostic quotes the literal it is about, so callers only
        // supply the predicate: "'<spelling>' <what>".
        [[nodiscard]] TemporalParseResult failure(std::string_view spelling, std::string_view what,
                                                  std::string hint = {})
        {
            TemporalParseResult result;
            result.error = quoted(spelling);
            result.error += ' ';
            result.error += what;
            result.hint = std::move(hint);
            return result;
        }

        [[nodiscard]] TemporalParseResult success(TemporalValue value)
        {
            TemporalParseResult result;
            result.value = std::move(value);
            return result;
        }

        [[nodiscard]] bool is_leap_year(int year) noexcept
        {
            return year % 4 == 0 && (year % 100 != 0 || year % 400 == 0);
        }

        [[nodiscard]] int days_in_month(int year, int month) noexcept
        {
            constexpr std::array<int, 12> lengths{31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
            if (month == 2 && is_leap_year(year))
            {
                return 29;
            }
            return lengths[static_cast<std::size_t>(month - 1)];
        }

        // A read cursor over the literal's spelling. Every `take_*` either
        // consumes exactly what it says and returns true, or consumes nothing
        // and returns false.
        struct Cursor
        {
            std::string_view text;
            std::size_t pos{0};

            [[nodiscard]] bool at_end() const noexcept
            {
                return pos >= text.size();
            }

            [[nodiscard]] char peek(std::size_t ahead = 0) const noexcept
            {
                const std::size_t index = pos + ahead;
                return index < text.size() ? text[index] : '\0';
            }

            [[nodiscard]] bool take(char c) noexcept
            {
                if (peek() != c)
                {
                    return false;
                }
                ++pos;
                return true;
            }

            // Exactly `count` decimal digits, as an int (count is at most four).
            [[nodiscard]] bool take_digits(std::size_t count, int &out) noexcept
            {
                int value = 0;
                for (std::size_t i = 0; i < count; ++i)
                {
                    const char c = peek(i);
                    if (!is_digit(c))
                    {
                        return false;
                    }
                    value = value * 10 + (c - '0');
                }
                pos += count;
                out = value;
                return true;
            }

            // The maximal run of characters satisfying `pred`.
            template <typename Pred>
            [[nodiscard]] std::string_view take_while(Pred pred) noexcept
            {
                const std::size_t start = pos;
                while (!at_end() && pred(text[pos]))
                {
                    ++pos;
                }
                return text.substr(start, pos - start);
            }
        };

        // ---- calendar dates and clock times -------------------------------

        // `what` receives the diagnostic predicate when parsing fails.
        [[nodiscard]] std::optional<std::int64_t> parse_calendar_date(Cursor &cursor, std::string &what)
        {
            int year = 0;
            int month = 0;
            int day = 0;
            if (!cursor.take_digits(4, year) || !cursor.take('-') || !cursor.take_digits(2, month) ||
                !cursor.take('-') || !cursor.take_digits(2, day))
            {
                what = "does not have the shape YYYY-MM-DD";
                return std::nullopt;
            }
            const auto days = days_from_civil(year, month, day);
            if (!days)
            {
                what = "is not a calendar date";
                return std::nullopt;
            }
            return days;
        }

        // HH:MM[:SS[.ffffff]] as microseconds since midnight.
        [[nodiscard]] std::optional<std::int64_t> parse_clock_time(Cursor &cursor, std::string &what)
        {
            constexpr std::string_view shape = "does not have the shape HH:MM[:SS[.ffffff]]";
            int hour = 0;
            int minute = 0;
            int second = 0;
            std::int64_t fraction_micros = 0;
            if (!cursor.take_digits(2, hour) || !cursor.take(':') || !cursor.take_digits(2, minute))
            {
                what = shape;
                return std::nullopt;
            }
            if (cursor.take(':'))
            {
                if (!cursor.take_digits(2, second))
                {
                    what = shape;
                    return std::nullopt;
                }
                if (cursor.take('.'))
                {
                    const std::string_view digits = cursor.take_while(is_digit);
                    if (digits.empty())
                    {
                        what = "has an empty fraction; write one to six digits after the point";
                        return std::nullopt;
                    }
                    if (digits.size() > 6)
                    {
                        what = "has a fraction longer than six digits; times resolve to microseconds";
                        return std::nullopt;
                    }
                    for (const char c : digits)
                    {
                        fraction_micros = fraction_micros * 10 + (c - '0');
                    }
                    for (std::size_t i = digits.size(); i < 6; ++i)
                    {
                        fraction_micros *= 10;
                    }
                }
            }
            if (second == 60)
            {
                what = "is a leap second; a time is earlier than 24:00:00 and has no second 60";
                return std::nullopt;
            }
            if (hour > 23 || minute > 59 || second > 59)
            {
                what = "is not a time of day";
                return std::nullopt;
            }
            return hour * micros_per_hour + minute * micros_per_minute + second * micros_per_second +
                   fraction_micros;
        }

        // Z | (+|-) HH [":" MM] as signed seconds east of UTC.
        [[nodiscard]] std::optional<std::int32_t> parse_utc_offset(Cursor &cursor, std::string &what)
        {
            constexpr std::string_view shape =
                "has an invalid offset; write Z, +HH, -HH, +HH:MM or -HH:MM with hours 00-23 and minutes 00-59";
            if (cursor.take('Z'))
            {
                return 0;
            }
            const char sign = cursor.peek();
            if (sign != '+' && sign != '-')
            {
                what = shape;
                return std::nullopt;
            }
            ++cursor.pos;
            int hours = 0;
            int minutes = 0;
            if (!cursor.take_digits(2, hours))
            {
                what = shape;
                return std::nullopt;
            }
            if (cursor.take(':') && !cursor.take_digits(2, minutes))
            {
                what = shape;
                return std::nullopt;
            }
            if (hours > 23 || minutes > 59)
            {
                what = shape;
                return std::nullopt;
            }
            const std::int32_t magnitude = static_cast<std::int32_t>(hours * seconds_per_hour + minutes * seconds_per_minute);
            return sign == '-' ? -magnitude : magnitude;
        }

        // "[" zone_name "]". On a syntactically invalid name `error` holds the
        // complete diagnostic (it quotes the annotation, not the literal).
        [[nodiscard]] std::optional<std::string> parse_zone_annotation(Cursor &cursor, std::string &what,
                                                                        std::string &error)
        {
            if (!cursor.take('['))
            {
                what = "is missing a zone annotation";
                return std::nullopt;
            }
            const std::string_view name = cursor.take_while([](char c) { return c != ']'; });
            if (!cursor.take(']'))
            {
                what = "has an unterminated zone annotation";
                return std::nullopt;
            }
            if (!is_valid_zone_name(name))
            {
                error = quoted(std::string{'['} + std::string{name} + ']') + " is not a valid zone name";
                return std::nullopt;
            }
            return std::string{name};
        }

        [[nodiscard]] TemporalParseResult zone_failure(std::string_view spelling, std::string_view what,
                                                       const std::string &error)
        {
            if (!error.empty())
            {
                TemporalParseResult result;
                result.error = error;
                return result;
            }
            return failure(spelling, what);
        }

        [[nodiscard]] TemporalParseResult trailing_failure(std::string_view spelling, const Cursor &cursor)
        {
            std::string what = "has unexpected '";
            what += cursor.peek();
            what += "' after the literal";
            return failure(spelling, what);
        }

        // ---- the "@" literal family ---------------------------------------

        [[nodiscard]] TemporalParseResult parse_date_family(std::string_view spelling, Cursor cursor)
        {
            std::string what;
            std::string zone_error;
            const auto days = parse_calendar_date(cursor, what);
            if (!days)
            {
                return failure(spelling, what);
            }
            if (cursor.at_end())
            {
                return success(TemporalValue{.kind = TemporalKind::Date, .micros = *days});
            }
            if (cursor.peek() == 't')
            {
                return failure(spelling, "separates the date and time with 't'; the separator is an upper-case 'T'");
            }
            if (!cursor.take('T'))
            {
                return trailing_failure(spelling, cursor);
            }
            const auto time_micros = parse_clock_time(cursor, what);
            if (!time_micros)
            {
                return failure(spelling, what);
            }
            const std::int64_t local_micros = *days * micros_per_day + *time_micros;

            std::optional<std::int32_t> offset;
            if (cursor.peek() == 'z')
            {
                return failure(spelling, "spells the UTC offset as 'z'; it is an upper-case 'Z'");
            }
            if (!cursor.at_end() && cursor.peek() != '[')
            {
                offset = parse_utc_offset(cursor, what);
                if (!offset)
                {
                    return failure(spelling, what);
                }
            }

            std::optional<std::string> zone;
            if (cursor.peek() == '[')
            {
                zone = parse_zone_annotation(cursor, what, zone_error);
                if (!zone)
                {
                    return zone_failure(spelling, what, zone_error);
                }
            }
            if (!cursor.at_end())
            {
                return trailing_failure(spelling, cursor);
            }

            if (zone && !offset)
            {
                std::string hint = "write the offset the zone had at that local time, for example '";
                hint += spelling.substr(0, spelling.find('['));
                hint += "+00:00[";
                hint += *zone;
                hint += "]', or resolve the civil datetime against @[";
                hint += *zone;
                hint += "] with explicit gap and fold policies";
                return failure(spelling, "has no offset; add it or use resolve()", std::move(hint));
            }
            if (!offset)
            {
                return success(TemporalValue{.kind = TemporalKind::CivilDateTime, .micros = local_micros});
            }
            const std::int64_t utc_micros = local_micros - static_cast<std::int64_t>(*offset) * micros_per_second;
            if (utc_micros < min_instant_micros || utc_micros > max_instant_micros)
            {
                return failure(spelling, "denotes an instant outside the years 0001 to 9999 in UTC");
            }
            if (!zone)
            {
                return success(TemporalValue{.kind = TemporalKind::DateTime, .micros = utc_micros});
            }
            return success(TemporalValue{.kind = TemporalKind::ZonedDateTime,
                                         .micros = utc_micros,
                                         .offset_seconds = *offset,
                                         .zone = std::move(*zone)});
        }

        [[nodiscard]] TemporalParseResult parse_time_family(std::string_view spelling, Cursor cursor)
        {
            std::string what;
            std::string zone_error;
            const auto time_micros = parse_clock_time(cursor, what);
            if (!time_micros)
            {
                return failure(spelling, what);
            }
            if (cursor.at_end())
            {
                return success(TemporalValue{.kind = TemporalKind::Time, .micros = *time_micros});
            }
            if (cursor.peek() != '[')
            {
                return trailing_failure(spelling, cursor);
            }
            auto zone = parse_zone_annotation(cursor, what, zone_error);
            if (!zone)
            {
                return zone_failure(spelling, what, zone_error);
            }
            if (!cursor.at_end())
            {
                return trailing_failure(spelling, cursor);
            }
            return success(TemporalValue{.kind = TemporalKind::ZonedTime, .micros = *time_micros, .zone = std::move(*zone)});
        }

        [[nodiscard]] TemporalParseResult parse_zone_family(std::string_view spelling, Cursor cursor)
        {
            std::string what;
            std::string zone_error;
            auto zone = parse_zone_annotation(cursor, what, zone_error);
            if (!zone)
            {
                return zone_failure(spelling, what, zone_error);
            }
            if (!cursor.at_end())
            {
                return trailing_failure(spelling, cursor);
            }
            return success(TemporalValue{.kind = TemporalKind::TimeZone, .zone = std::move(*zone)});
        }

        [[nodiscard]] TemporalParseResult parse_at_literal(std::string_view spelling)
        {
            Cursor cursor{spelling, 1};
            if (cursor.peek() == '[')
            {
                return parse_zone_family(spelling, cursor);
            }
            // The shape is chosen by the separator after the leading digits so
            // that a wrong digit count gets a diagnostic naming the intended shape.
            std::size_t digits = 0;
            while (is_digit(cursor.peek(digits)))
            {
                ++digits;
            }
            if (digits > 0 && cursor.peek(digits) == '-')
            {
                return parse_date_family(spelling, cursor);
            }
            if (digits > 0 && cursor.peek(digits) == ':')
            {
                return parse_time_family(spelling, cursor);
            }
            return failure(spelling, "is not a temporal literal; write @YYYY-MM-DD, @HH:MM, @YYYY-MM-DDTHH:MM with an offset, or @[Zone]");
        }

        // ---- durations ----------------------------------------------------

        [[nodiscard]] const DurationUnit *find_duration_unit(std::string_view name) noexcept
        {
            for (const auto &unit : duration_units)
            {
                if (unit.name == name)
                {
                    return &unit;
                }
            }
            return nullptr;
        }

        // Adds `addend` to `total`, reporting overflow of the 64-bit range.
        [[nodiscard]] bool checked_add(std::int64_t &total, std::int64_t addend) noexcept
        {
            if (addend > std::numeric_limits<std::int64_t>::max() - total)
            {
                return false;
            }
            total += addend;
            return true;
        }

        // The integer count of a part times its unit, or nullopt on overflow.
        [[nodiscard]] std::optional<std::int64_t> whole_part_micros(std::string_view digits, std::int64_t unit_micros) noexcept
        {
            constexpr std::int64_t limit = std::numeric_limits<std::int64_t>::max();
            std::int64_t count = 0;
            for (const char c : digits)
            {
                const int digit = c - '0';
                if (count > (limit - digit) / 10)
                {
                    return std::nullopt;
                }
                count = count * 10 + digit;
            }
            if (unit_micros != 0 && count > limit / unit_micros)
            {
                return std::nullopt;
            }
            return count * unit_micros;
        }

        // The fractional count of a part, `0.<digits>` units, in microseconds.
        // Computed exactly: the value is digits * unit / 10^n, which is a whole
        // number only when the tens can be cancelled between the two factors.
        // Returns nullopt when the part is not a whole number of microseconds.
        [[nodiscard]] std::optional<std::int64_t> fraction_part_micros(std::string_view digits, std::int64_t unit_micros) noexcept
        {
            while (!digits.empty() && digits.back() == '0')
            {
                digits.remove_suffix(1);
            }
            if (digits.empty())
            {
                return 0;
            }
            // No unit has more than eight factors of ten, so once the fraction
            // has this many significant digits it cannot be whole. This also
            // keeps the numerator inside 64 bits.
            if (digits.size() > 13)
            {
                return std::nullopt;
            }
            std::int64_t numerator = 0;
            for (const char c : digits)
            {
                numerator = numerator * 10 + (c - '0');
            }
            std::int64_t scale = unit_micros;
            std::size_t tens = digits.size();
            while (tens > 0 && scale % 10 == 0)
            {
                scale /= 10;
                --tens;
            }
            for (std::size_t i = 0; i < tens; ++i)
            {
                if (numerator % 5 == 0)
                {
                    numerator /= 5;
                }
                else if (scale % 5 == 0)
                {
                    scale /= 5;
                }
                else
                {
                    return std::nullopt;
                }
                if (numerator % 2 == 0)
                {
                    numerator /= 2;
                }
                else if (scale % 2 == 0)
                {
                    scale /= 2;
                }
                else
                {
                    return std::nullopt;
                }
            }
            // numerator * scale == digits * unit / 10^n < unit, so this fits.
            return numerator * scale;
        }

        [[nodiscard]] TemporalParseResult parse_duration_literal(std::string_view spelling)
        {
            Cursor cursor{spelling};
            std::int64_t total = 0;
            int previous_rank = -1;
            bool previous_had_fraction = false;
            if (spelling.empty() || !is_digit(cursor.peek()))
            {
                return failure(spelling, "is not a duration literal; write a number followed by one of " +
                                             std::string{unit_list});
            }
            while (!cursor.at_end())
            {
                if (!is_digit(cursor.peek()))
                {
                    return trailing_failure(spelling, cursor);
                }
                if (previous_had_fraction)
                {
                    return failure(spelling, "has a fraction on a part that is not last");
                }
                const std::string_view whole = cursor.take_while(is_digit);
                std::string_view fraction;
                bool has_fraction = false;
                if (cursor.take('.'))
                {
                    fraction = cursor.take_while(is_digit);
                    if (fraction.empty())
                    {
                        return failure(spelling, "has an empty fraction; write digits after the point");
                    }
                    has_fraction = true;
                }
                const std::string_view unit_name = cursor.take_while(is_alpha);
                if (unit_name.empty())
                {
                    return failure(spelling, "is missing a duration unit; units are " + std::string{unit_list});
                }
                const DurationUnit *unit = find_duration_unit(unit_name);
                if (unit == nullptr)
                {
                    return failure(spelling, "has unknown duration unit " + quoted(unit_name) + "; units are " +
                                                 std::string{unit_list});
                }
                if (unit->rank == previous_rank)
                {
                    return failure(spelling, "lists duration unit " + quoted(unit_name) + " twice");
                }
                if (unit->rank < previous_rank)
                {
                    return failure(spelling, "lists duration units out of descending order");
                }
                const auto whole_micros = whole_part_micros(whole, unit->micros);
                if (!whole_micros || !checked_add(total, *whole_micros))
                {
                    return failure(spelling, "is outside the 64-bit microsecond range of a duration");
                }
                if (has_fraction)
                {
                    const auto fraction_micros = fraction_part_micros(fraction, unit->micros);
                    if (!fraction_micros)
                    {
                        return failure(spelling, "is not a whole number of microseconds");
                    }
                    if (!checked_add(total, *fraction_micros))
                    {
                        return failure(spelling, "is outside the 64-bit microsecond range of a duration");
                    }
                }
                previous_rank = unit->rank;
                previous_had_fraction = has_fraction;
            }
            return success(TemporalValue{.kind = TemporalKind::Duration, .micros = total});
        }

        // ---- canonical spelling helpers -----------------------------------

        void append_padded(std::string &out, std::int64_t value, std::size_t width)
        {
            const std::string digits = std::to_string(value);
            for (std::size_t i = digits.size(); i < width; ++i)
            {
                out += '0';
            }
            out += digits;
        }

        void append_calendar_date(std::string &out, std::int64_t days)
        {
            const CivilDate date = civil_from_days(days);
            append_padded(out, date.year, 4);
            out += '-';
            append_padded(out, date.month, 2);
            out += '-';
            append_padded(out, date.day, 2);
        }

        // The shortest clock time: seconds only when non-zero, and a fraction
        // without trailing zeros only when non-zero.
        void append_clock_time(std::string &out, std::int64_t micros_since_midnight)
        {
            const std::int64_t hours = micros_since_midnight / micros_per_hour;
            const std::int64_t minutes = (micros_since_midnight % micros_per_hour) / micros_per_minute;
            const std::int64_t seconds = (micros_since_midnight % micros_per_minute) / micros_per_second;
            const std::int64_t fraction = micros_since_midnight % micros_per_second;
            append_padded(out, hours, 2);
            out += ':';
            append_padded(out, minutes, 2);
            if (seconds == 0 && fraction == 0)
            {
                return;
            }
            out += ':';
            append_padded(out, seconds, 2);
            if (fraction == 0)
            {
                return;
            }
            std::string digits;
            append_padded(digits, fraction, 6);
            while (digits.back() == '0')
            {
                digits.pop_back();
            }
            out += '.';
            out += digits;
        }

        // Splits epoch microseconds into days and the time within the day,
        // flooring so that instants before the epoch land on the right day.
        void split_epoch_micros(std::int64_t micros, std::int64_t &days, std::int64_t &within_day) noexcept
        {
            days = micros / micros_per_day;
            within_day = micros % micros_per_day;
            if (within_day < 0)
            {
                within_day += micros_per_day;
                --days;
            }
        }

        void append_local_datetime(std::string &out, std::int64_t local_micros)
        {
            std::int64_t days = 0;
            std::int64_t within_day = 0;
            split_epoch_micros(local_micros, days, within_day);
            append_calendar_date(out, days);
            out += 'T';
            append_clock_time(out, within_day);
        }

        // Z for zero, otherwise the shortest of +HH / +HH:MM.
        void append_utc_offset(std::string &out, std::int32_t offset_seconds)
        {
            if (offset_seconds == 0)
            {
                out += 'Z';
                return;
            }
            const std::int64_t magnitude = offset_seconds < 0 ? -static_cast<std::int64_t>(offset_seconds) : offset_seconds;
            out += offset_seconds < 0 ? '-' : '+';
            append_padded(out, magnitude / seconds_per_hour, 2);
            const std::int64_t minutes = (magnitude % seconds_per_hour) / seconds_per_minute;
            if (minutes != 0)
            {
                out += ':';
                append_padded(out, minutes, 2);
            }
        }

        void append_zone_annotation(std::string &out, std::string_view zone)
        {
            out += '[';
            out += zone;
            out += ']';
        }

        void append_duration(std::string &out, std::int64_t micros)
        {
            if (micros == 0)
            {
                out += "0s";
                return;
            }
            // Work unsigned so the most negative value negates cleanly.
            std::uint64_t remaining = 0;
            if (micros < 0)
            {
                out += '-';
                remaining = static_cast<std::uint64_t>(0) - static_cast<std::uint64_t>(micros);
            }
            else
            {
                remaining = static_cast<std::uint64_t>(micros);
            }
            for (const auto &unit : duration_units)
            {
                const auto unit_micros = static_cast<std::uint64_t>(unit.micros);
                const std::uint64_t count = remaining / unit_micros;
                remaining %= unit_micros;
                if (count != 0)
                {
                    out += std::to_string(count);
                    out += unit.name;
                }
            }
        }
    } // namespace

    std::string_view temporal_kind_name(TemporalKind kind) noexcept
    {
        switch (kind)
        {
            case TemporalKind::Date:
                return "date";
            case TemporalKind::Time:
                return "time";
            case TemporalKind::DateTime:
                return "datetime";
            case TemporalKind::CivilDateTime:
                return "civil_datetime";
            case TemporalKind::ZonedDateTime:
                return "zoned_datetime";
            case TemporalKind::ZonedTime:
                return "zoned_time";
            case TemporalKind::TimeZone:
                return "timezone";
            case TemporalKind::Duration:
                return "duration";
        }
        return "unknown";
    }

    TemporalParseResult parse_temporal_literal(std::string_view spelling)
    {
        if (!spelling.empty() && spelling.front() == '@')
        {
            return parse_at_literal(spelling);
        }
        return parse_duration_literal(spelling);
    }

    std::string canonical_spelling(const TemporalValue &value)
    {
        std::string out;
        switch (value.kind)
        {
            case TemporalKind::Date:
                out += '@';
                append_calendar_date(out, value.micros);
                break;
            case TemporalKind::Time:
                out += '@';
                append_clock_time(out, value.micros);
                break;
            case TemporalKind::DateTime:
                out += '@';
                append_local_datetime(out, value.micros);
                out += 'Z';
                break;
            case TemporalKind::CivilDateTime:
                out += '@';
                append_local_datetime(out, value.micros);
                break;
            case TemporalKind::ZonedDateTime:
                out += '@';
                append_local_datetime(out, value.micros + static_cast<std::int64_t>(value.offset_seconds) * micros_per_second);
                append_utc_offset(out, value.offset_seconds);
                append_zone_annotation(out, value.zone);
                break;
            case TemporalKind::ZonedTime:
                out += '@';
                append_clock_time(out, value.micros);
                append_zone_annotation(out, value.zone);
                break;
            case TemporalKind::TimeZone:
                out += '@';
                append_zone_annotation(out, value.zone);
                break;
            case TemporalKind::Duration:
                append_duration(out, value.micros);
                break;
        }
        return out;
    }

    bool is_valid_zone_name(std::string_view name) noexcept
    {
        if (name.empty() || name.size() > 255)
        {
            return false;
        }
        if (name.front() == '/' || name.back() == '/')
        {
            return false;
        }
        std::size_t component_start = 0;
        for (std::size_t i = 0; i <= name.size(); ++i)
        {
            const bool at_boundary = i == name.size() || name[i] == '/';
            if (!at_boundary)
            {
                const char c = name[i];
                const bool allowed = is_alpha(c) || is_digit(c) || c == '.' || c == '_' || c == '-' || c == '+';
                if (!allowed)
                {
                    return false;
                }
                continue;
            }
            const std::string_view component = name.substr(component_start, i - component_start);
            if (component.empty() || component == "." || component == "..")
            {
                return false;
            }
            component_start = i + 1;
        }
        return true;
    }

    // Howard Hinnant's days_from_civil, restricted to the years a literal can spell.
    std::optional<std::int64_t> days_from_civil(int year, int month, int day) noexcept
    {
        if (year < min_year || year > max_year || month < 1 || month > 12 || day < 1 || day > days_in_month(year, month))
        {
            return std::nullopt;
        }
        const std::int64_t y = year - (month <= 2 ? 1 : 0);
        const std::int64_t era = (y >= 0 ? y : y - 399) / 400;
        const std::int64_t yoe = y - era * 400;
        const std::int64_t mp = month + (month > 2 ? -3 : 9);
        const std::int64_t doy = (153 * mp + 2) / 5 + day - 1;
        const std::int64_t doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
        return era * 146097 + doe - 719468;
    }

    // Howard Hinnant's civil_from_days.
    CivilDate civil_from_days(std::int64_t days) noexcept
    {
        const std::int64_t z = days + 719468;
        const std::int64_t era = (z >= 0 ? z : z - 146096) / 146097;
        const std::int64_t doe = z - era * 146097;
        const std::int64_t yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
        const std::int64_t y = yoe + era * 400;
        const std::int64_t doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
        const std::int64_t mp = (5 * doy + 2) / 153;
        const std::int64_t d = doy - (153 * mp + 2) / 5 + 1;
        const std::int64_t m = mp < 10 ? mp + 3 : mp - 9;
        return CivilDate{static_cast<int>(y + (m <= 2 ? 1 : 0)), static_cast<int>(m), static_cast<int>(d)};
    }
} // namespace hgl::syntax
