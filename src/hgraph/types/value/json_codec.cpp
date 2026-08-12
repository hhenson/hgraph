#include <hgraph/types/value/json_codec.h>

#include <hgraph/types/metadata/value_plan_factory.h>
#include <hgraph/types/metadata/type_realization.h>
#include <hgraph/types/primitive_types.h>
#include <hgraph/types/static_schema.h>
#include <hgraph/types/temporal.h>
#include <hgraph/types/value/value_builder.h>
#include <hgraph/util/date_time.h>

#include <fmt/format.h>

#if defined(HGRAPH_TIME_ZONE_BACKEND_DATE)
#include <date/date.h>
#endif

#include <hgraph/util/scope.h>

#include <algorithm>
#include <array>
#include <charconv>
#include <chrono>
#include <cctype>
#include <locale>
#include <memory>
#include <mutex>
#include <optional>
#include <sstream>
#include <stdexcept>
#include <unordered_map>
#include <utility>
#include <vector>

namespace hgraph
{
    namespace
    {
        struct RegisteredJsonDateTimeFormats
        {
            std::vector<std::string> datetime{};
            std::vector<std::string> time{};
        };

        using JsonDateTimeFormatSnapshot =
            std::shared_ptr<const RegisteredJsonDateTimeFormats>;

        struct JsonDateTimeFormatRegistry
        {
            std::mutex mutex{};
            JsonDateTimeFormatSnapshot formats{
                std::make_shared<const RegisteredJsonDateTimeFormats>()};
        };

#if defined(HGRAPH_TIME_ZONE_BACKEND_DATE)
        template <typename Duration>
        using JsonSysTime = date::sys_time<Duration>;
        template <typename Duration>
        using JsonLocalTime = date::local_time<Duration>;
#else
        template <typename Duration>
        using JsonSysTime = std::chrono::sys_time<Duration>;
        template <typename Duration>
        using JsonLocalTime = std::chrono::local_time<Duration>;
#endif

        template <typename Stream, typename Value>
        void json_datetime_from_stream(
            Stream &stream, const char *format, Value &value)
        {
#if defined(HGRAPH_TIME_ZONE_BACKEND_DATE)
            date::from_stream(stream, format, value);
#else
            std::chrono::from_stream(stream, format, value);
#endif
        }

        JsonDateTimeFormatRegistry &registered_datetime_formats()
        {
            static JsonDateTimeFormatRegistry registry{};
            return registry;
        }

        [[nodiscard]] JsonDateTimeFormatSnapshot
            registered_datetime_format_snapshot()
        {
            auto &registry = registered_datetime_formats();
            std::lock_guard lock{registry.mutex};
            return registry.formats;
        }

        constexpr std::array<std::string_view, 8>
            builtin_json_datetime_formats{
                "%Y/%m/%d %H:%M:%S.%f",
                "%Y/%m/%d %H:%M:%S",
                "%Y/%m/%d",
                "%d-%b-%Y %H:%M:%S.%f",
                "%d-%b-%Y %H:%M:%S",
                "%d-%b-%Y",
                "%d %b %Y %H:%M:%S",
                "%d %b %Y",
            };
    }  // namespace

    namespace json_detail
    {
        // ---------------------------------------------------------------
        // Writing helpers
        // ---------------------------------------------------------------

        void append_escaped(std::string_view text, std::string &out)
        {
            out.push_back('"');
            for (const char c : text)
            {
                switch (c)
                {
                    case '"': out += "\\\""; break;
                    case '\\': out += "\\\\"; break;
                    case '\b': out += "\\b"; break;
                    case '\f': out += "\\f"; break;
                    case '\n': out += "\\n"; break;
                    case '\r': out += "\\r"; break;
                    case '\t': out += "\\t"; break;
                    default:
                        if (static_cast<unsigned char>(c) < 0x20)
                        {
                            out += fmt::format("\\u{:04x}", static_cast<unsigned char>(c));
                        }
                        else
                        {
                            out.push_back(c);
                        }
                }
            }
            out.push_back('"');
        }

        void append_date(Date value, std::string &out)
        {
            out += fmt::format("{:04}-{:02}-{:02}", static_cast<int>(value.year()),
                               static_cast<unsigned>(value.month()), static_cast<unsigned>(value.day()));
        }

        void append_time_of_day(std::int64_t micros_since_midnight, std::string &out)
        {
            const auto total_seconds = micros_since_midnight / 1'000'000;
            const auto micros = micros_since_midnight % 1'000'000;
            out += fmt::format("{:02}:{:02}:{:02}", total_seconds / 3'600,
                               (total_seconds / 60) % 60,
                               total_seconds % 60);
            if (micros != 0) { out += fmt::format(".{:06}", micros); }
        }

        // ---------------------------------------------------------------
        // Reader — a minimal recursive-descent tokenizer. Parsing is
        // meta-directed: at every position the converter knows what shape it
        // expects, so no DOM is built.
        // ---------------------------------------------------------------

        struct Reader
        {
            std::string_view text;
            std::size_t      pos{0};

            [[noreturn]] void fail(std::string_view message) const
            {
                throw std::invalid_argument(fmt::format("from_json: {} at offset {}", message, pos));
            }

            void skip_ws() noexcept
            {
                while (pos < text.size())
                {
                    const char c = text[pos];
                    if (c != ' ' && c != '\t' && c != '\n' && c != '\r') { break; }
                    ++pos;
                }
            }

            [[nodiscard]] char peek()
            {
                skip_ws();
                if (pos >= text.size()) { fail("unexpected end of input"); }
                return text[pos];
            }

            void expect(char c)
            {
                if (peek() != c) { fail(fmt::format("expected '{}'", c)); }
                ++pos;
            }

            [[nodiscard]] bool consume_if(char c)
            {
                if (pos < text.size() && peek() == c)
                {
                    ++pos;
                    return true;
                }
                return false;
            }

            [[nodiscard]] bool consume_keyword(std::string_view keyword)
            {
                skip_ws();
                if (text.substr(pos, keyword.size()) == keyword)
                {
                    pos += keyword.size();
                    return true;
                }
                return false;
            }

            [[nodiscard]] std::string parse_string()
            {
                expect('"');
                std::string result;
                while (true)
                {
                    if (pos >= text.size()) { fail("unterminated string"); }
                    const char c = text[pos++];
                    if (c == '"') { return result; }
                    if (c != '\\')
                    {
                        result.push_back(c);
                        continue;
                    }
                    if (pos >= text.size()) { fail("unterminated escape"); }
                    const char e = text[pos++];
                    switch (e)
                    {
                        case '"': result.push_back('"'); break;
                        case '\\': result.push_back('\\'); break;
                        case '/': result.push_back('/'); break;
                        case 'b': result.push_back('\b'); break;
                        case 'f': result.push_back('\f'); break;
                        case 'n': result.push_back('\n'); break;
                        case 'r': result.push_back('\r'); break;
                        case 't': result.push_back('\t'); break;
                        case 'u': {
                            if (pos + 4 > text.size()) { fail("truncated \\u escape"); }
                            unsigned code = 0;
                            for (int i = 0; i < 4; ++i)
                            {
                                const char h = text[pos++];
                                code <<= 4;
                                if (h >= '0' && h <= '9') { code += static_cast<unsigned>(h - '0'); }
                                else if (h >= 'a' && h <= 'f') { code += static_cast<unsigned>(h - 'a' + 10); }
                                else if (h >= 'A' && h <= 'F') { code += static_cast<unsigned>(h - 'A' + 10); }
                                else { fail("bad \\u escape"); }
                            }
                            // UTF-8 encode (BMP only; surrogate pairs are not combined).
                            if (code < 0x80) { result.push_back(static_cast<char>(code)); }
                            else if (code < 0x800)
                            {
                                result.push_back(static_cast<char>(0xC0 | (code >> 6)));
                                result.push_back(static_cast<char>(0x80 | (code & 0x3F)));
                            }
                            else
                            {
                                result.push_back(static_cast<char>(0xE0 | (code >> 12)));
                                result.push_back(static_cast<char>(0x80 | ((code >> 6) & 0x3F)));
                                result.push_back(static_cast<char>(0x80 | (code & 0x3F)));
                            }
                            break;
                        }
                        default: fail("unknown escape");
                    }
                }
            }

            [[nodiscard]] std::string_view parse_number_token()
            {
                skip_ws();
                const std::size_t start = pos;
                while (pos < text.size())
                {
                    const char c = text[pos];
                    if ((c >= '0' && c <= '9') || c == '-' || c == '+' || c == '.' || c == 'e' || c == 'E')
                    {
                        ++pos;
                    }
                    else { break; }
                }
                if (pos == start) { fail("expected a number"); }
                return text.substr(start, pos - start);
            }

            /** Skip one complete JSON value (for unknown bundle fields). */
            void skip_value()
            {
                const char c = peek();
                switch (c)
                {
                    case '{': {
                        ++pos;
                        if (consume_if('}')) { return; }
                        while (true)
                        {
                            (void)parse_string();
                            expect(':');
                            skip_value();
                            if (!consume_if(',')) { break; }
                        }
                        expect('}');
                        return;
                    }
                    case '[': {
                        ++pos;
                        if (consume_if(']')) { return; }
                        while (true)
                        {
                            skip_value();
                            if (!consume_if(',')) { break; }
                        }
                        expect(']');
                        return;
                    }
                    case '"': (void)parse_string(); return;
                    case 't':
                        if (!consume_keyword("true")) { fail("bad literal"); }
                        return;
                    case 'f':
                        if (!consume_keyword("false")) { fail("bad literal"); }
                        return;
                    case 'n':
                        if (!consume_keyword("null")) { fail("bad literal"); }
                        return;
                    default: (void)parse_number_token(); return;
                }
            }
        };

        // ---------------------------------------------------------------
        // Atomic parse helpers (Python strptime-format compatible)
        // ---------------------------------------------------------------

        [[nodiscard]] std::int64_t parse_int_token(std::string_view token, const Reader &reader)
        {
            std::int64_t value{};
            const auto [ptr, ec] = std::from_chars(token.data(), token.data() + token.size(), value);
            if (ec != std::errc{} || ptr != token.data() + token.size())
            {
                const_cast<Reader &>(reader).fail("bad integer");
            }
            return value;
        }

        [[nodiscard]] double parse_float_token(std::string_view token, const Reader &reader)
        {
            double value{};
            std::istringstream stream{std::string{token}};
            stream.imbue(std::locale::classic());
            stream >> std::noskipws >> value;
            if (!stream || !stream.eof())
            {
                const_cast<Reader &>(reader).fail("bad number");
            }
            return value;
        }

        [[nodiscard]] std::int64_t parse_fixed_int(std::string_view text, std::size_t &pos, Reader &reader)
        {
            std::size_t start = pos;
            while (pos < text.size() && text[pos] >= '0' && text[pos] <= '9') { ++pos; }
            if (pos == start) { reader.fail("bad date/time component"); }
            std::int64_t value{};
            (void)std::from_chars(text.data() + start, text.data() + pos, value);
            return value;
        }

        void expect_char(std::string_view text, std::size_t &pos, char c, Reader &reader)
        {
            if (pos >= text.size() || text[pos] != c) { reader.fail("bad date/time separator"); }
            ++pos;
        }

        struct TranslatedPythonDateTimeInput
        {
            std::string              format{};
            std::vector<std::string> candidates{};
        };

        [[nodiscard]] TranslatedPythonDateTimeInput
            translate_python_datetime_input(
                std::string_view text, std::string_view format)
        {
            TranslatedPythonDateTimeInput translated{
                std::string{format}, {std::string{text}}};
            const std::size_t fraction = format.find("%f");
            if (fraction == std::string_view::npos)
            {
                return translated;
            }

            const std::size_t seconds = format.rfind("%S", fraction);
            if (seconds == std::string_view::npos)
            {
                return translated;
            }
            const std::string_view separator = format.substr(
                seconds + 2, fraction - (seconds + 2));
            if (separator.find('%') != std::string_view::npos)
            {
                return translated;
            }

            // date::from_stream parses fractional seconds as part of %S,
            // whereas Python strptime exposes them as a separate %f. Remove
            // the literal separator and %f from the format, then normalize
            // that separator in candidate text to the classic locale's '.'.
            translated.format = std::string{format.substr(0, seconds + 2)};
            translated.format.append(format.substr(fraction + 2));

            const auto add_candidate = [&](std::string candidate) {
                if (std::ranges::find(
                        translated.candidates, candidate) ==
                    translated.candidates.end())
                {
                    translated.candidates.push_back(std::move(candidate));
                }
            };
            const auto is_digit = [](char value) {
                return std::isdigit(
                    static_cast<unsigned char>(value)) != 0;
            };

            if (separator == ".")
            {
                return translated;
            }
            if (separator.empty())
            {
                // For compact %S%f spellings, try each position after a
                // two-digit seconds field. Full-format parsing selects the
                // position consistent with the surrounding directives.
                for (std::size_t position = 2; position < text.size();
                     ++position)
                {
                    if (!is_digit(text[position - 2]) ||
                        !is_digit(text[position - 1]) ||
                        !is_digit(text[position]))
                    {
                        continue;
                    }
                    std::string candidate{text};
                    candidate.insert(position, 1, '.');
                    add_candidate(std::move(candidate));
                }
                return translated;
            }

            for (std::size_t position = text.find(separator);
                 position != std::string_view::npos;
                 position = text.find(separator, position + separator.size()))
            {
                const std::size_t fraction_start =
                    position + separator.size();
                if (fraction_start >= text.size() ||
                    !is_digit(text[fraction_start]))
                {
                    continue;
                }
                std::string candidate{text};
                candidate.replace(position, separator.size(), ".");
                add_candidate(std::move(candidate));
            }
            return translated;
        }

        [[nodiscard]] std::string colon_offset_format(
            std::string_view format)
        {
            std::string translated;
            translated.reserve(format.size() + 1);
            for (std::size_t position = 0; position < format.size();)
            {
                if (format.substr(position, 2) == "%z")
                {
                    translated += "%Ez";
                    position += 2;
                }
                else
                {
                    translated.push_back(format[position++]);
                }
            }
            return translated;
        }

        template <typename TimePoint>
        [[nodiscard]] std::optional<TimePoint> parse_datetime_format(
            std::string_view text, std::string_view python_format)
        {
            const auto translated =
                translate_python_datetime_input(text, python_format);
            const std::string &format = translated.format;
            const auto parse = [](std::string_view candidate,
                                  std::string_view candidate_format)
                -> std::optional<TimePoint> {
                std::istringstream stream{std::string{candidate}};
                stream.imbue(std::locale::classic());
                TimePoint value{};
                json_datetime_from_stream(
                    stream, std::string{candidate_format}.c_str(), value);
                if (stream.fail() || stream.rdbuf()->in_avail() != 0)
                {
                    return std::nullopt;
                }
                return value;
            };

            for (const std::string &candidate : translated.candidates)
            {
                if (auto value = parse(candidate, format)) { return value; }
            }

            const bool has_offset =
                format.find("%z") != std::string::npos ||
                format.find("%Ez") != std::string::npos;
            if (!has_offset) { return std::nullopt; }

            const std::string extended_format = colon_offset_format(format);
            if (extended_format != format)
            {
                for (const std::string &candidate : translated.candidates)
                {
                    if (auto value = parse(candidate, extended_format))
                    {
                        return value;
                    }
                }
            }
            for (const std::string &candidate : translated.candidates)
            {
                if (candidate.empty() ||
                    (candidate.back() != 'Z' && candidate.back() != 'z'))
                {
                    continue;
                }
                std::string normalized{
                    candidate.substr(0, candidate.size() - 1)};
                normalized += "+00:00";
                if (auto value = parse(normalized, extended_format))
                {
                    return value;
                }
            }
            return std::nullopt;
        }

        template <typename TimePoint, std::size_t N>
        [[nodiscard]] std::optional<TimePoint> parse_first_datetime_format(
            std::string_view text,
            const std::array<std::string_view, N> &formats)
        {
            for (const std::string_view format : formats)
            {
                if (auto value =
                        parse_datetime_format<TimePoint>(text, format))
                {
                    return value;
                }
            }
            return std::nullopt;
        }

        template <typename TimePoint>
        [[nodiscard]] std::optional<TimePoint> parse_json_datetime_value(
            std::string_view text)
        {
            static constexpr std::array<std::string_view, 3> compact_formats{
                "%Y%m%d", "%Y%m%d%H%M%S", "%Y%m%d%H%M%S"};
            static constexpr std::array<std::string_view, 12> iso_formats{
                "%Y-%m-%dT%H:%M:%S%Ez",
                "%Y-%m-%dT%H:%M%Ez",
                "%Y-%m-%d %H:%M:%S%Ez",
                "%Y-%m-%d %H:%M%Ez",
                "%Y-%m-%dT%H:%M:%S",
                "%Y-%m-%dT%H:%M",
                "%Y-%m-%d %H:%M:%S",
                "%Y-%m-%d %H:%M",
                "%Y-%m-%d",
                "%Y%m%d",
                "%Y-%m-%dT%H",
                "%Y-%m-%d %H",
            };
            const auto digits = [](std::string_view value) {
                return std::ranges::all_of(
                    value, [](char character) {
                        return std::isdigit(
                            static_cast<unsigned char>(character));
                    });
            };
            // Basic ISO date/time has adjacent variable-width numeric
            // directives which date::from_stream can partition incorrectly.
            // Normalize the unambiguous lengths before general parsing.
            if (text.size() >= 13 && text[8] == 'T' &&
                digits(text.substr(0, 8)) && digits(text.substr(9, 4)))
            {
                const bool has_seconds =
                    text.size() >= 15 && digits(text.substr(13, 2));
                std::string normalized;
                normalized.reserve(text.size() + 5);
                normalized.append(text.substr(0, 4));
                normalized.push_back('-');
                normalized.append(text.substr(4, 2));
                normalized.push_back('-');
                normalized.append(text.substr(6, 2));
                normalized.push_back('T');
                normalized.append(text.substr(9, 2));
                normalized.push_back(':');
                normalized.append(text.substr(11, 2));
                std::size_t remainder = 13;
                if (has_seconds)
                {
                    normalized.push_back(':');
                    normalized.append(text.substr(13, 2));
                    remainder = 15;
                }
                normalized.append(text.substr(remainder));
                const auto local_format = has_seconds ?
                    "%Y-%m-%dT%H:%M:%S" : "%Y-%m-%dT%H:%M";
                if (auto value = parse_datetime_format<TimePoint>(
                        normalized, local_format))
                {
                    return value;
                }
                const auto offset_format = has_seconds ?
                    "%Y-%m-%dT%H:%M:%S%Ez" :
                    "%Y-%m-%dT%H:%M%Ez";
                if (auto value = parse_datetime_format<TimePoint>(
                        normalized, offset_format))
                {
                    return value;
                }
            }
            const bool digits_only = std::ranges::all_of(
                text, [](char value) {
                    return std::isdigit(
                        static_cast<unsigned char>(value));
                });
            if (digits_only)
            {
                const std::size_t index =
                    text.size() == 8 ? 0 : text.size() == 14 ? 1 :
                    text.size() == 20 ? 2 : compact_formats.size();
                if (index < compact_formats.size())
                {
                    std::string normalized{text};
                    if (text.size() == 20) { normalized.insert(14, "."); }
                    if (auto value = parse_datetime_format<TimePoint>(
                            normalized, compact_formats[index]))
                    {
                        return value;
                    }
                }
            }
            if (auto value =
                    parse_first_datetime_format<TimePoint>(text, iso_formats))
            {
                return value;
            }
            if (auto value = parse_first_datetime_format<TimePoint>(
                    text, builtin_json_datetime_formats))
            {
                return value;
            }
            const auto registered = registered_datetime_format_snapshot();
            for (const std::string &format : registered->datetime)
            {
                if (auto value =
                        parse_datetime_format<TimePoint>(text, format))
                {
                    return value;
                }
            }
            return std::nullopt;
        }

        [[nodiscard]] std::optional<CivilTime> parse_json_time_value(
            std::string_view text)
        {
            static constexpr std::array<std::string_view, 3> iso_formats{
                "%H:%M:%S", "%H:%M", "%H"};
            const bool digits_only = std::ranges::all_of(
                text, [](char value) {
                    return std::isdigit(
                        static_cast<unsigned char>(value));
                });
            if (digits_only && (text.size() == 6 || text.size() == 12))
            {
                std::string normalized{text};
                if (text.size() == 12) { normalized.insert(6, "."); }
                std::chrono::microseconds value{};
                std::istringstream stream{normalized};
                stream.imbue(std::locale::classic());
                json_datetime_from_stream(stream, "%H%M%S", value);
                if (!stream.fail() && stream.rdbuf()->in_avail() == 0)
                {
                    return CivilTime{value.count()};
                }
            }
            const auto parse = [](std::string_view candidate,
                                  std::string_view python_format)
                -> std::optional<CivilTime> {
                const auto translated = translate_python_datetime_input(
                    candidate, python_format);
                for (const std::string &normalized : translated.candidates)
                {
                    std::istringstream stream{normalized};
                    stream.imbue(std::locale::classic());
                    std::chrono::microseconds value{};
                    json_datetime_from_stream(
                        stream, translated.format.c_str(), value);
                    if (stream.fail() || stream.rdbuf()->in_avail() != 0)
                    {
                        continue;
                    }
                    if (translated.format.find("%p") != std::string::npos)
                    {
                        std::string meridiem{normalized};
                        std::ranges::transform(
                            meridiem, meridiem.begin(), [](char value) {
                                return static_cast<char>(std::toupper(
                                    static_cast<unsigned char>(value)));
                            });
                        const bool is_am =
                            meridiem.find("AM") != std::string::npos;
                        const bool is_pm =
                            meridiem.find("PM") != std::string::npos;
                        if (is_am == is_pm) { continue; }

                        const auto hour = std::chrono::duration_cast<
                            std::chrono::hours>(value);
                        // Howard Hinnant date releases differ in whether
                        // parsing a duration applies %p. Normalize either.
                        if (is_pm && hour < std::chrono::hours{12})
                        {
                            value += std::chrono::hours{12};
                        }
                        else if (is_am && hour >= std::chrono::hours{12})
                        {
                            value -= std::chrono::hours{12};
                        }
                    }
                    if (value < std::chrono::microseconds{0} ||
                        value >= std::chrono::hours{24})
                    {
                        continue;
                    }
                    return CivilTime{value.count()};
                }
                return std::nullopt;
            };
            for (const std::string_view format : iso_formats)
            {
                if (auto value = parse(text, format)) { return value; }
            }
            const auto registered = registered_datetime_format_snapshot();
            for (const std::string &format : registered->time)
            {
                if (auto value = parse(text, format)) { return value; }
            }
            return std::nullopt;
        }

        [[nodiscard]] CivilDate json_date(
            std::string_view text, Reader &reader)
        {
            using LocalMicros =
                JsonLocalTime<std::chrono::microseconds>;
            auto value = parse_json_datetime_value<LocalMicros>(text);
            if (!value)
            {
                reader.fail(fmt::format(
                    "cannot parse '{}' as a date; it is not ISO 8601 nor any registered format",
                    text));
            }
            const auto day = std::chrono::floor<std::chrono::days>(*value);
            return CivilDate{std::chrono::sys_days{day.time_since_epoch()}};
        }

        [[nodiscard]] CivilTime json_time(
            std::string_view text, Reader &reader)
        {
            auto value = parse_json_time_value(text);
            if (!value)
            {
                reader.fail(fmt::format(
                    "cannot parse '{}' as a time; it is not ISO 8601 nor any registered format",
                    text));
            }
            return *value;
        }

        [[nodiscard]] Instant json_instant(
            std::string_view text, Reader &reader)
        {
            using SysMicros = JsonSysTime<std::chrono::microseconds>;
            auto value = parse_json_datetime_value<SysMicros>(text);
            if (!value)
            {
                reader.fail(fmt::format(
                    "cannot parse '{}' as a datetime; it is not ISO 8601 nor any registered format",
                    text));
            }
            return Instant{value->time_since_epoch()};
        }

        [[nodiscard]] Date parse_date_body(std::string_view s, std::size_t &i, Reader &reader)
        {
            const auto y = parse_fixed_int(s, i, reader);
            expect_char(s, i, '-', reader);
            const auto m = parse_fixed_int(s, i, reader);
            expect_char(s, i, '-', reader);
            const auto d = parse_fixed_int(s, i, reader);
            return Date{std::chrono::year{static_cast<int>(y)}, std::chrono::month{static_cast<unsigned>(m)},
                        std::chrono::day{static_cast<unsigned>(d)}};
        }

        [[nodiscard]] std::int64_t parse_time_body_micros(std::string_view s, std::size_t &i, Reader &reader)
        {
            const auto h = parse_fixed_int(s, i, reader);
            expect_char(s, i, ':', reader);
            const auto m = parse_fixed_int(s, i, reader);
            expect_char(s, i, ':', reader);
            const auto sec = parse_fixed_int(s, i, reader);
            std::int64_t micros = 0;
            if (i < s.size() && s[i] == '.')
            {
                ++i;
                const std::size_t start = i;
                micros                  = parse_fixed_int(s, i, reader);
                if (i - start > 6)
                {
                    reader.fail(
                        "time fractions support at most six digits");
                }
                for (std::size_t digits = i - start; digits < 6; ++digits) { micros *= 10; }
            }
            if (h > 23 || m > 59 || sec > 59)
            {
                reader.fail("invalid time fields");
            }
            return ((h * 60 + m) * 60 + sec) * 1'000'000 + micros;
        }

        [[nodiscard]] CivilDateTime parse_civil_datetime(
            std::string_view text, Reader &reader, bool instant_syntax)
        {
            std::size_t position = 0;
            const CivilDate date = parse_date_body(text, position, reader);
            if (position >= text.size() ||
                (text[position] != 'T' && text[position] != ' '))
            {
                reader.fail("bad date/time separator");
            }
            ++position;
            const auto micros = parse_time_body_micros(text, position, reader);
            // Version-2 instants end in Z.  Version-1 recordings used a
            // space separator and no suffix; accept both on ingest and
            // normalize through the native Instant representation.
            if (instant_syntax && position < text.size() &&
                text[position] == 'Z')
            {
                ++position;
            }
            if (position != text.size())
            {
                reader.fail("trailing civil datetime content");
            }
            return CivilDateTime{date, CivilTime{micros}};
        }

        [[nodiscard]] Duration parse_json_duration(
            std::string_view text, Reader &reader)
        {
            if (text.ends_with("us"))
            {
                return parse_duration(text);
            }

            // Legacy hgraph JSON used
            // ``days:hours:minutes:seconds.microseconds``. Python producers
            // normalize a negative value into a signed day plus a
            // non-negative remainder. Early native writers instead used
            // signed, truncation-based components; accept both shapes.
            std::array<std::string_view, 4> components{};
            std::size_t component_start = 0;
            for (std::size_t index = 0; index < 3; ++index)
            {
                const std::size_t separator =
                    text.find(':', component_start);
                if (separator == std::string_view::npos ||
                    separator == component_start)
                {
                    reader.fail("invalid duration");
                }
                components[index] =
                    text.substr(component_start,
                                separator - component_start);
                component_start = separator + 1;
            }
            components[3] = text.substr(component_start);
            if (components[3].empty() ||
                components[3].find(':') != std::string_view::npos)
            {
                reader.fail("invalid duration");
            }

            const auto parse_component =
                [&](std::string_view component,
                    std::string_view label) {
                    bool positive_sign = false;
                    if (component.starts_with('+'))
                    {
                        positive_sign = true;
                        component.remove_prefix(1);
                    }
                    if (component.empty())
                    {
                        reader.fail(
                            std::string{"invalid duration "} +
                            std::string{label});
                    }
                    std::int64_t result{};
                    const auto [end, error] = std::from_chars(
                        component.data(),
                        component.data() + component.size(), result);
                    if (error != std::errc{} ||
                        end != component.data() + component.size() ||
                        (positive_sign && result < 0))
                    {
                        reader.fail(
                            std::string{"invalid duration "} +
                            std::string{label});
                    }
                    return result;
                };

            const std::int64_t days =
                parse_component(components[0], "day field");
            const std::int64_t hours =
                parse_component(components[1], "hour field");
            const std::int64_t minutes =
                parse_component(components[2], "minute field");

            std::int64_t seconds{};
            std::int64_t micros{};
            const std::size_t decimal = components[3].find('.');
            if (decimal == std::string_view::npos)
            {
                seconds = parse_component(
                    components[3], "second field");
            }
            else
            {
                seconds = parse_component(
                    components[3].substr(0, decimal),
                    "second field");
                std::string_view fraction =
                    components[3].substr(decimal + 1);
                if (fraction.empty())
                {
                    reader.fail("invalid duration fraction");
                }
                const bool signed_fraction =
                    fraction.front() == '-' ||
                    fraction.front() == '+';
                micros = parse_component(
                    fraction, "fraction");
                const std::size_t digits =
                    fraction.size() - (signed_fraction ? 1U : 0U);
                if (digits > 6)
                {
                    reader.fail(
                        "duration fractions support at most six digits");
                }
                // Normal v1 text is a decimal fraction and is padded on the
                // right. Early native v1 writers could emit a signed
                // microsecond remainder such as ``.-00001``; preserve that
                // exact integer interpretation as a compatibility path.
                if (!signed_fraction)
                {
                    for (std::size_t index = digits; index < 6; ++index)
                    {
                        micros *= 10;
                    }
                }
            }

            constexpr auto minimum =
                std::numeric_limits<std::int64_t>::min();
            constexpr auto maximum =
                std::numeric_limits<std::int64_t>::max();
            std::int64_t result = 0;
            const auto accumulate =
                [&](std::int64_t component,
                    std::int64_t scale) {
                    if (component < minimum / scale ||
                        component > maximum / scale)
                    {
                        reader.fail("duration overflow");
                    }
                    const std::int64_t value = component * scale;
                    if ((value > 0 && result > maximum - value) ||
                        (value < 0 && result < minimum - value))
                    {
                        reader.fail("duration overflow");
                    }
                    result += value;
                };
            accumulate(days, 86'400'000'000);
            accumulate(hours, 3'600'000'000);
            accumulate(minutes, 60'000'000);
            accumulate(seconds, 1'000'000);
            accumulate(micros, 1);
            return Duration{result};
        }

        [[nodiscard]] Boundary parse_boundary(std::string_view value,
                                              Reader &reader)
        {
            if (value == "open") { return Boundary::Open; }
            if (value == "closed") { return Boundary::Closed; }
            reader.fail("range boundary must be 'open' or 'closed'");
        }

        void write_boundary(Boundary value, std::string &out)
        {
            append_escaped(
                value == Boundary::Closed ? "closed" : "open", out);
        }

        template <typename Range, typename Format>
        void write_range(const Range &range, Format &&format,
                         std::string &out)
        {
            if (range.empty())
            {
                out += "{\"empty\": true}";
                return;
            }
            out += "{\"start\": ";
            if (range.lower_bounded())
            {
                append_escaped(format(range.lower_value()), out);
            }
            else { out += "null"; }
            out += ", \"end\": ";
            if (range.upper_bounded())
            {
                append_escaped(format(range.upper_value()), out);
            }
            else { out += "null"; }
            out += ", \"lower\": ";
            write_boundary(range.lower_boundary(), out);
            out += ", \"upper\": ";
            write_boundary(range.upper_boundary(), out);
            out.push_back('}');
        }

        template <typename Range, typename Parse>
        [[nodiscard]] Range read_range(Reader &reader, Parse &&parse)
        {
            reader.expect('{');
            const std::string first_key = reader.parse_string();
            reader.expect(':');
            if (first_key == "empty")
            {
                if (!reader.consume_keyword("true"))
                {
                    reader.fail("empty range marker must be true");
                }
                reader.expect('}');
                return Range::make_empty();
            }
            if (first_key != "start")
            {
                reader.fail("range object must begin with 'start'");
            }
            using Endpoint = typename Range::value_type;
            std::optional<Endpoint> start;
            if (!reader.consume_keyword("null"))
            {
                start = parse(reader.parse_string(), reader);
            }
            reader.expect(',');
            if (reader.parse_string() != "end")
            {
                reader.fail("range object requires 'end'");
            }
            reader.expect(':');
            std::optional<Endpoint> end;
            if (!reader.consume_keyword("null"))
            {
                end = parse(reader.parse_string(), reader);
            }
            reader.expect(',');
            if (reader.parse_string() != "lower")
            {
                reader.fail("range object requires 'lower'");
            }
            reader.expect(':');
            const Boundary lower =
                parse_boundary(reader.parse_string(), reader);
            reader.expect(',');
            if (reader.parse_string() != "upper")
            {
                reader.fail("range object requires 'upper'");
            }
            reader.expect(':');
            const Boundary upper =
                parse_boundary(reader.parse_string(), reader);
            reader.expect('}');
            if (start && end)
            {
                return Range::bounded(*start, *end, lower, upper);
            }
            if (start) { return Range::from(*start, lower); }
            if (end) { return Range::until(*end, upper); }
            return Range::all();
        }
    }  // namespace json_detail

    void register_json_datetime_format(std::string format, bool time_only)
    {
        if (format.empty())
        {
            throw std::invalid_argument(
                "register_json_datetime_format: format must not be empty");
        }
        auto &registry = registered_datetime_formats();
        std::lock_guard lock{registry.mutex};
        const auto &current = registry.formats;
        const auto &formats =
            time_only ? current->time : current->datetime;
        if (std::ranges::find(formats, format) != formats.end())
        {
            return;
        }
        auto next = std::make_shared<RegisteredJsonDateTimeFormats>(*current);
        (time_only ? next->time : next->datetime).push_back(format);
        registry.formats = std::move(next);
    }

    namespace
    {
        using json_detail::Reader;
        using AtomicTag = JsonConverter::AtomicTag;

        [[nodiscard]] const TimeZoneProvider &codec_time_zone_provider()
        {
            static const auto provider = make_time_zone_provider();
            return *provider;
        }

        [[nodiscard]] bool structured_map_key(AtomicTag tag) noexcept
        {
            switch (tag)
            {
            case AtomicTag::Period:
            case AtomicTag::InstantRange:
            case AtomicTag::CivilDateRange:
            case AtomicTag::InstantRangeSet:
            case AtomicTag::CivilDateRangeSet:
                return true;
            default:
                return false;
            }
        }

        // ---------------------------------------------------------------
        // Write thunks
        // ---------------------------------------------------------------

        void write_enum(const JsonConverter &self, const ValueView &view, std::string &out)
        {
            static_cast<void>(self);
            // The member NAME as a JSON string (the enum ops' to_string).
            json_detail::append_escaped(view.to_string(), out);
        }

        Value read_enum(const JsonConverter &self, json_detail::Reader &reader)
        {
            const std::string name = reader.parse_string();
            const auto       *meta = self.meta;
            for (std::size_t index = 0; index < meta->field_count; ++index)
            {
                if (meta->fields[index].name != nullptr && name == meta->fields[index].name)
                {
                    Value out{self.binding};
                    // The enum payload IS the assigned integer (the Int plan).
                    *static_cast<Int *>(const_cast<void *>(out.view().data())) = meta->fields[index].enum_value;
                    return out;
                }
            }
            reader.fail(fmt::format("unknown member '{}' for enum '{}'", name,
                                    meta->name()));
        }

        void write_atomic(const JsonConverter &self, const ValueView &view, std::string &out)
        {
            switch (self.atomic_tag)
            {
                case AtomicTag::Bool: out += view.checked_as<Bool>() ? "true" : "false"; return;
                case AtomicTag::Int: out += fmt::format("{}", view.checked_as<Int>()); return;
                case AtomicTag::Float: out += fmt::format("{}", view.checked_as<Float>()); return;
                case AtomicTag::Str: json_detail::append_escaped(view.checked_as<Str>(), out); return;
                case AtomicTag::Date: {
                    out.push_back('"');
                    json_detail::append_date(view.checked_as<Date>(), out);
                    out.push_back('"');
                    return;
                }
                case AtomicTag::DateTime: {
                    json_detail::append_escaped(
                        format_instant(view.checked_as<Instant>()), out);
                    return;
                }
                case AtomicTag::TimeDelta: {
                    json_detail::append_escaped(
                        format_duration(view.checked_as<Duration>()), out);
                    return;
                }
                case AtomicTag::Time: {
                    out.push_back('"');
                    json_detail::append_time_of_day(view.checked_as<Time>().microseconds, out);
                    out.push_back('"');
                    return;
                }
                case AtomicTag::CivilDateTime:
                    json_detail::append_escaped(
                        format_civil_datetime(
                            view.checked_as<CivilDateTime>()),
                        out);
                    return;
                case AtomicTag::Period: {
                    const Period value = view.checked_as<Period>();
                    out += fmt::format(
                        "{{\"months\": {}, \"days\": {}}}",
                        value.total_months(), value.days());
                    return;
                }
                case AtomicTag::ZoneId:
                    json_detail::append_escaped(
                        view.checked_as<ZoneId>().name(), out);
                    return;
                case AtomicTag::ZonedDateTime: {
                    std::ostringstream text;
                    text << view.checked_as<ZonedDateTime>();
                    json_detail::append_escaped(text.str(), out);
                    return;
                }
                case AtomicTag::InstantRange:
                    json_detail::write_range(
                        view.checked_as<InstantRange>(),
                        [](Instant value) { return format_instant(value); },
                        out);
                    return;
                case AtomicTag::CivilDateRange:
                    json_detail::write_range(
                        view.checked_as<CivilDateRange>(),
                        [](CivilDate value) {
                            return format_civil_date(value);
                        },
                        out);
                    return;
                case AtomicTag::InstantRangeSet: {
                    out.push_back('[');
                    bool first = true;
                    for (const auto &range :
                         view.checked_as<InstantRangeSet>())
                    {
                        if (!std::exchange(first, false)) { out += ", "; }
                        json_detail::write_range(
                            range,
                            [](Instant value) {
                                return format_instant(value);
                            },
                            out);
                    }
                    out.push_back(']');
                    return;
                }
                case AtomicTag::CivilDateRangeSet: {
                    out.push_back('[');
                    bool first = true;
                    for (const auto &range :
                         view.checked_as<CivilDateRangeSet>())
                    {
                        if (!std::exchange(first, false)) { out += ", "; }
                        json_detail::write_range(
                            range,
                            [](CivilDate value) {
                                return format_civil_date(value);
                            },
                            out);
                    }
                    out.push_back(']');
                    return;
                }
                case AtomicTag::None: break;
            }
            throw std::logic_error("json: unsupported atomic write");
        }

        void write_composite(const JsonConverter &self, const ValueView &view, std::string &out)
        {
            // Bundle -> object; (un-named) tuple without field names -> array.
            const auto concrete = view.concrete();
            const JsonConverter *selected = &self;
            bool polymorphic = false;
            if (self.meta->is_named_bundle())
            {
                const auto *snapshot = active_type_realization();
                polymorphic = view.binding() != self.binding ||
                              (snapshot != nullptr && snapshot->is_polymorphic(self.meta));
                if (polymorphic)
                {
                    if (!TypeRegistry::instance().bundle_is_a(concrete.schema(), self.meta))
                    {
                        throw std::logic_error("json: polymorphic Bundle contains an invalid concrete type");
                    }
                    selected = &json_converter(concrete.schema());
                }
            }

            const auto indexed  = concrete.as_indexed_view();
            const bool as_array = selected->names.empty();
            out.push_back(as_array ? '[' : '{');
            bool first = true;
            if (polymorphic)
            {
                json_detail::append_escaped(self.meta->bundle_discriminator(), out);
                out += ": ";
                json_detail::append_escaped(concrete.schema()->name(), out);
                first = false;
            }
            for (std::size_t i = 0; i < selected->children.size(); ++i)
            {
                const auto child = indexed.at(i);
                if (!as_array && !child.has_value()) { continue; }
                if (!std::exchange(first, false)) { out += ", "; }
                if (!as_array)
                {
                    json_detail::append_escaped(selected->names[i], out);
                    out += ": ";
                }
                if (child.has_value()) { selected->children[i]->write(child, out); }
                else { out += "null"; }
            }
            out.push_back(as_array ? ']' : '}');
        }

        void write_owned(const JsonConverter &self, const ValueView &view, std::string &out)
        {
            const auto concrete = view.concrete();
            if (concrete.schema() == self.meta)
            {
                out += "null";
                return;
            }
            self.children[0]->write(concrete, out);
        }

        void write_sequence(const JsonConverter &self, const ValueView &view, std::string &out)
        {
            out.push_back('[');
            bool first = true;
            if (view.schema()->value_kind() == ValueTypeKind::List)
            {
                const auto list = view.as_list();
                for (std::size_t i = 0; i < list.size(); ++i)
                {
                    if (!std::exchange(first, false)) { out += ", "; }
                    self.children[0]->write(list.at(i), out);
                }
            }
            else
            {
                const auto set = view.as_set();
                for (const auto element : set)
                {
                    if (!std::exchange(first, false)) { out += ", "; }
                    self.children[0]->write(element, out);
                }
            }
            out.push_back(']');
        }

        void write_map(const JsonConverter &self, const ValueView &view, std::string &out)
        {
            const auto map = view.as_map();
            out.push_back('{');
            bool first = true;
            for (const auto [key, value] : map)
            {
                if (!std::exchange(first, false)) { out += ", "; }
                // A string-rendered key is used directly; other keys render
                // their token and are wrapped in quotes (the Python rule).
                std::string key_text;
                self.children[0]->write(key, key_text);
                if (!key_text.empty() && key_text.front() == '"') { out += key_text; }
                else { json_detail::append_escaped(key_text, out); }
                out += ": ";
                // An UNSET entry (a None-valued mapping value) is JSON null.
                if (!value.has_value()) { out += "null"; }
                else { self.children[1]->write(value, out); }
            }
            out.push_back('}');
        }

        // ---------------------------------------------------------------
        // Read thunks
        // ---------------------------------------------------------------

        Value read_atomic(const JsonConverter &self, Reader &reader)
        {
            switch (self.atomic_tag)
            {
                case AtomicTag::Bool: {
                    if (reader.consume_keyword("true")) { return Value{Bool{true}}; }
                    if (reader.consume_keyword("false")) { return Value{Bool{false}}; }
                    reader.fail("expected a boolean");
                }
                case AtomicTag::Int: return Value{json_detail::parse_int_token(reader.parse_number_token(), reader)};
                case AtomicTag::Float:
                    return Value{json_detail::parse_float_token(reader.parse_number_token(), reader)};
                case AtomicTag::Str: return Value{Str{reader.parse_string()}};
                case AtomicTag::Date: {
                    return Value{json_detail::json_date(
                        reader.parse_string(), reader)};
                }
                case AtomicTag::DateTime: {
                    return Value{json_detail::json_instant(
                        reader.parse_string(), reader)};
                }
                case AtomicTag::TimeDelta: {
                    return Value{
                        json_detail::parse_json_duration(
                            reader.parse_string(), reader)};
                }
                case AtomicTag::Time: {
                    return Value{json_detail::json_time(
                        reader.parse_string(), reader)};
                }
                case AtomicTag::CivilDateTime:
                    return Value{json_detail::parse_civil_datetime(
                        reader.parse_string(), reader, false)};
                case AtomicTag::Period: {
                    reader.expect('{');
                    if (reader.parse_string() != "months")
                    {
                        reader.fail("period object requires 'months'");
                    }
                    reader.expect(':');
                    const auto months = json_detail::parse_int_token(
                        reader.parse_number_token(), reader);
                    reader.expect(',');
                    if (reader.parse_string() != "days")
                    {
                        reader.fail("period object requires 'days'");
                    }
                    reader.expect(':');
                    const auto days = json_detail::parse_int_token(
                        reader.parse_number_token(), reader);
                    reader.expect('}');
                    return Value{Period{0, months, days}};
                }
                case AtomicTag::ZoneId:
                    return Value{ZoneId{reader.parse_string()}};
                case AtomicTag::ZonedDateTime: {
                    const std::string text = reader.parse_string();
                    const auto bracket = text.find('[');
                    if (bracket == std::string::npos ||
                        text.empty() || text.back() != ']')
                    {
                        reader.fail("invalid zoned datetime");
                    }
                    const auto sign_position =
                        text.find_last_of("+-", bracket);
                    if (sign_position == std::string::npos ||
                        sign_position + 6 != bracket ||
                        text[sign_position + 3] != ':')
                    {
                        reader.fail("invalid zoned datetime offset");
                    }
                    const CivilDateTime local =
                        json_detail::parse_civil_datetime(
                            std::string_view{text}.substr(0, sign_position),
                            reader, false);
                    const auto parse_two = [&](std::size_t position) {
                        const char first = text[position];
                        const char second = text[position + 1];
                        if (first < '0' || first > '9' ||
                            second < '0' || second > '9')
                        {
                            reader.fail("invalid zoned datetime offset");
                        }
                        return (first - '0') * 10 + second - '0';
                    };
                    const int magnitude =
                        parse_two(sign_position + 1) * 3600 +
                        parse_two(sign_position + 4) * 60;
                    const int offset =
                        text[sign_position] == '-' ? -magnitude : magnitude;
                    const ZoneId zone{
                        std::string_view{text}.substr(
                            bracket + 1, text.size() - bracket - 2)};
                    const Instant instant = checked_subtract(
                        Instant{Duration{local.epoch_microseconds()}},
                        Duration{static_cast<std::int64_t>(offset) *
                                 1'000'000});
                    const ZonedDateTime verified =
                        at_zone(instant, zone,
                                codec_time_zone_provider());
                    if (verified.offset_seconds() != offset)
                    {
                        reader.fail(
                            "zoned datetime offset disagrees with provider");
                    }
                    return Value{verified};
                }
                case AtomicTag::InstantRange:
                    return Value{json_detail::read_range<InstantRange>(
                        reader,
                        [](std::string_view value, Reader &nested) {
                            const CivilDateTime parsed =
                                json_detail::parse_civil_datetime(
                                    value, nested, true);
                            return Instant{
                                Duration{parsed.epoch_microseconds()}};
                        })};
                case AtomicTag::CivilDateRange:
                    return Value{json_detail::read_range<CivilDateRange>(
                        reader,
                        [](std::string_view value, Reader &nested) {
                            std::size_t position = 0;
                            const CivilDate parsed =
                                json_detail::parse_date_body(
                                    value, position, nested);
                            if (position != value.size())
                            {
                                nested.fail("trailing date content");
                            }
                            return parsed;
                        })};
                case AtomicTag::InstantRangeSet: {
                    std::array<InstantRange, 2> ranges{};
                    std::size_t size = 0;
                    reader.expect('[');
                    if (!reader.consume_if(']'))
                    {
                        while (true)
                        {
                            if (size == ranges.size())
                            {
                                reader.fail(
                                    "instant range set exceeds capacity");
                            }
                            ranges[size++] =
                                json_detail::read_range<InstantRange>(
                                    reader,
                                    [](std::string_view value,
                                       Reader &nested) {
                                        const auto parsed =
                                            json_detail::parse_civil_datetime(
                                                value, nested, true);
                                        return Instant{Duration{
                                            parsed.epoch_microseconds()}};
                                    });
                            if (!reader.consume_if(',')) { break; }
                        }
                        reader.expect(']');
                    }
                    return Value{InstantRangeSet{
                        std::span<const InstantRange>{ranges.data(), size}}};
                }
                case AtomicTag::CivilDateRangeSet: {
                    std::array<CivilDateRange, 2> ranges{};
                    std::size_t size = 0;
                    reader.expect('[');
                    if (!reader.consume_if(']'))
                    {
                        while (true)
                        {
                            if (size == ranges.size())
                            {
                                reader.fail(
                                    "civil date range set exceeds capacity");
                            }
                            ranges[size++] =
                                json_detail::read_range<CivilDateRange>(
                                    reader,
                                    [](std::string_view value,
                                       Reader &nested) {
                                        std::size_t position = 0;
                                        const auto parsed =
                                            json_detail::parse_date_body(
                                                value, position, nested);
                                        if (position != value.size())
                                        {
                                            nested.fail(
                                                "trailing date content");
                                        }
                                        return parsed;
                                    });
                            if (!reader.consume_if(',')) { break; }
                        }
                        reader.expect(']');
                    }
                    return Value{CivilDateRangeSet{
                        std::span<const CivilDateRange>{ranges.data(), size}}};
                }
                case AtomicTag::None: break;
            }
            throw std::logic_error("json: unsupported atomic read");
        }

        Value read_realized(const JsonConverter &converter, Reader &reader)
        {
            const auto *snapshot = active_type_realization();
            if (snapshot == nullptr || !snapshot->is_polymorphic(converter.meta))
            {
                return converter.read_(converter, reader);
            }

            Reader probe = reader;
            probe.expect('{');
            const std::string discriminator{converter.meta->bundle_discriminator()};
            std::string       requested;
            if (!probe.consume_if('}'))
            {
                while (true)
                {
                    const std::string key = probe.parse_string();
                    probe.expect(':');
                    if (key == discriminator) { requested = probe.parse_string(); }
                    else { probe.skip_value(); }
                    if (!probe.consume_if(',')) { break; }
                }
                probe.expect('}');
            }
            if (requested.empty())
            {
                probe.fail("polymorphic Bundle object requires its configured type discriminator");
            }

            const ValueTypeMetaData *selected = nullptr;
            for (const auto *alternative : snapshot->alternatives(converter.meta))
            {
                if (alternative->name() == requested || alternative->bundle_local_name() == requested)
                {
                    if (selected != nullptr) { probe.fail("polymorphic Bundle discriminator is ambiguous"); }
                    selected = alternative;
                }
            }
            if (selected == nullptr)
            {
                probe.fail("polymorphic Bundle discriminator names no valid alternative");
            }

            // Read the selected alternative exactly. Its own children still
            // use read_realized, but an instantiable parent remains a valid
            // concrete alternative even when it also has descendants.
            const auto &selected_converter = json_converter(selected);
            Value       concrete            = selected_converter.read_(selected_converter, reader);
            const auto  realized            = snapshot->type_for(converter.meta);
            Value       result{realized};
            auto        destination = result.begin_mutation();
            realized.ops_ref().copy_assign_from(
                realized, destination.mutable_data(), concrete.binding(), concrete.view().data());
            return result;
        }

        Value read_composite(const JsonConverter &self, Reader &reader)
        {
            auto binding = self.binding;
            if (const auto *snapshot = active_type_realization();
                snapshot != nullptr && !snapshot->is_polymorphic(self.meta))
            {
                binding = snapshot->type_for(self.meta);
            }
            BundleBuilder builder{binding};
            if (self.names.empty())
            {
                reader.expect('[');
                for (std::size_t i = 0; i < self.children.size(); ++i)
                {
                    if (i != 0) { reader.expect(','); }
                    if (reader.consume_keyword("null"))
                    {
                        if (self.meta->value_kind() != ValueTypeKind::Bundle)
                        {
                            reader.fail("null composite field is only supported for Bundle values");
                        }
                        continue;
                    }
                    builder.set(i, read_realized(*self.children[i], reader));
                }
                reader.expect(']');
            }
            else
            {
                reader.expect('{');
                if (!reader.consume_if('}'))
                {
                    while (true)
                    {
                        const std::string key = reader.parse_string();
                        reader.expect(':');
                        std::size_t index = self.names.size();
                        for (std::size_t i = 0; i < self.names.size(); ++i)
                        {
                            if (self.names[i] == key)
                            {
                                index = i;
                                break;
                            }
                        }
                        if (index == self.names.size()) { reader.skip_value(); }
                        else if (reader.consume_keyword("null")) {}
                        else { builder.set(index, read_realized(*self.children[index], reader)); }
                        if (!reader.consume_if(',')) { break; }
                    }
                    reader.expect('}');
                }
            }
            return builder.build();
        }

        Value read_owned(const JsonConverter &self, Reader &reader)
        {
            Value result{self.binding};
            if (reader.consume_keyword("null")) { return result; }

            Value pointee = read_realized(*self.children[0], reader);
            auto destination = result.begin_mutation();
            self.binding.ops_ref().copy_assign_from(
                self.binding, destination.mutable_data(), pointee.binding(), pointee.view().data());
            return result;
        }

        [[nodiscard]] ValueTypeRef realized_read_binding(const JsonConverter &converter)
        {
            if (const auto *snapshot = active_type_realization(); snapshot != nullptr)
            {
                return snapshot->type_for(converter.meta);
            }
            return converter.binding;
        }

        Value read_list(const JsonConverter &self, Reader &reader)
        {
            ListBuilder builder{
                realized_read_binding(*self.children[0]), *self.meta};
            reader.expect('[');
            if (!reader.consume_if(']'))
            {
                while (true)
                {
                    Value element = self.children[0]->read(reader);
                    builder.push_back(element.view());
                    if (!reader.consume_if(',')) { break; }
                }
                reader.expect(']');
            }
            return builder.build();
        }

        Value read_set(const JsonConverter &self, Reader &reader)
        {
            SetBuilder builder{realized_read_binding(*self.children[0])};
            reader.expect('[');
            if (!reader.consume_if(']'))
            {
                while (true)
                {
                    Value element = self.children[0]->read(reader);
                    (void)builder.insert(element.view());
                    if (!reader.consume_if(',')) { break; }
                }
                reader.expect(']');
            }
            return builder.build();
        }

        Value read_map(const JsonConverter &self, Reader &reader)
        {
            MapBuilder builder{
                realized_read_binding(*self.children[0]),
                realized_read_binding(*self.children[1])};
            reader.expect('{');
            if (!reader.consume_if('}'))
            {
                while (true)
                {
                    // The key arrives as a JSON string; string-tagged keys use
                    // its content, other keys parse the content as their token.
                    const std::string key_text = reader.parse_string();
                    Value             key;
                    if (self.children[0]->atomic_tag == AtomicTag::Str) { key = Value{Str{key_text}}; }
                    else
                    {
                        Reader key_reader{std::string_view{key_text}};
                        // Quoted forms (dates etc.) arrive without their quotes;
                        // re-wrap so the atomic reader sees its expected shape.
                        std::string requoted;
                        if (!structured_map_key(
                                self.children[0]->atomic_tag) &&
                            self.children[0]->atomic_tag != AtomicTag::Int &&
                            self.children[0]->atomic_tag != AtomicTag::Float &&
                            self.children[0]->atomic_tag != AtomicTag::Bool)
                        {
                            requoted   = fmt::format("\"{}\"", key_text);
                            key_reader = Reader{std::string_view{requoted}};
                            key        = self.children[0]->read(key_reader);
                        }
                        else { key = self.children[0]->read(key_reader); }
                    }
                    reader.expect(':');
                    if (reader.consume_keyword("null"))
                    {
                        // JSON null = an unset entry (a None-valued mapping
                        // value; element validity).
                        builder.set_item_unset(key.view());
                    }
                    else
                    {
                        Value value = self.children[1]->read(reader);
                        builder.set_item(key.view(), value.view());
                    }
                    if (!reader.consume_if(',')) { break; }
                }
                reader.expect('}');
            }
            return builder.build();
        }

        // ---------------------------------------------------------------
        // Synthesis + interning (cleared on registry reset)
        // ---------------------------------------------------------------

        // Converter synthesis normally happens during wiring/start. Multiple
        // independent graph engines may start concurrently, so protect the
        // process-wide interning table. A recursive mutex is required because
        // compound converter synthesis recursively interns child schemas.
        std::recursive_mutex g_converters_mutex;
        std::unordered_map<const ValueTypeMetaData *, std::unique_ptr<JsonConverter>> g_converters;

        [[nodiscard]] AtomicTag atomic_tag_for(const ValueTypeMetaData *meta)
        {
            if (meta == scalar_descriptor<Bool>::value_meta()) { return AtomicTag::Bool; }
            if (meta == scalar_descriptor<Int>::value_meta()) { return AtomicTag::Int; }
            if (meta == scalar_descriptor<Float>::value_meta()) { return AtomicTag::Float; }
            if (meta == scalar_descriptor<Str>::value_meta()) { return AtomicTag::Str; }
            if (meta == scalar_descriptor<Date>::value_meta()) { return AtomicTag::Date; }
            if (meta == scalar_descriptor<DateTime>::value_meta()) { return AtomicTag::DateTime; }
            if (meta == scalar_descriptor<TimeDelta>::value_meta()) { return AtomicTag::TimeDelta; }
            if (meta == scalar_descriptor<Time>::value_meta()) { return AtomicTag::Time; }
            if (meta == scalar_descriptor<CivilDateTime>::value_meta())
            {
                return AtomicTag::CivilDateTime;
            }
            if (meta == scalar_descriptor<Period>::value_meta())
            {
                return AtomicTag::Period;
            }
            if (meta == scalar_descriptor<ZoneId>::value_meta())
            {
                return AtomicTag::ZoneId;
            }
            if (meta == scalar_descriptor<ZonedDateTime>::value_meta())
            {
                return AtomicTag::ZonedDateTime;
            }
            if (meta == scalar_descriptor<InstantRange>::value_meta())
            {
                return AtomicTag::InstantRange;
            }
            if (meta == scalar_descriptor<CivilDateRange>::value_meta())
            {
                return AtomicTag::CivilDateRange;
            }
            if (meta == scalar_descriptor<InstantRangeSet>::value_meta())
            {
                return AtomicTag::InstantRangeSet;
            }
            if (meta == scalar_descriptor<CivilDateRangeSet>::value_meta())
            {
                return AtomicTag::CivilDateRangeSet;
            }
            return AtomicTag::None;
        }

        const JsonConverter *build_converter(const ValueTypeMetaData *meta);

        const JsonConverter *converter_for_locked(const ValueTypeMetaData *meta)
        {
            if (const auto it = g_converters.find(meta); it != g_converters.end()) { return it->second.get(); }
            return build_converter(meta);
        }

        const JsonConverter *build_converter(const ValueTypeMetaData *meta)
        {
            if (meta == nullptr) { throw std::logic_error("json: null value schema"); }

            auto converter     = std::make_unique<JsonConverter>();
            converter->meta    = meta;
            converter->binding = ValuePlanFactory::instance().type_for(meta);
            auto *raw          = converter.get();
            // Insert before recursing so self-referential schemas terminate;
            // the guard removes the half-built entry if synthesis throws.
            g_converters.emplace(meta, std::move(converter));
            auto unwind = UnwindCleanupGuard([&] { g_converters.erase(meta); });

            if (meta->is_owned())
            {
                raw->children.push_back(converter_for_locked(meta->element_type));
                raw->write_ = &write_owned;
                raw->read_ = &read_owned;
                unwind.release();
                return raw;
            }

            switch (meta->value_kind())
            {
                case ValueTypeKind::Atomic: {
                    if (meta->is_enum())
                    {
                        raw->write_ = &write_enum;
                        raw->read_  = &read_enum;
                        break;
                    }
                    raw->atomic_tag = atomic_tag_for(meta);
                    if (raw->atomic_tag == AtomicTag::None)
                    {
                        throw std::logic_error(fmt::format("json: unsupported atomic scalar '{}'",
                                                           meta->name()));
                    }
                    raw->write_ = &write_atomic;
                    raw->read_  = &read_atomic;
                    break;
                }
                case ValueTypeKind::Tuple:
                case ValueTypeKind::Bundle: {
                    for (std::size_t i = 0; i < meta->field_count; ++i)
                    {
                        raw->children.push_back(converter_for_locked(meta->fields[i].type));
                        if (meta->fields[i].name != nullptr) { raw->names.emplace_back(meta->fields[i].name); }
                    }
                    if (!raw->names.empty() && raw->names.size() != raw->children.size())
                    {
                        throw std::logic_error("json: partially-named composite is not supported");
                    }
                    raw->write_ = &write_composite;
                    raw->read_  = &read_composite;
                    break;
                }
                case ValueTypeKind::List: {
                    raw->children.push_back(converter_for_locked(meta->element_type));
                    raw->write_ = &write_sequence;
                    raw->read_  = &read_list;
                    break;
                }
                case ValueTypeKind::Set: {
                    raw->children.push_back(converter_for_locked(meta->element_type));
                    raw->write_ = &write_sequence;
                    raw->read_  = &read_set;
                    break;
                }
                case ValueTypeKind::Map: {
                    raw->children.push_back(converter_for_locked(meta->key_type));
                    raw->children.push_back(converter_for_locked(meta->element_type));
                    raw->write_ = &write_map;
                    raw->read_  = &read_map;
                    break;
                }
                default:
                    throw std::logic_error(fmt::format("json: unsupported value kind for '{}'",
                                                       meta->name()));
            }
            unwind.release();
            return raw;
        }
    }  // namespace

    Value JsonConverter::read(json_detail::Reader &reader) const { return read_realized(*this, reader); }

    const JsonConverter &json_converter(const ValueTypeMetaData *meta)
    {
        // Composed + interned once per schema. Per-tick operator paths do NOT
        // call this: nodes resolve their converter in ``start`` and carry it
        // in node State (the lifecycle "compose once" contract); this lookup
        // serves wiring/start-time resolution and ad-hoc utility use.
        std::scoped_lock lock{g_converters_mutex};
        return *converter_for_locked(meta);
    }

    void clear_json_converters() noexcept
    {
        std::scoped_lock lock{g_converters_mutex};
        g_converters.clear();
    }

    std::string to_json_string(const ValueView &view)
    {
        if (!view.valid()) { return "null"; }
        std::string out;
        json_converter(view.schema()).write(view, out);
        return out;
    }

    Value from_json_string(const JsonConverter &converter, std::string_view text)
    {
        json_detail::Reader reader{text};
        Value               result = converter.read(reader);
        reader.skip_ws();
        if (reader.pos != text.size()) { reader.fail("trailing content"); }
        return result;
    }

    Value from_json_string(const ValueTypeMetaData *meta, std::string_view text)
    {
        return from_json_string(json_converter(meta), text);
    }

    namespace json_fragment
    {
        bool consume(Cursor &cursor, char token)
        {
            json_detail::Reader reader{cursor.text, cursor.offset};
            reader.skip_ws();
            if (reader.pos < cursor.text.size() && cursor.text[reader.pos] == token)
            {
                cursor.offset = reader.pos + 1;
                return true;
            }
            cursor.offset = reader.pos;
            return false;
        }

        bool consume_null(Cursor &cursor)
        {
            json_detail::Reader reader{cursor.text, cursor.offset};
            reader.skip_ws();
            if (cursor.text.substr(reader.pos, 4) == "null")
            {
                cursor.offset = reader.pos + 4;
                return true;
            }
            cursor.offset = reader.pos;
            return false;
        }

        char peek(Cursor &cursor)
        {
            json_detail::Reader reader{cursor.text, cursor.offset};
            reader.skip_ws();
            cursor.offset = reader.pos;
            return reader.pos < cursor.text.size() ? cursor.text[reader.pos] : '\0';
        }

        std::string parse_string(Cursor &cursor)
        {
            json_detail::Reader reader{cursor.text, cursor.offset};
            std::string         result = reader.parse_string();
            cursor.offset              = reader.pos;
            return result;
        }

        Value parse_value(const JsonConverter &converter, Cursor &cursor)
        {
            json_detail::Reader reader{cursor.text, cursor.offset};
            Value               result = converter.read(reader);
            cursor.offset              = reader.pos;
            return result;
        }

        void fail(Cursor &cursor, std::string_view message)
        {
            json_detail::Reader reader{cursor.text, cursor.offset};
            reader.fail(message);
        }
    }  // namespace json_fragment
}  // namespace hgraph
