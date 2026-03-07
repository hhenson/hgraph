#pragma once

#include <hgraph/types/node.h>
#include <hgraph/types/value/type_registry.h>

#include <datetime.h>
#include <cmath>
#include <cstdint>
#include <cstdio>
#include <limits>
#include <string>
#include <string_view>

namespace hgraph {
    namespace ops {
        namespace date_time_ops_detail {
            inline TSBInputView input_bundle(Node& node) {
                return *node.input().try_as_bundle();
            }

            inline nb::object python_field(const TSBInputView& bundle, std::string_view field_name) {
                return bundle.field(field_name).to_python();
            }

            inline TSInputView input_field(const TSBInputView& bundle, std::string_view field_name) {
                return bundle.field(field_name);
            }

            inline void emit_python(Node& node, const nb::object& value) {
                node.output().from_python(value);
            }

            inline void emit_int(Node& node, int64_t value) {
                node.output().set_value(value::View(&value, value::scalar_type_meta<int64_t>()));
            }

            inline void emit_float(Node& node, double value) {
                node.output().set_value(value::View(&value, value::scalar_type_meta<double>()));
            }

            inline void emit_date(Node& node, engine_date_t value) {
                node.output().set_value(value::View(&value, value::scalar_type_meta<engine_date_t>()));
            }

            inline void emit_datetime(Node& node, engine_time_t value) {
                node.output().set_value(value::View(&value, value::scalar_type_meta<engine_time_t>()));
            }

            inline void emit_timedelta(Node& node, engine_time_delta_t value) {
                node.output().set_value(value::View(&value, value::scalar_type_meta<engine_time_delta_t>()));
            }

            struct time_parts {
                int64_t hour;
                int64_t minute;
                int64_t second;
                int64_t microsecond;
            };

            struct datetime_parts {
                int64_t year;
                int64_t month;
                int64_t day;
                int64_t hour;
                int64_t minute;
                int64_t second;
                int64_t microsecond;
                int64_t weekday;
                int64_t isoweekday;
            };

            constexpr int64_t kUsPerSecond = 1'000'000;
            constexpr int64_t kUsPerDay = 86'400 * kUsPerSecond;

            inline int64_t floor_div_i64(int64_t lhs, int64_t rhs) {
                int64_t q = lhs / rhs;
                const int64_t r = lhs % rhs;
                if (r != 0 && ((r > 0) != (rhs > 0))) {
                    --q;
                }
                return q;
            }

            inline datetime_parts split_datetime(engine_time_t value) {
                const auto day_point = std::chrono::floor<std::chrono::days>(value);
                const engine_date_t ymd{day_point};
                const auto tod = value - day_point;
                const std::chrono::hh_mm_ss<engine_time_delta_t> hms{tod};
                const auto wd = std::chrono::weekday(day_point);
                const int64_t py_weekday = (static_cast<int64_t>(wd.c_encoding()) + 6) % 7;
                return {
                    static_cast<int64_t>(static_cast<int>(ymd.year())),
                    static_cast<int64_t>(static_cast<unsigned>(ymd.month())),
                    static_cast<int64_t>(static_cast<unsigned>(ymd.day())),
                    static_cast<int64_t>(hms.hours().count()),
                    static_cast<int64_t>(hms.minutes().count()),
                    static_cast<int64_t>(hms.seconds().count()),
                    static_cast<int64_t>(hms.subseconds().count()),
                    py_weekday,
                    py_weekday + 1,
                };
            }

            inline int64_t date_weekday(engine_date_t value) {
                const auto wd = std::chrono::weekday(std::chrono::sys_days{value});
                return (static_cast<int64_t>(wd.c_encoding()) + 6) % 7;
            }

            inline void ensure_datetime_capi() {
                static const bool initialized = []() {
                    PyDateTime_IMPORT;
                    return true;
                }();
                (void)initialized;
            }

            inline time_parts split_python_time(const nb::object& value) {
                ensure_datetime_capi();
                if (!PyTime_Check(value.ptr())) {
                    throw std::runtime_error("Expected datetime.time for TS[time] input");
                }
                return {
                    static_cast<int64_t>(PyDateTime_TIME_GET_HOUR(value.ptr())),
                    static_cast<int64_t>(PyDateTime_TIME_GET_MINUTE(value.ptr())),
                    static_cast<int64_t>(PyDateTime_TIME_GET_SECOND(value.ptr())),
                    static_cast<int64_t>(PyDateTime_TIME_GET_MICROSECOND(value.ptr())),
                };
            }

            inline engine_time_t combine_date_time(engine_date_t date, const time_parts& tp) {
                const auto days = std::chrono::sys_days{date};
                const auto micros = std::chrono::hours{tp.hour}
                                  + std::chrono::minutes{tp.minute}
                                  + std::chrono::seconds{tp.second}
                                  + std::chrono::microseconds{tp.microsecond};
                return engine_time_t{
                    std::chrono::duration_cast<engine_time_delta_t>(days.time_since_epoch()) + micros
                };
            }

            inline int64_t timedelta_days(engine_time_delta_t delta) {
                return floor_div_i64(delta.count(), kUsPerDay);
            }

            inline engine_date_t add_days(engine_date_t date, int64_t days) {
                return engine_date_t{std::chrono::sys_days{date} + std::chrono::days{days}};
            }

            struct timedelta_parts {
                int64_t days;
                int64_t seconds;
                int64_t microseconds;
            };

            inline timedelta_parts split_timedelta(engine_time_delta_t delta) {
                const int64_t total_us = delta.count();
                const int64_t days = floor_div_i64(total_us, kUsPerDay);
                const int64_t rem_us = total_us - days * kUsPerDay;
                const int64_t seconds = rem_us / kUsPerSecond;
                const int64_t microseconds = rem_us % kUsPerSecond;
                return {days, seconds, microseconds};
            }

            inline double as_number(const TSInputView& ts) {
                const value::View v = ts.value();
                const value::TypeMeta* tm = v.schema();
                if (tm == value::scalar_type_meta<int64_t>()) {
                    return static_cast<double>(v.as<int64_t>());
                }
                if (tm == value::scalar_type_meta<double>()) {
                    return v.as<double>();
                }
                if (tm == value::scalar_type_meta<bool>()) {
                    return v.as<bool>() ? 1.0 : 0.0;
                }
                throw std::runtime_error("Unsupported NUMBER scalar for timedelta arithmetic in C++ node");
            }
        }  // namespace date_time_ops_detail

        struct ExplodeDateImplSpec {
            static constexpr const char* py_factory_name = "op_explode_date_impl";

            static void eval(Node& node) {
                auto bundle = date_time_ops_detail::input_bundle(node);
                const engine_date_t ts = date_time_ops_detail::input_field(bundle, "ts").value().template as<engine_date_t>();
                const int64_t year = static_cast<int64_t>(static_cast<int>(ts.year()));
                const int64_t month = static_cast<int64_t>(static_cast<unsigned>(ts.month()));
                const int64_t day = static_cast<int64_t>(static_cast<unsigned>(ts.day()));

                auto out = node.output();
                auto out_list = out.as_list();
                if (!out.valid() || !out_list.at(0).valid()) {
                    out_list.at(0).set_value(value::View(&year, value::scalar_type_meta<int64_t>()));
                    out_list.at(1).set_value(value::View(&month, value::scalar_type_meta<int64_t>()));
                    out_list.at(2).set_value(value::View(&day, value::scalar_type_meta<int64_t>()));
                    return;
                }

                const auto maybe_set = [&](size_t idx, int64_t value) {
                    auto child = out_list.at(idx);
                    if (!child.valid() || child.value().template as<int64_t>() != value) {
                        child.set_value(value::View(&value, value::scalar_type_meta<int64_t>()));
                    }
                };
                maybe_set(2, day);
                maybe_set(1, month);
                maybe_set(0, year);
            }
        };

        struct AddDateTimeNodeSpec {
            static constexpr const char* py_factory_name = "op_add_date_time_node";

            struct state {
                nb::object datetime_cls;
                nb::object zoneinfo_cls;
                bool imports_initialized{false};
            };

            static state make_state(Node&) { return {}; }

            static void eval(Node& node, state& state) {
                auto bundle = date_time_ops_detail::input_bundle(node);
                const engine_date_t lhs = date_time_ops_detail::input_field(bundle, "lhs").value().template as<engine_date_t>();
                const nb::object rhs = date_time_ops_detail::python_field(bundle, "rhs");
                const auto rhs_parts = date_time_ops_detail::split_python_time(rhs);

                auto tz_field = bundle.field("tz");
                if (tz_field && tz_field.valid()) {
                    if (!state.imports_initialized) {
                        const nb::object datetime_mod = nb::cast<nb::object>(nb::module_::import_("datetime"));
                        state.datetime_cls = nb::cast<nb::object>(datetime_mod.attr("datetime"));
                        state.zoneinfo_cls = nb::cast<nb::object>(nb::module_::import_("zoneinfo").attr("ZoneInfo"));
                        state.imports_initialized = true;
                    }
                    const nb::object lhs_py = nb::cast(lhs);
                    const nb::object tz = nb::cast<nb::object>(state.zoneinfo_cls(tz_field.to_python()));
                    const nb::object with_tz = nb::cast<nb::object>(state.datetime_cls.attr("combine")(lhs_py, rhs, tz));
                    const nb::object ts = nb::cast<nb::object>(with_tz.attr("timestamp")());
                    date_time_ops_detail::emit_python(node, nb::cast<nb::object>(state.datetime_cls.attr("utcfromtimestamp")(ts)));
                    return;
                }
                date_time_ops_detail::emit_datetime(node, date_time_ops_detail::combine_date_time(lhs, rhs_parts));
            }
        };

        struct DateTimeDateAsDateTimeSpec {
            static constexpr const char* py_factory_name = "op_datetime_date_as_datetime";

            static void eval(Node& node) {
                auto bundle = date_time_ops_detail::input_bundle(node);
                const engine_time_t ts = date_time_ops_detail::input_field(bundle, "ts").value().template as<engine_time_t>();
                const auto day_point = std::chrono::floor<std::chrono::days>(ts);
                const engine_time_t out{std::chrono::duration_cast<engine_time_delta_t>(day_point.time_since_epoch())};
                date_time_ops_detail::emit_datetime(node, out);
            }
        };

        struct DatetimePropertiesSpec {
            static constexpr const char* py_factory_name = "op_datetime_properties";

            enum class attr {
                year,
                month,
                day,
                hour,
                minute,
                second,
                microsecond,
            };

            struct state {
                attr attribute;
            };

            static state make_state(Node& node) {
                const nb::dict& scalars = node.scalars();
                const std::string attr_name = nb::cast<std::string>(nb::cast<nb::object>(scalars["attribute"]));
                if (attr_name == "year") {
                    return {attr::year};
                }
                if (attr_name == "month") {
                    return {attr::month};
                }
                if (attr_name == "day") {
                    return {attr::day};
                }
                if (attr_name == "hour") {
                    return {attr::hour};
                }
                if (attr_name == "minute") {
                    return {attr::minute};
                }
                if (attr_name == "second") {
                    return {attr::second};
                }
                if (attr_name == "microsecond") {
                    return {attr::microsecond};
                }
                throw std::runtime_error("Unsupported datetime property");
            }

            static void eval(Node& node, state& state) {
                auto bundle = date_time_ops_detail::input_bundle(node);
                const engine_time_t ts = date_time_ops_detail::input_field(bundle, "ts").value().template as<engine_time_t>();
                const auto parts = date_time_ops_detail::split_datetime(ts);
                switch (state.attribute) {
                    case attr::year:
                        return date_time_ops_detail::emit_int(node, parts.year);
                    case attr::month:
                        return date_time_ops_detail::emit_int(node, parts.month);
                    case attr::day:
                        return date_time_ops_detail::emit_int(node, parts.day);
                    case attr::hour:
                        return date_time_ops_detail::emit_int(node, parts.hour);
                    case attr::minute:
                        return date_time_ops_detail::emit_int(node, parts.minute);
                    case attr::second:
                        return date_time_ops_detail::emit_int(node, parts.second);
                    case attr::microsecond:
                        return date_time_ops_detail::emit_int(node, parts.microsecond);
                }
            }
        };

        struct DatetimeMethodsSpec {
            static constexpr const char* py_factory_name = "op_datetime_methods";

            enum class method {
                weekday,
                isoweekday,
                timestamp,
                date,
                time,
            };

            struct state {
                method meth;
                nb::object datetime_time_cls;
            };

            static state make_state(Node& node) {
                const nb::dict& scalars = node.scalars();
                const std::string attr_name = nb::cast<std::string>(nb::cast<nb::object>(scalars["attribute"]));
                if (attr_name == "weekday") {
                    return {method::weekday, {}};
                }
                if (attr_name == "isoweekday") {
                    return {method::isoweekday, {}};
                }
                if (attr_name == "timestamp") {
                    return {method::timestamp, {}};
                }
                if (attr_name == "date") {
                    return {method::date, {}};
                }
                if (attr_name == "time") {
                    const nb::object datetime_mod = nb::cast<nb::object>(nb::module_::import_("datetime"));
                    return {method::time, nb::cast<nb::object>(datetime_mod.attr("time"))};
                }
                throw std::runtime_error("Unsupported datetime method");
            }

            static void eval(Node& node, state& state) {
                auto bundle = date_time_ops_detail::input_bundle(node);
                const engine_time_t ts = date_time_ops_detail::input_field(bundle, "ts").value().template as<engine_time_t>();
                const auto parts = date_time_ops_detail::split_datetime(ts);
                switch (state.meth) {
                    case method::weekday:
                        return date_time_ops_detail::emit_int(node, parts.weekday);
                    case method::isoweekday:
                        return date_time_ops_detail::emit_int(node, parts.isoweekday);
                    case method::timestamp: {
                        const double out = static_cast<double>(ts.time_since_epoch().count())
                                         / static_cast<double>(date_time_ops_detail::kUsPerSecond);
                        return date_time_ops_detail::emit_float(node, out);
                    }
                    case method::date: {
                        const engine_date_t out{
                            std::chrono::floor<std::chrono::days>(ts)
                        };
                        return date_time_ops_detail::emit_date(node, out);
                    }
                    case method::time: {
                        const nb::object out = nb::cast<nb::object>(state.datetime_time_cls(
                            parts.hour, parts.minute, parts.second, parts.microsecond));
                        return date_time_ops_detail::emit_python(node, out);
                    }
                }
            }
        };

        struct DatePropertiesSpec {
            static constexpr const char* py_factory_name = "op_date_properties";

            enum class attr {
                year,
                month,
                day,
            };

            struct state {
                attr attribute;
            };

            static state make_state(Node& node) {
                const nb::dict& scalars = node.scalars();
                const std::string attr_name = nb::cast<std::string>(nb::cast<nb::object>(scalars["attribute"]));
                if (attr_name == "year") {
                    return {attr::year};
                }
                if (attr_name == "month") {
                    return {attr::month};
                }
                if (attr_name == "day") {
                    return {attr::day};
                }
                throw std::runtime_error("Unsupported date property");
            }

            static void eval(Node& node, state& state) {
                auto bundle = date_time_ops_detail::input_bundle(node);
                const engine_date_t ts = date_time_ops_detail::input_field(bundle, "ts").value().template as<engine_date_t>();
                switch (state.attribute) {
                    case attr::year:
                        return date_time_ops_detail::emit_int(node, static_cast<int64_t>(static_cast<int>(ts.year())));
                    case attr::month:
                        return date_time_ops_detail::emit_int(node, static_cast<int64_t>(static_cast<unsigned>(ts.month())));
                    case attr::day:
                        return date_time_ops_detail::emit_int(node, static_cast<int64_t>(static_cast<unsigned>(ts.day())));
                }
            }
        };

        struct DateMethodsSpec {
            static constexpr const char* py_factory_name = "op_date_methods";

            enum class method {
                weekday,
                isoweekday,
                isoformat,
            };

            struct state {
                method meth;
            };

            static state make_state(Node& node) {
                const nb::dict& scalars = node.scalars();
                const std::string attr_name = nb::cast<std::string>(nb::cast<nb::object>(scalars["attribute"]));
                if (attr_name == "weekday") {
                    return {method::weekday};
                }
                if (attr_name == "isoweekday") {
                    return {method::isoweekday};
                }
                if (attr_name == "isoformat") {
                    return {method::isoformat};
                }
                throw std::runtime_error("Unsupported date method");
            }

            static void eval(Node& node, state& state) {
                auto bundle = date_time_ops_detail::input_bundle(node);
                const engine_date_t ts = date_time_ops_detail::input_field(bundle, "ts").value().template as<engine_date_t>();
                switch (state.meth) {
                    case method::weekday:
                        return date_time_ops_detail::emit_int(node, date_time_ops_detail::date_weekday(ts));
                    case method::isoweekday:
                        return date_time_ops_detail::emit_int(node, date_time_ops_detail::date_weekday(ts) + 1);
                    case method::isoformat: {
                        char buf[11];
                        std::snprintf(
                            buf,
                            sizeof(buf),
                            "%04d-%02u-%02u",
                            static_cast<int>(ts.year()),
                            static_cast<unsigned>(ts.month()),
                            static_cast<unsigned>(ts.day()));
                        return date_time_ops_detail::emit_python(node, nb::str(buf));
                    }
                }
            }
        };

        struct TimePropertiesSpec {
            static constexpr const char* py_factory_name = "op_time_properties";

            enum class attr {
                hour,
                minute,
                second,
                microsecond,
            };

            struct state {
                attr attribute;
            };

            static state make_state(Node& node) {
                const nb::dict& scalars = node.scalars();
                const std::string attr_name = nb::cast<std::string>(nb::cast<nb::object>(scalars["attribute"]));
                if (attr_name == "hour") {
                    return {attr::hour};
                }
                if (attr_name == "minute") {
                    return {attr::minute};
                }
                if (attr_name == "second") {
                    return {attr::second};
                }
                if (attr_name == "microsecond") {
                    return {attr::microsecond};
                }
                throw std::runtime_error("Unsupported time property");
            }

            static void eval(Node& node, state& state) {
                auto bundle = date_time_ops_detail::input_bundle(node);
                const nb::object ts = date_time_ops_detail::python_field(bundle, "ts");
                const auto parts = date_time_ops_detail::split_python_time(ts);
                switch (state.attribute) {
                    case attr::hour:
                        return date_time_ops_detail::emit_int(node, parts.hour);
                    case attr::minute:
                        return date_time_ops_detail::emit_int(node, parts.minute);
                    case attr::second:
                        return date_time_ops_detail::emit_int(node, parts.second);
                    case attr::microsecond:
                        return date_time_ops_detail::emit_int(node, parts.microsecond);
                }
            }
        };

        struct TimeMethodsSpec {
            static constexpr const char* py_factory_name = "op_time_methods";

            enum class method {
                isoformat,
            };

            struct state {
                method meth;
            };

            static state make_state(Node& node) {
                const nb::dict& scalars = node.scalars();
                const std::string attr_name = nb::cast<std::string>(nb::cast<nb::object>(scalars["attribute"]));
                if (attr_name == "isoformat") {
                    return {method::isoformat};
                }
                throw std::runtime_error("Unsupported time method");
            }

            static void eval(Node& node, state& state) {
                auto bundle = date_time_ops_detail::input_bundle(node);
                const nb::object ts = date_time_ops_detail::python_field(bundle, "ts");
                const auto parts = date_time_ops_detail::split_python_time(ts);
                switch (state.meth) {
                    case method::isoformat: {
                        if (parts.microsecond == 0) {
                            char buf[16];
                            std::snprintf(
                                buf,
                                sizeof(buf),
                                "%02lld:%02lld:%02lld",
                                static_cast<long long>(parts.hour),
                                static_cast<long long>(parts.minute),
                                static_cast<long long>(parts.second));
                            return date_time_ops_detail::emit_python(node, nb::str(buf));
                        }
                        char buf[24];
                        std::snprintf(
                            buf,
                            sizeof(buf),
                            "%02lld:%02lld:%02lld.%06lld",
                            static_cast<long long>(parts.hour),
                            static_cast<long long>(parts.minute),
                            static_cast<long long>(parts.second),
                            static_cast<long long>(parts.microsecond));
                        return date_time_ops_detail::emit_python(node, nb::str(buf));
                    }
                }
            }
        };

        struct TimedeltaPropertiesSpec {
            static constexpr const char* py_factory_name = "op_timedelta_properties";

            enum class attr {
                days,
                seconds,
                microseconds,
            };

            struct state {
                attr attribute;
            };

            static state make_state(Node& node) {
                const nb::dict& scalars = node.scalars();
                const std::string attr_name = nb::cast<std::string>(nb::cast<nb::object>(scalars["attribute"]));
                if (attr_name == "days") {
                    return {attr::days};
                }
                if (attr_name == "seconds") {
                    return {attr::seconds};
                }
                if (attr_name == "microseconds") {
                    return {attr::microseconds};
                }
                throw std::runtime_error("Unsupported timedelta property");
            }

            static void eval(Node& node, state& state) {
                auto bundle = date_time_ops_detail::input_bundle(node);
                const engine_time_delta_t ts = date_time_ops_detail::input_field(bundle, "ts").value().template as<engine_time_delta_t>();
                const auto parts = date_time_ops_detail::split_timedelta(ts);
                switch (state.attribute) {
                    case attr::days:
                        return date_time_ops_detail::emit_int(node, parts.days);
                    case attr::seconds:
                        return date_time_ops_detail::emit_int(node, parts.seconds);
                    case attr::microseconds:
                        return date_time_ops_detail::emit_int(node, parts.microseconds);
                }
            }
        };

        struct TimedeltaMethodsSpec {
            static constexpr const char* py_factory_name = "op_timedelta_methods";

            static void eval(Node& node) {
                auto bundle = date_time_ops_detail::input_bundle(node);
                const engine_time_delta_t ts = date_time_ops_detail::input_field(bundle, "ts").value().template as<engine_time_delta_t>();
                const double total_seconds =
                    static_cast<double>(ts.count()) / static_cast<double>(date_time_ops_detail::kUsPerSecond);
                date_time_ops_detail::emit_float(node, total_seconds);
            }
        };

        struct AddDatetimeTimedeltaSpec {
            static constexpr const char* py_factory_name = "op_add_datetime_timedelta";

            static void eval(Node& node) {
                auto bundle = date_time_ops_detail::input_bundle(node);
                const engine_time_t lhs = date_time_ops_detail::input_field(bundle, "lhs").value().template as<engine_time_t>();
                const engine_time_delta_t rhs =
                    date_time_ops_detail::input_field(bundle, "rhs").value().template as<engine_time_delta_t>();
                date_time_ops_detail::emit_datetime(node, lhs + rhs);
            }
        };

        struct AddDateTimedeltaSpec {
            static constexpr const char* py_factory_name = "op_add_date_timedelta";

            static void eval(Node& node) {
                auto bundle = date_time_ops_detail::input_bundle(node);
                const engine_date_t lhs = date_time_ops_detail::input_field(bundle, "lhs").value().template as<engine_date_t>();
                const engine_time_delta_t rhs =
                    date_time_ops_detail::input_field(bundle, "rhs").value().template as<engine_time_delta_t>();
                date_time_ops_detail::emit_date(node, date_time_ops_detail::add_days(lhs, date_time_ops_detail::timedelta_days(rhs)));
            }
        };

        struct SubDatetimeTimedeltaSpec {
            static constexpr const char* py_factory_name = "op_sub_datetime_timedelta";

            static void eval(Node& node) {
                auto bundle = date_time_ops_detail::input_bundle(node);
                const engine_time_t lhs = date_time_ops_detail::input_field(bundle, "lhs").value().template as<engine_time_t>();
                const engine_time_delta_t rhs =
                    date_time_ops_detail::input_field(bundle, "rhs").value().template as<engine_time_delta_t>();
                date_time_ops_detail::emit_datetime(node, lhs - rhs);
            }
        };

        struct SubDateTimedeltaSpec {
            static constexpr const char* py_factory_name = "op_sub_date_timedelta";

            static void eval(Node& node) {
                auto bundle = date_time_ops_detail::input_bundle(node);
                const engine_date_t lhs = date_time_ops_detail::input_field(bundle, "lhs").value().template as<engine_date_t>();
                const engine_time_delta_t rhs =
                    date_time_ops_detail::input_field(bundle, "rhs").value().template as<engine_time_delta_t>();
                date_time_ops_detail::emit_date(node, date_time_ops_detail::add_days(lhs, -date_time_ops_detail::timedelta_days(rhs)));
            }
        };

        struct SubDatesSpec {
            static constexpr const char* py_factory_name = "op_sub_dates";

            static void eval(Node& node) {
                auto bundle = date_time_ops_detail::input_bundle(node);
                const engine_date_t lhs = date_time_ops_detail::input_field(bundle, "lhs").value().template as<engine_date_t>();
                const engine_date_t rhs = date_time_ops_detail::input_field(bundle, "rhs").value().template as<engine_date_t>();
                const auto day_delta = std::chrono::sys_days{lhs} - std::chrono::sys_days{rhs};
                const engine_time_delta_t out{
                    std::chrono::duration_cast<engine_time_delta_t>(day_delta)
                };
                date_time_ops_detail::emit_timedelta(node, out);
            }
        };

        struct SubDatetimesSpec {
            static constexpr const char* py_factory_name = "op_sub_datetimes";

            static void eval(Node& node) {
                auto bundle = date_time_ops_detail::input_bundle(node);
                const engine_time_t lhs = date_time_ops_detail::input_field(bundle, "lhs").value().template as<engine_time_t>();
                const engine_time_t rhs = date_time_ops_detail::input_field(bundle, "rhs").value().template as<engine_time_t>();
                date_time_ops_detail::emit_timedelta(node, lhs - rhs);
            }
        };

        struct MulTimedeltaNumberSpec {
            static constexpr const char* py_factory_name = "op_mul_timedelta_number";

            static void eval(Node& node) {
                auto bundle = date_time_ops_detail::input_bundle(node);
                const engine_time_delta_t lhs =
                    date_time_ops_detail::input_field(bundle, "lhs").value().template as<engine_time_delta_t>();
                const double rhs = date_time_ops_detail::as_number(date_time_ops_detail::input_field(bundle, "rhs"));
                const double out_us = static_cast<double>(lhs.count()) * rhs;
                date_time_ops_detail::emit_timedelta(node, engine_time_delta_t{static_cast<int64_t>(std::llround(out_us))});
            }
        };

        struct MulNumberTimedeltaSpec {
            static constexpr const char* py_factory_name = "op_mul_number_timedelta";

            static void eval(Node& node) {
                auto bundle = date_time_ops_detail::input_bundle(node);
                const double lhs = date_time_ops_detail::as_number(date_time_ops_detail::input_field(bundle, "lhs"));
                const engine_time_delta_t rhs =
                    date_time_ops_detail::input_field(bundle, "rhs").value().template as<engine_time_delta_t>();
                const double out_us = lhs * static_cast<double>(rhs.count());
                date_time_ops_detail::emit_timedelta(node, engine_time_delta_t{static_cast<int64_t>(std::llround(out_us))});
            }
        };

        struct DivTimedeltaNumberSpec {
            static constexpr const char* py_factory_name = "op_div_timedelta_number";

            static void eval(Node& node) {
                auto bundle = date_time_ops_detail::input_bundle(node);
                const engine_time_delta_t lhs =
                    date_time_ops_detail::input_field(bundle, "lhs").value().template as<engine_time_delta_t>();
                const double rhs = date_time_ops_detail::as_number(date_time_ops_detail::input_field(bundle, "rhs"));
                if (rhs == 0.0) {
                    PyErr_SetString(PyExc_ZeroDivisionError, "division by zero");
                    nb::raise_python_error();
                }
                const double out_us = static_cast<double>(lhs.count()) / rhs;
                date_time_ops_detail::emit_timedelta(node, engine_time_delta_t{static_cast<int64_t>(std::llround(out_us))});
            }
        };

        struct DivTimedeltasSpec {
            static constexpr const char* py_factory_name = "op_div_timedeltas";

            static void eval(Node& node) {
                auto bundle = date_time_ops_detail::input_bundle(node);
                const engine_time_delta_t lhs =
                    date_time_ops_detail::input_field(bundle, "lhs").value().template as<engine_time_delta_t>();
                const engine_time_delta_t rhs =
                    date_time_ops_detail::input_field(bundle, "rhs").value().template as<engine_time_delta_t>();
                if (rhs.count() == 0) {
                    PyErr_SetString(PyExc_ZeroDivisionError, "division by zero");
                    nb::raise_python_error();
                }
                const double out = static_cast<double>(lhs.count()) / static_cast<double>(rhs.count());
                date_time_ops_detail::emit_float(node, out);
            }
        };

        struct FloorDivTimedeltasSpec {
            static constexpr const char* py_factory_name = "op_floordiv_timedeltas";

            static void eval(Node& node) {
                auto bundle = date_time_ops_detail::input_bundle(node);
                const engine_time_delta_t lhs =
                    date_time_ops_detail::input_field(bundle, "lhs").value().template as<engine_time_delta_t>();
                const engine_time_delta_t rhs =
                    date_time_ops_detail::input_field(bundle, "rhs").value().template as<engine_time_delta_t>();
                if (rhs.count() == 0) {
                    PyErr_SetString(PyExc_ZeroDivisionError, "division by zero");
                    nb::raise_python_error();
                }
                const int64_t out = date_time_ops_detail::floor_div_i64(lhs.count(), rhs.count());
                date_time_ops_detail::emit_int(node, out);
            }
        };
    }  // namespace ops
}  // namespace hgraph
