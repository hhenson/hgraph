/*
    nanobind/stl/chrono.h: conversion between std::chrono and python's datetime

    Copyright (c) 2023 Hudson River Trading LLC <opensource@hudson-trading.com> and
                       Trent Houliston <trent@houliston.me> and
                       Wenzel Jakob <wenzel.jakob@epfl.ch>

    All rights reserved. Use of this source code is governed by a
    BSD-style license that can be found in the LICENSE file.
*/
#pragma once

#include <nanobind/nanobind.h>

#if !defined(__STDC_WANT_LIB_EXT1__)
#define __STDC_WANT_LIB_EXT1__ 1  // for gmtime_s
#endif
#include <time.h>

#include <chrono>
#include <cmath>
#include <ctime>
#include <limits>

#include <nanobind/stl/detail/chrono.h>

[[nodiscard]] constexpr bool python_datetime_representable(
    std::chrono::microseconds value) noexcept {
    namespace ch = std::chrono;

    constexpr auto min_value = ch::duration_cast<ch::microseconds>(
        ch::sys_days{ch::year{1} / ch::January / ch::day{1}}.time_since_epoch());
    constexpr auto max_value = ch::duration_cast<ch::microseconds>(
        ch::sys_days{ch::year{10000} / ch::January / ch::day{1}}.time_since_epoch());
    return value >= min_value && value < max_value;
}

// hgraph fast path for the stable-ABI datetime unpackers.
//
// nanobind's limited-API ``unpack_timedelta`` / ``unpack_datetime`` read each
// field with ``PyObject_GetAttrString``, which decodes the attribute name
// from UTF-8, hashes it and allocates a temporary ``str`` on every call. A
// generator yielding ``(timedelta, value)`` paid that three times per tick,
// about a fifth of the tick in the engine bake-off profiles. These helpers
// keep the attribute names as interned ``str`` objects and look them up with
// ``PyObject_GetAttr``, which hits the type's method cache. Type checks,
// results and overflow errors are identical to nanobind's versions. Outside
// the limited API nanobind already uses the ``datetime`` C API macros, so the
// helpers simply forward.
namespace hgraph_chrono_detail {

#if defined(Py_LIMITED_API) || defined(PYPY_VERSION)

inline bool int_attr(int *dest, PyObject *o, PyObject *&key,
                     const char *name) noexcept {
    if (key == nullptr) {
        key = PyUnicode_InternFromString(name);
        if (key == nullptr)
            return false;
    }
    PyObject *value = PyObject_GetAttr(o, key);
    if (value == nullptr)
        return false;
    const long lval = PyLong_AsLong(value);
    if (lval == -1 && PyErr_Occurred()) {
        Py_DECREF(value);
        return false;
    }
    if (lval < std::numeric_limits<int>::min() ||
        lval > std::numeric_limits<int>::max()) {
        PyErr_Format(PyExc_OverflowError,
                     "%R attribute '%s' (%R) does not fit in an int",
                     o, name, value);
        Py_DECREF(value);
        return false;
    }
    Py_DECREF(value);
    *dest = static_cast<int>(lval);
    return true;
}

struct TimedeltaKeys {
    PyObject *days{nullptr};
    PyObject *seconds{nullptr};
    PyObject *microseconds{nullptr};
};

struct DatetimeKeys {
    PyObject *year{nullptr};
    PyObject *month{nullptr};
    PyObject *day{nullptr};
    PyObject *hour{nullptr};
    PyObject *minute{nullptr};
    PyObject *second{nullptr};
    PyObject *microsecond{nullptr};
};

inline TimedeltaKeys &timedelta_keys() noexcept {
    static TimedeltaKeys keys;
    return keys;
}

inline DatetimeKeys &datetime_keys() noexcept {
    static DatetimeKeys keys;
    return keys;
}

inline bool unpack_timedelta(PyObject *o, int *days, int *secs, int *usecs) {
    datetime_types.ensure_ready();
    if (!PyType_IsSubtype(Py_TYPE(o),
                          (PyTypeObject *) datetime_types.timedelta.ptr()))
        return false;
    TimedeltaKeys &keys = timedelta_keys();
    if (!int_attr(days, o, keys.days, "days") ||
        !int_attr(secs, o, keys.seconds, "seconds") ||
        !int_attr(usecs, o, keys.microseconds, "microseconds"))
        raise_python_error();
    return true;
}

inline bool unpack_datetime(PyObject *o, int *year, int *month, int *day,
                            int *hour, int *minute, int *second, int *usec) {
    datetime_types.ensure_ready();
    PyTypeObject *tp = Py_TYPE(o);
    DatetimeKeys &keys = datetime_keys();
    if (PyType_IsSubtype(tp, (PyTypeObject *) datetime_types.datetime.ptr())) {
        if (!int_attr(usec, o, keys.microsecond, "microsecond") ||
            !int_attr(second, o, keys.second, "second") ||
            !int_attr(minute, o, keys.minute, "minute") ||
            !int_attr(hour, o, keys.hour, "hour") ||
            !int_attr(day, o, keys.day, "day") ||
            !int_attr(month, o, keys.month, "month") ||
            !int_attr(year, o, keys.year, "year"))
            raise_python_error();
        return true;
    }
    if (PyType_IsSubtype(tp, (PyTypeObject *) datetime_types.date.ptr())) {
        *usec = *second = *minute = *hour = 0;
        if (!int_attr(day, o, keys.day, "day") ||
            !int_attr(month, o, keys.month, "month") ||
            !int_attr(year, o, keys.year, "year"))
            raise_python_error();
        return true;
    }
    if (PyType_IsSubtype(tp, (PyTypeObject *) datetime_types.time.ptr())) {
        *day = 1;
        *month = 1;
        *year = 1970;
        if (!int_attr(usec, o, keys.microsecond, "microsecond") ||
            !int_attr(second, o, keys.second, "second") ||
            !int_attr(minute, o, keys.minute, "minute") ||
            !int_attr(hour, o, keys.hour, "hour"))
            raise_python_error();
        return true;
    }
    return false;
}

#else

inline bool unpack_timedelta(PyObject *o, int *days, int *secs, int *usecs) {
    return ::nanobind::detail::unpack_timedelta(o, days, secs, usecs);
}

inline bool unpack_datetime(PyObject *o, int *year, int *month, int *day,
                            int *hour, int *minute, int *second, int *usec) {
    return ::nanobind::detail::unpack_datetime(o, year, month, day, hour,
                                               minute, second, usec);
}

#endif

}  // namespace hgraph_chrono_detail

// Casts a std::chrono type (either a duration or a time_point) to/from
// Python timedelta objects, or from a Python float representing seconds.
template<typename type>
class duration_caster {
public:
    using rep = typename type::rep;
    using period = typename type::period;
    using duration_t = std::chrono::duration<rep, period>;

    bool from_python(handle src, uint8_t /*flags*/, cleanup_list *) noexcept {
        namespace ch = std::chrono;

        if (!src) return false;

        // support for signed 25 bits is required by the standard
        using days = ch::duration<int_least32_t, std::ratio<86400> >;

        // If invoked with datetime.delta object, unpack it
        int dd, ss, uu;
        try {
            if (hgraph_chrono_detail::unpack_timedelta(src.ptr(), &dd, &ss, &uu)) {
                value = type(ch::duration_cast<duration_t>(
                    days(dd) + ch::seconds(ss) + ch::microseconds(uu)));
                return true;
            }
        } catch (python_error &e) {
            e.discard_as_unraisable(src.ptr());
            return false;
        }

        // If invoked with a float we assume it is seconds and convert
        int is_float;
#if defined(Py_LIMITED_API)
        is_float = PyType_IsSubtype(Py_TYPE(src.ptr()), &PyFloat_Type);
#else
        is_float = PyFloat_Check(src.ptr());
#endif
        if (is_float) {
            value = type(ch::duration_cast<duration_t>(
                ch::duration<double>(PyFloat_AsDouble(src.ptr()))));
            return true;
        }
        return false;
    }

    // If this is a duration just return it back
    static const duration_t &get_duration(const duration_t &src) {
        return src;
    }

    // If this is a time_point get the time_since_epoch
    template<typename Clock>
    static duration_t get_duration(
        const std::chrono::time_point<Clock, duration_t> &src) {
        return src.time_since_epoch();
    }

    static handle from_cpp(const type &src, rv_policy, cleanup_list *) noexcept {
        namespace ch = std::chrono;

        // Use overloaded function to get our duration from our source
        // Works out if it is a duration or time_point and get the duration
        auto d = get_duration(src);

        // Declare these special duration types so the conversions happen with the correct primitive types (int)
        using dd_t = ch::duration<int, std::ratio<86400> >;
        using ss_t = ch::duration<int, std::ratio<1> >;
        using us_t = ch::duration<int, std::micro>;

        auto dd = ch::duration_cast<dd_t>(d);
        auto subd = d - dd;
        auto ss = ch::duration_cast<ss_t>(subd);
        auto us = ch::duration_cast<us_t>(subd - ss);
        return pack_timedelta(dd.count(), ss.count(), us.count());
    }

#if PY_VERSION_HEX < 0x03090000
    NB_TYPE_CASTER(type, io_name ("typing.Union[datetime.timedelta, float]",
                                     "datetime.timedelta")

    )
#else
    NB_TYPE_CASTER(type, io_name ("datetime.timedelta | float",
                                     "datetime.timedelta"))
#endif
};

// Cast between times on the system clock and datetime.datetime instances
// (also supports datetime.date and datetime.time for Python->C++ conversions)
template<typename Duration>
class type_caster<std::chrono::time_point<std::chrono::system_clock, Duration> > {
public:
    using type = std::chrono::time_point<std::chrono::system_clock, Duration>;

    bool from_python(handle src, uint8_t /*flags*/, cleanup_list *) noexcept {
        namespace ch = std::chrono;

        if (!src)
            return false;

        int yy, mon, dd, hh, min, ss, uu;
        try {
            if (!hgraph_chrono_detail::unpack_datetime(src.ptr(), &yy, &mon, &dd,
                                                       &hh, &min, &ss, &uu)) {
                return false;
            }
        } catch (python_error &e) {
            e.discard_as_unraisable(src.ptr());
            return false;
        }
        std::chrono::year_month_day ymd{
            std::chrono::year{yy}, std::chrono::month{static_cast<unsigned>(mon)},
            std::chrono::day{static_cast<unsigned>(dd)}
        };
        std::chrono::sys_days date_part = ymd;

        auto tod = std::chrono::hours{hh} + std::chrono::minutes{min} + std::chrono::seconds{ss} +
                   std::chrono::microseconds{uu};

        // date_part is a sys_days time_point (days resolution). Convert its
        // time_since_epoch to the target Duration and add the time-of-day also
        // cast to the target Duration so the resulting time_point has the
        // correct representation type.
        //
        // IMPORTANT: On platforms where system_clock uses nanosecond precision,
        // dates beyond ~2262 will overflow. We work in microseconds to avoid this.
        auto days_since_epoch = date_part.time_since_epoch();

        // Convert to microseconds (safe range)
        auto days_us = std::chrono::duration_cast<std::chrono::microseconds>(days_since_epoch);
        auto tod_us = std::chrono::duration_cast<std::chrono::microseconds>(tod);
        auto total_us = days_us + tod_us;

        // Native DateTime is a timezone-free UTC instant. Python's datetime
        // contract defines awareness through a non-None utcoffset(), so use
        // that result rather than merely inspecting tzinfo (whose utcoffset
        // may legitimately be None). Subtracting the offset preserves the
        // instant while deliberately dropping Python's timezone identity.
        try {
            object offset = borrow<object>(src).attr("utcoffset")();
            if (!offset.is_none()) {
                int offset_days, offset_seconds, offset_micros;
                if (!hgraph_chrono_detail::unpack_timedelta(
                        offset.ptr(), &offset_days, &offset_seconds, &offset_micros)) {
                    return false;
                }
                total_us -= std::chrono::duration_cast<std::chrono::microseconds>(
                    ch::days{offset_days} + ch::seconds{offset_seconds} +
                    ch::microseconds{offset_micros});
            }
        } catch (python_error &e) {
            e.discard_as_unraisable(src.ptr());
            return false;
        }

        // An aware datetime can itself be valid while its normalized UTC
        // instant lies outside Python's year 1..9999 range. Reject it here so
        // every accepted Python datetime can also be converted back to one.
        if (!python_datetime_representable(total_us)) {
            return false;
        }

        // Create a time_point with EXPLICIT microsecond duration to avoid overflow
        // DO NOT convert this to nanoseconds as it will overflow for dates > 2262
        using sys_clock = std::chrono::system_clock;
        using time_point_us = std::chrono::time_point<sys_clock, std::chrono::microseconds>;
        value = time_point_us(total_us);

        return true;
    }

    static handle from_cpp(const type &src, rv_policy, cleanup_list *) noexcept {
        namespace ch = std::chrono;

        auto current_day = std::chrono::floor<std::chrono::days>(src);
        std::chrono::year_month_day ymd{current_day};
        int year = static_cast<int>(ymd.year());
        unsigned int month = static_cast<unsigned int>(ymd.month());
        unsigned int day = static_cast<unsigned int>(ymd.day());

        auto time_of_day = src - current_day;
        std::chrono::hh_mm_ss hms{time_of_day};

        // 4. Extract time components
        // Python's datetime constructor accepts int components. floor<days>
        // keeps these counts within one day, so the narrowing is intentional.
        const int hour = static_cast<int>(hms.hours().count());
        const int minute = static_cast<int>(hms.minutes().count());
        const int second = static_cast<int>(hms.seconds().count());
        const auto subseconds = hms.subseconds(); // This is a duration
        const int microseconds = static_cast<int>(
            std::chrono::duration_cast<std::chrono::microseconds>(subseconds).count());

        return pack_datetime(year,
                             month,
                             day,
                             hour,
                             minute,
                             second,
                             microseconds);
    }
#if PY_VERSION_HEX < 0x03090000
    NB_TYPE_CASTER(type, io_name ("typing.Union[datetime.datetime, datetime.date, datetime.time]",
                                     "datetime.datetime")

    )
#else
    NB_TYPE_CASTER(type, io_name ("datetime.datetime | datetime.date | datetime.time",
                                     "datetime.datetime"))
#endif
};

// Other clocks that are not the system clock are not measured as
// datetime.datetime objects since they are not measured on calendar
// time. So instead we just make them timedeltas; or if they have
// passed us a time as a float, we convert that.
template<typename Clock, typename Duration>
class type_caster<std::chrono::time_point<Clock, Duration> >
        : public duration_caster<std::chrono::time_point<Clock, Duration> > {
};

template<typename Rep, typename Period>
class type_caster<std::chrono::duration<Rep, Period> >
        : public duration_caster<std::chrono::duration<Rep, Period> > {
};

// Support for date
template<>
class type_caster<std::chrono::year_month_day> {
public:
    using type = std::chrono::year_month_day;

    bool from_python(handle src, uint8_t /*flags*/, cleanup_list *) noexcept {
        namespace ch = std::chrono;

        if (!src)
            return false;

        int yy, mon, dd, hh, min, ss, uu;
        try {
            if (!hgraph_chrono_detail::unpack_datetime(src.ptr(), &yy, &mon, &dd,
                                                       &hh, &min, &ss, &uu)) {
                return false;
            }
        } catch (python_error &e) {
            e.discard_as_unraisable(src.ptr());
            return false;
        }
        std::chrono::year_month_day ymd{
            std::chrono::year{yy}, std::chrono::month{static_cast<unsigned>(mon)},
            std::chrono::day{static_cast<unsigned>(dd)}
        };

        value = ymd;
        return true;
    }

    static handle from_cpp(const type &src, rv_policy, cleanup_list *) noexcept {
        namespace ch = std::chrono;

        int year = static_cast<int>(src.year());
        unsigned int month = static_cast<unsigned int>(src.month());
        unsigned int day = static_cast<unsigned int>(src.day());

#if !defined(Py_LIMITED_API) && !defined(PYPY_VERSION)
        // Initialize PyDateTimeAPI if needed (required before using PyDate_FromDate)
        if (!PyDateTimeAPI) {
            PyDateTime_IMPORT;
            if (!PyDateTimeAPI) {
                return nanobind::none().release();
            }
        }
        PyObject *result = PyDate_FromDate(year, month, day);
#else
        // Use Python object creation for limited API
        PyObject *result = nullptr;
        try {
            datetime_types.ensure_ready();
            result = datetime_types.date(year, month, day).release().ptr();
        } catch (python_error &e) {
            e.restore();
            return nanobind::none().release();
        }
#endif
        if (!result) {
            PyErr_Clear();
            return nanobind::none().release();
        }
        return result;
    }
#if PY_VERSION_HEX < 0x03090000
    NB_TYPE_CASTER(type, io_name ("typing.Union[datetime.datetime, datetime.date, datetime.time]",
                                     "datetime.datetime")

    )
#else
    NB_TYPE_CASTER(type, io_name ("datetime.datetime | datetime.date | datetime.time",
                                     "datetime.datetime"))
#endif
};

NAMESPACE_END (detail)
NAMESPACE_END (NB_NAMESPACE)
