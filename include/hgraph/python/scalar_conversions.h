#ifndef HGRAPH_PYTHON_SCALAR_CONVERSIONS_H
#define HGRAPH_PYTHON_SCALAR_CONVERSIONS_H

#include <hgraph/config.h>

#if HGRAPH_ENABLE_PYTHON_USER_NODES

#include <hgraph/hgraph_export.h>
#include <hgraph/python/native_scalar_registration.h>
#include <hgraph/types/frame.h>
#include <hgraph/types/primitive_types.h>
#include <hgraph/types/series.h>
#include <hgraph/types/temporal.h>
#include <hgraph/types/time_series_reference.h>
#include <hgraph/types/value_callable.h>
#include <hgraph/types/wired_fn.h>

#include <nanobind/nanobind.h>

#include <cstdint>

/**
 * The core scalars' ``python_conversion_traits`` (RFC 0035): the
 * specialisations that lived beside their types in the type layer, now on
 * the bridge, plus the hook pairs the module installs for the types whose
 * Python glue needs pyarrow or the DSL. ``register_python_ops`` registers
 * every one of them at load.
 */
namespace hgraph
{
    template <>
    struct python_conversion_traits<Time>
    {
        static nanobind::object to_python(const Time &value)
        {
            const auto micro   = value.microseconds;
            const auto seconds = micro / 1'000'000;
            return nanobind::module_::import_("datetime")
                .attr("time")(static_cast<int>(seconds / 3600), static_cast<int>((seconds / 60) % 60),
                              static_cast<int>(seconds % 60), static_cast<int>(micro % 1'000'000));
        }

        static Time from_python(nanobind::handle source)
        {
            if (nanobind::hasattr(source, "tzinfo") && !source.attr("tzinfo").is_none())
            {
                throw nanobind::type_error("timezone-aware time values require a zoned time scalar");
            }
            if (nanobind::hasattr(source, "utcoffset"))
            {
                nanobind::object offset = source.attr("utcoffset")();
                if (!offset.is_none())
                {
                    throw nanobind::type_error("timezone-aware time values require a zoned time scalar");
                }
            }
            const auto hours   = nanobind::cast<std::int64_t>(source.attr("hour"));
            const auto minutes = nanobind::cast<std::int64_t>(source.attr("minute"));
            const auto seconds = nanobind::cast<std::int64_t>(source.attr("second"));
            const auto micro   = nanobind::cast<std::int64_t>(source.attr("microsecond"));
            return Time{((hours * 60 + minutes) * 60 + seconds) * 1'000'000 + micro};
        }
    };

    template <>
    struct python_conversion_traits<Bytes>
    {
        static nanobind::object to_python(const Bytes &value)
        {
            return nanobind::steal(
                PyBytes_FromStringAndSize(value.data.data(), static_cast<Py_ssize_t>(value.data.size())));
        }

        static Bytes from_python(nanobind::handle source)
        {
            char      *buffer = nullptr;
            Py_ssize_t length = 0;
            if (PyBytes_AsStringAndSize(source.ptr(), &buffer, &length) != 0) { throw nanobind::python_error(); }
            return Bytes{std::string{buffer, static_cast<std::size_t>(length)}};
        }
    };

    /** The temporal scalars bind through nanobind's registered classes. */
    template <typename T>
    struct temporal_native_python_conversion
    {
        static nanobind::object to_python(const T &value) { return nanobind::cast(value); }
        static T from_python(nanobind::handle source) { return nanobind::cast<T>(source); }
    };

    template <> struct python_conversion_traits<Period> : temporal_native_python_conversion<Period> {};
    template <> struct python_conversion_traits<CivilDateTime> : temporal_native_python_conversion<CivilDateTime> {};
    template <> struct python_conversion_traits<ZoneId> : temporal_native_python_conversion<ZoneId> {};
    template <> struct python_conversion_traits<ZonedDateTime> : temporal_native_python_conversion<ZonedDateTime> {};
    template <> struct python_conversion_traits<InstantRange> : temporal_native_python_conversion<InstantRange> {};
    template <> struct python_conversion_traits<CivilDateRange> : temporal_native_python_conversion<CivilDateRange> {};
    template <> struct python_conversion_traits<InstantRangeSet> : temporal_native_python_conversion<InstantRangeSet> {};
    template <> struct python_conversion_traits<CivilDateRangeSet> : temporal_native_python_conversion<CivilDateRangeSet> {};
    template <> struct python_conversion_traits<MonthEndPolicy> : temporal_native_python_conversion<MonthEndPolicy> {};
    template <> struct python_conversion_traits<AmbiguousTimePolicy> : temporal_native_python_conversion<AmbiguousTimePolicy> {};
    template <> struct python_conversion_traits<NonexistentTimePolicy> : temporal_native_python_conversion<NonexistentTimePolicy> {};
    template <> struct python_conversion_traits<Boundary> : temporal_native_python_conversion<Boundary> {};

/** A hook pair the module installs at import: the Python glue for these
    types needs pyarrow (Frame, Series) or the DSL's wrappers (the
    reference token, WiredFn, ValueCallable). Conversion before the hooks
    are installed throws a *hook not installed* error. */
#define HGRAPH_DECLARE_PYTHON_CONVERSION_HOOKS(Type)                                                           \
    template <>                                                                                                \
    struct python_conversion_traits<Type>                                                                      \
    {                                                                                                          \
        using ToPythonHook   = nanobind::object (*)(const Type &);                                             \
        using FromPythonHook = Type (*)(nanobind::handle);                                                     \
                                                                                                               \
        [[nodiscard]] HGRAPH_EXPORT static ToPythonHook   &to_python_hook() noexcept;                          \
        [[nodiscard]] HGRAPH_EXPORT static FromPythonHook &from_python_hook() noexcept;                        \
        HGRAPH_EXPORT static nanobind::object to_python(const Type &value);                                    \
        HGRAPH_EXPORT static Type             from_python(nanobind::handle source);                            \
    }

    HGRAPH_DECLARE_PYTHON_CONVERSION_HOOKS(Frame);
    HGRAPH_DECLARE_PYTHON_CONVERSION_HOOKS(Series);
    HGRAPH_DECLARE_PYTHON_CONVERSION_HOOKS(TimeSeriesReference);
    HGRAPH_DECLARE_PYTHON_CONVERSION_HOOKS(ValueCallable);
    HGRAPH_DECLARE_PYTHON_CONVERSION_HOOKS(WiredFn);

#undef HGRAPH_DECLARE_PYTHON_CONVERSION_HOOKS
}  // namespace hgraph

namespace hgraph::python_bridge
{
    /** Python-enum conversion hooks: installed by the python module (which
        owns the meta -> python-Enum-class registry); the provider's enum
        entries call through these slots. */
    using EnumToPythonFn   = nanobind::object (*)(const ValueTypeMetaData *meta, long long value);
    using EnumFromPythonFn = long long (*)(const ValueTypeMetaData *meta, nanobind::handle source);
    [[nodiscard]] HGRAPH_EXPORT EnumToPythonFn &enum_to_python_slot() noexcept;
    [[nodiscard]] HGRAPH_EXPORT EnumFromPythonFn &enum_from_python_slot() noexcept;
}  // namespace hgraph::python_bridge

#endif  // HGRAPH_ENABLE_PYTHON_USER_NODES
#endif  // HGRAPH_PYTHON_SCALAR_CONVERSIONS_H
