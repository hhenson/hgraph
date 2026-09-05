/**
 * Type-system bindings: type handles and constructors, scalar/size/type
 * patterns, ResolutionScope (the python DSL's wiring-time resolution window
 * onto the C++ type machinery), generic-target resolution, and
 * register_python_overload.
 */
#include "py_wiring.h"
#include "py_bindings.h"

#include <hgraph/python/native_scalar_registration.h>
#include <hgraph/types/temporal.h>

#include <nanobind/make_iterator.h>
#include <nanobind/stl/string_view.h>

#include <sstream>

namespace nb = nanobind;
using namespace hgraph;
using namespace hgraph::python_bridge;

namespace hgraph::python_bridge
{
    namespace
    {
        nb::object python_type_for_value_meta(const ValueTypeMetaData *meta);

        /** A type argument crosses back as the type it carries: a ``TsType``,
            the Python annotation of a scalar schema, or a plain size. */
        nb::object operator_scalar_to_py(const ValueView &value)
        {
            if (const auto *carrier = value.try_as<TypeCarrier>())
            {
                switch (carrier->kind())
                {
                    case ResolutionKind::TimeSeries: return nb::cast(PyTsType{carrier->ts()});
                    case ResolutionKind::Scalar: return python_type_for_value_meta(carrier->scalar());
                    default: return nb::int_(*carrier->size());
                }
            }
            return value_to_py(value);
        }

    /** The Python annotation a value schema came from, rebuilt from the
        registries (RFC 0033: the reverse binding). Shared by the module
        function and the type-argument unwrap. */
    nb::object python_type_for_value_meta(const ValueTypeMetaData *meta)
    {
        if (meta == nullptr) { return nb::none(); }
        if (meta == TypeRegistry::instance().any())
        {
            return nb::module_::import_("builtins").attr("object");
        }
        nb::object opaque = python_bridge::python_type_for_opaque(meta);
        if (!opaque.is_none()) { return opaque; }
        nb::object native =
            python_bridge::python_type_for_native_scalar(meta);
        if (!native.is_none()) { return native; }
        const auto bundle = bundle_class_info_registry().find(meta);
        if (bundle != bundle_class_info_registry().end() && bundle->second.type.is_valid())
        {
            return bundle->second.specialization.is_valid()
                       ? bundle->second.specialization
                       : bundle->second.type;
        }
        const auto enumeration = enum_class_registry().find(meta);
        if (enumeration != enum_class_registry().end()) { return enumeration->second; }

        const std::string_view name = meta->name();
        nb::module_ builtins = nb::module_::import_("builtins");
        if (name == "bool") { return builtins.attr("bool"); }
        if (name == "int") { return builtins.attr("int"); }
        if (name == "float") { return builtins.attr("float"); }
        if (name == "str") { return builtins.attr("str"); }
        if (name == "bytes") { return builtins.attr("bytes"); }
        return nb::cast(PyValueType{meta});
    }
    }  // namespace

    void register_builtin_native_scalar_types()
    {
        const nb::module_ builtins = nb::module_::import_("builtins");
        const nb::module_ datetime = nb::module_::import_("datetime");
        const auto register_type = [](nb::handle python_type,
                                      std::string_view native_name) {
            const auto *meta = TypeRegistry::instance().value_type(native_name);
            if (meta == nullptr)
            {
                throw std::logic_error(
                    "built-in native scalar schema is not registered");
            }
            python_bridge::register_native_scalar_type(python_type, meta);
        };
        register_type(builtins.attr("bool"), "bool");
        register_type(builtins.attr("int"), "int");
        register_type(builtins.attr("float"), "float");
        register_type(builtins.attr("str"), "str");
        register_type(builtins.attr("bytes"), "bytes");
        register_type(datetime.attr("datetime"), "datetime");
        register_type(datetime.attr("date"), "date");
        register_type(datetime.attr("time"), "time");
        register_type(datetime.attr("timedelta"), "timedelta");
    }

    void bind_type_system(nb::module_ &m)
    {
    nb::enum_<MonthEndPolicy>(
        m, "MonthEndPolicy",
        "Policy for calendar-period arithmetic when the target month does "
        "not contain the original day.")
        .value("REJECT", MonthEndPolicy::Reject,
               "Raise when the target month does not contain the source day.")
        .value("CLAMP", MonthEndPolicy::Clamp,
               "Clamp an invalid target day to the target month's final day.")
        .value("PRESERVE_END_OF_MONTH",
               MonthEndPolicy::PreserveEndOfMonth,
               "Map a source month-end to the target month-end; otherwise "
               "clamp only when the source day is invalid in the target month.");
    nb::enum_<AmbiguousTimePolicy>(
        m, "AmbiguousTimePolicy",
        "Policy for resolving a local civil time that occurs twice during "
        "a time-zone transition.")
        .value("REJECT", AmbiguousTimePolicy::Reject,
               "Raise when the local civil time identifies two instants.")
        .value("EARLIEST", AmbiguousTimePolicy::Earliest,
               "Select the earlier of the two matching instants.")
        .value("LATEST", AmbiguousTimePolicy::Latest,
               "Select the later of the two matching instants.");
    nb::enum_<NonexistentTimePolicy>(
        m, "NonexistentTimePolicy",
        "Policy for resolving a local civil time skipped by a time-zone "
        "transition.")
        .value("REJECT", NonexistentTimePolicy::Reject,
               "Raise when the local civil time falls inside a transition gap.")
        .value("NEXT_VALID", NonexistentTimePolicy::NextValid,
               "Select the first valid instant after the transition gap.")
        .value("PREVIOUS_VALID", NonexistentTimePolicy::PreviousValid,
               "Select the final valid instant before the transition gap.");
    nb::enum_<Boundary>(
        m, "Boundary",
        "Whether a temporal range includes or excludes one endpoint.")
        .value("OPEN", Boundary::Open, "Exclude the endpoint from the range.")
        .value("CLOSED", Boundary::Closed, "Include the endpoint in the range.");

    nb::class_<CivilDateTime>(
        m, "CivilDateTime",
        "A timezone-free calendar date and wall-clock time.\n\n"
        "CivilDateTime represents local civil time without an offset or "
        "time zone. Use hgraph.temporal.resolve() with a ZoneId to obtain a "
        "ZonedDateTime. Calendar Period arithmetic preserves civil-time "
        "semantics; timedelta arithmetic uses fixed elapsed durations.")
        .def(nb::init<CivilDate, int, int, int, int>(),
             nb::arg("date"), nb::arg("hour"),
             nb::arg("minute") = 0, nb::arg("second") = 0,
             nb::arg("microsecond") = 0)
        .def("__init__",
             [](nb::pointer_and_handle<CivilDateTime> self,
                CivilDate date, nb::handle time) {
                 new (self.p) CivilDateTime{
                     date,
                     python_conversion_traits<CivilTime>::from_python(
                         time)};
             },
             nb::arg("date"), nb::arg("time"))
        .def_prop_ro("date", &CivilDateTime::date)
        .def_prop_ro("time", &CivilDateTime::time)
        .def_prop_ro("year", &CivilDateTime::year)
        .def_prop_ro("month", &CivilDateTime::month)
        .def_prop_ro("day", &CivilDateTime::day)
        .def_prop_ro("hour", &CivilDateTime::hour)
        .def_prop_ro("minute", &CivilDateTime::minute)
        .def_prop_ro("second", &CivilDateTime::second)
        .def_prop_ro("microsecond", &CivilDateTime::microsecond)
        .def("weekday", &CivilDateTime::weekday,
             "Return the weekday as Monday=0 through Sunday=6.")
        .def("isoweekday", &CivilDateTime::isoweekday,
             "Return the ISO weekday as Monday=1 through Sunday=7.")
        .def_prop_ro("day_of_year", &CivilDateTime::day_of_year)
        .def("__eq__", [](const CivilDateTime &self,
                          const CivilDateTime &other) {
            return self == other;
        })
        .def("__lt__", [](const CivilDateTime &self,
                          const CivilDateTime &other) {
            return self < other;
        })
        .def("__le__", [](const CivilDateTime &self,
                          const CivilDateTime &other) {
            return self <= other;
        })
        .def("__add__", [](CivilDateTime self, Duration delta) {
            return checked_add(self, delta);
        })
        .def("__add__", [](CivilDateTime self, Period period) {
            return apply_period(self, period);
        })
        .def("__radd__", [](CivilDateTime self, Duration delta) {
            return checked_add(self, delta);
        })
        .def("__radd__", [](CivilDateTime self, Period period) {
            return apply_period(self, period);
        })
        .def("__sub__", [](CivilDateTime self, Duration delta) {
            return checked_subtract(self, delta);
        })
        .def("__sub__", [](CivilDateTime self, Period period) {
            return apply_period(self, checked_negate(period));
        })
        .def("__sub__", [](CivilDateTime self, CivilDateTime other) {
            return checked_subtract(self, other);
        })
        .def("__hash__", [](const CivilDateTime &self) {
            return std::hash<CivilDateTime>{}(self);
        })
        .def("__repr__", [](const CivilDateTime &self) {
            return "CivilDateTime('" +
                   format_civil_datetime(self) + "')";
        });

    nb::class_<Period>(
        m, "Period",
        "A calendar-relative duration measured in years, months, and days.\n\n"
        "Unlike datetime.timedelta, a Period depends on the starting civil "
        "date. Applying one uses MonthEndPolicy to handle short months and "
        "is not supported for timezone-independent instants.")
        .def(nb::init<std::int64_t, std::int64_t, std::int64_t>(),
             nb::arg("years") = 0, nb::arg("months") = 0,
             nb::arg("days") = 0)
        .def_prop_ro("total_months", &Period::total_months)
        .def_prop_ro("years", &Period::years)
        .def_prop_ro("months", &Period::months)
        .def_prop_ro("days", &Period::days)
        .def("__add__", [](Period self, Period other) {
            return checked_add(self, other);
        })
        .def("__sub__", [](Period self, Period other) {
            return checked_subtract(self, other);
        })
        .def("__neg__", [](Period self) {
            return checked_negate(self);
        })
        .def("__mul__", [](Period self, std::int64_t factor) {
            return checked_multiply(self, factor);
        })
        .def("__rmul__", [](Period self, std::int64_t factor) {
            return checked_multiply(self, factor);
        })
        .def("__radd__", [](Period self, nb::handle value) {
            const nb::object datetime_type =
                nb::module_::import_("datetime").attr("datetime");
            if (nb::isinstance(value, datetime_type))
            {
                throw nb::type_error(
                    "Period cannot be added to an Instant");
            }
            return apply_period(
                nb::cast<CivilDate>(value),
                self);
        })
        .def("__radd__", [](Period self, CivilDateTime value) {
            return apply_period(value, self);
        })
        .def("__rsub__", [](Period self, nb::handle value) {
            const nb::object datetime_type =
                nb::module_::import_("datetime").attr("datetime");
            if (nb::isinstance(value, datetime_type))
            {
                throw nb::type_error(
                    "Period cannot be subtracted from an Instant");
            }
            return apply_period(
                nb::cast<CivilDate>(value),
                checked_negate(self));
        })
        .def("__rsub__", [](Period self, CivilDateTime value) {
            return apply_period(value, checked_negate(self));
        })
        .def("__eq__", [](const Period &self, const Period &other) {
            return self == other;
        })
        .def("__hash__", [](const Period &self) {
            return std::hash<Period>{}(self);
        })
        .def("__repr__", [](const Period &self) {
            std::ostringstream out;
            out << self;
            return out.str();
        });

    nb::class_<ZoneId>(
        m, "ZoneId",
        "An immutable IANA time-zone identifier such as 'Europe/London'.")
        .def(nb::init<std::string_view>(), nb::arg("name"))
        .def_prop_ro("name", &ZoneId::name)
        .def_prop_ro("value", &ZoneId::value)
        .def("__str__", [](const ZoneId &self) {
            return std::string{self.name()};
        })
        .def("__repr__", [](const ZoneId &self) {
            return "ZoneId('" + std::string{self.name()} + "')";
        })
        .def("__eq__", [](const ZoneId &self, const ZoneId &other) {
            return self == other;
        })
        .def("__hash__", [](const ZoneId &self) {
            return std::hash<ZoneId>{}(self);
        });

    nb::class_<ZonedDateTime>(
        m, "ZonedDateTime",
        "An instant paired with its time zone, UTC offset, and civil time.\n\n"
        "Equality includes the zone representation. Use same_instant() when "
        "only the point on the UTC timeline matters.")
        .def_prop_ro("instant", &ZonedDateTime::instant)
        .def_prop_ro("zone", &ZonedDateTime::zone)
        .def_prop_ro("offset_seconds",
                     &ZonedDateTime::offset_seconds)
        .def_prop_ro("civil", &ZonedDateTime::civil)
        .def("same_instant", &ZonedDateTime::same_instant,
             "Return whether both values identify the same UTC instant.")
        .def("__eq__", [](const ZonedDateTime &self,
                          const ZonedDateTime &other) {
            return self == other;
        })
        .def("__hash__", [](const ZonedDateTime &self) {
            return std::hash<ZonedDateTime>{}(self);
        })
        .def("__repr__", [](const ZonedDateTime &self) {
            std::ostringstream out;
            out << self;
            return "ZonedDateTime('" + out.str() + "')";
        });

    const auto bind_range = [&]<typename Range>(const char *name,
                                                const char *doc) {
        using Endpoint = typename Range::value_type;
        nb::class_<Range>(m, name, doc)
            .def(nb::init<Endpoint, Endpoint, Boundary, Boundary>(),
                 nb::arg("start"), nb::arg("end"),
                 nb::arg("lower") = Boundary::Closed,
                 nb::arg("upper") = Boundary::Open)
            .def_static("empty", &Range::make_empty,
                        "Return the canonical empty range.")
            .def_static("all", &Range::all,
                        "Return the range with neither endpoint bounded.")
            .def_static("bounded",
                        [](Endpoint start, Endpoint end, Boundary lower,
                           Boundary upper) {
                            return Range::bounded(std::move(start),
                                                  std::move(end), lower,
                                                  upper);
                        },
                        nb::arg("start"), nb::arg("end"),
                        nb::arg("lower") = Boundary::Closed,
                        nb::arg("upper") = Boundary::Open)
            .def_static("from_", &Range::from, nb::arg("start"),
                        nb::arg("lower") = Boundary::Closed)
            .def_static("until", &Range::until, nb::arg("end"),
                        nb::arg("upper") = Boundary::Open)
            .def_prop_ro("start", [](const Range &self) -> nb::object {
                const auto value = self.start();
                return value ? nb::cast(*value) : nb::none();
            })
            .def_prop_ro("end", [](const Range &self) -> nb::object {
                const auto value = self.end();
                return value ? nb::cast(*value) : nb::none();
            })
            .def_prop_ro("lower", &Range::lower_boundary)
            .def_prop_ro("upper", &Range::upper_boundary)
            .def_prop_ro("is_empty", &Range::empty)
            .def_prop_ro("is_bounded",
                         [](const Range &self) {
                             return self.bounded();
                         })
            .def_prop_ro("lower_unbounded", &Range::lower_unbounded)
            .def_prop_ro("upper_unbounded", &Range::upper_unbounded)
            .def("contains",
                 nb::overload_cast<const Endpoint &>(
                     &Range::contains, nb::const_),
                 "Return whether this range contains the value.")
            .def("contains",
                 nb::overload_cast<const Range &>(
                     &Range::contains, nb::const_),
                 "Return whether this range completely contains the other "
                 "range.")
            .def("intersection", &Range::intersection,
                 "Return the overlap with the other range, or an empty "
                 "range when disjoint.")
            .def("overlaps", &Range::overlaps,
                 "Return whether the ranges share at least one value.")
            .def("touches", &Range::touches,
                 "Return whether one range's finite upper endpoint equals "
                 "the other's finite lower endpoint, regardless of boundary "
                 "inclusion.")
            .def("adjacent", &Range::adjacent,
                 "Return whether the ranges do not overlap, share an "
                 "endpoint, and exactly one meeting boundary is closed.")
            .def("mergeable", &Range::mergeable,
                 "Return whether the ranges can be represented as one "
                 "continuous range.")
            .def("merge", &Range::merge,
                 "Return the merged range, or None when the ranges are "
                 "disjoint.")
            .def("hull", &Range::hull,
                 "Return the smallest range containing both ranges.")
            .def("difference", &Range::difference,
                 "Return the portions of this range outside the other "
                 "range.")
            .def("set_union", &Range::set_union,
                 "Return the normalized union of both ranges.")
            .def("__eq__", [](const Range &self, const Range &other) {
                return self == other;
            })
            .def("__hash__", [](const Range &self) {
                return std::hash<Range>{}(self);
            })
            .def("__repr__", [](const Range &self) {
                std::ostringstream out;
                out << self;
                return out.str();
            });
    };
    bind_range.template operator()<InstantRange>(
        "InstantRange",
        "An immutable range of UTC instants with independently open or "
        "closed endpoints. Unbounded and empty ranges are supported.");
    bind_range.template operator()<CivilDateRange>(
        "CivilDateRange",
        "An immutable range of civil dates with independently open or "
        "closed endpoints. Unbounded and empty ranges are supported.");

    const auto bind_range_set = [&]<typename RangeSet>(const char *name,
                                                       const char *doc) {
        using Range = typename RangeSet::value_type;
        nb::class_<RangeSet>(m, name, doc)
            .def("__init__",
                 [](nb::pointer_and_handle<RangeSet> self,
                    nb::iterable source) {
                     std::array<Range, 2> ranges{};
                     std::size_t size = 0;
                     for (nb::handle item : source)
                     {
                         if (size == ranges.size())
                         {
                             throw nb::value_error(
                                 "range set capacity exceeded");
                         }
                         ranges[size++] = nb::cast<Range>(item);
                     }
                     new (self.p) RangeSet{
                         std::span<const Range>{ranges.data(), size}};
                 },
                 nb::arg("ranges"))
            .def("__len__", &RangeSet::size)
            .def("__getitem__", [](const RangeSet &self,
                                   std::ptrdiff_t index) {
                if (index < 0)
                {
                    index += static_cast<std::ptrdiff_t>(self.size());
                }
                if (index < 0 ||
                    static_cast<std::size_t>(index) >= self.size())
                {
                    throw nb::index_error();
                }
                return self[static_cast<std::size_t>(index)];
            })
            .def("__iter__",
                 [](const RangeSet &self) {
                     return nb::make_iterator(
                         nb::type<RangeSet>(), "iterator",
                         self.begin(), self.end());
                 },
                 nb::keep_alive<0, 1>())
            .def("__eq__", [](const RangeSet &self,
                              const RangeSet &other) {
                return self == other;
            })
            .def("__hash__", [](const RangeSet &self) {
                return std::hash<RangeSet>{}(self);
            });
    };
    bind_range_set.template operator()<InstantRangeSet>(
        "InstantRangeSet",
        "A normalized immutable collection of at most two disjoint instant "
        "ranges, typically returned by range set operations.");
    bind_range_set.template operator()<CivilDateRangeSet>(
        "CivilDateRangeSet",
        "A normalized immutable collection of at most two disjoint civil "
        "date ranges, typically returned by range set operations.");

    m.def("_temporal_checked_add",
          [](Duration lhs, Duration rhs) {
              return checked_add(lhs, rhs);
          });
    m.def("_temporal_checked_add",
          [](Instant value, Duration delta) {
              return checked_add(value, delta);
          });
    m.def("_temporal_checked_add",
          [](Duration delta, Instant value) {
              return checked_add(value, delta);
          });
    m.def("_temporal_checked_add",
          [](CivilDateTime value, Duration delta) {
              return checked_add(value, delta);
          });
    m.def("_temporal_checked_add",
          [](Duration delta, CivilDateTime value) {
              return checked_add(value, delta);
          });
    m.def("_temporal_checked_add",
          [](Period lhs, Period rhs) {
              return checked_add(lhs, rhs);
          });
    m.def("_temporal_checked_add",
          [](CivilDate value, Period period, MonthEndPolicy policy) {
              return apply_period(value, period, policy);
          },
          nb::arg("value"), nb::arg("period"),
          nb::arg("month_end_policy") = MonthEndPolicy::Reject);
    m.def("_temporal_checked_add",
          [](CivilDateTime value, Period period,
             MonthEndPolicy policy) {
              return apply_period(value, period, policy);
          },
          nb::arg("value"), nb::arg("period"),
          nb::arg("month_end_policy") = MonthEndPolicy::Reject);

    m.def("_temporal_checked_subtract",
          [](Duration lhs, Duration rhs) {
              return checked_subtract(lhs, rhs);
          });
    m.def("_temporal_checked_subtract",
          [](Instant value, Duration delta) {
              return checked_subtract(value, delta);
          });
    m.def("_temporal_checked_subtract",
          [](Instant lhs, Instant rhs) {
              return checked_subtract(lhs, rhs);
          });
    m.def("_temporal_checked_subtract",
          [](CivilDateTime value, Duration delta) {
              return checked_subtract(value, delta);
          });
    m.def("_temporal_checked_subtract",
          [](CivilDateTime lhs, CivilDateTime rhs) {
              return checked_subtract(lhs, rhs);
          });
    m.def("_temporal_checked_subtract",
          [](Period lhs, Period rhs) {
              return checked_subtract(lhs, rhs);
          });
    m.def("_temporal_checked_subtract",
          [](CivilDate value, Period period, MonthEndPolicy policy) {
              return apply_period(value, checked_negate(period), policy);
          },
          nb::arg("value"), nb::arg("period"),
          nb::arg("month_end_policy") = MonthEndPolicy::Reject);
    m.def("_temporal_checked_subtract",
          [](CivilDateTime value, Period period,
             MonthEndPolicy policy) {
              return apply_period(value, checked_negate(period), policy);
          },
          nb::arg("value"), nb::arg("period"),
          nb::arg("month_end_policy") = MonthEndPolicy::Reject);

    m.def("_temporal_checked_negate",
          [](Duration value) {
              return checked_negate(value);
          });
    m.def("_temporal_checked_negate",
          [](Period value) {
              return checked_negate(value);
          });
    m.def("_temporal_checked_multiply",
          [](Duration value, std::int64_t factor) {
              return checked_multiply(value, factor);
          });
    m.def("_temporal_checked_multiply",
          [](Duration value, double factor) {
              return checked_multiply(value, factor);
          });
    m.def("_temporal_checked_multiply",
          [](Period value, std::int64_t factor) {
              return checked_multiply(value, factor);
          });
    m.def("_temporal_checked_divide",
          [](Duration value, std::int64_t divisor) {
              return checked_divide(value, divisor);
          });
    m.def("_temporal_checked_divide",
          [](Duration value, double divisor) {
              return checked_divide(value, divisor);
          });
    m.def("_temporal_apply_period",
          nb::overload_cast<CivilDate, Period, MonthEndPolicy>(
              &apply_period),
          nb::arg("value"), nb::arg("period"),
          nb::arg("month_end_policy") = MonthEndPolicy::Reject);
    m.def("_temporal_apply_period",
          nb::overload_cast<CivilDateTime, Period, MonthEndPolicy>(
              &apply_period),
          nb::arg("value"), nb::arg("period"),
          nb::arg("month_end_policy") = MonthEndPolicy::Reject);
    m.def("_temporal_shift",
          nb::overload_cast<InstantRange, Duration>(&shift),
          nb::arg("range"), nb::arg("delta"));
    m.def("_temporal_shift",
          nb::overload_cast<CivilDateRange, Period, MonthEndPolicy>(
              &shift),
          nb::arg("range"), nb::arg("period"),
          nb::arg("month_end_policy") = MonthEndPolicy::Reject);
    m.def("_temporal_extent", &extent, nb::arg("range"));
    m.def("_temporal_floor",
          nb::overload_cast<Duration, Duration>(&floor),
          nb::arg("value"), nb::arg("quantum"));
    m.def("_temporal_floor",
          nb::overload_cast<Instant, Duration, Instant>(&floor),
          nb::arg("value"), nb::arg("quantum"),
          nb::arg("origin") = Instant{});
    m.def("_temporal_ceil",
          nb::overload_cast<Duration, Duration>(&ceil),
          nb::arg("value"), nb::arg("quantum"));
    m.def("_temporal_ceil",
          nb::overload_cast<Instant, Duration, Instant>(&ceil),
          nb::arg("value"), nb::arg("quantum"),
          nb::arg("origin") = Instant{});
    m.def("_temporal_round",
          nb::overload_cast<Duration, Duration>(&round),
          nb::arg("value"), nb::arg("quantum"));
    m.def("_temporal_round",
          nb::overload_cast<Instant, Duration, Instant>(&round),
          nb::arg("value"), nb::arg("quantum"),
          nb::arg("origin") = Instant{});
    m.def("_temporal_bucket", &bucket,
          nb::arg("value"), nb::arg("width"),
          nb::arg("origin") = Instant{});
    m.def("_temporal_parse_duration", &parse_duration,
          nb::arg("value"));
    m.def("_temporal_format_duration", &format_duration,
          nb::arg("value"));
    m.def("_temporal_format_instant", &format_instant,
          nb::arg("value"));
    m.def("_temporal_format_civil_date", &format_civil_date,
          nb::arg("value"));
    m.def("_temporal_format_civil_time", &format_civil_time,
          nb::arg("value"));
    m.def("_temporal_format_civil_datetime",
          &format_civil_datetime, nb::arg("value"));

    const auto register_temporal_type =
        [&](const char *python_name,
            const ValueTypeMetaData *native_meta) {
            python_bridge::register_native_scalar_type(
                m.attr(python_name), native_meta);
        };
    register_temporal_type(
        "CivilDateTime",
        scalar_descriptor<CivilDateTime>::value_meta());
    register_temporal_type("Period",
                           scalar_descriptor<Period>::value_meta());
    register_temporal_type("ZoneId",
                           scalar_descriptor<ZoneId>::value_meta());
    register_temporal_type(
        "ZonedDateTime",
        scalar_descriptor<ZonedDateTime>::value_meta());
    register_temporal_type(
        "InstantRange",
        scalar_descriptor<InstantRange>::value_meta());
    register_temporal_type(
        "CivilDateRange",
        scalar_descriptor<CivilDateRange>::value_meta());
    register_temporal_type(
        "InstantRangeSet",
        scalar_descriptor<InstantRangeSet>::value_meta());
    register_temporal_type(
        "CivilDateRangeSet",
        scalar_descriptor<CivilDateRangeSet>::value_meta());
    register_temporal_type(
        "MonthEndPolicy",
        scalar_descriptor<MonthEndPolicy>::value_meta());
    register_temporal_type(
        "AmbiguousTimePolicy",
        scalar_descriptor<AmbiguousTimePolicy>::value_meta());
    register_temporal_type(
        "NonexistentTimePolicy",
        scalar_descriptor<NonexistentTimePolicy>::value_meta());
    register_temporal_type(
        "Boundary",
        scalar_descriptor<Boundary>::value_meta());

    nb::class_<PyTsType>(m, "TsType")
        .def("__eq__", [](const PyTsType &self, nb::handle other) {
            return nb::isinstance<PyTsType>(other) && nb::cast<PyTsType &>(other).meta == self.meta;
        })
        .def("__hash__", [](const PyTsType &self) { return std::hash<const void *>{}(self.meta); })
        .def_prop_ro("kind", [](const PyTsType &self) { return static_cast<int>(self.meta->kind); })
        .def_prop_ro("value_kind",
                     [](const PyTsType &self) {
                         return self.meta->value_schema != nullptr
                                    ? static_cast<int>(self.meta->value_schema->value_kind())
                                    : -1;
                     })
        .def_prop_ro("fixed_size", [](const PyTsType &self) {
            return self.meta->kind == TSTypeKind::TSL ? self.meta->fixed_size() : 0;
        })
        .def_prop_ro("is_ts", [](const PyTsType &self) {
            return self.meta != nullptr && self.meta->kind == TSTypeKind::TS;
        })
        .def_prop_ro("is_tsd", [](const PyTsType &self) {
            return self.meta != nullptr && self.meta->kind == TSTypeKind::TSD;
        })
        .def_prop_ro("is_tsl", [](const PyTsType &self) {
            return self.meta != nullptr && self.meta->kind == TSTypeKind::TSL;
        })
        .def_prop_ro("is_tsb", [](const PyTsType &self) {
            return self.meta != nullptr && self.meta->kind == TSTypeKind::TSB;
        })
        .def_prop_ro("is_ref", [](const PyTsType &self) {
            return self.meta != nullptr && self.meta->kind == TSTypeKind::REF;
        })
        .def_prop_ro("is_fixed_tsl", [](const PyTsType &self) {
            return self.meta != nullptr && self.meta->kind == TSTypeKind::TSL && self.meta->fixed_size() > 0;
        })
        .def_prop_ro("is_ts_bundle", [](const PyTsType &self) {
            return self.meta != nullptr && self.meta->kind == TSTypeKind::TS &&
                   self.meta->value_schema != nullptr &&
                   self.meta->value_schema->value_kind() == ValueTypeKind::Bundle;
        })
        .def_prop_ro("is_ts_mapping", [](const PyTsType &self) {
            return self.meta != nullptr && self.meta->kind == TSTypeKind::TS &&
                   self.meta->value_schema != nullptr &&
                   self.meta->value_schema->value_kind() == ValueTypeKind::Map;
        })
        .def_prop_ro("is_tss", [](const PyTsType &self) {
            return self.meta != nullptr && self.meta->kind == TSTypeKind::TSS;
        })
        .def_prop_ro("is_ts_sequence", [](const PyTsType &self) {
            return self.meta != nullptr && self.meta->kind == TSTypeKind::TS &&
                   self.meta->value_schema != nullptr &&
                   (self.meta->value_schema->value_kind() == ValueTypeKind::Tuple ||
                    self.meta->value_schema->value_kind() == ValueTypeKind::List);
        })
        .def_prop_ro("is_ts_json", [](const PyTsType &self) {
            return stdlib::json_tree::is_json_ts(self.meta);
        })
        .def("__repr__", [](const PyTsType &self) {
            return self.meta != nullptr && !self.meta->name().empty() ? std::string{self.meta->name()}
                                                                      : std::string{"<ts?>"};
        });
    // ``variables`` reports the type-variable names in a pattern, in order of
    // first appearance. The wiring layer uses it to map an unnamed
    // pre-resolution item (``my_node[TS[int]]``) onto a type variable, so that
    // the mapping is derived from the same structure the matcher unifies
    // against rather than from a second, drift-prone python-side notion of what
    // a type variable is. Build-time only.
    nb::class_<PyScalarPattern>(m, "ScalarPattern")
        .def("__repr__", [](const PyScalarPattern &self) {
            return scalar_pattern_to_string(self.pattern);
        })
        .def_prop_ro("variables", [](const PyScalarPattern &self) {
            return pattern_variables(self.pattern);
        });
    nb::class_<PySizePattern>(m, "SizePattern")
        .def("__repr__", [](const PySizePattern &self) {
            return self.variable ? "~" + self.name : std::to_string(self.value);
        })
        .def_prop_ro("variables", [](const PySizePattern &self) {
            return self.variable && !self.name.empty()
                       ? std::vector<std::string>{self.name}
                       : std::vector<std::string>{};
        });
    nb::class_<PyTypePattern>(m, "TypePattern")
        .def("__repr__", [](const PyTypePattern &self) {
            return ts_pattern_to_string(self.pattern);
        })
        .def_prop_ro("variables", [](const PyTypePattern &self) {
            return pattern_variables(self.pattern);
        })
        .def_prop_ro("ts_kind", [](const PyTypePattern &self) -> int {
            // The TS KIND the pattern describes (structural introspection -
            // the python DSL never classifies by rendered labels). -1 = a
            // whole-time-series variable (kind unconstrained).
            switch (self.pattern.kind)
            {
                case TypePattern::Kind::Var: return -1;
                case TypePattern::Kind::Concrete:
                    return self.pattern.meta != nullptr ? static_cast<int>(self.pattern.meta->kind) : -1;
                case TypePattern::Kind::TS: return static_cast<int>(TSTypeKind::TS);
                case TypePattern::Kind::TSS: return static_cast<int>(TSTypeKind::TSS);
                case TypePattern::Kind::TSL: return static_cast<int>(TSTypeKind::TSL);
                case TypePattern::Kind::TSD: return static_cast<int>(TSTypeKind::TSD);
                case TypePattern::Kind::TSW: return static_cast<int>(TSTypeKind::TSW);
                case TypePattern::Kind::TSB: return static_cast<int>(TSTypeKind::TSB);
                case TypePattern::Kind::REF: return static_cast<int>(TSTypeKind::REF);
                case TypePattern::Kind::Signal: return static_cast<int>(TSTypeKind::SIGNAL);
            }
            return -1;
        });
    m.attr("TS_KIND_TS")  = static_cast<int>(TSTypeKind::TS);
    m.attr("TS_KIND_TSS") = static_cast<int>(TSTypeKind::TSS);
    m.attr("TS_KIND_TSL") = static_cast<int>(TSTypeKind::TSL);
    m.attr("TS_KIND_TSD") = static_cast<int>(TSTypeKind::TSD);
    m.attr("TS_KIND_TSB") = static_cast<int>(TSTypeKind::TSB);
    m.attr("TS_KIND_TSW") = static_cast<int>(TSTypeKind::TSW);

    m.def("ts_type", [](const std::string &name) {
        const auto *meta = TypeRegistry::instance().time_series_type(name);
        if (meta == nullptr) { throw nb::value_error(("unknown time-series type: " + name).c_str()); }
        return PyTsType{meta};
    });

    // --- time-series type CONSTRUCTION (the Python type layer builds
    // TS[...]/TSS[...]/TSD[...]/TSL[...]/TSB[...] through these) ---
    nb::class_<PyValueType>(m, "ValueType")
        .def("__eq__",
             [](const PyValueType &self, nb::handle other) {
                 if (!nb::isinstance<PyValueType>(other)) { return false; }
                 return self.meta == nb::cast<PyValueType &>(other).meta;   // metas are interned
             })
        .def("__hash__", [](const PyValueType &self) { return std::hash<const void *>{}(self.meta); })
        .def_prop_ro("name", [](const PyValueType &self) {
            return self.meta != nullptr ? std::string{self.meta->name()} : std::string{};
        })
        .def_prop_ro("namespace", [](const PyValueType &self) {
            return self.meta != nullptr ? std::string{self.meta->bundle_namespace()} : std::string{};
        })
        .def_prop_ro("local_name", [](const PyValueType &self) {
            return self.meta != nullptr ? std::string{self.meta->bundle_local_name()} : std::string{};
        })
        .def_prop_ro("is_hashable", [](const PyValueType &self) {
            return self.meta != nullptr && self.meta->is_hashable();
        })
        .def_prop_ro("is_equatable", [](const PyValueType &self) {
            return self.meta != nullptr && self.meta->is_equatable();
        })
        .def_prop_ro("is_comparable", [](const PyValueType &self) {
            return self.meta != nullptr && self.meta->is_comparable();
        })
        .def_prop_ro("is_shared", [](const PyValueType &self) {
            return self.meta != nullptr && self.meta->is_shared();
        })
        .def_prop_ro("is_opaque_python", [](const PyValueType &self) {
            return self.meta != nullptr && self.meta->is_opaque_python();
        })
        .def_prop_ro("fields", [](const PyValueType &self) {
            nb::list result;
            if (self.meta == nullptr) { return result; }
            for (std::size_t index = 0; index < self.meta->field_count; ++index)
            {
                const auto &field = self.meta->fields[index];
                result.append(nb::make_tuple(
                    std::string{field.name != nullptr ? field.name : ""},
                    PyValueType{field.type}));
            }
            return result;
        });
    m.def("value_type", [](const std::string &name) {
        const auto *meta = TypeRegistry::instance().value_type(name);
        if (meta == nullptr) { throw nb::value_error(("unknown value type: " + name).c_str()); }
        return PyValueType{meta};
    });
    m.def("opaque_python_vt",
          [](nb::handle annotation, const std::string &name, nb::list parent_types) {
              if (const auto *existing =
                      python_bridge::opaque_type_for_python(annotation))
              {
                  return PyValueType{existing};
              }
              std::vector<const ValueTypeMetaData *> parents;
              parents.reserve(nb::len(parent_types));
              for (nb::handle parent : parent_types)
              {
                  parents.push_back(nb::cast<PyValueType>(parent).meta);
              }
              const auto *meta =
                  TypeRegistry::instance().opaque_python(name, parents);
              python_bridge::register_python_opaque_type(annotation, meta);
              return PyValueType{meta};
          });
    m.def("register_native_scalar_type",
          [](nb::handle python_type, PyValueType native_value_type) {
              python_bridge::register_native_scalar_type(
                  python_type, native_value_type.meta);
          });
    m.def("native_scalar_value_type", [](nb::handle python_type) -> nb::object {
        const auto *meta =
            python_bridge::native_scalar_type_for_python(python_type);
        return meta != nullptr ? nb::cast(PyValueType{meta}) : nb::none();
    });
    m.def("python_type_for_value", [](PyValueType value) -> nb::object {
        return python_type_for_value_meta(value.meta);
    });
    register_builtin_native_scalar_types();
    m.def("ts", [](PyValueType v) { return PyTsType{TypeRegistry::instance().ts(v.meta)}; });
    m.def("ref_ts", [](PyTsType target) { return PyTsType{TypeRegistry::instance().ref(target.meta)}; });
    m.def("ref_target", [](PyTsType ref) { return PyTsType{TypeRegistry::instance().dereference(ref.meta)}; });
    m.def("set_vt", [](PyValueType e) { return PyValueType{TypeRegistry::instance().set(e.meta)}; });
    m.def("map_vt", [](PyValueType k, PyValueType v) {
        return PyValueType{TypeRegistry::instance().map(k.meta, v.meta)};
    });
    m.def("array_vt", [](PyValueType element, nb::list dimensions) {
        std::vector<std::size_t> shape;
        shape.reserve(nb::len(dimensions));
        for (nb::handle dimension : dimensions)
        {
            const auto value = nb::cast<std::int64_t>(dimension);
            if (value < 0)
            {
                if (value != -1) { throw nb::value_error("Array dimensions must be positive or -1"); }
                shape.push_back(0);
            }
            else
            {
                shape.push_back(static_cast<std::size_t>(value));
            }
        }
        return PyValueType{TypeRegistry::instance().array(element.meta, shape)};
    });
    m.def("owned_vt", [](PyValueType target) {
        return PyValueType{TypeRegistry::instance().owned(target.meta)};
    });
    m.def("shared_vt", [](PyValueType target) {
        return PyValueType{TypeRegistry::instance().shared(target.meta)};
    });
    m.def("tuple_vt", [](PyValueType e) {
        // A homogeneous variadic tuple (python's tuple[X, ...]).
        return PyValueType{TypeRegistry::instance().list(e.meta, 0, true)};
    });
    m.def("nullable_tuple_vt", [](PyValueType e) {
        // A NULLABLE variadic tuple (elements may be None holes).
        return PyValueType{TypeRegistry::instance().nullable_tuple(e.meta)};
    });
    m.def("series_vt", [](PyValueType e) {
        return PyValueType{TypeRegistry::instance().series(e.meta)};
    });
    m.def("un_named_bundle_vt", [](nb::list fields) {
        // The structural (un-named) bundle - python's compound_scalar()
        // anonymous compounds (nominal-vs-structural rule, scalar.rst).
        std::vector<std::pair<std::string, const ValueTypeMetaData *>> field_metas;
        field_metas.reserve(nb::len(fields));
        for (nb::handle field : fields)
        {
            auto pair = nb::cast<nb::tuple>(field);
            field_metas.emplace_back(nb::cast<std::string>(pair[0]),
                                     nb::cast<PyValueType &>(pair[1]).meta);
        }
        return PyValueType{TypeRegistry::instance().un_named_bundle(field_metas)};
    });
    m.def("frame_vt", [](PyValueType schema) {
        // Frame[Schema]: the typed frame meta carrying its column bundle.
        return PyValueType{TypeRegistry::instance().frame(schema.meta)};
    });
    m.def("frame_vt", [](PyValueType schema, PyValueType metadata) {
        // Frame[Schema, Metadata]: metadata is encoded into Arrow schema metadata.
        return PyValueType{TypeRegistry::instance().frame(schema.meta, metadata.meta)};
    });
    m.def("_with_frame_metadata", [](nb::handle table, PyValueType metadata_schema,
                                      nb::handle metadata) {
        Value frame_value = python_bridge::py_arrow_to_frame(table);
        Value metadata_value = python_bridge::py_to_value_as(metadata, metadata_schema.meta);
        Frame encoded = with_frame_metadata(
            frame_value.view().checked_as<Frame>(), std::move(metadata_value));
        return python_bridge::frame_to_py(encoded);
    });
    m.def("_frame_metadata", [](nb::handle table, PyValueType metadata_schema) {
        Value frame_value = python_bridge::py_arrow_to_frame(table);
        Value decoded = frame_metadata(
            frame_value.view().checked_as<Frame>(), metadata_schema.meta);
        return python_bridge::value_to_py(decoded.view());
    });
    m.def("_frame_metadata_reflective", [](nb::handle table) {
        Value frame_value = python_bridge::py_arrow_to_frame(table);
        Value decoded = frame_metadata(frame_value.view().checked_as<Frame>());
        return python_bridge::value_to_py(decoded.view());
    });
    m.def("_has_frame_metadata", [](nb::handle table) {
        Value frame_value = python_bridge::py_arrow_to_frame(table);
        return has_frame_metadata(frame_value.view().checked_as<Frame>());
    });
    m.def("_without_frame_metadata", [](nb::handle table) {
        Value frame_value = python_bridge::py_arrow_to_frame(table);
        Frame stripped = without_frame_metadata(frame_value.view().checked_as<Frame>());
        return python_bridge::frame_to_py(stripped);
    });
    m.def("table_schema_info", [](PyTsType ts, const std::string &date_key, const std::string &as_of_key) {
        // TABLE layout introspection (design record step 6): the C++ layout
        // is the single source; python's TableSchema maps it declaratively.
        const auto &layout =
            hgraph::stdlib::table_ts_detail::ts_table_layout(ts.meta, date_key, as_of_key);
        nb::dict info;
        nb::list keys, types, partition_keys, removed_keys;
        for (std::size_t i = 0; i < layout.keys.size(); ++i)
        {
            keys.append(nb::str(layout.keys[i].c_str()));
            const auto *meta = layout.col_metas[i];
            types.append(nb::str(meta != nullptr && meta->header.label != nullptr ? meta->header.label : "?"));
        }
        for (const auto &name : layout.partition_keys) { partition_keys.append(nb::str(name.c_str())); }
        for (const auto &name : layout.removed_keys) { removed_keys.append(nb::str(name.c_str())); }
        info["keys"]           = keys;
        info["types"]          = types;
        info["partition_keys"] = partition_keys;
        info["removed_keys"]   = removed_keys;
        info["date_key"]       = nb::str(layout.date_key.c_str());
        info["as_of_key"]      = nb::str(layout.as_of_key.c_str());
        info["is_multi_row"]   = layout.is_multi_row;
        return info;
    });
    m.def("fixed_tuple_vt", [](nb::list elements) {
        std::vector<const ValueTypeMetaData *> metas;
        metas.reserve(nb::len(elements));
        for (nb::handle element : elements) { metas.push_back(nb::cast<PyValueType &>(element).meta); }
        return PyValueType{TypeRegistry::instance().tuple(metas)};
    });
    // Type INTROSPECTION for wiring-time target inference (py convert etc.).
    m.def("ts_value_vt", [](PyTsType t) { return PyValueType{t.meta->value_schema}; });
    m.def("vt_kind", [](PyValueType v) { return static_cast<int>(v.meta->value_kind()); });
    m.def("vt_element", [](PyValueType v) { return PyValueType{v.meta->element_type}; });
    m.def("vt_key", [](PyValueType v) { return PyValueType{v.meta->key_type}; });
    m.def("tsd_key_vt", [](PyTsType t) { return PyValueType{t.meta->key_type()}; });
    m.def("tsd_element_ts", [](PyTsType t) { return PyTsType{t.meta->element_ts()}; });
    m.def("tsb_value_vt", [](PyTsType t) { return PyValueType{t.meta->value_schema}; });
    m.def("tsl_element_ts", [](PyTsType t) { return PyTsType{t.meta->element_ts()}; });
    m.def("tss", [](PyValueType v) { return PyTsType{TypeRegistry::instance().tss(v.meta)}; });
    m.def("tsd", [](PyValueType k, PyTsType v) { return PyTsType{TypeRegistry::instance().tsd(k.meta, v.meta)}; });
    m.def("tsw", [](PyValueType v, std::size_t period, std::size_t min_period) {
        return PyTsType{TypeRegistry::instance().tsw(v.meta, period, min_period)};
    }, nb::arg("value"), nb::arg("period"), nb::arg("min_period") = 0);
    // The scalar-level JSON builders (hgraph's to_json_builder /
    // from_json_builder): serialize/parse a plain value by its schema.
    m.def("value_to_json", [](PyValueType meta, nb::handle value) {
        const Value owned = python_bridge::py_to_value_as(value, meta.meta);
        return to_json_string(owned.view());
    });
    m.def("value_from_json", [](PyValueType meta, const std::string &text) {
        const auto realization = TypeRealizationSnapshot::capture(TypeRegistry::instance());
        TypeRealizationScope scope{realization.get()};
        const Value parsed = from_json_string(meta.meta, text);
        return python_bridge::value_to_py(parsed.view());
    });
    m.def("register_json_datetime_format", &register_json_datetime_format,
          nb::arg("format"), nb::arg("time_only") = false);
    m.def("enum_vt", [](const std::string &name, nb::list members, nb::object cls) {
        std::vector<std::pair<std::string, long long>> table;
        table.reserve(nb::len(members));
        for (nb::handle item : members)
        {
            auto pair = nb::cast<nb::tuple>(item);
            table.emplace_back(nb::cast<std::string>(pair[0]), nb::cast<long long>(pair[1]));
        }
        const auto *meta = TypeRegistry::instance().enum_type(name, table);
        nb::object python_members = cls.attr("__members__");
        auto &to_python = python_bridge::enum_to_python_registry()[meta];
        auto &from_python = python_bridge::enum_from_python_registry()[meta];
        for (const auto &[member_name, value] : table)
        {
            nb::object member = nb::borrow<nb::object>(
                python_members[nb::str(member_name.c_str())]);
            to_python.emplace(value, std::move(member));
            from_python.emplace(member_name, value);
        }
        python_bridge::enum_type_registry()[
            reinterpret_cast<PyTypeObject *>(cls.ptr())] = meta;
        python_bridge::enum_class_registry()[meta] = std::move(cls);
        return PyValueType{meta};
    });
    m.def("tsw_duration", [](PyValueType v, TimeDelta time_range, TimeDelta min_time_range) {
        return PyTsType{TypeRegistry::instance().tsw_duration(v.meta, time_range, min_time_range)};
    }, nb::arg("value"), nb::arg("time_range"), nb::arg("min_time_range") = TimeDelta{0});
    m.def("tsl", [](PyTsType e, std::size_t size) { return PyTsType{TypeRegistry::instance().tsl(e.meta, size)}; },
          nb::arg("element"), nb::arg("size") = 0);
    m.def("tsb", [](const std::string &name, nb::list fields) {
        std::vector<std::pair<std::string, const TSValueTypeMetaData *>> entries;
        entries.reserve(nb::len(fields));
        for (nb::handle field : fields)
        {
            auto pair = nb::cast<nb::tuple>(field);
            entries.emplace_back(nb::cast<std::string>(pair[0]), nb::cast<PyTsType &>(pair[1]).meta);
        }
        return PyTsType{TypeRegistry::instance().tsb(name, entries)};
    });
    m.def("tsb", [](PyValueType bundle) {
        return PyTsType{TypeRegistry::instance().tsb(bundle.meta)};
    });

    m.def("scalar_pattern_var", [](const std::string &name) {
        return PyScalarPattern{ScalarPattern::var(name)};
    });
    m.def("scalar_pattern_var", [](const std::string &name, nb::list constraints) {
        std::vector<const ValueTypeMetaData *> metas;
        metas.reserve(nb::len(constraints));
        for (nb::handle constraint : constraints)
        {
            metas.push_back(nb::cast<PyValueType &>(constraint).meta);
        }
        return PyScalarPattern{ScalarPattern::var(name, std::move(metas))};
    });
    m.def("scalar_pattern_var_bound", [](const std::string &name, PyValueType bound) {
        return PyScalarPattern{ScalarPattern::var(name, {}, bound.meta)};
    });
    m.def("scalar_pattern_value", [](PyValueType value) {
        return PyScalarPattern{ScalarPattern::concrete(value.meta)};
    });
    m.def("scalar_pattern_unknown_tuple", [] {
        return PyScalarPattern{ScalarPattern::unknown_tuple()};
    });
    m.def("scalar_pattern_unknown_tuple", [](PyScalarPattern element) {
        return PyScalarPattern{ScalarPattern::unknown_tuple(std::move(element.pattern))};
    });
    m.def("scalar_pattern_homogeneous_tuple", [](PyScalarPattern element) {
        return PyScalarPattern{ScalarPattern::homogeneous_tuple(std::move(element.pattern))};
    });
    m.def("scalar_pattern_fixed_tuple", [](nb::list elements) {
        std::vector<ScalarPattern> patterns;
        patterns.reserve(nb::len(elements));
        for (nb::handle element : elements) { patterns.push_back(nb::cast<PyScalarPattern>(element).pattern); }
        return PyScalarPattern{ScalarPattern::fixed_tuple(std::move(patterns))};
    });
    m.def("scalar_pattern_set", [](PyScalarPattern element) {
        return PyScalarPattern{ScalarPattern::set(std::move(element.pattern))};
    });
    m.def("scalar_pattern_map", [](PyScalarPattern key, PyScalarPattern value) {
        return PyScalarPattern{ScalarPattern::map(std::move(key.pattern), std::move(value.pattern))};
    });
    m.def("scalar_pattern_series", [](PyScalarPattern element) {
        return PyScalarPattern{ScalarPattern::series(std::move(element.pattern))};
    });
    m.def("scalar_pattern_frame", [](PyScalarPattern schema) {
        return PyScalarPattern{ScalarPattern::frame(std::move(schema.pattern))};
    });
    m.def("scalar_pattern_array", [](PyScalarPattern element, nb::list dimensions) {
        std::vector<DimensionPattern> shape;
        shape.reserve(nb::len(dimensions));
        for (nb::handle dimension : dimensions)
        {
            const auto &value = nb::cast<PySizePattern &>(dimension);
            shape.push_back(value.variable ? DimensionPattern::var(value.name)
                                           : DimensionPattern::fixed(value.value));
        }
        return PyScalarPattern{ScalarPattern::array(std::move(element.pattern), std::move(shape))};
    });
    m.def("scalar_pattern_bundle", [] {
        return PyScalarPattern{ScalarPattern::bundle()};
    });
    m.def("scalar_pattern_bundle", [](const std::string &schema_variable) {
        return PyScalarPattern{ScalarPattern::bundle_var(schema_variable)};
    });
    m.def("scalar_pattern_bundle_generic",
          [](const std::string &schema_variable, const std::string &qualified_origin,
             nb::list arguments) {
              std::vector<ScalarPattern> patterns;
              patterns.reserve(nb::len(arguments));
              for (nb::handle argument : arguments)
              {
                  patterns.push_back(nb::cast<PyScalarPattern &>(argument).pattern);
              }
              return PyScalarPattern{ScalarPattern::bundle_generic(
                  schema_variable, qualified_origin, std::move(patterns))};
          },
          nb::arg("schema_variable"), nb::arg("qualified_origin"), nb::arg("arguments"));

    m.def("size_pattern_var", [](const std::string &name) {
        return PySizePattern{true, name, 0};
    });
    m.def("size_pattern_value", [](std::size_t value) {
        return PySizePattern{false, {}, value};
    });

    // ------------------------------------------------------------------
    // ResolutionScope: the python DSL's wiring-time resolution window onto
    // the C++ type/pattern machinery (type_resolution.h). Input patterns
    // match against wired port schemas accumulating type-variable bindings;
    // outputs resolve from the same map. Python adds NO parallel classifier
    // (ruling 2026-07-11: identify the intent, serve it from the C++ type
    // system).
    // ------------------------------------------------------------------
    {
        nb::class_<PyResolutionScope>(m, "ResolutionScope")
            .def(nb::init<>())
            .def("match",
                 [](PyResolutionScope &self, PyTypePattern pattern, PyTsType actual) {
                     return input_ts_pattern_match(pattern.pattern, actual.meta, self.map);
                 },
                 nb::arg("pattern"), nb::arg("actual"))
            .def("resolve_ts",
                 [](PyResolutionScope &self, PyTypePattern pattern) -> std::optional<PyTsType> {
                     try
                     {
                         const auto *meta = ts_pattern_resolve(pattern.pattern, self.map);
                         if (meta == nullptr) { return std::nullopt; }
                         return PyTsType{meta};
                     }
                     catch (const std::exception &)
                     {
                         return std::nullopt;   // unresolved variables remain
                     }
                 },
                 nb::arg("pattern"))
            .def("match_carrier",
                 [](PyResolutionScope &self, nb::object pattern, nb::object value) -> bool {
                     // The one carrier matcher (RFC 0033): a type argument
                     // against its carried pattern, binding into this scope.
                     ParamPattern param;
                     param.kind = ParamPattern::Kind::TypeArg;
                     if (nb::isinstance<PyTypePattern>(pattern))
                     {
                         param.ts      = nb::cast<PyTypePattern &>(pattern).pattern;
                         param.carrier = ResolutionKind::TimeSeries;
                     }
                     else if (nb::isinstance<PyScalarPattern>(pattern))
                     {
                         param.scalar  = nb::cast<PyScalarPattern &>(pattern).pattern;
                         param.carrier = ResolutionKind::Scalar;
                     }
                     else
                     {
                         throw nb::type_error("match_carrier pattern must be a TypePattern or ScalarPattern");
                     }
                     TypeCarrier carrier;
                     if (nb::isinstance<PyTsType>(value)) { carrier = TypeCarrier::of_ts(nb::cast<PyTsType &>(value).meta); }
                     else if (nb::isinstance<PyValueType>(value))
                     {
                         carrier = TypeCarrier::of_scalar(nb::cast<PyValueType &>(value).meta);
                     }
                     else if (nb::isinstance<nb::int_>(value))
                     {
                         carrier       = TypeCarrier::of_size(nb::cast<std::size_t>(value));
                         param.carrier = ResolutionKind::Size;
                     }
                     else
                     {
                         throw nb::type_error("match_carrier value must be a TsType, a ValueType or a size");
                     }
                     return type_carrier_match(param, carrier, self.map);
                 },
                 nb::arg("pattern"), nb::arg("value"))
            .def("bind_ts", [](PyResolutionScope &self, const std::string &name, PyTsType meta) {
                self.map.bind_ts(name, meta.meta);
            })
            .def("bind_scalar", [](PyResolutionScope &self, const std::string &name, PyValueType meta) {
                self.map.bind_scalar(name, meta.meta);
            })
            .def("bind_size", [](PyResolutionScope &self, const std::string &name, std::size_t size) {
                self.map.bind_size(name, size);
            })
            .def("is_resolved", [](const PyResolutionScope &self, const std::string &name) {
                return self.map.is_resolved(name);
            })
            .def("find_ts",
                 [](const PyResolutionScope &self, const std::string &name) -> std::optional<PyTsType> {
                     const auto *meta = self.map.find_ts(name);
                     if (meta == nullptr) { return std::nullopt; }
                     return PyTsType{meta};
                 })
            .def("find_scalar",
                 [](const PyResolutionScope &self, const std::string &name) -> std::optional<PyValueType> {
                     const auto *meta = self.map.find_scalar(name);
                     if (meta == nullptr) { return std::nullopt; }
                     return PyValueType{meta};
                 })
            .def("find_size",
                 [](const PyResolutionScope &self, const std::string &name) -> std::optional<std::size_t> {
                     return self.map.find_size(name);
                 })
            .def_prop_ro("bindings", [](const PyResolutionScope &self) {
                // The resolver-lambda ``mapping`` argument: every bound
                // variable by name (ts handles, scalar metas, sizes).
                nb::dict out;
                for (const auto &[name, meta] : self.map.ts_vars) { out[nb::str(name.c_str())] = PyTsType{meta}; }
                for (const auto &[name, meta] : self.map.scalar_vars)
                {
                    out[nb::str(name.c_str())] = PyValueType{meta};
                }
                for (const auto &[name, size] : self.map.size_vars) { out[nb::str(name.c_str())] = size; }
                return out;
            });
    }

    m.def("type_pattern_var", [](const std::string &name) {
        return PyTypePattern{TypePattern::var(name)};
    });
    m.def("type_pattern_var", [](const std::string &name, nb::list constraints) {
        std::vector<const TSValueTypeMetaData *> metas;
        metas.reserve(nb::len(constraints));
        for (nb::handle constraint : constraints)
        {
            metas.push_back(nb::cast<PyTsType &>(constraint).meta);
        }
        return PyTypePattern{TypePattern::var(name, std::move(metas))};
    });
    m.def("type_pattern_concrete", [](PyTsType type) {
        return PyTypePattern{TypePattern::concrete(type.meta)};
    });
    m.def("type_pattern_ts", [](PyScalarPattern value) {
        return PyTypePattern{TypePattern::ts(std::move(value.pattern))};
    });
    m.def("type_pattern_tss", [] {
        return PyTypePattern{TypePattern::tss(ScalarPattern::var("T"))};
    });
    m.def("type_pattern_tss", [](PyScalarPattern element) {
        return PyTypePattern{TypePattern::tss(std::move(element.pattern))};
    });
    m.def("type_pattern_tsd", [] {
        return PyTypePattern{TypePattern::tsd(ScalarPattern::var("K"), TypePattern::var("V"))};
    });
    m.def("type_pattern_tsd", [](PyScalarPattern key, PyTypePattern value) {
        return PyTypePattern{TypePattern::tsd(std::move(key.pattern), std::move(value.pattern))};
    });
    m.def("type_pattern_tsl", [] {
        return PyTypePattern{TypePattern::tsl_var(TypePattern::var("V"), "SIZE")};
    });
    m.def("type_pattern_tsl", [](PyTypePattern element, PySizePattern size) {
        TypePattern pattern = size.variable
                                  ? TypePattern::tsl_var(std::move(element.pattern), size.name)
                                  : TypePattern::tsl(std::move(element.pattern), size.value);
        return PyTypePattern{std::move(pattern)};
    });
    m.def("type_pattern_tsw", [] {
        return PyTypePattern{TypePattern::tsw_any(ScalarPattern::var("T"))};
    });
    m.def("type_pattern_tsw", [](PyScalarPattern element) {
        return PyTypePattern{TypePattern::tsw_any(std::move(element.pattern))};
    });
    m.def("type_pattern_tsw", [](PyScalarPattern element, std::size_t period, std::size_t min_period) {
        return PyTypePattern{TypePattern::tsw(std::move(element.pattern), period, min_period)};
    }, nb::arg("element"), nb::arg("period"), nb::arg("min_period") = 0);
    m.def("type_pattern_tsb", [] {
        return PyTypePattern{TypePattern::tsb_var("SCHEMA")};
    });
    m.def("type_pattern_tsb", [](const std::string &schema_variable) {
        return PyTypePattern{TypePattern::tsb_var(schema_variable)};
    });
    m.def("type_pattern_tsb_fields", [](nb::list field_names, nb::list children) {
        if (nb::len(field_names) != nb::len(children))
        {
            throw nb::value_error("TSB pattern field names and child patterns must have the same size");
        }
        std::vector<std::string> names;
        std::vector<TypePattern> patterns;
        names.reserve(nb::len(field_names));
        patterns.reserve(nb::len(children));
        for (nb::handle name : field_names) { names.push_back(nb::cast<std::string>(name)); }
        for (nb::handle child : children)
        {
            patterns.push_back(nb::cast<PyTypePattern &>(child).pattern);
        }
        return PyTypePattern{TypePattern::tsb(std::move(names), std::move(patterns))};
    });
    m.def("type_pattern_substitute_scalars", [](PyTypePattern pattern, nb::dict replacements) {
        std::unordered_map<std::string, ScalarPattern> values;
        values.reserve(nb::len(replacements));
        for (auto [name, replacement] : replacements)
        {
            values.emplace(nb::cast<std::string>(name),
                           nb::cast<PyScalarPattern &>(replacement).pattern);
        }
        return PyTypePattern{substitute_scalar_patterns(std::move(pattern.pattern), values)};
    });
    m.def("type_pattern_substitute_sizes", [](PyTypePattern pattern, nb::dict replacements) {
        std::unordered_map<std::string, DimensionPattern> values;
        values.reserve(nb::len(replacements));
        for (auto [name, replacement] : replacements)
        {
            const auto &size = nb::cast<PySizePattern &>(replacement);
            values.emplace(nb::cast<std::string>(name),
                           size.variable ? DimensionPattern::var(size.name)
                                         : DimensionPattern::fixed(size.value));
        }
        return PyTypePattern{substitute_size_patterns(std::move(pattern.pattern), values)};
    });
    m.def("type_pattern_ref", [](PyTypePattern target) {
        return PyTypePattern{TypePattern::ref(std::move(target.pattern))};
    });
    m.def("type_pattern_signal", [] {
        return PyTypePattern{TypePattern::signal()};
    });
    const auto target_input_schemas = [](nb::tuple inputs) {
        std::vector<const TSValueTypeMetaData *> schemas;
        schemas.reserve(nb::len(inputs));
        for (nb::handle input : inputs)
        {
            if (nb::isinstance<PyPort>(input))
            {
                schemas.push_back(nb::cast<PyPort>(input).ref.schema);
            }
            else if (nb::isinstance<PyTsType>(input))
            {
                schemas.push_back(nb::cast<PyTsType>(input).meta);
            }
            else
            {
                Value value = py_to_value(input);
                const auto *scalar = value.schema();
                if (scalar == nullptr)
                {
                    throw nb::type_error(
                        "type target resolution input has no concrete scalar schema");
                }
                // Target inference happens before operator resolution. Model
                // the generic auto-const rule here without wiring a node; the
                // selected overload materializes the scalar exactly once.
                schemas.push_back(TypeRegistry::instance().ts(scalar));
            }
        }
        return schemas;
    };

    const auto selected_key_names = [](nb::object keys) {
        std::vector<std::string> selected;
        if (keys.is_none()) { return selected; }
        for (nb::handle key : keys) { selected.push_back(nb::cast<std::string>(key)); }
        return selected;
    };

    m.def("resolve_convert_target",
          [target_input_schemas, selected_key_names](PyTypePattern pattern, nb::tuple inputs, nb::object keys) {
              std::vector<const TSValueTypeMetaData *> schemas = target_input_schemas(inputs);
              std::vector<std::string> selected = selected_key_names(std::move(keys));
              return PyTsType{stdlib::resolve_convert_target(
                  pattern.pattern,
                  std::span<const TSValueTypeMetaData *const>{schemas.data(), schemas.size()},
                  std::span<const std::string>{selected.data(), selected.size()})};
          },
          nb::arg("pattern"), nb::arg("inputs"), nb::arg("keys") = nb::none());

    m.def("resolve_collect_target",
          [target_input_schemas](PyTypePattern pattern, nb::tuple inputs) {
              std::vector<const TSValueTypeMetaData *> schemas = target_input_schemas(inputs);
              return PyTsType{stdlib::resolve_collect_target(
                  pattern.pattern,
                  std::span<const TSValueTypeMetaData *const>{schemas.data(), schemas.size()})};
          },
          nb::arg("pattern"), nb::arg("inputs"));

    m.def("resolve_combine_target",
          [target_input_schemas](PyTypePattern pattern, nb::tuple inputs) {
              std::vector<const TSValueTypeMetaData *> schemas = target_input_schemas(inputs);
              return PyTsType{stdlib::resolve_combine_target(
                  pattern.pattern,
                  std::span<const TSValueTypeMetaData *const>{schemas.data(), schemas.size()})};
          },
          nb::arg("pattern"), nb::arg("inputs"));

    m.def("resolve_emit_target",
          [target_input_schemas](PyTsType value_ts, nb::tuple inputs) {
              std::vector<const TSValueTypeMetaData *> schemas = target_input_schemas(inputs);
              return PyTsType{stdlib::resolve_emit_target(
                  value_ts.meta,
                  std::span<const TSValueTypeMetaData *const>{schemas.data(), schemas.size()})};
          },
          nb::arg("value_ts"), nb::arg("inputs"));

    m.def("operator_output_is_selective", [](const std::string &name) {
        return OperatorRegistry::instance().output_is_selective(name);
    });
    m.def(
        "operator_carrier_parameters",
        [](const std::string &name) {
            // (names, positions) of the family's type-argument parameters
            // (RFC 0033): a property of the operator, never of its name.
            const auto carriers = OperatorRegistry::instance().carrier_parameters(name);
            return nb::make_tuple(carriers.names, carriers.positions);
        },
        nb::arg("name"));

    m.def("operator_parameter_shape", [](const std::string &name) -> nb::object {
        const auto shape = OperatorRegistry::instance().parameter_shape(name);
        if (!shape.has_value()) { return nb::none(); }

        nb::list parameters;
        for (const OperatorParameterShape &parameter : shape->parameters)
        {
            nb::object fixed = parameter.fixed_ts != nullptr
                                   ? nb::cast(PyTsType{parameter.fixed_ts})
                                   : nb::none();
            parameters.append(nb::make_tuple(
                parameter.name,
                parameter.kind == ParamPattern::Kind::Input,
                parameter.type_variable,
                std::move(fixed)));
        }
        return nb::make_tuple(std::move(parameters), shape->variadic);
    });

    m.def(
        "operator_overload_signatures",
        [](const std::string &name) {
            nb::list signatures;
            for (const OperatorOverloadSignature &signature :
                 OperatorRegistry::instance().overload_signatures(name))
            {
                nb::list parameters;
                for (const OperatorSignatureParameter &parameter : signature.parameters)
                {
                    // The fifth slot names a type argument's carrier form
                    // (RFC 0033) so the public vocabulary can render its
                    // variables as time-series, scalar or size names.
                    nb::object type_argument = nb::none();
                    if (parameter.carrier.has_value())
                    {
                        switch (*parameter.carrier)
                        {
                            case ResolutionKind::TimeSeries: type_argument = nb::str("time-series"); break;
                            case ResolutionKind::Scalar: type_argument = nb::str("scalar"); break;
                            default: type_argument = nb::str("size"); break;
                        }
                    }
                    parameters.append(nb::make_tuple(
                        parameter.name,
                        parameter.kind == ParamPattern::Kind::Input,
                        parameter.type_pattern,
                        parameter.has_default,
                        std::move(type_argument)));
                }
                nb::object kwargs_pattern = signature.kwargs_pattern.has_value()
                                                ? nb::cast(*signature.kwargs_pattern)
                                                : nb::none();
                nb::object output_pattern = signature.output_pattern.has_value()
                                                ? nb::cast(*signature.output_pattern)
                                                : nb::none();
                signatures.append(nb::make_tuple(
                    std::move(parameters),
                    signature.variadic,
                    signature.positional_params,
                    signature.has_kwargs,
                    std::move(kwargs_pattern),
                    signature.has_output,
                    std::move(output_pattern)));
            }
            return signatures;
        },
        nb::arg("name"),
        "Return complete registered overload signatures for documentation and typing tools.");

    // --- python operator overloads (end-game A2): a python @compute_node/
    // @graph registers as an ordinary OperatorImpl{Source::Python} candidate.
    // Matching/ranking/normalisation are ENTIRELY the C++ registry's (the
    // standing ruling); the wire closure calls back into the python wiring
    // function under a borrowed Wiring (the WiredFn trampoline pattern).
    m.def(
        "register_python_overload",
        [](const std::string &name, nb::list params, nb::object output, nb::object wire_fn,
           nb::object resolver_fn, nb::object requires_fn, bool variadic, bool has_kwargs,
           std::optional<std::size_t> positional_params, nb::object kwargs_pattern) {
            OperatorImpl impl;
            impl.name       = name;
            impl.source     = OperatorImpl::Source::Python;
            impl.variadic   = variadic;
            impl.has_kwargs = has_kwargs;
            if (!kwargs_pattern.is_none() && nb::isinstance<PyTypePattern>(kwargs_pattern))
            {
                impl.has_kwargs_pattern = true;
                impl.kwargs_pattern     = nb::cast<PyTypePattern &>(kwargs_pattern).pattern;
            }
            for (nb::handle item : params)
            {
                auto         entry = nb::cast<nb::tuple>(item);
                ParamPattern pp;
                pp.name = nb::cast<std::string>(entry[0]);
                if (nb::isinstance<PyTypePattern>(entry[1]))
                {
                    pp.kind = ParamPattern::Kind::Input;
                    pp.ts   = nb::cast<PyTypePattern &>(entry[1]).pattern;
                }
                else
                {
                    pp.kind   = ParamPattern::Kind::Scalar;
                    pp.scalar = nb::cast<PyScalarPattern &>(entry[1]).pattern;
                }
                if (nb::len(entry) > 2)
                {
                    // The third slot is the DEFAULT: python None on a ts
                    // param = the null source (an EMPTY Value); scalars
                    // convert by inference.
                    nb::handle default_value = entry[2];
                    if (default_value.is_none()) { pp.default_value = Value{}; }
                    else { pp.default_value = py_to_value(default_value); }
                }
                impl.params.push_back(std::move(pp));
            }
            if (!output.is_none())
            {
                impl.has_output = true;
                impl.output     = nb::cast<PyTypePattern &>(output).pattern;
            }
            if (positional_params.has_value()) { impl.positional_params = *positional_params; }
            impl.rank  = operator_dispatch_detail::operator_rank(impl.params);
            impl.label = [&] {
                std::string out = name + "(";
                for (std::size_t i = 0; i < impl.params.size(); ++i)
                {
                    if (i != 0) { out += ", "; }
                    if (impl.variadic && i + 1 == impl.params.size()) { out += "*"; }
                    out += impl.params[i].kind == ParamPattern::Kind::Input
                               ? ts_pattern_to_string(impl.params[i].ts)
                               : scalar_pattern_to_string(impl.params[i].scalar);
                    if (impl.params[i].default_value.has_value()) { out += "=…"; }
                }
                if (impl.has_kwargs)
                {
                    out += impl.params.empty() ? "**kwargs" : ", **kwargs";
                    if (impl.has_kwargs_pattern)
                    {
                        out += ": " + ts_pattern_to_string(impl.kwargs_pattern);
                    }
                }
                out += ") [py]";
                if (impl.has_output) { out += " -> " + ts_pattern_to_string(impl.output); }
                return out;
            }();
            if (!resolver_fn.is_none())
            {
                impl.default_resolver = [resolver_fn](ResolutionMap &map,
                                                      OperatorCallContext context) {
                    nb::gil_scoped_acquire gil;
                    PyResolutionScope      scope;
                    scope.map = map;
                    nb::dict scalars;
                    for (std::size_t i = 0; i < context.args.size() && i < context.params.size(); ++i)
                    {
                        if (context.params[i].kind != ParamPattern::Kind::Input &&
                            context.args[i].kind == WiringArg::Kind::Scalar &&
                            context.args[i].scalar_value.has_value())
                        {
                            scalars[nb::str(context.params[i].name.c_str())] =
                                operator_scalar_to_py(context.args[i].scalar_value.view());
                        }
                    }
                    nb::object resolved = resolver_fn(nb::cast(scope), scalars);
                    map = nb::cast<PyResolutionScope &>(resolved).map;
                };
            }
            if (!requires_fn.is_none())
            {
                impl.requires_predicate = [requires_fn](const ResolutionMap &map,
                                                        OperatorCallContext context) -> bool {
                    nb::gil_scoped_acquire gil;
                    PyResolutionScope      scope;
                    scope.map = map;
                    nb::dict scalars;
                    for (std::size_t i = 0; i < context.args.size() && i < context.params.size(); ++i)
                    {
                        if (context.params[i].kind != ParamPattern::Kind::Input &&
                            context.args[i].kind == WiringArg::Kind::Scalar &&
                            context.args[i].scalar_value.has_value())
                        {
                            scalars[nb::str(context.params[i].name.c_str())] =
                                operator_scalar_to_py(context.args[i].scalar_value.view());
                        }
                    }
                    return nb::cast<bool>(requires_fn(nb::cast(scope), scalars));
                };
            }
            impl.wire = [wire_fn](Wiring &w, const ResolutionMap &map, std::span<const WiringArg> args,
                                  std::span<const std::pair<std::string, WiringPortRef>> kwargs)
                -> OperatorWireResult {
                nb::gil_scoped_acquire gil;
                nb::list               py_args;
                for (const WiringArg &arg : args)
                {
                    if (arg.kind == WiringArg::Kind::TimeSeries)
                    {
                        // An unwired ts default (null source) crosses as None;
                        // the python node wires its own nothing-source.
                        if (arg.port.schema == nullptr) { py_args.append(nb::none()); }
                        else { py_args.append(nb::cast(PyPort{arg.port})); }
                    }
                    else if (!arg.scalar_value.has_value()) { py_args.append(nb::none()); }
                    else { py_args.append(operator_scalar_to_py(arg.scalar_value.view())); }
                }
                nb::dict py_kwargs;
                for (const auto &[kw_name, port] : kwargs)
                {
                    py_kwargs[nb::str(kw_name.c_str())] = nb::cast(PyPort{port});
                }
                nb::object borrowed = nb::cast(PyWiring::borrow(w));
                PyResolutionScope scope;
                scope.map = map;
                nb::object result = wire_fn(borrowed, nb::tuple(py_args), py_kwargs, nb::cast(scope));
                if (result.is_none()) { return OperatorWireResult{}; }
                return OperatorWireResult{true, Port<void>{w, nb::cast<PyPort &>(result).ref}};
            };
            OperatorRegistry::instance().register_overload(std::move(impl));
        },
        nb::arg("name"), nb::arg("params"), nb::arg("output").none(), nb::arg("wire_fn"),
        nb::arg("resolver_fn").none() = nb::none(), nb::arg("requires_fn").none() = nb::none(),
        nb::arg("variadic") = false,
        nb::arg("has_kwargs") = false, nb::arg("positional_params") = nb::none(),
        nb::arg("kwargs_pattern").none() = nb::none());
    }
}  // namespace hgraph::python_bridge
