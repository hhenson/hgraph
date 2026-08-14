#ifndef HGRAPH_LIB_STD_OPERATORS_IMPL_TEMPORAL_IMPL_H
#define HGRAPH_LIB_STD_OPERATORS_IMPL_TEMPORAL_IMPL_H

#include <hgraph/lib/std/operators/temporal.h>

#include <fmt/format.h>
#include <hgraph/runtime/node_scheduler.h>
#include <hgraph/runtime/global_state.h>
#include <hgraph/types/operator_dispatch.h>
#include <hgraph/types/primitive_types.h>
#include <hgraph/types/static_node.h>
#include <hgraph/types/static_schema.h>
#include <hgraph/types/temporal.h>

#include <chrono>
#include <cstddef>
#include <string>

namespace hgraph::stdlib
{
    struct day_of_month_impl
    {
        static void eval(In<"ts", TS<Date>> ts, Out<TS<Int>> out)
        {
            out.set(static_cast<Int>(static_cast<unsigned>(ts.value().day())));
        }
    };

    struct weekday_impl
    {
        static void eval(In<"ts", TS<Date>> ts, Out<TS<Int>> out)
        {
            // python date.weekday(): Monday == 0.
            const std::chrono::weekday wd{std::chrono::sys_days{ts.value()}};
            out.set(static_cast<Int>(wd.iso_encoding() - 1));
        }
    };

    struct isoweekday_impl
    {
        static void eval(In<"ts", TS<Date>> ts, Out<TS<Int>> out)
        {
            // python date.isoweekday(): Monday == 1.
            const std::chrono::weekday wd{std::chrono::sys_days{ts.value()}};
            out.set(static_cast<Int>(wd.iso_encoding()));
        }
    };

    struct sub_date_timedelta_impl
    {
        static constexpr auto name = "sub_date_timedelta";

        static void eval(In<"lhs", TS<Date>> lhs, In<"rhs", TS<TimeDelta>> rhs, Out<TS<Date>> out)
        {
            out.set(checked_add_days(
                lhs.value(),
                -std::chrono::floor<std::chrono::days>(
                    rhs.value()).count()));
        }
    };

    struct at_zone_impl
    {
        static void eval(In<"instant", TS<Instant>> instant,
                         In<"zone", TS<ZoneId>> zone,
                         GlobalStateView state,
                         Out<TS<ZonedDateTime>> out)
        {
            out.set(hgraph::at_zone(instant.value(), zone.value(),
                                    time_zone_provider(state)));
        }
    };

    template <AmbiguousTimePolicy Ambiguous,
              NonexistentTimePolicy Nonexistent>
    struct resolve_civil_impl
    {
        static auto defaults()
        {
            return std::tuple{arg<"ambiguous">(AmbiguousTimePolicy::Reject),
                              arg<"nonexistent">(NonexistentTimePolicy::Reject)};
        }

        static bool requires_(const ResolutionMap &,
                              OperatorCallContext context)
        {
            const auto *ambiguous =
                context.scalar_as<AmbiguousTimePolicy>("ambiguous");
            const auto *nonexistent =
                context.scalar_as<NonexistentTimePolicy>("nonexistent");
            return ambiguous != nullptr && nonexistent != nullptr &&
                   *ambiguous == Ambiguous &&
                   *nonexistent == Nonexistent;
        }

        static void eval(
            In<"local", TS<CivilDateTime>> local,
            In<"zone", TS<ZoneId>> zone,
            Scalar<"ambiguous", AmbiguousTimePolicy>,
            Scalar<"nonexistent", NonexistentTimePolicy>,
            GlobalStateView state, Out<TS<ZonedDateTime>> out)
        {
            out.set(hgraph::resolve(
                local.value(), zone.value(), time_zone_provider(state),
                Ambiguous, Nonexistent));
        }
    };

    struct convert_zone_impl
    {
        static void eval(In<"value", TS<ZonedDateTime>> value,
                         In<"zone", TS<ZoneId>> zone,
                         GlobalStateView state,
                         Out<TS<ZonedDateTime>> out)
        {
            out.set(hgraph::convert_zone(
                value.value(), zone.value(), time_zone_provider(state)));
        }
    };

    struct to_instant_impl
    {
        static void eval(In<"value", TS<ZonedDateTime>> value,
                         Out<TS<Instant>> out)
        {
            out.set(value.value().instant());
        }
    };

    struct to_civil_impl
    {
        static void eval(In<"value", TS<ZonedDateTime>> value,
                         Out<TS<CivilDateTime>> out)
        {
            out.set(value.value().civil());
        }
    };

    template <typename Range, typename ValueType>
    struct range_contains_value_impl
    {
        static void eval(In<"range", TS<Range>> range,
                         In<"value", TS<ValueType>> value,
                         Out<TS<Bool>> out)
        {
            out.set(range.value().contains(value.value()));
        }
    };

    template <typename Range>
    struct range_contains_range_impl
    {
        static void eval(In<"range", TS<Range>> range,
                         In<"value", TS<Range>> value,
                         Out<TS<Bool>> out)
        {
            out.set(range.value().contains(value.value()));
        }
    };

    template <typename Range>
    struct range_intersection_impl
    {
        static void eval(In<"lhs", TS<Range>> lhs,
                         In<"rhs", TS<Range>> rhs, Out<TS<Range>> out)
        {
            out.set(lhs.value().intersection(rhs.value()));
        }
    };

    template <typename Range, int Mode>
    struct range_relation_impl
    {
        static void eval(In<"lhs", TS<Range>> lhs,
                         In<"rhs", TS<Range>> rhs,
                         Out<TS<Bool>> out)
        {
            if constexpr (Mode == 0)
            {
                out.set(lhs.value().overlaps(rhs.value()));
            }
            else if constexpr (Mode == 1)
            {
                out.set(lhs.value().touches(rhs.value()));
            }
            else if constexpr (Mode == 2)
            {
                out.set(lhs.value().adjacent(rhs.value()));
            }
            else
            {
                out.set(lhs.value().mergeable(rhs.value()));
            }
        }
    };

    template <typename Range, typename RangeSet>
    struct range_difference_impl
    {
        static void eval(In<"lhs", TS<Range>> lhs,
                         In<"rhs", TS<Range>> rhs,
                         Out<TS<RangeSet>> out)
        {
            out.set(lhs.value().difference(rhs.value()));
        }
    };

    template <typename Range, typename RangeSet>
    struct range_union_impl
    {
        static void eval(In<"lhs", TS<Range>> lhs,
                         In<"rhs", TS<Range>> rhs,
                         Out<TS<RangeSet>> out)
        {
            out.set(lhs.value().set_union(rhs.value()));
        }
    };

    template <typename Range>
    struct range_merge_impl
    {
        static void eval(In<"lhs", TS<Range>> lhs,
                         In<"rhs", TS<Range>> rhs, Out<TS<Range>> out)
        {
            if (const auto value = lhs.value().merge(rhs.value()))
            {
                out.set(*value);
            }
        }
    };

    template <typename Range>
    struct range_hull_impl
    {
        static void eval(In<"lhs", TS<Range>> lhs,
                         In<"rhs", TS<Range>> rhs,
                         Out<TS<Range>> out)
        {
            out.set(lhs.value().hull(rhs.value()));
        }
    };

    struct instant_range_shift_impl
    {
        static auto defaults()
        {
            return std::tuple{arg<"month_end_policy">(MonthEndPolicy::Reject)};
        }

        static void eval(
            In<"range", TS<InstantRange>> range,
            In<"delta", TS<Duration>> delta,
            Scalar<"month_end_policy", MonthEndPolicy>,
            Out<TS<InstantRange>> out)
        {
            out.set(shift(range.value(), delta.value()));
        }
    };

    template <MonthEndPolicy Policy>
    struct civil_date_range_shift_impl
    {
        static auto defaults()
        {
            return std::tuple{arg<"month_end_policy">(MonthEndPolicy::Reject)};
        }

        static bool requires_(const ResolutionMap &,
                              OperatorCallContext context)
        {
            const auto *selected =
                context.scalar_as<MonthEndPolicy>(
                    "month_end_policy");
            return selected != nullptr && *selected == Policy;
        }

        static void eval(
            In<"range", TS<CivilDateRange>> range,
            In<"delta", TS<Period>> delta,
            Scalar<"month_end_policy", MonthEndPolicy>,
            Out<TS<CivilDateRange>> out)
        {
            out.set(shift(range.value(), delta.value(), Policy));
        }
    };

    struct instant_range_extent_impl
    {
        static void eval(In<"range", TS<InstantRange>> range,
                         Out<TS<Duration>> out)
        {
            if (const auto value = extent(range.value()))
            {
                out.set(*value);
            }
        }
    };

    template <typename ValueType, int Mode>
    struct quantize_impl
    {
        static void eval(In<"value", TS<ValueType>> value,
                         In<"quantum", TS<Duration>> quantum,
                         Out<TS<ValueType>> out)
        {
            if constexpr (Mode < 0)
            {
                out.set(hgraph::floor(value.value(), quantum.value()));
            }
            else if constexpr (Mode > 0)
            {
                out.set(hgraph::ceil(value.value(), quantum.value()));
            }
            else
            {
                out.set(hgraph::round(value.value(), quantum.value()));
            }
        }
    };

    template <int Mode>
    struct quantize_instant_impl
    {
        static auto defaults()
        {
            return std::tuple{arg<"origin">(Instant{})};
        }

        static void eval(In<"value", TS<Instant>> value,
                         In<"quantum", TS<Duration>> quantum,
                         Scalar<"origin", Instant> origin,
                         Out<TS<Instant>> out)
        {
            if constexpr (Mode < 0)
            {
                out.set(hgraph::floor(
                    value.value(), quantum.value(), origin.value()));
            }
            else if constexpr (Mode > 0)
            {
                out.set(hgraph::ceil(
                    value.value(), quantum.value(), origin.value()));
            }
            else
            {
                out.set(hgraph::round(
                    value.value(), quantum.value(), origin.value()));
            }
        }
    };

    struct temporal_bucket_impl
    {
        static auto defaults()
        {
            return std::tuple{arg<"origin">(Instant{})};
        }

        static void eval(In<"value", TS<Instant>> value,
                         In<"width", TS<Duration>> width,
                         Scalar<"origin", Instant> origin,
                         Out<TS<InstantRange>> out)
        {
            out.set(bucket(
                value.value(), width.value(), origin.value()));
        }
    };

    struct isoformat_impl
    {
        static constexpr auto name = "isoformat";

        static void eval(In<"ts", TS<Date>> ts, Out<TS<Str>> out)
        {
            const auto d = ts.value();
            out.set(fmt::format("{:04}-{:02}-{:02}", static_cast<int>(d.year()),
                                static_cast<unsigned>(d.month()), static_cast<unsigned>(d.day())));
        }
    };

    namespace time_range_detail
    {
        /** Publish + schedule for the [start, end] window at ``et``. */
        inline CmpResult classify_and_schedule(DateTime start, DateTime end, DateTime et,
                                               const NodeScheduler &scheduler)
        {
            if (start > end)
            {
                throw std::invalid_argument("evaluation_time_in_range: start must be before end");
            }
            if (et < start)
            {
                scheduler.schedule(start, "_next");
                return CmpResult::LT;
            }
            if (et <= end)
            {
                scheduler.schedule(end + MIN_TD, "_next");
                return CmpResult::EQ;
            }
            // Past the window: clear any pending boundary wake-up (the end
            // may have moved backwards).
            scheduler.un_schedule("_next");
            return CmpResult::GT;
        }

        [[nodiscard]] inline DateTime date_at(Date date, Time at) noexcept
        {
            return DateTime{std::chrono::sys_days{date}.time_since_epoch() +
                            std::chrono::microseconds{at.microseconds}};
        }
    }  // namespace time_range_detail

    struct evaluation_time_in_range_datetime_impl
    {
        static constexpr auto name = "evaluation_time_in_range_datetime";

        static void eval(In<"start_time", TS<DateTime>> start_time, In<"end_time", TS<DateTime>> end_time,
                         NodeScheduler scheduler, DateTime now, Out<TS<CmpResult>> out)
        {
            out.set(time_range_detail::classify_and_schedule(start_time.value(), end_time.value(), now, scheduler));
        }
    };

    struct evaluation_time_in_range_date_impl
    {
        static constexpr auto name = "evaluation_time_in_range_date";

        static void eval(In<"start_time", TS<Date>> start_time, In<"end_time", TS<Date>> end_time,
                         NodeScheduler scheduler, DateTime now, Out<TS<CmpResult>> out)
        {
            // hgraph: the date window spans [start 00:00:00.000000,
            // end 23:59:59.999999].
            const DateTime start = time_range_detail::date_at(start_time.value(), Time{});
            const DateTime end   = time_range_detail::date_at(end_time.value(), time_of_day(23, 59, 59, 999999));
            out.set(time_range_detail::classify_and_schedule(start, end, now, scheduler));
        }
    };

    struct evaluation_time_in_range_time_impl
    {
        static constexpr auto name = "evaluation_time_in_range_time";

        static void eval(In<"start_time", TS<Time>> start_time, In<"end_time", TS<Time>> end_time,
                         NodeScheduler scheduler, DateTime now, Out<TS<CmpResult>> out)
        {
            // A DAILY-recurring window, possibly wrapping midnight (hgraph's
            // time overload; after the window it reports LT for the NEXT
            // day's window and schedules its start).
            const Date today = Date{std::chrono::floor<std::chrono::days>(now)};
            DateTime   start = time_range_detail::date_at(today, start_time.value());
            DateTime   end   = time_range_detail::date_at(today, end_time.value());
            constexpr auto day = std::chrono::days{1};
            if (start > end)
            {
                if (now < end) { start -= day; }
                else { end += day; }
            }
            if (now < start)
            {
                scheduler.schedule(start, "_next");
                out.set(CmpResult::LT);
            }
            else if (now <= end)
            {
                scheduler.schedule(end + MIN_TD, "_next");
                out.set(CmpResult::EQ);
            }
            else
            {
                scheduler.schedule(start + day, "_next");
                out.set(CmpResult::LT);
            }
        }
    };

    // ---- timedelta attributes (python's normalized (days, seconds,
    //      microseconds) decomposition: days floors toward -inf, the
    //      remainders are non-negative) ----

    struct days_timedelta_impl
    {
        static void eval(In<"ts", TS<TimeDelta>> ts, Out<TS<Int>> out)
        {
            out.set(static_cast<Int>(std::chrono::floor<std::chrono::days>(ts.value()).count()));
        }
    };

    struct seconds_timedelta_impl
    {
        static void eval(In<"ts", TS<TimeDelta>> ts, Out<TS<Int>> out)
        {
            const TimeDelta remainder = ts.value() - std::chrono::floor<std::chrono::days>(ts.value());
            out.set(static_cast<Int>(std::chrono::floor<std::chrono::seconds>(remainder).count()));
        }
    };

    struct microseconds_timedelta_impl
    {
        static void eval(In<"ts", TS<TimeDelta>> ts, Out<TS<Int>> out)
        {
            const TimeDelta remainder = ts.value() - std::chrono::floor<std::chrono::seconds>(ts.value());
            out.set(static_cast<Int>(remainder.count()));
        }
    };

    struct total_seconds_timedelta_impl
    {
        static void eval(In<"ts", TS<TimeDelta>> ts, Out<TS<Float>> out)
        {
            out.set(std::chrono::duration<Float>(ts.value()).count());
        }
    };

    // ---- datetime component extraction (the datetime overloads of the
    //      date attribute operators plus the time-of-day attributes) ----

    namespace datetime_parts_detail
    {
        [[nodiscard]] inline Date civil_date(DateTime when)
        {
            return Date{std::chrono::floor<std::chrono::days>(when)};
        }

        [[nodiscard]] inline std::int64_t microseconds_of_day(DateTime when)
        {
            return time_of_day(when).microseconds;
        }
    }  // namespace datetime_parts_detail

    struct year_datetime_impl
    {
        static void eval(In<"ts", TS<DateTime>> ts, Out<TS<Int>> out)
        {
            out.set(static_cast<Int>(static_cast<int>(datetime_parts_detail::civil_date(ts.value()).year())));
        }
    };

    struct month_datetime_impl
    {
        static void eval(In<"ts", TS<DateTime>> ts, Out<TS<Int>> out)
        {
            out.set(static_cast<Int>(static_cast<unsigned>(datetime_parts_detail::civil_date(ts.value()).month())));
        }
    };

    struct day_datetime_impl
    {
        static void eval(In<"ts", TS<DateTime>> ts, Out<TS<Int>> out)
        {
            out.set(static_cast<Int>(static_cast<unsigned>(datetime_parts_detail::civil_date(ts.value()).day())));
        }
    };

    struct weekday_datetime_impl
    {
        static void eval(In<"ts", TS<DateTime>> ts, Out<TS<Int>> out)
        {
            const std::chrono::weekday wd{std::chrono::floor<std::chrono::days>(ts.value())};
            out.set(static_cast<Int>(wd.iso_encoding() - 1));
        }
    };

    struct isoweekday_datetime_impl
    {
        static void eval(In<"ts", TS<DateTime>> ts, Out<TS<Int>> out)
        {
            const std::chrono::weekday wd{std::chrono::floor<std::chrono::days>(ts.value())};
            out.set(static_cast<Int>(wd.iso_encoding()));
        }
    };

    struct hour_datetime_impl
    {
        static void eval(In<"ts", TS<DateTime>> ts, Out<TS<Int>> out)
        {
            out.set(static_cast<Int>(datetime_parts_detail::microseconds_of_day(ts.value()) / 3'600'000'000));
        }
    };

    struct minute_datetime_impl
    {
        static void eval(In<"ts", TS<DateTime>> ts, Out<TS<Int>> out)
        {
            out.set(static_cast<Int>(datetime_parts_detail::microseconds_of_day(ts.value()) / 60'000'000 % 60));
        }
    };

    struct second_datetime_impl
    {
        static void eval(In<"ts", TS<DateTime>> ts, Out<TS<Int>> out)
        {
            out.set(static_cast<Int>(datetime_parts_detail::microseconds_of_day(ts.value()) / 1'000'000 % 60));
        }
    };

    struct microsecond_datetime_impl
    {
        static void eval(In<"ts", TS<DateTime>> ts, Out<TS<Int>> out)
        {
            out.set(static_cast<Int>(datetime_parts_detail::microseconds_of_day(ts.value()) % 1'000'000));
        }
    };

    struct timestamp_datetime_impl
    {
        static void eval(In<"ts", TS<DateTime>> ts, Out<TS<Float>> out)
        {
            // Python's datetime.timestamp(): fractional epoch seconds.
            out.set(std::chrono::duration<Float>(ts.value().time_since_epoch()).count());
        }
    };

    // ---- time-of-day attributes over TS<Time> (upstream's time table) ----

    struct hour_time_impl
    {
        static void eval(In<"ts", TS<Time>> ts, Out<TS<Int>> out)
        {
            out.set(static_cast<Int>(ts.value().microseconds / 3'600'000'000));
        }
    };

    struct minute_time_impl
    {
        static void eval(In<"ts", TS<Time>> ts, Out<TS<Int>> out)
        {
            out.set(static_cast<Int>(ts.value().microseconds / 60'000'000 % 60));
        }
    };

    struct second_time_impl
    {
        static void eval(In<"ts", TS<Time>> ts, Out<TS<Int>> out)
        {
            out.set(static_cast<Int>(ts.value().microseconds / 1'000'000 % 60));
        }
    };

    struct microsecond_time_impl
    {
        static void eval(In<"ts", TS<Time>> ts, Out<TS<Int>> out)
        {
            out.set(static_cast<Int>(ts.value().microseconds % 1'000'000));
        }
    };

    struct month_of_year_impl
    {
        static void eval(In<"ts", TS<Date>> ts, Out<TS<Int>> out)
        {
            out.set(static_cast<Int>(static_cast<unsigned>(ts.value().month())));
        }
    };

    struct year_impl
    {
        static void eval(In<"ts", TS<Date>> ts, Out<TS<Int>> out)
        {
            out.set(static_cast<Int>(static_cast<int>(ts.value().year())));
        }
    };

    struct explode_date_impl
    {
        static void eval(In<"ts", TS<Date>> ts, Out<TSL<TS<Int>, 3>> out)
        {
            const Date value = ts.value();
            set_if_changed(out, 0, static_cast<Int>(static_cast<int>(value.year())));
            set_if_changed(out, 1, static_cast<Int>(static_cast<unsigned>(value.month())));
            set_if_changed(out, 2, static_cast<Int>(static_cast<unsigned>(value.day())));
        }

      private:
        static void set_if_changed(const Out<TSL<TS<Int>, 3>> &out, std::size_t index, Int value)
        {
            auto child = out[index];
            if (!child.valid() || child.value().template checked_as<Int>() != value) { child.set(value); }
        }
    };

    struct valid_impl
    {
        static constexpr bool schedule_on_start = true;

        static bool requires_(const ResolutionMap &, OperatorCallContext context)
        {
            // REF-shaped sources take the reference-aware overload below.
            return context.args.empty() || context.args[0].kind != WiringArg::Kind::TimeSeries ||
                   context.args[0].port.schema == nullptr ||
                   context.args[0].port.schema->kind != TSTypeKind::REF;
        }

        static void eval(In<"ts", TsVar<"S">, InputValidity::Unchecked> ts, Out<TS<Bool>> out)
        {
            // hgraph parity: ticks only when the answer CHANGES.
            const Bool value = ts.valid();
            const auto &erased = static_cast<const TSOutputView &>(out);
            if (erased.valid() && erased.data_view().value().checked_as<Bool>() == value) { return; }
            out.set(value);
        }
    };

    namespace valid_ref_detail
    {
        /** hgraph's UNBOUND-input validity is KIND-dependent: a leaf reads
            False, a dynamic container (TSD / TSS / dynamic TSL) reads
            vacuously TRUE - zero children, all valid. An EMPTY reference
            therefore leaves valid(tsd) True (pinned by
            test_tsd_validity_rebind). */
        [[nodiscard]] inline bool empty_reference_validity(const TSValueTypeMetaData *target) noexcept
        {
            if (target == nullptr) { return false; }
            switch (target->kind)
            {
                case TSTypeKind::TSD:
                case TSTypeKind::TSS: return true;
                case TSTypeKind::TSL: return target->fixed_size() == 0;
                default: return false;
            }
        }
    }  // namespace valid_ref_detail

    /** valid over a REFERENCE source (hgraph's valid_impl shape): the ACTIVE
        REF input observes retargets (an emptied reference is a value tick -
        UNBIND IS SILENT on the deref'd side, linking_strategies.rst); the
        deref'd input wakes the node when the bound target's validity
        transitions. Ticks only when the answer changes. */
    struct valid_ref_impl
    {
        static constexpr auto name = "valid_ref";

        static void eval(In<"ts", REF<TsVar<"S">>, InputValidity::Unchecked> ts,
                         In<"ts_value", TsVar<"S">, InputValidity::Unchecked> ts_value,
                         DateTime now,
                         Out<TS<Bool>> out)
        {
            // Active reference inputs force a start evaluation
            // (static_node.h); publish nothing until the reference first
            // arrives — upstream's valid_impl requires the REF valid, so a
            // REF-valued source that never ticks produces NO output.
            if (!ts.valid()) { return; }
            Bool value = false;
            {
                const auto reference = ts.value();
                if (reference.is_empty())
                {
                    value = valid_ref_detail::empty_reference_validity(ts.base().schema()->referenced_ts());
                }
                else { value = reference.is_valid(now) || ts_value.valid(); }
            }
            const auto &erased = static_cast<const TSOutputView &>(out);
            if (erased.valid() && erased.data_view().value().checked_as<Bool>() == value) { return; }
            out.set(value);
        }
    };

    /** valid(REF-shaped port): compose the REF + deref'd pair onto
        valid_ref_impl (the race values-input pattern). */
    struct valid_ref_graph_impl
    {
        static constexpr auto name = "valid";

        static bool requires_(const ResolutionMap &, OperatorCallContext context)
        {
            return !context.args.empty() && context.args[0].kind == WiringArg::Kind::TimeSeries &&
                   context.args[0].port.schema != nullptr &&
                   context.args[0].port.schema->kind == TSTypeKind::REF;
        }

        static void resolve_default_types(ResolutionMap &resolution, OperatorCallContext)
        {
            if (resolution.find_ts("__out__") != nullptr) { return; }
            resolution.bind_ts("__out__", TypeRegistry::instance().ts(scalar_descriptor<Bool>::value_meta()));
        }

        static WiringPortRef compose(Wiring &w, NamedPort<"ts", REF<TsVar<"S">>> ts)
        {
            WiringPortRef ref_port = ts.erased();
            WiringPortRef deref    = ref_port;
            deref.schema           = ref_port.schema->referenced_ts();   // the descriptive-schema patch
            return wire<valid_ref_impl>(w, Port<void>{w, std::move(ref_port)}, Port<void>{w, std::move(deref)})
                .erased();
        }
    };

    struct modified_impl
    {
        static constexpr bool schedule_on_start = true;

        static void eval(In<"ts", SIGNAL, InputValidity::Unchecked> ts, NodeScheduler scheduler, Out<TS<Bool>> out)
        {
            const Bool is_modified = ts.modified();
            out.set(is_modified);
            if (is_modified) { scheduler.schedule(MIN_TD, std::string{"reset"}); }
        }
    };

    struct last_modified_time_impl
    {
        static void eval(In<"ts", SIGNAL, InputValidity::Unchecked> ts, Out<TS<DateTime>> out)
        {
            out.set(ts.last_modified_time());
        }
    };

    struct last_modified_wall_clock_time_impl
    {
        static void eval(In<"ts", SIGNAL, InputValidity::Unchecked> ts, EvaluationClockView clock,
                         Out<TS<DateTime>> out)
        {
            static_cast<void>(ts);
            out.set(clock.now());
        }
    };

    struct last_modified_date_impl
    {
        static void eval(In<"ts", SIGNAL, InputValidity::Unchecked> ts, Out<TS<Date>> out)
        {
            out.set(Date{std::chrono::floor<std::chrono::days>(ts.last_modified_time())});
        }
    };

    void register_temporal_operators();
}  // namespace hgraph::stdlib

#endif  // HGRAPH_LIB_STD_OPERATORS_IMPL_TEMPORAL_IMPL_H
