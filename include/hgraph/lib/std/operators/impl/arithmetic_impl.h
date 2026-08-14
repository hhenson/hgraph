#ifndef HGRAPH_LIB_STD_OPERATORS_IMPL_ARITHMETIC_IMPL_H
#define HGRAPH_LIB_STD_OPERATORS_IMPL_ARITHMETIC_IMPL_H

#include <hgraph/lib/std/lifted_kernels.h>
#include <hgraph/lib/std/operators/arithmetic.h>
#include <hgraph/lib/std/operators/container.h>
#include <hgraph/lib/std/operators/logical.h>
#include <hgraph/lib/std/operators/collection.h>
#include <hgraph/types/value/value_builder.h>
#include <hgraph/types/value_callable.h>
#include <hgraph/lib/std/operators/impl/higher_order_impl.h>   // add_ / sub_ / mul_ / div_ / DivideByZero
#include <hgraph/lib/std/operators/impl/tsb_itemwise_impl.h>
#include <hgraph/lib/std/operators/impl/tsl_itemwise_impl.h>
#include <hgraph/runtime/global_state.h>
#include <hgraph/types/metadata/type_realization.h>
#include <hgraph/types/operator_dispatch.h>
#include <hgraph/types/lift.h>
#include <hgraph/types/primitive_types.h>
#include <hgraph/types/static_node.h>
#include <hgraph/types/static_schema.h>
#include <hgraph/types/temporal.h>
#include <hgraph/types/value/visitor.h>

#include <algorithm>
#include <chrono>
#include <cmath>
#include <cstdio>
#include <cstdlib>
#include <limits>
#include <optional>
#include <stdexcept>
#include <string>
#include <type_traits>
#include <utility>

namespace hgraph::stdlib
{
    /**
     * Implementations + registration for the arithmetic operators (``add_`` / ``sub_`` /
     * ``mul_`` / ``div_`` / friends). The abstract operator markers are in
     * ``<hgraph/lib/std/operators/arithmetic.h>``; this file provides a (still small) set
     * of concrete overloads and ``register_arithmetic_operators`` to register them.
     */

    struct repeat_string_right
    {
        static void eval(In<"lhs", TS<Str>> lhs, In<"rhs", TS<Int>> rhs, Out<TS<Str>> out)
        {
            out.set(repeat_string(lhs.value(), rhs.value()));
        }

        [[nodiscard]] static Str repeat_string(const Str &value, Int count)
        {
            if (count <= 0) { return {}; }
            Str result;
            result.reserve(value.size() * static_cast<std::size_t>(count));
            for (Int i = 0; i < count; ++i) { result += value; }
            return result;
        }
    };

    struct repeat_string_left
    {
        static void eval(In<"lhs", TS<Int>> lhs, In<"rhs", TS<Str>> rhs, Out<TS<Str>> out)
        {
            out.set(repeat_string_right::repeat_string(rhs.value(), lhs.value()));
        }
    };

    /** ``round_(ts, n_digits)`` — round to decimal places with python's
        correctly-rounded semantics (the printf round-trip renders the
        correctly-rounded decimal, exactly what CPython's round does for
        non-tie doubles). */
    struct round_float_impl
    {
        static void eval(In<"ts", TS<Float>> ts, In<"n_digits", TS<Int>> n_digits, Out<TS<Float>> out)
        {
            char buffer[64];
            const int digits = static_cast<int>(std::clamp<Int>(n_digits.value(), Int{0}, Int{40}));
            std::snprintf(buffer, sizeof buffer, "%.*f", digits, ts.value());
            out.set(std::strtod(buffer, nullptr));
        }
    };

    /** ``Date + TimeDelta -> Date`` — Python date arithmetic uses the
        timedelta's normalized, floor-based day component. */
    struct add_date_timedelta
    {
        static void eval(In<"lhs", TS<Date>> lhs, In<"rhs", TS<TimeDelta>> rhs,
                         Out<TS<Date>> out)
        {
            out.set(checked_add_days(
                lhs.value(),
                std::chrono::floor<std::chrono::days>(
                    rhs.value()).count()));
        }
    };

    /** ``Date - Date -> TimeDelta`` — the (whole-day) span between two calendar dates. */
    struct sub_dates
    {
        static void eval(In<"lhs", TS<Date>> lhs, In<"rhs", TS<Date>> rhs,
                         Out<TS<TimeDelta>> out)
        {
            out.set(hgraph::checked_subtract(lhs.value(), rhs.value()));
        }
    };

    // ---- div_ : true division with a divide-by-zero policy ----

    /**
     * Apply the ``DivideByZero`` policy. Returns the quotient, or — for a zero divisor —
     * the policy's value; ``NoTick`` returns ``nullopt`` (no tick this cycle) and ``Error``
     * raises.
     */
    [[nodiscard]] inline std::optional<Float> divide_with_policy(Float lhs, Float rhs, DivideByZero on_zero)
    {
        if (rhs != Float{0}) { return lhs / rhs; }
        switch (on_zero)
        {
            case DivideByZero::Nan: return std::numeric_limits<Float>::quiet_NaN();
            case DivideByZero::Inf: return std::numeric_limits<Float>::infinity();
            case DivideByZero::Zero: return Float{0};
            case DivideByZero::One: return Float{1};
            case DivideByZero::NoTick: return std::nullopt;
            case DivideByZero::Error:
            default: throw std::domain_error("div_: division by zero");
        }
    }

    [[nodiscard]] inline std::optional<Float> floor_divide_with_policy(Float lhs, Float rhs, DivideByZero on_zero)
    {
        if (rhs != Float{0}) { return std::floor(lhs / rhs); }
        switch (on_zero)
        {
            case DivideByZero::Nan: return std::numeric_limits<Float>::quiet_NaN();
            case DivideByZero::Inf: return std::numeric_limits<Float>::infinity();
            case DivideByZero::Zero: return Float{0};
            case DivideByZero::One: return Float{1};
            case DivideByZero::NoTick: return std::nullopt;
            case DivideByZero::Error:
            default: throw std::domain_error("floordiv_: division by zero");
        }
    }

    [[nodiscard]] inline std::optional<Float> modulo_with_policy(Float lhs, Float rhs, DivideByZero on_zero)
    {
        if (rhs != Float{0}) { return lhs - std::floor(lhs / rhs) * rhs; }
        switch (on_zero)
        {
            case DivideByZero::Nan: return std::numeric_limits<Float>::quiet_NaN();
            case DivideByZero::Inf: return std::numeric_limits<Float>::infinity();
            case DivideByZero::NoTick: return std::nullopt;
            case DivideByZero::Error:
            case DivideByZero::Zero:
            case DivideByZero::One:
            default: throw std::domain_error("mod_: division by zero");
        }
    }

    [[nodiscard]] inline std::optional<Float> pow_with_policy(Float lhs, Float rhs, DivideByZero on_zero)
    {
        if (!(lhs == Float{0} && rhs < Float{0})) { return std::pow(lhs, rhs); }
        switch (on_zero)
        {
            case DivideByZero::Nan: return std::numeric_limits<Float>::quiet_NaN();
            case DivideByZero::Inf: return std::numeric_limits<Float>::infinity();
            case DivideByZero::Zero: return Float{0};
            case DivideByZero::One: return Float{1};
            case DivideByZero::NoTick: return std::nullopt;
            case DivideByZero::Error:
            default: throw std::domain_error("pow_: zero cannot be raised to a negative power");
        }
    }

    [[nodiscard]] inline Int floor_divide_int(Int lhs, Int rhs)
    {
        if (rhs == 0) { throw std::domain_error("floordiv_: division by zero"); }
        if (lhs == std::numeric_limits<Int>::min() && rhs == Int{-1})
        {
            throw std::overflow_error("floordiv_: integer overflow");
        }
        Int quotient  = lhs / rhs;
        const Int rem = lhs % rhs;
        if (rem != 0 && ((rem < 0) != (rhs < 0))) { --quotient; }
        return quotient;
    }

    [[nodiscard]] inline Int modulo_int(Int lhs, Int rhs)
    {
        return lhs - floor_divide_int(lhs, rhs) * rhs;
    }

    [[nodiscard]] inline std::pair<Float, Float> divmod_float(Float lhs, Float rhs)
    {
        if (rhs == Float{0}) { throw std::domain_error("divmod_: division by zero"); }
        const Float quotient  = std::floor(lhs / rhs);
        const Float remainder = lhs - quotient * rhs;
        return {quotient, remainder};
    }

    [[nodiscard]] inline std::optional<Int> floor_divide_int_with_policy(Int lhs, Int rhs, DivideByZero on_zero)
    {
        if (rhs != 0) { return floor_divide_int(lhs, rhs); }
        switch (on_zero)
        {
            case DivideByZero::Zero: return Int{0};
            case DivideByZero::One: return Int{1};
            case DivideByZero::NoTick: return std::nullopt;
            case DivideByZero::Error:
            case DivideByZero::Nan:
            case DivideByZero::Inf:
            default: throw std::domain_error("floordiv_: division by zero");
        }
    }

    [[nodiscard]] inline std::optional<Int> modulo_int_with_policy(Int lhs, Int rhs, DivideByZero on_zero)
    {
        if (rhs != 0) { return modulo_int(lhs, rhs); }
        if (on_zero == DivideByZero::NoTick) { return std::nullopt; }
        throw std::domain_error("mod_: division by zero");
    }

    /** ``L / R -> Float`` with an explicit divide-by-zero policy (a wiring-time scalar). */
    template <typename L, typename R>
    struct div_numbers
    {
        static auto defaults()
        {
            return std::tuple{arg<"divide_by_zero">(DivideByZero::Error)};
        }

        static void eval(In<"lhs", TS<L>> lhs, In<"rhs", TS<R>> rhs,
                         Scalar<"divide_by_zero", DivideByZero> on_zero, Out<TS<Float>> out)
        {
            if (const std::optional<Float> result =
                    divide_with_policy(static_cast<Float>(lhs.value()), static_cast<Float>(rhs.value()), on_zero.value()))
            {
                out.set(*result);
            }
            // NoTick policy on a zero divisor: leave the output un-ticked (a gap).
        }
    };

    template <typename L, typename R>
    struct floordiv_numbers
    {
        static void eval(In<"lhs", TS<L>> lhs, In<"rhs", TS<R>> rhs,
                         Scalar<"divide_by_zero", DivideByZero> on_zero, Out<TS<Float>> out)
        {
            if (const std::optional<Float> result = floor_divide_with_policy(
                    static_cast<Float>(lhs.value()), static_cast<Float>(rhs.value()), on_zero.value()))
            {
                out.set(*result);
            }
        }
    };

    struct floordiv_ints
    {
        static void eval(In<"lhs", TS<Int>> lhs, In<"rhs", TS<Int>> rhs,
                         Scalar<"divide_by_zero", DivideByZero> on_zero, Out<TS<Int>> out)
        {
            if (const std::optional<Int> result = floor_divide_int_with_policy(lhs.value(), rhs.value(), on_zero.value()))
            {
                out.set(*result);
            }
        }
    };

    template <typename L, typename R>
    struct mod_numbers
    {
        static void eval(In<"lhs", TS<L>> lhs, In<"rhs", TS<R>> rhs,
                         Scalar<"divide_by_zero", DivideByZero> on_zero, Out<TS<Float>> out)
        {
            if (const std::optional<Float> result = modulo_with_policy(
                    static_cast<Float>(lhs.value()), static_cast<Float>(rhs.value()), on_zero.value()))
            {
                out.set(*result);
            }
        }
    };

    struct mod_ints
    {
        static void eval(In<"lhs", TS<Int>> lhs, In<"rhs", TS<Int>> rhs,
                         Scalar<"divide_by_zero", DivideByZero> on_zero, Out<TS<Int>> out)
        {
            if (const std::optional<Int> result = modulo_int_with_policy(lhs.value(), rhs.value(), on_zero.value()))
            {
                out.set(*result);
            }
        }
    };

    struct divmod_ints
    {
        static void eval(In<"lhs", TS<Int>> lhs, In<"rhs", TS<Int>> rhs, Out<TSL<TS<Int>, 2>> out)
        {
            out.set(0, floor_divide_int(lhs.value(), rhs.value()));
            out.set(1, modulo_int(lhs.value(), rhs.value()));
        }
    };

    template <typename L, typename R>
    struct divmod_numbers
    {
        static void eval(In<"lhs", TS<L>> lhs, In<"rhs", TS<R>> rhs, Out<TSL<TS<Float>, 2>> out)
        {
            const auto [quotient, remainder] =
                divmod_float(static_cast<Float>(lhs.value()), static_cast<Float>(rhs.value()));
            out.set(0, quotient);
            out.set(1, remainder);
        }
    };

    template <typename L, typename R>
    struct pow_numbers
    {
        using result_type = lifted_kernel_detail::pow_result_t<L, R>;

        static void eval(In<"lhs", TS<L>> lhs, In<"rhs", TS<R>> rhs,
                         Scalar<"divide_by_zero", DivideByZero> on_zero, Out<TS<result_type>> out)
        {
            if constexpr (std::is_same_v<result_type, Int>)
            {
                const Int base     = lhs.value();
                const Int exponent = rhs.value();
                if (exponent < 0)
                {
                    if (base == 0 && on_zero.value() == DivideByZero::NoTick) { return; }
                    if (base == 0)
                    {
                        throw std::domain_error("pow_: zero cannot be raised to a negative power");
                    }
                    throw std::domain_error("pow_: negative exponent cannot produce an integer result");
                }
                out.set(lifted_kernel_detail::integer_power(base, exponent));
            }
            else if (const std::optional<Float> result =
                         pow_with_policy(static_cast<Float>(lhs.value()),
                                         static_cast<Float>(rhs.value()),
                                         on_zero.value()))
            {
                out.set(*result);
            }
        }
    };

    /** ``TimeDelta / TimeDelta -> Float`` — the ratio of two durations. */
    struct div_timedeltas
    {
        static void eval(In<"lhs", TS<TimeDelta>> lhs, In<"rhs", TS<TimeDelta>> rhs,
                         Out<TS<Float>> out)
        {
            out.set(static_cast<Float>(lhs.value().count()) / static_cast<Float>(rhs.value().count()));
        }
    };

    struct abs_timedelta
    {
        static void eval(In<"ts", TS<TimeDelta>> ts, Out<TS<TimeDelta>> out)
        {
            const TimeDelta value = ts.value();
            out.set(value.count() < 0 ? checked_negate(value) : value);
        }
    };

    namespace arithmetic_impl_detail
    {
        /** Welford accumulator for the running mean/std/var family. */
        struct AggMoments
        {
            Int   count{0};
            Float mean{0.0};
            Float m2{0.0};

            friend bool operator==(const AggMoments &, const AggMoments &) noexcept = default;
        };

        /** Running sum over one series (optionally reset by a bool series). */
        template <typename T>
        struct running_sum_impl
        {
            static constexpr auto name = "sum_unary";

            static void eval(In<"ts", TS<T>> ts, Out<TS<T>> out)
            {
                const T prior = out.valid() ? out.value().template checked_as<T>() : T{};
                out.set(prior + ts.value());
            }
        };

        template <typename T>
        struct running_sum_reset_impl
        {
            static constexpr auto name = "sum_unary_reset";

            static void eval(In<"ts", TS<T>, InputValidity::Unchecked> ts,
                             In<"reset", TS<Bool>, InputValidity::Unchecked> reset, Out<TS<T>> out)
            {
                if (reset.valid() && reset.modified())
                {
                    out.set(ts.modified() ? ts.value() : T{});
                    return;
                }
                if (!ts.modified()) { return; }
                const T prior = out.valid() ? out.value().template checked_as<T>() : T{};
                out.set(prior + ts.value());
            }
        };

        /** Running mean (population) over one series. */
        template <typename T>
        struct running_mean_impl
        {
            static constexpr auto name = "mean_unary";

            static void eval(In<"ts", TS<T>> ts, State<Int> count, Out<TS<Float>> out)
            {
                const Int   n     = count.get() + 1;
                const Float prior = out.valid() ? out.value().checked_as<Float>() : 0.0;
                count.set(n);
                out.set(prior + (static_cast<Float>(ts.value()) - prior) / static_cast<Float>(n));
            }
        };

        /** Running population std / var (Welford). */
        template <typename T, bool Std>
        struct running_moments_impl
        {
            static constexpr auto name = Std ? "std_unary" : "var_unary";

            static void eval(In<"ts", TS<T>> ts, State<AggMoments> state, Out<TS<Float>> out)
            {
                AggMoments  m     = state.get();
                const Float value = static_cast<Float>(ts.value());
                m.count += 1;
                const Float delta = value - m.mean;
                m.mean += delta / static_cast<Float>(m.count);
                m.m2 += delta * (value - m.mean);
                state.set(m);
                const Float variance = m.count > 0 ? m.m2 / static_cast<Float>(m.count) : 0.0;
                out.set(Std ? std::sqrt(variance) : variance);
            }
        };

        /** N-ary sum / mean over the argument series (a fold of add_). */
        template <bool Mean>
        struct multi_sum_impl
        {
            static constexpr auto name = Mean ? "mean_multi" : "sum_multi";

            static bool requires_(const ResolutionMap &, OperatorCallContext context)
            {
                std::size_t ts_count = 0;
                for (const WiringArg &arg : context.args)
                {
                    if (arg.kind == WiringArg::Kind::TimeSeries) { ++ts_count; }
                }
                return ts_count >= 2;   // the unary overloads own single-arg calls
            }

            static void resolve_default_types(ResolutionMap &resolution, OperatorCallContext context)
            {
                const WiringArg *first_ts = nullptr;
                for (const WiringArg &arg : context.args)
                {
                    if (arg.kind == WiringArg::Kind::TimeSeries)
                    {
                        first_ts = &arg;
                        break;
                    }
                }
                if (first_ts == nullptr) { return; }
                if constexpr (Mean)
                {
                    higher_order_impl_detail::bind_graph_output(
                        resolution, TypeRegistry::instance().ts(scalar_descriptor<Float>::value_meta()), "O");
                }
                else
                {
                    higher_order_impl_detail::bind_graph_output(resolution, first_ts->port.schema, "O");
                }
            }

            static WiringPortRef compose(Wiring &w, VarIn<"ts", TsVar<"TS">> ts)
            {
                WiringPortRef acc = ts[0];
                for (std::size_t index = 1; index < ts.size(); ++index)
                {
                    WiringArg lhs_arg;
                    lhs_arg.kind = WiringArg::Kind::TimeSeries;
                    lhs_arg.port = acc;
                    WiringArg rhs_arg;
                    rhs_arg.kind = WiringArg::Kind::TimeSeries;
                    rhs_arg.port = ts[index];
                    std::array<WiringArg, 2> pair{lhs_arg, rhs_arg};
                    acc = wire_operator(w, "add_", pair).output.erased();
                }
                if constexpr (Mean)
                {
                    WiringArg n_arg;
                    n_arg.kind         = WiringArg::Kind::Scalar;
                    n_arg.scalar_value = Value{static_cast<Float>(ts.size())};
                    n_arg.scalar_meta  = n_arg.scalar_value.schema();
                    WiringArg n_const;
                    n_const.kind = WiringArg::Kind::TimeSeries;
                    n_const.port = wire_operator(w, "const", std::array<WiringArg, 1>{n_arg}, true,
                                                 TypeRegistry::instance().ts(scalar_descriptor<Float>::value_meta()))
                                       .output.erased();
                    WiringArg sum_arg;
                    sum_arg.kind = WiringArg::Kind::TimeSeries;
                    sum_arg.port = acc;
                    std::array<WiringArg, 2> pair{sum_arg, n_const};
                    // mean = sum / N (div_ promotes ints to float).
                    acc = wire_operator(w, "div_", pair).output.erased();
                }
                return acc;
            }
        };
    }  // namespace arithmetic_impl_detail

    namespace arithmetic_impl_detail
    {
        [[nodiscard]] inline const ValueTypeMetaData *resolved_t(const ResolutionMap &resolution)
        {
            return resolution.find_scalar("T");
        }

        [[nodiscard]] inline ValueTypeRef element_binding_of(const ValueTypeMetaData *meta)
        {
            const auto binding = value_type_for_active_realization(meta);
            if (binding == nullptr) { throw std::logic_error("container element schema has no binding"); }
            return binding;
        }

        /** Tuple/list concatenation (python's tuple + tuple). */
        struct concat_lists_impl
        {
            static constexpr auto name = "add_lists";

            static bool requires_(const ResolutionMap &resolution, OperatorCallContext)
            {
                const auto *meta = resolved_t(resolution);
                return meta != nullptr && meta->value_kind() == ValueTypeKind::List && !meta->is_mutable();
            }

            static void eval(In<"lhs", TS<ScalarVar<"T">>> lhs, In<"rhs", TS<ScalarVar<"T">>> rhs,
                             Out<TS<ScalarVar<"T">>> out)
            {
                const auto *meta = lhs.base().value().schema();
                ListBuilder builder{element_binding_of(meta->element_type), *meta};
                const auto lhs_values = lhs.base().value().as_list();
                const auto rhs_values = rhs.base().value().as_list();
                for (const ValueView &element : lhs_values) { builder.push_back(element); }
                for (const ValueView &element : rhs_values) { builder.push_back(element); }
                out.apply(builder.build().view());
            }
        };

        /** Remove every homogeneous tuple/list element matching ``rhs``.
            The optional runtime comparator supports heterogeneous rhs types;
            an empty callable uses the element type's native equality ops. */
        struct remove_list_items_impl
        {
            static constexpr auto name = "sub_list_item";

            static bool requires_(const ResolutionMap &, OperatorCallContext context)
            {
                const auto *lhs = ts_value_schema_at(context, 0);
                const auto *rhs = ts_value_schema_at(context, 1);
                if (lhs == nullptr || rhs == nullptr || lhs->value_kind() != ValueTypeKind::List ||
                    lhs->element_type == nullptr)
                {
                    return false;
                }
                const auto *cmp = context.scalar_as<ValueCallable>("cmp");
                return (cmp != nullptr && cmp->valid()) || lhs->element_type == rhs;
            }

            static auto defaults()
            {
                return std::tuple{arg<"cmp">(ValueCallable{})};
            }

            static void eval(In<"lhs", TS<ScalarVar<"T">>> lhs,
                             In<"rhs", TS<ScalarVar<"E">>> rhs,
                             Scalar<"cmp", ValueCallable> cmp,
                             Out<TS<ScalarVar<"T">>> out)
            {
                const ValueView rhs_value = rhs.base().value();
                const ValueCallable &predicate = cmp.value();
                const auto *meta = lhs.base().value().schema();
                ListBuilder builder{element_binding_of(meta->element_type), *meta};
                const auto lhs_values = lhs.base().value().as_list();
                for (const ValueView element : lhs_values)
                {
                    bool matches = false;
                    if (predicate.valid())
                    {
                        const ValueCallArg args[]{ValueCallArg{element}, ValueCallArg{rhs_value}};
                        const Value result = predicate.invoke(
                            std::span<const ValueCallArg>{args}, scalar_descriptor<Bool>::value_meta());
                        if (!result.has_value())
                        {
                            throw std::logic_error("tuple comparator did not return a value");
                        }
                        matches = result.view().checked_as<Bool>();
                    }
                    else { matches = element.equals(rhs_value); }
                    if (!matches) { builder.push_back(element); }
                }
                out.apply(builder.build().view());
            }
        };

        /** frozenset difference / intersection / union / symmetric difference. */
        enum class SetOpKind : std::uint8_t { Difference, Intersection, Union, SymmetricDifference };

        template <SetOpKind Kind>
        struct set_op_impl
        {
            static constexpr auto name = Kind == SetOpKind::Difference     ? "sub_sets"
                                         : Kind == SetOpKind::Intersection ? "and_sets"
                                         : Kind == SetOpKind::Union        ? "or_sets"
                                                                           : "xor_sets";

            static bool requires_(const ResolutionMap &resolution, OperatorCallContext)
            {
                const auto *meta = resolved_t(resolution);
                return meta != nullptr && meta->value_kind() == ValueTypeKind::Set;
            }

            static void eval(In<"lhs", TS<ScalarVar<"T">>> lhs, In<"rhs", TS<ScalarVar<"T">>> rhs,
                             Out<TS<ScalarVar<"T">>> out)
            {
                const auto *meta = lhs.base().value().schema();
                auto        lhs_set = lhs.base().value().as_set();
                auto        rhs_set = rhs.base().value().as_set();
                SetBuilder  builder{element_binding_of(meta->element_type)};
                for (const ValueView &element : lhs_set.values())
                {
                    const bool in_rhs = rhs_set.contains(element);
                    const bool keep   = Kind == SetOpKind::Difference     ? !in_rhs
                                        : Kind == SetOpKind::Intersection ? in_rhs
                                        : Kind == SetOpKind::Union        ? true
                                                                          : !in_rhs;
                    if (keep) { (void)builder.insert(element); }
                }
                if constexpr (Kind == SetOpKind::Union || Kind == SetOpKind::SymmetricDifference)
                {
                    for (const ValueView &element : rhs_set.values())
                    {
                        if (Kind == SetOpKind::Union || !lhs_set.contains(element))
                        {
                            (void)builder.insert(element);
                        }
                    }
                }
                out.apply(builder.build().view());
            }
        };

        /** timedelta scaling (python: timedelta * n, timedelta / n). */
        struct timedelta_scale_impl
        {
            static constexpr auto name = "mul_timedelta_int";

            static void eval(In<"lhs", TS<TimeDelta>> lhs, In<"rhs", TS<Int>> rhs, Out<TS<TimeDelta>> out)
            {
                out.set(checked_multiply(lhs.value(), rhs.value()));
            }
        };

        struct timedelta_div_impl
        {
            static constexpr auto name = "div_timedelta_int";

            static void eval(In<"lhs", TS<TimeDelta>> lhs, In<"rhs", TS<Int>> rhs, Out<TS<TimeDelta>> out)
            {
                out.set(checked_divide(lhs.value(), rhs.value()));
            }
        };

        struct timedelta_scale_float_impl
        {
            static constexpr auto name = "mul_timedelta_float";

            static void eval(In<"lhs", TS<Duration>> lhs,
                             In<"rhs", TS<Float>> rhs,
                             Out<TS<Duration>> out)
            {
                out.set(checked_multiply(lhs.value(), rhs.value()));
            }
        };

        struct timedelta_div_float_impl
        {
            static constexpr auto name = "div_timedelta_float";

            static void eval(In<"lhs", TS<Duration>> lhs,
                             In<"rhs", TS<Float>> rhs,
                             Out<TS<Duration>> out)
            {
                out.set(checked_divide(lhs.value(), rhs.value()));
            }
        };

        struct period_scale_impl
        {
            static void eval(In<"lhs", TS<Period>> lhs,
                             In<"rhs", TS<Int>> rhs,
                             Out<TS<Period>> out)
            {
                out.set(checked_multiply(lhs.value(), rhs.value()));
            }
        };

        struct int_scale_period_impl
        {
            static void eval(In<"lhs", TS<Int>> lhs,
                             In<"rhs", TS<Period>> rhs,
                             Out<TS<Period>> out)
            {
                out.set(checked_multiply(rhs.value(), lhs.value()));
            }
        };

        struct negate_duration_impl
        {
            static void eval(In<"ts", TS<Duration>> value,
                             Out<TS<Duration>> out)
            {
                out.set(checked_negate(value.value()));
            }
        };

        struct negate_period_impl
        {
            static void eval(In<"ts", TS<Period>> value,
                             Out<TS<Period>> out)
            {
                out.set(checked_negate(value.value()));
            }
        };

        /** frozendict difference (sub_: lhs entries whose key is not in rhs). */
        struct diff_maps_impl
        {
            static constexpr auto name = "sub_maps";

            static bool requires_(const ResolutionMap &resolution, OperatorCallContext)
            {
                const auto *meta = resolved_t(resolution);
                return meta != nullptr && meta->value_kind() == ValueTypeKind::Map;
            }

            static void eval(In<"lhs", TS<ScalarVar<"T">>> lhs, In<"rhs", TS<ScalarVar<"T">>> rhs,
                             Out<TS<ScalarVar<"T">>> out)
            {
                const auto *meta = lhs.base().value().schema();
                const auto  lhs_map = lhs.base().value().as_map();
                const auto  rhs_map = rhs.base().value().as_map();
                MapBuilder  builder{element_binding_of(meta->key_type), element_binding_of(meta->element_type)};
                for (const auto [key, item] : lhs_map)
                {
                    if (!rhs_map.contains(key)) { builder.set_item(key, item); }
                }
                out.apply(builder.build().view());
            }
        };

        /** Container truthiness (and_ / or_ over container scalars -> Bool). */
        template <bool IsAnd>
        struct container_truthy_impl
        {
            static constexpr auto name = IsAnd ? "and_containers" : "or_containers";

            static bool requires_(const ResolutionMap &resolution, OperatorCallContext)
            {
                const auto *meta = resolved_t(resolution);
                if (meta == nullptr) { return false; }
                switch (meta->value_kind())
                {
                    case ValueTypeKind::Map:
                    case ValueTypeKind::Set:
                    case ValueTypeKind::List:
                    case ValueTypeKind::Tuple: return true;
                    default: return false;
                }
            }

            [[nodiscard]] static bool truthy(const ValueView &container)
            {
                switch (container.schema()->value_kind())
                {
                    case ValueTypeKind::Map: return container.as_map().size() != 0;
                    case ValueTypeKind::Set: return container.as_set().size() != 0;
                    default: return container.as_list().size() != 0;
                }
            }

            static void eval(In<"lhs", TS<ScalarVar<"T">>> lhs, In<"rhs", TS<ScalarVar<"T">>> rhs,
                             Out<TS<Bool>> out)
            {
                const bool a = truthy(lhs.base().value());
                const bool b = truthy(rhs.base().value());
                out.set(IsAnd ? (a && b) : (a || b));
            }
        };

        /** getitem_ over a MAP-scalar: the value at the key (no tick when
            the key is absent). */
        struct getitem_map_scalar_impl
        {
            static constexpr auto name = "getitem_map_scalar";

            static bool requires_(const ResolutionMap &resolution, OperatorCallContext)
            {
                const auto *meta = resolved_t(resolution);
                return meta != nullptr && meta->value_kind() == ValueTypeKind::Map;
            }

            static void resolve_default_types(ResolutionMap &resolution, OperatorCallContext context)
            {
                for (const WiringArg &arg : context.args)
                {
                    if (arg.kind != WiringArg::Kind::TimeSeries || arg.port.schema == nullptr) { continue; }
                    const auto *value_meta = arg.port.schema->value_schema;
                    if (value_meta == nullptr || value_meta->value_kind() != ValueTypeKind::Map) { continue; }
                    resolution.bind_scalar("K", value_meta->key_type);
                    resolution.bind_scalar("E", value_meta->element_type);
                    return;
                }
            }

            static void eval(In<"ts", TS<ScalarVar<"T">>> ts, In<"key", TS<ScalarVar<"K">>> key,
                             Out<TS<ScalarVar<"E">>> out)
            {
                auto map = ts.base().value().as_map();
                const auto key_value = key.base().value();
                if (!map.contains(key_value)) { return; }
                const auto &erased = static_cast<const TSOutputView &>(out);
                auto mutation = erased.begin_mutation(erased.evaluation_time());
                static_cast<void>(mutation.copy_value_from(map.at(key_value)));
            }
        };

        /** frozendict merge (bit_or: rhs entries win). */
        struct merge_maps_impl
        {
            static constexpr auto name = "or_maps";

            static bool requires_(const ResolutionMap &resolution, OperatorCallContext)
            {
                const auto *meta = resolved_t(resolution);
                return meta != nullptr && meta->value_kind() == ValueTypeKind::Map;
            }

            static void eval(In<"lhs", TS<ScalarVar<"T">>> lhs, In<"rhs", TS<ScalarVar<"T">>> rhs,
                             Out<TS<ScalarVar<"T">>> out)
            {
                const auto *meta = lhs.base().value().schema();
                MapBuilder  builder{element_binding_of(meta->key_type), element_binding_of(meta->element_type)};
                const auto  lhs_map = lhs.base().value().as_map();
                const auto  rhs_map = rhs.base().value().as_map();
                for (const auto [key, item] : lhs_map)
                {
                    builder.set_item(key, item);
                }
                for (const auto [key, item] : rhs_map)
                {
                    builder.set_item(key, item);
                }
                out.apply(builder.build().view());
            }
        };

        /** Generic size of a container scalar (len_ over tuple/frozenset/map). */
        struct len_container_impl
        {
            static constexpr auto name = "len_container";

            static bool requires_(const ResolutionMap &resolution, OperatorCallContext)
            {
                const auto *meta = resolved_t(resolution);
                if (meta == nullptr) { return false; }
                switch (meta->value_kind())
                {
                    case ValueTypeKind::List:
                    case ValueTypeKind::Tuple:
                    case ValueTypeKind::Set:
                    case ValueTypeKind::Map: return true;
                    default: return false;
                }
            }

            static void eval(In<"ts", TS<ScalarVar<"T">>> ts, Out<TS<Int>> out)
            {
                const ValueView value = ts.base().value();
                const auto size = visit(
                    value,
                    [](ListView selected) { return static_cast<Int>(selected.size()); },
                    [](TupleView selected) { return static_cast<Int>(selected.size()); },
                    [](SetView selected) { return static_cast<Int>(selected.size()); },
                    [](MapView selected) { return static_cast<Int>(selected.size()); },
                    [](ValueView) -> Int { throw std::logic_error("len_: not a sized container"); });
                out.set(size);
            }
        };
    }  // namespace arithmetic_impl_detail

    namespace arithmetic_impl_detail
    {
        /** Per-tick aggregates over CONTAINER-SCALAR values (frozendict ->
            its values; frozenset/list -> its elements). NOT the running
            scalar aggregates: each tick aggregates the container's content. */
        enum class ContainerAgg : std::uint8_t { Min, Max, Sum, Mean, Std, Var };

        template <ValueTypeKind Kind>
        [[nodiscard]] inline const ValueTypeMetaData *container_agg_element_meta(
            const ValueTypeMetaData *meta) noexcept
        {
            if (meta == nullptr || meta->is_mutable() || meta->try_value_kind() != Kind) { return nullptr; }
            if constexpr (Kind == ValueTypeKind::Map || Kind == ValueTypeKind::Set ||
                          Kind == ValueTypeKind::List)
            {
                return meta->element_type;
            }
            else { return nullptr; }
        }

        template <ValueTypeKind Kind>
        [[nodiscard]] inline bool container_agg_requires(const ResolutionMap &resolution)
        {
            const auto *meta = resolution.find_scalar("T");
            return container_agg_element_meta<Kind>(meta) != nullptr;
        }

        template <ValueTypeKind Kind, typename Fn>
        void for_each_container_element(const ValueView &container, Fn &&fn)
        {
            if constexpr (Kind == ValueTypeKind::Map)
            {
                const auto map_values = container.as_map();
                const auto values     = map_values.values();
                for (const ValueView &item : values) { fn(item); }
            }
            else if constexpr (Kind == ValueTypeKind::Set)
            {
                const auto set_values = container.as_set();
                const auto values     = set_values.values();
                for (const ValueView &item : values) { fn(item); }
            }
            else if constexpr (Kind == ValueTypeKind::List)
            {
                const auto values = container.as_list();
                for (const ValueView &item : values) { fn(item); }
            }
            else { static_assert(Kind == ValueTypeKind::Map || Kind == ValueTypeKind::Set ||
                                 Kind == ValueTypeKind::List,
                                 "unsupported container aggregate kind"); }
        }

        inline void publish_value(const TSOutputView &out_view, Value value)
        {
            auto mutation = out_view.begin_mutation(out_view.evaluation_time());
            static_cast<void>(mutation.move_value_from(std::move(value)));
        }

        inline void publish_default_if_valid(const TSInputView *default_value, const TSOutputView &out_view)
        {
            if (default_value == nullptr || !default_value->valid()) { return; }
            auto mutation = out_view.begin_mutation(out_view.evaluation_time());
            static_cast<void>(mutation.copy_value_from(default_value->value()));
        }

        template <ValueTypeKind Kind>
        void resolve_container_agg_output(ResolutionMap &resolution,
                                          OperatorCallContext context,
                                          const ValueTypeMetaData *out_meta)
        {
            for (const WiringArg &arg : context.args)
            {
                if (arg.kind != WiringArg::Kind::TimeSeries || arg.port.schema == nullptr) { continue; }
                if (container_agg_element_meta<Kind>(arg.port.schema->value_schema) == nullptr) { continue; }
                resolution.bind_scalar("E", out_meta);
                return;
            }
        }

        template <typename Element>
        [[nodiscard]] constexpr Float numeric_as_float(Element value) noexcept
        {
            return static_cast<Float>(value);
        }

        template <ContainerAgg Agg, typename Element, ValueTypeKind Kind>
        struct numeric_container_stats
        {
            std::size_t count{0};
            Element     sum{};
            Float       mean_sum{0.0};
            Float       origin{0.0};
            Float       shifted_sum{0.0};
            Float       shifted_sum_sq{0.0};

            static numeric_container_stats collect(const ValueView &container)
            {
                numeric_container_stats stats;
                for_each_container_element<Kind>(container, [&](const ValueView &item) {
                    const Element value = item.checked_as<Element>();
                    if constexpr (Agg == ContainerAgg::Sum) { stats.sum += value; }
                    else if constexpr (Agg == ContainerAgg::Mean)
                    {
                        stats.mean_sum += numeric_as_float(value);
                    }
                    else
                    {
                        const Float x = numeric_as_float(value);
                        if (stats.count == 0) { stats.origin = x; }
                        const Float shifted = x - stats.origin;
                        stats.shifted_sum += shifted;
                        stats.shifted_sum_sq += shifted * shifted;
                    }
                    ++stats.count;
                });
                return stats;
            }
        };

        template <ContainerAgg Agg, bool HasDefault, ValueTypeKind Kind>
        struct container_extremum_impl
        {
            static constexpr auto name = Agg == ContainerAgg::Min   ? (HasDefault ? "min_container_d" : "min_container")
                                                                    : (HasDefault ? "max_container_d" : "max_container");

            static_assert(Agg == ContainerAgg::Min || Agg == ContainerAgg::Max);

            static bool requires_(const ResolutionMap &resolution, OperatorCallContext)
            {
                return container_agg_requires<Kind>(resolution);
            }

            static void resolve_default_types(ResolutionMap &resolution, OperatorCallContext context)
            {
                const auto *meta = resolution.find_scalar("T");
                const auto *element = container_agg_element_meta<Kind>(meta);
                if (element != nullptr) { resolve_container_agg_output<Kind>(resolution, context, element); }
            }

            static void do_eval(const ValueView &container, const TSInputView *default_value,
                                const TSOutputView &out_view)
            {
                Value best;
                for_each_container_element<Kind>(container, [&](const ValueView &item) {
                    if (!best.has_value() ||
                        (Agg == ContainerAgg::Min
                             ? item.compare(best.view()) == std::partial_ordering::less
                             : item.compare(best.view()) == std::partial_ordering::greater))
                    {
                        best = Value{item};
                    }
                });

                if (best.has_value())
                {
                    publish_value(out_view, std::move(best));
                    return;
                }

                publish_default_if_valid(default_value, out_view);
            }
        };

        template <ContainerAgg Agg, bool HasDefault, ValueTypeKind Kind, typename Element>
        struct container_numeric_agg_impl
        {
            static constexpr auto name = Agg == ContainerAgg::Sum   ? (HasDefault ? "sum_container_d" : "sum_container")
                                         : Agg == ContainerAgg::Mean ? (HasDefault ? "mean_container_d" : "mean_container")
                                         : Agg == ContainerAgg::Std ? (HasDefault ? "std_container_d" : "std_container")
                                                                    : (HasDefault ? "var_container_d" : "var_container");

            static_assert(Agg == ContainerAgg::Sum || Agg == ContainerAgg::Mean || Agg == ContainerAgg::Std ||
                          Agg == ContainerAgg::Var);
            static_assert(std::is_same_v<Element, Int> || std::is_same_v<Element, Float>);

            static bool requires_(const ResolutionMap &resolution, OperatorCallContext)
            {
                const auto *meta = resolution.find_scalar("T");
                return container_agg_element_meta<Kind>(meta) == scalar_descriptor<Element>::value_meta();
            }

            static void resolve_default_types(ResolutionMap &resolution, OperatorCallContext context)
            {
                const auto *out_meta = Agg == ContainerAgg::Sum ? scalar_descriptor<Element>::value_meta()
                                                                : scalar_descriptor<Float>::value_meta();
                resolve_container_agg_output<Kind>(resolution, context, out_meta);
            }

            static void do_eval(const ValueView &container, const TSOutputView &out_view)
            {
                const auto stats = numeric_container_stats<Agg, Element, Kind>::collect(container);
                if constexpr (Agg == ContainerAgg::Sum)
                {
                    publish_value(out_view, Value{stats.sum});
                }
                else if constexpr (Agg == ContainerAgg::Mean)
                {
                    const Float out = stats.count == 0
                                          ? std::numeric_limits<Float>::quiet_NaN()
                                          : stats.mean_sum / static_cast<Float>(stats.count);
                    publish_value(out_view, Value{out});
                }
                else
                {
                    Float variance = 0.0;
                    if (stats.count > 1)
                    {
                        const Float count = static_cast<Float>(stats.count);
                        variance = (stats.shifted_sum_sq - stats.shifted_sum * stats.shifted_sum / count) /
                                   static_cast<Float>(stats.count - 1);
                        if (variance < 0.0) { variance = 0.0; }
                    }
                    const Float out = Agg == ContainerAgg::Std ? std::sqrt(variance) : variance;
                    publish_value(out_view, Value{out});
                }
            }
        };

        template <ContainerAgg Agg, ValueTypeKind Kind>
        struct container_extremum_plain : container_extremum_impl<Agg, false, Kind>
        {
            static void eval(In<"ts", TS<ScalarVar<"T">>> ts, Out<TS<ScalarVar<"E">>> out)
            {
                container_extremum_impl<Agg, false, Kind>::do_eval(ts.base().value(), nullptr,
                                                                   static_cast<const TSOutputView &>(out));
            }
        };

        template <ContainerAgg Agg, ValueTypeKind Kind>
        struct container_extremum_default : container_extremum_impl<Agg, true, Kind>
        {
            static void eval(In<"ts", TS<ScalarVar<"T">>> ts,
                             In<"default_value", TS<ScalarVar<"E">>, InputValidity::Unchecked> default_value,
                             Out<TS<ScalarVar<"E">>> out)
            {
                const auto &fallback = default_value.base();
                container_extremum_impl<Agg, true, Kind>::do_eval(ts.base().value(), &fallback,
                                                                  static_cast<const TSOutputView &>(out));
            }
        };

        template <ContainerAgg Agg, ValueTypeKind Kind, typename Element>
        struct container_numeric_agg_plain : container_numeric_agg_impl<Agg, false, Kind, Element>
        {
            static void eval(In<"ts", TS<ScalarVar<"T">>> ts, Out<TS<ScalarVar<"E">>> out)
            {
                container_numeric_agg_impl<Agg, false, Kind, Element>::do_eval(
                    ts.base().value(), static_cast<const TSOutputView &>(out));
            }
        };

        template <ContainerAgg Agg, ValueTypeKind Kind, typename Element>
        struct container_numeric_agg_default : container_numeric_agg_impl<Agg, true, Kind, Element>
        {
            static void eval(In<"ts", TS<ScalarVar<"T">>> ts,
                             In<"default_value", TS<ScalarVar<"E">>, InputValidity::Unchecked> default_value,
                             Out<TS<ScalarVar<"E">>> out)
            {
                (void)default_value;
                container_numeric_agg_impl<Agg, true, Kind, Element>::do_eval(
                    ts.base().value(), static_cast<const TSOutputView &>(out));
            }
        };

        template <ValueTypeKind Kind>
        void register_container_extremum_aggregates()
        {
            register_overload<min_, container_extremum_plain<ContainerAgg::Min, Kind>>();
            register_overload<min_, container_extremum_default<ContainerAgg::Min, Kind>>();
            register_overload<max_, container_extremum_plain<ContainerAgg::Max, Kind>>();
            register_overload<max_, container_extremum_default<ContainerAgg::Max, Kind>>();
        }

        template <ValueTypeKind Kind, typename Element>
        void register_container_numeric_aggregates()
        {
            register_overload<sum_, container_numeric_agg_plain<ContainerAgg::Sum, Kind, Element>>();
            register_overload<sum_, container_numeric_agg_default<ContainerAgg::Sum, Kind, Element>>();
            register_overload<mean, container_numeric_agg_plain<ContainerAgg::Mean, Kind, Element>>();
            register_overload<mean, container_numeric_agg_default<ContainerAgg::Mean, Kind, Element>>();
        }

        template <ValueTypeKind Kind>
        void register_container_aggregates()
        {
            register_container_extremum_aggregates<Kind>();
            register_container_numeric_aggregates<Kind, Int>();
            register_container_numeric_aggregates<Kind, Float>();
        }
    }  // namespace arithmetic_impl_detail

    /** Register the arithmetic operator overloads (``add_`` / ``sub_`` / ``div_``). */
    void register_arithmetic_operators();
}  // namespace hgraph::stdlib


namespace hgraph::static_schema_detail
{
    template <>
    struct scalar_name<hgraph::stdlib::arithmetic_impl_detail::AggMoments>
    {
        static constexpr std::string_view value{"agg_moments"};
    };
}  // namespace hgraph::static_schema_detail

template <>
struct std::hash<hgraph::stdlib::arithmetic_impl_detail::AggMoments>
{
    [[nodiscard]] std::size_t operator()(const hgraph::stdlib::arithmetic_impl_detail::AggMoments &m) const noexcept
    {
        return std::hash<hgraph::Int>{}(m.count) ^ (std::hash<hgraph::Float>{}(m.mean) << 1) ^
               (std::hash<hgraph::Float>{}(m.m2) << 2);
    }
};

namespace hgraph::stdlib
{
    struct checked_add_durations
    {
        static void eval(In<"lhs", TS<Duration>> lhs,
                         In<"rhs", TS<Duration>> rhs,
                         Out<TS<Duration>> out)
        {
            out.set(hgraph::checked_add(lhs.value(), rhs.value()));
        }
    };

    struct checked_add_instant_duration
    {
        static void eval(In<"lhs", TS<Instant>> lhs,
                         In<"rhs", TS<Duration>> rhs,
                         Out<TS<Instant>> out)
        {
            out.set(hgraph::checked_add(lhs.value(), rhs.value()));
        }
    };

    struct checked_add_duration_instant
    {
        static void eval(In<"lhs", TS<Duration>> lhs,
                         In<"rhs", TS<Instant>> rhs,
                         Out<TS<Instant>> out)
        {
            out.set(hgraph::checked_add(rhs.value(), lhs.value()));
        }
    };

    struct checked_sub_durations
    {
        static void eval(In<"lhs", TS<Duration>> lhs,
                         In<"rhs", TS<Duration>> rhs,
                         Out<TS<Duration>> out)
        {
            out.set(hgraph::checked_subtract(lhs.value(), rhs.value()));
        }
    };

    struct checked_sub_instant_duration
    {
        static void eval(In<"lhs", TS<Instant>> lhs,
                         In<"rhs", TS<Duration>> rhs,
                         Out<TS<Instant>> out)
        {
            out.set(hgraph::checked_subtract(lhs.value(), rhs.value()));
        }
    };

    struct checked_sub_instants
    {
        static void eval(In<"lhs", TS<Instant>> lhs,
                         In<"rhs", TS<Instant>> rhs,
                         Out<TS<Duration>> out)
        {
            out.set(hgraph::checked_subtract(lhs.value(), rhs.value()));
        }
    };

    struct checked_add_periods
    {
        static void eval(In<"lhs", TS<Period>> lhs,
                         In<"rhs", TS<Period>> rhs, Out<TS<Period>> out)
        {
            out.set(hgraph::checked_add(lhs.value(), rhs.value()));
        }
    };

    struct checked_sub_periods
    {
        static void eval(In<"lhs", TS<Period>> lhs,
                         In<"rhs", TS<Period>> rhs, Out<TS<Period>> out)
        {
            out.set(hgraph::checked_subtract(lhs.value(), rhs.value()));
        }
    };

    struct checked_add_civil_datetime_duration
    {
        static void eval(In<"lhs", TS<CivilDateTime>> lhs,
                         In<"rhs", TS<Duration>> rhs,
                         Out<TS<CivilDateTime>> out)
        {
            out.set(hgraph::checked_add(lhs.value(), rhs.value()));
        }
    };

    struct combine_civil_date_time
    {
        static void eval(In<"lhs", TS<CivilDate>> lhs,
                         In<"rhs", TS<CivilTime>> rhs,
                         Out<TS<CivilDateTime>> out)
        {
            out.set(CivilDateTime{lhs.value(), rhs.value()});
        }
    };

    struct checked_add_zoned_duration
    {
        static void eval(In<"lhs", TS<ZonedDateTime>> lhs,
                         In<"rhs", TS<Duration>> rhs,
                         GlobalStateView state,
                         Out<TS<ZonedDateTime>> out)
        {
            out.set(hgraph::checked_add(
                lhs.value(), rhs.value(), time_zone_provider(state)));
        }
    };

    struct checked_add_duration_zoned
    {
        static void eval(In<"lhs", TS<Duration>> lhs,
                         In<"rhs", TS<ZonedDateTime>> rhs,
                         GlobalStateView state,
                         Out<TS<ZonedDateTime>> out)
        {
            out.set(hgraph::checked_add(
                rhs.value(), lhs.value(), time_zone_provider(state)));
        }
    };

    struct checked_sub_zoned_duration
    {
        static void eval(In<"lhs", TS<ZonedDateTime>> lhs,
                         In<"rhs", TS<Duration>> rhs,
                         GlobalStateView state,
                         Out<TS<ZonedDateTime>> out)
        {
            out.set(hgraph::checked_add(
                lhs.value(), checked_negate(rhs.value()),
                time_zone_provider(state)));
        }
    };

    struct checked_sub_civil_datetime_duration
    {
        static void eval(In<"lhs", TS<CivilDateTime>> lhs,
                         In<"rhs", TS<Duration>> rhs,
                         Out<TS<CivilDateTime>> out)
        {
            out.set(hgraph::checked_subtract(lhs.value(), rhs.value()));
        }
    };

    struct checked_sub_civil_datetimes
    {
        static void eval(In<"lhs", TS<CivilDateTime>> lhs,
                         In<"rhs", TS<CivilDateTime>> rhs,
                         Out<TS<Duration>> out)
        {
            out.set(hgraph::checked_subtract(lhs.value(), rhs.value()));
        }
    };

    template <typename Civil, MonthEndPolicy Policy>
    struct apply_period_add_impl
    {
        static auto defaults()
        {
            return std::tuple{arg<"month_end_policy">(MonthEndPolicy::Reject)};
        }

        static bool requires_(const ResolutionMap &,
                              OperatorCallContext context)
        {
            const auto *selected =
                context.scalar_as<MonthEndPolicy>("month_end_policy");
            return selected != nullptr && *selected == Policy;
        }

        static void eval(
            In<"lhs", TS<Civil>> lhs, In<"rhs", TS<Period>> rhs,
            Scalar<"month_end_policy", MonthEndPolicy>,
            Out<TS<Civil>> out)
        {
            out.set(apply_period(lhs.value(), rhs.value(), Policy));
        }
    };

    template <typename Civil, MonthEndPolicy Policy>
    struct apply_period_sub_impl
    {
        static auto defaults()
        {
            return std::tuple{arg<"month_end_policy">(MonthEndPolicy::Reject)};
        }

        static bool requires_(const ResolutionMap &,
                              OperatorCallContext context)
        {
            const auto *selected =
                context.scalar_as<MonthEndPolicy>("month_end_policy");
            return selected != nullptr && *selected == Policy;
        }

        static void eval(
            In<"lhs", TS<Civil>> lhs, In<"rhs", TS<Period>> rhs,
            Scalar<"month_end_policy", MonthEndPolicy>,
            Out<TS<Civil>> out)
        {
            out.set(apply_period(lhs.value(),
                                 hgraph::checked_negate(rhs.value()),
                                 Policy));
        }
    };
}  // namespace hgraph::stdlib

#endif  // HGRAPH_LIB_STD_OPERATORS_IMPL_ARITHMETIC_IMPL_H
