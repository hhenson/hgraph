#ifndef HGRAPH_LIB_STD_OPERATORS_IMPL_CONVERSION_IMPL_H
#define HGRAPH_LIB_STD_OPERATORS_IMPL_CONVERSION_IMPL_H

#include <hgraph/types/operator_type_resolution.h>
#include <hgraph/lib/std/operators/arithmetic.h>    // add_ / mul_ (zero_ op mapping)
#include <hgraph/lib/std/operators/collection.h>    // sum_     (zero_ op mapping)
#include <hgraph/lib/std/operators/comparison.h>    // min_ / max_ (zero_ op mapping)
#include <hgraph/lib/std/operators/conversion.h>    // const_ / zero_ / default_
#include <hgraph/runtime/node_scheduler.h>          // SingleShotScheduler
#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/operator_dispatch.h>
#include <hgraph/types/primitive_types.h>
#include <hgraph/types/series.h>
#include <hgraph/types/static_node.h>
#include <hgraph/types/static_schema.h>
#include <hgraph/types/subgraph_wiring.h>
#include <hgraph/types/type_resolution.h>
#include <hgraph/types/wired_fn.h>
#include <hgraph/types/value/table_codec.h>
#include <hgraph/types/value/value_builder.h>
#include <hgraph/types/value/value_conversion.h>
#include <hgraph/util/date_time.h>

#include <arrow/array.h>

#include <limits>
#include <deque>
#include <stdexcept>
#include <string_view>

namespace hgraph::stdlib
{
    using namespace hgraph::operator_type_resolution;

    namespace conversion_detail
    {
        [[nodiscard]] HGRAPH_EXPORT bool valid_utf8(std::string_view text) noexcept;
    }

    /**
     * Implementations + registration for the conversion / utility operators. The abstract
     * markers are in ``<hgraph/lib/std/operators/conversion.h>``; this file provides the
     * concrete overloads and ``register_conversion_operators`` to register them. Only the
     * additive-zero source (``zero_``) and the constant source (``const_``) are implemented
     * so far.
     */

    /**
     * Shared output-type resolution for the ``const_`` overloads. The value type is a
     * ``ScalarVar`` inferred from the configured value; without an explicit output schema the
     * output resolves to ``TS<T>``, and with one the configured value's schema must match
     * that output's value schema.
     */
    inline void const_resolve_output(ResolutionMap &resolution)
    {
        const auto *value_schema  = resolution.scalar("T");
        const auto *output_schema = resolution.find_ts("S");
        if (output_schema == nullptr)
        {
            resolution.bind_ts("S", TypeRegistry::instance().ts(value_schema));
            return;
        }
        // The configured value may be the output's current-value shape OR its
        // canonical DELTA shape (partial initialisation - python's const of
        // a partial dict over a TSL/TSD); out.apply handles both.
        if (value_schema == nullptr ||
            (!current_value_schema_compatible(*output_schema, *value_schema) &&
             value_schema != output_schema->delta_value_schema))
        {
            throw std::logic_error("const: configured value schema does not match the resolved output value schema");
        }
    }

    /** Apply a configured const value: the current-value shape copies in
        whole; the canonical DELTA shape (partial initialisation) applies as
        the initial tick. */
    inline void const_apply(const ValueView &value, const TSOutputView &out)
    {
        const auto *schema = out.schema();
        if (schema != nullptr && value.schema() == schema->delta_value_schema &&
            schema->delta_value_schema != schema->value_schema)
        {
            apply_delta(out, value);
            return;
        }
        apply_current_value(out, value);
    }

    /**
     * ``const_`` implementation — ``const(value)``: a single generic source that ticks the
     * configured ``value`` once at the start cycle (declared via ``schedule_on_start``).
     */
    struct const_source
    {
        static constexpr auto name              = "const";
        static constexpr bool schedule_on_start = true;

        static void resolve_default_types(ResolutionMap &resolution) { const_resolve_output(resolution); }

        static void eval(Scalar<"value", ScalarVar<"T">> value, Out<TsVar<"S">> out)
        {
            const_apply(value.value(), out);  // erased copy of the configured value
        }
    };

    /**
     * ``const_`` implementation — ``const(value, delay)``: as ``const_source`` but the single
     * tick is delayed to ``start_time + delay``. The ``delay`` drives the one-shot schedule in
     * ``start``; ``eval`` applies the value (matching Python's ``yield start_time + delay, value``).
     */
    struct const_delayed
    {
        static constexpr auto name = "const";

        static void resolve_default_types(ResolutionMap &resolution) { const_resolve_output(resolution); }

        static void start(Scalar<"delay", TimeDelta> delay, SingleShotScheduler sched)
        {
            sched.schedule(delay.value());   // now + delay (now == start_time during start)
        }

        static void eval(Scalar<"value", ScalarVar<"T">> value, Scalar<"delay", TimeDelta> delay,
                         Out<TsVar<"S">> out)
        {
            static_cast<void>(delay);   // delay drives the start schedule; eval just applies the value
            const_apply(value.value(), out);
        }
    };

    /**
     * ``zero_`` implementations — op-aware zero sources, mirroring Python
     * ``_impl/_operators/_zero.py``: the zero value depends on both the output
     * type and the operation (``add_``/``sum_`` -> identity of addition,
     * ``mul_`` -> identity of multiplication, ``min_``/``max_`` -> the
     * saturating bound). An unmapped operation is a wiring-time error, like
     * Python's ``KeyError``.
     */
    struct zero_int
    {
        static constexpr auto name = "zero_int";

        static Port<TS<Int>> compose(Wiring &w, Scalar<"op", WiredFn> op)
        {
            const WiredFn &f = op.value();
            Int            value{};
            if (f == fn<add_>() || f == fn<sum_>()) { value = Int{0}; }
            else if (f == fn<min_>()) { value = std::numeric_limits<Int>::max(); }
            else if (f == fn<max_>()) { value = std::numeric_limits<Int>::lowest(); }
            else if (f == fn<mul_>()) { value = Int{1}; }
            else { throw std::invalid_argument("zero: no TS<Int> zero value registered for the supplied operation"); }
            return wire<const_, TS<Int>>(w, value);
        }
    };

    struct zero_float
    {
        static constexpr auto name = "zero_float";

        static Port<TS<Float>> compose(Wiring &w, Scalar<"op", WiredFn> op)
        {
            const WiredFn &f = op.value();
            Float          value{};
            if (f == fn<add_>() || f == fn<sum_>()) { value = Float{0}; }
            else if (f == fn<min_>()) { value = std::numeric_limits<Float>::infinity(); }
            else if (f == fn<max_>()) { value = -std::numeric_limits<Float>::infinity(); }
            else if (f == fn<mul_>()) { value = Float{1}; }
            else
            {
                throw std::invalid_argument("zero: no TS<Float> zero value registered for the supplied operation");
            }
            return wire<const_, TS<Float>>(w, value);
        }
    };

    struct zero_str
    {
        static constexpr auto name = "zero_str";

        static Port<TS<Str>> compose(Wiring &w, Scalar<"op", WiredFn> op)
        {
            const WiredFn &f = op.value();
            if (f == fn<add_>() || f == fn<sum_>() || f == fn<mul_>())
            {
                return wire<const_, TS<Str>>(w, Str{});
            }
            throw std::invalid_argument("zero: no TS<Str> zero value registered for the supplied operation");
        }
    };

    /** A TSD identity is the correctly typed, permanently invalid source. */
    struct zero_tsd
    {
        static constexpr auto name = "zero_tsd";

        static void eval(Scalar<"op", WiredFn> op,
                         Out<TSD<ScalarVar<"K">, TsVar<"V">>> out)
        {
            static_cast<void>(op);
            static_cast<void>(out);
        }
    };

    /**
     * ``default`` implementation — the REF-forwarding node, mirroring Python
     * ``_impl/_operators/_graph_operators.py`` ``_default`` (``valid=()``):
     * while ``ts`` is invalid the output references ``default_value`` and ``ts``
     * is kept active (to notice its first tick); once valid the output
     * references ``ts`` and ``ts`` is made passive — downstream reads flow
     * *through* the reference without this node evaluating per tick. (Python
     * takes ``default_value`` as a ``REF`` input so value ticks never re-fire
     * the node; here it is a plain active input — for the ``const`` zeros it
     * ticks once, and a re-emit of the same reference is a cheap downstream
     * no-op rebind.)
     *
     * The output **is** a ``REF`` and its port keeps that computed schema —
     * the declared ``-> S`` contract holds because ``REF`` is transparent to
     * matching/unification (a variable binds the dereferenced type) and
     * consumers bind through the reference at runtime.
     */
    struct default_ref_impl
    {
        static constexpr auto name = "default_ref";

        static void eval(In<"ts_ref", REF<TsVar<"S">>, InputValidity::Unchecked> ts_ref,
                         In<"ts", TsVar<"S">, InputValidity::Unchecked> ts,
                         In<"default_value", REF<TsVar<"S">>, InputValidity::Unchecked> default_value,
                         Out<REF<TsVar<"S">>> out)
        {
            if (!ts.valid())
            {
                ts.make_active();
                out.set(default_value.value());
            }
            else
            {
                ts.make_passive();
                out.set(ts_ref.value());
            }
        }
    };

    /** Preserve the active-reference/value split of Python hgraph's
        ``default_impl``. The REF input observes endpoint rebinds while the
        dereferenced value input may remain passive after becoming valid. */
    struct default_impl
    {
        static constexpr auto name = "default";

        static void resolve_default_types(ResolutionMap &resolution, OperatorCallContext context)
        {
            if (const auto *source = time_series_schema_at(context, 0); source != nullptr)
            {
                resolution.bind_ts("__out__", source);
            }
        }

        static WiringPortRef compose(Wiring &w, NamedPort<"ts", TsVar<"S">> ts,
                                     NamedPort<"default_value", TsVar<"S">> default_value)
        {
            auto &registry = TypeRegistry::instance();

            WiringPortRef ts_value = ts.erased();
            WiringPortRef ts_ref = ts_value;
            ts_ref.schema = registry.ref(ts_value.schema);

            WiringPortRef default_ref = default_value.erased();
            default_ref.schema = registry.ref(default_ref.schema);

            return wire<default_ref_impl>(
                       w, Port<void>{w, std::move(ts_ref)}, Port<void>{w, std::move(ts_value)},
                       Port<void>{w, std::move(default_ref)})
                .erased();
        }
    };

    /** convert(ts, to=SAME) - identical schemas pass the value through. */
    struct convert_identity_impl
    {
        static constexpr auto name = "convert_identity";

        static bool requires_(const ResolutionMap &resolution, OperatorCallContext context)
        {
            const auto *out = output_schema(resolution);
            return out != nullptr &&
                   time_series_schema_at(context, 0) == out;
        }

        static void eval(In<"ts", TsVar<"S">> ts, Out<TsVar<"__out__">> out)
        {
            const auto &erased = static_cast<const TSOutputView &>(out);
            auto mutation = erased.data_view().begin_mutation(erased.evaluation_time());
            static_cast<void>(mutation.copy_value_from(ts.base().value()));
        }
    };

    /** Convert a concrete Bundle leaf to one of its registered base unions.

        The output's graph-scoped realization owns the closed-union storage;
        copying the concrete input value selects the corresponding leaf. */
    struct convert_bundle_upcast_impl
    {
        static constexpr auto name = "convert_bundle_upcast";

        static bool requires_(const ResolutionMap &resolution, OperatorCallContext context)
        {
            const auto *out = output_schema(resolution);
            const auto *input = time_series_schema_at(context, 0);
            const auto *in = ts_value_schema_at(context, 0);
            return out != nullptr && out->kind == TSTypeKind::TS &&
                   input != nullptr && input->kind == TSTypeKind::TS &&
                   in != nullptr && out->value_schema != nullptr &&
                   in->is_named_bundle() && out->value_schema->is_named_bundle() &&
                   TypeRegistry::instance().bundle_is_a(in, out->value_schema);
        }

        static void eval(In<"ts", TsVar<"S">> ts, Out<TsVar<"__out__">> out)
        {
            const auto &erased = static_cast<const TSOutputView &>(out);
            auto mutation = erased.data_view().begin_mutation(erased.evaluation_time());
            static_cast<void>(mutation.copy_value_from(ts.base().value()));
        }
    };

    /** Materialize the active leaf of a closed Bundle union as a derived TS.

        Dispatch selects the branch from the same active type before this node
        runs. The check remains here so ``downcast_`` is also safe when wired
        directly: a mismatched runtime leaf fails instead of interpreting the
        base union's storage as the requested derived Bundle. */
    struct downcast_bundle_impl
    {
        static constexpr auto name = "downcast_bundle";

        static bool requires_(const ResolutionMap &resolution, OperatorCallContext context)
        {
            const auto *out = output_schema(resolution);
            const auto *in = ts_value_schema_at(context, 0);
            return out != nullptr && output_matches<AnyTS>(resolution) && in != nullptr &&
                   in->is_named_bundle() && out->value_schema->is_named_bundle() &&
                   TypeRegistry::instance().bundle_is_a(out->value_schema, in);
        }

        static void eval(In<"ts", TsVar<"S">> ts, Out<TsVar<"__out__">> out)
        {
            const auto &erased = static_cast<const TSOutputView &>(out);
            auto concrete = ts.base().value().concrete();
            if (!concrete ||
                !TypeRegistry::instance().bundle_is_a(
                    concrete.schema(), erased.schema()->value_schema))
            {
                throw std::invalid_argument(
                    "downcast_: active Bundle value does not match the requested derived type");
            }
            auto mutation = erased.data_view().begin_mutation(erased.evaluation_time());
            static_cast<void>(mutation.copy_value_from(concrete));
        }
    };

    /** Re-label a reference with the requested target schema without
        inspecting the referenced value. The endpoint is unchanged; callers
        are responsible for selecting a compatible derived target. */
    struct downcast_ref_impl
    {
        static constexpr auto name = "downcast_ref";

        static void eval(In<"ts", REF<TsVar<"S">>, InputValidity::Unchecked> ts,
                         Out<REF<TsVar<"O">>> out)
        {
            const auto *output_schema = static_cast<const TSOutputView &>(out).schema();
            const auto *target_schema = output_schema != nullptr ? output_schema->referenced_ts() : nullptr;
            if (target_schema == nullptr)
            {
                throw std::logic_error("downcast_ref requires a resolved REF output schema");
            }
            out.set(ts.value().with_target_schema_unchecked(target_schema));
        }
    };

    /** Numeric/bool scalar conversions: TYPED kernels selected at node-
        selection time (the typed-kernel rule - no per-tick type branch). */
    template <typename From, typename To>
    struct convert_numeric_impl
    {
        static_assert(!std::same_as<From, To>);
        static constexpr auto name = "convert_numeric";

        // The concrete Out<TS<To>> is gated by the dispatcher's requested-
        // output match; requires_ only checks the INPUT.
        static bool requires_(const ResolutionMap &, OperatorCallContext context)
        {
            return ts_value_schema_at(context, 0) ==
                   scalar_descriptor<From>::value_meta();
        }

        static void eval(In<"ts", TS<From>> ts, Out<TS<To>> out)
        {
            if constexpr (std::same_as<To, Bool>) { out.set(ts.value() != From{}); }
            else { out.set(static_cast<To>(ts.value())); }
        }
    };

    /** UTF-8 text/binary conversions matching Python's str.encode() and
        bytes.decode() defaults. Str storage is already UTF-8, so these
        kernels only change the scalar schema and preserve the byte payload. */
    template <typename From, typename To>
    struct convert_text_bytes_impl
    {
        static_assert((std::same_as<From, Str> && std::same_as<To, Bytes>) ||
                      (std::same_as<From, Bytes> && std::same_as<To, Str>));
        static constexpr auto name = "convert_text_bytes";

        static bool requires_(const ResolutionMap &, OperatorCallContext context)
        {
            return ts_value_schema_at(context, 0) ==
                   scalar_descriptor<From>::value_meta();
        }

        static void eval(In<"ts", TS<From>> ts, Out<TS<To>> out)
        {
            if constexpr (std::same_as<From, Str>) { out.set(Bytes{ts.value()}); }
            else
            {
                if (!conversion_detail::valid_utf8(ts.value().data))
                {
                    throw std::invalid_argument("bytes value is not valid UTF-8");
                }
                out.set(Str{ts.value().data});
            }
        }
    };

    /** Python-style str() conversions (py parity: 0.0 -> "0.0",
        True -> "True", tuples print as "(1, 2)"). */
    template <typename From>
    struct convert_to_str_impl
    {
        static constexpr auto name = "convert_to_str";

        static bool requires_(const ResolutionMap &, OperatorCallContext context)
        {
            return ts_value_schema_at(context, 0) ==
                   scalar_descriptor<From>::value_meta();
        }

        static void eval(In<"ts", TS<From>> ts, Out<TS<Str>> out)
        {
            if constexpr (std::same_as<From, Bool>) { out.set(Str{ts.value() ? "True" : "False"}); }
            else if constexpr (std::same_as<From, Float>)
            {
                const Float value = ts.value();
                std::string text  = fmt::format("{}", value);
                if (text.find('.') == std::string::npos && text.find('e') == std::string::npos &&
                    text.find("inf") == std::string::npos && text.find("nan") == std::string::npos)
                {
                    text += ".0";   // python float repr always shows the point
                }
                out.set(Str{std::move(text)});
            }
            else { out.set(Str{fmt::format("{}", ts.value())}); }
        }
    };

    /** str() of a LIST-valued TS: python TUPLE style - "(1, 2)", "(1,)". */
    struct convert_list_to_str_impl
    {
        static constexpr auto name = "convert_list_to_str";

        static bool requires_(const ResolutionMap &, OperatorCallContext context)
        {
            const auto *in = ts_value_schema_at(context, 0);
            return in != nullptr && in->value_kind() == ValueTypeKind::List;
        }

        static void eval(In<"ts", TsVar<"S">> ts, Out<TS<Str>> out)
        {
            const auto  value = ts.base().value();
            auto        items = value.as_indexed_view();
            std::string text  = "(";
            for (std::size_t index = 0; index < items.size(); ++index)
            {
                if (index != 0) { text += ", "; }
                text += items.at(index).to_string();
            }
            text += items.size() == 1 ? ",)" : ")";
            out.set(Str{std::move(text)});
        }
    };

    /** bool() of a LIST-valued TS: python truthiness (non-empty). */
    struct convert_list_to_bool_impl
    {
        static constexpr auto name = "convert_list_to_bool";

        static bool requires_(const ResolutionMap &, OperatorCallContext context)
        {
            const auto *in = ts_value_schema_at(context, 0);
            return in != nullptr && in->value_kind() == ValueTypeKind::List;
        }

        static void eval(In<"ts", TsVar<"S">> ts, Out<TS<Bool>> out)
        {
            out.set(ts.base().value().as_indexed_view().size() > 0);
        }
    };

    /** date -> datetime (midnight). */
    struct convert_date_to_datetime_impl
    {
        static constexpr auto name = "convert_date_to_datetime";

        static bool requires_(const ResolutionMap &, OperatorCallContext context)
        {
            return ts_value_schema_at(context, 0) ==
                   scalar_descriptor<Date>::value_meta();
        }

        static void eval(In<"ts", TS<Date>> ts, Out<TS<DateTime>> out)
        {
            out.set(DateTime{std::chrono::sys_days{ts.value()}});
        }
    };

    /** datetime -> date: the calendar-date component (the day floor). The
        companion of convert_date_to_datetime. */
    struct convert_datetime_to_date_impl
    {
        static constexpr auto name = "convert_datetime_to_date";

        // Concrete Out<TS<Date>> is gated by the dispatcher's requested-
        // output match; requires_ checks the INPUT only (the dispatch rule).
        static bool requires_(const ResolutionMap &, OperatorCallContext context)
        {
            return ts_value_schema_at(context, 0) ==
                   scalar_descriptor<DateTime>::value_meta();
        }

        static void eval(In<"ts", TS<DateTime>> ts, Out<TS<Date>> out)
        {
            out.set(Date{std::chrono::floor<std::chrono::days>(ts.value())});
        }
    };

    /** Box a concrete scalar TS into the native Any value. */
    struct convert_to_any_impl
    {
        static constexpr auto name = "convert_to_any";

        static bool requires_(const ResolutionMap &resolution, OperatorCallContext context)
        {
            static_cast<void>(resolution);
            const auto *in = time_series_schema_at(context, 0);
            return in != nullptr && in->kind == TSTypeKind::TS &&
                   in->value_schema != TypeRegistry::instance().any();
        }

        static void eval(In<"ts", TsVar<"S">> ts, Out<TS<AnyValue>> out)
        {
            out.set(ts.base().value());
        }
    };

    /** Convert the concrete value contained by Any using the native runtime
        conversion table. */
    struct convert_from_any_impl
    {
        static constexpr auto name = "convert_from_any";

        static bool requires_(const ResolutionMap &resolution, OperatorCallContext context)
        {
            const auto *in = time_series_schema_at(context, 0);
            const auto *out = ts_value_schema(resolution.find_ts("O"));
            return in != nullptr && in->kind == TSTypeKind::TS &&
                   in->value_schema == TypeRegistry::instance().any() &&
                   out != nullptr && out != TypeRegistry::instance().any();
        }

        static void eval(In<"ts", TS<AnyValue>> ts, Out<TsVar<"O">> out)
        {
            const ValueView inner = ts.contained_value();
            const auto &erased = static_cast<const TSOutputView &>(out);
            Value converted = ValueConversionRegistry::instance().convert(
                inner, erased.schema()->value_schema);
            auto mutation = erased.begin_mutation(erased.evaluation_time());
            if (!mutation.move_value_from(std::move(converted)))
            {
                throw std::logic_error("convert from Any failed to apply the converted value");
            }
        }
    };

    /** convert TS[T] -> TS[Set[T]] / TS[tuple[T,...]]: the SINGLETON
        collection value. */
    struct convert_ts_to_collection_impl
    {
        static constexpr auto name = "convert_ts_to_collection";

        static bool requires_(const ResolutionMap &resolution, OperatorCallContext context)
        {
            const auto *out = output_ts_value_schema(resolution);
            const auto *in  = ts_value_schema_at(context, 0);
            return collection_element_schema(out) == in && in != nullptr;
        }

        static void eval(In<"ts", TsVar<"S">> ts, Out<TsVar<"__out__">> out)
        {
            const auto &erased = static_cast<const TSOutputView &>(out);
            const auto *meta   = erased.schema()->value_schema;
            const auto  value  = ts.base().value();
            Value       result;
            if (meta->value_kind() == ValueTypeKind::Set)
            {
                SetBuilder builder{ValuePlanFactory::instance().type_for(meta->element_type)};
                static_cast<void>(builder.insert_copy(value.data()));
                result = builder.build();
            }
            else
            {
                ListBuilder builder{ValuePlanFactory::instance().type_for(meta->element_type)};
                builder.push_back_copy(value.data());
                result = builder.build();
            }
            auto mutation = erased.data_view().begin_mutation(erased.evaluation_time());
            static_cast<void>(mutation.move_value_from(std::move(result)));
        }
    };

    /** convert between COLLECTION-VALUED TS: tuple <-> set (same element). */
    struct convert_collection_to_collection_impl
    {
        static constexpr auto name = "convert_collection_to_collection";

        static bool requires_(const ResolutionMap &resolution, OperatorCallContext context)
        {
            const auto *out         = output_ts_value_schema(resolution);
            const auto *in          = ts_value_schema_at(context, 0);
            const auto *out_element = collection_element_schema(out);
            const auto *in_element  = collection_element_schema(in);
            return out != nullptr && in != nullptr && out != in &&
                   out_element != nullptr && out_element == in_element;
        }

        static void eval(In<"ts", TsVar<"S">> ts, Out<TsVar<"__out__">> out)
        {
            const auto &erased = static_cast<const TSOutputView &>(out);
            const auto *meta   = erased.schema()->value_schema;
            const auto  value  = ts.base().value();
            auto        items  = value.as_indexed_view();
            Value       result;
            if (meta->value_kind() == ValueTypeKind::Set)
            {
                SetBuilder builder{ValuePlanFactory::instance().type_for(meta->element_type)};
                for (std::size_t index = 0; index < items.size(); ++index)
                {
                    static_cast<void>(builder.insert_copy(items.at(index).data()));
                }
                result = builder.build();
            }
            else
            {
                ListBuilder builder{ValuePlanFactory::instance().type_for(meta->element_type)};
                for (std::size_t index = 0; index < items.size(); ++index)
                {
                    builder.push_back_copy(items.at(index).data());
                }
                result = builder.build();
            }
            auto mutation = erased.data_view().begin_mutation(erased.evaluation_time());
            static_cast<void>(mutation.move_value_from(std::move(result)));
        }
    };

    /** ``convert[TS[tuple[T, ...]]](series)`` copies one Arrow column into
        the native compact variadic-tuple storage. Arrow nulls remain unset
        tuple elements and therefore bridge back to Python as ``None``. */
    struct convert_series_to_tuple_impl
    {
        static constexpr auto name = "convert_series_to_tuple";

        static bool requires_(const ResolutionMap &resolution, OperatorCallContext context)
        {
            const auto *out = output_ts_value_schema(resolution);
            const auto *in  = ts_value_schema_at(context, 0);
            return out != nullptr && in != nullptr &&
                   out->has(ValueTypeFlags::VariadicTuple) &&
                   TypeRegistry::instance().is_series(in) &&
                   out->element_type != nullptr && out->element_type == in->element_type;
        }

        static void eval(In<"ts", TS<ScalarVar<"S">>> ts, Out<TsVar<"__out__">> out)
        {
            const auto &erased       = static_cast<const TSOutputView &>(out);
            const auto *output_meta  = erased.schema()->value_schema;
            const auto *element_meta = output_meta->element_type;
            const auto  binding      = ValuePlanFactory::instance().type_for(element_meta);
            const auto  series_value = ts.base().value();
            const auto &series       = series_value.checked_as<Series>();

            ListBuilder builder{binding};
            if (series.has_value())
            {
                for (std::int64_t index = 0; index < series.array->length(); ++index)
                {
                    Value value = array_cell(*series.array, element_meta, index);
                    if (value.has_value()) { builder.push_back_copy(value.view().data()); }
                    else { builder.push_back_unset(); }
                }
            }

            ListStorage storage = builder.build_storage();
            Value result{compact_list_type(binding, *output_meta), &storage};
            auto mutation = erased.data_view().begin_mutation(erased.evaluation_time());
            static_cast<void>(mutation.move_value_from(std::move(result)));
        }
    };

    /** convert TSS[T] -> TS[Set[T]] / TS[tuple[T,...]]: the full membership
        as a scalar collection each tick. */
    struct convert_tss_to_collection_impl
    {
        static constexpr auto name = "convert_tss_to_collection";

        static bool requires_(const ResolutionMap &resolution, OperatorCallContext context)
        {
            const auto *out_element = collection_element_schema(output_ts_value_schema(resolution));
            const auto *in          = time_series_schema_at_as<AnyTSS>(context, 0);
            return out_element != nullptr &&
                   in != nullptr &&
                   out_element == in->value_schema->element_type;
        }

        static void eval(In<"ts", TsVar<"S">> ts, Out<TsVar<"__out__">> out)
        {
            const auto  &erased = static_cast<const TSOutputView &>(out);
            const auto  *meta   = erased.schema()->value_schema;
            TSSInputView set_input{ts.base().borrowed_ref()};
            auto         set    = set_input.data_view();
            Value        result;
            if (meta->value_kind() == ValueTypeKind::Set)
            {
                SetBuilder builder{ValuePlanFactory::instance().type_for(meta->element_type)};
                for (const ValueView &element : set.values())
                {
                    static_cast<void>(builder.insert_copy(element.data()));
                }
                result = builder.build();
            }
            else
            {
                ListBuilder builder{ValuePlanFactory::instance().type_for(meta->element_type)};
                for (const ValueView &element : set.values()) { builder.push_back_copy(element.data()); }
                result = builder.build();
            }
            auto mutation = erased.data_view().begin_mutation(erased.evaluation_time());
            static_cast<void>(mutation.move_value_from(std::move(result)));
        }
    };

    /** convert TS[Set[T]] / TS[tuple[T,...]] -> TSS[T]: desired-membership
        writes (adds + removals fall out of the diff). */
    struct convert_collection_to_tss_impl
    {
        static constexpr auto name = "convert_collection_to_tss";

        static bool requires_(const ResolutionMap &resolution, OperatorCallContext context)
        {
            const auto *out = output_schema(resolution);
            const auto *in  = ts_value_schema_at(context, 0);
            return output_matches<AnyTSS>(resolution) &&
                   collection_element_schema(in) == out->value_schema->element_type;
        }

        static void eval(In<"ts", TsVar<"S">> ts, Out<TsVar<"__out__">> out)
        {
            const auto &erased  = static_cast<const TSOutputView &>(out);
            auto        set     = erased.as_set();
            auto        mutation = set.begin_mutation(erased.evaluation_time());
            const auto  value   = ts.base().value();
            auto        items   = value.as_indexed_view();

            const auto contains_in_desired = [&](const ValueView &element) {
                for (std::size_t index = 0; index < items.size(); ++index)
                {
                    if (items.at(index).equals(element)) { return true; }
                }
                return false;
            };
            std::vector<Value> stale;
            const auto mutation_view = mutation.view();
            for (const ValueView &element : mutation_view.values())
            {
                if (!contains_in_desired(element)) { stale.emplace_back(element); }
            }
            for (const Value &element : stale) { static_cast<void>(mutation.remove(element.view())); }
            for (std::size_t index = 0; index < items.size(); ++index)
            {
                static_cast<void>(mutation.add(items.at(index)));
            }
        }
    };

    /** convert TSD[K, TS[V]] -> TS[Mapping[K, V]]: the full dictionary as a
        scalar map each tick. */
    struct convert_tsd_to_map_impl
    {
        static constexpr auto name = "convert_tsd_to_map";

        static bool requires_(const ResolutionMap &resolution, OperatorCallContext context)
        {
            const auto *out = output_ts_value_schema(resolution);
            const auto *in  = time_series_schema_at_as<AnyTSD>(context, 0);
            if (out == nullptr ||
                in == nullptr ||
                out->value_kind() != ValueTypeKind::Map)
            {
                return false;
            }
            const auto *element = time_series_schema_as<AnyTS>(in->element_ts());
            return element != nullptr &&
                   out->key_type == in->key_type() && out->element_type == element->value_schema;
        }

        static void eval(In<"ts", TsVar<"S">> ts, Out<TsVar<"__out__">> out)
        {
            const auto  &erased = static_cast<const TSOutputView &>(out);
            const auto  *meta   = erased.schema()->value_schema;
            const TSDInputView dict{ts.base().borrowed_ref()};
            MapBuilder         builder{ValuePlanFactory::instance().type_for(meta->key_type),
                                       ValuePlanFactory::instance().type_for(meta->element_type)};
            for (auto &&[key, child] : dict.items())
            {
                if (!child.valid()) { continue; }
                builder.set_item_copy(key.data(), child.value().data());
            }
            auto mutation = erased.data_view().begin_mutation(erased.evaluation_time());
            static_cast<void>(mutation.move_value_from(builder.build()));
        }
    };

    /** convert TS[Mapping[K, V]] -> TSD[K, TS[V]]: desired-map writes. */
    struct convert_map_to_tsd_impl
    {
        static constexpr auto name = "convert_map_to_tsd";

        static bool requires_(const ResolutionMap &resolution, OperatorCallContext context)
        {
            const auto *out = output_schema(resolution);
            const auto *in  = ts_value_schema_at(context, 0);
            if (!output_matches<AnyTSD>(resolution) ||
                in == nullptr || in->value_kind() != ValueTypeKind::Map)
            {
                return false;
            }
            const auto *element = time_series_schema_as<AnyTS>(out->element_ts());
            return element != nullptr &&
                   out->key_type() == in->key_type && element->value_schema == in->element_type;
        }

        static void eval(In<"ts", TsVar<"S">> ts, Out<TsVar<"__out__">> out)
        {
            const auto &erased = static_cast<const TSOutputView &>(out);
            auto        dict   = erased.as_dict();
            auto        mutation = dict.begin_mutation(erased.evaluation_time());
            const auto  value  = ts.base().value();
            auto        map    = value.as_map();

            // Erase keys no longer present, then write every desired entry
            // (equal values dedup before the write - READ-side compare, the
            // flip_keys lesson).
            std::vector<Value> stale;
            const auto mutation_view = mutation.view();
            for (const ValueView &key : mutation_view.keys())
            {
                if (!map.contains(key)) { stale.emplace_back(key); }
            }
            for (const Value &key : stale) { static_cast<void>(mutation.erase(key.view())); }
            for (const auto [key, entry_value] : map)
            {
                auto element = mutation.at(key);
                if (element.has_current_value() && element.value().equals(entry_value)) { continue; }
                auto element_mutation = element.begin_mutation(erased.evaluation_time());
                static_cast<void>(element_mutation.copy_value_from(entry_value));
            }
        }
    };

    /** convert TS[T] -> TSS[T]: the tick value becomes the SINGLETON set
        (write-desired semantics produce add-new/remove-old deltas). */
    struct convert_ts_to_tss_impl
    {
        static constexpr auto name = "convert_ts_to_tss";

        static bool requires_(const ResolutionMap &resolution, OperatorCallContext context)
        {
            const auto *out = output_schema(resolution);
            const auto *in  = time_series_schema_at_as<AnyTS>(context, 0);
            return output_matches<AnyTSS>(resolution) &&
                   in != nullptr &&
                   out->value_schema->element_type == in->value_schema;
        }

        static void eval(In<"ts", TsVar<"S">> ts, Out<TsVar<"__out__">> out)
        {
            const auto &erased = static_cast<const TSOutputView &>(out);
            auto        set    = erased.as_set();
            auto        mutation = set.begin_mutation(erased.evaluation_time());
            const auto  value  = ts.base().value();
            // Desired = {value}: remove everything else, insert the value.
            std::vector<Value> stale;
            const auto mutation_view = mutation.view();
            for (const ValueView &element : mutation_view.values())
            {
                if (!element.equals(value)) { stale.emplace_back(element); }
            }
            for (const Value &element : stale) { static_cast<void>(mutation.remove(element.view())); }
            static_cast<void>(mutation.add(value));
        }
    };

    /** convert[TSD](keys, value): the desired dictionary {current keys ->
        current value}; previous keys drop out. Keys may arrive as a scalar
        TS[K], a set-valued TS[Set[K]], or a TSS[K] membership. */
    struct convert_kv_to_tsd_impl
    {
        static constexpr auto name = "convert_kv_to_tsd";

        static bool requires_(const ResolutionMap &resolution, OperatorCallContext context)
        {
            const auto *out = output_schema(resolution);
            const auto *v   = ts_value_schema_at(context, 1);
            if (!output_matches<AnyTSD>(resolution) ||
                v == nullptr)
            {
                return false;
            }
            const auto *element = time_series_schema_as<AnyTS>(out->element_ts());
            if (element == nullptr || element->value_schema != v)
            {
                return false;
            }
            const auto *keys = time_series_schema_at(context, 0);
            if (keys == nullptr) { return false; }
            if (const auto *key_set = time_series_schema_as<AnyTSS>(keys))
            {
                return key_set->value_schema->element_type == out->key_type();
            }
            const auto *key_ts = time_series_schema_as<AnyTS>(keys);
            if (key_ts == nullptr) { return false; }
            const auto *key_value = key_ts->value_schema;
            if (key_value->value_kind() == ValueTypeKind::Set) { return key_value->element_type == out->key_type(); }
            return key_value == out->key_type();
        }

        static void eval(In<"key", TsVar<"K">> key, In<"ts", TsVar<"S">> ts, Out<TsVar<"__out__">> out)
        {
            const auto &erased  = static_cast<const TSOutputView &>(out);
            auto        dict    = erased.as_dict();
            auto        mutation = dict.begin_mutation(erased.evaluation_time());

            // The desired key set for THIS cycle.
            std::vector<Value> desired;
            const auto *key_schema = key.base().schema();
            if (key_schema->kind == TSTypeKind::TSS)
            {
                const TSSInputView set_input{key.base().borrowed_ref()};
                auto data = set_input.data_view();
                for (const ValueView &element : data.values()) { desired.emplace_back(element); }
            }
            else
            {
                const auto value = key.base().value();
                if (value.schema()->value_kind() == ValueTypeKind::Set)
                {
                    auto items = value.as_indexed_view();
                    for (std::size_t index = 0; index < items.size(); ++index)
                    {
                        desired.emplace_back(items.at(index));
                    }
                }
                else { desired.emplace_back(value); }
            }

            const auto is_desired = [&](const ValueView &candidate) {
                for (const Value &want : desired)
                {
                    if (want.view().equals(candidate)) { return true; }
                }
                return false;
            };
            std::vector<Value> stale;
            const auto mutation_view = mutation.view();
            for (const ValueView &existing : mutation_view.keys())
            {
                if (!is_desired(existing)) { stale.emplace_back(existing); }
            }
            for (const Value &existing : stale) { static_cast<void>(mutation.erase(existing.view())); }

            const auto value = ts.base().value();
            for (const Value &want : desired)
            {
                auto element = mutation.at(want.view());
                if (element.has_current_value() && element.value().equals(value)) { continue; }
                auto element_mutation = element.begin_mutation(erased.evaluation_time());
                static_cast<void>(element_mutation.copy_value_from(value));
            }
        }
    };

    // ----- combine over date/time scalars ---------------------------------

    /** combine[TS[date]](year=, month=, day=). */
    struct combine_date_impl
    {
        static constexpr auto name = "combine_date";

        static void eval(In<"year", TS<Int>> year, In<"month", TS<Int>> month, In<"day", TS<Int>> day,
                         Out<TS<Date>> out)
        {
            out.set(Date{std::chrono::year{static_cast<int>(year.value())},
                         std::chrono::month{static_cast<unsigned>(month.value())},
                         std::chrono::day{static_cast<unsigned>(day.value())}});
        }
    };

    /** combine[TS[timedelta]](weeks=, days=, ..., microseconds=): every
        component optional (the py bridge fills absent ones with const 0).
        STRICT is the default validity (all supplied inputs valid); the
        lenient variant treats not-yet-valid components as zero. */
    template <bool Strict>
    struct combine_timedelta_impl_base
    {
        static constexpr auto name = Strict ? "combine_timedelta" : "combine_timedelta_lenient";
        static constexpr InputValidity validity = Strict ? InputValidity::Valid : InputValidity::Unchecked;

        [[nodiscard]] static Int value_or_zero(const auto &input)
        {
            if constexpr (Strict) { return input.value(); }
            else { return input.valid() ? input.value() : Int{0}; }
        }

        static void compute(const auto &weeks, const auto &days, const auto &hours, const auto &minutes,
                            const auto &seconds, const auto &milliseconds, const auto &microseconds,
                            Out<TS<TimeDelta>> &out)
        {
            const Int total_days = value_or_zero(weeks) * 7 + value_or_zero(days);
            const Int total_seconds =
                (value_or_zero(hours) * 60 + value_or_zero(minutes)) * 60 + value_or_zero(seconds);
            out.set(TimeDelta{((total_days * 86'400 + total_seconds) * 1'000 + value_or_zero(milliseconds)) *
                                  1'000 +
                              value_or_zero(microseconds)});
        }

    };

    template <bool Strict>
    struct combine_timedelta_impl;

    template <>
    struct combine_timedelta_impl<true> : combine_timedelta_impl_base<true>
    {
        using base = combine_timedelta_impl_base<true>;

        static void eval(In<"weeks", TS<Int>, validity> weeks,
                         In<"days", TS<Int>, validity> days,
                         In<"hours", TS<Int>, validity> hours,
                         In<"minutes", TS<Int>, validity> minutes,
                         In<"seconds", TS<Int>, validity> seconds,
                         In<"milliseconds", TS<Int>, validity> milliseconds,
                         In<"microseconds", TS<Int>, validity> microseconds,
                         Out<TS<TimeDelta>> out)
        {
            base::compute(weeks, days, hours, minutes, seconds, milliseconds, microseconds, out);
        }
    };

    template <>
    struct combine_timedelta_impl<false> : combine_timedelta_impl_base<false>
    {
        using base = combine_timedelta_impl_base<false>;

        // The lenient variant carries the __strict__ scalar so dispatch can
        // tell the otherwise-identical signatures apart.
        static void eval(In<"weeks", TS<Int>, validity> weeks,
                         In<"days", TS<Int>, validity> days,
                         In<"hours", TS<Int>, validity> hours,
                         In<"minutes", TS<Int>, validity> minutes,
                         In<"seconds", TS<Int>, validity> seconds,
                         In<"milliseconds", TS<Int>, validity> milliseconds,
                         In<"microseconds", TS<Int>, validity> microseconds,
                         Scalar<"__strict__", Bool>,
                         Out<TS<TimeDelta>> out)
        {
            base::compute(weeks, days, hours, minutes, seconds, milliseconds, microseconds, out);
        }
    };

    /** combine[TS[datetime]](date=, time=). */
    struct combine_datetime_impl
    {
        static constexpr auto name = "combine_datetime";

        static void eval(In<"date", TS<Date>> date, In<"time", TS<Time>> time, Out<TS<DateTime>> out)
        {
            out.set(DateTime{std::chrono::sys_days{date.value()}} +
                    TimeDelta{time.value().microseconds});
        }
    };

    /** convert[TS[Mapping]](k, v): the SINGLETON mapping {k: v}. */
    struct convert_kv_to_map_impl
    {
        static constexpr auto name = "convert_kv_to_map";

        static bool requires_(const ResolutionMap &resolution, OperatorCallContext context)
        {
            const auto *out = output_ts_value_schema(resolution);
            const auto *k   = ts_value_schema_at(context, 0);
            const auto *v   = ts_value_schema_at(context, 1);
            return out != nullptr && out->value_kind() == ValueTypeKind::Map && k != nullptr && v != nullptr &&
                   out->key_type == k && out->element_type == v;
        }

        static void eval(In<"key", TsVar<"K">> key, In<"ts", TsVar<"S">> ts, Out<TsVar<"__out__">> out)
        {
            const auto &erased = static_cast<const TSOutputView &>(out);
            const auto *meta   = erased.schema()->value_schema;
            MapBuilder  builder{ValuePlanFactory::instance().type_for(meta->key_type),
                                ValuePlanFactory::instance().type_for(meta->element_type)};
            builder.set_item_copy(key.base().value().data(), ts.base().value().data());
            auto mutation = erased.data_view().begin_mutation(erased.evaluation_time());
            static_cast<void>(mutation.move_value_from(builder.build()));
        }
    };

    /** convert[TS[tuple[V,...]]](tsl): the VALID children of a fixed TSL as
        an ordered tuple (all-valid gated so a partial TSL stays invalid,
        hgraph parity). */
    template <bool Strict>
    struct convert_tsl_to_tuple_impl_base
    {
        static constexpr auto name = Strict ? "convert_tsl_to_tuple" : "convert_tsl_to_tuple_lenient";
        static constexpr InputValidity validity = Strict ? InputValidity::AllValid : InputValidity::Unchecked;

        static bool requires_(const ResolutionMap &resolution, OperatorCallContext context)
        {
            const auto *out = tuple_element_schema(output_ts_value_schema(resolution));
            const auto *in  = fixed_tsl_arg(context, 0);
            if (out == nullptr || in == nullptr) { return false; }
            const auto *element = time_series_schema_as<AnyTS>(in->element_ts());
            return element != nullptr && element->value_schema == out;
        }

        static void eval_impl(const TSInputView &tsl, const TSOutputView &erased)
        {
            const auto *meta = erased.schema()->value_schema;
            if (meta->value_kind() == ValueTypeKind::Tuple)
            {
                // FIXED tuple: per-slot validity survives (lenient holes).
                BundleBuilder builder{ValuePlanFactory::instance().type_for(meta)};
                for (std::size_t index = 0; index < tsl.schema()->fixed_size(); ++index)
                {
                    auto child = tsl.indexed_child_at(index);
                    if (child.valid()) { builder.set(index, Value{child.value()}); }
                }
                publish(erased, builder.build());
            }
            else
            {
                // Dynamic LIST (variadic tuple): invalid children become
                // HOLES via element validity (unknown-size nullability - the
                // sul-style bitset on the compact list). Strict is all-valid
                // gated, so it never holes.
                ListBuilder builder{ValuePlanFactory::instance().type_for(meta->element_type)};
                for (std::size_t index = 0; index < tsl.schema()->fixed_size(); ++index)
                {
                    auto child = tsl.indexed_child_at(index);
                    if (child.valid()) { builder.push_back_copy(child.value().data()); }
                    else { builder.push_back_unset(); }
                }
                publish(erased, builder.build());
            }
        }

        static void publish(const TSOutputView &erased, Value &&tuple)
        {
            if (erased.data_view().has_current_value() && erased.value().equals(tuple.view())) { return; }
            auto mutation = erased.data_view().begin_mutation(erased.evaluation_time());
            static_cast<void>(mutation.move_value_from(std::move(tuple)));
        }

    };

    template <bool Strict>
    struct convert_tsl_to_tuple_impl;

    template <>
    struct convert_tsl_to_tuple_impl<true> : convert_tsl_to_tuple_impl_base<true>
    {
        static void eval(In<"ts", TsVar<"S">, validity> ts, Out<TsVar<"__out__">> out)
        {
            eval_impl(ts, static_cast<const TSOutputView &>(out));
        }
    };

    template <>
    struct convert_tsl_to_tuple_impl<false> : convert_tsl_to_tuple_impl_base<false>
    {
        static void eval(In<"ts", TsVar<"S">, validity> ts, Scalar<"__strict__", Bool>,
                         Out<TsVar<"__out__">> out)
        {
            eval_impl(ts, static_cast<const TSOutputView &>(out));
        }
    };

    /** combine[TSS](a, b, ...): a TSS whose membership is the set of the
        CURRENT values of the valid scalar inputs (desired-membership
        reconciliation - adds/removes fall out of the diff). */
    struct combine_tss_scalars_impl
    {
        static constexpr auto name = "combine_tss_scalars";

        static bool requires_(const ResolutionMap &resolution, OperatorCallContext context)
        {
            const auto *out = output_schema(resolution);
            if (!output_matches<AnyTSS>(resolution) ||
                context.args.empty())
            {
                return false;
            }
            const auto *element = out->value_schema->element_type;
            for (std::size_t index = 0; index < context.args.size(); ++index)
            {
                const auto *arg = time_series_schema_at_as<AnyTS>(context, index);
                if (arg == nullptr || arg->value_schema != element) { return false; }
            }
            return true;
        }

        static WiringPortRef compose(Wiring &w, VarIn<"ts", TS<ScalarVar<"T">>> ts)
        {
            if (ts.empty()) { throw std::invalid_argument("combine[TSS] requires at least one input"); }
            auto &registry = TypeRegistry::instance();
            std::vector<WiringPortRef> children{ts.begin(), ts.end()};
            WiringPortRef packed =
                WiringPortRef::structural_source(registry.tsl(ts[0].schema, ts.size()), std::move(children));
            std::array<WiringArg, 1> args{};
            args[0].kind = WiringArg::Kind::TimeSeries;
            args[0].port = packed;
            auto result = wire_operator(w, "combine_tss_from_tsl", {args.data(), args.size()}, true);
            return result.output.erased();
        }
    };

    /** The packed-TSL kernel behind combine[TSS]: reconcile the desired
        membership from the valid children each tick. */
    struct combine_tss_from_tsl_impl
    {
        static constexpr auto name = "combine_tss_from_tsl";

        static void resolve_default_types(ResolutionMap &resolution, OperatorCallContext context)
        {
            if (output_bound(resolution)) { return; }
            const auto *tsl = fixed_tsl_arg(context, 0);
            if (tsl == nullptr) { return; }
            const auto *element = time_series_schema_as<AnyTS>(tsl->element_ts());
            if (element == nullptr) { return; }
            bind_output(resolution, TypeRegistry::instance().tss(element->value_schema));
        }

        static void eval(In<"ts", TSL<TS<ScalarVar<"T">>, SIZE<"N">>> tsl, Out<TsVar<"__out__">> out)
        {
            const auto &erased  = static_cast<const TSOutputView &>(out);
            auto        set     = erased.as_set();
            auto        mutation = set.begin_mutation(erased.evaluation_time());

            std::vector<Value> desired;
            for (std::size_t index = 0; index < tsl.size(); ++index)
            {
                auto child = tsl[index];
                if (child.valid()) { desired.emplace_back(child.base().value()); }
            }
            const auto wanted = [&](const ValueView &candidate) {
                for (const Value &d : desired) { if (d.view().equals(candidate)) { return true; } }
                return false;
            };
            std::vector<Value> stale;
            const auto mutation_view = mutation.view();
            for (const ValueView &element : mutation_view.values())
            {
                if (!wanted(element)) { stale.emplace_back(element); }
            }
            for (const Value &element : stale) { static_cast<void>(mutation.remove(element.view())); }
            for (const Value &d : desired) { static_cast<void>(mutation.add(d.view())); }
        }
    };

    /** convert[TSD](tsl): enumerate fixed TSL elements into a desired TSD.
        This is a graph-level adapter over combine_tsd so the dictionary
        reference/value semantics stay in one implementation. */
    struct convert_tsl_to_tsd_impl
    {
        static constexpr auto name = "convert_tsl_to_tsd";

        static auto defaults() { return std::tuple{arg<"__strict__">(Bool{false})}; }

        static bool requires_(const ResolutionMap &resolution, OperatorCallContext context)
        {
            const auto *out = output_schema(resolution);
            const auto *in  = fixed_tsl_arg(context, 0);
            if (!output_matches<AnyTSD>(resolution) ||
                in == nullptr)
            {
                return false;
            }
            auto &registry = TypeRegistry::instance();
            if (out->key_type() != registry.value_type("int")) { return false; }
            const auto *out_element = time_series_schema_as<AnyTS>(out->element_ts());
            const auto *in_element  = time_series_schema_as<AnyTS>(in->element_ts());
            return out_element != nullptr && out_element == in_element;
        }

        static WiringPortRef compose(Wiring &w, NamedPort<"ts", TsVar<"S">> ts,
                                     Scalar<"__strict__", Bool> strict)
        {
            auto       &registry = TypeRegistry::instance();
            const auto *schema   = time_series_schema_as<AnyTSL>(ts.erased().schema);
            if (schema == nullptr || schema->fixed_size() == 0)
            {
                throw std::invalid_argument("convert[TSD](tsl) requires a fixed-size TSL input");
            }

            ListBuilder keys{ValuePlanFactory::instance().type_for(registry.value_type("int"))};
            for (std::size_t index = 0; index < schema->fixed_size(); ++index)
            {
                keys.push_back(static_cast<Int>(index));
            }

            std::vector<WiringArg> args;
            args.reserve(schema->fixed_size() + 2);
            WiringArg keys_arg;
            keys_arg.kind         = WiringArg::Kind::Scalar;
            keys_arg.scalar_value = keys.build();
            keys_arg.scalar_meta  = keys_arg.scalar_value.schema();
            args.push_back(std::move(keys_arg));

            for (std::size_t index = 0; index < schema->fixed_size(); ++index)
            {
                WiringArg value_arg;
                value_arg.kind = WiringArg::Kind::TimeSeries;
                value_arg.port = subgraph_wiring_detail::tsl_element_ref(ts.erased(), index, schema->element_ts());
                args.push_back(std::move(value_arg));
            }

            WiringArg strict_arg;
            strict_arg.kind         = WiringArg::Kind::Scalar;
            strict_arg.scalar_value = Value{strict.value()};
            strict_arg.scalar_meta  = strict_arg.scalar_value.schema();
            strict_arg.name         = "__strict__";
            args.push_back(std::move(strict_arg));

            auto result = wire_operator(w, "combine_tsd", {args.data(), args.size()}, true);
            return result.output.erased();
        }
    };

    /** convert[TSD](k_tuple_ts, v_tuple_ts): ZIP two TS[tuple] into a desired
        dictionary each tick (previous keys drop). */
    struct convert_zip_to_tsd_impl
    {
        static constexpr auto name = "convert_zip_to_tsd";

        static bool requires_(const ResolutionMap &resolution, OperatorCallContext context)
        {
            const auto *out = output_schema(resolution);
            const auto *k   = ts_value_schema_at(context, 0);
            const auto *v   = ts_value_schema_at(context, 1);
            if (!output_matches<AnyTSD>(resolution) ||
                k == nullptr || v == nullptr || k->value_kind() != ValueTypeKind::List ||
                v->value_kind() != ValueTypeKind::List)
            {
                return false;
            }
            const auto *element = time_series_schema_as<AnyTS>(out->element_ts());
            return out->key_type() == k->element_type && element != nullptr &&
                   element->value_schema == v->element_type;
        }

        static void eval(In<"key", TsVar<"K">, InputValidity::Unchecked> key,
                         In<"ts", TsVar<"S">, InputValidity::Unchecked> ts, Out<TsVar<"__out__">> out)
        {
            if (!key.modified() && !ts.modified()) { return; }
            const auto &erased  = static_cast<const TSOutputView &>(out);
            auto        dict    = erased.as_dict();
            auto        mutation = dict.begin_mutation(erased.evaluation_time());
            auto        keys    = key.valid() ? std::optional{key.base().value().as_indexed_view()} : std::nullopt;
            auto        values  = ts.valid() ? std::optional{ts.base().value().as_indexed_view()} : std::nullopt;
            const std::size_t count = (keys && values) ? std::min(keys->size(), values->size()) : 0;

            const auto wanted = [&](const ValueView &candidate) {
                for (std::size_t index = 0; index < count; ++index)
                {
                    if (keys->at(index).equals(candidate)) { return true; }
                }
                return false;
            };
            std::vector<Value> stale;
            const auto mutation_view = mutation.view();
            for (const ValueView &existing : mutation_view.keys())
            {
                if (!wanted(existing)) { stale.emplace_back(existing); }
            }
            for (const Value &existing : stale) { static_cast<void>(mutation.erase(existing.view())); }
            for (std::size_t index = 0; index < count; ++index)
            {
                auto element = mutation.at(keys->at(index));
                if (element.has_current_value() && element.value().equals(values->at(index))) { continue; }
                auto element_mutation = element.begin_mutation(erased.evaluation_time());
                static_cast<void>(element_mutation.copy_value_from(values->at(index)));
            }
        }
    };

    /** convert[TS[Mapping]](k_tuple, v_tuple): ZIP the paired tuples into a
        mapping value. */
    struct convert_zip_to_map_impl
    {
        static constexpr auto name = "convert_zip_to_map";

        static bool requires_(const ResolutionMap &resolution, OperatorCallContext context)
        {
            const auto *out = output_ts_value_schema(resolution);
            const auto *k   = ts_value_schema_at(context, 0);
            const auto *v   = ts_value_schema_at(context, 1);
            return out != nullptr && out->value_kind() == ValueTypeKind::Map && k != nullptr && v != nullptr &&
                   k->value_kind() == ValueTypeKind::List && v->value_kind() == ValueTypeKind::List &&
                   out->key_type == k->element_type && out->element_type == v->element_type;
        }

        static void eval(In<"key", TsVar<"K">> key, In<"ts", TsVar<"S">> ts, Out<TsVar<"__out__">> out)
        {
            const auto &erased = static_cast<const TSOutputView &>(out);
            const auto *meta   = erased.schema()->value_schema;
            auto        keys   = key.base().value().as_indexed_view();
            auto        values = ts.base().value().as_indexed_view();
            MapBuilder  builder{ValuePlanFactory::instance().type_for(meta->key_type),
                                ValuePlanFactory::instance().type_for(meta->element_type)};
            const std::size_t count = std::min(keys.size(), values.size());
            for (std::size_t index = 0; index < count; ++index)
            {
                builder.set_item_copy(keys.at(index).data(), values.at(index).data());
            }
            auto mutation = erased.data_view().begin_mutation(erased.evaluation_time());
            static_cast<void>(mutation.move_value_from(builder.build()));
        }
    };

    /** convert[TS[Mapping[int, V]]](tsl): {index: element} over the VALID
        elements of a fixed TSL. */
    struct convert_tsl_to_map_impl
    {
        static constexpr auto name = "convert_tsl_to_map";

        static bool requires_(const ResolutionMap &resolution, OperatorCallContext context)
        {
            const auto *out = output_ts_value_schema(resolution);
            const auto *in  = fixed_tsl_arg(context, 0);
            if (out == nullptr || out->value_kind() != ValueTypeKind::Map || in == nullptr) { return false; }
            auto       &registry = TypeRegistry::instance();
            const auto *element  = time_series_schema_as<AnyTS>(in->element_ts());
            return out->key_type == registry.value_type("int") && element != nullptr &&
                   element->value_schema == out->element_type;
        }

        static void eval(In<"ts", TsVar<"S">> ts, Out<TsVar<"__out__">> out)
        {
            const auto  &erased = static_cast<const TSOutputView &>(out);
            const auto  *meta   = erased.schema()->value_schema;
            const TSInputView &tsl = ts;
            MapBuilder   builder{ValuePlanFactory::instance().type_for(meta->key_type),
                                 ValuePlanFactory::instance().type_for(meta->element_type)};
            for (std::size_t index = 0; index < tsl.schema()->fixed_size(); ++index)
            {
                auto child = tsl.indexed_child_at(index);
                if (!child.valid()) { continue; }
                Value key{static_cast<Int>(index)};
                builder.set_item_copy(key.view().data(), child.value().data());
            }
            auto mutation = erased.data_view().begin_mutation(erased.evaluation_time());
            static_cast<void>(mutation.move_value_from(builder.build()));
        }
    };

    /** convert[TS[Mapping[str, V]]](tsb): {field name: value} over the VALID
        fields of a homogeneous bundle. */
    struct convert_tsb_to_map_impl
    {
        static constexpr auto name = "convert_tsb_to_map";

        static bool requires_(const ResolutionMap &resolution, OperatorCallContext context)
        {
            const auto *out = output_ts_value_schema(resolution);
            const auto *in  = time_series_schema_at_as<AnyTSB>(context, 0);
            if (out == nullptr || out->value_kind() != ValueTypeKind::Map ||
                in == nullptr ||
                in->field_count() == 0)
            {
                return false;
            }
            auto &registry = TypeRegistry::instance();
            if (out->key_type != registry.value_type("str")) { return false; }
            for (std::size_t index = 0; index < in->field_count(); ++index)
            {
                const auto *field = time_series_schema_as<AnyTS>(in->fields()[index].type);
                if (field == nullptr || field->value_schema != out->element_type)
                {
                    return false;
                }
            }
            return true;
        }

        static void eval(In<"ts", TsVar<"S">> ts, Out<TsVar<"__out__">> out)
        {
            const auto        &erased = static_cast<const TSOutputView &>(out);
            const auto        *meta   = erased.schema()->value_schema;
            const TSInputView &bundle = ts;
            MapBuilder         builder{ValuePlanFactory::instance().type_for(meta->key_type),
                                       ValuePlanFactory::instance().type_for(meta->element_type)};
            for (std::size_t index = 0; index < bundle.schema()->field_count(); ++index)
            {
                auto child = bundle.indexed_child_at(index);
                if (!child.valid()) { continue; }
                Value key{Str{bundle.schema()->fields()[index].name}};
                builder.set_item_copy(key.view().data(), child.value().data());
            }
            auto mutation = erased.data_view().begin_mutation(erased.evaluation_time());
            static_cast<void>(mutation.move_value_from(builder.build()));
        }
    };

    /** collect[TS[Mapping]](k_tuple, v_tuple, reset=...): zip-accumulate. */
    /** ``combine(a=..., b=..., __strict__=True)``: the structural bundle
        GATED until every field is valid - the first emission is the full
        snapshot, deltas follow (hgraph's strict unnamed-TSB combine). */
    struct combine_tsb_strict_impl
    {
        static constexpr auto name = "combine_tsb_strict";

        static bool requires_(const ResolutionMap &, OperatorCallContext context)
        {
            const auto *in     = time_series_schema_at(context, 0);
            const auto *strict = context.scalar_as<Bool>("__strict__");
            return in != nullptr && in->kind == TSTypeKind::TSB && strict != nullptr && *strict;
        }

        static void resolve_default_types(ResolutionMap &resolution, OperatorCallContext context)
        {
            if (output_bound(resolution)) { return; }
            const auto *in = time_series_schema_at(context, 0);
            if (in != nullptr) { bind_output(resolution, in); }
        }

        static void eval(In<"ts", TsVar<"S">> ts, Scalar<"__strict__", Bool> strict,
                         Out<TsVar<"__out__">> out)
        {
            static_cast<void>(strict);
            if (!ts.base().all_valid()) { return; }
            const auto &erased     = static_cast<const TSOutputView &>(out);
            const bool  first_emit = !erased.valid();
            auto        bundle_in  = const_cast<TSInputView &>(ts.base()).as_bundle();
            auto        bundle_out = erased.as_bundle();
            for (std::size_t index = 0; index < bundle_in.size(); ++index)
            {
                auto child = bundle_in.at(index);
                if (!first_emit && !child.modified()) { continue; }
                apply_current_value(bundle_out.at(index), child.value());
            }
        }
    };

    struct collect_map_zip_impl
    {
        static constexpr auto name = "collect_map_zip";

        static std::vector<std::pair<std::string_view, Value>> defaults()
        {
            // None defaults: null sources - the inputs stay unwired.
            return {{"reset", Value{}}};
        }

        static bool requires_(const ResolutionMap &resolution, OperatorCallContext context)
        {
            const auto *out = output_ts_value_schema(resolution);
            const auto *k   = ts_value_schema_at(context, 0);
            const auto *v   = ts_value_schema_at(context, 1);
            return out != nullptr && out->value_kind() == ValueTypeKind::Map && k != nullptr && v != nullptr &&
                   k->value_kind() == ValueTypeKind::List && v->value_kind() == ValueTypeKind::List &&
                   out->key_type == k->element_type && out->element_type == v->element_type;
        }

        static void eval(In<"key", TsVar<"K">, InputValidity::Unchecked> key,
                         In<"ts", TsVar<"S">, InputValidity::Unchecked> ts,
                         In<"reset", TS<Bool>, InputValidity::Unchecked> reset,
                         Out<TsVar<"__out__">> out)
        {
            const bool fresh  = reset.valid() && reset.modified() && reset.value();
            const bool ticked = key.valid() && ts.valid() && (key.modified() || ts.modified());
            if (!fresh && !ticked) { return; }
            const auto &erased = static_cast<const TSOutputView &>(out);
            const auto *meta   = erased.schema()->value_schema;
            MapBuilder  builder{ValuePlanFactory::instance().type_for(meta->key_type),
                                ValuePlanFactory::instance().type_for(meta->element_type)};
            if (!fresh && erased.data_view().has_current_value())
            {
                const auto current = erased.value().as_map();
                for (const auto [k, v] : current) { builder.set_item_copy(k.data(), v.data()); }
            }
            if (ticked)
            {
                auto keys   = key.base().value().as_indexed_view();
                auto values = ts.base().value().as_indexed_view();
                const std::size_t count = std::min(keys.size(), values.size());
                for (std::size_t index = 0; index < count; ++index)
                {
                    builder.set_item_copy(keys.at(index).data(), values.at(index).data());
                }
            }
            auto mutation = erased.data_view().begin_mutation(erased.evaluation_time());
            static_cast<void>(mutation.move_value_from(builder.build()));
        }
    };

    /** combine[TS[Tuple]](a, b, ...): the FIXED tuple of the packed fields
        (heterogeneous allowed). Strict default; lenient fills None-slots...
        (hgraph relaxed = emit with whatever is valid; fixed tuples cannot
        hold holes, so lenient requires ALL-valid too but re-emits per tick). */
    template <bool Strict>
    struct combine_tuple_impl_base
    {
        static constexpr auto name = Strict ? "combine_tuple" : "combine_tuple_lenient";

        static void eval_impl(const TSInputView &fields, const TSOutputView &erased)
        {
            const auto *target = erased.schema()->value_schema;
            BundleBuilder builder{ValuePlanFactory::instance().type_for(target)};
            for (std::size_t index = 0; index < fields.schema()->field_count(); ++index)
            {
                auto child = fields.indexed_child_at(index);
                if (!child.valid())
                {
                    if constexpr (Strict) { return; }
                    continue;
                }
                builder.set(index, Value{child.value()});
            }
            Value tuple = builder.build();
            if (erased.data_view().has_current_value() && erased.value().equals(tuple.view())) { return; }
            auto mutation = erased.data_view().begin_mutation(erased.evaluation_time());
            static_cast<void>(mutation.move_value_from(std::move(tuple)));
        }

        static bool requires_(const ResolutionMap &resolution, OperatorCallContext)
        {
            const auto *out = output_ts_value_schema(resolution);
            return out != nullptr && out->value_kind() == ValueTypeKind::Tuple;
        }

    };

    template <bool Strict>
    struct combine_tuple_impl;

    template <>
    struct combine_tuple_impl<true> : combine_tuple_impl_base<true>
    {
        static void eval(In<"ts", TsVar<"S">, InputValidity::Unchecked> ts, Out<TsVar<"__out__">> out)
        {
            eval_impl(ts, static_cast<const TSOutputView &>(out));
        }
    };

    template <>
    struct combine_tuple_impl<false> : combine_tuple_impl_base<false>
    {
        static void eval(In<"ts", TsVar<"S">, InputValidity::Unchecked> ts, Scalar<"__strict__", Bool>,
                         Out<TsVar<"__out__">> out)
        {
            eval_impl(ts, static_cast<const TSOutputView &>(out));
        }
    };

    /** convert[TSL[TS[T], Size[N]]](tuple_ts): distribute the tuple's
        elements onto the fixed TSL's children. */
    struct convert_list_to_tsl_impl
    {
        static constexpr auto name = "convert_list_to_tsl";

        static bool requires_(const ResolutionMap &resolution, OperatorCallContext context)
        {
            const auto *out = output_schema(resolution);
            const auto *in  = ts_value_schema_at(context, 0);
            if (!output_matches<AnyTSL>(resolution) ||
                out->fixed_size() == 0 || in == nullptr)
            {
                return false;
            }
            if (in->value_kind() == ValueTypeKind::Tuple && in->field_count != out->fixed_size()) { return false; }
            const ValueTypeMetaData *source_element = tuple_element_schema(in);
            if (source_element == nullptr) { return false; }
            const auto *element = time_series_schema_as<AnyTS>(out->element_ts());
            return element != nullptr && element->value_schema == source_element;
        }

        static void eval(In<"ts", TsVar<"S">> ts, Out<TsVar<"__out__">> out)
        {
            const auto &erased = static_cast<const TSOutputView &>(out);
            auto        items  = ts.base().value().as_indexed_view();
            auto        list   = erased.as_list();
            const std::size_t count = std::min<std::size_t>(items.size(), erased.schema()->fixed_size());
            for (std::size_t index = 0; index < count; ++index)
            {
                auto child = list.at(index);
                if (child.data_view().has_current_value() && child.value().equals(items.at(index))) { continue; }
                auto mutation = child.data_view().begin_mutation(erased.evaluation_time());
                static_cast<void>(mutation.copy_value_from(items.at(index)));
            }
        }
    };

    /** convert[TS[bool]](tsb): python truthiness of a bundle - True once
        ANY field is valid (hgraph parity). */
    struct convert_tsb_to_bool_impl
    {
        static constexpr auto name = "convert_tsb_to_bool";

        // Concrete Out<TS<Bool>> is gated by the dispatcher's requested-
        // output match; requires_ checks the INPUT only (the dispatch rule).
        static bool requires_(const ResolutionMap &, OperatorCallContext context)
        {
            return time_series_arg_matches<AnyTSB>(context, 0);
        }

        static void eval(In<"ts", TsVar<"S">> ts, Out<TS<Bool>> out)
        {
            const TSInputView &bundle = ts;
            bool any = false;
            for (std::size_t index = 0; index < bundle.schema()->field_count(); ++index)
            {
                if (bundle.indexed_child_at(index).valid()) { any = true; break; }
            }
            out.set(any);
        }
    };

    /** convert[TSD[str, TS[V]]](tsb[, keys=...]): the bundle's VALID fields
        as a string-keyed dictionary (an optional keys tuple restricts). */
    template <bool WithKeys>
    struct convert_tsb_to_tsd_impl_base
    {
        static constexpr auto name = WithKeys ? "convert_tsb_to_tsd_keys" : "convert_tsb_to_tsd";

        [[nodiscard]] static bool shape_ok(const ResolutionMap &resolution, OperatorCallContext context)
        {
            const auto *out = output_schema(resolution);
            const auto *in  = time_series_schema_at_as<AnyTSB>(context, 0);
            if (!output_matches<AnyTSD>(resolution) || in == nullptr) { return false; }
            auto &registry = TypeRegistry::instance();
            if (out->key_type() != registry.value_type("str")) { return false; }
            const auto *element = time_series_schema_as<AnyTS>(out->element_ts());
            if (element == nullptr) { return false; }
            if constexpr (WithKeys)
            {
                // Only the SELECTED keys' fields must match the value type;
                // other fields (e.g. an extra bool) are simply not exported.
                const auto *keys = context.scalar_as<Str>("keys") == nullptr
                                       ? scalar_arg_at(context, 1)
                                       : nullptr;
                if (keys == nullptr) { return true; }   // presence checked at eval
                auto key_list = keys->scalar_value.view().as_indexed_view();
                for (std::size_t k = 0; k < key_list.size(); ++k)
                {
                    const auto  wanted = key_list.at(k).template checked_as<Str>();
                    const auto  index  = container_impl_detail_find(in, wanted);
                    if (!index.has_value()) { return false; }
                    const auto *field  = time_series_schema_as<AnyTS>(in->fields()[*index].type);
                    if (field == nullptr || field->value_schema != element->value_schema) { return false; }
                }
                return true;
            }
            else
            {
                for (std::size_t index = 0; index < in->field_count(); ++index)
                {
                    const auto *field = time_series_schema_as<AnyTS>(in->fields()[index].type);
                    if (field == nullptr || field->value_schema != element->value_schema) { return false; }
                }
                return true;
            }
        }

        [[nodiscard]] static std::optional<std::size_t> container_impl_detail_find(
            const TSValueTypeMetaData *bundle, const Str &field_name)
        {
            for (std::size_t index = 0; index < bundle->field_count(); ++index)
            {
                if (bundle->fields()[index].name != nullptr && field_name == bundle->fields()[index].name)
                {
                    return index;
                }
            }
            return std::nullopt;
        }

        static bool requires_(const ResolutionMap &resolution, OperatorCallContext context)
        {
            return shape_ok(resolution, context);
        }

        template <typename KeyFilter>
        static void eval_impl(const TSInputView &bundle, const TSOutputView &erased, KeyFilter &&wanted)
        {
            auto dict     = erased.as_dict();
            auto mutation = dict.begin_mutation(erased.evaluation_time());
            for (std::size_t index = 0; index < bundle.schema()->field_count(); ++index)
            {
                const char *field_name = bundle.schema()->fields()[index].name;
                if (field_name == nullptr || !wanted(field_name)) { continue; }
                auto child = bundle.indexed_child_at(index);
                if (!child.valid() || !child.modified()) { continue; }
                Value key{Str{field_name}};
                auto  element = mutation.at(key.view());
                if (element.has_current_value() && element.value().equals(child.value())) { continue; }
                auto element_mutation = element.begin_mutation(erased.evaluation_time());
                static_cast<void>(element_mutation.copy_value_from(child.value()));
            }
        }

    };

    template <bool WithKeys>
    struct convert_tsb_to_tsd_impl;

    template <>
    struct convert_tsb_to_tsd_impl<false> : convert_tsb_to_tsd_impl_base<false>
    {
        static void eval(In<"ts", TsVar<"S">> ts, Out<TsVar<"__out__">> out)
        {
            eval_impl(ts, static_cast<const TSOutputView &>(out), [](const char *) { return true; });
        }
    };

    template <>
    struct convert_tsb_to_tsd_impl<true> : convert_tsb_to_tsd_impl_base<true>
    {
        static void eval(In<"ts", TsVar<"S">> ts, Scalar<"keys", ScalarVar<"KS">> keys,
                         Out<TsVar<"__out__">> out)
        {
            auto key_list = keys.value().as_indexed_view();
            eval_impl(ts, static_cast<const TSOutputView &>(out), [&](const char *field_name) {
                for (std::size_t index = 0; index < key_list.size(); ++index)
                {
                    if (key_list.at(index).template checked_as<Str>() == field_name) { return true; }
                }
                return false;
            });
        }
    };

    // ----- collect / emit ------------------------------------------------

    /** collect[TS[Set/Tuple]](ts, reset=...): accumulate ticks into a
        growing collection; a True reset clears BEFORE that cycle's tick.
        A collection-valued tick unions ALL its elements in. */
    struct collect_collection_impl
    {
        static constexpr auto name = "collect_collection";

        static std::vector<std::pair<std::string_view, Value>> defaults()
        {
            // None defaults: null sources - the inputs stay unwired.
            return {{"reset", Value{}}};
        }

        static bool requires_(const ResolutionMap &resolution, OperatorCallContext context)
        {
            const auto *out_element = collection_element_schema(output_ts_value_schema(resolution));
            const auto *in_element  = collection_element_or_self_schema(ts_value_schema_at(context, 0));
            return out_element != nullptr && out_element == in_element;
        }

        static void eval(In<"ts", TsVar<"S">, InputValidity::Unchecked> ts,
                         In<"reset", TS<Bool>, InputValidity::Unchecked> reset,
                         Out<TsVar<"__out__">> out)
        {
            if (!ts.modified() && !(reset.valid() && reset.modified())) { return; }
            const auto &erased = static_cast<const TSOutputView &>(out);
            const auto *meta   = erased.schema()->value_schema;
            const bool  fresh  = reset.valid() && reset.modified() && reset.value();

            const auto add_all = [&](auto &builder) {
                if (!fresh && erased.data_view().has_current_value())
                {
                    auto prior = erased.value().as_indexed_view();
                    for (std::size_t index = 0; index < prior.size(); ++index)
                    {
                        add_one(builder, prior.at(index));
                    }
                }
                if (ts.valid() && ts.modified())
                {
                    const auto value = ts.base().value();
                    const auto *in_meta = value.schema();
                    if (collection_element_schema(in_meta) != nullptr)
                    {
                        auto items = value.as_indexed_view();
                        for (std::size_t index = 0; index < items.size(); ++index)
                        {
                            add_one(builder, items.at(index));
                        }
                    }
                    else { add_one(builder, value); }
                }
            };

            Value result;
            if (meta->value_kind() == ValueTypeKind::Set)
            {
                SetBuilder builder{ValuePlanFactory::instance().type_for(meta->element_type)};
                add_all(builder);
                result = builder.build();
            }
            else
            {
                ListBuilder builder{ValuePlanFactory::instance().type_for(meta->element_type)};
                add_all(builder);
                result = builder.build();
            }
            auto mutation = erased.data_view().begin_mutation(erased.evaluation_time());
            static_cast<void>(mutation.move_value_from(std::move(result)));
        }

      private:
        static void add_one(SetBuilder &builder, const ValueView &value)
        {
            static_cast<void>(builder.insert_copy(value.data()));
        }
        static void add_one(ListBuilder &builder, const ValueView &value)
        {
            builder.push_back_copy(value.data());
        }
    };

    /** collect[TS[Mapping]](k, v, reset=...): accumulate key/value pairs. */
    struct collect_map_impl
    {
        static constexpr auto name = "collect_map";

        static std::vector<std::pair<std::string_view, Value>> defaults()
        {
            // None defaults: null sources - the inputs stay unwired.
            return {{"reset", Value{}}};
        }

        static bool requires_(const ResolutionMap &resolution, OperatorCallContext context)
        {
            const auto *out = output_ts_value_schema(resolution);
            const auto *k   = ts_value_schema_at(context, 0);
            const auto *v   = ts_value_schema_at(context, 1);
            return out != nullptr && out->value_kind() == ValueTypeKind::Map && k != nullptr && v != nullptr &&
                   out->key_type == k && out->element_type == v;
        }

        static void eval(In<"key", TsVar<"K">, InputValidity::Unchecked> key,
                         In<"ts", TsVar<"S">, InputValidity::Unchecked> ts,
                         In<"reset", TS<Bool>, InputValidity::Unchecked> reset,
                         Out<TsVar<"__out__">> out)
        {
            const bool ticked = (key.modified() || ts.modified()) && key.valid() && ts.valid();
            if (!ticked && !(reset.valid() && reset.modified())) { return; }
            const auto &erased = static_cast<const TSOutputView &>(out);
            const auto *meta   = erased.schema()->value_schema;
            const bool  fresh  = reset.valid() && reset.modified() && reset.value();

            MapBuilder builder{ValuePlanFactory::instance().type_for(meta->key_type),
                               ValuePlanFactory::instance().type_for(meta->element_type)};
            if (!fresh && erased.data_view().has_current_value())
            {
                const auto current = erased.value().as_map();
                for (const auto [k, v] : current) { builder.set_item_copy(k.data(), v.data()); }
            }
            if (ticked)
            {
                builder.set_item_copy(key.base().value().data(), ts.base().value().data());
            }
            auto mutation = erased.data_view().begin_mutation(erased.evaluation_time());
            static_cast<void>(mutation.move_value_from(builder.build()));
        }
    };

    /** collect[TSD](k, v, reset=...): accumulate into an owned dictionary. */
    struct collect_tsd_impl
    {
        static constexpr auto name = "collect_tsd";

        static std::vector<std::pair<std::string_view, Value>> defaults()
        {
            // None defaults: null sources - the inputs stay unwired.
            return {{"reset", Value{}}};
        }

        static bool requires_(const ResolutionMap &resolution, OperatorCallContext context)
        {
            const auto *out = output_schema(resolution);
            const auto *k   = ts_value_schema_at(context, 0);
            const auto *v   = ts_value_schema_at(context, 1);
            if (!output_matches<AnyTSD>(resolution) ||
                k == nullptr || v == nullptr)
            {
                return false;
            }
            const auto *element = time_series_schema_as<AnyTS>(out->element_ts());
            return out->key_type() == k && element != nullptr &&
                   element->value_schema == v;
        }

        static void eval(In<"key", TsVar<"K">, InputValidity::Unchecked> key,
                         In<"ts", TsVar<"S">, InputValidity::Unchecked> ts,
                         In<"reset", TS<Bool>, InputValidity::Unchecked> reset,
                         Out<TsVar<"__out__">> out)
        {
            const bool ticked = (key.modified() || ts.modified()) && key.valid() && ts.valid();
            const bool fresh  = reset.valid() && reset.modified() && reset.value();
            if (!ticked && !fresh) { return; }
            const auto &erased  = static_cast<const TSOutputView &>(out);
            auto        dict    = erased.as_dict();
            auto        mutation = dict.begin_mutation(erased.evaluation_time());

            if (fresh)
            {
                std::vector<Value> stale;
                const auto key_value = key.base().value();
                const auto mutation_view = mutation.view();
                for (const ValueView &existing : mutation_view.keys())
                {
                    if (!(ticked && existing.equals(key_value))) { stale.emplace_back(existing); }
                }
                for (const Value &existing : stale) { static_cast<void>(mutation.erase(existing.view())); }
            }
            if (ticked)
            {
                auto element          = mutation.at(key.base().value());
                auto element_mutation = element.begin_mutation(erased.evaluation_time());
                static_cast<void>(element_mutation.copy_value_from(ts.base().value()));
            }
        }
    };

    /** collect[TSS](ts, reset=...): accumulate elements into an owned TSS.
        Scalar ticks add one; collection/TSS ticks add all their elements
        (TSS ticks add the DELTA additions). A True reset clears first. */
    struct collect_tss_impl
    {
        static constexpr auto name = "collect_tss";

        static std::vector<std::pair<std::string_view, Value>> defaults()
        {
            // None defaults: null sources - the inputs stay unwired.
            return {{"reset", Value{}}};
        }

        static bool requires_(const ResolutionMap &resolution, OperatorCallContext context)
        {
            const auto *out = output_schema(resolution);
            if (!output_matches<AnyTSS>(resolution))
            {
                return false;
            }
            const auto *element = out->value_schema->element_type;
            const auto *surface = time_series_schema_at(context, 0);
            if (surface == nullptr) { return false; }
            if (const auto *tss = time_series_schema_as<AnyTSS>(surface))
            {
                return tss->value_schema->element_type == element;
            }
            const auto *ts = time_series_schema_as<AnyTS>(surface);
            return ts != nullptr && collection_element_or_self_schema(ts->value_schema) == element;
        }

        static void eval(In<"ts", TsVar<"S">, InputValidity::Unchecked> ts,
                         In<"reset", TS<Bool>, InputValidity::Unchecked> reset,
                         Out<TsVar<"__out__">> out)
        {
            const bool fresh = reset.valid() && reset.modified() && reset.value();
            if (!ts.modified() && !fresh) { return; }
            const auto &erased  = static_cast<const TSOutputView &>(out);
            auto        set     = erased.as_set();
            auto        mutation = set.begin_mutation(erased.evaluation_time());
            if (fresh)
            {
                std::vector<Value> stale;
                const auto mutation_view = mutation.view();
                for (const ValueView &element : mutation_view.values()) { stale.emplace_back(element); }
                for (const Value &element : stale) { static_cast<void>(mutation.remove(element.view())); }
            }
            if (!ts.valid() || !ts.modified()) { return; }
            const auto *surface = ts.base().schema();
            if (surface->kind == TSTypeKind::TSS)
            {
                // A reset cleared the accumulator above; this cycle's own
                // additions then seed the fresh set (hgraph parity - reset
                // keeps only the current-cycle contribution, not the full
                // input membership).
                const TSSInputView in_set{ts.base().borrowed_ref()};
                auto data = in_set.data_view();
                for (const ValueView &element : data.added()) { static_cast<void>(mutation.add(element)); }
                return;
            }
            const auto value = ts.base().value();
            if (collection_element_schema(value.schema()) != nullptr)
            {
                auto items = value.as_indexed_view();
                for (std::size_t index = 0; index < items.size(); ++index)
                {
                    static_cast<void>(mutation.add(items.at(index)));
                }
            }
            else { static_cast<void>(mutation.add(value)); }
        }
    };

    /** convert[TSD[int, TS[V]]](TS[tuple[V,...]]): the ENUMERATED desired
        dictionary {index: element}; a shorter tuple drops the tail keys. */
    struct convert_list_to_enumerated_tsd_impl
    {
        static constexpr auto name = "convert_list_to_enumerated_tsd";

        static bool requires_(const ResolutionMap &resolution, OperatorCallContext context)
        {
            const auto *out = output_schema(resolution);
            const auto *in  = ts_value_schema_at(context, 0);
            if (!output_matches<AnyTSD>(resolution) ||
                in == nullptr || in->value_kind() != ValueTypeKind::List)
            {
                return false;
            }
            auto       &registry = TypeRegistry::instance();
            const auto *element  = time_series_schema_as<AnyTS>(out->element_ts());
            return out->key_type() == registry.value_type("int") && element != nullptr &&
                   element->value_schema == in->element_type;
        }

        static void eval(In<"ts", TsVar<"S">> ts, Out<TsVar<"__out__">> out)
        {
            const auto &erased  = static_cast<const TSOutputView &>(out);
            auto        dict    = erased.as_dict();
            auto        mutation = dict.begin_mutation(erased.evaluation_time());
            auto        items   = ts.base().value().as_indexed_view();

            std::vector<Value> stale;
            const auto mutation_view = mutation.view();
            for (const ValueView &existing : mutation_view.keys())
            {
                if (existing.checked_as<Int>() >= static_cast<Int>(items.size()))
                {
                    stale.emplace_back(existing);
                }
            }
            for (const Value &existing : stale) { static_cast<void>(mutation.erase(existing.view())); }
            for (std::size_t index = 0; index < items.size(); ++index)
            {
                Value key{static_cast<Int>(index)};
                auto  element = mutation.at(key.view());
                if (element.has_current_value() && element.value().equals(items.at(index))) { continue; }
                auto element_mutation = element.begin_mutation(erased.evaluation_time());
                static_cast<void>(element_mutation.copy_value_from(items.at(index)));
            }
        }
    };

    /** collect[TSD](k_tuple, v_tuple, reset=...): ZIP the paired tuples into
        the accumulated dictionary. */
    struct collect_tsd_zip_impl
    {
        static constexpr auto name = "collect_tsd_zip";

        static std::vector<std::pair<std::string_view, Value>> defaults()
        {
            // None defaults: null sources - the inputs stay unwired.
            return {{"reset", Value{}}};
        }

        static bool requires_(const ResolutionMap &resolution, OperatorCallContext context)
        {
            const auto *out = output_schema(resolution);
            const auto *k   = ts_value_schema_at(context, 0);
            const auto *v   = ts_value_schema_at(context, 1);
            if (!output_matches<AnyTSD>(resolution) ||
                k == nullptr || v == nullptr || k->value_kind() != ValueTypeKind::List ||
                v->value_kind() != ValueTypeKind::List)
            {
                return false;
            }
            const auto *element = time_series_schema_as<AnyTS>(out->element_ts());
            return out->key_type() == k->element_type && element != nullptr &&
                   element->value_schema == v->element_type;
        }

        static void eval(In<"key", TsVar<"K">, InputValidity::Unchecked> key,
                         In<"ts", TsVar<"S">, InputValidity::Unchecked> ts,
                         In<"reset", TS<Bool>, InputValidity::Unchecked> reset,
                         Out<TsVar<"__out__">> out)
        {
            const bool fresh  = reset.valid() && reset.modified() && reset.value();
            const bool ticked = key.valid() && ts.valid() && (key.modified() || ts.modified());
            if (!fresh && !ticked) { return; }
            const auto &erased  = static_cast<const TSOutputView &>(out);
            auto        dict    = erased.as_dict();
            auto        mutation = dict.begin_mutation(erased.evaluation_time());
            auto        keys    = key.base().value().as_indexed_view();
            auto        values  = ts.base().value().as_indexed_view();
            const std::size_t count = std::min(keys.size(), values.size());

            if (fresh)
            {
                const auto keeps = [&](const ValueView &candidate) {
                    for (std::size_t index = 0; index < count; ++index)
                    {
                        if (keys.at(index).equals(candidate)) { return true; }
                    }
                    return false;
                };
                std::vector<Value> stale;
                const auto mutation_view = mutation.view();
                for (const ValueView &existing : mutation_view.keys())
                {
                    if (!keeps(existing)) { stale.emplace_back(existing); }
                }
                for (const Value &existing : stale) { static_cast<void>(mutation.erase(existing.view())); }
            }
            if (!ticked) { return; }
            for (std::size_t index = 0; index < count; ++index)
            {
                auto element = mutation.at(keys.at(index));
                // A reset cycle RE-PUBLISHES surviving entries (hgraph's
                // delta contract); otherwise equal values dedup.
                if (!fresh && element.has_current_value() && element.value().equals(values.at(index)))
                {
                    continue;
                }
                auto element_mutation = element.begin_mutation(erased.evaluation_time());
                static_cast<void>(element_mutation.copy_value_from(values.at(index)));
            }
        }
    };

    /** collect[TSD](mapping_ts, reset=...): accumulate mapping entries. */
    struct collect_tsd_from_map_impl
    {
        static constexpr auto name = "collect_tsd_from_map";

        static std::vector<std::pair<std::string_view, Value>> defaults()
        {
            // None defaults: null sources - the inputs stay unwired.
            return {{"reset", Value{}}};
        }

        static bool requires_(const ResolutionMap &resolution, OperatorCallContext context)
        {
            const auto *out = output_schema(resolution);
            const auto *in  = ts_value_schema_at(context, 0);
            if (!output_matches<AnyTSD>(resolution) ||
                in == nullptr || in->value_kind() != ValueTypeKind::Map)
            {
                return false;
            }
            const auto *element = time_series_schema_as<AnyTS>(out->element_ts());
            return out->key_type() == in->key_type && element != nullptr &&
                   element->value_schema == in->element_type;
        }

        static void eval(In<"ts", TsVar<"S">, InputValidity::Unchecked> ts,
                         In<"reset", TS<Bool>, InputValidity::Unchecked> reset,
                         Out<TsVar<"__out__">> out)
        {
            const bool fresh = reset.valid() && reset.modified() && reset.value();
            if (!fresh && !ts.modified()) { return; }
            const auto &erased  = static_cast<const TSOutputView &>(out);
            auto        dict    = erased.as_dict();
            auto        mutation = dict.begin_mutation(erased.evaluation_time());
            auto        map     = ts.valid() ? std::optional{ts.base().value().as_map()} : std::nullopt;

            if (fresh)
            {
                std::vector<Value> stale;
                const auto mutation_view = mutation.view();
                for (const ValueView &existing : mutation_view.keys())
                {
                    if (!map.has_value() || !map->contains(existing)) { stale.emplace_back(existing); }
                }
                for (const Value &existing : stale) { static_cast<void>(mutation.erase(existing.view())); }
            }
            if (!ts.modified() || !map.has_value()) { return; }
            for (const auto [k, v] : *map)
            {
                auto element = mutation.at(k);
                if (!fresh && element.has_current_value() && element.value().equals(v)) { continue; }
                auto element_mutation = element.begin_mutation(erased.evaluation_time());
                static_cast<void>(element_mutation.copy_value_from(v));
            }
        }
    };

    /** collect[TSD](tsd, exclude=tss): accumulate a dictionary's DELTA
        entries; keys in the exclude set never enter (and drop on exclusion). */
    struct collect_tsd_from_tsd_impl
    {
        static constexpr auto name = "collect_tsd_from_tsd";

        static std::vector<std::pair<std::string_view, Value>> defaults()
        {
            // None defaults: null sources - the inputs stay unwired.
            return {{"reset", Value{}}, {"exclude", Value{}}};
        }

        static void resolve_default_types(ResolutionMap &resolution, OperatorCallContext context)
        {
            // A defaulted (unwired) exclude still needs its type variable:
            // the natural exclude set is TSS over the input's key.
            if (resolution.find_ts("E") != nullptr) { return; }
            const auto *in = time_series_schema_at(context, 0);
            if (in == nullptr || in->kind != TSTypeKind::TSD) { return; }
            resolution.bind_ts("E", TypeRegistry::instance().tss(in->key_type()));
        }

        static bool requires_(const ResolutionMap &resolution, OperatorCallContext context)
        {
            const auto *out = output_schema(resolution);
            const auto *in  = time_series_schema_at_as<AnyTSD>(context, 0);
            return output_matches<AnyTSD>(resolution) &&
                   in != nullptr &&
                   in == out;
        }

        static void eval(In<"ts", TsVar<"S">, InputValidity::Unchecked> ts,
                         In<"reset", TS<Bool>, InputValidity::Unchecked> reset,
                         In<"exclude", TsVar<"E">, InputValidity::Unchecked> exclude,
                         Out<TsVar<"__out__">> out)
        {
            const bool fresh = reset.valid() && reset.modified() && reset.value();
            if (!ts.modified() && !exclude.modified() && !fresh) { return; }
            const auto &erased  = static_cast<const TSOutputView &>(out);
            auto        dict    = erased.as_dict();
            auto        mutation = dict.begin_mutation(erased.evaluation_time());

            const auto excluded = [&](const ValueView &candidate) {
                if (!exclude.valid()) { return false; }
                const TSSInputView set_input{exclude.base().borrowed_ref()};
                return set_input.data_view().contains(candidate);
            };

            if (fresh)
            {
                std::vector<Value> stale;
                const auto mutation_view = mutation.view();
                for (const ValueView &existing : mutation_view.keys()) { stale.emplace_back(existing); }
                for (const Value &existing : stale) { static_cast<void>(mutation.erase(existing.view())); }
            }
            if (exclude.modified() && exclude.valid())
            {
                const TSSInputView set_input{exclude.base().borrowed_ref()};
                const auto         set_data = set_input.data_view();
                std::vector<Value> stale;
                for (const ValueView &added : set_data.added())
                {
                    if (mutation.view().contains(added)) { stale.emplace_back(added); }
                }
                for (const Value &existing : stale) { static_cast<void>(mutation.erase(existing.view())); }
            }
            if (ts.modified() && ts.valid())
            {
                const TSDInputView in_dict{ts.base().borrowed_ref()};
                for (auto &&[k, child] : in_dict.items())
                {
                    if (!child.modified() || !child.valid() || excluded(k)) { continue; }
                    auto element = mutation.at(k);
                    if (!fresh && element.has_current_value() && element.value().equals(child.value()))
                    {
                        continue;
                    }
                    auto element_mutation = element.begin_mutation(erased.evaluation_time());
                    static_cast<void>(element_mutation.copy_value_from(child.value()));
                }
            }
        }
    };

    namespace convert_detail
    {
        struct EmitQueueState
        {
            std::deque<Value> buffer{};
        };
    }  // namespace convert_detail

}  // namespace hgraph::stdlib

namespace hgraph::static_schema_detail
{
    template <>
    struct scalar_name<stdlib::convert_detail::EmitQueueState>
    {
        static constexpr std::string_view value{"stdlib.emit_queue_state"};
    };
}  // namespace hgraph::static_schema_detail

namespace hgraph::stdlib
{
    using namespace hgraph::operator_type_resolution;

    /** emit(TS[Set/Tuple]): drain the collection ONE ELEMENT PER CYCLE
        (scheduler-driven; a new tick appends its elements). */
    /** emit(TSL[TS[T], N]): drain the MODIFIED elements one per cycle (the
        structural counterpart to emit over a scalar collection). */
    struct emit_tsl_impl
    {
        static constexpr auto name = "emit_tsl";

        static bool requires_(const ResolutionMap &, OperatorCallContext context)
        {
            return fixed_tsl_arg(context, 0) != nullptr;
        }

        static void resolve_default_types(ResolutionMap &resolution, OperatorCallContext context)
        {
            if (output_bound(resolution)) { return; }
            const auto *tsl = fixed_tsl_arg(context, 0);
            if (tsl == nullptr) { return; }
            const auto *element = time_series_schema_as<AnyTS>(tsl->element_ts());
            if (element == nullptr) { return; }
            bind_output(resolution, TypeRegistry::instance().ts(element->value_schema));
        }

        static void eval(In<"ts", TsVar<"S">, InputValidity::Unchecked> ts,
                         NodeScheduler scheduler,
                         State<convert_detail::EmitQueueState> state,
                         Out<TsVar<"__out__">> out)
        {
            auto current = state.get();
            if (ts.modified())
            {
                const TSInputView &tsl = ts;
                for (std::size_t index = 0; index < tsl.schema()->fixed_size(); ++index)
                {
                    auto child = tsl.indexed_child_at(index);
                    if (child.modified() && child.valid()) { current.buffer.emplace_back(child.value()); }
                }
            }
            if (!current.buffer.empty())
            {
                const auto &erased = static_cast<const TSOutputView &>(out);
                Value       next   = std::move(current.buffer.front());
                current.buffer.pop_front();
                auto mutation = erased.data_view().begin_mutation(erased.evaluation_time());
                static_cast<void>(mutation.move_value_from(std::move(next)));
                if (!current.buffer.empty()) { scheduler.schedule(MIN_TD); }
            }
            state.set(std::move(current));
        }
    };

    struct emit_collection_impl
    {
        static constexpr auto name = "emit_collection";

        [[nodiscard]] static const ValueTypeMetaData *element_of(OperatorCallContext context)
        {
            const auto *surface = time_series_schema_at(context, 0);
            if (surface == nullptr) { return nullptr; }
            if (const auto *tss = time_series_schema_as<AnyTSS>(surface))
            {
                return tss->value_schema->element_type;
            }
            const auto *ts    = time_series_schema_as<AnyTS>(surface);
            const auto *value = ts != nullptr ? ts->value_schema : nullptr;
            return collection_element_schema(value);
        }

        static bool requires_(const ResolutionMap &, OperatorCallContext context)
        {
            return element_of(context) != nullptr;
        }

        static void resolve_default_types(ResolutionMap &resolution, OperatorCallContext context)
        {
            if (output_bound(resolution)) { return; }
            const auto *element = element_of(context);
            if (element == nullptr) { return; }
            bind_output(resolution, TypeRegistry::instance().ts(element));
        }

        static void eval(In<"ts", TsVar<"S">> ts,
                         NodeScheduler scheduler,
                         State<convert_detail::EmitQueueState> state,
                         Out<TsVar<"__out__">> out)
        {
            auto current = state.get();
            if (ts.modified())
            {
                if (ts.base().schema()->kind == TSTypeKind::TSS)
                {
                    const TSSInputView in_set{ts.base().borrowed_ref()};
                    auto data = in_set.data_view();
                    for (const ValueView &element : data.added()) { current.buffer.emplace_back(element); }
                }
                else
                {
                    auto items = ts.base().value().as_indexed_view();
                    for (std::size_t index = 0; index < items.size(); ++index)
                    {
                        current.buffer.emplace_back(items.at(index));
                    }
                }
            }
            if (!current.buffer.empty())
            {
                const auto &erased = static_cast<const TSOutputView &>(out);
                Value       next   = std::move(current.buffer.front());
                current.buffer.pop_front();
                auto mutation = erased.data_view().begin_mutation(erased.evaluation_time());
                static_cast<void>(mutation.move_value_from(std::move(next)));
                if (!current.buffer.empty()) { scheduler.schedule(MIN_TD); }
            }
            state.set(std::move(current));
        }
    };

    /** emit(TS[Mapping]) / emit(TSD): drain key/value PAIRS one per cycle
        into a {key, value} bundle (hgraph's KeyValue shape). A TSD input
        enqueues its DELTA entries; a mapping tick enqueues all entries. */
    struct emit_map_impl
    {
        static constexpr auto name = "emit_map";

        [[nodiscard]] static std::pair<const ValueTypeMetaData *, const ValueTypeMetaData *> kv_of(
            OperatorCallContext context)
        {
            const auto *surface = time_series_schema_at(context, 0);
            if (surface == nullptr) { return {nullptr, nullptr}; }
            if (const auto *tsd = time_series_schema_as<AnyTSD>(surface))
            {
                const auto *element = time_series_schema_as<AnyTS>(tsd->element_ts());
                if (element == nullptr) { return {nullptr, nullptr}; }
                return {tsd->key_type(), element->value_schema};
            }
            const auto *ts    = time_series_schema_as<AnyTS>(surface);
            const auto *value = ts != nullptr ? ts->value_schema : nullptr;
            if (value == nullptr || value->value_kind() != ValueTypeKind::Map) { return {nullptr, nullptr}; }
            return {value->key_type, value->element_type};
        }

        static bool requires_(const ResolutionMap &, OperatorCallContext context)
        {
            return kv_of(context).first != nullptr;
        }

        static void resolve_default_types(ResolutionMap &resolution, OperatorCallContext context)
        {

            const auto [k, v] = kv_of(context);
            if (k == nullptr) { return; }
            auto &registry = TypeRegistry::instance();
            bind_output(
                resolution, registry.un_named_tsb({{"key", registry.ts(k)}, {"value", registry.ts(v)}}));
        }

        static void eval(In<"ts", TsVar<"S">> ts,
                         NodeScheduler scheduler,
                         State<convert_detail::EmitQueueState> state,
                         Out<TsVar<"__out__">> out)
        {
            auto current = state.get();
            if (ts.modified())
            {
                const auto *surface = ts.base().schema();
                if (surface->kind == TSTypeKind::TSD)
                {
                    const TSDInputView dict{ts.base().borrowed_ref()};
                    for (auto &&[key, child] : dict.items())
                    {
                        if (!child.modified() || !child.valid()) { continue; }
                        current.buffer.emplace_back(key);
                        current.buffer.emplace_back(child.value());
                    }
                }
                else
                {
                    const auto values = ts.base().value().as_map();
                    for (const auto [key, value] : values)
                    {
                        current.buffer.emplace_back(key);
                        current.buffer.emplace_back(value);
                    }
                }
            }
            if (current.buffer.size() >= 2)
            {
                const auto &erased = static_cast<const TSOutputView &>(out);
                Value key   = std::move(current.buffer.front());
                current.buffer.pop_front();
                Value value = std::move(current.buffer.front());
                current.buffer.pop_front();
                // A KeyValue with only scalar-TS fields is a COMPACT bundle
                // (whole-value write); a non-scalar field (TSL/TSD/TSB value)
                // makes it STRUCTURAL (per-field write).
                const auto *out_schema = erased.schema();
                bool        compact    = out_schema->value_schema != nullptr &&
                               out_schema->value_schema->value_kind() == ValueTypeKind::Bundle;
                for (std::size_t index = 0; compact && index < out_schema->field_count(); ++index)
                {
                    if (out_schema->fields()[index].type->kind != TSTypeKind::TS) { compact = false; }
                }
                if (compact)
                {
                    // COMPACT (whole-value) bundle output: one bundle write.
                    BundleBuilder builder{ValuePlanFactory::instance().type_for(out_schema->value_schema)};
                    builder.set(0, std::move(key));
                    builder.set(1, std::move(value));
                    auto mutation = erased.data_view().begin_mutation(erased.evaluation_time());
                    static_cast<void>(mutation.move_value_from(builder.build()));
                }
                else
                {
                    // STRUCTURAL bundle (e.g. a TSL-valued KeyValue): write
                    // the fields individually.
                    auto bundle = erased.as_bundle();
                    {
                        auto field    = bundle.at(0);
                        auto mutation = field.data_view().begin_mutation(erased.evaluation_time());
                        static_cast<void>(mutation.copy_value_from(key.view()));
                    }
                    {
                        auto field = bundle.at(1);
                        if (field.schema() != nullptr && field.schema()->kind == TSTypeKind::TSL)
                        {
                            // The value is a tuple/list scalar distributed
                            // across the TSL field's slots.
                            auto items = value.view().as_indexed_view();
                            auto list  = field.as_list();
                            for (std::size_t index = 0;
                                 index < items.size() && index < field.schema()->fixed_size(); ++index)
                            {
                                auto slot     = list.at(index);
                                auto mutation = slot.data_view().begin_mutation(erased.evaluation_time());
                                static_cast<void>(mutation.copy_value_from(items.at(index)));
                            }
                        }
                        else
                        {
                            auto mutation = field.data_view().begin_mutation(erased.evaluation_time());
                            static_cast<void>(mutation.copy_value_from(value.view()));
                        }
                    }
                }
                if (!current.buffer.empty()) { scheduler.schedule(MIN_TD); }
            }
            state.set(std::move(current));
        }
    };

    struct str_impl
    {
        static bool requires_(const ResolutionMap &, OperatorCallContext context)
        {
            // Fixed TSLs print tuple-style via str_tsl_impl.
            return fixed_tsl_arg(context, 0) == nullptr;
        }

        static void eval(In<"ts", TsVar<"S">> ts, Out<TS<Str>> out)
        {
            out.set(ts.value().to_string());
        }
    };

    /** str_ over a fixed TSL prints TUPLE style - "(a, b)" not "[a, b]"
        (hgraph's python repr of the TSL value). */
    struct str_tsl_impl
    {
        static constexpr auto name = "str_tsl";

        static bool requires_(const ResolutionMap &, OperatorCallContext context)
        {
            return fixed_tsl_arg(context, 0) != nullptr;
        }

        static void eval(In<"ts", TsVar<"S">> ts, Out<TS<Str>> out)
        {
            const auto  value  = ts.value();
            auto        fields = value.as_indexed_view();
            std::string text   = "(";
            for (std::size_t index = 0; index < fields.size(); ++index)
            {
                if (index != 0) { text += ", "; }
                text += fields.at(index).to_string();
            }
            text += ")";
            out.set(Str{std::move(text)});
        }
    };

    /**
     * ``nothing`` implementation — a generic source of the requested output type that
     * **never ticks** (no inputs, not scheduled on start, ``eval`` never runs, so the
     * output stays perpetually invalid). Used as the rebindable placeholder for the
     * ``mesh_subscribe`` value input.
     */
    struct nothing_source
    {
        static constexpr auto name = "nothing";

        static void eval(Out<TsVar<"O">> out) { static_cast<void>(out); }  // never ticks
    };

    /** Register the conversion / utility operator overloads. */
    void register_conversion_operators();
}  // namespace hgraph::stdlib

#endif  // HGRAPH_LIB_STD_OPERATORS_IMPL_CONVERSION_IMPL_H
