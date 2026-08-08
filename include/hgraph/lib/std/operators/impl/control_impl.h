#ifndef HGRAPH_LIB_STD_OPERATORS_IMPL_CONTROL_IMPL_H
#define HGRAPH_LIB_STD_OPERATORS_IMPL_CONTROL_IMPL_H

#include <hgraph/lib/std/operators/control.h>
#include <hgraph/lib/std/operators/impl/higher_order_impl.h>
#include <hgraph/lib/std/operators/impl/collection_impl.h>
#include <hgraph/types/operator_type_resolution.h>
#include <hgraph/types/subgraph_wiring.h>
#include <hgraph/runtime/race_tsd_node.h>
#include <hgraph/lib/std/operators/conversion.h>
#include <hgraph/lib/std/operators/higher_order.h>
#include <hgraph/types/operator_dispatch.h>
#include <hgraph/types/primitive_types.h>
#include <hgraph/types/static_node.h>
#include <hgraph/types/static_schema.h>
#include <hgraph/types/time_series_reference.h>

#include <limits>
#include <stdexcept>
#include <string_view>
#include <vector>

namespace hgraph::stdlib
{
    using namespace hgraph::operator_type_resolution;

    namespace control_impl_detail
    {
        inline constexpr std::size_t no_race_winner = std::numeric_limits<std::size_t>::max();

        struct RaceState
        {
            std::vector<DateTime> first_valid_times{};
            std::size_t           winner{no_race_winner};
        };

        inline void ensure_race_state_size(RaceState &state, std::size_t size)
        {
            if (state.first_valid_times.size() == size) { return; }
            state.first_valid_times.assign(size, MAX_DT);
            state.winner = no_race_winner;
        }

        [[nodiscard]] inline bool ref_input_valid(const TSInputView &input)
        {
            if (!input.valid()) { return false; }
            const TimeSeriesReference ref = input.value().checked_as<TimeSeriesReference>();
            return ref.is_valid(input.evaluation_time());
        }

        struct merge_binary
        {
            static constexpr auto name = "merge_binary";

            static void start(State<Int> selected) { selected.set(Int{-1}); }

            static void eval(In<"lhs", TsVar<"S">, InputValidity::Unchecked> lhs,
                             In<"rhs", TsVar<"S">, InputValidity::Unchecked> rhs,
                             State<Int> selected,
                             Out<TsVar<"S">> out)
            {
                const Int current = selected.get();
                if (lhs.modified() && lhs.valid())
                {
                    selected.set(Int{0});
                    out.apply(lhs.value());
                    return;
                }
                if (rhs.modified() && rhs.valid())
                {
                    selected.set(Int{1});
                    out.apply(rhs.value());
                    return;
                }

                if (current == 0 && !lhs.valid())
                {
                    if (rhs.valid())
                    {
                        selected.set(Int{1});
                        out.apply(rhs.value());
                    }
                    else { selected.set(Int{-1}); }
                    return;
                }
                if (current == 1 && !rhs.valid())
                {
                    if (lhs.valid())
                    {
                        selected.set(Int{0});
                        out.apply(lhs.value());
                    }
                    else { selected.set(Int{-1}); }
                    return;
                }

                if (current < 0)
                {
                    if (lhs.valid())
                    {
                        selected.set(Int{0});
                        out.apply(lhs.value());
                    }
                    else if (rhs.valid())
                    {
                        selected.set(Int{1});
                        out.apply(rhs.value());
                    }
                }
            }
        };

        struct all_bool_binary
        {
            static constexpr auto name              = "all_bool_binary";
            static constexpr bool schedule_on_start = true;

            static void eval(In<"lhs", TS<Bool>, InputValidity::Unchecked> lhs,
                             In<"rhs", TS<Bool>, InputValidity::Unchecked> rhs,
                             Out<TS<Bool>> out)
            {
                out.set(lhs.valid() && lhs.value() && rhs.valid() && rhs.value());
            }
        };

        struct any_bool_binary
        {
            static constexpr auto name              = "any_bool_binary";
            static constexpr bool schedule_on_start = true;

            static void eval(In<"lhs", TS<Bool>, InputValidity::Unchecked> lhs,
                             In<"rhs", TS<Bool>, InputValidity::Unchecked> rhs,
                             Out<TS<Bool>> out)
            {
                out.set((lhs.valid() && lhs.value()) || (rhs.valid() && rhs.value()));
            }
        };

        [[nodiscard]] inline Port<TS<Bool>> bool_const(Wiring &w, Bool value)
        {
            return wire<const_, TS<Bool>>(w, value);
        }

    }  // namespace control_impl_detail

    namespace control_impl_detail
    {
        /** merge(*tsds, disjoint=True): the LEFTMOST dictionary holding a
            key supplies its element REFERENCE; keys absent from every input
            REMOVE (hgraph's merge_tsd_disjoint as stateless own-output
            reconciliation over the packed inputs). */
        struct merge_tsd_disjoint_node
        {
            static constexpr auto name = "merge_tsd_disjoint";

            static void resolve_default_types(ResolutionMap &resolution, OperatorCallContext context)
            {
                if (output_bound(resolution)) { return; }
                const auto *tsl = fixed_tsl_arg(context, 0);
                if (tsl == nullptr) { return; }
                auto &registry = TypeRegistry::instance();
                const auto *element = time_series_schema_as<AnyTSD>(tsl->element_ts());
                if (element == nullptr) { return; }
                bind_output(
                    resolution, registry.tsd(element->key_type(),
                                             registry.ref(registry.dereference(element->element_ts()))));
            }

            static void eval(In<"tsl", TSL<TSD<ScalarVar<"K">, TsVar<"V">>, SIZE<"N">>,
                                InputValidity::Unchecked> tsl,
                             Out<TsVar<"__out__">> out)
            {
                std::vector<std::pair<Value, Value>> pairs;
                const std::size_t size = tsl.size();
                for (std::size_t index = 0; index < size; ++index)
                {
                    TSDInputView dict{tsl[index].base().borrowed_ref()};
                    for (auto &&[key, child] : dict.items())
                    {
                        if (!child.valid()) { continue; }
                        bool seen = false;
                        for (const auto &pair : pairs)
                        {
                            if (pair.first.view().equals(key)) { seen = true; break; }
                        }
                        if (!seen) { pairs.emplace_back(Value{key}, Value{child.reference()}); }
                    }
                }
                collection_impl_detail::publish_tsd_refs(static_cast<const TSOutputView &>(out), pairs);
            }
        };
    }  // namespace control_impl_detail

    /** The graph overload gating merge's ``disjoint=True`` calling shape:
        collect the variadic dictionaries and wire the erased node. */
    struct merge_tsd_disjoint_graph_impl
    {
        static constexpr auto name = "merge_tsd_disjoint_graph";

        static auto defaults() { return std::tuple{arg<"disjoint">(Bool{false})}; }

        [[nodiscard]] static const TSValueTypeMetaData *first_dict_arg(OperatorCallContext context)
        {
            // The VarIn tail normalises AFTER the scalar: scan for the first
            // time-series argument.
            for (std::size_t index = 0; index < context.args.size(); ++index)
            {
                if (const auto *schema = time_series_schema_at_as<AnyTSD>(context, index)) { return schema; }
            }
            return nullptr;
        }

        static bool requires_(const ResolutionMap &, OperatorCallContext context)
        {
            const Bool *disjoint = context.scalar_as<Bool>("disjoint");
            if (disjoint == nullptr || !*disjoint) { return false; }
            return first_dict_arg(context) != nullptr;
        }

        static void resolve_default_types(ResolutionMap &resolution, OperatorCallContext context)
        {
            if (output_bound(resolution)) { return; }
            const auto *schema = first_dict_arg(context);
            if (schema == nullptr) { return; }
            auto &registry = TypeRegistry::instance();
            bind_output(
                resolution,
                registry.tsd(schema->key_type(), registry.ref(registry.dereference(schema->element_ts()))));
        }

        static WiringPortRef compose(Wiring &w, VarIn<"tsl", TSD<ScalarVar<"K">, TsVar<"V">>> ts,
                                     Scalar<"disjoint", Bool>)
        {
            if (ts.empty()) { throw std::invalid_argument("merge requires at least one input"); }
            auto &registry = TypeRegistry::instance();
            const auto *element = registry.dereference(ts[0].schema);
            std::vector<WiringPortRef> children{ts.begin(), ts.end()};
            WiringPortRef packed =
                WiringPortRef::structural_source(registry.tsl(element, ts.size()), std::move(children));
            std::array<WiringArg, 1> args{};
            args[0].kind = WiringArg::Kind::TimeSeries;
            args[0].port = packed;
            auto result = wire_operator(w, "merge_tsd_disjoint", {args.data(), args.size()}, true);
            return result.output.erased();
        }
    };

    /** merge over TSDs is PER-KEY (hgraph's merge_tsd): map_(merge, *tsl)
        recurses the merge into each key's elements. Whole-dict reduce (the
        generic merge below) would take the last-modified dict wholesale. */
    struct merge_tsd_graph_impl
    {
        static constexpr auto name = "merge_tsd";

        static void resolve_default_types(ResolutionMap &resolution, OperatorCallContext context)
        {
            if (output_bound(resolution)) { return; }
            const TSValueTypeMetaData *schema = time_series_schema_at_as<AnyTSD>(context, 0);
            if (schema == nullptr) { return; }
            bind_output(resolution, schema);
        }

        static auto compose(Wiring &w, VarIn<"tsl", TSD<ScalarVar<"K">, TsVar<"V">>> ts)
        {
            if (ts.empty()) { throw std::invalid_argument("merge requires at least one input"); }
            return wire<map_>(w, fn<merge>(), ts);
        }
    };

    /** merge over ONE fixed TSL argument expands its elements as the
        variadic inputs (hgraph's ``*tsl: TSL[...]`` calling shape). */
    struct merge_tsl_graph_impl
    {
        static constexpr auto name = "merge_tsl";

        static bool requires_(const ResolutionMap &, OperatorCallContext context)
        {
            if (context.args.size() != 1) { return false; }
            return fixed_tsl_arg(context, 0) != nullptr;
        }

        static void resolve_default_types(ResolutionMap &resolution, OperatorCallContext context)
        {
            if (output_bound(resolution)) { return; }
            const TSValueTypeMetaData *schema = fixed_tsl_arg(context, 0);
            if (schema == nullptr) { return; }
            bind_output(resolution, schema->element_ts());
        }

        static WiringPortRef compose(Wiring &w, NamedPort<"tsl", TsVar<"S">> ts)
        {
            const TSValueTypeMetaData *schema = TypeRegistry::instance().dereference(ts.erased().schema);
            std::vector<WiringPortRef> elements;
            elements.reserve(schema->fixed_size());
            for (std::size_t index = 0; index < schema->fixed_size(); ++index)
            {
                elements.push_back(subgraph_wiring_detail::tsl_element_ref(ts.erased(), index,
                                                                           schema->element_ts()));
            }
            return wire_erased_operator(w, "merge", {elements.data(), elements.size()}, true);
        }
    };

    struct merge_graph_impl
    {
        static constexpr auto name = "merge";

        static bool requires_(const ResolutionMap &, OperatorCallContext context)
        {
            // TSD arguments merge PER-KEY (merge_tsd_graph_impl); ONE fixed
            // TSL argument is the *tsl collection shape (merge_tsl_graph_impl).
            const TSValueTypeMetaData *schema = time_series_schema_at(context, 0);
            if (schema == nullptr) { return true; }
            if (time_series_schema_as<AnyTSD>(schema) != nullptr) { return false; }
            const auto *tsl = time_series_schema_as<AnyTSL>(schema);
            return !(context.args.size() == 1 && tsl != nullptr && tsl->fixed_size() > 0);
        }

        static void resolve_default_types(ResolutionMap &resolution, OperatorCallContext context)
        {
            if (output_bound(resolution)) { return; }
            bind_output_like_arg(resolution, context, 0, "S");
        }

        static auto compose(Wiring &w, VarIn<"tsl", TsVar<"S">> ts)
        {
            if (ts.empty()) { throw std::invalid_argument("merge requires at least one input"); }
            // merge_binary observes positional invalidity so that it can fall
            // back to another source. Public reduce_ deliberately skips
            // invalid values, therefore merge owns this static fold directly.
            return higher_order_impl_detail::reduce_layout(
                w, fn<control_impl_detail::merge_binary>(),
                std::vector<WiringPortRef>{ts.begin(), ts.end()});
        }
    };

}  // namespace hgraph::stdlib

namespace hgraph::static_schema_detail
{
    template <>
    struct scalar_name<stdlib::control_impl_detail::RaceState>
    {
        static constexpr std::string_view value{"stdlib.race_state"};
    };
}  // namespace hgraph::static_schema_detail

namespace hgraph::stdlib
{
    using namespace hgraph::operator_type_resolution;

    struct all_graph_impl
    {
        static constexpr auto name = "all";

        static Port<TS<Bool>> compose(Wiring &w, VarIn<"args", TS<Bool>> args)
        {
            if (args.empty()) { return control_impl_detail::bool_const(w, true); }
            std::vector<WiringPortRef> elements{args.begin(), args.end()};
            elements.push_back(control_impl_detail::bool_const(w, true).erased());
            return Port<TS<Bool>>{w, higher_order_impl_detail::reduce_layout(
                                        w, fn<control_impl_detail::all_bool_binary>(),
                                        std::move(elements))};
        }
    };

    struct any_graph_impl
    {
        static constexpr auto name = "any";

        static Port<TS<Bool>> compose(Wiring &w, VarIn<"args", TS<Bool>> args)
        {
            if (args.empty()) { return control_impl_detail::bool_const(w, false); }
            std::vector<WiringPortRef> elements{args.begin(), args.end()};
            elements.push_back(control_impl_detail::bool_const(w, false).erased());
            return Port<TS<Bool>>{w, higher_order_impl_detail::reduce_layout(
                                        w, fn<control_impl_detail::any_bool_binary>(),
                                        std::move(elements))};
        }
    };

    struct all_tsd_impl
    {
        static constexpr bool schedule_on_start = true;

        static void eval(In<"arg", TSD<ScalarVar<"K">, TS<Bool>>, InputValidity::Unchecked> arg,
                         Out<TS<Bool>> out)
        {
            Bool result = true;
            for (auto child : arg.values())
            {
                if (!child.valid() || !child.value())
                {
                    result = false;
                    break;
                }
            }
            out.set(result);
        }
    };

    struct any_tsd_impl
    {
        static constexpr bool schedule_on_start = true;

        static void eval(In<"arg", TSD<ScalarVar<"K">, TS<Bool>>, InputValidity::Unchecked> arg,
                         Out<TS<Bool>> out)
        {
            Bool result = false;
            for (auto child : arg.values())
            {
                if (child.valid() && child.value())
                {
                    result = true;
                    break;
                }
            }
            out.set(result);
        }
    };

    /**
     * The race node (hgraph semantics, ext/main _flow_control.race_default):
     * the winner is the candidate whose TARGET was valid first; a winner
     * that goes invalid triggers a re-race. The ``ts`` input carries the
     * candidate REFERENCES; references alone do not tick when a target
     * becomes valid, so the ``values`` input binds the candidates
     * DEREFERENCED (hgraph's hidden ``_values`` input) - a target validity
     * change wakes the node. hgraph toggles ``_values`` active/passive
     * around winner changes as an optimisation; here it stays active (the
     * output still only ticks on winner change or winner-reference change).
     */
    struct race_ref_impl
    {
        static void eval(In<"ts", TSL<REF<TsVar<"S">>, SIZE<"N">>, InputValidity::Unchecked> ts,
                         In<"values", TSL<TsVar<"S">, SIZE<"N">>, InputValidity::Unchecked> values,
                         State<control_impl_detail::RaceState> state,
                         DateTime now,
                         Out<REF<TsVar<"S">>> out)
        {
            static_cast<void>(values);   // wake-up only; reads go through the references
            auto current = state.get();
            control_impl_detail::ensure_race_state_size(current, ts.size());

            for (std::size_t i = 0; i < ts.size(); ++i)
            {
                const auto ref = ts[i];
                if (control_impl_detail::ref_input_valid(ref.base()))
                {
                    if (current.first_valid_times[i] == MAX_DT) { current.first_valid_times[i] = now; }
                }
                else { current.first_valid_times[i] = MAX_DT; }
            }

            std::size_t winner     = control_impl_detail::no_race_winner;
            DateTime    first_time = MAX_DT;
            for (std::size_t i = 0; i < current.first_valid_times.size(); ++i)
            {
                const DateTime candidate = current.first_valid_times[i];
                if (candidate < first_time)
                {
                    first_time = candidate;
                    winner     = i;
                }
            }

            if (winner != control_impl_detail::no_race_winner)
            {
                if (winner != current.winner || ts[winner].modified()) { out.set(ts[winner].value()); }
                current.winner = winner;
            }
            else { current.winner = control_impl_detail::no_race_winner; }

            state.set(std::move(current));
        }
    };

    struct race_graph_impl
    {
        static constexpr auto name = "race";

        static bool requires_(const ResolutionMap &, OperatorCallContext context)
        {
            if (context.args.empty()) { return false; }
            auto       &registry = TypeRegistry::instance();
            const auto *schema   = registry.dereference(context.args[0].port.schema);
            if (schema == nullptr) { return false; }
            for (const WiringArg &arg : context.args)
            {
                if (arg.kind != WiringArg::Kind::TimeSeries ||
                    !time_series_schema_equivalent(schema, registry.dereference(arg.port.schema)))
                {
                    return false;
                }
            }
            return true;
        }

        static void resolve_default_types(ResolutionMap &resolution, OperatorCallContext context)
        {
            if (context.args.empty() || context.args[0].kind != WiringArg::Kind::TimeSeries) { return; }
            auto       &registry = TypeRegistry::instance();
            const auto *schema   = registry.dereference(context.args[0].port.schema);
            resolution.bind_ts("S", schema);
            resolution.bind_size("N", context.args.size());
            resolution.bind_ts("__out__", registry.ref(schema));
        }

        static WiringPortRef compose(Wiring &w, VarIn<"ts", TsVar<"S">> ts)
        {
            if (ts.empty()) { throw std::invalid_argument("race requires at least one input"); }

            auto       &registry   = TypeRegistry::instance();
            const auto *target     = registry.dereference(ts[0].schema);
            const auto *ref_schema = registry.ref(target);
            std::vector<WiringPortRef> children;
            children.reserve(ts.size());
            for (const WiringPortRef &port : ts)
            {
                children.push_back(graph_wiring_detail::adapt_source_for_input(w, ref_schema, port));
            }

            WiringPortRef packed = WiringPortRef::structural_source(registry.tsl(ref_schema, ts.size()),
                                                                    std::move(children));
            std::vector<WiringPortRef> value_children{ts.begin(), ts.end()};
            WiringPortRef values = WiringPortRef::structural_source(registry.tsl(target, ts.size()),
                                                                    std::move(value_children));
            return wire<race_ref_impl>(w, Port<void>{w, std::move(packed)}, Port<void>{w, std::move(values)})
                .erased();
        }
    };

    struct race_tsd_graph_impl
    {
        static constexpr auto name = "reduce_tsd_with_race";

        static void resolve_default_types(ResolutionMap &resolution, OperatorCallContext context)
        {
            auto &registry = TypeRegistry::instance();
            for (const WiringArg &arg : context.args)
            {
                const auto *schema = time_series_schema_as<AnyTSD>(arg);
                if (schema == nullptr || schema->element_ts() == nullptr) { continue; }
                const auto *element = schema->element_ts();
                const auto *out     = element->kind == TSTypeKind::REF ? element
                                                                       : registry.ref(registry.dereference(element));
                higher_order_impl_detail::bind_graph_output(resolution, out, "O");
                return;
            }
        }

        static WiringPortRef compose(Wiring &w, NamedPort<"tsd", TSD<ScalarVar<"K">, REF<TsVar<"S">>>> tsd)
        {
            const WiringPortRef source = tsd.erased();
            if (time_series_schema_as<AnyTSD>(source.schema) == nullptr)
            {
                throw std::invalid_argument("reduce_tsd_with_race requires a TSD<K, REF<OUT>> input");
            }
            NodeBuilder builder = make_race_tsd_node(*source.schema);
            std::array<WiringPortRef, 1> inputs{source};
            return w.add_node(std::type_index(typeid(race_tsd_graph_impl)), std::move(builder),
                              std::span<const WiringPortRef>{inputs.data(), inputs.size()}, Value{});
        }
    };

    struct if_ref_impl
    {
        using output_schema = UnNamedTSB<Field<"true", REF<TsVar<"S">>>, Field<"false", REF<TsVar<"S">>>>;

        static void eval(In<"condition", TS<Bool>> condition,
                         In<"ts", REF<TsVar<"S">>, InputValidity::Unchecked> ts,
                         Out<output_schema> out)
        {
            const TimeSeriesReference empty = TimeSeriesReference::empty(ts.base().schema()->referenced_ts());
            if (condition.value())
            {
                out.template field<"true">().set(ts.valid() ? ts.value() : empty);
                out.template field<"false">().set(empty);
            }
            else
            {
                out.template field<"false">().set(ts.valid() ? ts.value() : empty);
                out.template field<"true">().set(empty);
            }
        }
    };

    struct route_by_index_ref_impl
    {
        static void eval(In<"index", TS<Int>> index,
                         In<"ts", REF<TsVar<"S">>, InputValidity::Unchecked> ts,
                         Out<TSL<REF<TsVar<"S">>, SIZE<"N">>> out)
        {
            const TimeSeriesReference empty = TimeSeriesReference::empty(ts.base().schema()->referenced_ts());
            const Int                 selected_index = index.value();
            for (std::size_t i = 0; i < out.size(); ++i)
            {
                if (selected_index >= 0 && static_cast<std::size_t>(selected_index) == i && ts.valid())
                {
                    out[i].set(ts.value());
                }
                else { out[i].set(empty); }
            }
        }
    };

    struct if_true_impl
    {
        static void eval(In<"condition", TS<Bool>> condition,
                         Scalar<"tick_once_only", Bool> tick_once_only,
                         State<Bool> ticked,
                         Out<TS<Bool>> out)
        {
            if (!condition.value()) { return; }
            if (!tick_once_only.value() || !ticked.get())
            {
                out.set(true);
                ticked.set(true);
            }
        }

        static std::vector<std::pair<std::string_view, Value>> defaults()
        {
            return {{"tick_once_only", Value{Bool{false}}}};
        }
    };

    struct if_then_else_impl
    {
        /** hgraph parity: REFERENCE-based - publishes the SELECTED branch's
            reference with same-reference dedup. Value ticks flow THROUGH
            the reference; a condition re-tick that selects the same target
            does not re-emit. */
        static void eval(In<"condition", TS<Bool>> condition,
                         In<"true_value", REF<TsVar<"S">>, InputValidity::Unchecked> true_value,
                         In<"false_value", REF<TsVar<"S">>, InputValidity::Unchecked> false_value,
                         Out<REF<TsVar<"S">>> out)
        {
            const TSInputView &selected = condition.value() ? true_value.base() : false_value.base();
            if (!(condition.modified() || selected.modified())) { return; }
            if (!selected.valid()) { return; }
            auto reference = selected.value();
            if (out.valid() &&
                out.value().checked_as<TimeSeriesReference>() == reference.checked_as<TimeSeriesReference>())
            {
                return;
            }
            const auto &erased = static_cast<const TSOutputView &>(out);
            auto mutation = erased.begin_mutation(erased.evaluation_time());
            static_cast<void>(mutation.copy_value_from(reference));
        }
    };

    struct if_cmp_impl
    {
        /** hgraph parity: REFERENCE-based like if_then_else - publishes the
            selected branch's reference with same-reference dedup. */
        static void eval(In<"cmp", TS<CmpResult>> cmp,
                         In<"lt", REF<TsVar<"O">>, InputValidity::Unchecked> lt,
                         In<"eq", REF<TsVar<"O">>, InputValidity::Unchecked> eq,
                         In<"gt", REF<TsVar<"O">>, InputValidity::Unchecked> gt,
                         Out<REF<TsVar<"O">>> out)
        {
            const TSInputView &selected = cmp.value() == CmpResult::LT ? lt.base()
                                      : cmp.value() == CmpResult::EQ ? eq.base()
                                                                     : gt.base();
            if (!(cmp.modified() || selected.modified())) { return; }
            if (!selected.valid()) { return; }
            auto reference = selected.value();
            if (out.valid() &&
                out.value().checked_as<TimeSeriesReference>() == reference.checked_as<TimeSeriesReference>())
            {
                return;
            }
            const auto &erased = static_cast<const TSOutputView &>(out);
            auto mutation = erased.begin_mutation(erased.evaluation_time());
            static_cast<void>(mutation.copy_value_from(reference));
        }
    };

    void register_control_operators();
}  // namespace hgraph::stdlib

#endif  // HGRAPH_LIB_STD_OPERATORS_IMPL_CONTROL_IMPL_H
