#ifndef HGRAPH_LIB_STD_OPERATORS_CONTROL_H
#define HGRAPH_LIB_STD_OPERATORS_CONTROL_H

#include <hgraph/lib/std/operators/comparison.h>   // CmpResult (if_cmp)
#include <hgraph/runtime/feedback_node.h>
#include <hgraph/types/operator_dispatch.h>
#include <hgraph/types/primitive_types.h>
#include <hgraph/types/static_node.h>
#include <hgraph/types/static_schema.h>

#include <array>
#include <concepts>
#include <memory>
#include <span>
#include <stdexcept>
#include <type_traits>
#include <typeindex>
#include <utility>

namespace hgraph::stdlib
{
    /**
     * Flow-control / routing operator **definitions** (markers only). Mirrors the Python
     * ``hgraph`` flow-control operators (``_flow_control.py``). ``merge`` / ``race`` /
     * ``all_`` / ``any_`` are variadic over their inputs.
     */

    /** ``merge`` — forward the first of the inputs to tick this cycle (variadic). */
    /** Runtime half of merge(disjoint=True): leftmost-wins reference merge
        over a packed TSL of dictionaries. */
    struct merge_tsd_disjoint_marker
        : Operator<"merge_tsd_disjoint", In<"tsl", TsVar<"S">>, Out<TsVar<"O">>>
    {
    };

    struct merge : Operator<"merge", VarIn<"tsl", TsVar<"S">>, Out<TsVar<"S">>>
    {
    };

    /** ``race`` — forward the first *valid* of the inputs, falling through as they invalidate. */
    /** ``reduce_tsd_with_race(tsd=TSD<K, REF<OUT>>) -> REF<OUT>`` — keyed race
        (hgraph parity; ``reduce_tsd_of_bundles_with_race`` is the same erased
        implementation registered under the bundle-flavoured name). */
    struct reduce_tsd_with_race
        : Operator<"reduce_tsd_with_race", In<"tsd", TSD<ScalarVar<"K">, REF<TsVar<"S">>>>, Out<REF<TsVar<"S">>>>
    {
    };

    struct reduce_tsd_of_bundles_with_race
        : Operator<"reduce_tsd_of_bundles_with_race", In<"tsd", TSD<ScalarVar<"K">, REF<TsVar<"S">>>>,
                   Out<REF<TsVar<"S">>>>
    {
    };

    struct race : Operator<"race", VarIn<"ts", TsVar<"S">>, Out<TsVar<"S">>>
    {
    };

    /** ``all_`` — graph ``all``: ``True`` when every boolean input is ``True`` (variadic). */
    struct all_ : Operator<"all_", VarIn<"args", TS<Bool>>, Out<TS<Bool>>>
    {
    };

    /** ``any_`` — graph ``any``: ``True`` when any boolean input is ``True`` (variadic). */
    struct any_ : Operator<"any_", VarIn<"args", TS<Bool>>, Out<TS<Bool>>>
    {
    };

    /** ``if_`` — route ``ts`` to a ``true`` / ``false`` bundle output by ``condition``. */
    struct if_ : Operator<"if_", In<"condition", TS<Bool>>, In<"ts", TsVar<"S">>, Out<TsVar<"O">>>
    {
    };

    /** ``route_by_index`` — forward ``ts`` to the ``index``-th of a list of outputs. */
    struct route_by_index : Operator<"route_by_index", In<"index", TS<Int>>, In<"ts", TsVar<"S">>, Out<TsVar<"O">>>
    {
    };

    /** ``if_true`` — tick ``True`` when ``condition`` ticks ``True`` (optional ``tick_once_only``). */
    struct if_true : Operator<"if_true", In<"condition", TS<Bool>>, Scalar<"tick_once_only", Bool>, Out<TS<Bool>>>
    {
    };

    /** ``if_then_else`` — select ``true_value`` or ``false_value`` per ``condition``. */
    struct if_then_else : Operator<"if_then_else", In<"condition", TS<Bool>>, In<"true_value", TsVar<"S">>,
                                   In<"false_value", TsVar<"S">>, Out<TsVar<"S">>>
    {
    };

    /** ``if_cmp`` — select ``lt`` / ``eq`` / ``gt`` according to a ``CmpResult``. */
    struct if_cmp : Operator<"if_cmp", In<"cmp", TS<CmpResult>>, In<"lt", TsVar<"O">>, In<"eq", TsVar<"O">>,
                             In<"gt", TsVar<"O">>, Out<TsVar<"O">>>
    {
    };

    namespace feedback_detail
    {
        struct feedback_source_node_tag
        {
        };

        struct feedback_sink_node_tag
        {
        };

        [[nodiscard]] inline const TSValueTypeMetaData *require_feedback_schema(
            const TSValueTypeMetaData *schema)
        {
            if (schema == nullptr)
            {
                throw std::invalid_argument("feedback<TSchema> requires a concrete time-series schema");
            }
            if (schema->delta_value_schema == nullptr)
            {
                throw std::invalid_argument("feedback<TSchema> requires a delta value schema");
            }
            return schema;
        }

        inline void validate_initial_delta(const TSValueTypeMetaData &schema,
                                           const Value               &delta)
        {
            if (!delta.has_value())
            {
                throw std::invalid_argument("feedback initial delta must have a value");
            }
            if (delta.schema() != schema.delta_value_schema)
            {
                throw std::invalid_argument("feedback initial delta schema does not match the time-series delta schema");
            }
        }
    }  // namespace feedback_detail

    template <typename TSchema>
    class FeedbackWiringPort
    {
      public:
        using schema = TSchema;

        FeedbackWiringPort() = default;

        FeedbackWiringPort(Wiring &w,
                           Port<TSchema> delegate,
                           const TSValueTypeMetaData &schema)
            : state_(std::make_shared<State>(State{&w, std::move(delegate), &schema}))
        {
        }

        [[nodiscard]] Port<TSchema> operator()() const
        {
            return state().delegate;
        }

        FeedbackWiringPort &operator()(Port<TSchema> ts)
        {
            return bind(std::move(ts));
        }

        FeedbackWiringPort &bind(Port<TSchema> ts)
        {
            State &s = state();
            if (s.bound) { throw std::logic_error("feedback is already bound"); }
            if (ts.wiring() != nullptr && ts.wiring() != s.wiring)
            {
                throw std::logic_error("feedback source and bound port belong to different wirings");
            }

            Wiring &w = *s.wiring;
            WiringPortRef ts_source =
                graph_wiring_detail::adapt_source_for_input(w, s.schema, ts.erased());
            WiringPortRef self_source =
                graph_wiring_detail::adapt_source_for_input(w, s.schema, s.delegate.erased());

            std::array<WiringPortRef, 2> sources{std::move(ts_source), std::move(self_source)};

            NodeBuilder builder = make_feedback_sink_node(*s.schema);
            builder.input_endpoint(graph_wiring_detail::input_endpoint_for_sources(
                builder.type().schema() != nullptr ? builder.type().schema()->input_schema : nullptr,
                std::span<const WiringPortRef>{sources.data(), sources.size()}));

            (void)w.add_node(std::type_index(typeid(feedback_detail::feedback_sink_node_tag)),
                             std::move(builder),
                             std::span<const WiringPortRef>{sources.data(), sources.size()},
                             Value{});
            s.bound = true;
            return *this;
        }

        [[nodiscard]] bool bound() const
        {
            return state().bound;
        }

      private:
        struct State
        {
            Wiring                  *wiring{nullptr};
            Port<TSchema>           delegate{};
            const TSValueTypeMetaData *schema{nullptr};
            bool                     bound{false};
        };

        [[nodiscard]] State &state() const
        {
            if (!state_) { throw std::logic_error("FeedbackWiringPort is not initialized"); }
            return *state_;
        }

        std::shared_ptr<State> state_{};
    };

    namespace feedback_detail
    {
        template <typename TSchema>
        [[nodiscard]] FeedbackWiringPort<TSchema> make_feedback(
            Wiring &w,
            Value initial_delta,
            bool has_initial_delta)
        {
            const TSValueTypeMetaData *schema =
                require_feedback_schema(schema_descriptor<TSchema>::ts_meta());
            if (has_initial_delta) { validate_initial_delta(*schema, initial_delta); }

            NodeBuilder builder = make_feedback_source_node(*schema, has_initial_delta);
            WiringPortRef ref = w.add_unique_node(
                std::type_index(typeid(feedback_source_node_tag)),
                std::move(builder),
                std::span<const WiringPortRef>{},
                std::move(initial_delta));
            return FeedbackWiringPort<TSchema>{w, Port<TSchema>{w, std::move(ref)}, *schema};
        }
    }  // namespace feedback_detail

    template <typename TSchema>
    [[nodiscard]] FeedbackWiringPort<TSchema> feedback(Wiring &w)
    {
        return feedback_detail::make_feedback<TSchema>(w, Value{}, false);
    }

    template <typename TSchema>
    [[nodiscard]] FeedbackWiringPort<TSchema> feedback(Wiring &w, Value initial_delta)
    {
        return feedback_detail::make_feedback<TSchema>(w, std::move(initial_delta), true);
    }

    template <typename TSchema, typename T>
        requires(!std::same_as<std::remove_cvref_t<T>, Value>)
    [[nodiscard]] FeedbackWiringPort<TSchema> feedback(Wiring &w, T &&initial_delta)
    {
        return feedback<TSchema>(w, Value{std::forward<T>(initial_delta)});
    }

    template <typename TSchema>
    [[nodiscard]] FeedbackWiringPort<TSchema> feedback(Port<TSchema> ts)
    {
        Wiring &w = ts.checked_wiring();
        auto    fb = feedback<TSchema>(w);
        fb(std::move(ts));
        return fb;
    }

    template <typename TSchema>
    [[nodiscard]] FeedbackWiringPort<TSchema> feedback(Port<TSchema> ts, Value initial_delta)
    {
        Wiring &w = ts.checked_wiring();
        auto    fb = feedback<TSchema>(w, std::move(initial_delta));
        fb(std::move(ts));
        return fb;
    }

    template <typename TSchema, typename T>
        requires(!std::same_as<std::remove_cvref_t<T>, Value>)
    [[nodiscard]] FeedbackWiringPort<TSchema> feedback(Port<TSchema> ts, T &&initial_delta)
    {
        return feedback(std::move(ts), Value{std::forward<T>(initial_delta)});
    }
}  // namespace hgraph::stdlib

#endif  // HGRAPH_LIB_STD_OPERATORS_CONTROL_H
