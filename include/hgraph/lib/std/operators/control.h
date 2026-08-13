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

    /** Provide the ``merge(disjoint=True)`` grouping: leftmost-wins reference
        merge over a packed TSL of dictionaries. */
    struct merge_tsd_disjoint_marker
        : Operator<"merge_tsd_disjoint", In<"tsl", TsVar<"S">>, Out<TsVar<"O">>>
    {
    };

    /** Forward the first input modified in each evaluation cycle.
        Input order is the tie-breaker when several streams tick together. For keyed
        dictionaries, distinct keys from all ticking inputs are combined; the leftmost
        input wins a same-key conflict.
        @param tsl Ordered input streams.
        @param disjoint When true, selects the faster TSD path and promises that input
                        dictionaries have no overlapping keys.
        @return A stream containing the cycle's first modified value, or merged TSD delta.
        @par Python example
        @code{.py}
        preferred_tick = hg.merge(primary, secondary)
        combined_books = hg.merge(bids, asks, disjoint=True)
        @endcode */
    struct merge : Operator<"merge", VarIn<"tsl", TsVar<"S">>, Out<TsVar<"S">>>
    {
    };

    /** Select the first valid referenced value in key iteration order.
        The output falls through when the selected entry is removed or invalidated.
        @param tsd Keyed references considered in deterministic key order.
        @return A reference to the first currently valid entry.
        @par Python example
        @code{.py}
        active = hg.reduce_tsd_with_race(candidates)
        @endcode */
    struct reduce_tsd_with_race
        : Operator<"reduce_tsd_with_race", In<"tsd", TSD<ScalarVar<"K">, REF<TsVar<"S">>>>, Out<REF<TsVar<"S">>>>
    {
    };

    /** Bundle-specialized keyed race reduction with the same first-valid and fall-through
        semantics as ``reduce_tsd_with_race``.
        @param tsd Keyed bundle references considered in deterministic key order.
        @return A reference to the first currently valid bundle.
        @par Python example
        @code{.py}
        active_bundle = hg.reduce_tsd_of_bundles_with_race(candidates)
        @endcode */
    struct reduce_tsd_of_bundles_with_race
        : Operator<"reduce_tsd_of_bundles_with_race", In<"tsd", TSD<ScalarVar<"K">, REF<TsVar<"S">>>>,
                   Out<REF<TsVar<"S">>>>
    {
    };

    /** Expose the first valid input in argument order, independently of which input ticks.
        If the selected input invalidates, the output falls through to the next valid input.
        @param ts Ordered candidate streams.
        @return The first currently valid candidate.
        @par Python example
        @code{.py}
        effective_price = hg.race(live_price, cached_price, fallback_price)
        @endcode */
    struct race : Operator<"race", VarIn<"ts", TsVar<"S">>, Out<TsVar<"S">>>
    {
    };

    /** Return true when every supplied boolean value is true.
        Variadic and keyed-dictionary forms recompute when any member changes.
        @param args Boolean inputs to test.
        @param arg Keyed boolean collection accepted by collection overloads.
        @return Boolean conjunction of all current inputs.
        @par Python example
        @code{.py}
        ready = hg.all_(has_price, has_quantity, is_open)
        @endcode */
    struct all_ : Operator<"all_", VarIn<"args", TS<Bool>>, Out<TS<Bool>>>
    {
    };

    /** Return true when at least one supplied boolean value is true.
        Variadic and keyed-dictionary forms recompute when any member changes.
        @param args Boolean inputs to test.
        @param arg Keyed boolean collection accepted by collection overloads.
        @return Boolean disjunction of all current inputs.
        @par Python example
        @code{.py}
        has_alert = hg.any_(price_alert, risk_alert)
        @endcode */
    struct any_ : Operator<"any_", VarIn<"args", TS<Bool>>, Out<TS<Bool>>>
    {
    };

    /** Route each source tick to the ``true`` or ``false`` field of a bundle according
        to the latest condition. The non-selected output does not receive that tick.
        @param condition Boolean route selector.
        @param ts Stream to route.
        @return A two-field bundle containing the mutually exclusive routed outputs.
        @par Python example
        @code{.py}
        routed = hg.if_(is_buy, order)
        buys, sells = routed.true, routed.false
        @endcode */
    struct if_ : Operator<"if_", In<"condition", TS<Bool>>, In<"ts", TsVar<"S">>, Out<TsVar<"O">>>
    {
    };

    /** Route each source tick to one element of an output list.
        @param index Zero-based destination selected by its latest value.
        @param ts Stream to route.
        @return A time-series list whose selected element receives each source tick.
        @par Python example
        @code{.py}
        partitions = hg.route_by_index(destination, event)
        @endcode */
    struct route_by_index : Operator<"route_by_index", In<"index", TS<Int>>, In<"ts", TsVar<"S">>, Out<TsVar<"O">>>
    {
    };

    /** Emit true whenever ``condition`` ticks with a true value.
        False values are suppressed rather than emitted. One-shot mode passivates the
        input after the first true tick.
        @param condition Boolean stream to observe.
        @param tick_once_only When true, emit at most one tick.
        @return A true-valued signal for qualifying condition ticks.
        @par Python example
        @code{.py}
        first_ready = hg.if_true(is_ready, tick_once_only=True)
        @endcode */
    struct if_true : Operator<"if_true", In<"condition", TS<Bool>>, Scalar<"tick_once_only", Bool>, Out<TS<Bool>>>
    {
    };

    /** Select between two value streams using the latest boolean condition.
        A tick from the active branch is forwarded; a tick from the inactive branch is not.
        Python scalar branch values are lifted to constant sources.  This
        preserves the nominal type of ``Enum``, ``IntEnum``, and ``StrEnum``
        members, so callers do not need to wrap them in typed ``const`` nodes.
        @param condition Boolean selector.
        @param true_value Value exposed while ``condition`` is true.
        @param false_value Value exposed while ``condition`` is false.
        @return The currently selected branch.
        @par Python example
        @code{.py}
        effective = hg.if_then_else(use_live, live_value, fallback_value)
        @endcode */
    struct if_then_else : Operator<"if_then_else", In<"condition", TS<Bool>>, In<"true_value", TsVar<"S">>,
                                   In<"false_value", TsVar<"S">>, Out<TsVar<"S">>>
    {
    };

    /** Select one of three value streams from a three-way comparison result.
        @param cmp ``LT``, ``EQ``, or ``GT`` selector, typically produced by ``cmp_``.
        @param lt Value selected for ``LT``.
        @param eq Value selected for ``EQ``.
        @param gt Value selected for ``GT``.
        @return The branch selected by ``cmp``.
        @par Python example
        @code{.py}
        label = hg.if_cmp(hg.cmp_(lhs, rhs), "low", "equal", "high")
        @endcode */
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
