#include <hgraph/runtime/feedback_node.h>

#include <hgraph/runtime/graph.h>
#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/time_series/endpoint_schema.h>
#include <hgraph/types/time_series/ts_delta.h>
#include <hgraph/types/time_series/ts_input/bundle_view.h>
#include <hgraph/types/time_series/ts_input/base_view.h>
#include <hgraph/types/time_series/ts_output/base_view.h>
#include <hgraph/types/value/value.h>
#include <hgraph/types/value/value_view.h>
#include <hgraph/util/date_time.h>

#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

namespace hgraph
{
    namespace
    {
        void validate_feedback_schema(const TSValueTypeMetaData &schema)
        {
            if (schema.delta_value_schema == nullptr)
            {
                throw std::invalid_argument("feedback node requires a time-series delta value schema");
            }
        }

        [[nodiscard]] bool try_copy_feedback_state(
            const ValueView &state, const ValueView &source)
        {
            if (!state.can_begin_mutation() || !source.has_value()) { return false; }

            const auto target_binding = state.binding();
            const auto source_binding = source.binding();
            if (!target_binding || !source_binding ||
                !target_binding.ops_ref().accepts_source(
                    target_binding, source_binding))
            {
                return false;
            }

            auto mutation = state.begin_mutation();
            target_binding.ops_ref().copy_assign_from(
                target_binding, mutation.mutable_data(),
                source_binding, source.data());
            return true;
        }

        void start_feedback_source_with_initial_delta(const NodeView &view, DateTime start_time)
        {
            const ValueView state = view.state();
            if (!try_copy_feedback_state(state, view.scalars()))
            {
                view.replace_state(view.scalars().clone());
            }

            if (GraphValue *graph = view.graph_value(); graph != nullptr)
            {
                graph->schedule_node(view.node_index(), start_time);
            }
        }

        void evaluate_feedback_source(const NodeView &view, DateTime evaluation_time)
        {
            apply_delta(view.output(evaluation_time), view.state());
        }

        void evaluate_feedback_sink(const NodeView &view, DateTime evaluation_time)
        {
            auto root    = view.input(evaluation_time);
            auto bundle  = root.as_bundle();
            auto ts      = bundle[0];
            auto ts_self = bundle[1];

            TSOutputView source_out  = ts_self.bound_output();
            NodeView   source_node   = source_out.owner_node();
            if (!source_node.valid())
            {
                throw std::logic_error("feedback sink could not recover the feedback source node");
            }
            if (!source_node.has_state())
            {
                throw std::logic_error("feedback sink target node has no delta state");
            }
            // Copy through the already-planned source state when its binding
            // accepts the observed delta. This is intentionally binding-aware:
            // a polymorphic TS[Base] may expose a concrete Derived delta, whose
            // schema differs while remaining a valid source for the graph's
            // closed Base realization. A sampled rebind can expose the current-
            // value schema instead of the delta schema, so retain capture_delta
            // as the general fallback. That fallback may replace the planned
            // state with immutable compact storage, in which case a later tick
            // must capture again rather than request a mutation view.
            const ValueView state = source_node.state();
            if (!try_copy_feedback_state(state, ts.delta_value()))
            {
                source_node.replace_state(capture_delta(ts));
            }

            GraphValue *graph = source_node.graph_value();
            if (graph == nullptr)
            {
                throw std::logic_error("feedback sink target node is not attached to a graph");
            }
            graph->schedule_node(source_node.node_index(), evaluation_time + MIN_TD);
        }

        [[nodiscard]] const TSValueTypeMetaData *feedback_sink_input_schema(
            const TSValueTypeMetaData &schema)
        {
            return TypeRegistry::instance().un_named_tsb({
                {"ts", &schema},
                {"ts_self", &schema},
            });
        }

        [[nodiscard]] TSEndpointSchema feedback_sink_endpoint_schema(
            const TSValueTypeMetaData &input_schema,
            const TSValueTypeMetaData &schema)
        {
            return TSEndpointSchema::non_peered(
                &input_schema,
                {
                    TSEndpointSchema::peered(&schema),
                    TSEndpointSchema::peered(&schema),
                });
        }
    }  // namespace

    NodeBuilder make_feedback_source_node(const TSValueTypeMetaData &output_schema,
                                          bool has_initial_delta)
    {
        validate_feedback_schema(output_schema);

        NodeTypeMetaData schema;
        schema.display_name  = "feedback_source";
        schema.output_schema = &output_schema;
        schema.state_schema  = output_schema.delta_value_schema;
        schema.scalar_schema = has_initial_delta ? output_schema.delta_value_schema : nullptr;
        schema.node_kind     = NodeKind::PullSource;

        NodeCallbacks callbacks;
        if (has_initial_delta) { callbacks.start = &start_feedback_source_with_initial_delta; }
        callbacks.evaluate = &evaluate_feedback_source;

        return NodeBuilder::native(std::move(schema), std::move(callbacks));
    }

    NodeBuilder make_feedback_sink_node(const TSValueTypeMetaData &schema)
    {
        validate_feedback_schema(schema);

        const TSValueTypeMetaData *input_schema = feedback_sink_input_schema(schema);

        NodeTypeMetaData node_schema;
        node_schema.display_name = "feedback_sink";
        node_schema.input_schema = input_schema;
        node_schema.node_kind    = NodeKind::Sink;
        node_schema.active_inputs = std::vector<std::size_t>{0};
        node_schema.valid_inputs  = std::vector<std::size_t>{0};

        NodeCallbacks callbacks;
        callbacks.evaluate = &evaluate_feedback_sink;

        return NodeBuilder::native(
            std::move(node_schema),
            std::move(callbacks),
            feedback_sink_endpoint_schema(*input_schema, schema));
    }
}  // namespace hgraph
