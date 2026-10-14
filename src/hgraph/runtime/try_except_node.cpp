#include <hgraph/runtime/try_except_node.h>

#include <hgraph/runtime/nested_bindings.h>
#include <hgraph/runtime/node_error.h>
#include <hgraph/runtime/canonical_error.h>
#include <hgraph/types/metadata/ts_value_type_meta_data.h>
#include <hgraph/types/time_series/ts_output/bundle_view.h>
#include <hgraph/util/scope.h>

#include <string>
#include <utility>

namespace hgraph
{
    namespace
    {
        // Write a NodeError to the node's exception output: the "exception" field
        // of a TSB output, or the whole output for a sink (bare TS<NodeError>).
        void write_try_except_error(const NodeView &view, const NodeView &failed_node,
                                    DateTime evaluation_time, std::string error_msg)
        {
            const NodeTypeMetaData *schema = view.schema();
            const ErrorCaptureOptions options = schema != nullptr ? schema->error_capture : ErrorCaptureOptions{};
            NodeErrorFields fields = capture_node_error(
                failed_node.valid() ? failed_node : view, evaluation_time, std::move(error_msg), options);

            Value error_value = make_node_error_value(fields);
            auto  output      = view.output(evaluation_time);

            const bool is_bundle = schema != nullptr && schema->output_schema != nullptr &&
                                   schema->output_schema->kind == TSTypeKind::TSB;
            if (is_bundle)
            {
                auto bundle   = output.as_bundle();
                auto target   = bundle.field("exception");
                auto mutation = target.begin_mutation(evaluation_time);
                (void)mutation.move_value_from(std::move(error_value));
            }
            else
            {
                auto mutation = output.begin_mutation(evaluation_time);
                (void)mutation.move_value_from(std::move(error_value));
            }
        }

        void try_except_start(const NodeView &view, DateTime evaluation_time)
        {
            auto nested = view.as<SingleNestedGraphNodeView>();
            nested.ensure_child_graph();
            single_nested_graph_bind_inputs(nested, evaluation_time);
            single_nested_graph_bind_output(nested, evaluation_time);
            if (nested.context().options.start_child_on_start)
            {
                nested.child_graph().start(evaluation_time);
                schedule_sampled_input_consumers(
                    nested.child_graph(),
                    evaluation_time,
                    nested.context().spec.input_bindings);
            }
            single_nested_graph_propagate_schedule(nested);
        }

        bool try_except_evaluate_impl(const void *, const NodeView &view, DateTime evaluation_time)
        {
            if (!view.started()) { return true; }

            auto nested = view.as<SingleNestedGraphNodeView>();
            nested.ensure_child_graph();
            single_nested_graph_bind_inputs(nested, evaluation_time);
            single_nested_graph_bind_output(nested, evaluation_time);
            std::exception_ptr active;
            const bool completed = fallback_on_exception(true,
                [&] { return nested.child_graph().evaluate(evaluation_time); },
                [&](const char *) { active = std::current_exception(); });
            if (active) {
                const auto *schema = view.schema();
                const auto failed = nested.child_graph().failed_node();
                const bool canonical = schema && schema->error_capture.canonical_errors;
                struct ObservedFailure {
                    std::string message;
                    std::optional<CanonicalError> diagnostic;
                    FailurePhase phase{FailurePhase::Evaluate};
                };
                auto failure = fallback_on_exception(ObservedFailure{"unknown error", {}}, [&]() -> ObservedFailure {
                    try { std::rethrow_exception(active); }
                    catch (const std::exception &cause) {
                        ObservedFailure observed{cause.what(), {}, execution_failure_phase(cause)};
                        if (canonical) {
                            observed.diagnostic = canonicalize_failure(
                                failed.valid() ? failed : view, evaluation_time, "evaluate", cause);
                        }
                        return observed;
                    }
                });
                if (canonical) {
                    if (!failure.diagnostic) {
                        const std::runtime_error unknown{failure.message};
                        failure.diagnostic = canonicalize_failure(
                            failed.valid() ? failed : view, evaluation_time, "evaluate", unknown);
                    }
                    auto error = std::move(*failure.diagnostic);
                    if (failure.phase != FailurePhase::Evaluate) { throw ExecutionFailure{std::move(error), failure.phase}; }
                    auto output = view.output(evaluation_time);
                    auto target = [&]() {
                        if (output.schema()->kind == TSTypeKind::TSB) { auto bundle = output.as_bundle(); return bundle.field("error"); }
                        return std::move(output);
                    }();
                    const auto binding = target.value().binding();
                    auto mutation = target.begin_mutation(evaluation_time);
                    static_cast<void>(mutation.copy_value_from(ValueView{binding, &error}));
                } else { write_try_except_error(view, failed, evaluation_time, std::move(failure.message)); }
                nested.child_graph().finish_failed_evaluation();
            }
            single_nested_graph_propagate_schedule(nested);
            return completed;
        }
    }  // namespace

    NodeBuilder try_except_node(NodeTypeMetaData meta, SingleNestedGraphNodeSpec spec,
                                SingleNestedGraphNodeOptions options,
                                ErrorCaptureOptions error_capture)
    {
        meta.error_capture = error_capture;
        options.manage_output_externally = !spec.output_binding.has_value();
        NodeTypeDescriptor descriptor =
            single_nested_graph_node_descriptor(std::move(meta), std::move(spec), options);
        descriptor.callbacks.start   = &try_except_start;
        descriptor.ops.evaluate_impl = &try_except_evaluate_impl;
        return NodeBuilder::from_descriptor(std::move(descriptor));
    }
}  // namespace hgraph
