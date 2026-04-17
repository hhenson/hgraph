#include <hgraph/types/v2/boundary_binding.h>
#include <hgraph/types/v2/graph.h>

#include <stdexcept>

namespace hgraph::v2
{
    namespace
    {
        constexpr int64_t key_set_path = -3;

        [[nodiscard]] TSOutputView bound_output_of(TSInputView view, const TSMeta *required_schema = nullptr) {
            const TSViewContext &context = view.context_ref();
            BaseState           *state   = context.ts_state;
            if (state == nullptr || state->storage_kind != TSStorageKind::TargetLink) { return TSOutputView{}; }

            const auto &link_state = *static_cast<const TargetLinkState *>(state);
            if (!link_state.is_bound()) { return TSOutputView{}; }

            const LinkedTSContext &target = link_state.target;
            const TSViewContext rooted_context = TSViewContext{target}.resolved();
            if (rooted_context.schema == nullptr || rooted_context.value_dispatch == nullptr || rooted_context.ts_dispatch == nullptr ||
                rooted_context.ts_state == nullptr) {
                return TSOutputView{};
            }

            TSOutputView rooted_output{
                rooted_context,
                TSViewContext::none(),
                view.evaluation_time(),
                target.owning_output,
                target.output_view_ops != nullptr ? target.output_view_ops : &hgraph::detail::default_output_view_ops(),
            };

            if (required_schema != nullptr && rooted_output.ts_schema() != required_schema) {
                if (target.owning_output == nullptr) {
                    throw std::logic_error("BoundaryBindingRuntime requires an owning output endpoint for schema adaptation");
                }
                rooted_output = target.owning_output->bindable_view(rooted_output, required_schema);
            }

            const TSViewContext cast_context = rooted_output.context_ref().resolved();
            return TSOutputView{
                TSViewContext{TSContext{
                    cast_context.schema,
                    cast_context.value_dispatch,
                    cast_context.ts_dispatch,
                    cast_context.value_data,
                    cast_context.ts_state,
                    rooted_output.owning_output(),
                    rooted_output.output_view_ops(),
                    context.notification_state != nullptr ? context.notification_state : context.ts_state,
                }},
                TSViewContext::none(),
                view.evaluation_time(),
                rooted_output.owning_output(),
                rooted_output.output_view_ops(),
            };
        }

        [[nodiscard]] const TSMeta *unwrap_navigation_schema(const TSMeta *schema) {
            if (schema != nullptr && schema->kind == TSKind::REF && schema->element_ts() != nullptr) {
                return schema->element_ts();
            }
            return schema;
        }

        [[nodiscard]] TSInputView navigate_input(TSInputView view, PathView path) {
            const TSMeta *schema = view.ts_schema();
            for (const int64_t slot : path) {
                const TSMeta *collection_schema = unwrap_navigation_schema(schema);
                if (collection_schema == nullptr) {
                    throw std::invalid_argument("BoundaryBindingRuntime input navigation requires a schema");
                }

                if (slot == key_set_path) {
                    if (collection_schema->kind != TSKind::TSD) {
                        throw std::logic_error("BoundaryBindingRuntime key_set input navigation requires a dict schema");
                    }
                    view   = view.as_dict().key_set().view();
                    schema = view.ts_schema();
                    continue;
                }

                switch (collection_schema->kind) {
                    case TSKind::TSB: view = view.as_bundle()[static_cast<size_t>(slot)]; break;
                    case TSKind::TSL: view = view.as_list()[static_cast<size_t>(slot)]; break;
                    default: throw std::invalid_argument("BoundaryBindingRuntime input navigation only supports TSB/TSL/key_set");
                }
                schema = view.ts_schema();
            }
            return view;
        }

        [[nodiscard]] const char *input_binding_mode_name(InputBindingMode mode) {
            switch (mode) {
                case InputBindingMode::BIND_DIRECT: return "BIND_DIRECT";
                case InputBindingMode::CLONE_REF_BINDING: return "CLONE_REF_BINDING";
                case InputBindingMode::BIND_MULTIPLEXED_ELEMENT: return "BIND_MULTIPLEXED_ELEMENT";
                case InputBindingMode::BIND_KEY_VALUE: return "BIND_KEY_VALUE";
                case InputBindingMode::DETACH_RESTORE_BLANK: return "DETACH_RESTORE_BLANK";
            }
            return "<unknown>";
        }

        [[nodiscard]] const char *output_binding_mode_name(OutputBindingMode mode) {
            switch (mode) {
                case OutputBindingMode::ALIAS_CHILD_OUTPUT: return "ALIAS_CHILD_OUTPUT";
                case OutputBindingMode::ALIAS_PARENT_INPUT: return "ALIAS_PARENT_INPUT";
            }
            return "<unknown>";
        }
    }  // namespace

    void BoundaryBindingRuntime::bind(const BoundaryBindingPlan &plan, Graph &child, Node &parent, engine_time_t eval_time) {
        for (const auto &spec : plan.inputs) {
            if (spec.child_node_index < 0) { continue; }

            switch (spec.mode) {
                case InputBindingMode::BIND_DIRECT:
                    {
                        // Navigate to the parent's input field by arg_name, then
                        // extract the upstream output it's bound to.
                        auto         parent_input    = parent.input_view(eval_time);
                        auto         parent_field    = parent_input.as_bundle().field(spec.arg_name);
                        // Navigate to the child's target input
                        auto       &child_dst   = child.node_at(static_cast<size_t>(spec.child_node_index));
                        TSInputView child_input = child_dst.input_view(eval_time);
                        if (!spec.child_input_path.empty()) { child_input = navigate_input(child_input, spec.child_input_path); }

                        TSOutputView upstream_output = bound_output_of(parent_field, child_input.ts_schema());
                        if (upstream_output.ts_schema() == nullptr) { break; }

                        // Bind child input to the same upstream output as the parent's input
                        child_input.bind_output(upstream_output);
                        child_input.make_active();
                        break;
                    }
                case InputBindingMode::CLONE_REF_BINDING:
                case InputBindingMode::BIND_MULTIPLEXED_ELEMENT:
                case InputBindingMode::BIND_KEY_VALUE:
                case InputBindingMode::DETACH_RESTORE_BLANK:
                    throw std::logic_error(std::string{"BoundaryBindingRuntime::bind does not support input mode "} +
                                           input_binding_mode_name(spec.mode));
            }
        }
    }

    void BoundaryBindingRuntime::bind_keyed(const BoundaryBindingPlan & /*plan*/, Graph & /*child*/, Node & /*parent*/,
                                            const value::View & /*key*/, engine_time_t /*eval_time*/) {
        throw std::logic_error("BoundaryBindingRuntime::bind_keyed is not implemented");
    }

    void BoundaryBindingRuntime::unbind(const BoundaryBindingPlan &plan, Graph &child) {
        for (const auto &spec : plan.inputs) {
            if (spec.child_node_index < 0) { continue; }

            if (spec.mode != InputBindingMode::BIND_DIRECT) {
                throw std::logic_error(std::string{"BoundaryBindingRuntime::unbind does not support input mode "} +
                                       input_binding_mode_name(spec.mode));
            }

            auto &child_node  = child.node_at(static_cast<size_t>(spec.child_node_index));
            auto  child_input = child_node.input_view();

            if (!spec.child_input_path.empty()) { child_input = navigate_input(child_input, spec.child_input_path); }

            if (child_input.valid()) {
                child_input.make_passive();
                child_input.unbind_output();
            }
        }
    }

    void BoundaryBindingRuntime::rebind(const BoundaryBindingPlan & /*plan*/, Graph & /*child*/, Node & /*parent*/,
                                        std::string_view /*arg_name*/, engine_time_t /*eval_time*/) {
        throw std::logic_error("BoundaryBindingRuntime::rebind is not implemented");
    }

    const char *boundary_output_binding_mode_name(OutputBindingMode mode) {
        return output_binding_mode_name(mode);
    }

}  // namespace hgraph::v2
