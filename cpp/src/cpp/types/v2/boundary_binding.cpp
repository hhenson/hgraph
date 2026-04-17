#include <hgraph/types/v2/boundary_binding.h>
#include <hgraph/types/v2/graph.h>
#include <hgraph/types/v2/path_constants.h>
#include <hgraph/types/v2/ref.h>

#include <stdexcept>

namespace hgraph::v2
{
    namespace
    {
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

                if (slot == k_key_set_path) {
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

        [[nodiscard]] TSInputView resolve_parent_arg(Node &parent, const InputBindingSpec &spec, engine_time_t eval_time)
        {
            return parent.input_view(eval_time).as_bundle().field(spec.arg_name);
        }

        [[nodiscard]] TSInputView resolve_parent_source(Node &parent, const InputBindingSpec &spec, engine_time_t eval_time)
        {
            TSInputView parent_input = resolve_parent_arg(parent, spec, eval_time);
            if (!spec.parent_input_path.empty()) { parent_input = navigate_input(parent_input, spec.parent_input_path); }
            return parent_input;
        }

        [[nodiscard]] TSInputView resolve_child_input(Graph &child, const InputBindingSpec &spec, engine_time_t eval_time)
        {
            auto       &child_node  = child.node_at(static_cast<size_t>(spec.child_node_index));
            TSInputView child_input = child_node.input_view(eval_time);
            if (!spec.child_input_path.empty()) { child_input = navigate_input(child_input, spec.child_input_path); }
            return child_input;
        }

        [[nodiscard]] bool input_is_bound(TSInputView view) noexcept
        {
            const LinkedTSContext *target = view.linked_target();
            return target != nullptr && target->is_bound();
        }

        void clear_target_link_history(BaseState *state) noexcept
        {
            if (state == nullptr || state->storage_kind != TSStorageKind::TargetLink) { return; }

            auto &link_state = *static_cast<TargetLinkState *>(state);
            link_state.reset_target();
            link_state.previous_target_value = {};
            link_state.switch_modified_time  = MIN_DT;
        }

        void restore_blank_input(TSInputView view)
        {
            if (!view.valid() && !input_is_bound(view) && !view.active()) { return; }

            if (view.active()) { view.make_passive(); }
            if (input_is_bound(view)) { view.unbind_output(); }

            TSViewContext &context = view.context_mutable();
            BaseState     *state   = context.ts_state;
            if (state == nullptr) { return; }

            clear_target_link_history(state);

            if (context.schema != nullptr && context.schema->kind == TSKind::REF && context.schema->value_type != nullptr &&
                context.value_dispatch != nullptr && context.value_data != nullptr) {
                View local_value{context.value_dispatch, context.value_data, context.schema->value_type};
                local_value.as_atomic().set(TimeSeriesReference::make());
            }

            state->last_modified_time = MIN_DT;
        }

        void set_local_value(TSInputView view, const View &value, engine_time_t eval_time)
        {
            TSViewContext &context = view.context_mutable();
            BaseState     *state   = context.ts_state;
            if (state == nullptr || context.schema == nullptr || context.schema->value_type == nullptr ||
                context.value_dispatch == nullptr || context.value_data == nullptr) {
                throw std::logic_error("BoundaryBindingRuntime local input assignment requires native input storage");
            }

            clear_target_link_history(state);

            View local_value{context.value_dispatch, context.value_data, context.schema->value_type};
            local_value.copy_from(value);
            state->mark_modified(eval_time);
        }

        void set_local_reference_value(TSInputView view, const TimeSeriesReference &value, engine_time_t eval_time)
        {
            TSViewContext &context = view.context_mutable();
            BaseState     *state   = context.ts_state;
            if (state == nullptr || context.schema == nullptr || context.schema->kind != TSKind::REF ||
                context.schema->value_type == nullptr || context.value_dispatch == nullptr || context.value_data == nullptr) {
                throw std::logic_error("BoundaryBindingRuntime REF local assignment requires native REF input storage");
            }

            clear_target_link_history(state);

            View local_value{context.value_dispatch, context.value_data, context.schema->value_type};
            local_value.as_atomic().set(value);
            state->mark_modified(eval_time);
        }

        void bind_child_to_output(TSInputView child_input, const TSOutputView &upstream_output)
        {
            if (upstream_output.ts_schema() == nullptr) {
                restore_blank_input(child_input);
                return;
            }

            child_input.bind_output(upstream_output);
            child_input.make_active();
        }

        void bind_direct_input(TSInputView child_input, TSInputView parent_field)
        {
            bind_child_to_output(child_input, bound_output_of(parent_field, child_input.ts_schema()));
        }

        void bind_cloned_reference(TSInputView child_input, TSInputView parent_field, engine_time_t eval_time)
        {
            const TSMeta *child_schema  = child_input.ts_schema();
            const TSMeta *parent_schema = parent_field.ts_schema();
            if (child_schema == nullptr) { throw std::logic_error("BoundaryBindingRuntime clone_ref_binding requires a child schema"); }

            if (TSOutputView upstream_output = bound_output_of(parent_field, child_schema); upstream_output.ts_schema() != nullptr) {
                bind_child_to_output(child_input, upstream_output);
                return;
            }

            const TimeSeriesReference reference = TimeSeriesReference::make(parent_field);

            if (child_schema->kind == TSKind::REF) {
                if (reference.is_empty()) {
                    restore_blank_input(child_input);
                } else {
                    set_local_reference_value(child_input, reference, eval_time);
                    child_input.make_active();
                }
                return;
            }

            if (parent_schema != nullptr && parent_schema->kind == TSKind::REF && reference.is_peered()) {
                TSOutputView target_output = reference.target_view(eval_time);
                if (target_output.ts_schema() != child_schema && target_output.owning_output() != nullptr) {
                    target_output = target_output.owning_output()->bindable_view(target_output, child_schema);
                }
                bind_child_to_output(child_input, target_output);
                return;
            }

            restore_blank_input(child_input);
        }

        [[nodiscard]] TSInputView select_multiplexed_parent_input(TSInputView parent_field, const value::View &key)
        {
            const TSMeta *schema = unwrap_navigation_schema(parent_field.ts_schema());
            if (schema == nullptr) {
                throw std::logic_error("BoundaryBindingRuntime multiplexed binding requires a collection schema");
            }

            switch (schema->kind) {
                case TSKind::TSD: return parent_field.as_dict()[key];
                case TSKind::TSL: return parent_field.as_list()[static_cast<size_t>(key.as_atomic().as<int>())];
                default:
                    throw std::logic_error("BoundaryBindingRuntime multiplexed binding only supports TSD and TSL parent inputs");
            }
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
                        bind_direct_input(resolve_child_input(child, spec, eval_time), resolve_parent_source(parent, spec, eval_time));
                        break;
                    }
                case InputBindingMode::CLONE_REF_BINDING:
                    {
                        bind_cloned_reference(resolve_child_input(child, spec, eval_time),
                                              resolve_parent_source(parent, spec, eval_time),
                                              eval_time);
                        break;
                    }
                case InputBindingMode::BIND_MULTIPLEXED_ELEMENT:
                case InputBindingMode::BIND_KEY_VALUE:
                    throw std::logic_error(std::string{"BoundaryBindingRuntime::bind requires bind_keyed() for input mode "} +
                                           input_binding_mode_name(spec.mode));
                case InputBindingMode::DETACH_RESTORE_BLANK:
                    break;
            }
        }
    }

    void BoundaryBindingRuntime::bind_keyed(const BoundaryBindingPlan &plan, Graph &child, Node &parent, const value::View &key,
                                            engine_time_t eval_time) {
        for (const auto &spec : plan.inputs) {
            if (spec.child_node_index < 0) { continue; }

            TSInputView child_input = resolve_child_input(child, spec, eval_time);
            switch (spec.mode) {
                case InputBindingMode::BIND_MULTIPLEXED_ELEMENT:
                    {
                        TSInputView parent_item = select_multiplexed_parent_input(resolve_parent_arg(parent, spec, eval_time), key);
                        if (!spec.parent_input_path.empty()) { parent_item = navigate_input(parent_item, spec.parent_input_path); }
                        bind_direct_input(child_input, parent_item);
                        break;
                    }
                case InputBindingMode::BIND_KEY_VALUE:
                    {
                        set_local_value(child_input, key, eval_time);
                        child_input.make_active();
                        break;
                    }
                case InputBindingMode::DETACH_RESTORE_BLANK:
                    break;
                case InputBindingMode::BIND_DIRECT:
                case InputBindingMode::CLONE_REF_BINDING:
                    throw std::logic_error(std::string{"BoundaryBindingRuntime::bind_keyed does not support input mode "} +
                                           input_binding_mode_name(spec.mode));
            }
        }
    }

    void BoundaryBindingRuntime::unbind(const BoundaryBindingPlan &plan, Graph &child) {
        for (const auto &spec : plan.inputs) {
            if (spec.child_node_index < 0) { continue; }
            TSInputView child_input = resolve_child_input(child, spec, MIN_DT);

            switch (spec.mode) {
                case InputBindingMode::DETACH_RESTORE_BLANK:
                    restore_blank_input(child_input);
                    break;
                case InputBindingMode::BIND_DIRECT:
                case InputBindingMode::CLONE_REF_BINDING:
                case InputBindingMode::BIND_MULTIPLEXED_ELEMENT:
                case InputBindingMode::BIND_KEY_VALUE:
                    if (child_input.active()) { child_input.make_passive(); }
                    if (input_is_bound(child_input)) { child_input.unbind_output(); }
                    break;
            }
        }
    }

    void BoundaryBindingRuntime::rebind(const BoundaryBindingPlan &plan, Graph &child, Node &parent, std::string_view arg_name,
                                        engine_time_t eval_time) {
        for (const auto &spec : plan.inputs) {
            if (spec.child_node_index < 0 || spec.arg_name != arg_name) { continue; }

            TSInputView child_input = resolve_child_input(child, spec, eval_time);
            switch (spec.mode) {
                case InputBindingMode::BIND_DIRECT:
                    bind_direct_input(child_input, resolve_parent_source(parent, spec, eval_time));
                    break;
                case InputBindingMode::CLONE_REF_BINDING:
                    bind_cloned_reference(child_input, resolve_parent_source(parent, spec, eval_time), eval_time);
                    break;
                case InputBindingMode::DETACH_RESTORE_BLANK:
                    restore_blank_input(child_input);
                    break;
                case InputBindingMode::BIND_MULTIPLEXED_ELEMENT:
                case InputBindingMode::BIND_KEY_VALUE:
                    throw std::logic_error(std::string{"BoundaryBindingRuntime::rebind requires key-aware handling for input mode "} +
                                           input_binding_mode_name(spec.mode));
            }
        }
    }

    const char *boundary_output_binding_mode_name(OutputBindingMode mode) {
        return output_binding_mode_name(mode);
    }

}  // namespace hgraph::v2
