#include <hgraph/types/boundary_binding.h>
#include <hgraph/types/graph.h>
#include <hgraph/types/path_constants.h>
#include <hgraph/types/ref.h>

#include <fmt/format.h>

#include <stdexcept>

namespace hgraph
{
    namespace
    {
        [[nodiscard]] std::string schema_debug_label(const value::TypeMeta *schema)
        {
            if (schema == nullptr) { return "<null>"; }
            return fmt::format("{}@{:p}[kind={}]", schema->name != nullptr ? schema->name : "<unnamed>",
                               static_cast<const void *>(schema),
                               static_cast<int>(schema->kind));
        }

        [[nodiscard]] TSOutputView bound_output_of(TSInputView view, const TSMeta *required_schema = nullptr) {
            const TSViewContext &input_context = view.context_ref();
            TSOutputView source_output;
            if (const LinkedTSContext *target = view.linked_target();
                target != nullptr && target->is_bound() && target->schema != nullptr && target->value_dispatch != nullptr &&
                target->ts_dispatch != nullptr && target->owning_output != nullptr) {
                source_output = TSOutputView{
                    TSViewContext{TSContext{
                        target->schema,
                        target->value_dispatch,
                        target->ts_dispatch,
                        target->value_data,
                        target->ts_state,
                        target->owning_output,
                        target->output_view_ops,
                        target->notification_state != nullptr ? target->notification_state : target->ts_state,
                        target->pending_dict_child,
                    }},
                    TSViewContext::none(),
                    view.evaluation_time(),
                    target->owning_output,
                    target->output_view_ops != nullptr ? target->output_view_ops : &hgraph::detail::default_output_view_ops(),
                };
            }

            const TSViewContext source_context = input_context.resolved();
            if (!source_output.valid()) {
                if (source_context.schema == nullptr || source_context.value_dispatch == nullptr || source_context.ts_dispatch == nullptr ||
                    source_context.owning_output == nullptr) {
                    return TSOutputView{};
                }

                source_output = TSOutputView{
                    TSViewContext{TSContext{
                        source_context.schema,
                        source_context.value_dispatch,
                        source_context.ts_dispatch,
                        source_context.ts_state != nullptr ? nullptr : source_context.value_data,
                        source_context.ts_state,
                        source_context.owning_output,
                        source_context.output_view_ops,
                        source_context.notification_state != nullptr ? source_context.notification_state : source_context.ts_state,
                        source_context.pending_dict_child,
                    }},
                    TSViewContext::none(),
                    view.evaluation_time(),
                    source_context.owning_output,
                    source_context.output_view_ops != nullptr ? source_context.output_view_ops : &hgraph::detail::default_output_view_ops(),
                };
            }

            if (required_schema != nullptr && !binding_compatible_ts_schema(source_output.ts_schema(), required_schema)) {
                if (source_context.owning_output == nullptr || !source_output.context_ref().is_bound()) { return TSOutputView{}; }
                source_output = source_context.owning_output->bindable_view(source_output, required_schema);
            }

            return source_output;
        }

        [[nodiscard]] const TSMeta *unwrap_navigation_schema(const TSMeta *schema) {
            if (schema != nullptr && schema->kind == TSKind::REF && schema->element_ts() != nullptr) {
                return schema->element_ts();
            }
            return schema;
        }

        [[nodiscard]] TSInputView navigate_input(TSInputView view, PathView path) {
            const TSMeta *schema = view.context_ref().schema;
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
                    schema = view.context_ref().schema;
                    continue;
                }

                switch (collection_schema->kind) {
                    case TSKind::TSB: view = view.as_bundle()[static_cast<size_t>(slot)]; break;
                    case TSKind::TSL: view = view.as_list()[static_cast<size_t>(slot)]; break;
                    default: throw std::invalid_argument("BoundaryBindingRuntime input navigation only supports TSB/TSL/key_set");
                }
                schema = view.context_ref().schema;
            }
            return view;
        }

        [[nodiscard]] TSOutputView navigate_output(TSOutputView view, PathView path) {
            const TSMeta *schema = view.ts_schema();
            for (const int64_t slot : path) {
                const TSMeta *collection_schema = unwrap_navigation_schema(schema);
                if (collection_schema == nullptr) {
                    throw std::invalid_argument("BoundaryBindingRuntime output navigation requires a schema");
                }

                if (slot == k_key_set_path) {
                    if (collection_schema->kind != TSKind::TSD) {
                        throw std::logic_error("BoundaryBindingRuntime output key_set navigation requires a dict schema");
                    }
                    view = view.as_dict().key_set().view();
                    schema = view.ts_schema();
                    continue;
                }

                switch (collection_schema->kind) {
                    case TSKind::TSB: view = view.as_bundle()[static_cast<size_t>(slot)]; break;
                    case TSKind::TSL: view = view.as_list()[static_cast<size_t>(slot)]; break;
                    default: throw std::invalid_argument("BoundaryBindingRuntime output navigation only supports TSB/TSL/key_set");
                }
                schema = view.ts_schema();
            }
            return view;
        }

        [[nodiscard]] TSInputView resolve_parent_spec_arg(Node &parent, const InputBindingSpec &spec, engine_time_t eval_time)
        {
            return parent.input_view(eval_time).as_bundle().field(spec.arg_name);
        }

        [[nodiscard]] TSInputView resolve_parent_source(Node &parent, const InputBindingSpec &spec, engine_time_t eval_time)
        {
            TSInputView parent_input = resolve_parent_spec_arg(parent, spec, eval_time);
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

        void detach_input_for_stop(TSInputView view) noexcept
        {
            TSViewContext &context = view.context_mutable();
            BaseState *state = context.ts_state;

            if (state != nullptr && view.scheduling_notifier() != nullptr) {
                state->unsubscribe(view.scheduling_notifier());
            }

            ActiveTriePosition &active_pos = view.active_position_mutable();
            if (active_pos.node != nullptr) {
                active_pos.node->locally_active = false;
                if (!active_pos.node->has_any_active() && active_pos.trie != nullptr) {
                    active_pos.trie->try_prune_root();
                }
                active_pos.node = nullptr;
            }

            clear_target_link_history(state);

            if (context.schema != nullptr && context.schema->kind == TSKind::REF && context.schema->value_type != nullptr &&
                context.value_dispatch != nullptr && context.value_data != nullptr) {
                View local_value{context.value_dispatch, context.value_data, context.schema->value_type};
                local_value.as_atomic().set(TimeSeriesReference::make());
            }

            if (state != nullptr) { state->last_modified_time = MIN_DT; }
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
            if (local_value.schema() != value.schema()) {
                throw std::logic_error(fmt::format("BoundaryBindingRuntime local input assignment schema mismatch: {} != {}",
                                                   schema_debug_label(local_value.schema()),
                                                   schema_debug_label(value.schema())));
            }
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
            if (child_input.evaluation_time() > MIN_DT && child_input.scheduling_notifier() != nullptr &&
                (child_input.modified() || child_input.valid())) {
                child_input.scheduling_notifier()->notify(child_input.evaluation_time());
            }
        }

        [[nodiscard]] bool output_endpoint_available(const TSOutputView &view) noexcept
        {
            const LinkedTSContext context = view.linked_context();
            return context.schema != nullptr && context.value_dispatch != nullptr && context.ts_dispatch != nullptr;
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

            const TimeSeriesReference reference = TimeSeriesReference::make(parent_field);
            if (child_schema->kind == TSKind::REF) {
                TimeSeriesReference child_reference = TimeSeriesReference::make();
                if (TSOutputView upstream_output = bound_output_of(parent_field); upstream_output.ts_schema() != nullptr) {
                    child_reference = TimeSeriesReference::make(upstream_output);
                }
                if (child_reference.is_empty()) { child_reference = reference; }

                set_local_reference_value(child_input, child_reference.is_empty() ? TimeSeriesReference::make() : child_reference, eval_time);
                child_input.make_active();
                return;
            }

            if (parent_schema != nullptr && parent_schema->kind == TSKind::REF && reference.is_peered()) {
                TSOutputView target_output = reference.target_view(eval_time);
                if (!binding_compatible_ts_schema(target_output.ts_schema(), child_schema) && target_output.owning_output() != nullptr) {
                    target_output = target_output.owning_output()->bindable_view(target_output, child_schema);
                }
                bind_child_to_output(child_input, target_output);
                return;
            }

            if (TSOutputView upstream_output = bound_output_of(parent_field, child_schema); upstream_output.ts_schema() != nullptr) {
                bind_child_to_output(child_input, upstream_output);
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
                case OutputBindingMode::ALIAS_KEY_VALUE: return "ALIAS_KEY_VALUE";
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
                    break;
                case InputBindingMode::DETACH_RESTORE_BLANK:
                    break;
            }
        }
    }

    void BoundaryBindingRuntime::bind_keyed(const BoundaryBindingPlan &plan, Graph &child, Node &parent, const TSOutputView &key_source,
                                            const value::View &key, engine_time_t eval_time) {
        for (const auto &spec : plan.inputs) {
            if (spec.child_node_index < 0) { continue; }

            TSInputView child_input = resolve_child_input(child, spec, eval_time);
            switch (spec.mode) {
                case InputBindingMode::BIND_MULTIPLEXED_ELEMENT:
                    {
                        TSInputView parent_source = resolve_parent_spec_arg(parent, spec, eval_time);
                        TSOutputView parent_output = bound_output_of(parent_source);
                        if (parent_output.ts_schema() != nullptr) {
                            const TSMeta *collection_schema = unwrap_navigation_schema(parent_output.ts_schema());
                            if (collection_schema != nullptr && parent_output.ts_schema() != collection_schema &&
                                parent_output.owning_output() != nullptr) {
                                parent_output = parent_output.owning_output()->bindable_view(parent_output, collection_schema);
                            }

                            TSOutputView parent_item_output;
                            collection_schema = parent_output.ts_schema();
                            if (collection_schema != nullptr) {
                                switch (collection_schema->kind) {
                                    case TSKind::TSD: parent_item_output = detail::ensure_dict_child_output_view(parent_output, key); break;
                                    case TSKind::TSL:
                                        parent_item_output = parent_output.as_list()[static_cast<size_t>(key.as_atomic().as<int>())];
                                        break;
                                    default: break;
                                }
                            }

                            if (output_endpoint_available(parent_item_output)) {
                                if (!spec.parent_input_path.empty()) { parent_item_output = navigate_output(parent_item_output, spec.parent_input_path); }

                                if (output_endpoint_available(parent_item_output)) {
                                    const TSMeta *child_schema = child_input.ts_schema();
                                    if (child_schema != nullptr && child_schema->kind == TSKind::REF) {
                                        set_local_reference_value(child_input, TimeSeriesReference::make(parent_item_output), eval_time);
                                        child_input.make_active();
                                        break;
                                    }
                                }

                                if (output_endpoint_available(parent_item_output)) {
                                    const TSMeta *child_schema = child_input.ts_schema();
                                    const TSMeta *parent_schema = parent_item_output.ts_schema();
                                    if (child_schema != nullptr && parent_schema != nullptr &&
                                        parent_schema->kind == TSKind::REF && child_schema->kind != TSKind::REF) {
                                        const LinkedTSContext dereferenced_context =
                                            detail::dereferenced_target_from_source(parent_item_output.linked_context());
                                        if (dereferenced_context.is_bound()) {
                                            TSOutputView target_output{
                                                TSViewContext{TSContext{
                                                    dereferenced_context.schema,
                                                    dereferenced_context.value_dispatch,
                                                    dereferenced_context.ts_dispatch,
                                                    nullptr,
                                                    parent_item_output.context_ref().ts_state,
                                                    parent_item_output.owning_output(),
                                                    parent_item_output.output_view_ops(),
                                                    parent_item_output.context_ref().notification_state != nullptr
                                                        ? parent_item_output.context_ref().notification_state
                                                        : parent_item_output.context_ref().ts_state,
                                                    parent_item_output.context_ref().pending_dict_child,
                                                }},
                                                TSViewContext::none(),
                                                eval_time,
                                                parent_item_output.owning_output(),
                                                parent_item_output.output_view_ops() != nullptr
                                                    ? parent_item_output.output_view_ops()
                                                    : &hgraph::detail::default_output_view_ops(),
                                            };
                                            if (!binding_compatible_ts_schema(target_output.ts_schema(), child_schema) &&
                                                target_output.owning_output() != nullptr) {
                                                target_output = target_output.owning_output()->bindable_view(target_output, child_schema);
                                            }
                                            bind_child_to_output(child_input, target_output);
                                            break;
                                        }
                                    }
                                    if (child_schema != nullptr &&
                                        !binding_compatible_ts_schema(parent_item_output.ts_schema(), child_schema) &&
                                        parent_item_output.owning_output() != nullptr) {
                                        parent_item_output = parent_item_output.owning_output()->bindable_view(parent_item_output, child_schema);
                                    }
                                    bind_child_to_output(child_input, parent_item_output);
                                    break;
                                }
                            }
                        }

                        TSInputView parent_item = select_multiplexed_parent_input(parent_source, key);
                        if (!spec.parent_input_path.empty()) { parent_item = navigate_input(parent_item, spec.parent_input_path); }
                        bind_cloned_reference(child_input, parent_item, eval_time);
                        break;
                    }
                case InputBindingMode::BIND_KEY_VALUE:
                    {
                        if (key_source.ts_schema() != nullptr) {
                            TSOutputView source_output = key_source;
                            if (child_input.ts_schema() != nullptr &&
                                !binding_compatible_ts_schema(source_output.ts_schema(), child_input.ts_schema()) &&
                                source_output.owning_output() != nullptr) {
                                source_output = source_output.owning_output()->bindable_view(source_output, child_input.ts_schema());
                            }
                            bind_child_to_output(child_input, source_output);
                        } else {
                            set_local_value(child_input, key, eval_time);
                            child_input.make_active();
                        }
                        break;
                    }
                case InputBindingMode::DETACH_RESTORE_BLANK:
                    break;
                case InputBindingMode::BIND_DIRECT:
                case InputBindingMode::CLONE_REF_BINDING:
                    break;
            }
        }
    }

    void BoundaryBindingRuntime::bind_from_output(TSInputView child_input,
                                                  TSOutputView source_output,
                                                  InputBindingMode mode,
                                                  engine_time_t eval_time)
    {
        const TSMeta *child_schema = child_input.ts_schema();

        switch (mode) {
            case InputBindingMode::BIND_DIRECT:
                if (source_output.ts_schema() == nullptr || !source_output.context_ref().is_bound()) {
                    restore_blank_input(child_input);
                    return;
                }

                if (child_schema != nullptr && !binding_compatible_ts_schema(source_output.ts_schema(), child_schema) &&
                    source_output.owning_output() != nullptr) {
                    source_output = source_output.owning_output()->bindable_view(source_output, child_schema);
                }
                bind_child_to_output(child_input, source_output);
                return;

            case InputBindingMode::CLONE_REF_BINDING:
                if (child_schema == nullptr) {
                    throw std::logic_error("BoundaryBindingRuntime::bind_from_output clone_ref_binding requires a child schema");
                }

                if (child_schema->kind == TSKind::REF) {
                    if (source_output.ts_schema() == nullptr || !source_output.context_ref().is_bound()) {
                        restore_blank_input(child_input);
                        return;
                    }

                    set_local_reference_value(child_input, TimeSeriesReference::make(source_output), eval_time);
                    child_input.make_active();
                    return;
                }

                if (source_output.ts_schema() == nullptr || !source_output.context_ref().is_bound()) {
                    restore_blank_input(child_input);
                    return;
                }

                if (!binding_compatible_ts_schema(source_output.ts_schema(), child_schema) &&
                    source_output.owning_output() != nullptr) {
                    source_output = source_output.owning_output()->bindable_view(source_output, child_schema);
                }
                bind_child_to_output(child_input, source_output);
                return;

            case InputBindingMode::DETACH_RESTORE_BLANK:
                restore_blank_input(child_input);
                return;

            case InputBindingMode::BIND_MULTIPLEXED_ELEMENT:
            case InputBindingMode::BIND_KEY_VALUE:
                throw std::logic_error("BoundaryBindingRuntime::bind_from_output requires key-aware handling for keyed modes");
        }
    }

    void BoundaryBindingRuntime::unbind(const BoundaryBindingPlan &plan, Graph &child) {
        for (const auto &spec : plan.inputs) {
            if (spec.child_node_index < 0) { continue; }
            TSInputView child_input = resolve_child_input(child, spec, MIN_DT);

            switch (spec.mode) {
                case InputBindingMode::DETACH_RESTORE_BLANK:
                    detach_input_for_stop(child_input);
                    break;
                case InputBindingMode::BIND_DIRECT:
                case InputBindingMode::CLONE_REF_BINDING:
                case InputBindingMode::BIND_MULTIPLEXED_ELEMENT:
                case InputBindingMode::BIND_KEY_VALUE:
                    detach_input_for_stop(child_input);
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

}  // namespace hgraph
