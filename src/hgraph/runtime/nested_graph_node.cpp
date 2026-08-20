#include <hgraph/runtime/nested_bindings.h>
#include <hgraph/runtime/nested_graph_node.h>

#include <array>
#include <memory>
#include <stdexcept>
#include <string_view>
#include <utility>
#include <vector>

namespace hgraph
{
    namespace
    {
        constexpr std::string_view child_graph_field_name{"single_nested_graph"};
        constexpr std::string_view child_graph_memory_field_name{"single_nested_graph_memory"};

        struct SingleNestedGraphNodeStorage
        {
            GraphValue graph{};
        };

        // Program-lifetime, intentionally-leaked storage for node contexts. A
        // context is type-level metadata referenced by interned ops tables, and it
        // owns a GraphBuilder that references other interned registries (types,
        // plans). Leaking avoids a static-destruction-order crash at exit: a
        // function-local static would be torn down relative to those registries in
        // an order we do not control, destroying the GraphBuilder after the
        // artifacts it references. Do NOT "simplify" this to a static container.
        [[nodiscard]] std::vector<std::unique_ptr<SingleNestedGraphNodeContext>> &single_nested_graph_contexts() noexcept
        {
            static auto *contexts = new std::vector<std::unique_ptr<SingleNestedGraphNodeContext>>;
            return *contexts;
        }

        [[nodiscard]] const SingleNestedGraphNodeContext *register_single_nested_graph_context(
            SingleNestedGraphNodeSpec spec,
            SingleNestedGraphNodeOptions options,
            std::size_t graph_storage_offset,
            std::size_t graph_memory_offset,
            MemoryUtils::StorageLayout graph_memory_layout)
        {
            auto context = std::make_unique<SingleNestedGraphNodeContext>(SingleNestedGraphNodeContext{
                .spec = std::move(spec),
                .graph_storage_offset = graph_storage_offset,
                .graph_memory_offset = graph_memory_offset,
                .graph_memory_layout = graph_memory_layout,
                .options = options,
            });
            const auto *result = context.get();
            single_nested_graph_contexts().push_back(std::move(context));
            return result;
        }

        [[nodiscard]] NodeStorageMetrics single_nested_storage_metrics(
            const void *raw_context, const void *memory) noexcept
        {
            const auto &context = *static_cast<const SingleNestedGraphNodeContext *>(raw_context);
            const auto &storage = *MemoryUtils::cast<const SingleNestedGraphNodeStorage>(
                MemoryUtils::advance(memory, context.graph_storage_offset));
            return NodeStorageMetrics{
                .nested_graph_count = storage.graph.has_value() ? 1U : 0U,
                .nested_graph_capacity = 1,
            };
        }

        void visit_single_nested_child(const void *raw_context,
                                       const NodeBuilder &,
                                       void *visitor_context,
                                       ChildGraphVisitor visitor)
        {
            const auto &context = *static_cast<const SingleNestedGraphNodeContext *>(raw_context);
            visitor(visitor_context, ChildGraphInspectionView{
                                         .graph = &context.spec.graph_builder,
                                         .output_binding = context.spec.output_binding
                                                               ? &*context.spec.output_binding
                                                               : nullptr,
                                     });
        }

        [[nodiscard]] SingleNestedGraphNodeView checked_nested_view(const NodeView &view)
        {
            return view.as<SingleNestedGraphNodeView>();
        }

        [[nodiscard]] const TSValueTypeMetaData *nested_output_child_schema(const TSValueTypeMetaData &schema,
                                                                            std::size_t index)
        {
            switch (schema.kind)
            {
                case TSTypeKind::TSB:
                    if (index >= schema.field_count())
                    {
                        throw std::out_of_range("single_nested_graph_node output target path is out of range for TSB");
                    }
                    return schema.fields()[index].type;

                case TSTypeKind::TSL:
                    if (schema.fixed_size() == 0)
                    {
                        throw std::invalid_argument(
                            "single_nested_graph_node output target path requires fixed-size TSL prefixes");
                    }
                    if (index >= schema.fixed_size())
                    {
                        throw std::out_of_range("single_nested_graph_node output target path is out of range for TSL");
                    }
                    return schema.element_ts();

                default:
                    throw std::invalid_argument(
                        "single_nested_graph_node output target path can only traverse TSB or fixed-size TSL");
            }
        }

        [[nodiscard]] std::size_t nested_output_child_count(const TSValueTypeMetaData &schema)
        {
            switch (schema.kind)
            {
                case TSTypeKind::TSB:
                    return schema.field_count();

                case TSTypeKind::TSL:
                    if (schema.fixed_size() == 0)
                    {
                        throw std::invalid_argument(
                            "single_nested_graph_node output target path requires fixed-size TSL prefixes");
                    }
                    return schema.fixed_size();

                default:
                    throw std::invalid_argument(
                        "single_nested_graph_node output target path can only traverse TSB or fixed-size TSL");
            }
        }

        [[nodiscard]] TSEndpointSchema nested_output_endpoint_schema_for(
            const TSValueTypeMetaData *schema,
            const std::vector<std::size_t> &target_path,
            std::size_t depth = 0)
        {
            if (schema == nullptr)
            {
                throw std::invalid_argument("single_nested_graph_node output binding requires an output schema");
            }
            if (depth == target_path.size())
            {
                return forwarding_output_endpoint_schema(schema);
            }

            const auto selected = target_path[depth];
            const auto count    = nested_output_child_count(*schema);
            if (selected >= count)
            {
                throw std::out_of_range("single_nested_graph_node output target path is out of range");
            }

            std::vector<TSEndpointSchema> children;
            children.reserve(count);
            for (std::size_t index = 0; index < count; ++index)
            {
                const auto *child_schema = nested_output_child_schema(*schema, index);
                children.push_back(index == selected
                                       ? nested_output_endpoint_schema_for(child_schema, target_path, depth + 1)
                                       : TSEndpointSchema::owned(child_schema));
            }
            return TSEndpointSchema::non_peered(schema, std::move(children));
        }

        bool single_nested_graph_evaluate_impl(const void *, const NodeView &view, DateTime evaluation_time)
        {
            return single_nested_graph_evaluate(view, evaluation_time);
        }

    }  // namespace

    const void *SingleNestedGraphNodeView::node_view_type_id() noexcept
    {
        static const char token{};
        return &token;
    }

    SingleNestedGraphNodeView SingleNestedGraphNodeView::from_node(NodeView view, const void *context)
    {
        if (context == nullptr) { throw std::logic_error("SingleNestedGraphNodeView requires a typed view context"); }
        const auto &typed_context = *static_cast<const SingleNestedGraphNodeContext *>(context);
        auto       *storage = MemoryUtils::cast<SingleNestedGraphNodeStorage>(
            MemoryUtils::advance(view.data(), typed_context.graph_storage_offset));
        return SingleNestedGraphNodeView{std::move(view), typed_context, storage->graph};
    }

    const NodeView &SingleNestedGraphNodeView::node() const noexcept
    {
        return view_;
    }

    const SingleNestedGraphNodeContext &SingleNestedGraphNodeView::context() const noexcept
    {
        return *context_;
    }

    GraphValue &SingleNestedGraphNodeView::child_graph_value() const noexcept
    {
        return *child_graph_;
    }

    GraphView SingleNestedGraphNodeView::child_graph() const
    {
        return child_graph_->view();
    }

    void SingleNestedGraphNodeView::ensure_child_graph() const
    {
        if (!child_graph_->has_value())
        {
            *child_graph_ = context_->spec.graph_builder.make_nested_graph(
                view_.pointer(),
                MemoryUtils::advance(view_.data(), context_->graph_memory_offset),
                context_->graph_memory_layout);
        }
    }

    SingleNestedGraphNodeView::SingleNestedGraphNodeView(NodeView view,
                                                         const SingleNestedGraphNodeContext &context,
                                                         GraphValue &child_graph) noexcept
        : view_(std::move(view)),
          context_(&context),
          child_graph_(&child_graph)
    {
    }

    NodeTypeDescriptor single_nested_graph_node_descriptor(
        NodeTypeMetaData meta,
        SingleNestedGraphNodeSpec spec,
        SingleNestedGraphNodeOptions options)
    {
        meta.requires_phase_runner =
            meta.requires_phase_runner || spec.graph_builder.requires_phase_runner();
        if (spec.output_binding.has_value() && !options.manage_output_externally)
        {
            if (meta.output_schema == nullptr)
            {
                throw std::invalid_argument("single_nested_graph_node output binding requires an output schema");
            }
            meta.output_endpoint_schema =
                nested_output_endpoint_schema_for(meta.output_schema, spec.output_binding->target_path);
        }
        meta.node_kind = NodeKind::Nested;

        NodeTypeDescriptor descriptor;
        descriptor.schema = std::move(meta);

        const MemoryUtils::StorageLayout child_graph_layout = spec.graph_builder.nested_storage_layout();
        const auto &child_graph_memory_plan = MemoryUtils::raw_storage_plan(child_graph_layout);
        const std::array fields{
            NodeStorageField{
                .name = child_graph_memory_field_name,
                .plan = &child_graph_memory_plan,
            },
            NodeStorageField{
                .name = child_graph_field_name,
                .plan = &MemoryUtils::plan_for<SingleNestedGraphNodeStorage>(),
            },
        };
        descriptor.storage_plan = &node_storage_plan_for(descriptor.schema, fields);
        descriptor.debug_fields.push_back(DebugField{
            .name = "child_graph",
            .offset = descriptor.storage_plan->component(child_graph_field_name).offset +
                      offsetof(SingleNestedGraphNodeStorage, graph) + GraphValue::debug_pointer_offset(),
            .type = spec.graph_builder.nested_type().record(),
            .flags = DebugFieldFlags::EmbeddedPointer,
        });

        descriptor.callbacks.start = &single_nested_graph_start;
        descriptor.callbacks.stop  = &single_nested_graph_stop;
        descriptor.ops.evaluate_impl = &single_nested_graph_evaluate_impl;
        descriptor.ops.storage_metrics_impl = &single_nested_storage_metrics;
        descriptor.ops.extended_view_type_id = SingleNestedGraphNodeView::node_view_type_id();
        const auto *context = register_single_nested_graph_context(
            std::move(spec),
            options,
            descriptor.storage_plan->component(child_graph_field_name).offset,
            descriptor.storage_plan->component(child_graph_memory_field_name).offset,
            child_graph_layout);
        descriptor.ops.extended_view_context = context;
        descriptor.ops.child_graph_inspection = ChildGraphInspectionOps{
            .context = context,
            .visit_impl = &visit_single_nested_child,
        };

        return descriptor;
    }

    NodeBuilder single_nested_graph_node(
        NodeTypeMetaData meta,
        SingleNestedGraphNodeSpec spec,
        SingleNestedGraphNodeOptions options)
    {
        return NodeBuilder::from_descriptor(
            single_nested_graph_node_descriptor(std::move(meta), std::move(spec), options));
    }

    void single_nested_graph_start(const NodeView &view, DateTime evaluation_time)
    {
        auto nested = checked_nested_view(view);
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

    void single_nested_graph_stop(const NodeView &view, DateTime)
    {
        auto nested = checked_nested_view(view);
        // The forwarding link is intentionally left intact: stop() only halts
        // evaluation; the child GraphValue (and thus the forwarded output) lives in
        // this node's storage and is torn down together with the parent output, so
        // the forwarded value stays observable after a run. ``switch_``-style
        // wrappers that swap the active child use single_nested_graph_clear_output_binding.
        if (nested.context().options.stop_child_on_stop && nested.child_graph_value().has_value())
        {
            nested.child_graph().stop();
        }
    }

    bool single_nested_graph_evaluate(const NodeView &view, DateTime evaluation_time)
    {
        if (!view.started()) { return true; }

        auto nested = checked_nested_view(view);
        nested.ensure_child_graph();
        // Re-bind boundaries each cycle: an upstream REF/forwarding output may have
        // re-pointed since the last evaluation. The bind_* helpers skip the rebind
        // when the endpoint already references the same output, so this is cheap when
        // the wiring is stable (and idempotent on a pause/resume re-entry).
        single_nested_graph_bind_inputs(nested, evaluation_time);
        single_nested_graph_bind_output(nested, evaluation_time);
        return nested.child_graph().evaluate(evaluation_time);
    }

    void single_nested_graph_bind_inputs(const SingleNestedGraphNodeView &nested,
                                         DateTime evaluation_time)
    {
        const auto &bindings = nested.context().spec.input_bindings;
        if (bindings.empty()) { return; }

        auto root_input = nested.node().input(evaluation_time);
        auto child_graph = nested.child_graph();

        for (const NestedGraphInputBinding &binding : bindings)
        {
            auto source_output = walk_source_to_output(root_input.borrowed_ref(), binding.source_path);

            auto target = walk_ts_path(
                child_graph.node_at(binding.target.node).input(evaluation_time),
                binding.target.path);
            bind_input_to_source(std::move(target), source_output);
        }
    }

    void single_nested_graph_bind_output(const SingleNestedGraphNodeView &nested,
                                         DateTime evaluation_time)
    {
        const auto &binding = nested.context().spec.output_binding;
        if (!binding.has_value()) { return; }

        auto target = walk_forwarding_target_path(nested.node().output(evaluation_time), binding->target_path);

        if (binding->kind == NestedGraphOutputBinding::Kind::ParentInput)
        {
            // Pass-through (alias_parent_input): forward whatever upstream output
            // this node's own input is bound to. Re-resolved each cycle like the
            // input bindings; cleared while the upstream is unbound.
            auto root_input = nested.node().input(evaluation_time);
            auto source     = walk_ts_path(root_input.borrowed_ref(), binding->parent_source_path).bound_output();
            if (!source.bound())
            {
                static_cast<void>(clear_forwarding_output_tree(
                    std::move(target)));
                return;
            }
            static_cast<void>(bind_forwarding_output_tree_to_source(
                std::move(target), source));
            return;
        }

        auto source = walk_ts_path(
            nested.child_graph().node_at(binding->source.node).output(evaluation_time),
            binding->source.path);
        static_cast<void>(bind_forwarding_output_tree_to_source(
            std::move(target), source));
    }

    void single_nested_graph_clear_output_binding(const SingleNestedGraphNodeView &nested,
                                                  DateTime evaluation_time)
    {
        const auto &binding = nested.context().spec.output_binding;
        if (!binding.has_value()) { return; }

        auto target = walk_forwarding_target_path(nested.node().output(evaluation_time), binding->target_path);
        static_cast<void>(clear_forwarding_output_tree(std::move(target)));
    }

    void single_nested_graph_propagate_schedule(const SingleNestedGraphNodeView &nested)
    {
        if (!nested.context().options.propagate_child_schedule || !nested.child_graph_value().has_value()) { return; }

        const DateTime next = nested.child_graph().next_scheduled_time();
        if (next != MAX_DT)
        {
            nested.node().graph().schedule_node(nested.node().node_index(), next);
        }
    }
}  // namespace hgraph
