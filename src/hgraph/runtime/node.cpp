#include <hgraph/runtime/node.h>

#include "registry_snapshot_detail.h"

#include <hgraph/runtime/executor.h>
#include <hgraph/runtime/graph.h>
#include <hgraph/runtime/node_error.h>
#include <hgraph/types/metadata/debug_descriptor.h>
#include <hgraph/types/metadata/type_record_registry.h>
#include <hgraph/types/metadata/value_plan_factory.h>
#include <hgraph/types/notifiable.h>
#include <hgraph/runtime/node_scheduler.h>
#include <hgraph/util/scope.h>

#include <algorithm>
#include <array>
#include <cassert>
#include <deque>
#include <exception>
#include <memory>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <utility>

namespace hgraph
{
    namespace
    {
        void schedule_node_from_storage(GraphValue *graph, std::size_t node_index, DateTime modified_time);

        struct NodeRuntimeStorage final : Notifiable
        {
            NodeRuntimeStorage(const NodeTypeMetaData &schema, std::string runtime_label)
                : label(std::move(runtime_label))
            {
                if (label.empty() && schema.display_name != nullptr) { label = schema.display_name; }
            }

            void notify(DateTime modified_time) override
            {
                schedule_node_from_storage(graph, node_index, modified_time);
            }

            GraphValue   *graph{nullptr};
            std::size_t   node_index{0};
            std::string   label{};
            bool          started{false};
            bool          starting{false};
        };

        [[nodiscard]] std::size_t node_runtime_graph_offset(const NodeTypeMetaData &schema)
        {
            const NodeRuntimeStorage sample{schema, {}};
            return static_cast<std::size_t>(
                reinterpret_cast<const std::byte *>(&sample.graph) -
                reinterpret_cast<const std::byte *>(&sample));
        }

        void schedule_node_from_storage(GraphValue *graph, std::size_t node_index, DateTime modified_time)
        {
            if (graph == nullptr) { return; }
            const DateTime when =
                modified_time != MIN_DT ? std::max(modified_time, graph->view().evaluation_time()) : graph->view().evaluation_time();
            graph->schedule_node(node_index, when);
        }

        struct NodeRuntimeLayout
        {
            static constexpr std::size_t npos = static_cast<std::size_t>(-1);

            std::size_t storage_offset{0};
            std::size_t input_offset{npos};
            std::size_t output_offset{npos};
            std::size_t state_offset{npos};
            std::size_t scalars_offset{npos};
            std::size_t scheduler_offset{npos};
            std::size_t global_state_offset{npos};
            std::size_t evaluation_clock_offset{npos};
            std::size_t error_output_offset{npos};
            std::size_t recordable_state_offset{npos};
            std::size_t prepared_inputs_offset{npos};

            [[nodiscard]] bool has_input() const noexcept { return input_offset != npos; }
            [[nodiscard]] bool has_output() const noexcept { return output_offset != npos; }
            [[nodiscard]] bool has_state() const noexcept { return state_offset != npos; }
            [[nodiscard]] bool has_scalars() const noexcept { return scalars_offset != npos; }
            [[nodiscard]] bool has_scheduler() const noexcept { return scheduler_offset != npos; }
            [[nodiscard]] bool has_global_state() const noexcept { return global_state_offset != npos; }
            [[nodiscard]] bool has_evaluation_clock() const noexcept { return evaluation_clock_offset != npos; }
            [[nodiscard]] bool has_error_output() const noexcept { return error_output_offset != npos; }
            [[nodiscard]] bool has_recordable_state() const noexcept { return recordable_state_offset != npos; }
        };

        struct NodeRuntimeContext
        {
            NodeCallbacks                    callbacks{};
            NodeRuntimeLayout                layout{};
            const MemoryUtils::StoragePlan  *plan{nullptr};
            const void *runtime_type_id{nullptr};
        };

        [[nodiscard]] const NodeRuntimeContext &runtime_context(const void *context)
        {
            if (context == nullptr) { throw std::logic_error("Node runtime context is null"); }
            return *static_cast<const NodeRuntimeContext *>(context);
        }

        [[nodiscard]] void *node_component(void *memory, std::size_t offset)
        {
            if (memory == nullptr) { throw std::logic_error("Node storage is null"); }
            if (offset == NodeRuntimeLayout::npos) { throw std::logic_error("Node component is not present"); }
            return MemoryUtils::advance(memory, offset);
        }

        [[nodiscard]] const void *node_component(const void *memory, std::size_t offset)
        {
            if (memory == nullptr) { throw std::logic_error("Node storage is null"); }
            if (offset == NodeRuntimeLayout::npos) { throw std::logic_error("Node component is not present"); }
            return MemoryUtils::advance(memory, offset);
        }

        [[nodiscard]] NodeRuntimeStorage &node_storage(const NodeRuntimeContext &context, void *memory)
        {
            return *MemoryUtils::cast<NodeRuntimeStorage>(node_component(memory, context.layout.storage_offset));
        }

        [[nodiscard]] const NodeRuntimeStorage &node_storage(const NodeRuntimeContext &context, const void *memory)
        {
            return *MemoryUtils::cast<NodeRuntimeStorage>(node_component(memory, context.layout.storage_offset));
        }

        [[nodiscard]] TSInput &node_input(const NodeRuntimeContext &context, void *memory)
        {
            return *MemoryUtils::cast<TSInput>(node_component(memory, context.layout.input_offset));
        }

        [[nodiscard]] TSOutput &node_output(const NodeRuntimeContext &context, void *memory)
        {
            return *MemoryUtils::cast<TSOutput>(node_component(memory, context.layout.output_offset));
        }

        [[nodiscard]] Value &node_state(const NodeRuntimeContext &context, void *memory)
        {
            return *MemoryUtils::cast<Value>(node_component(memory, context.layout.state_offset));
        }

        [[nodiscard]] Value &node_scalars(const NodeRuntimeContext &context, void *memory)
        {
            return *MemoryUtils::cast<Value>(node_component(memory, context.layout.scalars_offset));
        }

        [[nodiscard]] NodeSchedulerState &node_scheduler_state(const NodeRuntimeContext &context, void *memory)
        {
            return *MemoryUtils::cast<NodeSchedulerState>(node_component(memory, context.layout.scheduler_offset));
        }

        [[nodiscard]] std::optional<GlobalStateView> &node_global_state_view(const NodeRuntimeContext &context,
                                                                             void *memory)
        {
            return *MemoryUtils::cast<std::optional<GlobalStateView>>(
                node_component(memory, context.layout.global_state_offset));
        }

        [[nodiscard]] ClockPtr &node_evaluation_clock_ptr(const NodeRuntimeContext &context,
                                                          void *memory)
        {
            return *MemoryUtils::cast<ClockPtr>(
                node_component(memory, context.layout.evaluation_clock_offset));
        }

        [[nodiscard]] TSOutput &node_error_output(const NodeRuntimeContext &context, void *memory)
        {
            return *MemoryUtils::cast<TSOutput>(node_component(memory, context.layout.error_output_offset));
        }

        [[nodiscard]] TSOutput &node_recordable_state(const NodeRuntimeContext &context, void *memory)
        {
            return *MemoryUtils::cast<TSOutput>(node_component(memory, context.layout.recordable_state_offset));
        }

        void bind_endpoint_owners(const NodeRuntimeContext &context,
                                  void                     *memory,
                                  GraphValue               *graph,
                                  std::size_t               node_index)
        {
            NodeView node = graph != nullptr ? graph->view().node_at(node_index) : NodeView{};
            if (context.layout.has_input())
            {
                auto &input = node_input(context, memory);
                if (graph != nullptr)
                {
                    input.bind_node_parent(node, TSEndpointOwnerPort::Input);
                }
                else
                {
                    input.clear_node_parent();
                }
            }
            if (context.layout.has_output())
            {
                auto &output = node_output(context, memory);
                if (graph != nullptr)
                {
                    output.bind_node_parent(node, TSEndpointOwnerPort::Output);
                }
                else
                {
                    output.clear_node_parent();
                }
            }
            if (context.layout.has_error_output())
            {
                auto &output = node_error_output(context, memory);
                if (graph != nullptr)
                {
                    output.bind_node_parent(node, TSEndpointOwnerPort::ErrorOutput);
                }
                else
                {
                    output.clear_node_parent();
                }
            }
            if (context.layout.has_recordable_state())
            {
                auto &output = node_recordable_state(context, memory);
                if (graph != nullptr)
                {
                    output.bind_node_parent(node, TSEndpointOwnerPort::RecordableState);
                }
                else
                {
                    output.clear_node_parent();
                }
            }
        }

        // Build a NodeError from the node's identity + the exception message and
        // write it to the node's error output for this cycle. Error capture is not
        // transactional: a node may already have written ordinary output before
        // throwing, so callers must treat that output as unspecified.
        void write_node_error(const NodeRuntimeContext &context, const NodeView &view, DateTime evaluation_time,
                              std::string error_msg)
        {
            const NodeTypeMetaData *schema = view.schema();
            const ErrorCaptureOptions options = schema != nullptr ? schema->error_capture : ErrorCaptureOptions{};
            NodeErrorFields fields = capture_node_error(view, evaluation_time, std::move(error_msg), options);

            Value error_value = make_node_error_value(fields);
            auto  output      = node_error_output(context, view.data()).view(evaluation_time);
            auto  mutation    = output.begin_mutation(evaluation_time);
            (void)mutation.move_value_from(std::move(error_value));
        }

        [[nodiscard]] const NodeCallbacks &callbacks(const void *context)
        {
            return runtime_context(context).callbacks;
        }

        [[nodiscard]] TSEndpointSchema default_input_endpoint(const TSValueTypeMetaData &schema)
        {
            if (schema.kind != TSTypeKind::TSB)
            {
                return TSEndpointSchema::peered(&schema);
            }

            std::vector<TSEndpointSchema> children;
            children.reserve(schema.field_count());
            for (std::size_t index = 0; index < schema.field_count(); ++index)
            {
                children.push_back(TSEndpointSchema::peered(schema.fields()[index].type));
            }
            return TSEndpointSchema::non_peered(&schema, std::move(children));
        }

        [[nodiscard]] const TSInputBuilder &input_builder_for(const TSValueTypeMetaData &schema,
                                                              TSEndpointSchema endpoint)
        {
            if (endpoint.empty()) { endpoint = default_input_endpoint(schema); }
            return TSInputBuilderFactory::checked_builder_for(schema, endpoint);
        }

        [[nodiscard]] ValueTypeRef state_binding_for(const ValueTypeMetaData *schema)
        {
            if (schema == nullptr) { throw std::logic_error("Node state schema is null"); }
            const auto binding = ValuePlanFactory::instance().type_for(schema);
            if (!binding)
            {
                throw std::logic_error("Node state schema has no value binding");
            }
            return binding;
        }

        void destroy_constructed_components(
            const std::vector<const MemoryUtils::CompositeComponent *> &constructed,
            void *memory) noexcept
        {
            for (std::size_t index = constructed.size(); index > 0; --index)
            {
                const auto &component = *constructed[index - 1];
                component.plan->destroy(MemoryUtils::advance(memory, component.offset));
            }
        }

        void construct_node_storage_impl(const NodeRuntimeContext &context,
                                         const NodeTypeMetaData   &schema,
                                         TSEndpointSchema          input_endpoint,
                                         TSEndpointSchema          output_endpoint_override,
                                         ValueStorageVariant       output_value_storage,
                                         std::string               runtime_label,
                                         const Value              &scalars,
                                         void                     *memory)
        {
            if (context.plan == nullptr) { throw std::logic_error("Node runtime context has no storage plan"); }
            const MemoryUtils::StoragePlan &plan = *context.plan;

            std::vector<const MemoryUtils::CompositeComponent *> constructed;
            constructed.reserve(9);
            auto rollback = make_scope_exit([&]() noexcept {
                destroy_constructed_components(constructed, memory);
            });

            const auto *runtime_storage_component = plan.find_component("runtime_storage");
            if (runtime_storage_component == nullptr)
            {
                throw std::logic_error("Node storage plan is missing runtime_storage");
            }
            std::construct_at(MemoryUtils::cast<NodeRuntimeStorage>(
                                  MemoryUtils::advance(memory, runtime_storage_component->offset)),
                              schema, std::move(runtime_label));
            constructed.push_back(runtime_storage_component);

            if (context.layout.has_input())
            {
                const auto *component = plan.find_component("input");
                if (component == nullptr) { throw std::logic_error("Node storage plan is missing input"); }
                std::construct_at(MemoryUtils::cast<TSInput>(
                                      MemoryUtils::advance(memory, component->offset)),
                                  input_builder_for(*schema.input_schema, std::move(input_endpoint)));
                constructed.push_back(component);
            }

            if (context.layout.has_output())
            {
                const auto *component = plan.find_component("output");
                if (component == nullptr) { throw std::logic_error("Node storage plan is missing output"); }
                const TSEndpointSchema &output_endpoint =
                    !output_endpoint_override.empty() ? output_endpoint_override : schema.output_endpoint_schema;
                if (output_endpoint.empty())
                {
                    std::construct_at(MemoryUtils::cast<TSOutput>(
                                          MemoryUtils::advance(memory, component->offset)),
                                      *schema.output_schema,
                                      output_value_storage);
                }
                else
                {
                    std::construct_at(MemoryUtils::cast<TSOutput>(
                                          MemoryUtils::advance(memory, component->offset)),
                                      output_endpoint);
                }
                constructed.push_back(component);
            }

            if (context.layout.has_state())
            {
                const auto *component = plan.find_component("state");
                if (component == nullptr) { throw std::logic_error("Node storage plan is missing state"); }
                std::construct_at(MemoryUtils::cast<Value>(
                                      MemoryUtils::advance(memory, component->offset)),
                                  state_binding_for(schema.state_schema));
                constructed.push_back(component);
            }

            if (context.layout.has_scalars())
            {
                const auto *component = plan.find_component("scalars");
                if (component == nullptr) { throw std::logic_error("Node storage plan is missing scalars"); }
                if (!scalars.has_value())
                {
                    throw std::logic_error("Node has a scalar schema but no scalar configuration value was provided");
                }
                std::construct_at(MemoryUtils::cast<Value>(
                                      MemoryUtils::advance(memory, component->offset)),
                                  scalars);   // copy the per-instance scalar configuration
                constructed.push_back(component);
            }

            if (context.layout.has_scheduler())
            {
                const auto *component = plan.find_component("scheduler");
                if (component == nullptr) { throw std::logic_error("Node storage plan is missing scheduler"); }
                std::construct_at(MemoryUtils::cast<NodeSchedulerState>(
                                      MemoryUtils::advance(memory, component->offset)));
                constructed.push_back(component);
            }

            if (context.layout.has_global_state())
            {
                const auto *component = plan.find_component("global_state");
                if (component == nullptr) { throw std::logic_error("Node storage plan is missing global_state"); }
                std::construct_at(MemoryUtils::cast<std::optional<GlobalStateView>>(
                                      MemoryUtils::advance(memory, component->offset)));
                constructed.push_back(component);
            }

            if (context.layout.has_evaluation_clock())
            {
                const auto *component = plan.find_component("evaluation_clock");
                if (component == nullptr) { throw std::logic_error("Node storage plan is missing evaluation_clock"); }
                std::construct_at(MemoryUtils::cast<ClockPtr>(
                                      MemoryUtils::advance(memory, component->offset)));
                constructed.push_back(component);
            }

            if (context.layout.has_error_output())
            {
                const auto *component = plan.find_component("error_output");
                if (component == nullptr) { throw std::logic_error("Node storage plan is missing error_output"); }
                std::construct_at(MemoryUtils::cast<TSOutput>(
                                      MemoryUtils::advance(memory, component->offset)),
                                  *schema.error_output_schema);
                constructed.push_back(component);
            }

            if (context.layout.has_recordable_state())
            {
                const auto *component = plan.find_component("recordable_state");
                if (component == nullptr) { throw std::logic_error("Node storage plan is missing recordable_state"); }
                std::construct_at(MemoryUtils::cast<TSOutput>(
                                      MemoryUtils::advance(memory, component->offset)),
                                  *schema.recordable_state_schema);
                constructed.push_back(component);
            }

            for (const MemoryUtils::CompositeComponent &component : plan.components())
            {
                const auto constructed_it = std::find(constructed.begin(), constructed.end(), &component);
                if (constructed_it != constructed.end()) { continue; }
                if (component.plan == nullptr)
                {
                    throw std::logic_error("Node storage plan component is missing a child plan");
                }
                component.plan->default_construct(MemoryUtils::advance(memory, component.offset));
                constructed.push_back(&component);
            }

            rollback.release();
        }

        [[nodiscard]] NodeRuntimeLayout layout_for(const MemoryUtils::StoragePlan &plan)
        {
            NodeRuntimeLayout layout;
            layout.storage_offset = plan.component("runtime_storage").offset;

            // Optional components: recorded only when the schema declares them.
            const std::pair<const char *, std::size_t NodeRuntimeLayout::*> optional_components[]{
                {"input", &NodeRuntimeLayout::input_offset},
                {"output", &NodeRuntimeLayout::output_offset},
                {"state", &NodeRuntimeLayout::state_offset},
                {"scalars", &NodeRuntimeLayout::scalars_offset},
                {"scheduler", &NodeRuntimeLayout::scheduler_offset},
                {"global_state", &NodeRuntimeLayout::global_state_offset},
                {"evaluation_clock", &NodeRuntimeLayout::evaluation_clock_offset},
                {"error_output", &NodeRuntimeLayout::error_output_offset},
                {"recordable_state", &NodeRuntimeLayout::recordable_state_offset},
                {node_prepared_inputs_field.data(), &NodeRuntimeLayout::prepared_inputs_offset},
            };
            for (const auto &[name, member] : optional_components)
            {
                if (const auto *component = plan.find_component(name); component != nullptr)
                {
                    layout.*member = component->offset;
                }
            }
            return layout;
        }

        void activate_input_slots(const NodeView &view, DateTime evaluation_time)
        {
            if (!view.has_input()) { return; }

            const auto *schema = view.schema()->input_schema;
            auto        input  = view.input(evaluation_time);
            if (schema == nullptr || schema->kind != TSTypeKind::TSB)
            {
                if (!view.schema()->structural_inputs.empty()) { input.make_structural_active(); }
                else { input.make_active(); }
                return;
            }

            auto bundle = input.as_bundle();
            const auto &slots = view.schema()->active_inputs;
            if (!slots.has_value())
            {
                for (std::size_t slot = 0; slot < schema->field_count(); ++slot) { bundle[slot].make_active(); }
                return;
            }

            for (const std::size_t slot : *slots)
            {
                if (slot >= schema->field_count()) { throw std::out_of_range("Node active input selector is out of range"); }
                bundle[slot].make_active();
            }
            for (const std::size_t slot : view.schema()->structural_inputs)
            {
                if (slot >= schema->field_count())
                {
                    throw std::out_of_range("Node structural input selector is out of range");
                }
                bundle[slot].make_structural_active();
            }
        }

        void deactivate_input_slots(const NodeView &view, DateTime evaluation_time)
        {
            if (!view.has_input()) { return; }

            const auto *schema = view.schema()->input_schema;
            auto        input  = view.input(evaluation_time);
            if (schema == nullptr || schema->kind != TSTypeKind::TSB)
            {
                input.make_passive();
                return;
            }

            auto bundle = input.as_bundle();
            const auto &slots = view.schema()->active_inputs;
            if (!slots.has_value())
            {
                for (std::size_t slot = 0; slot < schema->field_count(); ++slot) { bundle[slot].make_passive(); }
                return;
            }

            for (const std::size_t slot : *slots)
            {
                if (slot >= schema->field_count()) { throw std::out_of_range("Node active input selector is out of range"); }
                bundle[slot].make_passive();
            }
            for (const std::size_t slot : view.schema()->structural_inputs)
            {
                if (slot >= schema->field_count())
                {
                    throw std::out_of_range("Node structural input selector is out of range");
                }
                bundle[slot].make_passive();
            }
        }

        [[nodiscard]] bool ready_to_evaluate(const NodeView &view, DateTime evaluation_time)
        {
            const auto *node_schema = view.schema();
            if (node_schema == nullptr || !node_schema->has_input()) { return true; }

            const auto *schema = node_schema->input_schema;
            auto input = view.input(evaluation_time);
            if (schema == nullptr || schema->kind != TSTypeKind::TSB)
            {
                if (node_schema->valid_inputs.has_value() ||
                    !node_schema->all_valid_inputs.empty())
                {
                    throw std::logic_error(
                        "Node input selectors require a TSB root input schema");
                }
                return input.valid();
            }

            // Prepared routes (RFC 0008 stage 5): the readiness gate is a
            // per-tick projection consumer too — when the node planned a
            // route array (sized to the TSB field count), gate checks
            // rebuild slot views from the cache.
            const auto *routes = static_cast<const detail::PreparedInputSlotRoute *>(
                view.prepared_input_routes());
            const auto slot_view = [&](std::size_t slot) {
                if (routes != nullptr && routes[slot].ready())
                {
                    return input.child_from_prepared(routes[slot]);
                }
                return input.indexed_child_at(slot);
            };
            const auto checked_slot = [&](std::size_t slot) {
                if (slot >= schema->field_count())
                {
                    throw std::out_of_range(
                        "Node input selector is out of range");
                }
                return slot_view(slot);
            };

            const auto &valid_slots = node_schema->valid_inputs;
            if (valid_slots.has_value())
            {
                for (const std::size_t slot : *valid_slots)
                {
                    if (!checked_slot(slot).valid()) { return false; }
                }
            }
            else
            {
                for (std::size_t slot = 0; slot < schema->field_count(); ++slot)
                {
                    if (!slot_view(slot).valid()) { return false; }
                }
            }

            for (const std::size_t slot : node_schema->all_valid_inputs)
            {
                if (!checked_slot(slot).all_valid()) { return false; }
            }

            return true;
        }

        void attach_graph_impl(const void *context, void *memory, GraphValue *graph, std::size_t node_index)
        {
            const auto &runtime = runtime_context(context);
            auto       &state   = node_storage(runtime, memory);
            state.graph = graph;
            state.node_index = node_index;
            bind_endpoint_owners(runtime, memory, graph, node_index);
        }

        GraphValue *graph_impl(const void *context, const void *memory) noexcept
        {
            return node_storage(runtime_context(context), memory).graph;
        }

        std::size_t node_index_impl(const void *context, const void *memory) noexcept
        {
            return node_storage(runtime_context(context), memory).node_index;
        }

        std::string_view label_impl(const void *context, const void *memory) noexcept
        {
            return node_storage(runtime_context(context), memory).label;
        }

        bool started_impl(const void *context, const void *memory) noexcept
        {
            return node_storage(runtime_context(context), memory).started;
        }

        bool has_input_impl(const void *context, const void *memory) noexcept
        {
            static_cast<void>(memory);
            return runtime_context(context).layout.has_input();
        }

        bool has_output_impl(const void *context, const void *memory) noexcept
        {
            static_cast<void>(memory);
            return runtime_context(context).layout.has_output();
        }

        bool has_state_impl(const void *context, const void *memory) noexcept
        {
            static_cast<void>(memory);
            return runtime_context(context).layout.has_state();
        }

        bool has_scalars_impl(const void *context, const void *memory) noexcept
        {
            static_cast<void>(memory);
            return runtime_context(context).layout.has_scalars();
        }

        bool has_scheduler_impl(const void *context, const void *memory) noexcept
        {
            static_cast<void>(memory);
            return runtime_context(context).layout.has_scheduler();
        }

        bool has_error_output_impl(const void *context, const void *memory) noexcept
        {
            static_cast<void>(memory);
            return runtime_context(context).layout.has_error_output();
        }

        bool has_recordable_state_impl(const void *context, const void *memory) noexcept
        {
            static_cast<void>(memory);
            return runtime_context(context).layout.has_recordable_state();
        }

        TSInputView input_view_impl(const void *context, void *memory, DateTime evaluation_time)
        {
            const auto &runtime = runtime_context(context);
            if (!runtime.layout.has_input()) { throw std::logic_error("Node has no input"); }
            return node_input(runtime, memory).view(&node_storage(runtime, memory), evaluation_time);
        }

        TSOutputView output_view_impl(const void *context, void *memory, DateTime evaluation_time)
        {
            const auto &runtime = runtime_context(context);
            if (!runtime.layout.has_output()) { throw std::logic_error("Node has no output"); }
            return node_output(runtime, memory).view(evaluation_time);
        }

        ValueView state_view_impl(const void *context, void *memory)
        {
            const auto &runtime = runtime_context(context);
            if (!runtime.layout.has_state()) { throw std::logic_error("Node has no state"); }
            return node_state(runtime, memory).view();
        }

        void replace_state_impl(const void *context, void *memory, Value value)
        {
            const auto &runtime = runtime_context(context);
            if (!runtime.layout.has_state()) { throw std::logic_error("Node has no state"); }
            if (!value.has_value()) { throw std::invalid_argument("Node state replacement requires a value"); }

            Value &state = node_state(runtime, memory);
            if (state.schema() != value.schema())
            {
                throw std::invalid_argument("Node state replacement schema does not match the node state schema");
            }
            state = std::move(value);
        }

        ValueView scalars_view_impl(const void *context, void *memory)
        {
            const auto &runtime = runtime_context(context);
            if (!runtime.layout.has_scalars()) { throw std::logic_error("Node has no scalar configuration"); }
            return node_scalars(runtime, memory).view();
        }

        NodeSchedulerState *scheduler_state_impl(const void *context, void *memory)
        {
            const auto &runtime = runtime_context(context);
            if (!runtime.layout.has_scheduler()) { throw std::logic_error("Node has no scheduler"); }
            return &node_scheduler_state(runtime, memory);
        }

        GlobalStateView global_state_view_impl(const void *context, void *memory)
        {
            const auto &runtime = runtime_context(context);
            auto       &state   = node_storage(runtime, memory);
            if (!runtime.layout.has_global_state())
            {
                if (state.graph == nullptr)
                {
                    throw std::logic_error("Node global state requires an attached graph");
                }
                return state.graph->view().root().global_state();
            }

            auto &cached = node_global_state_view(runtime, memory);
            if (!cached.has_value())
            {
                if (state.graph == nullptr)
                {
                    throw std::logic_error("Node global state cache requires an attached graph");
                }
                cached.emplace(state.graph->view().root().global_state());
            }
            return *cached;
        }

        ClockPtr evaluation_clock_ptr_impl(const void *context, void *memory)
        {
            const auto &runtime = runtime_context(context);
            auto       &state   = node_storage(runtime, memory);
            if (!runtime.layout.has_evaluation_clock())
            {
                if (state.graph == nullptr)
                {
                    throw std::logic_error("Node evaluation clock requires an attached graph");
                }
                return state.graph->view().executor().evaluation_clock_ptr();
            }

            auto &cached = node_evaluation_clock_ptr(runtime, memory);
            if (!cached.has_value())
            {
                if (state.graph == nullptr)
                {
                    throw std::logic_error("Node evaluation clock cache requires an attached graph");
                }
                cached = state.graph->view().executor().evaluation_clock_ptr();
            }
            return cached;
        }

        TSOutputView error_output_view_impl(const void *context, void *memory, DateTime evaluation_time)
        {
            const auto &runtime = runtime_context(context);
            if (!runtime.layout.has_error_output()) { throw std::logic_error("Node has no error output"); }
            return node_error_output(runtime, memory).view(evaluation_time);
        }

        TSOutputView recordable_state_view_impl(const void *context, void *memory, DateTime evaluation_time)
        {
            const auto &runtime = runtime_context(context);
            if (!runtime.layout.has_recordable_state()) { throw std::logic_error("Node has no recordable state output"); }
            return node_recordable_state(runtime, memory).view(evaluation_time);
        }

        void start_impl(const void *context, const NodeView &view, DateTime evaluation_time)
        {
            const auto &runtime = runtime_context(context);
            auto &state = node_storage(runtime, view.data());
            if (state.started) { return; }

            std::size_t activated = 0;
            auto rollback = UnwindCleanupGuard([&] {
                static_cast<void>(activated);
                deactivate_input_slots(view, evaluation_time);
            });

            state.starting = true;
            auto clear_starting = make_scope_exit([&] noexcept { state.starting = false; });
            activate_input_slots(view, evaluation_time);
            activated = 1;

            if (callbacks(context).start) { callbacks(context).start(view, evaluation_time); }
            state.started = true;
            rollback.release();

            // Declarative self-scheduling: a node with ``schedule_on_start`` is
            // marked to evaluate in the current cycle (the framework equivalent of
            // doing schedule_now() in a start hook). Done after the user start so
            // the node is fully started before it can be evaluated.
            if (view.schema() != nullptr && view.schema()->schedule_on_start && state.graph != nullptr)
            {
                state.graph->schedule_node(state.node_index, evaluation_time);
            }
        }

        void stop_impl(const void *context, const NodeView &view, DateTime evaluation_time)
        {
            const auto &runtime = runtime_context(context);
            auto &state = node_storage(runtime, view.data());
            if (!state.started) { return; }

            auto mark_stopped = make_scope_exit([&] noexcept { state.started = false; });
            auto deactivate = UnwindCleanupGuard([&] { deactivate_input_slots(view, evaluation_time); });
            if (callbacks(context).stop) { callbacks(context).stop(view, evaluation_time); }
            deactivate.complete();
        }

        bool evaluate_impl(const void *context, const NodeView &view, DateTime evaluation_time)
        {
            if (!view.started()) { return true; }

            // Graph scheduling is the activation gate. Node eval only enforces
            // lifecycle/validity policy, then lets node-specific code decide any
            // additional guards. This mirrors Python's NodeImpl.eval: active inputs
            // schedule the node by notification; eval does not re-poll modified
            // flags.
            const auto         &runtime      = runtime_context(context);
            const bool          has_scheduler = runtime.layout.has_scheduler();
            NodeSchedulerState *scheduler     = has_scheduler ? &node_scheduler_state(runtime, view.data()) : nullptr;
            const bool          scheduled_now = scheduler != nullptr && !scheduler->events.empty() &&
                                       scheduler->events.begin()->first == evaluation_time;

            bool do_eval = ready_to_evaluate(view, evaluation_time);

            if (do_eval)
            {
                if (callbacks(context).evaluate)
                {
                    const NodeTypeMetaData *schema = view.schema();
                    const bool capture = schema != nullptr && schema->captures_errors && runtime.layout.has_error_output();
                    if (capture)
                    {
                        static_cast<void>(fallback_on_exception(false,
                                                               [&] {
                                                                   callbacks(context).evaluate(view, evaluation_time);
                                                                   return true;
                                                               },
                                                               [&](const char *error) {
                                                                   write_node_error(runtime, view, evaluation_time, error);
                                                               }));
                    }
                    else { callbacks(context).evaluate(view, evaluation_time); }
                }
            }

            if (has_scheduler)
            {
                auto         &graph = *view.graph_value();
                NodeScheduler sched{*scheduler, &graph, view.node_index(), evaluation_time};
                if (scheduled_now)
                {
                    sched.advance();  // consume the fired event(s) and re-arm the next
                }
                else if (sched.is_scheduled())
                {
                    // Ran for another reason (an input ticked): just re-arm the timer.
                    graph.schedule_node(view.node_index(), sched.next_scheduled_time());
                }
            }
            return true;
        }

        struct NodeRuntimeRegistry
        {
          [[nodiscard]] static bool
          endpoint_schema_equivalent(const TSEndpointSchema &lhs,
                                     const TSEndpointSchema &rhs) noexcept {
            if (lhs.empty() || rhs.empty()) {
              return lhs.empty() == rhs.empty();
            }
            if (lhs.role() != rhs.role() || lhs.schema() != rhs.schema() ||
                lhs.child_count() != rhs.child_count()) {
              return false;
            }
            for (std::size_t index = 0; index < lhs.child_count(); ++index) {
              if (!endpoint_schema_equivalent(lhs.child(index),
                                              rhs.child(index))) {
                return false;
              }
            }
            return true;
          }

          [[nodiscard]] static bool
          schema_equivalent(const NodeTypeMetaData &lhs,
                            const NodeTypeMetaData &rhs) noexcept {
            const std::string_view lhs_name =
                lhs.display_name != nullptr ? lhs.display_name : "";
            const std::string_view rhs_name =
                rhs.display_name != nullptr ? rhs.display_name : "";
            return lhs_name == rhs_name &&
                   lhs.input_schema == rhs.input_schema &&
                   lhs.output_schema == rhs.output_schema &&
                   endpoint_schema_equivalent(lhs.output_endpoint_schema,
                                              rhs.output_endpoint_schema) &&
                   lhs.error_output_schema == rhs.error_output_schema &&
                   lhs.recordable_state_schema == rhs.recordable_state_schema &&
                   lhs.state_schema == rhs.state_schema &&
                   lhs.scalar_schema == rhs.scalar_schema &&
                   lhs.node_kind == rhs.node_kind &&
                   lhs.uses_scheduler == rhs.uses_scheduler &&
                   lhs.uses_global_state == rhs.uses_global_state &&
                   lhs.uses_evaluation_clock == rhs.uses_evaluation_clock &&
                   lhs.uses_python_values == rhs.uses_python_values &&
                   lhs.requires_phase_runner == rhs.requires_phase_runner &&
                   lhs.schedule_on_start == rhs.schedule_on_start &&
                   lhs.captures_errors == rhs.captures_errors &&
                   lhs.error_capture == rhs.error_capture &&
                   lhs.active_inputs == rhs.active_inputs &&
                   lhs.structural_inputs == rhs.structural_inputs &&
                   lhs.valid_inputs == rhs.valid_inputs &&
                   lhs.all_valid_inputs == rhs.all_valid_inputs;
          }

          [[nodiscard]] NodeTypeRef
          find_canonical(const void *runtime_type_id,
                         const NodeTypeMetaData &schema,
                         const MemoryUtils::StoragePlan &plan,
                         std::string_view implementation_label,
                         const std::vector<DebugField> &debug_fields,
                         const std::optional<NodeTypeDescriptor::DynamicDebug>
                             &dynamic_debug) const {
            // Supplied debug descriptors are uncommon and may carry
            // caller-owned names.  Keep them on the conservative path
            // until their complete value contract is part of this key.
            if (runtime_type_id == nullptr || !debug_fields.empty() ||
                dynamic_debug.has_value()) {
              return {};
            }
            const auto found = canonical_types.find(runtime_type_id);
            if (found == canonical_types.end()) {
              return {};
            }
            for (const NodeTypeRef candidate : found->second) {
              if (candidate.plan() == &plan &&
                  candidate.record()->implementation_name() ==
                      implementation_label &&
                  schema_equivalent(*candidate.schema(), schema)) {
                return candidate;
              }
            }
            return {};
          }

          NodeTypeRef make_type(NodeTypeMetaData schema,
                                NodeCallbacks callbacks,
                                const MemoryUtils::StoragePlan &plan,
                                NodeOps ops,
                                std::string_view implementation_label,
                                const void *runtime_type_id = nullptr,
                                std::vector<DebugField> debug_fields = {},
                                std::optional<NodeTypeDescriptor::DynamicDebug>
                                    dynamic_debug = {}) {
            if (const NodeTypeRef existing = find_canonical(
                    runtime_type_id, schema, plan, implementation_label,
                    debug_fields, dynamic_debug)) {
              return existing;
            }
            names.push_back(std::make_unique<std::string>(
                schema.display_name != nullptr
                    ? std::string{schema.display_name}
                    : std::string{}));
            if (!names.back()->empty()) {
              schema.display_name = names.back()->c_str();
            }
            schema.header = SchemaHeader{
                TypeFamily::Node, static_cast<TypeKind>(schema.node_kind),
                schema.display_name != nullptr && schema.display_name[0] != '\0'
                    ? schema.display_name
                    : "node"};

            contexts.push_back(NodeRuntimeContext{
                .callbacks = std::move(callbacks),
                .layout = layout_for(plan),
                .plan = &plan,
                .runtime_type_id = runtime_type_id,
            });
            schemas.push_back(std::move(schema));
            fill_default_ops(ops);
            ops.context = &contexts.back();
            ops_storage.push_back(ops);

            const NodeTypeRef type = intern_node_type(
                schemas.back(), plan, ops_storage.back(), implementation_label,
                debug_fields,
                dynamic_debug.has_value() ? dynamic_debug->key_type : nullptr,
                dynamic_debug.has_value() ? dynamic_debug->element_type
                                          : nullptr,
                dynamic_debug.has_value() ? &dynamic_debug->layout : nullptr);
            if (runtime_type_id != nullptr && debug_fields.empty() &&
                !dynamic_debug.has_value()) {
              canonical_types[runtime_type_id].push_back(type);
            }
            return type;
          }

            static void fill_default_ops(NodeOps &ops)
            {
                if (ops.attach_graph_impl == nullptr) { ops.attach_graph_impl = &attach_graph_impl; }
                if (ops.graph_impl == nullptr) { ops.graph_impl = &graph_impl; }
                if (ops.node_index_impl == nullptr) { ops.node_index_impl = &node_index_impl; }
                if (ops.label_impl == nullptr) { ops.label_impl = &label_impl; }
                if (ops.started_impl == nullptr) { ops.started_impl = &started_impl; }
                if (ops.start_impl == nullptr) { ops.start_impl = &start_impl; }
                if (ops.stop_impl == nullptr) { ops.stop_impl = &stop_impl; }
                if (ops.evaluate_impl == nullptr) { ops.evaluate_impl = &evaluate_impl; }
                if (ops.has_input_impl == nullptr) { ops.has_input_impl = &has_input_impl; }
                if (ops.has_output_impl == nullptr) { ops.has_output_impl = &has_output_impl; }
                if (ops.has_state_impl == nullptr) { ops.has_state_impl = &has_state_impl; }
                if (ops.has_scalars_impl == nullptr) { ops.has_scalars_impl = &has_scalars_impl; }
                if (ops.has_scheduler_impl == nullptr) { ops.has_scheduler_impl = &has_scheduler_impl; }
                if (ops.has_error_output_impl == nullptr) { ops.has_error_output_impl = &has_error_output_impl; }
                if (ops.has_recordable_state_impl == nullptr)
                {
                    ops.has_recordable_state_impl = &has_recordable_state_impl;
                }
                if (ops.input_view_impl == nullptr) { ops.input_view_impl = &input_view_impl; }
                if (ops.output_view_impl == nullptr) { ops.output_view_impl = &output_view_impl; }
                if (ops.state_view_impl == nullptr) { ops.state_view_impl = &state_view_impl; }
                if (ops.replace_state_impl == nullptr) { ops.replace_state_impl = &replace_state_impl; }
                if (ops.scalars_view_impl == nullptr) { ops.scalars_view_impl = &scalars_view_impl; }
                if (ops.scheduler_state_impl == nullptr) { ops.scheduler_state_impl = &scheduler_state_impl; }
                if (ops.global_state_view_impl == nullptr) { ops.global_state_view_impl = &global_state_view_impl; }
                if (ops.evaluation_clock_ptr_impl == nullptr)
                {
                    ops.evaluation_clock_ptr_impl = &evaluation_clock_ptr_impl;
                }
                if (ops.error_output_view_impl == nullptr) { ops.error_output_view_impl = &error_output_view_impl; }
                if (ops.recordable_state_view_impl == nullptr)
                {
                    ops.recordable_state_view_impl = &recordable_state_view_impl;
                }
            }

            void clear() noexcept
            {
                ops_storage.clear();
                contexts.clear();
                schemas.clear();
                names.clear();
                canonical_types.clear();
            }

            std::deque<NodeTypeMetaData>                 schemas{};
            std::deque<NodeRuntimeContext>               contexts{};
            std::deque<NodeOps>                          ops_storage{};
            std::vector<std::unique_ptr<std::string>>    names{};
            std::unordered_map<const void *, std::vector<NodeTypeRef>>
                canonical_types{};
        };

        NodeRuntimeRegistry &node_runtime_registry()
        {
            static NodeRuntimeRegistry registry;
            return registry;
        }

    }  // namespace

    namespace
    {
        void validate_node_record(const TypeRecord &record)
        {
            if (!record.valid() || record.schema->family != TypeFamily::Node ||
                record.role != TypeRole::Runtime)
            {
                throw std::invalid_argument("NodeTypeRef requires a Node/Runtime TypeRecord");
            }
            const auto *schema = reinterpret_cast<const NodeTypeMetaData *>(record.schema);
            if (record.schema->kind != static_cast<TypeKind>(schema->node_kind))
            {
                throw std::invalid_argument("NodeTypeRef requires matching common and node schema kinds");
            }
            if (record.ops_abi_version != NODE_OPS_ABI_VERSION || record.ops == nullptr)
            {
                throw std::invalid_argument("NodeTypeRef requires node ops ABI version 1");
            }
            if (record.capabilities != node_type_capabilities(*record.plan))
            {
                throw std::invalid_argument("NodeTypeRef capabilities do not match its storage plan");
            }
        }
    }  // namespace

    TypeCapabilities node_type_capabilities(const MemoryUtils::StoragePlan &plan)
    {
        TypeCapabilities result = TypeCapabilities::Viewable | TypeCapabilities::Mutable;
        if (plan.can_default_construct()) result |= TypeCapabilities::Constructible;
        if (plan.trivially_destructible || plan.lifecycle.can_destroy())
            result |= TypeCapabilities::Destructible;
        if (plan.can_copy_construct()) result |= TypeCapabilities::Copyable;
        if (plan.can_move_construct()) result |= TypeCapabilities::Movable;
        return result;
    }

    NodeTypeRef intern_node_type(const NodeTypeMetaData &schema,
                                 const MemoryUtils::StoragePlan &plan,
                                 const NodeOps &ops,
                                 std::string_view implementation_label,
                                 std::span<const DebugField> supplied_debug_fields,
                                 const TypeRecord *debug_key_type,
                                 const TypeRecord *debug_element_type,
                                 const DebugDynamicLayout *debug_dynamic_layout)
    {
        if (!schema.header.valid() || schema.header.family != TypeFamily::Node ||
            schema.header.kind != static_cast<TypeKind>(schema.node_kind))
        {
            throw std::invalid_argument("intern_node_type requires a valid node schema header");
        }
        std::vector<DebugField> debug_fields;
        const auto *runtime_storage = plan.find_component("runtime_storage");
        if (runtime_storage == nullptr)
            throw std::logic_error("node debug descriptor could not resolve runtime storage");
        debug_fields.push_back(DebugField{
            .name = "graph",
            .offset = runtime_storage->offset + node_runtime_graph_offset(schema),
            .flags = DebugFieldFlags::IndirectEmbeddedPointer,
        });
        const auto append_value_owner = [&](const char *name, const ValueTypeMetaData *value_schema) {
            if (value_schema == nullptr) { return; }
            const auto *component = plan.find_component(name);
            const auto value_type = ValuePlanFactory::instance().type_for(value_schema);
            if (component == nullptr || !value_type)
                throw std::logic_error("node debug descriptor could not resolve a value owner field");
            debug_fields.push_back(DebugField{
                .name = name,
                .offset = component->offset,
                .type = value_type.record(),
                .flags = DebugFieldFlags::EmbeddedOwner,
            });
        };
        append_value_owner("state", schema.state_schema);
        append_value_owner("scalars", schema.scalar_schema);
        debug_fields.insert(debug_fields.end(), supplied_debug_fields.begin(), supplied_debug_fields.end());
        const auto &debug = intern_structured_debug_descriptor(
            schema.header, plan, DebugLayoutKind::Node,
            debug_fields.empty() ? nullptr : debug_fields.data(), debug_fields.size(), debug_key_type,
            debug_element_type, debug_dynamic_layout);
        const TypeRecordDefinition definition{
            .key = TypeRecordKey{.schema = &schema.header,
                                 .role = TypeRole::Runtime,
                                 .plan = &plan,
                                 .ops = &ops,
                                 .debug = &debug},
            .ops_abi_version = NODE_OPS_ABI_VERSION,
            .capabilities = node_type_capabilities(plan),
            .implementation_label = implementation_label,
        };
        return NodeTypeRef{&TypeRecordRegistry::instance().intern(definition)};
    }

    NodeTypeRef NodeTypeRef::checked(AnyPtr pointer)
    {
        if (pointer.is_unbound()) return {};
        if (!pointer.well_formed() || pointer.record() == nullptr)
            throw std::invalid_argument("NodeTypeRef requires a well-formed pointer");
        validate_node_record(*pointer.record());
        return NodeTypeRef{pointer.record()};
    }

    bool NodeTypeRef::valid() const noexcept
    {
        if (record_ == nullptr) return false;
        return fallback_on_exception(false, [&] {
            validate_node_record(*record_);
            return true;
        });
    }

    const NodeTypeMetaData *NodeTypeRef::schema() const noexcept
    {
        return record_ != nullptr ? reinterpret_cast<const NodeTypeMetaData *>(record_->schema) : nullptr;
    }

    const MemoryUtils::StoragePlan &NodeTypeRef::checked_plan() const
    {
        if (plan() == nullptr) throw std::logic_error("NodeTypeRef is unbound");
        return *plan();
    }

    NodePtr NodeTypeRef::typed_null() const noexcept
    {
        return NodePtr{AnyPtr{record_, nullptr, AccessMode::ReadOnly}, NodePtr::UncheckedTag{}};
    }

    NodePtr NodeTypeRef::read_only(const void *data) const noexcept
    {
        return NodePtr{AnyPtr{record_, data, AccessMode::ReadOnly}, NodePtr::UncheckedTag{}};
    }

    NodePtr NodeTypeRef::writable(void *data) const noexcept
    {
        return NodePtr{AnyPtr{record_, data, AccessMode::Writable}, NodePtr::UncheckedTag{}};
    }

    void notify_node_endpoint_child_modified(NodePtr             node,
                                             TSEndpointOwnerPort port,
                                             DateTime            mutation_time)
    {
        if (!node.valid()) { return; }

        const auto type = NodeView{node}.type();
        const auto &runtime = runtime_context(type.ops_ref().context);
        void *node_data = const_cast<void *>(node.data());
        switch (port)
        {
            case TSEndpointOwnerPort::Input: return;
            case TSEndpointOwnerPort::Output:
                node_output(runtime, node_data).record_child_modified(TS_DATA_NO_CHILD_ID, mutation_time);
                return;
            case TSEndpointOwnerPort::ErrorOutput:
                node_error_output(runtime, node_data).record_child_modified(TS_DATA_NO_CHILD_ID, mutation_time);
                return;
            case TSEndpointOwnerPort::RecordableState:
                node_recordable_state(runtime, node_data).record_child_modified(TS_DATA_NO_CHILD_ID, mutation_time);
                return;
        }
    }

    const MemoryUtils::StoragePlan &node_storage_plan_for(
        const NodeTypeMetaData &schema,
        std::span<const NodeStorageField> extra_fields,
        std::span<const NodeStorageField> extra_fields_after_output)
    {
        auto builder = MemoryUtils::named_tuple();
        builder.add_field("runtime_storage", MemoryUtils::plan_for<NodeRuntimeStorage>());
        if (schema.input_schema != nullptr) { builder.add_field("input", MemoryUtils::plan_for<TSInput>()); }
        for (const NodeStorageField &field : extra_fields)
        {
            if (field.plan == nullptr) { throw std::logic_error("Node storage field requires a storage plan"); }
            builder.add_field(field.name, *field.plan);
        }
        if (schema.output_schema != nullptr) { builder.add_field("output", MemoryUtils::plan_for<TSOutput>()); }
        // Destroyed BEFORE the output (reverse-order destruction): for fields
        // holding links INTO the node's own output (see the header note).
        for (const NodeStorageField &field : extra_fields_after_output)
        {
            if (field.plan == nullptr) { throw std::logic_error("Node storage field requires a storage plan"); }
            builder.add_field(field.name, *field.plan);
        }
        if (schema.state_schema != nullptr) { builder.add_field("state", MemoryUtils::plan_for<Value>()); }
        if (schema.scalar_schema != nullptr) { builder.add_field("scalars", MemoryUtils::plan_for<Value>()); }
        if (schema.uses_scheduler)
        {
            builder.add_field("scheduler", MemoryUtils::plan_for<NodeSchedulerState>());
        }
        if (schema.uses_global_state)
        {
            builder.add_field("global_state", MemoryUtils::plan_for<std::optional<GlobalStateView>>());
        }
        if (schema.uses_evaluation_clock)
        {
            builder.add_field("evaluation_clock", MemoryUtils::plan_for<ClockPtr>());
        }
        if (schema.error_output_schema != nullptr)
        {
            builder.add_field("error_output", MemoryUtils::plan_for<TSOutput>());
        }
        if (schema.recordable_state_schema != nullptr)
        {
            builder.add_field("recordable_state", MemoryUtils::plan_for<TSOutput>());
        }
        return builder.build();
    }

    std::string_view NodeTypeMetaData::name() const noexcept
    {
        return display_name != nullptr ? std::string_view{display_name} : std::string_view{};
    }

    bool NodeTypeMetaData::has_input() const noexcept { return input_schema != nullptr; }
    bool NodeTypeMetaData::has_output() const noexcept { return output_schema != nullptr; }
    bool NodeTypeMetaData::has_state() const noexcept { return state_schema != nullptr; }
    bool NodeTypeMetaData::has_scalars() const noexcept { return scalar_schema != nullptr; }
    bool NodeTypeMetaData::has_error_output() const noexcept { return error_output_schema != nullptr; }
    bool NodeTypeMetaData::has_recordable_state() const noexcept { return recordable_state_schema != nullptr; }

    NodeView::NodeView() noexcept = default;

    NodeView::NodeView(NodePtr pointer) noexcept : pointer_(pointer) {}

    NodeView::NodeView(NodeTypeRef type, void *memory) noexcept
        : pointer_(type && memory != nullptr ? type.writable(memory) : NodePtr{})
    {
    }

    bool NodeView::valid() const noexcept { return pointer_.valid(); }
    NodeTypeRef NodeView::type() const noexcept { return NodeTypeRef{pointer_.record()}; }
    NodePtr NodeView::pointer() const noexcept { return pointer_; }
    const NodeTypeMetaData *NodeView::schema() const noexcept
    {
        return type().schema();
    }
    void *NodeView::data() const noexcept { return const_cast<void *>(pointer_.data()); }

    void *NodeView::prepared_input_routes() const noexcept
    {
        if (!valid()) { return nullptr; }
        const NodeOps &table = ops();
        if (table.context == nullptr) { return nullptr; }
        const auto &context = *static_cast<const NodeRuntimeContext *>(table.context);
        if (context.layout.prepared_inputs_offset == NodeRuntimeLayout::npos) { return nullptr; }
        return MemoryUtils::advance(data(), context.layout.prepared_inputs_offset);
    }

    std::string_view NodeView::label() const noexcept
    {
        return ops().label_impl(ops().context, data());
    }

    NodeKind NodeView::node_kind() const noexcept
    {
        return schema() != nullptr ? schema()->node_kind : NodeKind::Compute;
    }

    bool NodeView::started() const noexcept
    {
        return ops().started_impl(ops().context, data());
    }

    std::size_t NodeView::node_index() const noexcept
    {
        return ops().node_index_impl(ops().context, data());
    }

    GraphValue *NodeView::graph_value() const noexcept
    {
        return ops().graph_impl(ops().context, data());
    }

    GraphView NodeView::graph() const
    {
        auto *graph = graph_value();
        return graph != nullptr ? graph->view() : GraphView{};
    }

    bool NodeView::has_input() const noexcept
    {
        return ops().has_input_impl(ops().context, data());
    }

    bool NodeView::has_output() const noexcept
    {
        return ops().has_output_impl(ops().context, data());
    }

    bool NodeView::has_state() const noexcept
    {
        return ops().has_state_impl(ops().context, data());
    }

    bool NodeView::has_scalars() const noexcept
    {
        return ops().has_scalars_impl(ops().context, data());
    }

    bool NodeView::has_scheduler() const noexcept
    {
        return ops().has_scheduler_impl(ops().context, data());
    }

    bool NodeView::has_error_output() const noexcept
    {
        return ops().has_error_output_impl(ops().context, data());
    }

    bool NodeView::has_recordable_state() const noexcept
    {
        return ops().has_recordable_state_impl(ops().context, data());
    }

    TSInputView NodeView::input(DateTime evaluation_time) const
    {
        return ops().input_view_impl(ops().context, data(), evaluation_time);
    }

    TSOutputView NodeView::output(DateTime evaluation_time) const
    {
        return ops().output_view_impl(ops().context, data(), evaluation_time);
    }

    ValueView NodeView::state() const
    {
        return ops().state_view_impl(ops().context, data());
    }

    void NodeView::replace_state(Value value) const
    {
        ops().replace_state_impl(ops().context, data(), std::move(value));
    }

    ValueView NodeView::scalars() const
    {
        return ops().scalars_view_impl(ops().context, data());
    }

    NodeSchedulerState &NodeView::scheduler_state() const
    {
        return *ops().scheduler_state_impl(ops().context, data());
    }

    GlobalStateView NodeView::global_state() const
    {
        return ops().global_state_view_impl(ops().context, data());
    }

    ClockPtr NodeView::evaluation_clock_ptr() const
    {
        return ops().evaluation_clock_ptr_impl(ops().context, data());
    }

    EvaluationClockView NodeView::evaluation_clock() const
    {
        return EvaluationClockView{evaluation_clock_ptr()};
    }

    TSOutputView NodeView::error_output(DateTime evaluation_time) const
    {
        return ops().error_output_view_impl(ops().context, data(), evaluation_time);
    }

    TSOutputView NodeView::recordable_state(DateTime evaluation_time) const
    {
        return ops().recordable_state_view_impl(ops().context, data(), evaluation_time);
    }

    NodeStorageMetrics NodeView::storage_metrics() const noexcept
    {
        NodeStorageMetrics result{};
        if (!valid()) { return result; }
        if (const auto *plan = type().plan(); plan != nullptr)
        {
            result.static_bytes = plan->layout.size;
        }
        const NodeOps &node_ops = ops();
        if (node_ops.storage_metrics_impl != nullptr)
        {
            result = node_ops.storage_metrics_impl(
                node_ops.extended_view_context, data());
            result.static_bytes = type().plan() != nullptr ? type().plan()->layout.size : 0;
        }

        struct TSDataIdentity
        {
            const TypeRecord *record{nullptr};
            const void *memory{nullptr};
        };
        std::array<TSDataIdentity, 3> attributed_ts_data{};
        std::size_t attributed_count = 0;
        const auto add_ts_data_storage = [&result, &attributed_ts_data,
                                          &attributed_count](const TSOutputView &view) noexcept {
            const TSDataView &data = view.data_view();
            const TSDataIdentity identity{
                .record = data.storage_type().record(),
                .memory = data.data(),
            };
            bool already_attributed = false;
            for (std::size_t index = 0; index < attributed_count; ++index)
            {
                const auto &existing = attributed_ts_data[index];
                if (existing.record == identity.record &&
                    existing.memory == identity.memory)
                {
                    already_attributed = true;
                    break;
                }
            }
            if (already_attributed) { return; }
            attributed_ts_data[attributed_count++] = identity;
            const DynamicStorageMetrics metrics = data.dynamic_storage_metrics();
            result.dynamic_live_bytes += metrics.live_bytes;
            result.dynamic_reserved_bytes += metrics.reserved_bytes;
        };
        const auto add_value_storage = [&result](const ValueView &view) noexcept {
            const DynamicStorageMetrics metrics = view.dynamic_storage_metrics();
            result.dynamic_live_bytes += metrics.live_bytes;
            result.dynamic_reserved_bytes += metrics.reserved_bytes;
        };
        try
        {
            if (has_state()) { add_value_storage(state()); }
            if (has_scalars()) { add_value_storage(scalars()); }
            if (has_output()) { add_ts_data_storage(output(MIN_DT)); }
            if (has_error_output()) { add_ts_data_storage(error_output(MIN_DT)); }
            if (has_recordable_state()) { add_ts_data_storage(recordable_state(MIN_DT)); }
        }
        catch (...)
        {
            // Inspector is a cold-path diagnostic and must not affect graph execution.
        }
        return result;
    }

    void NodeView::start(DateTime evaluation_time) const { ops().start_impl(ops().context, *this, evaluation_time); }
    void NodeView::stop(DateTime evaluation_time) const { ops().stop_impl(ops().context, *this, evaluation_time); }
    bool NodeView::evaluate(DateTime evaluation_time) const
    {
        return ops().evaluate_impl(ops().context, *this, evaluation_time);
    }
    const NodeOps &NodeView::ops() const
    {
        return type().ops_ref();
    }

    NodeValue::NodeValue() noexcept = default;

    NodeValue::NodeValue(const NodeBuilder &builder, std::size_t node_index)
    {
        const auto type = builder.type();
        storage_ = storage_type::owning_constructed(*type.record(), [&](void *dst) {
            builder.construct_node_storage(dst, node_index);
        });
    }

    NodeValue::~NodeValue() = default;

    NodeValue::NodeValue(NodeValue &&other) noexcept
        : storage_(std::move(other.storage_))
    {
    }

    NodeValue &NodeValue::operator=(NodeValue &&other) noexcept
    {
        if (this != &other) { storage_ = std::move(other.storage_); }
        return *this;
    }

    bool NodeValue::has_value() const noexcept { return storage_.has_value(); }
    NodeTypeRef NodeValue::type() const noexcept { return NodeTypeRef{storage_.binding()}; }
    const NodeTypeMetaData *NodeValue::schema() const noexcept
    {
        return type().schema();
    }

    NodeView NodeValue::view()
    {
        return NodeView{type(), storage_.data()};
    }

    NodeView NodeValue::view() const
    {
        return NodeView{type(), const_cast<void *>(storage_.data())};
    }

    void NodeValue::attach_graph(GraphValue *graph, std::size_t node_index)
    {
        if (!has_value()) { return; }
        const auto node_type = type();
        const auto &table = node_type.ops_ref();
        table.attach_graph_impl(table.context, storage_.data(), graph, node_index);
    }

    NodeBuilder::NodeBuilder() = default;

    NodeBuilder NodeBuilder::native(NodeTypeMetaData schema,
                                    NodeCallbacks callbacks,
                                    TSEndpointSchema input_endpoint,
                                    std::string_view implementation_label)
    {
        NodeTypeDescriptor descriptor;
        descriptor.schema = std::move(schema);
        descriptor.callbacks = std::move(callbacks);
        descriptor.implementation_label = implementation_label;
        return from_descriptor(std::move(descriptor), std::move(input_endpoint));
    }

    NodeBuilder NodeBuilder::from_descriptor(NodeTypeDescriptor descriptor,
                                             TSEndpointSchema input_endpoint) {
      return from_descriptor_impl(std::move(descriptor), nullptr,
                                  std::move(input_endpoint));
    }

    NodeBuilder
    NodeBuilder::from_canonical_descriptor(NodeTypeDescriptor descriptor,
                                           const void *runtime_type_id,
                                           TSEndpointSchema input_endpoint) {
      if (runtime_type_id == nullptr) {
        throw std::invalid_argument(
            "from_canonical_descriptor requires a non-null runtime type id");
      }
      return from_descriptor_impl(std::move(descriptor), runtime_type_id,
                                  std::move(input_endpoint));
    }

    NodeBuilder
    NodeBuilder::from_descriptor_impl(NodeTypeDescriptor descriptor,
                                      const void *runtime_type_id,
                                      TSEndpointSchema input_endpoint) {
      if (descriptor.schema.input_schema != nullptr &&
          !input_endpoint.empty() &&
          !time_series_schema_equivalent(descriptor.schema.input_schema,
                                         input_endpoint.schema())) {
        throw std::invalid_argument("NodeBuilder input endpoint schema does "
                                    "not match node input schema");
      }
      if (descriptor.schema.output_schema != nullptr &&
          !descriptor.schema.output_endpoint_schema.empty() &&
          !time_series_schema_equivalent(
              descriptor.schema.output_schema,
              descriptor.schema.output_endpoint_schema.schema())) {
        throw std::invalid_argument("NodeBuilder output endpoint schema does "
                                    "not match node output schema");
      }
      if (descriptor.schema.output_schema == nullptr &&
          !descriptor.schema.output_endpoint_schema.empty()) {
        throw std::invalid_argument(
            "NodeBuilder output endpoint requires a node output schema");
      }

      const auto &plan = descriptor.storage_plan != nullptr
                             ? *descriptor.storage_plan
                             : node_storage_plan_for(descriptor.schema);
      const auto type = node_runtime_registry().make_type(
          std::move(descriptor.schema), std::move(descriptor.callbacks), plan,
          descriptor.ops, descriptor.implementation_label, runtime_type_id,
          std::move(descriptor.debug_fields),
          std::move(descriptor.dynamic_debug));
      return NodeBuilder{type, std::move(input_endpoint)};
    }

    NodeBuilder::NodeBuilder(NodeTypeRef type, TSEndpointSchema input_endpoint)
        : type_(type),
          input_endpoint_(std::move(input_endpoint))
    {
    }

    NodeBuilder &NodeBuilder::label(std::string label)
    {
        label_ = std::move(label);
        return *this;
    }

    std::string_view NodeBuilder::label() const noexcept
    {
        return label_;
    }

    NodeBuilder &NodeBuilder::input_endpoint(TSEndpointSchema endpoint)
    {
        if (!type_)
        {
            if (!endpoint.empty()) { throw std::logic_error("NodeBuilder has no binding"); }
        }
        else
        {
            const auto *node_schema = type_.schema();
            const auto *schema = node_schema != nullptr ? node_schema->input_schema : nullptr;
            if (schema != nullptr && !endpoint.empty() && !time_series_schema_equivalent(schema, endpoint.schema()))
            {
                throw std::invalid_argument("NodeBuilder input endpoint schema does not match node input schema");
            }
            if (schema == nullptr && !endpoint.empty())
            {
                throw std::invalid_argument("NodeBuilder input endpoint requires a node input schema");
            }
        }
        input_endpoint_ = std::move(endpoint);
        return *this;
    }

    NodeBuilder &NodeBuilder::scalars(Value scalars)
    {
        scalars_ = std::move(scalars);
        return *this;
    }

    const Value &NodeBuilder::scalars() const noexcept
    {
        return scalars_;
    }

    NodeTypeRef NodeBuilder::type() const
    {
        if (!type_) { throw std::logic_error("NodeBuilder has no type"); }
        return type_;
    }

    NodeBuilder NodeBuilder::with_error_capture(const TSValueTypeMetaData *error_schema,
                                                ErrorCaptureOptions options) const
    {
        if (!type_) { throw std::logic_error("NodeBuilder has no type"); }
        if (error_schema == nullptr) { throw std::invalid_argument("with_error_capture requires an error schema"); }

        const NodeOps &node_ops = type_.ops_ref();
        if (node_ops.context == nullptr)
        {
            throw std::invalid_argument(
                "with_error_capture: error capture is only supported on native nodes");
        }
        const auto &origin = *static_cast<const NodeRuntimeContext *>(node_ops.context);
        // A specialised native evaluator may retain a callback specifically
        // for derived error-capture types. Custom lifecycle nodes
        // (nested/map/switch) cannot be converted to the standard evaluator.
        const bool uses_standard_evaluator = node_ops.evaluate_impl == &evaluate_impl;
        const bool has_capture_fallback = node_ops.start_impl == &start_impl &&
                                          node_ops.stop_impl == &stop_impl &&
                                          static_cast<bool>(origin.callbacks.evaluate);
        if (!uses_standard_evaluator && !has_capture_fallback)
        {
            throw std::invalid_argument(
                "with_error_capture: error capture is only supported on native nodes");
        }

        NodeTypeMetaData schema = *type_.schema();
        schema.error_output_schema = error_schema;
        if (schema.captures_errors)
        {
            options.trace_back_depth =
                std::max(schema.error_capture.trace_back_depth, options.trace_back_depth);
            options.capture_values = schema.error_capture.capture_values || options.capture_values;
        }
        schema.captures_errors     = true;
        schema.error_capture       = options;

        // Preserve non-standard planned fields (the prepared-slot array):
        // callbacks resolve the field's offset through the rebuilt layout,
        // never from the origin plan.
        std::vector<NodeStorageField> extra_fields;
        if (const auto *prepared = origin.plan != nullptr
                                       ? origin.plan->find_component(node_prepared_inputs_field)
                                       : nullptr)
        {
            extra_fields.push_back(NodeStorageField{node_prepared_inputs_field, prepared->plan});
        }
        const auto &plan = node_storage_plan_for(schema, extra_fields);
        const auto type = node_runtime_registry().make_type(
            std::move(schema), origin.callbacks, plan, NodeOps{},
            type_.record()->implementation_name(), origin.runtime_type_id);

        NodeBuilder result{type, input_endpoint_};
        result.output_endpoint_ = output_endpoint_;
        result.output_value_storage_ = output_value_storage_;
        result.label_           = label_;
        result.scalars_         = scalars_;
        return result;
    }

    NodeBuilder NodeBuilder::with_passive_inputs(std::span<const std::size_t> slots) const
    {
        if (!type_) { throw std::logic_error("NodeBuilder has no type"); }
        if (slots.empty()) { return *this; }

        const NodeOps &node_ops = type_.ops_ref();
        // Nodes using the standard start/stop operations share the native
        // input-activation protocol even when they provide a specialised
        // evaluator. Custom lifecycle nodes (nested/map/switch) manage their
        // own activation and cannot be rebound this way.
        if (node_ops.start_impl != &start_impl || node_ops.stop_impl != &stop_impl ||
            node_ops.context == nullptr)
        {
            throw std::invalid_argument("passive inputs are only supported on native nodes");
        }
        const auto &origin = *static_cast<const NodeRuntimeContext *>(node_ops.context);

        NodeTypeMetaData schema = *type_.schema();
        const std::size_t input_count =
            schema.input_schema != nullptr && schema.input_schema->kind == TSTypeKind::TSB
                ? schema.input_schema->field_count()
                : (schema.input_schema != nullptr ? 1 : 0);

        std::vector<std::size_t> active;
        if (schema.active_inputs.has_value()) { active = *schema.active_inputs; }
        else
        {
            active.resize(input_count);
            for (std::size_t slot = 0; slot < input_count; ++slot) { active[slot] = slot; }
        }
        const bool had_scheduled_input = !active.empty() || !schema.structural_inputs.empty();
        for (const std::size_t slot : slots)
        {
            if (slot >= input_count) { throw std::out_of_range("passive input slot is out of range"); }
            std::erase(active, slot);
            std::erase(schema.structural_inputs, slot);
        }
        if (had_scheduled_input && active.empty() && schema.structural_inputs.empty())
        {
            throw std::invalid_argument(
                "passive would deactivate every input of the node — it could never evaluate");
        }
        schema.active_inputs = std::move(active);

        // Preserve non-standard planned fields (see with_error_capture).
        std::vector<NodeStorageField> extra_fields;
        if (const auto *prepared = origin.plan != nullptr
                                       ? origin.plan->find_component(node_prepared_inputs_field)
                                       : nullptr)
        {
            extra_fields.push_back(NodeStorageField{node_prepared_inputs_field, prepared->plan});
        }
        const auto &plan = node_storage_plan_for(schema, extra_fields);
        const auto type = node_runtime_registry().make_type(
            std::move(schema), origin.callbacks, plan, node_ops,
            type_.record()->implementation_name(), origin.runtime_type_id);

        NodeBuilder result{type, input_endpoint_};
        result.output_endpoint_ = output_endpoint_;
        result.output_value_storage_ = output_value_storage_;
        result.label_           = label_;
        result.scalars_         = scalars_;
        return result;
    }

    const TSEndpointSchema &NodeBuilder::input_endpoint() const noexcept
    {
        return input_endpoint_;
    }

    NodeBuilder &NodeBuilder::output_endpoint(TSEndpointSchema endpoint)
    {
        const auto *type_meta = type_.schema();
        if (type_meta == nullptr || type_meta->output_schema == nullptr)
        {
            throw std::invalid_argument("NodeBuilder output endpoint requires a node output schema");
        }
        if (!endpoint.empty() && !time_series_schema_equivalent(type_meta->output_schema, endpoint.schema()))
        {
            throw std::invalid_argument("NodeBuilder output endpoint schema does not match node output schema");
        }
        output_endpoint_ = std::move(endpoint);
        return *this;
    }

    const TSEndpointSchema &NodeBuilder::output_endpoint() const noexcept
    {
        return output_endpoint_;
    }

    NodeBuilder &NodeBuilder::output_value_storage(ValueStorageVariant storage) noexcept
    {
        output_value_storage_ = storage;
        return *this;
    }

    ValueStorageVariant NodeBuilder::output_value_storage() const noexcept
    {
        return output_value_storage_;
    }

    void NodeBuilder::construct_node_storage(void *memory, std::size_t node_index) const
    {
        if (memory == nullptr) { throw std::logic_error("NodeBuilder::construct_node_storage requires memory"); }

        const auto type = this->type();
        const auto &runtime = runtime_context(type.ops_ref().context);
        construct_node_storage_impl(runtime,
                                    *type.schema(),
                                    input_endpoint(),
                                    output_endpoint(),
                                    output_value_storage(),
                                    std::string{label()},
                                    scalars(),
                                    memory);

        const auto &table = type.ops_ref();
        table.attach_graph_impl(table.context, memory, nullptr, node_index);
    }

    NodeValue NodeBuilder::make_node(std::size_t node_index) const
    {
        return NodeValue{*this, node_index};
    }

    void clear_node_runtime_types() noexcept
    {
        clear_debug_descriptors(TypeFamily::Node);
        node_runtime_registry().clear();
    }

    std::size_t detail::node_runtime_type_count() noexcept
    {
        return node_runtime_registry().schemas.size();
    }

}  // namespace hgraph
