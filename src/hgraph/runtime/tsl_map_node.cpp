#include <hgraph/runtime/tsl_map_node.h>

#include <hgraph/runtime/nested_graph_storage.h>
#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/primitive_types.h>
#include <hgraph/util/scope.h>

#include "mapped_child_bindings.h"
#include "mapped_key_source.h"

#include <algorithm>
#include <array>
#include <cstddef>
#include <limits>
#include <memory>
#include <stdexcept>
#include <string_view>
#include <utility>
#include <vector>

namespace hgraph
{
    namespace
    {
        constexpr std::string_view tsl_map_storage_field_name{"tsl_map"};

        struct TslMapEntry
        {
            explicit TslMapEntry(std::size_t index_) : index(static_cast<Int>(index_)) {}

            // The child subscribes to index_source, so reverse destruction
            // must tear the graph down before the source and its Value.
            Value                           index{};
            runtime_detail::MappedKeySource index_source{};
            GraphValue                      graph{};
        };

        struct TslMapNodeStorage
        {
            TslMapNodeStorage() = default;

            TslMapNodeStorage(const TslMapNodeStorage &)            = delete;
            TslMapNodeStorage &operator=(const TslMapNodeStorage &) = delete;
            TslMapNodeStorage(TslMapNodeStorage &&)                 = delete;
            TslMapNodeStorage &operator=(TslMapNodeStorage &&)      = delete;

            ~TslMapNodeStorage() { stop_and_destroy_noexcept(); }

            InPlaceGraphSlotStore<TslMapEntry> entries{};
            std::vector<TSOutputHandle>        outer_sources{};
            std::vector<std::size_t>           multiplexed_sizes{};
            std::size_t                        live_count{0};

            static constexpr std::size_t npos = static_cast<std::size_t>(-1);
            std::size_t                  resume_index{npos};

            [[nodiscard]] std::size_t active_count() const noexcept {
                std::size_t count = 0;
                for (std::size_t index = 0; index < live_count; ++index) {
                    const auto *entry = entries.entry_at(index);
                    if (entry != nullptr && entry->graph.has_value() && entry->graph.view().started()) { ++count; }
                }
                return count;
            }

            void stop_and_destroy_noexcept() noexcept {
                for (std::size_t index = 0; index < entries.slot_capacity(); ++index) {
                    auto *entry = entries.entry_at(index);
                    if (entry == nullptr || !entry->graph.has_value() || !entry->graph.view().started()) { continue; }
                    static_cast<void>(fallback_on_exception(false, [&] {
                        entry->graph.view().stop();
                        return true;
                    }));
                }
                entries.destroy_all();
                outer_sources.clear();
                multiplexed_sizes.clear();
                live_count   = 0;
                resume_index = npos;
            }
        };

        struct TslMapNodeContext
        {
            TslMapNodeSpec             spec{};
            std::size_t                storage_offset{0};
            MemoryUtils::StorageLayout graph_layout{};
        };

        [[nodiscard]] std::vector<std::unique_ptr<TslMapNodeContext>> &tsl_map_node_contexts() noexcept {
            // Type-level contexts outlive all graphs and the registries their
            // builders reference; match the other nested node context stores.
            static auto *contexts = new std::vector<std::unique_ptr<TslMapNodeContext>>;
            return *contexts;
        }

        [[nodiscard]] const TslMapNodeContext &register_tsl_map_node_context(TslMapNodeSpec spec, std::size_t storage_offset,
                                                                             MemoryUtils::StorageLayout graph_layout) {
            auto        context = std::make_unique<TslMapNodeContext>(TslMapNodeContext{
                .spec           = std::move(spec),
                .storage_offset = storage_offset,
                .graph_layout   = graph_layout,
            });
            const auto *result  = context.get();
            tsl_map_node_contexts().push_back(std::move(context));
            return *result;
        }

        [[nodiscard]] NodeStorageMetrics tsl_map_storage_metrics(
            const void *raw_context, const void *memory) noexcept {
            const auto &context = *static_cast<const TslMapNodeContext *>(raw_context);
            const auto &storage = *MemoryUtils::cast<const TslMapNodeStorage>(
                MemoryUtils::advance(memory, context.storage_offset));
            return NodeStorageMetrics{
                .nested_graph_count = storage.entries.entry_count(),
                .nested_graph_capacity = storage.entries.slot_capacity(),
                .nested_graph_blocks = storage.entries.block_count(),
                .dynamic_live_bytes = storage.entries.live_bytes(),
                .dynamic_reserved_bytes = storage.entries.reserved_bytes(),
            };
        }

        [[nodiscard]] TSOutputHandle effective_output_handle(TSOutputView source) {
            if (!source.bound()) { return {}; }

            TSOutputHandle current = source.handle();
            while (source.forwarding()) {
                TSOutputHandle target = source.forwarding_target();
                if (!target.bound() || target.same_as(current)) { break; }
                current = target;
                source  = target.view(source.evaluation_time());
            }
            return current;
        }

        struct TslMapSourceStatus
        {
            bool        bindings_changed{false};
            std::size_t runtime_size{0};
        };

        [[nodiscard]] TslMapSourceStatus update_tsl_map_sources(const TSInputView &root_input, TslMapNodeStorage &storage,
                                                                const TslMapNodeSpec &spec) {
            const std::size_t outer_count         = root_input.as_bundle().size();
            const bool        handles_initialized = storage.outer_sources.size() == outer_count;
            storage.outer_sources.resize(outer_count);

            TslMapSourceStatus status;
            for (std::size_t index = 0; index < outer_count; ++index) {
                TSOutputHandle current = effective_output_handle(root_input.indexed_child_at(index).bound_output());
                if (!current.same_as(storage.outer_sources[index])) {
                    storage.outer_sources[index] = current;
                    status.bindings_changed      = status.bindings_changed || handles_initialized;
                }
            }

            const bool sizes_initialized = storage.multiplexed_sizes.size() == spec.multiplexed_inputs.size();
            storage.multiplexed_sizes.resize(spec.multiplexed_inputs.size());
            for (std::size_t mux = 0; mux < spec.multiplexed_inputs.size(); ++mux) {
                const std::size_t outer_index = spec.multiplexed_inputs[mux];
                auto              source      = root_input.indexed_child_at(outer_index).bound_output();
                const std::size_t size        = source.bound() ? source.as_list().size() : 0;
                status.runtime_size           = std::max(status.runtime_size, size);
                if (sizes_initialized && size != storage.multiplexed_sizes[mux]) { status.bindings_changed = true; }
                storage.multiplexed_sizes[mux] = size;
            }
            return status;
        }

        void create_tsl_map_entry(const NodeView &view, const TslMapNodeContext &context, TslMapNodeStorage &storage,
                                  std::size_t index, DateTime evaluation_time) {
            const TslMapNodeSpec &spec = context.spec;
            if (index > static_cast<std::size_t>(std::numeric_limits<Int>::max())) {
                throw std::length_error("tsl_map_node index exceeds int64 range");
            }
            if (storage.entries.entry_at(index) != nullptr) {
                throw std::logic_error("tsl_map_node cannot reconstruct a live list index");
            }

            auto &entry    = storage.entries.construct_at(index, index);
            auto  rollback = UnwindCleanupGuard([&] {
                if (entry.graph.has_value() && entry.graph.view().started()) { entry.graph.view().stop(); }
                storage.entries.destroy_at(index);
            });

            entry.graph = spec.child.graph_builder.make_nested_graph(view.pointer(), storage.entries.graph_memory(index),
                                                                     context.graph_layout);
            if (spec.index_output_schema != nullptr) {
                entry.index_source.bind(*spec.index_output_schema, entry.index, evaluation_time);
            }

            const TSOutputView index_source =
                entry.index_source.bound() ? entry.index_source.view(evaluation_time) : TSOutputView{};
            runtime_detail::bind_mapped_child_inputs(view, entry.graph.view(), evaluation_time, spec.child, spec.args,
                                                     entry.index.view(), index_source,
                                                     std::nullopt, false, true);
            runtime_detail::bind_mapped_child_output(view, entry.graph.view(), evaluation_time, spec.child.output_binding,
                                                     spec.args, entry.index.view(), index_source,
                                                     spec.output_binding_mode);
            entry.graph.view().start(evaluation_time);
            schedule_sampled_input_consumers(entry.graph.view(), evaluation_time, spec.child.input_bindings);
            rollback.release();
        }

        void refresh_tsl_map_bindings(const NodeView &view, const TslMapNodeContext &context, TslMapNodeStorage &storage,
                                      DateTime evaluation_time) {
            const auto &spec = context.spec;
            for (std::size_t index = 0; index < storage.live_count; ++index) {
                auto *entry = storage.entries.entry_at(index);
                if (entry == nullptr || !entry->graph.has_value()) { continue; }
                const TSOutputView index_source =
                    entry->index_source.bound() ? entry->index_source.view(evaluation_time) : TSOutputView{};
                runtime_detail::bind_mapped_child_inputs(view, entry->graph.view(), evaluation_time, spec.child, spec.args,
                                                         entry->index.view(), index_source);
            }
        }

        bool tsl_map_evaluate_impl(const void *, const NodeView &view, DateTime evaluation_time) {
            if (!view.started()) { return true; }

            auto        typed   = view.as<TslMapNodeView>();
            const auto &context = *static_cast<const TslMapNodeContext *>(typed.internal_context());
            auto       &storage = *MemoryUtils::cast<TslMapNodeStorage>(typed.internal_storage());
            storage.entries.bind_graph_layout(context.graph_layout);

            const bool resuming         = storage.resume_index != TslMapNodeStorage::npos;
            bool       bindings_changed = false;
            if (!resuming) {
                const TslMapSourceStatus sources = update_tsl_map_sources(view.input(evaluation_time), storage, context.spec);
                bindings_changed                 = sources.bindings_changed;
                storage.entries.reserve_to(sources.runtime_size);
                for (std::size_t index = storage.live_count; index < sources.runtime_size; ++index) {
                    create_tsl_map_entry(view, context, storage, index, evaluation_time);
                    ++storage.live_count;
                }
                if (bindings_changed) { refresh_tsl_map_bindings(view, context, storage, evaluation_time); }
            }

            const std::size_t start_index = resuming ? storage.resume_index : 0;
            for (std::size_t index = start_index; index < storage.live_count; ++index) {
                auto *entry = storage.entries.entry_at(index);
                if (entry == nullptr || !entry->graph.has_value() || !entry->graph.view().started()) { continue; }

                auto       child       = entry->graph.view();
                const bool resume_this = resuming && index == storage.resume_index;
                if (child.next_scheduled_time() <= evaluation_time || resume_this) {
                    if (!child.evaluate(evaluation_time)) {
                        storage.resume_index = index;
                        return false;
                    }
                    // Direct-write terminals already notify the list through
                    // their re-homed output endpoint. Only a preserved child
                    // terminal needs the explicit final-state reconciliation;
                    // doing it for direct writes can expose a partially
                    // evaluated list to same-cycle downstream nodes.
                    if (context.spec.output_binding_mode !=
                        MapOutputBindingMode::ChildTerminalWritesElement)
                    {
                        runtime_detail::finalize_mapped_child_output(
                            view, evaluation_time,
                            context.spec.child.output_binding,
                            entry->index.view());
                    }
                }
                const DateTime next = child.next_scheduled_time();
                if (next != MAX_DT && next > evaluation_time) { view.graph().schedule_node(view.node_index(), next); }
            }
            storage.resume_index = TslMapNodeStorage::npos;
            return true;
        }

        void tsl_map_node_stop(const NodeView &view, DateTime) {
            auto typed = view.as<TslMapNodeView>();
            MemoryUtils::cast<TslMapNodeStorage>(typed.internal_storage())->stop_and_destroy_noexcept();
        }

        void validate_tsl_map_node_spec(const NodeTypeMetaData &meta, const TslMapNodeSpec &spec) {
            const bool has_output = meta.output_schema != nullptr;
            if (spec.child.output_binding.has_value() != has_output) {
                throw std::invalid_argument("tsl_map_node child output binding must be "
                                            "present exactly when the map has an output");
            }
            if (meta.input_schema == nullptr || meta.input_schema->kind != TSTypeKind::TSB) {
                throw std::invalid_argument("tsl_map_node requires a TSB input schema");
            }
            if (has_output && (meta.output_schema->kind != TSTypeKind::TSL || meta.output_schema->fixed_size() != 0)) {
                throw std::invalid_argument("tsl_map_node output must be a dynamic TSL");
            }
            if (spec.multiplexed_inputs.empty()) {
                throw std::invalid_argument("tsl_map_node requires at least one multiplexed dynamic TSL");
            }

            const auto              *input_fields = meta.input_schema->fields();
            std::vector<std::size_t> seen_mux_inputs;
            seen_mux_inputs.reserve(spec.multiplexed_inputs.size());
            for (const std::size_t mux_index : spec.multiplexed_inputs) {
                if (mux_index >= meta.input_schema->field_count()) {
                    throw std::invalid_argument("tsl_map_node multiplexed input index is out of range");
                }
                if (std::find(seen_mux_inputs.begin(), seen_mux_inputs.end(), mux_index) != seen_mux_inputs.end()) {
                    throw std::invalid_argument("tsl_map_node multiplexed input indices must be unique");
                }
                seen_mux_inputs.push_back(mux_index);
                const auto *schema = TypeRegistry::instance().dereference(input_fields[mux_index].type);
                if (schema == nullptr || schema->kind != TSTypeKind::TSL || schema->fixed_size() != 0) {
                    throw std::invalid_argument("tsl_map_node multiplexed input index must "
                                                "select a dynamic TSL input field");
                }
            }

            const std::size_t child_node_count = spec.child.graph_builder.node_count();
            if (spec.child.output_binding.has_value()) {
                const auto &binding = *spec.child.output_binding;
                if (binding.kind != NestedGraphOutputBinding::Kind::ChildOutput) {
                    throw std::invalid_argument("tsl_map_node does not support child pass-through outputs");
                }
                if (binding.source.node >= child_node_count || !binding.source.path.empty() || !binding.target_path.empty()) {
                    throw std::invalid_argument("tsl_map_node child output binding must connect one whole child "
                                                "terminal to the list element");
                }
                if (spec.output_binding_mode ==
                    MapOutputBindingMode::OutputElementForwardsToParentSource) {
                    throw std::invalid_argument(
                        "tsl_map_node child output cannot use parent-source forwarding mode");
                }
            }

            bool index_source_seen   = false;
            bool element_source_seen = false;
            for (const MapArgSource &arg : spec.args) {
                switch (arg.kind) {
                    case MapArgSourceKind::Key: index_source_seen = true; break;
                    case MapArgSourceKind::Element:
                        element_source_seen = true;
                        if (arg.outer_index >= meta.input_schema->field_count() ||
                            std::find(spec.multiplexed_inputs.begin(), spec.multiplexed_inputs.end(), arg.outer_index) ==
                                spec.multiplexed_inputs.end()) {
                            throw std::invalid_argument("tsl_map_node element source must select a "
                                                        "multiplexed dynamic TSL");
                        }
                        break;
                    case MapArgSourceKind::OuterInput:
                        if (arg.outer_index >= meta.input_schema->field_count()) {
                            throw std::invalid_argument("tsl_map_node outer input source index is out of range");
                        }
                        break;
                }
            }
            if (!element_source_seen) { throw std::invalid_argument("tsl_map_node requires an element-sourced child argument"); }

            for (const NestedGraphInputBinding &binding : spec.child.input_bindings) {
                if (binding.source_path.empty() || binding.source_path[0] >= spec.args.size()) {
                    throw std::invalid_argument("tsl_map_node child input binding requires a "
                                                "valid boundary argument ordinal");
                }
                if (binding.target.node >= child_node_count) {
                    throw std::invalid_argument("tsl_map_node child input target node is out of range");
                }
            }

            if (index_source_seen) {
                const auto *index_schema = spec.index_output_schema;
                if (index_schema == nullptr || index_schema->kind != TSTypeKind::TS ||
                    index_schema->value_schema != scalar_descriptor<Int>::value_meta()) {
                    throw std::invalid_argument("tsl_map_node index source must be TS<int64>");
                }
            } else if (spec.index_output_schema != nullptr) {
                throw std::invalid_argument("tsl_map_node index output schema was supplied "
                                            "but no index argument is bound");
            }
        }
    }  // namespace

    const void *TslMapNodeView::node_view_type_id() noexcept {
        static const char token{};
        return &token;
    }

    TslMapNodeView TslMapNodeView::from_node(NodeView view, const void *context) {
        if (context == nullptr) { throw std::logic_error("TslMapNodeView requires a typed view context"); }
        const auto &typed_context = *static_cast<const TslMapNodeContext *>(context);
        void       *storage       = MemoryUtils::advance(view.data(), typed_context.storage_offset);
        return TslMapNodeView{std::move(view), context, storage};
    }

    const NodeView &TslMapNodeView::node() const noexcept { return view_; }

    std::size_t TslMapNodeView::active_count() const noexcept {
        return MemoryUtils::cast<TslMapNodeStorage>(storage_)->active_count();
    }

    std::size_t TslMapNodeView::child_graph_count() const noexcept {
        return MemoryUtils::cast<TslMapNodeStorage>(storage_)->live_count;
    }

    std::size_t TslMapNodeView::child_slot_block_count() const noexcept {
        return MemoryUtils::cast<TslMapNodeStorage>(storage_)->entries.block_count();
    }

    bool TslMapNodeView::child_graphs_use_in_place_storage() const noexcept {
        const auto &storage = *MemoryUtils::cast<TslMapNodeStorage>(storage_);
        for (std::size_t index = 0; index < storage.live_count; ++index) {
            const auto *entry = storage.entries.entry_at(index);
            if (entry != nullptr && entry->graph.has_value() && !entry->graph.uses_external_storage()) { return false; }
        }
        return true;
    }

    TslMapNodeView::TslMapNodeView(NodeView view, const void *context, void *storage) noexcept
        : view_(std::move(view)), context_(context), storage_(storage) {}

    NodeBuilder tsl_map_node(NodeTypeMetaData meta, TslMapNodeSpec spec) {
        validate_tsl_map_node_spec(meta, spec);

        meta.requires_phase_runner =
            meta.requires_phase_runner || spec.child.graph_builder.requires_phase_runner();
        meta.node_kind    = NodeKind::Nested;
        meta.valid_inputs = std::vector<std::size_t>{};
        if (meta.output_schema != nullptr &&
            spec.output_binding_mode !=
                MapOutputBindingMode::ChildTerminalWritesElement) {
            meta.output_endpoint_schema = TSEndpointSchema::non_peered_list(
                meta.output_schema,
                forwarding_output_endpoint_schema(
                    meta.output_schema->element_ts()));
        }

        const GraphTypeRef child_graph_type = spec.child.graph_builder.nested_type();
        const ValueTypeRef index_type       = ValuePlanFactory::instance().type_for(scalar_descriptor<Int>::value_meta());
        if (!child_graph_type || !index_type) { throw std::logic_error("tsl_map_node could not resolve debugger child types"); }

        NodeTypeDescriptor descriptor;
        descriptor.schema = std::move(meta);
        const std::array fields{NodeStorageField{
            .name = tsl_map_storage_field_name,
            .plan = &MemoryUtils::plan_for<TslMapNodeStorage>(),
        }};
        // The entry store destroys before the owned TSL output because child
        // terminal forwarding links point into output elements.
        descriptor.storage_plan = &node_storage_plan_for(descriptor.schema, {}, fields);

        descriptor.callbacks.stop            = &tsl_map_node_stop;
        descriptor.ops.evaluate_impl         = &tsl_map_evaluate_impl;
        descriptor.ops.storage_metrics_impl  = &tsl_map_storage_metrics;
        descriptor.ops.extended_view_type_id = TslMapNodeView::node_view_type_id();

        const MemoryUtils::StorageLayout graph_layout = spec.child.graph_builder.nested_storage_layout();
        TslMapNodeStorage                debug_exemplar;
        debug_exemplar.entries.bind_graph_layout(graph_layout);
        const std::size_t entries_offset = static_cast<std::size_t>(reinterpret_cast<const std::byte *>(&debug_exemplar.entries) -
                                                                    reinterpret_cast<const std::byte *>(&debug_exemplar));
        TslMapEntry       debug_entry{0};
        const std::size_t graph_pointer_offset = static_cast<std::size_t>(reinterpret_cast<const std::byte *>(&debug_entry.graph) -
                                                                          reinterpret_cast<const std::byte *>(&debug_entry)) +
                                                 GraphValue::debug_pointer_offset();
        descriptor.dynamic_debug               = NodeTypeDescriptor::DynamicDebug{
            .key_type     = index_type.record(),
            .element_type = child_graph_type.record(),
            .layout       = debug_exemplar.entries.debug_layout(
                descriptor.storage_plan->component(tsl_map_storage_field_name).offset + entries_offset, graph_pointer_offset, true),
        };
        descriptor.ops.extended_view_context = &register_tsl_map_node_context(
            std::move(spec), descriptor.storage_plan->component(tsl_map_storage_field_name).offset, graph_layout);

        return NodeBuilder::from_descriptor(std::move(descriptor));
    }
}  // namespace hgraph
