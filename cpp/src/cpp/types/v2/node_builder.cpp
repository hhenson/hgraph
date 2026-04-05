#include <hgraph/types/time_series/ts_output_builder.h>
#include <hgraph/types/v2/node_builder.h>
#include <hgraph/util/scope.h>

#include <algorithm>
#include <cassert>
#include <cstddef>
#include <cstring>
#include <memory>
#include <new>
#include <stdexcept>

namespace hgraph::v2
{
    namespace
    {
        [[nodiscard]] constexpr size_t align_up(size_t value, size_t alignment) noexcept
        {
            if (alignment == 0) { return value; }
            const size_t remainder = value % alignment;
            return remainder == 0 ? value : value + (alignment - remainder);
        }

        struct ResolvedNodeBuilders
        {
            const TSInputBuilder *input_builder{nullptr};
            const TSOutputBuilder *output_builder{nullptr};
            const ValueBuilder *state_builder{nullptr};
            const TSOutputBuilder *recordable_state_builder{nullptr};
        };

        struct NodeMemoryLayout
        {
            size_t total_size{0};
            size_t alignment{alignof(Node)};
            size_t spec_offset{0};
            size_t runtime_data_offset{0};
            size_t label_offset{0};
            size_t active_slots_offset{0};
            size_t valid_slots_offset{0};
            size_t all_valid_slots_offset{0};
            size_t input_object_offset{0};
            size_t input_storage_offset{0};
            size_t output_object_offset{0};
            size_t output_storage_offset{0};
            size_t state_storage_offset{0};
            size_t recordable_state_object_offset{0};
            size_t recordable_state_storage_offset{0};
        };

        [[nodiscard]] ResolvedNodeBuilders resolve_builders(const TSMeta *input_schema,
                                                            const TSMeta *output_schema,
                                                            bool has_state,
                                                            const value::TypeMeta *state_schema,
                                                            bool has_recordable_state,
                                                            const TSMeta *recordable_state_schema,
                                                            const std::vector<TSInputConstructionEdge> &inbound_edges)
        {
            ResolvedNodeBuilders builders;

            if (input_schema != nullptr) {
                const TSInputConstructionPlan plan = TSInputConstructionPlanCompiler::compile(*input_schema, inbound_edges);
                builders.input_builder = &TSInputBuilderFactory::checked_builder_for(plan);
            } else if (!inbound_edges.empty()) {
                throw std::invalid_argument("v2 nodes without an input schema cannot accept inbound edges");
            }

            if (output_schema != nullptr) { builders.output_builder = &TSOutputBuilderFactory::checked_builder_for(output_schema); }
            if (has_state && state_schema == nullptr) {
                throw std::invalid_argument("v2 nodes with typed State<...> require a resolved value schema");
            }
            if (has_state && state_schema != nullptr) {
                builders.state_builder = &ValueBuilderFactory::checked_builder_for(state_schema);
            }
            if (has_recordable_state && recordable_state_schema == nullptr) {
                throw std::invalid_argument("v2 nodes with RecordableState<...> require a resolved time-series schema");
            }
            if (recordable_state_schema != nullptr) {
                builders.recordable_state_builder = &TSOutputBuilderFactory::checked_builder_for(recordable_state_schema);
            }

            return builders;
        }

        [[nodiscard]] NodeMemoryLayout describe_layout(std::string_view label,
                                                       std::span<const size_t> active_inputs,
                                                       std::span<const size_t> valid_inputs,
                                                       std::span<const size_t> all_valid_inputs,
                                                       bool has_state,
                                                       const ResolvedNodeBuilders &builders) noexcept
        {
            NodeMemoryLayout layout;
            layout.alignment = std::max(alignof(Node), alignof(BuiltNodeSpec));

            // Each node occupies one contiguous chunk. The chunk starts with
            // the type-erased Node header and then packs the spec, runtime
            // payload, optional local state, copied selector metadata, and TS
            // endpoint objects/storage.
            size_t offset = sizeof(Node);
            offset = align_up(offset, alignof(BuiltNodeSpec));
            layout.spec_offset = offset;
            offset += sizeof(BuiltNodeSpec);

            layout.alignment = std::max(layout.alignment, alignof(detail::StaticNodeRuntimeData));
            offset = align_up(offset, alignof(detail::StaticNodeRuntimeData));
            layout.runtime_data_offset = offset;
            offset += sizeof(detail::StaticNodeRuntimeData);

            if (builders.state_builder != nullptr) {
                layout.alignment = std::max(layout.alignment, builders.state_builder->alignment());
                offset = align_up(offset, builders.state_builder->alignment());
                layout.state_storage_offset = offset;
                offset += builders.state_builder->size();
            }

            if (!label.empty()) {
                layout.label_offset = offset;
                offset += label.size();
            }

            layout.alignment = std::max(layout.alignment, alignof(size_t));
            offset = align_up(offset, alignof(size_t));
            layout.active_slots_offset = offset;
            offset += sizeof(size_t) * active_inputs.size();

            offset = align_up(offset, alignof(size_t));
            layout.valid_slots_offset = offset;
            offset += sizeof(size_t) * valid_inputs.size();

            offset = align_up(offset, alignof(size_t));
            layout.all_valid_slots_offset = offset;
            offset += sizeof(size_t) * all_valid_inputs.size();

            if (builders.input_builder != nullptr) {
                layout.alignment = std::max(layout.alignment, alignof(TSInput));
                offset = align_up(offset, alignof(TSInput));
                layout.input_object_offset = offset;
                offset += sizeof(TSInput);

                layout.alignment = std::max(layout.alignment, builders.input_builder->alignment());
                offset = align_up(offset, builders.input_builder->alignment());
                layout.input_storage_offset = offset;
                offset += builders.input_builder->size();
            }

            if (builders.output_builder != nullptr) {
                layout.alignment = std::max(layout.alignment, alignof(TSOutput));
                offset = align_up(offset, alignof(TSOutput));
                layout.output_object_offset = offset;
                offset += sizeof(TSOutput);

                layout.alignment = std::max(layout.alignment, builders.output_builder->alignment());
                offset = align_up(offset, builders.output_builder->alignment());
                layout.output_storage_offset = offset;
                offset += builders.output_builder->size();
            }

            if (builders.recordable_state_builder != nullptr) {
                layout.alignment = std::max(layout.alignment, alignof(TSOutput));
                offset = align_up(offset, alignof(TSOutput));
                layout.recordable_state_object_offset = offset;
                offset += sizeof(TSOutput);

                layout.alignment = std::max(layout.alignment, builders.recordable_state_builder->alignment());
                offset = align_up(offset, builders.recordable_state_builder->alignment());
                layout.recordable_state_storage_offset = offset;
                offset += builders.recordable_state_builder->size();
            }

            layout.total_size = offset;
            return layout;
        }

        [[nodiscard]] std::span<const size_t> materialize_slots(std::byte *base,
                                                                std::span<const size_t> source,
                                                                size_t slots_offset)
        {
            if (source.empty()) { return {}; }

            auto *slots = reinterpret_cast<size_t *>(base + slots_offset);
            std::copy(source.begin(), source.end(), slots);
            return {slots, source.size()};
        }

        void destruct_static_node(Node &node) noexcept
        {
            const BuiltNodeSpec &spec = node.spec();
            auto &runtime_data = detail::runtime_data<detail::StaticNodeRuntimeData>(node);

            // Destruct in reverse construction order for the objects we placed
            // into the node chunk manually.
            if (runtime_data.python_scalars.is_valid()) {
                nb::gil_scoped_acquire guard;
                std::destroy_at(&runtime_data.python_scalars);
            } else {
                std::destroy_at(&runtime_data.python_scalars);
            }
            if (runtime_data.recordable_state != nullptr) { runtime_data.recordable_state->~TSOutput(); }
            if (runtime_data.state_builder != nullptr && runtime_data.state_memory != nullptr) {
                runtime_data.state_builder->destruct(runtime_data.state_memory);
            }
            if (runtime_data.output != nullptr) { runtime_data.output->~TSOutput(); }
            if (runtime_data.input != nullptr) { runtime_data.input->~TSInput(); }

            std::destroy_at(const_cast<BuiltNodeSpec *>(&spec));
            std::destroy_at(&runtime_data);
        }
    }  // namespace

    NodeBuilder &NodeBuilder::label(std::string value)
    {
        m_label = std::move(value);
        return *this;
    }

    NodeBuilder &NodeBuilder::python_signature(nb::object value)
    {
        m_python_signature = std::move(value);
        return *this;
    }

    NodeBuilder &NodeBuilder::python_scalars(nb::dict value)
    {
        m_python_scalars = std::move(value);
        return *this;
    }

    NodeBuilder &NodeBuilder::python_input_builder(nb::object value)
    {
        m_python_input_builder = std::move(value);
        return *this;
    }

    NodeBuilder &NodeBuilder::python_output_builder(nb::object value)
    {
        m_python_output_builder = std::move(value);
        return *this;
    }

    NodeBuilder &NodeBuilder::python_error_builder(nb::object value)
    {
        m_python_error_builder = std::move(value);
        return *this;
    }

    NodeBuilder &NodeBuilder::python_recordable_state_builder(nb::object value)
    {
        m_python_recordable_state_builder = std::move(value);
        return *this;
    }

    NodeBuilder &NodeBuilder::implementation_name(std::string value)
    {
        m_implementation_name = std::move(value);
        return *this;
    }

    NodeBuilder &NodeBuilder::requires_resolved_schemas(bool value) noexcept
    {
        m_requires_resolved_schemas = value;
        return *this;
    }

    NodeBuilder &NodeBuilder::node_type(NodeTypeEnum value)
    {
        m_node_type = value;
        m_has_explicit_node_type = true;
        validate_push_source_contract();
        return *this;
    }

    NodeBuilder &NodeBuilder::input_schema(const TSMeta *value)
    {
        m_input_schema = value;
        return *this;
    }

    NodeBuilder &NodeBuilder::output_schema(const TSMeta *value)
    {
        m_output_schema = value;
        return *this;
    }

    NodeBuilder &NodeBuilder::active_input(size_t slot)
    {
        m_active_inputs.emplace_back(slot);
        return *this;
    }

    NodeBuilder &NodeBuilder::valid_input(size_t slot)
    {
        m_valid_inputs.emplace_back(slot);
        return *this;
    }

    NodeBuilder &NodeBuilder::all_valid_input(size_t slot)
    {
        m_all_valid_inputs.emplace_back(slot);
        return *this;
    }

    size_t NodeBuilder::size(const std::vector<TSInputConstructionEdge> &inbound_edges) const
    {
        assert(m_runtime_ops != nullptr);
        assert(m_node_type != NodeTypeEnum::PUSH_SOURCE_NODE || m_has_push_message_hook);
        assert(m_node_type != NodeTypeEnum::PUSH_SOURCE_NODE || m_push_source_runtime_ops != nullptr);
        const auto builders = resolve_builders(
            m_input_schema,
            m_output_schema,
            m_has_state,
            m_state_schema,
            m_has_recordable_state,
            m_recordable_state_schema,
            inbound_edges);
        return describe_layout(
                   m_label, m_active_inputs, m_valid_inputs, m_all_valid_inputs, m_has_state, builders)
            .total_size;
    }

    size_t NodeBuilder::alignment(const std::vector<TSInputConstructionEdge> &inbound_edges) const
    {
        assert(m_runtime_ops != nullptr);
        assert(m_node_type != NodeTypeEnum::PUSH_SOURCE_NODE || m_has_push_message_hook);
        assert(m_node_type != NodeTypeEnum::PUSH_SOURCE_NODE || m_push_source_runtime_ops != nullptr);
        const auto builders = resolve_builders(
            m_input_schema,
            m_output_schema,
            m_has_state,
            m_state_schema,
            m_has_recordable_state,
            m_recordable_state_schema,
            inbound_edges);
        return describe_layout(
                   m_label, m_active_inputs, m_valid_inputs, m_all_valid_inputs, m_has_state, builders)
            .alignment;
    }

    Node *NodeBuilder::construct_at(void *memory,
                                    int64_t node_index,
                                    const std::vector<TSInputConstructionEdge> &inbound_edges) const
    {
        if (memory == nullptr) { throw std::invalid_argument("v2 node builder requires non-null construction memory"); }
        assert(m_runtime_ops != nullptr);
        assert(m_node_type != NodeTypeEnum::PUSH_SOURCE_NODE || m_has_push_message_hook);
        assert(m_node_type != NodeTypeEnum::PUSH_SOURCE_NODE || m_push_source_runtime_ops != nullptr);

        const auto builders = resolve_builders(
            m_input_schema,
            m_output_schema,
            m_has_state,
            m_state_schema,
            m_has_recordable_state,
            m_recordable_state_schema,
            inbound_edges);
        const auto layout = describe_layout(
            m_label, m_active_inputs, m_valid_inputs, m_all_valid_inputs, m_has_state, builders);
        auto *base = static_cast<std::byte *>(memory);

        std::string_view label_view;
        if (!m_label.empty()) {
            auto *label_ptr = reinterpret_cast<char *>(base + layout.label_offset);
            std::memcpy(label_ptr, m_label.data(), m_label.size());
            label_view = std::string_view{label_ptr, m_label.size()};
        }

        const auto active_inputs = materialize_slots(base, m_active_inputs, layout.active_slots_offset);
        const auto valid_inputs = materialize_slots(base, m_valid_inputs, layout.valid_slots_offset);
        const auto all_valid_inputs = materialize_slots(base, m_all_valid_inputs, layout.all_valid_slots_offset);

        TSInput *input = nullptr;
        TSOutput *output = nullptr;
        void *state_memory = nullptr;
        TSOutput *recordable_state = nullptr;
        auto cleanup_input = UnwindCleanupGuard([&] {
            if (input != nullptr) { input->~TSInput(); }
        });
        auto cleanup_output = UnwindCleanupGuard([&] {
            if (output != nullptr) { output->~TSOutput(); }
        });
        auto cleanup_state = UnwindCleanupGuard([&] {
            if (builders.state_builder != nullptr && state_memory != nullptr) { builders.state_builder->destruct(state_memory); }
        });
        auto cleanup_recordable_state = UnwindCleanupGuard([&] {
            if (recordable_state != nullptr) { recordable_state->~TSOutput(); }
        });
        detail::StaticNodeRuntimeData *runtime_data = nullptr;
        auto cleanup_runtime_data = UnwindCleanupGuard([&] {
            if (runtime_data != nullptr) { std::destroy_at(runtime_data); }
        });
        BuiltNodeSpec *spec = nullptr;
        auto cleanup_spec = UnwindCleanupGuard([&] {
            if (spec != nullptr) { std::destroy_at(spec); }
        });

        if (builders.input_builder != nullptr) {
            input = new (base + layout.input_object_offset) TSInput{};
            builders.input_builder->construct_input(
                *input, base + layout.input_storage_offset, TSInputBuilder::MemoryOwnership::External);
        }

        if (builders.output_builder != nullptr) {
            output = new (base + layout.output_object_offset) TSOutput{};
            builders.output_builder->construct_output(
                *output, base + layout.output_storage_offset, TSOutputBuilder::MemoryOwnership::External);
        }

        if (builders.state_builder != nullptr) {
            state_memory = base + layout.state_storage_offset;
            builders.state_builder->construct(state_memory);
        }

        if (builders.recordable_state_builder != nullptr) {
            recordable_state = new (base + layout.recordable_state_object_offset) TSOutput{};
            builders.recordable_state_builder->construct_output(
                *recordable_state,
                base + layout.recordable_state_storage_offset,
                TSOutputBuilder::MemoryOwnership::External);
        }

        runtime_data = new (base + layout.runtime_data_offset)
            detail::StaticNodeRuntimeData{input,
                                          output,
                                          builders.state_builder,
                                          state_memory,
                                          recordable_state,
                                          m_python_scalars.is_valid()
                                              ? nb::borrow(m_python_scalars)
                                              : nb::object()};

        spec = new (base + layout.spec_offset) BuiltNodeSpec{
            m_runtime_ops,
            m_push_source_runtime_ops,
            &destruct_static_node,
            layout.runtime_data_offset,
            label_view,
            m_node_type,
            m_input_schema,
            m_output_schema,
            active_inputs,
            valid_inputs,
            all_valid_inputs,
        };

        auto *node = new (memory) Node(node_index, spec);
        cleanup_runtime_data.complete();
        cleanup_spec.complete();
        return node;
    }

    void NodeBuilder::destruct_at(Node &node) const noexcept
    {
        node.destruct();
    }
}  // namespace hgraph::v2
