#pragma once

#include <hgraph/hgraph_base.h>
#include <hgraph/types/time_series/ts_input_builder.h>
#include <hgraph/types/time_series/value/builder.h>
#include <hgraph/types/v2/node_impl.h>
#include <hgraph/types/v2/node.h>
#include <hgraph/types/v2/static_signature.h>

#include <cstddef>
#include <memory>
#include <stdexcept>
#include <string>
#include <string_view>
#include <type_traits>
#include <vector>

namespace hgraph::v2
{
    struct NodeBuilder;

    namespace detail
    {
        template <typename TState>
        [[nodiscard]] const TState &node_builder_type_state(const NodeBuilder &builder);
    }

    template <typename TImplementation>
    void export_compute_node(nb::module_ &m, std::string_view name = {});

    /**
     * Fluent builder for a single runtime node chunk.
     *
     * The public builder surface is uniform, but the family-specific builder
     * state and operations are type-erased behind an ops table. Static nodes
     * and Python-backed nodes therefore share one builder type without
     * carrying each other's bespoke state inline.
     */
    struct HGRAPH_EXPORT NodeBuilder
    {
        NodeBuilder() = default;

        NodeBuilder &label(std::string value);
        [[nodiscard]] std::string_view label() const noexcept { return m_label; }

        /** Override the reflected node type, e.g. push/pull source classification. */
        NodeBuilder &node_type(NodeTypeEnum value);
        [[nodiscard]] NodeTypeEnum node_type() const noexcept { return m_node_type; }

        NodeBuilder &input_schema(const TSMeta *value);
        [[nodiscard]] const TSMeta *input_schema() const noexcept { return m_input_schema; }

        NodeBuilder &output_schema(const TSMeta *value);
        [[nodiscard]] const TSMeta *output_schema() const noexcept { return m_output_schema; }

        NodeBuilder &recordable_state_schema(const TSMeta *value);
        [[nodiscard]] const TSMeta *recordable_state_schema() const noexcept { return m_recordable_state_schema; }

        NodeBuilder &active_input(size_t slot);
        NodeBuilder &valid_input(size_t slot);
        NodeBuilder &all_valid_input(size_t slot);

        [[nodiscard]] const std::vector<size_t> &active_inputs() const noexcept { return m_active_inputs; }
        [[nodiscard]] const std::vector<size_t> &valid_inputs() const noexcept { return m_valid_inputs; }
        [[nodiscard]] const std::vector<size_t> &all_valid_inputs() const noexcept { return m_all_valid_inputs; }

        NodeBuilder &python_signature(nb::object value);
        NodeBuilder &python_scalars(nb::dict value);
        NodeBuilder &python_input_builder(nb::object value);
        NodeBuilder &python_output_builder(nb::object value);
        NodeBuilder &python_error_builder(nb::object value);
        NodeBuilder &python_recordable_state_builder(nb::object value);
        NodeBuilder &python_implementation(
            nb::object eval_fn,
            nb::object start_fn = nb::none(),
            nb::object stop_fn = nb::none());
        NodeBuilder &implementation_name(std::string value);
        NodeBuilder &uses_scheduler(bool value) noexcept;
        NodeBuilder &requires_resolved_schemas(bool value) noexcept;

        [[nodiscard]] const nb::object &signature() const noexcept { return m_python_signature; }
        [[nodiscard]] const nb::object &scalars() const noexcept { return m_python_scalars; }
        [[nodiscard]] const nb::object &input_builder() const noexcept { return m_python_input_builder; }
        [[nodiscard]] const nb::object &output_builder() const noexcept { return m_python_output_builder; }
        [[nodiscard]] const nb::object &error_builder() const noexcept { return m_python_error_builder; }
        [[nodiscard]] const nb::object &recordable_state_builder() const noexcept { return m_python_recordable_state_builder; }
        [[nodiscard]] const std::string &implementation_name() const noexcept { return m_implementation_name; }
        [[nodiscard]] bool uses_scheduler() const noexcept { return m_uses_scheduler; }
        [[nodiscard]] bool requires_resolved_schemas() const noexcept { return m_requires_resolved_schemas; }
        [[nodiscard]] bool has_state() const noexcept { return m_has_state; }
        [[nodiscard]] const value::TypeMeta *state_schema() const noexcept { return m_state_schema; }

        template <typename TImplementation>
        NodeBuilder &implementation()
        {
            static_assert(std::is_class_v<TImplementation>, "Node implementations must be struct/class types");
            static_assert(std::is_empty_v<TImplementation>, "Node implementations must be stateless");
            static_assert(!std::is_polymorphic_v<TImplementation>, "Node implementations must not be polymorphic");

            using signature = StaticNodeSignature<TImplementation>;
            static_assert(
                signature::node_type() != NodeTypeEnum::PUSH_SOURCE_NODE || detail::HasApplyMessage<TImplementation>,
                "v2 push source nodes require a static bool apply_message(...) hook");

            if (!m_has_explicit_node_type) { m_node_type = signature::node_type(); }
            if (m_input_schema == nullptr) { m_input_schema = signature::input_schema(); }
            if (m_output_schema == nullptr) { m_output_schema = signature::output_schema(); }
            if (!m_has_state && signature::has_state()) {
                m_has_state = true;
                m_state_schema = signature::state_schema();
            }
            if (!m_has_recordable_state && signature::has_recordable_state()) {
                m_has_recordable_state = true;
                m_recordable_state_schema = signature::recordable_state_schema();
            }
            m_uses_scheduler = signature::has_scheduler();

            if (m_active_inputs.empty()) {
                for (const auto &name : signature::active_input_names()) { m_active_inputs.push_back(slot_for_input_name(name)); }
            }

            if (m_valid_inputs.empty()) {
                for (const auto &name : signature::valid_input_names()) { m_valid_inputs.push_back(slot_for_input_name(name)); }
            }

            if (m_all_valid_inputs.empty()) {
                for (const auto &name : signature::all_valid_input_names()) {
                    m_all_valid_inputs.push_back(slot_for_input_name(name));
                }
            }

            set_type_state(
                detail::runtime_ops_for<TImplementation>::value,
                detail::HasApplyMessage<TImplementation>
                    ? &detail::runtime_ops_for<TImplementation>::push_source_value
                    : nullptr,
                detail::HasApplyMessage<TImplementation>);
            return *this;
        }

        template <typename TImplementation>
        static void export_compute_node(nb::module_ &m, std::string_view name = {})
        {
            v2::export_compute_node<TImplementation>(m, name);
        }

        [[nodiscard]] size_t size(const std::vector<TSInputConstructionEdge> &inbound_edges) const;
        [[nodiscard]] size_t alignment(const std::vector<TSInputConstructionEdge> &inbound_edges) const;
        [[nodiscard]] Node *construct_at(void *memory,
                                         int64_t node_index,
                                         const std::vector<TSInputConstructionEdge> &inbound_edges) const;
        void destruct_at(Node &node) const noexcept;

      private:
        friend struct GraphBuilder;
        template <typename TState>
        friend const TState &detail::node_builder_type_state(const NodeBuilder &builder);

        struct TypeOps
        {
            void (*validate_contract)(const NodeBuilder &builder);
            [[nodiscard]] size_t (*size)(const NodeBuilder &builder,
                                         const std::vector<TSInputConstructionEdge> &inbound_edges);
            [[nodiscard]] size_t (*alignment)(const NodeBuilder &builder,
                                              const std::vector<TSInputConstructionEdge> &inbound_edges);
            [[nodiscard]] Node *(*construct_at)(const NodeBuilder &builder,
                                                void *memory,
                                                int64_t node_index,
                                                const std::vector<TSInputConstructionEdge> &inbound_edges);
            void (*destruct_at)(const NodeBuilder &builder, Node &node) noexcept;
        };

        template <typename TState>
        [[nodiscard]] const TState &type_state() const
        {
            if (!m_type_state) { throw std::logic_error("v2 node builder type state was not configured"); }
            return *static_cast<const TState *>(m_type_state.get());
        }

        void validate_complete() const
        {
            if (m_type_ops == nullptr || !m_type_state) {
                throw std::invalid_argument("v2 node builder requires an implementation to be set");
            }
            m_type_ops->validate_contract(*this);
        }

        void validate_named_selector_schema(std::string_view selector_name) const
        {
            if (m_input_schema == nullptr || m_input_schema->kind != TSKind::TSB) {
                throw std::logic_error(std::string{selector_name} + " selectors require a TSB root input schema");
            }
        }

        [[nodiscard]] size_t slot_for_input_name(std::string_view name) const
        {
            validate_named_selector_schema("Named input");

            for (size_t i = 0; i < m_input_schema->field_count(); ++i) {
                if (m_input_schema->fields()[i].name == name) { return i; }
            }

            throw std::invalid_argument("Named input selector was not found in the inferred input schema");
        }

        void set_type_state(const NodeRuntimeOps &runtime_ops,
                            const PushSourceNodeRuntimeOps *push_source_runtime_ops,
                            bool has_push_message_hook);
        void set_python_type_state(nb::object eval_fn, nb::object start_fn, nb::object stop_fn);
        [[nodiscard]] static const TypeOps &static_type_ops();
        [[nodiscard]] static const TypeOps &python_type_ops();

        std::string m_label;
        NodeTypeEnum m_node_type{NodeTypeEnum::COMPUTE_NODE};
        bool m_has_explicit_node_type{false};
        const TSMeta *m_input_schema{nullptr};
        const TSMeta *m_output_schema{nullptr};
        bool m_has_state{false};
        const value::TypeMeta *m_state_schema{nullptr};
        bool m_has_recordable_state{false};
        const TSMeta *m_recordable_state_schema{nullptr};
        bool m_uses_scheduler{false};
        std::vector<size_t> m_active_inputs;
        std::vector<size_t> m_valid_inputs;
        std::vector<size_t> m_all_valid_inputs;
        nb::object m_python_signature;
        nb::object m_python_scalars;
        nb::object m_python_input_builder;
        nb::object m_python_output_builder;
        nb::object m_python_error_builder;
        nb::object m_python_recordable_state_builder;
        std::string m_implementation_name;
        bool m_requires_resolved_schemas{false};

        const TypeOps *m_type_ops{nullptr};
        std::shared_ptr<const void> m_type_state;
    };
}  // namespace hgraph::v2
