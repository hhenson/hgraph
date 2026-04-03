#pragma once

#include <hgraph/hgraph_base.h>
#include <hgraph/types/time_series/ts_input_builder.h>
#include <hgraph/types/time_series/value/builder.h>
#include <hgraph/types/v2/node_impl.h>
#include <hgraph/types/v2/node.h>
#include <hgraph/types/v2/python_export.h>
#include <hgraph/types/v2/static_signature.h>

#include <cstddef>
#include <stdexcept>
#include <string>
#include <string_view>
#include <type_traits>
#include <vector>

namespace hgraph::v2
{
    /**
     * Fluent builder for a single runtime node chunk.
     *
     * NodeBuilder resolves the static implementation metadata, computes the
     * memory layout for one node, and placement-constructs the final chunk:
     * Node header, BuiltNodeSpec, node-local runtime payload, TS endpoints,
     * typed state, and copied selector metadata.
     */
    struct HGRAPH_EXPORT NodeBuilder
    {
        NodeBuilder() = default;

        NodeBuilder &label(std::string value);
        NodeBuilder &input_schema(const TSMeta *value);
        NodeBuilder &output_schema(const TSMeta *value);
        NodeBuilder &active_input(size_t slot);
        NodeBuilder &valid_input(size_t slot);
        NodeBuilder &all_valid_input(size_t slot);

        template <typename TImplementation>
        NodeBuilder &implementation()
        {
            // Static implementations are pure compile-time descriptors. They
            // are never instantiated and must not carry runtime state.
            static_assert(std::is_class_v<TImplementation>, "Node implementations must be struct/class types");
            static_assert(std::is_empty_v<TImplementation>, "Node implementations must be stateless");
            static_assert(!std::is_polymorphic_v<TImplementation>, "Node implementations must not be polymorphic");

            using signature = StaticNodeSignature<TImplementation>;

            if (m_input_schema == nullptr) { m_input_schema = signature::input_schema(); }
            if (m_output_schema == nullptr) { m_output_schema = StaticNodeSignature<TImplementation>::output_schema(); }
            if (!m_has_state && signature::has_state()) {
                m_has_state = true;
                m_state_schema = signature::state_schema();
            }
            if (!m_has_recordable_state && signature::has_recordable_state()) {
                m_has_recordable_state = true;
                m_recordable_state_schema = signature::recordable_state_schema();
            }

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

            m_runtime_ops = &detail::runtime_ops_for<TImplementation>::value;
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
        [[nodiscard]] size_t slot_for_input_name(std::string_view name) const
        {
            if (m_input_schema == nullptr || m_input_schema->kind != TSKind::TSB) {
                throw std::logic_error("Named input selectors require a TSB root input schema");
            }

            for (size_t i = 0; i < m_input_schema->field_count(); ++i) {
                if (m_input_schema->fields()[i].name == name) { return i; }
            }

            throw std::invalid_argument("Named input selector was not found in the inferred input schema");
        }

        std::string m_label;
        const TSMeta *m_input_schema{nullptr};
        const TSMeta *m_output_schema{nullptr};
        bool m_has_state{false};
        const value::TypeMeta *m_state_schema{nullptr};
        bool m_has_recordable_state{false};
        const TSMeta *m_recordable_state_schema{nullptr};
        std::vector<size_t> m_active_inputs;
        std::vector<size_t> m_valid_inputs;
        std::vector<size_t> m_all_valid_inputs;
        const NodeRuntimeOps *m_runtime_ops{nullptr};
    };
}  // namespace hgraph::v2
