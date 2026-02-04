#include <hgraph/types/node.h>
#include <hgraph/types/time_series_type.h>
#include <hgraph/types/tsb.h>
#include <hgraph/types/time_series/ts_meta.h>

#include <hgraph/builders/builder.h>
#include <hgraph/builders/graph_builder.h>
#include <hgraph/builders/input_builder.h>
#include <hgraph/builders/node_builder.h>
#include <hgraph/builders/output_builder.h>

// Include all the extracted builder headers
#include <hgraph/builders/nodes/python_node_builder.h>
#include <hgraph/builders/nodes/python_generator_node_builder.h>
#include <hgraph/builders/nodes/tsd_map_node_builder.h>
#include <hgraph/builders/nodes/reduce_node_builder.h>
#include <hgraph/builders/nodes/context_node_builder.h>
#include <hgraph/builders/nodes/base_nested_graph_node_builder.h>
#include <hgraph/builders/nodes/nested_graph_node_builder.h>
#include <hgraph/builders/nodes/component_node_builder.h>
#include <hgraph/builders/nodes/try_except_node_builder.h>
#include <hgraph/builders/nodes/switch_node_builder.h>
#include <hgraph/builders/nodes/tsd_non_associative_reduce_node_builder.h>
#include <hgraph/builders/nodes/mesh_node_builder.h>
#include <hgraph/builders/nodes/last_value_pull_node_builder.h>

#include <utility>

namespace hgraph {
    NodeBuilder::NodeBuilder(node_signature_s_ptr signature_, nb::dict scalars_,
                             std::optional<input_builder_s_ptr> input_builder_,
                             std::optional<output_builder_s_ptr> output_builder_,
                             std::optional<output_builder_s_ptr> error_builder_,
                             std::optional<output_builder_s_ptr> recordable_state_builder_)
        : signature(std::move(signature_)), scalars(std::move(scalars_)), input_builder(std::move(input_builder_)),
          output_builder(std::move(output_builder_)), error_builder(std::move(error_builder_)),
          recordable_state_builder(std::move(recordable_state_builder_)) {
    }

    NodeBuilder::NodeBuilder(NodeBuilder &&other) noexcept
        : signature(other.signature), scalars(std::move(other.scalars)), input_builder(other.input_builder),
          output_builder(other.output_builder), error_builder(other.error_builder),
          recordable_state_builder(other.recordable_state_builder) {
    }

    NodeBuilder &NodeBuilder::operator=(NodeBuilder &&other) noexcept {
        if (this != &other) {
            // Copy nanobind::ref members (inc_ref) instead of moving them, so both sides stay valid
            signature = other.signature;
            scalars = std::move(other.scalars);
            input_builder = other.input_builder;
            output_builder = other.output_builder;
            error_builder = other.error_builder;
            recordable_state_builder = other.recordable_state_builder;
        }
        return *this;
    }

    void NodeBuilder::release_instance(const node_s_ptr &item) const {
        // TSInput/TSOutput are stored inline in the Node and cleaned up automatically by Node's destructor
        // No explicit release needed
        dispose_component(*item);
    }

    size_t NodeBuilder::node_type_size() const {
        // Default implementation returns sizeof(Node)
        // Concrete builders should override this to return the size of their specific node type
        return sizeof(Node);
    }

    size_t NodeBuilder::node_type_alignment() const {
        // Default implementation returns alignof(Node)
        // Concrete builders should override this to return the alignment of their specific node type
        return alignof(Node);
    }

    size_t NodeBuilder::type_alignment() const {
        return node_type_alignment();
    }

    size_t NodeBuilder::memory_size() const {
        // Use node_type_size() to get the correct size for the concrete node type
        size_t total = add_canary_size(node_type_size());
        // Align and add each time-series builder's size using the builder's actual type alignment
        if (input_builder) {
            total = align_size(total, (*input_builder)->type_alignment());
            total += (*input_builder)->memory_size();
        }
        if (output_builder) {
            total = align_size(total, (*output_builder)->type_alignment());
            total += (*output_builder)->memory_size();
        }
        if (error_builder) {
            total = align_size(total, (*error_builder)->type_alignment());
            total += (*error_builder)->memory_size();
        }
        if (recordable_state_builder) {
            total = align_size(total, (*recordable_state_builder)->type_alignment());
            total += (*recordable_state_builder)->memory_size();
        }
        return total;
    }

    const TSMeta* NodeBuilder::input_meta() const {
        if (input_builder.has_value()) {
            return (*input_builder)->ts_meta();
        }
        return nullptr;
    }

    const TSMeta* NodeBuilder::output_meta() const {
        if (output_builder.has_value()) {
            return (*output_builder)->ts_meta();
        }
        return nullptr;
    }

    const TSMeta* NodeBuilder::error_output_meta() const {
        if (error_builder.has_value()) {
            return (*error_builder)->ts_meta();
        }
        return nullptr;
    }

    const TSMeta* NodeBuilder::recordable_state_meta() const {
        if (recordable_state_builder.has_value()) {
            return (*recordable_state_builder)->ts_meta();
        }
        return nullptr;
    }

    void NodeBuilder::register_with_nanobind(nb::module_ &m) {
        nb::class_ < NodeBuilder, Builder > (m, "NodeBuilder")
                .def("make_instance", &NodeBuilder::make_instance, "owning_graph_id"_a, "node_ndx"_a)
                .def("release_instance", &NodeBuilder::release_instance, "node"_a)
                .def_ro("signature", &NodeBuilder::signature)
                .def_ro("scalars", &NodeBuilder::scalars)
                .def_ro("input_builder", &NodeBuilder::input_builder)
                .def_ro("output_builder", &NodeBuilder::output_builder)
                .def_ro("error_builder", &NodeBuilder::error_builder)
                .def_ro("recordable_state_builder", &NodeBuilder::recordable_state_builder)
                .def("__str__", [](const NodeBuilder &self) {
                    return fmt::format("NodeBuilder@{:p}[sig={}]",
                                       static_cast<const void *>(&self), self.signature->name);
                })
                .def("__repr__", [](const NodeBuilder &self) {
                    return fmt::format("NodeBuilder@{:p}[sig={}]",
                                       static_cast<const void *>(&self), self.signature->name);
                });

        nb::class_<BaseNodeBuilder, NodeBuilder>(m, "BaseNodeBuilder");

        // Register all the extracted builder classes
        python_node_builder_register_with_nanobind(m);
        python_generator_node_builder_register_with_nanobind(m);
        tsd_map_node_builder_register_with_nanobind(m);
        reduce_node_builder_register_with_nanobind(m);
        context_node_builder_register_with_nanobind(m);
        base_nested_graph_node_builder_register_with_nanobind(m);
        nested_graph_node_builder_register_with_nanobind(m);
        component_node_builder_register_with_nanobind(m);
        try_except_node_builder_register_with_nanobind(m);
        switch_node_builder_register_with_nanobind(m);
        tsd_non_associative_reduce_node_builder_register_with_nanobind(m);
        mesh_node_builder_register_with_nanobind(m);
        last_value_pull_node_builder_register_with_nanobind(m);
    }

} // namespace hgraph
