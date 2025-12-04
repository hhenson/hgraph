#include <hgraph/types/node.h>
#include <hgraph/types/time_series_type.h>
#include <hgraph/types/tsb.h>

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
        if (input_builder) { (*input_builder)->release_instance(item->input().get()); }
        if (output_builder) { (*output_builder)->release_instance(item->output().get()); }
        if (error_builder) { (*error_builder)->release_instance(item->error_output().get()); }
        if (recordable_state_builder) { (*recordable_state_builder)->release_instance(item->recordable_state().get()); }
        dispose_component(*item);
    }

    size_t NodeBuilder::memory_size() const {
        // Base size for Node - concrete node builders should override if they create different node types
        // Add canary size to the base Node object
        size_t total = add_canary_size(sizeof(Node));
        // Align and add each time-series builder's size
        if (input_builder) {
            // We don't know the exact type, so use TimeSeriesType alignment as a conservative estimate
            total = align_size(total, alignof(TimeSeriesType));
            total += (*input_builder)->memory_size();
        }
        if (output_builder) {
            total = align_size(total, alignof(TimeSeriesType));
            total += (*output_builder)->memory_size();
        }
        if (error_builder) {
            total = align_size(total, alignof(TimeSeriesType));
            total += (*error_builder)->memory_size();
        }
        if (recordable_state_builder) {
            total = align_size(total, alignof(TimeSeriesType));
            total += (*recordable_state_builder)->memory_size();
        }
        return total;
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

    void BaseNodeBuilder::_build_inputs_and_outputs(node_ptr node) const {
        if (input_builder.has_value()) {
            auto ts_input = (*input_builder)->make_instance(node);
            // The input is always a TimeSeriesBundleInput at this level.
            node->set_input(std::static_pointer_cast<TimeSeriesBundleInput>(ts_input));
        }

        if (output_builder.has_value()) {
            auto ts_output = (*output_builder)->make_instance(node);
            node->set_output(ts_output);
        }

        if (error_builder.has_value()) {
            auto ts_error_output = (*error_builder)->make_instance(node);
            node->set_error_output(ts_error_output);
        }

        if (recordable_state_builder.has_value()) {
            auto ts_recordable_state = (*recordable_state_builder)->make_instance(node);
            // The recordable_state is always a TimeSeriesBundleOutput at this level.
            node->set_recordable_state(std::static_pointer_cast<TimeSeriesBundleOutput>(ts_recordable_state));
        }
    }
} // namespace hgraph
