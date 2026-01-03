#include <hgraph/builders/nodes/reduce_node_builder.h>
#include <hgraph/builders/input_builder.h>
#include <hgraph/builders/output_builder.h>
#include <hgraph/types/node.h>
#include <hgraph/types/tsb.h>
#include <hgraph/types/time_series_type.h>
#include <hgraph/nodes/reduce_node.h>
#include <hgraph/util/arena_enable_shared_from_this.h>

namespace hgraph {
    auto create_reduce_node_builder(ReduceNodeBuilder *self, const nb::args &args) {
        // Expected Python signature (positional):
        // (signature, scalars, input_builder, output_builder, error_builder, recordable_state_builder,
        //  nested_graph, input_node_ids, output_node_id)
        if (args.size() != 9) {
            throw nb::type_error("ReduceNodeBuilder expects 9 positional arguments: "
                "(signature, scalars, input_builder, output_builder, error_builder, "
                "recordable_state_builder, nested_graph, input_node_ids, output_node_id)");
        }

        auto signature_ = nb::cast<node_signature_s_ptr>(args[0]);
        auto scalars_ = nb::cast<nb::dict>(args[1]);
        std::optional<input_builder_s_ptr> input_builder_ =
                args[2].is_none()
                    ? std::nullopt
                    : std::optional<input_builder_s_ptr>(nb::cast<input_builder_s_ptr>(args[2]));
        std::optional<output_builder_s_ptr> output_builder_ =
                args[3].is_none()
                    ? std::nullopt
                    : std::optional<output_builder_s_ptr>(nb::cast<output_builder_s_ptr>(args[3]));
        std::optional<output_builder_s_ptr> error_builder_ =
                args[4].is_none()
                    ? std::nullopt
                    : std::optional<output_builder_s_ptr>(nb::cast<output_builder_s_ptr>(args[4]));
        std::optional<output_builder_s_ptr> recordable_state_builder_ =
                args[5].is_none()
                    ? std::nullopt
                    : std::optional<output_builder_s_ptr>(nb::cast<output_builder_s_ptr>(args[5]));
        auto nested_graph_builder = nb::cast<graph_builder_s_ptr>(args[6]);
        auto input_node_ids_tuple = nb::cast<std::tuple<int64_t, int64_t> >(args[7]);
        auto output_node_id = nb::cast<int64_t>(args[8]);

        return new(self) ReduceNodeBuilder(std::move(signature_), std::move(scalars_), std::move(input_builder_),
                           std::move(output_builder_),
                           std::move(error_builder_), std::move(recordable_state_builder_),
                           std::move(nested_graph_builder),
                           std::move(input_node_ids_tuple), std::move(output_node_id));
    }

    ReduceNodeBuilder::ReduceNodeBuilder(node_signature_s_ptr signature_, nb::dict scalars_,
                                         std::optional<input_builder_s_ptr> input_builder_,
                                         std::optional<output_builder_s_ptr> output_builder_,
                                         std::optional<output_builder_s_ptr> error_builder_,
                                         std::optional<output_builder_s_ptr> recordable_state_builder_,
                                         graph_builder_s_ptr nested_graph_builder_,
                                         const std::tuple<int64_t, int64_t> &input_node_ids_,
                                         int64_t output_node_id_)
        : BaseNodeBuilder(std::move(signature_), std::move(scalars_), std::move(input_builder_),
                          std::move(output_builder_),
                          std::move(error_builder_), std::move(recordable_state_builder_)),
          nested_graph_builder(std::move(nested_graph_builder_)), input_node_ids(input_node_ids_),
          output_node_id(output_node_id_) {
    }

    node_s_ptr ReduceNodeBuilder::make_instance(const std::vector<int64_t> &owning_graph_id, int64_t node_ndx) const {
        auto node = arena_make_shared_as<ReduceNode, Node>(
            node_ndx, owning_graph_id, signature, scalars, nested_graph_builder, input_node_ids, output_node_id);
        _build_inputs_and_outputs(node.get());
        return node;
    }

    void reduce_node_builder_register_with_nanobind(nb::module_ &m) {
        nb::class_<ReduceNodeBuilder, BaseNodeBuilder>(m, "ReduceNodeBuilder")
            .def("__init__", [](ReduceNodeBuilder *self, const nb::args &args) {
                create_reduce_node_builder(self, args);
            })
            .def_ro("nested_graph_builder", &ReduceNodeBuilder::nested_graph_builder)
            .def_ro("input_node_ids", &ReduceNodeBuilder::input_node_ids)
            .def_ro("output_node_id", &ReduceNodeBuilder::output_node_id);
    }
} // namespace hgraph
