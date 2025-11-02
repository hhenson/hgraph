#include <hgraph/builders/nodes/reduce_node_builder.h>
#include <hgraph/builders/input_builder.h>
#include <hgraph/builders/output_builder.h>
#include <hgraph/types/node.h>
#include <hgraph/types/tsb.h>
#include <hgraph/types/time_series_type.h>
#include <hgraph/nodes/reduce_node.h>

namespace hgraph {
    template<typename T>
    auto create_reduce_node_builder(T *self, const nb::args &args) {
        // Expected Python signature (positional):
        // (signature, scalars, input_builder, output_builder, error_builder, recordable_state_builder,
        //  nested_graph, input_node_ids, output_node_id)
        if (args.size() != 9) {
            throw nb::type_error("ReduceNodeBuilder expects 9 positional arguments: "
                "(signature, scalars, input_builder, output_builder, error_builder, "
                "recordable_state_builder, nested_graph, input_node_ids, output_node_id)");
        }

        auto signature_ = nb::cast<node_signature_ptr>(args[0]);
        auto scalars_ = nb::cast<nb::dict>(args[1]);
        std::optional<input_builder_ptr> input_builder_ =
                args[2].is_none()
                    ? std::nullopt
                    : std::optional<input_builder_ptr>(nb::cast<input_builder_ptr>(args[2]));
        std::optional<output_builder_ptr> output_builder_ =
                args[3].is_none()
                    ? std::nullopt
                    : std::optional<output_builder_ptr>(nb::cast<output_builder_ptr>(args[3]));
        std::optional<output_builder_ptr> error_builder_ =
                args[4].is_none()
                    ? std::nullopt
                    : std::optional<output_builder_ptr>(nb::cast<output_builder_ptr>(args[4]));
        std::optional<output_builder_ptr> recordable_state_builder_ =
                args[5].is_none()
                    ? std::nullopt
                    : std::optional<output_builder_ptr>(nb::cast<output_builder_ptr>(args[5]));
        auto nested_graph_builder = nb::cast<graph_builder_ptr>(args[6]);
        auto input_node_ids_tuple = nb::cast<std::tuple<int64_t, int64_t> >(args[7]);
        auto output_node_id = nb::cast<int64_t>(args[8]);

        return new(self) T(std::move(signature_), std::move(scalars_), std::move(input_builder_),
                           std::move(output_builder_),
                           std::move(error_builder_), std::move(recordable_state_builder_),
                           std::move(nested_graph_builder),
                           std::move(input_node_ids_tuple), std::move(output_node_id));
    }

    BaseReduceNodeBuilder::BaseReduceNodeBuilder(node_signature_ptr signature_, nb::dict scalars_,
                                                 std::optional<input_builder_ptr> input_builder_,
                                                 std::optional<output_builder_ptr> output_builder_,
                                                 std::optional<output_builder_ptr> error_builder_,
                                                 std::optional<output_builder_ptr> recordable_state_builder_,
                                                 graph_builder_ptr nested_graph_builder,
                                                 const std::tuple<int64_t, int64_t> &input_node_ids,
                                                 int64_t output_node_id)
        : BaseNodeBuilder(std::move(signature_), std::move(scalars_), std::move(input_builder_),
                          std::move(output_builder_),
                          std::move(error_builder_), std::move(recordable_state_builder_)),
          nested_graph_builder(std::move(nested_graph_builder)), input_node_ids(input_node_ids),
          output_node_id(output_node_id) {
    }

    template<typename T>
    node_ptr ReduceNodeBuilder<T>::make_instance(const std::vector<int64_t> &owning_graph_id, int64_t node_ndx) const {
        nb::ref<Node> node{
            new ReduceNode<T>(node_ndx, owning_graph_id, signature, scalars, nested_graph_builder, input_node_ids,
                              output_node_id)
        };
        _build_inputs_and_outputs(node);
        return node;
    }

    // Explicit template instantiations
    template struct ReduceNodeBuilder<bool>;
    template struct ReduceNodeBuilder<int64_t>;
    template struct ReduceNodeBuilder<double>;
    template struct ReduceNodeBuilder<engine_date_t>;
    template struct ReduceNodeBuilder<engine_time_t>;
    template struct ReduceNodeBuilder<engine_time_delta_t>;
    template struct ReduceNodeBuilder<nb::object>;

    void reduce_node_builder_register_with_nanobind(nb::module_ &m) {
        nb::class_ < BaseReduceNodeBuilder, BaseNodeBuilder > (m, "BaseReduceNodeBuilder")
                .def_ro("nested_graph_builder", &BaseReduceNodeBuilder::nested_graph_builder)
                .def_ro("input_node_ids", &BaseReduceNodeBuilder::input_node_ids)
                .def_ro("output_node_id", &BaseReduceNodeBuilder::output_node_id);

        nb::class_<ReduceNodeBuilder<bool>, BaseReduceNodeBuilder>(m, "ReduceNodeBuilder_bool")
                .def("__init__", [](ReduceNodeBuilder<bool> *self, const nb::args &args) {
                    create_reduce_node_builder(self, args);
                });

        nb::class_<ReduceNodeBuilder<int64_t>, BaseReduceNodeBuilder>(m, "ReduceNodeBuilder_int")
                .def("__init__", [](ReduceNodeBuilder<int64_t> *self, const nb::args &args) {
                    create_reduce_node_builder(self, args);
                });

        nb::class_<ReduceNodeBuilder<double>, BaseReduceNodeBuilder>(m, "ReduceNodeBuilder_float")
                .def("__init__", [](ReduceNodeBuilder<double> *self, const nb::args &args) {
                    create_reduce_node_builder(self, args);
                });

        nb::class_<ReduceNodeBuilder<engine_date_t>, BaseReduceNodeBuilder>(m, "ReduceNodeBuilder_date")
                .def("__init__", [](ReduceNodeBuilder<engine_date_t> *self, const nb::args &args) {
                    create_reduce_node_builder(self, args);
                });

        nb::class_<ReduceNodeBuilder<engine_time_t>, BaseReduceNodeBuilder>(m, "ReduceNodeBuilder_date_time")
                .def("__init__", [](ReduceNodeBuilder<engine_time_t> *self, const nb::args &args) {
                    create_reduce_node_builder(self, args);
                });

        nb::class_<ReduceNodeBuilder<engine_time_delta_t>, BaseReduceNodeBuilder>(m, "ReduceNodeBuilder_time_delta")
                .def("__init__", [](ReduceNodeBuilder<engine_time_delta_t> *self, const nb::args &args) {
                    create_reduce_node_builder(self, args);
                });

        nb::class_<ReduceNodeBuilder<nb::object>, BaseReduceNodeBuilder>(m, "ReduceNodeBuilder_object")
                .def("__init__", [](ReduceNodeBuilder<nb::object> *self, const nb::args &args) {
                    create_reduce_node_builder(self, args);
                });
    }
} // namespace hgraph