#include <hgraph/builders/nodes/tsd_map_node_builder.h>
#include <hgraph/builders/input_builder.h>
#include <hgraph/builders/output_builder.h>
#include <hgraph/types/node.h>
#include <hgraph/types/tsb.h>
#include <hgraph/types/time_series_type.h>
#include <hgraph/nodes/tsd_map_node.h>

namespace hgraph {
    template<typename T>
    auto create_tsd_map_node_builder(T *self, const nb::args &args) {
        // Expected Python signature (positional):
        // (signature, scalars, input_builder, output_builder, error_builder, recordable_state_builder,
        //  nested_graph, input_node_ids, output_node_id, multiplexed_args, key_arg)
        if (args.size() != 11) {
            throw nb::type_error("TsdMapNodeBuilder expects 11 positional arguments: "
                "(signature, scalars, input_builder, output_builder, error_builder, "
                "recordable_state_builder, nested_graph, input_node_ids, output_node_id, "
                "multiplexed_args, key_arg)");
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
        if (args[6].is_none()) {
            throw nb::type_error("TsdMapNodeBuilder requires a nested_graph (arg[6]) and it must not be None");
        }
        graph_builder_ptr nested_graph_builder = nb::cast<graph_builder_ptr>(args[6]);
        auto input_node_ids = nb::cast<std::unordered_map<std::string, int64_t> >(args[7]);
        auto output_node_id = nb::cast<int64_t>(args[8]);
        auto multiplexed_args = nb::cast<std::unordered_set<std::string> >(args[9]);
        auto key_arg = nb::cast<std::string>(args[10]);

        return new(self) T(std::move(signature_), std::move(scalars_), std::move(input_builder_),
                           std::move(output_builder_),
                           std::move(error_builder_), std::move(recordable_state_builder_),
                           std::move(nested_graph_builder),
                           std::move(input_node_ids), std::move(output_node_id), std::move(multiplexed_args),
                           std::move(key_arg));
    }

    BaseTsdMapNodeBuilder::BaseTsdMapNodeBuilder(
        node_signature_ptr signature_, nb::dict scalars_, std::optional<input_builder_ptr> input_builder_,
        std::optional<output_builder_ptr> output_builder_, std::optional<output_builder_ptr> error_builder_,
        std::optional<output_builder_ptr> recordable_state_builder_, graph_builder_ptr nested_graph_builder,
        const std::unordered_map<std::string, int64_t> &input_node_ids, int64_t output_node_id,
        const std::unordered_set<std::string> &multiplexed_args, const std::string &key_arg)
        : BaseNodeBuilder(std::move(signature_), std::move(scalars_), std::move(input_builder_),
                          std::move(output_builder_),
                          std::move(error_builder_), std::move(recordable_state_builder_)),
          nested_graph_builder(std::move(nested_graph_builder)), input_node_ids(input_node_ids),
          output_node_id(output_node_id),
          multiplexed_args(multiplexed_args), key_arg(key_arg) {
    }

    template<typename T>
    node_ptr TsdMapNodeBuilder<T>::make_instance(const std::vector<int64_t> &owning_graph_id, int64_t node_ndx) const {
        nb::ref<Node> node{
            new TsdMapNode<T>(node_ndx, owning_graph_id, signature, scalars, nested_graph_builder, input_node_ids,
                              output_node_id, multiplexed_args, key_arg)
        };
        _build_inputs_and_outputs(node);
        return node;
    }

    // Explicit template instantiations
    template struct TsdMapNodeBuilder<bool>;
    template struct TsdMapNodeBuilder<int64_t>;
    template struct TsdMapNodeBuilder<double>;
    template struct TsdMapNodeBuilder<engine_date_t>;
    template struct TsdMapNodeBuilder<engine_time_t>;
    template struct TsdMapNodeBuilder<engine_time_delta_t>;
    template struct TsdMapNodeBuilder<nb::object>;

    void tsd_map_node_builder_register_with_nanobind(nb::module_ &m) {
        nb::class_ < BaseTsdMapNodeBuilder, BaseNodeBuilder > (m, "BaseTsdMapNodeBuilder")
                .def_ro("nested_graph_builder", &BaseTsdMapNodeBuilder::nested_graph_builder)
                .def_ro("input_node_ids", &BaseTsdMapNodeBuilder::input_node_ids)
                .def_ro("output_node_id", &BaseTsdMapNodeBuilder::output_node_id)
                .def_ro("multiplexed_args", &BaseTsdMapNodeBuilder::multiplexed_args)
                .def_ro("key_arg", &BaseTsdMapNodeBuilder::key_arg);

        nb::class_<TsdMapNodeBuilder<bool>, BaseTsdMapNodeBuilder>(m, "TsdMapNodeBuilder_bool")
                .def("__init__", [](TsdMapNodeBuilder<bool> *self, const nb::args &args) {
                    create_tsd_map_node_builder(self, args);
                });

        nb::class_<TsdMapNodeBuilder<int64_t>, BaseTsdMapNodeBuilder>(m, "TsdMapNodeBuilder_int")
                .def("__init__", [](TsdMapNodeBuilder<int64_t> *self, const nb::args &args) {
                    create_tsd_map_node_builder(self, args);
                });

        nb::class_<TsdMapNodeBuilder<double>, BaseTsdMapNodeBuilder>(m, "TsdMapNodeBuilder_float")
                .def("__init__", [](TsdMapNodeBuilder<double> *self, const nb::args &args) {
                    create_tsd_map_node_builder(self, args);
                });

        nb::class_<TsdMapNodeBuilder<engine_date_t>, BaseTsdMapNodeBuilder>(m, "TsdMapNodeBuilder_date")
                .def("__init__", [](TsdMapNodeBuilder<engine_date_t> *self, const nb::args &args) {
                    create_tsd_map_node_builder(self, args);
                });

        nb::class_<TsdMapNodeBuilder<engine_time_t>, BaseTsdMapNodeBuilder>(m, "TsdMapNodeBuilder_date_time")
                .def("__init__", [](TsdMapNodeBuilder<engine_time_t> *self, const nb::args &args) {
                    create_tsd_map_node_builder(self, args);
                });

        nb::class_<TsdMapNodeBuilder<engine_time_delta_t>, BaseTsdMapNodeBuilder>(m, "TsdMapNodeBuilder_time_delta")
                .def("__init__", [](TsdMapNodeBuilder<engine_time_delta_t> *self, const nb::args &args) {
                    create_tsd_map_node_builder(self, args);
                });

        nb::class_<TsdMapNodeBuilder<nb::object>, BaseTsdMapNodeBuilder>(m, "TsdMapNodeBuilder_object")
                .def("__init__", [](TsdMapNodeBuilder<nb::object> *self, const nb::args &args) {
                    create_tsd_map_node_builder(self, args);
                });
    }
} // namespace hgraph