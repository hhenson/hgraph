#include <hgraph/builders/nodes/mesh_node_builder.h>
#include <hgraph/builders/input_builder.h>
#include <hgraph/builders/output_builder.h>
#include <hgraph/types/node.h>
#include <hgraph/types/tsb.h>
#include <hgraph/types/time_series_type.h>
#include <hgraph/nodes/mesh_node.h>

namespace hgraph {
    template<typename T>
    auto create_mesh_node_builder(T *self, const nb::args &args) {
        // Expected Python signature (positional):
        // (signature, scalars, input_builder, output_builder, error_builder, recordable_state_builder,
        //  nested_graph, input_node_ids, output_node_id, multiplexed_args, key_arg, context_path)
        if (args.size() != 12) {
            throw nb::type_error("MeshNodeBuilder expects 12 positional arguments");
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
        auto input_node_ids = nb::cast<std::unordered_map<std::string, int64_t> >(args[7]);
        auto output_node_id = nb::cast<int64_t>(args[8]);
        auto multiplexed_args = nb::cast<std::unordered_set<std::string> >(args[9]);
        auto key_arg = nb::cast<std::string>(args[10]);
        auto context_path = nb::cast<std::string>(args[11]);

        return new(self) T(std::move(signature_), std::move(scalars_), std::move(input_builder_),
                           std::move(output_builder_),
                           std::move(error_builder_), std::move(recordable_state_builder_),
                           std::move(nested_graph_builder),
                           std::move(input_node_ids), std::move(output_node_id), std::move(multiplexed_args),
                           std::move(key_arg),
                           std::move(context_path));
    }

    BaseMeshNodeBuilder::BaseMeshNodeBuilder(
        node_signature_ptr signature_, nb::dict scalars_, std::optional<input_builder_ptr> input_builder_,
        std::optional<output_builder_ptr> output_builder_, std::optional<output_builder_ptr> error_builder_,
        std::optional<output_builder_ptr> recordable_state_builder_, graph_builder_ptr nested_graph_builder,
        const std::unordered_map<std::string, int64_t> &input_node_ids, int64_t output_node_id,
        const std::unordered_set<std::string> &multiplexed_args, const std::string &key_arg,
        const std::string &context_path)
        : BaseNodeBuilder(std::move(signature_), std::move(scalars_), std::move(input_builder_),
                          std::move(output_builder_),
                          std::move(error_builder_), std::move(recordable_state_builder_)),
          nested_graph_builder(std::move(nested_graph_builder)), input_node_ids(input_node_ids),
          output_node_id(output_node_id),
          multiplexed_args(multiplexed_args), key_arg(key_arg), context_path(context_path) {
    }

    template<typename T>
    node_ptr MeshNodeBuilder<T>::make_instance(const std::vector<int64_t> &owning_graph_id, int64_t node_ndx) const {
        nb::ref<Node> node{
            new MeshNode<T>(node_ndx, owning_graph_id, signature, scalars, nested_graph_builder, input_node_ids,
                            output_node_id, multiplexed_args, key_arg, context_path)
        };
        _build_inputs_and_outputs(node);
        return node;
    }

    // Explicit template instantiations
    template struct MeshNodeBuilder<bool>;
    template struct MeshNodeBuilder<int64_t>;
    template struct MeshNodeBuilder<double>;
    template struct MeshNodeBuilder<engine_date_t>;
    template struct MeshNodeBuilder<engine_time_t>;
    template struct MeshNodeBuilder<engine_time_delta_t>;
    template struct MeshNodeBuilder<nb::object>;

    void mesh_node_builder_register_with_nanobind(nb::module_ &m) {
        nb::class_ < BaseMeshNodeBuilder, BaseNodeBuilder > (m, "BaseMeshNodeBuilder")
                .def_ro("nested_graph_builder", &BaseMeshNodeBuilder::nested_graph_builder)
                .def_ro("input_node_ids", &BaseMeshNodeBuilder::input_node_ids)
                .def_ro("output_node_id", &BaseMeshNodeBuilder::output_node_id)
                .def_ro("multiplexed_args", &BaseMeshNodeBuilder::multiplexed_args)
                .def_ro("key_arg", &BaseMeshNodeBuilder::key_arg)
                .def_ro("context_path", &BaseMeshNodeBuilder::context_path);

        nb::class_<MeshNodeBuilder<bool>, BaseMeshNodeBuilder>(m, "MeshNodeBuilder_bool")
                .def("__init__", [](MeshNodeBuilder<bool> *self, const nb::args &args) {
                    create_mesh_node_builder(self, args);
                });

        nb::class_<MeshNodeBuilder<int64_t>, BaseMeshNodeBuilder>(m, "MeshNodeBuilder_int")
                .def("__init__", [](MeshNodeBuilder<int64_t> *self, const nb::args &args) {
                    create_mesh_node_builder(self, args);
                });

        nb::class_<MeshNodeBuilder<double>, BaseMeshNodeBuilder>(m, "MeshNodeBuilder_float")
                .def("__init__", [](MeshNodeBuilder<double> *self, const nb::args &args) {
                    create_mesh_node_builder(self, args);
                });

        nb::class_<MeshNodeBuilder<engine_date_t>, BaseMeshNodeBuilder>(m, "MeshNodeBuilder_date")
                .def("__init__", [](MeshNodeBuilder<engine_date_t> *self, const nb::args &args) {
                    create_mesh_node_builder(self, args);
                });

        nb::class_<MeshNodeBuilder<engine_time_t>, BaseMeshNodeBuilder>(m, "MeshNodeBuilder_date_time")
                .def("__init__", [](MeshNodeBuilder<engine_time_t> *self, const nb::args &args) {
                    create_mesh_node_builder(self, args);
                });

        nb::class_<MeshNodeBuilder<engine_time_delta_t>, BaseMeshNodeBuilder>(m, "MeshNodeBuilder_time_delta")
                .def("__init__", [](MeshNodeBuilder<engine_time_delta_t> *self, const nb::args &args) {
                    create_mesh_node_builder(self, args);
                });

        nb::class_<MeshNodeBuilder<nb::object>, BaseMeshNodeBuilder>(m, "MeshNodeBuilder_object")
                .def("__init__", [](MeshNodeBuilder<nb::object> *self, const nb::args &args) {
                    create_mesh_node_builder(self, args);
                });
    }
} // namespace hgraph