#include <hgraph/builders/nodes/tsd_map_node_builder.h>
#include <hgraph/types/node.h>
#include <hgraph/types/tsb.h>
#include <hgraph/types/time_series_type.h>
#include <hgraph/nodes/tsd_map_node.h>
#include <hgraph/util/arena_enable_shared_from_this.h>

namespace hgraph {
    auto create_tsd_map_node_builder(TsdMapNodeBuilder *self, const nb::args &args) {
        // Signature (positional):
        // (signature, scalars, nested_graph, input_node_ids, output_node_id, multiplexed_args, key_arg)
        if (args.size() != 7) {
            throw nb::type_error("TsdMapNodeBuilder expects 7 positional arguments: "
                "(signature, scalars, nested_graph, input_node_ids, output_node_id, multiplexed_args, key_arg)");
        }

        auto signature_ = nb::cast<node_signature_s_ptr>(args[0]);
        auto scalars_ = nb::cast<nb::dict>(args[1]);
        graph_builder_s_ptr nested_graph_builder;
        std::unordered_map<std::string, int64_t> input_node_ids;
        int64_t output_node_id = -1;
        std::unordered_set<std::string> multiplexed_args;
        std::string key_arg;

        nested_graph_builder = nb::cast<graph_builder_s_ptr>(args[2]);
        input_node_ids = nb::cast<std::unordered_map<std::string, int64_t>>(args[3]);
        output_node_id = nb::cast<int64_t>(args[4]);
        multiplexed_args = nb::cast<std::unordered_set<std::string>>(args[5]);
        key_arg = nb::cast<std::string>(args[6]);

        if (!nested_graph_builder) {
            throw nb::type_error("TsdMapNodeBuilder requires a nested_graph and it must not be None");
        }

        return new(self) TsdMapNodeBuilder(std::move(signature_), std::move(scalars_), std::move(nested_graph_builder),
                                           std::move(input_node_ids), std::move(output_node_id),
                                           std::move(multiplexed_args), std::move(key_arg));
    }

    TsdMapNodeBuilder::TsdMapNodeBuilder(
        node_signature_s_ptr signature_, nb::dict scalars_, graph_builder_s_ptr nested_graph_builder_,
        const std::unordered_map<std::string, int64_t> &input_node_ids_, int64_t output_node_id_,
        const std::unordered_set<std::string> &multiplexed_args_, const std::string &key_arg_)
        : BaseNodeBuilder(std::move(signature_), std::move(scalars_)),
          nested_graph_builder(std::move(nested_graph_builder_)), input_node_ids(input_node_ids_),
          output_node_id(output_node_id_),
          multiplexed_args(multiplexed_args_), key_arg(key_arg_) {
    }

    node_s_ptr TsdMapNodeBuilder::make_instance(const std::vector<int64_t> &owning_graph_id, int64_t node_ndx) const {
        auto node = arena_make_shared_as<TsdMapNode, Node>(
            node_ndx, owning_graph_id, signature, scalars,
            input_meta(), output_meta(), error_output_meta(), recordable_state_meta(),
            nested_graph_builder, input_node_ids,
            output_node_id, multiplexed_args, key_arg);
        configure_node_instance(node);
        return node;
    }

    void tsd_map_node_builder_register_with_nanobind(nb::module_ &m) {
        nb::class_<TsdMapNodeBuilder, BaseNodeBuilder>(m, "TsdMapNodeBuilder")
            .def("__init__", [](TsdMapNodeBuilder *self, const nb::args &args) {
                create_tsd_map_node_builder(self, args);
            })
            .def_ro("nested_graph_builder", &TsdMapNodeBuilder::nested_graph_builder)
            .def_ro("input_node_ids", &TsdMapNodeBuilder::input_node_ids)
            .def_ro("output_node_id", &TsdMapNodeBuilder::output_node_id)
            .def_ro("multiplexed_args", &TsdMapNodeBuilder::multiplexed_args)
            .def_ro("key_arg", &TsdMapNodeBuilder::key_arg);
    }
} // namespace hgraph
