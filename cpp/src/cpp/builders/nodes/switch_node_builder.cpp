#include <hgraph/builders/nodes/switch_node_builder.h>
#include <hgraph/types/node.h>
#include <hgraph/types/tsb.h>
#include <hgraph/types/time_series_type.h>
#include <hgraph/nodes/switch_node.h>
#include <hgraph/util/arena_enable_shared_from_this.h>

namespace hgraph {

    // Helper to check if a Python object is the DEFAULT class or an instance of it
    static bool is_default_marker(const nb::handle &obj) {
        static nb::object default_class;
        if (!default_class.is_valid()) {
            try {
                auto scalar_types = nb::module_::import_("hgraph._types._scalar_types");
                default_class = scalar_types.attr("Default");
            } catch (...) {
                return false;
            }
        }
        // Check if it's the class itself or an instance of the class
        return obj.is(default_class) || nb::isinstance(obj, default_class);
    }

    SwitchNodeBuilder::SwitchNodeBuilder(
        node_signature_s_ptr signature_, nb::dict scalars_, const value::TypeMeta* key_type,
        graph_builders_map_ptr nested_graph_builders,
        input_node_ids_map_ptr input_node_ids,
        output_node_ids_map_ptr output_node_ids,
        bool reload_on_ticked,
        graph_builder_s_ptr default_graph_builder,
        std::unordered_map<std::string, int> default_input_node_ids,
        int default_output_node_id)
        : BaseNodeBuilder(std::move(signature_), std::move(scalars_)),
          _key_type(key_type),
          _nested_graph_builders(std::move(nested_graph_builders)),
          _input_node_ids(std::move(input_node_ids)),
          _output_node_ids(std::move(output_node_ids)),
          _reload_on_ticked(reload_on_ticked),
          _default_graph_builder(std::move(default_graph_builder)),
          _default_input_node_ids(std::move(default_input_node_ids)),
          _default_output_node_id(default_output_node_id) {
    }

    node_s_ptr SwitchNodeBuilder::make_instance(const std::vector<int64_t> &owning_graph_id, int64_t node_ndx) const {
        auto node = arena_make_shared_as<SwitchNode, Node>(
            node_ndx, owning_graph_id, signature, scalars,
            input_meta(), output_meta(), error_output_meta(), recordable_state_meta(),
            _key_type, _nested_graph_builders, _input_node_ids, _output_node_ids,
            _reload_on_ticked, _default_graph_builder, _default_input_node_ids, _default_output_node_id);
        configure_node_instance(node);
        return node;
    }

    void switch_node_builder_register_with_nanobind(nb::module_ &m) {
        nb::class_<SwitchNodeBuilder, BaseNodeBuilder>(m, "SwitchNodeBuilder")
            .def("__init__", [](SwitchNodeBuilder *self, const nb::args &args) {
                // Preferred signature (positional):
                // (signature, scalars, key_type_schema, nested_graph_builders, input_node_ids, output_node_ids, reload_on_ticked)
                // Transitional signature with legacy builder placeholders:
                // (signature, scalars, input_builder, output_builder, error_builder, recordable_state_builder,
                //  key_type_schema, nested_graph_builders, input_node_ids, output_node_ids, reload_on_ticked)
                // where builder args must be None.
                if (args.size() != 7 && args.size() != 11) {
                    throw nb::type_error("SwitchNodeBuilder expects 7 positional arguments");
                }

                auto signature_ = nb::cast<node_signature_s_ptr>(args[0]);
                auto scalars_ = nb::cast<nb::dict>(args[1]);
                size_t key_type_index = 2;
                size_t nested_graph_index = 3;
                size_t input_node_ids_index = 4;
                size_t output_node_ids_index = 5;
                size_t reload_on_ticked_index = 6;
                if (args.size() == 11) {
                    for (size_t i = 2; i < 6; ++i) {
                        if (!args[i].is_none()) {
                            throw nb::type_error(
                                "Legacy input/output/error/recordable builders are not supported in C++ runtime node builders");
                        }
                    }
                    key_type_index = 6;
                    nested_graph_index = 7;
                    input_node_ids_index = 8;
                    output_node_ids_index = 9;
                    reload_on_ticked_index = 10;
                }

                // Key type schema
                const value::TypeMeta* key_type = nb::cast<const value::TypeMeta*>(args[key_type_index]);

                // Convert Python dicts to Value-based C++ maps (as shared_ptr for sharing with node instances)
                // Extract DEFAULT builder separately and skip DEFAULT marker keys in maps
                auto py_nested_graph_builders = nb::cast<nb::dict>(args[nested_graph_index]);
                graph_builder_s_ptr default_graph_builder = nullptr;
                auto nested_graph_builders = std::make_shared<SwitchNodeBuilder::graph_builders_map>();
                for (auto item: py_nested_graph_builders) {
                    if (is_default_marker(item.first)) {
                        default_graph_builder = nb::cast<graph_builder_s_ptr>(item.second);
                        continue;
                    }
                    // Convert Python key to Value
                    value::Value key(key_type);
                    key.emplace();
                    key_type->ops().from_python(key.data(), nb::cast<nb::object>(item.first), key_type);
                    nested_graph_builders->emplace(std::move(key), nb::cast<graph_builder_s_ptr>(item.second));
                }

                auto py_input_node_ids = nb::cast<nb::dict>(args[input_node_ids_index]);
                std::unordered_map<std::string, int> default_input_node_ids;
                auto input_node_ids = std::make_shared<SwitchNodeBuilder::input_node_ids_map>();
                for (auto item: py_input_node_ids) {
                    if (is_default_marker(item.first)) {
                        default_input_node_ids = nb::cast<std::unordered_map<std::string, int>>(item.second);
                        continue;
                    }
                    value::Value key(key_type);
                    key.emplace();
                    key_type->ops().from_python(key.data(), nb::cast<nb::object>(item.first), key_type);
                    input_node_ids->emplace(std::move(key), nb::cast<std::unordered_map<std::string, int>>(item.second));
                }

                auto py_output_node_ids = nb::cast<nb::dict>(args[output_node_ids_index]);
                int default_output_node_id = -1;
                auto output_node_ids = std::make_shared<SwitchNodeBuilder::output_node_ids_map>();
                for (auto item: py_output_node_ids) {
                    if (is_default_marker(item.first)) {
                        default_output_node_id = nb::cast<int>(item.second);
                        continue;
                    }
                    value::Value key(key_type);
                    key.emplace();
                    key_type->ops().from_python(key.data(), nb::cast<nb::object>(item.first), key_type);
                    output_node_ids->emplace(std::move(key), nb::cast<int>(item.second));
                }

                auto reload_on_ticked = nb::cast<bool>(args[reload_on_ticked_index]);

                new(self) SwitchNodeBuilder(
                    std::move(signature_), std::move(scalars_), key_type, std::move(nested_graph_builders),
                    std::move(input_node_ids), std::move(output_node_ids), reload_on_ticked,
                    default_graph_builder, std::move(default_input_node_ids), default_output_node_id);
            })
            .def_prop_ro("reload_on_ticked", [](const SwitchNodeBuilder& self) { return self._reload_on_ticked; });
    }
} // namespace hgraph
