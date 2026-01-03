#include <hgraph/builders/nodes/switch_node_builder.h>
#include <hgraph/builders/input_builder.h>
#include <hgraph/builders/output_builder.h>
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
        node_signature_s_ptr signature_, nb::dict scalars_,
        std::optional<input_builder_s_ptr> input_builder_,
        std::optional<output_builder_s_ptr> output_builder_,
        std::optional<output_builder_s_ptr> error_builder_,
        std::optional<output_builder_s_ptr> recordable_state_builder_,
        const value::TypeMeta* key_type,
        graph_builders_map_ptr nested_graph_builders,
        input_node_ids_map_ptr input_node_ids,
        output_node_ids_map_ptr output_node_ids,
        bool reload_on_ticked,
        graph_builder_s_ptr default_graph_builder,
        std::unordered_map<std::string, int> default_input_node_ids,
        int default_output_node_id)
        : BaseNodeBuilder(std::move(signature_), std::move(scalars_), std::move(input_builder_),
                          std::move(output_builder_), std::move(error_builder_), std::move(recordable_state_builder_)),
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
            _key_type, _nested_graph_builders, _input_node_ids, _output_node_ids,
            _reload_on_ticked, _default_graph_builder, _default_input_node_ids, _default_output_node_id);
        _build_inputs_and_outputs(node.get());
        return node;
    }

    void switch_node_builder_register_with_nanobind(nb::module_ &m) {
        nb::class_<SwitchNodeBuilder, BaseNodeBuilder>(m, "SwitchNodeBuilder")
            .def("__init__", [](SwitchNodeBuilder *self, const nb::args &args) {
                // Expected Python signature (positional):
                // (signature, scalars, input_builder, output_builder, error_builder, recordable_state_builder,
                //  key_type_schema, nested_graph_builders, input_node_ids, output_node_ids, reload_on_ticked)
                if (args.size() != 11) {
                    throw nb::type_error("SwitchNodeBuilder expects 11 positional arguments");
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

                // Key type schema
                const value::TypeMeta* key_type = nb::cast<const value::TypeMeta*>(args[6]);

                // Convert Python dicts to Value-based C++ maps (as shared_ptr for sharing with node instances)
                // Extract DEFAULT builder separately and skip DEFAULT marker keys in maps
                auto py_nested_graph_builders = nb::cast<nb::dict>(args[7]);
                graph_builder_s_ptr default_graph_builder = nullptr;
                auto nested_graph_builders = std::make_shared<SwitchNodeBuilder::graph_builders_map>();
                for (auto item: py_nested_graph_builders) {
                    if (is_default_marker(item.first)) {
                        default_graph_builder = nb::cast<graph_builder_s_ptr>(item.second);
                        continue;
                    }
                    // Convert Python key to PlainValue
                    value::PlainValue key(key_type);
                    key_type->ops->from_python(key.data(), nb::cast<nb::object>(item.first), key_type);
                    nested_graph_builders->emplace(std::move(key), nb::cast<graph_builder_s_ptr>(item.second));
                }

                auto py_input_node_ids = nb::cast<nb::dict>(args[8]);
                std::unordered_map<std::string, int> default_input_node_ids;
                auto input_node_ids = std::make_shared<SwitchNodeBuilder::input_node_ids_map>();
                for (auto item: py_input_node_ids) {
                    if (is_default_marker(item.first)) {
                        default_input_node_ids = nb::cast<std::unordered_map<std::string, int>>(item.second);
                        continue;
                    }
                    value::PlainValue key(key_type);
                    key_type->ops->from_python(key.data(), nb::cast<nb::object>(item.first), key_type);
                    input_node_ids->emplace(std::move(key), nb::cast<std::unordered_map<std::string, int>>(item.second));
                }

                auto py_output_node_ids = nb::cast<nb::dict>(args[9]);
                int default_output_node_id = -1;
                auto output_node_ids = std::make_shared<SwitchNodeBuilder::output_node_ids_map>();
                for (auto item: py_output_node_ids) {
                    if (is_default_marker(item.first)) {
                        default_output_node_id = nb::cast<int>(item.second);
                        continue;
                    }
                    value::PlainValue key(key_type);
                    key_type->ops->from_python(key.data(), nb::cast<nb::object>(item.first), key_type);
                    output_node_ids->emplace(std::move(key), nb::cast<int>(item.second));
                }

                auto reload_on_ticked = nb::cast<bool>(args[10]);

                new(self) SwitchNodeBuilder(
                    std::move(signature_), std::move(scalars_),
                    std::move(input_builder_), std::move(output_builder_),
                    std::move(error_builder_), std::move(recordable_state_builder_),
                    key_type,
                    std::move(nested_graph_builders),
                    std::move(input_node_ids),
                    std::move(output_node_ids),
                    reload_on_ticked,
                    default_graph_builder,
                    std::move(default_input_node_ids),
                    default_output_node_id);
            })
            .def_prop_ro("reload_on_ticked", [](const SwitchNodeBuilder& self) { return self._reload_on_ticked; });
    }
} // namespace hgraph
