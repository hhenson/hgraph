#include <hgraph/builders/nodes/switch_node_builder.h>
#include <hgraph/builders/input_builder.h>
#include <hgraph/builders/output_builder.h>
#include <hgraph/types/node.h>
#include <hgraph/types/tsb.h>
#include <hgraph/types/time_series_type.h>
#include <hgraph/nodes/switch_node.h>

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

    template<typename T>
    auto create_switch_node_builder(T *self, const nb::args &args) {
        // Expected Python signature (positional):
        // (signature, scalars, input_builder, output_builder, error_builder, recordable_state_builder,
        //  nested_graph_builders, input_node_ids, output_node_ids, reload_on_ticked)
        if (args.size() != 10) {
            throw nb::type_error("SwitchNodeBuilder expects 10 positional arguments");
        }

        using K = typename T::key_type;

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

        // Convert Python dicts to typed C++ maps
        // For typed switches (K != nb::object), extract DEFAULT builder separately and skip DEFAULT marker keys in maps
        auto py_nested_graph_builders = nb::cast<nb::dict>(args[6]);
        graph_builder_ptr default_graph_builder = nullptr;
        std::unordered_map<K, graph_builder_ptr> nested_graph_builders;
        for (auto item: py_nested_graph_builders) {
            // Extract DEFAULT marker for typed switches
            if constexpr (!std::is_same_v<K, nb::object>) {
                if (is_default_marker(item.first)) {
                    default_graph_builder = nb::cast<graph_builder_ptr>(item.second);
                    continue;
                }
            }
            nested_graph_builders[nb::cast<K>(item.first)] = nb::cast<graph_builder_ptr>(item.second);
        }

        auto py_input_node_ids = nb::cast<nb::dict>(args[7]);
        std::unordered_map<K, std::unordered_map<std::string, int> > input_node_ids;
        std::unordered_map<std::string, int> default_input_node_ids;
        for (auto item: py_input_node_ids) {
            if constexpr (!std::is_same_v<K, nb::object>) {
                if (is_default_marker(item.first)) {
                    default_input_node_ids = nb::cast<std::unordered_map<std::string, int> >(item.second);
                    continue;
                }
            }
            input_node_ids[nb::cast<K>(item.first)] = nb::cast<std::unordered_map<std::string, int> >(item.second);
        }

        auto py_output_node_ids = nb::cast<nb::dict>(args[8]);
        std::unordered_map<K, int> output_node_ids;
        int default_output_node_id = -1;
        for (auto item: py_output_node_ids) {
            if constexpr (!std::is_same_v<K, nb::object>) {
                if (is_default_marker(item.first)) {
                    default_output_node_id = nb::cast<int>(item.second);
                    continue;
                }
            }
            output_node_ids[nb::cast<K>(item.first)] = nb::cast<int>(item.second);
        }

        auto reload_on_ticked = nb::cast<bool>(args[9]);

        return new(self) T(std::move(signature_), std::move(scalars_), std::move(input_builder_),
                           std::move(output_builder_),
                           std::move(error_builder_), std::move(recordable_state_builder_),
                           std::move(nested_graph_builders),
                           std::move(input_node_ids), std::move(output_node_ids), reload_on_ticked,
                           default_graph_builder,
                           std::move(default_input_node_ids), default_output_node_id);
    }

    template<typename K>
    SwitchNodeBuilder<K>::SwitchNodeBuilder(
        node_signature_ptr signature_, nb::dict scalars_, std::optional<input_builder_ptr> input_builder_,
        std::optional<output_builder_ptr> output_builder_, std::optional<output_builder_ptr> error_builder_,
        std::optional<output_builder_ptr> recordable_state_builder_,
        const std::unordered_map<K, graph_builder_ptr> &nested_graph_builders,
        const std::unordered_map<K, std::unordered_map<std::string, int> > &input_node_ids,
        const std::unordered_map<K, int> &output_node_ids, bool reload_on_ticked,
        graph_builder_ptr default_graph_builder,
        const std::unordered_map<std::string, int> &default_input_node_ids,
        int default_output_node_id)
        : BaseSwitchNodeBuilder(std::move(signature_), std::move(scalars_), std::move(input_builder_),
                                std::move(output_builder_),
                                std::move(error_builder_), std::move(recordable_state_builder_)),
          nested_graph_builders(nested_graph_builders), input_node_ids(input_node_ids),
          output_node_ids(output_node_ids),
          reload_on_ticked(reload_on_ticked), default_graph_builder(std::move(default_graph_builder)),
          default_input_node_ids(default_input_node_ids), default_output_node_id(default_output_node_id) {
    }

    template<typename K>
    node_ptr SwitchNodeBuilder<K>::make_instance(const std::vector<int64_t> &owning_graph_id, int64_t node_ndx) const {
        nb::ref<Node> node{
            new SwitchNode<K>(node_ndx, owning_graph_id, signature, scalars, nested_graph_builders, input_node_ids,
                              output_node_ids, reload_on_ticked, default_graph_builder,
                              default_input_node_ids, default_output_node_id)
        };
        _build_inputs_and_outputs(node);
        return node;
    }

    // Explicit template instantiations
    template struct SwitchNodeBuilder<bool>;
    template struct SwitchNodeBuilder<int64_t>;
    template struct SwitchNodeBuilder<double>;
    template struct SwitchNodeBuilder<engine_date_t>;
    template struct SwitchNodeBuilder<engine_time_t>;
    template struct SwitchNodeBuilder<engine_time_delta_t>;
    template struct SwitchNodeBuilder<nb::object>;

    void switch_node_builder_register_with_nanobind(nb::module_ &m) {
        nb::class_<BaseSwitchNodeBuilder, BaseNodeBuilder>(m, "BaseSwitchNodeBuilder");

        nb::class_<SwitchNodeBuilder<bool>, BaseSwitchNodeBuilder>(m, "SwitchNodeBuilder_bool")
                .def("__init__", [](SwitchNodeBuilder<bool> *self, const nb::args &args) {
                    create_switch_node_builder(self, args);
                })
                .def_ro("nested_graph_builders", &SwitchNodeBuilder<bool>::nested_graph_builders)
                .def_ro("input_node_ids", &SwitchNodeBuilder<bool>::input_node_ids)
                .def_ro("output_node_ids", &SwitchNodeBuilder<bool>::output_node_ids)
                .def_ro("reload_on_ticked", &SwitchNodeBuilder<bool>::reload_on_ticked);

        nb::class_<SwitchNodeBuilder<int64_t>, BaseSwitchNodeBuilder>(m, "SwitchNodeBuilder_int")
                .def("__init__", [](SwitchNodeBuilder<int64_t> *self, const nb::args &args) {
                    create_switch_node_builder(self, args);
                })
                .def_ro("nested_graph_builders", &SwitchNodeBuilder<int64_t>::nested_graph_builders)
                .def_ro("input_node_ids", &SwitchNodeBuilder<int64_t>::input_node_ids)
                .def_ro("output_node_ids", &SwitchNodeBuilder<int64_t>::output_node_ids)
                .def_ro("reload_on_ticked", &SwitchNodeBuilder<int64_t>::reload_on_ticked);

        nb::class_<SwitchNodeBuilder<double>, BaseSwitchNodeBuilder>(m, "SwitchNodeBuilder_float")
                .def("__init__", [](SwitchNodeBuilder<double> *self, const nb::args &args) {
                    create_switch_node_builder(self, args);
                })
                .def_ro("nested_graph_builders", &SwitchNodeBuilder<double>::nested_graph_builders)
                .def_ro("input_node_ids", &SwitchNodeBuilder<double>::input_node_ids)
                .def_ro("output_node_ids", &SwitchNodeBuilder<double>::output_node_ids)
                .def_ro("reload_on_ticked", &SwitchNodeBuilder<double>::reload_on_ticked);

        nb::class_<SwitchNodeBuilder<engine_date_t>, BaseSwitchNodeBuilder>(m, "SwitchNodeBuilder_date")
                .def("__init__", [](SwitchNodeBuilder<engine_date_t> *self, const nb::args &args) {
                    create_switch_node_builder(self, args);
                })
                .def_ro("nested_graph_builders", &SwitchNodeBuilder<engine_date_t>::nested_graph_builders)
                .def_ro("input_node_ids", &SwitchNodeBuilder<engine_date_t>::input_node_ids)
                .def_ro("output_node_ids", &SwitchNodeBuilder<engine_date_t>::output_node_ids)
                .def_ro("reload_on_ticked", &SwitchNodeBuilder<engine_date_t>::reload_on_ticked);

        nb::class_<SwitchNodeBuilder<engine_time_t>, BaseSwitchNodeBuilder>(m, "SwitchNodeBuilder_date_time")
                .def("__init__", [](SwitchNodeBuilder<engine_time_t> *self, const nb::args &args) {
                    create_switch_node_builder(self, args);
                })
                .def_ro("nested_graph_builders", &SwitchNodeBuilder<engine_time_t>::nested_graph_builders)
                .def_ro("input_node_ids", &SwitchNodeBuilder<engine_time_t>::input_node_ids)
                .def_ro("output_node_ids", &SwitchNodeBuilder<engine_time_t>::output_node_ids)
                .def_ro("reload_on_ticked", &SwitchNodeBuilder<engine_time_t>::reload_on_ticked);

        nb::class_<SwitchNodeBuilder<engine_time_delta_t>, BaseSwitchNodeBuilder>(m, "SwitchNodeBuilder_time_delta")
                .def("__init__", [](SwitchNodeBuilder<engine_time_delta_t> *self, const nb::args &args) {
                    create_switch_node_builder(self, args);
                })
                .def_ro("nested_graph_builders", &SwitchNodeBuilder<engine_time_delta_t>::nested_graph_builders)
                .def_ro("input_node_ids", &SwitchNodeBuilder<engine_time_delta_t>::input_node_ids)
                .def_ro("output_node_ids", &SwitchNodeBuilder<engine_time_delta_t>::output_node_ids)
                .def_ro("reload_on_ticked", &SwitchNodeBuilder<engine_time_delta_t>::reload_on_ticked);

        nb::class_<SwitchNodeBuilder<nb::object>, BaseSwitchNodeBuilder>(m, "SwitchNodeBuilder_object")
                .def("__init__", [](SwitchNodeBuilder<nb::object> *self, const nb::args &args) {
                    create_switch_node_builder(self, args);
                })
                .def_ro("nested_graph_builders", &SwitchNodeBuilder<nb::object>::nested_graph_builders)
                .def_ro("input_node_ids", &SwitchNodeBuilder<nb::object>::input_node_ids)
                .def_ro("output_node_ids", &SwitchNodeBuilder<nb::object>::output_node_ids)
                .def_ro("reload_on_ticked", &SwitchNodeBuilder<nb::object>::reload_on_ticked);
    }
} // namespace hgraph