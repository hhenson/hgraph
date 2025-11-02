#include <hgraph/python/global_keys.h>
#include <fmt/format.h>
#include <nanobind/stl/string.h>

namespace hgraph {
    namespace keys {
        std::string context_output_key(const std::vector<int64_t> &owning_graph_id, const std::string &path) {
            // Build Python tuple string for owning_graph_id to match Python reference formatting
            nb::list py_list;
            for (auto id: owning_graph_id) {
                py_list.append(id);
            }
            std::string og_tuple = nb::cast<std::string>(nb::str(nb::tuple(py_list)));
            return fmt::format("context-{}-{}", og_tuple, path);
        }
    } // namespace keys

    // Nanobind registration
    void OutputKeyBuilder::register_with_nanobind(nb::module_ &m) {
        nb::class_<OutputKeyBuilder>(m, "OutputKeyBuilder")
                .def(nb::init<>())
                .def("output_key", &OutputKeyBuilder::output_key)
                .def("output_subscriber_key", &OutputKeyBuilder::output_subscriber_key)
                .def("context_output_key", &OutputKeyBuilder::context_output_key)
                .def("component_key", &OutputKeyBuilder::component_key);
    }

    std::string OutputKeyBuilder::context_output_key(const nb::tuple &owning_graph_id, const std::string &path) const {
        // owning_graph_id already a Python tuple; convert to string consistent with Python formatting
        std::string og_tuple = nb::cast<std::string>(nb::str(owning_graph_id));
        return fmt::format("context-{}-{}", og_tuple, path);
    }

    std::string OutputKeyBuilder::component_key(const nb::object &id_or_label) const {
        if (nb::isinstance<nb::str>(id_or_label)) {
            return keys::component_key(nb::cast<std::string>(id_or_label));
        } else if (nb::isinstance<nb::int_>(id_or_label)) {
            return keys::component_key(nb::cast<int64_t>(id_or_label));
        }
        // Fallback: use Python str() conversion
        return keys::component_key(nb::cast<std::string>(nb::str(id_or_label)));
    }
} // namespace hgraph