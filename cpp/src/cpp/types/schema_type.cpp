#include <hgraph/types/schema_type.h>

namespace hgraph {
    void AbstractSchema::register_with_nanobind(nb::module_ &m) {
        nb::class_<AbstractSchema, intrusive_base>(m, "AbstractSchema")
                .def_prop_ro("keys", [](const AbstractSchema &self) {
                    nb::list py_list;
                    for (const auto &key: self.keys()) { py_list.append(key); }
                    return py_list;
                })
                .def("get_value", &AbstractSchema::get_value, "key"_a);
    }
} // namespace hgraph