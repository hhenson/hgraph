#include <hgraph/api/python/py_ts_type_registry.h>
#include <hgraph/api/python/py_value.h>

void export_types(nb::module_ &m) {
    hgraph::value_register_with_nanobind(m);
    hgraph::ts_type_registry_register_with_nanobind(m);
}
