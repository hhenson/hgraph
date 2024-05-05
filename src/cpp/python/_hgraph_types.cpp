
#include <hgraph/python/pyb_wiring.h>
#include <hgraph/types/node.h>

void export_types(py::module_ &m) {
    using namespace hgraph;

    node_type_enum_py_register(m);
    injectable_type_enum(m);
}
