/*
 * The entry point into the python _hgraph module exposing the C++ types to python.
 *
 * Note that as a pattern, we will return alias shared pointers for objects that have their life-times managed by an outer object
 * such as ExecutionGraph, where the life-time of the objects contained within are all directly managed by the outer object.
 * This reduces the number of shared pointers that need to be constructed inside the graph and thus provides a small improvement
 * on memory and general performance.
 *
 */

#include <hgraph/python/pyb_wiring.h>

void export_types(py::module_&);

PYBIND11_MODULE(_hgraph, m) {
    m.doc() = "The HGraph C++ runtime engine";

    export_types(m);
}