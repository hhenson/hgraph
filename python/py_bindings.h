/**
 * The binding entry points NB_MODULE (module.cpp) calls, one per domain
 * translation unit, plus the single python-node overload registration list
 * shared by module init and reset_registries (keep it single - the two call
 * sites drifting apart was a real bug class).
 */
#ifndef HGRAPH_PYTHON_PY_BINDINGS_H
#define HGRAPH_PYTHON_PY_BINDINGS_H

#include <nanobind/nanobind.h>

namespace hgraph::python_bridge
{
    void bind_type_system(nanobind::module_ &m);        // py_type_system.cpp
    void register_builtin_native_scalar_types();        // py_type_system.cpp
    void bind_ports(nanobind::module_ &m);              // py_ports.cpp
    void bind_wiring(nanobind::module_ &m);             // py_wiring.cpp
    void bind_state_and_services(nanobind::module_ &m); // py_state_services.cpp
    void register_python_overloads();                   // py_nodes.cpp
}  // namespace hgraph::python_bridge

#endif  // HGRAPH_PYTHON_PY_BINDINGS_H
