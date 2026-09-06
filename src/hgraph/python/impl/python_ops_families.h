#ifndef HGRAPH_PYTHON_IMPL_PYTHON_OPS_FAMILIES_H
#define HGRAPH_PYTHON_IMPL_PYTHON_OPS_FAMILIES_H

#include <hgraph/types/python_ops.h>

/**
 * The per-family fillers of the bridge's ``PythonOps`` table (RFC 0035).
 * ``python_ops.cpp`` builds the one table and calls each family's unit to
 * fill its section; a family's conversion bodies live in that unit beside
 * nothing but the public storage API they read.
 */
namespace hgraph::python_bridge
{
    void fill_compact_container_conversions(PythonOps::Compact &section) noexcept;
    void fill_mutable_container_conversions(PythonOps::Mutable &section) noexcept;
}  // namespace hgraph::python_bridge

#endif  // HGRAPH_PYTHON_IMPL_PYTHON_OPS_FAMILIES_H
