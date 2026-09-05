#ifndef HGRAPH_PYTHON_RETAINED_VALUE_H
#define HGRAPH_PYTHON_RETAINED_VALUE_H

#include <hgraph/python/bridge_state.h>
#include <hgraph/types/metadata/type_meta_data.h>

namespace hgraph::python_bridge
{
    /**
     * Python-aware value storage (``python_bridge.rst``, "Consumer-selected
     * Python value storage"): the retained-value entry -- the ops table for a
     * ``PyObject`` retained as the value itself behind a supported schema --
     * and the policy that says which schemas may retain. It lives on the
     * bridge and reaches the plan factory only through the
     * ``PythonStorageProvider`` table it registers: the unit registers the
     * table when it is loaded (it is compiled in exactly when Python user
     * nodes are enabled, with or without the ``_hgraph`` module, so a
     * standalone Python-user-node build has the policy too), and the module
     * initializer calls ``register_python_storage_provider`` again,
     * idempotently. The type layer sees Python only through registered ops
     * tables, never through a conditional of its own.
     */
    HGRAPH_EXPORT void register_python_storage_provider() noexcept;

    /**
     * Validate and, where the canonical Python read shape requires it,
     * normalize an object before it is retained in Python-aware storage
     * (a list returned for ``TS[tuple[T, ...]]`` is retained as a tuple).
     */
    [[nodiscard]] HGRAPH_EXPORT nb::object prepare_python_storage_value(const ValueTypeMetaData *schema,
                                                                         nb::handle source);
}  // namespace hgraph::python_bridge

#endif  // HGRAPH_PYTHON_RETAINED_VALUE_H
