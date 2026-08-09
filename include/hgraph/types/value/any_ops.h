#ifndef HGRAPH_CPP_ROOT_VALUE_ANY_OPS_H
#define HGRAPH_CPP_ROOT_VALUE_ANY_OPS_H

#include <hgraph/types/value/value_ops.h>

namespace hgraph
{
    /**
     * Canonical ``ValueOps`` for the ``Any`` kind.
     *
     * ``Any`` storage is an embedded owning ``Value`` (see
     * ``ValueTypeKind::Any``). These ops interpret the memory as that
     * ``Value`` and delegate ``hash`` / ``equals`` / ``compare`` /
     * ``to_string`` to it, treating an empty box as a distinct "no value"
     * state (empty == empty; empty < any non-empty; ``to_string`` =
     * ``"None"``). Copy / move are handled by the storage plan's lifecycle
     * (the embedded ``Value``'s own copy/move), so no view-copy hook is
     * installed. ``allows_mutation`` is true so the box can be reassigned.
     */
    [[nodiscard]] const ValueOps &any_ops() noexcept;

    /**
     * The canonical interned ``ValueTypeRef`` for the ``Any`` schema:
     * ``(registry.any(), plan_for<Value>, any_ops())``. Use it to construct
     * a ``Value`` whose kind is ``Any``.
     */
    [[nodiscard]] ValueTypeRef any_type();

    /** Meta-preserving form: a JSON-named Any schema keeps its identity
        while sharing the Any plan + ops. */
    [[nodiscard]] ValueTypeRef any_type(const ValueTypeMetaData &meta);
}  // namespace hgraph

#endif  // HGRAPH_CPP_ROOT_VALUE_ANY_OPS_H
