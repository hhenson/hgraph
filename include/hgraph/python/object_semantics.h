#ifndef HGRAPH_PYTHON_OBJECT_SEMANTICS_H
#define HGRAPH_PYTHON_OBJECT_SEMANTICS_H

#if HGRAPH_ENABLE_PYTHON_USER_NODES

#include <hgraph/hgraph_export.h>

#include <Python.h>

#include <compare>
#include <cstddef>
#include <string>

/**
 * The one set of Python-object value primitives: what hashing, equality,
 * ordering and rendering mean for a stored ``PyObject``. Every ops table that
 * stores one (Python-owned Bundles, retained values, the bridge's ``PyObj``
 * hash) delegates here; none re-derives the rules. This header is the
 * contract; the concrete CPython implementation lives behind the bridge's
 * ``impl`` boundary (``src/hgraph/python/impl/object_semantics.cpp``), so a
 * semantic owner depends on these four functions and never on bridge state.
 *
 * Rules: hash falls back to the object's address when the object is
 * unhashable; equality runs Python ``__eq__`` first so its exceptions stay
 * observable, then refuses to call two unhashable objects equal so equal
 * values keep equal hashes; ordering maps a Python comparison error to
 * ``unordered`` and an empty side below a live one. Each acquires the GIL
 * itself. ``object_hash`` requires a live object; callers decide what an
 * empty holder means.
 */
namespace hgraph::python_bridge {
[[nodiscard]] HGRAPH_EXPORT std::size_t object_hash(PyObject *object);
[[nodiscard]] HGRAPH_EXPORT bool object_equals(PyObject *lhs, PyObject *rhs);
[[nodiscard]] HGRAPH_EXPORT std::partial_ordering object_compare(PyObject *lhs,
                                                                PyObject *rhs) noexcept;
[[nodiscard]] HGRAPH_EXPORT std::string object_str(PyObject *object);
} // namespace hgraph::python_bridge

#endif // HGRAPH_ENABLE_PYTHON_USER_NODES
#endif // HGRAPH_PYTHON_OBJECT_SEMANTICS_H
