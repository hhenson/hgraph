#ifndef HGRAPH_TYPES_PYTHON_OBJECT_H
#define HGRAPH_TYPES_PYTHON_OBJECT_H

/**
 * The opaque CPython reference the type layer can name without a Python
 * header (RFC 0035).
 *
 * ``struct _object`` is CPython's ``PyObject`` tag; declaring it here is
 * compatible with ``<Python.h>`` (which completes it) and with the limited
 * API. The two wrappers make ownership visible at every ops-table slot:
 * a ``PyRef`` is borrowed for the duration of the call, a ``PyNewRef`` is
 * one new reference the receiver owns. Only bridge code
 * (``include/hgraph/python/conversion.h``) converts between them and the
 * binding library's handle types; the type layer stores and forwards them
 * and never dereferences one.
 */
struct _object;

namespace hgraph
{
    /** A borrowed reference: the caller keeps ownership for the call. */
    struct PyRef
    {
        ::_object *ptr{nullptr};
    };

    /** A new reference: the receiver owns exactly one reference. */
    struct PyNewRef
    {
        ::_object *ptr{nullptr};
    };
}  // namespace hgraph

#endif  // HGRAPH_TYPES_PYTHON_OBJECT_H
