#ifndef HGRAPH_PYTHON_CONVERSION_H
#define HGRAPH_PYTHON_CONVERSION_H

#include <hgraph/config.h>

#if HGRAPH_ENABLE_PYTHON_USER_NODES

#include <hgraph/hgraph_export.h>
#include <hgraph/types/python_object.h>
#include <hgraph/types/time_series/ts_data/base_view.h>
#include <hgraph/types/time_series/ts_input/base_view.h>
#include <hgraph/types/value/value.h>
#include <hgraph/types/value/value_ops.h>
#include <hgraph/types/value/value_view.h>
#include <hgraph/util/date_time.h>

#include <nanobind/nanobind.h>
// Every Python-aware unit must see the same std::string caster: an inline
// instantiation without it (the generic bound-class caster) can win the link
// for the whole library and every str crossing then fails with a bad cast.
#include <nanobind/stl/string.h>

/**
 * Value / TSData / TSInput <-> Python through the type-erased slots (RFC
 * 0035). The type layer's slots are typed on the opaque ``PyRef`` /
 * ``PyNewRef``; this header is the one place that converts between them
 * and nanobind, and it carries the former ``ValueOps::to_python``,
 * ``ValueView::to_python``, ``TSDataView::value_to_python`` ... members as
 * free functions so the type layer needs no Python member at all.
 */
namespace hgraph
{
    // The alias the type layer's guarded conversion bodies used from
    // value_ops.h; it moves out with them, family by family.
    namespace nb = nanobind;
}

namespace hgraph::python_bridge
{
    namespace nb = nanobind;

    // -- opaque reference helpers ------------------------------------------
    [[nodiscard]] inline PyRef borrow(nb::handle source) noexcept { return PyRef{source.ptr()}; }
    [[nodiscard]] inline nb::handle handle_of(PyRef source) noexcept { return nb::handle{source.ptr}; }
    /** Hand a new reference across a slot boundary. */
    [[nodiscard]] inline PyNewRef give(nb::object object) noexcept { return PyNewRef{object.release().ptr()}; }
    /** Receive a new reference from a slot; a null result is a broken slot. */
    [[nodiscard]] HGRAPH_EXPORT nb::object take(PyNewRef result);

    // -- slot adapters ---------------------------------------------------------
    // Wrap a nanobind-typed implementation as an opaque slot so a family's
    // conversion body keeps its ``nb::object`` / ``nb::handle`` signature
    // while it still lives beside its storage (the bodies move to bridge
    // units family by family; see the RFC's implementation plan).
    template <auto Fn>
    PyNewRef to_python_slot(const void *context, const void *memory)
    {
        return give(Fn(context, memory));
    }
    template <auto Fn>
    void from_python_slot(const void *context, const ValueTypeRef &binding, void *memory, PyRef source)
    {
        Fn(context, binding, memory, handle_of(source));
    }
    template <auto Fn>
    PyNewRef to_python_buffer_slot(const void *context, const ValueTypeRef &binding, const ValueArraySource &source)
    {
        return give(Fn(context, binding, source));
    }
    template <auto Fn>
    bool ts_from_python_slot(const void *context, void *memory, PyRef source, DateTime modified_time)
    {
        return Fn(context, memory, handle_of(source), modified_time);
    }
    template <auto Fn>
    PyNewRef ts_delta_to_python_slot(const void *context, const void *memory, DateTime evaluation_time)
    {
        return give(Fn(context, memory, evaluation_time));
    }

    // -- value conversions -----------------------------------------------------
    /** The binding's ``to_python`` slot over ``memory``; throws when the type has none. */
    [[nodiscard]] HGRAPH_EXPORT nb::object to_python(const ValueOps &ops, const void *memory);
    [[nodiscard]] inline nb::object to_python(const ValueTypeRef &binding, const void *memory)
    {
        return to_python(binding.ops_ref(), memory);
    }
    HGRAPH_EXPORT void from_python(const ValueOps &ops, const ValueTypeRef &binding, void *memory, nb::handle source);
    inline void from_python(const ValueTypeRef &binding, void *memory, nb::handle source)
    {
        from_python(binding.ops_ref(), binding, memory, source);
    }
    [[nodiscard]] HGRAPH_EXPORT bool can_to_python_buffer(const ValueOps &ops, const ValueTypeRef &binding) noexcept;
    [[nodiscard]] HGRAPH_EXPORT nb::object to_python_buffer(const ValueOps &ops, const ValueTypeRef &binding,
                                                            const ValueArraySource &source);

    /** ``ValueView::to_python``: requires a non-empty view. */
    [[nodiscard]] HGRAPH_EXPORT nb::object to_python(const ValueView &view);
    /** ``ValueView::from_python``: a mutation-protocol write into a non-empty view; ``None`` is refused. */
    HGRAPH_EXPORT void from_python(const ValueView &view, nb::handle source);
    /**
     * ``ValueView::assign_from_python``: a LIFECYCLE-level write like
     * ``copy_assign`` -- it requires writable storage but not the mutation
     * protocol, so compact (immutable-API) containers construct through it.
     */
    HGRAPH_EXPORT void assign_from_python(const ValueView &view, nb::handle source);
    /** ``Value::to_python``: ``None`` for an empty value. */
    [[nodiscard]] HGRAPH_EXPORT nb::object to_python(const Value &value);
    /** ``Value::from_python``: ``None`` resets; otherwise a schema-bound replacement. */
    HGRAPH_EXPORT void from_python(Value &value, nb::handle source);

    // -- time-series conversions ----------------------------------------------
    /**
     * Export the current value through the live representation's erased
     * TSDataOps. Structural implementations recurse through child TSDataOps
     * and produce the complete public Python shape; callers never switch on
     * TSTypeKind and rebuild it.
     */
    [[nodiscard]] HGRAPH_EXPORT nb::object value_to_python(const TSDataView &view);
    /** Export the delta for ``evaluation_time`` the same way; ``None`` when unchanged. */
    [[nodiscard]] HGRAPH_EXPORT nb::object delta_value_to_python(const TSDataView &view, DateTime evaluation_time);
    /** Apply a Python object through the binding's erased conversion; true when newly modified. */
    [[nodiscard]] HGRAPH_EXPORT bool from_python(TSDataMutationView &view, nb::handle source);
    [[nodiscard]] inline bool from_python(TSDataMutationView &&view, nb::handle source)
    {
        return from_python(view, source);
    }
    /** Export an input's current value through the resolved endpoint's erased TSDataOps. */
    [[nodiscard]] HGRAPH_EXPORT nb::object value_to_python(const TSInputView &view);
    /** Export an input's delta, preserving the sampled-rebind semantics TSInputView owns. */
    [[nodiscard]] HGRAPH_EXPORT nb::object delta_value_to_python(const TSInputView &view);
}  // namespace hgraph::python_bridge

#endif  // HGRAPH_ENABLE_PYTHON_USER_NODES
#endif  // HGRAPH_PYTHON_CONVERSION_H
