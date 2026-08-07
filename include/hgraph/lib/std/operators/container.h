#ifndef HGRAPH_LIB_STD_OPERATORS_CONTAINER_H
#define HGRAPH_LIB_STD_OPERATORS_CONTAINER_H

#include <hgraph/types/operator_dispatch.h>
#include <hgraph/types/primitive_types.h>
#include <hgraph/types/static_node.h>
#include <hgraph/types/static_schema.h>

namespace hgraph::stdlib
{
    /**
     * Container access / indexing operator **definitions** (markers only). Mirrors the
     * Python ``hgraph`` indexing operators — ``[]`` (``getitem_``), ``.`` (``getattr_`` /
     * ``setattr_``), ``in`` (``contains_``), ``len`` (``len_``), ``index_of`` and
     * ``is_empty``. These apply across the container time-series kinds (``TSL`` / ``TSD`` /
     * ``TSB`` / ``TSS`` and ``TS`` of a scalar container).
     */

    /** ``getitem_`` — the ``[]`` operator: ``ts[key]``. */
    struct getitem_ : Operator<"getitem_", In<"ts", TsVar<"S">>, In<"key", TsVar<"K">>, Out<TsVar<"O">>>
    {
    };

    /** ``getattr_`` — the ``.`` (attribute access) operator: ``ts.attr``. ``attr`` is a wiring-time
        string; an optional ``default_value`` may be supplied by an implementation. */
    struct getattr_ : Operator<"getattr_", In<"ts", TsVar<"S">>, Scalar<"attr", Str>, Out<TsVar<"O">>>
    {
    };

    /** ``setattr_`` — sets ``ts.attr`` to ``value`` and returns the updated bundle. */
    struct setattr_
        : Operator<"setattr_", In<"ts", TsVar<"S">>, Scalar<"attr", Str>, In<"value", TsVar<"V">>, Out<TsVar<"O">>>
    {
    };

    /** ``contains_`` — the ``in`` operator: ``item in ts`` -> ``TS<Bool>``. */
    struct contains_ : Operator<"contains_", In<"ts", TsVar<"S">>, In<"item", TsVar<"I">>, Out<TS<Bool>>>
    {
    };

    /** ``len_`` — the ``len`` operator -> ``TS<Int>``. */
    struct len_ : Operator<"len_", In<"ts", TsVar<"S">>, Out<TS<Int>>>
    {
    };

    /** ``index_of`` — the index of ``item`` within ``ts`` -> ``TS<Int>``. */
    struct index_of : Operator<"index_of", In<"ts", TsVar<"S">>, In<"item", TsVar<"I">>, Out<TS<Int>>>
    {
    };

    /** ``is_empty`` — whether the time-series value is considered empty -> ``TS<Bool>``. */
    struct is_empty : Operator<"is_empty", In<"ts", TsVar<"S">>, Out<TS<Bool>>>
    {
    };

    /** ``dereference`` — materialize ``REF[TSB[...]]`` as a bundle whose
        fields are references to the corresponding fields of the referenced
        bundle. */
    struct dereference : Operator<"dereference", In<"tsb", REF<TsVar<"S">>>, Out<TsVar<"O">>>
    {
    };
}  // namespace hgraph::stdlib

#endif  // HGRAPH_LIB_STD_OPERATORS_CONTAINER_H
