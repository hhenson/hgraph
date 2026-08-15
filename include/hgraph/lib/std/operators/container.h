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

    /** Select an item from a collection-valued time series.
        Python's ``ts[key]`` syntax wires this operator; live keys retarget the output
        when they change.
        @param ts Collection, mapping, list, bundle, or other indexable input.
        @param key Index, key, or slice selector supported by the chosen overload.
        @return The selected item as a time-series port.
        @par Python example
        @code{.py}
        selected_price = prices[symbol]
        @endcode */
    struct getitem_ : Operator<"getitem_", In<"ts", TsVar<"S">>, In<"key", TsVar<"K">>, Out<TsVar<"O">>>
    {
    };

    /** Project a named field or attribute from a structured time-series value.
        Attribute selection is a wiring-time operation because it determines the output schema;
        normal ``port.field`` syntax delegates to this operator.
        @param ts Structured input.
        @param attr Field name fixed when the graph is wired.
        @param default_value Optional fallback for overloads that support missing attributes.
        @return A port carrying the selected field or fallback.
        @par Python example
        @code{.py}
        price = quote.price  # equivalent to hg.getattr_(quote, "price")
        @endcode */
    struct getattr_ : Operator<"getattr_", In<"ts", TsVar<"S">>, Scalar<"attr", Str>, Out<TsVar<"O">>>
    {
    };

    /** Return a structured value with one field replaced by another time-series value.
        @param ts Original structured input.
        @param attr Field name fixed at wiring time.
        @param value Replacement field value.
        @return A structure with the same schema and updated field.
        @par Python example
        @code{.py}
        repriced = hg.setattr_(quote, "price", new_price)
        @endcode */
    struct setattr_
        : Operator<"setattr_", In<"ts", TsVar<"S">>, Scalar<"attr", Str>, In<"value", TsVar<"V">>, Out<TsVar<"O">>>
    {
    };

    /** Test membership of an item in the current collection value.
        Call the operator directly because Python's ``in`` syntax coerces its result to a
        scalar boolean and cannot represent a wiring port.
        @param ts Collection or mapping to search.
        @param item Candidate member or key.
        @return True while ``item`` is contained in ``ts``.
        @par Python example
        @code{.py}
        subscribed = hg.contains_(subscriptions, symbol)
        @endcode
        @note Cost: O(n) scan per tick for list/tuple inputs; O(1) for sets/dicts. */
    struct contains_ : Operator<"contains_", In<"ts", TsVar<"S">>, In<"item", TsVar<"I">>, Out<TS<Bool>>>
    {
    };

    /** Return the current number of elements in a collection-valued input.
        @param ts Collection, mapping, string, or supported structural value.
        @return An integer that updates when collection length changes.
        @par Python example
        @code{.py}
        active_count = hg.len_(active_orders)
        @endcode */
    struct len_ : Operator<"len_", In<"ts", TsVar<"S">>, Out<TS<Int>>>
    {
    };

    /** Locate an item within an ordered collection.
        @param ts Ordered collection to search.
        @param item Value whose position is requested.
        @return Zero-based index according to the selected overload's missing-item policy.
        @par Python example
        @code{.py}
        position = hg.index_of(priority_order, symbol)
        @endcode
        @note Cost: O(n) scan per tick. */
    struct index_of : Operator<"index_of", In<"ts", TsVar<"S">>, In<"item", TsVar<"I">>, Out<TS<Int>>>
    {
    };

    /** Test the selected type's empty-value semantics.
        @param ts Collection, string, mapping, or other supported value.
        @return True when the current value contains no elements or content.
        @par Python example
        @code{.py}
        no_orders = hg.is_empty(active_orders)
        @endcode */
    struct is_empty : Operator<"is_empty", In<"ts", TsVar<"S">>, Out<TS<Bool>>>
    {
    };

    /** Materialize a reference to a structured time series as a structural port.
        Fields or list elements remain references to the original sources rather than
        copied scalar snapshots.
        @param tsb Reference to a bundle or fixed time-series list.
        @return A structural port whose children preserve reference semantics.
        @par Python example
        @code{.py}
        live_bundle = hg.dereference(bundle_ref)
        @endcode */
    struct dereference : Operator<"dereference", In<"tsb", REF<TsVar<"S">>>, Out<TsVar<"O">>>
    {
    };
}  // namespace hgraph::stdlib

#endif  // HGRAPH_LIB_STD_OPERATORS_CONTAINER_H
