#ifndef HGRAPH_TYPES_TIME_SERIES_TS_DELTA_H
#define HGRAPH_TYPES_TIME_SERIES_TS_DELTA_H

#include <hgraph/hgraph_export.h>
#include <hgraph/types/time_series/ts_data/current_state_ops.h>

namespace hgraph
{
    class TSDataView;
    class TSInputView;
    class TSOutputView;
    struct TSValueTypeMetaData;
    struct ValueTypeMetaData;
    class ValueView;
    class Value;

    /**
     * Runtime, type-erased per-cycle delta capture / apply — the schema-as-data
     * twin of the compile-time ``ts_delta<S>`` (``static_node.h``). Both dispatch
     * through the live endpoint's ``TSDataOps`` table and recurse through child
     * ops, so a single (non-templated) API serves every replayable time-series
     * schema — the basis for the erased ``replay`` / ``record`` utility nodes.
     *
     * ``capture_delta`` reads a live input and **rebuilds** the canonical delta
     * ``Value`` whose schema is ``in.schema()->delta_value_schema`` (via the
     * value-layer builders, so the result is owned and copyable — the runtime's
     * transient delta storage omits value-layer copy hooks, so a direct copy of
     * ``delta_value()`` is not safe). ``apply_delta`` is the inverse: it re-creates
     * output ticks from such a canonical delta ``Value``.
     *
     * The canonical per-kind delta shape (``type_registry.cpp``):
     *   ``TS<T>`` / ``SIGNAL`` / ``TSW<T>`` -> scalar; ``TSS<T>`` ->
     *   ``Bundle{added: Set<T>, removed: Set<T>}``; ``TSD<K,V>`` ->
     *   ``Bundle{removed: Set<K>, modified: Map<K, delta(V)>}``;
     *   fixed ``TSL<C,N>`` -> ``Map<int, delta(C)>``; dynamic ``TSL<C,0>`` ->
     *   ``Bundle{removed: Set<int>, modified: Map<int, delta(C)>}`` (RFC 0031);
     *   ``TSB{f...}`` ->
     *   ``Bundle{f: delta(f)...}`` (recursive in children).
     *
     * ``REF`` throws a clear ``std::logic_error``: it is a separate
     * reference-binding surface rather than ordinary value replay.
     */
    [[nodiscard]] HGRAPH_EXPORT Value capture_delta(const TSInputView &in);

    /** Rebuild a canonical delta containing every currently-valid value.
        Unlike ``capture_delta``, this is independent of per-cycle modified
        flags and is used when a transport first observes an already-partial
        composite input. */
    [[nodiscard]] HGRAPH_EXPORT Value capture_current_delta(const TSInputView &in);

    /** True when a captured delta represents an externally observable tick.
        This includes invalid structural-removal and pre-valid tick-window
        events, while excluding scheduling-only empty structural deltas. */
    [[nodiscard]] HGRAPH_EXPORT bool delta_is_observable(
        const TSInputView &in, const ValueView &delta);

    HGRAPH_EXPORT void apply_delta(const TSOutputView &out, const ValueView &delta);

    /**
     * Apply a whole current value to a time-series output.
     *
     * This is the current-value counterpart to ``apply_delta``. It accepts the
     * time-series schema's value-layer shape and recurses through collection
     * children so non-peered structures such as fixed ``TSL`` and ``TSB`` can be
     * materialized from ordinary value-layer containers.
     */
    [[nodiscard]] HGRAPH_EXPORT bool current_value_schema_compatible(
        const TSValueTypeMetaData &schema, const ValueTypeMetaData &value_schema);
    HGRAPH_EXPORT void apply_current_value(const TSOutputView &out, const ValueView &value);

    /** Reconcile a publication/snapshot output with a live source TSData tree. */
    HGRAPH_EXPORT void reconcile_current_state(
        const TSOutputView &target, const TSInputView &source,
        TSCurrentReconcileOptions options = {});
    HGRAPH_EXPORT void reconcile_current_state(
        const TSOutputView &target, const TSDataView &source,
        TSCurrentReconcileOptions options = {});
}  // namespace hgraph

#endif  // HGRAPH_TYPES_TIME_SERIES_TS_DELTA_H
