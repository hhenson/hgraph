#ifndef HGRAPH_TYPES_TIME_SERIES_TS_DATA_EMPTY_DELTA_FIELDS_H
#define HGRAPH_TYPES_TIME_SERIES_TS_DATA_EMPTY_DELTA_FIELDS_H

#include <hgraph/types/value/value.h>

namespace hgraph
{
    struct ValueTypeMetaData;

    namespace ts_data_detail
    {
        /**
         * The interned empty ``Set<key>`` Value backing the always-empty
         * ``removed_strict`` field of PROJECTED TSD deltas (strict removals
         * only ever originate from user-authored Python dicts, never from a
         * live TSD's own tracking).
         *
         * Ownership rule (registry_reset.cpp): this registry OWNS Values, so
         * it is cleared BEFORE the common type records; the slot/proxy
         * contexts hold only non-owning pointers into it (their destructors
         * must stay trivial for the Value — the ASAN'd 2026-07-17 lesson:
         * contexts are cleared after the records their Values reference).
         * Interning happens at context construction (build time); per-tick
         * readers just dereference the pointer — no lock on the hot path.
         */
        [[nodiscard]] const Value *interned_empty_set(const ValueTypeMetaData *key_meta);

        /** Clears the interned empty sets (registry reset only). */
        void clear_interned_empty_sets() noexcept;
    }  // namespace ts_data_detail
}  // namespace hgraph

#endif  // HGRAPH_TYPES_TIME_SERIES_TS_DATA_EMPTY_DELTA_FIELDS_H
