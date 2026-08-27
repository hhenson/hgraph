// types/registry_reset.h — the ONE canonical, ordered teardown of every
// process-wide registry/factory. The ordering is load-bearing: several
// registries intern by *pointer* into artifacts another registry owns, so a
// borrower must always be cleared before its lender frees the pointees (a
// missing/misordered clear historically caused stale-pointer reuse and memory
// corruption in aggregate test runs). Any NEW pointer-keyed registry or
// binding cache MUST be added to reset_all_registries() — never grow a second
// teardown sequence elsewhere. Reset is a test-only facility; production
// processes never reset.
//
// The ONE exception is a cache that sits ABOVE this function in the link order
// (hgraph_stdlib links hgraph_wiring links hgraph_runtime, never the reverse),
// which reset_all_registries() cannot name without inverting the dependency.
// Such a cache compares ``TypeRegistry::reset_generation()`` and drops itself
// when it moves — see ``ts_table_layout`` and the table type-ops overrides
// beside it. That is self-invalidating, not a second teardown sequence: no
// caller has to remember it. Do NOT use it to opt a reachable cache out of the
// list above.
//
// This is a LINK constraint, not a style preference. Calling an hgraph_stdlib
// symbol from here compiles, and on ELF it even links — the undefined symbol
// is simply deferred to load time — so a Linux-only CI leg reports nothing.
// Mach-O's default ``-undefined error`` rejects it outright, so the failure
// surfaces as a broken macOS shared build far from the edit that caused it.
// Every clear named below is defined in hgraph_runtime or hgraph_wiring; keep
// it that way.
#ifndef HGRAPH_TYPES_REGISTRY_RESET_H
#define HGRAPH_TYPES_REGISTRY_RESET_H

#include <hgraph/hgraph_export.h>

namespace hgraph
{
    /**
     * Reset every process-wide registry/factory, in dependency order:
     *
     * 1. ``OperatorRegistry``, converter registries, interned native-zone
     *    bindings, and the test-only zone-name generation — candidates borrow
     *    schemas, their owned Values retain common type records, native-zone
     *    bindings borrow ``ZoneId`` handles, and stale handles must be
     *    invalidated between tests.
     * 2. The process-wide immutable shared-value arena — slots borrow concrete
     *    value records and must be withdrawn while those records are live.
     * 3. ``TypeRecordRegistry`` — records borrow plan and ops contexts from the
     *    time-series and value factories below. Cached record handles are
     *    trivially cleared later and are never dereferenced during reset.
     * 4. ``TSInputBuilderFactory`` / ``TSDataPlanFactory`` — clear the endpoint
     *    and TSData ops contexts after the records that pointed into them.
     * 5. ``ValuePlanFactory`` — plans borrow schema pointers (and the later
     *    clears release MemoryUtils synthesised composite/array plans).
     * 6. Compact, mutable-container, and synthesised plan registries.
     * 7. ``TypeRegistry`` — last, because it owns the schemas everyone above
     *    borrows; its reset re-seeds the standard scalar/TS vocabulary.
     */
    HGRAPH_EXPORT void reset_all_registries() noexcept;
}  // namespace hgraph

#endif  // HGRAPH_TYPES_REGISTRY_RESET_H
