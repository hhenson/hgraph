// types/registry_reset.h — the ONE canonical, ordered teardown of every
// process-wide registry/factory. The ordering is load-bearing: several
// registries intern by *pointer* into artifacts another registry owns, so a
// borrower must always be cleared before its lender frees the pointees (a
// missing/misordered clear historically caused stale-pointer reuse and memory
// corruption in aggregate test runs). Any NEW pointer-keyed registry or
// binding cache MUST be added to reset_all_registries() — never grow a second
// teardown sequence elsewhere. Reset is a test-only facility; production
// processes never reset.
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
     * 2. ``TypeRecordRegistry`` — records borrow plan and ops contexts from the
     *    time-series and value factories below. Cached record handles are
     *    trivially cleared later and are never dereferenced during reset.
     * 3. ``TSInputBuilderFactory`` / ``TSDataPlanFactory`` — clear the endpoint
     *    and TSData ops contexts after the records that pointed into them.
     * 4. ``ValuePlanFactory`` — plans borrow schema pointers (and the later
     *    clears release MemoryUtils synthesised composite/array plans).
     * 5. Compact, mutable-container, and synthesised plan registries.
     * 6. ``TypeRegistry`` — last, because it owns the schemas everyone above
     *    borrows; its reset re-seeds the standard scalar/TS vocabulary.
     */
    HGRAPH_EXPORT void reset_all_registries() noexcept;
}  // namespace hgraph

#endif  // HGRAPH_TYPES_REGISTRY_RESET_H
