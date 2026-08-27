// types/value/shared_value_pool.h -- process-wide storage for immutable
// Shared<T> values.  The public surface is deliberately observational; value
// construction and reference management remain behind the value ops table.
#ifndef HGRAPH_TYPES_VALUE_SHARED_VALUE_POOL_H
#define HGRAPH_TYPES_VALUE_SHARED_VALUE_POOL_H

#include <hgraph/hgraph_export.h>
#include <hgraph/types/value/value_type_ref.h>

#include <cstddef>
#include <cstdint>

namespace hgraph {
/** Aggregate, process-wide accounting for immutable Shared<T> storage. */
struct SharedValuePoolMetrics {
  std::size_t size_classes{0};
  std::size_t slabs{0};
  std::size_t capacity{0};
  std::size_t live_values{0};
  std::size_t reserved_bytes{0};
};

/** Cold-path inspection of the process-wide shared-value arena. */
[[nodiscard]] HGRAPH_EXPORT SharedValuePoolMetrics
shared_value_pool_metrics() noexcept;

namespace value_impl {
/** Opaque stable slot header. Shared<T> stores exactly one pointer to it. */
struct SharedValueAllocation;

/** Reserve an unconstructed slot for ``binding``. */
[[nodiscard]] HGRAPH_EXPORT SharedValueAllocation *
acquire_shared_value(ValueTypeRef binding);
/** Publish a successfully constructed slot with one strong reference. */
HGRAPH_EXPORT void
publish_shared_value(SharedValueAllocation *allocation) noexcept;
/** Return a reserved slot whose payload construction did not complete. */
HGRAPH_EXPORT void
abandon_shared_value(SharedValueAllocation *allocation) noexcept;
/** Add/release strong references. Payload destruction occurs on the final
 * release. */
HGRAPH_EXPORT void
retain_shared_value(SharedValueAllocation *allocation) noexcept;
HGRAPH_EXPORT void
release_shared_value(SharedValueAllocation *allocation) noexcept;

[[nodiscard]] HGRAPH_EXPORT ValueTypeRef
shared_value_type(const SharedValueAllocation &allocation);
[[nodiscard]] HGRAPH_EXPORT const void *
shared_value_memory(const SharedValueAllocation &allocation) noexcept;
[[nodiscard]] HGRAPH_EXPORT void *mutable_unpublished_shared_value_memory(
    SharedValueAllocation &allocation) noexcept;
[[nodiscard]] HGRAPH_EXPORT std::uint32_t
shared_value_use_count(const SharedValueAllocation &allocation) noexcept;

/** Test-only canonical-registry teardown hook; requires a quiescent arena. */
HGRAPH_EXPORT void reset_shared_value_pool() noexcept;
} // namespace value_impl
} // namespace hgraph

#endif // HGRAPH_TYPES_VALUE_SHARED_VALUE_POOL_H
