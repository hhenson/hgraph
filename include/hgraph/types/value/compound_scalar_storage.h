#ifndef HGRAPH_TYPES_VALUE_COMPOUND_SCALAR_STORAGE_H
#define HGRAPH_TYPES_VALUE_COMPOUND_SCALAR_STORAGE_H

#include <hgraph/hgraph_export.h>
#include <hgraph/types/storage_metrics.h>
#include <hgraph/types/value/value_type_ref.h>

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <type_traits>

namespace hgraph {
class CompoundScalarStorageBinding;

/** Cold-path, data-only summary of one root graph's polymorphic leaf pools. */
struct CompoundScalarStorageInspection {
  std::size_t leaf_pool_count{0};
  std::size_t live_slot_count{0};
  std::size_t slot_capacity{0};
};

struct CompoundScalarStorageOps {
  void (*destroy_impl)(void *context) noexcept;
  void *(*default_value_impl)(void *context, ValueTypeRef leaf);
  void *(*copy_value_impl)(void *context, ValueTypeRef leaf,
                           ValueTypeRef source, const void *source_memory);
  void *(*move_value_impl)(void *context, ValueTypeRef leaf,
                           ValueTypeRef source, void *source_memory);
  bool (*owns_impl)(const void *context, const void *payload) noexcept;
  DynamicStorageMetrics (*metrics_impl)(const void *context) noexcept;
  CompoundScalarStorageInspection (*inspect_impl)(const void *context) noexcept;
};

/**
 * Borrowed type-erased access to one root graph's compound-scalar storage.
 *
 * The concrete per-leaf stable-slot strategy is intentionally hidden.  A
 * view is two words and all ordinary calls dispatch through a non-null ops
 * table, including the canonical no-op view.
 */
class HGRAPH_EXPORT CompoundScalarStorageView {
public:
  CompoundScalarStorageView() noexcept;

  [[nodiscard]] bool available() const noexcept;
  [[nodiscard]] void *default_value(ValueTypeRef leaf) const;
  [[nodiscard]] void *copy_value(ValueTypeRef leaf, ValueTypeRef source,
                                 const void *source_memory) const;
  [[nodiscard]] void *move_value(ValueTypeRef leaf, ValueTypeRef source,
                                 void *source_memory) const;
  [[nodiscard]] bool owns(const void *payload) const noexcept;
  [[nodiscard]] DynamicStorageMetrics metrics() const noexcept;
  [[nodiscard]] CompoundScalarStorageInspection inspect() const noexcept;
  [[nodiscard]] bool
  same_storage(CompoundScalarStorageView other) const noexcept;

private:
  friend class CompoundScalarStorage;

  CompoundScalarStorageView(void *context,
                            const CompoundScalarStorageOps *ops) noexcept;

  void *context_{nullptr};
  const CompoundScalarStorageOps *ops_{nullptr};
};

/** Owning passive-ops facade for the concrete per-leaf pool registry. */
class HGRAPH_EXPORT CompoundScalarStorage {
public:
  CompoundScalarStorage() noexcept;
  ~CompoundScalarStorage();

  CompoundScalarStorage(const CompoundScalarStorage &) = delete;
  CompoundScalarStorage &operator=(const CompoundScalarStorage &) = delete;
  CompoundScalarStorage(CompoundScalarStorage &&other) noexcept;
  CompoundScalarStorage &operator=(CompoundScalarStorage &&other) noexcept;

  /**
   * Construct the runtime-selected graph-local storage strategy.  Its
   * concrete registry is allocated lazily on the first stored value.
   * Borrowed views are invalidated when this owner is moved or destroyed.
   */
  [[nodiscard]] static CompoundScalarStorage make_default();
  [[nodiscard]] CompoundScalarStorageView view() noexcept;
  [[nodiscard]] CompoundScalarStorageView view() const noexcept;

  /**
   * Bind this owner into ``binding`` for the rest of its life; the binding is
   * released when this owner is destroyed, and follows it when it is moved.
   * One object therefore owns both the pool and its binding, so an unwound
   * graph construction cannot leave a binding behind.
   */
  void bind(CompoundScalarStorageBinding &binding);

private:
  CompoundScalarStorage(void *context,
                        const CompoundScalarStorageOps *ops) noexcept;
  void reset() noexcept;

  void *context_{nullptr};
  const CompoundScalarStorageOps *ops_{nullptr};
  CompoundScalarStorageBinding *binding_{nullptr};
};

/**
 * Stable-address cell through which a realization's pooled value types reach
 * the pool of the root graph bound to them (RFC 0029).
 *
 * One cell belongs to a ``TypeRealizationSnapshot``; every pooled value type it
 * realizes holds a pointer to that cell.  A root graph's pool owner binds
 * itself for its own lifetime, so an allocating op reads a pointer rather than
 * asking which thread is asking.
 *
 * The binding is **exclusive**: a realization has at most one live root graph.
 * Binding a second is refused rather than silently overwriting the first.
 */
class HGRAPH_EXPORT CompoundScalarStorageBinding {
public:
  CompoundScalarStorageBinding() noexcept = default;
  CompoundScalarStorageBinding(const CompoundScalarStorageBinding &) = delete;
  CompoundScalarStorageBinding &
  operator=(const CompoundScalarStorageBinding &) = delete;

  [[nodiscard]] bool bound() const noexcept;
  /** The bound storage; throws when no root graph is bound. */
  [[nodiscard]] CompoundScalarStorageView storage() const;

  void bind(CompoundScalarStorageView storage);
  /** Re-point an existing binding after its owner moved. */
  void rebind(CompoundScalarStorageView storage) noexcept;
  void unbind() noexcept;

private:
  /**
   * Admission is claimed atomically: a realization cached by ``(generation,
   * options)`` is shared, so two root graphs can be constructed against it
   * concurrently and a check-then-set would let both through and tear the
   * two-word view.  The claim is taken once per root graph construction and
   * released once at its teardown — never on the per-tick path, which reads
   * ``storage_`` plainly and only from the graph that won the claim.
   */
  std::atomic<bool> claimed_{false};
  CompoundScalarStorageView storage_{};
};

/**
 * Operations on a one-word pooled holder.  ``payload`` is the stable
 * concrete leaf address published by the leaf pool.
 */
[[nodiscard]] HGRAPH_EXPORT ValueTypeRef
pooled_compound_scalar_leaf_type(const void *payload) noexcept;
[[nodiscard]] HGRAPH_EXPORT void *
retain_or_copy_pooled_compound_scalar(CompoundScalarStorageView target,
                                      const void *payload);
HGRAPH_EXPORT void release_pooled_compound_scalar(void *payload) noexcept;
[[nodiscard]] HGRAPH_EXPORT void *
writable_pooled_compound_scalar(void *payload);

static_assert(sizeof(CompoundScalarStorageView) == 2 * sizeof(void *));
static_assert(std::is_trivially_copyable_v<CompoundScalarStorageView>);
} // namespace hgraph

#endif // HGRAPH_TYPES_VALUE_COMPOUND_SCALAR_STORAGE_H
