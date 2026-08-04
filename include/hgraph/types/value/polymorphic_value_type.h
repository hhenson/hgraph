#ifndef HGRAPH_TYPES_VALUE_POLYMORPHIC_VALUE_TYPE_H
#define HGRAPH_TYPES_VALUE_POLYMORPHIC_VALUE_TYPE_H

#include <hgraph/hgraph_export.h>
#include <hgraph/types/value/value_type_ref.h>

#include <type_traits>

namespace hgraph {
/** Passive operations for an owned polymorphic value representation. */
struct PolymorphicValueTypeOps {
  void (*destroy_impl)(void *context) noexcept;
  ValueTypeRef (*binding_impl)(const void *context) noexcept;
};

/**
 * Type-erased owner of a polymorphic value representation.
 *
 * Concrete inline and indirect representations remain behind their
 * implementation boundaries.  Default and moved-from owners bind a
 * canonical no-op table, so ordinary queries never branch around a null
 * operations pointer.
 */
class HGRAPH_EXPORT PolymorphicValueType {
public:
  PolymorphicValueType() noexcept;
  PolymorphicValueType(void *context,
                       const PolymorphicValueTypeOps *ops) noexcept;
  ~PolymorphicValueType();

  PolymorphicValueType(const PolymorphicValueType &) = delete;
  PolymorphicValueType &operator=(const PolymorphicValueType &) = delete;
  PolymorphicValueType(PolymorphicValueType &&other) noexcept;
  PolymorphicValueType &operator=(PolymorphicValueType &&other) noexcept;

  [[nodiscard]] ValueTypeRef binding() const noexcept;

private:
  void reset() noexcept;

  void *context_{nullptr};
  const PolymorphicValueTypeOps *ops_{nullptr};
};

static_assert(sizeof(PolymorphicValueType) == 2 * sizeof(void *));
static_assert(std::is_standard_layout_v<PolymorphicValueType>);
} // namespace hgraph

#endif // HGRAPH_TYPES_VALUE_POLYMORPHIC_VALUE_TYPE_H
