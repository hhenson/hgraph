#include "pooled_polymorphic_value_type.h"

#include <hgraph/types/value/compound_scalar_storage.h>
#include <hgraph/types/value/container_ops.h>
#include <hgraph/types/value/value_range.h>
#include <hgraph/types/value/value_view.h>
#include <hgraph/util/scope.h>

#include <compare>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <utility>

namespace hgraph {
namespace {
void nop_destroy(void *) noexcept {}
ValueTypeRef nop_binding(const void *) noexcept { return {}; }

const PolymorphicValueTypeOps &nop_ops() noexcept {
  static const PolymorphicValueTypeOps ops{
      .destroy_impl = &nop_destroy,
      .binding_impl = &nop_binding,
  };
  return ops;
}

/** Pointer-sized graph-local closed union backed by root-graph pools. */
struct PooledUnionEntry {
  const ValueTypeMetaData *declared{nullptr};
  const CompoundScalarStorageBinding *pool_binding{nullptr};
  std::vector<ValueTypeRef> alternatives{};
  std::unordered_map<const TypeRecord *, ValueTypeRef> alternatives_by_record{};
  std::unordered_map<const ValueTypeMetaData *, ValueTypeRef>
      alternatives_by_schema{};
  ValueTypeRef default_type{};
#if HGRAPH_ENABLE_PYTHON_USER_NODES
  detail::PolymorphicPythonSourceResolver python_source{};
#endif
  MemoryUtils::StoragePlan plan{};
  IndexedValueOps ops{};
  ValueTypeRef binding{};

  PooledUnionEntry(const ValueTypeMetaData *schema,
                   const CompoundScalarStorageBinding *pool,
                   std::vector<ValueTypeRef> realized_alternatives
#if HGRAPH_ENABLE_PYTHON_USER_NODES
                   ,
                   detail::PolymorphicPythonSourceResolver source_resolver
#endif
                   )
      : declared(schema), pool_binding(pool),
        alternatives(std::move(realized_alternatives))
#if HGRAPH_ENABLE_PYTHON_USER_NODES
        ,
        python_source(source_resolver)
#endif
  {
    if (declared == nullptr) {
      throw std::invalid_argument(
          "pooled closed Bundle requires a declared schema");
    }
    if (alternatives.empty()) {
      throw std::logic_error(
          "abstract Bundle has no concrete alternative in this graph snapshot");
    }
    alternatives_by_record.reserve(alternatives.size());
    alternatives_by_schema.reserve(alternatives.size());
    for (const auto alternative : alternatives) {
      if (!alternative) {
        throw std::logic_error(
            "pooled closed Bundle alternative has no value binding");
      }
      alternatives_by_record.emplace(alternative.record(), alternative);
      alternatives_by_schema.emplace(alternative.schema(), alternative);
    }
    default_type = alternatives.front();
#if HGRAPH_ENABLE_PYTHON_USER_NODES
    if (python_source.context == nullptr || python_source.resolve == nullptr) {
      throw std::invalid_argument(
          "pooled closed Bundle requires a Python source resolver");
    }
#endif

    plan.layout = MemoryUtils::StorageLayout{
        .size = sizeof(void *),
        .alignment = alignof(void *),
    };
    plan.lifecycle = MemoryUtils::LifecycleOps{
        .construct = &default_construct,
        .destroy = &destroy,
        .copy_construct = &copy_construct,
        .move_construct = &move_construct,
        .copy_assign = &copy_assign,
        .move_assign = &move_assign,
    };
    plan.lifecycle_context = this;
    plan.trivially_destructible = false;
    plan.trivially_copyable = false;
    plan.trivially_move_constructible = false;

    ops.kind = ValueOpsKind::Indexed;
    ops.context = this;
    ops.allows_mutation = true;
    ops.hash_impl = &hash;
    ops.equals_impl = &equals;
    ops.compare_impl = &compare;
    ops.to_string_impl = &to_string;
#if HGRAPH_ENABLE_PYTHON_USER_NODES
    ops.to_python_impl = &to_python;
    ops.from_python_impl = &from_python;
#endif
    ops.accepts_source_impl = &accepts_source;
    ops.copy_assign_from_impl = &copy_assign_from;
    ops.move_assign_from_impl = &move_assign_from;
    ops.concrete_type_impl = &concrete_type;
    ops.concrete_memory_impl = &concrete_memory;
    ops.mutable_concrete_memory_impl = &mutable_concrete_memory;
    ops.dynamic_storage_metrics_impl = &dynamic_storage_metrics;
    ops.writable_concrete_memory_impl = &writable_concrete_memory;
    ops.size = &indexed_size;
    ops.element_at = &element_at;
    ops.element_binding = &element_binding;
    ops.make_range = &make_range;
    ops.make_mutable_range = &make_mutable_range;
    ops.mutable_element_at = &mutable_element_at;

    binding = intern_value_type(*declared, plan, ops);
  }

  [[nodiscard]] static const PooledUnionEntry &
  entry(const void *context) noexcept {
    return *static_cast<const PooledUnionEntry *>(context);
  }

  [[nodiscard]] static void *stored_payload(const void *memory) noexcept {
    return memory != nullptr ? *static_cast<void *const *>(memory) : nullptr;
  }

  static void set_stored_payload(void *memory, void *payload) noexcept {
    *static_cast<void **>(memory) = payload;
  }

  [[nodiscard]] ValueTypeRef active_type(const void *memory) const noexcept {
    const auto payload = stored_payload(memory);
    if (payload == nullptr)
      return {};
    const auto leaf = pooled_compound_scalar_leaf_type(payload);
    const auto found = alternatives_by_record.find(leaf.record());
    return found != alternatives_by_record.end() ? found->second
                                                 : ValueTypeRef{};
  }

  [[nodiscard]] ValueTypeRef
  alternative_for_schema(const ValueTypeMetaData *schema) const noexcept {
    const auto found = alternatives_by_schema.find(schema);
    return found != alternatives_by_schema.end() ? found->second
                                                 : ValueTypeRef{};
  }

  /** The pool of the root graph bound to this realization (RFC 0029). */
  [[nodiscard]] CompoundScalarStorageView storage() const {
    if (pool_binding == nullptr) {
      throw std::logic_error(
          "pooled closed Bundle was realized without a pool binding");
    }
    return pool_binding->storage();
  }

  [[nodiscard]] void *make_copy(ValueTypeRef target, ValueTypeRef source,
                                const void *source_memory) const {
    return storage().copy_value(target, source, source_memory);
  }

  [[nodiscard]] void *make_move(ValueTypeRef target, ValueTypeRef source,
                                void *source_memory) const {
    return storage().move_value(target, source, source_memory);
  }

  static void replace_payload(void *memory, void *replacement) noexcept {
    void *previous = stored_payload(memory);
    set_stored_payload(memory, replacement);
    release_pooled_compound_scalar(previous);
  }

  static void default_construct(void *memory, const void *context) {
    const auto &self = entry(context);
    set_stored_payload(memory, nullptr);
    set_stored_payload(memory,
                       self.storage().default_value(self.default_type));
  }

  static void destroy(void *memory, const void *) noexcept {
    void *payload = stored_payload(memory);
    set_stored_payload(memory, nullptr);
    release_pooled_compound_scalar(payload);
  }

  static void copy_construct(void *dst, const void *src, const void *context) {
    const auto payload = stored_payload(src);
    if (payload == nullptr) {
      throw std::logic_error(
          "pooled closed Bundle source has an invalid active type");
    }
    set_stored_payload(dst, nullptr);
    set_stored_payload(dst, retain_or_copy_pooled_compound_scalar(
                                entry(context).storage(), payload));
  }

  static void move_construct(void *dst, void *src, const void *context) {
    // A lifecycle move must leave the source constructed. Sharing
    // is cheaper and safer than mutating a potentially shared source.
    copy_construct(dst, src, context);
  }

  static void copy_assign(void *dst, const void *src, const void *context) {
    const auto payload = stored_payload(src);
    if (payload == nullptr) {
      throw std::logic_error(
          "pooled closed Bundle source has an invalid active type");
    }
    replace_payload(dst, retain_or_copy_pooled_compound_scalar(
                             entry(context).storage(), payload));
  }

  static void move_assign(void *dst, void *src, const void *context) {
    if (dst != src)
      copy_assign(dst, src, context);
  }

  [[nodiscard]] std::pair<ValueTypeRef, const void *>
  copy_source(ValueTypeRef source, const void *src) const {
    if (source == binding)
      return {active_type(src), stored_payload(src)};
    if (source.schema() == declared) {
      const auto concrete = source.ops_ref().concrete_type(source, src);
      return {concrete, source.ops_ref().concrete_memory(src)};
    }
    return {source, src};
  }

  [[nodiscard]] std::pair<ValueTypeRef, void *> move_source(ValueTypeRef source,
                                                            void *src) const {
    if (source == binding)
      return {active_type(src), stored_payload(src)};
    if (source.schema() == declared) {
      const auto concrete = source.ops_ref().concrete_type(source, src);
      return {concrete, source.ops_ref().writable_concrete_memory(src)};
    }
    return {source, src};
  }

  static bool accepts_source(const void *context, ValueTypeRef binding,
                             ValueTypeRef source) noexcept {
    const auto &self = entry(context);
    return binding == self.binding && source &&
           (source.schema() == self.declared ||
            self.alternative_for_schema(source.schema()));
  }

  static void copy_assign_from(const void *context, ValueTypeRef, void *dst,
                               ValueTypeRef source, const void *src) {
    const auto &self = entry(context);
    if (source == self.binding) {
      copy_assign(dst, src, context);
      return;
    }
    const auto [actual_type, actual_memory] = self.copy_source(source, src);
    const auto target = self.alternative_for_schema(actual_type.schema());
    if (!target || actual_memory == nullptr) {
      throw std::invalid_argument(
          "pooled closed Bundle source is outside this graph snapshot");
    }
    replace_payload(dst, self.make_copy(target, actual_type, actual_memory));
  }

  static void move_assign_from(const void *context, ValueTypeRef, void *dst,
                               ValueTypeRef source, void *src) {
    const auto &self = entry(context);
    if (source == self.binding) {
      move_assign(dst, src, context);
      return;
    }
    const auto [actual_type, actual_memory] = self.move_source(source, src);
    const auto target = self.alternative_for_schema(actual_type.schema());
    if (!target || actual_memory == nullptr) {
      throw std::invalid_argument(
          "pooled closed Bundle source is outside this graph snapshot");
    }
    replace_payload(dst, self.make_move(target, actual_type, actual_memory));
  }

  static ValueTypeRef concrete_type(const void *context, ValueTypeRef,
                                    const void *memory) noexcept {
    return entry(context).active_type(memory);
  }

  static const void *concrete_memory(const void *,
                                     const void *memory) noexcept {
    return stored_payload(memory);
  }

  static void *mutable_concrete_memory(const void *, void *memory) noexcept {
    return stored_payload(memory);
  }

  static void *writable_concrete_memory(const void *, void *memory) {
    void *payload = writable_pooled_compound_scalar(stored_payload(memory));
    set_stored_payload(memory, payload);
    return payload;
  }

  [[nodiscard]] static DynamicStorageMetrics
  dynamic_storage_metrics(const void *, const void *) noexcept {
    // The root graph reports the pool once, not once per holder.
    return {};
  }

  [[nodiscard]] static const IndexedValueOps &
  active_ops(const PooledUnionEntry &self, const void *memory) {
    const auto active = self.active_type(memory);
    if (!active) {
      throw std::logic_error("pooled closed Bundle has an invalid active type");
    }
    return *checked_value_ops<IndexedValueOps>(
        active, "pooled closed Bundle alternative");
  }

  [[nodiscard]] static const IndexedValueOps *
  try_active_ops(const PooledUnionEntry &self, const void *memory) noexcept {
    const auto active =
        memory != nullptr ? self.active_type(memory) : self.default_type;
    if (!active || active.ops() == nullptr ||
        active.ops()->kind != ValueOpsKind::Indexed) {
      return nullptr;
    }
    return static_cast<const IndexedValueOps *>(active.ops());
  }

  static std::size_t hash(const void *context, const void *memory) {
    const auto &self = entry(context);
    const auto active = self.active_type(memory);
    if (!active) {
      throw std::logic_error("pooled closed Bundle has an invalid active type");
    }
    return active.ops_ref().hash(stored_payload(memory));
  }

  static bool equals(const void *context, const void *lhs,
                     const void *rhs) noexcept {
    return fallback_on_exception(false, [&]() {
      const auto &self = entry(context);
      const auto lhs_type = self.active_type(lhs);
      const auto rhs_type = self.active_type(rhs);
      return lhs_type && lhs_type == rhs_type &&
             lhs_type.ops_ref().equals(stored_payload(lhs),
                                       stored_payload(rhs));
    });
  }

  static std::partial_ordering compare(const void *context, const void *lhs,
                                       const void *rhs) noexcept {
    return fallback_on_exception(std::partial_ordering::unordered, [&]() {
      const auto &self = entry(context);
      const auto lhs_type = self.active_type(lhs);
      const auto rhs_type = self.active_type(rhs);
      if (!lhs_type || lhs_type != rhs_type) {
        return std::partial_ordering::unordered;
      }
      return lhs_type.ops_ref().compare(stored_payload(lhs),
                                        stored_payload(rhs));
    });
  }

  static std::string to_string(const void *context, const void *memory) {
    const auto &self = entry(context);
    const auto active = self.active_type(memory);
    if (!active)
      return "<invalid pooled closed Bundle>";
    return std::string{active.schema()->name()} +
           active.ops_ref().to_string(stored_payload(memory));
  }

  static std::size_t indexed_size(const void *context,
                                  const void *memory) noexcept {
    const auto &self = entry(context);
    const auto *actual_ops = try_active_ops(self, memory);
    return actual_ops != nullptr && actual_ops->size != nullptr
               ? actual_ops->size(actual_ops->context,
                                  memory != nullptr ? stored_payload(memory)
                                                    : nullptr)
               : 0;
  }

  static const void *element_at(const void *context, const void *memory,
                                std::size_t index) {
    const auto &actual_ops = active_ops(entry(context), memory);
    return actual_ops.element_at(actual_ops.context, stored_payload(memory),
                                 index);
  }

  static ValueTypeRef element_binding(const void *context, const void *memory,
                                      std::size_t index) noexcept {
    const auto &self = entry(context);
    const auto *actual_ops = try_active_ops(self, memory);
    return actual_ops != nullptr && actual_ops->element_binding != nullptr
               ? actual_ops->element_binding(
                     actual_ops->context,
                     memory != nullptr ? stored_payload(memory) : nullptr,
                     index)
               : ValueTypeRef{};
  }

  static void *mutable_element_at(const void *context, void *memory,
                                  std::size_t index) {
    void *payload = writable_concrete_memory(context, memory);
    const auto &actual_ops = active_ops(entry(context), memory);
    if (actual_ops.mutable_element_at == nullptr) {
      return const_cast<void *>(
          actual_ops.element_at(actual_ops.context, payload, index));
    }
    return actual_ops.mutable_element_at(actual_ops.context, payload, index);
  }

  static ValueView range_project(const void *context, const void *memory,
                                 std::size_t index) {
    return ValueView{element_binding(context, memory, index),
                     element_at(context, memory, index)};
  }

  static ValueView mutable_range_project(const void *context,
                                         const void *memory,
                                         std::size_t index) {
    return ValueView{
        element_binding(context, memory, index),
        mutable_element_at(context, const_cast<void *>(memory), index)}
        .begin_mutation();
  }

  static Range<ValueView> make_range(const void *context, const void *memory) {
    return Range<ValueView>{
        .context = context,
        .memory = memory,
        .limit = indexed_size(context, memory),
        .predicate = nullptr,
        .projector = &range_project,
    };
  }

  static Range<ValueView> make_mutable_range(const void *context,
                                             void *memory) {
    return Range<ValueView>{
        .context = context,
        .memory = memory,
        .limit = indexed_size(context, memory),
        .predicate = nullptr,
        .projector = &mutable_range_project,
    };
  }

#if HGRAPH_ENABLE_PYTHON_USER_NODES
  static nb::object to_python(const void *context, const void *memory) {
    const auto &self = entry(context);
    const auto active = self.active_type(memory);
    if (!active) {
      throw std::logic_error("pooled closed Bundle has an invalid active type");
    }
    return active.ops_ref().to_python(stored_payload(memory));
  }

  static void from_python(const void *context, const ValueTypeRef &,
                          void *memory, nb::handle source) {
    const auto &self = entry(context);
    const auto external_type =
        self.python_source.resolve(self.python_source.context, source);
    const auto target = self.alternative_for_schema(external_type.schema());
    if (!target) {
      throw std::invalid_argument(
          "Python value is outside this graph's pooled Bundle snapshot");
    }
    void *replacement = self.storage().default_value(target);
    auto release = make_scope_exit(
        [&]() noexcept { release_pooled_compound_scalar(replacement); });
    target.ops_ref().from_python(target, replacement, source);
    replace_payload(memory, replacement);
    release.release();
  }
#endif
};

void destroy_pooled_union(void *context) noexcept {
  delete static_cast<PooledUnionEntry *>(context);
}

ValueTypeRef pooled_union_binding(const void *context) noexcept {
  return static_cast<const PooledUnionEntry *>(context)->binding;
}

const PolymorphicValueTypeOps &pooled_union_ops() noexcept {
  static const PolymorphicValueTypeOps ops{
      .destroy_impl = &destroy_pooled_union,
      .binding_impl = &pooled_union_binding,
  };
  return ops;
}
} // namespace

PolymorphicValueType::PolymorphicValueType() noexcept : ops_(&nop_ops()) {}

PolymorphicValueType::PolymorphicValueType(
    void *context, const PolymorphicValueTypeOps *ops) noexcept
    : context_(context), ops_(ops != nullptr ? ops : &nop_ops()) {}

PolymorphicValueType::~PolymorphicValueType() { reset(); }

PolymorphicValueType::PolymorphicValueType(
    PolymorphicValueType &&other) noexcept
    : context_(std::exchange(other.context_, nullptr)),
      ops_(std::exchange(other.ops_, &nop_ops())) {}

PolymorphicValueType &
PolymorphicValueType::operator=(PolymorphicValueType &&other) noexcept {
  if (this != &other) {
    reset();
    context_ = std::exchange(other.context_, nullptr);
    ops_ = std::exchange(other.ops_, &nop_ops());
  }
  return *this;
}

ValueTypeRef PolymorphicValueType::binding() const noexcept {
  return ops_->binding_impl(context_);
}

void PolymorphicValueType::reset() noexcept {
  ops_->destroy_impl(context_);
  context_ = nullptr;
  ops_ = &nop_ops();
}

namespace detail {
PolymorphicValueType
make_pooled_polymorphic_value_type(const ValueTypeMetaData *schema,
                                   const CompoundScalarStorageBinding *pool_binding,
                                   std::vector<ValueTypeRef> alternatives
#if HGRAPH_ENABLE_PYTHON_USER_NODES
                                   ,
                                   PolymorphicPythonSourceResolver python_source
#endif
) {
  auto *entry = new PooledUnionEntry{schema, pool_binding,
                                     std::move(alternatives)
#if HGRAPH_ENABLE_PYTHON_USER_NODES
                                                 ,
                                     python_source
#endif
  };
  return PolymorphicValueType{entry, &pooled_union_ops()};
}
} // namespace detail
} // namespace hgraph
