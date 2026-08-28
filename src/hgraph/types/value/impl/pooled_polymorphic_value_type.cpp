#include "pooled_polymorphic_value_type.h"

#include <hgraph/types/value/container_ops.h>
#include <hgraph/types/value/shared_value_pool.h>
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

using SharedValueAllocation = value_impl::SharedValueAllocation;

/** Pointer-sized closed union backed by the process-wide shared-value arena. */
struct PooledUnionEntry {
  const ValueTypeMetaData *declared{nullptr};
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
                   std::vector<ValueTypeRef> realized_alternatives
#if HGRAPH_ENABLE_PYTHON_USER_NODES
                   ,
                   detail::PolymorphicPythonSourceResolver source_resolver
#endif
                   )
      : declared(schema), alternatives(std::move(realized_alternatives))
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

  [[nodiscard]] static SharedValueAllocation *
  stored_allocation(const void *memory) noexcept {
    return memory != nullptr
               ? *static_cast<SharedValueAllocation *const *>(memory)
               : nullptr;
  }

  static void set_stored_allocation(
      void *memory, SharedValueAllocation *allocation) noexcept {
    *static_cast<SharedValueAllocation **>(memory) = allocation;
  }

  [[nodiscard]] static const void *
  payload(const SharedValueAllocation *allocation) noexcept {
    return allocation != nullptr
               ? value_impl::shared_value_memory(*allocation)
               : nullptr;
  }

  [[nodiscard]] ValueTypeRef active_type(const void *memory) const noexcept {
    const auto *allocation = stored_allocation(memory);
    if (allocation == nullptr)
      return {};
    const auto leaf = value_impl::shared_value_type(*allocation);
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

  template <typename Construct>
  [[nodiscard]] static SharedValueAllocation *
  construct_allocation(ValueTypeRef target, Construct &&construct) {
    auto *allocation = value_impl::acquire_shared_value(target);
    bool payload_constructed = false;
    auto cleanup = make_scope_exit([&]() noexcept {
      if (payload_constructed) {
        target.destroy_at(
            value_impl::mutable_unpublished_shared_value_memory(*allocation));
      }
      value_impl::abandon_shared_value(allocation);
    });
    void *memory =
        value_impl::mutable_unpublished_shared_value_memory(*allocation);
    target.default_construct_at(memory);
    payload_constructed = true;
    std::forward<Construct>(construct)(memory);
    value_impl::publish_shared_value(allocation);
    cleanup.release();
    return allocation;
  }

  [[nodiscard]] static SharedValueAllocation *
  make_default(ValueTypeRef target) {
    return construct_allocation(target, [](void *) {});
  }

  [[nodiscard]] static SharedValueAllocation *
  make_copy(ValueTypeRef target, ValueTypeRef source,
            const void *source_memory) {
    return construct_allocation(target, [&](void *memory) {
      target.ops_ref().copy_assign_from(target, memory, source, source_memory);
    });
  }

  [[nodiscard]] static SharedValueAllocation *
  make_move(ValueTypeRef target, ValueTypeRef source, void *source_memory) {
    return construct_allocation(target, [&](void *memory) {
      target.ops_ref().move_assign_from(target, memory, source, source_memory);
    });
  }

  [[nodiscard]] SharedValueAllocation *
  retain_or_copy(SharedValueAllocation *allocation) const {
    if (allocation == nullptr) {
      throw std::logic_error(
          "pooled closed Bundle source has an invalid active type");
    }
    if (value_impl::try_retain_shareable_shared_value(allocation)) {
      return allocation;
    }
    const auto type = value_impl::shared_value_type(*allocation);
    const auto target = alternative_for_schema(type.schema());
    if (!target) {
      throw std::invalid_argument(
          "pooled closed Bundle source is outside this graph snapshot");
    }
    return make_copy(target, type, payload(allocation));
  }

  static void replace_allocation(
      void *memory, SharedValueAllocation *replacement) noexcept {
    auto *previous = stored_allocation(memory);
    set_stored_allocation(memory, replacement);
    value_impl::release_shared_value(previous);
  }

  static void default_construct(void *memory, const void *context) {
    const auto &self = entry(context);
    set_stored_allocation(memory, nullptr);
    set_stored_allocation(memory, make_default(self.default_type));
  }

  static void destroy(void *memory, const void *) noexcept {
    auto *allocation = stored_allocation(memory);
    set_stored_allocation(memory, nullptr);
    value_impl::release_shared_value(allocation);
  }

  static void copy_construct(void *dst, const void *src, const void *context) {
    auto *allocation = stored_allocation(src);
    set_stored_allocation(dst, nullptr);
    set_stored_allocation(dst, entry(context).retain_or_copy(allocation));
  }

  static void move_construct(void *dst, void *src, const void *context) {
    // A lifecycle move must leave the source constructed. Sharing
    // is cheaper and safer than mutating a potentially shared source.
    copy_construct(dst, src, context);
  }

  static void copy_assign(void *dst, const void *src, const void *context) {
    replace_allocation(
        dst, entry(context).retain_or_copy(stored_allocation(src)));
  }

  static void move_assign(void *dst, void *src, const void *context) {
    if (dst != src)
      copy_assign(dst, src, context);
  }

  [[nodiscard]] std::pair<ValueTypeRef, const void *>
  copy_source(ValueTypeRef source, const void *src) const {
    if (source == binding)
      return {active_type(src), payload(stored_allocation(src))};
    if (source.schema() == declared) {
      const auto concrete = source.ops_ref().concrete_type(source, src);
      return {concrete, source.ops_ref().concrete_memory(src)};
    }
    return {source, src};
  }

  [[nodiscard]] std::pair<ValueTypeRef, void *> move_source(ValueTypeRef source,
                                                            void *src) const {
    if (source == binding)
      return {active_type(src),
              const_cast<void *>(payload(stored_allocation(src)))};
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
    replace_allocation(dst, make_copy(target, actual_type, actual_memory));
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
    replace_allocation(dst, make_move(target, actual_type, actual_memory));
  }

  static ValueTypeRef concrete_type(const void *context, ValueTypeRef,
                                    const void *memory) noexcept {
    return entry(context).active_type(memory);
  }

  static const void *concrete_memory(const void *,
                                     const void *memory) noexcept {
    return payload(stored_allocation(memory));
  }

  static void *mutable_concrete_memory(const void *, void *memory) noexcept {
    return const_cast<void *>(payload(stored_allocation(memory)));
  }

  static void *writable_concrete_memory(const void *context, void *memory) {
    const auto &self = entry(context);
    auto *allocation = stored_allocation(memory);
    if (value_impl::make_shared_value_unshareable(allocation)) {
      return value_impl::unshareable_shared_value_memory(*allocation);
    }
    const auto active = self.active_type(memory);
    if (!active || allocation == nullptr) {
      throw std::logic_error(
          "pooled closed Bundle has an invalid active type");
    }
    auto *replacement = make_copy(active, active, payload(allocation));
    if (!value_impl::make_shared_value_unshareable(replacement)) {
      value_impl::release_shared_value(replacement);
      throw std::logic_error(
          "new pooled closed Bundle allocation is not uniquely owned");
    }
    replace_allocation(memory, replacement);
    return value_impl::unshareable_shared_value_memory(*replacement);
  }

  [[nodiscard]] static DynamicStorageMetrics
  dynamic_storage_metrics(const void *, const void *) noexcept {
    // The process-wide shared-value arena reports storage once globally.
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
    return active.ops_ref().hash(payload(stored_allocation(memory)));
  }

  static bool equals(const void *context, const void *lhs,
                     const void *rhs) noexcept {
    return fallback_on_exception(false, [&]() {
      const auto &self = entry(context);
      const auto lhs_type = self.active_type(lhs);
      const auto rhs_type = self.active_type(rhs);
      return lhs_type && lhs_type == rhs_type &&
             lhs_type.ops_ref().equals(payload(stored_allocation(lhs)),
                                       payload(stored_allocation(rhs)));
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
      return lhs_type.ops_ref().compare(payload(stored_allocation(lhs)),
                                        payload(stored_allocation(rhs)));
    });
  }

  static std::string to_string(const void *context, const void *memory) {
    const auto &self = entry(context);
    const auto active = self.active_type(memory);
    if (!active)
      return "<invalid pooled closed Bundle>";
    return std::string{active.schema()->name()} +
           active.ops_ref().to_string(payload(stored_allocation(memory)));
  }

  static std::size_t indexed_size(const void *context,
                                  const void *memory) noexcept {
    const auto &self = entry(context);
    const auto *actual_ops = try_active_ops(self, memory);
    return actual_ops != nullptr && actual_ops->size != nullptr
               ? actual_ops->size(actual_ops->context,
                                  memory != nullptr
                                      ? payload(stored_allocation(memory))
                                                    : nullptr)
               : 0;
  }

  static const void *element_at(const void *context, const void *memory,
                                std::size_t index) {
    const auto &actual_ops = active_ops(entry(context), memory);
    return actual_ops.element_at(
        actual_ops.context, payload(stored_allocation(memory)), index);
  }

  static ValueTypeRef element_binding(const void *context, const void *memory,
                                      std::size_t index) noexcept {
    const auto &self = entry(context);
    const auto *actual_ops = try_active_ops(self, memory);
    return actual_ops != nullptr && actual_ops->element_binding != nullptr
               ? actual_ops->element_binding(
                     actual_ops->context,
                     memory != nullptr
                         ? payload(stored_allocation(memory))
                         : nullptr,
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
    return active.ops_ref().to_python(payload(stored_allocation(memory)));
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
    auto *replacement = construct_allocation(target, [&](void *payload) {
      target.ops_ref().from_python(target, payload, source);
    });
    replace_allocation(memory, replacement);
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
                                   std::vector<ValueTypeRef> alternatives
#if HGRAPH_ENABLE_PYTHON_USER_NODES
                                   ,
                                   PolymorphicPythonSourceResolver python_source
#endif
) {
  auto *entry = new PooledUnionEntry{schema, std::move(alternatives)
#if HGRAPH_ENABLE_PYTHON_USER_NODES
                                                 ,
                                     python_source
#endif
  };
  return PolymorphicValueType{entry, &pooled_union_ops()};
}
} // namespace detail
} // namespace hgraph
