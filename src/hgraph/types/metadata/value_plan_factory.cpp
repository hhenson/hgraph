#include <hgraph/types/metadata/value_plan_factory.h>

#include <hgraph/types/metadata/type_realization.h>

#if HGRAPH_ENABLE_PYTHON_USER_NODES
#include <hgraph/python/bridge_state.h>
#include <hgraph/types/primitive_types.h>
#include <hgraph/types/static_schema.h>
#endif

#include <hgraph/types/utils/intern_table.h>
#include <hgraph/types/value/any_ops.h>
#include <hgraph/types/value/compact_container_ops.h>
#include <hgraph/types/value/mutable_container_ops.h>
#include <hgraph/types/value/value.h>
#include <hgraph/util/scope.h>

#include <algorithm>
#include <compare>
#include <cstddef>
#include <cstdint>
#include <fmt/format.h>
#include <iterator>
#include <memory>
#include <mutex>
#include <new>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

namespace hgraph {
namespace {
struct CompositeIndexedContext {
  const ValueTypeMetaData *schema{nullptr};
  std::vector<ValueTypeRef> child_bindings{};
  std::vector<std::size_t> offsets{};
  std::size_t validity_offset{0};
  std::size_t validity_word_count{0};
};

using BundleValidityWord = std::uint64_t;
static constexpr std::size_t bundle_validity_bits_per_word =
    sizeof(BundleValidityWord) * 8U;

[[nodiscard]] constexpr std::size_t
bundle_validity_word_count(std::size_t field_count) noexcept {
  return (field_count + bundle_validity_bits_per_word - 1U) /
         bundle_validity_bits_per_word;
}

[[nodiscard]] const MemoryUtils::StoragePlan &
bundle_validity_plan(std::size_t field_count) {
  return MemoryUtils::array_plan<BundleValidityWord>(
      bundle_validity_word_count(field_count));
}

// ---- Bundle field validity (core_concepts.rst) ----
[[nodiscard]] inline const BundleValidityWord *
composite_validity(const CompositeIndexedContext *state,
                   const void *memory) noexcept {
  if (state->validity_word_count == 0) {
    return nullptr;
  } // dense Tuple, or empty Bundle
  return static_cast<const BundleValidityWord *>(
      MemoryUtils::advance(memory, state->validity_offset));
}

[[nodiscard]] inline BundleValidityWord *
mutable_composite_validity(const CompositeIndexedContext *state,
                           void *memory) noexcept {
  if (state->validity_word_count == 0) {
    return nullptr;
  }
  return static_cast<BundleValidityWord *>(
      MemoryUtils::advance(memory, state->validity_offset));
}

[[nodiscard]] inline bool
composite_field_set(const CompositeIndexedContext *state, const void *memory,
                    std::size_t index) noexcept {
  const auto *words = composite_validity(state, memory);
  if (words == nullptr) {
    return true;
  }
  if (index >= state->schema->field_count) {
    return false;
  }
  const auto word = index / bundle_validity_bits_per_word;
  const auto bit = index % bundle_validity_bits_per_word;
  return (words[word] & (BundleValidityWord{1} << bit)) != 0;
}

inline void composite_mark_field(const CompositeIndexedContext *state,
                                 void *memory, std::size_t index, bool set) {
  auto *words = mutable_composite_validity(state, memory);
  if (words == nullptr) {
    return;
  }
  if (index >= state->schema->field_count) {
    throw std::out_of_range("Bundle field index out of range");
  }
  const auto word = index / bundle_validity_bits_per_word;
  const auto bit = index % bundle_validity_bits_per_word;
  const auto mask = BundleValidityWord{1} << bit;
  if (set) {
    words[word] |= mask;
  } else {
    words[word] &= ~mask;
  }
}

[[maybe_unused]] inline void
composite_mark_all(const CompositeIndexedContext *state, void *memory,
                   bool set) {
  auto *words = mutable_composite_validity(state, memory);
  if (words == nullptr) {
    return;
  }
  const BundleValidityWord fill =
      set ? ~BundleValidityWord{0} : BundleValidityWord{0};
  for (std::size_t index = 0; index < state->validity_word_count; ++index) {
    words[index] = fill;
  }
  if (set && state->schema->field_count % bundle_validity_bits_per_word != 0) {
    const auto used_bits =
        state->schema->field_count % bundle_validity_bits_per_word;
    words[state->validity_word_count - 1U] =
        (BundleValidityWord{1} << used_bits) - 1U;
  }
}

struct ArrayIndexedContext {
  const ValueTypeMetaData *schema{nullptr};
  ValueTypeRef element_binding{nullptr};
  std::size_t capacity{0};
  std::size_t stride{0};
  std::size_t size_offset{0};
  std::size_t data_offset{0};
  bool bounded{false};
};

struct OwnedAllocation {
  const TypeRecord *record{nullptr};
};

struct OwnedValueEntry;

[[nodiscard]] constexpr std::size_t align_up(std::size_t value,
                                             std::size_t alignment) noexcept {
  return (value + alignment - 1U) & ~(alignment - 1U);
}

[[nodiscard]] const OwnedAllocation *
owned_allocation(const void *memory) noexcept {
  return memory != nullptr ? *static_cast<OwnedAllocation *const *>(memory)
                           : nullptr;
}

[[nodiscard]] OwnedAllocation *owned_allocation(void *memory) noexcept {
  return memory != nullptr ? *static_cast<OwnedAllocation **>(memory) : nullptr;
}

void set_owned_allocation(void *memory, OwnedAllocation *allocation) noexcept {
  *static_cast<OwnedAllocation **>(memory) = allocation;
}

[[nodiscard]] std::size_t
owned_payload_offset(const MemoryUtils::StoragePlan &plan) noexcept {
  return align_up(sizeof(OwnedAllocation), plan.layout.alignment);
}

[[nodiscard]] MemoryUtils::StorageLayout
owned_allocation_layout(const MemoryUtils::StoragePlan &plan) noexcept {
  const std::size_t alignment =
      std::max(alignof(OwnedAllocation), plan.layout.alignment);
  return MemoryUtils::StorageLayout{
      .size =
          align_up(owned_payload_offset(plan) + plan.layout.size, alignment),
      .alignment = alignment,
  };
}

[[nodiscard]] const void *
owned_payload(const OwnedAllocation &allocation) noexcept {
  return reinterpret_cast<const std::byte *>(&allocation) +
         owned_payload_offset(*allocation.record->plan);
}

[[nodiscard]] void *owned_payload(OwnedAllocation &allocation) noexcept {
  return reinterpret_cast<std::byte *>(&allocation) +
         owned_payload_offset(*allocation.record->plan);
}

[[nodiscard]] ValueTypeRef
owned_allocation_type(const OwnedAllocation &allocation) {
  return ValueTypeRef::checked(AnyPtr::typed_null(*allocation.record));
}

[[nodiscard]] ValueTypeRef
owned_target_type(const ValueTypeMetaData &owned_schema) {
  if (const auto *snapshot = active_type_realization(); snapshot != nullptr) {
    return snapshot->type_for(owned_schema.element_type);
  }
  return ValuePlanFactory::instance().type_for(owned_schema.element_type);
}

[[nodiscard]] OwnedAllocation *allocate_owned(ValueTypeRef binding) {
  const auto &plan = binding.checked_plan();
  const auto layout = owned_allocation_layout(plan);
  void *raw = MemoryUtils::allocator().allocate_storage(layout);
  auto release = make_scope_exit([&]() noexcept {
    MemoryUtils::allocator().deallocate_storage(raw, layout);
  });
  auto *allocation = ::new (raw) OwnedAllocation{binding.record()};
  binding.default_construct_at(owned_payload(*allocation));
  release.release();
  return allocation;
}

[[nodiscard]] OwnedAllocation *copy_owned(const OwnedAllocation &source) {
  const auto binding = owned_allocation_type(source);
  const auto layout = owned_allocation_layout(binding.checked_plan());
  void *raw = MemoryUtils::allocator().allocate_storage(layout);
  auto release = make_scope_exit([&]() noexcept {
    MemoryUtils::allocator().deallocate_storage(raw, layout);
  });
  auto *allocation = ::new (raw) OwnedAllocation{binding.record()};
  binding.copy_construct_at(owned_payload(*allocation), owned_payload(source));
  release.release();
  return allocation;
}

void destroy_owned_allocation(OwnedAllocation *allocation) noexcept {
  if (allocation == nullptr) {
    return;
  }
  const auto *plan = allocation->record->plan;
  plan->destroy(owned_payload(*allocation));
  MemoryUtils::allocator().deallocate_storage(allocation,
                                              owned_allocation_layout(*plan));
}

void owned_default_construct(void *memory, const void *) {
  set_owned_allocation(memory, nullptr);
}

void owned_destroy(void *memory, const void *) noexcept {
  auto *allocation = owned_allocation(memory);
  set_owned_allocation(memory, nullptr);
  destroy_owned_allocation(allocation);
}

void owned_copy_construct(void *dst, const void *src, const void *) {
  const auto *source = owned_allocation(src);
  set_owned_allocation(dst, source != nullptr ? copy_owned(*source) : nullptr);
}

void owned_move_construct(void *dst, void *src, const void *) {
  set_owned_allocation(dst, owned_allocation(src));
  set_owned_allocation(src, nullptr);
}

void owned_copy_assign(void *dst, const void *src, const void *) {
  const auto *source = owned_allocation(src);
  OwnedAllocation *replacement =
      source != nullptr ? copy_owned(*source) : nullptr;
  auto *previous = owned_allocation(dst);
  set_owned_allocation(dst, replacement);
  destroy_owned_allocation(previous);
}

void owned_move_assign(void *dst, void *src, const void *) {
  if (dst == src) {
    return;
  }
  auto *previous = owned_allocation(dst);
  set_owned_allocation(dst, owned_allocation(src));
  set_owned_allocation(src, nullptr);
  destroy_owned_allocation(previous);
}

[[nodiscard]] std::size_t combine_hash(std::size_t seed,
                                       std::size_t value) noexcept {
  seed ^= value + 0x9e3779b97f4a7c15ULL + (seed << 6U) + (seed >> 2U);
  return seed;
}

[[nodiscard]] std::size_t composite_indexed_size(const void *context,
                                                 const void *) noexcept {
  return static_cast<const CompositeIndexedContext *>(context)
      ->child_bindings.size();
}

[[nodiscard]] const void *composite_indexed_element_at(const void *context,
                                                       const void *memory,
                                                       std::size_t index) {
  const auto *state = static_cast<const CompositeIndexedContext *>(context);
  if (!composite_field_set(state, memory, index)) {
    return nullptr;
  } // UNSET field
  return static_cast<const std::byte *>(memory) + state->offsets[index];
}

[[nodiscard]] ValueTypeRef
composite_indexed_element_binding(const void *context, const void *,
                                  std::size_t index) noexcept {
  return static_cast<const CompositeIndexedContext *>(context)
      ->child_bindings[index];
}

[[nodiscard]] ValueView composite_indexed_range_projector(const void *context,
                                                          const void *memory,
                                                          std::size_t index) {
  return ValueView{composite_indexed_element_binding(context, memory, index),
                   composite_indexed_element_at(context, memory, index)};
}

[[nodiscard]] void *composite_indexed_mutable_element_at(const void *context,
                                                         void *memory,
                                                         std::size_t index) {
  const auto *state = static_cast<const CompositeIndexedContext *>(context);
  // Mutable access marks the field LIVE (a writer is committing a
  // value) and always returns the real storage.
  composite_mark_field(state, memory, index, true);
  return static_cast<std::byte *>(memory) + state->offsets[index];
}

[[nodiscard]] ValueView composite_indexed_mutable_range_projector(
    const void *context, const void *memory, std::size_t index) {
  return ValueView{composite_indexed_element_binding(context, memory, index),
                   composite_indexed_mutable_element_at(
                       context, const_cast<void *>(memory), index)}
      .begin_mutation();
}

[[nodiscard]] Range<ValueView>
composite_indexed_make_range(const void *context, const void *memory) {
  return Range<ValueView>{
      .context = context,
      .memory = memory,
      .limit = composite_indexed_size(context, memory),
      .predicate = nullptr,
      .projector = &composite_indexed_range_projector,
  };
}

[[nodiscard]] Range<ValueView>
composite_indexed_make_mutable_range(const void *context, void *memory) {
  return Range<ValueView>{
      .context = context,
      .memory = memory,
      .limit = composite_indexed_size(context, memory),
      .predicate = nullptr,
      .projector = &composite_indexed_mutable_range_projector,
  };
}

[[nodiscard]] bool composite_accepts_source(const void *context, ValueTypeRef,
                                            ValueTypeRef source) noexcept {
  const auto *state = static_cast<const CompositeIndexedContext *>(context);
  return state != nullptr && source && source.schema() == state->schema;
}

[[nodiscard]] DynamicStorageMetrics
composite_dynamic_storage_metrics(const void *context,
                                  const void *memory) noexcept {
  const auto *state = static_cast<const CompositeIndexedContext *>(context);
  DynamicStorageMetrics result{};
  for (std::size_t index = 0; index < state->child_bindings.size(); ++index) {
    const auto &binding = state->child_bindings[index];
    const auto *child =
        static_cast<const std::byte *>(memory) + state->offsets[index];
    const auto metrics = binding.ops_ref().dynamic_storage_metrics(child);
    result += metrics;
  }
  return result;
}

template <bool Move>
void composite_assign_from(const void *context, void *dst, ValueTypeRef source,
                           const void *src) {
  const auto *state = static_cast<const CompositeIndexedContext *>(context);
  const auto actual_type = source.ops_ref().concrete_type(source, src);
  const auto *actual_memory = source.ops_ref().concrete_memory(src);
  if (!actual_type || actual_type.schema() != state->schema ||
      actual_memory == nullptr) {
    throw std::invalid_argument("composite assignment requires the same "
                                "concrete Tuple or Bundle schema");
  }
  const auto *source_ops = checked_value_ops<IndexedValueOps>(
      actual_type, "composite assignment source");
  if (source_ops->element_at == nullptr ||
      source_ops->element_binding == nullptr || source_ops->size == nullptr ||
      source_ops->size(source_ops->context, actual_memory) !=
          state->child_bindings.size()) {
    throw std::invalid_argument(
        "composite assignment source has an incompatible field shape");
  }

  for (std::size_t index = 0; index < state->child_bindings.size(); ++index) {
    const auto *source_child =
        source_ops->element_at(source_ops->context, actual_memory, index);
    if (source_child == nullptr) {
      composite_mark_field(state, dst, index, false);
      continue;
    }
    const auto source_binding =
        source_ops->element_binding(source_ops->context, actual_memory, index);
    const auto target_binding = state->child_bindings[index];
    auto *target_child = static_cast<std::byte *>(dst) + state->offsets[index];
    if constexpr (Move) {
      target_binding.ops_ref().move_assign_from(
          target_binding, target_child, source_binding,
          const_cast<void *>(source_child));
    } else {
      target_binding.ops_ref().copy_assign_from(target_binding, target_child,
                                                source_binding, source_child);
    }
    composite_mark_field(state, dst, index, true);
  }
}

void composite_copy_assign_from(const void *context, ValueTypeRef, void *dst,
                                ValueTypeRef source, const void *src) {
  composite_assign_from<false>(context, dst, source, src);
}

void composite_move_assign_from(const void *context, ValueTypeRef, void *dst,
                                ValueTypeRef source, void *src) {
  composite_assign_from<true>(context, dst, source, src);
}

[[nodiscard]] std::size_t composite_value_hash(const void *context,
                                               const void *memory) {
  const auto *state = static_cast<const CompositeIndexedContext *>(context);
  std::size_t seed = 0;
  for (std::size_t index = 0; index < state->child_bindings.size(); ++index) {
    if (!composite_field_set(state, memory, index)) {
      seed = combine_hash(seed, 0x9e3779b97f4a7c15ULL); // UNSET marker
      continue;
    }
    const auto &ops = state->child_bindings[index].ops_ref();
    const auto *child =
        static_cast<const std::byte *>(memory) + state->offsets[index];
    seed = combine_hash(seed, ops.hash(child));
  }
  return seed;
}

[[nodiscard]] bool composite_value_equals(const void *context, const void *lhs,
                                          const void *rhs) noexcept {
  if (lhs == nullptr || rhs == nullptr) {
    return lhs == rhs;
  }
  return fallback_on_exception(false, [&] {
    const auto *state = static_cast<const CompositeIndexedContext *>(context);
    for (std::size_t index = 0; index < state->child_bindings.size(); ++index) {
      const bool lhs_set = composite_field_set(state, lhs, index);
      const bool rhs_set = composite_field_set(state, rhs, index);
      if (lhs_set != rhs_set) {
        return false;
      }
      if (!lhs_set) {
        continue;
      } // UNSET == UNSET
      const auto &ops = state->child_bindings[index].ops_ref();
      const auto *a =
          static_cast<const std::byte *>(lhs) + state->offsets[index];
      const auto *b =
          static_cast<const std::byte *>(rhs) + state->offsets[index];
      if (!ops.equals(a, b)) {
        return false;
      }
    }
    return true;
  });
}

[[nodiscard]] std::partial_ordering
composite_value_compare(const void *context, const void *lhs,
                        const void *rhs) noexcept {
  if (const auto order = value_ops_detail::null_order(lhs, rhs)) {
    return *order;
  }

  return fallback_on_exception(std::partial_ordering::unordered, [&]() {
    const auto *state = static_cast<const CompositeIndexedContext *>(context);
    for (std::size_t index = 0; index < state->child_bindings.size(); ++index) {
      const bool lhs_set = composite_field_set(state, lhs, index);
      const bool rhs_set = composite_field_set(state, rhs, index);
      if (lhs_set != rhs_set) {
        return lhs_set ? std::partial_ordering::greater
                       : std::partial_ordering::less;
      }
      if (!lhs_set) {
        continue;
      }
      const auto &ops = state->child_bindings[index].ops_ref();
      const auto *a =
          static_cast<const std::byte *>(lhs) + state->offsets[index];
      const auto *b =
          static_cast<const std::byte *>(rhs) + state->offsets[index];
      const auto c = ops.compare(a, b);
      if (c != 0) {
        return c;
      }
    }
    return std::partial_ordering::equivalent;
  });
}

[[nodiscard]] std::string composite_value_to_string(const void *context,
                                                    const void *memory) {
  if (memory == nullptr) {
    return {};
  }
  const auto *state = static_cast<const CompositeIndexedContext *>(context);
  const bool bundle = state->schema != nullptr &&
                      state->schema->value_kind() == ValueTypeKind::Bundle;
  fmt::memory_buffer out;
  fmt::format_to(std::back_inserter(out), "{}", bundle ? '{' : '(');
  for (std::size_t index = 0; index < state->child_bindings.size(); ++index) {
    if (index > 0) {
      fmt::format_to(std::back_inserter(out), ", ");
    }
    if (bundle) {
      const char *name = state->schema->fields[index].name;
      fmt::format_to(std::back_inserter(out),
                     "{}: ", name != nullptr ? name : "");
    }
    if (!composite_field_set(state, memory, index)) {
      fmt::format_to(std::back_inserter(out), "<unset>");
      continue;
    }
    const auto &ops = state->child_bindings[index].ops_ref();
    const auto *child =
        static_cast<const std::byte *>(memory) + state->offsets[index];
    fmt::format_to(std::back_inserter(out), "{}", ops.to_string(child));
  }
  fmt::format_to(std::back_inserter(out), "{}", bundle ? '}' : ')');
  return fmt::to_string(out);
}

#if HGRAPH_ENABLE_PYTHON_USER_NODES
void require_python_source(nb::handle source, const char *what) {
  if (source.is_none()) {
    throw std::invalid_argument(std::string{what} +
                                " requires a non-None value");
  }
}

[[nodiscard]] bool is_python_sequence(nb::handle source) {
  return PySequence_Check(source.ptr()) != 0;
}

void assign_child_from_python(const ValueTypeRef &binding, void *memory,
                              nb::handle source, const char *what) {
  if (memory == nullptr) {
    throw std::runtime_error(std::string{what} + " child memory is not live");
  }
  if (source.is_none()) {
    throw std::invalid_argument(std::string{what} +
                                " does not allow None elements");
  }
  binding.ops_ref().from_python(binding, memory, source);
}

[[nodiscard]] const python_bridge::PyBundleClassInfo *
python_bundle_info(const ValueTypeMetaData *schema) {
  const auto &registry = python_bridge::bundle_class_info_registry();
  const auto found = registry.find(schema);
  return found != registry.end() ? &found->second : nullptr;
}

[[nodiscard]] nb::object composite_value_to_python(const void *context,
                                                   const void *memory) {
  if (memory == nullptr) {
    throw std::runtime_error("composite to_python requires live value memory");
  }
  const auto *state = static_cast<const CompositeIndexedContext *>(context);
  const bool bundle = state->schema != nullptr &&
                      state->schema->value_kind() == ValueTypeKind::Bundle;

  if (bundle) {
    // A NAMED bundle with a registered python class rebuilds the
    // class (CompoundScalar read-back; UNSET fields -> None).
    const auto *bundle_info = !state->schema->name().empty()
                                  ? python_bundle_info(state->schema)
                                  : nullptr;
    if (bundle_info == nullptr) {
      nb::dict result;
      for (std::size_t index = 0; index < state->child_bindings.size();
           ++index) {
        const char *name = state->schema->fields[index].name;
        if (name == nullptr || *name == '\0') {
          continue;
        }
        if (!composite_field_set(state, memory, index)) {
          continue;
        }
        const auto &ops = state->child_bindings[index].ops_ref();
        const auto *child =
            static_cast<const std::byte *>(memory) + state->offsets[index];
        result[nb::str{name}] = ops.to_python(child);
      }
      return result;
    }

    nb::dict constructor_arguments;
    nb::object result;
    if (!bundle_info->requires_constructor) {
      // Ordinary dataclasses can be rebuilt without re-running
      // __init__/__post_init__ on every TS.value read.
      result = nb::steal(bundle_info->allocator(
          reinterpret_cast<PyTypeObject *>(bundle_info->type.ptr()), 0));
      if (!result.is_valid()) {
        nb::raise_python_error();
      }
    }
    for (std::size_t index = 0; index < state->child_bindings.size(); ++index) {
      const char *name = state->schema->fields[index].name;
      if (name == nullptr || *name == '\0') {
        continue;
      }
      nb::handle key = bundle_info->field_names[index];
      const bool set = composite_field_set(state, memory, index);
      nb::object value = set ? state->child_bindings[index].ops_ref().to_python(
                                   static_cast<const std::byte *>(memory) +
                                   state->offsets[index])
                             : nb::none();
      if (bundle_info->requires_constructor) {
        if (bundle_info->constructor_fields[index]) {
          constructor_arguments[key] = value;
        }
      } else if (bundle_info->field_overrides[index].is_valid()) {
        bundle_info->field_overrides[index](result, value);
      } else if (PyObject_GenericSetAttr(result.ptr(), key.ptr(),
                                         value.ptr()) != 0) {
        nb::raise_python_error();
      }
    }
    if (bundle_info->requires_constructor) {
      nb::tuple positional = nb::steal<nb::tuple>(PyTuple_New(0));
      result =
          nb::steal(PyObject_Call(bundle_info->type.ptr(), positional.ptr(),
                                  constructor_arguments.ptr()));
      if (!result.is_valid()) {
        nb::raise_python_error();
      }
    }
    return result;
  }

  nb::list result;
  for (std::size_t index = 0; index < state->child_bindings.size(); ++index) {
    // UNSET tuple fields read back as None (field validity - the
    // relaxed combine/partial convert convention).
    if (!composite_field_set(state, memory, index)) {
      result.append(nb::none());
      continue;
    }
    const auto &ops = state->child_bindings[index].ops_ref();
    const auto *child =
        static_cast<const std::byte *>(memory) + state->offsets[index];
    result.append(ops.to_python(child));
  }
  return nb::tuple(result);
}

void fill_composite_from_sequence(const CompositeIndexedContext *state,
                                  void *memory, nb::handle source,
                                  const char *what) {
  if (!is_python_sequence(source)) {
    throw std::invalid_argument(std::string{what} +
                                " expects a Python list or tuple");
  }

  nb::object object = nb::borrow<nb::object>(source);
  nb::sequence sequence = nb::cast<nb::sequence>(object);
  const auto count = static_cast<std::size_t>(nb::len(sequence));
  // hgraph parity: a LONGER python sequence fills a fixed tuple's
  // leading fields (python tuples don't length-validate upstream).
  if (count > state->child_bindings.size()) { /* truncate below */
  } else if (count != state->child_bindings.size()) {
    throw std::invalid_argument(fmt::format("{} expects {} elements, got {}",
                                            what, state->child_bindings.size(),
                                            count));
  }

  composite_mark_all(state, memory, true); // sequences supply every field
  const std::size_t fill_count = std::min(count, state->child_bindings.size());
  for (std::size_t index = 0; index < fill_count; ++index) {
    nb::object element = sequence[index];
    // None = UNSET (field validity) - the TABLE row convention:
    // to_python reads holes back as None, so None round-trips.
    if (element.is_none()) {
      composite_mark_field(state, memory, index, false);
      continue;
    }
    auto *child = static_cast<std::byte *>(memory) + state->offsets[index];
    assign_child_from_python(state->child_bindings[index], child, element,
                             what);
  }
}

void composite_value_from_python(const void *context, const ValueTypeRef &,
                                 void *memory, nb::handle source) {
  if (memory == nullptr) {
    throw std::runtime_error(
        "composite from_python requires live value memory");
  }
  require_python_source(source, "Composite value");

  const auto *state = static_cast<const CompositeIndexedContext *>(context);
  const bool bundle = state->schema != nullptr &&
                      state->schema->value_kind() == ValueTypeKind::Bundle;
  if (!bundle) {
    fill_composite_from_sequence(state, memory, source, "Tuple value");
    return;
  }

  nb::object object = nb::borrow<nb::object>(source);
  const auto *bundle_info = !state->schema->name().empty()
                                ? python_bundle_info(state->schema)
                                : nullptr;
  const bool exact_bundle_class =
      bundle_info != nullptr &&
      Py_TYPE(object.ptr()) ==
          reinterpret_cast<PyTypeObject *>(bundle_info->type.ptr());
  if (!exact_bundle_class && nb::isinstance<nb::dict>(object)) {
    nb::dict map = nb::cast<nb::dict>(object);
    composite_mark_all(state, memory, false);
    for (std::size_t index = 0; index < state->child_bindings.size(); ++index) {
      const char *name = state->schema->fields[index].name;
      if (name == nullptr || *name == '\0') {
        throw std::invalid_argument(
            "Bundle value has an unnamed field and cannot be loaded from dict");
      }
      nb::object fallback_key;
      nb::handle key;
      if (bundle_info != nullptr) {
        key = bundle_info->field_names[index];
      } else {
        fallback_key = nb::str{name};
        key = fallback_key;
      }
      // PARTIAL dicts mark exactly the provided keys (field
      // validity, core_concepts.rst) - absent = UNSET.
      if (!map.contains(key)) {
        continue;
      }
      nb::object value = map[key];
      // None = UNSET (field validity - the same convention as
      // the attribute form; eval_node bundles tick partially).
      if (value.is_none()) {
        continue;
      }
      auto *child = static_cast<std::byte *>(memory) + state->offsets[index];
      assign_child_from_python(state->child_bindings[index], child, value,
                               "Bundle value");
      composite_mark_field(state, memory, index, true);
    }
    return;
  }

  if (!exact_bundle_class && is_python_sequence(source)) {
    fill_composite_from_sequence(state, memory, source, "Bundle value");
    return;
  }

  // ATTRIBUTE form (dataclass / CompoundScalar instances): None
  // fields are UNSET (field validity - the CS convention).
  composite_mark_all(state, memory, false);
  for (std::size_t index = 0; index < state->child_bindings.size(); ++index) {
    const char *name = state->schema->fields[index].name;
    if (name == nullptr || *name == '\0') {
      throw std::invalid_argument("Bundle value has an unnamed field and "
                                  "cannot be loaded from attributes");
    }
    nb::object fallback_key;
    nb::handle key;
    if (bundle_info != nullptr) {
      key = bundle_info->field_names[index];
    } else {
      fallback_key = nb::str{name};
      key = fallback_key;
    }
    // Exact registered data objects do not need a user-defined
    // __getattribute__ dispatch for their declared fields. Keep
    // the general protocol for accepted proxy/attribute objects.
    PyObject *raw_value = exact_bundle_class
                              ? PyObject_GenericGetAttr(object.ptr(), key.ptr())
                              : PyObject_GetAttr(object.ptr(), key.ptr());
    if (raw_value == nullptr) {
      if (PyErr_ExceptionMatches(PyExc_AttributeError)) {
        PyErr_Clear();
        continue;
      }
      nb::raise_python_error();
    }
    nb::object value = nb::steal(raw_value);
    if (value.is_none()) {
      continue;
    }
    auto *child = static_cast<std::byte *>(memory) + state->offsets[index];
    assign_child_from_python(state->child_bindings[index], child, value,
                             "Bundle value");
    composite_mark_field(state, memory, index, true);
  }
}
#endif

[[nodiscard]] std::size_t array_indexed_size(const void *context,
                                             const void *memory) noexcept {
  const auto *state = static_cast<const ArrayIndexedContext *>(context);
  if (!state->bounded) {
    return state->capacity;
  }
  return *reinterpret_cast<const std::size_t *>(
      static_cast<const std::byte *>(memory) + state->size_offset);
}

void array_indexed_resize(const void *context, void *memory, std::size_t size) {
  const auto *state = static_cast<const ArrayIndexedContext *>(context);
  if (!state->bounded || size > state->capacity) {
    throw std::out_of_range(
        "bounded Array resize exceeds its declared capacity");
  }
  *reinterpret_cast<std::size_t *>(static_cast<std::byte *>(memory) +
                                   state->size_offset) = size;
}

[[nodiscard]] const void *array_indexed_element_at(const void *context,
                                                   const void *memory,
                                                   std::size_t index) {
  const auto *state = static_cast<const ArrayIndexedContext *>(context);
  return static_cast<const std::byte *>(memory) + state->data_offset +
         index * state->stride;
}

[[nodiscard]] ValueTypeRef array_indexed_element_binding(const void *context,
                                                         const void *,
                                                         std::size_t) noexcept {
  return static_cast<const ArrayIndexedContext *>(context)->element_binding;
}

[[nodiscard]] ValueView array_indexed_range_projector(const void *context,
                                                      const void *memory,
                                                      std::size_t index) {
  return ValueView{array_indexed_element_binding(context, memory, index),
                   array_indexed_element_at(context, memory, index)};
}

[[nodiscard]] ValueView
array_indexed_mutable_range_projector(const void *context, const void *memory,
                                      std::size_t index) {
  return ValueView{
      array_indexed_element_binding(context, memory, index),
      const_cast<void *>(array_indexed_element_at(context, memory, index))}
      .begin_mutation();
}

[[nodiscard]] Range<ValueView> array_indexed_make_range(const void *context,
                                                        const void *memory) {
  return Range<ValueView>{
      .context = context,
      .memory = memory,
      .limit = array_indexed_size(context, memory),
      .predicate = nullptr,
      .projector = &array_indexed_range_projector,
  };
}

[[nodiscard]] Range<ValueView>
array_indexed_make_mutable_range(const void *context, void *memory) {
  return Range<ValueView>{
      .context = context,
      .memory = memory,
      .limit = array_indexed_size(context, memory),
      .predicate = nullptr,
      .projector = &array_indexed_mutable_range_projector,
  };
}

[[nodiscard]] std::size_t array_value_hash(const void *context,
                                           const void *memory) {
  const auto *state = static_cast<const ArrayIndexedContext *>(context);
  const auto &ops = state->element_binding.ops_ref();
  std::size_t seed = 0;
  const auto size = array_indexed_size(context, memory);
  for (std::size_t index = 0; index < size; ++index) {
    const auto *child = static_cast<const std::byte *>(memory) +
                        state->data_offset + index * state->stride;
    seed = combine_hash(seed, ops.hash(child));
  }
  return seed;
}

[[nodiscard]] bool array_value_equals(const void *context, const void *lhs,
                                      const void *rhs) noexcept {
  if (lhs == nullptr || rhs == nullptr) {
    return lhs == rhs;
  }
  return fallback_on_exception(false, [&] {
    const auto *state = static_cast<const ArrayIndexedContext *>(context);
    const auto lhs_size = array_indexed_size(context, lhs);
    if (lhs_size != array_indexed_size(context, rhs)) {
      return false;
    }
    const auto &ops = state->element_binding.ops_ref();
    for (std::size_t index = 0; index < lhs_size; ++index) {
      const auto *a = static_cast<const std::byte *>(lhs) + state->data_offset +
                      index * state->stride;
      const auto *b = static_cast<const std::byte *>(rhs) + state->data_offset +
                      index * state->stride;
      if (!ops.equals(a, b)) {
        return false;
      }
    }
    return true;
  });
}

[[nodiscard]] std::partial_ordering
array_value_compare(const void *context, const void *lhs,
                    const void *rhs) noexcept {
  if (const auto order = value_ops_detail::null_order(lhs, rhs)) {
    return *order;
  }

  return fallback_on_exception(std::partial_ordering::unordered, [&]() {
    const auto *state = static_cast<const ArrayIndexedContext *>(context);
    const auto &ops = state->element_binding.ops_ref();
    const auto lhs_size = array_indexed_size(context, lhs);
    const auto rhs_size = array_indexed_size(context, rhs);
    const auto common_size = std::min(lhs_size, rhs_size);
    for (std::size_t index = 0; index < common_size; ++index) {
      const auto *a = static_cast<const std::byte *>(lhs) + state->data_offset +
                      index * state->stride;
      const auto *b = static_cast<const std::byte *>(rhs) + state->data_offset +
                      index * state->stride;
      const auto c = ops.compare(a, b);
      if (c != 0) {
        return c;
      }
    }
    if (lhs_size < rhs_size) {
      return std::partial_ordering::less;
    }
    if (lhs_size > rhs_size) {
      return std::partial_ordering::greater;
    }
    return std::partial_ordering::equivalent;
  });
}

[[nodiscard]] std::string array_value_to_string(const void *context,
                                                const void *memory) {
  if (memory == nullptr) {
    return "[]";
  }
  const auto *state = static_cast<const ArrayIndexedContext *>(context);
  const auto &ops = state->element_binding.ops_ref();
  fmt::memory_buffer out;
  fmt::format_to(std::back_inserter(out), "[");
  const auto size = array_indexed_size(context, memory);
  for (std::size_t index = 0; index < size; ++index) {
    if (index > 0) {
      fmt::format_to(std::back_inserter(out), ", ");
    }
    const auto *child = static_cast<const std::byte *>(memory) +
                        state->data_offset + index * state->stride;
    fmt::format_to(std::back_inserter(out), "{}", ops.to_string(child));
  }
  fmt::format_to(std::back_inserter(out), "]");
  return fmt::to_string(out);
}

[[nodiscard]] DynamicStorageMetrics
array_dynamic_storage_metrics(const void *context,
                              const void *memory) noexcept {
  const auto *state = static_cast<const ArrayIndexedContext *>(context);
  const auto &ops = state->element_binding.ops_ref();
  DynamicStorageMetrics result{};
  const auto size = array_indexed_size(context, memory);
  for (std::size_t index = 0; index < size; ++index) {
    const auto *child = static_cast<const std::byte *>(memory) +
                        state->data_offset + index * state->stride;
    result += ops.dynamic_storage_metrics(child);
  }
  return result;
}

#if HGRAPH_ENABLE_PYTHON_USER_NODES
[[nodiscard]] nb::object array_value_to_python(const void *context,
                                               const void *memory) {
  if (memory == nullptr) {
    throw std::runtime_error("array to_python requires live value memory");
  }
  const auto *state = static_cast<const ArrayIndexedContext *>(context);
  const auto &ops = state->element_binding.ops_ref();
  const auto size = array_indexed_size(context, memory);
  if (ops.can_to_python_buffer(state->element_binding)) {
    struct ArrayBufferOwner {
      const void *memory{nullptr};
      const ArrayIndexedContext *state{nullptr};
    };
    const ArrayBufferOwner owner{memory, state};
    const auto element_at = [](const void *owner_memory,
                               std::size_t index) -> const void * {
      const auto *owner_state =
          static_cast<const ArrayBufferOwner *>(owner_memory);
      return static_cast<const std::byte *>(owner_state->memory) +
             owner_state->state->data_offset +
             index * owner_state->state->stride;
    };
    return ops.to_python_buffer(
        state->element_binding,
        ValueArraySource{
            .owner = &owner,
            .size = size,
            .element_at = element_at,
            .first =
                ValueArraySpan{
                    .data = static_cast<const std::byte *>(memory) +
                            state->data_offset,
                    .size = size,
                    .stride = state->stride,
                },
        });
  }

  nb::list result;
  for (std::size_t index = 0; index < size; ++index) {
    const auto *child = static_cast<const std::byte *>(memory) +
                        state->data_offset + index * state->stride;
    result.append(ops.to_python(child));
  }
  return result;
}

[[nodiscard]] nb::object array_value_to_numpy(const void *context,
                                              const void *memory) {
  nb::object value = array_value_to_python(context, memory);
  return nb::module_::import_("numpy").attr("asarray")(std::move(value));
}

void array_value_from_python(const void *context, const ValueTypeRef &,
                             void *memory, nb::handle source) {
  if (memory == nullptr) {
    throw std::runtime_error("array from_python requires live value memory");
  }
  require_python_source(source, "Fixed List value");
  if (!is_python_sequence(source)) {
    throw std::invalid_argument(
        "Fixed List value expects a Python list or tuple");
  }

  const auto *state = static_cast<const ArrayIndexedContext *>(context);
  nb::object object = nb::borrow<nb::object>(source);
  nb::sequence sequence = nb::cast<nb::sequence>(object);
  const auto count = static_cast<std::size_t>(nb::len(sequence));
  if ((!state->bounded && count != state->capacity) ||
      (state->bounded && count > state->capacity)) {
    if (state->bounded) {
      throw std::invalid_argument(
          fmt::format("Array value accepts at most {} elements, got {}",
                      state->capacity, count));
    }
    throw std::invalid_argument(
        fmt::format("Fixed List value expects {} elements, got {}",
                    state->capacity, count));
  }
  if (state->bounded) {
    array_indexed_resize(context, memory, count);
  }

  for (std::size_t index = 0; index < count; ++index) {
    nb::object element = sequence[index];
    auto *child = static_cast<std::byte *>(memory) + state->data_offset +
                  index * state->stride;
    assign_child_from_python(state->element_binding, child, element,
                             "Fixed List value");
  }
}
#endif

struct OwnedValueEntry {
  const ValueTypeMetaData *schema{nullptr};
  MemoryUtils::StoragePlan plan{};
  IndexedValueOps ops{};
  ValueTypeRef binding{};

  explicit OwnedValueEntry(const ValueTypeMetaData &owned_schema)
      : schema{&owned_schema} {
    if (!owned_schema.is_owned() || owned_schema.element_type == nullptr) {
      throw std::logic_error(
          "Owned value entry requires an Owned target schema");
    }

    plan.layout = MemoryUtils::StorageLayout{
        .size = sizeof(OwnedAllocation *),
        .alignment = alignof(OwnedAllocation *),
    };
    plan.lifecycle = MemoryUtils::LifecycleOps{
        .construct = &owned_default_construct,
        .destroy = &owned_destroy,
        .copy_construct = &owned_copy_construct,
        .move_construct = &owned_move_construct,
        .copy_assign = &owned_copy_assign,
        .move_assign = &owned_move_assign,
    };
    plan.lifecycle_context = this;
    plan.trivially_destructible = false;
    plan.trivially_copyable = false;
    plan.trivially_move_constructible = false;

    ops.kind = ValueOpsKind::Indexed;
    ops.context = this;
    ops.allows_mutation = true;
    ops.hash_impl = owned_schema.is_hashable() ? &hash : nullptr;
    ops.equals_impl = owned_schema.is_equatable() ? &equals : nullptr;
    ops.compare_impl = owned_schema.is_comparable() ? &compare : nullptr;
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
    ops.writable_concrete_memory_impl = &writable_concrete_memory;
    ops.dynamic_storage_metrics_impl = &dynamic_storage_metrics;
    ops.size = &indexed_size;
    ops.element_at = &element_at;
    ops.element_binding = &element_binding;
    ops.make_range = &make_range;
    ops.make_mutable_range = &make_mutable_range;
    ops.mutable_element_at = &mutable_element_at;

    binding = intern_value_type(owned_schema, plan, ops);
  }

  [[nodiscard]] static const OwnedValueEntry &
  entry(const void *context) noexcept {
    return *static_cast<const OwnedValueEntry *>(context);
  }

  [[nodiscard]] static ValueTypeRef
  allocation_type(const OwnedAllocation *allocation) {
    return owned_allocation_type(*allocation);
  }

  [[nodiscard]] static const IndexedValueOps &indexed_ops(ValueTypeRef target) {
    const auto *indexed =
        checked_value_ops<IndexedValueOps>(target, "Owned target");
    if (indexed->size == nullptr || indexed->element_at == nullptr ||
        indexed->element_binding == nullptr) {
      throw std::logic_error(
          "Owned target does not provide indexed Bundle operations");
    }
    return *indexed;
  }

  [[nodiscard]] static OwnedAllocation *
  ensure_allocation(const OwnedValueEntry &self, void *memory) {
    if (auto *allocation = owned_allocation(memory); allocation != nullptr) {
      return allocation;
    }
    auto *allocation = allocate_owned(owned_target_type(*self.schema));
    set_owned_allocation(memory, allocation);
    return allocation;
  }

  [[nodiscard]] static std::pair<ValueTypeRef, const void *>
  projected(const OwnedAllocation &allocation) noexcept {
    return fallback_on_exception(
        std::pair<ValueTypeRef, const void *>{}, [&]() {
          const auto type = owned_allocation_type(allocation);
          return std::pair{
              type.ops_ref().concrete_type(type, owned_payload(allocation)),
              type.ops_ref().concrete_memory(owned_payload(allocation)),
          };
        });
  }

  [[nodiscard]] static std::size_t hash(const void *, const void *memory) {
    const auto *allocation = owned_allocation(memory);
    if (allocation == nullptr) {
      return 0x4f776e65644e756cULL;
    }
    const auto type = allocation_type(allocation);
    return type.ops_ref().hash(owned_payload(*allocation));
  }

  [[nodiscard]] static bool equals(const void *, const void *lhs,
                                   const void *rhs) noexcept {
    const auto *left = owned_allocation(lhs);
    const auto *right = owned_allocation(rhs);
    if (left == nullptr || right == nullptr) {
      return left == right;
    }
    const auto [left_type, left_memory] = projected(*left);
    const auto [right_type, right_memory] = projected(*right);
    return left_type && left_type == right_type &&
           left_type.ops_ref().equals(left_memory, right_memory);
  }

  [[nodiscard]] static std::partial_ordering
  compare(const void *, const void *lhs, const void *rhs) noexcept {
    const auto *left = owned_allocation(lhs);
    const auto *right = owned_allocation(rhs);
    if (left == nullptr || right == nullptr) {
      if (left == right) {
        return std::partial_ordering::equivalent;
      }
      return left == nullptr ? std::partial_ordering::less
                             : std::partial_ordering::greater;
    }
    const auto [left_type, left_memory] = projected(*left);
    const auto [right_type, right_memory] = projected(*right);
    if (!left_type || left_type != right_type) {
      return std::partial_ordering::unordered;
    }
    return left_type.ops_ref().compare(left_memory, right_memory);
  }

  [[nodiscard]] static std::string to_string(const void *, const void *memory) {
    const auto *allocation = owned_allocation(memory);
    if (allocation == nullptr) {
      return "None";
    }
    return allocation_type(allocation)
        .ops_ref()
        .to_string(owned_payload(*allocation));
  }

#if HGRAPH_ENABLE_PYTHON_USER_NODES
  [[nodiscard]] static nb::object to_python(const void *, const void *memory) {
    const auto *allocation = owned_allocation(memory);
    if (allocation == nullptr) {
      return nb::none();
    }
    return allocation_type(allocation)
        .ops_ref()
        .to_python(owned_payload(*allocation));
  }

  static void from_python(const void *context, const ValueTypeRef &,
                          void *memory, nb::handle source) {
    const auto &self = entry(context);
    if (source.is_none()) {
      auto *previous = owned_allocation(memory);
      set_owned_allocation(memory, nullptr);
      destroy_owned_allocation(previous);
      return;
    }

    const auto desired = owned_target_type(*self.schema);
    auto *allocation = owned_allocation(memory);
    if (allocation == nullptr || allocation->record != desired.record()) {
      auto *replacement = allocate_owned(desired);
      auto cleanup = make_scope_exit(
          [&]() noexcept { destroy_owned_allocation(replacement); });
      desired.ops_ref().from_python(desired, owned_payload(*replacement),
                                    source);
      auto *previous = allocation;
      set_owned_allocation(memory, replacement);
      cleanup.release();
      destroy_owned_allocation(previous);
      return;
    }
    desired.ops_ref().from_python(desired, owned_payload(*allocation), source);
  }
#endif

  [[nodiscard]] static std::size_t indexed_size(const void *context,
                                                const void *memory) noexcept {
    return fallback_on_exception(std::size_t{0}, [&]() {
      const auto *allocation = owned_allocation(memory);
      const auto type = allocation != nullptr
                            ? allocation_type(allocation)
                            : owned_target_type(*entry(context).schema);
      const auto &indexed = indexed_ops(type);
      return indexed.size(indexed.context, allocation != nullptr
                                               ? owned_payload(*allocation)
                                               : nullptr);
    });
  }

  [[nodiscard]] static const void *element_at(const void *, const void *memory,
                                              std::size_t index) {
    const auto *allocation = owned_allocation(memory);
    if (allocation == nullptr) {
      return nullptr;
    }
    const auto type = allocation_type(allocation);
    const auto &indexed = indexed_ops(type);
    return indexed.element_at(indexed.context, owned_payload(*allocation),
                              index);
  }

  [[nodiscard]] static ValueTypeRef
  element_binding(const void *context, const void *memory,
                  std::size_t index) noexcept {
    return fallback_on_exception(ValueTypeRef{}, [&]() {
      const auto *allocation = owned_allocation(memory);
      const auto type = allocation != nullptr
                            ? allocation_type(allocation)
                            : owned_target_type(*entry(context).schema);
      const auto &indexed = indexed_ops(type);
      return indexed.element_binding(
          indexed.context,
          allocation != nullptr ? owned_payload(*allocation) : nullptr, index);
    });
  }

  [[nodiscard]] static void *
  mutable_element_at(const void *context, void *memory, std::size_t index) {
    const auto &self = entry(context);
    auto *allocation = ensure_allocation(self, memory);
    const auto type = allocation_type(allocation);
    const auto &indexed = indexed_ops(type);
    if (indexed.mutable_element_at != nullptr) {
      return indexed.mutable_element_at(indexed.context,
                                        owned_payload(*allocation), index);
    }
    return const_cast<void *>(
        indexed.element_at(indexed.context, owned_payload(*allocation), index));
  }

  [[nodiscard]] static ValueView
  range_projector(const void *context, const void *memory, std::size_t index) {
    return ValueView{element_binding(context, memory, index),
                     element_at(context, memory, index)};
  }

  [[nodiscard]] static ValueView mutable_range_projector(const void *context,
                                                         const void *memory,
                                                         std::size_t index) {
    return ValueView{
        element_binding(context, memory, index),
        mutable_element_at(context, const_cast<void *>(memory), index)}
        .begin_mutation();
  }

  [[nodiscard]] static Range<ValueView> make_range(const void *context,
                                                   const void *memory) {
    return Range<ValueView>{
        .context = context,
        .memory = memory,
        .limit = indexed_size(context, memory),
        .predicate = nullptr,
        .projector = &range_projector,
    };
  }

  [[nodiscard]] static Range<ValueView> make_mutable_range(const void *context,
                                                           void *memory) {
    (void)ensure_allocation(entry(context), memory);
    return Range<ValueView>{
        .context = context,
        .memory = memory,
        .limit = indexed_size(context, memory),
        .predicate = nullptr,
        .projector = &mutable_range_projector,
    };
  }

  [[nodiscard]] static bool accepts_source(const void *context,
                                           ValueTypeRef binding,
                                           ValueTypeRef source) noexcept {
    return fallback_on_exception(false, [&]() {
      const auto &self = entry(context);
      if (source == binding) {
        return true;
      }
      const auto target = owned_target_type(*self.schema);
      return target.ops_ref().accepts_source(target, source) ||
             TypeRegistry::instance().bundle_is_a(source.schema(),
                                                  self.schema->element_type);
    });
  }

  [[nodiscard]] static ValueTypeRef
  destination_type(const OwnedValueEntry &self, ValueTypeRef source) {
    const auto target = owned_target_type(*self.schema);
    if (target.ops_ref().accepts_source(target, source)) {
      return target;
    }
    if (TypeRegistry::instance().bundle_is_a(source.schema(),
                                             self.schema->element_type)) {
      return source;
    }
    throw std::invalid_argument(
        "Owned value received an incompatible source type");
  }

  static void copy_assign_from(const void *context, ValueTypeRef binding,
                               void *dst, ValueTypeRef source,
                               const void *src) {
    if (source == binding) {
      owned_copy_assign(dst, src, context);
      return;
    }

    const auto &self = entry(context);
    auto *allocation = owned_allocation(dst);
    if (allocation != nullptr) {
      const auto current = allocation_type(allocation);
      if (current.ops_ref().accepts_source(current, source)) {
        current.ops_ref().copy_assign_from(current, owned_payload(*allocation),
                                           source, src);
        return;
      }
    }

    const auto target = destination_type(self, source);
    auto *replacement = allocate_owned(target);
    auto cleanup = make_scope_exit(
        [&]() noexcept { destroy_owned_allocation(replacement); });
    target.ops_ref().copy_assign_from(target, owned_payload(*replacement),
                                      source, src);
    auto *previous = allocation;
    set_owned_allocation(dst, replacement);
    cleanup.release();
    destroy_owned_allocation(previous);
  }

  static void move_assign_from(const void *context, ValueTypeRef binding,
                               void *dst, ValueTypeRef source, void *src) {
    if (source == binding) {
      owned_move_assign(dst, src, context);
      return;
    }

    const auto &self = entry(context);
    auto *allocation = owned_allocation(dst);
    if (allocation != nullptr) {
      const auto current = allocation_type(allocation);
      if (current.ops_ref().accepts_source(current, source)) {
        current.ops_ref().move_assign_from(current, owned_payload(*allocation),
                                           source, src);
        return;
      }
    }

    const auto target = destination_type(self, source);
    auto *replacement = allocate_owned(target);
    auto cleanup = make_scope_exit(
        [&]() noexcept { destroy_owned_allocation(replacement); });
    target.ops_ref().move_assign_from(target, owned_payload(*replacement),
                                      source, src);
    auto *previous = allocation;
    set_owned_allocation(dst, replacement);
    cleanup.release();
    destroy_owned_allocation(previous);
  }

  [[nodiscard]] static ValueTypeRef concrete_type(const void *,
                                                  ValueTypeRef binding,
                                                  const void *memory) noexcept {
    const auto *allocation = owned_allocation(memory);
    if (allocation == nullptr) {
      return binding;
    }
    return projected(*allocation).first;
  }

  [[nodiscard]] static const void *
  concrete_memory(const void *, const void *memory) noexcept {
    const auto *allocation = owned_allocation(memory);
    if (allocation == nullptr) {
      return memory;
    }
    return projected(*allocation).second;
  }

  [[nodiscard]] static void *mutable_concrete_memory(const void *,
                                                     void *memory) noexcept {
    auto *allocation = owned_allocation(memory);
    if (allocation == nullptr) {
      return memory;
    }
    return fallback_on_exception(static_cast<void *>(nullptr), [&]() {
      const auto type = allocation_type(allocation);
      return type.ops_ref().mutable_concrete_memory(owned_payload(*allocation));
    });
  }

  [[nodiscard]] static void *writable_concrete_memory(const void *,
                                                      void *memory) {
    auto *allocation = owned_allocation(memory);
    if (allocation == nullptr) {
      return memory;
    }
    const auto type = allocation_type(allocation);
    return type.ops_ref().writable_concrete_memory(owned_payload(*allocation));
  }

  [[nodiscard]] static DynamicStorageMetrics
  dynamic_storage_metrics(const void *, const void *memory) noexcept {
    const auto *allocation = owned_allocation(memory);
    if (allocation == nullptr) {
      return {};
    }

    const auto type = allocation_type(allocation);
    const auto layout = owned_allocation_layout(type.checked_plan());
    DynamicStorageMetrics result{
        .live_bytes = layout.size,
        .reserved_bytes = layout.size,
    };
    result +=
        type.ops_ref().dynamic_storage_metrics(owned_payload(*allocation));
    return result;
  }
};

struct CompositeIndexedOpsEntry {
  CompositeIndexedContext context{};
  IndexedValueOps ops{};

  CompositeIndexedOpsEntry(const CompositeIndexedOpsEntry &other)
      : context{other.context}, ops{other.ops} {
    ops.context = &context;
  }

  CompositeIndexedOpsEntry(CompositeIndexedOpsEntry &&other) noexcept
      : context{std::move(other.context)}, ops{other.ops} {
    ops.context = &context;
  }

  CompositeIndexedOpsEntry(const ValueTypeMetaData &schema,
                           const MemoryUtils::StoragePlan &plan,
                           const std::vector<ValueTypeRef> &child_bindings) {
    const std::size_t validity_words =
        (schema.value_kind() == ValueTypeKind::Bundle ||
         schema.value_kind() == ValueTypeKind::Tuple)
            ? bundle_validity_word_count(schema.field_count)
            : 0;
    const std::size_t validity_components = validity_words == 0 ? 0 : 1;
    if (!plan.is_composite() ||
        plan.component_count() != schema.field_count + validity_components) {
      throw std::logic_error("ValuePlanFactory: composite indexed ops require "
                             "matching composite plan");
    }

    if (child_bindings.size() != schema.field_count) {
      throw std::logic_error("ValuePlanFactory: composite binding count does "
                             "not match its schema");
    }
    context.child_bindings.reserve(schema.field_count);
    context.offsets.reserve(schema.field_count);
    const auto components = plan.components();
    for (std::size_t index = 0; index < schema.field_count; ++index) {
      const auto child_binding = child_bindings[index];
      if (child_binding == nullptr) {
        throw std::logic_error("ValuePlanFactory: composite field has no "
                               "resolvable indexed binding");
      }
      context.child_bindings.push_back(child_binding);
      context.offsets.push_back(components[index].offset);
    }

    context.schema = &schema;
    if (validity_components != 0) {
      // The hidden fixed-word validity component sits right
      // after the public fields (appended last at plan build).
      context.validity_offset = components[schema.field_count].offset;
      context.validity_word_count = validity_words;
    }

    ops = IndexedValueOps{
        {ValueOpsKind::Indexed, &context, true,
         schema.is_hashable() ? &composite_value_hash : nullptr,
         &composite_value_equals, &composite_value_compare,
         &composite_value_to_string
#if HGRAPH_ENABLE_PYTHON_USER_NODES
         ,
         &composite_value_to_python, &composite_value_from_python
#endif
        },
        &composite_indexed_size,
        &composite_indexed_element_at,
        &composite_indexed_element_binding,
        &composite_indexed_make_range,
        &composite_indexed_make_mutable_range,
    };
    ops.mutable_element_at = &composite_indexed_mutable_element_at;
    ops.accepts_source_impl = &composite_accepts_source;
    ops.copy_assign_from_impl = &composite_copy_assign_from;
    ops.move_assign_from_impl = &composite_move_assign_from;
    ops.dynamic_storage_metrics_impl = &composite_dynamic_storage_metrics;
  }

  CompositeIndexedOpsEntry(const ValueTypeMetaData &schema,
                           const MemoryUtils::StoragePlan &plan)
      : CompositeIndexedOpsEntry(schema, plan, [&]() {
          std::vector<ValueTypeRef> bindings;
          bindings.reserve(schema.field_count);
          for (std::size_t index = 0; index < schema.field_count; ++index) {
            bindings.push_back(ValuePlanFactory::instance().type_for(
                schema.fields[index].type));
          }
          return bindings;
        }()) {}
};

struct ArrayIndexedOpsEntry {
  ArrayIndexedContext context{};
  IndexedValueOps ops{};

  ArrayIndexedOpsEntry(const ArrayIndexedOpsEntry &other)
      : context{other.context}, ops{other.ops} {
    ops.context = &context;
  }

  ArrayIndexedOpsEntry(ArrayIndexedOpsEntry &&other) noexcept
      : context{other.context}, ops{other.ops} {
    ops.context = &context;
  }

  ArrayIndexedOpsEntry(const ValueTypeMetaData &schema,
                       const MemoryUtils::StoragePlan &plan) {
    const MemoryUtils::StoragePlan *elements_plan = &plan;
    std::size_t size_offset = 0;
    std::size_t data_offset = 0;
    bool bounded = false;
    if (schema.is_shaped_array() && plan.is_tuple() &&
        plan.component_count() == 2) {
      data_offset = plan.component(0).offset;
      size_offset = plan.component(1).offset;
      elements_plan = plan.component(0).plan;
      bounded = true;
    }
    if (!elements_plan->is_array()) {
      throw std::logic_error(
          "ValuePlanFactory: array indexed ops require an array plan");
    }

    const auto element_binding =
        ValuePlanFactory::instance().type_for(schema.element_type);
    if (element_binding == nullptr) {
      throw std::logic_error(
          "ValuePlanFactory: fixed list element has no resolvable binding");
    }

    context = ArrayIndexedContext{
        .schema = &schema,
        .element_binding = element_binding,
        .capacity = elements_plan->array_count(),
        .stride = elements_plan->array_stride(),
        .size_offset = size_offset,
        .data_offset = data_offset,
        .bounded = bounded,
    };

    ops = IndexedValueOps{
        {ValueOpsKind::Indexed, &context, true,
         schema.is_hashable() ? &array_value_hash : nullptr,
         &array_value_equals, &array_value_compare, &array_value_to_string
#if HGRAPH_ENABLE_PYTHON_USER_NODES
         ,
         schema.is_shaped_array() ? &array_value_to_numpy
                                  : &array_value_to_python,
         &array_value_from_python
#endif
        },
        &array_indexed_size,
        &array_indexed_element_at,
        &array_indexed_element_binding,
        &array_indexed_make_range,
        &array_indexed_make_mutable_range,
    };
    ops.resize = bounded ? &array_indexed_resize : nullptr;
    ops.dynamic_storage_metrics_impl = &array_dynamic_storage_metrics;
  }
};

[[nodiscard]] InternTable<const ValueTypeMetaData *, CompositeIndexedOpsEntry> &
composite_indexed_ops_cache() noexcept {
  static InternTable<const ValueTypeMetaData *, CompositeIndexedOpsEntry> cache;
  return cache;
}

[[nodiscard]] InternTable<const ValueTypeMetaData *, ArrayIndexedOpsEntry> &
array_indexed_ops_cache() noexcept {
  static InternTable<const ValueTypeMetaData *, ArrayIndexedOpsEntry> cache;
  return cache;
}

// Entries are address-published (interned TypeRecords point at their ops),
// so the table stores stable unique_ptr pointees.
[[nodiscard]] InternTable<const ValueTypeMetaData *,
                          std::unique_ptr<OwnedValueEntry>> &
owned_value_cache() noexcept {
  static InternTable<const ValueTypeMetaData *,
                     std::unique_ptr<OwnedValueEntry>>
      cache;
  return cache;
}

[[nodiscard]] const OwnedValueEntry &
owned_value_entry(const ValueTypeMetaData &schema) {
  // intern_serialized: the entry ctor interns a TypeRecord pointing at
  // itself, so a racing loser must never be constructed and destroyed.
  return *owned_value_cache().intern_serialized(
      &schema, [&] { return std::make_unique<OwnedValueEntry>(schema); });
}

struct RealizedCompositeKey {
  const ValueTypeMetaData *schema{nullptr};
  std::vector<ValueTypeRef> fields{};
  bool operator==(const RealizedCompositeKey &) const noexcept = default;
};

struct RealizedCompositeKeyHash {
  [[nodiscard]] std::size_t
  operator()(const RealizedCompositeKey &key) const noexcept {
    std::size_t seed = std::hash<const ValueTypeMetaData *>{}(key.schema);
    for (const auto field : key.fields) {
      seed = combine_hash(seed, std::hash<ValueTypeRef>{}(field));
    }
    return seed;
  }
};

[[nodiscard]] const MemoryUtils::StoragePlan &
realized_composite_plan(const ValueTypeMetaData &schema,
                        const std::vector<ValueTypeRef> &fields) {
  const std::size_t validity_words =
      bundle_validity_word_count(schema.field_count);
  if (schema.value_kind() == ValueTypeKind::Bundle) {
    auto builder = MemoryUtils::named_tuple();
    builder.reserve(fields.size() + (validity_words == 0 ? 0 : 1));
    for (std::size_t index = 0; index < fields.size(); ++index) {
      builder.add_field(
          schema.fields[index].name != nullptr ? schema.fields[index].name : "",
          fields[index].checked_plan());
    }
    if (validity_words != 0) {
      builder.add_hidden_plan(bundle_validity_plan(schema.field_count));
    }
    return builder.build();
  }

  auto builder = MemoryUtils::tuple();
  builder.reserve(fields.size() + (validity_words == 0 ? 0 : 1));
  for (const auto field : fields) {
    builder.add_plan(field.checked_plan());
  }
  if (validity_words != 0) {
    builder.add_hidden_plan(bundle_validity_plan(schema.field_count));
  }
  return builder.build();
}

struct RealizedCompositeEntry {
  const MemoryUtils::StoragePlan *plan{nullptr};
  CompositeIndexedOpsEntry ops;
  ValueTypeRef binding{};

  RealizedCompositeEntry(const ValueTypeMetaData &schema,
                         const std::vector<ValueTypeRef> &fields)
      : plan{&realized_composite_plan(schema, fields)},
        ops{schema, *plan, fields} {
    const auto &debug = intern_fixed_composite_debug_descriptor(schema, *plan);
    binding = intern_value_type(schema, *plan, ops.ops, &debug);
  }
};

// Entries are address-published (interned TypeRecords point at their ops),
// so the table stores stable unique_ptr pointees.
struct RealizedCompositeCache {
  InternTable<RealizedCompositeKey, std::unique_ptr<RealizedCompositeEntry>,
              RealizedCompositeKeyHash>
      table{};

  [[nodiscard]] ValueTypeRef get(const ValueTypeMetaData &schema,
                                 std::vector<ValueTypeRef> fields) {
    RealizedCompositeKey key{&schema, std::move(fields)};
    // intern_serialized: the entry ctor interns a TypeRecord pointing at
    // itself, so a racing loser must never be constructed and destroyed.
    return table
        .intern_serialized(key,
                           [&] {
                             return std::make_unique<RealizedCompositeEntry>(
                                 schema, key.fields);
                           })
        ->binding;
  }

  void clear() noexcept { table.clear(); }
};

[[nodiscard]] RealizedCompositeCache &realized_composite_cache() noexcept {
  static RealizedCompositeCache cache;
  return cache;
}

[[nodiscard]] const IndexedValueOps &
composite_indexed_ops(const ValueTypeMetaData &schema,
                      const MemoryUtils::StoragePlan &plan) {
  return composite_indexed_ops_cache().emplace(&schema, schema, plan).ops;
}

[[nodiscard]] const IndexedValueOps &
array_indexed_ops(const ValueTypeMetaData &schema,
                  const MemoryUtils::StoragePlan &plan) {
  return array_indexed_ops_cache().emplace(&schema, schema, plan).ops;
}

void clear_structured_indexed_ops() noexcept {
  composite_indexed_ops_cache().clear();
  array_indexed_ops_cache().clear();
  owned_value_cache().clear();
  realized_composite_cache().clear();
}

#if HGRAPH_ENABLE_PYTHON_USER_NODES
[[nodiscard]] bool
retained_python_supported(const ValueTypeMetaData *schema) noexcept {
  if (schema == nullptr) {
    return false;
  }
  if (python_bridge::is_python_bundle_schema(schema)) {
    return true;
  }
  switch (schema->value_kind()) {
  case ValueTypeKind::Atomic:
    return schema == scalar_descriptor<Bool>::value_meta() ||
           schema == scalar_descriptor<Int>::value_meta() ||
           schema == scalar_descriptor<Float>::value_meta() ||
           schema == scalar_descriptor<Str>::value_meta() ||
           schema == scalar_descriptor<Bytes>::value_meta();
  case ValueTypeKind::Tuple:
    for (std::size_t index = 0; index < schema->field_count; ++index) {
      if (!retained_python_supported(schema->fields[index].type)) {
        return false;
      }
    }
    return true;
  case ValueTypeKind::List:
    return schema->fixed_size == 0 &&
           !schema->has(ValueTypeFlags::ShapedArray) &&
           retained_python_supported(schema->element_type);
  case ValueTypeKind::Bundle:
    return python_bundle_info(schema) != nullptr;
  case ValueTypeKind::Set:
  case ValueTypeKind::Map:
  case ValueTypeKind::CyclicBuffer:
  case ValueTypeKind::Queue:
  case ValueTypeKind::Any:
    return false;
  }
  return false;
}

[[nodiscard]] bool
python_cache_beneficial(ValueTypeRef native_binding) noexcept {
  const auto *schema = native_binding.schema();
  return retained_python_supported(schema) &&
         !python_bridge::is_python_bundle_binding(native_binding) &&
         schema != scalar_descriptor<Bool>::value_meta() &&
         schema != scalar_descriptor<Int>::value_meta() &&
         schema != scalar_descriptor<Float>::value_meta();
}

[[nodiscard]] ValueTypeRef
python_bundle_storage_binding(ValueTypeRef source_binding) {
  if (python_bridge::is_python_bundle_binding(source_binding)) {
    return source_binding;
  }
  const auto *schema = source_binding.schema();
  if (!python_bridge::is_python_bundle_schema(schema)) {
    return {};
  }
  const auto *ops = try_value_ops<IndexedValueOps>(source_binding);
  if (ops == nullptr || ops->element_binding == nullptr) {
    return {};
  }
  std::vector<ValueTypeRef> fields;
  fields.reserve(schema->field_count);
  for (std::size_t index = 0; index < schema->field_count; ++index) {
    const auto field = ops->element_binding(ops->context, nullptr, index);
    if (!field) {
      return {};
    }
    fields.push_back(field);
  }
  return python_bridge::python_bundle_binding_for(schema, fields);
}

[[noreturn]] void invalid_retained_python_value(const ValueTypeMetaData &schema,
                                                std::string_view reason) {
  throw nb::type_error(("Python value for '" + std::string{schema.name()} +
                        "' " + std::string{reason})
                           .c_str());
}

[[nodiscard]] nb::object
prepare_retained_python_value(const ValueTypeMetaData *schema,
                              nb::handle source) {
  if (schema == nullptr || !source.is_valid() || source.is_none()) {
    throw nb::type_error(
        "retained Python output storage requires a typed non-None value");
  }
  if (schema->value_kind() == ValueTypeKind::Bundle) {
    const auto *info = python_bundle_info(schema);
    if (info == nullptr || !info->type.is_valid()) {
      invalid_retained_python_value(*schema, "has no registered Python class");
    }
    if (nb::isinstance(source, info->type)) {
      return nb::borrow<nb::object>(source);
    }
    // Dict and other accepted bridge inputs must retain the declared Python
    // read shape, not the raw source object.
    const auto binding = ValuePlanFactory::instance().type_for(schema);
    Value normalized{binding};
    binding.ops_ref().from_python(
        binding, const_cast<void *>(normalized.view().data()), source);
    return binding.ops_ref().to_python(normalized.view().data());
  }

  if (schema->value_kind() == ValueTypeKind::Atomic) {
    if (schema == scalar_descriptor<Bool>::value_meta()) {
      if (!PyBool_Check(source.ptr())) {
        invalid_retained_python_value(*schema, "requires bool");
      }
      return nb::borrow<nb::object>(source);
    }
    if (schema == scalar_descriptor<Int>::value_meta()) {
      if (PyLong_CheckExact(source.ptr())) {
        static_cast<void>(PyLong_AsLongLong(source.ptr()));
        if (PyErr_Occurred() != nullptr) {
          throw nb::python_error();
        }
        return nb::borrow<nb::object>(source);
      }
      if (PyUnicode_Check(source.ptr()) || PyBytes_Check(source.ptr())) {
        invalid_retained_python_value(*schema, "requires an integer");
      }
      nb::object normalized = nb::steal(PyNumber_Long(source.ptr()));
      if (!normalized.is_valid()) {
        throw nb::python_error();
      }
      static_cast<void>(PyLong_AsLongLong(normalized.ptr()));
      if (PyErr_Occurred() != nullptr) {
        throw nb::python_error();
      }
      return normalized;
    }
    if (schema == scalar_descriptor<Float>::value_meta()) {
      if (PyFloat_CheckExact(source.ptr())) {
        return nb::borrow<nb::object>(source);
      }
      if (PyUnicode_Check(source.ptr()) || PyBytes_Check(source.ptr())) {
        invalid_retained_python_value(*schema, "requires a number");
      }
      nb::object normalized = nb::steal(PyNumber_Float(source.ptr()));
      if (!normalized.is_valid()) {
        throw nb::python_error();
      }
      return normalized;
    }
    if (schema == scalar_descriptor<Str>::value_meta()) {
      if (!PyUnicode_Check(source.ptr())) {
        invalid_retained_python_value(*schema, "requires str");
      }
      return PyUnicode_CheckExact(source.ptr())
                 ? nb::borrow<nb::object>(source)
                 : nb::steal(PyUnicode_FromObject(source.ptr()));
    }
    if (schema == scalar_descriptor<Bytes>::value_meta()) {
      if (!PyBytes_Check(source.ptr())) {
        invalid_retained_python_value(*schema, "requires bytes");
      }
      if (PyBytes_CheckExact(source.ptr())) {
        return nb::borrow<nb::object>(source);
      }
      char *buffer = nullptr;
      Py_ssize_t length = 0;
      if (PyBytes_AsStringAndSize(source.ptr(), &buffer, &length) != 0) {
        throw nb::python_error();
      }
      return nb::steal(PyBytes_FromStringAndSize(buffer, length));
    }
    invalid_retained_python_value(*schema, "has no Python-only storage policy");
  }

  if (schema->value_kind() == ValueTypeKind::Tuple) {
    if (PySequence_Check(source.ptr()) == 0) {
      invalid_retained_python_value(*schema, "expects a Python list or tuple");
    }
    const Py_ssize_t count = PySequence_Size(source.ptr());
    if (count < 0) {
      throw nb::python_error();
    }
    const auto field_count = static_cast<Py_ssize_t>(schema->field_count);
    if (count < field_count) {
      invalid_retained_python_value(
          *schema, "does not provide every declared tuple field");
    }
    bool exact = PyTuple_CheckExact(source.ptr()) && count == field_count;
    nb::tuple normalized = nb::steal<nb::tuple>(PyTuple_New(field_count));
    if (!normalized.is_valid()) {
      throw nb::python_error();
    }
    for (Py_ssize_t index = 0; index < field_count; ++index) {
      nb::object item = nb::steal(PySequence_GetItem(source.ptr(), index));
      if (!item.is_valid()) {
        throw nb::python_error();
      }
      nb::object prepared =
          item.is_none()
              ? nb::none()
              : prepare_retained_python_value(
                    schema->fields[static_cast<std::size_t>(index)].type, item);
      exact = exact && prepared.is(item);
      if (PyTuple_SetItem(normalized.ptr(), index, prepared.release().ptr()) !=
          0) {
        throw nb::python_error();
      }
    }
    return exact ? nb::borrow<nb::object>(source)
                 : nb::object{std::move(normalized)};
  }

  if (schema->value_kind() == ValueTypeKind::List && schema->fixed_size == 0) {
    const bool sequence = PyList_Check(source.ptr()) ||
                          PyTuple_Check(source.ptr()) ||
                          nb::hasattr(source, "__array_interface__");
    if (!sequence) {
      invalid_retained_python_value(*schema, "expects a Python list or tuple");
    }
    nb::object source_object = nb::borrow<nb::object>(source);
    nb::list prepared_items;
    bool unchanged = schema->has(ValueTypeFlags::VariadicTuple)
                         ? PyTuple_CheckExact(source.ptr())
                         : PyList_CheckExact(source.ptr());
    for (nb::handle item : nb::iter(source_object)) {
      if (item.is_none()) {
        invalid_retained_python_value(*schema, "does not allow None elements");
      }
      nb::object prepared =
          prepare_retained_python_value(schema->element_type, item);
      unchanged = unchanged && prepared.is(item);
      prepared_items.append(std::move(prepared));
    }
    if (unchanged) {
      return source_object;
    }
    return schema->has(ValueTypeFlags::VariadicTuple)
               ? nb::object{nb::tuple(prepared_items)}
               : nb::object{std::move(prepared_items)};
  }

  invalid_retained_python_value(*schema, "has no Python-only storage policy");
}

struct PythonRetainedBindingEntry {
  const ValueTypeMetaData *schema{nullptr};
  ValueOps ops{};
  ValueTypeRef binding{};

  explicit PythonRetainedBindingEntry(const ValueTypeMetaData *value_schema)
      : schema(value_schema) {
    if (!retained_python_supported(schema) ||
        python_bridge::is_python_bundle_schema(schema)) {
      throw std::invalid_argument(
          "Python-retained binding requires a supported non-native schema");
    }
    ops.kind = ValueOpsKind::Base;
    ops.context = this;
    ops.allows_mutation = false;
    ops.hash_impl = schema->is_hashable() ? &hash : nullptr;
    ops.equals_impl = schema->is_equatable() ? &equals : nullptr;
    ops.compare_impl = schema->is_comparable() ? &compare : nullptr;
    ops.to_string_impl = &to_string;
    ops.to_python_impl = &to_python;
    ops.from_python_impl = &from_python;
    ops.accepts_source_impl = &accepts_source;
    ops.copy_assign_from_impl = &copy_assign_from;
    ops.move_assign_from_impl = &move_assign_from;
    ops.format_string_impl = &to_string;
    binding = intern_value_type(
        *schema, MemoryUtils::plan_for<python_bridge::PythonValueHolder>(),
        ops);
  }

  [[nodiscard]] static const PythonRetainedBindingEntry &
  entry(const void *context) noexcept {
    return *static_cast<const PythonRetainedBindingEntry *>(context);
  }

  [[nodiscard]] static python_bridge::PythonValueHolder &value(void *memory) {
    if (memory == nullptr) {
      throw std::logic_error(
          "Python-retained value operation requires live memory");
    }
    return *static_cast<python_bridge::PythonValueHolder *>(memory);
  }

  [[nodiscard]] static const python_bridge::PythonValueHolder &
  value(const void *memory) {
    if (memory == nullptr) {
      throw std::logic_error(
          "Python-retained value operation requires live memory");
    }
    return *static_cast<const python_bridge::PythonValueHolder *>(memory);
  }

  static std::size_t hash(const void *, const void *memory) {
    const auto &stored = value(memory);
    if (stored.object == nullptr) {
      throw std::logic_error("cannot hash an empty Python-retained value");
    }
    nb::gil_scoped_acquire gil;
    const Py_hash_t result = PyObject_Hash(stored.object);
    if (result == -1) {
      PyErr_Clear();
      return std::hash<const void *>{}(stored.object);
    }
    return static_cast<std::size_t>(result);
  }

  static bool equals(const void *, const void *lhs, const void *rhs) {
    const auto &left = value(lhs);
    const auto &right = value(rhs);
    if (left.object == right.object) {
      return true;
    }
    if (left.object == nullptr || right.object == nullptr) {
      return false;
    }
    nb::gil_scoped_acquire gil;
    const int result =
        PyObject_RichCompareBool(left.object, right.object, Py_EQ);
    if (result < 0) {
      throw nb::python_error();
    }
    return result == 1;
  }

  static std::partial_ordering compare(const void *, const void *lhs,
                                       const void *rhs) noexcept {
    nb::gil_scoped_acquire gil;
    try {
      const auto &left = value(lhs);
      const auto &right = value(rhs);
      if (left.object == right.object) {
        return std::partial_ordering::equivalent;
      }
      if (left.object == nullptr || right.object == nullptr) {
        return left.object == nullptr ? std::partial_ordering::less
                                      : std::partial_ordering::greater;
      }
      const int less =
          PyObject_RichCompareBool(left.object, right.object, Py_LT);
      if (less < 0) {
        PyErr_Clear();
        return std::partial_ordering::unordered;
      }
      if (less == 1) {
        return std::partial_ordering::less;
      }
      const int greater =
          PyObject_RichCompareBool(left.object, right.object, Py_GT);
      if (greater < 0) {
        PyErr_Clear();
        return std::partial_ordering::unordered;
      }
      return greater == 1 ? std::partial_ordering::greater
                          : std::partial_ordering::equivalent;
    } catch (...) {
      PyErr_Clear();
      return std::partial_ordering::unordered;
    }
  }

  static std::string to_string(const void *, const void *memory) {
    const auto &stored = value(memory);
    if (stored.object == nullptr) {
      return "<empty Python-retained value>";
    }
    nb::gil_scoped_acquire gil;
    nb::object text = nb::steal(PyObject_Str(stored.object));
    if (!text.is_valid()) {
      throw nb::python_error();
    }
    return nb::cast<std::string>(text);
  }

  static nb::object to_python(const void *, const void *memory) {
    return value(memory).get();
  }

  static void from_python(const void *context, const ValueTypeRef &,
                          void *memory, nb::handle source) {
    const auto &self = entry(context);
    value(memory).set(prepare_retained_python_value(self.schema, source));
  }

  static bool accepts_source(const void *context, ValueTypeRef binding,
                             ValueTypeRef source) noexcept {
    const auto &self = entry(context);
    return binding == self.binding && source && source.schema() == self.schema;
  }

  static void copy_assign_from(const void *context, ValueTypeRef, void *dst,
                               ValueTypeRef source, const void *src) {
    const auto &self = entry(context);
    if (source == self.binding) {
      value(dst) = value(src);
      return;
    }
    nb::gil_scoped_acquire gil;
    value(dst).set(prepare_retained_python_value(
        self.schema, source.ops_ref().to_python(src)));
  }

  static void move_assign_from(const void *context, ValueTypeRef binding,
                               void *dst, ValueTypeRef source, void *src) {
    const auto &self = entry(context);
    if (source == self.binding) {
      value(dst) = std::move(value(src));
      return;
    }
    copy_assign_from(context, binding, dst, source, src);
  }
};

// Entries are address-published AND immortal: python Values can outlive a
// registry reset, so a reset drops only the key index (clear_index) while
// every entry stays alive. The table itself is leaked for the same reason.
[[nodiscard]] InternTable<const ValueTypeMetaData *,
                          std::unique_ptr<PythonRetainedBindingEntry>> &
python_retained_bindings() noexcept {
  static auto *bindings =
      new InternTable<const ValueTypeMetaData *,
                      std::unique_ptr<PythonRetainedBindingEntry>>{};
  return *bindings;
}

[[nodiscard]] ValueTypeRef
python_retained_binding_for(const ValueTypeMetaData *schema) {
  // intern_serialized: the entry ctor interns a TypeRecord pointing at
  // itself, so a racing loser must never be constructed and destroyed.
  return python_retained_bindings()
      .intern_serialized(
          schema,
          [&] { return std::make_unique<PythonRetainedBindingEntry>(schema); })
      ->binding;
}

void clear_python_retained_bindings() noexcept {
  python_retained_bindings().clear_index();
}
#endif
} // namespace

ValuePlanFactory &ValuePlanFactory::instance() {
  // Immortal (see OperatorRegistry::instance): interned plans/bindings
  // must outlive every Value destructor, including static-teardown ones.
  static ValuePlanFactory *factory = new ValuePlanFactory();
  return *factory;
}

void ValuePlanFactory::register_atomic(const ValueTypeMetaData *schema,
                                       const MemoryUtils::StoragePlan *plan,
                                       const ValueOps *ops) {
  const DebugDescriptor *existing =
      schema != nullptr && plan != nullptr
          ? find_value_debug_descriptor(*schema, *plan)
          : nullptr;
  register_atomic(schema, plan, ops,
                  existing != nullptr ? existing->atomic_kind
                                      : DebugAtomicKind::Opaque);
}

void ValuePlanFactory::register_atomic(const ValueTypeMetaData *schema,
                                       const MemoryUtils::StoragePlan *plan,
                                       const ValueOps *ops,
                                       DebugAtomicKind atomic_kind) {
  if (schema == nullptr || plan == nullptr || ops == nullptr) {
    return;
  }
  const auto &debug =
      intern_atomic_debug_descriptor(*schema, *plan, atomic_kind);
  const ValueTypeRef type = intern_value_type(*schema, *plan, *ops, &debug);

  std::lock_guard lock(mutex_);
  if (const auto it = cache_.find(schema); it != cache_.end()) {
    if (it->second == plan) {
      const auto type_it = type_cache_.find(schema);
      if (type_it != type_cache_.end() && type_it->second == type)
        return;
      throw std::logic_error("ValuePlanFactory: atomic schema already "
                             "registered with a different type");
    }
    throw std::logic_error(
        std::string{"ValuePlanFactory: atomic schema already registered with a "
                    "different plan: "} +
        (schema->header.label ? schema->header.label : "?"));
  }
  cache_.emplace(schema, plan);
  type_cache_.emplace(schema, type);
}

void ValuePlanFactory::register_type(ValueTypeRef type) {
  if (!type.valid()) {
    return;
  }
  const auto *schema = type.schema();
  if (schema != nullptr && schema->try_value_kind() == ValueTypeKind::Bundle) {
    const auto *indexed = try_value_ops<IndexedValueOps>(type);
    if (indexed == nullptr || indexed->size == nullptr ||
        indexed->element_at == nullptr || indexed->element_binding == nullptr ||
        indexed->make_range == nullptr) {
      throw std::invalid_argument(
          "ValuePlanFactory: Bundle binding must expose complete read-only "
          "IndexedValueOps");
    }
    if (indexed->size(indexed->context, nullptr) != schema->field_count) {
      throw std::invalid_argument(
          "ValuePlanFactory: Bundle binding field count does not match its "
          "schema");
    }
    for (std::size_t index = 0; index < schema->field_count; ++index) {
      const auto field =
          indexed->element_binding(indexed->context, nullptr, index);
      if (!field || field.schema() != schema->fields[index].type) {
        throw std::invalid_argument(
            "ValuePlanFactory: Bundle binding field schema does not match "
            "its declared schema");
      }
    }
    if (!type.checked_plan().is_composite() &&
        (type.ops_ref().accepts_source_impl == nullptr ||
         (type.ops_ref().copy_assign_from_impl == nullptr &&
          type.ops_ref().move_assign_from_impl == nullptr))) {
      throw std::invalid_argument(
          "ValuePlanFactory: non-composite Bundle binding must support "
          "erased assignment");
    }
  }

  std::lock_guard lock(mutex_);
  if (const auto it = type_cache_.find(schema); it != type_cache_.end()) {
    if (it->second == type) {
      return;
    }
    throw std::logic_error(
        "ValuePlanFactory: schema already registered with a different type");
  }

  if (const auto it = cache_.find(schema); it != cache_.end()) {
    if (it->second != type.plan()) {
      throw std::logic_error(
          "ValuePlanFactory: schema already registered with a different plan");
    }
  } else {
    cache_.emplace(schema, type.plan());
  }

  type_cache_.emplace(schema, type);
}

const MemoryUtils::StoragePlan *
ValuePlanFactory::plan_for(const ValueTypeMetaData *schema) {
  if (schema == nullptr) {
    return nullptr;
  }

  {
    std::lock_guard lock(mutex_);
    if (const auto it = cache_.find(schema); it != cache_.end()) {
      return it->second;
    }
  }

  return synthesise(schema);
}

const MemoryUtils::StoragePlan *
ValuePlanFactory::find(const ValueTypeMetaData *schema) const {
  if (schema == nullptr) {
    return nullptr;
  }

  std::lock_guard lock(mutex_);
  const auto it = cache_.find(schema);
  return it == cache_.end() ? nullptr : it->second;
}

ValueTypeRef ValuePlanFactory::type_for(const ValueTypeMetaData *schema) {
  if (schema == nullptr) {
    return {};
  }

  {
    std::lock_guard lock(mutex_);
    if (const auto it = type_cache_.find(schema); it != type_cache_.end()) {
      return it->second;
    }
  }

  return synthesise_type(schema);
}

ValueStorageSelection
ValuePlanFactory::storage_for(ValueTypeRef native_binding,
                              ValueStorageVariant requested) {
  if (!native_binding) {
    throw std::invalid_argument(
        "ValuePlanFactory::storage_for requires a native value binding");
  }
  ValueStorageSelection native{
      .effective = ValueStorageVariant::Native,
      .value_binding = native_binding,
      .storage_plan = native_binding.plan(),
  };
#if !HGRAPH_ENABLE_PYTHON_USER_NODES
  static_cast<void>(requested);
  return native;
#else
  if (requested == ValueStorageVariant::Native) {
    return native;
  }
  const auto *schema = native_binding.schema();
  if (requested == ValueStorageVariant::PythonOnly) {
    if (const auto python_binding =
            python_bundle_storage_binding(native_binding);
        python_binding) {
      return ValueStorageSelection{
          .effective = ValueStorageVariant::PythonOnly,
          .value_binding = python_binding,
          .storage_plan = python_binding.plan(),
      };
    }
    if (!python_cache_beneficial(native_binding) ||
        python_bridge::is_python_bundle_schema(schema)) {
      return native;
    }
    const auto retained = python_retained_binding_for(schema);
    return ValueStorageSelection{
        .effective = ValueStorageVariant::PythonOnly,
        .value_binding = retained,
        .storage_plan = retained.plan(),
        .value_offset = 0,
        .python_value_offset = 0,
    };
  }
  if (!python_cache_beneficial(native_binding)) {
    return native;
  }
  auto builder = MemoryUtils::named_tuple();
  builder.reserve(2);
  builder.add_field("native", native_binding.checked_plan());
  builder.add_field("python",
                    MemoryUtils::plan_for<python_bridge::PythonValueHolder>());
  const auto &plan = builder.build();
  return ValueStorageSelection{
      .effective = ValueStorageVariant::NativeWithPythonCache,
      .value_binding = native_binding,
      .storage_plan = &plan,
      .value_offset = plan.component("native").offset,
      .python_value_offset = plan.component("python").offset,
  };
#endif
}

#if HGRAPH_ENABLE_PYTHON_USER_NODES
nb::object
ValuePlanFactory::prepare_python_storage_value(const ValueTypeMetaData *schema,
                                               nb::handle source) const {
  if (!retained_python_supported(schema)) {
    throw std::logic_error(
        "ValuePlanFactory has no retained-Python policy for this schema");
  }
  return prepare_retained_python_value(schema, source);
}
#endif

ValueTypeRef
ValuePlanFactory::find_type(const ValueTypeMetaData *schema) const {
  if (schema == nullptr) {
    return {};
  }

  std::lock_guard lock(mutex_);
  const auto it = type_cache_.find(schema);
  return it == type_cache_.end() ? ValueTypeRef{} : it->second;
}

ValueTypeRef ValuePlanFactory::realized_composite_type_for(
    const ValueTypeMetaData *schema,
    std::span<const ValueTypeRef> field_bindings) {
  if (schema == nullptr ||
      (schema->value_kind() != ValueTypeKind::Tuple &&
       schema->value_kind() != ValueTypeKind::Bundle) ||
      schema->is_owned()) {
    throw std::invalid_argument("realized_composite_type_for requires a "
                                "non-Owned Tuple or Bundle schema");
  }
  if (field_bindings.size() != schema->field_count) {
    throw std::invalid_argument(
        "realized_composite_type_for field count does not match its schema");
  }
  std::vector<ValueTypeRef> fields{field_bindings.begin(),
                                   field_bindings.end()};
  for (std::size_t index = 0; index < fields.size(); ++index) {
    const auto *field_schema = fields[index] ? fields[index].schema() : nullptr;
    const auto *declared = schema->fields[index].type;
    const bool cycle_owner = field_schema != nullptr &&
                             field_schema->is_owned() &&
                             field_schema->element_type == declared;
    if (field_schema != declared && !cycle_owner) {
      throw std::invalid_argument(
          "realized_composite_type_for received an incompatible field binding");
    }
  }
  return realized_composite_cache().get(*schema, std::move(fields));
}

ValueTypeRef ValuePlanFactory::projected_composite_type_for(
    const ValueTypeMetaData *schema,
    std::span<const ValueTypeRef> field_bindings) {
  if (schema == nullptr || (schema->value_kind() != ValueTypeKind::Tuple &&
                            schema->value_kind() != ValueTypeKind::Bundle)) {
    throw std::invalid_argument(
        "projected_composite_type_for requires a Tuple or Bundle schema");
  }
  if (field_bindings.size() != schema->field_count) {
    throw std::invalid_argument(
        "projected_composite_type_for field count does not match its schema");
  }
  std::vector<ValueTypeRef> fields{field_bindings.begin(),
                                   field_bindings.end()};
  for (std::size_t index = 0; index < fields.size(); ++index) {
    const auto *field_schema = fields[index] ? fields[index].schema() : nullptr;
    const auto *declared = schema->fields[index].type;
    const bool cycle_owner = field_schema != nullptr &&
                             field_schema->is_owned() &&
                             field_schema->element_type == declared;
    if (field_schema != declared && !cycle_owner) {
      throw std::invalid_argument(
          "projected_composite_type_for received an incompatible field "
          "binding");
    }
  }
  return realized_composite_cache().get(*schema, std::move(fields));
}

void ValuePlanFactory::reset() noexcept {
  std::lock_guard lock(mutex_);
  cache_.clear();
  type_cache_.clear();
  clear_structured_indexed_ops();
  clear_value_debug_descriptors();
#if HGRAPH_ENABLE_PYTHON_USER_NODES
  clear_python_retained_bindings();
#endif
}

const MemoryUtils::StoragePlan *
ValuePlanFactory::synthesise(const ValueTypeMetaData *schema) {
  if (schema->is_owned()) {
    const auto *plan = &owned_value_entry(*schema).plan;
    std::lock_guard lock(mutex_);
    const auto [it, _] = cache_.emplace(schema, plan);
    return it->second;
  }

  const MemoryUtils::StoragePlan *plan = nullptr;

  switch (schema->value_kind()) {
  case ValueTypeKind::Atomic:
    throw std::logic_error(
        "ValuePlanFactory: atomic schema has no canonical plan; register it "
        "via register_atomic "
        "(typically through TypeRegistry::register_scalar<T>)");

  case ValueTypeKind::Tuple: {
    auto builder = MemoryUtils::tuple();
    const std::size_t validity_words =
        bundle_validity_word_count(schema->field_count);
    builder.reserve(schema->field_count + (validity_words == 0 ? 0 : 1));
    for (size_t index = 0; index < schema->field_count; ++index) {
      const ValueTypeMetaData *field_type = schema->fields[index].type;
      const MemoryUtils::StoragePlan *field_plan = plan_for(field_type);
      if (field_plan == nullptr) {
        throw std::logic_error(
            "ValuePlanFactory: tuple field has no resolvable plan");
      }
      builder.add_plan(*field_plan);
    }
    if (validity_words != 0) {
      // Fixed tuples carry the same hidden field-validity words
      // as bundles so a partial tuple (relaxed combine) reads
      // its unset slots as None. Appended last: field i stays
      // component i.
      builder.add_hidden_plan(bundle_validity_plan(schema->field_count));
    }
    plan = &builder.build();
    break;
  }

  case ValueTypeKind::Bundle: {
    auto builder = MemoryUtils::named_tuple();
    const std::size_t validity_words =
        bundle_validity_word_count(schema->field_count);
    builder.reserve(schema->field_count + (validity_words == 0 ? 0 : 1));
    for (size_t index = 0; index < schema->field_count; ++index) {
      const ValueFieldMetaData &field = schema->fields[index];
      const MemoryUtils::StoragePlan *field_plan = plan_for(field.type);
      if (field_plan == nullptr) {
        throw std::logic_error(
            "ValuePlanFactory: bundle field has no resolvable plan");
      }
      builder.add_field(field.name != nullptr ? field.name : "", *field_plan);
    }
    if (validity_words != 0) {
      // Hidden trailing validity words (design record: Bundle
      // field validity, core_concepts.rst): one bit per field,
      // default-zero. Appended LAST so field i remains
      // component i; the component is intentionally unnamed.
      builder.add_hidden_plan(bundle_validity_plan(schema->field_count));
    }
    plan = &builder.build();
    break;
  }

  case ValueTypeKind::List: {
    if (schema->fixed_size == 0) {
      plan = type_for(schema).plan();
      break;
    }
    const MemoryUtils::StoragePlan *element_plan =
        plan_for(schema->element_type);
    if (element_plan == nullptr) {
      throw std::logic_error(
          "ValuePlanFactory: fixed list has no element plan");
    }
    const auto &elements =
        MemoryUtils::array_plan(*element_plan, schema->fixed_size);
    if (schema->is_shaped_array()) {
      auto builder = MemoryUtils::tuple();
      builder.reserve(2);
      builder.add_plan(elements);
      builder.add_type<std::size_t>();
      plan = &builder.build();
    } else {
      plan = &elements;
    }
    break;
  }

  case ValueTypeKind::Set:
  case ValueTypeKind::Map:
  case ValueTypeKind::CyclicBuffer:
  case ValueTypeKind::Queue:
  case ValueTypeKind::Any: {
    plan = type_for(schema).plan();
    break;
  }
  }

  if (plan == nullptr) {
    throw std::logic_error("ValuePlanFactory: unhandled ValueTypeKind");
  }

  std::lock_guard lock(mutex_);
  const auto it = cache_.find(schema);
  if (it != cache_.end()) {
    return it->second;
  }
  cache_.emplace(schema, plan);
  return plan;
}

ValueTypeRef
ValuePlanFactory::synthesise_type(const ValueTypeMetaData *schema) {
  if (schema->is_owned()) {
    const auto type = owned_value_entry(*schema).binding;
    std::lock_guard lock(mutex_);
    const auto [it, _] = type_cache_.emplace(schema, type);
    cache_.try_emplace(schema, type.plan());
    return it->second;
  }

  ValueTypeRef type{};

  switch (schema->value_kind()) {
  case ValueTypeKind::Atomic:
    throw std::logic_error(
        "ValuePlanFactory: atomic schema has no canonical binding; register it "
        "via TypeRegistry::register_scalar<T>");

  case ValueTypeKind::Tuple:
  case ValueTypeKind::Bundle: {
    for (size_t index = 0; index < schema->field_count; ++index) {
      if (!type_for(schema->fields[index].type)) {
        throw std::logic_error(
            "ValuePlanFactory: composite field has no resolvable binding");
      }
    }
    const MemoryUtils::StoragePlan *plan = plan_for(schema);
    if (plan == nullptr) {
      throw std::logic_error(
          "ValuePlanFactory: composite has no resolvable plan");
    }
    const auto &debug = intern_fixed_composite_debug_descriptor(*schema, *plan);
    type = intern_value_type(*schema, *plan,
                             composite_indexed_ops(*schema, *plan), &debug);
    break;
  }

  case ValueTypeKind::List: {
    const ValueTypeRef element_type = type_for(schema->element_type);
    if (!element_type) {
      throw std::logic_error(
          "ValuePlanFactory: list element has no resolvable binding");
    }
    if (schema->fixed_size == 0) {
      type = schema->is_mutable() ? mutable_list_type(element_type)
                                  : compact_list_type(element_type, *schema);
    } else {
      const MemoryUtils::StoragePlan *plan = plan_for(schema);
      if (plan == nullptr) {
        throw std::logic_error(
            "ValuePlanFactory: fixed list has no resolvable plan");
      }
      if (schema->is_nullable()) {
        type = intern_value_type(*schema, *plan,
                                 array_indexed_ops(*schema, *plan));
        break;
      }
      const auto *elements_plan = plan;
      std::size_t size_offset = 0;
      std::size_t data_offset = 0;
      DebugDynamicFlags flags = DebugDynamicFlags::SizeIsConstant;
      if (schema->is_shaped_array()) {
        data_offset = plan->component(0).offset;
        size_offset = plan->component(1).offset;
        elements_plan = plan->component(0).plan;
        flags = DebugDynamicFlags::None;
      }
      const auto &debug = intern_dynamic_debug_descriptor(
          schema->header, *plan, DebugLayoutKind::Sequence, nullptr,
          element_type.record(),
          DebugDynamicLayout{
              .magic = DEBUG_DYNAMIC_LAYOUT_MAGIC,
              .abi_version = DEBUG_DYNAMIC_LAYOUT_ABI_VERSION,
              .kind = DebugDynamicKind::Contiguous,
              .flags = flags,
              .size_offset = size_offset,
              .size_constant = schema->fixed_size,
              .data_offset = data_offset,
              .stride = elements_plan->array_stride(),
          });
      type = intern_value_type(*schema, *plan,
                               array_indexed_ops(*schema, *plan), &debug);
    }
    break;
  }

  case ValueTypeKind::Set: {
    const ValueTypeRef element_type = type_for(schema->element_type);
    if (!element_type) {
      throw std::logic_error(
          "ValuePlanFactory: set element has no resolvable binding");
    }
    type = schema->is_mutable() ? mutable_set_type(element_type)
                                : compact_set_type(element_type);
    break;
  }

  case ValueTypeKind::Map: {
    const ValueTypeRef key_type = type_for(schema->key_type);
    const ValueTypeRef value_type = type_for(schema->element_type);
    if (!key_type || !value_type) {
      throw std::logic_error(
          "ValuePlanFactory: map key/value has no resolvable binding");
    }
    type = schema->is_mutable() ? mutable_map_type(key_type, value_type)
                                : compact_map_type(key_type, value_type);
    break;
  }

  case ValueTypeKind::CyclicBuffer: {
    const ValueTypeRef element_type = type_for(schema->element_type);
    if (!element_type) {
      throw std::logic_error(
          "ValuePlanFactory: cyclic buffer element has no resolvable binding");
    }
    type = compact_cyclic_buffer_type(element_type, schema->fixed_size);
    break;
  }

  case ValueTypeKind::Queue: {
    const ValueTypeRef element_type = type_for(schema->element_type);
    if (!element_type) {
      throw std::logic_error(
          "ValuePlanFactory: queue element has no resolvable binding");
    }
    type = compact_queue_type(element_type, schema->fixed_size);
    break;
  }

  case ValueTypeKind::Any:
    type = any_type(*schema); // JSON keeps its identity
    break;
  }

  if (!type) {
    throw std::logic_error(
        "ValuePlanFactory: unhandled ValueTypeKind while synthesising binding");
  }

  std::lock_guard lock(mutex_);
  const auto it = type_cache_.find(schema);
  if (it != type_cache_.end()) {
    return it->second;
  }

  if (const auto plan_it = cache_.find(schema); plan_it != cache_.end()) {
    if (plan_it->second != type.plan()) {
      throw std::logic_error(
          "ValuePlanFactory: synthesised binding does not match cached plan");
    }
  } else {
    cache_.emplace(schema, type.plan());
  }

  type_cache_.emplace(schema, type);
  return type;
}
} // namespace hgraph
