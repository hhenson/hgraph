#include <hgraph/types/metadata/type_realization.h>

#include <hgraph/config.h>
#if HGRAPH_ENABLE_PYTHON_USER_NODES
#include <hgraph/python/bridge_state.h>
#endif

#include <hgraph/runtime/global_state.h>
#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/metadata/value_plan_factory.h>
#include <hgraph/types/primitive_types.h>
#include <hgraph/types/value/compact_container_ops.h>
#include <hgraph/types/value/container_ops.h>
#include <hgraph/types/value/mutable_container_ops.h>
#include <hgraph/types/value/polymorphic_value_type.h>
#include <hgraph/types/value/value.h>
#include <hgraph/types/value/value_range.h>
#include <hgraph/util/scope.h>

#include "../value/impl/pooled_polymorphic_value_type.h"

#include <algorithm>
#include <compare>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <mutex>

#include <hgraph/types/utils/counted_mutex.h>
#include <hgraph/types/value/compound_scalar_storage.h>
#include <new>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <utility>

namespace hgraph {
namespace {
thread_local const TypeRealizationSnapshot *active_snapshot = nullptr;
thread_local bool graph_value_realization = false;
inline constexpr std::string_view type_realization_options_key{
    "__hgraph.type_realization.options.pooled_polymorphic_compounds__"};

[[nodiscard]] constexpr std::size_t align_up(std::size_t value,
                                             std::size_t alignment) noexcept {
  return (value + alignment - 1U) & ~(alignment - 1U);
}

} // namespace

struct TypeRealizationSnapshot::Impl {
  struct UnionEntry {
    const ValueTypeMetaData *declared{nullptr};
    std::vector<ValueTypeRef> alternatives{};
    std::unordered_map<const TypeRecord *, ValueTypeRef>
        alternatives_by_record{};
    std::unordered_map<const ValueTypeMetaData *, ValueTypeRef>
        alternatives_by_schema{};
    ValueTypeRef default_type{};
    std::size_t payload_offset{0};
    MemoryUtils::StoragePlan plan{};
    IndexedValueOps ops{};
    ValueTypeRef binding{};

    UnionEntry(const ValueTypeMetaData *schema,
               std::vector<ValueTypeRef> realized_alternatives)
        : declared(schema), alternatives(std::move(realized_alternatives)) {
      std::size_t payload_size = 0;
      std::size_t payload_alignment = 1;
      alternatives_by_record.reserve(alternatives.size());
      alternatives_by_schema.reserve(alternatives.size());
      for (const auto alternative : alternatives) {
        if (!alternative) {
          throw std::logic_error(
              "closed Bundle alternative has no value binding");
        }
        alternatives_by_record.emplace(alternative.record(), alternative);
        alternatives_by_schema.emplace(alternative.schema(), alternative);
        payload_size =
            std::max(payload_size, alternative.checked_plan().layout.size);
        payload_alignment = std::max(
            payload_alignment, alternative.checked_plan().layout.alignment);
      }
      if (alternatives.empty()) {
        throw std::logic_error("abstract Bundle has no concrete alternative in "
                               "this graph snapshot");
      }
      default_type = alternatives.front();
      payload_offset = align_up(sizeof(const TypeRecord *), payload_alignment);
      const auto overall_alignment =
          std::max(alignof(const TypeRecord *), payload_alignment);

      plan.layout = MemoryUtils::StorageLayout{
          .size = align_up(payload_offset + payload_size, overall_alignment),
          .alignment = overall_alignment,
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
      ops.size = &indexed_size;
      ops.element_at = &element_at;
      ops.element_binding = &element_binding;
      ops.make_range = &make_range;
      ops.make_mutable_range = &make_mutable_range;
      ops.mutable_element_at = &mutable_element_at;

      binding = intern_value_type(*declared, plan, ops);
    }

    [[nodiscard]] static const UnionEntry &entry(const void *context) noexcept {
      return *static_cast<const UnionEntry *>(context);
    }

    [[nodiscard]] static const TypeRecord *
    active_record(const void *memory) noexcept {
      return memory == nullptr
                 ? nullptr
                 : *static_cast<const TypeRecord *const *>(memory);
    }

    static void set_active_record(void *memory,
                                  const TypeRecord *record) noexcept {
      *static_cast<const TypeRecord **>(memory) = record;
    }

    [[nodiscard]] static const void *payload(const UnionEntry &self,
                                             const void *memory) noexcept {
      return static_cast<const std::byte *>(memory) + self.payload_offset;
    }

    [[nodiscard]] static void *payload(const UnionEntry &self,
                                       void *memory) noexcept {
      return static_cast<std::byte *>(memory) + self.payload_offset;
    }

    [[nodiscard]] ValueTypeRef active_type(const void *memory) const noexcept {
      const auto *record = active_record(memory);
      if (record == nullptr) {
        return {};
      }
      const auto found = alternatives_by_record.find(record);
      return found != alternatives_by_record.end() ? found->second
                                                   : ValueTypeRef{};
    }

    [[nodiscard]] bool contains(ValueTypeRef source) const noexcept {
      if (!source) {
        return false;
      }
      const auto found = alternatives_by_record.find(source.record());
      return found != alternatives_by_record.end() && found->second == source;
    }

    [[nodiscard]] ValueTypeRef
    alternative_for_schema(const ValueTypeMetaData *schema) const noexcept {
      const auto found = alternatives_by_schema.find(schema);
      return found != alternatives_by_schema.end() ? found->second
                                                   : ValueTypeRef{};
    }

    void replace_copy_from(void *dst, ValueTypeRef target_type,
                           ValueTypeRef source_type,
                           const void *source_memory) const {
      const auto current = active_type(dst);
      if (current != target_type) {
        if (current) {
          current.destroy_at(payload(*this, dst));
        }
        set_active_record(dst, nullptr);
        target_type.default_construct_at(payload(*this, dst));
        set_active_record(dst, target_type.record());
      }
      target_type.ops_ref().copy_assign_from(target_type, payload(*this, dst),
                                             source_type, source_memory);
    }

    void replace_move_from(void *dst, ValueTypeRef target_type,
                           ValueTypeRef source_type,
                           void *source_memory) const {
      const auto current = active_type(dst);
      if (current != target_type) {
        if (current) {
          current.destroy_at(payload(*this, dst));
        }
        set_active_record(dst, nullptr);
        target_type.default_construct_at(payload(*this, dst));
        set_active_record(dst, target_type.record());
      }
      target_type.ops_ref().move_assign_from(target_type, payload(*this, dst),
                                             source_type, source_memory);
    }

    static void default_construct(void *memory, const void *context) {
      const auto &self = entry(context);
      set_active_record(memory, nullptr);
      self.default_type.default_construct_at(payload(self, memory));
      set_active_record(memory, self.default_type.record());
    }

    static void destroy(void *memory, const void *context) noexcept {
      const auto &self = entry(context);
      if (const auto active = self.active_type(memory)) {
        active.destroy_at(payload(self, memory));
      }
      set_active_record(memory, nullptr);
    }

    static void copy_construct(void *dst, const void *src,
                               const void *context) {
      const auto &self = entry(context);
      const auto active = self.active_type(src);
      if (!active) {
        throw std::logic_error("closed Bundle '" +
                               std::string{self.declared->name()} +
                               "' copy source has an invalid active type");
      }
      set_active_record(dst, nullptr);
      active.copy_construct_at(payload(self, dst), payload(self, src));
      set_active_record(dst, active.record());
    }

    static void move_construct(void *dst, void *src, const void *context) {
      const auto &self = entry(context);
      const auto active = self.active_type(src);
      if (!active) {
        throw std::logic_error("closed Bundle '" +
                               std::string{self.declared->name()} +
                               "' move source has an invalid active type");
      }
      set_active_record(dst, nullptr);
      active.move_construct_at(payload(self, dst), payload(self, src));
      set_active_record(dst, active.record());
    }

    void replace_copy(void *dst, ValueTypeRef source_type,
                      const void *source_memory) const {
      const auto current = active_type(dst);
      if (current == source_type) {
        current.copy_assign_at(payload(*this, dst), source_memory);
        return;
      }
      if (current) {
        current.destroy_at(payload(*this, dst));
      }
      set_active_record(dst, nullptr);
      auto restore = make_scope_exit([&]() noexcept {
        if (active_record(dst) == nullptr) {
          default_type.default_construct_at(payload(*this, dst));
          set_active_record(dst, default_type.record());
        }
      });
      source_type.copy_construct_at(payload(*this, dst), source_memory);
      set_active_record(dst, source_type.record());
      restore.release();
    }

    void replace_move(void *dst, ValueTypeRef source_type,
                      void *source_memory) const {
      const auto current = active_type(dst);
      if (current == source_type) {
        current.move_assign_at(payload(*this, dst), source_memory);
        return;
      }
      if (current) {
        current.destroy_at(payload(*this, dst));
      }
      set_active_record(dst, nullptr);
      auto restore = make_scope_exit([&]() noexcept {
        if (active_record(dst) == nullptr) {
          default_type.default_construct_at(payload(*this, dst));
          set_active_record(dst, default_type.record());
        }
      });
      source_type.move_construct_at(payload(*this, dst), source_memory);
      set_active_record(dst, source_type.record());
      restore.release();
    }

    static void copy_assign(void *dst, const void *src, const void *context) {
      const auto &self = entry(context);
      const auto source_type = self.active_type(src);
      if (!source_type) {
        throw std::logic_error(
            "closed Bundle '" + std::string{self.declared->name()} +
            "' copy-assignment source has an invalid active type");
      }
      self.replace_copy(dst, source_type, payload(self, src));
    }

    static void move_assign(void *dst, void *src, const void *context) {
      const auto &self = entry(context);
      const auto source_type = self.active_type(src);
      if (!source_type) {
        throw std::logic_error(
            "closed Bundle '" + std::string{self.declared->name()} +
            "' move-assignment source has an invalid active type");
      }
      self.replace_move(dst, source_type, payload(self, src));
    }

    static bool accepts_source(const void *context, ValueTypeRef binding,
                               ValueTypeRef source) noexcept {
      const auto &self = entry(context);
      return binding == self.binding && source &&
             (source == self.binding || self.contains(source) ||
              self.alternative_for_schema(source.schema()) ||
              source.schema() == self.declared);
    }

    static void copy_assign_from(const void *context, ValueTypeRef, void *dst,
                                 ValueTypeRef source, const void *src) {
      const auto &self = entry(context);
      if (source == self.binding) {
        copy_assign(dst, src, context);
        return;
      }
      if (!self.contains(source) && source.schema() == self.declared) {
        const auto source_type = source.ops_ref().concrete_type(source, src);
        const auto *source_memory = source.ops_ref().concrete_memory(src);
        const auto target_type =
            self.alternative_for_schema(source_type.schema());
        if (!target_type) {
          throw std::invalid_argument(
              "closed Bundle source alternative '" +
              std::string{source_type.schema() != nullptr
                              ? source_type.schema()->name()
                              : "<invalid>"} +
              "' is outside this graph snapshot");
        }
        self.replace_copy_from(dst, target_type, source_type, source_memory);
        return;
      }
      if (const auto alternative = self.alternative_for_schema(source.schema());
          alternative && alternative != source) {
        self.replace_copy_from(dst, alternative, source, src);
        return;
      }
      self.replace_copy(dst, source, src);
    }

    static void move_assign_from(const void *context, ValueTypeRef, void *dst,
                                 ValueTypeRef source, void *src) {
      const auto &self = entry(context);
      if (source == self.binding) {
        move_assign(dst, src, context);
        return;
      }
      if (!self.contains(source) && source.schema() == self.declared) {
        const auto source_type = source.ops_ref().concrete_type(source, src);
        auto *source_memory = source.ops_ref().writable_concrete_memory(src);
        const auto target_type =
            self.alternative_for_schema(source_type.schema());
        if (!target_type || source_memory == nullptr) {
          throw std::invalid_argument(
              "closed Bundle source alternative '" +
              std::string{source_type.schema() != nullptr
                              ? source_type.schema()->name()
                              : "<invalid>"} +
              "' is outside this graph snapshot");
        }
        self.replace_move_from(dst, target_type, source_type, source_memory);
        return;
      }
      if (const auto alternative = self.alternative_for_schema(source.schema());
          alternative && alternative != source) {
        self.replace_move_from(dst, alternative, source, src);
        return;
      }
      self.replace_move(dst, source, src);
    }

    static ValueTypeRef concrete_type(const void *context, ValueTypeRef,
                                      const void *memory) noexcept {
      return entry(context).active_type(memory);
    }

    static const void *concrete_memory(const void *context,
                                       const void *memory) noexcept {
      return payload(entry(context), memory);
    }

    static void *mutable_concrete_memory(const void *context,
                                         void *memory) noexcept {
      return payload(entry(context), memory);
    }

    [[nodiscard]] static DynamicStorageMetrics
    dynamic_storage_metrics(const void *context, const void *memory) noexcept {
      const auto &self = entry(context);
      const auto active = self.active_type(memory);
      return active ? active.ops_ref().dynamic_storage_metrics(
                          payload(self, memory))
                    : DynamicStorageMetrics{};
    }

    [[nodiscard]] static const IndexedValueOps &
    active_ops(const UnionEntry &self, const void *memory) {
      const auto active = self.active_type(memory);
      if (!active) {
        throw std::logic_error("closed Bundle has an invalid active type");
      }
      return *checked_value_ops<IndexedValueOps>(active,
                                                 "closed Bundle alternative");
    }

    [[nodiscard]] static const IndexedValueOps *
    try_active_ops(const UnionEntry &self, const void *memory) noexcept {
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
        throw std::logic_error("closed Bundle has an invalid active type");
      }
      // A closed-union value and its canonical concrete value are logically
      // equal even though their TypeRecords and physical layouts differ.
      // Hash only the concrete payload: concrete-type differences may collide,
      // but equal values must never hash differently because of realization.
      return active.ops_ref().hash(payload(self, memory));
    }

    static bool equals(const void *context, const void *lhs,
                       const void *rhs) noexcept {
      return fallback_on_exception(false, [&]() {
        const auto &self = entry(context);
        const auto lhs_type = self.active_type(lhs);
        const auto rhs_type = self.active_type(rhs);
        return lhs_type && lhs_type == rhs_type &&
               lhs_type.ops_ref().equals(payload(self, lhs),
                                         payload(self, rhs));
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
        return lhs_type.ops_ref().compare(payload(self, lhs),
                                          payload(self, rhs));
      });
    }

    static std::string to_string(const void *context, const void *memory) {
      const auto &self = entry(context);
      const auto active = self.active_type(memory);
      if (!active) {
        return "<invalid closed Bundle>";
      }
      return std::string{active.schema()->name()} +
             active.ops_ref().to_string(payload(self, memory));
    }

    static std::size_t indexed_size(const void *context,
                                    const void *memory) noexcept {
      const auto &self = entry(context);
      const auto *actual_ops = try_active_ops(self, memory);
      return actual_ops != nullptr && actual_ops->size != nullptr
                 ? actual_ops->size(actual_ops->context,
                                    memory != nullptr ? payload(self, memory)
                                                      : nullptr)
                 : 0;
    }

    static const void *element_at(const void *context, const void *memory,
                                  std::size_t index) {
      const auto &self = entry(context);
      const auto &actual_ops = active_ops(self, memory);
      return actual_ops.element_at(actual_ops.context, payload(self, memory),
                                   index);
    }

    static ValueTypeRef element_binding(const void *context, const void *memory,
                                        std::size_t index) noexcept {
      const auto &self = entry(context);
      const auto *actual_ops = try_active_ops(self, memory);
      return actual_ops != nullptr && actual_ops->element_binding != nullptr
                 ? actual_ops->element_binding(
                       actual_ops->context,
                       memory != nullptr ? payload(self, memory) : nullptr,
                       index)
                 : ValueTypeRef{};
    }

    static void *mutable_element_at(const void *context, void *memory,
                                    std::size_t index) {
      const auto &self = entry(context);
      const auto &actual_ops = active_ops(self, memory);
      if (actual_ops.mutable_element_at == nullptr) {
        return const_cast<void *>(actual_ops.element_at(
            actual_ops.context, payload(self, memory), index));
      }
      return actual_ops.mutable_element_at(actual_ops.context,
                                           payload(self, memory), index);
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

    static Range<ValueView> make_range(const void *context,
                                       const void *memory) {
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
        throw std::logic_error("closed Bundle has an invalid active type");
      }
      return active.ops_ref().to_python(payload(self, memory));
    }

    [[nodiscard]] ValueTypeRef python_source_type(nb::handle source) const {
      nb::object object = nb::borrow<nb::object>(source);
      const nb::object source_class = nb::getattr(object, "__class__");
      std::vector<ValueTypeRef> class_matches;
      std::size_t best_distance = std::numeric_limits<std::size_t>::max();
      for (const auto alternative : alternatives) {
        const auto found = python_bridge::bundle_class_info_registry().find(
            alternative.schema());
        if (found == python_bridge::bundle_class_info_registry().end() ||
            !found->second.type.is_valid() ||
            !nb::isinstance(object, found->second.type)) {
          continue;
        }
        const auto mro =
            nb::cast<nb::tuple>(nb::getattr(source_class, "__mro__"));
        std::size_t distance = std::numeric_limits<std::size_t>::max();
        for (std::size_t index = 0; index < mro.size(); ++index) {
          if (mro[index].is(found->second.type)) {
            distance = index;
            break;
          }
        }
        if (distance < best_distance) {
          best_distance = distance;
          class_matches.clear();
        }
        if (distance == best_distance) {
          class_matches.push_back(alternative);
        }
      }
      if (class_matches.size() == 1) {
        return class_matches.front();
      }
      if (!class_matches.empty() && nb::hasattr(object, "__orig_class__")) {
        const auto alias = nb::getattr(object, "__orig_class__");
        ValueTypeRef matched{};
        for (const auto candidate : class_matches) {
          const auto &info = python_bridge::bundle_class_info_registry().at(
              candidate.schema());
          if (!info.specialization.is_valid()) {
            continue;
          }
          const int equal = PyObject_RichCompareBool(
              alias.ptr(), info.specialization.ptr(), Py_EQ);
          if (equal < 0) {
            nb::raise_python_error();
          }
          if (equal == 0) {
            continue;
          }
          if (matched) {
            throw std::invalid_argument(
                "Python structured scalar specialization is ambiguous");
          }
          matched = candidate;
        }
        if (matched) {
          return matched;
        }
      }
      if (class_matches.size() > 1) {
        using InferValueFn = Value (*)(nb::handle);
        const auto infer = reinterpret_cast<InferValueFn>(
            python_bridge::py_infer_value_slot());
        if (infer == nullptr) {
          throw std::logic_error(
              "Python Bundle inference hook is not installed");
        }
        ValueTypeRef best{};
        std::size_t best_score = 0;
        bool ambiguous = false;
        for (const auto candidate : class_matches) {
          std::size_t score = 0;
          for (std::size_t index = 0; index < candidate.schema()->field_count;
               ++index) {
            const auto &field = candidate.schema()->fields[index];
            if (field.name == nullptr || !nb::hasattr(object, field.name)) {
              continue;
            }
            const auto field_value = nb::getattr(object, field.name);
            if (field_value.is_none() || field_value.ptr() == object.ptr()) {
              continue;
            }
            const Value inferred = infer(field_value);
            if (inferred.schema() == field.type) {
              score += 2;
            } else if (inferred.schema() != nullptr && field.type != nullptr &&
                       inferred.schema()->value_kind() ==
                           ValueTypeKind::Bundle &&
                       field.type->value_kind() == ValueTypeKind::Bundle &&
                       TypeRegistry::instance().bundle_is_a(inferred.schema(),
                                                            field.type)) {
              ++score;
            }
          }
          if (!best || score > best_score) {
            best = candidate;
            best_score = score;
            ambiguous = false;
          } else if (score == best_score) {
            ambiguous = true;
          }
        }
        if (!ambiguous) {
          return best;
        }
        throw std::invalid_argument(
            "Python structured scalar schema inference is ambiguous");
      }

      if (nb::isinstance<nb::dict>(object)) {
        const auto discriminator = declared->bundle_discriminator();
        nb::dict map = nb::cast<nb::dict>(object);
        nb::str key{std::string{discriminator}.c_str()};
        if (!map.contains(key)) {
          throw std::invalid_argument("polymorphic Bundle dictionaries require "
                                      "the configured type discriminator");
        }
        const auto requested = nb::cast<std::string>(map[key]);
        ValueTypeRef match{};
        for (const auto alternative : alternatives) {
          if (alternative.schema()->matches_bundle_discriminator(requested)) {
            if (match) {
              throw std::invalid_argument(
                  "polymorphic Bundle discriminator is ambiguous");
            }
            match = alternative;
          }
        }
        if (match) {
          return match;
        }
        throw std::invalid_argument(
            "polymorphic Bundle discriminator names no valid alternative");
      }
      const nb::object source_module = nb::getattr(source_class, "__module__");
      const nb::object source_qualname =
          nb::getattr(source_class, "__qualname__");
      const std::string source_name = nb::cast<std::string>(source_module) +
                                      "." +
                                      nb::cast<std::string>(source_qualname);
      throw std::invalid_argument("value of Python type '" + source_name +
                                  "' is not an instance of closed Bundle '" +
                                  std::string{declared->name()} + "'");
    }

    static void from_python(const void *context, const ValueTypeRef &,
                            void *memory, nb::handle source) {
      const auto &self = entry(context);
      const auto requested = self.python_source_type(source);
      const auto current = self.active_type(memory);
      if (current != requested) {
        if (current) {
          current.destroy_at(payload(self, memory));
        }
        set_active_record(memory, nullptr);
        auto restore = make_scope_exit([&]() noexcept {
          if (active_record(memory) == nullptr) {
            self.default_type.default_construct_at(payload(self, memory));
            set_active_record(memory, self.default_type.record());
          }
        });
        requested.default_construct_at(payload(self, memory));
        set_active_record(memory, requested.record());
        restore.release();
      }
      requested.ops_ref().from_python(requested, payload(self, memory), source);
    }
#endif
  };

  std::uint64_t generation{0};
  TypeRealizationOptions options{};
  std::vector<const ValueTypeMetaData *> registration_order{};
  std::unordered_map<const ValueTypeMetaData *,
                     std::vector<const ValueTypeMetaData *>>
      parents{};
  std::unordered_map<const ValueTypeMetaData *, bool> abstract_bundles{};
  std::unordered_map<const ValueTypeMetaData *, bool> polymorphic_bases{};
  mutable TypeSystemMutex mutex{};
  mutable std::unordered_map<const ValueTypeMetaData *,
                             std::vector<const ValueTypeMetaData *>>
      closures{};
  mutable std::unordered_map<const ValueTypeMetaData *, ValueTypeRef>
      exact_types{};
  mutable std::unordered_map<const ValueTypeMetaData *,
                             std::unique_ptr<UnionEntry>>
      union_types{};
  mutable std::unordered_map<const ValueTypeMetaData *, ValueTypeRef>
      graph_exact_types{};
  mutable std::unordered_map<const ValueTypeMetaData *, ValueTypeRef>
      graph_union_bindings{};
  mutable std::unordered_map<const ValueTypeMetaData *,
                             std::unique_ptr<UnionEntry>>
      graph_inline_union_types{};
  mutable std::unordered_map<const ValueTypeMetaData *, PolymorphicValueType>
      graph_pooled_union_types{};
  // Stable address handed to every pooled entry realized here; the root graph
  // that runs this realization binds its pool into it.
  std::unique_ptr<CompoundScalarStorageBinding> pool_binding{
      std::make_unique<CompoundScalarStorageBinding>()};
  enum class RealizationKind : std::uint8_t {
    ExternalExact,
    ExternalUnion,
    GraphExact,
    GraphUnion,
  };
  struct RealizationStep {
    const ValueTypeMetaData *schema{nullptr};
    RealizationKind kind{RealizationKind::ExternalExact};

    bool operator==(const RealizationStep &) const = default;
  };
  mutable std::vector<RealizationStep> realization_path{};

  explicit Impl(TypeRegistry &registry, TypeRealizationOptions requested_options)
      : options(requested_options) {
    const auto snapshot = registry.bundle_hierarchy_snapshot();
    generation = snapshot.generation;
    registration_order.reserve(snapshot.entries.size());
    parents.reserve(snapshot.entries.size());
    abstract_bundles.reserve(snapshot.entries.size());
    polymorphic_bases.reserve(snapshot.entries.size());
    for (const auto &entry : snapshot.entries) {
      registration_order.push_back(entry.schema);
      parents.emplace(entry.schema, entry.parents);
      abstract_bundles.emplace(entry.schema, entry.is_abstract);
      polymorphic_bases.emplace(entry.schema,
                                entry.is_abstract || entry.has_children);
    }
  }

  [[nodiscard]] bool derives_from(const ValueTypeMetaData *candidate,
                                  const ValueTypeMetaData *base) const {
    if (candidate == base) {
      return true;
    }
    std::vector<const ValueTypeMetaData *> pending{candidate};
    std::unordered_map<const ValueTypeMetaData *, bool> seen;
    while (!pending.empty()) {
      const auto *current = pending.back();
      pending.pop_back();
      if (!seen.emplace(current, true).second) {
        continue;
      }
      const auto found = parents.find(current);
      if (found == parents.end()) {
        continue;
      }
      for (const auto *parent : found->second) {
        if (parent == base) {
          return true;
        }
        pending.push_back(parent);
      }
    }
    return false;
  }

  [[nodiscard]] const std::vector<const ValueTypeMetaData *> &
  closure_for_locked(const ValueTypeMetaData *schema) const {
    if (const auto found = closures.find(schema); found != closures.end()) {
      return found->second;
    }
    std::vector<const ValueTypeMetaData *> closure;
    for (const auto *candidate : registration_order) {
      const auto abstract = abstract_bundles.find(candidate);
      if (abstract != abstract_bundles.end() && !abstract->second &&
          derives_from(candidate, schema)) {
        closure.push_back(candidate);
      }
    }
    return closures.emplace(schema, std::move(closure)).first->second;
  }

  [[nodiscard]] bool
  polymorphic(const ValueTypeMetaData *schema) const noexcept {
    const auto found = polymorphic_bases.find(schema);
    return found != polymorphic_bases.end() && found->second;
  }

  [[nodiscard]] auto enter_realization(const ValueTypeMetaData *schema,
                                       RealizationKind kind) const {
    const RealizationStep requested{schema, kind};
    if (const auto cycle = std::ranges::find(realization_path, requested);
        cycle != realization_path.end()) {
      std::string message{"recursive value schema requires an Owned edge: "};
      for (auto current = cycle; current != realization_path.end(); ++current) {
        if (current != cycle) {
          message.append(" -> ");
        }
        message.append(current->schema->name());
        message.append(current->kind == RealizationKind::ExternalUnion ||
                               current->kind == RealizationKind::GraphUnion
                           ? "[union]"
                           : "[exact]");
      }
      message.append(" -> ");
      message.append(schema->name());
      message.append(kind == RealizationKind::ExternalUnion ||
                             kind == RealizationKind::GraphUnion
                         ? "[union]"
                         : "[exact]");
      throw std::logic_error(message);
    }
    realization_path.push_back(requested);
    return make_scope_exit([this]() noexcept { realization_path.pop_back(); });
  }

  [[nodiscard]] ValueTypeRef
  exact_type_for_locked(const ValueTypeMetaData *schema) const {
    if (schema == nullptr) {
      return {};
    }
    if (const auto found = exact_types.find(schema);
        found != exact_types.end()) {
      return found->second;
    }
    auto realization =
        enter_realization(schema, RealizationKind::ExternalExact);

    auto &factory = ValuePlanFactory::instance();
    if (!schema->is_owned() && schema->value_kind() == ValueTypeKind::List) {
      const auto element = type_for_locked(schema->element_type);
      if (element != factory.type_for(schema->element_type)) {
        if (schema->fixed_size != 0) {
          throw std::logic_error("polymorphic Bundle elements are not "
                                 "supported in fixed List schemas");
        }
        const auto result = schema->is_mutable()
                                ? mutable_list_type(element)
                                : compact_list_type(element, *schema);
        exact_types.emplace(schema, result);
        return result;
      }
    }

    const auto canonical = factory.type_for(schema);
    if (schema->is_owned()) {
      exact_types.emplace(schema, canonical);
      return canonical;
    }

    ValueTypeRef result = canonical;
    switch (schema->value_kind()) {
    case ValueTypeKind::Tuple:
    case ValueTypeKind::Bundle: {
      std::vector<ValueTypeRef> fields;
      fields.reserve(schema->field_count);
      bool changed = false;
      for (std::size_t index = 0; index < schema->field_count; ++index) {
        const auto child = type_for_locked(schema->fields[index].type);
        fields.push_back(child);
        changed =
            changed || child != factory.type_for(schema->fields[index].type);
      }
      if (changed) {
#if HGRAPH_ENABLE_PYTHON_USER_NODES
        if (const auto python =
                python_bridge::python_bundle_binding_for(schema, fields)) {
          result = python;
        } else
#endif
        {
          result = factory.realized_composite_type_for(schema, fields);
        }
      }
      break;
    }
    case ValueTypeKind::List:
      break;
    case ValueTypeKind::Set: {
      const auto element = type_for_locked(schema->element_type);
      if (element != factory.type_for(schema->element_type)) {
        if (schema->is_mutable()) {
          throw std::logic_error("polymorphic Bundle elements are not "
                                 "supported in mutable Set schemas");
        }
        result = compact_set_type(element);
      }
      break;
    }
    case ValueTypeKind::Map: {
      const auto key = type_for_locked(schema->key_type);
      const auto value = type_for_locked(schema->element_type);
      if (key != factory.type_for(schema->key_type) ||
          value != factory.type_for(schema->element_type)) {
        if (schema->is_mutable()) {
          throw std::logic_error("polymorphic Bundle keys or values are not "
                                 "supported in mutable Map schemas");
        }
        result = compact_map_type(key, value);
      }
      break;
    }
    case ValueTypeKind::CyclicBuffer: {
      const auto element = type_for_locked(schema->element_type);
      if (element != factory.type_for(schema->element_type)) {
        result = compact_cyclic_buffer_type(element, schema->fixed_size);
      }
      break;
    }
    case ValueTypeKind::Queue: {
      const auto element = type_for_locked(schema->element_type);
      if (element != factory.type_for(schema->element_type)) {
        result = compact_queue_type(element, schema->fixed_size);
      }
      break;
    }
    case ValueTypeKind::Atomic:
    case ValueTypeKind::Any:
      break;
    }
    exact_types.emplace(schema, result);
    return result;
  }

  [[nodiscard]] ValueTypeRef
  type_for_locked(const ValueTypeMetaData *schema) const {
    if (!polymorphic(schema)) {
      return exact_type_for_locked(schema);
    }
    if (const auto found = union_types.find(schema);
        found != union_types.end()) {
      return found->second->binding;
    }
    const auto &alternative_schemas = closure_for_locked(schema);
    const bool closes_active_exact = std::ranges::any_of(
        realization_path, [&](const RealizationStep &step) {
          return step.kind == RealizationKind::ExternalExact &&
                 std::ranges::find(alternative_schemas, step.schema) !=
                     alternative_schemas.end();
        });
    if (closes_active_exact ||
        std::ranges::find(
            realization_path,
            RealizationStep{schema, RealizationKind::ExternalUnion}) !=
            realization_path.end()) {
      return ValuePlanFactory::instance().type_for(
          TypeRegistry::instance().owned(schema));
    }
    auto realization =
        enter_realization(schema, RealizationKind::ExternalUnion);
    std::vector<ValueTypeRef> alternatives;
    alternatives.reserve(alternative_schemas.size());
    for (const auto *alternative : alternative_schemas) {
      alternatives.push_back(exact_type_for_locked(alternative));
    }
    auto created =
        std::make_unique<UnionEntry>(schema, std::move(alternatives));
    const auto result = created->binding;
    union_types.emplace(schema, std::move(created));
    return result;
  }

  [[nodiscard]] ValueTypeRef
  graph_exact_type_for_locked(const ValueTypeMetaData *schema) const {
    if (schema == nullptr) {
      return {};
    }
    if (const auto found = graph_exact_types.find(schema);
        found != graph_exact_types.end()) {
      return found->second;
    }
    auto realization = enter_realization(schema, RealizationKind::GraphExact);

    auto &factory = ValuePlanFactory::instance();
    const auto canonical = factory.type_for(schema);
    const auto external = exact_type_for_locked(schema);
    if (schema->is_owned()) {
      graph_exact_types.emplace(schema, canonical);
      return canonical;
    }

    ValueTypeRef result = canonical;
    switch (schema->value_kind()) {
    case ValueTypeKind::Tuple:
    case ValueTypeKind::Bundle: {
      std::vector<ValueTypeRef> fields;
      fields.reserve(schema->field_count);
      bool changed = false;
      for (std::size_t index = 0; index < schema->field_count; ++index) {
        const auto child = graph_type_for_locked(schema->fields[index].type);
        fields.push_back(child);
        changed =
            changed || child != factory.type_for(schema->fields[index].type);
      }
      if (changed) {
#if HGRAPH_ENABLE_PYTHON_USER_NODES
        if (const auto python =
                python_bridge::python_bundle_binding_for(schema, fields)) {
          result = python;
        } else
#endif
        {
          result = factory.realized_composite_type_for(schema, fields);
        }
      }
      break;
    }
    case ValueTypeKind::List: {
      const auto element = graph_type_for_locked(schema->element_type);
      if (element != factory.type_for(schema->element_type)) {
        if (schema->fixed_size != 0) {
          throw std::logic_error("polymorphic Bundle elements are not "
                                 "supported in fixed List schemas");
        }
        result = schema->is_mutable() ? mutable_list_type(element)
                                      : compact_list_type(element, *schema);
      }
      break;
    }
    case ValueTypeKind::Set: {
      const auto element = graph_type_for_locked(schema->element_type);
      if (element != factory.type_for(schema->element_type)) {
        if (schema->is_mutable()) {
          throw std::logic_error("polymorphic Bundle elements are not "
                                 "supported in mutable Set schemas");
        }
        result = compact_set_type(element);
      }
      break;
    }
    case ValueTypeKind::Map: {
      const auto key = graph_type_for_locked(schema->key_type);
      const auto value = graph_type_for_locked(schema->element_type);
      if (key != factory.type_for(schema->key_type) ||
          value != factory.type_for(schema->element_type)) {
        if (schema->is_mutable()) {
          throw std::logic_error("polymorphic Bundle keys or values are not "
                                 "supported in mutable Map schemas");
        }
        result = compact_map_type(key, value);
      }
      break;
    }
    case ValueTypeKind::CyclicBuffer: {
      const auto element = graph_type_for_locked(schema->element_type);
      if (element != factory.type_for(schema->element_type)) {
        result = compact_cyclic_buffer_type(element, schema->fixed_size);
      }
      break;
    }
    case ValueTypeKind::Queue: {
      const auto element = graph_type_for_locked(schema->element_type);
      if (element != factory.type_for(schema->element_type)) {
        result = compact_queue_type(element, schema->fixed_size);
      }
      break;
    }
    case ValueTypeKind::Atomic:
    case ValueTypeKind::Any:
      break;
    }
    graph_exact_types.emplace(schema, result);
    if (result != external) {
      register_value_owning_type(result, external);
    }
    return result;
  }

  [[nodiscard]] ValueTypeRef
  graph_type_for_locked(const ValueTypeMetaData *schema) const {
    if (!polymorphic(schema)) {
      return graph_exact_type_for_locked(schema);
    }
    if (const auto found = graph_union_bindings.find(schema);
        found != graph_union_bindings.end()) {
      return found->second;
    }
    const auto &alternative_schemas = closure_for_locked(schema);
    const bool closes_active_exact = std::ranges::any_of(
        realization_path, [&](const RealizationStep &step) {
          return step.kind == RealizationKind::GraphExact &&
                 std::ranges::find(alternative_schemas, step.schema) !=
                     alternative_schemas.end();
        });
    if (closes_active_exact ||
        std::ranges::find(
            realization_path,
            RealizationStep{schema, RealizationKind::GraphUnion}) !=
            realization_path.end()) {
      return ValuePlanFactory::instance().type_for(
          TypeRegistry::instance().owned(schema));
    }
    auto realization = enter_realization(schema, RealizationKind::GraphUnion);
    std::vector<ValueTypeRef> alternatives;
    alternatives.reserve(alternative_schemas.size());
    std::size_t minimum_size = std::numeric_limits<std::size_t>::max();
    std::size_t maximum_size = 0;
    for (const auto *alternative : alternative_schemas) {
      const auto realized = graph_exact_type_for_locked(alternative);
      alternatives.push_back(realized);
      minimum_size =
          std::min(minimum_size, realized.checked_plan().layout.size);
      maximum_size =
          std::max(maximum_size, realized.checked_plan().layout.size);
    }

    constexpr std::size_t pooling_size_spread_threshold = 32;
    const bool use_pool =
        options.polymorphic_compound_storage ==
            PolymorphicCompoundStoragePolicy::Pooled &&
        !alternatives.empty() &&
        maximum_size - minimum_size > pooling_size_spread_threshold;
    const auto external_binding = type_for_locked(schema);
    const auto external_found = union_types.find(schema);
    if (external_found == union_types.end()) {
      throw std::logic_error("external closed Bundle realization is missing");
    }

    ValueTypeRef result{};
    if (use_pool) {
      auto created = detail::make_pooled_polymorphic_value_type(
          schema, pool_binding.get(), std::move(alternatives)
#if HGRAPH_ENABLE_PYTHON_USER_NODES
                      ,
          detail::PolymorphicPythonSourceResolver{
              .context = external_found->second.get(),
              .resolve =
                  [](const void *context, nb::handle source) {
                    return static_cast<const UnionEntry *>(context)
                        ->python_source_type(source);
                  },
          }
#endif
      );
      result = created.binding();
      graph_pooled_union_types.emplace(schema, std::move(created));
    } else {
      auto created =
          std::make_unique<UnionEntry>(schema, std::move(alternatives));
      result = created->binding;
      graph_inline_union_types.emplace(schema, std::move(created));
    }
    graph_union_bindings.emplace(schema, result);
    if (result != external_binding) {
      register_value_owning_type(result, external_binding);
    }
    return result;
  }

  [[nodiscard]] ValueTypeRef type_for(const ValueTypeMetaData *schema) const {
    std::lock_guard lock(mutex);
    return type_for_locked(schema);
  }

  [[nodiscard]] ValueTypeRef
  graph_type_for(const ValueTypeMetaData *schema) const {
    std::lock_guard lock(mutex);
    return graph_type_for_locked(schema);
  }

  [[nodiscard]] TypeRealizationInspection
  inspect(const ValueTypeMetaData *schema) const {
    if (schema == nullptr) {
      return {};
    }
    std::lock_guard lock(mutex);
    const auto external = type_for_locked(schema);
    const auto graph = graph_type_for_locked(schema);
    TypeRealizationInspection result{
        .representation = GraphValueRepresentation::Exact,
        .external_size = external.checked_plan().layout.size,
        .graph_size = graph.checked_plan().layout.size,
    };
    if (!polymorphic(schema)) {
      return result;
    }
    const auto &schemas = closure_for_locked(schema);
    result.alternative_count = schemas.size();
    result.minimum_leaf_size = std::numeric_limits<std::size_t>::max();
    for (const auto *alternative : schemas) {
      const auto leaf = graph_exact_type_for_locked(alternative);
      result.minimum_leaf_size =
          std::min(result.minimum_leaf_size, leaf.checked_plan().layout.size);
      result.maximum_leaf_size =
          std::max(result.maximum_leaf_size, leaf.checked_plan().layout.size);
    }
    if (schemas.empty()) {
      result.minimum_leaf_size = 0;
    }
    result.representation = graph_pooled_union_types.contains(schema)
                                ? GraphValueRepresentation::PooledUnion
                                : GraphValueRepresentation::InlineUnion;
    return result;
  }
};

namespace {
struct SnapshotKey {
  std::uint64_t generation{0};
  TypeRealizationOptions options{};

  bool operator==(const SnapshotKey &) const = default;
};

struct SnapshotKeyHash {
  [[nodiscard]] std::size_t operator()(const SnapshotKey &key) const noexcept {
    std::size_t result = std::hash<std::uint64_t>{}(key.generation);
    result ^= static_cast<std::size_t>(
                  key.options.polymorphic_compound_storage) +
              0x9e3779b97f4a7c15ULL + (result << 6U) + (result >> 2U);
    return result;
  }
};

TypeSystemMutex &snapshot_mutex() {
  static auto *value = new TypeSystemMutex();
  return *value;
}

std::unordered_map<SnapshotKey, std::shared_ptr<const TypeRealizationSnapshot>,
                   SnapshotKeyHash> &
snapshots() {
  static auto *value =
      new std::unordered_map<SnapshotKey,
                             std::shared_ptr<const TypeRealizationSnapshot>,
                             SnapshotKeyHash>();
  return *value;
}
} // namespace

void set_type_realization_options(GlobalStateView state,
                                  TypeRealizationOptions options) {
  if (!state.valid()) {
    throw std::logic_error(
        "type realization configuration requires GlobalState");
  }
  state.set(type_realization_options_key,
            Value{Bool{options.polymorphic_compound_storage ==
                       PolymorphicCompoundStoragePolicy::Pooled}});
}

void set_pooled_compound_scalar_storage(GlobalStateView state, bool enabled) {
  set_type_realization_options(
      state,
      TypeRealizationOptions{
          .polymorphic_compound_storage =
              enabled ? PolymorphicCompoundStoragePolicy::Pooled
                      : PolymorphicCompoundStoragePolicy::Inline,
      });
}

TypeRealizationOptions type_realization_options(GlobalStateView state) {
  if (!state.valid()) {
    return {};
  }
  const ValueView configured = state.get(type_realization_options_key);
  if (!configured.valid()) {
    return {};
  }
  return TypeRealizationOptions{
      .polymorphic_compound_storage =
          configured.checked_as<Bool>()
              ? PolymorphicCompoundStoragePolicy::Pooled
              : PolymorphicCompoundStoragePolicy::Inline,
  };
}

TypeRealizationSnapshot::TypeRealizationSnapshot(
    std::shared_ptr<Impl> impl) noexcept
    : impl_(std::move(impl)) {}

std::shared_ptr<const TypeRealizationSnapshot>
TypeRealizationSnapshot::capture(TypeRegistry &registry,
                                 TypeRealizationOptions options) {
  const auto requested_generation = registry.bundle_hierarchy_generation();
  const SnapshotKey requested_key{requested_generation, options};
  {
    std::lock_guard lock(snapshot_mutex());
    if (const auto found = snapshots().find(requested_key);
        found != snapshots().end()) {
      return found->second;
    }
  }
  auto snapshot = std::shared_ptr<const TypeRealizationSnapshot>{
      new TypeRealizationSnapshot{std::make_shared<Impl>(registry, options)}};
  std::lock_guard lock(snapshot_mutex());
  return snapshots()
      .try_emplace(SnapshotKey{snapshot->generation(), snapshot->options()},
                   snapshot)
      .first->second;
}

std::uint64_t TypeRealizationSnapshot::generation() const noexcept {
  return impl_->generation;
}

TypeRealizationOptions TypeRealizationSnapshot::options() const noexcept {
  return impl_->options;
}

CompoundScalarStorageBinding &
TypeRealizationSnapshot::pool_binding() const noexcept {
  return *impl_->pool_binding;
}

bool TypeRealizationSnapshot::pooled_compound_storage_enabled() const noexcept {
  return impl_->options.polymorphic_compound_storage ==
         PolymorphicCompoundStoragePolicy::Pooled;
}

bool TypeRealizationSnapshot::is_polymorphic(
    const ValueTypeMetaData *schema) const noexcept {
  return impl_->polymorphic(schema);
}

std::vector<const ValueTypeMetaData *>
TypeRealizationSnapshot::alternatives(const ValueTypeMetaData *schema) const {
  if (schema == nullptr ||
      impl_->parents.find(schema) == impl_->parents.end()) {
    return {};
  }
  std::lock_guard lock(impl_->mutex);
  return impl_->closure_for_locked(schema);
}

ValueTypeRef
TypeRealizationSnapshot::exact_type_for(const ValueTypeMetaData *schema) const {
  if (schema == nullptr) {
    return {};
  }
  std::lock_guard lock(impl_->mutex);
  return impl_->exact_type_for_locked(schema);
}

ValueTypeRef
TypeRealizationSnapshot::type_for(const ValueTypeMetaData *schema) const {
  if (schema == nullptr) {
    return {};
  }
  return impl_->type_for(schema);
}

ValueTypeRef
TypeRealizationSnapshot::graph_type_for(const ValueTypeMetaData *schema) const {
  if (schema == nullptr) {
    return {};
  }
  return impl_->graph_type_for(schema);
}

TypeRealizationInspection
TypeRealizationSnapshot::inspect(const ValueTypeMetaData *schema) const {
  return impl_->inspect(schema);
}

TypeRealizationScope::TypeRealizationScope(
    const TypeRealizationSnapshot *snapshot) noexcept
    : previous_(std::exchange(active_snapshot, snapshot)) {}

TypeRealizationScope::~TypeRealizationScope() noexcept {
  active_snapshot = previous_;
}

const TypeRealizationSnapshot *active_type_realization() noexcept {
  return active_snapshot;
}

GraphValueRealizationScope::GraphValueRealizationScope() noexcept
    : previous_(std::exchange(graph_value_realization, true)) {}

GraphValueRealizationScope::~GraphValueRealizationScope() noexcept {
  graph_value_realization = previous_;
}

bool active_graph_value_realization() noexcept {
  return graph_value_realization;
}

ValueTypeRef
value_type_for_active_realization(const ValueTypeMetaData *schema) {
  if (active_snapshot == nullptr) {
    return ValuePlanFactory::instance().type_for(schema);
  }
  return graph_value_realization ? active_snapshot->graph_type_for(schema)
                                 : active_snapshot->type_for(schema);
}

ValueTypeRef value_type_for_wiring(const ValueTypeMetaData *schema) {
  if (active_snapshot != nullptr) {
    return active_snapshot->type_for(schema);
  }
  GlobalState *state = GlobalContext::active_state();
  const TypeRealizationOptions options =
      state != nullptr ? type_realization_options(state->view())
                       : TypeRealizationOptions{};
  return TypeRealizationSnapshot::capture(TypeRegistry::instance(), options)
      ->type_for(schema);
}

void clear_type_realization_snapshots() noexcept {
  std::lock_guard lock(snapshot_mutex());
  snapshots().clear();
}
} // namespace hgraph
