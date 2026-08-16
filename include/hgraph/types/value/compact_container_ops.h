#ifndef HGRAPH_CPP_ROOT_VALUE_COMPACT_CONTAINER_OPS_H
#define HGRAPH_CPP_ROOT_VALUE_COMPACT_CONTAINER_OPS_H

#include <hgraph/types/metadata/debug_descriptor.h>
#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/value/compact_storage.h>
#include <hgraph/types/value/container_ops.h>
#include <hgraph/types/value/specialized_views.h>
#include <hgraph/types/value/value_ops.h>
#include <hgraph/types/value/value_range.h>
#include <hgraph/types/value/value_view.h>

#include <algorithm>
#include <compare>
#include <cstddef>
#include <fmt/format.h>
#include <iterator>
#include <stdexcept>
#include <string>
#include <utility>

namespace hgraph
{
    /**
     * Compact-storage-backed implementations of the per-kind value-layer
     * ops surfaces declared in ``container_ops.h``.
     *
     * The ops *types* (``ValueOps``, ``IndexedValueOps``,
     * ``ListValueOps``, ``CyclicBufferValueOps``, ``QueueValueOps``,
     * ``SetValueOps``, ``MapValueOps``) live in ``container_ops.h`` —
     * those are layout-agnostic and shared across implementations. This
     * header carries the *compact* implementation: thunks that downcast
     * the data pointer to the matching ``compact_storage.h`` class, the
     * canonical-ops factory functions, and the canonical-binding
     * accessors that intern the (schema, plan, ops) triple.
     *
     * When a slot-store-backed time-series variant of a kind arrives it
     * will live in its own ``ts_container_ops.h`` (or similar), reusing
     * the same ops *types* but supplying its own thunks. View code does
     * not change.
     */

    namespace container_ops_detail
    {
        [[nodiscard]] inline std::size_t combine_hash(std::size_t seed, std::size_t value) noexcept
        {
            seed ^= value + 0x9e3779b97f4a7c15ULL + (seed << 6U) + (seed >> 2U);
            return seed;
        }

        template <typename WriteElement>
        [[nodiscard]] inline std::string format_delimited(char open,
                                                          char close,
                                                          std::size_t size,
                                                          WriteElement write_element)
        {
            fmt::memory_buffer out;
            fmt::format_to(std::back_inserter(out), "{}", open);
            for (std::size_t i = 0; i < size; ++i)
            {
                if (i > 0) { fmt::format_to(std::back_inserter(out), ", "); }
                write_element(out, i);
            }
            fmt::format_to(std::back_inserter(out), "{}", close);
            return fmt::to_string(out);
        }

        // ----- List -----------------------------------------------------

        inline std::size_t list_hash(const void *, const void *memory)
        {
            const auto *storage = static_cast<const ListStorage *>(memory);
            if (storage == nullptr) { throw std::logic_error("List hash requires live storage"); }
            if (storage->element_binding() == nullptr)
            {
                throw std::logic_error("List hash requires an element binding");
            }
            const auto element_binding = storage->element_binding();
            const auto &ops = element_binding.ops_ref();
            std::size_t seed = 0;
            for (std::size_t i = 0; i < storage->size(); ++i)
            {
                // UNSET holes hash with a distinct marker (element validity).
                seed = combine_hash(seed, storage->element_set(i)
                                              ? ops.hash(storage->element_at(i))
                                              : 0x9e3779b97f4a7c15ULL);
            }
            return seed;
        }

        inline bool list_equals(const void *, const void *lhs, const void *rhs) noexcept
        {
            const auto *a = static_cast<const ListStorage *>(lhs);
            const auto *b = static_cast<const ListStorage *>(rhs);
            if (a == nullptr || b == nullptr) { return a == b; }
            if (a->size() != b->size()) { return false; }
            if (a->element_binding() == nullptr || b->element_binding() == nullptr) { return false; }
            if (a->element_binding() != b->element_binding()) { return false; }
            const auto element_binding = a->element_binding();
            const auto &ops = element_binding.ops_ref();
            for (std::size_t i = 0; i < a->size(); ++i)
            {
                const bool a_set = a->element_set(i);
                if (a_set != b->element_set(i)) { return false; }
                if (!a_set) { continue; }   // UNSET == UNSET
                if (!ops.equals(a->element_at(i), b->element_at(i))) { return false; }
            }
            return true;
        }

        inline std::partial_ordering list_compare(const void *, const void *lhs, const void *rhs) noexcept
        {
            const auto *a = static_cast<const ListStorage *>(lhs);
            const auto *b = static_cast<const ListStorage *>(rhs);
            if (const auto order = value_ops_detail::null_order(a, b)) { return *order; }

            const auto a_binding = a->element_binding();
            const auto b_binding = b->element_binding();
            if (const auto order = value_ops_detail::null_order(a_binding, b_binding)) { return *order; }
            if (a_binding != b_binding) { return std::partial_ordering::unordered; }

            const auto &ops = a_binding.ops_ref();
            const auto  n   = std::min(a->size(), b->size());
            for (std::size_t i = 0; i < n; ++i)
            {
                const bool a_set = a->element_set(i);
                const bool b_set = b->element_set(i);
                if (a_set != b_set) { return a_set ? std::partial_ordering::greater : std::partial_ordering::less; }
                if (!a_set) { continue; }
                const auto c = ops.compare(a->element_at(i), b->element_at(i));
                if (c != 0) { return c; }
            }
            if (a->size() < b->size()) { return std::partial_ordering::less; }
            if (a->size() > b->size()) { return std::partial_ordering::greater; }
            return std::partial_ordering::equivalent;
        }

        inline std::string list_to_string(const void *, const void *memory)
        {
            const auto *storage = static_cast<const ListStorage *>(memory);
            if (storage == nullptr || storage->element_binding() == nullptr) { return "[]"; }
            const auto element_binding = storage->element_binding();
            const auto &ops = element_binding.ops_ref();
            return format_delimited('[', ']', storage->size(), [&](fmt::memory_buffer &out, std::size_t i) {
                if (storage->element_set(i))
                {
                    fmt::format_to(std::back_inserter(out), "{}", ops.to_string(storage->element_at(i)));
                }
                else { fmt::format_to(std::back_inserter(out), "None"); }
            });
        }

#if HGRAPH_ENABLE_PYTHON_USER_NODES
        inline const void *list_value_array_element_at(const void *owner, std::size_t index)
        {
            return static_cast<const ListStorage *>(owner)->element_at(index);
        }

        inline nb::object sequence_to_python_buffer(const ValueTypeRef &element_binding,
                                                    ValueArraySource        source)
        {
            const auto &ops = element_binding.ops_ref();
            return ops.can_to_python_buffer(element_binding)
                       ? ops.to_python_buffer(element_binding, source)
                       : nb::object{};
        }

        [[nodiscard]] inline ValueArraySpan compact_sequence_span(const ValueTypeRef &element_binding,
                                                                  const void             *data,
                                                                  std::size_t             size)
        {
            return ValueArraySpan{
                .data   = size == 0 ? nullptr : data,
                .size   = size,
                .stride = element_binding.checked_plan().layout.size,
            };
        }

        inline nb::object list_to_python(const void *, const void *memory)
        {
            const auto *storage = static_cast<const ListStorage *>(memory);
            if (storage == nullptr || storage->element_binding() == nullptr)
            {
                throw std::runtime_error("List to_python requires live storage with an element binding");
            }
            // Any hole disables the trivially-copyable buffer fast-path.
            bool dense = true;
            for (std::size_t i = 0; i < storage->size(); ++i)
            {
                if (!storage->element_set(i)) { dense = false; break; }
            }
            if (dense)
            {
                if (nb::object buffer = sequence_to_python_buffer(storage->element_binding(),
                                                                  ValueArraySource{
                                                                      .owner      = storage,
                                                                      .size       = storage->size(),
                                                                      .element_at = &list_value_array_element_at,
                                                                      .first      = compact_sequence_span(
                                                                          storage->element_binding(),
                                                                          storage->size() == 0 ? nullptr
                                                                                                : storage->element_at(0),
                                                                          storage->size()),
                                                                  });
                    buffer.is_valid())
                {
                    return buffer;
                }
            }

            const auto element_binding = storage->element_binding();
            const auto &ops = element_binding.ops_ref();
            nb::list result;
            for (std::size_t i = 0; i < storage->size(); ++i)
            {
                // UNSET holes read back as None.
                result.append(storage->element_set(i) ? ops.to_python(storage->element_at(i)) : nb::none());
            }
            return result;
        }

        void list_from_python(const void *, const ValueTypeRef &binding, void *memory, nb::handle source);

        /** The VARIADIC-TUPLE variant: same storage, python reads back a
            TUPLE (ops-variant selection at binding time - the type-erasure
            rule: no runtime flag checks). */
        inline nb::object list_to_python_tuple(const void *context, const void *memory)
        {
            return nb::tuple(list_to_python(context, memory));
        }

        /** Shaped-array variant selected when the binding is interned. */
        inline nb::object list_to_python_array(const void *context, const void *memory)
        {
            nb::object value = list_to_python(context, memory);
            return nb::module_::import_("numpy").attr("asarray")(std::move(value));
        }
#endif

        // ----- CyclicBuffer (read in ring order) ------------------------

        inline std::size_t cyclic_buffer_hash(const void *, const void *memory)
        {
            const auto *storage = static_cast<const CyclicBufferStorage *>(memory);
            if (storage == nullptr) { throw std::logic_error("CyclicBuffer hash requires live storage"); }
            if (storage->element_binding() == nullptr)
            {
                throw std::logic_error("CyclicBuffer hash requires an element binding");
            }
            const auto element_binding = storage->element_binding();
            const auto &ops = element_binding.ops_ref();
            std::size_t seed = 0;
            for (std::size_t i = 0; i < storage->size(); ++i)
            {
                // UNSET holes hash with a distinct marker (element validity).
                seed = combine_hash(seed, storage->element_set(i)
                                              ? ops.hash(storage->element_at(i))
                                              : 0x9e3779b97f4a7c15ULL);
            }
            return seed;
        }

        inline bool cyclic_buffer_equals(const void *, const void *lhs, const void *rhs) noexcept
        {
            const auto *a = static_cast<const CyclicBufferStorage *>(lhs);
            const auto *b = static_cast<const CyclicBufferStorage *>(rhs);
            if (a == nullptr || b == nullptr) { return a == b; }
            if (a->size() != b->size()) { return false; }
            if (a->element_binding() == nullptr || b->element_binding() == nullptr) { return false; }
            if (a->element_binding() != b->element_binding()) { return false; }
            const auto element_binding = a->element_binding();
            const auto &ops = element_binding.ops_ref();
            for (std::size_t i = 0; i < a->size(); ++i)
            {
                const bool a_set = a->element_set(i);
                if (a_set != b->element_set(i)) { return false; }
                if (!a_set) { continue; }   // UNSET == UNSET
                if (!ops.equals(a->element_at(i), b->element_at(i))) { return false; }
            }
            return true;
        }

        inline std::partial_ordering cyclic_buffer_compare(const void *, const void *lhs, const void *rhs) noexcept
        {
            const auto *a = static_cast<const CyclicBufferStorage *>(lhs);
            const auto *b = static_cast<const CyclicBufferStorage *>(rhs);
            if (const auto order = value_ops_detail::null_order(a, b)) { return *order; }

            const auto a_binding = a->element_binding();
            const auto b_binding = b->element_binding();
            if (const auto order = value_ops_detail::null_order(a_binding, b_binding)) { return *order; }
            if (a_binding != b_binding) { return std::partial_ordering::unordered; }

            const auto &ops = a_binding.ops_ref();
            const auto  n   = std::min(a->size(), b->size());
            for (std::size_t i = 0; i < n; ++i)
            {
                const bool a_set = a->element_set(i);
                const bool b_set = b->element_set(i);
                if (a_set != b_set) { return a_set ? std::partial_ordering::greater : std::partial_ordering::less; }
                if (!a_set) { continue; }
                const auto c = ops.compare(a->element_at(i), b->element_at(i));
                if (c != 0) { return c; }
            }
            if (a->size() < b->size()) { return std::partial_ordering::less; }
            if (a->size() > b->size()) { return std::partial_ordering::greater; }
            return std::partial_ordering::equivalent;
        }

        inline std::string cyclic_buffer_to_string(const void *, const void *memory)
        {
            const auto *storage = static_cast<const CyclicBufferStorage *>(memory);
            if (storage == nullptr || storage->element_binding() == nullptr) { return "(empty)"; }
            const auto element_binding = storage->element_binding();
            const auto &ops = element_binding.ops_ref();
            return format_delimited('(', ')', storage->size(), [&](fmt::memory_buffer &out, std::size_t i) {
                fmt::format_to(std::back_inserter(out), "{}", ops.to_string(storage->element_at(i)));
            });
        }

#if HGRAPH_ENABLE_PYTHON_USER_NODES
        inline const void *cyclic_buffer_value_array_element_at(const void *owner, std::size_t index)
        {
            return static_cast<const CyclicBufferStorage *>(owner)->element_at(index);
        }

        inline nb::object cyclic_buffer_to_python(const void *, const void *memory)
        {
            const auto *storage = static_cast<const CyclicBufferStorage *>(memory);
            if (storage == nullptr || storage->element_binding() == nullptr)
            {
                throw std::runtime_error("CyclicBuffer to_python requires live storage with an element binding");
            }
            if (nb::object buffer = sequence_to_python_buffer(storage->element_binding(),
                                                              [&]() {
                                                                  const auto size = storage->size();
                                                                  const auto head = storage->head();
                                                                  const auto first_size = size == 0 ? 0 : size - head;
                                                                  return ValueArraySource{
                                                                      .owner      = storage,
                                                                      .size       = size,
                                                                      .element_at = &cyclic_buffer_value_array_element_at,
                                                                      .first = compact_sequence_span(
                                                                          storage->element_binding(),
                                                                          first_size == 0 ? nullptr
                                                                                          : storage->element_at(0),
                                                                          first_size),
                                                                      .second = compact_sequence_span(
                                                                          storage->element_binding(),
                                                                          first_size == size
                                                                              ? nullptr
                                                                              : storage->element_at(first_size),
                                                                          size - first_size),
                                                                  };
                                                              }());
                buffer.is_valid())
            {
                return buffer;
            }

            const auto element_binding = storage->element_binding();
            const auto &ops = element_binding.ops_ref();
            nb::list result;
            for (std::size_t i = 0; i < storage->size(); ++i)
            {
                result.append(ops.to_python(storage->element_at(i)));
            }
            return result;
        }

        void cyclic_buffer_from_python(const void *, const ValueTypeRef &binding, void *memory,
                                       nb::handle source);
#endif

        // ----- Queue (read in arrival order) ----------------------------

        inline std::size_t queue_hash(const void *, const void *memory)
        {
            const auto *storage = static_cast<const QueueStorage *>(memory);
            if (storage == nullptr) { throw std::logic_error("Queue hash requires live storage"); }
            if (storage->element_binding() == nullptr)
            {
                throw std::logic_error("Queue hash requires an element binding");
            }
            const auto element_binding = storage->element_binding();
            const auto &ops = element_binding.ops_ref();
            std::size_t seed = 0;
            for (std::size_t i = 0; i < storage->size(); ++i)
            {
                // UNSET holes hash with a distinct marker (element validity).
                seed = combine_hash(seed, storage->element_set(i)
                                              ? ops.hash(storage->element_at(i))
                                              : 0x9e3779b97f4a7c15ULL);
            }
            return seed;
        }

        inline bool queue_equals(const void *, const void *lhs, const void *rhs) noexcept
        {
            const auto *a = static_cast<const QueueStorage *>(lhs);
            const auto *b = static_cast<const QueueStorage *>(rhs);
            if (a == nullptr || b == nullptr) { return a == b; }
            if (a->size() != b->size()) { return false; }
            if (a->element_binding() == nullptr || b->element_binding() == nullptr) { return false; }
            if (a->element_binding() != b->element_binding()) { return false; }
            const auto element_binding = a->element_binding();
            const auto &ops = element_binding.ops_ref();
            for (std::size_t i = 0; i < a->size(); ++i)
            {
                const bool a_set = a->element_set(i);
                if (a_set != b->element_set(i)) { return false; }
                if (!a_set) { continue; }   // UNSET == UNSET
                if (!ops.equals(a->element_at(i), b->element_at(i))) { return false; }
            }
            return true;
        }

        inline std::partial_ordering queue_compare(const void *, const void *lhs, const void *rhs) noexcept
        {
            const auto *a = static_cast<const QueueStorage *>(lhs);
            const auto *b = static_cast<const QueueStorage *>(rhs);
            if (const auto order = value_ops_detail::null_order(a, b)) { return *order; }

            const auto a_binding = a->element_binding();
            const auto b_binding = b->element_binding();
            if (const auto order = value_ops_detail::null_order(a_binding, b_binding)) { return *order; }
            if (a_binding != b_binding) { return std::partial_ordering::unordered; }

            const auto &ops = a_binding.ops_ref();
            const auto  n   = std::min(a->size(), b->size());
            for (std::size_t i = 0; i < n; ++i)
            {
                const bool a_set = a->element_set(i);
                const bool b_set = b->element_set(i);
                if (a_set != b_set) { return a_set ? std::partial_ordering::greater : std::partial_ordering::less; }
                if (!a_set) { continue; }
                const auto c = ops.compare(a->element_at(i), b->element_at(i));
                if (c != 0) { return c; }
            }
            if (a->size() < b->size()) { return std::partial_ordering::less; }
            if (a->size() > b->size()) { return std::partial_ordering::greater; }
            return std::partial_ordering::equivalent;
        }

        inline std::string queue_to_string(const void *, const void *memory)
        {
            const auto *storage = static_cast<const QueueStorage *>(memory);
            if (storage == nullptr || storage->element_binding() == nullptr) { return "<>"; }
            const auto element_binding = storage->element_binding();
            const auto &ops = element_binding.ops_ref();
            return format_delimited('<', '>', storage->size(), [&](fmt::memory_buffer &out, std::size_t i) {
                fmt::format_to(std::back_inserter(out), "{}", ops.to_string(storage->element_at(i)));
            });
        }

#if HGRAPH_ENABLE_PYTHON_USER_NODES
        inline const void *queue_value_array_element_at(const void *owner, std::size_t index)
        {
            return static_cast<const QueueStorage *>(owner)->element_at(index);
        }

        inline nb::object queue_to_python(const void *, const void *memory)
        {
            const auto *storage = static_cast<const QueueStorage *>(memory);
            if (storage == nullptr || storage->element_binding() == nullptr)
            {
                throw std::runtime_error("Queue to_python requires live storage with an element binding");
            }
            if (nb::object buffer = sequence_to_python_buffer(storage->element_binding(),
                                                              ValueArraySource{
                                                                  .owner      = storage,
                                                                  .size       = storage->size(),
                                                                  .element_at = &queue_value_array_element_at,
                                                                  .first      = compact_sequence_span(
                                                                      storage->element_binding(),
                                                                      storage->size() == 0 ? nullptr
                                                                                            : storage->element_at(0),
                                                                      storage->size()),
                                                              });
                buffer.is_valid())
            {
                return buffer;
            }

            const auto element_binding = storage->element_binding();
            const auto &ops = element_binding.ops_ref();
            nb::list result;
            for (std::size_t i = 0; i < storage->size(); ++i)
            {
                result.append(ops.to_python(storage->element_at(i)));
            }
            return result;
        }

        void queue_from_python(const void *, const ValueTypeRef &binding, void *memory, nb::handle source);
#endif

        // ----- Set (order-independent) ----------------------------------

        inline std::size_t set_hash(const void *, const void *memory)
        {
            const auto *storage = static_cast<const SetStorage *>(memory);
            if (storage == nullptr) { throw std::logic_error("Set hash requires live storage"); }
            if (storage->element_binding() == nullptr)
            {
                throw std::logic_error("Set hash requires an element binding");
            }
            const auto element_binding = storage->element_binding();
            const auto &ops = element_binding.ops_ref();
            std::size_t result = 0;
            for (std::size_t i = 0; i < storage->size(); ++i)
            {
                result ^= ops.hash(storage->element_at(i));
            }
            return result;
        }

        inline bool set_equals(const void *, const void *lhs, const void *rhs) noexcept
        {
            const auto *a = static_cast<const SetStorage *>(lhs);
            const auto *b = static_cast<const SetStorage *>(rhs);
            if (a == nullptr || b == nullptr) { return a == b; }
            if (a->size() != b->size()) { return false; }
            if (a->element_binding() == nullptr || b->element_binding() == nullptr) { return false; }
            if (a->element_binding() != b->element_binding()) { return false; }
            for (std::size_t i = 0; i < a->size(); ++i)
            {
                if (!b->contains(a->element_at(i))) { return false; }
            }
            return true;
        }

        inline std::partial_ordering set_compare(const void *, const void *lhs, const void *rhs) noexcept
        {
            const auto *a = static_cast<const SetStorage *>(lhs);
            const auto *b = static_cast<const SetStorage *>(rhs);
            if (const auto order = value_ops_detail::null_order(a, b)) { return *order; }

            const auto a_binding = a->element_binding();
            const auto b_binding = b->element_binding();
            if (const auto order = value_ops_detail::null_order(a_binding, b_binding)) { return *order; }
            if (a_binding != b_binding) { return std::partial_ordering::unordered; }

            if (a->size() < b->size()) { return std::partial_ordering::less; }
            if (a->size() > b->size()) { return std::partial_ordering::greater; }
            return set_equals(nullptr, lhs, rhs) ? std::partial_ordering::equivalent
                                                 : std::partial_ordering::unordered;
        }

        inline std::string set_to_string(const void *, const void *memory)
        {
            const auto *storage = static_cast<const SetStorage *>(memory);
            if (storage == nullptr || storage->element_binding() == nullptr) { return "{}"; }
            const auto element_binding = storage->element_binding();
            const auto &ops = element_binding.ops_ref();
            return format_delimited('{', '}', storage->size(), [&](fmt::memory_buffer &out, std::size_t i) {
                fmt::format_to(std::back_inserter(out), "{}", ops.to_string(storage->element_at(i)));
            });
        }

#if HGRAPH_ENABLE_PYTHON_USER_NODES
        inline nb::object set_to_python(const void *, const void *memory)
        {
            const auto *storage = static_cast<const SetStorage *>(memory);
            if (storage == nullptr || storage->element_binding() == nullptr)
            {
                throw std::runtime_error("Set to_python requires live storage with an element binding");
            }
            const auto element_binding = storage->element_binding();
            const auto &ops = element_binding.ops_ref();
            nb::list items;
            for (std::size_t i = 0; i < storage->size(); ++i)
            {
                items.append(ops.to_python(storage->element_at(i)));
            }
            // A compact Set is the value-layer realization of Python's
            // immutable frozenset scalar. This concrete ValueOps strategy must
            // establish that public representation itself; TSData strategies
            // and Python facades consume the erased result unchanged. Slot-
            // backed TSS collections install different ValueOps and retain
            // their collection-specific mutable set surface.
            return nb::steal(PyFrozenSet_New(items.ptr()));
        }

        void set_from_python(const void *, const ValueTypeRef &binding, void *memory, nb::handle source);
#endif

        // ----- Map (order-independent over keys) ------------------------

        inline std::size_t map_hash(const void *, const void *memory)
        {
            const auto *storage = static_cast<const MapStorage *>(memory);
            if (storage == nullptr) { throw std::logic_error("Map hash requires live storage"); }
            if (storage->key_binding() == nullptr || storage->value_binding() == nullptr)
            {
                throw std::logic_error("Map hash requires key and value bindings");
            }
            const auto key_binding   = storage->key_binding();
            const auto value_binding = storage->value_binding();
            const auto &key_ops      = key_binding.ops_ref();
            const auto &value_ops    = value_binding.ops_ref();
            std::size_t result    = 0;
            for (std::size_t i = 0; i < storage->size(); ++i)
            {
                const std::size_t key_hash = key_ops.hash(storage->key_at(i));
                // UNSET values (None-valued entries) hash with a distinct
                // marker (element validity).
                const std::size_t value_hash =
                    storage->value_set(i) ? value_ops.hash(storage->value_at_index(i))
                                          : 0x9e3779b97f4a7c15ULL;
                result ^= combine_hash(key_hash, value_hash);
            }
            return result;
        }

        inline bool map_equals(const void *, const void *lhs, const void *rhs) noexcept
        {
            const auto *a = static_cast<const MapStorage *>(lhs);
            const auto *b = static_cast<const MapStorage *>(rhs);
            if (a == nullptr || b == nullptr) { return a == b; }
            if (a->size() != b->size()) { return false; }
            if (a->key_binding() == nullptr || b->key_binding() == nullptr ||
                a->value_binding() == nullptr || b->value_binding() == nullptr)
            {
                return false;
            }
            if (a->key_binding() != b->key_binding() || a->value_binding() != b->value_binding())
            {
                return false;
            }
            const auto value_binding = a->value_binding();
            const auto &val_ops = value_binding.ops_ref();
            const auto *a_const = static_cast<const MapStorage *>(lhs);
            for (std::size_t i = 0; i < a->size(); ++i)
            {
                const void *a_key  = a->key_at(i);
                const auto  b_slot = b->find_slot(a_key);
                if (b_slot == -1) { return false; }
                const bool a_set = a_const->value_set(i);
                const bool b_set = b->value_set(static_cast<std::size_t>(b_slot));
                if (a_set != b_set) { return false; }
                if (!a_set) { continue; }   // UNSET == UNSET
                if (!val_ops.equals(a_const->value_at_index(i),
                                    b->value_at_index(static_cast<std::size_t>(b_slot))))
                {
                    return false;
                }
            }
            return true;
        }

        inline std::partial_ordering map_compare(const void *, const void *lhs, const void *rhs) noexcept
        {
            const auto *a = static_cast<const MapStorage *>(lhs);
            const auto *b = static_cast<const MapStorage *>(rhs);
            if (const auto order = value_ops_detail::null_order(a, b)) { return *order; }

            const auto a_key_binding = a->key_binding();
            const auto b_key_binding = b->key_binding();
            if (const auto order = value_ops_detail::null_order(a_key_binding, b_key_binding)) { return *order; }
            if (a_key_binding != b_key_binding) { return std::partial_ordering::unordered; }

            const auto a_value_binding = a->value_binding();
            const auto b_value_binding = b->value_binding();
            if (const auto order = value_ops_detail::null_order(a_value_binding, b_value_binding)) { return *order; }
            if (a_value_binding != b_value_binding) { return std::partial_ordering::unordered; }

            return map_equals(nullptr, lhs, rhs) ? std::partial_ordering::equivalent
                                                 : std::partial_ordering::unordered;
        }

        inline std::string map_to_string(const void *, const void *memory)
        {
            const auto *storage = static_cast<const MapStorage *>(memory);
            if (storage == nullptr || storage->key_binding() == nullptr || storage->value_binding() == nullptr)
            {
                return "{}";
            }
            const auto key_binding   = storage->key_binding();
            const auto value_binding = storage->value_binding();
            const auto &key_ops      = key_binding.ops_ref();
            const auto &value_ops    = value_binding.ops_ref();
            return format_delimited('{', '}', storage->size(), [&](fmt::memory_buffer &out, std::size_t i) {
                fmt::format_to(std::back_inserter(out),
                               "{}: {}",
                               key_ops.to_string(storage->key_at(i)),
                               storage->value_set(i) ? value_ops.to_string(storage->value_at_index(i))
                                                     : std::string{"None"});
            });
        }

#if HGRAPH_ENABLE_PYTHON_USER_NODES
        inline nb::object map_to_python(const void *, const void *memory)
        {
            const auto *storage = static_cast<const MapStorage *>(memory);
            if (storage == nullptr || storage->key_binding() == nullptr || storage->value_binding() == nullptr)
            {
                throw std::runtime_error("Map to_python requires live storage with key/value bindings");
            }
            const auto key_binding   = storage->key_binding();
            const auto value_binding = storage->value_binding();
            const auto &key_ops      = key_binding.ops_ref();
            const auto &value_ops    = value_binding.ops_ref();
            nb::dict result;
            for (std::size_t i = 0; i < storage->size(); ++i)
            {
                // UNSET values (None-valued entries) read back as None.
                result[key_ops.to_python(storage->key_at(i))] =
                    storage->value_set(i) ? value_ops.to_python(storage->value_at_index(i)) : nb::none();
            }
            return result;
        }

        void map_from_python(const void *, const ValueTypeRef &binding, void *memory, nb::handle source);
#endif

        // ----- Read accessors that go through the storage's public surface.
        // Each container kind has one of these per accessor; views call
        // through the ops table rather than casting to a concrete storage
        // type, which keeps the views layout-agnostic.

        // ----- List -----
        inline std::size_t list_size(const void *, const void *memory) noexcept
        {
            return static_cast<const ListStorage *>(memory)->size();
        }
        inline const void *list_element_at(const void *, const void *memory, std::size_t index)
        {
            const auto *storage = static_cast<const ListStorage *>(memory);
            return storage->element_set(index) ? storage->element_at(index) : nullptr;
        }
        inline ValueTypeRef list_element_binding(const void *, const void *memory, std::size_t) noexcept
        {
            return static_cast<const ListStorage *>(memory)->element_binding();
        }
        inline bool list_element_valid(const void *, const void *memory,
                                       std::size_t index) noexcept
        {
            return static_cast<const ListStorage *>(memory)->element_set(index);
        }

        // ----- CyclicBuffer -----
        inline std::size_t cyclic_buffer_size(const void *, const void *memory) noexcept
        {
            return static_cast<const CyclicBufferStorage *>(memory)->size();
        }
        inline std::size_t cyclic_buffer_head(const void *memory) noexcept
        {
            return static_cast<const CyclicBufferStorage *>(memory)->head();
        }
        inline const void *cyclic_buffer_element_at(const void *, const void *memory, std::size_t index)
        {
            return static_cast<const CyclicBufferStorage *>(memory)->element_at(index);
        }
        inline ValueTypeRef cyclic_buffer_element_binding(const void *, const void *memory,
                                                                     std::size_t) noexcept
        {
            return static_cast<const CyclicBufferStorage *>(memory)->element_binding();
        }

        // ----- Queue -----
        inline std::size_t queue_size(const void *, const void *memory) noexcept
        {
            return static_cast<const QueueStorage *>(memory)->size();
        }
        inline const void *queue_front(const void *memory)
        {
            return static_cast<const QueueStorage *>(memory)->front();
        }
        inline const void *queue_element_at(const void *, const void *memory, std::size_t index)
        {
            return static_cast<const QueueStorage *>(memory)->element_at(index);
        }
        inline ValueTypeRef queue_element_binding(const void *, const void *memory, std::size_t) noexcept
        {
            return static_cast<const QueueStorage *>(memory)->element_binding();
        }

        // ----- Set -----
        inline std::size_t set_size(const void *, const void *memory) noexcept
        {
            return static_cast<const SetStorage *>(memory)->size();
        }
        inline bool set_contains(const void *, const void *memory, const void *key)
        {
            return static_cast<const SetStorage *>(memory)->contains(key);
        }
        inline const void *set_element_at(const void *, const void *memory, std::size_t index)
        {
            return static_cast<const SetStorage *>(memory)->element_at(index);
        }
        inline ValueTypeRef set_element_binding(const void *, const void *memory, std::size_t) noexcept
        {
            return static_cast<const SetStorage *>(memory)->element_binding();
        }

        // ----- Range builders for the compact storage shapes -------
        // For compact (dense) storage every ordinal is a live slot, so
        // the predicate is null (the Range walks every index in
        // [0, size)). The projector reads the element from the range
        // memory pointer and wraps it in a ``ValueView`` using the
        // storage's element binding.

        template <auto SizeFn, auto ElementAtFn, auto ElementBindingFn>
        ValueView dense_range_projector(const void *, const void *memory, std::size_t index)
        {
            return ValueView{ElementBindingFn(nullptr, memory, index),
                             ElementAtFn(nullptr, memory, index)};
        }

        template <auto SizeFn, auto ElementAtFn, auto ElementBindingFn>
        ValueView dense_mutable_range_projector(const void *, const void *memory, std::size_t index)
        {
            return ValueView{ElementBindingFn(nullptr, memory, index),
                             const_cast<void *>(ElementAtFn(nullptr, memory, index))}
                .begin_mutation();
        }

        template <auto SizeFn, auto ElementAtFn, auto ElementBindingFn>
        Range<ValueView> dense_make_range(const void *context, const void *memory)
        {
            return Range<ValueView>{
                .context   = context,
                .memory    = memory,
                .limit     = SizeFn(nullptr, memory),
                .predicate = nullptr,
                .projector = &dense_range_projector<SizeFn, ElementAtFn, ElementBindingFn>,
            };
        }

        template <auto SizeFn, auto ElementAtFn, auto ElementBindingFn>
        Range<ValueView> dense_make_mutable_range(const void *context, void *memory)
        {
            return Range<ValueView>{
                .context   = context,
                .memory    = memory,
                .limit     = SizeFn(nullptr, memory),
                .predicate = nullptr,
                .projector = &dense_mutable_range_projector<SizeFn, ElementAtFn, ElementBindingFn>,
            };
        }

        template <auto SizeFn, auto ElementAtFn, auto ElementBindingFn>
        Range<ValueView> dense_make_range_no_context(const void *memory)
        {
            return dense_make_range<SizeFn, ElementAtFn, ElementBindingFn>(nullptr, memory);
        }

        // Map-specific KV projector: pairs key with value at the same
        // ordinal slot. ``KeyAtFn`` resolves the key memory;
        // ``ValueAtIndexFn`` resolves the matching value memory.
        template <auto SizeFn, auto KeyAtFn, auto ValueAtIndexFn, auto KeyBindingFn, auto ValueBindingFn>
        std::pair<ValueView, ValueView> dense_kv_range_projector(const void *, const void *memory,
                                                                  std::size_t index)
        {
            return std::pair<ValueView, ValueView>{
                ValueView{KeyBindingFn(nullptr, memory, index), KeyAtFn(nullptr, memory, index)},
                ValueView{ValueBindingFn(nullptr, memory), ValueAtIndexFn(nullptr, memory, index)},
            };
        }

        template <auto SizeFn, auto KeyAtFn, auto ValueAtIndexFn, auto KeyBindingFn, auto ValueBindingFn>
        KeyValueRange<ValueView, ValueView> dense_make_kv_range(const void *context, const void *memory)
        {
            return KeyValueRange<ValueView, ValueView>{
                .context   = context,
                .memory    = memory,
                .limit     = SizeFn(nullptr, memory),
                .predicate = nullptr,
                .projector = &dense_kv_range_projector<SizeFn, KeyAtFn, ValueAtIndexFn, KeyBindingFn, ValueBindingFn>,
            };
        }

        // ----- Map -----
        // ``map_element_at`` and ``map_element_binding`` (filled into
        // the IndexedValueOps base) point at the *key* surface so map
        // iteration walks keys; the value side is reached via
        // ``MapValueOps::value_at_index`` and ``value_binding``.
        inline std::size_t map_size(const void *, const void *memory) noexcept
        {
            return static_cast<const MapStorage *>(memory)->size();
        }
        inline bool map_contains(const void *, const void *memory, const void *key)
        {
            return static_cast<const MapStorage *>(memory)->contains(key);
        }
        inline const void *map_value_at(const void *, const void *memory, const void *key)
        {
            return static_cast<const MapStorage *>(memory)->value_at(key);
        }
        inline const void *map_key_at_index(const void *, const void *memory, std::size_t index)
        {
            return static_cast<const MapStorage *>(memory)->key_at(index);
        }
        inline const void *map_value_at_index(const void *, const void *memory, std::size_t index)
        {
            return static_cast<const MapStorage *>(memory)->value_at_index(index);
        }
        inline ValueTypeRef map_key_binding(const void *, const void *memory, std::size_t) noexcept
        {
            return static_cast<const MapStorage *>(memory)->key_binding();
        }
        inline ValueTypeRef map_value_binding(const void *, const void *memory) noexcept
        {
            return static_cast<const MapStorage *>(memory)->value_binding();
        }
        inline ValueTypeRef map_value_binding_indexed(const void *, const void *memory, std::size_t) noexcept
        {
            return map_value_binding(nullptr, memory);
        }

        // ----- Map-as-Set adapter (used by ``MapView::key_set``) ---------
        // The thunks delegate to the map's read accessors — they never
        // access a separate ``SetStorage`` layout. Hash / equals /
        // compare / to_string treat the map's keys as a logical set
        // (order-independent xor of element hashes; subset+superset for
        // equality; ``{a, b, c}`` for formatting).
        inline std::size_t map_key_adapter_hash(const void *, const void *memory)
        {
            const auto *storage = static_cast<const MapStorage *>(memory);
            if (storage == nullptr) { throw std::logic_error("Map key-set hash requires live storage"); }
            if (storage->key_binding() == nullptr)
            {
                throw std::logic_error("Map key-set hash requires a key binding");
            }
            const auto key_binding = storage->key_binding();
            const auto &ops = key_binding.ops_ref();
            std::size_t result = 0;
            for (std::size_t i = 0; i < storage->size(); ++i)
            {
                result ^= ops.hash(storage->key_at(i));
            }
            return result;
        }
        inline bool map_key_adapter_equals(const void *, const void *lhs, const void *rhs) noexcept
        {
            const auto *a = static_cast<const MapStorage *>(lhs);
            const auto *b = static_cast<const MapStorage *>(rhs);
            if (a == nullptr || b == nullptr) { return a == b; }
            if (a->size() != b->size()) { return false; }
            if (a->key_binding() == nullptr || b->key_binding() == nullptr) { return false; }
            if (a->key_binding() != b->key_binding()) { return false; }
            for (std::size_t i = 0; i < a->size(); ++i)
            {
                if (!b->contains(a->key_at(i))) { return false; }
            }
            return true;
        }
        inline std::partial_ordering map_key_adapter_compare(const void *, const void *lhs, const void *rhs) noexcept
        {
            const auto *a = static_cast<const MapStorage *>(lhs);
            const auto *b = static_cast<const MapStorage *>(rhs);
            if (const auto order = value_ops_detail::null_order(a, b)) { return *order; }

            const auto a_binding = a->key_binding();
            const auto b_binding = b->key_binding();
            if (const auto order = value_ops_detail::null_order(a_binding, b_binding)) { return *order; }
            if (a_binding != b_binding) { return std::partial_ordering::unordered; }

            if (a->size() < b->size()) { return std::partial_ordering::less; }
            if (a->size() > b->size()) { return std::partial_ordering::greater; }
            return map_key_adapter_equals(nullptr, lhs, rhs) ? std::partial_ordering::equivalent
                                                             : std::partial_ordering::unordered;
        }
        inline std::string map_key_adapter_to_string(const void *, const void *memory)
        {
            const auto *storage = static_cast<const MapStorage *>(memory);
            if (storage == nullptr || storage->key_binding() == nullptr) { return "{}"; }
            const auto key_binding = storage->key_binding();
            const auto &ops = key_binding.ops_ref();
            return format_delimited('{', '}', storage->size(), [&](fmt::memory_buffer &out, std::size_t i) {
                fmt::format_to(std::back_inserter(out), "{}", ops.to_string(storage->key_at(i)));
            });
        }
#if HGRAPH_ENABLE_PYTHON_USER_NODES
        inline nb::object map_key_adapter_to_python(const void *, const void *memory)
        {
            const auto *storage = static_cast<const MapStorage *>(memory);
            if (storage == nullptr || storage->key_binding() == nullptr)
            {
                throw std::runtime_error("Map key-set to_python requires live storage with a key binding");
            }
            const auto key_binding = storage->key_binding();
            const auto &ops = key_binding.ops_ref();
            nb::set result;
            for (std::size_t i = 0; i < storage->size(); ++i)
            {
                result.add(ops.to_python(storage->key_at(i)));
            }
            return result;
        }

#endif
    }  // namespace container_ops_detail

    // -----------------------------------------------------------------
    // Per-kind canonical ``ValueOps`` (function-local-static; stable
    // address for the program lifetime).
    //
    // Aggregate init mirrors the ops hierarchy: the outer braces hold
    // the kind-specific extensions; the next brace holds the
    // IndexedValueOps additions; the innermost brace holds the base
    // ValueOps slots.
    // -----------------------------------------------------------------

    // Forward declaration; ``compact_map_ops`` references the thunk,
    // and the thunk's body references ``compact_map_key_set_type``
    // and ``SetView``. The definition follows the binding accessors.
    SetView compact_map_key_set_thunk(const void *context, ValueTypeRef map_binding, const void *memory);

    namespace container_ops_detail
    {
        [[nodiscard]] bool compact_accepts_source(
            const void *, ValueTypeRef binding, ValueTypeRef source) noexcept;
        void compact_list_copy_assign_from(
            const void *, ValueTypeRef binding, void *dst,
            ValueTypeRef source, const void *src);
        void compact_set_copy_assign_from(
            const void *, ValueTypeRef binding, void *dst,
            ValueTypeRef source, const void *src);
        void compact_map_copy_assign_from(
            const void *, ValueTypeRef binding, void *dst,
            ValueTypeRef source, const void *src);
        void compact_cyclic_buffer_copy_assign_from(
            const void *, ValueTypeRef binding, void *dst,
            ValueTypeRef source, const void *src);
        void compact_queue_copy_assign_from(
            const void *, ValueTypeRef binding, void *dst,
            ValueTypeRef source, const void *src);

        /** Shared move-assign: identical bindings move storage directly;
            cross-representation moves rebuild through the kind's
            copy_assign_from (compact storages have nothing cheaper to
            steal across representations). One template replaces five
            hand-written identical wrappers (audit finding, 2026-08-16). */
        template <auto CopyAssignFrom>
        void compact_move_assign_via_copy(const void *context, ValueTypeRef binding, void *dst,
                                          ValueTypeRef source, void *src)
        {
            if (binding == source)
            {
                binding.move_assign_at(dst, src);
                return;
            }
            CopyAssignFrom(context, binding, dst, source, src);
        }

        template <typename Storage>
        [[nodiscard]] DynamicStorageMetrics compact_dynamic_storage_metrics(
            const void *, const void *memory) noexcept
        {
            return static_cast<const Storage *>(memory)->dynamic_storage_metrics();
        }

        [[nodiscard]] inline DynamicStorageMetrics compact_map_key_set_dynamic_storage_metrics(
            const void *, const void *memory) noexcept
        {
            return static_cast<const MapStorage *>(memory)->key_set_dynamic_storage_metrics();
        }

        template <bool VariadicTuple, bool ShapedArray = false>
        [[nodiscard]] const ListValueOps &compact_list_ops_impl() noexcept;
    }

    namespace container_ops_detail
    {
        template <bool VariadicTuple, bool ShapedArray>
        [[nodiscard]] const ListValueOps &compact_list_ops_impl() noexcept
        {
            static const ListValueOps ops = [] {
                auto value = ListValueOps{
                {{// ValueOps:
                  ValueOpsKind::List,
                  nullptr,
                  false,
                  &container_ops_detail::list_hash,
                  &container_ops_detail::list_equals,
                  &container_ops_detail::list_compare,
                  &container_ops_detail::list_to_string
#if HGRAPH_ENABLE_PYTHON_USER_NODES
                  ,
                  ShapedArray ? &container_ops_detail::list_to_python_array
                              : VariadicTuple ? &container_ops_detail::list_to_python_tuple
                                              : &container_ops_detail::list_to_python,
                  &container_ops_detail::list_from_python
#endif
                 },
                 // IndexedValueOps:
                 &container_ops_detail::list_size,
                 &container_ops_detail::list_element_at,
                 &container_ops_detail::list_element_binding,
                 &container_ops_detail::dense_make_range<&container_ops_detail::list_size,
                                                          &container_ops_detail::list_element_at,
                                                          &container_ops_detail::list_element_binding>,
                 nullptr},
                // ListValueOps: no additions
                };
                value.dynamic_storage_metrics_impl =
                    &compact_dynamic_storage_metrics<ListStorage>;
                value.element_valid = &container_ops_detail::list_element_valid;
                value.accepts_source_impl = &compact_accepts_source;
                value.copy_assign_from_impl = &compact_list_copy_assign_from;
                value.move_assign_from_impl =
                &container_ops_detail::compact_move_assign_via_copy<
                    &container_ops_detail::compact_list_copy_assign_from>;
                return value;
            }();
            return ops;
        }
    }  // namespace container_ops_detail

    [[nodiscard]] inline const ListValueOps &compact_list_ops() noexcept
    {
        return container_ops_detail::compact_list_ops_impl<false>();
    }

    [[nodiscard]] inline const SetValueOps &compact_set_ops() noexcept
    {
        static const SetValueOps ops = [] {
            auto value = SetValueOps{
            {{ValueOpsKind::Set,
              nullptr,
              false,
              &container_ops_detail::set_hash,
              &container_ops_detail::set_equals,
              &container_ops_detail::set_compare,
              &container_ops_detail::set_to_string
#if HGRAPH_ENABLE_PYTHON_USER_NODES
              ,
              &container_ops_detail::set_to_python,
              &container_ops_detail::set_from_python
#endif
             },
             &container_ops_detail::set_size,
             &container_ops_detail::set_element_at,
             &container_ops_detail::set_element_binding,
             &container_ops_detail::dense_make_range<&container_ops_detail::set_size,
                                                     &container_ops_detail::set_element_at,
                                                     &container_ops_detail::set_element_binding>,
             nullptr},
            &container_ops_detail::set_contains,
            };
            value.dynamic_storage_metrics_impl =
                &container_ops_detail::compact_dynamic_storage_metrics<SetStorage>;
            value.accepts_source_impl = &container_ops_detail::compact_accepts_source;
            value.copy_assign_from_impl =
                &container_ops_detail::compact_set_copy_assign_from;
            value.move_assign_from_impl =
                &container_ops_detail::compact_move_assign_via_copy<
                    &container_ops_detail::compact_set_copy_assign_from>;
            return value;
        }();
        return ops;
    }

    [[nodiscard]] inline const MapValueOps &compact_map_ops() noexcept
    {
        // ``element_at`` / ``element_binding`` / make_range on the
        // indexed base point at the key surface so map iteration of
        // the indexed kind yields keys. ``make_kv_range`` exposes the
        // paired (key, value) surface.
        static const MapValueOps ops = [] {
            auto value = MapValueOps{
            {{ValueOpsKind::Map,
              nullptr,
              false,
              &container_ops_detail::map_hash,
              &container_ops_detail::map_equals,
              &container_ops_detail::map_compare,
              &container_ops_detail::map_to_string
#if HGRAPH_ENABLE_PYTHON_USER_NODES
              ,
              &container_ops_detail::map_to_python,
              &container_ops_detail::map_from_python
#endif
             },
             &container_ops_detail::map_size,
             &container_ops_detail::map_key_at_index,
             &container_ops_detail::map_key_binding,
             &container_ops_detail::dense_make_range<&container_ops_detail::map_size,
                                                      &container_ops_detail::map_key_at_index,
                                                      &container_ops_detail::map_key_binding>,
             nullptr},
            &container_ops_detail::map_contains,
            &container_ops_detail::map_value_at,
            &container_ops_detail::map_value_at_index,
            &container_ops_detail::map_value_binding,
            // make_keys_range — same as the IndexedValueOps base
            // since the indexed surface walks keys.
            &container_ops_detail::dense_make_range<&container_ops_detail::map_size,
                                                     &container_ops_detail::map_key_at_index,
                                                     &container_ops_detail::map_key_binding>,
            // make_values_range — projector wraps the value side.
            &container_ops_detail::dense_make_range<&container_ops_detail::map_size,
                                                     &container_ops_detail::map_value_at_index,
                                                     &container_ops_detail::map_value_binding_indexed>,
            &container_ops_detail::dense_make_kv_range<&container_ops_detail::map_size,
                                                        &container_ops_detail::map_key_at_index,
                                                        &container_ops_detail::map_value_at_index,
                                                        &container_ops_detail::map_key_binding,
                                                        &container_ops_detail::map_value_binding>,
            &compact_map_key_set_thunk,
            };
            value.dynamic_storage_metrics_impl =
                &container_ops_detail::compact_dynamic_storage_metrics<MapStorage>;
            value.accepts_source_impl = &container_ops_detail::compact_accepts_source;
            value.copy_assign_from_impl =
                &container_ops_detail::compact_map_copy_assign_from;
            value.move_assign_from_impl =
                &container_ops_detail::compact_move_assign_via_copy<
                    &container_ops_detail::compact_map_copy_assign_from>;
            return value;
        }();
        return ops;
    }

    [[nodiscard]] inline const CyclicBufferValueOps &compact_cyclic_buffer_ops() noexcept
    {
        static const CyclicBufferValueOps ops = [] {
            auto value = CyclicBufferValueOps{
            {{ValueOpsKind::CyclicBuffer,
              nullptr,
              false,
              &container_ops_detail::cyclic_buffer_hash,
              &container_ops_detail::cyclic_buffer_equals,
              &container_ops_detail::cyclic_buffer_compare,
              &container_ops_detail::cyclic_buffer_to_string
#if HGRAPH_ENABLE_PYTHON_USER_NODES
              ,
              &container_ops_detail::cyclic_buffer_to_python,
              &container_ops_detail::cyclic_buffer_from_python
#endif
             },
             &container_ops_detail::cyclic_buffer_size,
             &container_ops_detail::cyclic_buffer_element_at,
             &container_ops_detail::cyclic_buffer_element_binding,
             &container_ops_detail::dense_make_range<&container_ops_detail::cyclic_buffer_size,
                                                      &container_ops_detail::cyclic_buffer_element_at,
                                                      &container_ops_detail::cyclic_buffer_element_binding>,
             nullptr},
            &container_ops_detail::cyclic_buffer_head,
            };
            value.dynamic_storage_metrics_impl =
                &container_ops_detail::compact_dynamic_storage_metrics<CyclicBufferStorage>;
            value.accepts_source_impl = &container_ops_detail::compact_accepts_source;
            value.copy_assign_from_impl =
                &container_ops_detail::compact_cyclic_buffer_copy_assign_from;
            value.move_assign_from_impl =
                &container_ops_detail::compact_move_assign_via_copy<
                    &container_ops_detail::compact_cyclic_buffer_copy_assign_from>;
            return value;
        }();
        return ops;
    }

    [[nodiscard]] inline const QueueValueOps &compact_queue_ops() noexcept
    {
        static const QueueValueOps ops = [] {
            auto value = QueueValueOps{
            {{ValueOpsKind::Queue,
              nullptr,
              false,
              &container_ops_detail::queue_hash,
              &container_ops_detail::queue_equals,
              &container_ops_detail::queue_compare,
              &container_ops_detail::queue_to_string
#if HGRAPH_ENABLE_PYTHON_USER_NODES
              ,
              &container_ops_detail::queue_to_python,
              &container_ops_detail::queue_from_python
#endif
             },
             &container_ops_detail::queue_size,
             &container_ops_detail::queue_element_at,
             &container_ops_detail::queue_element_binding,
             &container_ops_detail::dense_make_range<&container_ops_detail::queue_size,
                                                      &container_ops_detail::queue_element_at,
                                                      &container_ops_detail::queue_element_binding>,
             nullptr},
            &container_ops_detail::queue_front,
            };
            value.dynamic_storage_metrics_impl =
                &container_ops_detail::compact_dynamic_storage_metrics<QueueStorage>;
            value.accepts_source_impl = &container_ops_detail::compact_accepts_source;
            value.copy_assign_from_impl =
                &container_ops_detail::compact_queue_copy_assign_from;
            value.move_assign_from_impl =
                &container_ops_detail::compact_move_assign_via_copy<
                    &container_ops_detail::compact_queue_copy_assign_from>;
            return value;
        }();
        return ops;
    }

    [[nodiscard]] inline const SetValueOps &compact_map_key_set_ops() noexcept
    {
        // Reuses ``map_size`` / ``map_key_at_index`` / ``map_key_binding``
        // / ``map_contains`` / range projector from the map ops
        // because the underlying memory is still a ``MapStorage`` —
        // the adapter just reframes the read surface as a Set.
        static const SetValueOps ops = [] {
            auto value = SetValueOps{
            {{ValueOpsKind::Set,
              nullptr,
              false,
              &container_ops_detail::map_key_adapter_hash,
              &container_ops_detail::map_key_adapter_equals,
              &container_ops_detail::map_key_adapter_compare,
              &container_ops_detail::map_key_adapter_to_string
#if HGRAPH_ENABLE_PYTHON_USER_NODES
              ,
              &container_ops_detail::map_key_adapter_to_python,
              // Read-only projection: null = unsupported, the one idiom for
              // the fact (the wrapper throws); a bespoke throwing thunk was
              // a second idiom for the same statement.
              nullptr
#endif
             },
             &container_ops_detail::map_size,
             &container_ops_detail::map_key_at_index,
             &container_ops_detail::map_key_binding,
             &container_ops_detail::dense_make_range<&container_ops_detail::map_size,
                                                      &container_ops_detail::map_key_at_index,
                                                      &container_ops_detail::map_key_binding>,
             nullptr},
            &container_ops_detail::map_contains,
            };
            value.dynamic_storage_metrics_impl =
                &container_ops_detail::compact_map_key_set_dynamic_storage_metrics;
            return value;
        }();
        return ops;
    }

    // -----------------------------------------------------------------
    // Canonical bindings — interned ``(schema, plan, ops)`` triples
    // for each compact container kind. Use these to construct a
    // ``Value`` over a container schema.
    // -----------------------------------------------------------------

    [[nodiscard]] HGRAPH_EXPORT ValueTypeRef compact_list_type(const ValueTypeRef &element_binding);

    /** Meta-preserving form: a variadic-TUPLE schema keeps its identity (and
        its python read-back as a tuple) while sharing the list plan+ops. */
    [[nodiscard]] HGRAPH_EXPORT ValueTypeRef compact_list_type(const ValueTypeRef &element_binding,
                                                                const ValueTypeMetaData &meta);

    [[nodiscard]] HGRAPH_EXPORT ValueTypeRef compact_set_type(const ValueTypeRef &element_binding);

    [[nodiscard]] HGRAPH_EXPORT ValueTypeRef compact_map_type(const ValueTypeRef &key_binding,
                                                              const ValueTypeRef &value_binding);

    [[nodiscard]] HGRAPH_EXPORT ValueTypeRef compact_cyclic_buffer_type(const ValueTypeRef &element_binding,
                                                                        std::size_t capacity);

    [[nodiscard]] HGRAPH_EXPORT ValueTypeRef compact_queue_type(const ValueTypeRef &element_binding,
                                                                std::size_t max_capacity);

    /**
     * Binding for the read-only ``SetView`` returned by
     * ``MapView::key_set()``. The schema is ``Set<KeyType>``; the plan
     * is the *map's* plan (the underlying memory is still a
     * ``MapStorage``); the ops are the set adapter that delegates
     * into the map's read accessors. This binding lets the SetView
     * coexist with the map's own binding without any layout
     * unification.
     */
    [[nodiscard]] HGRAPH_EXPORT ValueTypeRef compact_map_key_set_type(const ValueTypeRef &key_binding,
                                                                      const ValueTypeRef &value_binding);

    inline SetView compact_map_key_set_thunk(const void *, ValueTypeRef /*map_binding*/, const void *memory)
    {
        // Compact-storage implementation of ``MapValueOps::key_set``;
        // casting ``memory`` to ``MapStorage *`` is layout-correct
        // here. The set-adapter binding is interned once per
        // ``(key_binding, value_binding)`` pair and reused on every
        // call.
        const auto *storage = static_cast<const MapStorage *>(memory);
        if (storage == nullptr)
        {
            throw std::logic_error("compact_map_key_set: map storage is null");
        }
        if (storage->key_binding() == nullptr || storage->value_binding() == nullptr)
        {
            throw std::logic_error("compact_map_key_set: map storage missing key/value binding");
        }
        const auto adapter = compact_map_key_set_type(storage->key_binding(), storage->value_binding());
        return ValueView{adapter, memory}.as_set();
    }
}  // namespace hgraph

#endif  // HGRAPH_CPP_ROOT_VALUE_COMPACT_CONTAINER_OPS_H
