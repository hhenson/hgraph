#ifndef HGRAPH_CPP_ROOT_VALUE_OPS_H
#define HGRAPH_CPP_ROOT_VALUE_OPS_H

#include <hgraph/config.h>
#include <hgraph/hgraph_export.h>
#include <hgraph/types/metadata/value_type_meta_data.h>
#include <hgraph/types/primitive_types.h>
#include <hgraph/types/storage_metrics.h>
#include <hgraph/types/utils/memory_utils.h>
#include <hgraph/types/value/value_type_ref.h>

#include <fmt/format.h>

#include <algorithm>
#include <compare>
#include <concepts>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <functional>
#include <memory>
#include <optional>
#include <sstream>
#include <stdexcept>
#include <string>
#include <type_traits>
#include <typeinfo>

#include <hgraph/types/python_ops.h>

namespace hgraph
{
    enum class ValueOpsKind : std::uint8_t
    {
        Invalid      = 0,
        Base         = 1,
        Indexed      = 2,
        List         = 3,
        MutableList  = 4,
        CyclicBuffer = 5,
        Queue        = 6,
        Set          = 7,
        MutableSet   = 8,
        Map          = 9,
        MutableMap   = 10,
    };

    static_assert(sizeof(ValueOpsKind) == 1);
    inline constexpr std::uint16_t VALUE_OPS_ABI_VERSION = 7;

    struct ValueOps;
    using ValueArrayElementAt = const void *(*)(const void *owner, std::size_t index);

    /**
     * Contiguous run of value elements in logical order. ``stride`` is
     * measured in bytes so the descriptor works with type-erased storage.
     */
    struct ValueArraySpan
    {
        const void *data{nullptr};
        std::size_t size{0};
        std::size_t stride{0};
    };

    /**
     * Logical sequence of homogeneous value elements used as input when
     * exporting value storage to a Python array. Callers can supply up to
     * two contiguous spans for fast copy paths, plus an indexed fallback
     * for storage that needs per-element conversion or lookup.
     */
    struct ValueArraySource
    {
        const void            *owner{nullptr};
        std::size_t            size{0};
        ValueArrayElementAt    element_at{nullptr};
        ValueArraySpan         first{};
        ValueArraySpan         second{};
    };

    /**
     * Runtime behaviour vtable for value-layer types.
     *
     * Each entry is a function pointer whose first argument is the ops
     * context and whose remaining arguments are the memory addresses being
     * operated on. ``ValueOps`` is deliberately independent of
     * ``LifecycleOps`` (which lives on the storage plan): lifecycle ops
     * bring the memory into and out of existence, behaviour ops act on
     * already-constructed memory.
     *
     * Slots:
     *
     * - ``allows_mutation`` — whether a writable view may be opened
     *   with ``begin_mutation()``. Compact immutable storage leaves
     *   this false even when the generic view machinery can represent
     *   mutability.
     * - ``hash(memory)`` — content hash. Types without hash support do
     *   not install a hash implementation; callers get an exception
     *   rather than a sentinel value.
     * - ``equals(lhs, rhs)`` — deep equality.
     * - ``compare(lhs, rhs)`` — C++ comparison-category result following
     *   ``operator<=>`` conventions. Non-comparable types may return
     *   ``std::partial_ordering::equivalent``.
     * - ``to_string(memory)`` — diagnostic string. Non-streamable types
     *   may return the type name.
     * - ``format_string(memory)`` — user-facing scalar text. It falls back
     *   to the diagnostic string unless the type supplies a distinct form.
     */
    struct ValueOps
    {
        ValueOpsKind kind{ValueOpsKind::Invalid};
        const void *context{nullptr};
        bool        allows_mutation{false};
        std::size_t (*hash_impl)(const void *context, const void *memory) = nullptr;
        bool (*equals_impl)(const void *context, const void *lhs, const void *rhs) = nullptr;
        std::partial_ordering (*compare_impl)(const void *context, const void *lhs,
                                              const void *rhs) noexcept = nullptr;
        std::string (*to_string_impl)(const void *context, const void *memory) = nullptr;
        // Python conversion (RFC 0035): opaque references, filled with the
        // type layer's forwarders into the registered ``PythonOps`` table.
        PyNewRef (*to_python_impl)(const void *context, const void *memory) = nullptr;
        void (*from_python_impl)(const void *context, const ValueTypeRef &binding, void *memory,
                                 PyRef source) = nullptr;
        PyNewRef (*to_python_buffer_impl)(const void *context, const ValueTypeRef &binding,
                                          const ValueArraySource &source) = nullptr;
        void (*copy_construct_view_impl)(const void *context, const ValueTypeRef &binding, void *dst,
                                         const void *memory) = nullptr;
        void (*copy_assign_view_impl)(const void *context, const ValueTypeRef &binding, void *dst,
                                      const void *memory) = nullptr;
        ValueTypeRef (*owning_type_impl)(const void *context, ValueTypeRef view_type) = nullptr;
        bool (*accepts_source_impl)(const void *context, ValueTypeRef binding,
                                    ValueTypeRef source) noexcept = nullptr;
        void (*copy_assign_from_impl)(const void *context, ValueTypeRef binding, void *dst,
                                      ValueTypeRef source, const void *src) = nullptr;
        void (*move_assign_from_impl)(const void *context, ValueTypeRef binding, void *dst,
                                      ValueTypeRef source, void *src) = nullptr;
        ValueTypeRef (*concrete_type_impl)(const void *context, ValueTypeRef binding,
                                           const void *memory) noexcept = nullptr;
        const void *(*concrete_memory_impl)(const void *context, const void *memory) noexcept = nullptr;
        void *(*mutable_concrete_memory_impl)(const void *context, void *memory) noexcept = nullptr;
        std::string (*format_string_impl)(const void *context, const void *memory) = nullptr;
        bool (*can_materialize_source_impl)(const void *context,
                                            ValueTypeRef source,
                                            const void *memory) = nullptr;
        DynamicStorageMetrics (*dynamic_storage_metrics_impl)(const void *context,
                                                              const void *memory) noexcept = nullptr;
        /**
         * Return writable concrete storage, detaching shared representations
         * when required.  This is deliberately a throwing hook: obtaining a
         * writable projection may allocate and copy.
         */
        void *(*writable_concrete_memory_impl)(const void *context, void *memory) = nullptr;

        [[nodiscard]] std::size_t hash(const void *memory) const
        {
            if (memory == nullptr) { throw std::logic_error("ValueOps::hash requires live value memory"); }
            if (hash_impl == nullptr)
            {
                throw std::logic_error("ValueOps::hash is not available for this value type");
            }
            return hash_impl(context, memory);
        }

        [[nodiscard]] bool can_begin_mutation() const noexcept { return allows_mutation; }

        [[nodiscard]] bool equals(const void *lhs, const void *rhs) const
        {
            return equals_impl != nullptr ? equals_impl(context, lhs, rhs) : lhs == rhs;
        }

        [[nodiscard]] std::partial_ordering compare(const void *lhs, const void *rhs) const noexcept
        {
            if (compare_impl == nullptr)
            {
                if (lhs == nullptr && rhs == nullptr) { return std::partial_ordering::equivalent; }
                if (lhs == nullptr) { return std::partial_ordering::less; }
                if (rhs == nullptr) { return std::partial_ordering::greater; }
                return lhs == rhs ? std::partial_ordering::equivalent : std::partial_ordering::unordered;
            }
            return compare_impl(context, lhs, rhs);
        }

        [[nodiscard]] std::string to_string(const void *memory) const
        {
            return to_string_impl != nullptr ? to_string_impl(context, memory) : std::string{};
        }

        [[nodiscard]] std::string format_string(const void *memory) const
        {
            return format_string_impl != nullptr
                       ? format_string_impl(context, memory)
                       : to_string(memory);
        }


        [[nodiscard]] ValueTypeRef owning_type(ValueTypeRef view_type) const
        {
            if (owning_type_impl == nullptr) { return view_type; }
            const auto result = owning_type_impl(context, view_type);
            if (!result)
            {
                throw std::logic_error("ValueOps::owning_type returned an unbound value type");
            }
            return result;
        }

        void copy_construct_view(const ValueTypeRef &binding, void *dst, const void *memory) const
        {
            if (dst == nullptr) { throw std::logic_error("ValueOps::copy_construct_view requires destination memory"); }
            if (memory == nullptr) { throw std::logic_error("ValueOps::copy_construct_view requires live memory"); }
            if (copy_construct_view_impl != nullptr)
            {
                copy_construct_view_impl(context, binding, dst, memory);
                return;
            }
            binding.checked_plan().copy_construct(dst, memory);
        }

        void copy_assign_view(const ValueTypeRef &binding, void *dst, const void *memory) const
        {
            if (dst == nullptr || memory == nullptr)
            {
                throw std::logic_error("ValueOps::copy_assign_view requires live memory");
            }
            if (copy_assign_view_impl != nullptr)
            {
                copy_assign_view_impl(context, binding, dst, memory);
                return;
            }
            binding.checked_plan().copy_assign(dst, memory);
        }

        [[nodiscard]] bool accepts_source(ValueTypeRef binding, ValueTypeRef source) const noexcept
        {
            if (accepts_source_impl != nullptr) { return accepts_source_impl(context, binding, source); }
            return binding && source && binding.plan() == source.plan();
        }

        /**
         * Whether an incomplete indexed source contains enough fields to
         * construct this owning representation. The default is deliberately
         * false: ordinary composite Bundles retain all-fields-valid
         * materialisation semantics.
         */
        [[nodiscard]] bool can_materialize_source(ValueTypeRef source,
                                                  const void *memory) const
        {
            return can_materialize_source_impl != nullptr &&
                   can_materialize_source_impl(context, source, memory);
        }

        [[nodiscard]] bool has_source_materialization_policy() const noexcept
        {
            return can_materialize_source_impl != nullptr;
        }

        void copy_assign_from(ValueTypeRef binding, void *dst, ValueTypeRef source, const void *src) const
        {
            if (!accepts_source(binding, source))
            {
                throw std::invalid_argument(
                    "ValueOps::copy_assign_from received incompatible bindings: target " +
                    std::string{binding && binding.schema() ? binding.schema()->name() : "<invalid>"} +
                    ", source " +
                    std::string{source && source.schema() ? source.schema()->name() : "<invalid>"});
            }
            if (copy_assign_from_impl != nullptr)
            {
                copy_assign_from_impl(context, binding, dst, source, src);
                return;
            }
            binding.checked_plan().copy_assign(dst, src);
        }

        void move_assign_from(ValueTypeRef binding, void *dst, ValueTypeRef source, void *src) const
        {
            if (!accepts_source(binding, source))
            {
                throw std::invalid_argument(
                    "ValueOps::move_assign_from received incompatible bindings: target " +
                    std::string{binding && binding.schema() ? binding.schema()->name() : "<invalid>"} +
                    ", source " +
                    std::string{source && source.schema() ? source.schema()->name() : "<invalid>"});
            }
            if (move_assign_from_impl != nullptr)
            {
                move_assign_from_impl(context, binding, dst, source, src);
                return;
            }
            binding.checked_plan().move_assign(dst, src);
        }

        [[nodiscard]] ValueTypeRef concrete_type(ValueTypeRef binding, const void *memory) const noexcept
        {
            return concrete_type_impl != nullptr ? concrete_type_impl(context, binding, memory) : binding;
        }

        [[nodiscard]] const void *concrete_memory(const void *memory) const noexcept
        {
            return concrete_memory_impl != nullptr ? concrete_memory_impl(context, memory) : memory;
        }

        [[nodiscard]] void *mutable_concrete_memory(void *memory) const noexcept
        {
            return mutable_concrete_memory_impl != nullptr
                       ? mutable_concrete_memory_impl(context, memory)
                       : memory;
        }

        [[nodiscard]] void *writable_concrete_memory(void *memory) const
        {
            return writable_concrete_memory_impl != nullptr
                       ? writable_concrete_memory_impl(context, memory)
                       : mutable_concrete_memory(memory);
        }

        /** Heap storage exclusively owned by this value payload. */
        [[nodiscard]] DynamicStorageMetrics dynamic_storage_metrics(const void *memory) const noexcept
        {
            return memory != nullptr && dynamic_storage_metrics_impl != nullptr
                       ? dynamic_storage_metrics_impl(context, memory)
                       : DynamicStorageMetrics{};
        }
    };

    static_assert(offsetof(ValueOps, kind) == 0);

    namespace value_ops_detail
    {
        template <typename T>
        [[nodiscard]] std::optional<std::partial_ordering> null_order(const T *lhs, const T *rhs) noexcept
        {
            if (lhs == nullptr && rhs == nullptr) { return std::partial_ordering::equivalent; }
            if (lhs == nullptr) { return std::partial_ordering::less; }
            if (rhs == nullptr) { return std::partial_ordering::greater; }
            return std::nullopt;
        }

        [[nodiscard]] inline std::optional<std::partial_ordering> null_order(ValueTypeRef lhs,
                                                                             ValueTypeRef rhs) noexcept
        {
            if (!lhs && !rhs) { return std::partial_ordering::equivalent; }
            if (!lhs) { return std::partial_ordering::less; }
            if (!rhs) { return std::partial_ordering::greater; }
            return std::nullopt;
        }

        template <detail::Hashable T>
        std::size_t hash_thunk(const void *, const void *memory)
        {
            return std::hash<T>{}(*static_cast<const T *>(memory));
        }

        template <typename T>
        bool equals_thunk(const void *, const void *lhs, const void *rhs) noexcept
        {
            if constexpr (requires(const T &a, const T &b) { { a == b } -> std::convertible_to<bool>; })
            {
                return *static_cast<const T *>(lhs) == *static_cast<const T *>(rhs);
            }
            else
            {
                return lhs == rhs;
            }
        }

        template <typename T>
        std::partial_ordering compare_thunk(const void *, const void *lhs, const void *rhs) noexcept
        {
            if constexpr (requires(const T &a, const T &b) {
                              { a <=> b } -> std::convertible_to<std::partial_ordering>;
                          })
            {
                return *static_cast<const T *>(lhs) <=> *static_cast<const T *>(rhs);
            }
            else if constexpr (requires(const T &a, const T &b) { { a < b } -> std::convertible_to<bool>; })
            {
                const T &a = *static_cast<const T *>(lhs);
                const T &b = *static_cast<const T *>(rhs);
                if (a < b) { return std::partial_ordering::less; }
                if (b < a) { return std::partial_ordering::greater; }
                return std::partial_ordering::equivalent;
            }
            else
            {
                return lhs == rhs ? std::partial_ordering::equivalent : std::partial_ordering::unordered;
            }
        }

        template <typename T>
        [[nodiscard]] constexpr auto hash_impl_for() noexcept
        {
            if constexpr (detail::Hashable<T>)
            {
                return &hash_thunk<T>;
            }
            else
            {
                return static_cast<std::size_t (*)(const void *, const void *)>(nullptr);
            }
        }

        template <typename T>
        [[nodiscard]] constexpr auto equals_impl_for() noexcept
        {
            if constexpr (detail::Equatable<T>)
            {
                return &equals_thunk<T>;
            }
            else
            {
                return static_cast<bool (*)(const void *, const void *, const void *)>(nullptr);
            }
        }

        template <typename T>
        [[nodiscard]] constexpr auto compare_impl_for() noexcept
        {
            if constexpr (detail::Comparable<T>)
            {
                return &compare_thunk<T>;
            }
            else
            {
                return static_cast<std::partial_ordering (*)(const void *, const void *, const void *) noexcept>(
                    nullptr);
            }
        }

        template <typename T>
        std::string to_string_thunk(const void *, const void *memory)
        {
            if constexpr (std::is_same_v<T, std::string>)
            {
                return *static_cast<const std::string *>(memory);
            }
            else if constexpr (std::is_same_v<T, bool>)
            {
                return *static_cast<const bool *>(memory) ? "true" : "false";
            }
            else if constexpr (std::is_integral_v<T> && sizeof(T) == 1)
            {
                // 1-byte integers (int8/uint8/char) stream as characters via
                // ``operator<<``; render their numeric value instead.
                return std::to_string(static_cast<long long>(*static_cast<const T *>(memory)));
            }
            else if constexpr (std::is_floating_point_v<T>)
            {
                // SHORTEST ROUND-TRIP, not the stream default of six
                // significant figures. ``os << 1.0/3`` yields "0.333333", so
                // every string built from a double was silently truncated and
                // str_ then cast_(float, ...) did not return the value it
                // started from (issue #831). fmt's default float formatting is
                // the shortest form that reads back exactly, which is also the
                // rule Python's repr uses.
                return fmt::format("{}", *static_cast<const T *>(memory));
            }
            else if constexpr (requires(const T &v, std::ostringstream &os) { os << v; })
            {
                std::ostringstream os;
                os << *static_cast<const T *>(memory);
                return os.str();
            }
            else
            {
                const char *type_name = typeid(T).name();
                std::string result;
                result.reserve(std::char_traits<char>::length(type_name) + 2);
                result.push_back('<');
                result.append(type_name);
                result.push_back('>');
                return result;
            }
        }

        template <typename T>
        std::string format_string_thunk(const void *context, const void *memory)
        {
            if constexpr (std::is_same_v<T, bool>)
            {
                return *static_cast<const bool *>(memory) ? "True" : "False";
            }
            return to_string_thunk<T>(context, memory);
        }

        [[nodiscard]] inline DynamicStorageMetrics string_dynamic_storage_metrics(
            const std::string &value) noexcept
        {
            const auto object_begin = reinterpret_cast<std::uintptr_t>(std::addressof(value));
            const auto object_end   = object_begin + sizeof(value);
            const auto data         = reinterpret_cast<std::uintptr_t>(value.data());
            if (data >= object_begin && data < object_end) { return {}; }

            return {
                .live_bytes = value.size() + 1,
                .reserved_bytes = value.capacity() + 1,
            };
        }

        template <typename T>
        [[nodiscard]] DynamicStorageMetrics dynamic_storage_metrics_thunk(
            const void *, const void *memory) noexcept
        {
            if constexpr (std::is_same_v<T, std::string>)
            {
                return string_dynamic_storage_metrics(*static_cast<const std::string *>(memory));
            }
            else if constexpr (std::is_same_v<T, Bytes>)
            {
                return string_dynamic_storage_metrics(static_cast<const Bytes *>(memory)->data);
            }
            else
            {
                return {};
            }
        }

    }  // namespace value_ops_detail

    /**
     * Synthesise the canonical ``ValueOps`` for a C++ type ``T``.
     *
     * The returned reference is stable for the program lifetime: each
     * instantiation has its own function-local-static, so two callers asking
     * for ``ops_for<std::int32_t>()`` get the same address. ``TypeRegistry::register_scalar``
     * uses this helper to pair an atomic schema with its ops at registration.
     */
    template <typename T>
    [[nodiscard]] HGRAPH_NOINLINE inline const ValueOps &ops_for() noexcept
    {
        static const ValueOps ops{
            .kind = ValueOpsKind::Base,
            .context = nullptr,
            .allows_mutation = true,
            .hash_impl = value_ops_detail::hash_impl_for<T>(),
            .equals_impl = value_ops_detail::equals_impl_for<T>(),
            .compare_impl = value_ops_detail::compare_impl_for<T>(),
            .to_string_impl = &value_ops_detail::to_string_thunk<T>,
            .to_python_impl = &python_ops_detail::scalar_to_python<T>,
            .from_python_impl = &python_ops_detail::scalar_from_python<T>,
            .to_python_buffer_impl = &python_ops_detail::scalar_to_python_buffer<T>,
            .format_string_impl = &value_ops_detail::format_string_thunk<T>,
            .dynamic_storage_metrics_impl = &value_ops_detail::dynamic_storage_metrics_thunk<T>,
        };
        return ops;
    }
}  // namespace hgraph

#endif  // HGRAPH_CPP_ROOT_VALUE_OPS_H
