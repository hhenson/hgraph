#ifndef HGRAPH_CPP_ROOT_VALUE_OPS_H
#define HGRAPH_CPP_ROOT_VALUE_OPS_H

#include <hgraph/config.h>
#include <hgraph/hgraph_export.h>
#include <hgraph/types/metadata/value_type_meta_data.h>
#include <hgraph/types/primitive_types.h>
#include <hgraph/types/storage_metrics.h>
#include <hgraph/types/utils/memory_utils.h>
#include <hgraph/types/value/value_type_ref.h>

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

#if HGRAPH_ENABLE_PYTHON_USER_NODES
#include <hgraph/python/chrono.h>
#include <nanobind/ndarray.h>
#include <nanobind/nanobind.h>
#include <nanobind/stl/string.h>

namespace nb = nanobind;
#endif

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
    inline constexpr std::uint16_t VALUE_OPS_ABI_VERSION = 5;

    struct ValueOps;
#if HGRAPH_ENABLE_PYTHON_USER_NODES
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
#endif

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
#if HGRAPH_ENABLE_PYTHON_USER_NODES
        nb::object (*to_python_impl)(const void *context, const void *memory) = nullptr;
        void (*from_python_impl)(const void *context, const ValueTypeRef &binding, void *memory,
                                 nb::handle source) = nullptr;
        nb::object (*to_python_buffer_impl)(const void *context, const ValueTypeRef &binding,
                                            const ValueArraySource &source) = nullptr;
#endif
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

#if HGRAPH_ENABLE_PYTHON_USER_NODES
        [[nodiscard]] nb::object to_python(const void *memory) const
        {
            if (to_python_impl == nullptr)
            {
                throw std::logic_error("ValueOps::to_python is not available for this value type");
            }
            return to_python_impl(context, memory);
        }

        void from_python(const ValueTypeRef &binding, void *memory, nb::handle source) const
        {
            if (from_python_impl == nullptr)
            {
                throw std::logic_error("ValueOps::from_python is not available for this value type");
            }
            from_python_impl(context, binding, memory, source);
        }

        [[nodiscard]] bool can_to_python_buffer(const ValueTypeRef &binding) const noexcept
        {
            return binding.schema() != nullptr && binding.schema()->is_buffer_compatible() &&
                   to_python_buffer_impl != nullptr;
        }

        [[nodiscard]] nb::object to_python_buffer(const ValueTypeRef &binding,
                                                  const ValueArraySource &source) const
        {
            if (!can_to_python_buffer(binding))
            {
                throw std::logic_error("ValueOps::to_python_buffer is not available for this value type");
            }
            if (source.element_at == nullptr && source.first.size + source.second.size != source.size)
            {
                throw std::logic_error("ValueOps::to_python_buffer requires an element accessor or complete spans");
            }
            return to_python_buffer_impl(context, binding, source);
        }
#endif

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

#if HGRAPH_ENABLE_PYTHON_USER_NODES
    /**
     * Python-conversion customization point for scalar types the generic
     * nanobind cast cannot handle (the type-erasure rule: conversion binds
     * onto the type's OPS at registration - specializations must be visible
     * wherever ``register_scalar<T>`` / ``ops_for<T>`` first instantiates).
     * Provide:
     *   static nb::object to_python(const T &);
     *   static T          from_python(nb::handle);
     */
    template <typename T>
    struct python_conversion_traits;

    template <>
    struct python_conversion_traits<Time>
    {
        static nb::object to_python(const Time &value)
        {
            const auto  micro   = value.microseconds;
            const auto  seconds = micro / 1'000'000;
            return nb::module_::import_("datetime")
                .attr("time")(static_cast<int>(seconds / 3600), static_cast<int>((seconds / 60) % 60),
                              static_cast<int>(seconds % 60), static_cast<int>(micro % 1'000'000));
        }

        static Time from_python(nb::handle source)
        {
            if (nb::hasattr(source, "tzinfo") &&
                !source.attr("tzinfo").is_none())
            {
                throw nb::type_error(
                    "timezone-aware time values require a zoned time scalar");
            }
            if (nb::hasattr(source, "utcoffset"))
            {
                nb::object offset = source.attr("utcoffset")();
                if (!offset.is_none())
                {
                    throw nb::type_error(
                        "timezone-aware time values require a zoned time scalar");
                }
            }
            const auto hours   = nb::cast<std::int64_t>(source.attr("hour"));
            const auto minutes = nb::cast<std::int64_t>(source.attr("minute"));
            const auto seconds = nb::cast<std::int64_t>(source.attr("second"));
            const auto micro   = nb::cast<std::int64_t>(source.attr("microsecond"));
            return Time{((hours * 60 + minutes) * 60 + seconds) * 1'000'000 + micro};
        }
    };

    template <>
    struct python_conversion_traits<Bytes>
    {
        static nb::object to_python(const Bytes &value)
        {
            return nb::steal(PyBytes_FromStringAndSize(value.data.data(),
                                                       static_cast<Py_ssize_t>(value.data.size())));
        }

        static Bytes from_python(nb::handle source)
        {
            char       *buffer = nullptr;
            Py_ssize_t  length = 0;
            if (PyBytes_AsStringAndSize(source.ptr(), &buffer, &length) != 0)
            {
                throw nb::python_error();
            }
            return Bytes{std::string{buffer, static_cast<std::size_t>(length)}};
        }
    };

#endif  // HGRAPH_ENABLE_PYTHON_USER_NODES

    namespace value_ops_detail
    {
#if HGRAPH_ENABLE_PYTHON_USER_NODES
        template <typename T>
        constexpr bool python_scalar_castable =
            std::is_arithmetic_v<T> || std::is_same_v<T, std::string> || std::is_same_v<T, Date> ||
            std::is_same_v<T, DateTime> || std::is_same_v<T, TimeDelta>;

        template <typename T>
        concept has_python_conversion_traits = requires(const T &value, nb::handle source) {
            { python_conversion_traits<T>::to_python(value) } -> std::same_as<nb::object>;
            { python_conversion_traits<T>::from_python(source) } -> std::same_as<T>;
        };

        template <typename T>
        nb::object to_python_thunk(const void *, const void *memory)
        {
            if constexpr (python_scalar_castable<T>)
            {
                return nb::cast(*static_cast<const T *>(memory));
            }
            else if constexpr (has_python_conversion_traits<T>)
            {
                return python_conversion_traits<T>::to_python(*static_cast<const T *>(memory));
            }
            else
            {
                throw std::logic_error("ValueOps::to_python is not available for this scalar type");
            }
        }

        template <typename T>
        void from_python_thunk(const void *, const ValueTypeRef &, void *memory, nb::handle source)
        {
            if constexpr (python_scalar_castable<T>)
            {
                if constexpr (std::is_arithmetic_v<T>)
                {
                    // Pythonic strictness: numeric scalars never convert from
                    // strings (PyNumber coercion would accept "1"); numeric
                    // cross-conversions stay permitted.
                    if (nb::isinstance<nb::str>(source) || nb::isinstance<nb::bytes>(source))
                    {
                        throw nb::type_error("cannot convert a python string to a numeric scalar");
                    }
                }
                *static_cast<T *>(memory) = nb::cast<T>(source);
            }
            else if constexpr (has_python_conversion_traits<T>)
            {
                *static_cast<T *>(memory) = python_conversion_traits<T>::from_python(source);
            }
            else
            {
                throw std::logic_error("ValueOps::from_python is not available for this scalar type");
            }
        }

        template <typename T>
        struct python_buffer_traits
        {
            using storage_type = T;

            static storage_type convert(const void *memory)
            {
                return *static_cast<const T *>(memory);
            }

            [[nodiscard]] static constexpr const char *numpy_view_dtype() noexcept { return nullptr; }
        };

        template <>
        struct python_buffer_traits<DateTime>
        {
            using storage_type = std::int64_t;

            static storage_type convert(const void *memory)
            {
                return static_cast<storage_type>(
                    static_cast<const DateTime *>(memory)->time_since_epoch().count());
            }

            [[nodiscard]] static constexpr const char *numpy_view_dtype() noexcept { return "datetime64[us]"; }
        };

        template <>
        struct python_buffer_traits<TimeDelta>
        {
            using storage_type = std::int64_t;

            static storage_type convert(const void *memory)
            {
                return static_cast<storage_type>(static_cast<const TimeDelta *>(memory)->count());
            }

            [[nodiscard]] static constexpr const char *numpy_view_dtype() noexcept { return "timedelta64[us]"; }
        };

        template <>
        struct python_buffer_traits<Date>
        {
            using storage_type = std::int64_t;

            static storage_type convert(const void *memory)
            {
                const auto days = std::chrono::sys_days{*static_cast<const Date *>(memory)}
                                      .time_since_epoch()
                                      .count();
                return static_cast<storage_type>(days);
            }

            [[nodiscard]] static constexpr const char *numpy_view_dtype() noexcept { return "datetime64[D]"; }
        };

        template <typename Storage>
        void copy_value_array_span(Storage *&dst, ValueArraySpan span)
        {
            if (span.size == 0) { return; }
            if (span.data == nullptr)
            {
                throw std::logic_error("ValueOps::to_python_buffer span has null data");
            }

            if (span.stride == sizeof(Storage))
            {
                std::memcpy(dst, span.data, span.size * sizeof(Storage));
                dst += span.size;
                return;
            }

            const auto *src = static_cast<const std::byte *>(span.data);
            for (std::size_t index = 0; index < span.size; ++index)
            {
                std::memcpy(dst + index, src + index * span.stride, sizeof(Storage));
            }
            dst += span.size;
        }

        template <typename Storage>
        void delete_python_buffer(void *memory) noexcept
        {
            delete[] static_cast<Storage *>(memory);
        }

        template <typename T>
        nb::object to_python_buffer_thunk(const void *,
                                          const ValueTypeRef &,
                                          const ValueArraySource &source)
        {
            if constexpr (detail::buffer_compatible_type<T>)
            {
                using traits      = python_buffer_traits<T>;
                using storage_type = typename traits::storage_type;

                auto          owner = std::make_unique<storage_type[]>(std::max<std::size_t>(source.size, 1));
                storage_type *data  = owner.get();

                constexpr bool direct_copy =
                    std::is_same_v<std::remove_cv_t<T>, storage_type> && std::is_trivially_copyable_v<storage_type>;
                if constexpr (direct_copy)
                {
                    if (source.first.size + source.second.size == source.size)
                    {
                        storage_type *dst = data;
                        copy_value_array_span<storage_type>(dst, source.first);
                        copy_value_array_span<storage_type>(dst, source.second);
                    }
                    else
                    {
                        if (source.element_at == nullptr)
                        {
                            throw std::logic_error("ValueOps::to_python_buffer requires an element accessor");
                        }
                        for (std::size_t index = 0; index < source.size; ++index)
                        {
                            data[index] = traits::convert(source.element_at(source.owner, index));
                        }
                    }
                }
                else
                {
                    if (source.element_at == nullptr)
                    {
                        throw std::logic_error("ValueOps::to_python_buffer requires an element accessor");
                    }
                    for (std::size_t index = 0; index < source.size; ++index)
                    {
                        data[index] = traits::convert(source.element_at(source.owner, index));
                    }
                }

                storage_type *owned = owner.release();
                nb::capsule   owner_capsule{owned, &delete_python_buffer<storage_type>};
                nb::ndarray<nb::numpy, const storage_type, nb::ndim<1>> array{owned, {source.size}, owner_capsule};
                nb::object result = array.cast();
                if constexpr (traits::numpy_view_dtype() != nullptr)
                {
                    return result.attr("view")(nb::str{traits::numpy_view_dtype()});
                }
                return result;
            }
            else
            {
                throw std::logic_error("ValueOps::to_python_buffer is not available for this scalar type");
            }
        }
#endif
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
    [[nodiscard]] inline const ValueOps &ops_for() noexcept
    {
        static const ValueOps ops{
            .kind = ValueOpsKind::Base,
            .context = nullptr,
            .allows_mutation = true,
            .hash_impl = value_ops_detail::hash_impl_for<T>(),
            .equals_impl = value_ops_detail::equals_impl_for<T>(),
            .compare_impl = value_ops_detail::compare_impl_for<T>(),
            .to_string_impl = &value_ops_detail::to_string_thunk<T>,
#if HGRAPH_ENABLE_PYTHON_USER_NODES
            .to_python_impl = &value_ops_detail::to_python_thunk<T>,
            .from_python_impl = &value_ops_detail::from_python_thunk<T>,
            .to_python_buffer_impl = &value_ops_detail::to_python_buffer_thunk<T>,
#endif
            .format_string_impl = &value_ops_detail::format_string_thunk<T>,
            .dynamic_storage_metrics_impl = &value_ops_detail::dynamic_storage_metrics_thunk<T>,
        };
        return ops;
    }
#if HGRAPH_ENABLE_PYTHON_USER_NODES
    /** Python-enum conversion hooks: installed by the python module (which
        owns the meta -> python-Enum-class registry); the core enum ops call
        through these slots. */
    using EnumToPythonFn   = nanobind::object (*)(const ValueTypeMetaData *meta, long long value);
    using EnumFromPythonFn = long long (*)(const ValueTypeMetaData *meta, nanobind::handle source);
    HGRAPH_EXPORT EnumToPythonFn &enum_to_python_slot() noexcept;
    HGRAPH_EXPORT EnumFromPythonFn &enum_from_python_slot() noexcept;
#endif
}  // namespace hgraph

#endif  // HGRAPH_CPP_ROOT_VALUE_OPS_H
