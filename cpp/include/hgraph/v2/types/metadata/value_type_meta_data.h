//
// Created by Howard Henson on 20/04/2026.
//

#ifndef HGRAPH_CPP_ROOT_VALUE_TYPE_META_DATA_H
#define HGRAPH_CPP_ROOT_VALUE_TYPE_META_DATA_H

#include <hgraph/util/date_time.h>
#include <hgraph/v2/types/metadata/type_meta_data.h>

#include <concepts>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <type_traits>

namespace hgraph::v2
{
    struct ValueTypeMetaData;

    enum class ValueTypeKind : uint8_t
    {
        Atomic,
        Tuple,
        Bundle,
        List,
        Set,
        Map,
        CyclicBuffer,
        Queue,
    };

    enum class ValueTypeFlags : uint32_t
    {
        None = 0,
        TriviallyConstructible = 1u << 0,
        TriviallyDestructible = 1u << 1,
        TriviallyCopyable = 1u << 2,
        Hashable = 1u << 3,
        Comparable = 1u << 4,
        Equatable = 1u << 5,
        BufferCompatible = 1u << 6,
        VariadicTuple = 1u << 7,
    };

    constexpr ValueTypeFlags operator|(ValueTypeFlags lhs, ValueTypeFlags rhs) noexcept
    {
        return static_cast<ValueTypeFlags>(static_cast<uint32_t>(lhs) | static_cast<uint32_t>(rhs));
    }

    constexpr ValueTypeFlags operator&(ValueTypeFlags lhs, ValueTypeFlags rhs) noexcept
    {
        return static_cast<ValueTypeFlags>(static_cast<uint32_t>(lhs) & static_cast<uint32_t>(rhs));
    }

    constexpr ValueTypeFlags operator~(ValueTypeFlags value) noexcept
    {
        return static_cast<ValueTypeFlags>(~static_cast<uint32_t>(value));
    }

    constexpr ValueTypeFlags &operator|=(ValueTypeFlags &lhs, ValueTypeFlags rhs) noexcept
    {
        lhs = lhs | rhs;
        return lhs;
    }

    constexpr bool has_flag(ValueTypeFlags flags, ValueTypeFlags flag) noexcept
    {
        return (static_cast<uint32_t>(flags) & static_cast<uint32_t>(flag)) != 0u;
    }

    struct ValueFieldMetaData
    {
        const char *name{nullptr};
        size_t index{0};
        size_t offset{0};
        const ValueTypeMetaData *type{nullptr};
    };

    struct ValueTypeMetaData final : TypeMetaData
    {
        constexpr ValueTypeMetaData() noexcept
            : TypeMetaData(MetaCategory::Value)
        {
        }

        constexpr ValueTypeMetaData(ValueTypeKind kind_,
                                    size_t size_,
                                    size_t alignment_,
                                    ValueTypeFlags flags_,
                                    const char *display_name_ = nullptr) noexcept
            : TypeMetaData(MetaCategory::Value, display_name_)
            , size(size_)
            , alignment(alignment_)
            , kind(kind_)
            , flags(flags_)
        {
        }

        size_t size{0};
        size_t alignment{1};
        ValueTypeKind kind{ValueTypeKind::Atomic};
        ValueTypeFlags flags{ValueTypeFlags::None};
        const ValueTypeMetaData *element_type{nullptr};
        const ValueTypeMetaData *key_type{nullptr};
        const ValueFieldMetaData *fields{nullptr};
        size_t field_count{0};
        size_t fixed_size{0};

        [[nodiscard]] constexpr bool is_fixed_size() const noexcept { return fixed_size > 0; }
        [[nodiscard]] constexpr bool has(ValueTypeFlags flag) const noexcept { return has_flag(flags, flag); }
        [[nodiscard]] constexpr bool is_trivially_constructible() const noexcept
        {
            return has(ValueTypeFlags::TriviallyConstructible);
        }
        [[nodiscard]] constexpr bool is_trivially_destructible() const noexcept
        {
            return has(ValueTypeFlags::TriviallyDestructible);
        }
        [[nodiscard]] constexpr bool is_trivially_copyable() const noexcept
        {
            return has(ValueTypeFlags::TriviallyCopyable);
        }
        [[nodiscard]] constexpr bool is_hashable() const noexcept { return has(ValueTypeFlags::Hashable); }
        [[nodiscard]] constexpr bool is_comparable() const noexcept { return has(ValueTypeFlags::Comparable); }
        [[nodiscard]] constexpr bool is_equatable() const noexcept { return has(ValueTypeFlags::Equatable); }
        [[nodiscard]] constexpr bool is_buffer_compatible() const noexcept
        {
            return has(ValueTypeFlags::BufferCompatible);
        }
        [[nodiscard]] constexpr bool is_variadic_tuple() const noexcept
        {
            return has(ValueTypeFlags::VariadicTuple);
        }
    };

    namespace detail
    {
        template <typename T>
        concept Hashable = requires(const T &value) {
            { std::hash<T>{}(value) } -> std::convertible_to<size_t>;
        };

        template <typename T>
        concept Equatable = requires(const T &lhs, const T &rhs) {
            { lhs == rhs } -> std::convertible_to<bool>;
        };

        template <typename T>
        concept Comparable = requires(const T &lhs, const T &rhs) {
            { lhs < rhs } -> std::convertible_to<bool>;
        };

        template <typename T>
        constexpr bool buffer_compatible_type =
            std::is_arithmetic_v<T> || std::is_same_v<T, engine_date_t> || std::is_same_v<T, engine_time_t> ||
            std::is_same_v<T, engine_time_delta_t>;
    }  // namespace detail

    template <typename T>
    constexpr ValueTypeFlags compute_scalar_flags() noexcept
    {
        ValueTypeFlags flags = ValueTypeFlags::None;

        if constexpr (std::is_trivially_default_constructible_v<T>)
        {
            flags |= ValueTypeFlags::TriviallyConstructible;
        }
        if constexpr (std::is_trivially_destructible_v<T>)
        {
            flags |= ValueTypeFlags::TriviallyDestructible;
        }
        if constexpr (std::is_trivially_copyable_v<T>)
        {
            flags |= ValueTypeFlags::TriviallyCopyable;
        }
        if constexpr (detail::Hashable<T>)
        {
            flags |= ValueTypeFlags::Hashable;
        }
        if constexpr (detail::Equatable<T>)
        {
            flags |= ValueTypeFlags::Equatable;
        }
        if constexpr (detail::Comparable<T>)
        {
            flags |= ValueTypeFlags::Comparable;
        }
        if constexpr (detail::buffer_compatible_type<T>)
        {
            flags |= ValueTypeFlags::BufferCompatible;
        }

        return flags;
    }
}  // namespace hgraph::v2

#endif  // HGRAPH_CPP_ROOT_VALUE_TYPE_META_DATA_H
