//
// Created by Howard Henson on 20/04/2026.
//

#ifndef HGRAPH_CPP_ROOT_VALUE_TYPE_META_DATA_H
#define HGRAPH_CPP_ROOT_VALUE_TYPE_META_DATA_H

#include <hgraph/util/date_time.h>
#include <hgraph/types/metadata/type_record.h>

#include <concepts>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <optional>
#include <stdexcept>
#include <string_view>
#include <type_traits>
#include <vector>

namespace hgraph
{
    struct ValueTypeMetaData;

    /**
     * Mutable registration-time ancestry for a named Bundle schema.
     *
     * The metadata object itself has a stable address for the process lifetime.
     * Parent/child vectors may grow as additional bundle classes are registered;
     * graph compilation copies the visible closure into an immutable realization.
     */
    struct BundleHierarchyMetaData
    {
        const char *namespace_name{nullptr};
        const char *local_name{nullptr};
        std::vector<const ValueTypeMetaData *> parents{};
        std::vector<const ValueTypeMetaData *> children{};
        std::vector<const ValueTypeMetaData *> generic_arguments{};
        bool is_abstract{false};
        const char *discriminator{"__type__"};
        std::uint64_t generation{0};
    };

    /**
     * Nine value-layer kinds, matching the ``Value Kinds`` section of the
     * developer guide. Every ``ValueTypeMetaData`` carries one of these
     * tags; consumers dispatch on it to interpret the rest of the metadata
     * fields.
     */
    enum class ValueTypeKind : std::uint8_t
    {
        /** Single scalar (integer, floating-point, bool, string, date/time). */
        Atomic = 0,
        /** Fixed-arity ordered fields, addressed by index. */
        Tuple = 1,
        /** Named tuple; fields addressed by name with field-name metadata preserved. */
        Bundle = 2,
        /** Ordered sequence of one element type; ``fixed_size`` distinguishes static vs dynamic. */
        List = 3,
        /** Unordered set of unique elements of one type. */
        Set = 4,
        /** Key-value mapping with one key type and one value type. */
        Map = 5,
        /** Fixed-capacity ring buffer of one element type. */
        CyclicBuffer = 6,
        /** FIFO queue with capacity and ordering. */
        Queue = 7,
        /**
         * Type-erased "any value" box. Compile-time schema knowledge ends
         * here: the box stores an embedded owning ``Value`` whose own schema
         * is only known at run time. Storage is the ``Value`` handle; the
         * contained value's memory is allocated on demand when content is
         * assigned, after which it carries its own schema and owned memory.
         * Carries no ``element_type`` / ``key_type`` / ``fields`` — it is
         * unconstrained. The slot type for heterogeneous mutable containers
         * (and the eventual analogue of a generic / Python ``object``).
         */
        Any = 8,
    };

    /** Convert a compact kind when it belongs to the value family. */
    [[nodiscard]] constexpr std::optional<ValueTypeKind> try_value_type_kind(TypeKind kind) noexcept
    {
        if (kind > static_cast<TypeKind>(ValueTypeKind::Any)) { return std::nullopt; }
        return static_cast<ValueTypeKind>(kind);
    }

    /** Convert the compact schema kind to the value-family enum, rejecting foreign values. */
    [[nodiscard]] constexpr ValueTypeKind checked_value_type_kind(TypeKind kind)
    {
        const auto result = try_value_type_kind(kind);
        if (!result.has_value()) { throw std::invalid_argument("invalid value type kind"); }
        return *result;
    }

    /**
     * Capability flags carried on each ``ValueTypeMetaData``.
     *
     * Trivially-* flags drive fast paths (memcpy moves, no-destroy on
     * teardown). The hashable / equatable / comparable bits indicate which
     * ``ValueOps`` hooks the registry should generate for the type.
     * ``BufferCompatible`` marks scalars that can be exposed as a
     * contiguous Arrow-style buffer. ``VariadicTuple`` marks tuple
     * metadata used as a variadic-element list.
     */
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
        /** Container schema backed by structurally-mutable (slot-store) storage. */
        Mutable = 1u << 8,
        /** List/tuple whose elements may be UNSET (holes). Known-size uses a
            pre-allocated bitset (like a fixed tuple / TSB); unknown-size uses
            the sul-style bitset on the compact list. */
        Nullable = 1u << 9,
        /** A NAMED atomic whose payload is a member's assigned integer value;
            the ordered member table rides ``fields`` (name + assigned value).
            Nominal identity, like a named bundle. */
        Enum = 1u << 10,
        /** Bundle-shaped indirection whose inline storage is exactly one owner pointer. */
        Owned = 1u << 11,
        /** A list schema that represents one dimension of a shaped numerical array. */
        ShapedArray = 1u << 12,
    };

    /** Bitwise OR over ``ValueTypeFlags``. */
    constexpr ValueTypeFlags operator|(ValueTypeFlags lhs, ValueTypeFlags rhs) noexcept
    {
        return static_cast<ValueTypeFlags>(static_cast<uint32_t>(lhs) | static_cast<uint32_t>(rhs));
    }

    /** Bitwise AND over ``ValueTypeFlags``. */
    constexpr ValueTypeFlags operator&(ValueTypeFlags lhs, ValueTypeFlags rhs) noexcept
    {
        return static_cast<ValueTypeFlags>(static_cast<uint32_t>(lhs) & static_cast<uint32_t>(rhs));
    }

    /** Bitwise NOT over ``ValueTypeFlags``. */
    constexpr ValueTypeFlags operator~(ValueTypeFlags value) noexcept
    {
        return static_cast<ValueTypeFlags>(~static_cast<uint32_t>(value));
    }

    /** In-place OR-assign for ``ValueTypeFlags``. */
    constexpr ValueTypeFlags &operator|=(ValueTypeFlags &lhs, ValueTypeFlags rhs) noexcept
    {
        lhs = lhs | rhs;
        return lhs;
    }

    /** True when every bit in ``flag`` is also set in ``flags``. */
    constexpr bool has_flag(ValueTypeFlags flags, ValueTypeFlags flag) noexcept
    {
        return (static_cast<uint32_t>(flags) & static_cast<uint32_t>(flag)) != 0u;
    }

    /**
     * Field descriptor for ``Tuple`` and ``Bundle`` schemas.
     *
     * ``name`` is null for tuples and non-null (registry-interned) for
     * bundles. Layout information (byte offsets within the composite) is
     * not carried here; it lives on the corresponding
     * ``MemoryUtils::CompositeComponent`` of the bound storage plan.
     */
    struct ValueFieldMetaData
    {
        /** Field name; null for tuple fields, registry-interned for bundle fields. */
        const char *name{nullptr};
        /** Field index in declaration order. */
        size_t index{0};
        /** For ENUM members only: the member's ASSIGNED integer value (the
            stored payload; comparison order). Zero for bundle/tuple fields. */
        long long enum_value{0};
        /** Schema for this field. */
        const ValueTypeMetaData *type{nullptr};
    };

    /**
     * Schema descriptor for value-layer types.
     *
     * Describes what a value *is*: kind, capability flags, and the
     * kind-specific child references (element/key types for collections,
     * field arrays for tuples and bundles, ``fixed_size`` for sized lists
     * and ring buffers). Memory-layout information — byte size, alignment,
     * field offsets, lifecycle hooks — belongs to the matching
     * ``MemoryUtils::StoragePlan`` and is not carried on the schema.
     *
     * Always interned by ``TypeRegistry``: equivalent descriptions resolve
     * to the same pointer.
     */
    struct ValueTypeMetaData final
    {
        /** Default-construct an invalid descriptor for factory population. */
        constexpr ValueTypeMetaData() noexcept = default;

        /** Construct a value-family descriptor with kind, flags, and canonical label. */
        constexpr ValueTypeMetaData(ValueTypeKind kind_,
                                    ValueTypeFlags flags_,
                                    const char *label) noexcept
            : header(TypeFamily::Value, static_cast<TypeKind>(kind_), label)
            , flags(flags_)
        {
        }

        /** Common family/kind/label prefix shared by all unified schemas. */
        SchemaHeader header{};
        /** Capability flags. */
        ValueTypeFlags flags{ValueTypeFlags::None};
        /** Element type for ``List``, ``Set``, ``Map`` (value), ``CyclicBuffer``, ``Queue``. */
        const ValueTypeMetaData *element_type{nullptr};
        /** Key type for ``Map``. */
        const ValueTypeMetaData *key_type{nullptr};
        /** Field array for ``Tuple`` and ``Bundle`` (registry-owned, ``field_count`` entries). */
        const ValueFieldMetaData *fields{nullptr};
        /** Number of entries in ``fields``. */
        size_t field_count{0};
        /** Fixed size for static lists, ring buffer capacity for ``CyclicBuffer``, max queue size for ``Queue``. */
        size_t fixed_size{0};
        /**
         * For a *named* ``Bundle``, points to the structural (un-named) bundle
         * with the same field list. ``nullptr`` on the un-named bundle itself
         * and on every non-bundle metadata. Used to distinguish nominal
         * identity (two named bundles with the same fields but different
         * names are separate) from structural identity (an un-named bundle is
         * shared by every bundle that has its field list).
         */
        const ValueTypeMetaData *wrapped_un_named{nullptr};
        /** Qualified identity and ancestry for named bundles; null for structural bundles and other kinds. */
        BundleHierarchyMetaData *bundle_hierarchy{nullptr};

        /** Checked value-family kind. */
        [[nodiscard]] constexpr ValueTypeKind value_kind() const
        {
            return checked_value_type_kind(header.kind);
        }
        /** Nonthrowing value-family kind query for diagnostic and predicate paths. */
        [[nodiscard]] constexpr std::optional<ValueTypeKind> try_value_kind() const noexcept
        {
            return try_value_type_kind(header.kind);
        }
        /** Registry-owned canonical diagnostic label. */
        [[nodiscard]] constexpr std::string_view name() const noexcept
        {
            return header.label == nullptr ? std::string_view{} : std::string_view{header.label};
        }
        /** Common schema prefix. */
        [[nodiscard]] constexpr const SchemaHeader &schema_header() const noexcept { return header; }

        /** True when this metadata is a nominal named bundle. */
        [[nodiscard]] constexpr bool is_named_bundle() const noexcept
        {
            return try_value_kind() == ValueTypeKind::Bundle && wrapped_un_named != nullptr;
        }
        /** True when this metadata is a structural (un-named) bundle. */
        [[nodiscard]] constexpr bool is_un_named_bundle() const noexcept
        {
            return try_value_kind() == ValueTypeKind::Bundle && wrapped_un_named == nullptr &&
                   !has(ValueTypeFlags::Owned);
        }

        [[nodiscard]] constexpr std::string_view bundle_namespace() const noexcept
        {
            return bundle_hierarchy == nullptr || bundle_hierarchy->namespace_name == nullptr
                       ? std::string_view{}
                       : std::string_view{bundle_hierarchy->namespace_name};
        }
        [[nodiscard]] constexpr std::string_view bundle_local_name() const noexcept
        {
            return bundle_hierarchy == nullptr || bundle_hierarchy->local_name == nullptr
                       ? std::string_view{}
                       : std::string_view{bundle_hierarchy->local_name};
        }
        [[nodiscard]] constexpr bool is_abstract_bundle() const noexcept
        {
            return bundle_hierarchy != nullptr && bundle_hierarchy->is_abstract;
        }
        [[nodiscard]] constexpr std::string_view bundle_discriminator() const noexcept
        {
            return bundle_hierarchy != nullptr && bundle_hierarchy->discriminator != nullptr
                       ? std::string_view{bundle_hierarchy->discriminator}
                       : std::string_view{"__type__"};
        }
        [[nodiscard]] const std::vector<const ValueTypeMetaData *> &bundle_generic_arguments() const noexcept
        {
            static const std::vector<const ValueTypeMetaData *> empty;
            return bundle_hierarchy != nullptr ? bundle_hierarchy->generic_arguments : empty;
        }

        /** True when ``fixed_size`` is non-zero. Note: this is a semantic property (capacity / staticness), not a layout property. */
        [[nodiscard]] constexpr bool is_fixed_size() const noexcept { return fixed_size > 0; }
        /** True when ``flag`` is set in ``flags``. */
        [[nodiscard]] constexpr bool has(ValueTypeFlags flag) const noexcept { return has_flag(flags, flag); }
        /** True when the underlying C++ type is trivially default constructible. */
        [[nodiscard]] constexpr bool is_trivially_constructible() const noexcept
        {
            return has(ValueTypeFlags::TriviallyConstructible);
        }
        /** True when the underlying C++ type is trivially destructible. */
        [[nodiscard]] constexpr bool is_trivially_destructible() const noexcept
        {
            return has(ValueTypeFlags::TriviallyDestructible);
        }
        /** True when the underlying C++ type is trivially copyable. */
        [[nodiscard]] constexpr bool is_trivially_copyable() const noexcept
        {
            return has(ValueTypeFlags::TriviallyCopyable);
        }
        /** True when ``std::hash`` is available for the underlying C++ type. */
        [[nodiscard]] constexpr bool is_hashable() const noexcept { return has(ValueTypeFlags::Hashable); }
        /** True when the underlying C++ type supports ``operator<``. */
        [[nodiscard]] constexpr bool is_comparable() const noexcept { return has(ValueTypeFlags::Comparable); }
        /** True when the underlying C++ type supports ``operator==``. */
        [[nodiscard]] constexpr bool is_equatable() const noexcept { return has(ValueTypeFlags::Equatable); }
        /** True when the type can be exposed as a contiguous buffer (Arrow-compatible). */
        [[nodiscard]] constexpr bool is_buffer_compatible() const noexcept
        {
            return has(ValueTypeFlags::BufferCompatible);
        }
        /** True when this metadata represents a variadic-element tuple. */
        [[nodiscard]] constexpr bool is_variadic_tuple() const noexcept
        {
            return has(ValueTypeFlags::VariadicTuple);
        }
        /** True when this container schema is backed by structurally-mutable storage. */
        [[nodiscard]] constexpr bool is_mutable() const noexcept { return has(ValueTypeFlags::Mutable); }
        [[nodiscard]] constexpr bool is_nullable() const noexcept { return has(ValueTypeFlags::Nullable); }

        [[nodiscard]] constexpr bool is_enum() const noexcept { return has(ValueTypeFlags::Enum); }
        [[nodiscard]] constexpr bool is_owned() const noexcept { return has(ValueTypeFlags::Owned); }
        [[nodiscard]] constexpr bool is_shaped_array() const noexcept
        {
            return try_value_kind() == ValueTypeKind::List && has(ValueTypeFlags::ShapedArray);
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
            std::is_arithmetic_v<T> || std::is_same_v<T, Date> || std::is_same_v<T, DateTime> ||
            std::is_same_v<T, TimeDelta>;
    }  // namespace detail

    /**
     * Compile-time scalar capability inference for ``T``.
     *
     * Combines trivially-* type traits with concept checks for
     * hashability, equatability, and comparability, plus the
     * ``BufferCompatible`` test for arithmetic / date/time types.
     * Used by ``TypeRegistry::register_scalar<T>`` to derive the flags
     * for an atomic schema.
     */
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
}  // namespace hgraph

#endif  // HGRAPH_CPP_ROOT_VALUE_TYPE_META_DATA_H
