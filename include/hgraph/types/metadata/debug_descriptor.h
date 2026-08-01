#ifndef HGRAPH_TYPES_METADATA_DEBUG_DESCRIPTOR_H
#define HGRAPH_TYPES_METADATA_DEBUG_DESCRIPTOR_H

#include <hgraph/hgraph_export.h>
#include <hgraph/types/utils/memory_utils.h>

#include <cstddef>
#include <cstdint>
#include <string>
#include <type_traits>

namespace hgraph
{
    enum class TypeFamily : std::uint8_t;
    struct SchemaHeader;
    struct TypeRecord;
    struct ValueTypeMetaData;
    struct DebugDynamicLayout;
    struct DebugTimeSeriesLayout;

    inline constexpr std::uint32_t DEBUG_DESCRIPTOR_MAGIC = 0x48474444u;
    inline constexpr std::uint16_t DEBUG_DESCRIPTOR_ABI_VERSION = 3;
    inline constexpr std::uint32_t DEBUG_DYNAMIC_LAYOUT_MAGIC = 0x4847444cu;
    inline constexpr std::uint16_t DEBUG_DYNAMIC_LAYOUT_ABI_VERSION = 3;
    inline constexpr std::size_t DEBUG_OWNER_IDENTITY_OFFSET = 0;
    inline constexpr std::size_t DEBUG_OWNER_STATE_OFFSET = sizeof(void *);
    inline constexpr std::size_t DEBUG_OWNER_STORAGE_OFFSET = 2 * sizeof(void *);
    inline constexpr std::uintptr_t DEBUG_OWNER_STATE_MASK = 0x3u;
    inline constexpr std::uintptr_t DEBUG_OWNER_INLINE_STATE = 0x1u;
    inline constexpr std::uintptr_t DEBUG_OWNER_HEAP_STATE = 0x2u;
    inline constexpr std::uintptr_t DEBUG_OWNER_RESERVED_STATE = 0x3u;

    enum class DebugLayoutKind : std::uint8_t
    {
        Opaque = 0,
        Atomic = 1,
        FixedComposite = 2,
        Sequence = 3,
        KeyedSlots = 4,
        Node = 5,
        Graph = 6,
        TimeSeries = 7,
    };

    enum class DebugAtomicKind : std::uint8_t
    {
        Opaque = 0,
        Boolean = 1,
        SignedInteger = 2,
        UnsignedInteger = 3,
        FloatingPoint = 4,
        String = 5,
    };

    enum class DebugDescriptorFlags : std::uint32_t
    {
        None = 0,
        HasValidityBitmap = 1u << 0u,
    };

    enum class DebugFieldFlags : std::uint32_t
    {
        None = 0,
        Optional = 1u << 0u,
        EmbeddedOwner = 1u << 1u,
        EmbeddedPointer = 1u << 2u,
        IndirectEmbeddedPointer = 1u << 3u,
    };

    enum class DebugDynamicKind : std::uint8_t
    {
        Contiguous = 1,
        StableSlots = 2,
    };

    enum class DebugDynamicFlags : std::uint32_t
    {
        None = 0,
        SizeIsConstant = 1u << 0u,
        DataIsIndirect = 1u << 1u,
        KeyDataIsIndirect = 1u << 2u,
        DataIsPointerTable = 1u << 3u,
        KeyDataIsPointerTable = 1u << 4u,
        HasSlotState = 1u << 5u,
        HasHead = 1u << 6u,
        ElementsAreOwners = 1u << 7u,
        KeysAreOwners = 1u << 8u,
        ElementsArePointers = 1u << 9u,
        DataPointersAreTagged = 1u << 10u,
        KeyPointersAreTagged = 1u << 11u,
        SlotStateIsTaggedPointer = 1u << 12u,
        SlotIndexIsIndirect = 1u << 13u,
        SlotSizeIsIndirect = 1u << 14u,
    };

    [[nodiscard]] constexpr DebugDescriptorFlags operator|(DebugDescriptorFlags lhs, DebugDescriptorFlags rhs) noexcept
    {
        return static_cast<DebugDescriptorFlags>(static_cast<std::uint32_t>(lhs) | static_cast<std::uint32_t>(rhs));
    }

    [[nodiscard]] constexpr bool has_flag(DebugDescriptorFlags flags, DebugDescriptorFlags requested) noexcept
    {
        return (static_cast<std::uint32_t>(flags) & static_cast<std::uint32_t>(requested)) ==
               static_cast<std::uint32_t>(requested);
    }

    [[nodiscard]] constexpr DebugFieldFlags operator|(DebugFieldFlags lhs, DebugFieldFlags rhs) noexcept
    {
        return static_cast<DebugFieldFlags>(static_cast<std::uint32_t>(lhs) | static_cast<std::uint32_t>(rhs));
    }

    [[nodiscard]] constexpr bool has_flag(DebugFieldFlags flags, DebugFieldFlags requested) noexcept
    {
        return (static_cast<std::uint32_t>(flags) & static_cast<std::uint32_t>(requested)) ==
               static_cast<std::uint32_t>(requested);
    }

    [[nodiscard]] constexpr DebugDynamicFlags operator|(DebugDynamicFlags lhs, DebugDynamicFlags rhs) noexcept
    {
        return static_cast<DebugDynamicFlags>(static_cast<std::uint32_t>(lhs) | static_cast<std::uint32_t>(rhs));
    }

    [[nodiscard]] constexpr bool has_flag(DebugDynamicFlags flags, DebugDynamicFlags requested) noexcept
    {
        return (static_cast<std::uint32_t>(flags) & static_cast<std::uint32_t>(requested)) ==
               static_cast<std::uint32_t>(requested);
    }

    struct DebugField
    {
        const char *name{nullptr};
        std::size_t offset{0};
        const TypeRecord *type{nullptr};
        std::uint32_t validity_bit{0};
        DebugFieldFlags flags{DebugFieldFlags::None};
    };

    /** Data-only offsets used by Sequence and KeyedSlots descriptors. */
    struct HGRAPH_EXPORT DebugDynamicLayout
    {
        std::uint32_t magic{0};
        std::uint16_t abi_version{0};
        DebugDynamicKind kind{DebugDynamicKind::Contiguous};
        std::uint8_t reserved0{0};
        DebugDynamicFlags flags{DebugDynamicFlags::None};
        std::uint32_t key_auxiliary_offset{0};
        std::size_t size_offset{0};
        std::size_t size_constant{0};
        std::size_t data_offset{0};
        std::size_t stride{0};
        std::size_t key_data_offset{0};
        std::size_t key_stride{0};
        std::size_t state_offset{0};
        std::size_t auxiliary_offset{0};
        std::size_t entry_offset{0};

        [[nodiscard]] bool valid() const noexcept;
        [[nodiscard]] constexpr bool operator==(const DebugDynamicLayout &) const noexcept = default;
    };

    struct HGRAPH_EXPORT DebugDescriptor
    {
        std::uint32_t magic{0};
        std::uint16_t abi_version{0};
        DebugLayoutKind layout{DebugLayoutKind::Opaque};
        DebugAtomicKind atomic_kind{DebugAtomicKind::Opaque};
        DebugDescriptorFlags flags{DebugDescriptorFlags::None};
        std::uint32_t field_count{0};
        const DebugField *fields{nullptr};
        std::size_t validity_offset{0};
        std::uint32_t validity_word_size{0};
        std::uint32_t reserved0{0};
        const TypeRecord *key_type{nullptr};
        const TypeRecord *element_type{nullptr};
        const DebugDynamicLayout *dynamic_layout{nullptr};
        const DebugTimeSeriesLayout *time_series_layout{nullptr};

        [[nodiscard]] bool valid() const noexcept;
    };

    /** Stable, data-only navigation for one TSData allocation. */
    struct HGRAPH_EXPORT DebugTimeSeriesLayout
    {
        const TypeRecord *value_type{nullptr};
        const TypeRecord *delta_type{nullptr};
        std::size_t value_offset{0};
        std::size_t tracking_offset{0};
        std::size_t last_modified_offset{0};
        std::size_t parent_offset{0};
        std::size_t observers_offset{0};
        bool delta_aliases_value{false};

        [[nodiscard]] bool valid() const noexcept;
        [[nodiscard]] constexpr bool operator==(const DebugTimeSeriesLayout &) const noexcept = default;
    };

    template <typename T> [[nodiscard]] consteval DebugAtomicKind debug_atomic_kind_for() noexcept
    {
        using Value = std::remove_cv_t<T>;
        if constexpr (std::is_same_v<Value, bool>)
            return DebugAtomicKind::Boolean;
        else if constexpr (std::is_integral_v<Value> && std::is_signed_v<Value>)
            return DebugAtomicKind::SignedInteger;
        else if constexpr (std::is_integral_v<Value> && std::is_unsigned_v<Value>)
            return DebugAtomicKind::UnsignedInteger;
        else if constexpr (std::is_floating_point_v<Value>)
            return DebugAtomicKind::FloatingPoint;
        else if constexpr (std::is_same_v<Value, std::string>)
            return DebugAtomicKind::String;
        else
            return DebugAtomicKind::Opaque;
    }

    [[nodiscard]] HGRAPH_EXPORT const DebugDescriptor &intern_atomic_debug_descriptor(
        const ValueTypeMetaData &schema, const MemoryUtils::StoragePlan &plan, DebugAtomicKind atomic_kind);

    [[nodiscard]] HGRAPH_EXPORT const DebugDescriptor &intern_fixed_composite_debug_descriptor(
        const ValueTypeMetaData &schema, const MemoryUtils::StoragePlan &plan);

    [[nodiscard]] HGRAPH_EXPORT const DebugDescriptor &intern_dynamic_debug_descriptor(
        const SchemaHeader &schema, const MemoryUtils::StoragePlan &plan, DebugLayoutKind layout,
        const TypeRecord *key_type, const TypeRecord *element_type, DebugDynamicLayout dynamic_layout,
        const void *representation = nullptr);

    [[nodiscard]] HGRAPH_EXPORT const DebugDescriptor &intern_structured_debug_descriptor(
        const SchemaHeader &schema, const MemoryUtils::StoragePlan &plan, DebugLayoutKind layout,
        const DebugField *fields, std::size_t field_count, const TypeRecord *key_type = nullptr,
        const TypeRecord *element_type = nullptr, const DebugDynamicLayout *dynamic_layout = nullptr);

    [[nodiscard]] HGRAPH_EXPORT const DebugDescriptor &intern_time_series_debug_descriptor(
        const SchemaHeader &schema, const MemoryUtils::StoragePlan &plan,
        const TypeRecord &value_type, const TypeRecord &delta_type,
        std::size_t value_offset, std::size_t tracking_offset,
        std::size_t last_modified_offset, std::size_t parent_offset, std::size_t observers_offset,
        bool delta_aliases_value, const DebugField *fields, std::size_t field_count,
        const void *representation);

    [[nodiscard]] HGRAPH_EXPORT const DebugDescriptor *find_value_debug_descriptor(
        const ValueTypeMetaData &schema, const MemoryUtils::StoragePlan &plan) noexcept;

    HGRAPH_EXPORT void clear_value_debug_descriptors() noexcept;
    HGRAPH_EXPORT void clear_debug_descriptors(TypeFamily family) noexcept;

    static_assert(sizeof(DebugField) == 4 * sizeof(void *));
    static_assert(alignof(DebugField) == alignof(void *));
    static_assert(std::is_standard_layout_v<DebugField>);
    static_assert(std::is_trivially_copyable_v<DebugField>);
    static_assert(offsetof(DebugField, name) == 0);
    static_assert(offsetof(DebugField, offset) == sizeof(void *));
    static_assert(offsetof(DebugField, type) == 2 * sizeof(void *));
    static_assert(offsetof(DebugField, validity_bit) == 3 * sizeof(void *));

    static_assert(sizeof(DebugDescriptor) == 9 * sizeof(void *));
    static_assert(alignof(DebugDescriptor) == alignof(void *));
    static_assert(std::is_standard_layout_v<DebugDescriptor>);
    static_assert(std::is_trivially_copyable_v<DebugDescriptor>);
    static_assert(offsetof(DebugDescriptor, magic) == 0);
    static_assert(offsetof(DebugDescriptor, fields) == 2 * sizeof(void *));
    static_assert(offsetof(DebugDescriptor, validity_offset) == 3 * sizeof(void *));
    static_assert(offsetof(DebugDescriptor, key_type) == 5 * sizeof(void *));
    static_assert(offsetof(DebugDescriptor, dynamic_layout) == 7 * sizeof(void *));
    static_assert(offsetof(DebugDescriptor, time_series_layout) == 8 * sizeof(void *));

    static_assert(sizeof(DebugDynamicLayout) == 11 * sizeof(void *));
    static_assert(alignof(DebugDynamicLayout) == alignof(void *));
    static_assert(std::is_standard_layout_v<DebugDynamicLayout>);
    static_assert(std::is_trivially_copyable_v<DebugDynamicLayout>);
    static_assert(offsetof(DebugDynamicLayout, magic) == 0);
    static_assert(offsetof(DebugDynamicLayout, size_offset) == 2 * sizeof(void *));
    static_assert(offsetof(DebugDynamicLayout, data_offset) == 4 * sizeof(void *));
    static_assert(offsetof(DebugDynamicLayout, state_offset) == 8 * sizeof(void *));
} // namespace hgraph

#endif // HGRAPH_TYPES_METADATA_DEBUG_DESCRIPTOR_H
