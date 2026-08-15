#include <hgraph/types/metadata/debug_descriptor.h>

#include <hgraph/types/metadata/value_plan_factory.h>
#include <hgraph/types/metadata/value_type_meta_data.h>
#include <hgraph/types/value/value_type_ref.h>

#include <limits>
#include <memory>
#include <mutex>

#include <hgraph/types/utils/counted_mutex.h>
#include <stdexcept>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

namespace hgraph
{
    namespace
    {
        using CommonErasedOwner =
            MemoryUtils::ErasedOwner<MemoryUtils::InlineStoragePolicy<>, TypeRecord>;

        static_assert(CommonErasedOwner::debug_identity_offset() == DEBUG_OWNER_IDENTITY_OFFSET);
        static_assert(CommonErasedOwner::debug_state_offset() == DEBUG_OWNER_STATE_OFFSET);
        static_assert(CommonErasedOwner::debug_storage_offset() == DEBUG_OWNER_STORAGE_OFFSET);
        inline constexpr std::uint32_t KNOWN_DESCRIPTOR_FLAGS = 1u;
        inline constexpr std::uint32_t KNOWN_FIELD_FLAGS = (1u << 4u) - 1u;
        inline constexpr std::uint32_t KNOWN_DYNAMIC_FLAGS = (1u << 15u) - 1u;
        inline constexpr std::uint32_t VALIDITY_WORD_SIZE = sizeof(std::uint64_t);

        struct DescriptorKey
        {
            const SchemaHeader *schema{nullptr};
            const MemoryUtils::StoragePlan *plan{nullptr};
            const void *representation{nullptr};

            [[nodiscard]] bool operator==(const DescriptorKey &) const noexcept = default;
        };

        struct DescriptorKeyHash
        {
            [[nodiscard]] std::size_t operator()(const DescriptorKey &key) const noexcept
            {
                const auto schema_hash = std::hash<const SchemaHeader *>{}(key.schema);
                const auto plan_hash = std::hash<const MemoryUtils::StoragePlan *>{}(key.plan);
                const auto representation_hash = std::hash<const void *>{}(key.representation);
                const auto combined = schema_hash ^ (plan_hash + 0x9e3779b9u + (schema_hash << 6u) + (schema_hash >> 2u));
                return combined ^ (representation_hash + 0x9e3779b9u + (combined << 6u) + (combined >> 2u));
            }
        };

        struct DescriptorEntry
        {
            std::vector<std::unique_ptr<std::string>> field_names{};
            std::vector<DebugField> fields{};
            DebugDynamicLayout dynamic_layout{};
            DebugTimeSeriesLayout time_series_layout{};
            DebugDescriptor descriptor{};

            void bind_fields() noexcept
            {
                descriptor.fields = fields.empty() ? nullptr : fields.data();
                descriptor.field_count = static_cast<std::uint32_t>(fields.size());
            }

            void own_field_names()
            {
                field_names.reserve(fields.size());
                for (DebugField &field : fields)
                {
                    if (field.name == nullptr) { continue; }
                    field_names.push_back(std::make_unique<std::string>(field.name));
                    field.name = field_names.back()->c_str();
                }
            }
        };

        class ValueDebugDescriptorRegistry
        {
          public:
            [[nodiscard]] const DebugDescriptor &intern_atomic(const ValueTypeMetaData &schema,
                                                               const MemoryUtils::StoragePlan &plan,
                                                               DebugAtomicKind atomic_kind)
            {
                if (schema.value_kind() != ValueTypeKind::Atomic)
                    throw std::invalid_argument("atomic debug descriptor requires an atomic value schema");

                auto entry = std::make_unique<DescriptorEntry>();
                entry->descriptor = DebugDescriptor{
                    .magic = DEBUG_DESCRIPTOR_MAGIC,
                    .abi_version = DEBUG_DESCRIPTOR_ABI_VERSION,
                    .layout = DebugLayoutKind::Atomic,
                    .atomic_kind = atomic_kind,
                };
                return intern(DescriptorKey{&schema.header, &plan}, std::move(entry));
            }

            [[nodiscard]] const DebugDescriptor &intern_fixed_composite(const ValueTypeMetaData &schema,
                                                                        const MemoryUtils::StoragePlan &plan)
            {
                if (schema.value_kind() != ValueTypeKind::Tuple && schema.value_kind() != ValueTypeKind::Bundle)
                    throw std::invalid_argument("fixed-composite debug descriptor requires tuple or bundle schema");
                if (schema.field_count > std::numeric_limits<std::uint32_t>::max())
                    throw std::length_error("fixed-composite debug descriptor exceeds the field-count ABI");
                if (!plan.is_composite() ||
                    plan.component_count() != schema.field_count + (schema.field_count == 0 ? 0 : 1))
                    throw std::invalid_argument("fixed-composite debug descriptor requires matching public fields "
                                                "and validity storage");

                auto entry = std::make_unique<DescriptorEntry>();
                entry->fields.reserve(schema.field_count);
                const auto components = plan.components();
                for (std::size_t index = 0; index < schema.field_count; ++index)
                {
                    const ValueTypeRef child = ValuePlanFactory::instance().type_for(schema.fields[index].type);
                    if (!child.valid())
                        throw std::logic_error("fixed-composite debug descriptor child has no "
                                               "canonical type record");
                    entry->fields.push_back(DebugField{
                        .name = schema.fields[index].name,
                        .offset = components[index].offset,
                        .type = child.record(),
                        .validity_bit = static_cast<std::uint32_t>(index),
                        .flags = DebugFieldFlags::Optional,
                    });
                }

                entry->descriptor = DebugDescriptor{
                    .magic = DEBUG_DESCRIPTOR_MAGIC,
                    .abi_version = DEBUG_DESCRIPTOR_ABI_VERSION,
                    .layout = DebugLayoutKind::FixedComposite,
                    .atomic_kind = DebugAtomicKind::Opaque,
                    .flags =
                        schema.field_count == 0 ? DebugDescriptorFlags::None : DebugDescriptorFlags::HasValidityBitmap,
                    .validity_offset = schema.field_count == 0 ? 0 : components[schema.field_count].offset,
                    .validity_word_size = schema.field_count == 0 ? 0 : VALIDITY_WORD_SIZE,
                };
                entry->own_field_names();
                entry->bind_fields();
                return intern(DescriptorKey{&schema.header, &plan}, std::move(entry));
            }

            [[nodiscard]] const DebugDescriptor &intern_dynamic(const SchemaHeader &schema,
                                                                 const MemoryUtils::StoragePlan &plan,
                                                                 DebugLayoutKind layout,
                                                                 const TypeRecord *key_type,
                                                                 const TypeRecord *element_type,
                                                                 DebugDynamicLayout dynamic_layout,
                                                                 const void *representation)
            {
                if (layout != DebugLayoutKind::Sequence && layout != DebugLayoutKind::KeyedSlots)
                    throw std::invalid_argument("dynamic debug descriptor requires sequence or keyed-slots layout");
                if (!dynamic_layout.valid())
                    throw std::invalid_argument(
                        "dynamic debug descriptor requires a valid dynamic layout (kind=" +
                        std::to_string(static_cast<unsigned>(dynamic_layout.kind)) +
                        ", flags=" +
                        std::to_string(static_cast<std::uint32_t>(dynamic_layout.flags)) +
                        ", size_offset=" + std::to_string(dynamic_layout.size_offset) +
                        ", stride=" + std::to_string(dynamic_layout.stride) + ")");
                if (element_type == nullptr)
                    throw std::invalid_argument("dynamic debug descriptor requires an element type");
                if (layout == DebugLayoutKind::Sequence && key_type != nullptr)
                    throw std::invalid_argument("sequence debug descriptor cannot carry a key type");

                auto entry = std::make_unique<DescriptorEntry>();
                entry->dynamic_layout = dynamic_layout;
                entry->descriptor = DebugDescriptor{
                    .magic = DEBUG_DESCRIPTOR_MAGIC,
                    .abi_version = DEBUG_DESCRIPTOR_ABI_VERSION,
                    .layout = layout,
                    .key_type = key_type,
                    .element_type = element_type,
                    .dynamic_layout = &entry->dynamic_layout,
                };
                return intern(DescriptorKey{&schema, &plan, representation}, std::move(entry));
            }

            [[nodiscard]] const DebugDescriptor &intern_structured(const SchemaHeader &schema,
                                                                    const MemoryUtils::StoragePlan &plan,
                                                                    DebugLayoutKind layout,
                                                                    const DebugField *fields,
                                                                    std::size_t field_count,
                                                                    const TypeRecord *key_type,
                                                                    const TypeRecord *element_type,
                                                                    const DebugDynamicLayout *dynamic_layout)
            {
                if (layout != DebugLayoutKind::Node && layout != DebugLayoutKind::Graph)
                    throw std::invalid_argument("structured debug descriptor requires node or graph layout");
                if (field_count > std::numeric_limits<std::uint32_t>::max())
                    throw std::length_error("structured debug descriptor exceeds the field-count ABI");
                if ((field_count == 0) != (fields == nullptr))
                    throw std::invalid_argument("structured debug descriptor fields are inconsistent");
                if (dynamic_layout != nullptr && (!dynamic_layout->valid() || element_type == nullptr))
                    throw std::invalid_argument("structured dynamic navigation requires a valid layout and element");

                auto entry = std::make_unique<DescriptorEntry>();
                if (field_count != 0) { entry->fields.assign(fields, fields + field_count); }
                entry->own_field_names();
                if (dynamic_layout != nullptr) { entry->dynamic_layout = *dynamic_layout; }
                entry->descriptor = DebugDescriptor{
                    .magic = DEBUG_DESCRIPTOR_MAGIC,
                    .abi_version = DEBUG_DESCRIPTOR_ABI_VERSION,
                    .layout = layout,
                    .key_type = key_type,
                    .element_type = element_type,
                    .dynamic_layout = dynamic_layout == nullptr ? nullptr : &entry->dynamic_layout,
                };
                entry->bind_fields();
                return intern(DescriptorKey{&schema, &plan}, std::move(entry));
            }

            [[nodiscard]] const DebugDescriptor &intern_time_series(
                const SchemaHeader &schema, const MemoryUtils::StoragePlan &plan,
                const TypeRecord &value_type, const TypeRecord &delta_type,
                std::size_t value_offset, std::size_t tracking_offset,
                std::size_t last_modified_offset, std::size_t parent_offset, std::size_t observers_offset,
                bool delta_aliases_value, const DebugField *fields, std::size_t field_count,
                const void *representation)
            {
                auto entry = std::make_unique<DescriptorEntry>();
                if (field_count != 0) { entry->fields.assign(fields, fields + field_count); }
                entry->own_field_names();
                entry->time_series_layout = DebugTimeSeriesLayout{
                    .value_type = &value_type,
                    .delta_type = &delta_type,
                    .value_offset = value_offset,
                    .tracking_offset = tracking_offset,
                    .last_modified_offset = last_modified_offset,
                    .parent_offset = parent_offset,
                    .observers_offset = observers_offset,
                    .delta_aliases_value = delta_aliases_value,
                };
                entry->descriptor = DebugDescriptor{
                    .magic = DEBUG_DESCRIPTOR_MAGIC,
                    .abi_version = DEBUG_DESCRIPTOR_ABI_VERSION,
                    .layout = DebugLayoutKind::TimeSeries,
                    .time_series_layout = &entry->time_series_layout,
                };
                entry->bind_fields();
                return intern(DescriptorKey{&schema, &plan, representation}, std::move(entry));
            }

            [[nodiscard]] const DebugDescriptor *find(const ValueTypeMetaData &schema,
                                                      const MemoryUtils::StoragePlan &plan) const noexcept
            {
                const std::lock_guard lock(mutex_);
                const auto found = entries_.find(DescriptorKey{&schema.header, &plan});
                return found == entries_.end() ? nullptr : &found->second->descriptor;
            }

            void clear(TypeFamily family) noexcept
            {
                const std::lock_guard lock(mutex_);
                std::erase_if(entries_, [family](const auto &entry) {
                    return entry.first.schema != nullptr && entry.first.schema->family == family;
                });
            }

          private:
            [[nodiscard]] const DebugDescriptor &intern(DescriptorKey key, std::unique_ptr<DescriptorEntry> candidate)
            {
                if (!candidate->descriptor.valid())
                    throw std::invalid_argument("cannot intern an invalid debug descriptor");

                const std::lock_guard lock(mutex_);
                if (const auto found = entries_.find(key); found != entries_.end())
                {
                    const auto &existing = found->second->descriptor;
                    if (existing.layout != candidate->descriptor.layout ||
                        existing.atomic_kind != candidate->descriptor.atomic_kind ||
                        existing.key_type != candidate->descriptor.key_type ||
                        existing.element_type != candidate->descriptor.element_type ||
                        ((existing.dynamic_layout == nullptr) != (candidate->descriptor.dynamic_layout == nullptr)) ||
                        (existing.dynamic_layout != nullptr &&
                         *existing.dynamic_layout != *candidate->descriptor.dynamic_layout) ||
                        ((existing.time_series_layout == nullptr) !=
                         (candidate->descriptor.time_series_layout == nullptr)) ||
                        (existing.time_series_layout != nullptr &&
                         *existing.time_series_layout != *candidate->descriptor.time_series_layout) ||
                        existing.field_count != candidate->descriptor.field_count)
                    {
                        throw std::logic_error("debug descriptor conflicts with the canonical "
                                               "schema/plan descriptor");
                    }
                    for (std::uint32_t index = 0; index < existing.field_count; ++index)
                    {
                        const DebugField &lhs = existing.fields[index];
                        const DebugField &rhs = candidate->descriptor.fields[index];
                        const bool same_name = lhs.name == rhs.name ||
                                               (lhs.name != nullptr && rhs.name != nullptr &&
                                                std::string_view{lhs.name} == std::string_view{rhs.name});
                        if (!same_name || lhs.offset != rhs.offset || lhs.type != rhs.type ||
                            lhs.validity_bit != rhs.validity_bit || lhs.flags != rhs.flags)
                        {
                            throw std::logic_error("debug descriptor fields conflict with the canonical "
                                                   "schema/plan descriptor");
                        }
                    }
                    return existing;
                }
                const DebugDescriptor *result = &candidate->descriptor;
                entries_.emplace(key, std::move(candidate));
                return *result;
            }

            mutable TypeSystemMutex mutex_{};
            std::unordered_map<DescriptorKey, std::unique_ptr<DescriptorEntry>, DescriptorKeyHash> entries_{};
        };

        [[nodiscard]] ValueDebugDescriptorRegistry &descriptor_registry()
        {
            static auto *registry = new ValueDebugDescriptorRegistry();
            return *registry;
        }
    } // namespace

    bool DebugDescriptor::valid() const noexcept
    {
        const auto layout_value = static_cast<std::uint8_t>(layout);
        const auto atomic_value = static_cast<std::uint8_t>(atomic_kind);
        if (magic != DEBUG_DESCRIPTOR_MAGIC || abi_version != DEBUG_DESCRIPTOR_ABI_VERSION ||
            layout_value > static_cast<std::uint8_t>(DebugLayoutKind::TimeSeries) ||
            atomic_value > static_cast<std::uint8_t>(DebugAtomicKind::String) || reserved0 != 0 ||
            (static_cast<std::uint32_t>(flags) & ~KNOWN_DESCRIPTOR_FLAGS) != 0 ||
            (field_count == 0) != (fields == nullptr))
        {
            return false;
        }
        if (has_flag(flags, DebugDescriptorFlags::HasValidityBitmap))
        {
            if (layout != DebugLayoutKind::FixedComposite || validity_word_size != VALIDITY_WORD_SIZE)
                return false;
        }
        else if (validity_offset != 0 || validity_word_size != 0)
        {
            return false;
        }
        for (std::uint32_t index = 0; index < field_count; ++index)
        {
            const DebugField &field = fields[index];
            if ((field.type == nullptr && !has_flag(field.flags, DebugFieldFlags::EmbeddedOwner) &&
                 !has_flag(field.flags, DebugFieldFlags::EmbeddedPointer) &&
                 !has_flag(field.flags, DebugFieldFlags::IndirectEmbeddedPointer)) ||
                (static_cast<std::uint32_t>(field.flags) & ~KNOWN_FIELD_FLAGS) != 0)
                return false;
            if (has_flag(field.flags, DebugFieldFlags::EmbeddedOwner) &&
                has_flag(field.flags, DebugFieldFlags::EmbeddedPointer))
                return false;
            if (has_flag(field.flags, DebugFieldFlags::IndirectEmbeddedPointer) &&
                (has_flag(field.flags, DebugFieldFlags::EmbeddedOwner) ||
                 has_flag(field.flags, DebugFieldFlags::EmbeddedPointer)))
                return false;
        }
        const bool requires_dynamic = layout == DebugLayoutKind::Sequence || layout == DebugLayoutKind::KeyedSlots;
        const bool permits_dynamic = requires_dynamic || layout == DebugLayoutKind::Node || layout == DebugLayoutKind::Graph;
        if ((requires_dynamic && dynamic_layout == nullptr) || (!permits_dynamic && dynamic_layout != nullptr)) return false;
        if (dynamic_layout != nullptr && element_type == nullptr) return false;
        if (dynamic_layout != nullptr && !dynamic_layout->valid()) return false;
        if ((layout == DebugLayoutKind::TimeSeries) != (time_series_layout != nullptr)) return false;
        if (time_series_layout != nullptr && !time_series_layout->valid()) return false;
        return true;
    }

    bool DebugTimeSeriesLayout::valid() const noexcept
    {
        return value_type != nullptr && delta_type != nullptr &&
               last_modified_offset >= tracking_offset && parent_offset >= tracking_offset &&
               observers_offset >= tracking_offset;
    }

    bool DebugDynamicLayout::valid() const noexcept
    {
        const auto kind_value = static_cast<std::uint8_t>(kind);
        if (magic != DEBUG_DYNAMIC_LAYOUT_MAGIC || abi_version != DEBUG_DYNAMIC_LAYOUT_ABI_VERSION ||
            kind_value < static_cast<std::uint8_t>(DebugDynamicKind::Contiguous) ||
            kind_value > static_cast<std::uint8_t>(DebugDynamicKind::StableSlots) || reserved0 != 0 ||
            (static_cast<std::uint32_t>(flags) & ~KNOWN_DYNAMIC_FLAGS) != 0 || stride == 0)
            return false;
        if (has_flag(flags, DebugDynamicFlags::SizeIsConstant) == (size_offset != 0)) return false;
        if (kind == DebugDynamicKind::Contiguous && has_flag(flags, DebugDynamicFlags::DataIsPointerTable))
            return false;
        if (has_flag(flags, DebugDynamicFlags::ElementsAreOwners) &&
            has_flag(flags, DebugDynamicFlags::ElementsArePointers))
            return false;
        if (kind == DebugDynamicKind::StableSlots &&
            (!has_flag(flags, DebugDynamicFlags::DataIsPointerTable) ||
             !has_flag(flags, DebugDynamicFlags::DataIsIndirect)))
            return false;
        if (has_flag(flags, DebugDynamicFlags::HasSlotState) && kind != DebugDynamicKind::StableSlots)
            return false;
        if (has_flag(flags, DebugDynamicFlags::DataPointersAreTagged) &&
            !has_flag(flags, DebugDynamicFlags::DataIsPointerTable))
            return false;
        if (has_flag(flags, DebugDynamicFlags::KeyPointersAreTagged) &&
            !has_flag(flags, DebugDynamicFlags::KeyDataIsPointerTable))
            return false;
        if (has_flag(flags, DebugDynamicFlags::SlotStateIsTaggedPointer) &&
            (!has_flag(flags, DebugDynamicFlags::HasSlotState) || state_offset == 0 ||
             (!has_flag(flags, DebugDynamicFlags::DataPointersAreTagged) &&
              !has_flag(flags, DebugDynamicFlags::KeyPointersAreTagged))))
            return false;
        if (has_flag(flags, DebugDynamicFlags::SlotIndexIsIndirect) &&
            (kind != DebugDynamicKind::StableSlots ||
             !has_flag(flags, DebugDynamicFlags::DataIsIndirect) ||
             !has_flag(flags, DebugDynamicFlags::DataIsPointerTable) ||
             has_flag(flags, DebugDynamicFlags::HasHead) ||
             (key_stride != 0 &&
              (!has_flag(flags, DebugDynamicFlags::KeyDataIsIndirect) ||
               !has_flag(flags, DebugDynamicFlags::KeyDataIsPointerTable)))))
            return false;
        if (has_flag(flags, DebugDynamicFlags::SlotSizeIsIndirect) &&
            (!has_flag(flags, DebugDynamicFlags::SlotIndexIsIndirect) ||
             has_flag(flags, DebugDynamicFlags::SizeIsConstant)))
            return false;
        if (key_auxiliary_offset != 0 &&
            (!has_flag(flags, DebugDynamicFlags::SlotIndexIsIndirect) || key_stride == 0))
            return false;
        return true;
    }

    const DebugDescriptor &intern_atomic_debug_descriptor(const ValueTypeMetaData &schema,
                                                          const MemoryUtils::StoragePlan &plan,
                                                          DebugAtomicKind atomic_kind)
    {
        return descriptor_registry().intern_atomic(schema, plan, atomic_kind);
    }

    const DebugDescriptor &intern_fixed_composite_debug_descriptor(const ValueTypeMetaData &schema,
                                                                   const MemoryUtils::StoragePlan &plan)
    {
        return descriptor_registry().intern_fixed_composite(schema, plan);
    }

    const DebugDescriptor &intern_dynamic_debug_descriptor(const SchemaHeader &schema,
                                                           const MemoryUtils::StoragePlan &plan,
                                                           DebugLayoutKind layout,
                                                           const TypeRecord *key_type,
                                                           const TypeRecord *element_type,
                                                           DebugDynamicLayout dynamic_layout,
                                                           const void *representation)
    {
        return descriptor_registry().intern_dynamic(
            schema, plan, layout, key_type, element_type, dynamic_layout, representation);
    }

    const DebugDescriptor &intern_structured_debug_descriptor(const SchemaHeader &schema,
                                                              const MemoryUtils::StoragePlan &plan,
                                                              DebugLayoutKind layout,
                                                              const DebugField *fields,
                                                              std::size_t field_count,
                                                              const TypeRecord *key_type,
                                                              const TypeRecord *element_type,
                                                              const DebugDynamicLayout *dynamic_layout)
    {
        return descriptor_registry().intern_structured(schema, plan, layout, fields, field_count, key_type,
                                                       element_type, dynamic_layout);
    }

    const DebugDescriptor &intern_time_series_debug_descriptor(
        const SchemaHeader &schema, const MemoryUtils::StoragePlan &plan,
        const TypeRecord &value_type, const TypeRecord &delta_type,
        std::size_t value_offset, std::size_t tracking_offset,
        std::size_t last_modified_offset, std::size_t parent_offset, std::size_t observers_offset,
        bool delta_aliases_value, const DebugField *fields, std::size_t field_count,
        const void *representation)
    {
        return descriptor_registry().intern_time_series(
            schema, plan, value_type, delta_type, value_offset, tracking_offset,
            last_modified_offset, parent_offset, observers_offset, delta_aliases_value,
            fields, field_count, representation);
    }

    const DebugDescriptor *find_value_debug_descriptor(const ValueTypeMetaData &schema,
                                                       const MemoryUtils::StoragePlan &plan) noexcept
    {
        return descriptor_registry().find(schema, plan);
    }

    void clear_value_debug_descriptors() noexcept
    {
        descriptor_registry().clear(TypeFamily::Value);
    }

    void clear_debug_descriptors(TypeFamily family) noexcept
    {
        descriptor_registry().clear(family);
    }
} // namespace hgraph
