#ifndef HGRAPH_CPP_ROOT_TS_DATA_PLAN_FACTORY_DETAIL_H
#define HGRAPH_CPP_ROOT_TS_DATA_PLAN_FACTORY_DETAIL_H

#include <hgraph/hgraph_export.h>
#include <hgraph/types/metadata/ts_value_type_meta_data.h>
#include <hgraph/types/metadata/value_plan_factory.h>
#include <hgraph/types/value/container_ops.h>
#include <hgraph/types/time_series/ts_data.h>
#include <hgraph/types/utils/memory_utils.h>

#include <cstddef>
#include <cstdint>
#include <stdexcept>
#include <vector>

namespace hgraph::ts_data_plan_factory_detail
{
    /**
     * Materialise a projected delta bundle into canonical owning storage.
     *
     * Projected TS deltas (TSS/TSD/dynamic TSL) expose their delta as a
     * read-only bundle over live storage. Copying one out has to walk the
     * destination's structured plan and copy each field through its own
     * projecting ops, then stamp the bundle's trailing validity component so
     * an explicitly-empty ``removed`` / ``added`` field still reads as present.
     */
    template <typename SourceBinding, typename SourceMemory>
    void copy_projected_bundle_fields(const ValueTypeRef &binding,
                                      void *destination_memory,
                                      std::size_t field_count,
                                      SourceBinding source_binding,
                                      SourceMemory source_memory)
    {
        const auto &plan = binding.checked_plan();
        const auto components = plan.components();
        const auto &destination_ops = *checked_value_ops<IndexedValueOps>(
            binding, "projected delta destination");
        for (std::size_t index = 0; index < field_count; ++index)
        {
            const auto destination = destination_ops.element_binding(
                destination_ops.context, destination_memory, index);
            const auto source = source_binding(index);
            if (!destination || !source)
            {
                throw std::logic_error("projected bundle field binding is unresolved");
            }
            const auto &source_ops = source.ops_ref();
            const auto owning_source = source_ops.owning_type(source);
            if (destination != owning_source)
            {
                throw std::logic_error("projected bundle field has an incompatible owning binding");
            }
            source_ops.copy_assign_view(
                destination,
                MemoryUtils::advance(destination_memory, components[index].offset),
                source_memory(index));
        }

        // Bundle validity is the trailing composite component. Projected
        // TS deltas always expose every canonical delta field, including
        // explicitly-empty added/removed collections.
        if (components.size() > field_count)
        {
            auto *words = MemoryUtils::cast<std::uint64_t>(
                MemoryUtils::advance(destination_memory, components[field_count].offset));
            constexpr std::size_t bits_per_word = sizeof(std::uint64_t) * 8U;
            for (std::size_t index = 0; index < field_count; ++index)
            {
                words[index / bits_per_word] |= std::uint64_t{1} << (index % bits_per_word);
            }
        }
    }

    [[nodiscard]] const TSDataOps &atomic_ts_data_ops(TSTypeKind                     kind,
                                                      const ValueTypeRef         &value_binding,
                                                      const ValueTypeRef         &delta_binding,
                                                      const MemoryUtils::StoragePlan &plan, std::size_t value_offset,
                                                      std::size_t tracking_offset,
                                                      ValueStorageVariant storage =
                                                          ValueStorageVariant::Native,
                                                      std::size_t python_value_offset =
                                                          ValueStorageSelection::no_offset);

    void clear_atomic_ts_data_ops() noexcept;

    [[nodiscard]] bool is_compact_atomic_ts_data(const TSValueTypeMetaData &schema) noexcept;
    [[nodiscard]] bool is_fixed_structured_ts_data(const TSValueTypeMetaData &schema) noexcept;
    [[nodiscard]] bool is_dynamic_list_ts_data(const TSValueTypeMetaData &schema) noexcept;
    [[nodiscard]] bool is_window_ts_data(const TSValueTypeMetaData &schema) noexcept;
    [[nodiscard]] bool is_slot_ts_data(const TSValueTypeMetaData &schema) noexcept;

    [[nodiscard]] const MemoryUtils::StoragePlan &ts_data_aux_plan(const TSValueTypeMetaData &schema);
    [[nodiscard]] const MemoryUtils::StoragePlan &ts_data_aux_plan(const TSValueTypeMetaData &schema,
                                                                  TypeRole role);
    [[nodiscard]] ValueTypeRef fixed_value_storage_binding(
        const TSValueTypeMetaData &schema,
        ValueTypeRef value_binding);
    [[nodiscard]] const MemoryUtils::StoragePlan *synthesise_fixed_plan(const TSValueTypeMetaData &schema);
    [[nodiscard]] const MemoryUtils::StoragePlan *synthesise_fixed_plan(
        const TSValueTypeMetaData &schema,
        TypeRole role);
    [[nodiscard]] const MemoryUtils::StoragePlan *synthesise_dynamic_list_plan(const TSValueTypeMetaData &schema);
    [[nodiscard]] const MemoryUtils::StoragePlan *synthesise_window_plan(const TSValueTypeMetaData &schema);
    [[nodiscard]] const MemoryUtils::StoragePlan *synthesise_window_plan(
        const TSValueTypeMetaData &schema,
        ValueTypeRef element_binding);
    [[nodiscard]] const MemoryUtils::StoragePlan *synthesise_slot_plan(const TSValueTypeMetaData &schema);
    [[nodiscard]] const MemoryUtils::StoragePlan *synthesise_slot_plan(
        const TSValueTypeMetaData &schema,
        ValueTypeRef key_binding);
    [[nodiscard]] const MemoryUtils::StoragePlan *synthesise_slot_tsd_plan(
        const TSValueTypeMetaData &schema,
        TSRoleTypeRef           element_type);
    [[nodiscard]] const MemoryUtils::StoragePlan *synthesise_slot_tsd_plan(
        const TSValueTypeMetaData &schema,
        ValueTypeRef key_binding,
        TSRoleTypeRef element_type);

    [[nodiscard]] TSRoleTypeRef embedded_ts_storage_type(const TSValueTypeMetaData      &schema,
                                                            TypeRole                         role,
                                                            const MemoryUtils::StoragePlan &root_plan,
                                                            std::size_t value_offset,
                                                            std::size_t aux_offset,
                                                            bool root_record = false);

    /** Resolve independently owned storage for a canonical TS role.
        ``embedded`` changes logical identity and labels, never the physical plan. */
    [[nodiscard]] TSRoleTypeRef standalone_ts_storage_type(const TSValueTypeMetaData &schema,
                                                              TypeRole role,
                                                              bool embedded = false);

    [[nodiscard]] const TSDataOps &fixed_structured_ts_data_ops(const TSValueTypeMetaData      &schema,
                                                                const MemoryUtils::StoragePlan &plan,
                                                                TypeRole role,
                                                                std::size_t value_offset, std::size_t aux_offset,
                                                                std::size_t tracking_offset,
                                                                std::vector<TSRoleTypeRef> element_types,
                                                                std::vector<std::size_t> element_data_offsets);

    [[nodiscard]] const TSDataOps &dynamic_list_ts_data_ops(const TSValueTypeMetaData      &schema,
                                                            const MemoryUtils::StoragePlan &plan,
                                                            std::size_t storage_offset,
                                                            TSRoleTypeRef element_type,
                                                            TypeRole role,
                                                            bool embedded = false);

    [[nodiscard]] const TSDataOps &window_ts_data_ops(const TSValueTypeMetaData      &schema,
                                                      const MemoryUtils::StoragePlan &plan,
                                                      std::size_t value_offset,
                                                      std::size_t tracking_offset,
                                                      TypeRole role = TypeRole::Data,
                                                      bool embedded = false);
    [[nodiscard]] const TSDataOps &window_ts_data_ops(const TSValueTypeMetaData      &schema,
                                                      const MemoryUtils::StoragePlan &plan,
                                                      std::size_t value_offset,
                                                      std::size_t tracking_offset,
                                                      ValueTypeRef element_binding,
                                                      TypeRole role,
                                                      bool embedded = false);

    [[nodiscard]] const TSDataOps &slot_ts_data_ops(const TSValueTypeMetaData      &schema,
                                                    const MemoryUtils::StoragePlan &plan,
                                                    std::size_t storage_offset,
                                                    TypeRole role = TypeRole::Data,
                                                    bool embedded = false);
    [[nodiscard]] const TSDataOps &slot_ts_data_ops(const TSValueTypeMetaData      &schema,
                                                    const MemoryUtils::StoragePlan &plan,
                                                    std::size_t storage_offset,
                                                    ValueTypeRef key_binding,
                                                    TypeRole role,
                                                    bool embedded = false);
    [[nodiscard]] const TSDataOps &slot_tsd_ts_data_ops(const TSValueTypeMetaData      &schema,
                                                        const MemoryUtils::StoragePlan &plan,
                                                        std::size_t storage_offset,
                                                        TSRoleTypeRef                element_type,
                                                        TypeRole role,
                                                        bool embedded = false,
                                                        bool composite = false);
    [[nodiscard]] const TSDataOps &slot_tsd_ts_data_ops(const TSValueTypeMetaData      &schema,
                                                        const MemoryUtils::StoragePlan &plan,
                                                        std::size_t storage_offset,
                                                        ValueTypeRef key_binding,
                                                        TSRoleTypeRef element_type,
                                                        TypeRole role,
                                                        bool embedded = false,
                                                        bool composite = false);
    [[nodiscard]] HGRAPH_EXPORT TSRoleTypeRef tsd_value_projection_type(
        TSRoleTypeRef element_type,
        TypeRole role);

    void clear_fixed_ts_data_contexts() noexcept;
    void clear_dynamic_list_ts_data_contexts() noexcept;
    void clear_window_ts_data_contexts() noexcept;
    void clear_slot_ts_data_contexts() noexcept;
} // namespace hgraph::ts_data_plan_factory_detail

#endif // HGRAPH_CPP_ROOT_TS_DATA_PLAN_FACTORY_DETAIL_H
