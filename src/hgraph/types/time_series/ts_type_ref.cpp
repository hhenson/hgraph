#include <hgraph/types/time_series/ts_type_ref.h>

#include <hgraph/types/metadata/type_record_registry.h>
#include <hgraph/types/metadata/debug_descriptor.h>
#include <hgraph/types/time_series/ts_data/ops.h>
#include <hgraph/util/scope.h>

#include <stdexcept>
#include <string>
#include <vector>

namespace hgraph
{
    namespace
    {
        [[nodiscard]] bool migrated_kind(const TSValueTypeMetaData &schema) noexcept
        {
            return schema.kind == TSTypeKind::TS || schema.kind == TSTypeKind::SIGNAL ||
                   schema.kind == TSTypeKind::TSS || schema.kind == TSTypeKind::TSD ||
                   schema.kind == TSTypeKind::REF || schema.kind == TSTypeKind::TSB ||
                   schema.kind == TSTypeKind::TSL || schema.kind == TSTypeKind::TSW;
        }

        void validate_ts_record(const TypeRecord &record)
        {
            if (!record.valid() || record.schema->family != TypeFamily::TimeSeries ||
                (record.role != TypeRole::Data && record.role != TypeRole::Input && record.role != TypeRole::Output))
            {
                throw std::invalid_argument("TSRoleTypeRef requires a TimeSeries Data/Input/Output TypeRecord");
            }
            const auto *schema = reinterpret_cast<const TSValueTypeMetaData *>(record.schema);
            if (record.schema->kind != static_cast<TypeKind>(schema->kind))
            {
                throw std::invalid_argument("TSRoleTypeRef requires matching common and time-series schema kinds");
            }
            if (record.ops_abi_version != TS_DATA_OPS_ABI_VERSION)
            {
                throw std::invalid_argument(
                    "TSRoleTypeRef requires TSData ops ABI version " +
                    std::to_string(TS_DATA_OPS_ABI_VERSION));
            }
            const auto *ops = static_cast<const TSDataOps *>(record.ops);
            if (!migrated_kind(*schema) || ops == nullptr || ops->kind != schema->kind)
            {
                throw std::invalid_argument("TSRoleTypeRef requires matching migrated TSData ops");
            }
            if (record.capabilities != ts_type_capabilities(record.role, *record.plan, *ops))
            {
                throw std::invalid_argument("TSRoleTypeRef capabilities do not match its role, plan, and ops");
            }
        }
    }

    bool is_migrated_ts_root_schema(const TSValueTypeMetaData *schema) noexcept
    {
        return schema != nullptr && migrated_kind(*schema);
    }

    TypeCapabilities ts_type_capabilities(TypeRole role,
                                          const MemoryUtils::StoragePlan &plan,
                                          const TSDataOps &ops)
    {
        if (role != TypeRole::Data && role != TypeRole::Input && role != TypeRole::Output)
        {
            throw std::invalid_argument("time-series type capabilities require Data, Input, or Output role");
        }
        TypeCapabilities result = TypeCapabilities::Viewable;
        if (ops.kind == TSTypeKind::TSB || ops.kind == TSTypeKind::TSL || ops.kind == TSTypeKind::TSD)
            result |= TypeCapabilities::HasChildren;
        if (plan.can_default_construct()) result |= TypeCapabilities::Constructible;
        if (plan.trivially_destructible || plan.lifecycle.can_destroy()) result |= TypeCapabilities::Destructible;
        if (plan.can_copy_construct()) result |= TypeCapabilities::Copyable;
        if (plan.can_move_construct()) result |= TypeCapabilities::Movable;
        if ((role == TypeRole::Data || role == TypeRole::Output) && ops.allows_mutation)
            result |= TypeCapabilities::Mutable;
        return result;
    }

    TSRoleTypeRef intern_ts_type(const TSValueTypeMetaData &schema,
                                 TypeRole role,
                                 const MemoryUtils::StoragePlan &plan,
                                 const TSDataOps &ops,
                                 std::string_view implementation_label)
    {
        if (!schema.header.valid() || schema.header.family != TypeFamily::TimeSeries ||
            schema.header.kind != static_cast<TypeKind>(schema.kind) ||
            !migrated_kind(schema))
        {
            throw std::invalid_argument("intern_ts_type does not support this time-series schema");
        }
        if (ops.kind != schema.kind)
        {
            throw std::invalid_argument("intern_ts_type requires ops kind matching the schema");
        }
        const TSDataLayout *layout = ops.layout_impl != nullptr ? ops.layout_impl(ops.context) : nullptr;
        const DebugDescriptor *debug = nullptr;
        if (layout != nullptr && layout->value_binding && layout->delta_binding)
        {
            std::vector<DebugField> debug_fields;
            if (schema.kind == TSTypeKind::TSB)
            {
                const auto &bundle = static_cast<const FixedTSBDataLayout &>(*layout);
                debug_fields.reserve(bundle.fields.size());
                for (std::size_t index = 0; index < bundle.fields.size(); ++index)
                {
                    debug_fields.push_back(DebugField{
                        .name = schema.fields()[index].name,
                        .offset = bundle.fields[index].data_offset,
                        .type = bundle.fields[index].type.record(),
                    });
                }
            }
            const TSDataTracking tracking_sample{};
            const auto *tracking_base = reinterpret_cast<const std::byte *>(&tracking_sample);
            const auto last_modified_in_tracking = static_cast<std::size_t>(
                reinterpret_cast<const std::byte *>(&tracking_sample.last_modified_time) - tracking_base);
            const auto parent_in_tracking = static_cast<std::size_t>(
                reinterpret_cast<const std::byte *>(&tracking_sample.parent) - tracking_base);
            const auto observers_in_tracking = static_cast<std::size_t>(
                reinterpret_cast<const std::byte *>(&tracking_sample.observers) - tracking_base);
            debug = &intern_time_series_debug_descriptor(
                schema.header, plan, *layout->value_binding.record(), *layout->delta_binding.record(),
                layout->value_offset, layout->tracking_offset,
                layout->tracking_offset + last_modified_in_tracking,
                layout->tracking_offset + parent_in_tracking,
                layout->tracking_offset + observers_in_tracking,
                schema.kind == TSTypeKind::TS || schema.kind == TSTypeKind::SIGNAL,
                debug_fields.data(), debug_fields.size(),
                &ops);
        }
        const TypeRecordDefinition definition{
            .key = TypeRecordKey{.schema = &schema.header,
                                 .role = role,
                                 .plan = &plan,
                                 .ops = &ops,
                                 .debug = debug,
                                 .implementation_label = implementation_label},
            .ops_abi_version = TS_DATA_OPS_ABI_VERSION,
            .capabilities = ts_type_capabilities(role, plan, ops),
        };
        return TSRoleTypeRef{&TypeRecordRegistry::instance().intern(definition)};
    }

    TSRoleTypeRef TSRoleTypeRef::checked(AnyPtr pointer)
    {
        if (pointer.is_unbound()) return {};
        if (!pointer.well_formed() || pointer.record() == nullptr)
            throw std::invalid_argument("TSRoleTypeRef requires a well-formed pointer");
        validate_ts_record(*pointer.record());
        if (pointer.writable_access() &&
            !has_capability(pointer.record()->capabilities, TypeCapabilities::Mutable))
            throw std::invalid_argument("TSRoleTypeRef writable pointer requires a mutable role");
        return TSRoleTypeRef{pointer.record()};
    }

    bool TSRoleTypeRef::valid() const noexcept
    {
        if (record_ == nullptr) return false;
        return fallback_on_exception(false, [&] {
            validate_ts_record(*record_);
            return true;
        });
    }

    const MemoryUtils::StoragePlan &TSRoleTypeRef::checked_plan() const
    {
        if (plan() == nullptr) throw std::logic_error("TSRoleTypeRef is unbound");
        return *plan();
    }

} // namespace hgraph
