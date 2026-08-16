#include <hgraph/types/metadata/ts_data_plan_factory.h>
#include <hgraph/types/time_series/ts_data/ts_labels.h>

#include <hgraph/types/metadata/debug_descriptor.h>
#include <hgraph/types/metadata/ts_data_plan_factory_detail.h>
#include <hgraph/types/metadata/type_realization.h>

#include <hgraph/types/metadata/value_plan_factory.h>
#include <hgraph/types/time_series/endpoint_schema.h>

#include <fmt/format.h>

#include <stdexcept>

namespace hgraph
{
    namespace plan_detail = ts_data_plan_factory_detail;

    namespace
    {
        [[nodiscard]] bool fixed_root(const TSValueTypeMetaData *schema) noexcept
        {
            return schema != nullptr &&
                   (schema->kind == TSTypeKind::TSB ||
                    (schema->kind == TSTypeKind::TSL && schema->fixed_size() != 0));
        }

        [[nodiscard]] ValueTypeRef realized_value_binding(const ValueTypeMetaData *schema)
        {
            if (const auto *snapshot = active_type_realization(); snapshot != nullptr)
            {
                if (const auto realized = value_type_for_active_realization(schema)) { return realized; }
            }
            return ValuePlanFactory::instance().type_for(schema);
        }

        [[nodiscard]] bool slot_root(const TSValueTypeMetaData *schema) noexcept
        {
            return schema != nullptr && (schema->kind == TSTypeKind::TSS || schema->kind == TSTypeKind::TSD);
        }

        [[nodiscard]] std::string_view root_label(TSTypeKind kind, TypeRole role)
        {
            using ts_labels::Family;
            if (kind == TSTypeKind::TSS)
                return ts_labels::record_label(Family::TSS, role, ts_labels::Position::Root);
            if (kind == TSTypeKind::TSD)
                return ts_labels::record_label(Family::TSD, role, ts_labels::Position::Root);
            if (kind == TSTypeKind::REF)
                return ts_labels::record_label(Family::REF, role, ts_labels::Position::Root);
            return {};
        }

        [[nodiscard]] std::string_view standalone_label(const TSValueTypeMetaData &schema,
                                                        TypeRole role,
                                                        bool embedded)
        {
            const auto position = embedded ? ts_labels::Position::Embedded : ts_labels::Position::Root;
            if (schema.kind == TSTypeKind::TSL && schema.fixed_size() == 0)
                return ts_labels::record_label(ts_labels::Family::TSLDynamic, role, position);
            if (schema.kind == TSTypeKind::TSW)
                return ts_labels::record_label(schema.is_duration_based()
                                                   ? ts_labels::Family::TSWDuration
                                                   : ts_labels::Family::TSWTick,
                                               role, position);
            return {};
        }

        [[nodiscard]] bool migrated_root(const TSValueTypeMetaData *schema) noexcept
        {
            return is_migrated_ts_root_schema(schema);
        }

        [[noreturn]] void unsupported(const TSValueTypeMetaData *schema)
        {
            const auto kind = schema == nullptr ? -1 : static_cast<int>(schema->kind);
            throw std::logic_error(
                fmt::format("TSDataPlanFactory: slot-oriented TSData storage is not implemented for kind {}", kind));
        }
    } // namespace

    namespace ts_data_plan_factory_detail
    {
        TSRoleTypeRef standalone_ts_storage_type(const TSValueTypeMetaData &schema,
                                                    TypeRole role,
                                                    bool embedded)
        {
            if (role != TypeRole::Data && role != TypeRole::Input && role != TypeRole::Output)
                throw std::invalid_argument("standalone TSData storage requires a time-series role");
            if (!is_migrated_ts_root_schema(&schema))
                throw std::invalid_argument("standalone TSData storage requires a migrated schema");

            if (role == TypeRole::Input)
            {
                if (is_dynamic_list_ts_data(schema))
                {
                    const auto *element_schema = schema.element_ts();
                    if (element_schema == nullptr)
                        throw std::logic_error("dynamic TSL element schema is not resolved");
                    const auto *plan = synthesise_dynamic_list_plan(schema);
                    if (plan == nullptr)
                        throw std::logic_error("standalone dynamic TSL storage plan is not resolved");
                    const bool element_embedded = is_dynamic_list_ts_data(*element_schema) ||
                                                  is_window_ts_data(*element_schema);
                    const auto element_type = standalone_ts_storage_type(
                        *element_schema, role, element_embedded);
                    const auto &ops = dynamic_list_ts_data_ops(
                        schema, *plan, 0, element_type, role, embedded);
                    return TSRoleTypeRef{
                        intern_ts_type(schema, role, *plan, ops, standalone_label(schema, role, embedded))};
                }

                if (is_window_ts_data(schema))
                {
                    const auto element_binding = realized_value_binding(schema.value_type);
                    const auto *plan = synthesise_window_plan(schema, element_binding);
                    const auto *window = plan != nullptr ? plan->find_component("window") : nullptr;
                    const auto *tracking = plan != nullptr ? plan->find_component("tracking") : nullptr;
                    if (plan == nullptr || window == nullptr || tracking == nullptr)
                        throw std::logic_error("standalone TSW storage components are not resolved");
                    const auto &ops = window_ts_data_ops(
                        schema, *plan, window->offset, tracking->offset,
                        element_binding, role, embedded);
                    return TSRoleTypeRef{
                        intern_ts_type(schema, role, *plan, ops, standalone_label(schema, role, embedded))};
                }

                if (is_fixed_structured_ts_data(schema))
                {
                    const auto *plan = synthesise_fixed_plan(schema, role);
                    const auto *value = plan != nullptr ? plan->find_component("value") : nullptr;
                    const auto *aux = plan != nullptr ? plan->find_component("aux") : nullptr;
                    if (plan == nullptr || value == nullptr || aux == nullptr)
                        throw std::logic_error("standalone fixed TSData components are not resolved");
                    return embedded_ts_storage_type(
                        schema, role, *plan, value->offset, aux->offset, !embedded);
                }

                if (is_slot_ts_data(schema))
                {
                    const auto *key_schema = schema.kind == TSTypeKind::TSS
                                                 ? schema.value_schema->element_type
                                                 : schema.key_type();
                    const auto key_binding = realized_value_binding(key_schema);
                    if (!key_binding)
                        throw std::logic_error("standalone keyed TSData key binding is not resolved");

                    const MemoryUtils::StoragePlan *plan = nullptr;
                    const TSDataOps *ops = nullptr;
                    if (schema.kind == TSTypeKind::TSD)
                    {
                        const auto *element_schema = schema.element_ts();
                        if (element_schema == nullptr)
                            throw std::logic_error("standalone TSD element schema is not resolved");
                        const auto element_type = standalone_ts_storage_type(*element_schema, role);
                        plan = synthesise_slot_tsd_plan(schema, key_binding, element_type);
                        if (plan != nullptr)
                            ops = &slot_tsd_ts_data_ops(
                                schema, *plan, 0, key_binding, element_type, role, embedded);
                    }
                    else
                    {
                        plan = synthesise_slot_plan(schema, key_binding);
                        if (plan != nullptr)
                            ops = &slot_ts_data_ops(
                                schema, *plan, 0, key_binding, role, embedded);
                    }
                    if (plan == nullptr || ops == nullptr)
                        throw std::logic_error("standalone keyed TSData storage is not resolved");
                    const auto label = ts_labels::record_label(
                        schema.kind == TSTypeKind::TSS ? ts_labels::Family::TSS : ts_labels::Family::TSD,
                        TypeRole::Input,
                        embedded ? ts_labels::Position::Embedded : ts_labels::Position::Root);
                    return TSRoleTypeRef{intern_ts_type(schema, role, *plan, *ops, label)};
                }

                const auto value_type = realized_value_binding(schema.value_schema);
                const auto delta_type = realized_value_binding(schema.delta_value_schema);
                if (!value_type || !delta_type)
                    throw std::logic_error("standalone scalar TSData value types are not resolved");
                auto builder = MemoryUtils::named_tuple();
                builder.reserve(2);
                builder.add_field("value", value_type.checked_plan());
                builder.add_field("tracking", MemoryUtils::plan_for<TSDataTracking>());
                const auto &plan = builder.build();
                const auto *value = plan.find_component("value");
                const auto *tracking = plan.find_component("tracking");
                if (value == nullptr || tracking == nullptr)
                    throw std::logic_error("standalone scalar TSData components are not resolved");
                if (embedded)
                    return embedded_ts_storage_type(
                        schema, role, plan, value->offset, tracking->offset, false);
                const auto &ops = atomic_ts_data_ops(
                    schema.kind, value_type, delta_type, plan, value->offset, tracking->offset);
                const auto label = schema.kind == TSTypeKind::REF
                                       ? ts_labels::record_label(ts_labels::Family::REF, TypeRole::Input,
                                                                 ts_labels::Position::Root)
                                       : std::string_view{};
                return TSRoleTypeRef{intern_ts_type(schema, role, plan, ops, label)};
            }

            auto &factory = TSDataPlanFactory::instance();
            const auto *plan = factory.plan_for(&schema);
            if (plan == nullptr) throw std::logic_error("standalone TSData storage plan is not resolved");

            if (is_dynamic_list_ts_data(schema))
            {
                const auto *element_schema = schema.element_ts();
                if (element_schema == nullptr)
                    throw std::logic_error("dynamic TSL element schema is not resolved");
                const bool element_embedded = is_dynamic_list_ts_data(*element_schema) ||
                                              is_window_ts_data(*element_schema);
                const auto element_type = standalone_ts_storage_type(*element_schema, role, element_embedded);
                const auto &ops = dynamic_list_ts_data_ops(schema, *plan, 0, element_type, role, embedded);
                return TSRoleTypeRef{
                    intern_ts_type(schema, role, *plan, ops, standalone_label(schema, role, embedded))};
            }

            if (is_window_ts_data(schema))
            {
                const auto *window = plan->find_component("window");
                const auto *tracking = plan->find_component("tracking");
                if (window == nullptr || tracking == nullptr)
                    throw std::logic_error("standalone TSW storage components are not resolved");
                const auto &ops = window_ts_data_ops(schema, *plan, window->offset, tracking->offset,
                                                     role, embedded);
                return TSRoleTypeRef{
                    intern_ts_type(schema, role, *plan, ops, standalone_label(schema, role, embedded))};
            }

            if (is_fixed_structured_ts_data(schema))
            {
                const auto *value = plan->find_component("value");
                const auto *aux = plan->find_component("aux");
                if (value == nullptr || aux == nullptr)
                    throw std::logic_error("standalone fixed TSData components are not resolved");
                return embedded_ts_storage_type(schema, role, *plan, value->offset, aux->offset, !embedded);
            }

            if (is_slot_ts_data(schema))
            {
                const auto &ops = slot_ts_data_ops(schema, *plan, 0, role, embedded);
                const auto label = ts_labels::record_label(
                    schema.kind == TSTypeKind::TSS ? ts_labels::Family::TSS : ts_labels::Family::TSD,
                    role, embedded ? ts_labels::Position::Embedded : ts_labels::Position::Root);
                return TSRoleTypeRef{intern_ts_type(schema, role, *plan, ops, label)};
            }

            const auto *value = plan->find_component("value");
            const auto *tracking = plan->find_component("tracking");
            if (value == nullptr || tracking == nullptr)
                throw std::logic_error("standalone scalar TSData components are not resolved");
            if (embedded)
                return embedded_ts_storage_type(schema, role, *plan, value->offset, tracking->offset, false);
            const auto value_type = ValuePlanFactory::instance().type_for(schema.value_schema);
            const auto delta_type = ValuePlanFactory::instance().type_for(schema.delta_value_schema);
            if (!value_type || !delta_type)
                throw std::logic_error("standalone scalar TSData value types are not resolved");
            const auto &ops = atomic_ts_data_ops(schema.kind, value_type, delta_type, *plan,
                                                 value->offset, tracking->offset);
            const auto label = schema.kind == TSTypeKind::REF
                                   ? ts_labels::record_label(ts_labels::Family::REF, role,
                                                             ts_labels::Position::Root)
                                   : std::string_view{};
            return TSRoleTypeRef{intern_ts_type(schema, role, *plan, ops, label)};
        }
    }  // namespace ts_data_plan_factory_detail

    TSDataPlanFactory &TSDataPlanFactory::instance()
    {
        // Immortal (see OperatorRegistry::instance).
        static TSDataPlanFactory *factory = new TSDataPlanFactory();
        return *factory;
    }

    const MemoryUtils::StoragePlan *TSDataPlanFactory::plan_for(const TSValueTypeMetaData *schema)
    {
        if (schema == nullptr)
        {
            return nullptr;
        }

        {
            std::lock_guard lock(mutex_);
            if (const auto it = cache_.find(schema); it != cache_.end())
            {
                return it->second;
            }
        }

        return synthesise(schema);
    }

    const MemoryUtils::StoragePlan *TSDataPlanFactory::find(const TSValueTypeMetaData *schema) const
    {
        if (schema == nullptr)
        {
            return nullptr;
        }

        std::lock_guard lock(mutex_);
        const auto                  it = cache_.find(schema);
        return it == cache_.end() ? nullptr : it->second;
    }

    void TSDataPlanFactory::reset() noexcept
    {
        std::lock_guard lock(mutex_);
        clear_debug_descriptors(TypeFamily::TimeSeries);
        cache_.clear();
        data_type_cache_.clear();
        output_type_cache_.clear();
        realized_output_type_cache_.clear();
        plan_detail::clear_atomic_ts_data_ops();
        plan_detail::clear_fixed_ts_data_contexts();
        plan_detail::clear_dynamic_list_ts_data_contexts();
        plan_detail::clear_window_ts_data_contexts();
        plan_detail::clear_slot_ts_data_contexts();
        clear_tsd_proxy_contexts();
    }

    TSDataTypeRef TSDataPlanFactory::data_type_for(const TSValueTypeMetaData *schema)
    {
        if (!migrated_root(schema)) throw std::invalid_argument("data_type_for requires a migrated TS schema");
        {
            std::lock_guard lock(mutex_);
            if (const auto it = data_type_cache_.find(schema); it != data_type_cache_.end()) return it->second;
        }
        if (plan_detail::is_dynamic_list_ts_data(*schema) || plan_detail::is_window_ts_data(*schema))
        {
            const auto storage_type = plan_detail::standalone_ts_storage_type(*schema, TypeRole::Data);
            const auto type = TSDataTypeRef::checked(storage_type);
            std::lock_guard lock(mutex_);
            return data_type_cache_.try_emplace(schema, type).first->second;
        }
        const auto *plan = plan_for(schema);
        if (slot_root(schema))
        {
            if (plan == nullptr) throw std::logic_error("data_type_for could not resolve keyed storage");
            const auto &ops = plan_detail::slot_ts_data_ops(*schema, *plan, 0, TypeRole::Data);
            const auto type = TSDataTypeRef::checked(intern_ts_type(
                *schema, TypeRole::Data, *plan, ops, root_label(schema->kind, TypeRole::Data)));
            std::lock_guard lock(mutex_);
            return data_type_cache_.try_emplace(schema, type).first->second;
        }
        if (fixed_root(schema))
        {
            const auto *value = plan != nullptr ? plan->find_component("value") : nullptr;
            const auto *aux = plan != nullptr ? plan->find_component("aux") : nullptr;
            if (plan == nullptr || value == nullptr || aux == nullptr)
                throw std::logic_error("data_type_for could not resolve fixed storage");
            const auto storage_type = plan_detail::embedded_ts_storage_type(
                *schema, TypeRole::Data, *plan, value->offset, aux->offset, true);
            const auto type = TSDataTypeRef::checked(storage_type);
            std::lock_guard lock(mutex_);
            return data_type_cache_.try_emplace(schema, type).first->second;
        }
        const auto value_type = ValuePlanFactory::instance().type_for(schema->value_schema);
        const auto delta_type = ValuePlanFactory::instance().type_for(schema->delta_value_schema);
        const auto *value = plan != nullptr ? plan->find_component("value") : nullptr;
        const auto *tracking = plan != nullptr ? plan->find_component("tracking") : nullptr;
        if (plan == nullptr || !value_type || !delta_type || value == nullptr || tracking == nullptr)
            throw std::logic_error("data_type_for could not resolve scalar storage");
        const auto &ops = plan_detail::atomic_ts_data_ops(schema->kind, value_type, delta_type, *plan,
                                                          value->offset, tracking->offset);
        const auto type = checked_ts_role_type(intern_ts_type(
                                                   *schema, TypeRole::Data, *plan, ops,
                                                   root_label(schema->kind, TypeRole::Data)),
                                               std::integral_constant<TypeRole, TypeRole::Data>{});
        std::lock_guard lock(mutex_);
        return data_type_cache_.try_emplace(schema, type).first->second;
    }

    TSOutputTypeRef TSDataPlanFactory::output_type_for(const TSValueTypeMetaData *schema)
    {
        if (!migrated_root(schema)) throw std::invalid_argument("output_type_for requires a migrated TS schema");
        {
            std::lock_guard lock(mutex_);
            if (const auto it = output_type_cache_.find(schema); it != output_type_cache_.end()) return it->second;
        }
        if (plan_detail::is_dynamic_list_ts_data(*schema) || plan_detail::is_window_ts_data(*schema))
        {
            const auto storage_type = plan_detail::standalone_ts_storage_type(*schema, TypeRole::Output);
            const auto type = TSOutputTypeRef::checked(storage_type);
            std::lock_guard lock(mutex_);
            return output_type_cache_.try_emplace(schema, type).first->second;
        }
        if (fixed_root(schema))
        {
            const auto *plan = plan_for(schema);
            const auto *value = plan != nullptr ? plan->find_component("value") : nullptr;
            const auto *aux = plan != nullptr ? plan->find_component("aux") : nullptr;
            if (plan == nullptr || value == nullptr || aux == nullptr)
                throw std::logic_error("output_type_for could not resolve fixed storage");
            const auto storage_type = plan_detail::embedded_ts_storage_type(
                *schema, TypeRole::Output, *plan, value->offset, aux->offset, true);
            const auto type = TSOutputTypeRef::checked(storage_type);
            std::lock_guard lock(mutex_);
            return output_type_cache_.try_emplace(schema, type).first->second;
        }
        if (slot_root(schema))
        {
            const auto *plan = plan_for(schema);
            if (plan == nullptr) throw std::logic_error("output_type_for could not resolve keyed storage");
            const auto &ops = plan_detail::slot_ts_data_ops(*schema, *plan, 0, TypeRole::Output);
            const auto type = TSOutputTypeRef::checked(intern_ts_type(
                *schema, TypeRole::Output, *plan, ops, root_label(schema->kind, TypeRole::Output)));
            std::lock_guard lock(mutex_);
            return output_type_cache_.try_emplace(schema, type).first->second;
        }
        const auto data_type = data_type_for(schema);
        const auto type = checked_ts_role_type(
            intern_ts_type(*schema, TypeRole::Output, data_type.checked_plan(), data_type.ops_ref(),
                           root_label(schema->kind, TypeRole::Output)),
            std::integral_constant<TypeRole, TypeRole::Output>{});
        std::lock_guard lock(mutex_);
        return output_type_cache_.try_emplace(schema, type).first->second;
    }

    TSOutputTypeRef TSDataPlanFactory::output_type_for(
        const TSValueTypeMetaData *schema,
        ValueStorageVariant requested)
    {
        if (requested == ValueStorageVariant::Native ||
            schema == nullptr ||
            schema->kind != TSTypeKind::TS ||
            !plan_detail::is_compact_atomic_ts_data(*schema))
        {
            return output_type_for(schema);
        }
        return output_type_for(
            schema,
            ValuePlanFactory::instance().type_for(schema->value_schema),
            requested);
    }

    TSOutputTypeRef TSDataPlanFactory::output_type_for(const TSValueTypeMetaData *schema,
                                                       ValueTypeRef value_binding,
                                                       ValueStorageVariant requested)
    {
        if (schema == nullptr || !migrated_root(schema))
        {
            throw std::invalid_argument("realized output_type_for requires a migrated TS schema");
        }
        const bool scalar = plan_detail::is_compact_atomic_ts_data(*schema);
        const bool fixed = schema->kind == TSTypeKind::TSB &&
                           plan_detail::is_fixed_structured_ts_data(*schema);
        if ((!scalar && !fixed) || !value_binding || value_binding.schema() != schema->value_schema)
        {
            throw std::invalid_argument(
                "realized output_type_for requires an atomic or fixed TS and a binding for its declared value schema");
        }

        const auto selection =
            scalar && schema->kind == TSTypeKind::TS
                ? ValuePlanFactory::instance().storage_for(value_binding,
                                                           requested)
                : ValueStorageSelection{
                      .effective = ValueStorageVariant::Native,
                      .value_binding = value_binding,
                      .storage_plan = value_binding.plan(),
                  };
        if (scalar && selection.effective == ValueStorageVariant::Native &&
            value_binding ==
                ValuePlanFactory::instance().type_for(schema->value_schema))
        {
            return output_type_for(schema);
        }
        const RealizedOutputKey key{schema, value_binding,
                                    selection.effective};
        {
            std::lock_guard lock(mutex_);
            if (const auto found = realized_output_type_cache_.find(key);
                found != realized_output_type_cache_.end())
            {
                return found->second;
            }
        }

        const auto storage_binding =
            fixed ? plan_detail::fixed_value_storage_binding(*schema, value_binding)
                  : value_binding;
        auto builder = MemoryUtils::named_tuple();
        builder.reserve(2);
        builder.add_field(
            "value",
            scalar ? *selection.storage_plan : storage_binding.checked_plan());
        if (scalar) { builder.add_field("tracking", MemoryUtils::plan_for<TSDataTracking>()); }
        else
        {
            builder.add_field("aux", plan_detail::ts_data_aux_plan(*schema, TypeRole::Output));
        }
        const auto &plan = builder.build();
        const auto &value = plan.component("value");

        TSOutputTypeRef type;
        if (scalar)
        {
            const auto &tracking = plan.component("tracking");
            const auto value_offset = value.offset + selection.value_offset;
            const auto python_value_offset =
                selection.has_python_value()
                    ? value.offset + selection.python_value_offset
                    : ValueStorageSelection::no_offset;
            const auto &ops = plan_detail::atomic_ts_data_ops(
                schema->kind, selection.value_binding,
                selection.value_binding, plan, value_offset, tracking.offset,
                selection.effective, python_value_offset);
            type = checked_ts_role_type(
                intern_ts_type(
                    *schema, TypeRole::Output, plan, ops,
                    selection.effective == ValueStorageVariant::PythonOnly
                        ? "ts.output.python_only"
                    : selection.effective ==
                              ValueStorageVariant::NativeWithPythonCache
                        ? "ts.output.python_cached"
                        : "ts.output.realized"),
                std::integral_constant<TypeRole, TypeRole::Output>{});
        }
        else
        {
            const auto &aux = plan.component("aux");
            type = TSOutputTypeRef::checked(plan_detail::embedded_ts_storage_type(
                *schema, TypeRole::Output, plan, value.offset, aux.offset, true));
        }

        std::lock_guard lock(mutex_);
        return realized_output_type_cache_.try_emplace(key, type).first->second;
    }

    TSOutputTypeRef TSDataPlanFactory::output_type_for(const TSValueTypeMetaData *schema,
                                                       TSRoleTypeRef element_type)
    {
        if (schema == nullptr || schema->kind != TSTypeKind::TSD)
        {
            throw std::invalid_argument(
                "realized TSD output_type_for requires a TSD schema");
        }
        return keyed_output_type_for(
            schema, ValuePlanFactory::instance().type_for(schema->key_type()), element_type);
    }

    TSOutputTypeRef TSDataPlanFactory::keyed_output_type_for(
        const TSValueTypeMetaData *schema,
        ValueTypeRef key_binding,
        TSRoleTypeRef element_type)
    {
        const bool is_tss = schema != nullptr && schema->kind == TSTypeKind::TSS;
        const bool is_tsd = schema != nullptr && schema->kind == TSTypeKind::TSD;
        const auto *key_schema = is_tss && schema->value_schema != nullptr
                                     ? schema->value_schema->element_type
                                     : is_tsd ? schema->key_type() : nullptr;
        if ((!is_tss && !is_tsd) || !key_binding || key_binding.schema() != key_schema ||
            (is_tsd && (!element_type ||
                        !time_series_schema_equivalent(schema->element_ts(), element_type.schema()))) ||
            (is_tss && element_type))
        {
            throw std::invalid_argument(
                "realized keyed output_type_for requires compatible key and element bindings");
        }

        const auto *plan = is_tsd
                               ? plan_detail::synthesise_slot_tsd_plan(
                                     *schema, key_binding, element_type)
                               : plan_detail::synthesise_slot_plan(*schema, key_binding);
        if (plan == nullptr)
        {
            throw std::logic_error("realized keyed output_type_for could not resolve its slot plan");
        }
        const auto &ops = is_tsd
                              ? plan_detail::slot_tsd_ts_data_ops(
                                    *schema, *plan, 0, key_binding, element_type,
                                    TypeRole::Output)
                              : plan_detail::slot_ts_data_ops(
                                    *schema, *plan, 0, key_binding, TypeRole::Output);
        return checked_ts_role_type(
            intern_ts_type(*schema, TypeRole::Output, *plan, ops,
                           ts_labels::record_label(is_tsd ? ts_labels::Family::TSD : ts_labels::Family::TSS,
                                                   TypeRole::Output, ts_labels::Position::Root)),
            std::integral_constant<TypeRole, TypeRole::Output>{});
    }

    TSDataTypeRef TSDataPlanFactory::find_data_type(const TSValueTypeMetaData *schema) const noexcept
    {
        std::lock_guard lock(mutex_);
        const auto it = data_type_cache_.find(schema);
        return it == data_type_cache_.end() ? TSDataTypeRef{} : it->second;
    }

    TSOutputTypeRef TSDataPlanFactory::find_output_type(const TSValueTypeMetaData *schema) const noexcept
    {
        std::lock_guard lock(mutex_);
        const auto it = output_type_cache_.find(schema);
        return it == output_type_cache_.end() ? TSOutputTypeRef{} : it->second;
    }

    const MemoryUtils::StoragePlan *TSDataPlanFactory::synthesise(const TSValueTypeMetaData *schema)
    {
        if (schema == nullptr)
        {
            return nullptr;
        }
        if (plan_detail::is_dynamic_list_ts_data(*schema))
        {
            const auto *plan = plan_detail::synthesise_dynamic_list_plan(*schema);

            std::lock_guard lock(mutex_);
            if (const auto it = cache_.find(schema); it != cache_.end())
            {
                return it->second;
            }
            cache_.emplace(schema, plan);
            return plan;
        }
        if (plan_detail::is_fixed_structured_ts_data(*schema))
        {
            const auto *plan = plan_detail::synthesise_fixed_plan(*schema);

            std::lock_guard lock(mutex_);
            if (const auto it = cache_.find(schema); it != cache_.end())
            {
                return it->second;
            }
            cache_.emplace(schema, plan);
            return plan;
        }
        if (plan_detail::is_window_ts_data(*schema))
        {
            const auto *plan = plan_detail::synthesise_window_plan(*schema);

            std::lock_guard lock(mutex_);
            if (const auto it = cache_.find(schema); it != cache_.end())
            {
                return it->second;
            }
            cache_.emplace(schema, plan);
            return plan;
        }
        if (plan_detail::is_slot_ts_data(*schema))
        {
            const auto *plan = plan_detail::synthesise_slot_plan(*schema);

            std::lock_guard lock(mutex_);
            if (const auto it = cache_.find(schema); it != cache_.end())
            {
                return it->second;
            }
            cache_.emplace(schema, plan);
            return plan;
        }
        if (!plan_detail::is_compact_atomic_ts_data(*schema))
        {
            unsupported(schema);
        }

        const auto value_plan = ValuePlanFactory::instance().plan_for(schema->value_schema);
        if (value_plan == nullptr)
        {
            throw std::logic_error("TSDataPlanFactory: atomic TSData value plan is not resolvable");
        }

        auto builder = MemoryUtils::named_tuple();
        builder.reserve(2);
        builder.add_field("value", *value_plan);
        builder.add_field("tracking", MemoryUtils::plan_for<TSDataTracking>());
        const auto *plan = &builder.build();

        std::lock_guard lock(mutex_);
        if (const auto it = cache_.find(schema); it != cache_.end())
        {
            return it->second;
        }
        cache_.emplace(schema, plan);
        return plan;
    }

} // namespace hgraph
