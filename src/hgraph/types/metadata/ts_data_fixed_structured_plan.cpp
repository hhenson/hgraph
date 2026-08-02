#include <hgraph/types/metadata/ts_data_plan_factory_detail.h>

#include <hgraph/types/metadata/ts_data_plan_factory.h>
#include <hgraph/types/metadata/type_realization.h>
#include <hgraph/types/metadata/value_plan_factory.h>
#include <hgraph/types/value/specialized_views.h>

#include <fmt/format.h>

#include <cstddef>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

namespace hgraph::ts_data_plan_factory_detail
{
    [[nodiscard]] std::string field_component_name(std::size_t index);
    [[nodiscard]] constexpr std::string_view tsl_elements_component_name() noexcept;
    [[nodiscard]] std::size_t fixed_element_count(const TSValueTypeMetaData &schema) noexcept;
    [[nodiscard]] const TSValueTypeMetaData *fixed_element_schema(const TSValueTypeMetaData &schema,
                                                                  std::size_t index);

    namespace
    {
        [[nodiscard]] ValueTypeRef realized_value_binding(const ValueTypeMetaData *schema)
        {
            if (const auto *snapshot = active_type_realization(); snapshot != nullptr)
            {
                if (const auto realized = snapshot->type_for(schema)) { return realized; }
            }
            return ValuePlanFactory::instance().type_for(schema);
        }

        [[nodiscard]] ValueTypeRef fixed_value_storage_binding(const TSValueTypeMetaData &schema)
        {
            return ts_data_plan_factory_detail::fixed_value_storage_binding(
                schema, realized_value_binding(schema.value_schema));
        }

        [[nodiscard]] TSRoleTypeRef realized_output_type(const TSValueTypeMetaData &schema)
        {
            auto &factory = TSDataPlanFactory::instance();
            const auto *snapshot = active_type_realization();
            if (snapshot != nullptr && schema.value_schema != nullptr &&
                (schema.kind == TSTypeKind::TS || schema.kind == TSTypeKind::TSB))
            {
                const auto realized = snapshot->type_for(schema.value_schema);
                const auto canonical = ValuePlanFactory::instance().type_for(schema.value_schema);
                if (realized && realized != canonical)
                {
                    return factory.output_type_for(&schema, realized).as_role();
                }
            }
            if (snapshot != nullptr && schema.kind == TSTypeKind::TSD &&
                schema.key_type() != nullptr && schema.element_ts() != nullptr)
            {
                const auto key = snapshot->type_for(schema.key_type());
                const auto element = realized_output_type(*schema.element_ts());
                const auto canonical_key = ValuePlanFactory::instance().type_for(schema.key_type());
                const auto canonical_element = factory.output_type_for(schema.element_ts()).as_role();
                if (key != canonical_key || element != canonical_element)
                {
                    return factory.keyed_output_type_for(&schema, key, element).as_role();
                }
            }
            if (snapshot != nullptr && schema.kind == TSTypeKind::TSS &&
                schema.value_schema != nullptr)
            {
                const auto *key_schema = schema.value_schema->element_type;
                const auto key = snapshot->type_for(key_schema);
                if (key != ValuePlanFactory::instance().type_for(key_schema))
                {
                    return factory.keyed_output_type_for(&schema, key).as_role();
                }
            }
            return factory.output_type_for(&schema).as_role();
        }

        [[nodiscard]] TSRoleTypeRef realized_input_type(const TSValueTypeMetaData &schema)
        {
            return standalone_ts_storage_type(schema, TypeRole::Input);
        }

        [[nodiscard]] const MemoryUtils::StoragePlan &ts_data_aux_plan_for_role(
            const TSValueTypeMetaData &schema, TypeRole role)
        {
            auto builder = MemoryUtils::named_tuple();
            if (is_compact_atomic_ts_data(schema))
            {
                builder.reserve(1);
                builder.add_field("tracking", MemoryUtils::plan_for<TSDataTracking>());
                return builder.build();
            }
            if (is_slot_ts_data(schema))
            {
                if (role == TypeRole::Input || role == TypeRole::Output)
                {
                    const auto *key_schema = schema.kind == TSTypeKind::TSS
                                                 ? schema.value_schema->element_type
                                                 : schema.key_type();
                    const auto key_binding = realized_value_binding(key_schema);
                    if (schema.kind == TSTypeKind::TSD)
                    {
                        const auto *element_schema = schema.element_ts();
                        if (element_schema == nullptr)
                        {
                            throw std::logic_error("TSDataPlanFactory: TSD element schema is not resolved");
                        }
                        const auto element_type = role == TypeRole::Input
                                                      ? realized_input_type(*element_schema)
                                                      : realized_output_type(*element_schema);
                        const auto *plan = synthesise_slot_tsd_plan(schema, key_binding, element_type);
                        if (plan == nullptr)
                        {
                            throw std::logic_error("TSDataPlanFactory: realized TSD auxiliary plan is not resolved");
                        }
                        return *plan;
                    }
                    const auto *plan = synthesise_slot_plan(schema, key_binding);
                    if (plan == nullptr)
                    {
                        throw std::logic_error("TSDataPlanFactory: realized TSS auxiliary plan is not resolved");
                    }
                    return *plan;
                }
                const auto *plan = synthesise_slot_plan(schema);
                if (plan == nullptr)
                {
                    throw std::logic_error("TSDataPlanFactory: slot auxiliary plan is not resolved");
                }
                return *plan;
            }
            if (is_dynamic_list_ts_data(schema))
            {
                const auto *plan = synthesise_dynamic_list_plan(schema);
                if (plan == nullptr)
                {
                    throw std::logic_error("TSDataPlanFactory: dynamic-list auxiliary plan is not resolved");
                }
                return *plan;
            }
            if (is_window_ts_data(schema))
            {
                const auto *plan = role == TypeRole::Input
                                       ? synthesise_window_plan(
                                             schema, realized_value_binding(schema.value_type))
                                       : synthesise_window_plan(schema);
                if (plan == nullptr)
                {
                    throw std::logic_error("TSDataPlanFactory: window auxiliary plan is not resolved");
                }
                return *plan;
            }
            if (!is_fixed_structured_ts_data(schema))
            {
                throw std::logic_error(
                    fmt::format("TSDataPlanFactory: TSData storage is not implemented for kind {}",
                                static_cast<int>(schema.kind)));
            }

            if (schema.kind == TSTypeKind::TSB)
            {
                const auto count = fixed_element_count(schema);
                builder.reserve(count + 1);
                for (std::size_t index = 0; index < count; ++index)
                {
                    const auto *element_schema = fixed_element_schema(schema, index);
                    if (element_schema == nullptr)
                    {
                        throw std::logic_error("TSDataPlanFactory: fixed TSData element schema is not resolved");
                    }
                    builder.add_field(field_component_name(index),
                                      ts_data_aux_plan_for_role(*element_schema, role));
                }
                builder.add_field("tracking", MemoryUtils::plan_for<TSDataTracking>());
                return builder.build();
            }

            const auto *element_schema = schema.element_ts();
            if (element_schema == nullptr)
            {
                throw std::logic_error("TSDataPlanFactory: fixed TSL element schema is not resolved");
            }
            const auto &element_aux_plan = ts_data_aux_plan_for_role(*element_schema, role);
            builder.reserve(2);
            builder.add_field(tsl_elements_component_name(),
                              MemoryUtils::array_plan(element_aux_plan, schema.fixed_size()));
            builder.add_field("tracking", MemoryUtils::plan_for<TSDataTracking>());
            return builder.build();
        }
    }

    ValueTypeRef fixed_value_storage_binding(
        const TSValueTypeMetaData &schema,
        ValueTypeRef value_binding)
    {
        if (schema.kind != TSTypeKind::TSB || !value_binding)
        {
            return value_binding;
        }

        // A TSB always stores its children independently. A named Bundle may
        // nevertheless have a non-composite owning representation, including
        // a Python-owned object or a closed polymorphic union. Project every
        // nested TSB field recursively so an anonymous outer Bundle cannot
        // embed a child's owning representation in storage that the child
        // treats as structural.
        const auto *value_schema = schema.value_schema;
        if (value_schema == nullptr)
        {
            throw std::logic_error(
                "TSDataPlanFactory: TSB value schema is unresolved");
        }
        const auto *indexed = checked_value_ops<IndexedValueOps>(
            value_binding, "TSDataPlanFactory: TSB value");
        std::vector<ValueTypeRef> fields;
        fields.reserve(value_schema->field_count);
        bool requires_projection = !value_binding.checked_plan().is_composite();
        for (std::size_t index = 0; index < value_schema->field_count; ++index)
        {
            auto field =
                indexed->element_binding(indexed->context, nullptr, index);
            if (!field)
            {
                throw std::logic_error(
                    "TSDataPlanFactory: TSB field binding is unresolved");
            }
            const auto *child_schema = fixed_element_schema(schema, index);
            if (child_schema == nullptr)
            {
                throw std::logic_error(
                    "TSDataPlanFactory: TSB field schema is unresolved");
            }
            const auto projected =
                fixed_value_storage_binding(*child_schema, field);
            requires_projection = requires_projection || projected != field;
            field = projected;
            fields.push_back(field);
        }
        if (!requires_projection)
        {
            return value_binding;
        }
        return ValuePlanFactory::instance().projected_composite_type_for(
            value_schema, fields);
    }

    [[nodiscard]] std::string field_component_name(std::size_t index)
    {
        return fmt::format("field_{}", index);
    }

    [[nodiscard]] constexpr std::string_view tsl_elements_component_name() noexcept
    {
        return "elements";
    }

    [[nodiscard]] bool is_compact_atomic_ts_data(const TSValueTypeMetaData &schema) noexcept
    {
        // Whole-value time series: the delta IS the value, stored as a single
        // value + tracking. The value is an atomic scalar or a compound scalar
        // (a value-layer ``Bundle``, e.g. ``TS<NodeError>``) — both copy whole
        // through the value plan. Time-series *structure* (TSB/TSL/TSD/…) is a
        // different schema kind and handled by the structured/slot paths.
        switch (schema.kind)
        {
        case TSTypeKind::TS:
        case TSTypeKind::REF:
        case TSTypeKind::SIGNAL:
        {
            // Whole-value scalars: atomics, compound scalars (Bundle), and
            // IMMUTABLE container scalars (Tuple/List/Set/Map - python's
            // tuple/frozenset/frozendict values) - all copy whole through
            // their value plans. Mutable (slot-store-backed) containers are
            // NOT whole-value copyable and stay excluded.
            const auto whole_value = [](const ValueTypeMetaData &meta) noexcept {
                const auto kind = meta.try_value_kind();
                if (!kind.has_value()) { return false; }
                switch (*kind)
                {
                    case ValueTypeKind::Atomic:
                    case ValueTypeKind::Bundle:
                    // Any-storage scalars (Any itself and the JSON tree):
                    // the box copies whole through the Any value plan.
                    case ValueTypeKind::Any: return true;
                    case ValueTypeKind::Tuple:
                    case ValueTypeKind::List:
                    case ValueTypeKind::Set:
                    case ValueTypeKind::Map: return !meta.is_mutable();
                    default: return false;
                }
            };
            return schema.value_schema != nullptr && schema.delta_value_schema != nullptr &&
                   schema.value_schema == schema.delta_value_schema &&
                   whole_value(*schema.value_schema);
        }
        default:
            return false;
        }
    }

    [[nodiscard]] bool is_fixed_structured_ts_data(const TSValueTypeMetaData &schema) noexcept
    {
        switch (schema.kind)
        {
        case TSTypeKind::TSB:
            return schema.value_schema != nullptr && schema.delta_value_schema != nullptr &&
                   (schema.field_count() == 0 || schema.fields() != nullptr);
        case TSTypeKind::TSL:
            return schema.fixed_size() != 0 && schema.element_ts() != nullptr && schema.value_schema != nullptr &&
                   schema.delta_value_schema != nullptr;
        default:
            return false;
        }
    }

    [[nodiscard]] std::size_t fixed_element_count(const TSValueTypeMetaData &schema) noexcept
    {
        switch (schema.kind)
        {
        case TSTypeKind::TSB:
            return schema.field_count();
        case TSTypeKind::TSL:
            return schema.fixed_size();
        default:
            return 0;
        }
    }

    [[nodiscard]] const TSValueTypeMetaData *fixed_element_schema(const TSValueTypeMetaData &schema, std::size_t index)
    {
        switch (schema.kind)
        {
        case TSTypeKind::TSB:
            if (index >= schema.field_count())
            {
                throw std::out_of_range("TSB field index out of range");
            }
            return schema.fields()[index].type;
        case TSTypeKind::TSL:
            if (index >= schema.fixed_size())
            {
                throw std::out_of_range("TSL element index out of range");
            }
            return schema.element_ts();
        default:
            return nullptr;
        }
    }

    [[nodiscard]] const MemoryUtils::StoragePlan &ts_data_aux_plan(const TSValueTypeMetaData &schema)
    {
        return ts_data_aux_plan_for_role(schema, TypeRole::Data);
    }

    [[nodiscard]] const MemoryUtils::StoragePlan &ts_data_aux_plan(const TSValueTypeMetaData &schema,
                                                                   TypeRole role)
    {
        return ts_data_aux_plan_for_role(schema, role);
    }

    [[nodiscard]] std::size_t fixed_element_value_offset(const TSValueTypeMetaData      &schema,
                                                         const MemoryUtils::StoragePlan &value_plan, std::size_t index)
    {
        switch (schema.kind)
        {
        case TSTypeKind::TSB:
            return value_plan.component(index).offset;
        case TSTypeKind::TSL:
            return value_plan.element_offset(index);
        default:
            throw std::logic_error("TSDataPlanFactory: fixed element value offset requires TSB or TSL");
        }
    }

    [[nodiscard]] std::size_t tracking_offset_in_aux(const MemoryUtils::StoragePlan &aux_plan)
    {
        const auto *tracking_component = aux_plan.find_component("tracking");
        if (tracking_component == nullptr)
        {
            throw std::logic_error("TSDataPlanFactory: TSData auxiliary plan is missing tracking");
        }
        return tracking_component->offset;
    }

    [[nodiscard]] std::size_t fixed_element_aux_offset(const TSValueTypeMetaData      &schema,
                                                       const MemoryUtils::StoragePlan &aux_plan, std::size_t index)
    {
        switch (schema.kind)
        {
        case TSTypeKind::TSB:
        {
            const auto *child_aux_component = aux_plan.find_component(field_component_name(index));
            if (child_aux_component == nullptr)
            {
                throw std::logic_error("TSDataPlanFactory: fixed TSB auxiliary plan is missing a field");
            }
            return child_aux_component->offset;
        }
        case TSTypeKind::TSL:
        {
            const auto *elements_component = aux_plan.find_component(tsl_elements_component_name());
            if (elements_component == nullptr || elements_component->plan == nullptr ||
                !elements_component->plan->is_array())
            {
                throw std::logic_error("TSDataPlanFactory: fixed TSL auxiliary plan is missing element array");
            }
            return elements_component->offset + elements_component->plan->element_offset(index);
        }
        default:
            throw std::logic_error("TSDataPlanFactory: fixed element auxiliary offset requires TSB or TSL");
        }
    }

    namespace
    {
        [[nodiscard]] std::string_view fixed_record_label(const TSValueTypeMetaData &schema,
                                                          TypeRole role,
                                                          bool root_record)
        {
            if (!root_record)
            {
                if (schema.kind == TSTypeKind::TSS)
                    return role == TypeRole::Data ? "ts.tss.data.embedded"
                         : role == TypeRole::Input ? "ts.tss.input.embedded"
                                                   : "ts.tss.output.embedded";
                if (schema.kind == TSTypeKind::TSD)
                    return role == TypeRole::Data ? "ts.tsd.data.embedded"
                         : role == TypeRole::Input ? "ts.tsd.input.embedded"
                                                   : "ts.tsd.output.embedded";
                if (schema.kind == TSTypeKind::REF)
                    return role == TypeRole::Data ? "ts.ref.data.embedded"
                         : role == TypeRole::Input ? "ts.ref.input.embedded"
                                                   : "ts.ref.output.embedded";
                switch (role)
                {
                case TypeRole::Data: return "ts.fixed.data.embedded";
                case TypeRole::Input: return "ts.fixed.input.embedded";
                case TypeRole::Output: return "ts.fixed.output.embedded";
                default: break;
                }
            }
            else
            {
                switch (role)
                {
                case TypeRole::Data: return "ts.fixed.data.root";
                case TypeRole::Input: return "ts.fixed.input.owned";
                case TypeRole::Output: return "ts.fixed.output.root";
                default: break;
                }
            }
            throw std::invalid_argument("fixed TSData role must be Data, Input, or Output");
        }
    }

    TSRoleTypeRef embedded_ts_storage_type(const TSValueTypeMetaData      &schema,
                                              TypeRole                         role,
                                              const MemoryUtils::StoragePlan &root_plan,
                                              std::size_t value_offset,
                                              std::size_t aux_offset,
                                              bool root_record)
    {
        if (role != TypeRole::Data && role != TypeRole::Input && role != TypeRole::Output)
            throw std::invalid_argument("embedded TSData storage requires a time-series role");

        if (is_slot_ts_data(schema))
        {
            const auto *key_schema = schema.kind == TSTypeKind::TSS
                                         ? schema.value_schema->element_type
                                         : schema.key_type();
            const auto key_binding = role == TypeRole::Input || role == TypeRole::Output
                                         ? realized_value_binding(key_schema)
                                         : ValuePlanFactory::instance().type_for(key_schema);
            const auto *plan = synthesise_slot_plan(schema, key_binding);
            if (plan == nullptr) throw std::logic_error("embedded keyed TSData plan is not resolved");
            if ((role == TypeRole::Input || role == TypeRole::Output) && schema.kind == TSTypeKind::TSD)
            {
                const auto *element_schema = schema.element_ts();
                if (element_schema == nullptr)
                {
                    throw std::logic_error("embedded TSD element schema is not resolved");
                }
                const auto element_type = role == TypeRole::Input
                                              ? realized_input_type(*element_schema)
                                              : realized_output_type(*element_schema);
                plan = synthesise_slot_tsd_plan(schema, key_binding, element_type);
                if (plan == nullptr)
                {
                    throw std::logic_error("embedded output TSD plan is not resolved");
                }
                const auto &ops = slot_tsd_ts_data_ops(
                    schema, *plan, 0, key_binding, element_type, role, true);
                return TSRoleTypeRef{intern_ts_type(
                    schema, role, *plan, ops, fixed_record_label(schema, role, false))};
            }
            const auto &ops = slot_ts_data_ops(
                schema, *plan, 0, key_binding, role, true);
            return TSRoleTypeRef{intern_ts_type(
                schema, role, *plan, ops, fixed_record_label(schema, role, false))};
        }

        if (is_dynamic_list_ts_data(schema) || is_window_ts_data(schema))
            return standalone_ts_storage_type(schema, role, true);

        const bool scalar = schema.kind == TSTypeKind::TS || schema.kind == TSTypeKind::SIGNAL ||
                            schema.kind == TSTypeKind::REF;
        if (!scalar && !is_fixed_structured_ts_data(schema))
            throw std::logic_error("embedded TSData storage kind is not supported");

        const auto &aux_plan = ts_data_aux_plan_for_role(schema, role);
        const auto tracking_offset = aux_offset + tracking_offset_in_aux(aux_plan);
        if (scalar)
        {
            const auto value_type = realized_value_binding(schema.value_schema);
            const auto delta_type = realized_value_binding(schema.delta_value_schema);
            if (!value_type || !delta_type)
                throw std::logic_error("embedded scalar TSData value bindings are not resolved");
            const auto &ops = atomic_ts_data_ops(schema.kind, value_type, delta_type, root_plan,
                                                 value_offset, tracking_offset);
            return TSRoleTypeRef{intern_ts_type(
                schema, role, root_plan, ops, fixed_record_label(schema, role, false))};
        }

        const auto value_type = fixed_value_storage_binding(schema);
        if (!value_type) throw std::logic_error("embedded fixed TSData value binding is not resolved");

        const auto &value_plan = value_type.checked_plan();
        const auto count = fixed_element_count(schema);
        std::vector<TSRoleTypeRef> element_types;
        std::vector<std::size_t> element_data_offsets;
        element_types.reserve(count);
        element_data_offsets.reserve(count);
        for (std::size_t index = 0; index < count; ++index)
        {
            const auto *element_schema = fixed_element_schema(schema, index);
            if (element_schema == nullptr)
                throw std::logic_error("embedded fixed TSData element schema is not resolved");
            const auto child_value_offset = value_offset + fixed_element_value_offset(schema, value_plan, index);
            const auto child_aux_offset = aux_offset + fixed_element_aux_offset(schema, aux_plan, index);
            const auto child_type = embedded_ts_storage_type(*element_schema, role, root_plan,
                                                             child_value_offset, child_aux_offset);
            element_data_offsets.push_back(child_type.plan() == &root_plan ? 0 : child_aux_offset);
            element_types.push_back(child_type);
        }
        // Pointer binding: the returned reference is to an interned long-lived context, but
        // GCC's -Wdangling-reference heuristic flags a reference bound from a call that also
        // receives temporaries (the moved vectors).
        const auto *ops = &fixed_structured_ts_data_ops(schema, root_plan, role, value_offset, aux_offset,
                                                        tracking_offset, std::move(element_types),
                                                        std::move(element_data_offsets));
        return TSRoleTypeRef{
            intern_ts_type(schema, role, root_plan, *ops, fixed_record_label(schema, role, root_record))};
    }

    [[nodiscard]] const MemoryUtils::StoragePlan *synthesise_fixed_plan(const TSValueTypeMetaData &schema)
    {
        return synthesise_fixed_plan(schema, TypeRole::Data);
    }

    [[nodiscard]] const MemoryUtils::StoragePlan *synthesise_fixed_plan(
        const TSValueTypeMetaData &schema,
        TypeRole role)
    {
        const auto value_binding = fixed_value_storage_binding(schema);
        if (!value_binding)
        {
            throw std::logic_error("TSDataPlanFactory: fixed TSData value plan is not resolved");
        }
        auto builder = MemoryUtils::named_tuple();
        builder.reserve(2);
        builder.add_field("value", value_binding.checked_plan());
        builder.add_field("aux", ts_data_aux_plan(schema, role));
        return &builder.build();
    }

} // namespace hgraph::ts_data_plan_factory_detail
