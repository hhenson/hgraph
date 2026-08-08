#include <hgraph/types/time_series/ts_input.h>

#include "ts_input/target_link_ops.h"
#include "ts_data/ownership.h"

#include <hgraph/runtime/graph.h>
#include <hgraph/runtime/node.h>
#include <hgraph/types/time_series/ts_input/detail.h>

#include <hgraph/types/metadata/ts_data_plan_factory.h>
#include <hgraph/types/metadata/ts_data_plan_factory_detail.h>
#include <hgraph/types/metadata/type_realization.h>
#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/metadata/value_plan_factory.h>
#include <hgraph/types/value/container_ops.h>
#include <hgraph/types/value/specialized_views.h>
#include <hgraph/types/value/value.h>
#include <hgraph/types/value/value_builder.h>
#include <hgraph/util/scope.h>

#include <algorithm>
#include <array>
#include <cstdint>
#include <fmt/format.h>
#include <iterator>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <utility>

namespace hgraph
{
    namespace
    {
        inline constexpr std::size_t input_npos = static_cast<std::size_t>(-1);

        [[nodiscard]] constexpr std::size_t ts_kind_index(TSTypeKind kind) noexcept
        {
            return static_cast<std::size_t>(kind);
        }

        [[nodiscard]] std::size_t combine_hash(std::size_t seed, std::size_t value) noexcept
        {
            seed ^= value + 0x9e3779b97f4a7c15ULL + (seed << 6U) + (seed >> 2U);
            return seed;
        }

        [[nodiscard]] bool output_view_bound(const TSOutputView &output) noexcept
        {
            return output.output() != nullptr && output.data_view().valid();
        }

        [[nodiscard]] const TSDataView &empty_ts_data_view() noexcept
        {
            static const TSDataView empty{};
            return empty;
        }

        void validate_endpoint_kind(const TSValueTypeMetaData *schema, TSTypeKind expected, const char *what)
        {
            if (schema == nullptr || schema->kind != expected)
            {
                throw std::invalid_argument(std::string{what} + " requires a matching time-series shape");
            }
        }

        void validate_input_endpoint_schema(const TSEndpointSchema &endpoint_schema, bool root)
        {
            const auto *schema = endpoint_schema.schema();
            if (schema == nullptr) { throw std::invalid_argument("TSInput endpoint annotation requires a schema"); }

            if (root)
            {
                const bool scalar = schema->kind == TSTypeKind::TS || schema->kind == TSTypeKind::SIGNAL ||
                                    schema->kind == TSTypeKind::REF;
                const bool direct_peered = endpoint_schema.is_peered();
                const bool owned_scalar = scalar && endpoint_schema.is_owned();
                const bool owned_fixed = (schema->kind == TSTypeKind::TSB ||
                                          (schema->kind == TSTypeKind::TSL && schema->fixed_size() != 0)) &&
                                         endpoint_schema.is_owned();
                const bool owned_keyed = (schema->kind == TSTypeKind::TSS || schema->kind == TSTypeKind::TSD) &&
                                         endpoint_schema.is_owned();
                const bool owned_dynamic = ((schema->kind == TSTypeKind::TSL && schema->fixed_size() == 0) ||
                                            schema->kind == TSTypeKind::TSW) &&
                                           endpoint_schema.is_owned();
                const bool structural_root =
                    (schema->kind == TSTypeKind::TSB ||
                     schema->kind == TSTypeKind::TSD ||
                     (schema->kind == TSTypeKind::TSL &&
                      schema->fixed_size() == 0)) &&
                    endpoint_schema.is_non_peered();
                if (!direct_peered && !owned_scalar && !owned_fixed && !owned_keyed && !owned_dynamic &&
                    !structural_root)
                {
                    throw std::invalid_argument(
                        "TSInput root must be peered, owned, or a supported non-peered composite");
                }
            }

            if (endpoint_schema.is_owned()) { return; }
            if (endpoint_schema.is_peered()) { return; }
            if (schema->kind == TSTypeKind::TSD)
            {
                if (endpoint_schema.child_count() != 1)
                {
                    throw std::invalid_argument("TSInput non-peered TSD prefixes require one element annotation");
                }
                validate_input_endpoint_schema(endpoint_schema.child(0), false);
                return;
            }
            if (schema->kind == TSTypeKind::TSL && schema->fixed_size() == 0)
            {
                if (endpoint_schema.child_count() != 1)
                {
                    throw std::invalid_argument(
                        "TSInput non-peered dynamic TSL prefixes require one element annotation");
                }
                validate_input_endpoint_schema(endpoint_schema.child(0), false);
                return;
            }
            if (schema->kind != TSTypeKind::TSB && schema->kind != TSTypeKind::TSL)
            {
                throw std::invalid_argument("TSInput non-peered prefixes require TSB or fixed-size TSL schemas");
            }
            for (const auto &child : endpoint_schema.children()) { validate_input_endpoint_schema(child, false); }
        }

        void append_endpoint_key(std::string &key, const TSEndpointSchema &endpoint_schema)
        {
            const auto role = static_cast<std::uint8_t>(endpoint_schema.role());
            key.append(reinterpret_cast<const char *>(&role), sizeof(role));

            const auto schema_bits = reinterpret_cast<std::uintptr_t>(endpoint_schema.schema());
            key.append(reinterpret_cast<const char *>(&schema_bits), sizeof(schema_bits));

            if (endpoint_schema.is_non_peered())
            {
                const auto &children = endpoint_schema.children();
                const auto  size     = children.size();
                key.append(reinterpret_cast<const char *>(&size), sizeof(size));
                for (const auto &child : children) { append_endpoint_key(key, child); }
            }
        }

        [[nodiscard]] std::string plan_cache_key(const TSInputConstructionPlan &plan)
        {
            std::string key;
            key.reserve(128);
            append_endpoint_key(key, plan.endpoint_schema());
            return key;
        }

        [[nodiscard]] std::string binding_cache_key(const TSEndpointSchema           &endpoint_schema,
                                                    const MemoryUtils::StoragePlan   &root_plan,
                                                    std::size_t                       storage_offset,
                                                    TypeRole                         storage_role = TypeRole::Input)
        {
            std::string key;
            key.reserve(160);
            const auto root_bits = reinterpret_cast<std::uintptr_t>(&root_plan);
            key.append(reinterpret_cast<const char *>(&root_bits), sizeof(root_bits));
            key.append(reinterpret_cast<const char *>(&storage_offset), sizeof(storage_offset));
            const auto role = static_cast<std::uint8_t>(storage_role);
            key.append(reinterpret_cast<const char *>(&role), sizeof(role));
            append_endpoint_key(key, endpoint_schema);
            return key;
        }

        [[nodiscard]] std::size_t no_endpoint_child_count(const TSValueTypeMetaData *) noexcept { return 0; }
        [[nodiscard]] std::string_view no_endpoint_key_at(const TSValueTypeMetaData *, std::size_t) noexcept { return {}; }
        [[nodiscard]] std::size_t no_endpoint_find_key(const TSValueTypeMetaData *, std::string_view) noexcept
        {
            return input_npos;
        }
        [[nodiscard]] const TSValueTypeMetaData *no_endpoint_child_schema(const TSValueTypeMetaData *,
                                                                          std::size_t) noexcept
        {
            return nullptr;
        }

        [[nodiscard]] std::size_t tsb_endpoint_child_count(const TSValueTypeMetaData *schema) noexcept
        {
            return schema != nullptr ? schema->field_count() : 0;
        }

        [[nodiscard]] std::string_view tsb_endpoint_key_at(const TSValueTypeMetaData *schema,
                                                           std::size_t                index) noexcept
        {
            if (schema == nullptr || index >= schema->field_count()) { return {}; }
            const auto *name = schema->fields()[index].name;
            return name != nullptr ? std::string_view{name} : std::string_view{};
        }

        [[nodiscard]] std::size_t tsb_endpoint_find_key(const TSValueTypeMetaData *schema,
                                                        std::string_view          name) noexcept
        {
            if (schema == nullptr) { return input_npos; }
            for (std::size_t index = 0; index < schema->field_count(); ++index)
            {
                const auto *field_name = schema->fields()[index].name;
                if (field_name != nullptr && name == field_name) { return index; }
            }
            return input_npos;
        }

        [[nodiscard]] const TSValueTypeMetaData *tsb_endpoint_child_schema(const TSValueTypeMetaData *schema,
                                                                           std::size_t                index) noexcept
        {
            return schema != nullptr && index < schema->field_count() ? schema->fields()[index].type : nullptr;
        }

        [[nodiscard]] std::size_t tsl_endpoint_child_count(const TSValueTypeMetaData *schema) noexcept
        {
            return schema != nullptr ? schema->fixed_size() : 0;
        }

        [[nodiscard]] const TSValueTypeMetaData *tsl_endpoint_child_schema(const TSValueTypeMetaData *schema,
                                                                           std::size_t                index) noexcept
        {
            if (schema == nullptr) { return nullptr; }
            if (schema->fixed_size() == 0) { return schema->element_ts(); }
            return index < schema->fixed_size() ? schema->element_ts() : nullptr;
        }

        [[nodiscard]] const TSValueTypeMetaData *tsd_endpoint_child_schema(const TSValueTypeMetaData *schema,
                                                                           std::size_t) noexcept
        {
            return schema != nullptr ? schema->element_ts() : nullptr;
        }

        [[nodiscard]] TSDataView tsb_target_child_at(const TSDataView &parent, std::size_t index)
        {
            auto bundle = parent.as_bundle();
            return bundle.at(index);
        }

        [[nodiscard]] TSDataView tsl_target_child_at(const TSDataView &parent, std::size_t index)
        {
            auto list = parent.as_list();
            return list.at(index);
        }

        [[nodiscard]] TSDataView tsd_target_child_at(const TSDataView &parent, std::size_t slot)
        {
            auto dict = parent.as_dict();
            return dict.at_slot(slot);
        }

        [[nodiscard]] TSDataView tss_structural_observation(const TSDataView &source)
        {
            return source.borrowed_ref();
        }

        [[nodiscard]] TSDataView tsd_structural_observation(const TSDataView &source)
        {
            return source.as_dict().key_set().base();
        }

        [[nodiscard]] bool tss_has_published_structural_state(const TSDataView &source,
                                                              DateTime transition_time)
        {
            const bool modified_now = source.modified(transition_time);
            auto set = source.as_set();
            for (std::size_t slot = 0; slot < set.slot_capacity(); ++slot)
            {
                if (!set.slot_occupied(slot) || (modified_now && set.slot_added(slot))) { continue; }
                if (set.slot_live(slot) || set.slot_removed(slot)) { return true; }
            }
            return false;
        }

        [[nodiscard]] bool tsd_has_published_structural_state(const TSDataView &source,
                                                              DateTime transition_time)
        {
            const bool modified_now = source.modified(transition_time);
            auto dict = source.as_dict();
            for (std::size_t slot = 0; slot < dict.slot_capacity(); ++slot)
            {
                if (!dict.slot_occupied(slot) || (modified_now && dict.slot_added(slot))) { continue; }
                if (dict.slot_removed(slot) ||
                    (dict.slot_live(slot) && dict.at_slot(slot).has_current_value()))
                {
                    return true;
                }
            }
            return false;
        }

        [[nodiscard]] TimeSeriesReference input_leaf_reference(const TSInputView &view)
        {
            return TimeSeriesReference::empty(view.schema());
        }

        [[nodiscard]] TimeSeriesReference input_tsl_reference(const TSInputView &view)
        {
            const auto *schema = view.schema();
            if (schema == nullptr || schema->fixed_size() == 0) { return TimeSeriesReference::empty(schema); }

            auto list = view.as_list();
            std::vector<TimeSeriesReference> items;
            items.reserve(list.size());
            for (std::size_t index = 0; index < list.size(); ++index)
            {
                items.emplace_back(list.at(index).reference());
            }
            return TimeSeriesReference::non_peered(schema, std::move(items));
        }

        [[nodiscard]] TimeSeriesReference input_tsb_reference(const TSInputView &view)
        {
            const auto *schema = view.schema();
            if (schema == nullptr)
            {
                throw std::logic_error("TSInputView::reference requires a typed TSB input view");
            }

            auto bundle = view.as_bundle();
            std::vector<TimeSeriesReference> items;
            items.reserve(bundle.size());
            for (std::size_t index = 0; index < bundle.size(); ++index)
            {
                items.emplace_back(bundle.at(index).reference());
            }
            return TimeSeriesReference::non_peered(schema, std::move(items));
        }

#if HGRAPH_ENABLE_PYTHON_USER_NODES
        [[nodiscard]] nb::object input_tsb_to_python(const void *context, const void *memory);
        [[nodiscard]] nb::object input_tsl_to_python(const void *context, const void *memory);
        [[nodiscard]] nb::object input_tsb_delta_to_python(const void *context,
                                                           const void *memory,
                                                           DateTime evaluation_time);
        [[nodiscard]] nb::object input_tsl_delta_to_python(const void *context,
                                                           const void *memory,
                                                           DateTime evaluation_time);
#endif

        const detail::TSInputEndpointOps endpoint_ts_ops{
            .name = "TS",
            .child_count = &no_endpoint_child_count,
            .key_at = &no_endpoint_key_at,
            .find_key = &no_endpoint_find_key,
            .child_schema = &no_endpoint_child_schema,
            .reference = &input_leaf_reference,
        };

        const detail::TSInputEndpointOps endpoint_tss_ops{
            .name = "TSS",
            .child_count = &no_endpoint_child_count,
            .key_at = &no_endpoint_key_at,
            .find_key = &no_endpoint_find_key,
            .child_schema = &no_endpoint_child_schema,
            .structural_observation = &tss_structural_observation,
            .has_published_structural_state = &tss_has_published_structural_state,
            .reference = &input_leaf_reference,
        };

        const detail::TSInputEndpointOps endpoint_tsd_ops{
            .name = "TSD",
            .child_count = &no_endpoint_child_count,
            .key_at = &no_endpoint_key_at,
            .find_key = &no_endpoint_find_key,
            .child_schema = &tsd_endpoint_child_schema,
            .target_child = &tsd_target_child_at,
            .structural_observation = &tsd_structural_observation,
            .has_published_structural_state = &tsd_has_published_structural_state,
            .reference = &input_leaf_reference,
        };

        const detail::TSInputEndpointOps endpoint_tsl_ops{
            .name = "TSL",
            .supports_input_projection = true,
            .child_count = &tsl_endpoint_child_count,
            .key_at = &no_endpoint_key_at,
            .find_key = &no_endpoint_find_key,
            .child_schema = &tsl_endpoint_child_schema,
            .target_child = &tsl_target_child_at,
            .reference = &input_tsl_reference,
#if HGRAPH_ENABLE_PYTHON_USER_NODES
            .to_python = &input_tsl_to_python,
            .delta_to_python = &input_tsl_delta_to_python,
#endif
        };

        const detail::TSInputEndpointOps endpoint_tsw_ops{
            .name = "TSW",
            .child_count = &no_endpoint_child_count,
            .key_at = &no_endpoint_key_at,
            .find_key = &no_endpoint_find_key,
            .child_schema = &no_endpoint_child_schema,
            .reference = &input_leaf_reference,
        };

        const detail::TSInputEndpointOps endpoint_tsb_ops{
            .name = "TSB",
            .supports_input_projection = true,
            .named_value_projection = true,
            .value_open = '{',
            .value_close = '}',
            .child_count = &tsb_endpoint_child_count,
            .key_at = &tsb_endpoint_key_at,
            .find_key = &tsb_endpoint_find_key,
            .child_schema = &tsb_endpoint_child_schema,
            .target_child = &tsb_target_child_at,
            .reference = &input_tsb_reference,
#if HGRAPH_ENABLE_PYTHON_USER_NODES
            .to_python = &input_tsb_to_python,
            .delta_to_python = &input_tsb_delta_to_python,
#endif
        };

        const detail::TSInputEndpointOps endpoint_ref_ops{
            .name = "REF",
            .child_count = &no_endpoint_child_count,
            .key_at = &no_endpoint_key_at,
            .find_key = &no_endpoint_find_key,
            .child_schema = &no_endpoint_child_schema,
            .reference = &input_leaf_reference,
        };

        const detail::TSInputEndpointOps endpoint_signal_ops{
            .name = "SIGNAL",
            .child_count = &no_endpoint_child_count,
            .key_at = &no_endpoint_key_at,
            .find_key = &no_endpoint_find_key,
            .child_schema = &no_endpoint_child_schema,
            .reference = &input_leaf_reference,
        };

        [[nodiscard]] const detail::TSInputEndpointOps &input_endpoint_ops_for(const TSValueTypeMetaData *schema)
        {
            static constexpr std::size_t kind_count = ts_kind_index(TSTypeKind::SIGNAL) + 1U;
            static const std::array<const detail::TSInputEndpointOps *, kind_count> table{
                &endpoint_ts_ops,
                &endpoint_tss_ops,
                &endpoint_tsd_ops,
                &endpoint_tsl_ops,
                &endpoint_tsw_ops,
                &endpoint_tsb_ops,
                &endpoint_ref_ops,
                &endpoint_signal_ops,
            };

            if (schema == nullptr) { throw std::logic_error("TSInput endpoint ops require a schema"); }
            const auto index = ts_kind_index(schema->kind);
            if (index >= table.size() || table[index] == nullptr)
            {
                throw std::logic_error("TSInput endpoint ops are not registered for the schema kind");
            }
            return *table[index];
        }

        [[nodiscard]] const detail::TSInputEndpointOps &
        non_peered_input_endpoint_ops_for(const TSEndpointSchema &endpoint_schema)
        {
            if (endpoint_schema.is_peered())
            {
                throw std::logic_error("TSInput non-peered endpoint ops requested for a peered endpoint");
            }

            const auto &ops = input_endpoint_ops_for(endpoint_schema.schema());
            if (!ops.supports_input_projection)
            {
                throw std::logic_error("TSInput non-peered endpoint ops are not available for this shape");
            }
            return ops;
        }

        [[nodiscard]] TSRoleTypeRef regular_ts_data_type_for(const TSValueTypeMetaData *schema)
        {
            return schema != nullptr ? TSDataPlanFactory::instance().data_type_for(schema).as_role()
                                     : TSRoleTypeRef{};
        }

        [[nodiscard]] ValueTypeRef regular_value_binding_for(const TSValueTypeMetaData *schema)
        {
            return schema != nullptr ? ValuePlanFactory::instance().type_for(schema->value_schema) : nullptr;
        }

        [[nodiscard]] ValueTypeRef realized_input_value_binding_for(const ValueTypeMetaData *schema)
        {
            if (schema == nullptr) { return {}; }
            if (const auto *snapshot = active_type_realization(); snapshot != nullptr)
            {
                if (const auto realized = value_type_for_active_realization(schema)) { return realized; }
            }
            return ValuePlanFactory::instance().type_for(schema);
        }

        [[nodiscard]] ValueTypeRef realized_input_key_binding_for(const TSValueTypeMetaData &schema)
        {
            const auto *key_schema = schema.kind == TSTypeKind::TSS
                                         ? schema.value_schema != nullptr
                                               ? schema.value_schema->element_type
                                               : nullptr
                                         : schema.kind == TSTypeKind::TSD ? schema.key_type() : nullptr;
            const auto binding = realized_input_value_binding_for(key_schema);
            if (!binding) { throw std::logic_error("TSInput owned keyed binding is not resolved"); }
            return binding;
        }

        [[nodiscard]] const MemoryUtils::StoragePlan &input_storage_plan(const TSEndpointSchema &endpoint_schema);
        [[nodiscard]] TSRoleTypeRef input_storage_type_for(const TSEndpointSchema         &endpoint_schema,
                                                           const MemoryUtils::StoragePlan &root_plan,
                                                           std::size_t storage_offset,
                                                           bool root_record,
                                                           TypeRole storage_role);

        [[nodiscard]] TSRoleTypeRef owned_element_type_for(const TSValueTypeMetaData &schema,
                                                           TypeRole storage_role)
        {
            const auto *element_schema = schema.element_ts();
            if (element_schema == nullptr)
            {
                throw std::logic_error("TSInput owned dynamic element schema is not resolved");
            }
            const auto endpoint = TSEndpointSchema::owned(element_schema);
            const auto &plan = input_storage_plan(endpoint);
            return input_storage_type_for(endpoint, plan, 0, true, storage_role);
        }

        [[nodiscard]] const MemoryUtils::StoragePlan &
        owned_input_storage_plan(const TSValueTypeMetaData &schema)
        {
            const bool scalar = schema.kind == TSTypeKind::TS || schema.kind == TSTypeKind::SIGNAL ||
                                schema.kind == TSTypeKind::REF;
            if (scalar)
            {
                const auto value = realized_input_value_binding_for(schema.value_schema);
                if (!value) { throw std::logic_error("TSInput owned scalar value binding is not resolved"); }
                auto builder = MemoryUtils::named_tuple();
                builder.reserve(2);
                builder.add_field("value", value.checked_plan());
                builder.add_field("tracking", MemoryUtils::plan_for<TSDataTracking>());
                return builder.build();
            }
            if (schema.kind == TSTypeKind::TSB ||
                (schema.kind == TSTypeKind::TSL && schema.fixed_size() != 0))
            {
                const auto *plan = ts_data_plan_factory_detail::synthesise_fixed_plan(
                    schema, TypeRole::Input);
                if (plan == nullptr) { throw std::logic_error("TSInput owned fixed storage plan is not resolved"); }
                return *plan;
            }

            if (schema.kind == TSTypeKind::TSS)
            {
                const auto *plan = ts_data_plan_factory_detail::synthesise_slot_plan(
                    schema, realized_input_key_binding_for(schema));
                if (plan == nullptr) { throw std::logic_error("TSInput owned TSS plan is not resolved"); }
                return *plan;
            }
            if (schema.kind == TSTypeKind::TSD)
            {
                const auto *plan = ts_data_plan_factory_detail::synthesise_slot_tsd_plan(
                    schema, realized_input_key_binding_for(schema),
                    owned_element_type_for(schema, TypeRole::Input));
                if (plan == nullptr) { throw std::logic_error("TSInput owned TSD plan is not resolved"); }
                return *plan;
            }
            if (schema.kind == TSTypeKind::TSL && schema.fixed_size() == 0)
            {
                const auto *plan = ts_data_plan_factory_detail::synthesise_dynamic_list_plan(schema);
                if (plan == nullptr)
                {
                    throw std::logic_error("TSInput owned dynamic TSL plan is not resolved");
                }
                return *plan;
            }
            if (schema.kind == TSTypeKind::TSW)
            {
                const auto *plan = ts_data_plan_factory_detail::synthesise_window_plan(
                    schema, realized_input_value_binding_for(schema.value_type));
                if (plan == nullptr) { throw std::logic_error("TSInput owned TSW plan is not resolved"); }
                return *plan;
            }

            const auto type = regular_ts_data_type_for(&schema);
            if (!type) { throw std::logic_error("TSInput owned endpoint storage requires a TSData type"); }
            return type.checked_plan();
        }

        [[nodiscard]] TSRoleTypeRef input_data_type_for(const TSEndpointSchema         &endpoint_schema,
                                                        const MemoryUtils::StoragePlan &root_plan,
                                                        std::size_t storage_offset,
                                                        TypeRole storage_role,
                                                        std::string_view implementation_label);
        [[nodiscard]] std::string child_component_name(const TSValueTypeMetaData *schema, std::size_t index)
        {
            if (schema != nullptr && schema->kind == TSTypeKind::TSB)
            {
                return fmt::format("field_{}", index);
            }
            return fmt::format("element_{}", index);
        }

        [[nodiscard]] std::size_t child_storage_offset(const TSEndpointSchema           &endpoint_schema,
                                                       const MemoryUtils::StoragePlan   &storage_plan,
                                                       std::size_t                       index)
        {
            const auto *schema = endpoint_schema.schema();
            const auto *component = storage_plan.find_component(child_component_name(schema, index));
            if (component == nullptr)
            {
                throw std::logic_error("TSInput storage plan is missing a child component");
            }
            return component->offset;
        }

        [[nodiscard]] std::size_t tracking_offset(const MemoryUtils::StoragePlan &storage_plan)
        {
            const auto *component = storage_plan.find_component("tracking");
            if (component == nullptr) { throw std::logic_error("TSInput storage plan is missing tracking"); }
            return component->offset;
        }

        [[nodiscard]] const MemoryUtils::StoragePlan &input_storage_plan(const TSEndpointSchema &endpoint_schema)
        {
            if (endpoint_schema.is_peered())
            {
                const auto *schema = endpoint_schema.schema();
                if (schema == nullptr)
                {
                    throw std::logic_error("TSInput peered endpoint storage requires a TSData schema");
                }
                return detail::target_link_storage_plan_for(schema->kind);
            }
            if (endpoint_schema.is_owned())
            {
                const auto *schema = endpoint_schema.schema();
                if (schema == nullptr)
                {
                    throw std::logic_error("TSInput owned endpoint storage requires a TSData schema");
                }
                return owned_input_storage_plan(*schema);
            }

            const auto *schema = endpoint_schema.schema();
            if (schema != nullptr && schema->kind == TSTypeKind::TSD)
            {
                if (endpoint_schema.child_count() != 1)
                {
                    throw std::logic_error("TSInput non-peered TSD storage requires one element annotation");
                }
                const auto element_type = input_storage_type_for(
                    endpoint_schema.child(0), input_storage_plan(endpoint_schema.child(0)), 0, true,
                    TypeRole::Input);
                if (!element_type)
                {
                    throw std::logic_error("TSInput non-peered TSD element type is not resolved");
                }
                const auto *plan = ts_data_plan_factory_detail::synthesise_slot_tsd_plan(
                    *schema, realized_input_key_binding_for(*schema), element_type);
                if (plan == nullptr)
                {
                    throw std::logic_error("TSInput non-peered TSD storage plan is not resolved");
                }
                return *plan;
            }
            if (schema != nullptr && schema->kind == TSTypeKind::TSL &&
                schema->fixed_size() == 0)
            {
                if (endpoint_schema.child_count() != 1)
                {
                    throw std::logic_error(
                        "TSInput non-peered dynamic TSL storage requires one element annotation");
                }
                const auto *plan =
                    ts_data_plan_factory_detail::synthesise_dynamic_list_plan(*schema);
                if (plan == nullptr)
                {
                    throw std::logic_error(
                        "TSInput non-peered dynamic TSL storage plan is not resolved");
                }
                return *plan;
            }

            auto        builder = MemoryUtils::named_tuple();
            builder.reserve(endpoint_schema.children().size() + 1);
            for (std::size_t index = 0; index < endpoint_schema.children().size(); ++index)
            {
                builder.add_field(child_component_name(schema, index),
                                  input_storage_plan(endpoint_schema.children()[index]));
            }
            builder.add_field("tracking", MemoryUtils::plan_for<TSDataTracking>());
            return builder.build();
        }

        // Cached child descriptor for a non-peered TSInput binding. Instances
        // live in process-lifetime binding contexts, not per input allocation
        // or per tick; keep this as schema/binding pointers plus offsets only.
        // Runtime child payload and validity live in the storage plan.
        struct InputChild
        {
            const TSValueTypeMetaData *schema{nullptr};
            TSRoleTypeRef           input_type{};
            TSRoleTypeRef           regular_type{};
            ValueTypeRef regular_value_binding{nullptr};
            std::size_t                data_offset{0};
            bool                       target_link{false};
            bool                       direct_child_memory{false};
            bool                       local_storage{false};
        };

        struct InputBundleDeltaSurface
        {
            IndexedValueOps ops{};
        };

        struct InputListDeltaSurface
        {
            MapValueOps               map_ops{};
            SetValueOps               key_set_ops{};
            ValueTypeRef ordinal_key_binding{nullptr};
            ValueTypeRef map_value_binding{nullptr};
            ValueTypeRef key_set_binding{nullptr};
            std::vector<std::int64_t> ordinal_keys{};
        };

        class InputDeltaSurface
        {
          public:
            enum class Kind : std::uint8_t
            {
                Empty,
                Bundle,
                List,
            };

            InputDeltaSurface() noexcept = default;
            InputDeltaSurface(const InputDeltaSurface &) = delete;
            InputDeltaSurface &operator=(const InputDeltaSurface &) = delete;
            InputDeltaSurface(InputDeltaSurface &&) = delete;
            InputDeltaSurface &operator=(InputDeltaSurface &&) = delete;

            ~InputDeltaSurface() noexcept { destroy(); }

            [[nodiscard]] Kind kind() const noexcept { return kind_; }

            [[nodiscard]] InputBundleDeltaSurface &emplace_bundle()
            {
                destroy();
                std::construct_at(&storage_.bundle);
                kind_ = Kind::Bundle;
                return storage_.bundle;
            }

            [[nodiscard]] InputListDeltaSurface &emplace_list()
            {
                destroy();
                std::construct_at(&storage_.list);
                kind_ = Kind::List;
                return storage_.list;
            }

            [[nodiscard]] InputBundleDeltaSurface &bundle() noexcept { return storage_.bundle; }
            [[nodiscard]] const InputBundleDeltaSurface &bundle() const noexcept { return storage_.bundle; }
            [[nodiscard]] InputListDeltaSurface &list() noexcept { return storage_.list; }
            [[nodiscard]] const InputListDeltaSurface &list() const noexcept { return storage_.list; }

          private:
            union Storage
            {
                InputBundleDeltaSurface bundle;
                InputListDeltaSurface   list;

                Storage() noexcept {}
                ~Storage() noexcept {}
            };

            void destroy() noexcept
            {
                switch (kind_)
                {
                    case Kind::Bundle:
                        std::destroy_at(&storage_.bundle);
                        break;
                    case Kind::List:
                        std::destroy_at(&storage_.list);
                        break;
                    case Kind::Empty:
                        break;
                }
                kind_ = Kind::Empty;
            }

            Kind    kind_{Kind::Empty};
            Storage storage_{};
        };

        struct InputBindingContext
        {
            const TSValueTypeMetaData        *schema{nullptr};
            const detail::TSInputEndpointOps *endpoint_ops{nullptr};
            TSDataLayout                      layout{};
            FixedTSBDataLayout                bundle_layout{};
            IndexedTSDataOps                  ts_data_ops{};
            IndexedValueOps                   value_ops{};
            InputDeltaSurface                 delta{};
            ValueTypeRef value_binding{nullptr};
            ValueTypeRef delta_binding{nullptr};
            std::vector<InputChild>           children{};
        };

        using TargetLinkContext = detail::TSInputTargetLinkContext;
        using TargetLinkContextCache = std::unordered_map<std::string, std::unique_ptr<TargetLinkContext>>;
        using InputBindingContextCache = std::unordered_map<std::string, std::unique_ptr<InputBindingContext>>;
        using TSInputBuilderCache = std::unordered_map<std::string, std::unique_ptr<TSInputBuilder>>;

        [[nodiscard]] TargetLinkContextCache &target_link_context_cache()
        {
            static TargetLinkContextCache cache;
            return cache;
        }

        [[nodiscard]] std::mutex &target_link_context_cache_mutex()
        {
            static std::mutex mutex;
            return mutex;
        }

        [[nodiscard]] InputBindingContextCache &input_binding_context_cache()
        {
            static InputBindingContextCache cache;
            return cache;
        }

        [[nodiscard]] std::recursive_mutex &input_binding_context_cache_mutex()
        {
            static std::recursive_mutex mutex;
            return mutex;
        }

        [[nodiscard]] TSInputBuilderCache &input_builder_cache()
        {
            static TSInputBuilderCache cache;
            return cache;
        }

        [[nodiscard]] std::mutex &input_builder_cache_mutex()
        {
            static std::mutex mutex;
            return mutex;
        }

        void clear_input_binding_caches() noexcept
        {
            {
                std::lock_guard lock{target_link_context_cache_mutex()};
                target_link_context_cache().clear();
            }
            {
                std::lock_guard lock{input_binding_context_cache_mutex()};
                input_binding_context_cache().clear();
            }
            {
                std::lock_guard lock{input_builder_cache_mutex()};
                input_builder_cache().clear();
            }
        }

        [[nodiscard]] const InputBindingContext *input_context_for(const TSDataOps *ops) noexcept;
        [[nodiscard]] const InputBindingContext *input_context_for(TSRoleTypeRef type) noexcept;
        [[nodiscard]] const TargetLinkContext *target_context_for(TSRoleTypeRef type) noexcept;

        [[nodiscard]] const void *advance(const void *memory, std::size_t offset) noexcept
        {
            return static_cast<const std::byte *>(memory) + offset;
        }

        [[nodiscard]] void *advance(void *memory, std::size_t offset) noexcept
        {
            return static_cast<std::byte *>(memory) + offset;
        }

        [[nodiscard]] const detail::TSInputTargetLinkStorage *target_storage(const TSDataView &view) noexcept
        {
            const auto *context = target_context_for(view.storage_type());
            return context != nullptr && view.data() != nullptr
                       ? detail::target_link_storage_at(*context, view.data())
                       : nullptr;
        }

        [[nodiscard]] detail::TSInputTargetLinkStorage *mutable_target_storage(const TSDataView &view)
        {
            const auto *context = target_context_for(view.storage_type());
            return context != nullptr && view.data() != nullptr
                       ? detail::target_link_storage_at(*context, const_cast<void *>(view.data()))
                       : nullptr;
        }

        [[nodiscard]] const detail::TSInputTargetLinkStorage *child_target_storage(const InputChild &child,
                                                                                   const void       *memory) noexcept
        {
            if (!child.target_link || memory == nullptr) { return nullptr; }
            const auto *context = target_context_for(child.input_type);
            return context != nullptr ? detail::target_link_storage_at(*context, memory) : nullptr;
        }

        [[nodiscard]] const TSDataLayout *input_layout(const void *context) noexcept
        {
            const auto *state = static_cast<const InputBindingContext *>(context);
            return state->schema->kind == TSTypeKind::TSB
                       ? static_cast<const TSDataLayout *>(&state->bundle_layout)
                       : &state->layout;
        }

        [[nodiscard]] const TSDataTracking *input_tracking(const void *context, const void *memory) noexcept
        {
            return MemoryUtils::cast<TSDataTracking>(advance(memory, input_layout(context)->tracking_offset));
        }

        [[nodiscard]] TSDataTracking *input_mutable_tracking(const void *context, void *memory) noexcept
        {
            return MemoryUtils::cast<TSDataTracking>(advance(memory, input_layout(context)->tracking_offset));
        }

        [[nodiscard]] TSRoleTypeRef input_value_storage_type(const void *context,
                                                                const void *memory,
                                                                std::size_t index) noexcept
        {
            const auto *state = static_cast<const InputBindingContext *>(context);
            if (index >= state->children.size()) { return {}; }
            const auto &child = state->children[index];
            if (!child.target_link) { return child.input_type; }
            const auto *link = child_target_storage(child, memory);
            return link != nullptr && link->bound() ? link->target_output().data_view().storage_type()
                                                    : child.regular_type;
        }

        [[nodiscard]] const void *input_element_memory(const void *context,
                                                       const void *memory,
                                                       std::size_t index) noexcept
        {
            const auto *state = static_cast<const InputBindingContext *>(context);
            if (index >= state->children.size()) { return nullptr; }
            const auto &child = state->children[index];
            if (!child.target_link)
            {
                return child.direct_child_memory && child.local_storage && memory != nullptr
                           ? advance(memory, child.data_offset)
                           : memory;
            }
            const auto *link = child_target_storage(child, memory);
            return link != nullptr && link->bound() ? link->target_output().data_view().data() : nullptr;
        }

        [[nodiscard]] const void *input_element_storage_memory(const void *context,
                                                               const void *memory,
                                                               std::size_t index) noexcept
        {
            const auto *state = static_cast<const InputBindingContext *>(context);
            if (index >= state->children.size() || memory == nullptr) { return nullptr; }
            const auto &child = state->children[index];
            // Embedded records use root-relative ops. Independently planned
            // children use their local allocation base.
            return child.direct_child_memory && child.local_storage && child.data_offset != 0
                       ? advance(memory, child.data_offset)
                       : memory;
        }

        [[nodiscard]] std::size_t input_owned_child_count(const void *context, const void *) noexcept
        {
            return static_cast<const InputBindingContext *>(context)->children.size();
        }

        [[nodiscard]] detail::TSDataOwnedChild input_owned_child_at(const void *context,
                                                                    void       *memory,
                                                                    std::size_t index) noexcept
        {
            const auto *state = static_cast<const InputBindingContext *>(context);
            if (memory == nullptr || index >= state->children.size()) { return {}; }
            return detail::TSDataOwnedChild{
                .type = state->children[index].input_type,
                .data = const_cast<void *>(input_element_storage_memory(context, memory, index)),
            };
        }

        [[nodiscard]] const detail::TSDataOwnershipOps &input_ownership_ops() noexcept
        {
            static const detail::TSDataOwnershipOps ops{
                .child_count = &input_owned_child_count,
                .child_at = &input_owned_child_at,
            };
            return ops;
        }

        [[nodiscard]] void *input_mutable_element_memory(const void *context,
                                                         void *memory,
                                                         std::size_t index) noexcept
        {
            return const_cast<void *>(input_element_memory(context, memory, index));
        }

        [[nodiscard]] bool input_has_current_value(const void *context, const void *memory)
        {
            const auto *state = static_cast<const InputBindingContext *>(context);
            for (std::size_t index = 0; index < state->children.size(); ++index)
            {
                const auto type = input_value_storage_type(context, memory, index);
                const auto *data    = input_element_memory(context, memory, index);
                if (!type || data == nullptr) { continue; }
                const auto &ops = *type.ops();
                if (ops.has_current_value_impl(ops.context, data)) { return true; }
            }
            return false;
        }

        [[nodiscard]] bool input_all_valid(const void *context, const void *memory)
        {
            const auto *state = static_cast<const InputBindingContext *>(context);
            if (!input_has_current_value(context, memory)) { return false; }
            for (std::size_t index = 0; index < state->children.size(); ++index)
            {
                const auto type = input_value_storage_type(context, memory, index);
                const auto *data    = input_element_memory(context, memory, index);
                if (!type || data == nullptr) { return false; }
                const auto &ops = *type.ops();
                if (!ops.all_valid_impl(ops.context, data)) { return false; }
            }
            return true;
        }

        [[nodiscard]] const void *input_value_memory(const void *, const void *memory) noexcept { return memory; }
        [[nodiscard]] void *input_mutable_value_memory(const void *, void *memory) noexcept { return memory; }
        [[nodiscard]] const void *input_delta_memory(const void *, const void *memory) noexcept { return memory; }
        [[nodiscard]] void *input_mutable_delta_memory(const void *, void *memory) noexcept { return memory; }


        [[nodiscard]] std::size_t input_indexed_size(const void *context, const void *) noexcept
        {
            return static_cast<const InputBindingContext *>(context)->children.size();
        }

        [[nodiscard]] ValueTypeRef input_value_element_binding(const void *context,
                                                                          const void *memory,
                                                                          std::size_t index) noexcept
        {
            const auto *state = static_cast<const InputBindingContext *>(context);
            if (index >= state->children.size()) { return nullptr; }
            const auto &child = state->children[index];
            if (!child.target_link)
            {
                const auto *ops = child.input_type.ops();
                const auto *layout = ops != nullptr ? ops->layout_impl(ops->context) : nullptr;
                return layout != nullptr ? layout->value_binding : ValueTypeRef{};
            }
            const auto *link = child_target_storage(child, memory);
            return link != nullptr && link->bound() ? link->target_view().value().binding() : child.regular_value_binding;
        }

        [[nodiscard]] const void *input_value_element_at(const void *context,
                                                         const void *memory,
                                                         std::size_t index) noexcept
        {
            const auto *state = static_cast<const InputBindingContext *>(context);
            if (index >= state->children.size()) { return nullptr; }
            const auto &child = state->children[index];
            if (!child.target_link)
            {
                const auto &ops = *child.input_type.ops();
                // Same validity rule for DIRECT children: an invalid child
                // is UNSET in the assembled value, never a default.
                const void *child_memory = child.direct_child_memory && child.local_storage && memory != nullptr
                                               ? advance(memory, child.data_offset)
                                               : memory;
                if (!ops.has_current_value_impl(ops.context, child_memory)) { return nullptr; }
                return ops.value_memory_impl(ops.context, child_memory);
            }
            const auto *link = child_target_storage(child, memory);
            if (link == nullptr || !link->bound()) { return nullptr; }
            // A bound-but-INVALID child is UNSET in the assembled value
            // (Bundle field validity, core_concepts.rst): never a default.
            auto target = link->target_view();
            if (!target.valid()) { return nullptr; }
            const auto &target_ops = target.ops();
            if (!target_ops.has_current_value_impl(target_ops.context, target.data())) { return nullptr; }
            return target.value().data();
        }

        [[nodiscard]] ValueView input_value_project_value(const void *context, const void *memory, std::size_t index)
        {
            return ValueView{input_value_element_binding(context, memory, index),
                             input_value_element_at(context, memory, index)};
        }

        [[nodiscard]] Range<ValueView> input_value_make_range(const void *context, const void *memory)
        {
            return Range<ValueView>{.context = context, .memory = memory, .limit = input_indexed_size(context, memory),
                                    .predicate = nullptr, .projector = &input_value_project_value};
        }

        [[nodiscard]] std::size_t input_value_hash(const void *context, const void *memory)
        {
            std::size_t seed = 0;
            const auto  size = input_indexed_size(context, memory);
            for (std::size_t index = 0; index < size; ++index)
            {
                const auto binding = input_value_element_binding(context, memory, index);
                const auto *child   = input_value_element_at(context, memory, index);
                const auto  value   = child != nullptr && binding != nullptr ? binding.ops_ref().hash(child) : 0;
                seed ^= value + 0x9e3779b97f4a7c15ULL + (seed << 6U) + (seed >> 2U);
            }
            return seed;
        }

        [[nodiscard]] bool input_value_equals(const void *context, const void *lhs, const void *rhs) noexcept
        {
            if (lhs == nullptr || rhs == nullptr) { return lhs == rhs; }
            return fallback_on_exception(false, [&] {
                const auto size = input_indexed_size(context, lhs);
                if (input_indexed_size(context, rhs) != size) { return false; }
                for (std::size_t index = 0; index < size; ++index)
                {
                    const auto binding = input_value_element_binding(context, lhs, index);
                    const auto *a       = input_value_element_at(context, lhs, index);
                    const auto *b       = input_value_element_at(context, rhs, index);
                    if (a == nullptr || b == nullptr)
                    {
                        if (a != b) { return false; }
                        continue;
                    }
                    if (binding == nullptr || !binding.ops_ref().equals(a, b)) { return false; }
                }
                return true;
            });
        }

        [[nodiscard]] std::partial_ordering input_value_compare(const void *context,
                                                                const void *lhs,
                                                                const void *rhs) noexcept
        {
            if (const auto order = value_ops_detail::null_order(lhs, rhs)) { return *order; }
            return fallback_on_exception(std::partial_ordering::unordered, [&] {
                const auto size = std::min(input_indexed_size(context, lhs), input_indexed_size(context, rhs));
                for (std::size_t index = 0; index < size; ++index)
                {
                    const auto binding = input_value_element_binding(context, lhs, index);
                    const auto *a       = input_value_element_at(context, lhs, index);
                    const auto *b       = input_value_element_at(context, rhs, index);
                    if (const auto order = value_ops_detail::null_order(a, b)) { return *order; }
                    if (binding == nullptr)
                    {
                        if (a != b) { return std::partial_ordering::unordered; }
                        continue;
                    }
                    const auto order = binding.ops_ref().compare(a, b);
                    if (order != 0) { return order; }
                }
                const auto lhs_size = input_indexed_size(context, lhs);
                const auto rhs_size = input_indexed_size(context, rhs);
                if (lhs_size < rhs_size) { return std::partial_ordering::less; }
                if (lhs_size > rhs_size) { return std::partial_ordering::greater; }
                return std::partial_ordering::equivalent;
            });
        }

        [[nodiscard]] std::string input_value_to_string(const void *context, const void *memory)
        {
            if (memory == nullptr) { return {}; }
            const auto *state = static_cast<const InputBindingContext *>(context);
            const auto &endpoint_ops = *state->endpoint_ops;
            const bool  named = endpoint_ops.named_value_projection;
            fmt::memory_buffer out;
            fmt::format_to(std::back_inserter(out), "{}", endpoint_ops.value_open);
            const auto size = input_indexed_size(context, memory);
            for (std::size_t index = 0; index < size; ++index)
            {
                if (index > 0) { fmt::format_to(std::back_inserter(out), ", "); }
                if (named)
                {
                    const auto key = endpoint_ops.key_at != nullptr ? endpoint_ops.key_at(state->schema, index)
                                                                     : std::string_view{};
                    fmt::format_to(std::back_inserter(out), "{}: ", key);
                }
                const auto binding = input_value_element_binding(context, memory, index);
                const auto *child   = input_value_element_at(context, memory, index);
                if (binding != nullptr && child != nullptr)
                {
                    fmt::format_to(std::back_inserter(out), "{}", binding.ops_ref().to_string(child));
                }
            }
            fmt::format_to(std::back_inserter(out), "{}", endpoint_ops.value_close);
            return fmt::to_string(out);
        }

        [[nodiscard]] ValueTypeRef input_canonical_value_type(const void *, ValueTypeRef view_type)
        {
            const auto type = ValuePlanFactory::instance().type_for(view_type.schema());
            if (!type) { throw std::logic_error("TSInput projected value has no canonical owning type"); }
            return type;
        }

        [[nodiscard]] Value build_input_value_snapshot(const void *context,
                                                       ValueTypeRef binding,
                                                       const void *memory)
        {
            const auto *state = static_cast<const InputBindingContext *>(context);
            if (binding.schema() != state->schema->value_schema)
            {
                throw std::logic_error("TSInput value snapshot requires the canonical parent value schema");
            }

            if (state->endpoint_ops->named_value_projection)
            {
                BundleBuilder builder{binding};
                for (std::size_t index = 0; index < state->children.size(); ++index)
                {
                    auto child_view = input_value_project_value(context, memory, index);
                    if (!child_view.has_value()) { continue; }
                    builder.set(index, Value{child_view});
                }
                return builder.build();
            }

            const auto &plan = binding.checked_plan();
            if (!plan.is_array() || plan.array_count() != state->children.size())
            {
                throw std::logic_error("TSInput fixed-list snapshot requires a matching canonical array plan");
            }
            if (!plan.can_default_construct() || !plan.array_element_plan().can_default_construct())
            {
                throw std::logic_error(
                    "TSInput fixed-list snapshot cannot default-fill invalid children for this canonical element type");
            }
            Value snapshot{binding};
            auto *bytes = static_cast<std::byte *>(const_cast<void *>(snapshot.view().data()));
            const auto &element_plan = plan.array_element_plan();
            for (std::size_t index = 0; index < state->children.size(); ++index)
            {
                auto child_view = input_value_project_value(context, memory, index);
                if (!child_view.has_value()) { continue; }
                Value child{child_view};
                element_plan.copy_assign(bytes + plan.element_offset(index), child.view().data());
            }
            return snapshot;
        }

        void input_value_copy_construct_view(const void *context,
                                             const ValueTypeRef &binding,
                                             void *dst,
                                             const void *memory)
        {
            Value snapshot = build_input_value_snapshot(context, binding, memory);
            binding.copy_construct_at(dst, snapshot.view().data());
        }

        void input_value_copy_assign_view(const void *context,
                                          const ValueTypeRef &binding,
                                          void *dst,
                                          const void *memory)
        {
            Value snapshot = build_input_value_snapshot(context, binding, memory);
            binding.copy_assign_at(dst, snapshot.view().data());
        }

        [[nodiscard]] ValueTypeRef input_child_delta_binding(const void *context,
                                                                        const void *memory,
                                                                        std::size_t index)
        {
            const auto type = input_value_storage_type(context, memory, index);
            if (!type) { return nullptr; }
            const auto &ops = *type.ops();
            const auto *layout = ops.layout_impl(ops.context);
            return layout != nullptr ? layout->delta_binding : nullptr;
        }

        [[nodiscard]] bool input_child_modified_for_parent_time(const void *context,
                                                                const void *memory,
                                                                std::size_t index)
        {
            const auto type = input_value_storage_type(context, memory, index);
            const auto *data = input_element_memory(context, memory, index);
            if (!type || data == nullptr) { return false; }

            const auto &ops = *type.ops();
            const auto *tracking = ops.tracking_impl(ops.context, data);
            return tracking != nullptr && tracking->last_modified_time == input_tracking(context, memory)->last_modified_time;
        }

        [[nodiscard]] ValueView input_child_delta_view(const void *context,
                                                       const void *memory,
                                                       std::size_t index)
        {
            const auto binding = input_child_delta_binding(context, memory, index);
            if (binding == nullptr) { return {}; }
            if (!input_child_modified_for_parent_time(context, memory, index))
            {
                return ValueView{binding, nullptr};
            }

            const auto child_type = input_value_storage_type(context, memory, index);
            const auto *child_data = input_element_memory(context, memory, index);
            const auto &child_ops = *child_type.ops();
            return ValueView{binding, child_ops.delta_memory_impl(child_ops.context, child_data)};
        }

        [[nodiscard]] std::size_t input_view_hash(ValueView view)
        {
            if (!view.has_value()) { return std::hash<ValueTypeRef>{}(view.binding()); }
            return view.hash();
        }

        [[nodiscard]] const void *input_delta_bundle_element_at(const void *context,
                                                                const void *memory,
                                                                std::size_t index)
        {
            return input_child_delta_view(context, memory, index).data();
        }

        [[nodiscard]] ValueTypeRef input_delta_bundle_element_binding(const void *context,
                                                                                 const void *memory,
                                                                                 std::size_t index) noexcept
        {
            return fallback_on_exception<ValueTypeRef>(nullptr, [&] {
                return input_child_delta_binding(context, memory, index);
            });
        }

        [[nodiscard]] ValueView input_delta_bundle_projector(const void *context,
                                                             const void *memory,
                                                             std::size_t index)
        {
            return input_child_delta_view(context, memory, index);
        }

        [[nodiscard]] Range<ValueView> input_delta_bundle_make_range(const void *context, const void *memory)
        {
            return Range<ValueView>{
                .context = context,
                .memory = memory,
                .limit = input_indexed_size(context, memory),
                .predicate = nullptr,
                .projector = &input_delta_bundle_projector,
            };
        }

        [[nodiscard]] std::size_t input_delta_bundle_hash(const void *context, const void *memory)
        {
            std::size_t seed = 0;
            const auto size = input_indexed_size(context, memory);
            for (std::size_t index = 0; index < size; ++index)
            {
                seed = combine_hash(seed, input_view_hash(input_child_delta_view(context, memory, index)));
            }
            return seed;
        }

        [[nodiscard]] bool input_delta_bundle_equals(const void *context,
                                                     const void *lhs,
                                                     const void *rhs) noexcept
        {
            if (lhs == nullptr || rhs == nullptr) { return lhs == rhs; }
            return fallback_on_exception(false, [&] {
                const auto size = input_indexed_size(context, lhs);
                if (input_indexed_size(context, rhs) != size) { return false; }
                for (std::size_t index = 0; index < size; ++index)
                {
                    if (!input_child_delta_view(context, lhs, index).equals(input_child_delta_view(context, rhs, index)))
                    {
                        return false;
                    }
                }
                return true;
            });
        }

        [[nodiscard]] std::partial_ordering input_delta_bundle_compare(const void *context,
                                                                       const void *lhs,
                                                                       const void *rhs) noexcept
        {
            if (const auto order = value_ops_detail::null_order(lhs, rhs)) { return *order; }
            return fallback_on_exception(std::partial_ordering::unordered, [&] {
                const auto size = input_indexed_size(context, lhs);
                if (input_indexed_size(context, rhs) != size) { return std::partial_ordering::unordered; }
                for (std::size_t index = 0; index < size; ++index)
                {
                    const auto order =
                        input_child_delta_view(context, lhs, index).compare(input_child_delta_view(context, rhs, index));
                    if (order != 0) { return order; }
                }
                return std::partial_ordering::equivalent;
            });
        }

        [[nodiscard]] std::string input_delta_bundle_to_string(const void *context, const void *memory)
        {
            const auto *state = static_cast<const InputBindingContext *>(context);
            fmt::memory_buffer out;
            fmt::format_to(std::back_inserter(out), "{{");
            for (std::size_t index = 0; index < state->children.size(); ++index)
            {
                if (index > 0) { fmt::format_to(std::back_inserter(out), ", "); }
                const auto *name = state->schema->fields()[index].name;
                fmt::format_to(std::back_inserter(out), "{}: {}",
                               name != nullptr ? name : "",
                               input_child_delta_view(context, memory, index).to_string());
            }
            fmt::format_to(std::back_inserter(out), "}}");
            return fmt::to_string(out);
        }

        [[nodiscard]] Value build_input_delta_bundle_snapshot(const void *context,
                                                              ValueTypeRef binding,
                                                              const void *memory)
        {
            const auto *state = static_cast<const InputBindingContext *>(context);
            if (binding.schema() != state->schema->delta_value_schema ||
                binding.schema() == nullptr || binding.schema()->value_kind() != ValueTypeKind::Bundle)
            {
                throw std::logic_error("TSInput bundle delta snapshot requires the canonical parent delta schema");
            }
            BundleBuilder builder{binding};
            for (std::size_t index = 0; index < state->children.size(); ++index)
            {
                auto child_view = input_child_delta_view(context, memory, index);
                if (!child_view.has_value()) { continue; }
                builder.set(index, Value{child_view});
            }
            return builder.build();
        }

        void input_delta_bundle_copy_construct_view(const void *context,
                                                    const ValueTypeRef &binding,
                                                    void *dst,
                                                    const void *memory)
        {
            Value snapshot = build_input_delta_bundle_snapshot(context, binding, memory);
            binding.copy_construct_at(dst, snapshot.view().data());
        }

        void input_delta_bundle_copy_assign_view(const void *context,
                                                 const ValueTypeRef &binding,
                                                 void *dst,
                                                 const void *memory)
        {
            Value snapshot = build_input_delta_bundle_snapshot(context, binding, memory);
            binding.copy_assign_at(dst, snapshot.view().data());
        }

        [[nodiscard]] bool input_delta_child_predicate(const void *context, const void *memory, std::size_t index)
        {
            return input_child_modified_for_parent_time(context, memory, index);
        }

        [[nodiscard]] std::size_t input_delta_map_size(const void *context, const void *memory) noexcept
        {
            return fallback_on_exception(std::size_t{0}, [&] {
                const auto *state = static_cast<const InputBindingContext *>(context);
                std::size_t count = 0;
                for (std::size_t index = 0; index < state->children.size(); ++index)
                {
                    if (input_child_modified_for_parent_time(context, memory, index)) { ++count; }
                }
                return count;
            });
        }

        [[nodiscard]] std::size_t input_nth_modified_child(const InputBindingContext *state,
                                                           const void *memory,
                                                           std::size_t ordinal)
        {
            std::size_t seen = 0;
            for (std::size_t index = 0; index < state->children.size(); ++index)
            {
                if (!input_child_modified_for_parent_time(state, memory, index)) { continue; }
                if (seen++ == ordinal) { return index; }
            }
            throw std::out_of_range("TSInput TSL delta map index out of range");
        }

        [[nodiscard]] const void *input_delta_map_key_at_index(const void *context,
                                                               const void *memory,
                                                               std::size_t index)
        {
            const auto *state = static_cast<const InputBindingContext *>(context);
            const auto &delta = state->delta.list();
            return &delta.ordinal_keys[input_nth_modified_child(state, memory, index)];
        }

        [[nodiscard]] ValueTypeRef input_delta_map_key_binding(const void *context,
                                                                          const void *,
                                                                          std::size_t) noexcept
        {
            return static_cast<const InputBindingContext *>(context)->delta.list().ordinal_key_binding;
        }

        [[nodiscard]] const void *input_delta_map_value_at_index(const void *context,
                                                                 const void *memory,
                                                                 std::size_t index)
        {
            const auto *state = static_cast<const InputBindingContext *>(context);
            return input_child_delta_view(context, memory, input_nth_modified_child(state, memory, index)).data();
        }

        [[nodiscard]] ValueTypeRef input_delta_map_value_binding(const void *context,
                                                                            const void *) noexcept
        {
            return static_cast<const InputBindingContext *>(context)->delta.list().map_value_binding;
        }

        [[nodiscard]] bool input_delta_map_contains(const void *context, const void *memory, const void *key)
        {
            const auto *state = static_cast<const InputBindingContext *>(context);
            const auto index = *MemoryUtils::cast<std::int64_t>(key);
            return index >= 0 && static_cast<std::size_t>(index) < state->children.size() &&
                   input_child_modified_for_parent_time(context, memory, static_cast<std::size_t>(index));
        }

        [[nodiscard]] const void *input_delta_map_value_at(const void *context, const void *memory, const void *key)
        {
            const auto *state = static_cast<const InputBindingContext *>(context);
            const auto index = *MemoryUtils::cast<std::int64_t>(key);
            if (index < 0) { return nullptr; }
            const auto slot = static_cast<std::size_t>(index);
            if (slot >= state->children.size() || !input_child_modified_for_parent_time(context, memory, slot))
            {
                return nullptr;
            }
            return input_child_delta_view(context, memory, slot).data();
        }

        [[nodiscard]] ValueView input_delta_map_key_projector(const void *context,
                                                              const void *,
                                                              std::size_t index)
        {
            const auto *state = static_cast<const InputBindingContext *>(context);
            const auto &delta = state->delta.list();
            return ValueView{delta.ordinal_key_binding, &delta.ordinal_keys[index]};
        }

        [[nodiscard]] ValueView input_delta_map_value_projector(const void *context,
                                                                const void *memory,
                                                                std::size_t index)
        {
            return input_child_delta_view(context, memory, index);
        }

        [[nodiscard]] std::pair<ValueView, ValueView> input_delta_map_kv_projector(const void *context,
                                                                                   const void *memory,
                                                                                   std::size_t index)
        {
            return {input_delta_map_key_projector(context, memory, index),
                    input_delta_map_value_projector(context, memory, index)};
        }

        [[nodiscard]] Range<ValueView> input_delta_map_make_keys_range(const void *context, const void *memory)
        {
            return Range<ValueView>{
                .context = context,
                .memory = memory,
                .limit = input_indexed_size(context, memory),
                .predicate = &input_delta_child_predicate,
                .projector = &input_delta_map_key_projector,
            };
        }

        [[nodiscard]] Range<ValueView> input_delta_map_make_values_range(const void *context, const void *memory)
        {
            return Range<ValueView>{
                .context = context,
                .memory = memory,
                .limit = input_indexed_size(context, memory),
                .predicate = &input_delta_child_predicate,
                .projector = &input_delta_map_value_projector,
            };
        }

        [[nodiscard]] KeyValueRange<ValueView, ValueView> input_delta_map_make_kv_range(const void *context,
                                                                                        const void *memory)
        {
            return KeyValueRange<ValueView, ValueView>{
                .context = context,
                .memory = memory,
                .limit = input_indexed_size(context, memory),
                .predicate = &input_delta_child_predicate,
                .projector = &input_delta_map_kv_projector,
            };
        }

        [[nodiscard]] SetView input_delta_map_key_set(const void *context,
                                                      ValueTypeRef,
                                                      const void *memory)
        {
            const auto *state = static_cast<const InputBindingContext *>(context);
            return ValueView{state->delta.list().key_set_binding, memory}.as_set();
        }

        [[nodiscard]] std::size_t input_delta_map_hash(const void *context, const void *memory)
        {
            const auto *state = static_cast<const InputBindingContext *>(context);
            const auto &delta = state->delta.list();
            std::size_t result = 0;
            for (std::size_t index = 0; index < state->children.size(); ++index)
            {
                if (!input_child_modified_for_parent_time(context, memory, index)) { continue; }
                const auto key_hash = delta.ordinal_key_binding.ops_ref().hash(&delta.ordinal_keys[index]);
                const auto value_hash = input_view_hash(input_child_delta_view(context, memory, index));
                result ^= combine_hash(key_hash, value_hash);
            }
            return result;
        }

        [[nodiscard]] bool input_delta_map_equals(const void *context, const void *lhs, const void *rhs) noexcept
        {
            if (lhs == nullptr || rhs == nullptr) { return lhs == rhs; }
            return fallback_on_exception(false, [&] {
                const auto *state = static_cast<const InputBindingContext *>(context);
                if (input_delta_map_size(context, lhs) != input_delta_map_size(context, rhs)) { return false; }
                for (std::size_t index = 0; index < state->children.size(); ++index)
                {
                    if (!input_child_modified_for_parent_time(context, lhs, index)) { continue; }
                    if (!input_child_modified_for_parent_time(context, rhs, index)) { return false; }
                    if (!input_child_delta_view(context, lhs, index).equals(input_child_delta_view(context, rhs, index)))
                    {
                        return false;
                    }
                }
                return true;
            });
        }

        [[nodiscard]] std::partial_ordering input_delta_map_compare(const void *context,
                                                                    const void *lhs,
                                                                    const void *rhs) noexcept
        {
            if (const auto order = value_ops_detail::null_order(lhs, rhs)) { return *order; }
            return input_delta_map_equals(context, lhs, rhs) ? std::partial_ordering::equivalent
                                                             : std::partial_ordering::unordered;
        }

        [[nodiscard]] std::string input_delta_map_to_string(const void *context, const void *memory)
        {
            const auto *state = static_cast<const InputBindingContext *>(context);
            const auto &delta = state->delta.list();
            fmt::memory_buffer out;
            fmt::format_to(std::back_inserter(out), "{{");
            bool first = true;
            for (std::size_t index = 0; index < state->children.size(); ++index)
            {
                if (!input_child_modified_for_parent_time(context, memory, index)) { continue; }
                if (!first) { fmt::format_to(std::back_inserter(out), ", "); }
                first = false;
                fmt::format_to(std::back_inserter(out), "{}: {}",
                               delta.ordinal_keys[index],
                               input_child_delta_view(context, memory, index).to_string());
            }
            fmt::format_to(std::back_inserter(out), "}}");
            return fmt::to_string(out);
        }

        [[nodiscard]] MapStorage build_input_delta_map_storage(const void *context,
                                                               ValueTypeRef binding,
                                                               const void *memory)
        {
            const auto *state = static_cast<const InputBindingContext *>(context);
            if (binding.schema() != state->schema->delta_value_schema ||
                binding.schema() == nullptr || binding.schema()->value_kind() != ValueTypeKind::Map)
            {
                throw std::logic_error("TSInput list delta snapshot requires the canonical parent delta map schema");
            }
            const auto key_type = ValuePlanFactory::instance().type_for(binding.schema()->key_type);
            const auto value_type = ValuePlanFactory::instance().type_for(binding.schema()->element_type);
            if (!key_type || !value_type)
            {
                throw std::logic_error("TSInput list delta snapshot bindings are not resolved");
            }

            MapBuilder builder{key_type, value_type};
            for (const auto [key, value] : input_delta_map_make_kv_range(context, memory))
            {
                if (!value.has_value())
                {
                    builder.set_item_unset(key.data());
                    continue;
                }
                Value owned{value};
                if (owned.binding() != value_type)
                {
                    throw std::logic_error("TSInput list delta snapshot materialized the wrong value type");
                }
                builder.set_item_copy(key.data(), owned.view().data());
            }
            return builder.build_storage();
        }

        void input_delta_map_copy_construct_view(const void *context,
                                                 const ValueTypeRef &binding,
                                                 void *dst,
                                                 const void *memory)
        {
            auto storage = build_input_delta_map_storage(context, binding, memory);
            std::construct_at(static_cast<MapStorage *>(dst), std::move(storage));
        }

        void input_delta_map_copy_assign_view(const void *context,
                                              const ValueTypeRef &binding,
                                              void *dst,
                                              const void *memory)
        {
            *static_cast<MapStorage *>(dst) = build_input_delta_map_storage(context, binding, memory);
        }

        [[nodiscard]] std::size_t input_delta_key_set_hash(const void *context, const void *memory)
        {
            const auto *state = static_cast<const InputBindingContext *>(context);
            const auto &delta = state->delta.list();
            std::size_t result = 0;
            for (std::size_t index = 0; index < state->children.size(); ++index)
            {
                if (!input_child_modified_for_parent_time(context, memory, index)) { continue; }
                result ^= delta.ordinal_key_binding.ops_ref().hash(&delta.ordinal_keys[index]);
            }
            return result;
        }

        [[nodiscard]] bool input_delta_key_set_equals(const void *context,
                                                      const void *lhs,
                                                      const void *rhs) noexcept
        {
            if (lhs == nullptr || rhs == nullptr) { return lhs == rhs; }
            return fallback_on_exception(false, [&] {
                const auto *state = static_cast<const InputBindingContext *>(context);
                for (std::size_t index = 0; index < state->children.size(); ++index)
                {
                    if (input_child_modified_for_parent_time(context, lhs, index) !=
                        input_child_modified_for_parent_time(context, rhs, index))
                    {
                        return false;
                    }
                }
                return true;
            });
        }

        [[nodiscard]] std::partial_ordering input_delta_key_set_compare(const void *context,
                                                                        const void *lhs,
                                                                        const void *rhs) noexcept
        {
            if (const auto order = value_ops_detail::null_order(lhs, rhs)) { return *order; }
            const auto lhs_size = input_delta_map_size(context, lhs);
            const auto rhs_size = input_delta_map_size(context, rhs);
            if (lhs_size < rhs_size) { return std::partial_ordering::less; }
            if (lhs_size > rhs_size) { return std::partial_ordering::greater; }
            return input_delta_key_set_equals(context, lhs, rhs) ? std::partial_ordering::equivalent
                                                                 : std::partial_ordering::unordered;
        }

        [[nodiscard]] std::string input_delta_key_set_to_string(const void *context, const void *memory)
        {
            const auto *state = static_cast<const InputBindingContext *>(context);
            const auto &delta = state->delta.list();
            fmt::memory_buffer out;
            fmt::format_to(std::back_inserter(out), "{{");
            bool first = true;
            for (std::size_t index = 0; index < state->children.size(); ++index)
            {
                if (!input_child_modified_for_parent_time(context, memory, index)) { continue; }
                if (!first) { fmt::format_to(std::back_inserter(out), ", "); }
                first = false;
                fmt::format_to(std::back_inserter(out), "{}", delta.ordinal_keys[index]);
            }
            fmt::format_to(std::back_inserter(out), "}}");
            return fmt::to_string(out);
        }

        [[nodiscard]] SetStorage build_input_delta_key_set_storage(const void *context,
                                                                   ValueTypeRef binding,
                                                                   const void *memory)
        {
            const auto *state = static_cast<const InputBindingContext *>(context);
            if (binding.schema() == nullptr || binding.schema()->value_kind() != ValueTypeKind::Set)
            {
                throw std::logic_error("TSInput list delta key-set snapshot requires a canonical set schema");
            }
            const auto key_type = ValuePlanFactory::instance().type_for(binding.schema()->element_type);
            if (!key_type || key_type != state->delta.list().ordinal_key_binding)
            {
                throw std::logic_error("TSInput list delta key-set snapshot key type is not resolved");
            }
            SetBuilder builder{key_type};
            for (const auto key : input_delta_map_make_keys_range(context, memory))
            {
                builder.insert_copy(key.data());
            }
            return builder.build_storage();
        }

        void input_delta_key_set_copy_construct_view(const void *context,
                                                     const ValueTypeRef &binding,
                                                     void *dst,
                                                     const void *memory)
        {
            auto storage = build_input_delta_key_set_storage(context, binding, memory);
            std::construct_at(static_cast<SetStorage *>(dst), std::move(storage));
        }

        void input_delta_key_set_copy_assign_view(const void *context,
                                                  const ValueTypeRef &binding,
                                                  void *dst,
                                                  const void *memory)
        {
            *static_cast<SetStorage *>(dst) = build_input_delta_key_set_storage(context, binding, memory);
        }

#if HGRAPH_ENABLE_PYTHON_USER_NODES
        [[nodiscard]] nb::object input_delta_bundle_value_to_python(const void *context, const void *memory)
        {
            const auto *state = static_cast<const InputBindingContext *>(context);
            nb::dict result;
            for (std::size_t index = 0; index < state->children.size(); ++index)
            {
                if (!input_child_modified_for_parent_time(context, memory, index)) { continue; }
                const auto *name = state->schema->fields()[index].name;
                if (name == nullptr || *name == '\0') { continue; }
                result[nb::str{name}] = input_child_delta_view(context, memory, index).to_python();
            }
            return result;
        }

        [[nodiscard]] nb::object input_delta_map_value_to_python(const void *context, const void *memory)
        {
            const auto *state = static_cast<const InputBindingContext *>(context);
            const auto &delta = state->delta.list();
            nb::dict result;
            for (std::size_t index = 0; index < state->children.size(); ++index)
            {
                if (!input_child_modified_for_parent_time(context, memory, index)) { continue; }
                result[nb::int_{delta.ordinal_keys[index]}] = input_child_delta_view(context, memory, index).to_python();
            }
            return result;
        }

        [[nodiscard]] nb::object input_delta_key_set_to_python(const void *context, const void *memory)
        {
            const auto *state = static_cast<const InputBindingContext *>(context);
            const auto &delta = state->delta.list();
            nb::set result;
            for (std::size_t index = 0; index < state->children.size(); ++index)
            {
                if (input_child_modified_for_parent_time(context, memory, index))
                {
                    result.add(nb::int_{delta.ordinal_keys[index]});
                }
            }
            return result;
        }

        [[nodiscard]] nb::object child_value_to_python(TSRoleTypeRef type, const void *memory)
        {
            if (!type || memory == nullptr) { return nb::none(); }
            const auto &ops = *type.ops();
            if (!ops.has_current_value_impl(ops.context, memory)) { return nb::none(); }
            return ops.to_python_impl(ops.context, memory);
        }

        [[nodiscard]] nb::object child_delta_to_python(TSRoleTypeRef type,
                                                       const void          *memory,
                                                       DateTime        evaluation_time)
        {
            if (!type || memory == nullptr) { return nb::none(); }
            const auto &ops = *type.ops();
            const auto *tracking = ops.tracking_impl(ops.context, memory);
            if (tracking == nullptr || tracking->last_modified_time != evaluation_time) { return nb::none(); }
            return ops.delta_to_python_impl(ops.context, memory, evaluation_time);
        }

        [[nodiscard]] nb::object input_tsb_to_python(const void *context, const void *memory)
        {
            const auto *state = static_cast<const InputBindingContext *>(context);
            nb::dict result;
            for (std::size_t index = 0; index < state->children.size(); ++index)
            {
                const auto &field = state->schema->fields()[index];
                if (field.name == nullptr) { continue; }
                result[nb::str{field.name}] =
                    child_value_to_python(input_value_storage_type(context, memory, index),
                                          input_element_memory(context, memory, index));
            }
            return result;
        }

        [[nodiscard]] nb::object input_tsl_to_python(const void *context, const void *memory)
        {
            const auto *state = static_cast<const InputBindingContext *>(context);
            nb::list result;
            for (std::size_t index = 0; index < state->children.size(); ++index)
            {
                result.append(child_value_to_python(input_value_storage_type(context, memory, index),
                                                    input_element_memory(context, memory, index)));
            }
            return result;
        }

        [[nodiscard]] nb::object input_to_python(const void *context, const void *memory)
        {
            const auto *state = static_cast<const InputBindingContext *>(context);
            const auto &endpoint_ops = *state->endpoint_ops;
            if (endpoint_ops.to_python == nullptr)
            {
                throw std::logic_error("TSInput non-peered to_python is not available for this endpoint shape");
            }
            return endpoint_ops.to_python(context, memory);
        }

        [[nodiscard]] nb::object input_tsb_delta_to_python(const void *context,
                                                           const void *memory,
                                                           DateTime evaluation_time)
        {
            const auto *state = static_cast<const InputBindingContext *>(context);
            nb::dict result;
            for (std::size_t index = 0; index < state->children.size(); ++index)
            {
                const auto &field = state->schema->fields()[index];
                if (field.name == nullptr) { continue; }
                auto child_delta = child_delta_to_python(input_value_storage_type(context, memory, index),
                                                         input_element_memory(context, memory, index),
                                                         evaluation_time);
                if (!child_delta.is_none()) { result[nb::str{field.name}] = child_delta; }
            }
            return result;
        }

        [[nodiscard]] nb::object input_tsl_delta_to_python(const void *context,
                                                           const void *memory,
                                                           DateTime evaluation_time)
        {
            const auto *state = static_cast<const InputBindingContext *>(context);
            nb::dict result;
            for (std::size_t index = 0; index < state->children.size(); ++index)
            {
                auto child_delta = child_delta_to_python(input_value_storage_type(context, memory, index),
                                                         input_element_memory(context, memory, index),
                                                         evaluation_time);
                if (!child_delta.is_none()) { result[nb::int_{index}] = child_delta; }
            }
            return result;
        }

        [[nodiscard]] nb::object input_delta_to_python(const void *context,
                                                       const void *memory,
                                                       DateTime evaluation_time)
        {
            const auto *state = static_cast<const InputBindingContext *>(context);
            const auto &endpoint_ops = *state->endpoint_ops;
            if (endpoint_ops.delta_to_python == nullptr)
            {
                throw std::logic_error("TSInput non-peered delta_to_python is not available for this endpoint shape");
            }
            return endpoint_ops.delta_to_python(context, memory, evaluation_time);
        }
#endif

        [[nodiscard]] const TSDataOps &target_link_ops_for(const TSEndpointSchema         &endpoint_schema,
                                                           const MemoryUtils::StoragePlan &root_plan,
                                                           std::size_t storage_offset)
        {
            const auto key = binding_cache_key(endpoint_schema, root_plan, storage_offset);
            std::lock_guard lock{target_link_context_cache_mutex()};
            auto &cache = target_link_context_cache();
            if (const auto it = cache.find(key); it != cache.end())
            {
                return *it->second->active_ops;
            }

            const auto *schema = endpoint_schema.schema();
            const auto regular_type = regular_ts_data_type_for(schema);
            if (schema == nullptr || !regular_type)
            {
                throw std::logic_error("TSInput target link requires a resolved regular TSData type");
            }

            const auto &regular_ops = regular_type.ops_ref();
            const auto *regular_layout = regular_ops.layout_impl(regular_ops.context);
            if (regular_layout == nullptr)
            {
                throw std::logic_error("TSInput target-link binding requires a resolved regular TSData layout");
            }

            auto context = detail::target_link_context_builder_for(schema->kind)(
                *schema, root_plan, storage_offset, *regular_layout);
            const auto *ops = context->active_ops;
            cache.emplace(key, std::move(context));
            return *ops;
        }

        [[nodiscard]] TSRoleTypeRef input_data_type_for(const TSEndpointSchema         &endpoint_schema,
                                                        const MemoryUtils::StoragePlan &root_plan,
                                                        std::size_t storage_offset,
                                                        TypeRole storage_role,
                                                        std::string_view implementation_label)
        {
            if (endpoint_schema.is_peered())
            {
                const auto *schema = endpoint_schema.schema();
                if (schema == nullptr) return {};
                return intern_ts_type(*schema, storage_role, root_plan,
                                      target_link_ops_for(endpoint_schema, root_plan, storage_offset),
                                      implementation_label);
            }
            if (endpoint_schema.is_owned())
            {
                const auto &local_plan = input_storage_plan(endpoint_schema);
                const bool root_record = storage_offset == 0 && &root_plan == &local_plan;
                return input_storage_type_for(
                    endpoint_schema, root_plan, storage_offset, root_record, storage_role);
            }

            const auto *schema = endpoint_schema.schema();
            if (schema != nullptr && schema->kind == TSTypeKind::TSD)
            {
                if (endpoint_schema.child_count() != 1)
                {
                    throw std::logic_error("TSInput non-peered TSD binding requires one element annotation");
                }
                if (storage_offset != 0)
                {
                    throw std::logic_error("TSInput non-peered TSD binding currently requires root storage");
                }
                const auto &child_schema = endpoint_schema.child(0);
                const auto &child_plan = input_storage_plan(child_schema);
                const auto element_type = input_storage_type_for(
                    child_schema, child_plan, 0, true, storage_role);
                if (!element_type)
                {
                    throw std::logic_error("TSInput non-peered TSD element type is not resolved");
                }
                const auto key_binding = realized_input_key_binding_for(*schema);
                const auto *expected_plan = ts_data_plan_factory_detail::synthesise_slot_tsd_plan(
                    *schema, key_binding, element_type);
                if (expected_plan == nullptr || expected_plan != &root_plan)
                {
                    throw std::logic_error("TSInput non-peered TSD binding received the wrong storage plan");
                }
                const auto &ops = ts_data_plan_factory_detail::slot_tsd_ts_data_ops(
                    *schema, root_plan, storage_offset, key_binding, element_type, storage_role);
                return intern_ts_type(*schema, storage_role, root_plan, ops,
                                      implementation_label);
            }
            if (schema != nullptr && schema->kind == TSTypeKind::TSL &&
                schema->fixed_size() == 0)
            {
                if (endpoint_schema.child_count() != 1)
                {
                    throw std::logic_error(
                        "TSInput non-peered dynamic TSL binding requires one element annotation");
                }
                const auto &child_schema = endpoint_schema.child(0);
                const auto &child_plan = input_storage_plan(child_schema);
                const auto element_type = input_storage_type_for(
                    child_schema, child_plan, 0, true, storage_role);
                if (!element_type)
                {
                    throw std::logic_error(
                        "TSInput non-peered dynamic TSL element type is not resolved");
                }
                const auto *expected_plan =
                    ts_data_plan_factory_detail::synthesise_dynamic_list_plan(*schema);
                if (expected_plan == nullptr)
                {
                    throw std::logic_error(
                        "TSInput non-peered dynamic TSL binding has no storage plan");
                }
                const bool root_record =
                    storage_offset == 0 && expected_plan == &root_plan;
                const auto &ops = ts_data_plan_factory_detail::dynamic_list_ts_data_ops(
                    *schema, root_plan, storage_offset, element_type, storage_role,
                    !root_record);
                return intern_ts_type(*schema, storage_role, root_plan, ops,
                                      implementation_label);
            }

            const auto key = binding_cache_key(endpoint_schema, root_plan, storage_offset, storage_role);
            std::lock_guard lock{input_binding_context_cache_mutex()};
            auto &cache = input_binding_context_cache();
            if (const auto it = cache.find(key); it != cache.end())
            {
                return intern_ts_type(*endpoint_schema.schema(), storage_role, root_plan,
                                      it->second->ts_data_ops, implementation_label);
            }

            const auto &local_plan = input_storage_plan(endpoint_schema);
            auto context = std::make_unique<InputBindingContext>();
            context->schema = endpoint_schema.schema();
            context->endpoint_ops = &non_peered_input_endpoint_ops_for(endpoint_schema);
            context->layout.tracking_offset = storage_offset + tracking_offset(local_plan);
            context->bundle_layout.tracking_offset = context->layout.tracking_offset;
            if (context->schema->kind == TSTypeKind::TSB)
            {
                context->bundle_layout.fields.reserve(endpoint_schema.children().size());
            }
            context->children.reserve(endpoint_schema.children().size());
            InputListDeltaSurface *list_delta = nullptr;
            if (!context->endpoint_ops->named_value_projection)
            {
                list_delta = &context->delta.emplace_list();
                list_delta->ordinal_keys.reserve(endpoint_schema.children().size());
            }

            for (std::size_t index = 0; index < endpoint_schema.children().size(); ++index)
            {
                const auto &child_schema = endpoint_schema.children()[index];
                const auto child_offset = storage_offset + child_storage_offset(endpoint_schema, local_plan, index);
                const auto child_type = input_storage_type_for(
                    child_schema, root_plan, child_offset, false, storage_role);
                if (storage_role == TypeRole::Output && child_type.role() != TypeRole::Output)
                {
                    throw std::logic_error("fixed output child storage requires an Output role record");
                }
                context->children.push_back(InputChild{
                    .schema = child_schema.schema(),
                    .input_type = child_type,
                    .regular_type = regular_ts_data_type_for(child_schema.schema()),
                    .regular_value_binding = regular_value_binding_for(child_schema.schema()),
                    .data_offset = child_offset,
                    .target_link = child_schema.is_peered(),
                    .direct_child_memory = child_schema.is_owned(),
                    .local_storage = child_type.plan() != &root_plan,
                });
                if (context->schema->kind == TSTypeKind::TSB)
                {
                    const auto *child_layout = child_type.ops_ref().layout_impl(child_type.ops_ref().context);
                    context->bundle_layout.fields.push_back(FixedTSDataFieldLayout{
                        .type = child_type,
                        .layout = child_layout,
                        .data_offset = child_offset,
                    });
                }
                if (list_delta != nullptr)
                {
                    list_delta->ordinal_keys.push_back(static_cast<std::int64_t>(index));
                }
            }

            context->value_ops = IndexedValueOps{
                {ValueOpsKind::Indexed, context.get(), false, &input_value_hash, &input_value_equals,
                 &input_value_compare,
                 &input_value_to_string},
                &input_indexed_size,
                &input_value_element_at,
                &input_value_element_binding,
                &input_value_make_range,
                nullptr,
            };
            context->value_ops.owning_type_impl = &input_canonical_value_type;
            context->value_ops.copy_construct_view_impl = &input_value_copy_construct_view;
            context->value_ops.copy_assign_view_impl = &input_value_copy_assign_view;

            context->value_binding = intern_value_type(*context->schema->value_schema, root_plan,
                                                               context->value_ops);
            context->layout.value_binding = context->value_binding;
            context->bundle_layout.value_binding = context->value_binding;

            const auto *delta_schema = context->schema->delta_value_schema;
            if (delta_schema == nullptr)
            {
                throw std::logic_error("TSInput data binding requires a delta schema");
            }

            if (context->endpoint_ops->named_value_projection)
            {
                auto &delta = context->delta.emplace_bundle();
                delta.ops = IndexedValueOps{
                    {ValueOpsKind::Indexed, context.get(), false, &input_delta_bundle_hash, &input_delta_bundle_equals,
                     &input_delta_bundle_compare, &input_delta_bundle_to_string
#if HGRAPH_ENABLE_PYTHON_USER_NODES
                     ,
                     &input_delta_bundle_value_to_python
#endif
                    },
                    &input_indexed_size,
                    &input_delta_bundle_element_at,
                    &input_delta_bundle_element_binding,
                    &input_delta_bundle_make_range,
                    nullptr,
                };
                delta.ops.owning_type_impl = &input_canonical_value_type;
                delta.ops.copy_construct_view_impl = &input_delta_bundle_copy_construct_view;
                delta.ops.copy_assign_view_impl = &input_delta_bundle_copy_assign_view;
                context->delta_binding = intern_value_type(*delta_schema, root_plan, delta.ops);
            }
            else
            {
                auto &delta = *list_delta;
                delta.ordinal_key_binding = ValuePlanFactory::instance().type_for(delta_schema->key_type);
                delta.map_value_binding = input_child_delta_binding(context.get(), nullptr, 0);
                if (delta.ordinal_key_binding == nullptr || delta.map_value_binding == nullptr)
                {
                    throw std::logic_error("TSInput fixed-list delta bindings are not resolved");
                }

                delta.map_ops = MapValueOps{
                    {{ValueOpsKind::Map, context.get(), false, &input_delta_map_hash, &input_delta_map_equals,
                      &input_delta_map_compare, &input_delta_map_to_string
#if HGRAPH_ENABLE_PYTHON_USER_NODES
                      ,
                      &input_delta_map_value_to_python
#endif
                     },
                     &input_delta_map_size,
                     &input_delta_map_key_at_index,
                     &input_delta_map_key_binding,
                     &input_delta_map_make_keys_range,
                     nullptr},
                    &input_delta_map_contains,
                    &input_delta_map_value_at,
                    &input_delta_map_value_at_index,
                    &input_delta_map_value_binding,
                    &input_delta_map_make_keys_range,
                    &input_delta_map_make_values_range,
                    &input_delta_map_make_kv_range,
                    &input_delta_map_key_set,
                };
                delta.map_ops.owning_type_impl = &input_canonical_value_type;
                delta.map_ops.copy_construct_view_impl = &input_delta_map_copy_construct_view;
                delta.map_ops.copy_assign_view_impl = &input_delta_map_copy_assign_view;

                delta.key_set_ops = SetValueOps{
                    {{ValueOpsKind::Set, context.get(), false, &input_delta_key_set_hash, &input_delta_key_set_equals,
                      &input_delta_key_set_compare, &input_delta_key_set_to_string
#if HGRAPH_ENABLE_PYTHON_USER_NODES
                      ,
                      &input_delta_key_set_to_python
#endif
                     },
                     &input_delta_map_size,
                     &input_delta_map_key_at_index,
                     &input_delta_map_key_binding,
                     &input_delta_map_make_keys_range,
                     nullptr},
                    &input_delta_map_contains,
                };
                delta.key_set_ops.owning_type_impl = &input_canonical_value_type;
                delta.key_set_ops.copy_construct_view_impl = &input_delta_key_set_copy_construct_view;
                delta.key_set_ops.copy_assign_view_impl = &input_delta_key_set_copy_assign_view;

                const auto *key_set_schema = TypeRegistry::instance().set(delta_schema->key_type);
                delta.key_set_binding = intern_value_type(*key_set_schema, root_plan, delta.key_set_ops);
                context->delta_binding = intern_value_type(*delta_schema, root_plan, delta.map_ops);
            }
            context->layout.delta_binding = context->delta_binding;
            context->bundle_layout.delta_binding = context->delta_binding;

            context->ts_data_ops = IndexedTSDataOps{};
            TSDataOps &base_ops = context->ts_data_ops;
            base_ops = TSDataOps{
                .context = context.get(),
                .kind = context->schema->kind,
                .allows_mutation = true,
                .ownership_ops = &input_ownership_ops(),
                .layout_impl = &input_layout,
                .tracking_impl = &input_tracking,
                .mutable_tracking_impl = &input_mutable_tracking,
                .has_current_value_impl = &input_has_current_value,
                .all_valid_impl = &input_all_valid,
                .value_memory_impl = &input_value_memory,
                .mutable_value_memory_impl = &input_mutable_value_memory,
                .delta_memory_impl = &input_delta_memory,
                .mutable_delta_memory_impl = &input_mutable_delta_memory,
                .capture_delta_impl = context->schema->kind == TSTypeKind::TSB
                                          ? &ts_data_detail::capture_delta_tsb
                                          : &ts_data_detail::capture_delta_tsl,
                .indexed_child_count_impl = &input_indexed_size,
                .indexed_child_binding_impl = &input_value_storage_type,
                .indexed_child_memory_impl = &input_element_memory,
                .mutable_indexed_child_memory_impl = &input_mutable_element_memory,
#if HGRAPH_ENABLE_PYTHON_USER_NODES
                .to_python_impl = &input_to_python,
                .delta_to_python_impl = &input_delta_to_python,
#endif
            };
            context->ts_data_ops.size_impl = &input_indexed_size;
            context->ts_data_ops.element_binding_impl = &input_value_storage_type;
            context->ts_data_ops.element_memory_impl = &input_element_memory;
            context->ts_data_ops.mutable_element_memory_impl = &input_mutable_element_memory;

            const auto type = intern_ts_type(
                *context->schema, storage_role, root_plan, context->ts_data_ops, implementation_label);
            cache.emplace(key, std::move(context));
            return type;
        }

        [[nodiscard]] const InputBindingContext *input_context_for(const TSDataOps *ops) noexcept
        {
            return ops != nullptr && ops->ownership_ops == &input_ownership_ops()
                       ? static_cast<const InputBindingContext *>(ops->context)
                       : nullptr;
        }

        [[nodiscard]] const InputBindingContext *input_context_for(TSRoleTypeRef type) noexcept
        {
            return input_context_for(type.ops());
        }

        [[nodiscard]] const TargetLinkContext *target_context_for(TSRoleTypeRef type) noexcept
        {
            return detail::target_link_context_for_ops(type.ops());
        }

        [[nodiscard]] TSInputTypeRef scalar_input_type_for(const TSEndpointSchema &endpoint_schema)
        {
            const auto *schema = endpoint_schema.schema();
            if (schema == nullptr || (schema->kind != TSTypeKind::TS && schema->kind != TSTypeKind::SIGNAL &&
                                      schema->kind != TSTypeKind::REF) ||
                endpoint_schema.is_non_peered())
            {
                throw std::invalid_argument("scalar input type requires a peered or owned TS/SIGNAL endpoint");
            }

            if (endpoint_schema.is_owned())
            {
                const auto &root_plan = input_storage_plan(endpoint_schema);
                return TSInputTypeRef::checked(input_storage_type_for(
                    endpoint_schema, root_plan, 0, true, TypeRole::Input));
            }

            const auto data_type = TSDataPlanFactory::instance().data_type_for(schema);
            const auto &root_plan = input_storage_plan(endpoint_schema);
            const auto key = binding_cache_key(endpoint_schema, root_plan, 0);
            std::lock_guard lock{target_link_context_cache_mutex()};
            auto &cache = target_link_context_cache();
            if (const auto it = cache.find(key); it != cache.end())
            {
                return checked_ts_role_type(
                    intern_ts_type(*schema, TypeRole::Input, root_plan, *it->second->active_ops),
                    std::integral_constant<TypeRole, TypeRole::Input>{});
            }
            const auto *layout = data_type.ops_ref().layout_impl(data_type.ops_ref().context);
            if (layout == nullptr) throw std::logic_error("scalar input type requires a resolved data layout");
            auto context = detail::target_link_context_builder_for(schema->kind)(*schema, root_plan, 0, *layout);
            const auto type = checked_ts_role_type(
                intern_ts_type(*schema, TypeRole::Input, root_plan, *context->active_ops,
                               schema->kind == TSTypeKind::REF
                                   ? std::string_view{"ts.ref.input.target"}
                                   : std::string_view{}),
                std::integral_constant<TypeRole, TypeRole::Input>{});
            cache.emplace(key, std::move(context));
            return type;
        }

        [[nodiscard]] bool fixed_migrated_schema(const TSValueTypeMetaData *schema) noexcept
        {
            return schema != nullptr &&
                   (schema->kind == TSTypeKind::TSB ||
                    (schema->kind == TSTypeKind::TSL && schema->fixed_size() != 0));
        }

        [[nodiscard]] std::string_view dynamic_owned_label(const TSValueTypeMetaData &schema,
                                                           TypeRole role,
                                                           bool root_record)
        {
            const auto by_role = [role](std::string_view data,
                                        std::string_view input,
                                        std::string_view output) {
                switch (role)
                {
                case TypeRole::Data: return data;
                case TypeRole::Input: return input;
                case TypeRole::Output: return output;
                default: throw std::invalid_argument("owned dynamic TSData role is not supported");
                }
            };
            if (schema.kind == TSTypeKind::TSL && schema.fixed_size() == 0)
            {
                return root_record
                           ? by_role("ts.tsl.dynamic.data.root", "ts.tsl.dynamic.input.owned",
                                     "ts.tsl.dynamic.output.root")
                           : by_role("ts.tsl.dynamic.data.embedded", "ts.tsl.dynamic.input.embedded",
                                     "ts.tsl.dynamic.output.embedded");
            }
            if (schema.kind == TSTypeKind::TSW)
            {
                if (schema.is_duration_based())
                {
                    return root_record
                               ? by_role("ts.tsw.duration.data.root", "ts.tsw.duration.input.owned",
                                         "ts.tsw.duration.output.root")
                               : by_role("ts.tsw.duration.data.embedded", "ts.tsw.duration.input.embedded",
                                         "ts.tsw.duration.output.embedded");
                }
                return root_record
                           ? by_role("ts.tsw.tick.data.root", "ts.tsw.tick.input.owned",
                                     "ts.tsw.tick.output.root")
                           : by_role("ts.tsw.tick.data.embedded", "ts.tsw.tick.input.embedded",
                                     "ts.tsw.tick.output.embedded");
            }
            throw std::invalid_argument("owned dynamic TSData label requires dynamic TSL or TSW");
        }

        [[nodiscard]] std::string_view dynamic_composite_label(
            TypeRole role, bool root_record)
        {
            switch (role)
            {
                case TypeRole::Data:
                    return root_record
                               ? std::string_view{"ts.tsl.dynamic.data.composite"}
                               : std::string_view{"ts.tsl.dynamic.data.composite.embedded"};
                case TypeRole::Input:
                    return root_record
                               ? std::string_view{"ts.tsl.dynamic.input.composite"}
                               : std::string_view{"ts.tsl.dynamic.input.composite.embedded"};
                case TypeRole::Output:
                    return root_record
                               ? std::string_view{"ts.tsl.dynamic.output.composite"}
                               : std::string_view{"ts.tsl.dynamic.output.composite.embedded"};
                default:
                    throw std::invalid_argument(
                        "dynamic TSL composite role is not supported");
            }
        }

        [[nodiscard]] TSRoleTypeRef input_storage_type_for(const TSEndpointSchema         &endpoint_schema,
                                                              const MemoryUtils::StoragePlan &root_plan,
                                                              std::size_t storage_offset,
                                                              bool root_record,
                                                              TypeRole storage_role)
        {
            const auto *schema = endpoint_schema.schema();
            if (schema == nullptr) { return {}; }
            const bool scalar = schema->kind == TSTypeKind::TS || schema->kind == TSTypeKind::SIGNAL ||
                                schema->kind == TSTypeKind::REF;
            const bool fixed = fixed_migrated_schema(schema);
            const bool keyed = schema->kind == TSTypeKind::TSS || schema->kind == TSTypeKind::TSD;
            const bool dynamic_list = schema->kind == TSTypeKind::TSL && schema->fixed_size() == 0;
            const bool window = schema->kind == TSTypeKind::TSW;
            if (!scalar && !fixed && !keyed && !dynamic_list && !window)
                throw std::invalid_argument("TSInput storage type does not support this schema kind");

            if (endpoint_schema.is_peered())
            {
                const auto &ops = target_link_ops_for(endpoint_schema, root_plan, storage_offset);
                const auto role = storage_role == TypeRole::Output ? TypeRole::Output : TypeRole::Input;
                const auto label = schema->kind == TSTypeKind::TSS ? std::string_view{"ts.tss.input.target"}
                                 : schema->kind == TSTypeKind::TSD ? std::string_view{"ts.tsd.input.target"}
                                 : dynamic_list ? std::string_view{"ts.tsl.dynamic.input.target"}
                                 : window ? std::string_view{"ts.tsw.input.target"}
                                 : schema->kind == TSTypeKind::REF ? std::string_view{"ts.ref.input.target"}
                                 : role == TypeRole::Output
                                       ? std::string_view{"ts.fixed.output.embedded"}
                                       : fixed || !root_record
                                             ? std::string_view{"ts.fixed.input.target"}
                                             : std::string_view{};
                return intern_ts_type(*schema, role, root_plan, ops, label);
            }

            if (endpoint_schema.is_owned())
            {
                const auto &local_plan = input_storage_plan(endpoint_schema);
                if (dynamic_list)
                {
                    const auto element_type = owned_element_type_for(*schema, storage_role);
                    const auto &ops = ts_data_plan_factory_detail::dynamic_list_ts_data_ops(
                        *schema, local_plan, 0, element_type, storage_role, !root_record);
                    return intern_ts_type(
                        *schema, storage_role, local_plan, ops,
                        dynamic_owned_label(*schema, storage_role, root_record));
                }
                if (window)
                {
                    const auto *value = local_plan.find_component("window");
                    const auto *tracking = local_plan.find_component("tracking");
                    const auto element_binding = realized_input_value_binding_for(schema->value_type);
                    if (value == nullptr || tracking == nullptr || !element_binding)
                    {
                        throw std::logic_error("owned TSW input storage components are not resolved");
                    }
                    const auto &ops = ts_data_plan_factory_detail::window_ts_data_ops(
                        *schema, local_plan, value->offset, tracking->offset,
                        element_binding, storage_role, !root_record);
                    return intern_ts_type(
                        *schema, storage_role, local_plan, ops,
                        dynamic_owned_label(*schema, storage_role, root_record));
                }
                if (keyed)
                {
                    const auto &keyed_plan = root_record ? root_plan : local_plan;
                    const auto key_binding = realized_input_key_binding_for(*schema);
                    const auto *ops = schema->kind == TSTypeKind::TSD
                                          ? &ts_data_plan_factory_detail::slot_tsd_ts_data_ops(
                                                *schema, keyed_plan, 0, key_binding,
                                                owned_element_type_for(*schema, storage_role),
                                                storage_role, !root_record)
                                          : &ts_data_plan_factory_detail::slot_ts_data_ops(
                                                *schema, keyed_plan, 0, key_binding,
                                                storage_role, !root_record);
                    const auto label = schema->kind == TSTypeKind::TSS
                                           ? root_record ? std::string_view{"ts.tss.input.owned"}
                                                         : std::string_view{"ts.tss.input.embedded"}
                                           : root_record ? std::string_view{"ts.tsd.input.owned"}
                                                         : std::string_view{"ts.tsd.input.embedded"};
                    return intern_ts_type(*schema, storage_role, keyed_plan, *ops, label);
                }
                if (fixed)
                {
                    const auto *value = local_plan.find_component("value");
                    const auto *aux = local_plan.find_component("aux");
                    if (value == nullptr || aux == nullptr)
                        throw std::logic_error("owned fixed input storage components are not resolved");
                    return ts_data_plan_factory_detail::embedded_ts_storage_type(
                        *schema, storage_role, root_plan, storage_offset + value->offset,
                        storage_offset + aux->offset, root_record);
                }

                const auto *value = local_plan.find_component("value");
                const auto *tracking = local_plan.find_component("tracking");
                const auto value_type = realized_input_value_binding_for(schema->value_schema);
                const auto delta_type = realized_input_value_binding_for(schema->delta_value_schema);
                if (value == nullptr || tracking == nullptr || !value_type || !delta_type)
                    throw std::logic_error("owned scalar input storage components are not resolved");
                const auto &ops = ts_data_plan_factory_detail::atomic_ts_data_ops(
                    schema->kind, value_type, delta_type, root_plan, storage_offset + value->offset,
                    storage_offset + tracking->offset);
                const auto scalar_label = schema->kind == TSTypeKind::REF
                                              ? std::string_view{"ts.ref.input.owned"}
                                              : root_record ? std::string_view{}
                                                            : storage_role == TypeRole::Output
                                                                  ? std::string_view{"ts.fixed.output.embedded"}
                                                                  : std::string_view{"ts.fixed.input.embedded"};
                return intern_ts_type(*schema, storage_role, root_plan, ops, scalar_label);
            }

            if (schema->kind == TSTypeKind::TSD)
            {
                if (endpoint_schema.child_count() != 1)
                    throw std::logic_error("non-peered TSD storage requires one element annotation");
                const auto &child_schema = endpoint_schema.child(0);
                const auto &child_plan = input_storage_plan(child_schema);
                const auto element_type = input_storage_type_for(
                    child_schema, child_plan, 0, true, storage_role);
                const auto key_binding = realized_input_key_binding_for(*schema);
                const auto &ops = ts_data_plan_factory_detail::slot_tsd_ts_data_ops(
                    *schema, root_plan, storage_offset, key_binding, element_type,
                    storage_role, false, true);
                const auto label = storage_role == TypeRole::Output
                                       ? std::string_view{"ts.tsd.output.root"}
                                       : std::string_view{"ts.tsd.input.composite"};
                return intern_ts_type(*schema, storage_role, root_plan, ops, label);
            }

            if (dynamic_list)
            {
                if (endpoint_schema.child_count() != 1)
                {
                    throw std::logic_error(
                        "non-peered dynamic TSL storage requires one element annotation");
                }
                const auto &child_schema = endpoint_schema.child(0);
                const auto &child_plan = input_storage_plan(child_schema);
                const auto element_type = input_storage_type_for(
                    child_schema, child_plan, 0, true, storage_role);
                const auto &ops = ts_data_plan_factory_detail::dynamic_list_ts_data_ops(
                    *schema, root_plan, storage_offset, element_type, storage_role,
                    !root_record);
                return intern_ts_type(
                    *schema, storage_role, root_plan, ops,
                    dynamic_composite_label(storage_role, root_record));
            }

            if (window)
                throw std::invalid_argument("non-peered TSW inputs are not supported");

            const auto composite_label = schema->kind == TSTypeKind::TSD
                                             ? std::string_view{"ts.tsd.input.composite"}
                                             : root_record ? storage_role == TypeRole::Output
                                                                 ? std::string_view{"ts.fixed.output.root"}
                                                                 : std::string_view{"ts.fixed.input.composite"}
                                                           : storage_role == TypeRole::Output
                                                                 ? std::string_view{"ts.fixed.output.embedded"}
                                                                 : std::string_view{"ts.fixed.input.embedded"};
            const auto type = input_data_type_for(
                endpoint_schema, root_plan, storage_offset, storage_role, composite_label);
            if (!type) throw std::logic_error("composite input storage type is not resolved");
            return type;
        }

        [[nodiscard]] TSRoleTypeRef input_storage_type_for(const TSEndpointSchema &endpoint_schema)
        {
            const auto *schema = endpoint_schema.schema();
            if (schema != nullptr &&
                (schema->kind == TSTypeKind::TS || schema->kind == TSTypeKind::SIGNAL ||
                 schema->kind == TSTypeKind::REF))
                return scalar_input_type_for(endpoint_schema).as_role();
            const auto &root_plan = input_storage_plan(endpoint_schema);
            return input_storage_type_for(endpoint_schema, root_plan, 0, true, TypeRole::Input);
        }

    }  // namespace

    namespace detail
    {
        bool output_view_bound(const TSOutputView &output) noexcept
        {
            return ::hgraph::output_view_bound(output);
        }

        const TSDataView &empty_ts_data_view() noexcept
        {
            return ::hgraph::empty_ts_data_view();
        }

        void validate_input_view_kind(const TSValueTypeMetaData *schema, TSTypeKind expected, const char *what)
        {
            ::hgraph::validate_endpoint_kind(schema, expected, what);
        }

        const TSInputEndpointOps &input_endpoint_ops_for(const TSValueTypeMetaData *schema)
        {
            return ::hgraph::input_endpoint_ops_for(schema);
        }

        TSDataView structural_observation_for(const TSDataView &source)
        {
            if (!source.valid() || source.schema() == nullptr)
            {
                throw std::logic_error("Structural observation requires live TSData with a schema");
            }

            const auto &ops = input_endpoint_ops_for(source.schema());
            if (ops.structural_observation == nullptr)
            {
                throw std::invalid_argument(
                    "Structural input activity is not supported for this time-series shape");
            }
            return ops.structural_observation(source);
        }

        bool has_published_structural_state(const TSDataView &source,
                                            DateTime transition_time)
        {
            if (!source.valid() || source.schema() == nullptr) { return false; }

            const auto &ops = input_endpoint_ops_for(source.schema());
            if (ops.has_published_structural_state == nullptr)
            {
                throw std::invalid_argument(
                    "Published structural state is not supported for this time-series shape");
            }

            // A previously published empty collection is still structural
            // state. It must produce a sampled empty transition when a
            // forwarding endpoint moves to a fresh, currently-unset target.
            if (!source.modified(transition_time) && source.has_current_value()) { return true; }
            return ops.has_published_structural_state(source, transition_time);
        }

        TSRoleTypeRef output_data_storage_type_for(const TSEndpointSchema &endpoint_schema)
        {
            const auto *schema = endpoint_schema.schema();
            if (schema != nullptr && schema->kind == TSTypeKind::TSD &&
                endpoint_schema.is_non_peered())
            {
                if (endpoint_schema.child_count() != 1)
                {
                    throw std::logic_error(
                        "TSOutput non-peered TSD storage requires one element annotation");
                }

                const auto &child_schema = endpoint_schema.child(0);
                const auto &child_plan = ::hgraph::input_storage_plan(child_schema);
                const auto element_type = ::hgraph::input_storage_type_for(
                    child_schema, child_plan, 0, true, TypeRole::Output);
                if (!element_type)
                {
                    throw std::logic_error(
                        "TSOutput non-peered TSD element type is not resolved");
                }

                const auto *key_schema = schema->key_type();
                const auto *snapshot = active_type_realization();
                const ValueTypeRef key_binding = snapshot != nullptr
                                                     ? value_type_for_active_realization(key_schema)
                                                     : ValuePlanFactory::instance().type_for(key_schema);
                const auto *plan = ts_data_plan_factory_detail::synthesise_slot_tsd_plan(
                    *schema, key_binding, element_type);
                if (plan == nullptr)
                {
                    throw std::logic_error(
                        "TSOutput non-peered TSD storage plan is not resolved");
                }
                const auto &ops = ts_data_plan_factory_detail::slot_tsd_ts_data_ops(
                    *schema, *plan, 0, key_binding, element_type,
                    TypeRole::Output, false, true);
                return intern_ts_type(
                    *schema, TypeRole::Output, *plan, ops,
                    "ts.tsd.output.root");
            }

            const auto &root_plan = ::hgraph::input_storage_plan(endpoint_schema);
            return ::hgraph::input_storage_type_for(
                endpoint_schema, root_plan, 0, true, TypeRole::Output);
        }

        const TSInputTargetLinkStorage *target_link_storage(const TSDataView &view) noexcept
        {
            return ::hgraph::target_storage(view);
        }

        TSInputTargetLinkStorage *mutable_target_link_storage(const TSDataView &view)
        {
            return ::hgraph::mutable_target_storage(view);
        }

        const TSValueTypeMetaData *target_link_schema(const TSDataView &view) noexcept
        {
            const auto *context = ::hgraph::target_context_for(view.storage_type());
            return context != nullptr ? context->schema : nullptr;
        }

        bool has_input_children(const TSDataView &data) noexcept
        {
            return ::hgraph::input_context_for(data.storage_type()) != nullptr;
        }

        DynamicStorageMetrics input_target_link_dynamic_storage_metrics(
            const TSDataView &view) noexcept
        {
            if (!view.valid()) { return {}; }
            try
            {
                if (const auto *link = target_link_storage(view); link != nullptr)
                {
                    return link->dynamic_storage_metrics();
                }
                if (view.schema() == nullptr) { return {}; }

                DynamicStorageMetrics result{};
                switch (view.schema()->kind)
                {
                    case TSTypeKind::TSB:
                    case TSTypeKind::TSL:
                        for (std::size_t index = 0; index < view.indexed_child_count(); ++index)
                        {
                            if (has_input_children(view))
                            {
                                auto child = input_child_projection(view, index);
                                result += input_target_link_dynamic_storage_metrics(
                                    child.target_link.valid() ? child.target_link : child.visible);
                            }
                            else
                            {
                                result += input_target_link_dynamic_storage_metrics(
                                    view.indexed_child_at(index));
                            }
                        }
                        break;
                    case TSTypeKind::TSD:
                    {
                        auto dict = view.as_dict();
                        for (std::size_t slot = 0; slot < dict.slot_capacity(); ++slot)
                        {
                            if (dict.slot_occupied(slot))
                            {
                                result += input_target_link_dynamic_storage_metrics(dict.at_slot(slot));
                            }
                        }
                        break;
                    }
                    default:
                        break;
                }
                return result;
            }
            catch (...)
            {
                return {};
            }
        }

        TSInputChildProjection input_child_projection(const TSDataView &parent, std::size_t index)
        {
            const auto *context = ::hgraph::input_context_for(parent.storage_type());
            if (context == nullptr || index >= context->children.size())
            {
                throw std::logic_error("TSInput child projection requires non-peered input storage");
            }

            const auto &child = context->children[index];
            if (child.target_link)
            {
                // Owned input storage attaches the complete parent tree when
                // it is constructed or moved. Projection is a read-only hot
                // path and must not rebind the same tracking link per access.
                TSDataView link{child.input_type, input_element_storage_memory(context, parent.data(), index)};
                const auto *storage = ::hgraph::target_storage(link);
                if (storage != nullptr && storage->bound())
                {
                    return TSInputChildProjection{storage->target_view(), std::move(link)};
                }
                return TSInputChildProjection{TSDataView{child.regular_type, static_cast<const void *>(nullptr)},
                                              std::move(link)};
            }

            return TSInputChildProjection{
                TSDataView{child.input_type, input_element_storage_memory(context, parent.data(), index)}, {}};
        }

        void TSInputSchedulingNotifier::notify(DateTime modified_time)
        {
            if (target != nullptr) { target->notify(modified_time); }
        }

        TSInputActiveTarget::TSInputActiveTarget() noexcept
        {
        }

        TSInputActiveTarget::TSInputActiveTarget(TSInputActiveTarget *parent_, std::size_t slot_) noexcept
            : parent(parent_),
              slot(slot_)
        {
        }

        TSInputActiveTarget::~TSInputActiveTarget() noexcept
        {
            unsubscribe();
        }

        TSInputActiveTarget *TSInputActiveTarget::child_at(std::size_t slot_) const noexcept
        {
            return children.find(slot_);
        }

        bool TSInputActiveTarget::has_any_active() const noexcept
        {
            if (active) { return true; }
            return children.any_of([](std::size_t, const TSInputActiveTarget &child) {
                return child.has_any_active();
            });
        }

        DynamicStorageMetrics TSInputActiveTarget::dynamic_storage_metrics() const noexcept
        {
            DynamicStorageMetrics result = children.dynamic_storage_metrics();
            children.for_each([&](std::size_t, const TSInputActiveTarget &child) {
                result.live_bytes += sizeof(TSInputActiveTarget);
                result.reserved_bytes += sizeof(TSInputActiveTarget);
                result += child.dynamic_storage_metrics();
            });
            return result;
        }

        TSInputActiveTarget &TSInputActiveTarget::ensure_child(std::size_t slot_)
        {
            return children.ensure(slot_, [&] { return std::make_unique<TSInputActiveTarget>(this, slot_); });
        }

        void TSInputActiveTarget::subscribe(const TSDataView &observed_, Notifiable *target_notifier)
        {
            if (observed_.valid() && observed.valid() && observed_.data() == observed.data() &&
                observed_.storage_type() == observed.storage_type())
            {
                notifier.target = target_notifier;
                return;
            }

            unsubscribe();
            notifier.target = target_notifier;
            if (target_notifier == nullptr || !observed_.valid()) { return; }
            const auto observed_ref = observed_.storage_ref();
            TSDataView observed_view{observed_ref};
            observed_view.subscribe(&notifier);
            observed = observed_ref;
        }

        void TSInputActiveTarget::unsubscribe() noexcept
        {
            if (!observed.valid()) { return; }
            TSDataView            observed_view{observed};
            [[maybe_unused]] auto reset_observed = make_scope_exit([this]() noexcept {
                observed.reset();
                notifier.target = nullptr;
            });
            [[maybe_unused]] auto unsubscribe_observer =
                make_scope_exit<true>([&] { observed_view.unsubscribe(&notifier); });
        }

    }  // namespace detail

    TSInputConstructionPlan::TSInputConstructionPlan(const TSValueTypeMetaData &root_schema,
                                                     TSEndpointSchema           endpoint_schema)
        : schema_(&root_schema),
          endpoint_schema_(std::move(endpoint_schema))
    {
        if (!time_series_schema_equivalent(&root_schema, endpoint_schema_.schema()))
        {
            throw std::invalid_argument("TSInput construction annotation schema does not match the root schema");
        }
        validate_input_endpoint_schema(endpoint_schema_, true);
    }

    const TSValueTypeMetaData &TSInputConstructionPlan::schema() const noexcept
    {
        return *schema_;
    }

    const TSEndpointSchema &TSInputConstructionPlan::endpoint_schema() const noexcept
    {
        return endpoint_schema_;
    }

    TSInputConstructionPlan TSInputPlanFactory::compile(const TSValueTypeMetaData &root_schema,
                                                        const TSEndpointSchema    &endpoint_schema)
    {
        return TSInputConstructionPlan{root_schema, endpoint_schema};
    }

    namespace
    {
        [[nodiscard]] TSInputTypeRef resolved_input_storage_type(const TSInputConstructionPlan &plan,
                                                                 std::string_view owner)
        {
            const auto type = TSInputTypeRef::checked(input_storage_type_for(plan.endpoint_schema()));
            if (!type) { throw std::logic_error(std::string{owner} + " could not resolve input storage type"); }
            return type;
        }
    }

    TSInputBuilder::TSInputBuilder(TSInputConstructionPlan plan)
        : plan_(std::move(plan))
    {
        if (detail::input_storage_type_is_realization_invariant(plan_.endpoint_schema()))
        {
            storage_type_ = resolved_input_storage_type(plan_, "TSInputBuilder");
        }
    }

    const TSValueTypeMetaData &TSInputBuilder::schema() const noexcept
    {
        return plan_.schema();
    }

    TSInput TSInputBuilder::make_input() const
    {
        return TSInput{*this};
    }

    const TSInputBuilder *TSInputBuilderFactory::builder_for(const TSValueTypeMetaData &root_schema,
                                                            const TSEndpointSchema    &endpoint_schema)
    {
        return builder_for(TSInputPlanFactory::compile(root_schema, endpoint_schema));
    }

    const TSInputBuilder &TSInputBuilderFactory::checked_builder_for(const TSValueTypeMetaData &root_schema,
                                                                    const TSEndpointSchema    &endpoint_schema)
    {
        return checked_builder_for(TSInputPlanFactory::compile(root_schema, endpoint_schema));
    }

    const TSInputBuilder *TSInputBuilderFactory::builder_for(const TSInputConstructionPlan &plan)
    {
        const auto *schema = plan.endpoint_schema().schema();
        const bool scalar = schema != nullptr && (schema->kind == TSTypeKind::TS || schema->kind == TSTypeKind::SIGNAL ||
                                                  schema->kind == TSTypeKind::REF);
        const bool direct_peered = schema != nullptr && plan.endpoint_schema().is_peered();
        const bool owned_scalar = scalar && plan.endpoint_schema().is_owned();
        const bool owned_fixed = schema != nullptr &&
                                 (schema->kind == TSTypeKind::TSB ||
                                  (schema->kind == TSTypeKind::TSL && schema->fixed_size() != 0)) &&
                                 plan.endpoint_schema().is_owned();
        const bool owned_keyed = schema != nullptr &&
                                 (schema->kind == TSTypeKind::TSS || schema->kind == TSTypeKind::TSD) &&
                                 plan.endpoint_schema().is_owned();
        const bool owned_dynamic = schema != nullptr &&
                                   ((schema->kind == TSTypeKind::TSL && schema->fixed_size() == 0) ||
                                    schema->kind == TSTypeKind::TSW) &&
                                   plan.endpoint_schema().is_owned();
        const bool structural_root = schema != nullptr &&
                                     (schema->kind == TSTypeKind::TSB ||
                                      schema->kind == TSTypeKind::TSD ||
                                      (schema->kind == TSTypeKind::TSL &&
                                       schema->fixed_size() == 0)) &&
                                     plan.endpoint_schema().is_non_peered();
        if (!direct_peered && !owned_scalar && !owned_fixed && !owned_keyed && !owned_dynamic && !structural_root)
        {
            return nullptr;
        }

        const auto key = plan_cache_key(plan);
        std::lock_guard lock{input_builder_cache_mutex()};
        auto &cache = input_builder_cache();
        if (const auto it = cache.find(key); it != cache.end()) { return it->second.get(); }

        auto builder = std::unique_ptr<TSInputBuilder>(new TSInputBuilder(plan));
        const auto *result = builder.get();
        cache.emplace(key, std::move(builder));
        return result;
    }

    const TSInputBuilder &TSInputBuilderFactory::checked_builder_for(const TSInputConstructionPlan &plan)
    {
        if (const auto *builder = builder_for(plan); builder != nullptr) { return *builder; }
        throw std::invalid_argument(
            "TSInputBuilderFactory requires a peered, owned, or supported non-peered composite root");
    }

    void TSInputBuilderFactory::reset() noexcept
    {
        clear_input_binding_caches();
    }

    TSInput::TSInput() noexcept = default;

    TSInput::TSInput(const TSInputBuilder &builder)
        : builder_(&builder),
          schema_(&builder.plan_.schema()),
          data_(builder.storage_type_ ? builder.storage_type_
                                     : resolved_input_storage_type(builder.plan_, "TSInput"))
    {
        attach_root_parent();
    }

    TSInput::TSInput(const TSInputConstructionPlan &plan)
    {
        rebuild_from_plan(plan);
    }

    TSInput::TSInput(const TSInput &other)
        : builder_(other.builder_),
          schema_(other.schema_),
          data_(other.data_)
    {
        attach_root_parent();
    }

    TSInput &TSInput::operator=(const TSInput &other)
    {
        if (this == &other) { return *this; }
        builder_ = other.builder_;
        schema_ = other.schema_;
        data_ = other.data_;
        active_root_.reset();
        attach_root_parent();
        return *this;
    }

    TSInput::TSInput(TSInput &&other) noexcept
        : builder_(std::exchange(other.builder_, nullptr)),
          schema_(std::exchange(other.schema_, nullptr)),
          data_(std::move(other.data_)),
          active_root_(std::move(other.active_root_))
    {
        attach_root_parent();
    }

    TSInput &TSInput::operator=(TSInput &&other) noexcept
    {
        if (this != &other)
        {
            builder_ = std::exchange(other.builder_, nullptr);
            schema_ = std::exchange(other.schema_, nullptr);
            data_ = std::move(other.data_);
            active_root_ = std::move(other.active_root_);
            attach_root_parent();
        }
        return *this;
    }

    TSInput::~TSInput() = default;

    bool TSInput::has_value() const noexcept
    {
        return data_.has_value();
    }

    const TSValueTypeMetaData *TSInput::schema() const noexcept
    {
        return schema_;
    }

    TSInputTypeRef TSInput::type_ref() const
    {
        const auto type = data_.type_ref();
        return type ? TSInputTypeRef::checked(type) : TSInputTypeRef{};
    }

    DynamicStorageMetrics TSInput::dynamic_storage_metrics() const noexcept
    {
        DynamicStorageMetrics result{};
        if (data_.has_value())
        {
            const auto view = data_.view();
            result += view.dynamic_storage_metrics();
            result += detail::input_target_link_dynamic_storage_metrics(view);
        }
        if (active_root_ != nullptr)
        {
            result.live_bytes += sizeof(detail::TSInputActiveTarget);
            result.reserved_bytes += sizeof(detail::TSInputActiveTarget);
            result += active_root_->dynamic_storage_metrics();
        }
        return result;
    }

    NodeView TSInput::owner_node() const
    {
        return has_value() ? data_.view().owner_node() : NodeView{};
    }

    GraphView TSInput::owner_graph() const
    {
        return has_value() ? data_.view().owner_graph() : GraphView{};
    }

    void TSInput::bind_node_parent(const NodeView &node, TSEndpointOwnerPort port)
    {
        if (has_value()) { data_.view().bind_parent(node, port); }
    }

    void TSInput::clear_node_parent()
    {
        attach_root_parent();
    }

    TSInputView TSInput::view(Notifiable *scheduling_notifier, DateTime evaluation_time)
    {
        return TSInputView{this, data_.view(), {}, nullptr, scheduling_notifier, evaluation_time};
    }

    TSInputView TSInput::view(Notifiable *scheduling_notifier, DateTime evaluation_time) const
    {
        return TSInputView{const_cast<TSInput *>(this), data_.view(), {}, nullptr, scheduling_notifier, evaluation_time};
    }

    void TSInput::rebuild_from_plan(const TSInputConstructionPlan &plan)
    {
        schema_ = &plan.schema();
        const auto type = input_storage_type_for(plan.endpoint_schema());
        if (!type) { throw std::logic_error("TSInput could not resolve input storage type"); }
        data_ = TSData{type};
        attach_root_parent();
        active_root_.reset();
    }

    void TSInput::attach_root_parent()
    {
        if (has_value())
        {
            auto root = data_.view();
            root.bind_parent(*this, TS_DATA_NO_CHILD_ID);
            detail::attach_owned_ts_data_parents(root.borrowed_ref());
        }
    }

    void TSInput::record_child_modified(std::size_t, DateTime)
    {
    }

    void TSInput::make_active(std::vector<std::size_t> path, TSDataView observed, Notifiable *target_notifier)
    {
        if (!active_root_) { active_root_ = std::make_unique<detail::TSInputActiveTarget>(); }
        auto *active = active_root_.get();
        for (const auto slot : path) { active = &active->ensure_child(slot); }
        active->active = true;
        active->subscribe(observed, target_notifier);
    }

    void TSInput::make_passive(const std::vector<std::size_t> &path)
    {
        auto *active = active_root_.get();
        for (const auto slot : path)
        {
            if (active == nullptr) { return; }
            active = active->child_at(slot);
        }
        if (active == nullptr || !active->active) { return; }

        active->unsubscribe();
        active->active = false;

        while (active != nullptr && !active->has_any_active())
        {
            auto *parent = active->parent;
            if (parent == nullptr)
            {
                active_root_.reset();
                return;
            }
            const auto slot = active->slot;
            active = parent;
            static_cast<void>(active->children.erase(slot));
        }
    }

    bool TSInput::active(const std::vector<std::size_t> &path) const noexcept
    {
        auto *active = active_root_.get();
        for (const auto slot : path)
        {
            if (active == nullptr) { return false; }
            active = active->child_at(slot);
        }
        return active != nullptr && active->active;
    }

}  // namespace hgraph
