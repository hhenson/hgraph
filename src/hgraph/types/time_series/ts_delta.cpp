#include <hgraph/types/time_series/ts_delta.h>

#include <hgraph/types/metadata/ts_data_plan_factory.h>
#include <hgraph/types/metadata/type_realization.h>
#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/metadata/ts_value_type_meta_data.h>
#include <hgraph/types/metadata/value_plan_factory.h>
#include <hgraph/types/metadata/value_type_meta_data.h>
#include <hgraph/types/time_series/ts_input.h>
#include <hgraph/types/time_series/ts_data/impl/current_state_ops.h>
#include <hgraph/types/time_series/ts_output.h>
#include <hgraph/types/value/value.h>
#include <hgraph/types/value/value_builder.h>
#include <hgraph/types/value/value_view.h>

#include <ankerl/unordered_dense.h>
#include <fmt/format.h>

#include <array>
#include <cassert>
#include <concepts>
#include <cstdint>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string_view>
#include <utility>
#include <vector>

namespace hgraph
{
    namespace
    {
        [[nodiscard]] ValueTypeRef binding_for(const ValueTypeMetaData *meta, const char *fn)
        {
            const auto *snapshot = active_type_realization();
            const auto binding = snapshot != nullptr ? snapshot->type_for(meta)
                                                     : ValuePlanFactory::instance().type_for(meta);
            if (!binding) { throw std::logic_error(fmt::format("{}: unresolved value binding", fn)); }
            return binding;
        }

        using MaterializedOwner =
            MemoryUtils::ErasedOwner<MemoryUtils::InlineStoragePolicy<>, TypeRecord>;

        [[nodiscard]] MaterializedOwner materialize_as(
            ValueTypeRef target, const ValueView &source, const char *fn)
        {
            if (!target || !source.binding())
            {
                throw std::logic_error(
                    fmt::format("{}: unresolved value materialization", fn));
            }
            MaterializedOwner result{*target.record()};
            target.ops_ref().copy_assign_from(
                target, result.data(), source.binding(), source.data());
            return result;
        }


        /**
         * Canonical delta field layouts. ``type_registry.cpp`` builds these in
         * one place, so the apply path indexes them directly:
         *
         *   TSS           {added, removed}
         *   TSD observed  {removed, modified}
         *   TSD authored  {removed, modified, removed_strict}
         *
         * ``BundleView::field`` looks a field up by scanning the schema and
         * comparing field-name strings. That is the wrong algorithm here - it
         * runs on every tick, for every keyed structure, to find a field whose
         * position is fixed and known at compile time.
         *
         * ``delta_field_is`` re-checks the assumption under ``assert`` so a
         * reordered schema fails loudly in a debug build instead of silently
         * reading the wrong field; release builds pay nothing.
         */
        inline constexpr std::size_t tss_delta_added           = 0;
        inline constexpr std::size_t tss_delta_removed         = 1;
        inline constexpr std::size_t tsl_delta_removed         = 0;
        inline constexpr std::size_t tsl_delta_modified        = 1;
        inline constexpr std::size_t tsd_delta_removed         = 0;
        inline constexpr std::size_t tsd_delta_modified        = 1;
        inline constexpr std::size_t tsd_delta_removed_strict  = 2;
        /** An authored TSD delta has the third field; an observed one does not. */
        inline constexpr std::size_t tsd_authored_delta_fields = 3;

        [[maybe_unused]] [[nodiscard]] inline bool delta_field_is(const ValueView &bundle, std::size_t index,
                                                 std::string_view name) noexcept
        {
            const auto *schema = bundle.binding() ? bundle.binding().schema() : nullptr;
            if (schema == nullptr || index >= schema->field_count) { return false; }
            const char *field = schema->fields[index].name;
            return field != nullptr && name == field;
        }

        /**
         * A key or element ready to hand to a builder, borrowed where possible.
         *
         * The keys a ``TSD`` hands out are views into its ``KeySlotStore``, and
         * the elements a ``TSS`` hands out are views into its own storage, so
         * the source binding is usually already the builder's binding. In that
         * case the builder can copy straight from that storage.
         *
         * Materialising unconditionally cost an allocation plus a SECOND copy
         * of every key, every tick — the intermediate existed only to convert a
         * binding that normally needs no conversion.
         *
         * The owner is an inline-storage ``ErasedOwner``, so it must outlive
         * the returned pointer and must not be moved after the pointer is
         * taken; callers keep it in the loop body's scope.
         */
        class BorrowedOperand
        {
          public:
            BorrowedOperand(ValueTypeRef target, const ValueView &source, const char *fn)
            {
                if (target && source.binding() == target) { data_ = source.data(); return; }
                owner_.emplace(materialize_as(target, source, fn));
                data_ = owner_->data();
            }

            BorrowedOperand(const BorrowedOperand &)            = delete;
            BorrowedOperand &operator=(const BorrowedOperand &) = delete;

            [[nodiscard]] const void *data() const noexcept { return data_; }

          private:
            std::optional<MaterializedOwner> owner_{};
            const void                      *data_{nullptr};
        };

        [[nodiscard]] TSRoleTypeRef ts_type_for(const TSValueTypeMetaData *schema, const char *fn)
        {
            const auto type = TSDataPlanFactory::instance().data_type_for(schema);
            if (!type) { throw std::logic_error(fmt::format("{}: unresolved TSData type", fn)); }
            return type.as_role();
        }

        [[nodiscard]] const TSValueTypeMetaData &require_schema(const TSValueTypeMetaData *schema, const char *fn)
        {
            if (schema == nullptr) { throw std::logic_error(fmt::format("{}: view has no TSData schema", fn)); }
            return *schema;
        }

        void initialize_tsb_delta_defaults(TSRoleTypeRef type, BundleBuilder &builder)
        {
            const auto *schema = type.schema();
            if (schema == nullptr || schema->kind != TSTypeKind::TSB)
            {
                throw std::logic_error("empty_delta_tsb: binding is not a TSB schema");
            }
            for (std::size_t index = 0; index < schema->field_count(); ++index)
            {
                const TSValueTypeMetaData *child_schema = schema->fields()[index].type;
                if (child_schema == nullptr) { throw std::logic_error("empty_delta_tsb: TSB field schema is null"); }
                if (!child_schema->is_collection()) { continue; }

                const auto child_type = ts_type_for(child_schema, "empty_delta_tsb");
                const auto &child_ops = child_type.ops_ref();
                Value empty = child_ops.empty_delta_impl(child_type);
                builder.set(index, std::move(empty));
            }
        }

        [[nodiscard]] const TSCurrentStateOps &current_state_ops(const TSRoleTypeRef &type,
                                                                 const char *fn)
        {
            if (!type) { throw std::logic_error(fmt::format("{}: unresolved TSData type", fn)); }
            const auto *ops = type.ops_ref().current_state_ops;
            if (ops == nullptr)
            {
                throw std::logic_error(fmt::format("{}: TSData current-state policy is null", fn));
            }
            return *ops;
        }

        /** Trusted variant: reach the policy table through the live data
            view's ops. The role-typed overload above validates the whole
            record (TSOutputTypeRef::checked replays validate_ts_record);
            interned records were validated once and the apply path runs per
            node per tick, so output-side callers use this form. */
        [[nodiscard]] const TSCurrentStateOps &current_state_ops(const TSDataView &data,
                                                                 const char *fn)
        {
            if (!data.valid()) { throw std::logic_error(fmt::format("{}: unresolved TSData type", fn)); }
            const auto *ops = data.ops().current_state_ops;
            if (ops == nullptr)
            {
                throw std::logic_error(fmt::format("{}: TSData current-state policy is null", fn));
            }
            return *ops;
        }

        [[nodiscard]] const TSCurrentStateOps &current_state_ops_for_input(
            const TSInputView &input, const char *fn)
        {
            if (const auto type = input.type_ref(); type)
            {
                return current_state_ops(type.as_role(), fn);
            }
            const auto &data = input.data_view();
            if (data.valid())
            {
                const auto *ops = data.ops().current_state_ops;
                if (ops != nullptr) { return *ops; }
            }
            static_cast<void>(require_schema(input.schema(), fn));
            throw std::logic_error(fmt::format("{} requires a canonical input type record", fn));
        }

        [[nodiscard]] bool field_name_equal(const TSFieldMetaData &ts_field,
                                            const ValueFieldMetaData &value_field) noexcept
        {
            const std::string_view ts_name =
                ts_field.name != nullptr ? std::string_view{ts_field.name} : std::string_view{};
            const std::string_view value_name =
                value_field.name != nullptr ? std::string_view{value_field.name} : std::string_view{};
            return ts_name == value_name;
        }

        [[nodiscard]] bool current_value_schema_compatible_atomic(
            const TSValueTypeMetaData &schema,
            const ValueTypeMetaData &value_schema) noexcept
        {
            if (schema.value_schema == &value_schema) { return true; }
            if (schema.value_schema != nullptr &&
                TypeRegistry::instance().value_is_a(&value_schema, schema.value_schema))
            {
                return true;
            }
            // Variadic tuple vs list: distinct schema identity, one layout
            // (same element type; flags differ only by VariadicTuple).
            const auto *target = schema.value_schema;
            return target != nullptr && target->try_value_kind() == ValueTypeKind::List &&
                   value_schema.try_value_kind() == ValueTypeKind::List && target->fixed_size == 0 &&
                   value_schema.fixed_size == 0 && !target->is_mutable() && !value_schema.is_mutable() &&
                   target->element_type == value_schema.element_type;
        }

        [[nodiscard]] bool current_value_schema_compatible_set(
            const TSValueTypeMetaData &schema,
            const ValueTypeMetaData &value_schema) noexcept
        {
            return schema.value_schema == &value_schema;
        }

        [[nodiscard]] bool current_value_schema_compatible_dict(
            const TSValueTypeMetaData &schema,
            const ValueTypeMetaData &value_schema)
        {
            if (value_schema.try_value_kind() != ValueTypeKind::Map ||
                schema.key_type() != value_schema.key_type || schema.element_ts() == nullptr ||
                value_schema.element_type == nullptr)
            {
                return false;
            }
            const auto *child_schema = schema.element_ts();
            const auto &child_ops = ts_current_state_detail::current_state_ops_for(
                child_schema->kind);
            return child_ops.value_schema_compatible_impl(
                *child_schema, *value_schema.element_type);
        }

        [[nodiscard]] bool current_value_schema_compatible_list(
            const TSValueTypeMetaData &schema,
            const ValueTypeMetaData &value_schema)
        {
            if (value_schema.try_value_kind() != ValueTypeKind::List ||
                schema.element_ts() == nullptr || value_schema.element_type == nullptr ||
                (schema.fixed_size() != 0 && value_schema.fixed_size != 0 &&
                 schema.fixed_size() != value_schema.fixed_size))
            {
                return false;
            }
            const auto *child_schema = schema.element_ts();
            const auto &child_ops = ts_current_state_detail::current_state_ops_for(
                child_schema->kind);
            return child_ops.value_schema_compatible_impl(
                *child_schema, *value_schema.element_type);
        }

        [[nodiscard]] bool current_value_schema_compatible_bundle(
            const TSValueTypeMetaData &schema,
            const ValueTypeMetaData &value_schema)
        {
            if (value_schema.try_value_kind() != ValueTypeKind::Bundle ||
                schema.field_count() != value_schema.field_count)
            {
                return false;
            }
            for (std::size_t index = 0; index < schema.field_count(); ++index)
            {
                const auto &ts_field    = schema.fields()[index];
                const auto &value_field = value_schema.fields[index];
                if (!field_name_equal(ts_field, value_field) || ts_field.type == nullptr ||
                    value_field.type == nullptr)
                {
                    return false;
                }
                const auto &child_ops = ts_current_state_detail::current_state_ops_for(
                    ts_field.type->kind);
                if (!child_ops.value_schema_compatible_impl(
                        *ts_field.type, *value_field.type))
                {
                    return false;
                }
            }
            return true;
        }

        [[nodiscard]] bool current_ts_schema_compatible(
            const TSValueTypeMetaData &target,
            const TSValueTypeMetaData &source);

        [[nodiscard]] bool current_ts_schema_compatible_atomic(
            const TSValueTypeMetaData &target,
            const TSValueTypeMetaData &source)
        {
            return source.kind == target.kind && source.value_schema != nullptr &&
                   current_value_schema_compatible_atomic(target, *source.value_schema);
        }

        [[nodiscard]] bool current_ts_schema_compatible_set(
            const TSValueTypeMetaData &target,
            const TSValueTypeMetaData &source) noexcept
        {
            return source.kind == target.kind && source.value_schema != nullptr &&
                   current_value_schema_compatible_set(target, *source.value_schema);
        }

        [[nodiscard]] bool current_ts_schema_compatible_dict(
            const TSValueTypeMetaData &target,
            const TSValueTypeMetaData &source)
        {
            return source.kind == target.kind && target.key_type() == source.key_type() &&
                   target.element_ts() != nullptr && source.element_ts() != nullptr &&
                   current_ts_schema_compatible(*target.element_ts(), *source.element_ts());
        }

        [[nodiscard]] bool current_ts_schema_compatible_list(
            const TSValueTypeMetaData &target,
            const TSValueTypeMetaData &source)
        {
            return source.kind == target.kind && target.fixed_size() == source.fixed_size() &&
                   target.element_ts() != nullptr && source.element_ts() != nullptr &&
                   current_ts_schema_compatible(*target.element_ts(), *source.element_ts());
        }

        [[nodiscard]] bool current_ts_schema_compatible_bundle(
            const TSValueTypeMetaData &target,
            const TSValueTypeMetaData &source)
        {
            if (source.kind != target.kind || target.field_count() != source.field_count())
            {
                return false;
            }
            for (std::size_t index = 0; index < target.field_count(); ++index)
            {
                const auto &target_field = target.fields()[index];
                const auto &source_field = source.fields()[index];
                const std::string_view target_name =
                    target_field.name != nullptr ? std::string_view{target_field.name}
                                                 : std::string_view{};
                const std::string_view source_name =
                    source_field.name != nullptr ? std::string_view{source_field.name}
                                                 : std::string_view{};
                if (target_name != source_name || target_field.type == nullptr ||
                    source_field.type == nullptr ||
                    !current_ts_schema_compatible(*target_field.type, *source_field.type))
                {
                    return false;
                }
            }
            return true;
        }

        [[nodiscard]] bool current_ts_schema_compatible_ref(
            const TSValueTypeMetaData &target,
            const TSValueTypeMetaData &source)
        {
            return source.kind == target.kind && target.referenced_ts() != nullptr &&
                   source.referenced_ts() != nullptr &&
                   current_ts_schema_compatible(*target.referenced_ts(), *source.referenced_ts());
        }

        [[nodiscard]] bool current_ts_schema_compatible_window(
            const TSValueTypeMetaData &target,
            const TSValueTypeMetaData &source)
        {
            if (!current_ts_schema_compatible_atomic(target, source) ||
                target.is_duration_based() != source.is_duration_based())
            {
                return false;
            }
            return target.is_duration_based()
                       ? target.time_range() == source.time_range() &&
                             target.min_time_range() == source.min_time_range()
                       : target.period() == source.period() &&
                             target.min_period() == source.min_period();
        }

        [[nodiscard]] bool current_ts_schema_compatible(
            const TSValueTypeMetaData &target,
            const TSValueTypeMetaData &source)
        {
            const auto &ops = ts_current_state_detail::current_state_ops_for(target.kind);
            return ops.reconcile_schema_compatible_impl(target, source);
        }

        void apply_current_value_direct(const TSOutputView &out, const ValueView &value)
        {
            auto mutation = out.begin_mutation(out.evaluation_time());
            static_cast<void>(mutation.copy_value_from(value));
        }

        void apply_current_value_dict(const TSOutputView &out, const ValueView &value)
        {
            auto       dict_out   = out.as_dict();
            auto       mutation   = dict_out.begin_mutation(out.evaluation_time());
            const auto source_map = value.as_map();

            mutation.touch();
            for (const auto &[key, child_value] : source_map)
            {
                auto child = mutation.at(key);
                apply_current_value(TSOutputView{out.output(), child, out.evaluation_time()}, child_value);
            }

            std::vector<Value> removals;
            for (const auto key : mutation.keys())
            {
                if (!source_map.contains(key)) { removals.emplace_back(key); }
            }
            for (const auto &key : removals) { static_cast<void>(mutation.erase(key.view())); }
        }

        void apply_current_value_list(const TSOutputView &out, const ValueView &value)
        {
            const auto *schema = out.schema();
            if (schema == nullptr)
            {
                throw std::logic_error("apply_current_value: TSL output schema is missing");
            }
            const auto source_values = value.as_indexed_view();
            if (schema->fixed_size() != 0 && source_values.size() != schema->fixed_size())
            {
                throw std::invalid_argument("apply_current_value: fixed TSL value has the wrong child count");
            }

            auto list_out = out.as_list();
            // RFC 0031: a dynamic TSL current value IS the list, so a shorter
            // source truncates rather than being rejected.
            if (schema->fixed_size() == 0 && source_values.size() != list_out.size())
            {
                list_out.resize(source_values.size());
            }

            for (std::size_t index = 0; index < source_values.size(); ++index)
            {
                auto source_child = source_values.at(index);
                // Current-value application is a patch: an UNSET structural
                // child leaves the target child untouched.
                if (!source_child.valid()) { continue; }
                apply_current_value(list_out.at(index), source_child);
            }
        }

        void apply_current_value_bundle(const TSOutputView &out, const ValueView &value)
        {
            const auto *schema = out.schema();
            if (schema == nullptr)
            {
                throw std::logic_error("apply_current_value: TSB output schema is missing");
            }
            const auto source_values = value.as_indexed_view();
            if (source_values.size() != schema->field_count())
            {
                throw std::invalid_argument("apply_current_value: TSB value has the wrong field count");
            }

            auto bundle_out = out.as_bundle();
            for (std::size_t index = 0; index < source_values.size(); ++index)
            {
                auto source_child = source_values.at(index);
                // As above, bundle field validity gives patch semantics.
                if (!source_child.valid()) { continue; }
                apply_current_value(bundle_out.at(index), source_child);
            }
        }

        [[nodiscard]] Value capture_current_atomic(const TSInputView &input)
        {
            return Value{input.value()};
        }

        [[nodiscard]] Value capture_current_signal(const TSInputView &)
        {
            return Value{true};
        }

        [[nodiscard]] Value capture_current_set(const TSInputView &input)
        {
            const auto &schema = require_schema(input.schema(), "capture_current_delta");
            const ValueTypeRef element = binding_for(
                schema.value_schema->element_type, "capture_current_delta");
            SetBuilder added{element};
            const auto set = input.as_set();
            for (const auto &value : set.values())
            {
                const BorrowedOperand borrowed{element, value, "capture_current_delta"};
                static_cast<void>(added.insert_copy(borrowed.data()));
            }
            SetBuilder removed{element};
            BundleBuilder bundle{binding_for(schema.delta_value_schema, "capture_current_delta")};
            bundle.set("added", added.build());
            bundle.set("removed", removed.build());
            return bundle.build();
        }

        [[nodiscard]] Value capture_current_dict(const TSInputView &input)
        {
            const auto &schema = require_schema(input.schema(), "capture_current_delta");
            const ValueTypeRef key_binding = binding_for(
                schema.key_type(), "capture_current_delta");
            const auto *element_schema = schema.element_ts();
            const ValueTypeRef delta_binding = binding_for(
                element_schema->delta_value_schema, "capture_current_delta");

            SetBuilder removed{key_binding};
            MapBuilder modified{key_binding, delta_binding};
            const auto dict = input.as_dict();
            for (const auto &[key, child] : dict.valid_items())
            {
                Value child_delta = capture_current_delta(child);
                modified.set_item(key, child_delta.view());
            }

            Value removed_delta = removed.build();
            Value modified_delta = modified.build();
            const std::array field_bindings{
                removed_delta.binding(), modified_delta.binding()};
            const auto bundle_binding = ValuePlanFactory::instance().realized_composite_type_for(
                schema.delta_value_schema, field_bindings);
            BundleBuilder bundle{bundle_binding};
            bundle.set("removed", std::move(removed_delta));
            bundle.set("modified", std::move(modified_delta));
            return bundle.build();
        }

        [[nodiscard]] Value capture_current_list(const TSInputView &input)
        {
            const auto &schema = require_schema(input.schema(), "capture_current_delta");
            const ValueTypeRef key_binding = binding_for(
                schema.delta_value_schema->key_type, "capture_current_delta");
            const ValueTypeRef delta_binding = binding_for(
                schema.delta_value_schema->element_type, "capture_current_delta");
            MapBuilder builder{key_binding, delta_binding};
            const auto list = input.as_list();
            for (const auto &[index, child] : list.valid_items())
            {
                const std::int64_t key = static_cast<std::int64_t>(index);
                Value child_delta = capture_current_delta(child);
                builder.set_item(ValueView{key_binding, std::addressof(key)}, child_delta.view());
            }
            return builder.build();
        }

        [[nodiscard]] Value capture_current_bundle(const TSInputView &input)
        {
            const auto &schema = require_schema(input.schema(), "capture_current_delta");
            const auto type = ts_type_for(&schema, "capture_current_delta");
            BundleBuilder builder{binding_for(schema.delta_value_schema, "capture_current_delta")};
            initialize_tsb_delta_defaults(type, builder);
            auto bundle = input.as_bundle();
            for (std::size_t index = 0; index < bundle.size(); ++index)
            {
                auto child = bundle.at(index);
                if (child.valid()) { builder.set(index, capture_current_delta(child)); }
            }
            return builder.build();
        }

        [[nodiscard]] Value capture_current_transient(const TSInputView &input)
        {
            if (input.modified()) { return capture_delta(input); }
            throw std::logic_error(
                "capture_current_delta cannot reconstruct an unmodified TSW or REF");
        }

        [[nodiscard]] bool observable_atomic(const TSInputView &input,
                                             const ValueView &delta)
        {
            return input.modified() && input.valid() && delta.has_value();
        }

        [[nodiscard]] bool observable_window(const TSInputView &input,
                                             const ValueView &delta)
        {
            // Tick windows emit before they reach min-size validity.
            return input.modified() && delta.has_value();
        }

        [[nodiscard]] bool observable_set(const TSInputView &input,
                                          const ValueView &delta)
        {
            if (!input.modified() || !delta.has_value()) { return false; }
            if (input.valid()) { return true; }
            const auto bundle = delta.as_bundle();
            return bundle.at(tss_delta_removed).as_indexed_view().size() != 0;
        }

        [[nodiscard]] bool observable_dict(const TSInputView &input,
                                           const ValueView &delta)
        {
            if (!input.modified() || !delta.has_value()) { return false; }
            if (input.valid()) { return true; }
            const auto bundle = delta.as_bundle();
            return bundle.at(tsd_delta_removed).as_indexed_view().size() != 0 ||
                   bundle.at(tsd_delta_modified).as_map().size() != 0;
        }

        [[nodiscard]] bool observable_list(const TSInputView &input,
                                           const ValueView &delta)
        {
            if (!input.modified() || !delta.has_value()) { return false; }
            const auto &schema = require_schema(input.schema(), "delta_is_observable");
            if (schema.fixed_size() != 0) { return delta.as_map().size() != 0; }
            // A dynamic TSL delta is Bundle{removed, modified} (RFC 0031): a
            // truncation is an observable event in its own right, and a valid
            // list still reports its structural ticks.
            const auto bundle = delta.as_bundle();
            const bool has_children = bundle.at(tsl_delta_removed).as_indexed_view().size() != 0 ||
                                      bundle.at(tsl_delta_modified).as_map().size() != 0;
            return input.valid() || has_children;
        }

        [[nodiscard]] bool observable_bundle(const TSInputView &input,
                                             const ValueView &delta)
        {
            if (!input.modified() || !delta.has_value()) { return false; }
            const auto bundle = delta.as_bundle();
            auto children = input.as_bundle();
            for (std::size_t index = 0; index < children.size(); ++index)
            {
                auto child = children.at(index);
                if (!child.modified()) { continue; }
                const auto &ops = current_state_ops_for_input(child, "delta_is_observable");
                if (ops.delta_is_observable_impl(child, bundle.at(index))) { return true; }
            }
            return false;
        }

        void reconcile_current(const TSOutputView &target, const TSInputView &source,
                               TSCurrentReconcileOptions options);
        void reconcile_current(const TSOutputView &target, const TSDataView &source,
                               TSCurrentReconcileOptions options);

        template <typename Source>
        [[nodiscard]] bool source_live(const Source &source)
        {
            if constexpr (std::same_as<Source, TSInputView>)
            {
                return source.valid();
            }
            else
            {
                return source.valid() && source.has_current_value();
            }
        }

        template <typename Source>
        [[nodiscard]] bool source_modified(const Source &source,
                                           const TSOutputView &target)
        {
            if constexpr (std::same_as<Source, TSInputView>)
            {
                return source.modified();
            }
            else
            {
                if (!source.valid()) { return false; }
                return source.modified(target.evaluation_time());
            }
        }

        template <typename Source>
        [[nodiscard]] bool should_visit_source(const Source &source,
                                               const TSOutputView &target,
                                               TSCurrentReconcileOptions options)
        {
            return options.scope == TSCurrentReconcileScope::Full || options.sample_all ||
                   source_modified(source, target);
        }

        template <typename SourceChild>
        [[nodiscard]] bool source_child_live(const SourceChild &child)
        {
            if constexpr (std::same_as<SourceChild, TSInputView>)
            {
                return child.valid();
            }
            else
            {
                return child.valid() && child.has_current_value();
            }
        }

        void invalidate_target(const TSOutputView &target)
        {
            if (!target.data_view().has_current_value()) { return; }
            auto mutation = target.begin_mutation(target.evaluation_time());
            static_cast<void>(mutation.invalidate());
        }

        template <typename Source>
        void reconcile_atomic_impl(const TSOutputView &target, const Source &source,
                                   TSCurrentReconcileOptions options)
        {
            if (!should_visit_source(source, target, options)) { return; }
            if (!source_live(source))
            {
                if (options.scope == TSCurrentReconcileScope::Full) { invalidate_target(target); }
                return;
            }

            const auto source_value = source.value();
            const bool equal = target.data_view().has_current_value() &&
                               target.value().equals(source_value);
            if (equal && !options.sample_all) { return; }
            auto mutation = target.begin_mutation(target.evaluation_time());
            static_cast<void>(mutation.copy_value_from(source_value));
            if (options.sample_all) { mutation.mark_modified(); }
        }

        template <typename Source>
        void reconcile_set_impl(const TSOutputView &target, const Source &source,
                                TSCurrentReconcileOptions options)
        {
            if (!should_visit_source(source, target, options)) { return; }
            auto target_set = target.as_set();

            if (!source_live(source))
            {
                if (target.data_view().has_current_value())
                {
                    auto mutation = target_set.begin_mutation(target.evaluation_time());
                    mutation.clear();
                }
                return;
            }
            auto mutation = target_set.begin_mutation(target.evaluation_time());
            const auto source_set = source.as_set();

            if (options.scope == TSCurrentReconcileScope::Full)
            {
                std::vector<Value> removals;
                for (const auto key : target_set.values())
                {
                    if (!source_set.contains(key)) { removals.emplace_back(key); }
                }
                for (const auto &key : removals)
                {
                    static_cast<void>(mutation.remove(key.view()));
                }
                for (const auto key : source_set.values())
                {
                    if (!target_set.contains(key)) { static_cast<void>(mutation.add(key)); }
                }
                if (options.sample_all) { mutation.touch(); }
                return;
            }

            for (const auto key : source_set.removed())
            {
                static_cast<void>(mutation.remove(key));
            }
            for (const auto key : source_set.added())
            {
                if (!target_set.contains(key)) { static_cast<void>(mutation.add(key)); }
            }
            if (options.sample_all) { mutation.touch(); }
        }

        template <typename Source>
        void reconcile_dict_impl(const TSOutputView &target, const Source &source,
                                 TSCurrentReconcileOptions options)
        {
            if (!should_visit_source(source, target, options)) { return; }
            auto target_dict = target.as_dict();

            if (!source_live(source))
            {
                if (target.data_view().has_current_value())
                {
                    auto mutation = target_dict.begin_mutation(target.evaluation_time());
                    mutation.clear();
                }
                return;
            }
            auto mutation = target_dict.begin_mutation(target.evaluation_time());
            const auto source_dict = source.as_dict();

            if (options.scope == TSCurrentReconcileScope::Full)
            {
                std::vector<Value> removals;
                for (const auto key : target_dict.keys())
                {
                    const auto source_child = source_dict.at(key);
                    if (!source_child_live(source_child))
                    {
                        removals.emplace_back(key);
                    }
                }
                for (const auto &key : removals)
                {
                    static_cast<void>(mutation.erase(key.view()));
                }
                for (auto &&[key, source_child] : source_dict.valid_items())
                {
                    auto target_child = mutation.at(key);
                    reconcile_current(
                        TSOutputView{target.output(), target_child, target.evaluation_time()},
                        source_child,
                        TSCurrentReconcileOptions{TSCurrentReconcileScope::Full,
                                                  options.sample_all});
                }
                return;
            }

            const bool structure_modified = [&]() {
                if constexpr (std::same_as<Source, TSInputView>)
                {
                    return source_dict.structure_modified();
                }
                else
                {
                    return source_dict.structural_delta_current(target.evaluation_time());
                }
            }();
            if (structure_modified)
            {
                for (const auto key : source_dict.removed_keys())
                {
                    static_cast<void>(mutation.erase(key));
                }
            }
            auto modified_items = [&]() {
                if constexpr (std::same_as<Source, TSInputView>)
                {
                    return source_dict.modified_items();
                }
                else
                {
                    return source_dict.modified_items(target.evaluation_time());
                }
            }();
            for (auto &&[key, source_child] : modified_items)
            {
                if (!source_child_live(source_child)) { continue; }
                auto target_child = mutation.at(key);
                reconcile_current(
                    TSOutputView{target.output(), target_child, target.evaluation_time()},
                    source_child,
                    TSCurrentReconcileOptions{TSCurrentReconcileScope::Full,
                                              options.sample_all});
            }
        }

        template <typename Source>
        void reconcile_indexed_impl(const TSOutputView &target, const Source &source,
                                    TSCurrentReconcileOptions options)
        {
            if (!should_visit_source(source, target, options)) { return; }
            const std::size_t target_size = target.data_view().indexed_child_count();
            const bool full = options.scope == TSCurrentReconcileScope::Full;

            // A default view is the explicit "no source" sentinel used while
            // a forwarding publication changes owners. It has no topology to
            // traverse; a full reconciliation invalidates the current fixed
            // or indexed state without attempting representation dispatch.
            if (source.schema() == nullptr)
            {
                if (full)
                {
                    for (std::size_t index = 0; index < target_size; ++index)
                    {
                        invalidate_target(target.indexed_child_at(index));
                    }
                }
                return;
            }

            const std::size_t source_size = [&]() {
                if constexpr (std::same_as<Source, TSInputView>)
                {
                    return source.data_view().indexed_child_count();
                }
                else
                {
                    return source.indexed_child_count();
                }
            }();

            // RFC 0031: a dynamic TSL's length IS part of its current state, so
            // a full reconciliation resizes it instead of invalidating the
            // surplus children the way a fixed shape must.
            const bool resizable = full && target.data_view().ops().indexed_child_growth;
            if (resizable && source_size != target_size) { target.as_list().resize(source_size); }

            for (std::size_t index = 0; index < source_size; ++index)
            {
                auto source_child = [&]() {
                    if constexpr (std::same_as<Source, TSInputView>)
                    {
                        return source.indexed_child_at(index);
                    }
                    else
                    {
                        return source.indexed_child_at(index);
                    }
                }();
                if (!full && !options.sample_all &&
                    !source_modified(source_child, target))
                {
                    continue;
                }
                auto target_child = target.indexed_child_at(index);
                reconcile_current(
                    target_child, source_child,
                    TSCurrentReconcileOptions{full ? TSCurrentReconcileScope::Full
                                                   : TSCurrentReconcileScope::Incremental,
                                              options.sample_all});
            }

            if (full && !resizable)
            {
                for (std::size_t index = source_size; index < target_size; ++index)
                {
                    invalidate_target(target.indexed_child_at(index));
                }
            }

            if (source_live(source) && source_size == 0 &&
                (!target.valid() || options.sample_all))
            {
                auto mutation = target.begin_mutation(target.evaluation_time());
                static_cast<void>(mutation.copy_value_from(source.value()));
                if (options.sample_all) { mutation.mark_modified(); }
            }
        }

        void reconcile_current(const TSOutputView &target, const TSInputView &source,
                               TSCurrentReconcileOptions options)
        {
            const auto *ops = target.data_view().ops().current_state_ops;
            if (ops == nullptr)
            {
                throw std::logic_error("reconcile_current_state: target policy is null");
            }
            ops->reconcile_input_impl(target, source, options);
        }

        void reconcile_current(const TSOutputView &target, const TSDataView &source,
                               TSCurrentReconcileOptions options)
        {
            const auto *ops = target.data_view().ops().current_state_ops;
            if (ops == nullptr)
            {
                throw std::logic_error("reconcile_current_state: target policy is null");
            }
            ops->reconcile_data_impl(target, source, options);
        }

        [[noreturn]] bool missing_schema_compatible(const TSValueTypeMetaData &,
                                                    const ValueTypeMetaData &)
        {
            throw std::logic_error("TSData current-state schema compatibility is not available");
        }

        [[noreturn]] bool missing_reconcile_schema_compatible(
            const TSValueTypeMetaData &,
            const TSValueTypeMetaData &)
        {
            throw std::logic_error(
                "TSData current-state topology compatibility is not available");
        }

        [[noreturn]] Value missing_capture_current(const TSInputView &)
        {
            throw std::logic_error("TSData current-state capture is not available");
        }

        [[noreturn]] void missing_apply_current(const TSOutputView &, const ValueView &)
        {
            throw std::logic_error("TSData current-value application is not available");
        }

        [[noreturn]] bool missing_observable_delta(const TSInputView &, const ValueView &)
        {
            throw std::logic_error("TSData observable-delta classification is not available");
        }

        [[noreturn]] void missing_reconcile_input(const TSOutputView &, const TSInputView &,
                                                  TSCurrentReconcileOptions)
        {
            throw std::logic_error("TSData current-state reconciliation is not available");
        }

        [[noreturn]] void missing_reconcile_data(const TSOutputView &, const TSDataView &,
                                                 TSCurrentReconcileOptions)
        {
            throw std::logic_error("TSData current-state reconciliation is not available");
        }

        [[nodiscard]] const TSCurrentStateOps &atomic_current_state_ops() noexcept
        {
            static const TSCurrentStateOps ops{
                &current_value_schema_compatible_atomic,
                &current_ts_schema_compatible_atomic,
                &capture_current_atomic,
                &apply_current_value_direct,
                &observable_atomic,
                &reconcile_atomic_impl<TSInputView>,
                &reconcile_atomic_impl<TSDataView>,
            };
            return ops;
        }

        [[nodiscard]] const TSCurrentStateOps &signal_current_state_ops() noexcept
        {
            static const TSCurrentStateOps ops{
                &current_value_schema_compatible_atomic,
                &current_ts_schema_compatible_atomic,
                &capture_current_signal,
                &apply_current_value_direct,
                &observable_atomic,
                &reconcile_atomic_impl<TSInputView>,
                &reconcile_atomic_impl<TSDataView>,
            };
            return ops;
        }

        [[nodiscard]] const TSCurrentStateOps &transient_current_state_ops() noexcept
        {
            static const TSCurrentStateOps ops{
                &current_value_schema_compatible_atomic,
                &current_ts_schema_compatible_ref,
                &capture_current_transient,
                &apply_current_value_direct,
                &observable_atomic,
                &reconcile_atomic_impl<TSInputView>,
                &reconcile_atomic_impl<TSDataView>,
            };
            return ops;
        }

        [[nodiscard]] const TSCurrentStateOps &window_current_state_ops() noexcept
        {
            static const TSCurrentStateOps ops{
                &current_value_schema_compatible_atomic,
                &current_ts_schema_compatible_window,
                &capture_current_transient,
                &apply_current_value_direct,
                &observable_window,
                &reconcile_atomic_impl<TSInputView>,
                &reconcile_atomic_impl<TSDataView>,
            };
            return ops;
        }

        [[nodiscard]] const TSCurrentStateOps &set_current_state_ops() noexcept
        {
            static const TSCurrentStateOps ops{
                &current_value_schema_compatible_set,
                &current_ts_schema_compatible_set,
                &capture_current_set,
                &apply_current_value_direct,
                &observable_set,
                &reconcile_set_impl<TSInputView>,
                &reconcile_set_impl<TSDataView>,
            };
            return ops;
        }

        [[nodiscard]] const TSCurrentStateOps &dict_current_state_ops() noexcept
        {
            static const TSCurrentStateOps ops{
                &current_value_schema_compatible_dict,
                &current_ts_schema_compatible_dict,
                &capture_current_dict,
                &apply_current_value_dict,
                &observable_dict,
                &reconcile_dict_impl<TSInputView>,
                &reconcile_dict_impl<TSDataView>,
            };
            return ops;
        }

        [[nodiscard]] const TSCurrentStateOps &list_current_state_ops() noexcept
        {
            static const TSCurrentStateOps ops{
                &current_value_schema_compatible_list,
                &current_ts_schema_compatible_list,
                &capture_current_list,
                &apply_current_value_list,
                &observable_list,
                &reconcile_indexed_impl<TSInputView>,
                &reconcile_indexed_impl<TSDataView>,
            };
            return ops;
        }

        [[nodiscard]] const TSCurrentStateOps &bundle_current_state_ops() noexcept
        {
            static const TSCurrentStateOps ops{
                &current_value_schema_compatible_bundle,
                &current_ts_schema_compatible_bundle,
                &capture_current_bundle,
                &apply_current_value_bundle,
                &observable_bundle,
                &reconcile_indexed_impl<TSInputView>,
                &reconcile_indexed_impl<TSDataView>,
            };
            return ops;
        }
    }  // namespace

    namespace ts_current_state_detail
    {
        const TSCurrentStateOps &missing_current_state_ops() noexcept
        {
            static const TSCurrentStateOps ops{
                &missing_schema_compatible,
                &missing_reconcile_schema_compatible,
                &missing_capture_current,
                &missing_apply_current,
                &missing_observable_delta,
                &missing_reconcile_input,
                &missing_reconcile_data,
            };
            return ops;
        }

        const TSCurrentStateOps &current_state_ops_for(TSTypeKind kind) noexcept
        {
            static const std::array<const TSCurrentStateOps *, 8> table{
                &atomic_current_state_ops(),
                &set_current_state_ops(),
                &dict_current_state_ops(),
                &list_current_state_ops(),
                &window_current_state_ops(),
                &bundle_current_state_ops(),
                &transient_current_state_ops(),
                &signal_current_state_ops(),
            };
            const auto index = static_cast<std::size_t>(kind);
            return index < table.size() ? *table[index] : missing_current_state_ops();
        }
    }  // namespace ts_current_state_detail

    namespace ts_data_detail
    {
        [[nodiscard]] Value empty_delta_atomic(const TSRoleTypeRef &binding)
        {
            const auto *schema = binding.schema();
            if (schema == nullptr || schema->delta_value_schema == nullptr)
            {
                throw std::logic_error("empty_delta_atomic: schema is not resolved");
            }
            return Value{*schema->delta_value_schema};
        }

        [[nodiscard]] Value empty_delta_tss(const TSRoleTypeRef &binding)
        {
            const auto *schema = binding.schema();
            if (schema == nullptr || schema->value_schema == nullptr || schema->delta_value_schema == nullptr)
            {
                throw std::logic_error("empty_delta_tss: schema is not resolved");
            }

            const ValueTypeRef elem_binding =
                binding_for(schema->value_schema->element_type, "empty_delta_tss");
            SetBuilder    added{elem_binding};
            SetBuilder    removed{elem_binding};
            BundleBuilder bundle{binding_for(schema->delta_value_schema, "empty_delta_tss")};
            bundle.set("added", added.build());
            bundle.set("removed", removed.build());
            return bundle.build();
        }

        [[nodiscard]] Value empty_delta_tsd(const TSRoleTypeRef &binding)
        {
            const auto *schema = binding.schema();
            if (schema == nullptr || schema->delta_value_schema == nullptr || schema->element_ts() == nullptr)
            {
                throw std::logic_error("empty_delta_tsd: schema is not resolved");
            }

            const ValueTypeRef key_binding = binding_for(schema->key_type(), "empty_delta_tsd");
            const ValueTypeRef delta_binding =
                binding_for(schema->element_ts()->delta_value_schema, "empty_delta_tsd");
            SetBuilder    removed{key_binding};
            MapBuilder    modified{key_binding, delta_binding};
            BundleBuilder bundle{binding_for(schema->delta_value_schema, "empty_delta_tsd")};
            bundle.set("removed", removed.build());
            bundle.set("modified", modified.build());
            return bundle.build();
        }

        /** The modified-index map inside a TSL delta: the whole delta for a
            fixed TSL, and the second bundle field for a dynamic one (RFC 0031). */
        [[nodiscard]] const ValueTypeMetaData *tsl_modified_map_schema(const TSValueTypeMetaData &schema,
                                                                        const char *what)
        {
            const ValueTypeMetaData *delta = schema.delta_value_schema;
            if (delta == nullptr) { throw std::logic_error(std::string{what} + ": schema is not resolved"); }
            if (schema.fixed_size() != 0) { return delta; }
            if (delta->value_kind() != ValueTypeKind::Bundle || delta->field_count != 2)
            {
                throw std::logic_error(std::string{what} + ": dynamic TSL delta must be a bundle");
            }
            return delta->fields[tsl_delta_modified].type;
        }

        [[nodiscard]] Value empty_delta_tsl(const TSRoleTypeRef &binding)
        {
            const auto *schema = binding.schema();
            if (schema == nullptr || schema->delta_value_schema == nullptr)
            {
                throw std::logic_error("empty_delta_tsl: schema is not resolved");
            }

            const ValueTypeMetaData *map_meta = tsl_modified_map_schema(*schema, "empty_delta_tsl");
            const ValueTypeRef key_binding = binding_for(map_meta->key_type, "empty_delta_tsl");
            const ValueTypeRef val_binding = binding_for(map_meta->element_type, "empty_delta_tsl");
            MapBuilder               builder{key_binding, val_binding};
            if (schema->fixed_size() != 0) { return builder.build(); }

            SetBuilder removed{key_binding};
            BundleBuilder bundle{binding_for(schema->delta_value_schema, "empty_delta_tsl")};
            bundle.set("removed", removed.build());
            bundle.set("modified", builder.build());
            return bundle.build();
        }

        [[nodiscard]] Value empty_delta_tsb(const TSRoleTypeRef &binding)
        {
            const auto *schema = binding.schema();
            if (schema == nullptr || schema->delta_value_schema == nullptr)
            {
                throw std::logic_error("empty_delta_tsb: schema is not resolved");
            }

            BundleBuilder builder{binding_for(schema->delta_value_schema, "empty_delta_tsb")};
            initialize_tsb_delta_defaults(binding, builder);
            return builder.build();
        }

        Value capture_delta_ts(const TSInputView &in)
        {
            const ValueView value = in.value();
            if (value.type()) { return Value{value}; }

            // A modified atomic endpoint may be scheduled by a reference
            // unbind while carrying no value. Preserve the canonical delta
            // binding as a typed null so generic consumers can classify the
            // event without reconstructing the representation policy.
            const auto &schema = require_schema(in.schema(), "capture_delta");
            if (schema.delta_value_schema == nullptr)
            {
                throw std::logic_error("capture_delta: atomic delta schema is missing");
            }
            return Value{*schema.delta_value_schema};
        }

        Value capture_delta_signal(const TSInputView &)
        {
            return Value{true};
        }

        Value capture_delta_tsw(const TSInputView &in)
        {
            if (in.data_view().as_window().cleared(in.evaluation_time()))
            {
                throw std::logic_error(
                    "capture_delta: TSW clear ticks are not representable by the legacy scalar delta");
            }
            const ValueView delta = in.delta_value();
            if (!delta.has_value())
            {
                throw std::logic_error(
                    "capture_delta: TSW clear/removal-only ticks are not representable by the legacy scalar delta");
            }
            return Value{delta};
        }

        Value capture_delta_tss(const TSInputView &in)
        {
            const TSValueTypeMetaData *schema = in.schema();
            const ValueTypeMetaData *bundle_meta = schema->delta_value_schema;
            const ValueTypeMetaData *elem_meta   = schema->value_schema->element_type;  // the Set's element E
            const ValueTypeRef elem_binding = binding_for(elem_meta, "capture_delta");

            const auto set = in.as_set();
            SetBuilder added{elem_binding};
            for (const auto &e : set.added())
            {
                const BorrowedOperand borrowed{elem_binding, e, "capture_delta"};
                (void)added.insert_copy(borrowed.data());
            }
            SetBuilder removed{elem_binding};
            for (const auto &e : set.removed())
            {
                const BorrowedOperand borrowed{elem_binding, e, "capture_delta"};
                (void)removed.insert_copy(borrowed.data());
            }

            BundleBuilder bundle{binding_for(bundle_meta, "capture_delta")};
            bundle.set("added", added.build());
            bundle.set("removed", removed.build());
            return bundle.build();
        }

        Value capture_delta_tsd(const TSInputView &in)
        {
            const ValueTypeRef key_binding =
                binding_for(in.schema()->key_type(), "capture_delta");
            const auto *element_schema = in.schema()->element_ts();
            const ValueTypeRef delta_binding =
                binding_for(element_schema->delta_value_schema, "capture_delta");
            if (delta_binding.schema() != in.schema()->element_ts()->delta_value_schema)
            {
                throw std::logic_error("capture_delta_tsd resolved the wrong element delta binding");
            }

            const auto dict = in.as_dict();
            SetBuilder removed{key_binding};
            for (const auto &key : dict.removed_keys())
            {
                const BorrowedOperand borrowed{key_binding, key, "capture_delta"};
                (void)removed.insert_copy(borrowed.data());
            }

            MapBuilder modified{key_binding, delta_binding};
            for (const auto &[key, child] : dict.modified_items())
            {
                if (!child.valid()) { continue; }   // empty-reference elements have no value
                if (child.schema() != in.schema()->element_ts())
                {
                    throw std::logic_error("capture_delta_tsd resolved the wrong child TS schema");
                }
                const BorrowedOperand borrowed{key_binding, key, "capture_delta"};
                const Value child_delta = capture_delta(child);
                if (child_delta.binding() == delta_binding)
                {
                    modified.set_item_copy(
                        borrowed.data(), child_delta.view().data());
                    continue;
                }
                Value stored_delta{delta_binding};
                // An atomic closed union stores the concrete leaf selected by
                // the child TS in its owning alternative binding.
                delta_binding.ops_ref().copy_assign_from(
                    delta_binding,
                    const_cast<void *>(stored_delta.view().data()),
                    child_delta.binding(),
                    child_delta.view().data());
                modified.set_item_copy(
                    borrowed.data(), stored_delta.view().data());
            }

            // No strict removals: a capture reports what happened, and
            // "strict" is an expectation an author holds about the target.
            Value removed_delta = removed.build();
            Value modified_delta = modified.build();
            const std::array field_bindings{removed_delta.binding(), modified_delta.binding()};
            const auto bundle_binding = ValuePlanFactory::instance().realized_composite_type_for(
                in.schema()->delta_value_schema,
                field_bindings);
            BundleBuilder bundle{bundle_binding};
            bundle.set("removed", std::move(removed_delta));
            bundle.set("modified", std::move(modified_delta));
            return bundle.build();
        }

        Value capture_delta_tsl(const TSInputView &in)
        {
            const TSValueTypeMetaData *schema = in.schema();
            const ValueTypeMetaData *map_meta = tsl_modified_map_schema(*schema, "capture_delta");
            const ValueTypeRef key_binding = binding_for(map_meta->key_type, "capture_delta");
            const ValueTypeRef val_binding = binding_for(map_meta->element_type, "capture_delta");

            MapBuilder builder{key_binding, val_binding};
            const auto list = in.as_list();
            for (const auto &[index, child] : list.modified_items())
            {
                if (!child.valid()) { continue; }   // empty-reference elements have no value
                const std::int64_t key = static_cast<std::int64_t>(index);
                // The child's canonical delta is exactly the map's value schema, so a
                // whole-value copy of the (owned, rebuilt) child delta is correct for
                // both scalar children (delta == value) and container children.
                const Value child_delta = capture_delta(child);
                builder.set_item_copy(std::addressof(key), child_delta.view().data());
            }
            if (schema->fixed_size() != 0) { return builder.build(); }

            // RFC 0031: a dynamic TSL also reports the indices truncated away
            // this cycle. They are always a contiguous suffix.
            SetBuilder removed{key_binding};
            for (const std::size_t index : list.removed_indices())
            {
                const std::int64_t key = static_cast<std::int64_t>(index);
                static_cast<void>(removed.insert_copy(std::addressof(key)));
            }
            BundleBuilder bundle{binding_for(schema->delta_value_schema, "capture_delta")};
            bundle.set("removed", removed.build());
            bundle.set("modified", builder.build());
            return bundle.build();
        }

        Value capture_delta_tsb(const TSInputView &in)
        {
            const auto type = ts_type_for(&require_schema(in.schema(), "capture_delta"), "capture_delta");
            const auto *schema = type.schema();
            BundleBuilder builder{binding_for(schema->delta_value_schema, "capture_delta")};
            initialize_tsb_delta_defaults(type, builder);
            auto          bundle = in.as_bundle();
            for (std::size_t index = 0; index < bundle.size(); ++index)
            {
                auto child = bundle.at(index);
                // modified-but-INVALID children are skipped: a from-REF
                // rebind marks the position modified, but an EMPTY reference
                // leaves it unbound - there is no value to capture.
                if (!child.modified() || !child.valid()) { continue; }
                Value child_delta = capture_delta(child);
                builder.set(index, std::move(child_delta));
            }
            return builder.build();
        }

        bool delta_has_effect_atomic(const TSOutputView &, const ValueView &delta)
        {
            return delta.has_value();
        }

        bool delta_has_effect_tss(const TSOutputView &out, const ValueView &delta)
        {
            if (!delta.has_value()) { return false; }
            const auto bundle = delta.as_bundle();
            assert(delta_field_is(delta, tss_delta_added, "added"));
            assert(delta_field_is(delta, tss_delta_removed, "removed"));
            if (bundle.at(tss_delta_added).as_indexed_view().size() != 0 ||
                bundle.at(tss_delta_removed).as_indexed_view().size() != 0)
            {
                return true;
            }
            // An explicitly EMPTY tick VALIDATES a fresh set (hgraph: the
            // TSS becomes valid with the empty value); on an already-valid
            // set it stays a no-op (dedup).
            return !out.valid();
        }

        bool delta_has_effect_tsd(const TSOutputView &out, const ValueView &delta)
        {
            if (!delta.has_value()) { return false; }
            const auto bundle = delta.as_bundle();
            assert(delta_field_is(delta, tsd_delta_modified, "modified"));
            const auto modified = bundle.at(tsd_delta_modified).as_map();
            if (modified.size() != 0) { return true; }
            // Only an AUTHORED delta carries strict removals; an observed one
            // has no such field.
            if (bundle.size() == tsd_authored_delta_fields &&
                bundle.at(tsd_delta_removed_strict).as_indexed_view().size() != 0)
            {
                return true;
            }

            assert(delta_field_is(delta, tsd_delta_removed, "removed"));
            const auto removed = bundle.at(tsd_delta_removed).as_indexed_view();
            if (removed.size() != 0)
            {
                const auto dict_out = out.as_dict();
                for (std::size_t index = 0; index < removed.size(); ++index)
                {
                    if (dict_out.contains(removed.at(index))) { return true; }
                }
                // Lenient removals of absent keys are not an empty
                // validating tick, even when the TSD is still fresh.
                return false;
            }

            // The empty-tick validation rule, as for TSS.
            return !out.valid();
        }

        bool delta_has_effect_tsl(const TSOutputView &out, const ValueView &delta)
        {
            if (!delta.has_value()) { return false; }
            const auto *schema = out.schema();
            if (schema == nullptr || schema->fixed_size() != 0)
            {
                return delta.as_map().size() != 0;
            }
            // Dynamic TSL: either a child change or a truncation (RFC 0031).
            const auto bundle = delta.as_bundle();
            assert(delta_field_is(delta, tsl_delta_removed, "removed"));
            assert(delta_field_is(delta, tsl_delta_modified, "modified"));
            if (bundle.at(tsl_delta_modified).as_map().size() != 0) { return true; }
            const auto removed = bundle.at(tsl_delta_removed).as_indexed_view();
            if (removed.size() == 0) { return false; }
            // Lenient: a removal below the current length truncates, one at or
            // above it is already satisfied.
            const auto list_out = out.as_list();
            for (std::size_t index = 0; index < removed.size(); ++index)
            {
                const auto position = removed.at(index).template checked_as<std::int64_t>();
                if (position >= 0 && static_cast<std::size_t>(position) < list_out.size()) { return true; }
            }
            return false;
        }

        bool delta_has_effect_tsb(const TSOutputView &out, const ValueView &delta)
        {
            if (!delta.has_value()) { return false; }

            auto       bundle_out = out.as_bundle();
            const auto bundle     = delta.as_bundle();
            for (std::size_t index = 0; index < bundle.size(); ++index)
            {
                auto        child     = bundle_out.at(index);
                const auto &child_ops = child.data_view().ops();
                if (child_ops.delta_has_effect_impl(child, bundle.at(index))) { return true; }
            }
            return false;
        }

        void apply_delta_atomic(const TSOutputView &out, const ValueView &delta)
        {
            auto mutation = out.begin_mutation(out.evaluation_time());
            // copy_value_from returns FIRST-FOR-TIME, not success (the
            // move_value_from contract) — genuine failures throw inside the
            // ops. A repeat apply in one cycle (e.g. a throttle release
            // merging with a same-cycle tick) is a benign overwrite.
            static_cast<void>(mutation.copy_value_from(delta));
        }

        void apply_delta_tsw(const TSOutputView &out, const ValueView &delta)
        {
            auto window_out = out.as_window();
            window_out.begin_mutation(out.evaluation_time()).push(delta);
        }

        void apply_delta_tss(const TSOutputView &out, const ValueView &delta)
        {
            // Contract (ruling 2026-07-28): a canonical set delta's added and
            // removed lists are DISJOINT — an element in both is incorrect
            // data, rejected at the construction boundaries (the _SetDelta
            // literal, the recipe decoder); captured deltas are disjoint by
            // slot-mask construction. Under that invariant the application
            // order is immaterial.
            const auto bundle  = delta.as_bundle();
            auto       set_out = out.as_set();
            auto       mutation = set_out.begin_mutation(out.evaluation_time());
            assert(delta_field_is(delta, tss_delta_added, "added"));
            assert(delta_field_is(delta, tss_delta_removed, "removed"));
            const auto removed = bundle.at(tss_delta_removed).as_indexed_view();
            for (std::size_t i = 0; i < removed.size(); ++i) { (void)mutation.remove(removed.at(i)); }
            const auto added = bundle.at(tss_delta_added).as_indexed_view();
            for (std::size_t i = 0; i < added.size(); ++i) { (void)mutation.add(added.at(i)); }
            // Touch LAST: the once-per-time notification rides the FIRST
            // record and must carry the real changes; the trailing touch
            // VALIDATES an explicitly-empty tick (hgraph: an empty first
            // tick makes the set valid with the empty value).
            mutation.touch();
        }

        void apply_delta_tsd(const TSOutputView &out, const ValueView &delta)
        {
            const auto bundle = delta.as_bundle();
            auto       dict_out = out.as_dict();
            auto       mutation = dict_out.begin_mutation(out.evaluation_time());

            // Applying a removal is ALWAYS lenient, whatever it meant to its
            // producer: a captured removal is a fact about the source, and the
            // target being replayed or replicated into need not hold the key.
            assert(delta_field_is(delta, tsd_delta_removed, "removed"));
            const auto removed = bundle.at(tsd_delta_removed).as_indexed_view();
            for (std::size_t i = 0; i < removed.size(); ++i) { (void)mutation.erase(removed.at(i)); }

            // The one exception, and the only strictness in the model: keys a
            // USER authored as REMOVE, asserting the target holds them. Present
            // only on an AUTHORED delta (``authored_delta_schema``); apply
            // accepts either shape.
            if (bundle.size() == tsd_authored_delta_fields)
            {
                assert(delta_field_is(delta, tsd_delta_removed_strict, "removed_strict"));
                const auto removed_strict = bundle.at(tsd_delta_removed_strict).as_indexed_view();
                for (std::size_t i = 0; i < removed_strict.size(); ++i)
                {
                    if (!mutation.erase(removed_strict.at(i)))
                    {
                        throw std::runtime_error(
                            "REMOVE: key not present in TSD (use REMOVE_IF_EXISTS to remove-if-present)");
                    }
                }
            }

            assert(delta_field_is(delta, tsd_delta_modified, "modified"));
            const auto modified = bundle.at(tsd_delta_modified).as_map();
            for (const auto &[key, child_delta] : modified)
            {
                auto child = mutation.at(key);
                apply_delta(TSOutputView{out.output(), child, out.evaluation_time()}, child_delta);
            }
            mutation.touch();   // LAST - the empty-tick validation rule (see apply_delta_tss)
        }

        void apply_delta_tsl(const TSOutputView &out, const ValueView &delta)
        {
            auto        list_out = out.as_list();
            const auto *schema   = out.schema();
            const bool  dynamic  = schema != nullptr && schema->fixed_size() == 0;
            if (dynamic)
            {
                assert(delta_field_is(delta, tsl_delta_removed, "removed"));
                assert(delta_field_is(delta, tsl_delta_modified, "modified"));
                // Truncation is applied FIRST so a same-cycle re-grow through
                // `modified` reproduces the captured net structure (RFC 0031).
                const auto removed = delta.as_bundle().at(tsl_delta_removed).as_indexed_view();
                auto       truncate_to = list_out.size();
                for (std::size_t index = 0; index < removed.size(); ++index)
                {
                    const auto position = removed.at(index).template checked_as<std::int64_t>();
                    if (position < 0) { continue; }
                    truncate_to = std::min(truncate_to, static_cast<std::size_t>(position));
                }
                if (truncate_to < list_out.size()) { list_out.resize(truncate_to); }
            }
            const auto apply_modified = [&](const ValueView &map_view) {
                for (const auto &[key, child_delta] : map_view.as_map())
                {
                    auto child =
                        list_out.at(static_cast<std::size_t>(key.template checked_as<std::int64_t>()));
                    apply_delta(child, child_delta);
                }
            };
            if (dynamic) { apply_modified(delta.as_bundle().at(tsl_delta_modified)); }
            else { apply_modified(delta); }
        }

        void apply_delta_tsb(const TSOutputView &out, const ValueView &delta)
        {
            auto       bundle_out = out.as_bundle();
            const auto bundle     = delta.as_bundle();
            for (std::size_t index = 0; index < bundle.size(); ++index)
            {
                apply_delta(bundle_out.at(index), bundle.at(index));
            }
        }
    }  // namespace ts_data_detail

    Value capture_delta(const TSInputView &in)
    {
        if (const auto type = in.type_ref(); type) return type.ops_ref().capture_delta_impl(in);
        const auto &data = in.data_view();
        if (data.valid()) return data.ops().capture_delta_impl(in);
        static_cast<void>(require_schema(in.schema(), "capture_delta"));
        throw std::logic_error("capture_delta requires a canonical input type record");
    }

    Value capture_current_delta(const TSInputView &in)
    {
        static_cast<void>(require_schema(in.schema(), "capture_current_delta"));
        if (!in.valid())
        {
            throw std::invalid_argument("capture_current_delta requires a valid input");
        }
        const auto &ops = current_state_ops_for_input(in, "capture_current_delta");
        return ops.capture_current_delta_impl(in);
    }

    bool delta_is_observable(const TSInputView &in, const ValueView &delta)
    {
        static_cast<void>(require_schema(in.schema(), "delta_is_observable"));
        const auto &ops = current_state_ops_for_input(in, "delta_is_observable");
        return ops.delta_is_observable_impl(in, delta);
    }

    void apply_delta(const TSOutputView &out, const ValueView &delta)
    {
        // Trusted ops access: out.type_ref() replayed the full intern-time
        // record validation (capability recomputation included) per node per
        // child per tick — the audit's heaviest per-tick check. The record
        // was validated when interned; reach ops through the live data view,
        // as the python-bridge apply already does.
        const auto &data = out.data_view();
        if (!data.valid()) throw std::logic_error("apply_delta requires a canonical output type record");
        const auto &ops = data.ops();
        if (!ops.delta_has_effect_impl(out, delta)) { return; }
        ops.apply_delta_impl(out, delta);
    }

    bool current_value_schema_compatible(const TSValueTypeMetaData &schema,
                                         const ValueTypeMetaData   &value_schema)
    {
        const auto &ops = ts_current_state_detail::current_state_ops_for(schema.kind);
        return ops.value_schema_compatible_impl(schema, value_schema);
    }

    void apply_current_value(const TSOutputView &out, const ValueView &value)
    {
        const auto &schema = require_schema(out.schema(), "apply_current_value");
        if (!value.has_value())
        {
            throw std::invalid_argument("apply_current_value requires a live source value");
        }
        const auto *value_schema = value.schema();
        if (value_schema == nullptr || !current_value_schema_compatible(schema, *value_schema))
        {
            throw std::invalid_argument(fmt::format(
                "apply_current_value cannot apply source value '{}' to output '{}' (time-series kind {})",
                value_schema != nullptr ? value_schema->name() : std::string_view{"<unbound>"},
                schema.value_schema != nullptr ? schema.value_schema->name() : std::string_view{"<unbound>"},
                static_cast<std::size_t>(schema.kind)));
        }

        const auto &ops = current_state_ops(out.data_view(), "apply_current_value");
        ops.apply_current_value_impl(out, value);
    }

    namespace
    {
        template <typename Source>
        void validate_reconcile_source(const TSValueTypeMetaData &schema,
                                       const TSCurrentStateOps &ops,
                                       const Source &source)
        {
            const auto *source_schema = source.schema();
            // A default, non-live view is the contract's explicit absent
            // source. It carries no topology and asks the selected strategy
            // to clear/invalidate its target state.
            if (source_schema == nullptr && !source.valid()) { return; }
            if (source_schema == nullptr ||
                !ops.reconcile_schema_compatible_impl(schema, *source_schema))
            {
                throw std::invalid_argument(fmt::format(
                    "reconcile_current_state requires compatible source and target time-series topology "
                    "(target '{}', source '{}')",
                    schema.name(),
                    source_schema != nullptr ? source_schema->name()
                                             : std::string_view{"<unbound>"}));
            }
        }
    }

    void reconcile_current_state(const TSOutputView &target, const TSInputView &source,
                                 TSCurrentReconcileOptions options)
    {
        const auto &schema = require_schema(target.schema(), "reconcile_current_state");
        if (!target.bound())
        {
            throw std::invalid_argument("reconcile_current_state requires a bound target");
        }
        const auto &ops = current_state_ops(target.data_view(), "reconcile_current_state");
        validate_reconcile_source(schema, ops, source);
        ops.reconcile_input_impl(target, source, options);
    }

    void reconcile_current_state(const TSOutputView &target, const TSDataView &source,
                                 TSCurrentReconcileOptions options)
    {
        const auto &schema = require_schema(target.schema(), "reconcile_current_state");
        if (!target.bound())
        {
            throw std::invalid_argument("reconcile_current_state requires a bound target");
        }
        const auto &ops = current_state_ops(target.data_view(), "reconcile_current_state");
        validate_reconcile_source(schema, ops, source);
        ops.reconcile_data_impl(target, source, options);
    }
}  // namespace hgraph
