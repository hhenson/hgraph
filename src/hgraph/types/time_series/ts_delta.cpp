#include <hgraph/types/time_series/ts_delta.h>

#include "detail/ts_input_seams.h"
#include "../metadata/detail/window_change.h"
#include "../../runtime/window_removals.h"

#include <hgraph/types/time_series/endpoint_schema.h>

#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/metadata/type_realization.h>
#include <hgraph/types/metadata/ts_value_type_meta_data.h>
#include <hgraph/types/metadata/value_type_meta_data.h>
#include <hgraph/types/time_series/ts_input.h>
#include <hgraph/types/time_series/ts_data/impl/current_state_ops.h>
#include <hgraph/types/time_series/ts_output.h>
#include <hgraph/types/value/compact_container_ops.h>
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
        inline constexpr std::size_t tsw_delta_cleared         = 0;
        inline constexpr std::size_t tsw_delta_added           = 1;
        inline constexpr std::size_t tsw_delta_removed         = 2;
        inline constexpr std::size_t tsw_delta_replacement     = 3;
        inline constexpr std::size_t tsw_sample_value          = 0;
        inline constexpr std::size_t tsw_sample_time           = 1;
        inline constexpr std::size_t tss_delta_added           = 0;
        inline constexpr std::size_t tss_delta_removed         = 1;
        inline constexpr std::size_t tsl_delta_removed         = 0;
        inline constexpr std::size_t tsl_delta_modified        = 1;
        inline constexpr std::size_t tsl_delta_size            = 2;
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

        [[nodiscard]] const TSValueTypeMetaData &require_schema(const TSValueTypeMetaData *schema, const char *fn)
        {
            if (schema == nullptr) { throw std::logic_error(fmt::format("{}: view has no TSData schema", fn)); }
            return *schema;
        }

        /**
         * The portable delta type a layout records (``TSDataLayout::
         * canonical_delta_binding``, ABI 16): what a captured or empty delta
         * is built as. Read from the view's bound data, or from a type's own
         * layout; never resolved through the realization snapshot per tick
         * (the 2026-07-02 lock-free ruling).
         */
        [[nodiscard]] ValueTypeRef require_canonical_delta(ValueTypeRef binding, const TSValueTypeMetaData *schema,
                                                           const char *fn)
        {
            if (!binding) { throw std::logic_error(fmt::format("{}: layout records no canonical delta binding", fn)); }
            // A storage category (Owned, Shared) is a layout detail: an input
            // declared TS[X] may bind an output laid out as TS[Owned[X]], as
            // time_series_schema_equivalent allows, and its data then records
            // the output's canonical binding.
            if (schema != nullptr && binding.schema() != schema->delta_value_schema &&
                value_schema_without_storage(binding.schema()) != value_schema_without_storage(schema->delta_value_schema))
            {
                throw std::logic_error(fmt::format(
                    "{}: the layout's canonical delta binding ({}) is not the delta schema of {} ({})", fn,
                    binding.schema() != nullptr ? binding.schema()->name() : "<null>", schema->name(),
                    schema->delta_value_schema != nullptr ? schema->delta_value_schema->name() : "<null>"));
            }
            return binding;
        }

        [[nodiscard]] ValueTypeRef canonical_delta_binding(const TSInputView &in, const char *fn)
        {
            const auto data = in.input_data_view();
            if (!data.valid()) { throw std::logic_error(fmt::format("{}: input has no bound data", fn)); }
            return require_canonical_delta(data.layout().canonical_delta_binding, in.schema(), fn);
        }

        [[nodiscard]] ValueTypeRef canonical_delta_binding(const TSRoleTypeRef &type, const char *fn)
        {
            if (!type) { throw std::logic_error(fmt::format("{}: unresolved TSData type", fn)); }
            const auto &ops    = type.ops_ref();
            const auto *layout = ops.layout_impl(ops.context);
            if (layout == nullptr) { throw std::logic_error(fmt::format("{}: type has no layout", fn)); }
            return require_canonical_delta(layout->canonical_delta_binding, type.schema(), fn);
        }

        [[nodiscard]] Value empty_delta_value(const TSValueTypeMetaData &schema, ValueTypeRef binding);

        /** Assign an EMPTY compact set / map to bundle field ``index``: built
            through the builders (a default-constructed compact storage has no
            element binding), copied into the field, nothing interned. */
        void set_empty_surface(BundleBuilder &bundle, std::size_t index)
        {
            const auto field = bundle.field_binding(index);
            switch (field.ops_ref().kind)
            {
                case ValueOpsKind::Set:
                {
                    SetBuilder builder{compact_element_binding(field)};
                    SetStorage storage = builder.build_storage();
                    bundle.set(index, ValueView{field, &storage});
                    return;
                }
                case ValueOpsKind::Map:
                {
                    const auto [key, value] = compact_map_bindings(field);
                    MapBuilder builder{key, value};
                    MapStorage storage = builder.build_storage();
                    bundle.set(index, ValueView{field, &storage});
                    return;
                }
                default: throw std::logic_error("empty_delta: a delta surface is neither a set nor a map");
            }
        }

        /** The empty delta of ``schema`` built as ``binding`` (its canonical
            delta type): a keyed collection's surfaces are empty sets / maps,
            a TSL has no positional or size changes, a TSB has no present
            children, and an atomic delta is a typed null. */
        Value empty_delta_value(const TSValueTypeMetaData &schema, ValueTypeRef binding)
        {
            if (!binding) { throw std::logic_error("empty_delta: canonical delta binding is unresolved"); }
            switch (schema.kind)
            {
                case TSTypeKind::TSS:
                case TSTypeKind::TSD:
                {
                    BundleBuilder bundle{binding};
                    for (std::size_t index = 0; index < bundle.size(); ++index) { set_empty_surface(bundle, index); }
                    return bundle.build();
                }
                case TSTypeKind::TSL:
                {
                    BundleBuilder bundle{binding};
                    set_empty_surface(bundle, tsl_delta_removed);
                    set_empty_surface(bundle, tsl_delta_modified);
                    return bundle.build();
                }
                case TSTypeKind::TSB:
                {
                    BundleBuilder builder{binding};
                    return builder.build();
                }
                default: return Value::typed_null(binding);
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
            const auto data = input.input_data_view();
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
                   value_schema.try_value_kind() == ValueTypeKind::List && !target->is_fixed_size() &&
                   !value_schema.is_fixed_size() && !target->is_mutable() && !value_schema.is_mutable() &&
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
                (!schema.is_unbounded_tsl() && value_schema.is_fixed_size() &&
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

        void invalidate_target(const TSOutputView &target);
        template <bool Sample>
        void apply_boxed_current_value(const TSOutputView &out, const ValueView &value);

        template <bool Sample = false>
        void apply_current_value_dict(const TSOutputView &out, const ValueView &value)
        {
            auto       dict_out   = out.as_dict();
            auto       mutation   = dict_out.begin_mutation(out.evaluation_time());
            const auto source_map = value.as_map();

            if (source_map.empty() && dict_out.empty() && out.valid()) { return; }
            mutation.touch();
            for (const auto &[key, child_value] : source_map)
            {
                TSOutputView child{out.output(), mutation.at(key), out.evaluation_time()};
                // A present key can have no published child value (TS-19).
                if (child_value.has_value()) { apply_boxed_current_value<Sample>(child, child_value); }
                else { static_cast<void>(child.begin_mutation(out.evaluation_time()).invalidate()); }
            }

            std::vector<Value> removals;
            for (const auto key : mutation.keys())
            {
                if (!source_map.contains(key)) { removals.emplace_back(key); }
            }
            for (const auto &key : removals) { static_cast<void>(mutation.erase(key.view())); }
        }

        template <bool Sample = false>
        void apply_current_value_list(const TSOutputView &out, const ValueView &value)
        {
            const auto *schema = out.schema();
            if (schema == nullptr)
            {
                throw std::logic_error("apply_current_value: TSL output schema is missing");
            }
            const auto source_values = value.as_indexed_view();
            if (!schema->is_unbounded_tsl() && source_values.size() != schema->fixed_size())
            {
                throw std::invalid_argument("apply_current_value: fixed TSL value has the wrong child count");
            }

            auto list_out = out.as_list();
            // RFC 0031: a dynamic TSL current value IS the list, so a shorter
            // source truncates rather than being rejected.
            if (schema->is_unbounded_tsl())
            {
                list_out.resize(source_values.size());
            }

            for (std::size_t index = 0; index < source_values.size(); ++index)
            {
                auto source_child = source_values.at(index);
                if (source_child.valid()) { apply_boxed_current_value<Sample>(list_out.at(index), source_child); }
                else if constexpr (Sample) { invalidate_target(list_out.at(index)); }
            }
        }

        template <bool Sample = false>
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
                if (source_child.valid()) { apply_boxed_current_value<Sample>(bundle_out.at(index), source_child); }
                else if constexpr (Sample) { invalidate_target(bundle_out.at(index)); }
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
            BundleBuilder bundle{canonical_delta_binding(input, "capture_current_delta")};
            const auto added_type   = bundle.field_binding(tss_delta_added);
            const auto removed_type = bundle.field_binding(tss_delta_removed);
            const auto element      = compact_element_binding(added_type);
            SetBuilder added{element};
            const auto set = input.as_set();
            for (const auto &value : set.values())
            {
                const BorrowedOperand borrowed{element, value, "capture_current_delta"};
                static_cast<void>(added.insert_copy(borrowed.data()));
            }
            SetBuilder removed{element};
            SetStorage added_storage   = added.build_storage();
            SetStorage removed_storage = removed.build_storage();
            bundle.set(tss_delta_added, ValueView{added_type, &added_storage});
            bundle.set(tss_delta_removed, ValueView{removed_type, &removed_storage});
            return bundle.build();
        }

        [[nodiscard]] Value capture_current_dict(const TSInputView &input)
        {
            BundleBuilder bundle{canonical_delta_binding(input, "capture_current_delta")};
            const auto removed_type              = bundle.field_binding(tsd_delta_removed);
            const auto modified_type             = bundle.field_binding(tsd_delta_modified);
            const auto key_binding               = compact_element_binding(removed_type);
            const auto [map_key, delta_binding]  = compact_map_bindings(modified_type);
            if (map_key != key_binding)
            {
                throw std::logic_error("capture_current_delta: TSD delta key bindings disagree");
            }
            SetBuilder removed{key_binding};
            MapBuilder modified{key_binding, delta_binding};
            const auto dict = input.as_dict();
            for (const auto &[key, child] : dict.valid_items())
            {
                Value child_delta = capture_current_delta(child);
                modified.set_item(key, child_delta.view());
            }
            SetStorage removed_storage  = removed.build_storage();
            MapStorage modified_storage = modified.build_storage();
            bundle.set(tsd_delta_removed, ValueView{removed_type, &removed_storage});
            bundle.set(tsd_delta_modified, ValueView{modified_type, &modified_storage});
            return bundle.build();
        }

        [[nodiscard]] Value capture_current_list(const TSInputView &input)
        {
            const auto &schema = require_schema(input.schema(), "capture_current_delta");
            BundleBuilder bundle{canonical_delta_binding(input, "capture_current_delta")};
            set_empty_surface(bundle, tsl_delta_removed);
            const auto map_type = bundle.field_binding(tsl_delta_modified);
            const auto [key_binding, delta_binding] = compact_map_bindings(map_type);
            MapBuilder builder{key_binding, delta_binding};
            const auto list = input.as_list();
            for (const auto &[index, child] : list.valid_items())
            {
                const std::int64_t key = static_cast<std::int64_t>(index);
                Value child_delta = capture_current_delta(child);
                builder.set_item(ValueView{key_binding, std::addressof(key)}, child_delta.view());
            }
            MapStorage storage = builder.build_storage();
            bundle.set(tsl_delta_modified, ValueView{map_type, &storage});
            if (schema.is_unbounded_tsl()) { bundle.set(tsl_delta_size, Value{static_cast<Int>(list.size())}); }
            return bundle.build();
        }

        [[nodiscard]] Value capture_current_bundle(const TSInputView &input)
        {
            BundleBuilder builder{canonical_delta_binding(input, "capture_current_delta")};
            auto bundle = input.as_bundle();
            for (std::size_t index = 0; index < bundle.size(); ++index)
            {
                auto child = bundle.at(index);
                if (child.valid()) { builder.set(index, capture_current_delta(child)); }
            }
            return builder.build();
        }

        void set_window_samples(BundleBuilder &delta, std::size_t field,
                                Range<ValueView> values, Range<DateTime> times)
        {
            const auto list_type = delta.field_binding(field);
            const auto sample_type = compact_element_binding(list_type);
            ListBuilder samples{sample_type};
            auto time = times.begin();
            for (const auto value : values)
            {
                BundleBuilder sample{sample_type};
                sample.set(tsw_sample_value, value);
                sample.set(tsw_sample_time, Value{*time});
                samples.push_back(sample.build());
                ++time;
            }
            auto storage = samples.build_storage();
            delta.set(field, ValueView{list_type, &storage});
        }

        Value build_window_delta(const TSWDataView &window, DateTime now, bool snapshot)
        {
            const bool cleared = !snapshot && window.cleared(now);
            auto arrival = snapshot ? ValueView{} : window.delta_value(now);
            const auto samples = snapshot ? window.values() : window.removed_values(now);
            // An observer can still hold an old view after the root has closed
            // the event. An empty fabricated change would be unsafe to replay.
            if (!snapshot && !cleared && !arrival.has_value() && samples.limit == 0)
            {
                const auto data = window.base();
                if (window_removals_closed(data.parent_link(), now)) { return {}; }
                const auto &ops = static_cast<const TSWDataOps &>(data.ops());
                if (data.last_modified_time() == now && ops.change_view_impl)
                {
                    const auto change = ops.change_view_impl(ops.context, data.data(), now, false);
                    return change.has_value() ? Value{change} : Value{};
                }
            }
            return ts_data_detail::build_window_change(
                window.base().layout().canonical_delta_binding, cleared, std::move(arrival), snapshot, samples,
                snapshot ? window.value_times() : window.removed_value_times(now));
        }

        Value capture_current_window(const TSInputView &input)
        {
            return build_window_delta(input.as_window().data_view(), input.evaluation_time(), true);
        }

        [[nodiscard]] Value capture_current_transient(const TSInputView &input)
        {
            if (input.modified()) { return capture_delta(input); }
            throw std::logic_error(
                "capture_current_delta cannot reconstruct an unmodified REF");
        }

        [[nodiscard]] bool observable_atomic(const TSInputView &input,
                                             const ValueView &delta)
        {
            return input.modified() && input.valid() && delta.has_value();
        }

        [[nodiscard]] bool observable_window(const TSInputView &input,
                                             const ValueView &delta)
        {
            if (!input.modified() || !delta.has_value()) { return false; }
            const auto fields = delta.as_bundle();
            return fields.at(tsw_delta_replacement).has_value() || fields.at(tsw_delta_cleared).checked_as<bool>() ||
                   fields.at(tsw_delta_added).as_indexed_view().size() || fields.at(tsw_delta_removed).as_indexed_view().size();
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
            const auto bundle = delta.as_bundle();
            if (bundle.element_valid(tsl_delta_size) || !bundle.at(tsl_delta_removed).as_set().empty() ||
                !bundle.at(tsl_delta_modified).as_map().empty())
                return true;
            if (!input.valid()) return false;
            if (input.delta_is_sampled_rebind() || input.data_view().tracking().last_empty_delta_time == input.evaluation_time())
                return true;
            // A forwarded valid empty list can tick without a local structural window.
            return input.schema()->is_unbounded_tsl() &&
                   input.data_view().as_list().structural_delta(input.evaluation_time()).time == MIN_DT;
        }

        [[nodiscard]] bool observable_bundle(const TSInputView &input,
                                             const ValueView &delta)
        {
            if (!input.modified() || !delta.has_value()) { return false; }
            // A sampled input publishes the valid source's current state at
            // the link's time, even when its empty publication was earlier.
            if (input.valid() && (input.delta_is_sampled_rebind() ||
                input.data_view().tracking().last_empty_delta_time == input.evaluation_time())) { return true; }
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
            return options.scope != TSCurrentReconcileScope::Incremental || options.sample_all ||
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
        void reconcile_window_impl(const TSOutputView &target, const Source &source,
                                   TSCurrentReconcileOptions options)
        {
            if (!should_visit_source(source, target, options)) { return; }
            if (!source_live(source))
            {
                if (options.scope == TSCurrentReconcileScope::Full) { invalidate_target(target); }
                return;
            }

            const auto incoming = [&] {
                if constexpr (std::same_as<Source, TSInputView>) { return source.as_window().data_view(); }
                else { return source.as_window(); }
            }();
            const auto output = target.as_window();
            if (!options.sample_all && target.valid() && output.size() == incoming.size() &&
                target.value().equals(incoming.value()))
            {
                bool same_times = true;
                for (std::size_t i = 0; i < output.size(); ++i)
                {
                    if (output.time_at(i) != incoming.time_at(i)) { same_times = false; break; }
                }
                if (same_times) { return; }
            }

            std::vector<DateTime> times;
            times.reserve(incoming.size());
            for (DateTime time : incoming.value_times()) { times.push_back(time); }
            output.begin_mutation(target.evaluation_time()).replace_samples(incoming.value(), times);
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
            const TSCurrentReconcileOptions child_options{
                options.scope == TSCurrentReconcileScope::Sample ? TSCurrentReconcileScope::Sample
                                                                : TSCurrentReconcileScope::Full,
                options.sample_all, options.membership};

            if (options.scope != TSCurrentReconcileScope::Incremental)
            {
                std::vector<Value> removals;
                for (const auto key : target_dict.keys())
                {
                    const bool keep = options.membership ? source_dict.contains(key)
                                                         : source_child_live(source_dict.at(key));
                    if (!keep) { removals.emplace_back(key); }
                }
                for (const auto &key : removals)
                {
                    static_cast<void>(mutation.erase(key.view()));
                }
                if (options.membership)
                {
                    for (const auto key : source_dict.keys())
                    {
                        const auto source_child = source_dict.at(key);
                        TSOutputView target_child{target.output(), mutation.at(key), target.evaluation_time()};
                        if (source_child_live(source_child))
                        {
                            reconcile_current(target_child, source_child, child_options);
                        }
                        else { invalidate_target(target_child); }
                    }
                    return;
                }
                for (auto &&[key, source_child] : source_dict.valid_items())
                {
                    auto target_child = mutation.at(key);
                    reconcile_current(
                        TSOutputView{target.output(), target_child, target.evaluation_time()},
                        source_child,
                        child_options);
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
                if (options.membership)
                {
                    // A key joins whether or not its child has a value (TS-19).
                    for (const auto key : source_dict.added_keys())
                    {
                        static_cast<void>(mutation.at(key));
                    }
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
                if (!source_child_live(source_child))
                {
                    // An exact mirror withdraws a child whose source child was
                    // withdrawn while its key stayed a member.
                    if (options.membership && target_dict.contains(key))
                    {
                        invalidate_target(TSOutputView{target.output(), mutation.at(key), target.evaluation_time()});
                    }
                    continue;
                }
                auto target_child = mutation.at(key);
                reconcile_current(
                    TSOutputView{target.output(), target_child, target.evaluation_time()},
                    source_child,
                    child_options);
            }
        }

        template <typename Source>
        void reconcile_indexed_impl(const TSOutputView &target, const Source &source,
                                    TSCurrentReconcileOptions options)
        {
            if (!should_visit_source(source, target, options)) { return; }
            const std::size_t target_size = target.data_view().indexed_child_count();
            const bool full = options.scope != TSCurrentReconcileScope::Incremental;

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
                if (options.scope == TSCurrentReconcileScope::Sample && !source_child_live(source_child))
                {
                    invalidate_target(target_child);
                }
                else { reconcile_current(target_child, source_child, options); }
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
                if (resizable) { mutation.mark_empty_delta(); }
                else if (options.sample_all) { mutation.mark_modified(); }
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

        template <auto Reconcile, bool RetainsSampleTimes, bool Structural = false, typename Source>
        void reconcile_or_sample(const TSOutputView &target, const Source &source, TSCurrentReconcileOptions options)
        {
            if (options.scope == TSCurrentReconcileScope::Sample)
            {
                if (!RetainsSampleTimes && !options.sample_all)
                {
                    // A plain sampled value already carries child validity in its boxed
                    // projection. Ordinary sparse writes keep their existing patch semantics.
                    apply_boxed_current_value<Structural>(target, source.value());
                    return;
                }
                // A window-bearing snapshot also copies the original observation times.
                // Keep Sample while descending through a structure so an invalid child is
                // withdrawn even when it is itself a collection with a live empty value.
                options = {Structural ? TSCurrentReconcileScope::Sample : TSCurrentReconcileScope::Full, true, true};
            }
            Reconcile(target, source, options);
        }

        [[nodiscard]] const TSCurrentStateOps &atomic_current_state_ops() noexcept
        {
            static const TSCurrentStateOps ops{
                &current_value_schema_compatible_atomic,
                &current_ts_schema_compatible_atomic,
                &capture_current_atomic,
                &apply_current_value_direct,
                &observable_atomic,
                &reconcile_or_sample<reconcile_atomic_impl<TSInputView>, false>,
                &reconcile_or_sample<reconcile_atomic_impl<TSDataView>, false>,
                /* captures_while_invalid: */ false,   // an atomic input has no value to capture once invalid
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
                &reconcile_or_sample<reconcile_atomic_impl<TSInputView>, false>,
                &reconcile_or_sample<reconcile_atomic_impl<TSDataView>, false>,
                /* captures_while_invalid: */ false,   // a signal carries no value at all
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
                &reconcile_or_sample<reconcile_atomic_impl<TSInputView>, false>,
                &reconcile_or_sample<reconcile_atomic_impl<TSDataView>, false>,
                /* captures_while_invalid: */ false,   // an unbound reference has nothing to capture
            };
            return ops;
        }

        [[nodiscard]] const TSCurrentStateOps &window_current_state_ops() noexcept
        {
            static const TSCurrentStateOps ops{
                &current_value_schema_compatible_atomic,
                &current_ts_schema_compatible_window,
                &capture_current_window,
                &apply_current_value_direct,
                &observable_window,
                &reconcile_or_sample<reconcile_window_impl<TSInputView>, true>,
                &reconcile_or_sample<reconcile_window_impl<TSDataView>, true>,
                /* captures_while_invalid: */ false,   // a cleared or partially warm window stays valid
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
                &reconcile_or_sample<reconcile_set_impl<TSInputView>, false>,
                &reconcile_or_sample<reconcile_set_impl<TSDataView>, false>,
                /* captures_while_invalid: */ true,   // an emptied TSS still owes the removals its link holds
            };
            return ops;
        }

        template <bool RetainsSampleTimes = false>
        [[nodiscard]] const TSCurrentStateOps &dict_current_state_ops() noexcept
        {
            static const TSCurrentStateOps ops{
                &current_value_schema_compatible_dict,
                &current_ts_schema_compatible_dict,
                &capture_current_dict,
                &apply_current_value_dict<false>,
                &observable_dict,
                &reconcile_or_sample<reconcile_dict_impl<TSInputView>, RetainsSampleTimes, true>,
                &reconcile_or_sample<reconcile_dict_impl<TSDataView>, RetainsSampleTimes, true>,
                /* captures_while_invalid: */ true,   // an emptied TSD still owes the removals its link holds
            };
            return ops;
        }

        template <bool RetainsSampleTimes = false>
        [[nodiscard]] const TSCurrentStateOps &list_current_state_ops() noexcept
        {
            static const TSCurrentStateOps ops{
                &current_value_schema_compatible_list,
                &current_ts_schema_compatible_list,
                &capture_current_list,
                &apply_current_value_list<false>,
                &observable_list,
                &reconcile_or_sample<reconcile_indexed_impl<TSInputView>, RetainsSampleTimes, true>,
                &reconcile_or_sample<reconcile_indexed_impl<TSDataView>, RetainsSampleTimes, true>,
                /* captures_while_invalid: */ false,   // a truncation is reported through the list's own delta, not a child capture
            };
            return ops;
        }

        template <bool RetainsSampleTimes = false>
        [[nodiscard]] const TSCurrentStateOps &bundle_current_state_ops() noexcept
        {
            static const TSCurrentStateOps ops{
                &current_value_schema_compatible_bundle,
                &current_ts_schema_compatible_bundle,
                &capture_current_bundle,
                &apply_current_value_bundle<false>,
                &observable_bundle,
                &reconcile_or_sample<reconcile_indexed_impl<TSInputView>, RetainsSampleTimes, true>,
                &reconcile_or_sample<reconcile_indexed_impl<TSDataView>, RetainsSampleTimes, true>,
                /* captures_while_invalid: */ false,   // a bundle's own capture already walks its children
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
                /* captures_while_invalid: */ false,   // no representation
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

        const TSCurrentStateOps &current_state_ops_for(const TSValueTypeMetaData &schema) noexcept
        {
            if (schema.contains_window())
            {
                switch (schema.kind)
                {
                    case TSTypeKind::TSB: return bundle_current_state_ops<true>();
                    case TSTypeKind::TSL: return list_current_state_ops<true>();
                    case TSTypeKind::TSD: return dict_current_state_ops<true>();
                    default: break;
                }
            }
            return current_state_ops_for(schema.kind);
        }
    }  // namespace ts_current_state_detail

    namespace ts_data_detail
    {
        [[nodiscard]] Value empty_delta_atomic(const TSRoleTypeRef &binding)
        {
            return Value::typed_null(canonical_delta_binding(binding, "empty_delta_atomic"));
        }

        [[nodiscard]] Value empty_delta_tss(const TSRoleTypeRef &binding)
        {
            return empty_delta_value(require_schema(binding.schema(), "empty_delta_tss"),
                                     canonical_delta_binding(binding, "empty_delta_tss"));
        }

        [[nodiscard]] Value empty_delta_tsd(const TSRoleTypeRef &binding)
        {
            return empty_delta_value(require_schema(binding.schema(), "empty_delta_tsd"),
                                     canonical_delta_binding(binding, "empty_delta_tsd"));
        }

        [[nodiscard]] Value empty_delta_tsl(const TSRoleTypeRef &binding)
        {
            return empty_delta_value(require_schema(binding.schema(), "empty_delta_tsl"),
                                     canonical_delta_binding(binding, "empty_delta_tsl"));
        }

        [[nodiscard]] Value empty_delta_tsb(const TSRoleTypeRef &binding)
        {
            return empty_delta_value(require_schema(binding.schema(), "empty_delta_tsb"),
                                     canonical_delta_binding(binding, "empty_delta_tsb"));
        }

        Value capture_delta_ts(const TSInputView &in)
        {
            const ValueView value = in.value();
            if (value.type())
            {
                auto binding = canonical_delta_binding(in, "capture_delta");
                // Bound across a storage category (an input declared TS[X] on
                // an output laid out as TS[Owned[X]]): the delta is the value
                // in the type the consumer declared, not the producer's storage
                // (require_canonical_delta). A wrapper holding an allocation
                // projects to it, so the view already has the consumer's type;
                // an empty wrapper (a null owner) has nothing to project and
                // crosses as a typed null of that type, resolved as the
                // no-value case below is.
                if (const auto *declared = in.schema(); declared != nullptr &&
                    binding.schema() != declared->delta_value_schema)
                {
                    const auto *viewed = value.binding().schema();
                    if (viewed != nullptr && viewed->is_indirect()) { return Value{*declared->delta_value_schema}; }
                    return Value{value};
                }
                if (value.binding() == binding) { return Value{value}; }
                if (!binding.ops_ref().accepts_source(binding, value.binding()))
                {
                    const auto realized = value_type_for_active_realization(binding.schema());
                    if (!realized || !realized.ops_ref().accepts_source(realized, value.binding()))
                    {
                        throw std::logic_error(fmt::format(
                            "capture_delta: neither canonical nor realized binding for {} accepts {}",
                            binding.schema()->name(), value.binding().schema()->name()));
                    }
                    binding = realized;
                }
                return Value{binding, value};
            }

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
            if (in.delta_is_sampled_rebind()) { return capture_current_window(in); }
            return capture_window_delta(in.as_window().data_view(), in.evaluation_time());
        }

        Value capture_delta_tss(const TSInputView &in)
        {
            BundleBuilder bundle{canonical_delta_binding(in, "capture_delta")};
            const auto added_type   = bundle.field_binding(tss_delta_added);
            const auto removed_type = bundle.field_binding(tss_delta_removed);
            const auto elem_binding = compact_element_binding(added_type);

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

            SetStorage added_storage   = added.build_storage();
            SetStorage removed_storage = removed.build_storage();
            bundle.set(tss_delta_added, ValueView{added_type, &added_storage});
            bundle.set(tss_delta_removed, ValueView{removed_type, &removed_storage});
            return bundle.build();
        }

        // A vacant list can still have changed its extent or lost a position.
        // Its containing delta carries that structural edit without publishing a
        // value on the list. A detached reference has no owned list to capture.
        [[nodiscard]] bool has_list_edit(const TSInputView &child)
        {
            const auto &data = child.data_view();
            return data.valid() && data.ops().kind == TSTypeKind::TSL &&
                   child.evaluation_time() != MIN_DT && data.tracking().last_modified_time == child.evaluation_time();
        }

        /** A child that reads invalid is not always silent: a keyed
            collection that lost its reference still owes its consumer the
            removals its link holds (``linking_strategies.rst``, keyed
            structural unbind). Ask the child's own representation before
            skipping it, rather than assuming invalid means no news. */
        [[nodiscard]] bool child_delta_worth_capturing(const TSInputView &child)
        {
            if (child.valid() || has_list_edit(child)) { return true; }
            // Reaching the policy is itself conditional. A child with no
            // canonical type record cannot answer -- and an invalid child is
            // exactly the case where the data view has no ops to fall back on,
            // so current_state_ops_for_input would raise rather than reply.
            // An unanswerable child keeps the previous behaviour and is
            // skipped; only one that positively says it still owes removals is
            // captured.
            const auto type = child.type_ref();
            if (!type) { return false; }
            const auto *ops = type.as_role().ops_ref().current_state_ops;
            return ops != nullptr && ops->captures_while_invalid;
        }

        /**
         * The TSD capture, optionally restricted to the keys ``selects``
         * accepts.
         *
         * One implementation rather than two: a ``dmap_`` caller needs the
         * same delta as an ordinary capture, minus the keys another worker
         * owns, and a second copy of the binding and borrow handling below
         * would be free to drift from this one. A null selector is the
         * ordinary capture.
         *
         * The selector is a function pointer and a context, not a
         * std::function: this runs once per key per cycle, and the per-tick
         * path does not allocate (CLAUDE.md, single-threaded evaluation).
         */
        Value capture_delta_tsd_where(const TSInputView &in, DeltaKeySelector selects,
                                      const void *context)
        {
            BundleBuilder bundle{canonical_delta_binding(in, "capture_delta")};
            const auto removed_type             = bundle.field_binding(tsd_delta_removed);
            const auto modified_type            = bundle.field_binding(tsd_delta_modified);
            const auto key_binding              = compact_element_binding(removed_type);
            const auto [map_key, delta_binding] = compact_map_bindings(modified_type);
            if (map_key != key_binding) { throw std::logic_error("capture_delta_tsd: delta key bindings disagree"); }
            if (delta_binding.schema() != in.schema()->element_ts()->delta_value_schema)
            {
                throw std::logic_error("capture_delta_tsd resolved the wrong element delta binding");
            }

            const auto dict = in.as_dict();
            SetBuilder removed{key_binding};
            // A list-valued key can outlive the last published position of its
            // child. Removing that key still changes membership, even when
            // there is no longer a published child for the value-removal view.
            if (in.schema()->element_ts()->kind == TSTypeKind::TSL && dict.structure_modified()) {
                const auto data = dict.data_view();
                for (auto slot = data.next_membership_removed_slot(); slot != TS_DATA_NO_CHILD_ID;
                     slot = data.next_membership_removed_slot(slot)) {
                    if (data.slot_removed(slot)) { continue; }  // Included in the value removals below.
                    const auto key = data.key_at_slot(slot);
                    if (selects != nullptr && !selects(context, key)) { continue; }
                    const BorrowedOperand borrowed{key_binding, key, "capture_delta"};
                    (void)removed.insert_copy(borrowed.data());
                }
            }
            const auto removed_keys = dict.structure_modified() ? dict.data_view().removed_keys() : Range<ValueView>{};
            for (const auto &key : removed_keys)
            {
                if (selects != nullptr && !selects(context, key)) { continue; }
                // A vacant list can retain this key and its addressable positions.
                // Encode the edit under the key; a membership removal still erases it.
                if (in.schema()->element_ts()->kind == TSTypeKind::TSL &&
                    dict.contains(key) && has_list_edit(dict.at(key))) { continue; }
                const BorrowedOperand borrowed{key_binding, key, "capture_delta"};
                (void)removed.insert_copy(borrowed.data());
            }

            MapBuilder modified{key_binding, delta_binding};
            for (const auto &[key, child] : dict.modified_items())
            {
                if (selects != nullptr && !selects(context, key)) { continue; }
                if (!child_delta_worth_capturing(child)) { continue; }
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
            SetStorage removed_storage  = removed.build_storage();
            MapStorage modified_storage = modified.build_storage();
            bundle.set(tsd_delta_removed, ValueView{removed_type, &removed_storage});
            bundle.set(tsd_delta_modified, ValueView{modified_type, &modified_storage});
            return bundle.build();
        }

        Value capture_delta_tsd(const TSInputView &in)
        {
            return capture_delta_tsd_where(in, nullptr, nullptr);
        }

        Value capture_delta_tsl(const TSInputView &in)
        {
            const auto canonical = canonical_delta_binding(in, "capture_delta");
            const auto list      = in.as_list();

            BundleBuilder bundle{canonical};
            const auto removed_type  = bundle.field_binding(tsl_delta_removed);
            const auto modified_type = bundle.field_binding(tsl_delta_modified);
            const auto [key_binding, val_binding] = compact_map_bindings(modified_type);
            SetBuilder removed{compact_element_binding(removed_type)};
            for (const auto index : list.removed_positions()) {
                const auto key = static_cast<Int>(index);
                static_cast<void>(removed.insert_copy(&key));
            }

            MapBuilder builder{key_binding, val_binding};
            const auto capture_child = [&](std::size_t index, const TSInputView &child) {
                const auto key = static_cast<Int>(index);
                // A forwarded dictionary can lose its reference and still owe a child delta.
                // An explicit clear of the list position is already in the removed set.
                if (!child.valid() && (removed.contains(&key) || !child_delta_worth_capturing(child))) return;
                const Value captured = capture_delta(child);
                builder.set_item(ValueView{key_binding, &key}, captured.view());
            };
            if (in.schema()->element_ts()->kind == TSTypeKind::TSL) {
                for (const auto &[index, child] : list.items()) {
                    if (child.modified() || has_list_edit(child)) { capture_child(index, child); }
                }
            } else {
                for (const auto &[index, child] : list.modified_items()) { capture_child(index, child); }
            }
            SetStorage removed_storage  = removed.build_storage();
            MapStorage modified_storage = builder.build_storage();
            bundle.set(tsl_delta_removed, ValueView{removed_type, &removed_storage});
            bundle.set(tsl_delta_modified, ValueView{modified_type, &modified_storage});
            if (const auto size = list.size_modified()) { bundle.set(tsl_delta_size, Value{static_cast<Int>(*size)}); }
            return bundle.build();
        }

        Value capture_delta_tsb(const TSInputView &in)
        {
            const auto &schema = require_schema(in.schema(), "capture_delta");
            const auto canonical = canonical_delta_binding(in, "capture_delta");
            BundleBuilder builder{canonical};
            auto          bundle = in.as_bundle();
            std::vector<std::optional<Value>> captured(bundle.size());
            std::vector<ValueTypeRef> field_bindings;
            field_bindings.reserve(bundle.size());
            bool requires_realized_owner = false;
            for (std::size_t index = 0; index < bundle.size(); ++index)
            {
                field_bindings.push_back(builder.field_binding(index));
                auto child = bundle.at(index);
                if ((!child.modified() && !has_list_edit(child)) || !child_delta_worth_capturing(child)) { continue; }
                captured[index].emplace(capture_delta(child));
                const auto child_binding = captured[index]->binding();
                if (field_bindings[index] != child_binding)
                {
                    field_bindings[index] = child_binding;
                    requires_realized_owner = true;
                }
            }

            if (requires_realized_owner)
            {
                builder = BundleBuilder{
                    ValuePlanFactory::instance().realized_composite_type_for(
                        schema.delta_value_schema, field_bindings)};
            }
            for (std::size_t index = 0; index < captured.size(); ++index)
            {
                if (captured[index])
                {
                    builder.set(index, std::move(*captured[index]));
                }
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
            if (!out.valid()) { return true; }
            const auto set = out.as_set();
            const auto added = bundle.at(tss_delta_added).as_indexed_view();
            const auto removed = bundle.at(tss_delta_removed).as_indexed_view();
            for (std::size_t i = 0; i < added.size(); ++i)
                if (!set.contains(added.at(i))) { return true; }
            for (std::size_t i = 0; i < removed.size(); ++i)
                if (set.contains(removed.at(i))) { return true; }
            return false;
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

        struct ListPatch
        {
            std::optional<MapView>     modified;
            std::optional<SetView>     removed;
            std::optional<std::size_t> size;
            bool                       legacy_truncation{false};

            ListPatch(const ValueView &delta, const TSLOutputView &list) {
                // A retained pre-1815 fixed-list recording contains just the index map.
                if (delta.schema()->value_kind() == ValueTypeKind::Map) {
                    modified = delta.as_map();
                    return;
                }
                const auto bundle = delta.as_bundle();
                if (bundle.element_valid(tsl_delta_modified)) modified = bundle.at(tsl_delta_modified).as_map();
                if (bundle.element_valid(tsl_delta_removed)) removed = bundle.at(tsl_delta_removed).as_set();
                // Earlier dynamic recordings have a two-field schema and used removal for truncation.
                if (bundle.size() == 2) {
                    legacy_truncation = true;
                    if (removed) {
                        for (auto key : *removed) {
                            const auto index = key.checked_as<Int>();
                            if (index < 0) throw std::out_of_range("a TSL index must be non-negative");
                            if (static_cast<std::size_t>(index) < size.value_or(list.size())) size = index;
                        }
                    }
                    return;
                }
                if (bundle.element_valid(tsl_delta_size)) {
                    const auto length = bundle.at(tsl_delta_size).checked_as<Int>();
                    if (length < 0 || !list.schema()->is_unbounded_tsl()) throw std::invalid_argument("invalid TSL resize");
                    size = static_cast<std::size_t>(length);
                }
            }

            bool has_updates() const { return modified && !modified->empty(); }
            bool has_removals() const { return removed && !removed->empty(); }
        };

        bool delta_has_effect_tsl(const TSOutputView &out, const ValueView &delta) {
            if (!delta.has_value()) return false;
            auto            list = out.as_list();
            const ListPatch patch{delta, list};
            if (patch.has_updates() || (patch.size && *patch.size != list.size())) return true;
            if (patch.removed) {
                for (auto key : *patch.removed) {
                    const auto index = key.checked_as<Int>();
                    if (index < 0) throw std::out_of_range("a TSL index must be non-negative");
                    if (static_cast<std::size_t>(index) < list.size() && list.at(index).valid()) return true;
                }
            }
            return !patch.has_removals() && list.empty() && !out.valid();
        }

        bool delta_has_effect_tsb(const TSOutputView &out, const ValueView &delta)
        {
            if (!delta.has_value()) { return false; }

            auto       bundle_out = out.as_bundle();
            const auto bundle     = delta.as_bundle();
            bool has_entry = false;
            for (std::size_t index = 0; index < bundle.size(); ++index)
            {
                if (!bundle.element_valid(index)) { continue; }
                has_entry = true;
                auto        child     = bundle_out.at(index);
                const auto &child_ops = child.data_view().ops();
                if (child_ops.delta_has_effect_impl(child, bundle.at(index))) { return true; }
            }
            return !has_entry && !out.valid();
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
            auto window = out.as_window();
            auto mutation = window.begin_mutation(out.evaluation_time());
            if (delta.schema() == out.schema()->authored_delta_schema)
            {
                mutation.push(delta);
                return;
            }
            validate_window_delta(*out.schema(), delta);
            const auto fields = delta.as_bundle();
            if (fields.at(tsw_delta_replacement).has_value())
            {
                const auto snapshot = fields.at(tsw_delta_replacement).as_indexed_view();
                const auto added_type = fields.at(tsw_delta_added).binding();
                ListBuilder values{compact_element_binding(added_type)};
                std::vector<DateTime> times;
                times.reserve(snapshot.size());
                for (std::size_t i = 0; i < snapshot.size(); ++i)
                {
                    const auto sample = snapshot.at(i).as_bundle();
                    values.push_back(sample.at(tsw_sample_value));
                    times.push_back(sample.at(tsw_sample_time).checked_as<DateTime>());
                }
                auto storage = values.build_storage();
                mutation.replace_samples(ValueView{added_type, &storage}, times);
                return;
            }
            const auto added = fields.at(tsw_delta_added).as_indexed_view();
            const auto removed = fields.at(tsw_delta_removed).as_indexed_view();
            const bool cleared = fields.at(tsw_delta_cleared).checked_as<bool>();
            if (cleared || added.size() || removed.size())
                mutation.apply_delta(cleared, added.size() ? added.at(0) : ValueView{}, removed);
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

        template <auto ApplyChild>
        void apply_tsd_children(const TSOutputView &out, const ValueView &delta)
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
                ApplyChild(TSOutputView{out.output(), child, out.evaluation_time()}, child_delta);
            }
            // Existing child updates notify the parent themselves. An empty
            // patch validates only the parent, without creating child ticks.
            if (modified.size() == 0 && removed.size() == 0 && (ApplyChild == &apply_observed_delta || !out.valid())) {
                mutation.touch();
            }
        }

        template <auto ApplyChild>
        void apply_tsl_children(const TSOutputView &out, const ValueView &delta)
        {
            auto            list = out.as_list();
            const ListPatch patch{delta, list};
            // Check coordinates before changing the list or any earlier child.
            const auto limit =
                patch.legacy_truncation ? list.schema()->fixed_size() : patch.size.value_or(list.schema()->fixed_size());
            if (patch.modified) {
                for (auto [key, value] : *patch.modified) {
                    const auto i = key.checked_as<Int>();
                    if (i < 0 || static_cast<std::size_t>(i) >= limit)
                        throw std::out_of_range("TSL update outside the declared size");
                    if (!patch.legacy_truncation && patch.removed && patch.removed->contains(key))
                        throw std::invalid_argument("a TSL position cannot be modified and removed together");
                }
            }
            if (patch.removed) {
                for (auto key : *patch.removed) {
                    const auto i = key.checked_as<Int>();
                    if (i < 0 || static_cast<std::size_t>(i) >= list.schema()->fixed_size())
                        throw std::out_of_range("TSL removal outside the declared size");
                }
            }
            if (patch.size) list.resize(*patch.size);
            if (patch.removed && !patch.legacy_truncation) {
                for (auto key : *patch.removed) {
                    const auto i = static_cast<std::size_t>(key.checked_as<Int>());
                    // A replayed shrink also names the tail already dropped by resize.
                    if (i < list.size()) static_cast<void>(list.at(i).begin_mutation(out.evaluation_time()).invalidate());
                }
            }
            if (patch.modified) {
                for (auto [key, value] : *patch.modified)
                    ApplyChild(list.at(static_cast<std::size_t>(key.checked_as<Int>())), value);
            }
            if (!patch.has_updates() && !patch.has_removals()) {
                if ((ApplyChild == &apply_observed_delta && out.valid()) ||
                    (list.empty() && !out.valid())) {
                    out.data_view().begin_mutation(out.evaluation_time()).mark_empty_delta();
                }
            }
        }

        template <auto ApplyChild>
        void apply_tsb_children(const TSOutputView &out, const ValueView &delta)
        {
            auto       bundle_out = out.as_bundle();
            const auto bundle     = delta.as_bundle();
            bool has_entry = false;
            for (std::size_t index = 0; index < bundle.size(); ++index)
            {
                if (!bundle.element_valid(index)) { continue; }
                has_entry = true;
                ApplyChild(bundle_out.at(index), bundle.at(index));
            }
            if (!has_entry && (ApplyChild == &apply_observed_delta || !out.valid()))
            {
                out.data_view().begin_mutation(out.evaluation_time()).mark_empty_delta();
            }
        }
        void apply_delta_tsd(const TSOutputView &out, const ValueView &delta) { apply_tsd_children<&apply_delta>(out, delta); }
        void apply_delta_tsl(const TSOutputView &out, const ValueView &delta) { apply_tsl_children<&apply_delta>(out, delta); }
        void apply_delta_tsb(const TSOutputView &out, const ValueView &delta) { apply_tsb_children<&apply_delta>(out, delta); }
    }  // namespace ts_data_detail

    Value capture_delta(const TSInputView &in)
    {
        if (const auto type = in.type_ref(); type) return type.ops_ref().capture_delta_impl(in);
        const auto data = in.input_data_view();
        if (data.valid()) return data.ops().capture_delta_impl(in);
        static_cast<void>(require_schema(in.schema(), "capture_delta"));
        throw std::logic_error("capture_delta requires a canonical input type record");
    }

    namespace
    {
        void forward_changes(const TSOutputView &out, const TSInputView &in);

        // Build the destination cursor only for a real change. A wide bundle
        // commonly forwards just one field in this evaluation.
        template <typename Destination>
        void forward_child(const TSInputView &in, Destination destination)
        {
            const auto &data = in.data_view();
            if (data.valid() && data.tracking().cleared_at(in.evaluation_time()) && !in.valid())
            {
                invalidate_target(destination());
                return;
            }
            if (!in.modified() && !ts_data_detail::has_list_edit(in)) { return; }
            if (ts_data_detail::child_delta_worth_capturing(in)) { forward_changes(destination(), in); }
            // A followed child can lose its designation. A detached keyed link
            // can still owe removals above; a lost scalar has no delta to copy.
            else if (!data.valid()) { invalidate_target(destination()); }
        }

        void forward_changes(const TSOutputView &out, const TSInputView &in)
        {
            const auto apply = out.data_view().ops().apply_delta_impl;
            if (apply == &ts_data_detail::apply_delta_tsb)
            {
                const auto bundle = in.as_bundle();
                const auto dest = out.as_bundle();
                for (std::size_t i = 0; i < bundle.size(); ++i)
                {
                    forward_child(bundle.at(i), [&] { return dest.at(i); });
                }
            }
            else if (apply == &ts_data_detail::apply_delta_tsd)
            {
                const auto dict = in.as_dict();
                const auto dest = out.as_dict();
                auto mutation = dest.begin_mutation(out.evaluation_time());
                if (dict.structure_modified())
                {
                    for (auto key : dict.removed_keys())
                    {
                        // A reopened filter can already have discarded this key.
                        if (dest.contains(key) && !dict.contains(key)) { static_cast<void>(mutation.erase(key)); }
                    }
                    for (auto key : dict.added_keys()) { static_cast<void>(mutation.at(key)); }
                }
                for (const auto &[key, child] : dict.modified_items())
                {
                    if (dict.contains(key))
                    {
                        forward_child(child, [&] { return TSOutputView{out.output(), mutation.at(key), out.evaluation_time()}; });
                    }
                }
            }
            else if (apply == &ts_data_detail::apply_delta_tsl)
            {
                const auto list = in.as_list();
                auto dest = out.as_list();
                if (const auto size = list.size_modified()) { dest.resize(*size); }
                for (const auto i : list.removed_positions())
                {
                    if (i < dest.size()) { invalidate_target(dest.at(i)); }
                }
                // An inner list may have changed extent without any live position.
                if (in.schema()->element_ts()->kind == TSTypeKind::TSL)
                {
                    for (const auto &[i, child] : list.items()) { forward_child(child, [&] { return dest.at(i); }); }
                }
                else
                {
                    for (const auto &[i, child] : list.modified_items()) { forward_child(child, [&] { return dest.at(i); }); }
                }
            }
            else
            {
                apply_delta(out, capture_delta(in).view());
                return;
            }
            // An empty first patch validates a collection. Changes with no
            // effect on an already valid destination keep its existing cadence.
            if (!out.valid() && in.valid() && in.modified())
            {
                out.data_view().begin_mutation(out.evaluation_time()).mark_empty_delta();
            }
        }
    }

    void forward_delta(const TSOutputView &out, const TSInputView &in)
    {
        const auto &schema = require_schema(out.schema(), "forward_delta");
        if (!out.bound() || in.schema() != &schema)
        {
            throw std::invalid_argument("forward_delta requires matching time-series endpoints and a bound output");
        }
        forward_changes(out, in);
    }

    Value capture_dict_delta_where(const TSInputView &in, DeltaKeySelector selects,
                                   const void *context)
    {
        const auto &schema = require_schema(in.schema(), "capture_dict_delta_where");
        if (schema.kind != TSTypeKind::TSD)
        {
            throw std::logic_error("capture_dict_delta_where requires a TSD input");
        }
        return ts_data_detail::capture_delta_tsd_where(in, selects, context);
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

    Value ts_data_detail::build_window_change(ValueTypeRef binding, bool cleared, ValueView arrival,
                                              bool replacement, Range<ValueView> samples, Range<DateTime> times)
    {
        BundleBuilder delta{binding};
        delta.set(tsw_delta_cleared, Value{cleared});
        const auto added_type = delta.field_binding(tsw_delta_added);
        ListBuilder added{compact_element_binding(added_type)};
        if (arrival.has_value()) { added.push_back(arrival); }
        auto added_storage = added.build_storage();
        delta.set(tsw_delta_added, ValueView{added_type, &added_storage});
        if (replacement)
        {
            set_window_samples(delta, tsw_delta_removed, {}, {});
            set_window_samples(delta, tsw_delta_replacement, samples, times);
        }
        else { set_window_samples(delta, tsw_delta_removed, samples, times); }
        return delta.build();
    }

    Value capture_window_delta(const TSWDataView &window, DateTime now)
    {
        const auto data = window.base();
        if (data.ops().is_target_link)
        {
            // A forwarding output can sample held history even when the immediate
            // input stayed bound. The inner link owns that sampling event.
            const auto &ops = static_cast<const TSWDataOps &>(data.ops());
            const auto change = ops.change_view_impl(ops.context, data.data(), now, false);
            if (change.has_value()) { return Value{change}; }
        }
        return build_window_delta(window, now, window.replaced(now));
    }

    void validate_window_delta(const TSValueTypeMetaData &schema, const ValueView &delta)
    {
        if (!delta.has_value() || delta.schema() != schema.delta_value_schema ||
            schema.kind != TSTypeKind::TSW)
            throw std::invalid_argument("window delta has the wrong schema");
        const auto fields = delta.as_bundle();
        if (!fields.at(tsw_delta_cleared).has_value() || !fields.at(tsw_delta_added).has_value() || !fields.at(tsw_delta_removed).has_value())
            throw std::invalid_argument("window delta requires cleared, added and removed fields");
        const auto added = fields.at(tsw_delta_added).as_indexed_view();
        if (added.size() > 1 || (added.size() == 1 && !added.at(0).has_value()))
            throw std::invalid_argument("window delta must carry zero or one live arrival");
        const auto removed = fields.at(tsw_delta_removed).as_indexed_view();
        const auto replacement = fields.at(tsw_delta_replacement);
        if (replacement.has_value() && (fields.at(tsw_delta_cleared).checked_as<bool>() || added.size() || removed.size()))
            throw std::invalid_argument("window replacement cannot contain incremental changes");
        const auto samples = fields.at(replacement.has_value() ? tsw_delta_replacement : tsw_delta_removed).as_indexed_view();
        if (replacement.has_value() && !schema.is_duration_based() && samples.size() > schema.period())
            throw std::invalid_argument("window replacement exceeds configured period");
        DateTime first = MIN_DT;
        DateTime previous = MIN_DT;
        for (std::size_t i = 0; i < samples.size(); ++i)
        {
            const auto entry = samples.at(i);
            if (!entry.has_value()) throw std::invalid_argument("window delta has an unset sample");
            const auto sample = entry.as_bundle();
            if (!sample.at(tsw_sample_value).has_value() || !sample.at(tsw_sample_time).has_value())
                throw std::invalid_argument("window sample requires a value and timestamp");
            const auto time = sample.at(tsw_sample_time).checked_as<DateTime>();
            if (time == MIN_DT || time < previous)
                throw std::invalid_argument("window sample timestamps must be valid and chronological");
            if (i == 0) { first = time; }
            previous = time;
        }
        if (replacement.has_value() && schema.is_duration_based() && samples.size() > 1 &&
            static_cast<std::uint64_t>(previous.time_since_epoch().count()) -
                static_cast<std::uint64_t>(first.time_since_epoch().count()) >
                static_cast<std::uint64_t>(schema.time_range().count()))
            throw std::invalid_argument("window replacement exceeds configured duration");
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

    void apply_observed_delta(const TSOutputView &out, const ValueView &delta)
    {
        const auto &data = out.data_view();
        if (!data.valid() || !delta.has_value()) { apply_delta(out, delta); return; }
        const auto &ops = data.ops();
        if (ops.is_target_link) {
            apply_observed_delta(ts_input_seams::target_link_delta_target(ops.context, out), delta);
            return;
        }
        // A structural capture names precisely the children that published. Recurse
        // only through the corresponding writable strategies; other roles retain
        // their registered behavior, including their validation and write errors.
        const auto apply = ops.apply_delta_impl;
        if (apply == &ts_data_detail::apply_delta_tss) {
            apply(out, delta);
        } else if (apply == &ts_data_detail::apply_delta_tsd) {
            ts_data_detail::apply_tsd_children<&apply_observed_delta>(out, delta);
        } else if (apply == &ts_data_detail::apply_delta_tsl) {
            ts_data_detail::apply_tsl_children<&apply_observed_delta>(out, delta);
        } else if (apply == &ts_data_detail::apply_delta_tsb) {
            ts_data_detail::apply_tsb_children<&apply_observed_delta>(out, delta);
        } else if (ops.delta_has_effect_impl(out, delta)) {
            apply(out, delta);
        }
    }

    bool current_value_schema_compatible(const TSValueTypeMetaData &schema,
                                         const ValueTypeMetaData   &value_schema)
    {
        const auto &ops = ts_current_state_detail::current_state_ops_for(schema.kind);
        return ops.value_schema_compatible_impl(schema, value_schema);
    }

    namespace
    {
        template <bool Sample>
        void apply_boxed_current_value(const TSOutputView &out, const ValueView &value)
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
            const auto apply = ops.apply_current_value_impl;
            if constexpr (Sample)
            {
                // Use the realized output's writer; only the standard structural writers
                // need a distinct policy for an unset field or position.
                if (apply == &apply_current_value_list<false>) { return apply_current_value_list<true>(out, value); }
                if (apply == &apply_current_value_bundle<false>) { return apply_current_value_bundle<true>(out, value); }
                if (apply == &apply_current_value_dict<false>) { return apply_current_value_dict<true>(out, value); }
            }
            apply(out, value);
        }
    }

    void apply_current_value(const TSOutputView &out, const ValueView &value)
    {
        apply_boxed_current_value<false>(out, value);
    }

    namespace
    {
        template <typename Source>
        void validate_reconcile_source(const TSValueTypeMetaData &schema,
                                       const TSCurrentStateOps &ops,
                                       const Source &source)
        {
            const auto *source_schema = source.schema();
            // Interned identical topologies need no recursive compatibility walk.
            if (source_schema == &schema) { return; }
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
