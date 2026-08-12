#include <hgraph/types/time_series/ts_delta.h>


#include <hgraph/types/metadata/ts_data_plan_factory.h>
#include <hgraph/types/metadata/type_realization.h>
#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/metadata/ts_value_type_meta_data.h>
#include <hgraph/types/metadata/value_plan_factory.h>
#include <hgraph/types/metadata/value_type_meta_data.h>
#include <hgraph/types/time_series/ts_input.h>
#include <hgraph/types/time_series/ts_output.h>
#include <hgraph/types/value/value.h>
#include <hgraph/types/value/value_builder.h>
#include <hgraph/types/value/value_view.h>

#include <ankerl/unordered_dense.h>
#include <fmt/format.h>

#include <array>
#include <cassert>
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

        [[nodiscard]] bool field_name_equal(const TSFieldMetaData &ts_field,
                                            const ValueFieldMetaData &value_field) noexcept
        {
            const std::string_view ts_name =
                ts_field.name != nullptr ? std::string_view{ts_field.name} : std::string_view{};
            const std::string_view value_name =
                value_field.name != nullptr ? std::string_view{value_field.name} : std::string_view{};
            return ts_name == value_name;
        }

        [[nodiscard]] bool current_value_schema_compatible_ts(const TSValueTypeMetaData &schema,
                                                              const ValueTypeMetaData   &value_schema) noexcept
        {
            if (schema.value_schema == &value_schema) { return true; }
            if (schema.value_schema != nullptr &&
                TypeRegistry::instance().bundle_is_a(&value_schema, schema.value_schema))
            {
                return true;
            }
            // Variadic tuple vs list: distinct schema identity, ONE layout
            // (same element type; flags differ only by VariadicTuple).
            const auto *target = schema.value_schema;
            return target != nullptr && target->try_value_kind() == ValueTypeKind::List &&
                   value_schema.try_value_kind() == ValueTypeKind::List && target->fixed_size == 0 &&
                   value_schema.fixed_size == 0 && !target->is_mutable() && !value_schema.is_mutable() &&
                   target->element_type == value_schema.element_type;
        }

        [[nodiscard]] bool current_value_schema_compatible_tss(const TSValueTypeMetaData &schema,
                                                               const ValueTypeMetaData   &value_schema) noexcept
        {
            return schema.value_schema == &value_schema;
        }

        [[nodiscard]] bool current_value_schema_compatible_tsd(const TSValueTypeMetaData &schema,
                                                               const ValueTypeMetaData   &value_schema)
        {
            return value_schema.value_kind() == ValueTypeKind::Map &&
                   schema.key_type() == value_schema.key_type &&
                   schema.element_ts() != nullptr &&
                   value_schema.element_type != nullptr &&
                   current_value_schema_compatible(*schema.element_ts(), *value_schema.element_type);
        }

        [[nodiscard]] bool current_value_schema_compatible_tsl(const TSValueTypeMetaData &schema,
                                                               const ValueTypeMetaData   &value_schema)
        {
            return value_schema.value_kind() == ValueTypeKind::List &&
                   schema.element_ts() != nullptr &&
                   value_schema.element_type != nullptr &&
                   (schema.fixed_size() == 0 || value_schema.fixed_size == 0 ||
                    schema.fixed_size() == value_schema.fixed_size) &&
                   current_value_schema_compatible(*schema.element_ts(), *value_schema.element_type);
        }

        [[nodiscard]] bool current_value_schema_compatible_tsb(const TSValueTypeMetaData &schema,
                                                               const ValueTypeMetaData   &value_schema)
        {
            if (value_schema.value_kind() != ValueTypeKind::Bundle ||
                schema.field_count() != value_schema.field_count)
            {
                return false;
            }
            for (std::size_t index = 0; index < schema.field_count(); ++index)
            {
                const auto &ts_field    = schema.fields()[index];
                const auto &value_field = value_schema.fields[index];
                if (!field_name_equal(ts_field, value_field) ||
                    ts_field.type == nullptr ||
                    value_field.type == nullptr ||
                    !current_value_schema_compatible(*ts_field.type, *value_field.type))
                {
                    return false;
                }
            }
            return true;
        }

        using CurrentValueSchemaCompatibleFn = bool (*)(const TSValueTypeMetaData &, const ValueTypeMetaData &);

        [[nodiscard]] constexpr std::size_t ts_kind_index(TSTypeKind kind) noexcept
        {
            return static_cast<std::size_t>(kind);
        }

        [[nodiscard]] CurrentValueSchemaCompatibleFn current_value_schema_compatible_for(
            TSTypeKind kind) noexcept
        {
            static constexpr std::size_t kind_count = ts_kind_index(TSTypeKind::SIGNAL) + 1U;
            static const std::array<CurrentValueSchemaCompatibleFn, kind_count> table{
                &current_value_schema_compatible_ts,
                &current_value_schema_compatible_tss,
                &current_value_schema_compatible_tsd,
                &current_value_schema_compatible_tsl,
                &current_value_schema_compatible_ts,
                &current_value_schema_compatible_tsb,
                &current_value_schema_compatible_ts,
                &current_value_schema_compatible_ts,
            };

            const auto index = ts_kind_index(kind);
            return index < table.size() ? table[index] : &current_value_schema_compatible_ts;
        }

        [[nodiscard]] bool schema_contains_tsd(const TSValueTypeMetaData *schema) noexcept
        {
            if (schema == nullptr) { return false; }
            if (schema->kind == TSTypeKind::TSD) { return true; }
            if (schema->kind == TSTypeKind::TSL) { return schema_contains_tsd(schema->element_ts()); }
            if (schema->kind == TSTypeKind::TSB)
            {
                for (std::size_t index = 0; index < schema->field_count(); ++index)
                {
                    if (schema_contains_tsd(schema->fields()[index].type)) { return true; }
                }
            }
            return false;
        }

        void apply_current_value_direct(const TSOutputView &out, const ValueView &value)
        {
            auto mutation = out.begin_mutation(out.evaluation_time());
            static_cast<void>(mutation.copy_value_from(value));
        }

        void apply_current_value_tsd(const TSOutputView &out, const ValueView &value)
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

        void apply_current_value_tsl(const TSOutputView &out, const ValueView &value)
        {
            const auto *schema = out.schema();
            if (schema == nullptr) { throw std::logic_error("apply_current_value: TSL output schema is missing"); }
            if (value.schema() == schema->value_schema && !schema_contains_tsd(schema))
            {
                apply_current_value_direct(out, value);
                return;
            }

            const auto source_values = value.as_indexed_view();
            if (schema->fixed_size() != 0 && source_values.size() != schema->fixed_size())
            {
                throw std::invalid_argument("apply_current_value: fixed TSL value has the wrong child count");
            }

            auto list_out = out.as_list();
            if (schema->fixed_size() == 0 && source_values.size() < list_out.size())
            {
                throw std::invalid_argument("apply_current_value: dynamic TSL current value cannot shrink");
            }

            for (std::size_t index = 0; index < source_values.size(); ++index)
            {
                auto source_child = source_values.at(index);
                // UNSET source children are skipped (the TSL carries the
                // same field-validity contract as TSB).
                if (!source_child.valid()) { continue; }
                apply_current_value(list_out.at(index), source_child);
            }
        }

        void apply_current_value_tsb(const TSOutputView &out, const ValueView &value)
        {
            const auto *schema = out.schema();
            if (schema == nullptr) { throw std::logic_error("apply_current_value: TSB output schema is missing"); }
            const auto source_values = value.as_indexed_view();
            if (source_values.size() != schema->field_count())
            {
                throw std::invalid_argument("apply_current_value: TSB value has the wrong field count");
            }

            auto bundle_out = out.as_bundle();
            for (std::size_t index = 0; index < source_values.size(); ++index)
            {
                auto source_child = source_values.at(index);
                // UNSET source fields are skipped (Bundle field validity):
                // applying a partially-valid value writes only its live
                // fields - never defaults.
                if (!source_child.valid()) { continue; }
                apply_current_value(bundle_out.at(index), source_child);
            }
        }

        using CurrentValueApplyFn = void (*)(const TSOutputView &, const ValueView &);

        [[nodiscard]] CurrentValueApplyFn current_value_apply_for(TSTypeKind kind) noexcept
        {
            static constexpr std::size_t kind_count = ts_kind_index(TSTypeKind::SIGNAL) + 1U;
            static const std::array<CurrentValueApplyFn, kind_count> table{
                &apply_current_value_direct,
                &apply_current_value_direct,
                &apply_current_value_tsd,
                &apply_current_value_tsl,
                &apply_current_value_direct,
                &apply_current_value_tsb,
                &apply_current_value_direct,
                &apply_current_value_direct,
            };

            const auto index = ts_kind_index(kind);
            return index < table.size() ? table[index] : &apply_current_value_direct;
        }
    }  // namespace

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

        [[nodiscard]] Value empty_delta_tsl(const TSRoleTypeRef &binding)
        {
            const auto *schema = binding.schema();
            if (schema == nullptr || schema->delta_value_schema == nullptr)
            {
                throw std::logic_error("empty_delta_tsl: schema is not resolved");
            }

            const ValueTypeMetaData *map_meta    = schema->delta_value_schema;
            const ValueTypeRef key_binding = binding_for(map_meta->key_type, "empty_delta_tsl");
            const ValueTypeRef val_binding = binding_for(map_meta->element_type, "empty_delta_tsl");
            MapBuilder               builder{key_binding, val_binding};
            return builder.build();
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
            return Value{in.value()};
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
            const ValueTypeMetaData *map_meta    = schema->delta_value_schema;
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
            return builder.build();
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

        bool delta_has_effect_tsl(const TSOutputView &, const ValueView &delta)
        {
            return delta.has_value() && delta.as_map().size() != 0;
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
            auto       list_out = out.as_list();
            const auto map      = delta.as_map();
            for (const auto &[key, child_delta] : map)
            {
                auto child = list_out.at(static_cast<std::size_t>(key.template checked_as<std::int64_t>()));
                apply_delta(child, child_delta);
            }
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
        const auto &schema = require_schema(in.schema(), "capture_current_delta");
        if (!in.valid())
        {
            throw std::invalid_argument("capture_current_delta requires a valid input");
        }

        switch (schema.kind)
        {
            case TSTypeKind::TS: return Value{in.value()};
            case TSTypeKind::SIGNAL: return Value{true};
            case TSTypeKind::TSS:
            {
                const ValueTypeRef element = binding_for(
                    schema.value_schema->element_type, "capture_current_delta");
                SetBuilder added{element};
                const auto set = in.as_set();
                for (const auto &value : set.values())
                {
                    const BorrowedOperand borrowed{element, value, "capture_current_delta"};
                    static_cast<void>(added.insert_copy(borrowed.data()));
                }
                SetBuilder removed{element};
                BundleBuilder bundle{
                    binding_for(schema.delta_value_schema, "capture_current_delta")};
                bundle.set("added", added.build());
                bundle.set("removed", removed.build());
                return bundle.build();
            }
            case TSTypeKind::TSD:
            {
                const ValueTypeRef key_binding = binding_for(
                    schema.key_type(), "capture_current_delta");
                const auto *element_schema = schema.element_ts();
                const ValueTypeRef delta_binding = binding_for(
                    element_schema->delta_value_schema, "capture_current_delta");

                SetBuilder removed{key_binding};
                MapBuilder modified{key_binding, delta_binding};
                const auto dict = in.as_dict();
                for (const auto &[key, child] : dict.valid_items())
                {
                    const BorrowedOperand borrowed{key_binding, key, "capture_current_delta"};
                    Value child_delta = capture_current_delta(child);
                    if (child_delta.binding() == delta_binding)
                    {
                        modified.set_item_copy(
                            borrowed.data(), child_delta.view().data());
                        continue;
                    }
                    Value stored_delta{delta_binding};
                    delta_binding.ops_ref().copy_assign_from(
                        delta_binding,
                        const_cast<void *>(stored_delta.view().data()),
                        child_delta.binding(),
                        child_delta.view().data());
                    modified.set_item_copy(
                        borrowed.data(), stored_delta.view().data());
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
            case TSTypeKind::TSL:
            {
                const ValueTypeRef key_binding = binding_for(
                    schema.delta_value_schema->key_type, "capture_current_delta");
                const ValueTypeRef delta_binding = binding_for(
                    schema.delta_value_schema->element_type, "capture_current_delta");
                MapBuilder builder{key_binding, delta_binding};
                const auto list = in.as_list();
                for (const auto &[index, child] : list.valid_items())
                {
                    const std::int64_t key = static_cast<std::int64_t>(index);
                    Value child_delta = capture_current_delta(child);
                    builder.set_item_copy(std::addressof(key), child_delta.view().data());
                }
                return builder.build();
            }
            case TSTypeKind::TSB:
            {
                const auto type = ts_type_for(&schema, "capture_current_delta");
                BundleBuilder builder{
                    binding_for(schema.delta_value_schema, "capture_current_delta")};
                initialize_tsb_delta_defaults(type, builder);
                auto bundle = in.as_bundle();
                for (std::size_t index = 0; index < bundle.size(); ++index)
                {
                    auto child = bundle.at(index);
                    if (child.valid())
                    {
                        builder.set(index, capture_current_delta(child));
                    }
                }
                return builder.build();
            }
            case TSTypeKind::TSW:
            case TSTypeKind::REF:
                if (in.modified()) { return capture_delta(in); }
                throw std::logic_error(
                    "capture_current_delta cannot reconstruct an unmodified TSW or REF");
        }
        throw std::logic_error("capture_current_delta received an unknown time-series kind");
    }

    void apply_delta(const TSOutputView &out, const ValueView &delta)
    {
        const auto type = out.type_ref();
        if (!type) throw std::logic_error("apply_delta requires a canonical output type record");
        const auto &ops = type.ops_ref();
        if (!ops.delta_has_effect_impl(out, delta)) { return; }
        ops.apply_delta_impl(out, delta);
    }

    bool current_value_schema_compatible(const TSValueTypeMetaData &schema,
                                         const ValueTypeMetaData   &value_schema)
    {
        return current_value_schema_compatible_for(schema.kind)(schema, value_schema);
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

        current_value_apply_for(schema.kind)(out, value);
    }
}  // namespace hgraph

