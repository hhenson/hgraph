#include <hgraph/python/ts_data_conversion.h>

#include <hgraph/python/bridge_state.h>
#include <hgraph/types/metadata/ts_data_plan_factory.h>
#include <hgraph/types/metadata/type_realization.h>
#include <hgraph/types/metadata/value_type_meta_data.h>
#include <hgraph/types/time_series/ts_data/dict_view.h>
#include <hgraph/types/time_series/ts_data/ops.h>
#include <hgraph/types/time_series/ts_data/set_view.h>
#include <hgraph/types/time_series/ts_delta.h>
#include <hgraph/types/time_series/ts_output.h>
#include <hgraph/types/time_series_reference.h>
#include <hgraph/types/value/value.h>
#include <hgraph/types/value/value_builder.h>

#include <nanobind/stl/string.h>

#include <cstdint>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <vector>

namespace hgraph::python_bridge
{
    namespace
    {
        [[nodiscard]] const TSValueTypeMetaData &schema_of(TSRoleTypeRef type,
                                                            const char *operation)
        {
            if (!type || type.schema() == nullptr)
            {
                throw std::logic_error(std::string{operation} +
                                       ": unresolved time-series type");
            }
            return *type.schema();
        }

        [[nodiscard]] const TSDataLayout &layout_of(TSRoleTypeRef type,
                                                    const char *operation)
        {
            const auto &ops = type.ops_ref();
            const auto *layout = ops.layout_impl(ops.context);
            if (layout == nullptr)
            {
                throw std::logic_error(std::string{operation} +
                                       ": unresolved time-series layout");
            }
            return *layout;
        }

        [[nodiscard]] ValueTypeRef binding_for(const ValueTypeMetaData *schema,
                                               const char *operation)
        {
            const auto binding = value_type_for_active_realization(schema);
            if (!binding)
            {
                throw std::logic_error(std::string{operation} +
                                       ": unresolved value binding");
            }
            return binding;
        }

        [[nodiscard]] Value value_from_python(nb::handle source,
                                              const ValueTypeMetaData *schema)
        {
            const auto convert = py_value_from_schema_slot();
            if (convert == nullptr)
            {
                throw std::logic_error(
                    "target-directed Python value conversion is not installed");
            }
            return convert(source, schema);
        }

        [[nodiscard]] const PythonTSDataOps &python_ops_for(TSRoleTypeRef type)
        {
            // TSDataOps binds either a concrete strategy or the canonical
            // throwing table. A null check here would weaken that invariant.
            return *type.ops_ref().python_ops;
        }

        [[nodiscard]] bool requires_authored(TSRoleTypeRef type,
                                             nb::handle source)
        {
            return python_ops_for(type)
                .requires_authored_delta_impl(type, source);
        }

        [[nodiscard]] Value build_delta(TSRoleTypeRef type,
                                        nb::handle source,
                                        bool authored)
        {
            return python_ops_for(type)
                .delta_from_python_impl(type, source, authored);
        }

        [[nodiscard]] bool never_requires_authored(TSRoleTypeRef,
                                                   nb::handle)
        {
            return false;
        }

        [[nodiscard]] bool missing_requires_authored(TSRoleTypeRef,
                                                     nb::handle)
        {
            throw std::logic_error(
                "time-series representation has no Python authored-delta strategy");
        }

        [[nodiscard]] Value missing_delta_from_python(TSRoleTypeRef,
                                                      nb::handle,
                                                      bool)
        {
            throw std::logic_error(
                "time-series representation has no Python authored-delta strategy");
        }

        void missing_apply_result(const TSOutputView &output, nb::handle)
        {
            const auto type = output.storage_type();
            const auto implementation =
                type.record() != nullptr ? type.record()->implementation_name()
                                         : std::string_view{};
            throw std::logic_error(
                "time-series representation '" + std::string{implementation} +
                "' has no Python result strategy");
        }

        [[nodiscard]] Value atomic_delta_from_python(TSRoleTypeRef type,
                                                     nb::handle source,
                                                     bool)
        {
            return value_from_python(source,
                                     schema_of(type, "atomic delta_from_python")
                                         .value_schema);
        }

        [[nodiscard]] Value window_delta_from_python(TSRoleTypeRef type,
                                                     nb::handle source,
                                                     bool)
        {
            return value_from_python(
                source,
                schema_of(type, "TSW delta_from_python").delta_value_schema);
        }

        void apply_replacement_result(const TSOutputView &output,
                                      nb::handle result)
        {
            static_cast<void>(
                output.begin_mutation(output.evaluation_time()).from_python(result));
        }

        void apply_ref_result(const TSOutputView &output, nb::handle result)
        {
            Value raw = value_from_python(
                result, schema_of(output.storage_type(), "REF result").value_schema);
            TimeSeriesReference reference =
                raw.view().checked_as<TimeSeriesReference>();
            if (output.data_view().has_current_value() &&
                output.data_view().value().checked_as<TimeSeriesReference>() ==
                    reference)
            {
                return;
            }
            auto mutation = output.begin_mutation(output.evaluation_time());
            static_cast<void>(
                mutation.move_value_from(Value{std::move(reference)}));
        }

        [[nodiscard]] bool is_removed_marker(nb::handle item)
        {
            return removed_class_slot().is_valid()
                       ? nb::isinstance(item, removed_class_slot())
                       : (nb::hasattr(item, "item") &&
                          nb::cast<std::string>(item.type().attr("__name__")) ==
                              "Removed");
        }

        [[nodiscard]] Value set_delta_from_parts(TSRoleTypeRef type,
                                                 nb::handle add_from,
                                                 nb::handle remove_from)
        {
            const auto &schema = schema_of(type, "TSS delta_from_python");
            const auto element_binding = binding_for(
                schema.value_schema->element_type, "TSS element");
            SetBuilder added{element_binding};
            SetBuilder removed{element_binding};

            if (add_from.is_valid())
            {
                for (nb::handle item : add_from)
                {
                    if (is_removed_marker(item))
                    {
                        Value value = value_from_python(
                            item.attr("item"), schema.value_schema->element_type);
                        static_cast<void>(removed.insert(value.view()));
                    }
                    else
                    {
                        Value value = value_from_python(
                            item, schema.value_schema->element_type);
                        static_cast<void>(added.insert(value.view()));
                    }
                }
            }
            if (remove_from.is_valid())
            {
                for (nb::handle item : remove_from)
                {
                    Value value = value_from_python(
                        item, schema.value_schema->element_type);
                    static_cast<void>(removed.insert(value.view()));
                }
            }

            BundleBuilder bundle{binding_for(
                schema_of(type, "TSS delta_from_python").delta_value_schema,
                "TSS delta_from_python")};
            bundle.set("added", added.build());
            bundle.set("removed", removed.build());
            return bundle.build();
        }

        [[nodiscard]] Value set_delta_from_python(TSRoleTypeRef type,
                                                  nb::handle source,
                                                  bool)
        {
            if (nb::isinstance<nb::dict>(source) && nb::len(source) != 0)
            {
                throw nb::type_error(
                    "a dict is not a TSS value/delta: use a set, set_delta(...), "
                    "or Removed(...) markers ({} is accepted as the empty set)");
            }
            if (set_delta_class_slot().is_valid() &&
                nb::isinstance(source, set_delta_class_slot()))
            {
                return set_delta_from_parts(type, source.attr("added"),
                                            source.attr("removed"));
            }
            return set_delta_from_parts(type, source, nb::handle{});
        }

        void apply_set_result(const TSOutputView &output, nb::handle result)
        {
            if (nb::isinstance<nb::dict>(result) && nb::len(result) != 0)
            {
                throw nb::type_error(
                    "a dict is not a TSS value/delta: use a set, set_delta(...), "
                    "or Removed(...) markers ({} is accepted as the empty set)");
            }
            const auto &layout = static_cast<const TSSDataLayout &>(
                layout_of(output.storage_type(), "TSS apply result"));
            auto set_out = output.as_set();

            // The per-tick path: element conversion binds through the layout's
            // wiring-resolved key binding — no registry access, no builders.
            std::vector<Value> to_add;
            std::vector<Value> to_remove;
            const auto convert = [&](nb::handle item) {
                Value value{layout.key_binding};
                value.view().assign_from_python(item);
                return value;
            };

            if (PyFrozenSet_CheckExact(result.ptr()) != 0)
            {
                // Class identity selects semantics: a frozenset is the FULL
                // membership, so additions are result-minus-current and
                // removals current-minus-result.
                for (nb::handle item : result)
                {
                    Value value = convert(item);
                    if (!set_out.contains(value.view()))
                    {
                        to_add.push_back(std::move(value));
                    }
                }
                for (const auto &key : set_out.values())
                {
                    const nb::object obj = key.to_python();
                    const int held = PySet_Contains(result.ptr(), obj.ptr());
                    if (held == -1) { throw nb::python_error(); }
                    if (held == 0) { to_remove.push_back(convert(obj)); }
                }
            }
            else
            {
                nb::handle add_from = result;
                nb::handle remove_from{};
                nb::object added_part;
                nb::object removed_part;
                if (set_delta_class_slot().is_valid() &&
                    nb::isinstance(result, set_delta_class_slot()))
                {
                    added_part = result.attr("added");
                    removed_part = result.attr("removed");
                    add_from = added_part;
                    remove_from = removed_part;
                }
                if (add_from.is_valid())
                {
                    for (nb::handle item : add_from)
                    {
                        if (is_removed_marker(item))
                        {
                            to_remove.push_back(convert(item.attr("item")));
                        }
                        else { to_add.push_back(convert(item)); }
                    }
                }
                if (remove_from.is_valid())
                {
                    for (nb::handle item : remove_from)
                    {
                        to_remove.push_back(convert(item));
                    }
                }
            }

            // The has-effect gate (was set_delta_changes_valid_output): a
            // valid set only ticks when membership actually changes; a fresh
            // one validates on any result, including an explicitly empty one.
            bool changes = !output.valid();
            for (auto it = to_add.begin(); !changes && it != to_add.end(); ++it)
            {
                changes = !set_out.contains(it->view());
            }
            for (auto it = to_remove.begin(); !changes && it != to_remove.end();
                 ++it)
            {
                changes = set_out.contains(it->view());
            }
            if (!changes) { return; }

            auto mutation = set_out.begin_mutation(output.evaluation_time());
            for (const auto &value : to_remove)
            {
                static_cast<void>(mutation.remove(value.view()));
            }
            for (const auto &value : to_add)
            {
                static_cast<void>(mutation.add(value.view()));
            }
            // Touch LAST (the empty-tick validation rule, as apply_delta_tss).
            mutation.touch();
        }

        [[nodiscard]] bool dict_requires_authored(TSRoleTypeRef type,
                                                  nb::handle source)
        {
            if (!nb::isinstance<nb::dict>(source)) { return false; }
            const auto &layout = static_cast<const TSDDataLayout &>(
                layout_of(type, "TSD authored-delta query"));
            for (auto [key, item] : nb::cast<nb::dict>(source))
            {
                static_cast<void>(key);
                if (item.is_none()) { continue; }
                if (removed_sentinel_slot().is_valid() &&
                    item.is(removed_sentinel_slot()))
                {
                    return true;
                }
                if (remove_if_exists_sentinel_slot().is_valid() &&
                    item.is(remove_if_exists_sentinel_slot()))
                {
                    continue;
                }
                if (requires_authored(layout.element_type, item)) { return true; }
            }
            return false;
        }

        [[nodiscard]] Value dict_delta_from_python(TSRoleTypeRef type,
                                                   nb::handle source,
                                                   bool authored)
        {
            if (!nb::isinstance<nb::dict>(source))
            {
                throw nb::type_error("a TSD delta must be a mapping");
            }
            const auto &schema = schema_of(type, "TSD delta_from_python");
            const auto &layout = static_cast<const TSDDataLayout &>(
                layout_of(type, "TSD delta_from_python"));
            const auto key_binding =
                binding_for(schema.key_type(), "TSD key delta");
            SetBuilder removed{key_binding};
            SetBuilder removed_strict{key_binding};
            const auto *child_delta_schema =
                authored ? layout.element_type.schema()->authored_delta_schema
                         : layout.element_type.schema()->delta_value_schema;
            MapBuilder modified{key_binding,
                                binding_for(child_delta_schema,
                                            "TSD child delta")};

            bool saw_item = false;
            bool saw_non_none = false;
            for (auto [key, item] : nb::cast<nb::dict>(source))
            {
                saw_item = true;
                if (item.is_none()) { continue; }
                saw_non_none = true;
                Value key_value =
                    value_from_python(key, schema.key_type());
                const bool strict_remove =
                    removed_sentinel_slot().is_valid() &&
                    item.is(removed_sentinel_slot());
                const bool lenient_remove =
                    remove_if_exists_sentinel_slot().is_valid() &&
                    item.is(remove_if_exists_sentinel_slot());
                if (strict_remove && !authored)
                {
                    throw std::logic_error(
                        "TSD strict removal requires an authored delta schema");
                }
                if (strict_remove)
                {
                    static_cast<void>(removed_strict.insert(key_value.view()));
                }
                else if (lenient_remove)
                {
                    static_cast<void>(removed.insert(key_value.view()));
                }
                else
                {
                    Value child = build_delta(layout.element_type, item, authored);
                    if (child.has_value())
                    {
                        modified.set_item(key_value.view(), child.view());
                    }
                }
            }
            if (saw_item && !saw_non_none)
            {
                return Value{*schema.delta_value_schema};
            }

            BundleBuilder bundle{binding_for(
                authored ? schema.authored_delta_schema
                         : schema.delta_value_schema,
                "TSD delta_from_python")};
            bundle.set("removed", removed.build());
            bundle.set("modified", modified.build());
            if (authored)
            {
                bundle.set("removed_strict", removed_strict.build());
            }
            return bundle.build();
        }

        void apply_dict_result(const TSOutputView &output, nb::handle result)
        {
            if (!nb::isinstance<nb::dict>(result))
            {
                throw nb::type_error("a TSD delta must be a mapping");
            }
            const auto type = output.storage_type();
            const auto &layout = static_cast<const TSDDataLayout &>(
                layout_of(type, "TSD apply result"));
            auto dict_out = output.as_dict();
            const auto evaluation_time = output.evaluation_time();

            const nb::dict source = nb::cast<nb::dict>(result);
            if (source.size() == 0)
            {
                // The empty-tick validation rule (see delta_has_effect_tsd):
                // {} validates a fresh TSD and is a no-op on a valid one.
                if (!output.valid())
                {
                    dict_out.begin_mutation(evaluation_time).touch();
                }
                return;
            }

            // The per-tick path: every type decision below was resolved when
            // the plan was built. The layout carries the key binding, the
            // element role carries the child ops, and the sentinels are
            // pointer identities — nothing here may consult a registry.
            const auto &child_ops = python_ops_for(layout.element_type);
            const nb::handle strict_sentinel = removed_sentinel_slot();
            const nb::handle lenient_sentinel = remove_if_exists_sentinel_slot();
            Value key_scratch{layout.key_binding};

            std::optional<TSDDataMutationView> mutation;
            const auto ensure_mutation = [&]() -> TSDDataMutationView & {
                if (!mutation)
                {
                    mutation.emplace(dict_out.begin_mutation(evaluation_time));
                    mutation->reserve(source.size());
                }
                return *mutation;
            };

            for (auto [key, item] : source)
            {
                if (item.is_none()) { continue; }
                key_scratch.view().assign_from_python(key);
                const auto key_view = key_scratch.view();
                if (strict_sentinel.is_valid() && item.is(strict_sentinel))
                {
                    if (!ensure_mutation().erase(key_view))
                    {
                        throw std::runtime_error(
                            "REMOVE: key not present in TSD (use REMOVE_IF_EXISTS "
                            "to remove-if-present)");
                    }
                }
                else if (lenient_sentinel.is_valid() && item.is(lenient_sentinel))
                {
                    // A lenient removal of an absent key is not an effect: it
                    // must not begin a mutation on an otherwise untouched
                    // output (delta_has_effect_tsd's rule).
                    if (!mutation && !dict_out.contains(key_view)) { continue; }
                    static_cast<void>(ensure_mutation().erase(key_view));
                }
                else
                {
                    auto child = ensure_mutation().at(key_view);
                    child_ops.apply_result_impl(
                        TSOutputView{output.output(), child, evaluation_time},
                        item);
                }
            }
            // Touch LAST, only when something applied — mirrors
            // apply_delta_tsd's trailing touch behind the has-effect gate.
            if (mutation) { mutation->touch(); }
        }

        [[nodiscard]] TSRoleTypeRef list_element_type(TSRoleTypeRef type,
                                                      const char *operation)
        {
            const auto &layout = static_cast<const FixedTSLDataLayout &>(
                layout_of(type, operation));
            if (!layout.element_type)
            {
                throw std::logic_error(std::string{operation} +
                                       ": unresolved element type");
            }
            return layout.element_type;
        }

        [[nodiscard]] bool list_requires_authored(TSRoleTypeRef type,
                                                  nb::handle source)
        {
            const auto child_type =
                list_element_type(type, "TSL authored-delta query");
            if (nb::isinstance<nb::dict>(source))
            {
                for (auto [key, item] : nb::cast<nb::dict>(source))
                {
                    static_cast<void>(key);
                    if (!item.is_none() && requires_authored(child_type, item))
                    {
                        return true;
                    }
                }
                return false;
            }
            if (nb::isinstance<nb::str>(source) ||
                !nb::isinstance<nb::sequence>(source))
            {
                return false;
            }
            for (nb::handle item : source)
            {
                if (!item.is_none() && requires_authored(child_type, item))
                {
                    return true;
                }
            }
            return false;
        }

        [[nodiscard]] Value list_delta_from_python(TSRoleTypeRef type,
                                                   nb::handle source,
                                                   bool authored)
        {
            const auto &schema = schema_of(type, "TSL delta_from_python");
            const auto child_type =
                list_element_type(type, "TSL delta_from_python");
            const auto *map_schema = authored ? schema.authored_delta_schema
                                              : schema.delta_value_schema;
            MapBuilder builder{binding_for(map_schema->key_type,
                                           "TSL delta index"),
                               binding_for(map_schema->element_type,
                                           "TSL child delta")};
            if (nb::isinstance<nb::dict>(source))
            {
                for (auto [key, item] : nb::cast<nb::dict>(source))
                {
                    if (item.is_none()) { continue; }
                    Value index = value_from_python(key, map_schema->key_type);
                    Value child = build_delta(child_type, item, authored);
                    if (child.has_value())
                    {
                        builder.set_item(index.view(), child.view());
                    }
                }
                return builder.build();
            }

            std::int64_t index = 0;
            for (nb::handle item : source)
            {
                if (!item.is_none())
                {
                    Value child = build_delta(child_type, item, authored);
                    if (child.has_value())
                    {
                        builder.set_item_copy(std::addressof(index),
                                              child.view().data());
                    }
                }
                ++index;
            }
            return builder.build();
        }

        void apply_list_result(const TSOutputView &output, nb::handle result)
        {
            const auto type = output.storage_type();
            const auto child_type = list_element_type(type, "TSL apply result");
            const auto &child_ops = python_ops_for(child_type);
            auto list_out = output.as_list();
            // No parent touch: a TSL delta is only its children's changes
            // (mirrors apply_delta_tsl).
            if (nb::isinstance<nb::dict>(result))
            {
                for (auto [key, item] : nb::cast<nb::dict>(result))
                {
                    if (item.is_none()) { continue; }
                    const auto index = nb::cast<std::int64_t>(key);
                    child_ops.apply_result_impl(
                        list_out.at(static_cast<std::size_t>(index)), item);
                }
                return;
            }
            std::size_t index = 0;
            for (nb::handle item : result)
            {
                if (!item.is_none())
                {
                    child_ops.apply_result_impl(list_out.at(index), item);
                }
                ++index;
            }
        }

        [[nodiscard]] nb::object bundle_field_source(nb::handle source,
                                                     bool mapping,
                                                     nb::dict fields,
                                                     const char *name)
        {
            nb::str key{name};
            if (mapping)
            {
                return fields.contains(key)
                           ? nb::borrow<nb::object>(fields[key])
                           : nb::object{};
            }
            return nb::hasattr(source, key) ? nb::getattr(source, key)
                                            : nb::object{};
        }

        void validate_bundle_source(TSRoleTypeRef type, nb::handle source,
                                    bool mapping)
        {
            if (mapping) { return; }
            const auto &schema = schema_of(type, "TSB delta_from_python");
            const auto mapped = tsb_compound_value_registry().find(&schema);
            if (mapped == tsb_compound_value_registry().end())
            {
                throw nb::type_error(
                    "a TSB delta must be a dict unless the TSB represents a "
                    "CompoundScalar");
            }
            const auto info = bundle_class_info_registry().find(mapped->second);
            if (info == bundle_class_info_registry().end() ||
                !info->second.type.is_valid() ||
                !nb::isinstance(source, info->second.type))
            {
                throw nb::type_error(
                    "a TSB CompoundScalar snapshot must be an instance of its "
                    "registered class");
            }
        }

        [[nodiscard]] bool bundle_requires_authored(TSRoleTypeRef type,
                                                    nb::handle source)
        {
            const auto &schema = schema_of(type, "TSB authored-delta query");
            const bool mapping = nb::isinstance<nb::dict>(source);
            validate_bundle_source(type, source, mapping);
            nb::dict fields;
            if (mapping) { fields = nb::cast<nb::dict>(source); }
            const auto &layout = static_cast<const FixedTSBDataLayout &>(
                layout_of(type, "TSB authored-delta query"));
            for (std::size_t index = 0; index < schema.field_count(); ++index)
            {
                nb::object item = bundle_field_source(
                    source, mapping, fields, schema.fields()[index].name);
                if (!item.is_valid() || item.is_none()) { continue; }
                if (requires_authored(layout.field(index).type, item))
                {
                    return true;
                }
            }
            return false;
        }

        [[nodiscard]] Value bundle_delta_from_python(TSRoleTypeRef type,
                                                     nb::handle source,
                                                     bool authored)
        {
            const auto &schema = schema_of(type, "TSB delta_from_python");
            const bool mapping = nb::isinstance<nb::dict>(source);
            validate_bundle_source(type, source, mapping);
            nb::dict fields;
            if (mapping) { fields = nb::cast<nb::dict>(source); }
            const auto &layout = static_cast<const FixedTSBDataLayout &>(
                layout_of(type, "TSB delta_from_python"));
            BundleBuilder builder{binding_for(
                authored ? schema.authored_delta_schema
                         : schema.delta_value_schema,
                "TSB delta_from_python")};
            for (std::size_t index = 0; index < schema.field_count(); ++index)
            {
                nb::object item = bundle_field_source(
                    source, mapping, fields, schema.fields()[index].name);
                if (!item.is_valid() || item.is_none()) { continue; }
                Value child =
                    build_delta(layout.field(index).type, item, authored);
                if (child.has_value()) { builder.set(index, std::move(child)); }
            }
            return builder.build();
        }

        void apply_delta_result(const TSOutputView &output, nb::handle result)
        {
            Value delta = delta_from_python(output.storage_type(), result);
            if (!delta.has_value()) { return; }
            const auto &ops = output.data_view().ops();
            if (!ops.delta_has_effect_impl(output, delta.view())) { return; }
            ops.apply_delta_impl(output, delta.view());
        }
    }  // namespace

    const PythonTSDataOps &missing_python_ts_data_ops() noexcept
    {
        static const PythonTSDataOps ops{
            .requires_authored_delta_impl = &missing_requires_authored,
            .delta_from_python_impl = &missing_delta_from_python,
            .apply_result_impl = &missing_apply_result,
        };
        return ops;
    }

    const PythonTSDataOps &atomic_python_ts_data_ops() noexcept
    {
        static const PythonTSDataOps ops{
            .requires_authored_delta_impl = &never_requires_authored,
            .delta_from_python_impl = &atomic_delta_from_python,
            .apply_result_impl = &apply_replacement_result,
        };
        return ops;
    }

    const PythonTSDataOps &ref_python_ts_data_ops() noexcept
    {
        static const PythonTSDataOps ops{
            .requires_authored_delta_impl = &never_requires_authored,
            .delta_from_python_impl = &atomic_delta_from_python,
            .apply_result_impl = &apply_ref_result,
        };
        return ops;
    }

    const PythonTSDataOps &set_python_ts_data_ops() noexcept
    {
        static const PythonTSDataOps ops{
            .requires_authored_delta_impl = &never_requires_authored,
            .delta_from_python_impl = &set_delta_from_python,
            .apply_result_impl = &apply_set_result,
        };
        return ops;
    }

    const PythonTSDataOps &dict_python_ts_data_ops() noexcept
    {
        static const PythonTSDataOps ops{
            .requires_authored_delta_impl = &dict_requires_authored,
            .delta_from_python_impl = &dict_delta_from_python,
            .apply_result_impl = &apply_dict_result,
        };
        return ops;
    }

    const PythonTSDataOps &list_python_ts_data_ops() noexcept
    {
        static const PythonTSDataOps ops{
            .requires_authored_delta_impl = &list_requires_authored,
            .delta_from_python_impl = &list_delta_from_python,
            .apply_result_impl = &apply_list_result,
        };
        return ops;
    }

    const PythonTSDataOps &bundle_python_ts_data_ops() noexcept
    {
        static const PythonTSDataOps ops{
            .requires_authored_delta_impl = &bundle_requires_authored,
            .delta_from_python_impl = &bundle_delta_from_python,
            .apply_result_impl = &apply_replacement_result,
        };
        return ops;
    }

    const PythonTSDataOps &window_python_ts_data_ops() noexcept
    {
        static const PythonTSDataOps ops{
            .requires_authored_delta_impl = &never_requires_authored,
            .delta_from_python_impl = &window_delta_from_python,
            .apply_result_impl = &apply_delta_result,
        };
        return ops;
    }

    Value delta_from_python(const TSValueTypeMetaData *schema,
                            nb::handle source)
    {
        if (schema == nullptr)
        {
            throw std::invalid_argument("delta_from_python requires a schema");
        }
        return delta_from_python(
            TSDataPlanFactory::instance().data_type_for(schema).as_role(), source);
    }

    Value delta_from_python(TSRoleTypeRef type, nb::handle source)
    {
        // Delta builders must all resolve against one realization.  In
        // particular, a TSD whose key/value schema is a polymorphic Bundle
        // needs the builder and every converted concrete leaf to share the
        // same closed-union binding.  Conversion can also run on an external
        // push thread, where the graph's realization scope is not installed,
        // so capture one for the complete recursive conversion rather than
        // allowing individual leaves to capture unrelated snapshots.
        std::shared_ptr<const TypeRealizationSnapshot> conversion_snapshot;
        const auto *snapshot = active_type_realization();
        if (snapshot == nullptr)
        {
            conversion_snapshot =
                TypeRealizationSnapshot::capture(TypeRegistry::instance());
            snapshot = conversion_snapshot.get();
        }
        TypeRealizationScope realization_scope{snapshot};
        return build_delta(type, source, requires_authored(type, source));
    }

    void apply_python_result(const TSOutputView &output, nb::handle result)
    {
        if (result.is_none()) { return; }
        python_ops_for(output.storage_type())
            .apply_result_impl(output, result);
    }
}  // namespace hgraph::python_bridge
