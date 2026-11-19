#include "ts_child_delta.h"
#include "list_delta_source.h"
#include "python_ops_families.h"

#include "../../types/time_series/detail/ts_input_seams.h"

#include <hgraph/python/bridge_state.h>
#include <hgraph/python/conversion.h>
#include <hgraph/types/metadata/ts_value_type_meta_data.h>
#include <hgraph/types/time_series/ts_data/ops.h>
#include <hgraph/types/value/value.h>

#include <nanobind/nanobind.h>

#include <cstddef>
#include <utility>

/**
 * TS input facades <-> Python (RFC 0035, PR 5): the non-peered TSB / TSL
 * input bindings' endpoint-shape slots and value / delta projections, and
 * the bound target of a target link. Children are reached through their own
 * erased TSDataOps; the binding's shape comes through ``ts_input_seams.h``.
 * Each body keeps the semantics it had beside its facade.
 */
namespace hgraph::python_bridge
{
    namespace
    {
        namespace seams = ts_input_seams;

        [[nodiscard]] nb::object child_value_to_python(TSRoleTypeRef type, const void *memory, ValueForm form)
        {
            if (!type || memory == nullptr) { return nb::none(); }
            return current_value_to_python(*type.ops(), memory, form);
        }

        [[nodiscard]] nb::object child_delta_to_python(TSRoleTypeRef type, const void *memory, DateTime evaluation_time)
        {
            if (!type || memory == nullptr) { return {}; }
            const auto &ops      = *type.ops();
            const auto *tracking = ops.tracking_impl(ops.context, memory);
            if (tracking == nullptr || tracking->last_modified_time != evaluation_time) { return {}; }
            return published_child_delta(ops, memory, evaluation_time);
        }

        // -- endpoint shapes -----------------------------------------------------------
        // Each value body is written once for both forms of a current value
        // (RFC 0050); ``Form`` is the form each child is asked for.
        template <ValueForm Form>
        [[nodiscard]] nb::object input_bundle_to_python(const void *context, const void *memory)
        {
            const auto &schema = seams::input_schema(context);
            nb::dict    result;
            for (std::size_t index = 0, count = seams::input_child_count(context); index < count; ++index)
            {
                const auto &field = schema.fields()[index];
                if (field.name == nullptr) { continue; }
                result[nb::str{field.name}] = child_value_to_python(seams::input_child_type(context, memory, index),
                                                                    seams::input_child_memory(context, memory, index),
                                                                    Form);
            }
            return materialize_tsb_python_value(&schema, std::move(result));
        }

        template <ValueForm Form>
        [[nodiscard]] nb::object input_list_to_python(const void *context, const void *memory)
        {
            if constexpr (Form == ValueForm::Delta) {
                return python_list_snapshot(seams::input_child_count(context), false, [&](std::size_t index) {
                    return child_value_to_python(seams::input_child_type(context, memory, index),
                                                 seams::input_child_memory(context, memory, index), Form);
                });
            }
            nb::list result;
            for (std::size_t index = 0, count = seams::input_child_count(context); index < count; ++index)
            {
                result.append(child_value_to_python(seams::input_child_type(context, memory, index),
                                                    seams::input_child_memory(context, memory, index), Form));
            }
            return nb::tuple(result);
        }

        [[nodiscard]] nb::object input_bundle_delta_to_python(const void *context, const void *memory,
                                                              DateTime evaluation_time)
        {
            const auto &schema = seams::input_schema(context);
            nb::dict    result;
            for (std::size_t index = 0, count = seams::input_child_count(context); index < count; ++index)
            {
                const auto &field = schema.fields()[index];
                if (field.name == nullptr) { continue; }
                auto child_delta = child_delta_to_python(seams::input_child_type(context, memory, index),
                                                         seams::input_child_memory(context, memory, index),
                                                         evaluation_time);
                if (child_delta.is_valid()) { result[nb::str{field.name}] = child_delta; }
            }
            return result;
        }

        [[nodiscard]] nb::object input_list_delta_to_python(const void *context, const void *memory,
                                                            DateTime evaluation_time)
        {
            nb::dict result;
            for (std::size_t index = 0, count = seams::input_child_count(context); index < count; ++index)
            {
                auto child_delta = child_delta_to_python(seams::input_child_type(context, memory, index),
                                                         seams::input_child_memory(context, memory, index),
                                                         evaluation_time);
                if (child_delta.is_valid()) { result[nb::int_{index}] = child_delta; }
            }
            const auto bundle = ValueView{seams::input_delta_binding(context),memory}.as_bundle();
            return python_list_delta(std::move(result),to_python(bundle.at(0).binding(),memory),nb::none());
        }

        // -- value / delta projections -------------------------------------------------
        // Projection storage is materialised exclusively through its erased
        // owning-type and copy hooks; converting the snapshot keeps ValueOps
        // complete without teaching the caller which endpoint produced it.
        [[nodiscard]] nb::object input_value_projection_to_python(const void *context, const void *memory)
        {
            return to_python(Value{ValueView{seams::input_value_binding(context), memory}});
        }

        [[nodiscard]] nb::object input_delta_projection_to_python(const void *context, const void *memory)
        {
            return to_python(Value{ValueView{seams::input_delta_binding(context), memory}});
        }

        [[nodiscard]] nb::object input_delta_map_to_python(const void *context, const void *memory) {
            const auto modified = ValueView{seams::input_delta_binding(context), memory}.as_bundle().at(1);
            return to_python(Value{modified});
        }

        [[nodiscard]] nb::object input_delta_key_set_to_python(const void *context, const void *memory)
        {
            nb::set result;
            for (std::size_t index = 0, count = seams::input_child_count(context); index < count; ++index)
            {
                if (seams::input_child_modified(context, memory, index))
                {
                    result.add(nb::int_{seams::input_list_ordinal_key(context, index)});
                }
            }
            return result;
        }

        // -- target links ----------------------------------------------------------------
        [[nodiscard]] nb::object target_link_to_python(const void *context, const void *memory)
        {
            return value_to_python(seams::target_link_target_view(context, memory));
        }

        /** A link's value in the form a delta has is its target's. */
        [[nodiscard]] nb::object target_link_value_as_delta_to_python(const void *context, const void *memory)
        {
            return value_as_delta_to_python(seams::target_link_target_view(context, memory));
        }

        [[nodiscard]] nb::object target_link_delta_to_python(const void *context, const void *memory,
                                                             DateTime evaluation_time)
        {
            auto target = seams::target_link_target_view(context, memory);
            // A sampled link's delta is its target's current value, in the
            // form the target gives a delta (RFC 0050).
            return seams::target_link_sampled(context, memory, evaluation_time)
                       ? value_as_delta_to_python(target) : delta_value_to_python(target, evaluation_time);
        }

        [[nodiscard]] nb::object target_link_dict_delta_to_python(const void *context, const void *memory,
                                                                  DateTime evaluation_time)
        {
            auto target = seams::target_link_target_view(context, memory);
            if (!seams::target_link_transition(context, memory, evaluation_time))
            {
                return delta_value_to_python(target, evaluation_time);
            }
            nb::set removed;
            nb::dict modified;
            for (const auto &key : seams::target_link_removed_keys(context, memory))
            {
                removed.add(to_python(key));
            }
            if (seams::target_link_sampled(context, memory, evaluation_time))
            {
                const auto dict = target.as_dict();
                const auto items = dict.items();
                for (const auto &[key, child] : items)
                {
                    if (child.has_current_value()) { modified[to_python(key)] = value_as_delta_to_python(child); }
                }
            }
            nb::dict result;
            result[nb::str{"removed"}] = std::move(removed);
            result[nb::str{"modified"}] = std::move(modified);
            return result;
        }
    }  // namespace

    void fill_ts_input_conversions(PythonOps::TSData &section) noexcept
    {
        section.input_bundle_to_python           = &to_python_slot<&input_bundle_to_python<ValueForm::Value>>;
        section.input_bundle_value_as_delta_to_python = &to_python_slot<&input_bundle_to_python<ValueForm::Delta>>;
        section.input_list_value_as_delta_to_python   = &to_python_slot<&input_list_to_python<ValueForm::Delta>>;
        section.target_link_value_as_delta_to_python  = &to_python_slot<&target_link_value_as_delta_to_python>;
        section.input_bundle_delta_to_python     = &ts_delta_to_python_slot<&input_bundle_delta_to_python>;
        section.input_list_to_python             = &to_python_slot<&input_list_to_python<ValueForm::Value>>;
        section.input_list_delta_to_python       = &ts_delta_to_python_slot<&input_list_delta_to_python>;
        section.input_value_projection_to_python = &to_python_slot<&input_value_projection_to_python>;
        section.input_delta_bundle_to_python     = &to_python_slot<&input_delta_projection_to_python>;
        section.input_delta_map_to_python        = &to_python_slot<&input_delta_map_to_python>;
        section.input_delta_key_set_to_python    = &to_python_slot<&input_delta_key_set_to_python>;
        section.target_link_to_python            = &to_python_slot<&target_link_to_python>;
        section.target_link_delta_to_python      = &ts_delta_to_python_slot<&target_link_delta_to_python>;
        section.target_link_dict_delta_to_python = &ts_delta_to_python_slot<&target_link_dict_delta_to_python>;
    }
}  // namespace hgraph::python_bridge
