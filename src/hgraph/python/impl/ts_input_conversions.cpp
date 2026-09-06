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

        [[nodiscard]] nb::object child_value_to_python(TSRoleTypeRef type, const void *memory)
        {
            if (!type || memory == nullptr) { return nb::none(); }
            const auto &ops = *type.ops();
            if (!ops.has_current_value_impl(ops.context, memory)) { return nb::none(); }
            return take(ops.to_python_impl(ops.context, memory));
        }

        [[nodiscard]] nb::object child_delta_to_python(TSRoleTypeRef type, const void *memory, DateTime evaluation_time)
        {
            if (!type || memory == nullptr) { return nb::none(); }
            const auto &ops      = *type.ops();
            const auto *tracking = ops.tracking_impl(ops.context, memory);
            if (tracking == nullptr || tracking->last_modified_time != evaluation_time) { return nb::none(); }
            return take(ops.delta_to_python_impl(ops.context, memory, evaluation_time));
        }

        // -- endpoint shapes -----------------------------------------------------------
        [[nodiscard]] nb::object input_bundle_to_python(const void *context, const void *memory)
        {
            const auto &schema = seams::input_schema(context);
            nb::dict    result;
            for (std::size_t index = 0, count = seams::input_child_count(context); index < count; ++index)
            {
                const auto &field = schema.fields()[index];
                if (field.name == nullptr) { continue; }
                result[nb::str{field.name}] = child_value_to_python(seams::input_child_type(context, memory, index),
                                                                    seams::input_child_memory(context, memory, index));
            }
            return materialize_tsb_python_value(&schema, std::move(result));
        }

        [[nodiscard]] nb::object input_list_to_python(const void *context, const void *memory)
        {
            nb::list result;
            for (std::size_t index = 0, count = seams::input_child_count(context); index < count; ++index)
            {
                result.append(child_value_to_python(seams::input_child_type(context, memory, index),
                                                    seams::input_child_memory(context, memory, index)));
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
                if (!child_delta.is_none()) { result[nb::str{field.name}] = child_delta; }
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
                if (!child_delta.is_none()) { result[nb::int_{index}] = child_delta; }
            }
            return result;
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

        [[nodiscard]] nb::object target_link_delta_to_python(const void *context, const void *memory,
                                                             DateTime evaluation_time)
        {
            return delta_value_to_python(seams::target_link_target_view(context, memory), evaluation_time);
        }
    }  // namespace

    void fill_ts_input_conversions(PythonOps::TSData &section) noexcept
    {
        section.input_bundle_to_python           = &to_python_slot<&input_bundle_to_python>;
        section.input_bundle_delta_to_python     = &ts_delta_to_python_slot<&input_bundle_delta_to_python>;
        section.input_list_to_python             = &to_python_slot<&input_list_to_python>;
        section.input_list_delta_to_python       = &ts_delta_to_python_slot<&input_list_delta_to_python>;
        section.input_value_projection_to_python = &to_python_slot<&input_value_projection_to_python>;
        section.input_delta_bundle_to_python     = &to_python_slot<&input_delta_projection_to_python>;
        section.input_delta_map_to_python        = &to_python_slot<&input_delta_projection_to_python>;
        section.input_delta_key_set_to_python    = &to_python_slot<&input_delta_key_set_to_python>;
        section.target_link_to_python            = &to_python_slot<&target_link_to_python>;
        section.target_link_delta_to_python      = &ts_delta_to_python_slot<&target_link_delta_to_python>;
    }
}  // namespace hgraph::python_bridge
