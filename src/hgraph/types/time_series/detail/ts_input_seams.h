#ifndef HGRAPH_TYPES_TIME_SERIES_DETAIL_TS_INPUT_SEAMS_H
#define HGRAPH_TYPES_TIME_SERIES_DETAIL_TS_INPUT_SEAMS_H

#include <hgraph/types/time_series/ts_data/base_view.h>
#include <hgraph/types/time_series/ts_input/detail.h>
#include <hgraph/types/time_series/ts_type_ref.h>
#include <hgraph/types/value/value_type_ref.h>

#include <cstddef>
#include <cstdint>

/**
 * Private seams of the TS input facades (RFC 0035, PR 5): what the bridge's
 * ``ts_input_conversions.cpp`` needs from a non-peered TSB / TSL input
 * binding and from a target link, with no Python in it. The binding context
 * and the link storage stay private to ``ts_input.cpp`` and
 * ``ts_input/target_link_ops.cpp``; the bridge converts what the seams
 * answer. Private to ``hgraph_runtime``.
 */
namespace hgraph
{
    class TSOutputView;
    struct TSValueTypeMetaData;
}  // namespace hgraph

namespace hgraph::ts_input_seams
{
    // -- non-peered TSB / TSL input bindings (ts_input.cpp) ---------------------
    // ``context`` is the binding context the facade's ops carry. Children are
    // reached through their own erased TSDataOps (``input_child_type``); a
    // bound target-link child answers the target output's storage type and
    // memory, an owned child its local storage.
    [[nodiscard]] const TSValueTypeMetaData &input_schema(const void *context) noexcept;
    /** The endpoint-shape table (its ``to_python`` / ``delta_to_python`` slots). */
    [[nodiscard]] const detail::TSInputEndpointOps &input_endpoint_ops(const void *context) noexcept;
    /** The value-projection binding (what the ``to_python`` value slot converts). */
    [[nodiscard]] ValueTypeRef input_value_binding(const void *context) noexcept;
    /** The delta-projection binding (the TSB delta bundle / TSL delta map). */
    [[nodiscard]] ValueTypeRef input_delta_binding(const void *context) noexcept;
    [[nodiscard]] std::size_t input_child_count(const void *context) noexcept;
    [[nodiscard]] TSRoleTypeRef input_child_type(const void *context, const void *memory, std::size_t index) noexcept;
    [[nodiscard]] const void *input_child_memory(const void *context, const void *memory, std::size_t index) noexcept;
    /** True when child ``index`` was modified at the parent's last modified time. */
    [[nodiscard]] bool input_child_modified(const void *context, const void *memory, std::size_t index);
    /** The TSL delta's ordinal key for child ``index``. */
    [[nodiscard]] std::int64_t input_list_ordinal_key(const void *context, std::size_t index) noexcept;

    // -- target links (ts_input/target_link_ops.cpp) ----------------------------
    /** The bound target output's data view, or an empty view when unbound. */
    [[nodiscard]] TSDataView target_link_target_view(const void *context, const void *memory);
    /** The target output a delta applied through the link lands on (the
        write-through of ``apply_delta``); throws when the link is unbound. */
    [[nodiscard]] TSOutputView target_link_delta_target(const void *context, const TSOutputView &output);
}  // namespace hgraph::ts_input_seams

#endif  // HGRAPH_TYPES_TIME_SERIES_DETAIL_TS_INPUT_SEAMS_H
