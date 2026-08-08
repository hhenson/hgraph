#ifndef HGRAPH_LIB_STD_OPERATORS_CONVERT_TARGET_H
#define HGRAPH_LIB_STD_OPERATORS_CONVERT_TARGET_H

#include <hgraph/hgraph_export.h>
#include <hgraph/types/type_pattern.h>

#include <span>
#include <string>

namespace hgraph::stdlib
{
    [[nodiscard]] HGRAPH_EXPORT const TSValueTypeMetaData *resolve_convert_target(
        const TypePattern &pattern,
        std::span<const TSValueTypeMetaData *const> inputs,
        std::span<const std::string> selected_keys = {});

    [[nodiscard]] HGRAPH_EXPORT const TSValueTypeMetaData *resolve_collect_target(
        const TypePattern &pattern,
        std::span<const TSValueTypeMetaData *const> inputs);

    /**
     * Resolve a (possibly generic) ``combine`` target from the input ports:
     * ``TSS`` = the common element of N scalar time-series; tuple-shaped
     * ``TS`` = a fixed tuple of the N port elements; ``TSL`` = a fixed list
     * of N same-typed ports; ``TSD`` = the (keys, values) zip pair. Other
     * patterns fall through to the convert inference.
     */
    [[nodiscard]] HGRAPH_EXPORT const TSValueTypeMetaData *resolve_combine_target(
        const TypePattern &pattern,
        std::span<const TSValueTypeMetaData *const> inputs);

    /**
     * Resolve ``emit``'s hinted KeyValue output: ``emit[VALUE_TS](dict)``
     * yields ``TSB{key: TS[K], value: VALUE_TS}`` with ``K`` derived from
     * the dict-like input (TSD key or mapping-value key).
     */
    [[nodiscard]] HGRAPH_EXPORT const TSValueTypeMetaData *resolve_emit_target(
        const TSValueTypeMetaData *value_ts,
        std::span<const TSValueTypeMetaData *const> inputs);
}  // namespace hgraph::stdlib

#endif  // HGRAPH_LIB_STD_OPERATORS_CONVERT_TARGET_H
