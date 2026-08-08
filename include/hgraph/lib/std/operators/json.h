#ifndef HGRAPH_LIB_STD_OPERATORS_JSON_H
#define HGRAPH_LIB_STD_OPERATORS_JSON_H

#include <hgraph/types/operator_dispatch.h>
#include <hgraph/types/primitive_types.h>
#include <hgraph/types/static_node.h>
#include <hgraph/types/static_schema.h>

#include <compare>

namespace hgraph
{
    class ValueView;
    struct TSValueTypeMetaData;
    struct ValueTypeMetaData;
}  // namespace hgraph

namespace hgraph::stdlib::json_tree
{
    [[nodiscard]] const ValueTypeMetaData *json_meta();
    [[nodiscard]] HGRAPH_EXPORT bool is_json_ts(const TSValueTypeMetaData *ts) noexcept;
    [[nodiscard]] bool equals(const ValueView &lhs, const ValueView &rhs);
    [[nodiscard]] std::partial_ordering compare(const ValueView &lhs, const ValueView &rhs);
}  // namespace hgraph::stdlib::json_tree

namespace hgraph::stdlib
{
    /**
     * JSON serialization operators (design record: *Record/replay, tables and
     * const_fn*, step 1). The wire format is the Python one — see
     * ``types/value/json_codec.h``.
     *
     * ``to_json(ts, delta=false)`` serialises the time-series VALUE per tick;
     * with ``delta=true`` it serialises the canonical per-tick delta value
     * (``capture_delta``) instead — the canonical delta *is* the recorded
     * delta wire form.
     *
     * ``from_json`` parses into the resolved output type and applies the
     * parsed value as the tick's delta:
     * ``wire<from_json, TS<MySchema>>(w, ts)``.
     */
    struct to_json : Operator<"to_json", In<"ts", TsVar<"S">>, Scalar<"delta", Bool>, Out<TS<Str>>>
    {
    };

    /** Dynamic-JSON tree operators (design record: parity_matrix.rst,
        ruling 2026-07-06 — the tree is a C++ value; python is sugar). */
    struct combine_json : Operator<"combine_json", In<"values", TsVar<"V">>, Out<TsVar<"O">>>
    {
    };

    /** Runtime node behind ``combine_json`` (internal). */
    struct json_object_ : Operator<"__json_object", In<"values", TsVar<"V">>, Out<TsVar<"O">>>
    {
    };

    struct json_encode : Operator<"json_encode", In<"ts", TsVar<"S">>, Out<TS<Str>>>
    {
    };

    struct json_decode : Operator<"json_decode", In<"ts", TS<Str>>, Out<TsVar<"O">>>
    {
    };

    struct json_as_int : Operator<"json_as_int", In<"ts", TsVar<"S">>, Out<TS<Int>>>
    {
    };

    struct json_as_float : Operator<"json_as_float", In<"ts", TsVar<"S">>, Out<TS<Float>>>
    {
    };

    struct json_as_str : Operator<"json_as_str", In<"ts", TsVar<"S">>, Out<TS<Str>>>
    {
    };

    struct json_as_bool : Operator<"json_as_bool", In<"ts", TsVar<"S">>, Out<TS<Bool>>>
    {
    };

    struct from_json : Operator<"from_json", In<"ts", TS<Str>>, Out<TsVar<"O">>>
    {
    };
}  // namespace hgraph::stdlib

#endif  // HGRAPH_LIB_STD_OPERATORS_JSON_H
