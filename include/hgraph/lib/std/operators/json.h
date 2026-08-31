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
    [[nodiscard]] HGRAPH_EXPORT const ValueTypeMetaData *json_meta();
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
    /** Serialize each time-series tick as JSON text.
        Full-value mode encodes the current value; delta mode encodes the canonical
        per-tick delta used by record/replay and preserves removals explicitly.
        @param ts Value or structure to encode.
        @param delta When true, encode only the current tick's canonical delta.
        @return JSON text for each source tick.
        @par Python example
        @code{.py}
        payload = hg.to_json(prices, delta=True)
        @endcode */
    struct to_json : Operator<"to_json", In<"ts", TsVar<"S">>, Scalar<"delta", Bool>, Out<TS<Str>>>
    {
    };

    /** Build a dynamic JSON object from named ports or a JSON array from
        positional ports. The JSON tree remains a C++ value; Python is
        authoring sugar. */
    struct combine_json : Operator<"combine_json", In<"values", TsVar<"V">>, Out<TsVar<"O">>>
    {
    };

    /** Runtime node behind ``combine_json`` (internal). */
    struct json_object_ : Operator<"__json_object", In<"values", TsVar<"V">>, Out<TsVar<"O">>>
    {
    };

    /** Runtime node behind positional ``combine[TS[JSON]]`` (internal). */
    struct json_array_ : Operator<"__json_array", In<"values", TsVar<"V">>, Out<TsVar<"O">>>
    {
    };

    /** Encode a dynamic JSON-tree value as standards-compliant JSON text.
        @param ts Dynamic JSON value.
        @return Compact JSON string.
        @par Python example
        @code{.py}
        text = hg.json_encode(json_value)
        @endcode */
    struct json_encode : Operator<"json_encode", In<"ts", TsVar<"S">>, Out<TS<Str>>>
    {
    };

    /** Parse JSON text into hgraph's dynamic JSON-tree value.
        @param ts JSON text.
        @return Dynamic JSON value preserving object, array, scalar, and null structure.
        @par Python example
        @code{.py}
        json_value = hg.json_decode(text)
        @endcode */
    struct json_decode : Operator<"json_decode", In<"ts", TS<Str>>, Out<TsVar<"O">>>
    {
    };

    /** Coerce a dynamic JSON scalar leaf to an integer.
        @param ts JSON scalar value.
        @return Integer representation, raising for incompatible JSON shapes or values.
        @par Python example
        @code{.py}
        count = hg.json_as_int(json_value)
        @endcode */
    struct json_as_int : Operator<"json_as_int", In<"ts", TsVar<"S">>, Out<TS<Int>>>
    {
    };

    /** Coerce a dynamic JSON scalar leaf to floating point.
        @param ts JSON scalar value.
        @return Floating-point representation.
        @par Python example
        @code{.py}
        price = hg.json_as_float(json_value)
        @endcode */
    struct json_as_float : Operator<"json_as_float", In<"ts", TsVar<"S">>, Out<TS<Float>>>
    {
    };

    /** Coerce a dynamic JSON scalar leaf to a string.
        @param ts JSON scalar value.
        @return String representation.
        @par Python example
        @code{.py}
        label = hg.json_as_str(json_value)
        @endcode */
    struct json_as_str : Operator<"json_as_str", In<"ts", TsVar<"S">>, Out<TS<Str>>>
    {
    };

    /** Coerce a dynamic JSON scalar leaf to a boolean.
        @param ts JSON scalar value.
        @return Boolean representation.
        @par Python example
        @code{.py}
        enabled = hg.json_as_bool(json_value)
        @endcode */
    struct json_as_bool : Operator<"json_as_bool", In<"ts", TsVar<"S">>, Out<TS<Bool>>>
    {
    };

    /** Parse JSON text directly into an explicitly selected time-series schema.
        Each parsed value is applied as that tick's delta, so collection removals and
        structural updates follow the target type's normal delta semantics.

        Being a delta, an absent member is UNCHANGED rather than removed: a bare
        ``[..]`` for a ``TSS`` adds its members, and removal needs the explicit
        ``{"added": [..], "removed": [..]}`` form. A ``null`` element of a ``TSL``
        array means that element does not tick.
        @param ts JSON text.
        @param delta Accepted for release/0.5 compatibility
               (``from_json_generic(ts, _tp, delta=False)``). 0.5 threaded the
               flag through its converter tree but never branched on it, so
               decoding is identical either way and this does not select an
               overload. ``to_json``'s ``delta`` IS significant.
        @return Parsed values in the selected output schema.
        @par Python example
        @code{.py}
        prices = hg.from_json[TSD[str, TS[float]]](payload)
        @endcode */
    struct from_json
        : Operator<"from_json", In<"ts", TS<Str>>, Scalar<"delta", Bool>,
                   Out<TsVar<"O">>>
    {
    };
}  // namespace hgraph::stdlib

#endif  // HGRAPH_LIB_STD_OPERATORS_JSON_H
