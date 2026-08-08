#ifndef HGRAPH_LIB_STD_OPERATORS_TABLE_H
#define HGRAPH_LIB_STD_OPERATORS_TABLE_H

#include <hgraph/types/frame.h>
#include <hgraph/types/operator_dispatch.h>
#include <hgraph/types/static_node.h>
#include <hgraph/types/static_schema.h>

namespace hgraph::stdlib
{
    /**
     * Table serialization operators (design record: *Record/replay, tables
     * and const_fn*, P4 + step 6). ``to_table`` is the Python-parity TUPLE-ROW
     * protocol: each tick converts to bitemporal row values
     * ``[date, as_of, {removed, *keys}(per TSD level), *value columns]`` —
     * ``TS<tuple[...]>`` for single-row types, ``TS<tuple[tuple[...], ...]>``
     * for partitioned (TSD) or multi-row (``Frame``-valued) types; unset
     * cells are tuple field validity (Python ``None``). The output schema is
     * computed from the resolved input at wiring. ``mode`` is a ``ToTableMode``
     * enum time-series (Tick/Sample/Snap) defaulting to Tick.
     *
     * ``from_table`` reverses it, applying each row as the tick's delta at
     * the resolved output (supplied at the wiring site:
     * ``wire<from_table, TS<MySchema>>(w, ts)``); removed flags map to TSD
     * key removals. The record/replay backends bypass both and drive the
     * Arrow serializer ops directly (``types/value/table_codec.h``).
     */
    struct to_table : Operator<"to_table", In<"ts", TsVar<"S">>, In<"mode", TS<ScalarVar<"M">>>, Out<TsVar<"__out__">>>
    {
    };

    struct from_table : Operator<"from_table", In<"ts", TsVar<"T">>, Out<TsVar<"O">>>
    {
    };

    /** ``from_table_const`` — the const-evaluable read of a recorded FRAME
        value (the replay_const seam): extract the (last) row of a frame
        VALUE as the output type. */
    struct from_table_const : Operator<"from_table_const", Scalar<"value", Frame>, Out<TsVar<"O">>>
    {
    };
}  // namespace hgraph::stdlib

#endif  // HGRAPH_LIB_STD_OPERATORS_TABLE_H
