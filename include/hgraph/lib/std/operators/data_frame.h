#ifndef HGRAPH_LIB_STD_OPERATORS_DATA_FRAME_H
#define HGRAPH_LIB_STD_OPERATORS_DATA_FRAME_H

#include <hgraph/types/frame.h>
#include <hgraph/types/operator_dispatch.h>
#include <hgraph/types/static_node.h>
#include <hgraph/types/static_schema.h>

namespace hgraph::stdlib
{
    /**
     * Data-frame convenience operators (design record: *Record/replay,
     * tables and const_fn*, step 6): the same layout/codec machinery as
     * ``to_table`` with a plain ``date`` column and no ``as_of``.
     *
     * ``from_data_frame[OUT](df, dt_col="date", key_col="key",
     * value_col="value", offset=0)`` replays a frame VALUE by its date
     * column (a pull source; TSD forms take the key from ``key_col``).
     * ``to_data_frame(ts, ...)`` snapshots the time-series per tick into a
     * one-tick frame whose columns come from the requested output
     * ``Frame[Schema]``. ``group_by(ts, by)`` partitions a Frame-valued TS
     * into ``TSD[key, TS[Frame]]`` by column name(s).
     */
    struct from_data_frame : Operator<"from_data_frame", Scalar<"df", Frame>, Scalar<"dt_col", Str>,
                                      Scalar<"key_col", Str>, Scalar<"value_col", Str>,
                                      Scalar<"offset", TimeDelta>, Out<TsVar<"O">>>
    {
    };

    /** Replay successively supplied frame batches without concatenating the
        source. Each batch must arrive no later than its first retained row. */
    struct from_data_frame_batches
        : Operator<"from_data_frame_batches", In<"frames", TS<Frame>>,
                   Scalar<"dt_col", Str>, Scalar<"key_col", Str>,
                   Scalar<"value_col", Str>, Scalar<"offset", TimeDelta>,
                   Out<TsVar<"O">>>
    {
    };

    /** Replay a canonical bitemporal table frame through the native table
        protocol, selecting the latest as-of revision per partition. */
    struct replay_data_frame
        : Operator<"replay_data_frame", Scalar<"data_frame", Frame>,
                   Scalar<"as_of_time", DateTime>, Out<TsVar<"O">>>
    {
    };

    struct to_data_frame : Operator<"to_data_frame", In<"ts", TsVar<"S">>, Scalar<"dt_col", Str>,
                                    Scalar<"key_col", Str>, Scalar<"value_col", Str>,
                                    Out<TsVar<"__out__">>>
    {
    };

    struct group_by
        : Operator<"group_by", In<"ts", TS<ScalarVar<"F">>>, Scalar<"by", ScalarVar<"B">>, Out<TsVar<"__out__">>>
    {
    };

    /** ``sorted_(ts, by, descending=false)`` — order a typed frame by one column. */
    struct sorted_
        : Operator<"sorted_", In<"ts", TS<FrameOf<ScalarVar<"R">>>>, Scalar<"by", Str>,
                   Scalar<"descending", Bool>, Out<TS<FrameOf<ScalarVar<"R">>>>>
    {
    };

    /** ``concat(ts1, ts2)`` — append rows from two frames with the same schema. */
    struct concat
        : Operator<"concat", In<"ts1", TS<FrameOf<ScalarVar<"R">>>>,
                   In<"ts2", TS<FrameOf<ScalarVar<"R">>>>, Out<TS<FrameOf<ScalarVar<"R">>>>>
    {
    };

    namespace data_frame
    {
        /** Arrow-native equi-join. The namespace avoids colliding with the
            string join marker; both remain overloads in the public family. */
        struct join
            : Operator<"join", In<"lhs", TS<FrameOf<ScalarVar<"L">>>>,
                       In<"rhs", TS<FrameOf<ScalarVar<"R">>>>,
                       Scalar<"on", ScalarVar<"K">>, Scalar<"how", Str>,
                       Scalar<"suffix", Str>, Out<TS<FrameOf<ScalarVar<"O">>>>>
        {
        };

        /** Filter a frame by the currently valid fields of a structural TSB. */
        struct filter_frame
            : Operator<"filter_frame", In<"ts", TS<FrameOf<ScalarVar<"R">>>>,
                       In<"predicate", TsVar<"P">>,
                       Out<TS<FrameOf<ScalarVar<"R">>>>>
        {
        };

        /** Filter a frame by the set fields of one compound scalar value. */
        struct filter_cs
            : Operator<"filter_cs", In<"ts", TS<FrameOf<ScalarVar<"R">>>>,
                       In<"predicate", TS<ScalarVar<"P">>>,
                       Out<TS<FrameOf<ScalarVar<"R">>>>>
        {
        };

        /** Concatenate the valid values of a keyed frame collection. */
        struct ungroup
            : Operator<"ungroup", In<"ts", TsVar<"S">>,
                       Out<TS<FrameOf<ScalarVar<"O">>>>>
        {
        };

        /** Ungroup while materializing a scalar or tuple key into columns. */
        struct ungroup_with_keys
            : Operator<"ungroup", In<"ts", TsVar<"S">>,
                       Scalar<"key_col", ScalarVar<"C">>,
                       Out<TS<FrameOf<ScalarVar<"O">>>>>
        {
        };

        /** Replace/add structural columns and project to the output row schema. */
        struct with_columns
            : Operator<"with_columns", In<"ts", TS<FrameOf<ScalarVar<"R">>>>,
                       In<"columns", TsVar<"C">>,
                       Out<TS<FrameOf<ScalarVar<"O">>>>>
        {
        };
    }  // namespace data_frame
}  // namespace hgraph::stdlib

#endif  // HGRAPH_LIB_STD_OPERATORS_DATA_FRAME_H
