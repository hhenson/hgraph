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
    /** Replay rows from a wiring-time frame according to a timestamp column.
        Scalar output reads ``value_col``; keyed output additionally uses ``key_col``.
        @param df Frame containing replay rows.
        @param dt_col Timestamp column controlling tick times.
        @param key_col Key column used by keyed output schemas.
        @param value_col Value column used by scalar output schemas.
        @param offset Duration added to every replay timestamp.
        @return A source of the explicitly selected time-series schema.
        @par Python example
        @code{.py}
        prices = hg.from_data_frame[TSD[str, TS[float]]](frame, dt_col="date", key_col="symbol")
        @endcode */
    struct from_data_frame : Operator<"from_data_frame", Scalar<"df", Frame>, Scalar<"dt_col", Str>,
                                      Scalar<"key_col", Str>, Scalar<"value_col", Str>,
                                      Scalar<"offset", TimeDelta>, Out<TsVar<"O">>>
    {
    };

    /** Replay successively supplied frame batches without concatenating the source.
        Each batch must arrive no later than its first retained row, preserving bounded
        memory while maintaining global timestamp order.
        @param frames Stream of frame batches.
        @param dt_col Timestamp column controlling tick times.
        @param key_col Key column used by keyed output schemas.
        @param value_col Value column used by scalar output schemas.
        @param offset Duration added to every replay timestamp.
        @return A source of the explicitly selected time-series schema.
        @par Python example
        @code{.py}
        prices = hg.from_data_frame_batches[TSD[str, TS[float]]](batches, dt_col="date")
        @endcode */
    struct from_data_frame_batches
        : Operator<"from_data_frame_batches", In<"frames", TS<Frame>>,
                   Scalar<"dt_col", Str>, Scalar<"key_col", Str>,
                   Scalar<"value_col", Str>, Scalar<"offset", TimeDelta>,
                   Out<TsVar<"O">>>
    {
    };

    /** Replay a canonical bitemporal table frame, selecting the latest as-of revision
        for each partition before applying event-time deltas.
        @param data_frame Canonical table frame to replay.
        @param as_of_time Revision cutoff fixed at wiring time.
        @return A source of the explicitly selected table-compatible schema.
        @par Python example
        @code{.py}
        positions = hg.replay_data_frame[TSD[str, TS[float]]](frame, as_of_time=cutoff)
        @endcode */
    struct replay_data_frame
        : Operator<"replay_data_frame", Scalar<"data_frame", Frame>,
                   Scalar<"as_of_time", DateTime>, Out<TsVar<"O">>>
    {
    };

    /** Snapshot each input tick into a typed one-tick frame.
        The explicitly selected ``Frame`` schema determines column types and names.
        @param ts Stream to snapshot.
        @param dt_col Optional evaluation-time column name.
        @param key_col Key column name for keyed inputs.
        @param value_col Value column name for scalar inputs.
        @return A frame-valued time series representing each input delta.
        @par Python example
        @code{.py}
        rows = hg.to_data_frame[TS[Frame[PriceRow]]](prices, key_col="symbol")
        @endcode */
    struct to_data_frame : Operator<"to_data_frame", In<"ts", TsVar<"S">>, Scalar<"dt_col", Str>,
                                    Scalar<"key_col", Str>, Scalar<"value_col", Str>,
                                    Out<TsVar<"__out__">>>
    {
    };

    /** Partition each frame into a keyed dictionary of frames by one or more columns.
        @param ts Frame-valued input.
        @param by Wiring-time column name or tuple of column names forming the key.
        @return A keyed dictionary whose values contain rows for each distinct key.
        @par Python example
        @code{.py}
        by_symbol = hg.group_by(rows, by="symbol")
        @endcode */
    struct group_by
        : Operator<"group_by", In<"ts", TS<ScalarVar<"F">>>, Scalar<"by", ScalarVar<"B">>, Out<TsVar<"__out__">>>
    {
    };

    /** Order rows in each frame by one column.
        @param ts Frame-valued input.
        @param by Wiring-time sort column.
        @param descending Reverse the ascending order when true.
        @return A frame with the same row schema in sorted order.
        @par Python example
        @code{.py}
        ranked = hg.sorted_(rows, by="price", descending=True)
        @endcode */
    struct sorted_
        : Operator<"sorted_", In<"ts", TS<FrameOf<ScalarVar<"R">>>>, Scalar<"by", Str>,
                   Scalar<"descending", Bool>, Out<TS<FrameOf<ScalarVar<"R">>>>>
    {
    };

    /** Append rows from two frames with the same row schema.
        @param ts1 First frame.
        @param ts2 Frame appended after ``ts1``.
        @return The vertically concatenated frame.
        @par Python example
        @code{.py}
        all_rows = hg.concat(primary_rows, secondary_rows)
        @endcode */
    struct concat
        : Operator<"concat", In<"ts1", TS<FrameOf<ScalarVar<"R">>>>,
                   In<"ts2", TS<FrameOf<ScalarVar<"R">>>>, Out<TS<FrameOf<ScalarVar<"R">>>>>
    {
    };

    namespace data_frame
    {
        /** Join two frames by equality of one or more key columns.
            This is part of the public ``join`` overload family alongside string joining.
            @param lhs Left frame.
            @param rhs Right frame.
            @param on Wiring-time join column or columns.
            @param how Join policy such as inner, left, right, or full where supported.
            @param suffix Suffix applied to colliding right-hand column names.
            @return A frame with the explicitly resolved joined row schema.
            @par Python example
            @code{.py}
            enriched = hg.join(trades, instruments, on="instrument_id", how="left")
            @endcode */
        struct join
            : Operator<"join", In<"lhs", TS<FrameOf<ScalarVar<"L">>>>,
                       In<"rhs", TS<FrameOf<ScalarVar<"R">>>>,
                       Scalar<"on", ScalarVar<"K">>, Scalar<"how", Str>,
                       Scalar<"suffix", Str>, Out<TS<FrameOf<ScalarVar<"O">>>>>
        {
        };

        /** Keep frame rows matching every currently valid field of a structural predicate.
            Invalid predicate fields are ignored rather than compared.
            @param ts Frame-valued input.
            @param predicate Structural bundle of column equality filters.
            @return The matching rows with the original frame schema.
            @par Python example
            @code{.py}
            matching = hg.filter_frame(rows, filters)
            @endcode */
        struct filter_frame
            : Operator<"filter_frame", In<"ts", TS<FrameOf<ScalarVar<"R">>>>,
                       In<"predicate", TsVar<"P">>,
                       Out<TS<FrameOf<ScalarVar<"R">>>>>
        {
        };

        /** Keep frame rows matching the populated fields of one compound-scalar predicate.
            @param ts Frame-valued input.
            @param predicate Compound scalar whose populated fields define equality filters.
            @return The matching rows with the original frame schema.
            @par Python example
            @code{.py}
            matching = hg.filter_cs(rows, filter_value)
            @endcode */
        struct filter_cs
            : Operator<"filter_cs", In<"ts", TS<FrameOf<ScalarVar<"R">>>>,
                       In<"predicate", TS<ScalarVar<"P">>>,
                       Out<TS<FrameOf<ScalarVar<"R">>>>>
        {
        };

        /** Concatenate every currently valid frame in a keyed collection.
            @param ts Keyed collection of same-schema frames.
            @return One frame containing rows from all valid keyed values.
            @par Python example
            @code{.py}
            rows = hg.ungroup(rows_by_symbol)
            @endcode */
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

        /** Replace or add columns from a structural input and project to the requested row schema.
            @param ts Original frame.
            @param columns Bundle or mapping of replacement column values.
            @return A frame with the explicitly selected output row schema.
            @par Python example
            @code{.py}
            enriched = hg.with_columns[TS[Frame[EnrichedRow]]](rows, columns)
            @endcode */
        struct with_columns
            : Operator<"with_columns", In<"ts", TS<FrameOf<ScalarVar<"R">>>>,
                       In<"columns", TsVar<"C">>,
                       Out<TS<FrameOf<ScalarVar<"O">>>>>
        {
        };
    }  // namespace data_frame
}  // namespace hgraph::stdlib

#endif  // HGRAPH_LIB_STD_OPERATORS_DATA_FRAME_H
