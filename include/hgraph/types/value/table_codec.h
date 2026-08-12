#ifndef HGRAPH_TYPES_VALUE_TABLE_CODEC_H
#define HGRAPH_TYPES_VALUE_TABLE_CODEC_H

#include <hgraph/hgraph_export.h>
#include <hgraph/types/frame.h>
#include <hgraph/types/metadata/value_type_meta_data.h>
#include <hgraph/types/value/value.h>
#include <hgraph/types/value/value_view.h>
#include <hgraph/util/date_time.h>

#include <cstdint>
#include <memory>
#include <span>
#include <string>
#include <string_view>
#include <vector>

namespace arrow
{
    class Array;
    class ArrayBuilder;
    class DataType;
    class Schema;
    struct Datum;
}  // namespace arrow

namespace hgraph
{
    /**
     * Interned per-schema table converter — the Arrow arm of the serializer-
     * ops pattern (design record: *Record/replay, tables and const_fn*, P4).
     * One converter per value schema: the flattened column layout (Python's
     * ``extract_table_schema``) plus per-column append/read fn-ptrs that
     * write DIRECTLY into Arrow array builders and read from Arrow arrays —
     * no per-tick row tuples.
     *
     * The row shape is bitemporal: ``[date_key, as_of_key, *value columns]``
     * with the key names supplied at synthesis. Bundles flatten
     * to dotted column names. v1 covers atomic leaves and depth-1 bundles;
     * TSD partition keys land with the record/replay backend (step 4).
     */
    class HGRAPH_EXPORT TableConverter
    {
      public:
        struct Column
        {
            using AppendFn = void (*)(const Column &, const ValueView &leaf, arrow::ArrayBuilder &);
            using ReadFn   = Value (*)(const Column &, const arrow::Array &, std::int64_t row);

            std::string                      name{};
            const ValueTypeMetaData         *leaf_meta{nullptr};
            std::vector<std::size_t>         path{};   ///< field-index chain into the value
            std::shared_ptr<arrow::DataType> type{};
            AppendFn                         append{nullptr};
            ReadFn                           read{nullptr};
        };

        const ValueTypeMetaData        *meta{nullptr};
        std::shared_ptr<arrow::Schema>  arrow_schema{};   ///< [date, as_of, *columns]
        std::string                     date_key{};
        std::string                     as_of_key{};
        std::vector<Column>             columns{};        ///< the value columns (date/as_of excluded)
    };

    /** The converter interned by value schema and bitemporal column names. */
    [[nodiscard]] HGRAPH_EXPORT const TableConverter &table_converter(
        const ValueTypeMetaData *meta,
        std::string_view date_key = "__date_time__",
        std::string_view as_of_key = "__as_of__");

    /** Clear the interned converters (registry reset). */
    HGRAPH_EXPORT void clear_table_converters() noexcept;

    /**
     * Node-State payload carrying the converter resolved in ``start`` (the
     * lifecycle form of the builder pattern: compose once, read per tick).
     */
    struct TableCodecState
    {
        const TableConverter *converter{nullptr};
    };

    /**
     * Multi-tick frame accumulator: append one bitemporal row per tick
     * directly into Arrow array builders (the P4 fused path — no per-tick
     * row values), then ``finish`` into a ``Frame``. Constructed in a node's
     * ``start`` (against the pre-resolved converter) and finished in
     * ``stop``. Move-only; Arrow internals stay behind the pimpl.
     */
    /**
     * Records table rows straight into Arrow builders, driven by a COLUMN
     * DESCRIPTION rather than by a value schema (RFC 0019).
     *
     * ``FrameRecorder`` is built from a ``TableConverter``, so it can only
     * record what a converter can describe: atomic leaves and depth-1 bundles.
     * A partitioned time-series is described by its table layout instead - the
     * flattened column names and their leaf metadata, which already account for
     * ``TSD`` key columns, removed flags and expanded frames - and that is all
     * this needs.
     *
     * Rows arrive as cells already resolved to leaves, which is what the row
     * sink in ``table_impl`` produces, so no row value is built on the way in.
     * An unset cell appends a null: a removal row carries no value columns, and
     * a tick that did not modify a column leaves it absent.
     *
     * Build-time construction, per-row appends; the builders are the only
     * accumulation.
     */
    class HGRAPH_EXPORT TableRecorder
    {
      public:
        /** ``names`` and ``leaf_metas`` are parallel and in row order. */
        TableRecorder(std::span<const std::string> names,
                      std::span<const ValueTypeMetaData *const> leaf_metas);
        TableRecorder(TableRecorder &&) noexcept;
        TableRecorder &operator=(TableRecorder &&) noexcept;
        TableRecorder(const TableRecorder &)            = delete;
        TableRecorder &operator=(const TableRecorder &) = delete;
        ~TableRecorder();

        /** Write one cell of the row under construction. Delivering a column
            twice in one row is an error rather than a silent overwrite. */
        void append_cell(std::size_t column, const ValueView &value);

        /** Close the row. Columns this row never delivered append a NULL: a
            removal row has no value columns, and a tick that did not modify a
            column leaves it absent, so a default would record zero where
            nothing happened. */
        void end_row();

        [[nodiscard]] std::int64_t rows() const noexcept;

        /** The accumulated rows, leaving the recorder empty. */
        [[nodiscard]] Frame finish();

        [[nodiscard]] const std::shared_ptr<arrow::Schema> &arrow_schema() const noexcept;

      private:
        struct Impl;
        std::unique_ptr<Impl> impl_;
    };

    class HGRAPH_EXPORT FrameRecorder
    {
      public:
        explicit FrameRecorder(const TableConverter &converter);
        FrameRecorder(FrameRecorder &&) noexcept;
        FrameRecorder &operator=(FrameRecorder &&) noexcept;
        FrameRecorder(const FrameRecorder &)            = delete;
        FrameRecorder &operator=(const FrameRecorder &) = delete;
        ~FrameRecorder();

        void append(DateTime value_time, DateTime as_of, const ValueView &value);
        [[nodiscard]] Frame finish();

      private:
        struct Impl;
        std::unique_ptr<Impl> impl_;
    };

    /** One cell, read back as its leaf type. The inverse of what
        ``TableRecorder`` appends, for a column the recorder described rather
        than a converter did - a ``TSD`` key column has no place in any value
        schema. */
    [[nodiscard]] HGRAPH_EXPORT Value read_table_cell(const ValueTypeMetaData *leaf_meta,
                                                     const arrow::Array &array, std::int64_t row);

    /** The ``date_key`` (value-time) column entry for ``row``. */
    [[nodiscard]] HGRAPH_EXPORT DateTime frame_value_time(const TableConverter &converter, const Frame &frame,
                                                          std::int64_t row);

    /** Build a one-row bitemporal frame for one tick's value. */
    [[nodiscard]] HGRAPH_EXPORT Frame single_row_frame(const TableConverter &converter, DateTime value_time,
                                                       DateTime as_of, const ValueView &value);

    /**
     * Reconstruct the value from ``row`` of ``frame``. Column resolution is
     * BY NAME (the input-minimum rule: extra columns in the frame are
     * ignored; a missing required column throws).
     */
    [[nodiscard]] HGRAPH_EXPORT Value read_row(const TableConverter &converter, const Frame &frame,
                                               std::int64_t row);

    /**
     * Build a frame from row TUPLE values, value columns only — no
     * bitemporal columns (the ``from_table`` frame rebuild; design record
     * step 6). ``first_column`` is the tuple index of the first value
     * column; unset tuple cells become Arrow nulls.
     */
    [[nodiscard]] HGRAPH_EXPORT Frame frame_from_rows(const TableConverter &converter,
                                                      std::span<const ValueView> rows,
                                                      std::size_t first_column);

    /**
     * Build a frame from whole row VALUES (each row a value of the
     * converter's schema — bundle or atomic), value columns only, no
     * bitemporal columns (to_data_frame / group_by; design record step 6).
     */
    [[nodiscard]] HGRAPH_EXPORT Frame frame_from_values(const TableConverter &converter,
                                                        std::span<const Value> rows);

    /** The frame's own column names, in schema order. */
    [[nodiscard]] HGRAPH_EXPORT std::vector<std::string> frame_column_names(const Frame &frame);

    /**
     * Read one typed scalar from an Arrow array. An Arrow null yields an
     * empty ``Value``. The array's physical type is validated against
     * ``leaf`` before the value is decoded.
     */
    [[nodiscard]] HGRAPH_EXPORT Value array_cell(const arrow::Array &array,
                                                 const ValueTypeMetaData *leaf,
                                                 std::int64_t row);

    /** Encode one native scalar as an Arrow Datum using the frame codec's
        type mapping. An empty Value produces a null scalar Datum. */
    [[nodiscard]] HGRAPH_EXPORT arrow::Datum arrow_scalar(
        const ValueView &value, const ValueTypeMetaData *leaf);

    /**
     * Read one cell by column NAME at ``row``, typed by ``leaf`` (an atomic
     * scalar meta). An Arrow null yields an empty ``Value``; a missing
     * column throws (the input-minimum rule).
     */
    [[nodiscard]] HGRAPH_EXPORT Value frame_cell(const Frame &frame, std::string_view column,
                                                 const ValueTypeMetaData *leaf, std::int64_t row);

    /** Rename columns per (from, to) pairs (convert frame->frame mapping). */
    [[nodiscard]] HGRAPH_EXPORT Frame frame_rename_columns(
        const Frame &frame, std::span<const std::pair<std::string, std::string>> renames);
}  // namespace hgraph

#endif  // HGRAPH_TYPES_VALUE_TABLE_CODEC_H
