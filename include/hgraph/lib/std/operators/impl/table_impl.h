#ifndef HGRAPH_LIB_STD_OPERATORS_IMPL_TABLE_IMPL_H
#define HGRAPH_LIB_STD_OPERATORS_IMPL_TABLE_IMPL_H

#include <hgraph/lib/std/operators/table.h>
#include <hgraph/types/operator_dispatch.h>
#include <hgraph/types/operator_type_resolution.h>
#include <hgraph/types/record_replay.h>
#include <hgraph/types/static_node.h>
#include <hgraph/types/time_series/ts_delta.h>
#include <hgraph/types/value/table_codec.h>

#include <optional>
#include <string>
#include <string_view>
#include <span>
#include <vector>

namespace hgraph::stdlib
{
    using namespace hgraph::operator_type_resolution;

    /**
     * The TS-level TABLE row layout (design record: *Record/replay, tables
     * and const_fn*, step 6) — synthesised once per (resolved TS schema,
     * bitemporal names) and interned (the json_ts_detail precedent: TS-level
     * walkers live with the operator impls; the value-level TableConverter
     * is untouched underneath). Cleared on registry reset (the
     * plan-registries rule).
     */
    namespace table_ts_detail
    {
        struct TsTableLayout
        {
            /** One flattened leaf column of the row tuple. */
            struct Column
            {
                std::string               name{};
                const ValueTypeMetaData  *leaf{nullptr};
                std::vector<std::size_t>  ts_path{};      ///< TSB child chain below the leaf TS
                std::vector<std::size_t>  value_path{};   ///< field chain within that node's value
            };

            /** One TSD nesting level: its removed flag + flattened key columns. */
            struct Level
            {
                const ValueTypeMetaData               *key_meta{nullptr};
                std::size_t                            removed_col{0};
                std::size_t                            first_key_col{0};
                std::vector<std::vector<std::size_t>>  key_paths{};   ///< per key column, path into the key value
            };

            const TSValueTypeMetaData             *ts_schema{nullptr};
            const TSValueTypeMetaData             *leaf_ts{nullptr};    ///< the TS below all TSD levels
            std::vector<Level>                     levels{};
            std::vector<Column>                    value_cols{};
            std::size_t                            value_col_start{0};
            bool                                   is_multi_row{false};   ///< Frame-valued leaf
            const TableConverter                  *frame_converter{nullptr};   ///< when is_multi_row
            std::vector<std::string>               keys{};       ///< ALL column names in row order
            std::vector<const ValueTypeMetaData *> col_metas{};  ///< per column (incl. date/as_of)
            std::vector<std::string>               partition_keys{};
            std::vector<std::string>               removed_keys{};
            std::string                            date_key{};
            std::string                            as_of_key{};
            const ValueTypeMetaData               *row_meta{nullptr};    ///< fixed tuple over col_metas
            const ValueTypeMetaData               *rows_meta{nullptr};   ///< variadic tuple of row_meta
            const TSValueTypeMetaData             *output_ts{nullptr};   ///< TS(row_meta) or TS(rows_meta)

            [[nodiscard]] bool partitioned() const noexcept { return !levels.empty(); }
            [[nodiscard]] bool multi() const noexcept { return partitioned() || is_multi_row; }
        };


        /**
         * Where a row's cells go: written straight to their destination as
         * they are resolved, with ``end_row`` closing the row. Columns the
         * traversal did not deliver were not present, and each sink says what
         * that means in its own terms - unset in a planned row, null in a
         * builder.
         */
        struct RowSink
        {
            void *context{nullptr};
            void (*cell)(void *context, std::size_t column, const ValueView &value){nullptr};
            void (*end_row)(void *context){nullptr};
        };

        /**
         * How a recording is shaped (RFC 0019, step 3).
         *
         * Supplied to the recorder rather than looked up, with the global
         * ``record_replay::Config`` as the default. Two recordings in one
         * graph therefore differ by being configured differently, not through
         * a registry keyed on name - which is what pulled the data-frame
         * adaptor into a second override dictionary beside ``Config``.
         *
         * Defaults follow today's configuration, including ``removes``, which
         * the adaptor's ``_OverrideState`` leaves off.
         */
        struct TableRecordingOptions
        {
            enum class AsOf
            {
                Track,   ///< an as-of column carrying the evaluation as-of
                Omit,    ///< no as-of column at all
                Fixed,   ///< an as-of column carrying ``as_of_value``
            };
            enum class Removes
            {
                Omit,    ///< no removed columns, and no rows for removals
                Track,   ///< a removed flag per level
            };

            AsOf                     as_of{AsOf::Track};
            std::optional<DateTime>  as_of_value{};
            Removes                  removes{Removes::Omit};
            /** Empty means the layout's own name. */
            std::string              date_key{};
            std::string              as_of_key{};
            /** One name per FLATTENED key column, not per level: a compound key
                such as ``TSD[tuple[int, str], ...]`` occupies several columns.
                Empty means the layout's names. */
            std::vector<std::string> partition_names{};
            /** One name per level; empty means the layout's names. */
            std::vector<std::string> removed_names{};
            /** Prefix for the columns of an expanded frame-valued leaf. */
            std::string              frame_prefix{};
        };

        /**
         * The columns a recording actually has, projected from a layout.
         *
         * ``source`` maps each output column back to its column in the
         * layout's row, because omitting a column shifts every index after it
         * and the row cells are still laid out by the layout.
         */
        struct RecordingColumns
        {
            std::vector<std::string>               names{};
            std::vector<const ValueTypeMetaData *> metas{};
            std::vector<std::size_t>               source{};

            [[nodiscard]] std::size_t size() const noexcept { return names.size(); }
        };

        /** Project ``layout`` through ``options``. Throws when the options do
            not fit the layout - a rename list of the wrong length, or a name
            that collides after the configured prefix is applied. */
        [[nodiscard]] HGRAPH_EXPORT RecordingColumns recording_columns(
            const TsTableLayout &layout, const TableRecordingOptions &options);

        [[nodiscard]] HGRAPH_EXPORT const TsTableLayout &ts_table_layout(
            const TSValueTypeMetaData *ts, std::string_view date_key, std::string_view as_of_key);
        void clear_ts_table_layouts() noexcept;

        /** ToTableMode::Tick - the per-tick mode, matching the Python enum. */
        inline constexpr Int kToTableModeTick = 1;

        /** The ToTableMode enum meta (registers it on first use). */
        [[nodiscard]] const ValueTypeMetaData *to_table_mode_meta();
        [[nodiscard]] Value                    to_table_mode_value(Int member);

        /**
         * Emit this tick's rows to ``sink``.
         *
         * ``emit_removals`` is false when the recording omits removals: a
         * removal then does nothing at all, rather than producing a row whose
         * removed flag has nowhere to go.
         */
        HGRAPH_EXPORT void emit_rows_to(const TsTableLayout &layout, const TSInputView &ts, Int mode,
                                        DateTime now, DateTime as_of, bool emit_removals,
                                        const RowSink &sink);

        /** Emit this tick's rows into ``out`` (TS<row> or TS<rows>). */
        void emit_rows(const TsTableLayout &layout, const TSInputView &ts, Int mode, DateTime now,
                       DateTime as_of, const TSOutputView &out);

        /** Apply a row/rows VALUE as the tick's delta at ``out``. */
        void apply_rows(const TsTableLayout &layout, const ValueView &value, const TSOutputView &out);

        /**
         * Rebuild a value from its flattened leaves — the exact inverse of the
         * flattening a layout applies to a TSD key.
         *
         * A compound key (``TSD[tuple[int, str], ...]``, or a bundle, nested to
         * any depth) occupies one column per leaf, so replay cannot read the
         * key back as a single cell the way an atomic key allows. ``paths`` are
         * the level's ``key_paths`` and ``leaves`` the cells read from those
         * columns, parallel and in column order; the two are consumed together
         * in the field order ``flatten_value`` emitted them in.
         *
         * Every leaf must carry a value: a key missing a component is not a
         * key, and silently building a partial one would replay ticks under a
         * key that never existed.
         */
        [[nodiscard]] HGRAPH_EXPORT Value assemble_from_paths(
            const ValueTypeMetaData *meta, std::span<const std::vector<std::size_t>> paths,
            std::span<const Value> leaves);

        /**
         * Apply the recorded rows ``[first, first + count)`` as one
         * frame-valued tick at ``out``.
         *
         * A ``TS[Frame[Row]]`` leaf records one row per FRAME row, so a tick is
         * a run of recorded rows sharing a value time rather than a single row.
         * The recorded value columns are the frame's own columns, so the tick's
         * frame is a projection of the recording — the columns selected and the
         * run sliced — not a cell-by-cell rebuild.
         */
        HGRAPH_EXPORT void apply_recorded_frame_rows(const TsTableLayout &layout, const Frame &recorded,
                                                     std::int64_t first, std::int64_t count,
                                                     const TSOutputView &out);
    }  // namespace table_ts_detail

    struct TableLayoutState
    {
        const table_ts_detail::TsTableLayout *layout{nullptr};
    };
}  // namespace hgraph::stdlib

namespace hgraph::static_schema_detail
{
    template <>
    struct scalar_name<stdlib::TableLayoutState>
    {
        static constexpr std::string_view value{"TableLayoutState"};
    };

    template <>
    struct scalar_name<TableCodecState>
    {
        static constexpr std::string_view value{"TableCodecState"};
    };
}  // namespace hgraph::static_schema_detail

namespace hgraph::stdlib
{
    /**
     * ``to_table`` — the tuple-row parity operator. Output schema computed
     * from the resolved input (the window-operator precedent); the layout is
     * resolved once in ``start`` and carried in State (the lifecycle form of
     * the builder pattern).
     */
    struct to_table_rows_impl
    {
        static constexpr auto name = "to_table";

        static void resolve_default_types(ResolutionMap &resolution, OperatorCallContext context)
        {
            if (output_bound(resolution)) { return; }
            const auto *schema = time_series_schema_at(context, 0);   // any TS kind
            if (schema == nullptr) { return; }
            const auto config = record_replay::config(context.global_state);
            bind_output(resolution,
                        table_ts_detail::ts_table_layout(schema, config.date_key, config.as_of_key).output_ts);
        }

        static std::vector<std::pair<std::string_view, Value>> defaults()
        {
            return {{"mode", table_ts_detail::to_table_mode_value(1)}};   // ToTableMode.Tick
        }

        static void start(In<"ts", TsVar<"S">, InputValidity::Unchecked> ts, GlobalStateView gs,
                          State<TableLayoutState> state)
        {
            const auto config = record_replay::config(gs);
            state.set(TableLayoutState{
                &table_ts_detail::ts_table_layout(ts.base().schema(), config.date_key, config.as_of_key)});
        }

        static void eval(In<"ts", TsVar<"S">> ts, In<"mode", TS<ScalarVar<"M">>, InputValidity::Unchecked> mode,
                         State<TableLayoutState> state, GlobalStateView gs, DateTime now,
                         Out<TsVar<"__out__">> out)
        {
            if (!ts.modified()) { return; }   // a mode tick alone emits nothing
            Int         mode_value = 1;
            const auto &mode_view  = mode.base();
            if (mode_view.valid())
            {
                mode_value = *static_cast<const Int *>(mode_view.value().data());
            }
            const auto as_of = record_replay::config(gs).as_of.value_or(now);
            table_ts_detail::emit_rows(*state.get().layout, ts.base(), mode_value, now, as_of,
                                       static_cast<const TSOutputView &>(out));
        }
    };

    /**
     * ``from_table`` — applies each incoming row as this tick's delta at the
     * resolved output (rows apply in order; removed flags become TSD key
     * removals; a multi-row Frame output is rebuilt from the tick's rows).
     */
    struct from_table_rows_impl
    {
        static constexpr auto name = "from_table";

        static void start(Out<TsVar<"O">> out, GlobalStateView gs, State<TableLayoutState> state)
        {
            const auto &erased = static_cast<const TSOutputView &>(out);
            const auto  config = record_replay::config(gs);
            state.set(TableLayoutState{
                &table_ts_detail::ts_table_layout(erased.schema(), config.date_key, config.as_of_key)});
        }

        static void eval(In<"ts", TsVar<"T">> ts, State<TableLayoutState> state, Out<TsVar<"O">> out)
        {
            table_ts_detail::apply_rows(*state.get().layout, ts.value(),
                                        static_cast<const TSOutputView &>(out));
        }
    };

    /**
     * ``from_table_const`` — const-evaluable (the const_fn ruling, P1): the
     * eager kernel extracts the frame's last row at the resolved output
     * schema; the wired form emits the same value once at start.
     */
    struct from_table_const_impl
    {
        static constexpr auto name              = "from_table_const";
        static constexpr bool schedule_on_start = true;

        static Value const_eval(const TSValueTypeMetaData *resolved_output, OperatorCallContext context)
        {
            const auto *frame = context.scalar_as<Frame>("value");
            if (frame == nullptr || !frame->has_value() || frame_rows(*frame) == 0) { return Value{}; }
            const auto  config = record_replay::config(context.global_state);
            const auto &converter =
                table_converter(resolved_output->value_schema, config.date_key, config.as_of_key);
            return read_row(converter, *frame, frame_rows(*frame) - 1);
        }

        static void eval(Scalar<"value", Frame> value, GlobalStateView gs, Out<TsVar<"O">> out)
        {
            const auto &erased = static_cast<const TSOutputView &>(out);
            const auto &frame  = value.value();
            if (!frame.has_value() || frame_rows(frame) == 0) { return; }
            const auto  config = record_replay::config(gs);
            const auto &converter =
                table_converter(erased.schema()->value_schema, config.date_key, config.as_of_key);
            Value       row       = read_row(converter, frame, frame_rows(frame) - 1);
            out.apply(row.view());
        }
    };

    /** Register the table operator overloads. */
    void register_table_operators();
}  // namespace hgraph::stdlib

#endif  // HGRAPH_LIB_STD_OPERATORS_IMPL_TABLE_IMPL_H
