#ifndef HGRAPH_LIB_STD_OPERATORS_TABLE_ROWS_H
#define HGRAPH_LIB_STD_OPERATORS_TABLE_ROWS_H

#include <hgraph/hgraph_export.h>
#include <hgraph/lib/std/operators/table.h>
#include <hgraph/types/table_type_ops.h>
#include <hgraph/types/time_series/ts_delta.h>
#include <hgraph/types/value/table_codec.h>

#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <vector>

/**
 * The SUPPORTED public seam for TS-level bitemporal table rows (RFC 0025,
 * checkpoint 3): the interned per-schema table layout and its traversal —
 * projection through recording options, row emission, replay column
 * resolution, and compound-key reassembly.  A separately built extension
 * implementing a durable record/replay backend consumes exactly this
 * header plus the public value-level ``table_codec.h`` recorders; it must
 * not reach into ``operators/impl/``.
 */
namespace hgraph::stdlib
{
    /**
     * The TS-level TABLE row layout (design record: *Record/replay, tables
     * and const_fn*, step 6) — synthesised once per (resolved TS schema,
     * bitemporal names) and interned; the value-level TableConverter is
     * untouched underneath. Cleared on registry reset (the plan-registries
     * rule). Promoted from ``impl/table_impl.h`` to this SUPPORTED public
     * header at RFC 0025 checkpoint 3 — the seam a durable-backend
     * extension consumes.
     */
    namespace table_ts_detail
    {
        using TsTableLayout = TableLayout;
        using RowSink = TableRowSink;
        using TableRecordingOptions = hgraph::TableRecordingOptions;
        using RecordingColumns = TableRecordingColumns;

        /** Project ``layout`` through ``options``. Throws when the options do
            not fit the layout - a rename list of the wrong length, or a name
            that collides after the configured prefix is applied. */
        [[nodiscard]] HGRAPH_EXPORT RecordingColumns
        recording_columns(const TsTableLayout &layout, const TableRecordingOptions &options);

        /** Attach the exact recording projection to a completed frame.
         *
         * ``stored_names`` is indexed by layout column. ``nullopt`` records
         * that the column was deliberately omitted. Replay uses this
         * writer-supplied metadata to distinguish omission from a renamed
         * optional column without inferring from position or type. Frames that
         * predate the metadata, including hand-built Arrow inputs, retain the
         * legacy explicit-name resolution rules.
         */
        [[nodiscard]] HGRAPH_EXPORT Frame annotate_recording_projection(
            Frame frame, std::span<const std::optional<std::string>> stored_names);

        [[nodiscard]] HGRAPH_EXPORT const TsTableLayout &ts_table_layout(
            const TSValueTypeMetaData *ts, std::string_view date_key, std::string_view as_of_key);
        HGRAPH_EXPORT void clear_ts_table_layouts() noexcept;

        /** ToTableMode::Tick - the per-tick mode, matching the Python enum. */
        inline constexpr Int kToTableModeTick = static_cast<Int>(ToTableMode::Tick);

        /** The ToTableMode enum meta (registers it on first use). */
        [[nodiscard]] HGRAPH_EXPORT const ValueTypeMetaData *to_table_mode_meta();

        /**
         * Emit this tick's rows to ``sink``.
         *
         * ``emit_removals`` is false when the recording omits removals: a
         * removal then does nothing at all, rather than producing a row whose
         * removed flag has nowhere to go.
         */
        HGRAPH_EXPORT void emit_rows_to(const TsTableLayout &layout, const TSInputView &ts,
                                        Int mode, DateTime now, DateTime as_of, bool emit_removals,
                                        const RowSink &sink, bool reject_empty_frames = false);

        /** Emit this tick's rows into ``out`` (TS<row> or TS<rows>). */
        HGRAPH_EXPORT void emit_rows(const TsTableLayout &layout, const TSInputView &ts, Int mode, DateTime now,
                       DateTime as_of, const TSOutputView &out);

        /** Apply a row/rows VALUE as the tick's delta at ``out``. */
        HGRAPH_EXPORT void apply_rows(const TsTableLayout &layout, const ValueView &value,
                        const TSOutputView &out);

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
        /**
         * Resolve every layout column to its position in a stored table.
         *
         * ``stored_names`` is the caller's projection, indexed by layout
         * column; an empty entry means the layout's own name. The result is
         * parallel to ``layout.keys``: a column index into ``frame``, or ``-1``
         * for a column the recording legitimately does not carry (the as-of
         * column under ``as_of: Omit``, a level's removed flag under
         * ``removes: Omit``).
         *
         * A REQUIRED column that cannot be found throws, naming it. Replay
         * never infers a column from position or type — see RFC 0019,
         * *Projection is explicit or it fails*.
         *
         * Hgraph-produced recordings carry the exact projection in Arrow
         * schema metadata. For those frames, the caller's projection must
         * match even for optional columns: omitting ``as_of_key`` or
         * ``removed_names`` cannot silently lose a column recorded under a
         * non-default name. Unannotated legacy/hand-built frames cannot make
         * that distinction, so ``as_of_named`` / ``removes_named`` retain the
         * compatibility rule that only an explicitly named optional column is
         * required.
         *
         * Resolving to positions rather than renaming the stored table keeps
         * the stored names intact, so a table that genuinely contains a column
         * called ``__key_1__`` stays readable and the caller's projection is
         * never discarded.
         */
        [[nodiscard]] HGRAPH_EXPORT std::vector<int> resolve_replay_columns(
            const Frame &frame, const TsTableLayout &layout,
            std::span<const std::string> stored_names, bool as_of_named = false,
            bool removes_named = false);

        [[nodiscard]] HGRAPH_EXPORT Value assemble_from_paths(
            const ValueTypeMetaData *meta, std::span<const std::vector<std::size_t>> paths,
            std::span<const Value> leaves);

    }  // namespace table_ts_detail
}  // namespace hgraph::stdlib

#endif  // HGRAPH_LIB_STD_OPERATORS_TABLE_ROWS_H
