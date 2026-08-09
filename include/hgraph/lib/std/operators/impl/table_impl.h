#ifndef HGRAPH_LIB_STD_OPERATORS_IMPL_TABLE_IMPL_H
#define HGRAPH_LIB_STD_OPERATORS_IMPL_TABLE_IMPL_H

#include <hgraph/lib/std/operators/table.h>
#include <hgraph/types/operator_dispatch.h>
#include <hgraph/types/operator_type_resolution.h>
#include <hgraph/types/record_replay.h>
#include <hgraph/types/static_node.h>
#include <hgraph/types/time_series/ts_delta.h>
#include <hgraph/types/value/table_codec.h>

#include <string>
#include <string_view>
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

        [[nodiscard]] HGRAPH_EXPORT const TsTableLayout &ts_table_layout(
            const TSValueTypeMetaData *ts, std::string_view date_key, std::string_view as_of_key);
        void clear_ts_table_layouts() noexcept;

        /** The ToTableMode enum meta (registers it on first use). */
        [[nodiscard]] const ValueTypeMetaData *to_table_mode_meta();
        [[nodiscard]] Value                    to_table_mode_value(Int member);

        /** Emit this tick's rows into ``out`` (TS<row> or TS<rows>). */
        void emit_rows(const TsTableLayout &layout, const TSInputView &ts, Int mode, DateTime now,
                       DateTime as_of, const TSOutputView &out);

        /** Apply a row/rows VALUE as the tick's delta at ``out``. */
        void apply_rows(const TsTableLayout &layout, const ValueView &value, const TSOutputView &out);
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
