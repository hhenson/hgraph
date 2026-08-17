#ifndef HGRAPH_LIB_STD_OPERATORS_IMPL_TABLE_IMPL_H
#define HGRAPH_LIB_STD_OPERATORS_IMPL_TABLE_IMPL_H

#include <hgraph/lib/std/operators/table.h>
#include <hgraph/lib/std/operators/table_rows.h>
#include <hgraph/types/operator_dispatch.h>
#include <hgraph/types/operator_type_resolution.h>
#include <hgraph/types/table_config.h>
#include <hgraph/types/static_node.h>
#include <hgraph/types/table_type_ops.h>
#include <hgraph/types/time_series/ts_delta.h>
#include <hgraph/types/value/table_codec.h>

#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <vector>

namespace hgraph::stdlib
{
    using namespace hgraph::operator_type_resolution;


    struct TableLayoutState
    {
        const table_ts_detail::TsTableLayout *layout{nullptr};
        /** Start-resolved run-fixed as-of override; reading it per tick from
            GlobalState copies the whole table configuration (two heap
            strings). */
        std::optional<DateTime> fixed_as_of{};
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
            if (output_bound(resolution))
            {
                return;
            }
            const auto *schema = time_series_schema_at(context, 0);  // any TS kind
            if (schema == nullptr)
            {
                return;
            }
            const auto config = table::config(context.global_state);
            bind_output(resolution,
                        table_ts_detail::ts_table_layout(schema, config.date_key, config.as_of_key)
                            .output_ts);
        }

        static auto defaults()
        {
            return std::tuple{arg<"mode">(ToTableMode::Tick)};
        }

        static void start(In<"ts", TsVar<"S">, InputValidity::Unchecked> ts, GlobalStateView gs,
                          State<TableLayoutState> state)
        {
            const auto config = table::config(gs);
            state.set(TableLayoutState{
                .layout = &table_ts_detail::ts_table_layout(ts.base().schema(), config.date_key,
                                                            config.as_of_key),
                .fixed_as_of = config.as_of});
        }

        static void eval(In<"ts", TsVar<"S">>                                  ts,
                         In<"mode", TS<ToTableMode>, InputValidity::Unchecked> mode,
                         State<TableLayoutState> state, GlobalStateView gs, DateTime now,
                         Out<TsVar<"__out__">> out)
        {
            if (!ts.modified())
            {
                return;
            }  // a mode tick alone emits nothing
            static_cast<void>(gs);
            const Int mode_value =
                mode.valid() ? static_cast<Int>(mode.value()) : table_ts_detail::kToTableModeTick;
            const auto resolved = state.get();
            const auto as_of    = resolved.fixed_as_of.value_or(now);
            table_ts_detail::emit_rows(*resolved.layout, ts.base(), mode_value, now, as_of,
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
            const auto  config = table::config(gs);
            state.set(TableLayoutState{&table_ts_detail::ts_table_layout(
                erased.schema(), config.date_key, config.as_of_key)});
        }

        static void eval(In<"ts", TsVar<"T">> ts, State<TableLayoutState> state,
                         Out<TsVar<"O">> out)
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
        static constexpr auto name = "from_table_const";
        static constexpr bool schedule_on_start = true;

        static Value const_eval(const TSValueTypeMetaData *resolved_output,
                                OperatorCallContext        context)
        {
            const auto *frame = context.scalar_as<Frame>("value");
            if (frame == nullptr || !frame->has_value() || frame_rows(*frame) == 0)
            {
                return Value{};
            }
            const auto  config = table::config(context.global_state);
            const auto &converter =
                table_converter(resolved_output->value_schema, config.date_key, config.as_of_key);
            return read_row(converter, *frame, frame_rows(*frame) - 1);
        }

        static void eval(Scalar<"value", Frame> value, GlobalStateView gs, Out<TsVar<"O">> out)
        {
            const auto &erased = static_cast<const TSOutputView &>(out);
            const auto &frame = value.value();
            if (!frame.has_value() || frame_rows(frame) == 0)
            {
                return;
            }
            const auto  config = table::config(gs);
            const auto &converter =
                table_converter(erased.schema()->value_schema, config.date_key, config.as_of_key);
            Value row = read_row(converter, frame, frame_rows(frame) - 1);
            out.apply(row.view());
        }
    };

    /** Register the table operator overloads. */
    void register_table_operators();
}  // namespace hgraph::stdlib

#endif  // HGRAPH_LIB_STD_OPERATORS_IMPL_TABLE_IMPL_H
