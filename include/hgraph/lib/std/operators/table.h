#ifndef HGRAPH_LIB_STD_OPERATORS_TABLE_H
#define HGRAPH_LIB_STD_OPERATORS_TABLE_H

#include <hgraph/types/frame.h>
#include <hgraph/types/operator_dispatch.h>
#include <hgraph/types/static_node.h>
#include <hgraph/types/static_schema.h>

#include <cstdint>

namespace hgraph::stdlib
{
    /** Row-selection policy shared by ``to_table`` and table recording. */
    enum class ToTableMode : std::int64_t
    {
        Tick = 1,
        Sample = 2,
        Snap = 3,
    };

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
     * key removals. The record/replay backends bypass tuple materialisation
     * and drive the selected ``TableTypeOps`` into Arrow builders directly.
     *
     * @param ts Time-series value or structure to flatten.
     * @param mode Tick emits deltas, Sample emits complete modified entries,
     *             and Snap emits a complete snapshot when the source ticks.
     * @return One row per scalar tick, or a tuple of rows for partitioned and
     *         frame-valued inputs.
     * @par Python example
     * @code{.py}
     * rows = hg.to_table(positions, hg.ToTableMode.Sample)
     * @endcode
     */
    struct to_table : Operator<"to_table", In<"ts", TsVar<"S">>, In<"mode", TS<ToTableMode>>,
                               Out<TsVar<"__out__">>>
    {
    };

    /** ``from_table`` — apply bitemporal tuple rows as deltas of the selected output schema. */
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

namespace hgraph::static_schema_detail
{
    template <>
    struct scalar_name<hgraph::stdlib::ToTableMode>
    {
        static constexpr std::string_view value{"ToTableMode"};
    };
}  // namespace hgraph::static_schema_detail

#if HGRAPH_ENABLE_PYTHON_USER_NODES
#include <hgraph/python/bridge_state.h>

namespace hgraph
{
    template <>
    struct python_conversion_traits<stdlib::ToTableMode>
    {
        static nb::object to_python(const stdlib::ToTableMode &value)
        {
            return nb::cast(static_cast<std::int64_t>(value));
        }

        static stdlib::ToTableMode from_python(nb::handle source)
        {
            if (nb::hasattr(source, "value"))
            {
                return static_cast<stdlib::ToTableMode>(
                    nb::cast<std::int64_t>(source.attr("value")));
            }
            return static_cast<stdlib::ToTableMode>(nb::cast<std::int64_t>(source));
        }
    };
}  // namespace hgraph
#endif  // HGRAPH_ENABLE_PYTHON_USER_NODES

namespace hgraph
{
    extern template HGRAPH_EXPORT const MemoryUtils::StoragePlan &MemoryUtils::plan_for<
        stdlib::ToTableMode>() noexcept;
    extern template HGRAPH_EXPORT const ValueOps &ops_for<stdlib::ToTableMode>() noexcept;
}  // namespace hgraph

#endif  // HGRAPH_LIB_STD_OPERATORS_TABLE_H
