#ifndef HGRAPH_LIB_STD_OPERATORS_IO_H
#define HGRAPH_LIB_STD_OPERATORS_IO_H

#include <hgraph/types/operator_dispatch.h>
#include <hgraph/types/primitive_types.h>
#include <hgraph/types/static_node.h>
#include <hgraph/types/static_schema.h>
#include <hgraph/types/value_callable.h>

namespace hgraph::stdlib
{
    /**
     * I/O, logging and record/replay operator **definitions** (markers only). Mirrors the
     * Python ``hgraph`` debug tools (``_debug_tools.py``), graph-utility sinks
     * (``_graph_operators.py``) and record/replay operators (``_record_replay.py``). The
     * sinks (``debug_print`` / ``null_sink`` / ``print_`` / ``log_`` / ``assert_`` /
     * ``record`` / ``compare``) have no output.
     *
     * .. note::
     *
     *    The data-frame record/replay operators are deferred with the rest of the table /
     *    data-frame family (see *conversion*).
     */

    /** ``debug_print`` — print ``label: value`` on each tick of ``ts`` (a diagnostic sink).
        (Python also takes ``print_delta`` / ``sample`` — not yet modelled.) */
    struct debug_print : Operator<"debug_print", Scalar<"label", Str>, In<"ts", TsVar<"S">>>
    {
    };

    /** ``null_sink`` — consume ``ts`` and do nothing (a terminal sink). */
    struct null_sink : Operator<"null_sink", In<"ts", TsVar<"S">>>
    {
    };

    /** ``stop_engine`` — request an orderly engine stop after this cycle. */
    struct stop_engine : Operator<"stop_engine", In<"ts", SIGNAL>, Scalar<"msg", Str>>
    {
    };

    /** ``apply`` — invoke a ticking runtime callable and publish its result. */
    struct apply_op : Operator<"apply", In<"fn", TS<ValueCallable>>, VarIn<"args", TsVar<"S">>,
                               VarKwIn<"kwargs">, Out<TsVar<"O">>>
    {
    };

    /** ``call`` — invoke a ticking runtime callable for side effects. */
    struct call_op : Operator<"call", In<"fn", TS<ValueCallable>>, VarIn<"args", TsVar<"S">>,
                              VarKwIn<"kwargs">>
    {
    };

    /** ``print_`` — format and write the supplied values to std-out (a sink, variadic args). */
    struct print_ : Operator<"print_", In<"fmt", TS<Str>>, In<"args", TsVar<"A">>>
    {
    };

    /** ``log_`` — format and log the supplied values at ``level`` (a sink,
        with positional and named time-series arguments). */
    struct log_ : Operator<"log_", In<"fmt", TS<Str>>, In<"args", TsVar<"A">>,
                           Scalar<"level", Int>, Scalar<"sample_count", Int>>
    {
    };

    /** ``assert_`` — assert ``condition`` holds, raising ``error_msg`` otherwise (a sink). */
    struct assert_ : Operator<"assert_", In<"condition", TS<Bool>>, Scalar<"error_msg", Str>>
    {
    };

    /** ``__print_sink`` — the runtime half of ``print_`` (internal; wired by
        the print_ compose with the arguments packed into one bundle). */
    struct print_sink_op
        : Operator<"__print_sink", In<"fmt", TS<Str>>, In<"args", TsVar<"A">>, Scalar<"to_stdout", Bool>>
    {
    };

    /** ``__log_sink`` — the runtime half of ``log_`` after its positional and
        named arguments have been packed into one bundle. */
    struct log_sink_op
        : Operator<"__log_sink", In<"fmt", TS<Str>>, In<"args", TsVar<"A">>,
                   Scalar<"level", Int>, Scalar<"sample_count", Int>>
    {
    };

    /** ``__assert_fmt`` — the runtime half of the format-args ``assert_``. */
    struct assert_fmt_op
        : Operator<"__assert_fmt", In<"condition", TS<Bool>>, Scalar<"error_msg", Str>, In<"args", TsVar<"A">>>
    {
    };

    /** Internal packed-argument runtime nodes used by ``apply`` / ``call``. */
    struct apply_value_callable_op
        : Operator<"__apply_value_callable", In<"fn", TS<ValueCallable>>,
                   In<"args", TsVar<"A">>, Scalar<"positional_count", Int>, Out<TsVar<"O">>>
    {
    };

    struct call_value_callable_op
        : Operator<"__call_value_callable", In<"fn", TS<ValueCallable>>,
                   In<"args", TsVar<"A">>, Scalar<"positional_count", Int>>
    {
    };

    /** ``record`` — record ``ts`` under ``key`` (a sink). */
    struct record : Operator<"record", In<"ts", TsVar<"S">>, Scalar<"key", Str>>
    {
    };

    /** ``replay`` — replay a recorded series for ``key`` as the requested output type (a source). */
    struct replay : Operator<"replay", Scalar<"key", Str>, Out<TsVar<"O">>>
    {
    };

    /** ``replay_const`` — replay the const value(s) at/under ``key`` valid up to the start time. */
    struct replay_const : Operator<"replay_const", Scalar<"key", Str>, Out<TsVar<"O">>>
    {
    };

    /** ``compare`` — the backtesting comparison sink (COMPARE mode): records
        per-tick equality of ``lhs`` vs ``rhs`` through the registered frame
        store (P6) under ``fq_recordable_id.__compare__``. */
    struct compare : Operator<"compare", In<"lhs", TsVar<"S">>, In<"rhs", TsVar<"S">>,
                              Scalar<"recordable_id", Str>>
    {
    };
}  // namespace hgraph::stdlib

#endif  // HGRAPH_LIB_STD_OPERATORS_IO_H
