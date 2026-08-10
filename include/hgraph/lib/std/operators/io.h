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

    /** Print a labelled representation of each source tick for graph diagnostics.
        @param label Wiring-time prefix identifying the stream.
        @param ts Value printed when it ticks.
        @param print_delta Print only the tick delta instead of the full current value when supported.
        @param sample Emit one diagnostic line for every nth source tick.
        @return No output; this is a diagnostic sink.
        @par Python example
        @code{.py}
        hg.debug_print("price", price)
        @endcode */
    struct debug_print : Operator<"debug_print", Scalar<"label", Str>, In<"ts", TsVar<"S">>>
    {
    };

    /** Consume a stream without producing output or side effects.
        Use this to make an otherwise unused branch part of the executable graph.
        @param ts Stream to keep connected and active.
        @return No output.
        @par Python example
        @code{.py}
        hg.null_sink(background_updates)
        @endcode */
    struct null_sink : Operator<"null_sink", In<"ts", TsVar<"S">>>
    {
    };

    /** Request a graceful graph-engine stop when the trigger ticks.
        The current evaluation cycle completes before execution terminates.
        @param ts Trigger signal; its value is ignored.
        @param msg Optional diagnostic reason attached to the stop request.
        @return No output.
        @par Python example
        @code{.py}
        hg.stop_engine(done, msg="processing complete")
        @endcode */
    struct stop_engine : Operator<"stop_engine", In<"ts", SIGNAL>, Scalar<"msg", Str>>
    {
    };

    /** Invoke the latest runtime callable when it or one of its inputs ticks.
        This differs from higher-order graph operators: ``fn`` is a value available at
        runtime, not a graph callable compiled at wiring time.
        @param fn Time series carrying the callable.
        @param args Positional values supplied to the callable.
        @param kwargs Named values supplied to the callable.
        @return The callable's result.
        @par Python example
        @code{.py}
        result = hg.apply(runtime_function, lhs, rhs)
        @endcode */
    struct apply_op : Operator<"apply", In<"fn", TS<ValueCallable>>, VarIn<"args", TsVar<"S">>,
                               VarKwIn<"kwargs">, Out<TsVar<"O">>>
    {
    };

    /** Invoke a runtime callable for side effects and discard its return value.
        @param fn Time series carrying the callable.
        @param args Positional values supplied to the callable.
        @param kwargs Named values supplied to the callable.
        @return No output.
        @par Python example
        @code{.py}
        hg.call(runtime_callback, event)
        @endcode */
    struct call_op : Operator<"call", In<"fn", TS<ValueCallable>>, VarIn<"args", TsVar<"S">>,
                              VarKwIn<"kwargs">>
    {
    };

    /** Format time-series values and write a line to standard output when they tick.
        @param fmt Python-style format string, supplied as a port or liftable value.
        @param args Positional and packed named values referenced by the format string.
        @param __sample__ Optional sampling interval for reducing output frequency.
        @return No output.
        @par Python example
        @code{.py}
        hg.print_("{}: {:.2f}", symbol, price)
        @endcode */
    struct print_ : Operator<"print_", In<"fmt", TS<Str>>, In<"args", TsVar<"A">>>
    {
    };

    /** Format time-series values and send them to the configured logger.
        @param fmt Python-style format string.
        @param args Positional and packed named values used by the format string.
        @param level Wiring-time logging severity.
        @param sample_count Emit one message for every ``sample_count`` qualifying ticks.
        @return No output.
        @par Python example
        @code{.py}
        hg.log_("price={:.2f}", price, level=logging.INFO)
        @endcode */
    struct log_ : Operator<"log_", In<"fmt", TS<Str>>, In<"args", TsVar<"A">>,
                           Scalar<"level", Int>, Scalar<"sample_count", Int>>
    {
    };

    /** Raise ``AssertionError`` when a ticking condition is false.
        Additional overloads format the error message from live arguments.
        @param condition Boolean stream to enforce.
        @param error_msg Wiring-time error message or format string.
        @param args Values used to format the message.
        @param kwargs Named values used to format the message.
        @return No output.
        @par Python example
        @code{.py}
        hg.assert_(quantity >= 0, "quantity must be non-negative")
        @endcode */
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

    /** Persist source ticks through the active record/replay backend.
        The effective location combines graph recordable context with ``key``.
        @param ts Stream to record.
        @param key Wiring-time name within the current recordable context.
        @return No output.
        @par Python example
        @code{.py}
        hg.record(price, key="price")
        @endcode */
    struct record : Operator<"record", In<"ts", TsVar<"S">>, Scalar<"key", Str>>
    {
    };

    /** Replay stored ticks for a key as an explicitly selected output type.
        Replay timing and availability follow the active record/replay mode and backend.
        @param key Wiring-time name within the current recordable context.
        @return A source reproducing the recorded stream.
        @par Python example
        @code{.py}
        price = hg.replay[TS[float]](key="price")
        @endcode */
    struct replay : Operator<"replay", Scalar<"key", Str>, Out<TsVar<"O">>>
    {
    };

    /** Read the latest value recorded at or before graph start and emit it as a constant.
        @param key Wiring-time name within the current recordable context.
        @return A single value representing recorded state at graph start.
        @par Python example
        @code{.py}
        opening_price = hg.replay_const[TS[float]](key="price")
        @endcode */
    struct replay_const : Operator<"replay_const", Scalar<"key", Str>, Out<TsVar<"O">>>
    {
    };

    /** Compare two streams during backtesting and record their per-tick equality result.
        This sink is active in compare mode and stores results beneath the recordable id.
        @param lhs Actual or newly computed stream.
        @param rhs Expected or reference stream.
        @param recordable_id Optional explicit identity; context supplies it when omitted.
        @return No output.
        @par Python example
        @code{.py}
        hg.compare(actual, expected, recordable_id="pricing")
        @endcode */
    struct compare : Operator<"compare", In<"lhs", TsVar<"S">>, In<"rhs", TsVar<"S">>,
                              Scalar<"recordable_id", Str>>
    {
    };
}  // namespace hgraph::stdlib

#endif  // HGRAPH_LIB_STD_OPERATORS_IO_H
