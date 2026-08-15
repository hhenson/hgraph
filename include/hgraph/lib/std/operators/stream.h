#ifndef HGRAPH_LIB_STD_OPERATORS_STREAM_H
#define HGRAPH_LIB_STD_OPERATORS_STREAM_H

#include <hgraph/types/operator_dispatch.h>
#include <hgraph/types/primitive_types.h>
#include <hgraph/types/static_node.h>
#include <hgraph/types/static_schema.h>
#include <hgraph/util/date_time.h>

namespace hgraph::stdlib
{
    /**
     * Streaming / temporal-shaping operator **definitions** (markers only). Mirrors the
     * Python ``hgraph`` stream operators (``_stream.py``). The numerical analytical
     * family is provided by ``hgraph-analytics``. A ``period`` / ``count`` that
     * Python types as ``INT_OR_TIME_DELTA`` is modelled here as ``Int`` (ticks) —
     * an implementation may also accept ``TimeDelta``.
     */

    /** Sample the latest valid value of ``ts`` whenever ``signal`` ticks.
        Ticks from ``ts`` alone update the value available to sample but do not produce output.
        @param signal Trigger whose tick timing controls output; its value is ignored.
        @param ts Input whose latest value is sampled.
        @return ``ts`` reticked at the signal's times.
        @par Python example
        @code{.py}
        snapshot = hg.sample(clock, price)
        @endcode */
    struct sample : Operator<"sample", In<"signal", SIGNAL>, In<"ts", TsVar<"S">>, Out<TsVar<"S">>>
    {
    };

    /** Delay every input tick by a tick count or elapsed duration.
        Tick-count lag replays the value after that many later source ticks; duration lag
        schedules it for ``input_time + period``.
        @param ts Stream to delay.
        @param period Positive tick count or duration selected at wiring time.
        @param proxy Optional proxy stream whose count defines progress for proxy-lag overloads.
        @return The original values with delayed tick times.
        @par Python example
        @code{.py}
        previous = hg.lag(price, 1)
        delayed = hg.lag(price, timedelta(seconds=5))
        @endcode
        @note Cost: O(delta) per tick; retains up to ``period`` pending deltas. */
    struct lag : Operator<"lag", In<"ts", TsVar<"S">>, Scalar<"period", Int>, Out<TsVar<"S">>>
    {
    };

    /** ``__lag_proxy`` — the proxy-lag runtime node (internal; wired by the
        ``lag(ts, period, proxy)`` overloads): each tick of ``ts`` is cached
        under the live proxy count ``c`` and replayed when the LAGGED count
        ``lag_c`` reaches it. */
    struct lag_proxy_node
        : Operator<"__lag_proxy", In<"ts", TsVar<"S">>, In<"c", TS<Int>>, In<"lag_c", TS<Int>>, Out<TsVar<"S">>>
    {
    };

    /** Create a periodic ``True`` signal driven by engine time.
        @param delay Positive interval between ticks; time-series overloads allow it to change.
        @param initial_delay When true, wait one ``delay`` before the first tick; when false,
                             tick at graph start.
        @param max_ticks Optional upper bound after which the source becomes passive.
        @param start Optional time-series start instant that re-bases the schedule grid.
        @return A boolean signal ticking at the requested schedule.
        @par Python example
        @code{.py}
        every_second = hg.schedule(timedelta(seconds=1))
        @endcode */
    struct schedule : Operator<"schedule", Scalar<"delay", TimeDelta>, Out<TS<Bool>>>
    {
    };

    /** Allocate a process-unique integer identifier when the node starts.
        @param hash Required wiring-time namespace discriminator retained for service compatibility.
        @return A single identifier tick suitable for correlating request/reply flows.
        @par Python example
        @code{.py}
        correlation_id = hg.request_id(1)
        @endcode */
    struct request_id : Operator<"request_id", Scalar<"hash", Int>, Out<TS<Int>>>
    {
    };

    /** Suppress an input tick when its value compares equal to the last emitted value.
        The first valid value always passes through.
        @param ts Stream to de-duplicate.
        @return ``ts`` with consecutive equal values removed.
        @par Python example
        @code{.py}
        changes_only = hg.dedup(status)
        @endcode */
    struct dedup : Operator<"dedup", In<"ts", TsVar<"S">>, Out<TsVar<"S">>>
    {
    };

    /** Forward source ticks only while the latest ``condition`` value is true.
        When the condition changes from false to true, the operator publishes the
        latest source value if the source changed while the gate was closed. A
        condition-only tick does not emit when there is no blocked change to replay.
        @param condition Boolean gate controlling whether source ticks pass.
        @param ts Stream to filter.
        @return Source ticks accepted while ``condition`` is true, plus the latest
                blocked value when the condition reopens.
        @par Python example
        @code{.py}
        positive_prices = hg.filter_(price > 0.0, price)
        @endcode */
    struct filter_ : Operator<"filter_", In<"condition", TS<Bool>>, In<"ts", TsVar<"S">>, Out<TsVar<"S">>>
    {
    };

    /** Filter a collection by evaluating a predicate graph for each candidate value.
        Additional named inputs are forwarded to the predicate; keyed dictionary forms
        preserve the keys whose predicate output is true.
        @param ts Collection or keyed dictionary to filter.
        @param expr Predicate graph receiving each value.
        @param kwargs Additional inputs forwarded to ``expr``.
        @return A collection containing only values accepted by the predicate.
        @par Python example
        @code{.py}
        expensive = hg.filter_by(prices, lambda value, limit: value > limit, limit=limit)
        @endcode */
    struct filter_by : Operator<"filter_by", In<"ts", TsVar<"S">>, Out<TsVar<"S">>>
    {
    };

    /** Evaluate a predicate until it first succeeds, emitting false for failed ticks and
        true for the first successful tick before passivating the input.
        @param ts Input tested by the selected predicate overload.
        @param predicate Predicate callable or expression used by applicable overloads.
        @return A boolean stream ending with the first true result.
        @par Python example
        @code{.py}
        reached_target = hg.until_true(lambda value: value >= target, price)
        @endcode */
    struct until_true : Operator<"until_true", In<"ts", TsVar<"S">>, Out<TS<Bool>>>
    {
    };

    /** Forward source ticks until ``predicate`` first ticks true, then retain the last
        value and passivate the source permanently.
        @param predicate Boolean stream that freezes the output when true.
        @param ts Stream to forward until frozen.
        @return The source stream up to the freeze point, retaining its last value.
        @par Python example
        @code{.py}
        final_price = hg.freeze(done, price)
        @endcode */
    struct freeze : Operator<"freeze", In<"predicate", TS<Bool>>, In<"ts", TsVar<"S">>, Out<TsVar<"S">>>
    {
    };

    /** Limit output frequency while preserving the latest pending source value.
        Unlike ``hgraph_analytics.resample``, no output is produced during an
        interval with no source tick.
        @param ts Stream whose tick rate is limited.
        @param period Minimum elapsed time between output ticks; it may vary at runtime.
        @return A rate-limited stream of the latest source values.
        @par Python example
        @code{.py}
        limited = hg.throttle(updates, timedelta(milliseconds=100))
        @endcode
        @note Cost: O(delta) per tick; retains the pending deltas of one period. */
    struct throttle : Operator<"throttle", In<"ts", TsVar<"S">>, In<"period", TS<TimeDelta>>, Out<TsVar<"S">>>
    {
    };

    /** Forward the first ``count`` source ticks, then passivate the input.
        @param ts Stream to truncate.
        @param count Non-negative number of ticks to forward, fixed at wiring time.
        @return At most the first ``count`` ticks of ``ts``.
        @par Python example
        @code{.py}
        first_ten = hg.take(updates, 10)
        @endcode */
    struct take : Operator<"take", In<"ts", TsVar<"S">>, Scalar<"count", Int>, Out<TsVar<"S">>>
    {
    };

    /** Suppress the first ``count`` source ticks and forward every later tick.
        @param ts Stream whose prefix is removed.
        @param count Non-negative number of ticks to discard, fixed at wiring time.
        @return ``ts`` without its first ``count`` ticks.
        @par Python example
        @code{.py}
        after_warmup = hg.drop(updates, 10)
        @endcode */
    struct drop : Operator<"drop", In<"ts", TsVar<"S">>, Scalar<"count", Int>, Out<TsVar<"S">>>
    {
    };

    /** Retain a trailing tick-count or time-duration buffer and expose both values and
        their evaluation timestamps. Tick windows use a circular buffer; duration windows
        evict entries older than the requested horizon.
        @param ts Values to retain.
        @param period Maximum tick count or elapsed horizon.
        @param min_window_period Optional minimum population before output becomes valid.
        @return A bundle containing the trailing values and corresponding timestamps.
        @par Python example
        @code{.py}
        recent = hg.window(price, 20)
        @endcode
        @note Cost: O(W) per tick (the value/time bundle is rebuilt) and O(W) retained in private state. Deprecated-parity shape — prefer ``to_window``, whose TSW substrate appends/evicts in O(1). */
    struct window : Operator<"window", In<"ts", TsVar<"S">>, Scalar<"period", Int>, Out<TsVar<"O">>>
    {
    };

    /** Convert a stream into a typed trailing ``TSW`` window.
        The output becomes valid after ``min_window_period`` values. When ``reset`` and
        the source tick together, retained values are cleared before the new tick is added.
        Wiring rejects a non-positive period, a negative minimum, or a minimum greater
        than the period.
        @param ts Values admitted to the window.
        @param period Maximum number of retained ticks, fixed at wiring time.
        @param min_window_period Minimum retained count required for validity.
        @param reset Optional signal that clears retained ticks.
        @return A typed time-series window over the most recent values.
        @par Python example
        @code{.py}
        recent = hg.to_window(price, period=20, min_window_period=5, reset=session_start)
        @endcode
        @note Cost: O(1) append/evict per tick; O(W) retained by the TSW itself. Aggregates over the window (``min_`` / ``max_`` / ``sum_`` / ``mean`` / ``std``) recompute in O(W) per window tick — recorded beside their kernels. */
    struct to_window : Operator<"to_window", In<"ts", TsVar<"S">>, Scalar<"period", Int>,
                                Scalar<"min_window_period", Int>, Out<TsVar<"O">>>
    {
    };

    /** Queue source ticks while ``condition`` is false and release them in order after
        it becomes true. A positive ``buffer_length`` raises on overflow; ``-1`` keeps
        only the most recent gated value.
        @param condition False to buffer and true to release/forward.
        @param ts Stream passing through the gate.
        @param buffer_length Maximum queued ticks, or ``-1`` for last-value mode.
        @return The original ticks, delayed while the gate is closed.
        @par Python example
        @code{.py}
        released = hg.gate(is_ready, updates, buffer_length=1000)
        @endcode */
    struct gate : Operator<"gate", In<"condition", TS<Bool>>, In<"ts", TsVar<"S">>, Scalar<"buffer_length", Int>,
                           Out<TsVar<"S">>>
    {
    };

    /** Queue ticks while ``condition`` is false, then drain them in batches separated
        by ``delay``. Raises if a positive ``buffer_length`` is exceeded.
        @param condition False to buffer and true to begin draining.
        @param ts Stream to buffer.
        @param delay Engine-time interval between released batches.
        @param buffer_length Maximum number of queued values.
        @return Buffered values released in delayed batches.
        @par Python example
        @code{.py}
        paced = hg.batch(is_ready, updates, timedelta(milliseconds=10), buffer_length=1000)
        @endcode */
    struct batch : Operator<"batch", In<"condition", TS<Bool>>, In<"ts", TsVar<"S">>,
                            Scalar<"delay", TimeDelta>, Scalar<"buffer_length", Int>, Out<TsVar<"O">>>
    {
    };

    /** Forward one source tick for each ``step_size`` input ticks.
        @param ts Stream to subsample.
        @param step_size Positive stride fixed at wiring time.
        @return Every ``step_size``-th tick of ``ts``.
        @par Python example
        @code{.py}
        every_tenth = hg.step(updates, 10)
        @endcode */
    struct step : Operator<"step", In<"ts", TsVar<"S">>, Scalar<"step_size", Int>, Out<TsVar<"S">>>
    {
    };

    /** Slice a stream by tick position, combining prefix removal, truncation, and stride.
        @param ts Stream to slice.
        @param start Zero-based inclusive first tick position.
        @param stop Zero-based exclusive stop position.
        @param step_size Positive stride between forwarded ticks.
        @return Ticks at positions described by ``slice(start, stop, step_size)``.
        @par Python example
        @code{.py}
        middle_even_ticks = hg.slice_(updates, start=2, stop=10, step_size=2)
        @endcode */
    struct slice_ : Operator<"slice_", In<"ts", TsVar<"S">>, Scalar<"start", Int>, Scalar<"stop", Int>,
                             Scalar<"step_size", Int>, Out<TsVar<"S">>>
    {
    };

}  // namespace hgraph::stdlib

#endif  // HGRAPH_LIB_STD_OPERATORS_STREAM_H
