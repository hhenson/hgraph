#ifndef HGRAPH_LIB_STD_OPERATORS_IMPL_OUTPUT_ELISION_H
#define HGRAPH_LIB_STD_OPERATORS_IMPL_OUTPUT_ELISION_H

#include <hgraph/types/static_node.h>
#include <hgraph/types/static_schema.h>

namespace hgraph::stdlib
{
    /**
     * Publish ``value`` only when it differs from what the output already
     * holds.
     *
     * An operator opts in, and most should not. ``out.set`` always ticks, and
     * that is right for an operator whose result is its own value: a
     * recomputed value is news even when it is equal, and comparing costs a
     * read the operator would otherwise not make. Explicit writes tick
     * unconditionally by ruling (roadmap.rst, 2026-07-17).
     *
     * Reach for this where the operator PROJECTS part of a larger value, so an
     * unchanged projection is genuinely not an event: a collection's size
     * across a replaced element, a date's month across two days of the same
     * month. Both directions of the parity argument land here -- the
     * no-change-means-no-tick ruling covers the collection cases, and the
     * released implementation elides the date components by building them on
     * an ``explode`` that publishes only what changed.
     */
    template <typename T> void set_if_changed(const Out<TS<T>> &out, const T &value)
    {
        if (!out.valid() || out.value().template checked_as<T>() != value) { out.set(value); }
    }

    /**
     * The same elision for an operator that republishes a whole value it did
     * not compute -- ``merge`` re-selecting a source after the selected one
     * went away.
     *
     * Same opt-in rule as ``set_if_changed``: this is for a republication, not
     * a result. ``merge`` reaches for it on its FALLBACK path only, matching
     * the released ``merge_ts_scalar``, whose re-selection branch ends
     * ``if out is not None and out != _output.value``. Its modified path has
     * no such guard and neither does ours, because an input that ticked is
     * news whatever it carries.
     */
    template <typename TOut> void apply_if_changed(const TOut &out, const ValueView &value)
    {
        if (!out.valid() || out.value() != value) { out.apply(value); }
    }
}  // namespace hgraph::stdlib

#endif  // HGRAPH_LIB_STD_OPERATORS_IMPL_OUTPUT_ELISION_H
