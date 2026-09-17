#ifndef HGRAPH_TESTS_DISTRIBUTED_WORKER_RECIPES_H
#define HGRAPH_TESTS_DISTRIBUTED_WORKER_RECIPES_H

// The child a distributed worker runs, and its registration (RFC 0037).
//
// This header exists because of the bootstrap rule: a worker process cannot be
// sent its graph, so both programs must LINK the same child. That makes the
// kernel a named type in a shared translation unit rather than a local one in
// a test file -- a type in an anonymous namespace would mangle differently in
// the two programs, and the recipe key is a mangled name, so the two sides
// would fail to agree on exactly the thing they exist to agree on.

#include <hgraph/runtime/node_scheduler.h>
#include <hgraph/types/graph_wiring.h>
#include <hgraph/types/static_node.h>
#include <hgraph/types/static_schema.h>

namespace hgraph_test
{
    using namespace hgraph;

    using KeyedInts = TSD<Int, TS<Int>>;

    /** Per-key state, so a key handled by the wrong worker gives a wrong sum. */
    struct RunningTotalNode
    {
        static constexpr auto name = "dmap_running_total";

        static void eval(In<"ts", TS<Int>> ts, State<Int> total, Out<TS<Int>> out)
        {
            total.modify() += ts.value();
            out.set(total.get());
        }
    };

    struct RunningTotalG
    {
        static constexpr auto name = "dmap_running_total_g";
        static Port<TS<Int>>  compose(Wiring &w, Port<TS<Int>> ts)
        {
            return wire<RunningTotalNode>(w, ts).as<TS<Int>>();
        }
    };

    /**
     * A child that wakes ITSELF: the input arms it and echoes, and ten times
     * the value follows two microseconds later, on a cycle nothing outside the
     * child ticks.
     *
     * The point is that nothing in the calling graph knows the second tick is
     * pending. The only route back is the child's ``next_scheduled_time``
     * travelling in the reply and the ``dmap_`` node scheduling itself on it
     * -- so if that propagation is missing, the answer never arrives at all.
     */
    struct DelayedDoubleNode
    {
        static constexpr auto name = "dmap_delayed_double";

        static void eval(In<"ts", TS<Int>> ts, NodeScheduler scheduler, State<Int> pending,
                         DateTime now, Out<TS<Int>> out)
        {
            if (ts.modified())
            {
                pending.modify() = ts.value();
                scheduler.schedule(now + TimeDelta{2});
                out.set(ts.value());
                return;
            }
            out.set(pending.get() * 10);
        }
    };

    struct DelayedDoubleG
    {
        static constexpr auto name = "dmap_delayed_double_g";
        static Port<TS<Int>>  compose(Wiring &w, Port<TS<Int>> ts)
        {
            return wire<DelayedDoubleNode>(w, ts).as<TS<Int>>();
        }
    };

    /**
     * The same, but SILENT while armed: the child exists and has no value.
     *
     * This is the shape that exposes a recorded deviation from ``map_`` -- see
     * the test, and RFC 0037's *Known deviations*. It is kept as its own
     * kernel so the deviation is pinned by a case that is about nothing else.
     */
    struct ArmSilentlyNode
    {
        static constexpr auto name = "dmap_arm_silently";

        static void eval(In<"ts", TS<Int>> ts, NodeScheduler scheduler, State<Int> pending,
                         DateTime now, Out<TS<Int>> out)
        {
            if (ts.modified())
            {
                pending.modify() = ts.value();
                scheduler.schedule(now + TimeDelta{2});
                return;
            }
            out.set(pending.get() * 10);
        }
    };

    struct ArmSilentlyG
    {
        static constexpr auto name = "dmap_arm_silently_g";
        static Port<TS<Int>>  compose(Wiring &w, Port<TS<Int>> ts)
        {
            return wire<ArmSilentlyNode>(w, ts).as<TS<Int>>();
        }
    };

    /**
     * A recipe name with a space in it, registered alongside the derived ones.
     *
     * A derived key is a mangled name, and whether that contains a space is a
     * property of the COMPILER: Itanium mangling has none, MSVC renders
     * ``struct ns::Name<...>`` and has several. The name is passed to the
     * worker on a command line, so an unquoted launch splits it -- which is a
     * platform-specific bug that only one platform can catch by accident.
     * This makes every platform catch it.
     */
    inline constexpr const char *awkward_recipe_name = "hgraph test recipe \"with\" spaces \xCE\xBB \xE6\xB5\x8B";

    using PreparedRow = TSB<"DistributedPreparedRow", Field<"value", TS<Int>>, Field<"label", TS<Str>>>;

    struct PreparedAdd
    {
        static void eval(In<"lhs", TS<Int>> lhs, In<"rhs", TS<Int>> rhs,
                         In<"offset", TS<Int>> offset, Out<TS<Int>> out)
        { out.set(lhs.value() + rhs.value() + offset.value()); }
    };
    struct PreparedKeyOnly
    {
        static void eval(In<"key", TS<Str>> key, Out<TS<Str>> out)
        { out.set(key.value() + "!"); }
    };
    struct PreparedBundleIdentity
    {
        static Port<PreparedRow> compose(Wiring &, Port<PreparedRow> value) { return value; }
    };
    inline constexpr const char *prepared_add_name = "prepared: add \"quoted\"";
    inline constexpr const char *prepared_keys_name = "prepared: keys";
    inline constexpr const char *prepared_bundle_name = "prepared: bundle";

    /** Register what a worker process may be asked to build. */
    void register_distributed_test_recipes();
}  // namespace hgraph_test

#endif  // HGRAPH_TESTS_DISTRIBUTED_WORKER_RECIPES_H
