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

#include <hgraph/lib/std/component.h>
#include <hgraph/lib/std/std_operators.h>
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

    /** Per-key state held where a checkpoint can see it (RFC 0039). */
    using PreparedRunningState = TSB<"DistributedPreparedRunningState", Field<"total", TS<Int>>>;
    struct PreparedAccumulate
    {
        static constexpr auto name = "prepared_accumulate";
        static void eval(In<"ts", TS<Int>> ts, RecordableState<PreparedRunningState> state, Out<TS<Int>> out)
        {
            auto      total = state.field<"total">();
            const Int value = (total.valid() ? total.value().checked_as<Int>() : 0) + ts.value();
            total.set(value);
            out.set(value);
        }
    };
    inline constexpr const char *prepared_accumulate_name = "prepared accumulate: recoverable";

    /**
     * A worker child that is itself a dynamic owner: a nested ``map_`` and a
     * ``mesh_``, each with per-key recordable state, folded by ``reduce``.
     * Everything a worker image can hold one level down.
     */
    struct PreparedAddPair
    {
        static constexpr auto name = "prepared_add_pair";
        static void eval(In<"lhs", TS<Int>> lhs, In<"rhs", TS<Int>> rhs, Out<TS<Int>> out)
        {
            out.set(lhs.value() + rhs.value());
        }
    };
    struct PreparedNestedOwners
    {
        static constexpr auto name = "prepared_nested_owners";
        static Port<TS<Int>>  compose(Wiring &w, Port<TSD<Str, TS<Int>>> ts)
        {
            using Inner = TSD<Str, TS<Int>>;
            auto mapped = wire<stdlib::map_>(w, fn<PreparedAccumulate>(), ts).as<Inner>();
            auto meshed = wire<stdlib::mesh_>(w, fn<PreparedAccumulate>(), ts).as<Inner>();
            auto lhs    = wire<stdlib::reduce_>(w, fn<PreparedAddPair>(), mapped, Int{0}).as<TS<Int>>();
            auto rhs    = wire<stdlib::reduce_>(w, fn<PreparedAddPair>(), meshed, Int{0}).as<TS<Int>>();
            return wire<PreparedAddPair>(w, lhs, rhs).as<TS<Int>>();
        }
    };
    inline constexpr const char *prepared_nested_name = "prepared nested owners: recoverable";

    // --- a component INSIDE the dmap_ child (RFC 0039) -----------------------
    // The recoverable unit is the component. What else the child holds is
    // processed: it starts again with every run.
    inline constexpr const char *dmap_component_id = "dmap-accumulate";
    struct PreparedAccumulateBody
    {
        static Port<TS<Int>> compose(Wiring &w, NamedPort<"ts", TS<Int>> ts)
        { return wire<PreparedAccumulate>(w, ts).as<TS<Int>>(); }
    };
    /** The child is the component and nothing else. */
    struct PreparedHostedChild
    {
        static constexpr auto name = "prepared_hosted_child";
        static Port<TS<Int>>  compose(Wiring &w, Port<TS<Int>> ts)
        { return stdlib::component<PreparedAccumulateBody>(w, dmap_component_id, ts); }
    };
    /** After the component, a node that keeps ordinary ``State``: outside the
        component, so neither recoverable nor required to be. */
    struct PreparedHostedThenForgetful
    {
        static constexpr auto name = "prepared_hosted_then_forgetful";
        static Port<TS<Int>>  compose(Wiring &w, Port<TS<Int>> ts)
        { return wire<RunningTotalNode>(w, stdlib::component<PreparedAccumulateBody>(w, dmap_component_id, ts)).as<TS<Int>>(); }
    };
    /** Beside the component, a constant: a fresh node with start-time work,
        which a restored child has to run and the owner has to be woken for. */
    struct PreparedHostedWithConstant
    {
        static constexpr auto name = "prepared_hosted_with_constant";
        static Port<TS<Int>>  compose(Wiring &w, Port<TS<Int>> ts)
        {
            auto total = stdlib::component<PreparedAccumulateBody>(w, dmap_component_id, ts);
            return wire<PreparedAddPair>(w, total, wire<stdlib::const_>(w, Int{1000}).as<TS<Int>>()).as<TS<Int>>();
        }
    };
    /** A component that cannot be recovered, hosted in the child. */
    struct PreparedForgetfulBody
    {
        static Port<TS<Int>> compose(Wiring &w, NamedPort<"ts", TS<Int>> ts)
        { return wire<RunningTotalNode>(w, ts).as<TS<Int>>(); }
    };
    struct PreparedHostedForgetful
    {
        static constexpr auto name = "prepared_hosted_forgetful";
        static Port<TS<Int>>  compose(Wiring &w, Port<TS<Int>> ts)
        { return stdlib::component<PreparedForgetfulBody>(w, dmap_component_id, ts); }
    };
    inline constexpr const char *prepared_hosted_name           = "prepared hosted component";
    inline constexpr const char *prepared_hosted_forgetful_name = "prepared hosted component, then forgetful";
    inline constexpr const char *prepared_hosted_constant_name  = "prepared hosted component, with constant";

    /** The same recoverable child over integer keys, for the typed ``dmap_`` form. */
    struct AccumulateG
    {
        static constexpr auto name = "dmap_accumulate_g";
        static Port<TS<Int>>  compose(Wiring &w, Port<TS<Int>> ts)
        {
            return wire<PreparedAccumulate>(w, ts).as<TS<Int>>();
        }
    };

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

    /** A timer owned by a transient sink, independent of recovered state. */
    template <Int Delay> struct CheckpointTimerSink
    {
        static void start(NodeScheduler scheduler) { scheduler.schedule(scheduler.now() + MIN_TD * Delay); }
        static void eval(In<"ts", TS<Int>, InputValidity::Unchecked>, NodeScheduler) {}
    };
    struct CheckpointImmediateSink
    {
        static void start(NodeScheduler scheduler, State<Int> calls)
        { calls.set(Int{0}); scheduler.schedule(scheduler.now()); }
        static void eval(In<"ts", TS<Int>, InputValidity::Unchecked>, NodeScheduler, State<Int> calls)
        { calls.set(calls.get() + 1); }
        static void stop(State<Int> calls)
        { if (calls.get() == 0) throw std::runtime_error("transient sink startup work was lost"); }
    };
    struct HostedWithTimer
    {
        static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> ts)
        {
            auto total = stdlib::component<PreparedAccumulateBody>(w, dmap_component_id, ts);
            wire<CheckpointTimerSink<100>>(w, total);
            return total;
        }
    };
    struct ChildWithImmediateSink
    {
        static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> ts)
        {
            auto total = wire<PreparedAccumulate>(w, ts).as<TS<Int>>();
            wire<CheckpointImmediateSink>(w, total);
            return total;
        }
    };
    struct NestedCheckpointBody
    {
        static Port<TS<Int>> compose(Wiring &w, NamedPort<"ts", TS<Int>> ts)
        {
            auto intermediate = wire<PreparedAccumulate>(w, ts).as<TS<Int>>();
            return stdlib::component<PreparedAccumulateBody>(w, "inner", intermediate);
        }
    };
    struct HostedNestedComponent
    {
        static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> ts)
        { return stdlib::component<NestedCheckpointBody>(w, dmap_component_id, ts); }
    };
    struct CheckpointTransientSink { static void eval(In<"ts", TS<Int>>) {} };
    template <bool Sink> struct CompatibleCheckpointBody
    {
        static Port<TS<Int>> compose(Wiring &w, NamedPort<"ts", TS<Int>> ts)
        {
            if constexpr (Sink) wire<CheckpointTransientSink>(w, ts);
            return wire<PreparedAccumulate>(w, ts).template as<TS<Int>>();
        }
    };
    template <bool Sink> struct HostedCompatibleComponent
    {
        static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> ts)
        { return stdlib::component<CompatibleCheckpointBody<Sink>>(w, dmap_component_id, ts); }
    };
    template <fixed_string Id> struct HostedNamedComponent
    {
        static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> ts)
        { return stdlib::component<PreparedAccumulateBody>(w, Id.sv(), ts); }
    };
    struct CheckpointPendingCompute
    {
        static const NodeCheckpointOps &checkpoint_ops() noexcept
        { static const NodeCheckpointOps ops{.supported = true}; return ops; }
        static void start(NodeScheduler scheduler) { scheduler.schedule(scheduler.now() + MIN_TD * 100); }
        static void eval(In<"ts", TS<Int>> ts, NodeScheduler, Out<TS<Int>> out) { out.set(ts.value()); }
    };

    /** Leaves a file behind when it is stopped, which a killed worker never does. */
    inline constexpr const char *stop_marker_directory_variable = "HGRAPH_TEST_STOP_MARKERS";
    struct CheckpointStopMarker
    {
        static void eval(In<"ts", TS<Int>>) {}
        static void stop();
    };
    /** Recoverable, with a sink whose stop hook can be counted. */
    struct AccumulateWithStopMarker
    {
        static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> ts)
        {
            auto total = wire<PreparedAccumulate>(w, ts).as<TS<Int>>();
            wire<CheckpointStopMarker>(w, total);
            return total;
        }
    };
    inline constexpr const char *accumulate_stop_marker_name = "accumulate, stop marker";
    struct PendingComputeWithStopMarker
    {
        static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> ts)
        {
            auto out = wire<CheckpointPendingCompute>(w, ts).as<TS<Int>>();
            wire<CheckpointStopMarker>(w, out);
            return out;
        }
    };

    /** Register what a worker process may be asked to build. */
    void register_distributed_test_recipes();
}  // namespace hgraph_test

#endif  // HGRAPH_TESTS_DISTRIBUTED_WORKER_RECIPES_H
