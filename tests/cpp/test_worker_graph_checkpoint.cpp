// Whole-graph recovery of a stepped graph (RFC 0039, "Whole-graph coordinator").
//
// A worker-hosted graph is captured between two steps and a fresh graph is
// started from that image. The one property worth having is that the break is
// invisible: every test that restores compares the interrupted run, cycle for
// cycle, with a run that was never interrupted.
//
// The image crosses the break as BYTES, because that is how it will cross a
// worker channel -- an image that survives only as an owned object proves
// nothing about the transport form.

#include <hgraph/lib/std/std_operators.h>
#include <hgraph/runtime/checkpoint_codec.h>
#include <hgraph/runtime/distributed_child.h>
#include <hgraph/runtime/executor.h>
#include <hgraph/runtime/node_scheduler.h>
#include <hgraph/types/graph_wiring.h>
#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/static_node.h>
#include <hgraph/types/static_schema.h>

#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers_string.hpp>

#include <optional>
#include <string>
#include <vector>

namespace
{
    using namespace hgraph;
    using namespace hgraph::distributed;
    using Catch::Matchers::ContainsSubstring;

    using KeyedInts    = TSD<Int, TS<Int>>;
    using RunningState = TSB<"worker_checkpoint_state", Field<"total", TS<Int>>>;

    /** Per-key state that a lost, repeated or re-ticked cycle changes. */
    struct Accumulate
    {
        static constexpr auto name = "worker_checkpoint_accumulate";
        static void eval(In<"ts", TS<Int>> ts, RecordableState<RunningState> state, Out<TS<Int>> out)
        {
            auto      total = state.field<"total">();
            const Int value = (total.valid() ? total.value().checked_as<Int>() : 0) + ts.value();
            total.set(value);
            out.set(value);
        }
    };
    struct AccumulateG
    {
        static constexpr auto name = "worker_checkpoint_accumulate_g";
        static Port<TS<Int>>  compose(Wiring &w, Port<TS<Int>> ts)
        {
            return wire<Accumulate>(w, ts).as<TS<Int>>();
        }
    };

    /** The mask names the live keys: a set bit ticks its key, a clear one removes it. */
    struct Spread
    {
        static constexpr auto name = "worker_checkpoint_spread";
        static void           eval(In<"in", TS<Int>> in, Out<KeyedInts> out)
        {
            const Int mask = in.value();
            for (Int key = 0; key < 8; ++key)
            {
                if ((mask & (Int{1} << key)) != 0) { out[key].set(key + 1); }
                else { (void)out.erase(key); }
            }
        }
    };

    /**
     * Key-sensitive, so two keys that swapped totals do not cancel.
     *
     * ``scale`` comes from a ``const_``, which schedules itself on start. A
     * restored start that let that bootstrap schedule survive would re-tick the
     * constant and evaluate this node on a cycle nothing was staged for.
     */
    struct Digest
    {
        static constexpr auto name = "worker_checkpoint_digest";
        static void           eval(In<"d", KeyedInts> d, In<"scale", TS<Int>> scale, Out<TS<Int>> out)
        {
            Int total = 0;
            for (auto &&[key, child] : d.valid_items()) { total += (key.checked_as<Int>() + 1) * child.value(); }
            out.set(total * scale.value());
        }
    };

    /** The shape a ``dmap_`` worker hosts: staged in, a keyed ``map_``, read out. */
    template <fixed_string Slot> struct KeyedWorker
    {
        static constexpr auto name = "worker_checkpoint_keyed_graph";
        static void           compose(Wiring &w)
        {
            const auto enclosing = w.checkpoint_component("worker");
            auto       in        = wire<boundary_source_impl, TS<Int>>(w, Str{Slot.sv()});
            auto       dict      = wire<Spread>(w, in).template as<KeyedInts>();
            auto       totals    = wire<stdlib::map_>(w, fn<AccumulateG>(), dict).template as<KeyedInts>();
            auto       scale     = wire<stdlib::const_>(w, Int{2}).template as<TS<Int>>();
            wire<boundary_sink_impl>(w, wire<Digest>(w, totals, scale), Str{"out"});
            (void)w.checkpoint_component(enclosing);
        }
    };
    using Worker = KeyedWorker<"in">;

    /**
     * Arms a ``NodeScheduler`` alarm in ``start``, as a ``dmap_`` owner does.
     * The alarm lives twice: in the graph's schedule slot and in the node's
     * own scheduler state.
     */
    struct ArmedAtStart
    {
        static constexpr auto name = "worker_checkpoint_armed_at_start";
        static const NodeCheckpointOps &checkpoint_ops() noexcept
        {
            static const NodeCheckpointOps ops{.supported = true};
            return ops;
        }
        static void start(NodeScheduler scheduler) { scheduler.schedule(scheduler.now()); }
        static void eval(In<"ts", TS<Int>, InputValidity::Unchecked> ts, NodeScheduler, Out<TS<Int>> out)
        {
            if (ts.modified()) { out.set(ts.value()); }
        }
    };
    struct ArmedWorker
    {
        static constexpr auto name = "worker_checkpoint_armed_graph";
        static void           compose(Wiring &w)
        {
            const auto enclosing = w.checkpoint_component("worker");
            auto       in        = wire<boundary_source_impl, TS<Int>>(w, Str{"in"});
            wire<boundary_sink_impl>(w, wire<ArmedAtStart>(w, in), Str{"out"});
            (void)w.checkpoint_component(enclosing);
        }
    };

    /** The same graph outside any checkpoint scope: its nodes have no identity. */
    struct UnscopedWorker
    {
        static constexpr auto name = "worker_checkpoint_unscoped_graph";
        static void           compose(Wiring &w)
        {
            auto in = wire<boundary_source_impl, TS<Int>>(w, Str{"in"});
            wire<boundary_sink_impl>(w, wire<Accumulate>(w, in), Str{"out"});
        }
    };

    const DateTime test_end = MIN_ST + TimeDelta{1000};

    using Collected = std::vector<std::optional<Int>>;

    DateTime cycle_time(std::size_t cycle) { return MIN_ST + MIN_TD * static_cast<std::int64_t>(cycle); }

    /** One caller cycle: stage, step at the caller's time, read back. */
    void drive(DistributedChildHost &host, std::size_t cycle, Int mask, Collected &collected)
    {
        host.stage("in", Value{mask}.view());
        REQUIRE(host.step(cycle_time(cycle)));
        Value out = host.collect("out");
        collected.push_back(out.has_value() ? std::optional<Int>{out.view().checked_as<Int>()} : std::nullopt);
    }

    Collected run_uninterrupted(const std::vector<Int> &masks)
    {
        DistributedChildHost host{build_graph<Worker>(), test_end};
        host.start(MIN_ST);
        Collected collected;
        for (std::size_t cycle = 0; cycle < masks.size(); ++cycle) { drive(host, cycle, masks[cycle], collected); }
        host.stop();
        return collected;
    }

    /** Run ``split`` cycles, and leave the worker's image as the bytes a channel would carry. */
    std::string run_until(const std::vector<Int> &masks, std::size_t split, Collected &collected)
    {
        DistributedChildHost host{build_graph<Worker>(), test_end};
        host.start(MIN_ST);
        for (std::size_t cycle = 0; cycle < split; ++cycle) { drive(host, cycle, masks[cycle], collected); }
        std::string bytes;
        encode_graph_checkpoint(host.capture(), bytes, cycle_time(split));
        host.stop();
        return bytes;
    }

    void register_types()
    {
        (void)TypeRegistry::instance().register_scalar<Int>("int");
        stdlib::register_standard_operators();
    }

    // Keys arrive, accumulate, leave and come back, so the image has to carry
    // membership, slot placement and per-key recordable state. Key 0 lives
    // through every cycle and keys 4 and 5 through most: a removed key's child
    // is destroyed, so a boundary that no accumulated key crosses would be
    // invisible to a worker that had simply lost everything.
    const std::vector<Int> churn{0b00001111, 0b00110011, 0b10110001, 0b00110101, 0b11110001, 0b00010101};
}  // namespace

TEST_CASE("worker graph checkpoint: a break at any cycle boundary is invisible", "[checkpoint][worker]")
{
    register_types();
    const auto expected = run_uninterrupted(churn);
    // Pinned so the comparison cannot pass on empty output: cycle 0 ticks keys
    // 0..3 with 1..4, and the key-weighted digest is 1 + 4 + 9 + 16, scaled by 2.
    REQUIRE(expected.size() == churn.size());
    REQUIRE(expected.front() == std::optional<Int>{60});

    for (std::size_t split = 1; split < churn.size(); ++split)
    {
        CAPTURE(split);
        Collected  collected;
        const auto bytes = run_until(churn, split, collected);
        const auto image = decode_graph_checkpoint(bytes);

        DistributedChildHost resumed{build_graph<Worker>(), test_end};
        resumed.start_restored(cycle_time(split), image);
        auto amnesiac = collected;
        for (std::size_t cycle = split; cycle < churn.size(); ++cycle) { drive(resumed, cycle, churn[cycle], collected); }
        resumed.stop();
        CHECK(collected == expected);

        // The control: a worker that starts from nothing at the same boundary
        // must be visibly wrong, or the comparison above proves nothing.
        DistributedChildHost fresh{build_graph<Worker>(), test_end};
        fresh.start(cycle_time(split));
        for (std::size_t cycle = split; cycle < churn.size(); ++cycle) { drive(fresh, cycle, churn[cycle], amnesiac); }
        fresh.stop();
        CHECK(amnesiac != expected);
    }
}

TEST_CASE("worker graph checkpoint: a break at every cycle boundary is invisible", "[checkpoint][worker]")
{
    register_types();
    // Each generation restores the one before, so an image of a RESTORED graph
    // has to be as good as an image of one that ran from nothing.
    Collected   collected;
    std::string bytes = run_until(churn, 1, collected);
    for (std::size_t cycle = 1; cycle < churn.size(); ++cycle)
    {
        CAPTURE(cycle);
        const auto           image = decode_graph_checkpoint(bytes);
        DistributedChildHost host{build_graph<Worker>(), test_end};
        host.start_restored(cycle_time(cycle), image);
        drive(host, cycle, churn[cycle], collected);
        bytes.clear();
        encode_graph_checkpoint(host.capture(), bytes, cycle_time(cycle + 1));
        host.stop();
    }
    CHECK(collected == run_uninterrupted(churn));
}

TEST_CASE("worker graph checkpoint: starting from an image ticks nothing", "[checkpoint][worker]")
{
    register_types();
    Collected  before;
    const auto image = decode_graph_checkpoint(run_until(churn, 3, before));

    DistributedChildHost resumed{build_graph<Worker>(), test_end};
    resumed.start_restored(cycle_time(3), image);
    // The sources hold their baselines and no bootstrap schedule survives, so
    // the graph wants nothing and a cycle with nothing staged produces nothing.
    CHECK(resumed.next_scheduled_time() == MAX_DT);
    REQUIRE(resumed.step(cycle_time(3)));
    CHECK_FALSE(resumed.collect("out").has_value());
    resumed.stop();
}

TEST_CASE("worker graph checkpoint: an image of another graph is refused and nothing starts",
          "[checkpoint][worker]")
{
    register_types();
    Collected  before;
    const auto image = decode_graph_checkpoint(run_until(churn, 2, before));

    // Same nodes, another boundary slot: only the contract signature differs.
    GraphExecutorBuilder eb;
    eb.graph_builder(build_graph<KeyedWorker<"other">>())
        .mode(GraphExecutorMode::ExternallyDriven).start_time(MIN_ST).end_time(test_end);
    GraphExecutorValue executor = eb.make_executor();
    auto               view     = executor.view();
    REQUIRE_THROWS_WITH(view.start_external_restored(cycle_time(2), image), ContainsSubstring("incompatible node"));
    CHECK_FALSE(view.graph().started());
}

TEST_CASE("worker graph checkpoint: restored state must precede the start", "[checkpoint][worker]")
{
    register_types();
    Collected  before;
    const auto image = decode_graph_checkpoint(run_until(churn, 3, before));

    // The last captured cycle ran at cycle_time(2); starting there again would
    // put restored timestamps inside the first evaluation.
    DistributedChildHost resumed{build_graph<Worker>(), test_end};
    REQUIRE_THROWS_WITH(resumed.start_restored(cycle_time(2), image), ContainsSubstring("exceeds the completed cut"));
    CHECK_FALSE(resumed.graph().started());
}

TEST_CASE("worker graph checkpoint: a graph wired outside a checkpoint scope is refused",
          "[checkpoint][worker]")
{
    register_types();
    DistributedChildHost host{build_graph<UnscopedWorker>(), test_end};
    host.start(MIN_ST);
    REQUIRE_THROWS_WITH(host.capture(), ContainsSubstring("has no checkpoint identity"));
    host.stop();
}

TEST_CASE("worker graph checkpoint: the verbs belong to the stepped mode, and capture to a started graph",
          "[checkpoint][worker]")
{
    register_types();
    {
        GraphExecutorBuilder eb;
        eb.graph_builder(build_graph<Worker>()).start_time(MIN_ST).end_time(test_end);
        GraphExecutorValue executor = eb.make_executor();
        CHECK_THROWS_AS(executor.view().capture_external(), std::logic_error);
        CHECK_THROWS_AS(executor.view().start_external_restored(MIN_ST, GraphCheckpointImage{}), std::logic_error);
    }
    GraphExecutorBuilder eb;
    eb.graph_builder(build_graph<Worker>())
        .mode(GraphExecutorMode::ExternallyDriven).start_time(MIN_ST).end_time(test_end);
    GraphExecutorValue executor = eb.make_executor();
    REQUIRE_THROWS_WITH(executor.view().capture_external(), ContainsSubstring("requires a started graph"));
}

TEST_CASE("worker graph checkpoint: work still due at the cut is refused, not captured unevaluated",
          "[checkpoint][worker]")
{
    register_types();
    // The constant schedules itself on start. Captured before its first step
    // it would be saved invalid, and a restored start discards bootstrap
    // schedules -- so it would stay invalid for good.
    DistributedChildHost host{build_graph<Worker>(), test_end};
    host.start(MIN_ST);
    REQUIRE(host.next_scheduled_time() == MIN_ST);
    REQUIRE_THROWS_WITH(host.capture(), ContainsSubstring("work is still due at the cut"));
    Collected collected;
    drive(host, 0, churn[0], collected);
    CHECK_NOTHROW(host.capture());
    host.stop();
}

TEST_CASE("worker graph checkpoint: a worker that sat out a quiet day is captured without a step",
          "[checkpoint][worker]")
{
    register_types();
    // A dmap_ owner steps its workers only when it evaluates. On a day with no
    // input a restored worker is never stepped, and its day still has to end
    // in an image as good as the one it started from.
    const std::size_t split = 3;
    Collected         collected;
    auto              bytes = run_until(churn, split, collected);
    {
        DistributedChildHost quiet{build_graph<Worker>(), test_end};
        quiet.start_restored(cycle_time(split), decode_graph_checkpoint(bytes));
        bytes.clear();
        encode_graph_checkpoint(quiet.capture(), bytes, cycle_time(split));
        quiet.stop();
    }
    // The quiet day took a slot on the clock, so the remaining cycles run one later.
    DistributedChildHost resumed{build_graph<Worker>(), test_end};
    resumed.start_restored(cycle_time(split + 1), decode_graph_checkpoint(bytes));
    for (std::size_t cycle = split; cycle < churn.size(); ++cycle)
    {
        resumed.stage("in", Value{churn[cycle]}.view());
        REQUIRE(resumed.step(cycle_time(cycle + 1)));
        Value out = resumed.collect("out");
        collected.push_back(out.has_value() ? std::optional<Int>{out.view().checked_as<Int>()} : std::nullopt);
    }
    resumed.stop();
    CHECK(collected == run_uninterrupted(churn));
}

TEST_CASE("worker graph checkpoint: a restored node's bootstrap alarm is discarded whole", "[checkpoint][worker]")
{
    register_types();
    std::string bytes;
    {
        DistributedChildHost host{build_graph<ArmedWorker>(), test_end};
        host.start(MIN_ST);
        host.stage("in", Value{Int{1}}.view());
        REQUIRE(host.step(cycle_time(0)));
        REQUIRE(host.collect("out").has_value());
        encode_graph_checkpoint(host.capture(), bytes, cycle_time(1));
        host.stop();
    }
    DistributedChildHost resumed{build_graph<ArmedWorker>(), test_end};
    resumed.start_restored(cycle_time(1), decode_graph_checkpoint(bytes));
    // A quiet first cycle leaves the start-time alarm unconsumed. Discarding
    // only the graph's slot left it in the node's scheduler state, to be
    // re-armed after the next evaluation -- in the past, which the graph
    // refuses.
    REQUIRE(resumed.step(cycle_time(1)));
    CHECK_FALSE(resumed.collect("out").has_value());
    resumed.stage("in", Value{Int{2}}.view());
    REQUIRE(resumed.step(cycle_time(2)));
    const Value out = resumed.collect("out");
    REQUIRE(out.has_value());
    CHECK(out.view().checked_as<Int>() == 2);
    resumed.stop();
}
