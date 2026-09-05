// Tests for the GlobalState injectable: a mutable string -> Any store created
// at wiring time on the GraphBuilder, carried (copied) onto each GraphValue,
// readable/mutable at run time via GraphView::global_state().

#include <catch2/catch_test_macros.hpp>

#include <cstdint>

#include <hgraph/lib/testing/mock_runtime.h>
#include <hgraph/runtime/runtime.h>
#include <hgraph/types/graph_wiring.h>
#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/metadata/value_plan_factory.h>
#include <hgraph/types/static_node.h>
#include <hgraph/types/value/value.h>

#include <string>

namespace
{
    using namespace hgraph;

    // Source node that emits a value read from the graph's GlobalState.
    struct EmitSeed
    {
        static constexpr auto name              = "emit_seed";
        static constexpr bool schedule_on_start = true;
        static void           eval(GlobalStateView gs, Out<TS<std::int32_t>> out) { out.set(gs.get_as<std::int32_t>("seed")); }
    };

    // Source node that writes into the GlobalState and emits a constant.
    struct StashConst
    {
        static constexpr auto name              = "stash_const";
        static constexpr bool schedule_on_start = true;
        static void           eval(GlobalStateView gs, Out<TS<std::int32_t>> out)
        {
            gs.set("stashed", Value{std::int32_t{5}});
            out.set(5);
        }
    };

    // Source node that reads "counter" from the global state, increments it back
    // into the store, and emits the new value.
    struct BumpCounter
    {
        static constexpr auto name              = "bump_counter";
        static constexpr bool schedule_on_start = true;
        static void           eval(GlobalStateView gs, Out<TS<std::int32_t>> out)
        {
            const int next = gs.get_as<std::int32_t>("counter") + 1;
            gs.set("counter", Value{next});
            out.set(next);
        }
    };

    // A graph whose compose body seeds the global state at wiring time, then
    // wires a node that modifies it during evaluation.
    struct CounterGraph
    {
        static constexpr auto name = "counter_graph";
        static void           compose(Wiring &w)
        {
            w.global_state().set("counter", Value{std::int32_t{100}});  // set during wiring (compose)
            wire<BumpCounter>(w);
        }
    };
}  // namespace

TEST_CASE("global state: set / get / contains / erase with heterogeneous values")
{
    using namespace hgraph;
    auto &registry = TypeRegistry::instance();
    (void)registry.register_scalar<std::int32_t>("int32");
    (void)registry.register_scalar<double>("double");

    GlobalState     owner;
    GlobalStateView gs = owner.view();  // value held by the owner; access via the view
    CHECK(gs.size() == 0);
    CHECK_FALSE(gs.contains("missing"));
    CHECK_FALSE(gs.get("missing").valid());

    gs.set("count", Value{std::int32_t{42}});
    gs.set("ratio", Value{1.5});
    REQUIRE(gs.size() == 2);
    CHECK(gs.contains("count"));
    CHECK(gs.get_as<std::int32_t>("count") == 42);
    CHECK(gs.get_as<double>("ratio") == 1.5);

    // Replace with a different schema entirely.
    gs.set("count", Value{std::string{"many"}});
    CHECK(gs.get_as<std::string>("count") == "many");

    CHECK(gs.erase("ratio"));
    CHECK_FALSE(gs.erase("ratio"));
    CHECK(gs.size() == 1);
    CHECK_FALSE(gs.contains("ratio"));
}

TEST_CASE("global state: seeded on the builder at wiring time, read back at run time")
{
    using namespace hgraph;
    auto &registry = TypeRegistry::instance();
    (void)registry.register_scalar<std::int32_t>("int32");

    GraphBuilder builder;
    builder.global_state().set("seed", Value{std::int32_t{7}});

    testing::MockRootGraph graph{builder};
    auto       view  = graph.graph();

    // Wiring-time entry is visible at run time...
    CHECK(view.global_state().get_as<std::int32_t>("seed") == 7);
    // ...and the runtime state is mutable.
    view.global_state().set("runtime", Value{std::int32_t{99}});
    CHECK(view.global_state().get_as<std::int32_t>("runtime") == 99);

    // root() of a flattened graph is the graph itself.
    CHECK(view.root().global_state().get_as<std::int32_t>("seed") == 7);

    // The builder's own copy is untouched by the runtime mutation.
    CHECK_FALSE(builder.global_state().contains("runtime"));
}

TEST_CASE("global state: the builder is reusable; each graph gets its own state")
{
    using namespace hgraph;
    auto &registry = TypeRegistry::instance();
    (void)registry.register_scalar<std::int32_t>("int32");

    GraphBuilder builder;
    builder.global_state().set("seed", Value{std::int32_t{1}});

    testing::MockRootGraph a{builder};
    testing::MockRootGraph b{builder};

    a.graph().global_state().set("only_a", Value{std::int32_t{2}});

    CHECK(a.graph().global_state().contains("seed"));
    CHECK(b.graph().global_state().contains("seed"));   // both seeded
    CHECK(a.graph().global_state().contains("only_a"));
    CHECK_FALSE(b.graph().global_state().contains("only_a"));  // independent runtime state
}

TEST_CASE("global state: a node reads its graph's GlobalState via the injectable selector")
{
    using namespace hgraph;
    auto &registry = TypeRegistry::instance();
    (void)registry.register_scalar<std::int32_t>("int32");

    GraphBuilder builder;
    builder.add_node(NodeBuilder{}.implementation<EmitSeed>());
    builder.global_state().set("seed", Value{std::int32_t{77}});

    testing::MockRootGraph graph{builder};
    auto       view  = graph.graph();
    const auto t1    = MIN_ST;

    view.start(t1);
    view.evaluate(t1);

    CHECK(view.node_at(0).output(t1).value().checked_as<std::int32_t>() == 77);
}

TEST_CASE("global state: a node writes into its graph's GlobalState during eval")
{
    using namespace hgraph;
    auto &registry = TypeRegistry::instance();
    (void)registry.register_scalar<std::int32_t>("int32");

    GraphBuilder builder;
    builder.add_node(NodeBuilder{}.implementation<StashConst>());

    testing::MockRootGraph graph{builder};
    auto       view  = graph.graph();
    const auto t1    = MIN_ST;

    CHECK_FALSE(view.global_state().contains("stashed"));
    view.start(t1);
    view.evaluate(t1);

    REQUIRE(view.global_state().contains("stashed"));
    CHECK(view.global_state().get_as<std::int32_t>("stashed") == 5);
    CHECK(view.node_at(0).output(t1).value().checked_as<std::int32_t>() == 5);
}

TEST_CASE("global state: reachable from an evaluated executor")
{
    using namespace hgraph;
    auto &registry = TypeRegistry::instance();
    (void)registry.register_scalar<std::int32_t>("int32");

    GraphBuilder builder;
    builder.add_node(NodeBuilder{}.implementation<StashConst>());
    builder.global_state().set("seed", Value{std::int32_t{11}});  // seeded at wiring time

    GraphExecutorBuilder executor_builder;
    executor_builder.graph_builder(std::move(builder))
        .start_time(MIN_ST)
        .end_time(MIN_ST + TimeDelta{2});

    GraphExecutorValue executor      = executor_builder.make_executor();
    auto               executor_view = executor.view();
    executor_view.run();

    // Reach the global state through the executor's graph after the run.
    GlobalStateView gs = executor_view.graph().global_state();
    CHECK(gs.get_as<std::int32_t>("seed") == 11);     // wiring-time seed carried through the executor
    CHECK(gs.get_as<std::int32_t>("stashed") == 5);   // written by the node during evaluation
}

TEST_CASE("global state: set in a compose block, then modified in an eval block")
{
    using namespace hgraph;
    auto &registry = TypeRegistry::instance();
    (void)registry.register_scalar<std::int32_t>("int32");

    // compose() seeds "counter" = 100 at wiring time; build_graph runs it once.
    GraphBuilder builder = build_graph<CounterGraph>();

    GraphExecutorBuilder executor_builder;
    executor_builder.graph_builder(std::move(builder))
        .start_time(MIN_ST)
        .end_time(MIN_ST + TimeDelta{2});

    GraphExecutorValue executor      = executor_builder.make_executor();
    auto               executor_view = executor.view();
    executor_view.run();

    // The eval block bumped the wiring-time seed by one.
    GlobalStateView gs = executor_view.graph().global_state();
    CHECK(gs.get_as<std::int32_t>("counter") == 101);  // 100 set in compose + 1 set in eval
    CHECK(executor_view.graph().node_at(0).output(MIN_ST).value().checked_as<std::int32_t>() == 101);
}

TEST_CASE("global state: a stored mutable value comes back mutable and can be edited in place")
{
    using namespace hgraph;
    auto       &registry = TypeRegistry::instance();
    const auto *int_meta = registry.register_scalar<std::int32_t>("int32");

    // Build a mutable List<int> [1, 2] and stash it.
    const auto *mutable_schema  = registry.mutable_list(int_meta);
    const auto mutable_binding = ValuePlanFactory::instance().type_for(mutable_schema);
    Value       list{mutable_binding};
    {
        auto m = list.as_list().begin_mutation();
        m.push_back(Value{std::int32_t{1}}.view());
        m.push_back(Value{std::int32_t{2}}.view());
    }

    GlobalState gs;
    gs.view().set("buf", list);

    // The GlobalState is a mutable view: a mutable value comes back writable, so
    // it can be appended in place...
    gs.view().get("buf").as_list().begin_mutation().push_back(Value{std::int32_t{3}}.view());

    // ...and the edit persists in the store.
    const auto stored = gs.view().get("buf").as_list();
    REQUIRE(stored.size() == 3);
    CHECK(stored.at(0).checked_as<std::int32_t>() == 1);
    CHECK(stored.at(2).checked_as<std::int32_t>() == 3);
}

TEST_CASE("global state: a stored immutable value stays read-only")
{
    using namespace hgraph;
    auto       &registry = TypeRegistry::instance();
    const auto *int_meta = registry.register_scalar<std::int32_t>("int32");

    // An immutable (compact) list — its ops do not opt into mutation.
    const auto *immutable_schema  = registry.list(int_meta);
    const auto immutable_binding = ValuePlanFactory::instance().type_for(immutable_schema);
    Value       list{immutable_binding};

    GlobalState gs;
    gs.view().set("frozen", list);

    // Mutability is honoured: the immutable value is refused mutation even though
    // the store itself is mutable.
    CHECK_THROWS(gs.view().get("frozen").as_list().begin_mutation());
}

TEST_CASE("global context: seeds builders by copy and does not nest")
{
    using namespace hgraph;
    auto &registry = TypeRegistry::instance();
    (void)registry.register_scalar<std::int32_t>("int32");

    GlobalState seed;
    seed.view().set("seed", Value{std::int32_t{7}});

    GraphBuilder builder;
    {
        GlobalContext context{seed};
        CHECK(GlobalContext::active_state() == &seed);
        CHECK_THROWS_AS(GlobalContext{}, std::logic_error);

        builder = build_graph<CounterGraph>();
        builder.global_state().set("builder_only", Value{std::int32_t{9}});
        CHECK_FALSE(seed.view().contains("builder_only"));
    }
    CHECK(GlobalContext::active_state() == nullptr);

    GraphExecutorBuilder executor_builder;
    executor_builder.graph_builder(std::move(builder))
        .start_time(MIN_ST)
        .end_time(MIN_ST + TimeDelta{2});
    GraphExecutorValue executor = executor_builder.make_executor();
    executor.view().run();

    CHECK(executor.view().graph().global_state().get_as<std::int32_t>("counter") == 101);
    // The WIRING write ("counter" = 100 in compose) lands on the LIVE
    // selected seed (ruling 2026-07-27); the RUNTIME bump to 101 happens on
    // the graph's isolation copy taken at build and never reaches the seed.
    CHECK(seed.view().get_as<std::int32_t>("counter") == 100);
}

TEST_CASE("global context: the selected state must span the wiring process")
{
    using namespace hgraph;
    auto &registry = TypeRegistry::instance();
    (void)registry.register_scalar<std::int32_t>("int32");

    // A wiring whose selecting context exits early holds NO retained
    // pointer: reads fall back to the internal store (no dangling), and
    // fixing the seed at build fails loudly.
    Wiring wiring;
    {
        GlobalState  seed;
        seed.view().set("seed", Value{std::int32_t{7}});
        GlobalContext context{seed};
        Wiring        live_wiring;
        CHECK(live_wiring.global_state().get_as<std::int32_t>("seed") == 7);
        wiring = std::move(live_wiring);
    }
    CHECK_FALSE(wiring.global_state().contains("seed"));
    CounterGraph::compose(wiring);
    CHECK_THROWS_AS(std::move(wiring).finish(), std::logic_error);
}

TEST_CASE("global state: a wiring binds an explicit seed without a context")
{
    // The bridge's path (no-thread-locals ruling 2026-09-05): the seed is
    // handed to the Wiring, read and written live through the binding, and
    // fixed by copy at the build; two such wirings coexist with distinct
    // seeds, which is what lets distinct runs proceed on distinct threads.
    GlobalState first;
    GlobalState second;
    first.view().set("who", Value{std::string{"first"}});
    second.view().set("who", Value{std::string{"second"}});
    CHECK(GlobalContext::active() == nullptr);

    Wiring wiring_a{first};
    Wiring wiring_b{second};
    CHECK(wiring_a.seed_state() == &first);
    CHECK(wiring_b.seed_state() == &second);
    CHECK(wiring_a.global_state().get_as<std::string>("who") == "first");
    CHECK(wiring_b.global_state().get_as<std::string>("who") == "second");

    wiring_a.global_state().set("written", Value{std::int32_t{1}});
    CHECK(first.view().contains("written"));          // live: written through the binding
    CHECK_FALSE(second.view().contains("written"));

    CounterGraph::compose(wiring_a);
    GraphBuilder builder = std::move(wiring_a).finish();
    CHECK(builder.global_state().get_as<std::string>("who") == "first");
    builder.global_state().set("builder_only", Value{std::int32_t{9}});
    CHECK_FALSE(first.view().contains("builder_only"));   // the build fixed a copy

    // An explicit seed is never combined with an active context.
    GlobalContext context;
    CHECK_THROWS_AS(Wiring{second}, std::logic_error);
}

TEST_CASE("global state: a child wiring reads its root's seed through its own binding")
{
    GlobalState seed;
    seed.view().set("root", Value{std::int32_t{3}});
    Wiring root{seed};
    Wiring child = root.child_wiring();
    CHECK(child.seed_state() == &seed);
    CHECK(child.operator_state().get_as<std::int32_t>("root") == 3);
    root.release_seed();
    CHECK(root.seed_state() == nullptr);
    CHECK_FALSE(root.global_state().contains("root"));
}
