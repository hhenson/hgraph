// Tests for the GlobalState injectable: a mutable string -> Any store created
// at wiring time on the GraphBuilder, carried (copied) onto each GraphValue,
// readable/mutable at run time via GraphView::global_state().

#include <catch2/catch_test_macros.hpp>

#include <cstdint>

#include <hgraph/lib/testing/mock_runtime.h>
#include <hgraph/runtime/runtime.h>
#include <hgraph/types/graph_wiring.h>
#include <hgraph/types/subgraph_wiring.h>
#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/metadata/value_plan_factory.h>
#include <hgraph/types/static_node.h>
#include <hgraph/types/value/value.h>
#include <hgraph/types/value/value_builder.h>
#include <hgraph/types/value/mutable_container_ops.h>
#include <hgraph/types/utils/counted_mutex.h>

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

TEST_CASE("global state: a child wiring shares its root's seed binding, and its detach")
{
    GlobalState seed;
    seed.view().set("root", Value{std::int32_t{3}});
    Wiring root{seed};
    Wiring child = root.child_wiring();
    CHECK(child.seed_state() == &seed);
    CHECK(child.operator_state().get_as<std::int32_t>("root") == 3);
    // The owner's release detaches the binding for every holder: the child
    // never copied the raw pointer out.
    root.release_seed();
    CHECK(root.seed_state() == nullptr);
    CHECK(child.seed_state() == nullptr);
    CHECK_FALSE(root.global_state().contains("root"));
    CHECK_FALSE(child.operator_state().contains("root"));
}

TEST_CASE("global state: a child wiring that outlives its context is detached with it")
{
    // Review finding on the binding: a child created under a context must
    // not keep a raw copy of the context-owned state.
    Wiring child;
    {
        GlobalContext context;
        context.state().view().set("root", Value{std::int32_t{5}});
        Wiring root;
        child = root.child_wiring();
        CHECK(child.seed_state() == &context.state());
        CHECK(child.operator_state().get_as<std::int32_t>("root") == 5);
    }
    CHECK(child.seed_state() == nullptr);
    CHECK_FALSE(child.operator_state().contains("root"));
    CompiledSubGraph compiled = std::move(child).finish_subgraph(std::nullopt, {});
    CHECK_FALSE(compiled.graph_builder.global_state().contains("root"));
}

TEST_CASE("prepared global entries separate absent values from present empty values", "[global-state][prepared]")
{
    using namespace hgraph;
    GlobalState owner;
    Value empty{std::string{}};
    const auto entry = owner.view().prepare("text", empty.binding());
    CHECK(entry.binding() == empty.binding());
    CHECK_FALSE(owner.view().get("text").has_value());
    CHECK_FALSE(owner.view().contains("text"));
    CHECK(owner.view().size() == 0U);
    CHECK_THROWS(entry.get());
    entry.set(empty.view());
    CHECK(entry.get().checked_as<std::string>().empty());
    CHECK(owner.view().contains("text"));
    CHECK(owner.view().get_as<std::string>("text").empty());
}

TEST_CASE("prepared global entries reject absent writes and keep the absent count exact", "[global-state][prepared]")
{
    using namespace hgraph;
    GlobalState owner;
    Value       seed{std::int64_t{1}};
    const Value null  = Value::typed_null(seed.binding());
    const auto  entry = owner.view().prepare("counter", seed.binding());
    CHECK(owner.view().size() == 0U);

    // A typed null has no payload to retain: rejected, cell unchanged, count intact.
    CHECK_THROWS_AS(entry.set(null.view()), std::invalid_argument);
    CHECK_THROWS_AS(entry.set(null.view()), std::invalid_argument);
    CHECK_FALSE(owner.view().contains("counter"));
    CHECK(owner.view().size() == 0U);
    CHECK_THROWS(entry.get());

    entry.set(seed.view());
    CHECK(owner.view().contains("counter"));
    CHECK(owner.view().size() == 1U);
    CHECK(entry.get().checked_as<std::int64_t>() == 1);

    // The keyed write path reaches the same handle and the same rule.
    CHECK_THROWS_AS(owner.view().set("counter", null.view()), std::invalid_argument);
    CHECK_THROWS_AS(entry.set(ValueView{}), std::invalid_argument);
    CHECK(owner.view().contains("counter"));
    CHECK(owner.view().size() == 1U);
    CHECK(entry.get().checked_as<std::int64_t>() == 1);
}

TEST_CASE("prepared global entries reconcile exact types before use", "[global-state][prepared]")
{
    using namespace hgraph;
    GlobalState owner;
    Value integer{std::int32_t{3}};
    Value text{std::string{"seed"}};
    owner.view().set("seeded", text);
    CHECK_THROWS(owner.view().prepare("seeded", integer.binding()));
    CHECK(owner.view().get_as<std::string>("seeded") == "seed");
    auto first = owner.view().prepare("counter", integer.binding());
    CHECK_THROWS(owner.view().prepare("counter", text.binding()));
    auto second = owner.view().prepare("counter", integer.binding());
    first.set(integer.view());
    CHECK(second.get().checked_as<std::int32_t>() == 3);
    integer = Value{std::int32_t{7}};
    second.set(integer.view());
    CHECK(first.get().checked_as<std::int32_t>() == 7);
}

TEST_CASE("prepared global handles survive unrelated store growth and rejected replacement", "[global-state][prepared]")
{
    using namespace hgraph;
    GlobalState owner;
    Value initial{std::int32_t{5}};
    auto entry = owner.view().prepare("kept", initial.binding());
    entry.set(initial.view());
    for (int index = 0; index < 4096; ++index) {
        owner.view().set("other-" + std::to_string(index), initial);
    }
    CHECK(entry.get().checked_as<std::int32_t>() == 5);
    CHECK_THROWS(owner.view().erase("kept"));
    GlobalState replacement;
    CHECK_THROWS(owner.view().copy_from(replacement.view()));
    CHECK_THROWS(owner.view().set("kept", Value{std::string{"wrong type"}}));
    CHECK(entry.get().checked_as<std::int32_t>() == 5);
    owner.view().set("kept", Value{std::int32_t{9}});
    CHECK(entry.get().checked_as<std::int32_t>() == 9);
    CHECK(owner.view().erase("other-0"));
    CHECK(entry.get().checked_as<std::int32_t>() == 9);
}

TEST_CASE("prepared global entries retain independent mutable values", "[global-state][prepared]")
{
    using namespace hgraph;
    auto &registry = TypeRegistry::instance();
    const auto *element = registry.register_scalar<std::int32_t>("int32");
    const auto binding = ValuePlanFactory::instance().type_for(registry.mutable_list(element));
    Value source{binding};
    source.as_list().begin_mutation().push_back(Value{std::int32_t{1}}.view());
    GlobalState owner;
    auto entry = owner.view().prepare("values", binding);
    entry.set(source.view());
    source.as_list().begin_mutation().push_back(Value{std::int32_t{2}}.view());
    REQUIRE(entry.get().as_list().size() == 1U);
    CHECK(entry.get().as_list().at(0).checked_as<std::int32_t>() == 1);
    Value empty{binding};
    entry.set(empty.view());
    CHECK(entry.get().as_list().empty());
}

TEST_CASE("prepared entries reject conflicting layouts of the same schema", "[global-state][prepared]")
{
    auto &registry = TypeRegistry::instance();
    const auto *element = registry.register_scalar<std::int32_t>("int32");
    const auto *schema = registry.list(element);
    const auto element_binding = ValuePlanFactory::instance().type_for(element);
    const auto compact = compact_list_type(element_binding, *schema);
    const auto slots = intern_value_type(*schema, mutable_list_plan(element_binding), mutable_list_ops());
    REQUIRE(compact.schema() == slots.schema());
    REQUIRE(compact != slots);
    ListBuilder values{element_binding, *schema};
    values.push_back(std::int32_t{7});
    const Value compact_value = values.build();
    const Value slot_value{slots, compact_value.view()};

    for (const bool compact_first : {false, true}) {
        for (const bool seeded : {false, true}) {
            INFO("compact first: " << compact_first << ", seeded: " << seeded);
            const auto binding = compact_first ? compact : slots;
            const auto other_binding = compact_first ? slots : compact;
            const auto &other_value = compact_first ? slot_value : compact_value;
            GlobalState owner;
            if (seeded) { owner.view().set("values", other_value); }
            auto first = owner.view().prepare("values", binding);
            const auto *access = checked_value_ops<IndexedValueOps>(binding, "prepared list test");
            CHECK_THROWS_AS(owner.view().prepare("values", other_binding), std::invalid_argument);
            CHECK(owner.view().contains("values") == seeded);
            if (!seeded) { first.set(other_value.view()); }
            REQUIRE(first.get().binding() == binding);
            CHECK(access->size(access->context, first.get().data()) == 1U);
            CHECK(first.get().as_list().at(0).checked_as<std::int32_t>() == 7);
            // Ordinary keyed writes still normalize compatible representations.
            owner.view().set("values", other_value);
            auto repeated = owner.view().prepare("values", binding);
            REQUIRE(repeated.get().binding() == binding);
            CHECK(access->size(access->context, first.get().data()) == 1U);
            CHECK(owner.view().size() == 1U);
        }
    }
}

TEST_CASE("wiring rejects conflicting entry layouts before runtime preparation", "[global-state][prepared][wiring]")
{
    auto &registry = TypeRegistry::instance();
    const auto *element = registry.register_scalar<std::int32_t>("int32");
    const auto *schema = registry.list(element);
    const auto element_binding = ValuePlanFactory::instance().type_for(element);
    const auto compact = compact_list_type(element_binding, *schema);
    const auto slots = intern_value_type(*schema, mutable_list_plan(element_binding), mutable_list_ops());
    REQUIRE(compact.schema() == slots.schema());
    REQUIRE(compact != slots);
    for (const bool compact_first : {false, true}) {
        const auto binding = compact_first ? compact : slots;
        const auto other_binding = compact_first ? slots : compact;
        Wiring root;
        root.prepare_global_entry("values", binding);
        auto child = root.child_wiring();
        CHECK_THROWS_AS(child.prepare_global_entry("values", other_binding), std::invalid_argument);
        CHECK_NOTHROW(child.prepare_global_entry("values", binding));
        auto builder = std::move(root).finish();
        auto entry = builder.global_state().prepare("values", binding);
        CHECK_THROWS_AS(builder.global_state().prepare("values", other_binding), std::invalid_argument);
        Value empty{other_binding};
        entry.set(empty.view());
        REQUIRE(entry.get().binding() == binding);
        CHECK(entry.get().as_list().empty());
    }
}

TEST_CASE("copied global stores bind new prepared handles to independent cells", "[global-state][prepared]")
{
    using namespace hgraph;
    Value seed{std::int32_t{1}};
    GlobalState original;
    auto old = original.view().prepare("counter", seed.binding());
    old.set(seed.view());
    GlobalState copied{original};
    auto fresh = copied.view().prepare("counter", seed.binding());
    fresh.set(Value{std::int32_t{2}}.view());
    CHECK(old.get().checked_as<std::int32_t>() == 1);
    CHECK(fresh.get().checked_as<std::int32_t>() == 2);
    old.set(Value{std::int32_t{3}}.view());
    CHECK(fresh.get().checked_as<std::int32_t>() == 2);
}

TEST_CASE("repeated prepared global access does not acquire type-system locks", "[global-state][prepared]")
{
    using namespace hgraph;
    GlobalState owner;
    Value value{std::int32_t{42}};
    auto entry = owner.view().prepare("value", value.binding());
    const auto before = type_system_lock_count();
    for (int index = 0; index < 1024; ++index) {
        entry.set(value.view());
        REQUIRE(entry.get().checked_as<std::int32_t>() == 42);
    }
    CHECK(type_system_lock_count() == before);
}

TEST_CASE("node preparation precedes all start hooks and survives graph moves", "[global-state][prepared]")
{
    using namespace hgraph;
    unsigned prepared = 0;
    unsigned started = 0;
    Value seed{std::int32_t{23}};
    PreparedGlobalEntry entry;
    NodeTypeMetaData meta;
    meta.display_name = "prepared_global_test";
    meta.node_kind = NodeKind::Sink;
    NodeCallbacks callbacks;
    callbacks.prepare = [&](const NodeView &node) {
        ++prepared;
        CHECK(started == 0U);
        entry = node.graph().global_state().prepare("seed", seed.binding());
    };
    callbacks.start = [&](const NodeView &, DateTime) {
        ++started;
        CHECK(prepared == 1U);
        CHECK(entry.get().checked_as<std::int32_t>() == 23);
    };
    GraphBuilder builder;
    builder.global_state().set("seed", seed);
    builder.add_node(NodeBuilder::native(std::move(meta), std::move(callbacks)));
    testing::MockRootGraph executor{GraphBuilder{}};
    auto graph = builder.make_root_graph(executor.executor().pointer());
    CHECK(prepared == 1U);
    CHECK(started == 0U);
    auto moved = std::move(graph);
    CHECK(prepared == 1U);
    moved.view().start(MIN_ST);
    CHECK(started == 1U);
    moved.view().stop(MIN_ST);
}

TEST_CASE("failed node preparation prevents every start hook", "[global-state][prepared]")
{
    using namespace hgraph;
    unsigned prepared = 0;
    unsigned started = 0;
    GraphBuilder builder;
    for (unsigned index = 0; index < 2; ++index) {
        NodeTypeMetaData meta;
        meta.display_name = index == 0 ? "preparation_first" : "preparation_failure";
        meta.node_kind = NodeKind::Sink;
        NodeCallbacks callbacks;
        callbacks.prepare = [&, index](const NodeView &) {
            ++prepared;
            if (index == 1U) { throw std::runtime_error("preparation failed"); }
        };
        callbacks.start = [&](const NodeView &, DateTime) { ++started; };
        builder.add_node(NodeBuilder::native(std::move(meta), std::move(callbacks)));
    }
    CHECK_THROWS(testing::MockRootGraph{builder});
    CHECK(prepared == 2U);
    CHECK(started == 0U);
}

namespace {
    struct PreparedStaticSource {
        static constexpr auto name = "prepared_static_source";
        static constexpr bool schedule_on_start = true;
        static void prepare(const hgraph::NodeView &node) {
            node.graph().global_state().set("prepared", hgraph::Value{std::int32_t{17}});
        }
        static void start(hgraph::GlobalStateView state) {
            state.set("started", hgraph::Value{state.get_as<std::int32_t>("prepared") + 1});
        }
        static void eval(hgraph::GlobalStateView state, hgraph::Out<hgraph::TS<std::int32_t>> out) {
            out.set(state.get_as<std::int32_t>("started"));
        }
    };
}

TEST_CASE("static native nodes expose preparation before their start selector", "[global-state][prepared]")
{
    using namespace hgraph;
    GraphBuilder builder;
    builder.add_node(NodeBuilder{}.implementation<PreparedStaticSource>());
    testing::MockRootGraph graph{builder};
    CHECK(graph.graph().global_state().get_as<std::int32_t>("prepared") == 17);
    CHECK_FALSE(graph.graph().global_state().contains("started"));
    graph.graph().start(MIN_ST);
    graph.graph().evaluate(MIN_ST);
    CHECK(graph.graph().node_at(0).output(MIN_ST).value().checked_as<std::int32_t>() == 18);
    graph.graph().stop(MIN_ST);
}

TEST_CASE("copied prepared stores track absent entries independently", "[global-state][prepared]")
{
    using namespace hgraph;
    Value seed{std::int32_t{1}};
    GlobalState original;
    auto old = original.view().prepare("counter", seed.binding());
    GlobalState copied{original};
    auto fresh = copied.view().prepare("counter", seed.binding());
    CHECK(original.view().size() == 0U);
    CHECK(copied.view().size() == 0U);
    fresh.set(seed.view());
    CHECK(copied.view().size() == 1U);
    CHECK(original.view().size() == 0U);
    CHECK_FALSE(original.view().contains("counter"));
    old.set(seed.view());
    CHECK(original.view().size() == 1U);
    CHECK(copied.view().size() == 1U);
    old.set(seed.view());
    CHECK(original.view().size() == 1U);
}

TEST_CASE("copy out omits unpublished prepared entries", "[global-state][prepared]")
{
    using namespace hgraph;
    GlobalState running;
    Value seed{std::int32_t{7}};
    auto absent = running.view().prepare("absent", seed.binding());
    auto present = running.view().prepare("present", seed.binding());
    present.set(seed.view());
    GlobalState result;
    result.view().copy_from(running.view());
    CHECK(result.view().size() == 1U);
    CHECK_FALSE(result.view().contains("absent"));
    CHECK(result.view().get_as<std::int32_t>("present") == 7);
    absent.set(seed.view());
    result.view().copy_from(running.view());
    CHECK(result.view().size() == 2U);
    CHECK(result.view().get_as<std::int32_t>("absent") == 7);
}

TEST_CASE("wiring entry declarations preserve live seed copy-out and independent runs", "[global-state][prepared][wiring]")
{
    Value integer{std::int32_t{4}};
    GlobalState seed;
    seed.view().set("entry", integer);
    Wiring wiring{seed};
    wiring.prepare_global_entry("entry", integer.binding());
    wiring.prepare_global_entry("unpublished", integer.binding());
    CHECK(wiring.has_global_entry("entry"));
    CHECK_FALSE(seed.view().is_prepared("entry"));
    CHECK_FALSE(seed.view().is_prepared("unpublished"));
    CHECK(seed.view().size() == 1);

    auto builder = std::move(wiring).finish();
    auto entry = builder.global_state().prepare("entry", integer.binding());
    entry.set(Value{std::int32_t{9}}.view());
    CHECK(seed.view().get_as<std::int32_t>("entry") == 4);
    // The same copy-out operation used by LowerExecution::run must still
    // succeed: wiring declarations never turn the owner into a prepared store.
    seed.view().copy_from(builder.global_state());
    CHECK(seed.view().get_as<std::int32_t>("entry") == 9);
    CHECK_FALSE(seed.view().contains("unpublished"));
    CHECK_FALSE(seed.view().is_prepared("entry"));

    Value text{std::string{"new run"}};
    seed.view().set("entry", text);
    Wiring next{seed};
    next.prepare_global_entry("entry", text.binding());
    auto next_builder = std::move(next).finish();
    CHECK(next_builder.global_state().get_as<std::string>("entry") == "new run");
}

TEST_CASE("root and known child wirings reconcile declarations without sharing stores", "[global-state][prepared][wiring]")
{
    Value integer{std::int32_t{4}};
    Value text{std::string{"wrong"}};
    Wiring root;
    root.global_state().set("seed", integer);
    auto child = root.child_wiring();
    auto grandchild = child.child_wiring();
    grandchild.prepare_global_entry("child_entry", integer.binding());
    CHECK(root.has_global_entry("child_entry"));
    CHECK(child.has_global_entry("child_entry"));
    CHECK_FALSE(child.global_state().contains("seed"));
    CHECK_THROWS(root.prepare_global_entry("child_entry", text.binding()));
    auto compiled = std::move(grandchild).finish_subgraph(std::nullopt, {});
    CHECK_FALSE(compiled.graph_builder.global_state().is_prepared("child_entry"));
    auto builder = std::move(root).finish();
    CHECK(builder.global_state().is_prepared("child_entry"));
    CHECK_FALSE(builder.global_state().contains("child_entry"));
    CHECK_NOTHROW(child.prepare_global_entry("child_entry", integer.binding()));
    CHECK_THROWS(child.prepare_global_entry("unknown_late_key", integer.binding()));
}

TEST_CASE("entry declarations validate initial and final seeds before runtime preparation", "[global-state][prepared][wiring]")
{
    Value integer{std::int32_t{4}};
    Value text{std::string{"wrong"}};
    GlobalState seed;
    seed.view().set("entry", text);
    Wiring root{seed};
    CHECK_THROWS(root.prepare_global_entry("entry", integer.binding()));
    CHECK_FALSE(root.has_global_entry("entry"));
    seed.view().set("entry", integer);
    root.prepare_global_entry("entry", integer.binding());
    seed.view().set("entry", text);
    CHECK_THROWS(std::move(root).finish());
    CHECK(seed.view().get_as<std::string>("entry") == "wrong");
    CHECK_FALSE(seed.view().is_prepared("entry"));
}

TEST_CASE("wiring snapshots realize independent copies of declaration plans", "[global-state][prepared][wiring]")
{
    Value integer{std::int32_t{4}};
    Wiring wiring;
    wiring.prepare_global_entry("first", integer.binding());
    auto first = wiring.snapshot();
    wiring.prepare_global_entry("second", integer.binding());
    auto second = wiring.snapshot();
    CHECK(first.global_state().is_prepared("first"));
    CHECK_FALSE(first.global_state().is_prepared("second"));
    CHECK(second.global_state().is_prepared("first"));
    CHECK(second.global_state().is_prepared("second"));
    CHECK_FALSE(wiring.global_state().is_prepared("first"));
    auto first_entry = first.global_state().prepare("first", integer.binding());
    first_entry.set(integer.view());
    CHECK_FALSE(second.global_state().contains("first"));
}
