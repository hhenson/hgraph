// Recovery through ``dmap_`` (RFC 0039, "Recovery of worker-hosted graphs").
//
// The owner is a dynamic-graph owner whose children live in workers. Its
// checkpoint state is one image per worker, so the acceptance is the one
// every recoverable owner has: a run restarted at each completed-day boundary
// emits the same output deltas as a run that was never interrupted.

#include <hgraph/lib/std/component.h>
#include <hgraph/lib/std/std_operators.h>
#include <hgraph/lib/testing/check_output.h>
#include <hgraph/lib/testing/eval_node.h>
#include <hgraph/runtime/component_checkpoint.h>
#include <hgraph/runtime/distributed_map_wiring.h>
#include <hgraph/runtime/checkpoint_codec.h>
#include <hgraph/runtime/distributed_process.h>
#include <hgraph/runtime/distributed_worker.h>
#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/value/value_builder.h>

#include "distributed_worker_recipes.h"

#include <catch2/catch_test_macros.hpp>
#include <fmt/format.h>
#include <catch2/matchers/catch_matchers_string.hpp>

#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <memory>
#include <optional>
#include <vector>

namespace
{
    using namespace hgraph;
    using namespace hgraph::distributed;
    using namespace hgraph::testing;
    using hgraph_test::PreparedAccumulate;
    using Dict = TSD<Str, TS<Int>>;

    template <typename In, typename Out, typename Child, fixed_string Recipe, WorkerHosting Hosting>
    struct PreparedBody
    {
        static Port<Out> compose(Wiring &w, NamedPort<"ts", In> ts)
        {
            const std::vector<DistributedMapInput> inputs{{ts.erased().schema}};
            WorkerPoolConfig config;
            config.workers = 3;
            config.hosting = Hosting;
            if (Hosting == WorkerHosting::Process) { config.program = HGRAPH_TEST_WORKER_PROGRAM; }
            auto plan = prepare_distributed_map_pool(fn<Child>(), inputs, {}, config);
            if (Hosting == WorkerHosting::Process) { bind_distributed_map_recipe(plan, Recipe.sv()); }
            return wire_distributed_map(w, ts.erased(), std::make_shared<const DistributedMapPlan>(std::move(plan)))
                .template as<Out>();
        }
    };
    template <typename In, typename Out, typename Child, fixed_string Recipe, WorkerHosting Hosting>
    struct PreparedComponent
    {
        static Port<Out> compose(Wiring &w, Port<In> ts)
        {
            return stdlib::component<PreparedBody<In, Out, Child, Recipe, Hosting>>(w, "distributed-map", ts);
        }
    };
    template <WorkerHosting Hosting>
    using DistributedComponent = PreparedComponent<Dict, Dict, PreparedAccumulate, "prepared accumulate: recoverable", Hosting>;
    using NestedDict = TSD<Str, Dict>;
    template <WorkerHosting Hosting>
    using NestedComponent = PreparedComponent<NestedDict, Dict, hgraph_test::PreparedNestedOwners,
                                              "prepared nested owners: recoverable", Hosting>;

    EvalNodeRunOptions interval(std::size_t begin, std::size_t end)
    {
        return {.start_time = MIN_ST + MIN_TD * static_cast<Int>(begin),
                .end_time   = MIN_ST + MIN_TD * static_cast<Int>(end)};
    }

    // --- a component INSIDE the dmap_ child -----------------------------------
    // The dmap_ is in no component. Recovery is configured for the component
    // its child hosts, and the dmap_ node stands in for it in this graph.
    template <typename Child, fixed_string Recipe, WorkerHosting Hosting> struct HostingGraph
    {
        static Port<Dict> compose(Wiring &w, Port<Dict> ts)
        {
            const std::vector<DistributedMapInput> inputs{{ts.erased().schema}};
            WorkerPoolConfig config;
            config.workers = 3;
            config.hosting = Hosting;
            if (Hosting == WorkerHosting::Process) { config.program = HGRAPH_TEST_WORKER_PROGRAM; }
            auto plan = prepare_distributed_map_pool(fn<Child>(), inputs, {}, config);
            if (Hosting == WorkerHosting::Process) { bind_distributed_map_recipe(plan, Recipe.sv()); }
            return wire_distributed_map(w, ts.erased(), std::make_shared<const DistributedMapPlan>(std::move(plan)))
                .template as<Dict>();
        }
    };
    template <WorkerHosting Hosting>
    using HostedGraph = HostingGraph<hgraph_test::PreparedHostedChild, "prepared hosted component", Hosting>;
    template <WorkerHosting Hosting>
    using HostedThenForgetfulGraph =
        HostingGraph<hgraph_test::PreparedHostedThenForgetful, "prepared hosted component, then forgetful", Hosting>;
    template <WorkerHosting Hosting>
    using HostedWithConstantGraph =
        HostingGraph<hgraph_test::PreparedHostedWithConstant, "prepared hosted component, with constant", Hosting>;

    template <WorkerHosting Hosting> struct TypedHostedGraph
    {
        static Port<TSD<Int, TS<Int>>> compose(Wiring &w, Port<TSD<Int, TS<Int>>> ts);
    };

    /** Days of ``Graph``, cut where ``cuts`` say, recovering ``component`` when one is named. */
    template <typename Graph>
    std::vector<std::optional<Value>> days(const std::vector<std::optional<Value>> &input,
                                           std::initializer_list<std::size_t> cuts, const char *component)
    {
        std::optional<ComponentCheckpoint> completed;
        std::vector<std::optional<Value>>  actual;
        std::size_t                        begin = 0;
        std::vector<std::size_t>           ends{cuts};
        ends.push_back(input.size());
        for (const auto end : ends)
        {
            GlobalContext context;
            if (component != nullptr)
            {
                configure_component_recovery(context.state().view(), {
                    .component_id = component, .load = [&] { return completed; },
                    .commit = [&](const auto &image) { completed = image; }});
            }
            auto result = eval_node_with_options<Graph>(
                interval(begin, end), std::vector<std::optional<Value>>{input.begin() + begin, input.begin() + end});
            REQUIRE(result.size() <= end - begin);
            result.resize(end - begin);
            actual.insert(actual.end(), result.begin(), result.end());
            begin = end;
        }
        return actual;
    }

    // The typed form builds its worker graph in ``start`` from the child
    // function, so it signs and restores through a different route than the
    // prepared form while sharing the one owner contract.
    using IntDict = TSD<Int, TS<Int>>;
    template <WorkerHosting Hosting> struct TypedBody
    {
        static Port<IntDict> compose(Wiring &w, NamedPort<"ts", IntDict> ts)
        {
            return wire<dmap_impl<Int, Int, Int>>(w, ts, fn<hgraph_test::AccumulateG>(), Int{3},
                                                 Bool{Hosting == WorkerHosting::InProcess},
                                                 Str{HGRAPH_TEST_WORKER_PROGRAM})
                .template as<IntDict>();
        }
    };
    template <WorkerHosting Hosting> struct TypedComponent
    {
        static Port<IntDict> compose(Wiring &w, Port<IntDict> ts)
        {
            return stdlib::component<TypedBody<Hosting>>(w, "distributed-map", ts);
        }
    };

    template <WorkerHosting Hosting>
    Port<TSD<Int, TS<Int>>> TypedHostedGraph<Hosting>::compose(Wiring &w, Port<TSD<Int, TS<Int>>> ts)
    {
        return wire_dmap<Int, Int, Int>(w, ts, fn<hgraph_test::PreparedHostedChild>(), Int{3},
                                        Bool{Hosting == WorkerHosting::InProcess}, Str{HGRAPH_TEST_WORKER_PROGRAM});
    }

    std::vector<std::optional<Value>> typed_events()
    {
        return values<Value>(dict_delta<Int, TS<Int>>({{1, 1}, {2, 10}, {3, 100}}),
                             dict_delta<Int, TS<Int>>({{1, 2}, {4, 1000}}),
                             none,
                             dict_delta<Int, TS<Int>>({{1, 3}}, {2}),
                             dict_delta<Int, TS<Int>>({{1, 4}, {2, 20}, {5, 5}}),
                             dict_delta<Int, TS<Int>>({{3, 200}, {1, 5}}, {4}));
    }

    // A worker graph names the nodes of every component in it, configured or
    // not. That has to be invisible: no node added, no binding changed.
    struct WrappedAccumulateBody
    {
        static Port<TS<Int>> compose(Wiring &w, NamedPort<"ts", TS<Int>> ts)
        { return wire<PreparedAccumulate>(w, ts).as<TS<Int>>(); }
    };
    struct WrappedAccumulate
    {
        static constexpr auto name = "dmap_checkpoint_wrapped_accumulate";
        static Port<TS<Int>>  compose(Wiring &w, Port<TS<Int>> ts)
        { return stdlib::component<WrappedAccumulateBody>(w, "wrapped", ts); }
    };
    template <typename Child> struct Undistinguished
    {
        static Port<Dict> compose(Wiring &w, Port<Dict> ts)
        {
            const std::vector<DistributedMapInput> inputs{{ts.erased().schema}};
            WorkerPoolConfig config;
            config.workers = 3;
            config.hosting = WorkerHosting::InProcess;
            auto plan = prepare_distributed_map_pool(fn<Child>(), inputs, {}, config);
            return wire_distributed_map(w, ts.erased(), std::make_shared<const DistributedMapPlan>(std::move(plan)))
                .template as<Dict>();
        }
    };

    template <typename Child, std::size_t Workers> struct PlainBody
    {
        static Port<Dict> compose(Wiring &w, NamedPort<"ts", Dict> ts)
        {
            const std::vector<DistributedMapInput> inputs{{ts.erased().schema}};
            WorkerPoolConfig config;
            config.workers = Workers;
            config.hosting = WorkerHosting::InProcess;
            auto plan = prepare_distributed_map_pool(fn<Child>(), inputs, {}, config);
            return wire_distributed_map(w, ts.erased(), std::make_shared<const DistributedMapPlan>(std::move(plan)))
                .template as<Dict>();
        }
    };
    template <typename Child, std::size_t Workers> struct PlainComponent
    {
        static Port<Dict> compose(Wiring &w, Port<Dict> ts)
        {
            return stdlib::component<PlainBody<Child, Workers>>(w, "distributed-map", ts);
        }
    };

    /** Holds its total in ordinary ``State``, which no checkpoint can see. */
    struct Forgetful
    {
        static constexpr auto name = "dmap_checkpoint_forgetful";
        static void eval(In<"ts", TS<Int>> ts, State<Int> total, Out<TS<Int>> out)
        {
            total.modify() += ts.value();
            out.set(total.get());
        }
    };

    // Keys spread over three workers; "a" accumulates across every boundary,
    // "b" leaves and returns (its state must NOT survive its removal), and the
    // quiet cycles leave a boundary with nothing in flight.
    std::vector<std::optional<Value>> events()
    {
        return values<Value>(dict_delta<Str, TS<Int>>({{"a", 1}, {"b", 10}, {"c", 100}}),
                             dict_delta<Str, TS<Int>>({{"a", 2}, {"d", 1000}}),
                             none,
                             dict_delta<Str, TS<Int>>({{"a", 3}}, {"b"}),
                             dict_delta<Str, TS<Int>>({{"a", 4}, {"b", 20}, {"e", 5}}),
                             dict_delta<Str, TS<Int>>({{"c", 200}}, {"d"}),
                             dict_delta<Str, TS<Int>>({{"a", 5}, {"b", 30}, {"c", 300}, {"e", 6}}));
    }

    // Outer keys spread over the workers; inside each, inner keys arrive,
    // accumulate, leave and return, and a whole outer key is dropped and
    // re-created, so nested membership and nested state both cross boundaries.
    std::vector<std::optional<Value>> nested_events()
    {
        const auto inner = [](std::initializer_list<std::pair<Str, Int>> ticks, std::initializer_list<Str> removed = {}) {
            return dict_delta<Str, TS<Int>>(ticks, removed);
        };
        return values<Value>(
            dict_delta<Str, Dict>({{"x", inner({{"a", 1}, {"b", 2}})}, {"y", inner({{"c", 10}})}, {"z", inner({{"d", 100}})}}),
            dict_delta<Str, Dict>({{"x", inner({{"a", 3}})}, {"z", inner({{"d", 200}, {"e", 5}})}}),
            none,
            dict_delta<Str, Dict>({{"x", inner({{"a", 4}}, {"b"})}}),
            dict_delta<Str, Dict>({{"x", inner({{"a", 5}, {"b", 7}})}}, {"y"}),
            dict_delta<Str, Dict>({{"x", inner({{"a", 6}})}, {"y", inner({{"c", 4}})}, {"z", inner({{"d", 300}})}}));
    }

    template <typename Graph>
    void every_cut(const std::vector<std::optional<Value>> &input, const char *component = "distributed-map")
    {
        std::vector<std::optional<Value>> expected;
        {
            GlobalContext context;
            expected = eval_node_with_options<Graph>(interval(0, input.size()), input);
            expected.resize(input.size());
        }
        // Pinned so the comparison cannot pass on silence: every cycle that
        // was given input has to have produced output.
        for (std::size_t cycle = 0; cycle < input.size(); ++cycle)
        {
            CAPTURE(cycle);
            REQUIRE(expected[cycle].has_value() == input[cycle].has_value());
        }
        // Each individual cut, followed by one campaign that restarts every tick.
        for (std::size_t split = 1; split <= input.size(); ++split)
        {
            CAPTURE(split);
            std::optional<ComponentCheckpoint> completed;
            std::vector<std::optional<Value>>  actual;
            std::size_t                        begin = 0;
            while (begin < input.size())
            {
                const auto end = split == input.size() ? begin + 1 : (begin == 0 ? split : input.size());
                GlobalContext context;
                configure_component_recovery(context.state().view(), {
                    .component_id = component, .load = [&] { return completed; },
                    .commit = [&](const auto &image) { completed = image; }});
                const std::vector<std::optional<Value>> day{input.begin() + begin, input.begin() + end};
                auto result = eval_node_with_options<Graph>(interval(begin, end), day);
                REQUIRE(result.size() <= end - begin);
                result.resize(end - begin);
                actual.insert(actual.end(), result.begin(), result.end());
                REQUIRE(completed);
                begin = end;
            }
            CHECK_OUTPUT(actual, expected);
        }
    }
}  // namespace

TEST_CASE("dmap_ recovery: a restart at any completed day is invisible, in process", "[checkpoint][dmap]")
{
    stdlib::register_standard_operators();
    every_cut<DistributedComponent<WorkerHosting::InProcess>>(events());
}

TEST_CASE("dmap_ recovery: a restart at any completed day is invisible, across processes", "[checkpoint][dmap]")
{
    stdlib::register_standard_operators();
    every_cut<DistributedComponent<WorkerHosting::Process>>(events());
}

TEST_CASE("dmap_ recovery: map_, mesh_ and reduce nested in the worker restart invisibly", "[checkpoint][dmap]")
{
    stdlib::register_standard_operators();
    hgraph_test::register_distributed_test_recipes();
    every_cut<NestedComponent<WorkerHosting::InProcess>>(nested_events());
    every_cut<NestedComponent<WorkerHosting::Process>>(nested_events());
}

TEST_CASE("dmap_ recovery: a component inside the child restarts invisibly, in both hosting modes",
          "[checkpoint][dmap][hosted]")
{
    stdlib::register_standard_operators();
    hgraph_test::register_distributed_test_recipes();
    // The same churn as the member form: keys arrive, accumulate, leave and
    // return -- including a removal AFTER a restart, which only reaches the
    // worker because the dmap_'s input enters through a component boundary.
    every_cut<HostedGraph<WorkerHosting::InProcess>>(events(), hgraph_test::dmap_component_id);
    every_cut<HostedGraph<WorkerHosting::Process>>(events(), hgraph_test::dmap_component_id);
    // The control: the same restarts with nothing configured lose every total.
    const auto expected = days<HostedGraph<WorkerHosting::InProcess>>(events(), {}, nullptr);
    CHECK(days<HostedGraph<WorkerHosting::InProcess>>(events(), {3}, nullptr) != expected);
    CHECK(days<HostedGraph<WorkerHosting::InProcess>>(events(), {3}, hgraph_test::dmap_component_id) == expected);
}

TEST_CASE("dmap_ recovery: the typed form hosts a component too", "[checkpoint][dmap][hosted]")
{
    stdlib::register_standard_operators();
    hgraph_test::register_distributed_test_recipes();
    every_cut<TypedHostedGraph<WorkerHosting::InProcess>>(typed_events(), hgraph_test::dmap_component_id);
    every_cut<TypedHostedGraph<WorkerHosting::Process>>(typed_events(), hgraph_test::dmap_component_id);
}

TEST_CASE("dmap_ recovery: what the child holds outside the component is processed, recoverable or not",
          "[checkpoint][dmap][hosted]")
{
    stdlib::register_standard_operators();
    hgraph_test::register_distributed_test_recipes();
    // After the component comes a node keeping ordinary State. It is outside
    // the component: no part of the contract, not restored. The dmap_ wires,
    // every day completes, and that node starts again -- so the trace differs
    // from an unbroken run exactly where its forgotten total shows.
    const auto input = values<Value>(dict_delta<Str, TS<Int>>({{"a", 1}}), dict_delta<Str, TS<Int>>({{"a", 2}}),
                                     dict_delta<Str, TS<Int>>({{"a", 3}}));
    for (const auto hosting : {WorkerHosting::InProcess, WorkerHosting::Process})
    {
        CAPTURE(static_cast<int>(hosting));
        const auto run = [&](std::initializer_list<std::size_t> cuts, const char *component) {
            return hosting == WorkerHosting::InProcess
                ? days<HostedThenForgetfulGraph<WorkerHosting::InProcess>>(input, cuts, component)
                : days<HostedThenForgetfulGraph<WorkerHosting::Process>>(input, cuts, component);
        };
        // Component totals 1, 3, 6; the node after it sums those: 1, 4, 10.
        CHECK_OUTPUT(run({}, nullptr), values<Value>(dict_delta<Str, TS<Int>>({{"a", 1}}), dict_delta<Str, TS<Int>>({{"a", 4}}),
                                                     dict_delta<Str, TS<Int>>({{"a", 10}})));
        // Restarted after two days the component resumes at 6, and the node
        // after it, having forgotten 1 + 3, reports 6.
        CHECK_OUTPUT(run({2}, hgraph_test::dmap_component_id),
                     values<Value>(dict_delta<Str, TS<Int>>({{"a", 1}}), dict_delta<Str, TS<Int>>({{"a", 4}}),
                                   dict_delta<Str, TS<Int>>({{"a", 6}})));
    }
}

// A fixed-size list is unrolled inline, once per index, on the worker's own wiring.
template <std::size_t Workers> struct HostedFixedList
{
    using List = TSL<TS<Int>, 4>;
    static Port<List> compose(Wiring &w, Port<List> ts)
    {
        const std::vector<DistributedMapInput> inputs{{ts.erased().schema}};
        WorkerPoolConfig config;
        config.workers = Workers;
        config.hosting = WorkerHosting::InProcess;
        auto plan = prepare_distributed_map_pool(fn<hgraph_test::PreparedHostedThenForgetful>(), inputs, {}, config);
        return wire_distributed_map(w, ts.erased(), std::make_shared<const DistributedMapPlan>(std::move(plan)))
            .template as<List>();
    }
};

TEST_CASE("dmap_ recovery: a component hosted over a fixed-size list restarts invisibly", "[checkpoint][dmap][hosted]")
{
    stdlib::register_standard_operators();
    // Four instances of one component, two to a worker, then all in one. What
    // follows the component in each is the user's and merely processed, as it
    // is under a keyed dmap_: wired inline it used to take the runtime's scope,
    // be selected, and refuse the capture for being unrecoverable.
    const auto input = values<Value>(list_delta<TS<Int>>({1, 10, 100, 1000}), list_delta<TS<Int>>({2, std::nullopt, 200, std::nullopt}),
                                     none, list_delta<TS<Int>>({3, 30, std::nullopt, 3000}));
    // Component totals, then the forgetful node's running sum of those.
    const auto unbroken = values<Value>(list_delta<TS<Int>>({1, 10, 100, 1000}), list_delta<TS<Int>>({4, std::nullopt, 400, std::nullopt}),
                                        none, list_delta<TS<Int>>({10, 50, std::nullopt, 5000}));
    // Restarted after day two: components resume (6, 40, 300, 4000), the node after them starts again.
    const auto restarted = values<Value>(list_delta<TS<Int>>({1, 10, 100, 1000}), list_delta<TS<Int>>({4, std::nullopt, 400, std::nullopt}),
                                         none, list_delta<TS<Int>>({6, 40, std::nullopt, 4000}));
    for (const std::size_t workers : {std::size_t{1}, std::size_t{2}})
    {
        CAPTURE(workers);
        const auto run = [&](std::initializer_list<std::size_t> cuts, const char *component) {
            return workers == 1 ? days<HostedFixedList<1>>(input, cuts, component)
                                : days<HostedFixedList<2>>(input, cuts, component);
        };
        CHECK_OUTPUT(run({}, nullptr), unbroken);
        CHECK_OUTPUT(run({2}, hgraph_test::dmap_component_id), restarted);
        // The control: the same restart with nothing recovered loses the totals too.
        CHECK_OUTPUT(run({2}, nullptr), values<Value>(list_delta<TS<Int>>({1, 10, 100, 1000}),
                                                      list_delta<TS<Int>>({4, std::nullopt, 400, std::nullopt}), none,
                                                      list_delta<TS<Int>>({3, 30, std::nullopt, 3000})));
    }
}

template <std::size_t Workers> struct HostedDynamicList
{
    using List = TSL<TS<Int>, unbounded_tsl_size>;
    static Port<List> compose(Wiring &w, Port<List> ts)
    {
        const std::vector<DistributedMapInput> inputs{{ts.erased().schema}};
        WorkerPoolConfig config;
        config.workers = Workers;
        config.hosting = WorkerHosting::InProcess;
        auto plan = prepare_distributed_map_pool(fn<hgraph_test::PreparedHostedChild>(), inputs, {}, config);
        return wire_distributed_map(w, ts.erased(), std::make_shared<const DistributedMapPlan>(std::move(plan)))
            .template as<List>();
    }
};

TEST_CASE("dmap_ recovery: a component hosted over an unbounded list recovers with one worker, and says so with more",
          "[checkpoint][dmap][hosted]")
{
    stdlib::register_standard_operators();
    const auto input = values<Value>(dynamic_list_delta<TS<Int>>({{0, 1}, {1, 10}}), dynamic_list_delta<TS<Int>>({{0, 2}, {2, 100}}),
                                     none, dynamic_list_delta<TS<Int>>({{0, 3}, {1, 30}}));
    const auto expected = values<Value>(dynamic_list_delta<TS<Int>>({{0, 1}, {1, 10}}), dynamic_list_delta<TS<Int>>({{0, 3}, {2, 100}}),
                                        none, dynamic_list_delta<TS<Int>>({{0, 6}, {1, 40}}));
    CHECK_OUTPUT(days<HostedDynamicList<1>>(input, {}, nullptr), expected);
    CHECK_OUTPUT(days<HostedDynamicList<1>>(input, {2}, hgraph_test::dmap_component_id), expected);
    // A list partitioned over workers is a known limit (RFC 0039): refused when
    // the graph is wired, never recovered wrongly -- and only when recovering.
    CHECK_OUTPUT(days<HostedDynamicList<2>>(input, {}, nullptr), expected);
    REQUIRE_THROWS_WITH(days<HostedDynamicList<2>>(input, {2}, hgraph_test::dmap_component_id),
                        Catch::Matchers::ContainsSubstring("partitioned list maps are not recoverable"));
}

struct FixedListOfComponents
{
    using List = TSL<TS<Int>, 3>;
    static Port<List> compose(Wiring &w, Port<List> ts)
    { return wire<stdlib::map_>(w, fn<hgraph_test::PreparedHostedChild>(), ts).template as<List>(); }
};

struct RecordedFixedListOfComponents
{
    using List = TSL<TS<Int>, 3>;
    static Port<List> compose(Wiring &w, Port<List> ts)
    {
        record_replay::scope mode{record_replay::Mode::Record};
        return wire<stdlib::map_>(w, fn<hgraph_test::PreparedHostedChild>(), ts).template as<List>();
    }
};

// Two DISTINCT components that share an id, in the function a fixed list unrolls.
struct TwoComponentsOneId
{
    static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> ts)
    {
        auto first = stdlib::component<hgraph_test::PreparedAccumulateBody>(w, hgraph_test::dmap_component_id, ts);
        return stdlib::component<hgraph_test::PreparedAccumulateBody>(w, hgraph_test::dmap_component_id, first);
    }
};
struct FixedListOfClashingComponents
{
    using List = TSL<TS<Int>, 3>;
    static Port<List> compose(Wiring &w, Port<List> ts)
    {
        // Through dmap_: the map_ operator resolves its output type by wiring the
        // function once on a probe wiring, which rejects this before any repeat
        // opens -- by accident, and as "output type could not be resolved".
        const std::vector<DistributedMapInput> inputs{{ts.erased().schema}};
        WorkerPoolConfig config;
        config.workers = 1;
        config.hosting = WorkerHosting::InProcess;
        auto plan = prepare_distributed_map_pool(fn<TwoComponentsOneId>(), inputs, {}, config);
        return wire_distributed_map(w, ts.erased(), std::make_shared<const DistributedMapPlan>(std::move(plan)))
            .template as<List>();
    }
};

TEST_CASE("component: a repeat across list indices is an instance, a repeat within one index is still a duplicate",
          "[checkpoint][hosted]")
{
    stdlib::register_standard_operators();
    // Only the unrolling repeats a component legitimately. Two call sites
    // sharing an id inside ONE index are the user error the claim exists to
    // catch, and the first cut of InlineRepeat let it through (Codex, #998).
    GlobalContext context;
    const auto input = values<Value>(list_delta<TS<Int>>({1, 10, 100}));
    REQUIRE_THROWS_WITH((eval_node_with_options<FixedListOfClashingComponents>(interval(0, 1), input)),
                        Catch::Matchers::ContainsSubstring("duplicate recordable id"));
}

TEST_CASE("component: mapped over a fixed-size list it is one component wired per index", "[checkpoint][hosted]")
{
    stdlib::register_standard_operators();
    const auto input = values<Value>(list_delta<TS<Int>>({1, 10, 100}), list_delta<TS<Int>>({2, std::nullopt, 200}));
    {
        // Nothing recorded, nothing recovered: a graph wired three times. The
        // second index used to be refused as a duplicate recordable id.
        GlobalContext context;
        CHECK_OUTPUT(eval_node_with_options<FixedListOfComponents>(interval(0, 2), input),
                     values<Value>(list_delta<TS<Int>>({1, 10, 100}), list_delta<TS<Int>>({3, std::nullopt, 300})));
    }
    {
        // Recording it is another matter: three instances would share one id
        // and one set of recordings. Refused at wiring, with the remedies.
        GlobalContext context;
        REQUIRE_THROWS_WITH((eval_node_with_options<RecordedFixedListOfComponents>(interval(0, 2), input)),
                            Catch::Matchers::ContainsSubstring("once per index of a fixed-size list") &&
                                Catch::Matchers::ContainsSubstring("cannot be recorded or recovered apart"));
    }
    // So is recovering it, and sooner: an instance's input is an element of
    // the list, not a source of its own, which a component input has to be.
    // The first instance is refused for that before a second one is reached.
    GlobalContext context;
    configure_component_recovery(context.state().view(), {
        .component_id = hgraph_test::dmap_component_id, .load = [] { return std::optional<ComponentCheckpoint>{}; },
        .commit = [](const auto &) {}});
    REQUIRE_THROWS_WITH((eval_node_with_options<FixedListOfComponents>(interval(0, 2), input)),
                        Catch::Matchers::ContainsSubstring("external inputs require direct owned pull-source outputs"));
}

// KNOWN LIMIT, pinned as the target behaviour (RFC 0039, "Known limits": "A hosted
// dmap_ contract covers the whole child"). [!shouldfail]: it fails today, and the
// day it passes the tag comes off.
TEST_CASE("dmap_ recovery: editing what the child holds outside the component keeps the component's image",
          "[checkpoint][dmap][hosted][!shouldfail]")
{
    stdlib::register_standard_operators();
    hgraph_test::register_distributed_test_recipes();
    // What is outside the component is processed, not recovered, so it should
    // be no part of the contract: a deploy that adds a node after the component
    // should still restore the component, as the same edit does under spawn_.
    // The worker's own map_ is a runtime node and IS selected, and it signs
    // every node of its child at wiring, before any selection exists -- so this
    // edit reads as another contract and is refused ("incompatible component,
    // revision or graph signature"). Fail-closed: a cold start, never a wrong
    // restore. Found independently by two adversarial reviewers.
    const auto input = values<Value>(dict_delta<Str, TS<Int>>({{"a", 1}}), dict_delta<Str, TS<Int>>({{"a", 2}}),
                                     dict_delta<Str, TS<Int>>({{"a", 3}}));
    const auto run = [&]<WorkerHosting Hosting>() {
        std::optional<ComponentCheckpoint> completed;
        const auto day = [&]<typename Graph>(std::size_t begin, std::size_t end) {
            GlobalContext context;
            configure_component_recovery(context.state().view(), {
                .component_id = hgraph_test::dmap_component_id, .load = [&] { return completed; },
                .commit = [&](const auto &image) { completed = image; }});
            return eval_node_with_options<Graph>(
                interval(begin, end), std::vector<std::optional<Value>>{input.begin() + begin, input.begin() + end});
        };
        CHECK_OUTPUT(day.template operator()<HostedGraph<Hosting>>(0, 2),
                     values<Value>(dict_delta<Str, TS<Int>>({{"a", 1}}), dict_delta<Str, TS<Int>>({{"a", 3}})));
        REQUIRE(completed);
        // The component resumes at 3 + 3; the new node after it starts from nothing.
        CHECK_OUTPUT(day.template operator()<HostedThenForgetfulGraph<Hosting>>(2, 3),
                     values<Value>(dict_delta<Str, TS<Int>>({{"a", 6}})));
    };
    run.template operator()<WorkerHosting::InProcess>();
    run.template operator()<WorkerHosting::Process>();
}

TEST_CASE("dmap_ recovery: a fresh node beside a restored component still gets its start-time work",
          "[checkpoint][dmap][hosted]")
{
    stdlib::register_standard_operators();
    hgraph_test::register_distributed_test_recipes();
    // Beside the component sits a constant, which schedules itself on start.
    // In a restored child it is fresh, so that schedule is LIVE work: the
    // worker's map_ has to keep it through the restored start, the worker has
    // to report it, and the owner -- whose own bootstrap is discarded -- has to
    // be woken for it. The engine never skips scheduled work; dropping this
    // would have done so silently. The day after the restart opens quiet, so
    // nothing else would have evaluated the child.
    const auto input = values<Value>(dict_delta<Str, TS<Int>>({{"a", 1}}), dict_delta<Str, TS<Int>>({{"a", 2}}), none,
                                     dict_delta<Str, TS<Int>>({{"a", 3}}));
    for (const auto hosting : {WorkerHosting::InProcess, WorkerHosting::Process})
    {
        CAPTURE(static_cast<int>(hosting));
        const auto run = [&](std::initializer_list<std::size_t> cuts, const char *component) {
            return hosting == WorkerHosting::InProcess
                ? days<HostedWithConstantGraph<WorkerHosting::InProcess>>(input, cuts, component)
                : days<HostedWithConstantGraph<WorkerHosting::Process>>(input, cuts, component);
        };
        CHECK_OUTPUT(run({}, nullptr), values<Value>(dict_delta<Str, TS<Int>>({{"a", 1001}}), dict_delta<Str, TS<Int>>({{"a", 1003}}),
                                                     none, dict_delta<Str, TS<Int>>({{"a", 1006}})));
        // The restart shows as one extra tick: the fresh constant ticks again
        // beside the restored total of 3. That it is 1003 proves both halves --
        // the total was restored, and the constant's start-time work ran.
        CHECK_OUTPUT(run({2}, hgraph_test::dmap_component_id),
                     values<Value>(dict_delta<Str, TS<Int>>({{"a", 1001}}), dict_delta<Str, TS<Int>>({{"a", 1003}}),
                                   dict_delta<Str, TS<Int>>({{"a", 1003}}), dict_delta<Str, TS<Int>>({{"a", 1006}})));
    }
}

TEST_CASE("dmap_ recovery: an unrecoverable node INSIDE the hosted component is refused at wiring",
          "[checkpoint][dmap][hosted]")
{
    stdlib::register_standard_operators();
    using Graph = HostingGraph<hgraph_test::PreparedHostedForgetful, "unused", WorkerHosting::InProcess>;
    const auto day = values<Value>(dict_delta<Str, TS<Int>>({{"a", 1}}));
    {
        GlobalContext context;
        configure_component_recovery(context.state().view(), {
            .component_id = hgraph_test::dmap_component_id, .load = [] { return std::optional<ComponentCheckpoint>{}; },
            .commit = [](const auto &) {}});
        REQUIRE_THROWS_WITH((eval_node_with_options<Graph>(interval(0, 1), day)),
                            Catch::Matchers::ContainsSubstring("dmap_running_total") &&
                                Catch::Matchers::ContainsSubstring("cannot be recovered"));
    }
    // Recovery not configured: the same child wires and runs.
    GlobalContext context;
    const auto    result = eval_node_with_options<Graph>(interval(0, 1), day);
    REQUIRE(result.size() == 1);
    CHECK(result.front().has_value());
}

TEST_CASE("dmap_ recovery: the typed form restarts invisibly in both hosting modes", "[checkpoint][dmap]")
{
    stdlib::register_standard_operators();
    hgraph_test::register_distributed_test_recipes();
    every_cut<TypedComponent<WorkerHosting::InProcess>>(typed_events());
    every_cut<TypedComponent<WorkerHosting::Process>>(typed_events());
}

TEST_CASE("dmap_ recovery: an unrecoverable child is refused where a component learns everything else",
          "[checkpoint][dmap]")
{
    stdlib::register_standard_operators();
    using Catch::Matchers::ContainsSubstring;
    const auto day = values<Value>(dict_delta<Str, TS<Int>>({{"a", 1}}));
    {
        // Recovery configured: the owner walks its worker plans at wiring, and
        // names both the node and what the worker scope recorded about it.
        GlobalContext context;
        configure_component_recovery(context.state().view(), {
            .component_id = "distributed-map", .load = [] { return std::optional<ComponentCheckpoint>{}; },
            .commit = [](const auto &) {}});
        REQUIRE_THROWS_WITH((eval_node_with_options<PlainComponent<Forgetful, 2>>(interval(0, 1), day)),
                            ContainsSubstring("dmap_checkpoint_forgetful") && ContainsSubstring("cannot be recovered"));
    }
    // Recovery not configured: the same child wires and runs, as most do.
    GlobalContext context;
    const auto    result = eval_node_with_options<PlainComponent<Forgetful, 2>>(interval(0, 1), day);
    REQUIRE(result.size() == 1);
    CHECK(result.front().has_value());
}

TEST_CASE("dmap_ recovery: another worker count is another contract", "[checkpoint][dmap]")
{
    stdlib::register_standard_operators();
    std::optional<ComponentCheckpoint> completed;
    const auto configure = [&](GlobalContext &context) {
        configure_component_recovery(context.state().view(), {
            .component_id = "distributed-map", .load = [&] { return completed; },
            .commit = [&](const auto &image) { completed = image; }});
    };
    {
        GlobalContext context;
        configure(context);
        (void)eval_node_with_options<PlainComponent<PreparedAccumulate, 2>>(
            interval(0, 1), values<Value>(dict_delta<Str, TS<Int>>({{"a", 1}, {"b", 2}})));
        REQUIRE(completed);
    }
    // Placement is hash % workers and is not stored: three workers would
    // silently re-partition the keys two workers hold.
    GlobalContext context;
    configure(context);
    REQUIRE_THROWS_WITH((eval_node_with_options<PlainComponent<PreparedAccumulate, 3>>(
                            interval(1, 2), values<Value>(dict_delta<Str, TS<Int>>({{"a", 1}})))),
                        Catch::Matchers::ContainsSubstring("incompatible"));
}

TEST_CASE("dmap_ recovery: an owner image with no worker images is refused, not started fresh",
          "[checkpoint][dmap]")
{
    stdlib::register_standard_operators();
    std::optional<ComponentCheckpoint> completed;
    const auto configure = [&](GlobalContext &context) {
        configure_component_recovery(context.state().view(), {
            .component_id = "distributed-map", .load = [&] { return completed; },
            .commit = [&](const auto &image) { completed = image; }});
    };
    {
        GlobalContext context;
        configure(context);
        (void)eval_node_with_options<PlainComponent<PreparedAccumulate, 2>>(
            interval(0, 1), values<Value>(dict_delta<Str, TS<Int>>({{"a", 1}, {"b", 2}})));
        REQUIRE(completed);
    }
    // Schema-correct and empty. Downstream, "no images" means "start fresh",
    // so accepting this would throw the workers' state away without a word.
    std::size_t owners = 0;
    for (auto &node : completed->graph.nodes)
    {
        if (!node.custom.payload.has_value()) { continue; }
        auto       &registry = TypeRegistry::instance();
        auto        images   = ListBuilder{registry.scalar_type<Bytes>()}.build();
        auto        extents  = ListBuilder{registry.scalar_type<Int>()}.build();
        BundleBuilder tuple{ValuePlanFactory::instance().type_for(registry.tuple({images.schema(), extents.schema()}))};
        tuple.set(0, std::move(images));
        tuple.set(1, std::move(extents));
        node.custom.payload = tuple.build();
        ++owners;
    }
    REQUIRE(owners == 1);
    GlobalContext context;
    configure(context);
    REQUIRE_THROWS_WITH((eval_node_with_options<PlainComponent<PreparedAccumulate, 2>>(
                            interval(1, 2), values<Value>(dict_delta<Str, TS<Int>>({{"a", 1}})))),
                        Catch::Matchers::ContainsSubstring("holds no worker images"));
}

TEST_CASE("dmap_ recovery: a control frame can never be a cycle request", "[checkpoint][dmap]")
{
    // A request opens with its evaluation time as eight little-endian bytes.
    for (const std::string_view marker : {checkpoint_frame, restore_frame_prefix})
    {
        std::int64_t micros = 0;
        for (std::size_t byte = 0; byte < 8; ++byte)
        {
            micros |= static_cast<std::int64_t>(static_cast<unsigned char>(marker[byte])) << (8 * byte);
        }
        CHECK(micros > MAX_ET.time_since_epoch().count());
    }
}

TEST_CASE("dmap_ recovery: a worker process serves the control frames", "[checkpoint][dmap]")
{
    stdlib::register_standard_operators();
    hgraph_test::register_distributed_test_recipes();
    const auto end    = MIN_ST + TimeDelta{10000};
    const auto recipe = prepared_worker_recipe_key(hgraph_test::prepared_accumulate_name, 0, 1);
    const auto slots  = prepared_worker_recipe(hgraph_test::prepared_accumulate_name)->build(0, 1).slots;
    const auto cycle  = [&](WorkerProcess &worker, DateTime when) {
        CycleRequest request;
        request.evaluation_time = when;
        worker.channel().send(encode_request(slots, request));
        std::string payload;
        REQUIRE(worker.channel().receive(payload));
        return decode_reply(slots, payload);
    };

    std::string image;
    {
        WorkerProcess worker = spawn_worker(HGRAPH_TEST_WORKER_PROGRAM, recipe, MIN_ST, end);
        CHECK(cycle(worker, MIN_ST).error.empty());
        worker.channel().send(checkpoint_frame);
        std::string payload;
        REQUIRE(worker.channel().receive(payload));
        image = decode_checkpoint_reply(payload);
        CHECK(is_checkpoint_image(image));

        // A restore is a worker's first frame or it is an error; the graph
        // that is already running carries on.
        worker.channel().send(encode_restore_frame(image));
        REQUIRE(worker.channel().receive(payload));
        CHECK_THAT(decode_reply(slots, payload).error, Catch::Matchers::ContainsSubstring("must be the first frame"));
        CHECK(cycle(worker, MIN_ST + MIN_TD).error.empty());
        CHECK(worker.wait_for_exit() == 0);
    }
    {
        WorkerProcess worker = spawn_worker(HGRAPH_TEST_WORKER_PROGRAM, recipe, MIN_ST + MIN_TD, end);
        worker.channel().send(encode_restore_frame(image));
        std::string payload;
        REQUIRE(worker.channel().receive(payload));
        const auto reply = decode_reply(slots, payload);
        CHECK(reply.error.empty());
        CHECK(reply.next_scheduled_time == MAX_DT);
        CHECK(cycle(worker, MIN_ST + MIN_TD).error.empty());
        CHECK(worker.wait_for_exit() == 0);
    }
    {
        // A refused image is reported and the worker finishes: it is never
        // started fresh instead.
        WorkerProcess worker = spawn_worker(HGRAPH_TEST_WORKER_PROGRAM, recipe, MIN_ST + MIN_TD, end);
        worker.channel().send(encode_restore_frame("not an image"));
        std::string payload;
        REQUIRE(worker.channel().receive(payload));
        CHECK_FALSE(decode_reply(slots, payload).error.empty());
        CHECK(worker.wait_for_exit() == 0);
    }
}

namespace
{
    /** Where ``CheckpointStopMarker`` leaves its files, for this scope. */
    struct StopMarkers
    {
        std::filesystem::path directory{std::filesystem::temp_directory_path() /
            ("hgraph-stop-markers-" + std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()))};
        StopMarkers()
        {
            std::filesystem::create_directories(directory);
            set(directory.string().c_str());
        }
        ~StopMarkers()
        {
            set("");
            std::error_code ignored;
            std::filesystem::remove_all(directory, ignored);
        }
        StopMarkers(const StopMarkers &) = delete;
        StopMarkers &operator=(const StopMarkers &) = delete;
        /** The stop hooks that ran since the last call. */
        [[nodiscard]] std::ptrdiff_t take() const
        {
            const auto count = std::distance(std::filesystem::directory_iterator{directory},
                                             std::filesystem::directory_iterator{});
            for (const auto &entry : std::filesystem::directory_iterator{directory}) { std::filesystem::remove(entry); }
            return count;
        }

      private:
        static void set(const char *value)
        {
#ifdef _WIN32
            _putenv_s(hgraph_test::stop_marker_directory_variable, value);
#else
            if (*value == '\0') { ::unsetenv(hgraph_test::stop_marker_directory_variable); }
            else { ::setenv(hgraph_test::stop_marker_directory_variable, value, 1); }
#endif
        }
    };
}  // namespace

TEST_CASE("dmap_ recovery: one refused image stops the workers that did restore", "[checkpoint][dmap]")
{
    stdlib::register_standard_operators();
    hgraph_test::register_distributed_test_recipes();
    // A pool that fails to come up never reaches its node, so nothing else
    // will stop the workers that did start. They were killed (processes) or
    // simply dropped (in process), stop hooks and all (found by adversarial review).
    constexpr std::ptrdiff_t keys = 12;
    std::ptrdiff_t           first_worker_keys = -1;
    const auto run = [&]<WorkerHosting Hosting>() {
        using Graph = PreparedComponent<Dict, Dict, hgraph_test::AccumulateWithStopMarker, "accumulate, stop marker", Hosting>;
        StopMarkers markers;
        std::optional<ComponentCheckpoint> completed;
        const auto day = [&](std::size_t begin) {
            GlobalContext context;
            configure_component_recovery(context.state().view(), {
                .component_id = "distributed-map", .load = [&] { return completed; },
                .commit = [&](const auto &image) { completed = image; }});
            return eval_node_with_options<Graph>(interval(begin, begin + 1), values<Value>(dict_delta<Str, TS<Int>>(
                {{"a", 1}, {"b", 2}, {"c", 3}, {"d", 4}, {"e", 5}, {"f", 6},
                 {"g", 7}, {"h", 8}, {"i", 9}, {"j", 10}, {"k", 11}, {"l", 12}})));
        };
        (void)day(0);
        REQUIRE(completed);
        REQUIRE(markers.take() == keys);

        // The saved owner state with worker ``broken``'s image replaced.
        const auto saved = *completed;
        const auto spoil = [&](std::size_t broken) {
            completed = saved;
            std::size_t owners = 0;
            for (auto &node : completed->graph.nodes)
            {
                if (!node.custom.payload.has_value()) { continue; }
                const auto tuple = node.custom.payload.view().as_tuple();
                std::vector<std::string> images;
                std::vector<std::size_t> extents;
                for (const auto &image : tuple.at(0).as_list()) { images.push_back(image.template checked_as<Bytes>().data); }
                for (const auto &extent : tuple.at(1).as_list())
                    extents.push_back(static_cast<std::size_t>(extent.template checked_as<Int>()));
                REQUIRE(images.size() == 3);
                images[broken] = "not an image";
                node.custom.payload = worker_checkpoint::state_of(std::move(images), extents).payload;
                ++owners;
            }
            REQUIRE(owners == 1);
        };
        const auto refused = [&](std::size_t broken) {
            spoil(broken);
            REQUIRE_THROWS_WITH(day(1), Catch::Matchers::ContainsSubstring(fmt::format("partition {}", broken)) &&
                                            Catch::Matchers::ContainsSubstring("refused its image"));
            return markers.take();
        };
        const auto behind_last = refused(2);
        // The workers before the broken one must hold keys, or a count of zero proves nothing.
        REQUIRE(behind_last > 0);
        if (first_worker_keys < 0) { first_worker_keys = behind_last; }
        CHECK(behind_last == first_worker_keys);
        const auto behind_first = refused(0);
        // Processes are all restored at once, so the two good ones had started
        // and are stopped. In process they are raised in turn, and a refusal by
        // the first means the others were never started: nothing to stop.
        if (Hosting == WorkerHosting::InProcess) { CHECK(behind_first == 0); }
        else { CHECK(behind_first > 0); CHECK(behind_first <= keys); }
    };
    run.template operator()<WorkerHosting::Process>();
    run.template operator()<WorkerHosting::InProcess>();
}

TEST_CASE("dmap_ recovery: a checkpoint reply is never mistaken for a cycle reply, nor the reverse",
          "[checkpoint][dmap]")
{
    // A stage that fails answers the checkpoint frame's slot with a cycle
    // reply. Its first byte is zero, which an unmarked reply read as "here is
    // the image" (found by adversarial review).
    const BoundarySlots slots;
    const auto failure = encode_reply(slots, CycleReply{MAX_DT, {}, "boom"});
    CHECK_THROWS_WITH(decode_checkpoint_reply(failure),
                      Catch::Matchers::ContainsSubstring("did not answer the checkpoint request"));
    CHECK_THROWS_WITH(decode_checkpoint_reply(""), Catch::Matchers::ContainsSubstring("did not answer"));
    CHECK_THROWS_WITH(decode_checkpoint_reply(checkpoint_reply_prefix),
                      Catch::Matchers::ContainsSubstring("did not answer"));

    CHECK(decode_checkpoint_reply(encode_checkpoint_reply("image bytes")) == "image bytes");
    CHECK(decode_checkpoint_reply(encode_checkpoint_reply("")).empty());
    // A refusal is the worker's considered answer and is typed as one: the
    // worker that gave it is intact, which a broken channel never promises.
    CHECK_THROWS_AS(decode_checkpoint_reply(encode_checkpoint_error("no")), CheckpointRefused);
    CHECK_THROWS_WITH(decode_checkpoint_reply(encode_checkpoint_error("no")), Catch::Matchers::ContainsSubstring("no"));
}

TEST_CASE("dmap_ recovery: a refused capture fails the day and leaves the workers to stop properly",
          "[checkpoint][dmap]")
{
    stdlib::register_standard_operators();
    hgraph_test::register_distributed_test_recipes();
    // Workers that answered "no" are healthy. Terminating them, as an earlier
    // cut did, lost every stop hook in every worker over one refusal.
    const auto run = []<WorkerHosting Hosting>() {
        StopMarkers markers;
        using Graph = PreparedComponent<Dict, Dict, hgraph_test::RefusingComputeWithStopMarker,
                                        "refusing compute, stop marker", Hosting>;
        // Three keys, so that with three workers more than one worker holds a
        // child whatever the hash does; every child's sink must be stopped.
        const auto ticks = values<Value>(dict_delta<Str, TS<Int>>({{"a", 1}, {"b", 2}, {"c", 3}}));
        REQUIRE_THROWS_WITH(days<Graph>(ticks, {}, "distributed-map"),
                            Catch::Matchers::ContainsSubstring("test capture refusal"));
        CHECK(markers.take() == 3);
    };
    run.template operator()<WorkerHosting::InProcess>();
    run.template operator()<WorkerHosting::Process>();
}

TEST_CASE("dmap_: wrapping the child in a component changes nothing when nothing is recovered", "[checkpoint][dmap]")
{
    stdlib::register_standard_operators();
    // The same child with and without the wrapper, tick for tick -- including
    // the cycle a key's child is created in, and the removals. An earlier cut
    // gave a hosted component the forwarding input boundary a configured one
    // has; inside a child created mid-cycle that lost the first tick and the
    // removals, with no recovery configured at all.
    const auto run = [](auto graph) {
        GlobalContext context;
        return graph();
    };
    const auto plain   = run([] { return eval_node_with_options<Undistinguished<PreparedAccumulate>>(interval(0, 7), events()); });
    const auto wrapped = run([] { return eval_node_with_options<Undistinguished<WrappedAccumulate>>(interval(0, 7), events()); });
    REQUIRE(plain.size() >= 1);
    REQUIRE(plain.front().has_value());
    CHECK_OUTPUT(wrapped, plain);
}

TEST_CASE("dmap_ recovery: transient timers do not become pending recovered work in their owners",
          "[checkpoint][dmap][hosted]")
{
    stdlib::register_standard_operators();
    hgraph_test::register_distributed_test_recipes();
    const auto run = []<WorkerHosting Hosting>() {
        using Graph = HostingGraph<hgraph_test::HostedWithTimer, "hosted timer", Hosting>;
        const auto ticks = values<Value>(dict_delta<Str, TS<Int>>({{"a", 1}}), dict_delta<Str, TS<Int>>({{"a", 2}}));
        CHECK_OUTPUT(days<Graph>(ticks, {1}, hgraph_test::dmap_component_id),
                     values<Value>(dict_delta<Str, TS<Int>>({{"a", 1}}), dict_delta<Str, TS<Int>>({{"a", 3}})));
        // A selected compute child's pending alarm now has its own image,
        // independent of whether the surrounding owner hosts transient timers.
        using Pending = PreparedComponent<Dict, Dict, hgraph_test::CheckpointPendingCompute, "pending compute", Hosting>;
        auto pending_ticks = ticks;
        pending_ticks.resize(102);
        auto expected = pending_ticks;
        expected[100] = dict_delta<Str, TS<Int>>({{"a", 2}});
        CHECK_OUTPUT(days<Pending>(pending_ticks, {1}, "distributed-map"), expected);
    };
    run.template operator()<WorkerHosting::InProcess>();
    run.template operator()<WorkerHosting::Process>();
}

TEST_CASE("dmap_ recovery: whole workers run transient sink startup work on a quiet restored day", "[checkpoint][dmap]")
{
    stdlib::register_standard_operators();
    hgraph_test::register_distributed_test_recipes();
    const auto run = []<WorkerHosting Hosting>() {
        using Graph = PreparedComponent<Dict, Dict, hgraph_test::ChildWithImmediateSink, "immediate sink", Hosting>;
        // On day two only the sink's start-time alarm can run it. Its stop
        // hook fails if that work was discarded or was never driven.
        const auto ticks = values<Value>(dict_delta<Str, TS<Int>>({{"a", 1}}), none,
                                         dict_delta<Str, TS<Int>>({{"a", 2}}));
        CHECK_OUTPUT(days<Graph>(ticks, {1, 2}, "distributed-map"),
                     values<Value>(dict_delta<Str, TS<Int>>({{"a", 1}}), none,
                                   dict_delta<Str, TS<Int>>({{"a", 3}})));
    };
    run.template operator()<WorkerHosting::InProcess>();
    run.template operator()<WorkerHosting::Process>();
}

// A transient sink is outside even a whole image, so it starts fresh in a restored
// child, and its start-time alarm is live work its owner has to be woken for. map_,
// the list map_ and dmap_ answer for their children; this pins that the OTHER
// dynamic owners do as well (found by adversarial review: mesh_, reduce and ordered
// reduce kept the default "no live work", and the coordinator discarded it).
template <bool Mesh> struct KeyedSinkBody
{
    static Port<Dict> compose(Wiring &w, NamedPort<"ts", Dict> ts)
    {
        if constexpr (Mesh) { return wire<stdlib::mesh_>(w, fn<hgraph_test::ChildWithImmediateSink>(), ts).as<Dict>(); }
        else { return wire<stdlib::map_>(w, fn<hgraph_test::ChildWithImmediateSink>(), ts).as<Dict>(); }
    }
};
template <bool Mesh> struct KeyedSinkComponent
{
    static Port<Dict> compose(Wiring &w, Port<Dict> ts)
    { return stdlib::component<KeyedSinkBody<Mesh>>(w, "distributed-map", ts); }
};

TEST_CASE("recovery: a transient sink's start-time work runs in a restored mesh_ child, as in a map_ child",
          "[checkpoint][dmap][hosted]")
{
    stdlib::register_standard_operators();
    // On day two only the sink's start-time alarm can run it. Its stop hook
    // throws if that work was discarded or never driven.
    const auto ticks = values<Value>(dict_delta<Str, TS<Int>>({{"a", 1}}), none, dict_delta<Str, TS<Int>>({{"a", 2}}));
    const auto expected = values<Value>(dict_delta<Str, TS<Int>>({{"a", 1}}), none, dict_delta<Str, TS<Int>>({{"a", 3}}));
    CHECK_OUTPUT(days<KeyedSinkComponent<false>>(ticks, {1, 2}, "distributed-map"), expected);
    CHECK_OUTPUT(days<KeyedSinkComponent<true>>(ticks, {1, 2}, "distributed-map"), expected);
}

TEST_CASE("dmap_ recovery: dependencies are checked against the selected ancestor component", "[checkpoint][dmap][hosted]")
{
    stdlib::register_standard_operators();
    hgraph_test::register_distributed_test_recipes();
    const auto run = []<WorkerHosting Hosting>() {
        using Graph = HostingGraph<hgraph_test::HostedNestedComponent, "hosted nested", Hosting>;
        const auto ticks = values<Value>(dict_delta<Str, TS<Int>>({{"a", 1}}), dict_delta<Str, TS<Int>>({{"a", 2}}));
        CHECK_OUTPUT(days<Graph>(ticks, {1}, hgraph_test::dmap_component_id),
                     values<Value>(dict_delta<Str, TS<Int>>({{"a", 1}}), dict_delta<Str, TS<Int>>({{"a", 4}})));
        // Selecting only the inner component does not restore its producer.
        REQUIRE_THROWS_WITH(days<Graph>(ticks, {}, "dmap-accumulate.inner"),
                            Catch::Matchers::ContainsSubstring("fed from outside its component"));
    };
    run.template operator()<WorkerHosting::InProcess>();
    run.template operator()<WorkerHosting::Process>();
}

TEST_CASE("dmap_ recovery: inserting a transient sink preserves child binding identities", "[checkpoint][dmap][hosted]")
{
    stdlib::register_standard_operators();
    hgraph_test::register_distributed_test_recipes();
    const auto run = []<WorkerHosting Hosting>() {
        using Before = HostingGraph<hgraph_test::HostedCompatibleComponent<false>, "compatible before", Hosting>;
        using After = HostingGraph<hgraph_test::HostedCompatibleComponent<true>, "compatible after", Hosting>;
        std::optional<ComponentCheckpoint> completed;
        const auto configure = [&](GlobalContext &context) {
            configure_component_recovery(context.state().view(), {
                .component_id = hgraph_test::dmap_component_id, .load = [&] { return completed; },
                .commit = [&](const auto &image) { completed = image; }});
        };
        {
            GlobalContext context;
            configure(context);
            (void)eval_node_with_options<Before>(interval(0, 1), values<Value>(dict_delta<Str, TS<Int>>({{"a", 1}})));
        }
        REQUIRE(completed);
        {
            GlobalContext context;
            configure(context);
            CHECK_OUTPUT(eval_node_with_options<After>(interval(1, 2), values<Value>(dict_delta<Str, TS<Int>>({{"a", 2}}))),
                         values<Value>(dict_delta<Str, TS<Int>>({{"a", 3}})));
        }
    };
    run.template operator()<WorkerHosting::InProcess>();
    run.template operator()<WorkerHosting::Process>();
}

TEST_CASE("dmap_ recovery: worker scope labels cannot impersonate user components", "[checkpoint][dmap][hosted]")
{
    stdlib::register_standard_operators();
    hgraph_test::register_distributed_test_recipes();
    const auto run = []<WorkerHosting Hosting>() {
        using Plain = HostingGraph<PreparedAccumulate, "prepared accumulate: recoverable", Hosting>;
        const auto ticks = values<Value>(dict_delta<Str, TS<Int>>({{"a", 1}}), dict_delta<Str, TS<Int>>({{"a", 2}}));
        for (const char *id : {"worker", "worker.boundary"})
            REQUIRE_THROWS_WITH(days<Plain>(ticks, {}, id),
                                Catch::Matchers::ContainsSubstring("configured component was not wired"));
        using Named = HostingGraph<hgraph_test::HostedNamedComponent<"worker">, "named worker", Hosting>;
        using BoundaryNamed = HostingGraph<hgraph_test::HostedNamedComponent<"worker.boundary">, "named worker boundary", Hosting>;
        const auto expected = values<Value>(dict_delta<Str, TS<Int>>({{"a", 1}}), dict_delta<Str, TS<Int>>({{"a", 3}}));
        CHECK_OUTPUT(days<Named>(ticks, {1}, "worker"), expected);
        CHECK_OUTPUT(days<BoundaryNamed>(ticks, {1}, "worker.boundary"), expected);
    };
    run.template operator()<WorkerHosting::InProcess>();
    run.template operator()<WorkerHosting::Process>();
    GlobalState state;
    Wiring wiring{state};
    for (const std::string_view id : {std::string_view{"@hgraph"}, worker_checkpoint_scope,
                                      worker_boundary_checkpoint_scope})
    {
        REQUIRE_THROWS_WITH(GraphCheckpointSelection::owned_by(std::string{id}),
                            Catch::Matchers::ContainsSubstring("reserved"));
        REQUIRE_THROWS_WITH(wiring.claim_component_id(id), Catch::Matchers::ContainsSubstring("reserved"));
    }
}
