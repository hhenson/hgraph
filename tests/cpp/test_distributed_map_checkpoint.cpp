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
#include <catch2/matchers/catch_matchers_string.hpp>

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

    std::vector<std::optional<Value>> typed_events()
    {
        return values<Value>(dict_delta<Int, TS<Int>>({{1, 1}, {2, 10}, {3, 100}}),
                             dict_delta<Int, TS<Int>>({{1, 2}, {4, 1000}}),
                             none,
                             dict_delta<Int, TS<Int>>({{1, 3}}, {2}),
                             dict_delta<Int, TS<Int>>({{1, 4}, {2, 20}, {5, 5}}),
                             dict_delta<Int, TS<Int>>({{3, 200}, {1, 5}}, {4}));
    }

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

    EvalNodeRunOptions interval(std::size_t begin, std::size_t end)
    {
        return {.start_time = MIN_ST + MIN_TD * static_cast<Int>(begin),
                .end_time   = MIN_ST + MIN_TD * static_cast<Int>(end)};
    }

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

    template <typename Graph> void every_cut(const std::vector<std::optional<Value>> &input)
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
                    .component_id = "distributed-map", .load = [&] { return completed; },
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
