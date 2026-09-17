// Distributing a map_ across workers, end to end (RFC 0037).
//
// Every piece built so far, wired together: the caller partitions each cycle's
// TSD delta by key, each worker hosts an ORDINARY map_ over its own group and
// is driven at the caller's evaluation time, and the replies are applied
// straight into the caller's output.
//
// The assertion is the one the RFC makes central: the result must equal
// plain map_, whatever the worker count. Distribution is a throughput
// decision, never a semantic one -- so a difference here is not a performance
// problem, it is a wrong answer.
//
// Most of what follows uses in-process workers. That is deliberate: it
// isolates the model from the transport, so a failure there is a design fault
// rather than a pipe. The last section runs the same assertions with real
// worker PROCESSES, which is what dmap_ is for -- and the two being separable
// is what makes a disagreement between them attributable.

#include <hgraph/lib/std/std_operators.h>
#include <hgraph/lib/testing/eval_node.h>
#include <hgraph/lib/testing/check_output.h>
#include <hgraph/lib/testing/record_replay.h>
#include <hgraph/runtime/distributed_child.h>
#include <hgraph/runtime/distributed_map.h>
#include <hgraph/runtime/distributed_map_wiring.h>
#include <hgraph/runtime/push_source_node.h>
#include <hgraph/types/service_wiring.h>
#include <hgraph/runtime/distributed_protocol.h>
#include <hgraph/runtime/runtime.h>
#include <hgraph/types/graph_wiring.h>
#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/static_node.h>
#include <hgraph/types/static_schema.h>
#include <hgraph/types/time_series/ts_delta.h>

#include "distributed_worker_recipes.h"

#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers_string.hpp>

#include <algorithm>
#include <memory>
#include <string>
#include <stdexcept>
#include <utility>
#include <vector>

namespace
{
    using namespace hgraph;
    using namespace hgraph::distributed;

    // The kernel is shared with the worker PROGRAM rather than declared here:
    // both sides must link the same child, and a type local to this file would
    // mangle differently over there (distributed_worker_recipes.h).
    using hgraph_test::KeyedInts;
    using hgraph_test::RunningTotalG;

    /** What a worker runs: an ordinary map_ between a staged in and a read out. */
    struct WorkerGraph
    {
        static constexpr auto name = "dmap_worker_graph";
        static void           compose(Wiring &w)
        {
            auto in  = wire<boundary_source_impl, KeyedInts>(w, Str{"in"});
            auto out = wire<stdlib::map_>(w, fn<RunningTotalG>(), in).as<KeyedInts>();
            wire<boundary_sink_impl>(w, out, Str{"out"});
        }
    };

    /** Ticks the keys named by a replayed bitmask, so a test chooses them. */
    struct Spread
    {
        static constexpr auto name = "dmap_spread";
        static void           eval(In<"in", TS<Int>> in, Out<KeyedInts> out)
        {
            const Int mask = in.value();
            for (Int key = 0; key < 8; ++key)
            {
                if ((mask & (Int{1} << key)) != 0) { out[key].set(key + 1); }
            }
        }
    };

    /**
     * Folds the result into one comparable number.
     *
     * Key-sensitive on purpose: a plain sum would let two keys swap totals
     * unnoticed, which is exactly the failure a mis-routed key produces.
     */
    struct Digest
    {
        static constexpr auto name = "dmap_digest";
        static void           eval(In<"d", KeyedInts> d, Out<TS<Int>> out)
        {
            Int total = 0;
            for (auto &&[key, child] : d.valid_items())
            {
                total += (key.checked_as<Int>() + 1) * child.value();
            }
            out.set(total);
        }
    };

    /** The reference: the same computation, undistributed. */
    struct LocalGraph
    {
        static constexpr auto name = "dmap_local_graph";
        static void           compose(Wiring &w)
        {
            auto src  = wire<stdlib::replay_impl, TS<Int>>(w, Str{"in"});
            auto dict = wire<Spread>(w, src).as<KeyedInts>();
            auto out  = wire<stdlib::map_>(w, fn<RunningTotalG>(), dict).as<KeyedInts>();
            wire<stdlib::dense_record_impl>(w, wire<Digest>(w, out), Str{"out"});
        }
    };

    // --- the caller's side ---------------------------------------------------
    // Held test-locally rather than in node state: a node that owns worker
    // processes is the next piece of work, and putting it here keeps this test
    // about the MODEL rather than about node storage.

    struct Workers
    {
        std::vector<std::unique_ptr<DistributedChildHost>> hosts{};
        BoundarySlots                                      slots{};
        std::size_t                                        in_slot{0};
        std::size_t                                        out_slot{0};
        std::size_t                                        count{0};
    };

    Workers &workers()
    {
        static Workers state;
        return state;
    }

    /** The partition: a key belongs to exactly one worker. */
    bool owns_key(const void *context, const ValueView &key)
    {
        const auto group = *static_cast<const std::size_t *>(context);
        const auto value = static_cast<std::size_t>(key.checked_as<Int>());
        return (value % workers().count) == group;
    }

    /**
     * The caller: partition, dispatch, apply. This is the body a ``dmap_``
     * node will have.
     */
    struct DistributeNode
    {
        static constexpr auto name = "dmap_distribute";

        static void eval(In<"ts", KeyedInts> ts, DateTime now, Out<KeyedInts> out)
        {
            auto &state = workers();
            for (std::size_t group = 0; group < state.count; ++group)
            {
                CycleRequest request;
                request.evaluation_time = now;
                request.staged.push_back(
                    SlotDelta{state.in_slot,
                              capture_dict_delta_where(ts.base(), &owns_key, &group)});

                const CycleReply reply = serve_cycle(*state.hosts[group], state.slots, request);
                if (!reply.error.empty()) { throw std::runtime_error(reply.error); }

                // Applied STRAIGHT into the output, with no merge: the groups
                // hold disjoint keys, so applying them in turn is applying
                // their union.
                for (const auto &collected : reply.collected)
                {
                    apply_delta(out.base(), collected.delta.view());
                }
            }
        }
    };

    struct DistributedGraph
    {
        static constexpr auto name = "dmap_distributed_graph";
        static void           compose(Wiring &w)
        {
            auto src  = wire<stdlib::replay_impl, TS<Int>>(w, Str{"in"});
            auto dict = wire<Spread>(w, src).as<KeyedInts>();
            auto out  = wire<DistributeNode>(w, dict).as<KeyedInts>();
            wire<stdlib::dense_record_impl>(w, wire<Digest>(w, out), Str{"out"});
        }
    };

    const DateTime test_end = MIN_ST + TimeDelta{10000};

    std::vector<std::optional<Int>> run_local(const std::vector<std::optional<Int>> &masks)
    {
        GraphBuilder gb = build_graph<LocalGraph>();
        testing::set_replay_values<Int>(gb.global_state(), "in", masks);
        GraphExecutorBuilder eb;
        eb.graph_builder(std::move(gb)).start_time(MIN_ST).end_time(test_end);
        GraphExecutorValue ex = eb.make_executor();
        ex.view().run();
        return testing::get_recorded_values<Int>(ex.view().graph().global_state(), "out");
    }

    std::vector<std::optional<Int>> run_distributed(const std::vector<std::optional<Int>> &masks,
                                                    std::size_t worker_count)
    {
        auto &state = workers();
        state       = Workers{};
        state.count = worker_count;
        // The slots carry the TSD's DELTA schema, not its value schema: what
        // crosses the boundary each cycle is a delta.
        const auto *delta = schema_descriptor<KeyedInts>::ts_meta()->delta_value_schema;
        state.in_slot     = state.slots.add("in", delta, SlotDirection::Input);
        state.out_slot    = state.slots.add("out", delta, SlotDirection::Output);

        for (std::size_t i = 0; i < worker_count; ++i)
        {
            auto host = std::make_unique<DistributedChildHost>(build_graph<WorkerGraph>(), test_end);
            host->start(MIN_ST);
            state.hosts.push_back(std::move(host));
        }

        GraphBuilder gb = build_graph<DistributedGraph>();
        testing::set_replay_values<Int>(gb.global_state(), "in", masks);
        GraphExecutorBuilder eb;
        eb.graph_builder(std::move(gb)).start_time(MIN_ST).end_time(test_end);
        GraphExecutorValue ex = eb.make_executor();
        ex.view().run();
        auto recorded = testing::get_recorded_values<Int>(ex.view().graph().global_state(), "out");

        for (auto &host : state.hosts) { host->stop(); }
        state = Workers{};
        return recorded;
    }
}  // namespace

TEST_CASE("distributed map: the result equals plain map_, at every worker count")
{
    (void)TypeRegistry::instance().register_scalar<Int>("int");
    stdlib::register_standard_operators();

    // Overlapping key sets across cycles, so per-key state accumulates and a
    // key routed to the wrong worker produces a visibly wrong total.
    const std::vector<std::optional<Int>> masks{Int{0b00001111}, Int{0b00110011},
                                                Int{0b10101010}, Int{0b00000001}};

    const auto expected = run_local(masks);
    // Pinned so the comparison below cannot pass on empty or all-null output.
    // Cycle 1 ticks keys 0..3 with values 1..4, so each key's running total is
    // its own value and the key-weighted digest is 1+4+9+16 = 30.
    REQUIRE(expected.size() >= 2);
    REQUIRE(expected[0].has_value());
    CHECK(*expected[0] == Int{30});

    // The partition count must not be observable in the result -- which is the
    // determinism contract, stated as a test.
    for (const std::size_t count : {std::size_t{1}, std::size_t{2}, std::size_t{3},
                                    std::size_t{8}})
    {
        CHECK(run_distributed(masks, count) == expected);
    }
}

TEST_CASE("distributed map: per-key state lives in the worker that owns the key")
{
    (void)TypeRegistry::instance().register_scalar<Int>("int");
    stdlib::register_standard_operators();

    // One key ticking repeatedly: its running total is the whole answer, so a
    // key whose history split across two workers cannot pass.
    const std::vector<std::optional<Int>> masks{Int{0b0100}, Int{0b0100}, Int{0b0100}};

    const auto expected = run_local(masks);
    CHECK(run_distributed(masks, 2) == expected);
    CHECK(run_distributed(masks, 5) == expected);
}

// --- the node ---------------------------------------------------------------
// The same model, now owned by a node rather than by the test: the worker pool
// lives on the heap behind the node's State (the start-lifecycle pattern), so
// a caller writes dmap_ and nothing else.

namespace
{
    struct NodeDistributedGraph
    {
        static constexpr auto name = "dmap_node_graph";
        static void compose(Wiring &w, Scalar<"workers", Int> workers,
                            Scalar<"in_process", Bool> in_process, Scalar<"program", Str> program)
        {
            auto src  = wire<stdlib::replay_impl, TS<Int>>(w, Str{"in"});
            auto dict = wire<Spread>(w, src).as<KeyedInts>();
            auto out  = wire<dmap_impl<Int, Int, Int>>(w, dict, fn<RunningTotalG>(),
                                                      workers.value(), in_process.value(),
                                                      program.value())
                            .as<KeyedInts>();
            wire<stdlib::dense_record_impl>(w, wire<Digest>(w, out), Str{"out"});
        }
    };

    std::vector<std::optional<Int>> run_node(const std::vector<std::optional<Int>> &masks,
                                             Int workers, Bool in_process, const Str &program)
    {
        GraphBuilder gb = build_graph<NodeDistributedGraph>(workers, in_process, program);
        testing::set_replay_values<Int>(gb.global_state(), "in", masks);
        GraphExecutorBuilder eb;
        eb.graph_builder(std::move(gb)).start_time(MIN_ST).end_time(test_end);
        GraphExecutorValue ex = eb.make_executor();
        ex.view().run();
        return testing::get_recorded_values<Int>(ex.view().graph().global_state(), "out");
    }
}  // namespace

TEST_CASE("dmap_: the node produces what map_ produces")
{
    (void)TypeRegistry::instance().register_scalar<Int>("int");
    stdlib::register_standard_operators();

    const std::vector<std::optional<Int>> masks{Int{0b00001111}, Int{0b00110011},
                                                Int{0b10101010}, Int{0b00000001}};
    const auto expected = run_local(masks);
    REQUIRE(expected.size() >= 2);
    REQUIRE(expected[0].has_value());

    for (const Int workers : {Int{1}, Int{2}, Int{4}})
    {
        CHECK(run_node(masks, workers, true, Str{}) == expected);
    }
}

// --- what a distributed child may not contain -------------------------------
// Neither rejection is new detection. A worker graph is TOP-LEVEL, so a service
// consumer already fails to wire, and a push source is already refused outside
// a real-time executor. What dmap_ adds is a diagnostic that says which of the
// caller's decisions caused it -- the underlying messages name a service path
// or an executor, neither of which points at the kernel.

namespace
{
    struct ProbePricesService
    {
        static constexpr std::string_view name{"dmap_probe_prices"};
        using output_schema = TSD<Int, TS<Int>>;
    };

    /** Reaches for a service, whose source would live in the CALLING graph. */
    struct ServiceKernelG
    {
        static constexpr auto name = "dmap_service_kernel";
        static Port<TS<Int>>  compose(Wiring &w, Port<TS<Int>> ts)
        {
            static_cast<void>(wire<ProbePricesService>(w, service::path("probe")));
            return ts;
        }
    };
}  // namespace

TEST_CASE("dmap_: a child that consumes a service is refused, and told why")
{
    (void)TypeRegistry::instance().register_scalar<Int>("int");
    stdlib::register_standard_operators();

    CHECK_THROWS_WITH(
        (WorkerPool::build<Int, Int, Int>(fn<ServiceKernelG>(), 2, MIN_ST, test_end)),
        Catch::Matchers::ContainsSubstring("Services, contexts and shared outputs are not") &&
            Catch::Matchers::ContainsSubstring("dmap_"));
}

TEST_CASE("dmap_: a worker count of zero is refused")
{
    (void)TypeRegistry::instance().register_scalar<Int>("int");
    stdlib::register_standard_operators();

    CHECK_THROWS_WITH(
        (WorkerPool::build<Int, Int, Int>(fn<RunningTotalG>(), 0, MIN_ST, test_end)),
        Catch::Matchers::ContainsSubstring("at least one worker"));
    CHECK_THROWS_WITH(run_node({1}, -1, true, Str{}),
                      Catch::Matchers::ContainsSubstring("at least one worker"));
}

// --- across processes -------------------------------------------------------
// The same assertions, with the workers in separate OS processes. Nothing
// above the transport changes: the partition, the messages and the apply are
// the ones the in-process cases above already exercised, which is what makes a
// difference here readable as a transport fault.

namespace
{
    /** Wired but never registered, so the caller cannot find a worker for it. */
    struct UnregisteredG
    {
        static constexpr auto name = "dmap_unregistered_g";
        static Port<TS<Int>>  compose(Wiring &, Port<TS<Int>> ts) { return ts; }
    };
}  // namespace

TEST_CASE("dmap_: the node produces what map_ produces, in worker processes")
{
    (void)TypeRegistry::instance().register_scalar<Int>("int");
    stdlib::register_standard_operators();
    hgraph_test::register_distributed_test_recipes();

    const std::vector<std::optional<Int>> masks{Int{0b00001111}, Int{0b00110011},
                                                Int{0b10101010}, Int{0b00000001}};
    const auto expected = run_local(masks);
    REQUIRE(expected.size() >= 2);
    REQUIRE(expected[0].has_value());
    CHECK(*expected[0] == Int{30});

    for (const Int workers : {Int{1}, Int{2}, Int{4}})
    {
        CHECK(run_node(masks, workers, false, Str{HGRAPH_TEST_WORKER_PROGRAM}) == expected);
    }
}

TEST_CASE("dmap_: per-key state lives in the worker process that owns the key")
{
    (void)TypeRegistry::instance().register_scalar<Int>("int");
    stdlib::register_standard_operators();
    hgraph_test::register_distributed_test_recipes();

    // One key ticking repeatedly: its running total is the whole answer, so a
    // key whose history split across two processes cannot pass -- and neither
    // can one whose child was rebuilt between cycles.
    const std::vector<std::optional<Int>> masks{Int{0b0100}, Int{0b0100}, Int{0b0100}};
    const auto expected = run_local(masks);

    CHECK(run_node(masks, 2, false, Str{HGRAPH_TEST_WORKER_PROGRAM}) == expected);
    CHECK(run_node(masks, 5, false, Str{HGRAPH_TEST_WORKER_PROGRAM}) == expected);
}

TEST_CASE("dmap_: a kernel no worker program can build is refused, by name")
{
    (void)TypeRegistry::instance().register_scalar<Int>("int");
    stdlib::register_standard_operators();

    WorkerPoolConfig config;
    config.workers  = 2;
    config.end_time = test_end;
    config.hosting  = WorkerHosting::Process;
    config.program  = HGRAPH_TEST_WORKER_PROGRAM;

    // The failure is at the CALLER, before any process starts: a worker that
    // could not find its recipe would otherwise fail as an exit code from a
    // process the caller never chose to look at.
    CHECK_THROWS_WITH(
        (WorkerPool::build<Int, Int, Int>(fn<UnregisteredG>(), config)),
        Catch::Matchers::ContainsSubstring("no distributed worker is registered") &&
            Catch::Matchers::ContainsSubstring("register_distributed_map_worker"));
}

TEST_CASE("dmap_: a worker program that cannot be started fails the node")
{
    (void)TypeRegistry::instance().register_scalar<Int>("int");
    stdlib::register_standard_operators();
    hgraph_test::register_distributed_test_recipes();

    // Also the falsification for the cases above: if ``in_process == false``
    // did not really spawn anything, a program path that cannot exist would be
    // ignored and this would pass.
    const std::vector<std::optional<Int>> masks{Int{0b0001}};
    CHECK_THROWS(run_node(masks, 2, false, Str{"/hgraph/no/such/worker/program"}));
}

// --- a child that schedules itself ------------------------------------------
// RFC 0037 acceptance criterion 2, and the one the Python prototype could not
// attempt: engine time was not external there, so every prototype kernel had
// to be time-independent.
//
// The child's answer arrives on a cycle the calling graph has no reason to
// evaluate. Under map_ the nested scheduler propagates it; under dmap_ the
// only route is the child's next_scheduled_time riding back in the reply.

namespace
{
    using hgraph_test::DelayedDoubleG;

    struct LocalDelayedGraph
    {
        static constexpr auto name = "dmap_local_delayed_graph";
        static void           compose(Wiring &w)
        {
            auto src  = wire<stdlib::replay_impl, TS<Int>>(w, Str{"in"});
            auto dict = wire<Spread>(w, src).as<KeyedInts>();
            auto out  = wire<stdlib::map_>(w, fn<DelayedDoubleG>(), dict).as<KeyedInts>();
            wire<stdlib::dense_record_impl>(w, wire<Digest>(w, out), Str{"out"});
        }
    };

    struct NodeDelayedGraph
    {
        static constexpr auto name = "dmap_node_delayed_graph";
        static void compose(Wiring &w, Scalar<"workers", Int> workers,
                            Scalar<"in_process", Bool> in_process, Scalar<"program", Str> program)
        {
            auto src  = wire<stdlib::replay_impl, TS<Int>>(w, Str{"in"});
            auto dict = wire<Spread>(w, src).as<KeyedInts>();
            auto out  = wire<dmap_impl<Int, Int, Int>>(w, dict, fn<DelayedDoubleG>(),
                                                      workers.value(), in_process.value(),
                                                      program.value())
                            .as<KeyedInts>();
            wire<stdlib::dense_record_impl>(w, wire<Digest>(w, out), Str{"out"});
        }
    };

    /** A recorded series as text, so a disagreement says WHICH cycle differs. */
    std::string describe(const std::vector<std::optional<Int>> &series)
    {
        std::string text{"["};
        for (std::size_t i = 0; i < series.size(); ++i)
        {
            if (i != 0) { text += ", "; }
            text += series[i].has_value() ? std::to_string(*series[i]) : std::string{"-"};
        }
        return text + "]";
    }

    template <typename TGraph, typename... TArgs>
    std::vector<std::optional<Int>> run_recorded(const std::vector<std::optional<Int>> &masks,
                                                 TArgs &&...args)
    {
        GraphBuilder gb = build_graph<TGraph>(std::forward<TArgs>(args)...);
        testing::set_replay_values<Int>(gb.global_state(), "in", masks);
        GraphExecutorBuilder eb;
        eb.graph_builder(std::move(gb)).start_time(MIN_ST).end_time(test_end);
        GraphExecutorValue ex = eb.make_executor();
        ex.view().run();
        return testing::get_recorded_values<Int>(ex.view().graph().global_state(), "out");
    }
}  // namespace

TEST_CASE("dmap_: a child that schedules itself fires at the same engine times as map_")
{
    (void)TypeRegistry::instance().register_scalar<Int>("int");
    stdlib::register_standard_operators();
    hgraph_test::register_distributed_test_recipes();

    const std::vector<std::optional<Int>> masks{Int{0b0011}, Int{0b0110}};
    const auto expected = run_recorded<LocalDelayedGraph>(masks);

    // Pinned: the delayed answer must actually arrive, or "equal to map_"
    // would be satisfied by two runs that both produce nothing.
    REQUIRE(expected.size() >= 3);
    const auto ticks = std::count_if(expected.begin(), expected.end(),
                                     [](const auto &value) { return value.has_value(); });
    CHECK(ticks >= 2);

    for (const Int workers : {Int{1}, Int{3}})
    {
        CHECK(describe(run_recorded<NodeDelayedGraph>(masks, workers, true, Str{})) ==
              describe(expected));
        CHECK(describe(run_recorded<NodeDelayedGraph>(masks, workers, false,
                                                      Str{HGRAPH_TEST_WORKER_PROGRAM})) ==
              describe(expected));
    }
}

// --- a recorded deviation from map_ -----------------------------------------
// A child whose output gains a key BEFORE that key has a value. Under map_
// that structural change ticks the caller's output; through a distributed
// child it does not, and the key appears on the cycle it first has a value.
//
// The cause is below dmap_ and is pinned here rather than hidden: the
// canonical TSD delta is Bundle{removed, modified}, which has no way to say
// "this key exists and has no value yet", so capture_delta / apply_delta --
// the pair record/replay also uses -- cannot carry it. The in-process mode is
// what makes that attribution safe: it encodes nothing, so a difference that
// survives it is not the transport.
//
// If this test starts FAILING because the two agree, the deviation has been
// fixed and RFC 0037's *Known deviations* should lose an entry.

namespace
{
    using hgraph_test::ArmSilentlyG;

    struct LocalSilentGraph
    {
        static constexpr auto name = "dmap_local_silent_graph";
        static void           compose(Wiring &w)
        {
            auto src  = wire<stdlib::replay_impl, TS<Int>>(w, Str{"in"});
            auto dict = wire<Spread>(w, src).as<KeyedInts>();
            auto out  = wire<stdlib::map_>(w, fn<ArmSilentlyG>(), dict).as<KeyedInts>();
            wire<stdlib::dense_record_impl>(w, wire<Digest>(w, out), Str{"out"});
        }
    };

    struct NodeSilentGraph
    {
        static constexpr auto name = "dmap_node_silent_graph";
        static void compose(Wiring &w, Scalar<"workers", Int> workers,
                            Scalar<"in_process", Bool> in_process, Scalar<"program", Str> program)
        {
            auto src  = wire<stdlib::replay_impl, TS<Int>>(w, Str{"in"});
            auto dict = wire<Spread>(w, src).as<KeyedInts>();
            auto out  = wire<dmap_impl<Int, Int, Int>>(w, dict, fn<ArmSilentlyG>(),
                                                      workers.value(), in_process.value(),
                                                      program.value())
                            .as<KeyedInts>();
            wire<stdlib::dense_record_impl>(w, wire<Digest>(w, out), Str{"out"});
        }
    };
}  // namespace

TEST_CASE("dmap_: a key with no value yet does not tick the caller -- a recorded deviation")
{
    (void)TypeRegistry::instance().register_scalar<Int>("int");
    stdlib::register_standard_operators();
    hgraph_test::register_distributed_test_recipes();

    const std::vector<std::optional<Int>> masks{Int{0b0011}, Int{0b0110}};

    // Cycle 1 adds key 2, whose child is armed and silent. map_ ticks on that
    // structural change; dmap_ does not, and every value that follows agrees.
    // Cycle 0 ticks under both -- not because the structure crossed, but
    // because it is the cycle that first makes the caller's output valid.
    CHECK(describe(run_recorded<LocalSilentGraph>(masks)) == "[0, 0, 50, 140]");
    CHECK(describe(run_recorded<NodeSilentGraph>(masks, 1, true, Str{})) == "[0, -, 50, 140]");
    CHECK(describe(run_recorded<NodeSilentGraph>(masks, 3, false,
                                                 Str{HGRAPH_TEST_WORKER_PROGRAM})) ==
          "[0, -, 50, 140]");
}

// --- keys leaving -----------------------------------------------------------
// RFC 0037 criterion 5. Every case above only ever ADDS keys, which leaves the
// more interesting half untested: a key removed from the caller's input must
// reach the worker that owns it, tear that child down, and disappear from the
// caller's output. If removal did not cross, children would accumulate in the
// workers for the life of the run and the output would keep answering with
// stale keys -- and no test here would have noticed.

namespace
{
    /** Ticks the keys the mask names, and REMOVES the ones it does not. */
    struct SpreadExact
    {
        static constexpr auto name = "dmap_spread_exact";
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

    struct LocalRemovalGraph
    {
        static constexpr auto name = "dmap_local_removal_graph";
        static void           compose(Wiring &w)
        {
            auto src  = wire<stdlib::replay_impl, TS<Int>>(w, Str{"in"});
            auto dict = wire<SpreadExact>(w, src).as<KeyedInts>();
            auto out  = wire<stdlib::map_>(w, fn<RunningTotalG>(), dict).as<KeyedInts>();
            wire<stdlib::dense_record_impl>(w, wire<Digest>(w, out), Str{"out"});
        }
    };

    struct NodeRemovalGraph
    {
        static constexpr auto name = "dmap_node_removal_graph";
        static void compose(Wiring &w, Scalar<"workers", Int> workers,
                            Scalar<"in_process", Bool> in_process, Scalar<"program", Str> program)
        {
            auto src  = wire<stdlib::replay_impl, TS<Int>>(w, Str{"in"});
            auto dict = wire<SpreadExact>(w, src).as<KeyedInts>();
            auto out  = wire<dmap_impl<Int, Int, Int>>(w, dict, fn<RunningTotalG>(),
                                                      workers.value(), in_process.value(),
                                                      program.value())
                            .as<KeyedInts>();
            wire<stdlib::dense_record_impl>(w, wire<Digest>(w, out), Str{"out"});
        }
    };
}  // namespace

TEST_CASE("dmap_: a key removed from the input is removed from the output")
{
    (void)TypeRegistry::instance().register_scalar<Int>("int");
    stdlib::register_standard_operators();
    hgraph_test::register_distributed_test_recipes();

    // Keys appear, then leave, then a subset comes back. A returning key gets a
    // FRESH child, so its running total restarts -- which is what makes this
    // sensitive to a teardown that did not happen: a worker that kept the old
    // child would carry the old total forward and the digest would differ.
    const std::vector<std::optional<Int>> masks{Int{0b1111}, Int{0b0110}, Int{0b0000},
                                                Int{0b1111}};

    // Pinned, because "equals map_" would be satisfied by two runs that both
    // failed to tear anything down. Cycle 0 builds keys 0-3 with totals 1-4
    // (digest 1+4+9+16 = 30); cycle 3 rebuilds them and must give 30 again. A
    // worker that kept the old children would carry the totals forward and
    // answer 60.
    CHECK(describe(run_recorded<LocalRemovalGraph>(masks)) == "[30, 26, 0, 30]");
    const auto expected = run_recorded<LocalRemovalGraph>(masks);

    for (const Int workers : {Int{1}, Int{2}, Int{3}})
    {
        CHECK(describe(run_recorded<NodeRemovalGraph>(masks, workers, true, Str{})) ==
              describe(expected));
        CHECK(describe(run_recorded<NodeRemovalGraph>(masks, workers, false,
                                                      Str{HGRAPH_TEST_WORKER_PROGRAM})) ==
              describe(expected));
    }
}

namespace {
struct PreparedMapGraph {
    static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> masks) {
        auto input = wire<Spread>(w, masks);
        auto plan = prepare_distributed_map(fn<RunningTotalG>(), input.erased().schema);
        plan.config.hosting = WorkerHosting::InProcess;
        plan.config.workers = 3;
        auto out = wire_distributed_map(w, input.erased(),
            std::make_shared<const DistributedMapPlan>(std::move(plan)));
        return wire<Digest>(w, out);
    }
};
}

TEST_CASE("dmap_: prepared native wiring shares the worker pool semantics") {
    using namespace hgraph::testing;
    stdlib::register_standard_operators();
    auto actual = eval_node<PreparedMapGraph>(values<Int>(1, 3, 1));
    CHECK(actual == std::vector<std::optional<Int>>{1, 6, 7});
}
