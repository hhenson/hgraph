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
// The workers are in-process. That is deliberate: it isolates the model from
// the transport, so a failure here is a design fault rather than a pipe.

#include <hgraph/lib/std/std_operators.h>
#include <hgraph/lib/testing/eval_node.h>
#include <hgraph/lib/testing/record_replay.h>
#include <hgraph/runtime/distributed_child.h>
#include <hgraph/runtime/distributed_map.h>
#include <hgraph/runtime/push_source_node.h>
#include <hgraph/types/service_wiring.h>
#include <hgraph/runtime/distributed_protocol.h>
#include <hgraph/runtime/runtime.h>
#include <hgraph/types/graph_wiring.h>
#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/static_node.h>
#include <hgraph/types/static_schema.h>
#include <hgraph/types/time_series/ts_delta.h>

#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers_string.hpp>

#include <memory>
#include <stdexcept>
#include <vector>

namespace
{
    using namespace hgraph;
    using namespace hgraph::distributed;

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
        static void           compose(Wiring &w, Scalar<"workers", Int> workers)
        {
            auto src  = wire<stdlib::replay_impl, TS<Int>>(w, Str{"in"});
            auto dict = wire<Spread>(w, src).as<KeyedInts>();
            auto out  = wire<dmap_impl<Int, Int, Int>>(w, dict, fn<RunningTotalG>(),
                                                      workers.value())
                            .as<KeyedInts>();
            wire<stdlib::dense_record_impl>(w, wire<Digest>(w, out), Str{"out"});
        }
    };
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
        GraphBuilder gb = build_graph<NodeDistributedGraph>(workers);
        testing::set_replay_values<Int>(gb.global_state(), "in", masks);
        GraphExecutorBuilder eb;
        eb.graph_builder(std::move(gb)).start_time(MIN_ST).end_time(test_end);
        GraphExecutorValue ex = eb.make_executor();
        ex.view().run();
        CHECK(testing::get_recorded_values<Int>(ex.view().graph().global_state(), "out") ==
              expected);
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
}
