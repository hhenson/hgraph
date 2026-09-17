// What dmap_ costs, and where it starts paying (RFC 0037, criterion 9).
//
// dmap_ is not a drop-in speed-up. Every cycle it captures a delta per
// partition, encodes it, writes it, reads a reply, decodes it and applies it,
// and then waits for the slowest worker. That is a fixed cost per cycle per
// worker, paid whether or not the children did anything worth distributing --
// so for a cheap child it is pure loss, by a wide margin, and the only
// question worth measuring is where the curve crosses.
//
// This program is its own worker, which is the shape RFC 0037 intends: one
// program, launched again with worker flags. Run it directly; it is not a
// ctest case, because a timing number is evidence rather than a pass/fail.
//
//     hgraph_distributed_perf [cycles]

#include <hgraph/lib/std/std_operators.h>
#include <hgraph/runtime/distributed_map.h>
#include <hgraph/runtime/distributed_worker.h>
#include <hgraph/runtime/global_state.h>
#include <hgraph/runtime/node_scheduler.h>
#include <hgraph/runtime/runtime.h>
#include <hgraph/types/graph_wiring.h>
#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/static_node.h>
#include <hgraph/types/static_schema.h>

#include <fmt/format.h>

#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <exception>
#include <string>
#include <utility>
#include <vector>

namespace
{
    using namespace hgraph;
    using namespace hgraph::distributed;

    using KeyedInts = TSD<Int, TS<Int>>;

    /**
     * Per-key work, as a compile-time count.
     *
     * A multiply-xor chain whose result is written to the output, so the
     * optimiser cannot drop it -- and cheap per iteration, so the count is a
     * reasonable proxy for "how much does one child do per tick".
     */
    template <int Iterations> Int burn(Int seed)
    {
        Int acc = seed | 1;
        for (int i = 0; i < Iterations; ++i)
        {
            acc = acc * 6364136223846793005LL + 1442695040888963407LL;
            acc ^= acc >> 13;
        }
        return acc;
    }
}  // namespace

// One named kernel per work level: a recipe is keyed by the kernel's TYPE, so
// the work has to be part of the type rather than a scalar (RFC 0037, worker
// bootstrap -- a recipe carries a name and nothing else).
#define HGRAPH_PERF_KERNEL(Tag, Iterations)                                                        \
    namespace                                                                                      \
    {                                                                                              \
        struct Tag##Node                                                                           \
        {                                                                                          \
            static constexpr auto name = "perf_kernel_" #Tag;                                      \
            static void eval(In<"ts", TS<Int>> ts, Out<TS<Int>> out)                               \
            {                                                                                      \
                out.set(burn<Iterations>(ts.value()));                                             \
            }                                                                                      \
        };                                                                                         \
        struct Tag##G                                                                              \
        {                                                                                          \
            static constexpr auto name = "perf_kernel_graph_" #Tag;                                \
            static Port<TS<Int>>  compose(Wiring &w, Port<TS<Int>> ts)                             \
            {                                                                                      \
                return wire<Tag##Node>(w, ts).as<TS<Int>>();                                       \
            }                                                                                      \
        };                                                                                         \
    }

HGRAPH_PERF_KERNEL(Trivial, 0)
HGRAPH_PERF_KERNEL(Small, 128)
HGRAPH_PERF_KERNEL(Medium, 2048)
HGRAPH_PERF_KERNEL(Large, 16384)

namespace
{
    /** Ticks every key, every cycle, for a fixed number of cycles. */
    struct Generator
    {
        static constexpr auto name               = "perf_generator";
        static constexpr bool schedule_on_start  = true;

        static void eval(Scalar<"keys", Int> keys, Scalar<"cycles", Int> cycles,
                         State<Int> cycle, NodeScheduler scheduler, DateTime now,
                         Out<KeyedInts> out)
        {
            const Int index = cycle.get();
            for (Int key = 0; key < keys.value(); ++key) { out[key].set(index + key); }
            cycle.modify() += 1;
            if (cycle.get() < cycles.value()) { scheduler.schedule(now + MIN_TD); }
        }
    };

    /** Folds the result so the run cannot be optimised away, and can be compared. */
    struct Checksum
    {
        static constexpr auto name = "perf_checksum";

        static void eval(In<"d", KeyedInts> d, State<Int> total)
        {
            Int sum = 0;
            for (auto &&[key, child] : d.valid_items()) { sum += child.value(); }
            total.modify() += sum;
        }

        static void stop(State<Int> total, GlobalStateView gs)
        {
            gs.set("checksum", Value{total.get()});
        }
    };

    template <typename TKernel> struct LocalGraph
    {
        static constexpr auto name = "perf_local_graph";
        static void compose(Wiring &w, Scalar<"keys", Int> keys, Scalar<"cycles", Int> cycles)
        {
            auto source = wire<Generator>(w, keys.value(), cycles.value()).as<KeyedInts>();
            auto mapped = wire<stdlib::map_>(w, fn<TKernel>(), source).template as<KeyedInts>();
            wire<Checksum>(w, mapped);
        }
    };

    template <typename TKernel> struct DistributedGraph
    {
        static constexpr auto name = "perf_distributed_graph";
        static void compose(Wiring &w, Scalar<"keys", Int> keys, Scalar<"cycles", Int> cycles,
                            Scalar<"workers", Int> workers, Scalar<"in_process", Bool> in_process)
        {
            auto source = wire<Generator>(w, keys.value(), cycles.value()).as<KeyedInts>();
            auto mapped = wire<dmap_impl<Int, Int, Int>>(w, source, fn<TKernel>(), workers.value(),
                                                         in_process.value(), Str{})
                              .template as<KeyedInts>();
            wire<Checksum>(w, mapped);
        }
    };

    struct Measurement
    {
        double micros_per_cycle{0};
        /**
         * What it costs once, before steady state: starting the graph plus the
         * first cycle.
         *
         * Both halves matter and neither alone is the answer. ``posix_spawn``
         * and ``CreateProcess`` return as soon as the child EXISTS, not when
         * it is ready, so the parent's spawn is sub-millisecond and the
         * child's real startup -- loading the image, registering operators,
         * wiring its graph -- is paid by whoever waits for the first reply.
         * Charging that to the graph start would flatter it; leaving it in the
         * per-cycle figure would make the answer depend on run length.
         */
        double setup_micros{0};
        Int    checksum{0};
    };

    /**
     * Time the start and the cycles SEPARATELY, by driving the graph.
     *
     * Spawning a worker is expensive and happens once; dividing one run's wall
     * time by its cycle count charges that to every cycle and makes the answer
     * a function of how long the benchmark happens to be. Inferring the split
     * from two run lengths is worse still -- the fixed cost varies by more
     * between runs than the per-cycle cost being measured.
     *
     * So the benchmark uses the ExternallyDriven executor: ``start_external``
     * plus the first cycle is the setup, and the rest of the step loop is the
     * steady state, with nothing to separate after the fact. (That mode exists
     * for RFC 0037's workers; this is a second use of it.)
     */
    template <typename TGraph, typename... TArgs> Measurement measure(Int cycles, TArgs &&...args)
    {
        GraphBuilder         gb = build_graph<TGraph>(std::forward<TArgs>(args)...);
        GraphExecutorBuilder eb;
        eb.graph_builder(std::move(gb))
            .mode(GraphExecutorMode::ExternallyDriven)
            .start_time(MIN_ST)
            .end_time(MAX_ET);
        GraphExecutorValue ex = eb.make_executor();

        const auto before_start = std::chrono::steady_clock::now();
        ex.view().start_external(MIN_ST);

        // The first cycle is where a worker's own startup is actually paid,
        // so it is timed apart from the rest rather than averaged into it.
        Int      stepped = 0;
        DateTime when    = ex.view().graph().next_scheduled_time();
        if (when != MAX_DT)
        {
            (void)ex.view().step(when);
            when = ex.view().graph().next_scheduled_time();
            ++stepped;
        }
        const auto after_first = std::chrono::steady_clock::now();

        while (when != MAX_DT && stepped < cycles)
        {
            (void)ex.view().step(when);
            when = ex.view().graph().next_scheduled_time();
            ++stepped;
        }
        const auto after_steps = std::chrono::steady_clock::now();
        ex.view().stop_external();

        const Int steady = stepped > 1 ? stepped - 1 : 1;
        Measurement result;
        result.setup_micros =
            std::chrono::duration<double, std::micro>(after_first - before_start).count();
        result.micros_per_cycle =
            std::chrono::duration<double, std::micro>(after_steps - after_first).count() /
            static_cast<double>(steady);
        const ValueView checksum = ex.view().graph().global_state().get("checksum");
        result.checksum          = checksum.valid() ? checksum.checked_as<Int>() : Int{-1};
        return result;
    }

    struct Row
    {
        const char *kernel;
        Int         keys;
        double      local;
        Int         local_checksum;
        struct Point
        {
            Int         workers;
            Measurement in_process;
            Measurement processes;
        };
        std::vector<Point> points;
    };

    template <typename TKernel>
    Row sweep(const char *label, Int keys, Int cycles, const std::vector<Int> &worker_counts)
    {
        Row row;
        row.kernel = label;
        row.keys   = keys;

        const Measurement local = measure<LocalGraph<TKernel>>(cycles, keys, cycles);
        row.local               = local.micros_per_cycle;
        row.local_checksum      = local.checksum;

        for (const Int workers : worker_counts)
        {
            Row::Point point;
            point.workers = workers;
            point.in_process =
                measure<DistributedGraph<TKernel>>(cycles, keys, cycles, workers, Bool{true});
            point.processes =
                measure<DistributedGraph<TKernel>>(cycles, keys, cycles, workers, Bool{false});
            row.points.push_back(point);
        }
        return row;
    }

    void register_recipes()
    {
        register_distributed_map_worker<TrivialG, Int, Int, Int>();
        register_distributed_map_worker<SmallG, Int, Int, Int>();
        register_distributed_map_worker<MediumG, Int, Int, Int>();
        register_distributed_map_worker<LargeG, Int, Int, Int>();
    }

    void report(const std::vector<Row> &rows, Int cycles)
    {
        std::fputs("\n  Steady-state cost per cycle, in microseconds.\n"
                   "  'start' is graph start plus the first cycle, in milliseconds -- for\n"
                   "  processes that is where the workers' own startup is paid.\n"
                   "  'ratio' is dmap_ over processes divided by map_: under 1.00 is a win.\n\n"
                   "  kernel     keys   map_ us/cy   workers   in-proc    procs   ratio"
                   "   start ms\n", stdout);
        std::fputs("  ----------------------------------------------------------------------------\n",
                   stdout);
        for (const Row &row : rows)
        {
            for (const Row::Point &point : row.points)
            {
                // A wrong answer makes a fast one worthless, so the checksum
                // is printed with the timing rather than checked elsewhere.
                const char *agrees = (point.processes.checksum == row.local_checksum &&
                                      point.in_process.checksum == row.local_checksum)
                                         ? " "
                                         : "!";
                std::fputs(
                    fmt::format("{} {:<9} {:>5} {:>12.1f} {:>9} {:>9.1f} {:>8.1f} {:>7.2f} {:>9.1f}\n",
                                agrees, row.kernel, row.keys, row.local, point.workers,
                                point.in_process.micros_per_cycle,
                                point.processes.micros_per_cycle,
                                point.processes.micros_per_cycle / row.local,
                                point.processes.setup_micros / 1000.0)
                        .c_str(),
                    stdout);
            }
        }

        std::fputs("\n{\n  \"cycles\": ", stdout);
        std::fputs(fmt::format("{},\n  \"rows\": [\n", cycles).c_str(), stdout);
        bool first_row = true;
        for (const Row &row : rows)
        {
            for (const Row::Point &point : row.points)
            {
                if (!first_row) { std::fputs(",\n", stdout); }
                first_row = false;
                std::fputs(
                    fmt::format(
                        "    {{\"kernel\": \"{}\", \"keys\": {}, \"workers\": {}, "
                        "\"map_us_per_cycle\": {:.3f}, \"dmap_in_process_us_per_cycle\": {:.3f}, "
                        "\"dmap_process_us_per_cycle\": {:.3f}, \"ratio\": {:.4f}, "
                        "\"dmap_process_setup_us\": {:.1f}, \"checksums_agree\": {}}}",
                        row.kernel, row.keys, point.workers, row.local,
                        point.in_process.micros_per_cycle, point.processes.micros_per_cycle,
                        point.processes.micros_per_cycle / row.local,
                        point.processes.setup_micros,
                        (point.processes.checksum == row.local_checksum &&
                         point.in_process.checksum == row.local_checksum)
                            ? "true"
                            : "false")
                        .c_str(),
                    stdout);
            }
        }
        std::fputs("\n  ]\n}\n", stdout);
    }
}  // namespace

int main(int argc, char **argv)
{
    try
    {
        (void)TypeRegistry::instance().register_scalar<Int>("int");
        stdlib::register_standard_operators();
        register_recipes();

        // This program is its own worker. A host application's main looks
        // exactly like this line.
        if (run_worker_if_requested(argc, argv)) { return 0; }

        const Int cycles = argc > 1 ? static_cast<Int>(std::atoll(argv[1])) : Int{200};
        const std::vector<Int> worker_counts{1, 2, 4, 8};

        std::vector<Row> rows;
        for (const Int keys : {Int{64}, Int{256}})
        {
            rows.push_back(sweep<TrivialG>("trivial", keys, cycles, worker_counts));
            rows.push_back(sweep<SmallG>("small", keys, cycles, worker_counts));
            rows.push_back(sweep<MediumG>("medium", keys, cycles, worker_counts));
            rows.push_back(sweep<LargeG>("large", keys, cycles, worker_counts));
            std::fflush(stdout);
        }
        report(rows, cycles);
        return 0;
    }
    catch (const std::exception &error)
    {
        std::fprintf(stderr, "hgraph_distributed_perf: %s\n", error.what());
        return 1;
    }
}
