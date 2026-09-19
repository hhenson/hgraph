// Save and restore cost of recovery through worker-hosted graphs (RFC 0039).
//
// Explicitly selected; the ordinary gates do not run timing work:
//   hgraph_unit_tests '[recovery-benchmark]'
//
// End to end, through the public wiring: a day that creates N keyed children with
// recordable state, saved at its completed boundary, and a quiet day restored from that
// image. Each cost is the difference between the same day with and without recovery
// configured, so wiring, child creation and -- for process hosting -- raising the workers
// cancel out and what is left is what recovery added.
//
// The bar is the plain ``map_`` component: the same state, saved and restored in one
// process with no transport. ``dmap_`` should cost a small multiple of it, and every row
// has to scale FLAT: the per-key cost at 8n is compared with the per-key cost at n, because
// a quadratic walk is invisible at the sizes unit tests use (CLAUDE.md, guardrail iv).

#include <hgraph/lib/std/component.h>
#include <hgraph/lib/std/std_operators.h>
#include <hgraph/lib/testing/eval_node.h>
#include <hgraph/runtime/checkpoint_codec.h>
#include <hgraph/runtime/component_checkpoint.h>
#include <hgraph/runtime/distributed_map.h>

#include "distributed_worker_recipes.h"

#include <catch2/catch_test_macros.hpp>

#include <algorithm>
#include <chrono>
#include <iomanip>
#include <iostream>
#include <map>
#include <optional>
#include <string>
#include <vector>

namespace
{
    using namespace hgraph;
    using namespace hgraph::distributed;
    using namespace hgraph::testing;
    using Keyed = TSD<Int, TS<Int>>;

    constexpr std::size_t repetitions = 5;
    constexpr Int         workers     = 4;

    /** The bar: the same per-key recordable state under a plain map_, in one process. */
    struct MapBody
    {
        static Port<Keyed> compose(Wiring &w, NamedPort<"ts", Keyed> ts)
        { return wire<stdlib::map_>(w, fn<hgraph_test::PreparedAccumulate>(), ts).as<Keyed>(); }
    };
    struct MapComponent
    {
        static constexpr auto component = "benchmark";
        static Port<Keyed>    compose(Wiring &w, Port<Keyed> ts) { return stdlib::component<MapBody>(w, component, ts); }
    };

    /** dmap_ as a MEMBER of the component: its workers are saved whole. */
    template <bool InProcess> struct MemberBody
    {
        static Port<Keyed> compose(Wiring &w, NamedPort<"ts", Keyed> ts)
        {
            return wire<dmap_impl<Int, Int, Int>>(w, ts, fn<hgraph_test::AccumulateG>(), workers, Bool{InProcess},
                                                 Str{HGRAPH_TEST_WORKER_PROGRAM})
                .template as<Keyed>();
        }
    };
    template <bool InProcess> struct MemberComponent
    {
        static constexpr auto component = "benchmark";
        static Port<Keyed>    compose(Wiring &w, Port<Keyed> ts)
        { return stdlib::component<MemberBody<InProcess>>(w, component, ts); }
    };

    /** The component INSIDE the dmap_ child: the dmap_ stands in for it, images are selected. */
    template <bool InProcess> struct Hosting
    {
        static constexpr auto component = hgraph_test::dmap_component_id;
        static Port<Keyed>    compose(Wiring &w, Port<Keyed> ts)
        {
            return wire_dmap<Int, Int, Int>(w, ts, fn<hgraph_test::PreparedHostedChild>(), workers, Bool{InProcess},
                                            Str{HGRAPH_TEST_WORKER_PROGRAM});
        }
    };

    Value burst(std::size_t keys)
    {
        std::map<Int, Int> modified;
        for (std::size_t key = 0; key < keys; ++key) { modified.emplace(static_cast<Int>(key), static_cast<Int>(key % 97)); }
        return static_node_detail::build_dict_delta<Int, TS<Int>>(modified, {});
    }

    EvalNodeRunOptions interval(std::size_t begin, std::size_t end)
    {
        return {.start_time = MIN_ST + MIN_TD * static_cast<Int>(begin), .end_time = MIN_ST + MIN_TD * static_cast<Int>(end)};
    }

    template <typename Action> double median_ms(Action &&action)
    {
        std::vector<double> timings;
        for (std::size_t repetition = 0; repetition < repetitions; ++repetition)
        {
            const auto start = std::chrono::steady_clock::now();
            action();
            timings.push_back(std::chrono::duration<double, std::milli>{std::chrono::steady_clock::now() - start}.count());
        }
        std::sort(timings.begin(), timings.end());
        return timings[timings.size() / 2];
    }

    struct Row
    {
        std::size_t keys{};
        double      save_ms{}, restore_ms{};
        std::size_t bytes{};
    };

    template <typename Graph> Row measure(std::size_t keys)
    {
        const std::vector<std::optional<Value>> first{burst(keys)};
        const std::vector<std::optional<Value>> quiet{std::nullopt};
        std::optional<ComponentCheckpoint>      completed;
        const auto day = [&](std::size_t begin, const std::vector<std::optional<Value>> &events, bool recover,
                             std::optional<ComponentCheckpoint> previous) {
            GlobalContext context;
            if (recover)
            {
                configure_component_recovery(context.state().view(), {
                    .component_id = Graph::component, .load = [previous] { return previous; },
                    .commit = [&](const auto &image) { completed = image; }});
            }
            (void)eval_node_with_options<Graph>(interval(begin, begin + 1), events);
        };
        Row row{.keys = keys};
        const double busy_plain = median_ms([&] { day(0, first, false, std::nullopt); });
        const double busy_saved = median_ms([&] { day(0, first, true, std::nullopt); });
        REQUIRE(completed);
        const auto image = *completed;
        std::string bytes;
        encode_component_checkpoint(image, bytes);
        row.bytes   = bytes.size();
        row.save_ms = busy_saved - busy_plain;
        // A quiet day: nothing is evaluated, so what recovery adds is the restore, and one
        // more save of the same state at the end of it.
        const double quiet_plain    = median_ms([&] { day(1, quiet, false, std::nullopt); });
        const double quiet_restored = median_ms([&] { day(1, quiet, true, image); });
        row.restore_ms = (quiet_restored - quiet_plain) - row.save_ms;
        return row;
    }

    template <typename Graph> std::vector<Row> sweep(const char *label, std::initializer_list<std::size_t> sizes)
    {
        std::vector<Row> rows;
        for (const auto keys : sizes)
        {
            rows.push_back(measure<Graph>(keys));
            const auto &row = rows.back();
            std::cout << std::fixed << std::setprecision(2) << "{\"variant\":\"" << label << "\",\"keys\":" << row.keys
                      << ",\"save_ms\":" << row.save_ms << ",\"restore_ms\":" << row.restore_ms
                      << ",\"save_us_per_key\":" << row.save_ms * 1000.0 / static_cast<double>(row.keys)
                      << ",\"restore_us_per_key\":" << row.restore_ms * 1000.0 / static_cast<double>(row.keys)
                      << ",\"bytes_per_key\":" << static_cast<double>(row.bytes) / static_cast<double>(row.keys) << "}"
                      << std::endl;
        }
        return rows;
    }

    // Flat means the per-key cost at 8n is not a multiple of the per-key cost at n. The bound
    // is loose on purpose -- allocator and cache effects are real -- and still far below the
    // 8x a quadratic walk shows. Costs too small to measure are not judged.
    void require_flat(const std::vector<Row> &rows, const char *label)
    {
        const auto per_key = [](double ms, std::size_t keys) { return ms * 1000.0 / static_cast<double>(keys); };
        const auto &small = rows.front();
        const auto &large = rows.back();
        INFO(label);
        if (small.save_ms > 2.0)
            CHECK(per_key(large.save_ms, large.keys) < 3.0 * per_key(small.save_ms, small.keys) + 1.0);
        if (small.restore_ms > 2.0)
            CHECK(per_key(large.restore_ms, large.keys) < 3.0 * per_key(small.restore_ms, small.keys) + 1.0);
    }
}  // namespace

TEST_CASE("recovery: save and restore cost through dmap_, against a plain map_ component", "[.][recovery-benchmark]")
{
    stdlib::register_standard_operators();
    hgraph_test::register_distributed_test_recipes();
    const auto sizes = {std::size_t{5000}, std::size_t{10000}, std::size_t{20000}, std::size_t{40000}};

    const auto plain = sweep<MapComponent>("map_ component (the bar)", sizes);
    require_flat(plain, "map_ component");
    require_flat(sweep<MemberComponent<true>>("dmap_ member, in process", sizes), "dmap_ member, in process");
    require_flat(sweep<Hosting<true>>("dmap_ hosting a component, in process", sizes), "dmap_ hosting, in process");
    require_flat(sweep<MemberComponent<false>>("dmap_ member, processes", sizes), "dmap_ member, processes");
    require_flat(sweep<Hosting<false>>("dmap_ hosting a component, processes", sizes), "dmap_ hosting, processes");
}
