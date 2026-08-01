#include <hgraph/lib/std/std_operators.h>
#include <hgraph/lib/std/value_util.h>
#include <hgraph/runtime/inspector.h>
#include <hgraph/types/graph_wiring.h>
#include <hgraph/types/static_node.h>
#include <hgraph/types/subgraph_wiring.h>

#include <catch2/catch_test_macros.hpp>

#include <algorithm>
#include <optional>
#include <stdexcept>
#include <string_view>

namespace
{
    using namespace hgraph;

    struct InspectAddOne
    {
        static constexpr auto name = "inspect_add_one";

        static Port<TS<Int>> compose(Wiring &, Port<TS<Int>> value)
        {
            using namespace hgraph::stdlib::syntax;
            return (value + Int{1}).as<TS<Int>>();
        }
    };

    struct InspectionGraph
    {
        static constexpr auto name = "inspection_graph";

        static void compose(Wiring &w)
        {
            auto scalar = wire<stdlib::const_, TS<Int>>(w, Int{3});
            static_cast<void>(nested_<InspectAddOne>(w, scalar));

            auto dict = wire<stdlib::const_, TSD<Str, TS<Int>>>(
                w, stdlib::make_map<Str, Int>({
                       {Str{"a"}, Int{1}},
                       {Str{"b"}, Int{2}},
                       {Str{"c"}, Int{3}},
                   }));
            static_cast<void>(wire<stdlib::const_, TSS<Int>>(
                w, stdlib::make_set<Int>({Int{1}, Int{2}, Int{3}})));
            static_cast<void>(wire<stdlib::to_window>(w, scalar, Int{64}, Int{1}));
            static_cast<void>(wire<stdlib::map_>(w, fn<InspectAddOne>(), dict));
            static_cast<void>(wire<stdlib::mesh_>(w, fn<InspectAddOne>(), dict));
            static_cast<void>(wire<stdlib::reduce_>(w, fn<stdlib::add_>(), dict));

            auto ordered = wire<stdlib::const_, TSD<Int, TS<Int>>>(
                w, stdlib::make_map<Int, Int>({
                       {Int{0}, Int{1}},
                       {Int{1}, Int{2}},
                       {Int{2}, Int{3}},
                   }));
            auto zero = wire<stdlib::const_, TS<Int>>(w, Int{0});
            static_cast<void>(wire<stdlib::reduce_>(
                w, fn<stdlib::add_>(), ordered, zero, Bool{false}));

            auto key = wire<stdlib::const_, TS<Str>>(w, Str{"active"});
            static_cast<void>(wire<stdlib::switch_>(
                w, key,
                stdlib::switch_cases({
                    {Value{Str{"active"}}, fn<InspectAddOne>()},
                }),
                scalar));
        }
    };

    struct SnapshotCapture final : LifecycleObserver
    {
        explicit SnapshotCapture(Inspector &inspector_) : inspector(inspector_) {}

        void on_after_graph_evaluation(const GraphView &graph) override
        {
            if (graph.is_root()) { live = inspector.snapshot(); }
        }

        Inspector &inspector;
        std::optional<InspectionSnapshot> live{};
    };

    struct ActiveResetProbe final : LifecycleObserver
    {
        explicit ActiveResetProbe(Inspector &inspector_) : inspector(inspector_) {}

        void on_after_start_graph(const GraphView &graph) override
        {
            if (graph.is_root()) { rejected = throws_logic_error(); }
        }

        [[nodiscard]] bool throws_logic_error()
        {
            try
            {
                inspector.reset();
            }
            catch (const std::logic_error &)
            {
                return true;
            }
            return false;
        }

        Inspector &inspector;
        bool rejected{false};
    };

    [[nodiscard]] const InspectionEntry &entry_containing(
        const InspectionSnapshot &snapshot, std::string_view label)
    {
        const auto found = std::ranges::find_if(
            snapshot.entries, [&](const InspectionEntry &entry) {
                return entry.label.contains(label);
            });
        REQUIRE(found != snapshot.entries.end());
        return *found;
    }
}  // namespace

TEST_CASE("inspector: native snapshots own hierarchy, timings, schedules and storage")
{
    stdlib::register_standard_operators();
    Inspector inspector;
    SnapshotCapture capture{inspector};

    {
        GraphExecutorBuilder builder;
        builder.graph_builder(build_graph<InspectionGraph>())
            .add_lifecycle_observer(&inspector)
            .add_lifecycle_observer(&capture);
        GraphExecutorValue executor = builder.make_executor();
        executor.view().run();
    }

    REQUIRE(capture.live.has_value());
    const InspectionSnapshot &live = *capture.live;
    CHECK(live.graph_cycles == 1);
    CHECK(live.planned_bytes > 0);
    CHECK(live.dynamic_live_bytes > 0);
    CHECK(live.dynamic_reserved_bytes >= live.dynamic_live_bytes);
    CHECK(live.peak_dynamic_reserved_bytes >= live.dynamic_reserved_bytes);
    CHECK_FALSE(live.entries.empty());

    const InspectionEntry &root = entry_containing(live, "inspection_graph");
    CHECK(root.kind == InspectionEntityKind::Graph);
    CHECK(root.parent_id == 0);
    CHECK(root.started);
    CHECK(root.evaluation.count == 1);
    CHECK_FALSE(root.children.empty());
    CHECK_FALSE(root.schema_label.empty());

    const InspectionEntry &map = entry_containing(live, "map");
    CHECK(map.kind == InspectionEntityKind::Node);
    CHECK(map.node_kind == NodeKind::Nested);
    CHECK(map.storage.nested_graph_count == 3);
    CHECK(map.storage.nested_graph_capacity >= map.storage.nested_graph_count);
    CHECK(map.storage.dynamic_reserved_bytes > 0);

    const InspectionEntry &mesh = entry_containing(live, "mesh");
    CHECK(mesh.storage.nested_graph_count == 3);
    CHECK(mesh.storage.dynamic_reserved_bytes > 0);

    const InspectionEntry &switched = entry_containing(live, "switch");
    CHECK(switched.storage.nested_graph_count == 1);
    CHECK(switched.storage.nested_graph_capacity == 2);
    CHECK(switched.storage.dynamic_live_bytes > 0);
    CHECK(switched.storage.dynamic_reserved_bytes >=
          switched.storage.dynamic_live_bytes);

    const auto keyed_output_nodes = std::ranges::count_if(
        live.entries, [](const InspectionEntry &entry) {
            return entry.kind == InspectionEntityKind::Node &&
                   entry.storage.nested_graph_capacity == 0 &&
                   entry.storage.dynamic_reserved_bytes > 0;
        });
    CHECK(keyed_output_nodes >= 2);

    const InspectionEntry &window = entry_containing(live, "to_window");
    CHECK(window.storage.dynamic_live_bytes > 0);
    CHECK(window.storage.dynamic_reserved_bytes > window.storage.dynamic_live_bytes);

    for (const InspectionEntry &entry : live.entries)
    {
        if (entry.parent_id == 0) { continue; }
        const auto parent = std::ranges::find(
            live.entries, entry.parent_id, &InspectionEntry::id);
        REQUIRE(parent != live.entries.end());
        CHECK(std::ranges::find(parent->children, entry.id) !=
              parent->children.end());
    }

    const InspectionSnapshot stopped = inspector.snapshot();
    CHECK(stopped.entries.size() == live.entries.size());
    CHECK(stopped.dynamic_live_bytes == 0);
    CHECK(stopped.dynamic_reserved_bytes == 0);
    CHECK(stopped.peak_dynamic_reserved_bytes > 0);
    CHECK(std::ranges::all_of(stopped.entries, [](const InspectionEntry &entry) {
        return entry.stopped;
    }));

    const InspectionSnapshot repeated = inspector.snapshot();
    CHECK(repeated.entries.size() == stopped.entries.size());
    CHECK(repeated.peak_dynamic_reserved_bytes ==
          stopped.peak_dynamic_reserved_bytes);
}

TEST_CASE("inspector: copies share snapshots and reset clears owned history")
{
    stdlib::register_standard_operators();
    Inspector inspector;
    Inspector retained = inspector;

    GraphExecutorBuilder builder;
    builder.graph_builder(build_graph<InspectionGraph>())
        .add_lifecycle_observer(&inspector);
    GraphExecutorValue executor = builder.make_executor();
    executor.view().run();

    REQUIRE_FALSE(retained.snapshot().entries.empty());
    retained.reset();
    CHECK(inspector.snapshot().entries.empty());
}

TEST_CASE("inspector: reset rejects active graph state")
{
    stdlib::register_standard_operators();
    Inspector inspector;
    ActiveResetProbe reset_probe{inspector};

    GraphExecutorBuilder builder;
    builder.graph_builder(build_graph<InspectionGraph>())
        .add_lifecycle_observer(&inspector)
        .add_lifecycle_observer(&reset_probe);
    GraphExecutorValue executor = builder.make_executor();
    executor.view().run();

    CHECK(reset_probe.rejected);
    CHECK_FALSE(inspector.snapshot().entries.empty());
}
