#include <hgraph/lib/std/std_operators.h>
#include <hgraph/lib/std/value_util.h>
#include <hgraph/runtime/graph_diagnostics.h>
#include <hgraph/types/graph_wiring.h>
#include <hgraph/types/static_node.h>
#include <hgraph/types/subgraph_wiring.h>

#include <arrow/api.h>

#include <catch2/catch_test_macros.hpp>

#include <algorithm>
#include <cstdint>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string_view>
#include <vector>

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

    struct DiagnosticsGraph
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

    struct DiagnosticsRefSource
    {
        static constexpr auto name = "diagnostics_ref_source";
        static constexpr bool schedule_on_start = true;

        static void eval(Out<TS<Int>> out) { out.set(Int{42}); }
    };

    struct DiagnosticsRefPublisher
    {
        static constexpr auto name = "diagnostics_ref_publisher";
        static constexpr bool schedule_on_start = true;

        static void eval(In<"value", TS<Int>> value,
                         Out<REF<TS<Int>>> out)
        {
            out.set(value.reference());
        }
    };

    struct DiagnosticsRefGraph
    {
        static constexpr auto name = "diagnostics_ref_graph";

        static void compose(Wiring &w)
        {
            auto value = wire<DiagnosticsRefSource>(w);
            static_cast<void>(wire<DiagnosticsRefPublisher>(w, value));
        }
    };

    struct DiagnosticsNestedRefPublisher
    {
        static constexpr auto name = "diagnostics_nested_ref_publisher";

        static void eval(In<"value", TS<Int>> value,
                         Out<TSD<Str, REF<TS<Int>>>> out)
        {
            out.set(Str{"selected"}, value.reference());
        }
    };

    struct DiagnosticsNestedRefGraph
    {
        static constexpr auto name = "diagnostics_nested_ref_graph";

        static void compose(Wiring &w)
        {
            auto value = wire<DiagnosticsRefSource>(w);
            static_cast<void>(wire<DiagnosticsNestedRefPublisher>(w, value));
        }
    };

    using DiagnosticsRefBundle = TSB<"DiagnosticsRefBundle",
                                     Field<"a", TS<Int>>,
                                     Field<"b", TS<Str>>>;

    struct DiagnosticsPartialBundleSource
    {
        static constexpr auto name = "diagnostics_partial_bundle_source";
        static constexpr bool schedule_on_start = true;

        static void eval(Out<DiagnosticsRefBundle> out)
        {
            out.field<"a">().set(Int{42});
        }
    };

    struct DiagnosticsBundleRefPublisher
    {
        static constexpr auto name = "diagnostics_bundle_ref_publisher";

        static void eval(In<"value", REF<DiagnosticsRefBundle>> value,
                         Out<REF<DiagnosticsRefBundle>> out)
        {
            out.set(value.value());
        }
    };

    struct DiagnosticsBundleRefGraph
    {
        static constexpr auto name = "diagnostics_bundle_ref_graph";

        static void compose(Wiring &w)
        {
            auto value = wire<DiagnosticsPartialBundleSource>(w);
            static_cast<void>(wire<DiagnosticsBundleRefPublisher>(w, value));
        }
    };

    void require_arrow(const arrow::Status &status)
    {
        if (!status.ok()) { throw std::runtime_error(status.ToString()); }
    }

    [[nodiscard]] Frame diagnostics_frame()
    {
        arrow::Int64Builder builder;
        require_arrow(builder.AppendValues(std::vector<std::int64_t>{1, 2}));
        std::shared_ptr<arrow::Array> values;
        require_arrow(builder.Finish(&values));
        return Frame{arrow::Table::Make(
            arrow::schema({arrow::field("value", arrow::int64())}),
            {std::move(values)})};
    }

    struct DiagnosticsFrameSource
    {
        static constexpr auto name = "diagnostics_frame_source";
        static constexpr bool schedule_on_start = true;

        static void eval(Out<TS<Frame>> out) { out.set(diagnostics_frame()); }
    };

    struct DiagnosticsFrameGraph
    {
        static constexpr auto name = "diagnostics_frame_graph";

        static void compose(Wiring &w)
        {
            static_cast<void>(wire<DiagnosticsFrameSource>(w));
        }
    };

    struct SnapshotCapture final : LifecycleObserver
    {
        explicit SnapshotCapture(GraphDiagnostics &diagnostics_) : diagnostics(diagnostics_) {}

        void on_after_graph_evaluation(const GraphView &graph) override
        {
            if (graph.is_root()) { live = diagnostics.snapshot(); }
        }

        GraphDiagnostics &diagnostics;
        std::optional<GraphDiagnosticsSnapshot> live{};
    };

    struct ActiveResetProbe final : LifecycleObserver
    {
        explicit ActiveResetProbe(GraphDiagnostics &diagnostics_) : diagnostics(diagnostics_) {}

        void on_after_start_graph(const GraphView &graph) override
        {
            if (graph.is_root()) { rejected = throws_logic_error(); }
        }

        [[nodiscard]] bool throws_logic_error()
        {
            try
            {
                diagnostics.reset();
            }
            catch (const std::logic_error &)
            {
                return true;
            }
            return false;
        }

        GraphDiagnostics &diagnostics;
        bool rejected{false};
    };

    [[nodiscard]] const GraphDiagnosticEntry &entry_containing(
        const GraphDiagnosticsSnapshot &snapshot, std::string_view label)
    {
        const auto found = std::ranges::find_if(
            snapshot.entries, [&](const GraphDiagnosticEntry &entry) {
                return entry.label.contains(label);
            });
        REQUIRE(found != snapshot.entries.end());
        return *found;
    }
}  // namespace

TEST_CASE("diagnostics: native snapshots own hierarchy, timings, schedules and storage")
{
    stdlib::register_standard_operators();
    GraphDiagnostics diagnostics;
    SnapshotCapture capture{diagnostics};

    {
        GraphExecutorBuilder builder;
        builder.graph_builder(build_graph<DiagnosticsGraph>())
            .add_lifecycle_observer(&diagnostics)
            .add_lifecycle_observer(&capture);
        GraphExecutorValue executor = builder.make_executor();
        executor.view().run();
    }

    REQUIRE(capture.live.has_value());
    const GraphDiagnosticsSnapshot &live = *capture.live;
    CHECK(live.graph_cycles == 1);
    CHECK(live.planned_bytes > 0);
    CHECK(live.dynamic_live_bytes > 0);
    CHECK(live.dynamic_reserved_bytes >= live.dynamic_live_bytes);
    CHECK(live.peak_dynamic_reserved_bytes >= live.dynamic_reserved_bytes);
    CHECK_FALSE(live.entries.empty());

    const GraphDiagnosticEntry &root = entry_containing(live, "inspection_graph");
    CHECK(root.kind == GraphDiagnosticEntityKind::Graph);
    CHECK(root.parent_id == 0);
    CHECK(root.started);
    CHECK(root.evaluation.count == 1);
    CHECK_FALSE(root.children.empty());
    CHECK_FALSE(root.schema_label.empty());

    const GraphDiagnosticEntry &map = entry_containing(live, "map");
    CHECK(map.kind == GraphDiagnosticEntityKind::Node);
    CHECK(map.node_kind == NodeKind::Nested);
    CHECK(map.storage.nested_graph_count == 3);
    CHECK(map.storage.nested_graph_capacity >= map.storage.nested_graph_count);
    CHECK(map.storage.dynamic_reserved_bytes > 0);

    const GraphDiagnosticEntry &mesh = entry_containing(live, "mesh");
    CHECK(mesh.storage.nested_graph_count == 3);
    CHECK(mesh.storage.dynamic_reserved_bytes > 0);

    const GraphDiagnosticEntry &switched = entry_containing(live, "switch");
    CHECK(switched.storage.nested_graph_count == 1);
    CHECK(switched.storage.nested_graph_capacity == 2);
    CHECK(switched.storage.dynamic_live_bytes > 0);
    CHECK(switched.storage.dynamic_reserved_bytes >=
          switched.storage.dynamic_live_bytes);

    const auto keyed_output_nodes = std::ranges::count_if(
        live.entries, [](const GraphDiagnosticEntry &entry) {
            return entry.kind == GraphDiagnosticEntityKind::Node &&
                   entry.storage.nested_graph_capacity == 0 &&
                   entry.storage.dynamic_reserved_bytes > 0;
        });
    CHECK(keyed_output_nodes >= 2);

    const GraphDiagnosticEntry &window = entry_containing(live, "to_window");
    CHECK(window.storage.dynamic_live_bytes > 0);
    CHECK(window.storage.dynamic_reserved_bytes > window.storage.dynamic_live_bytes);

    for (const GraphDiagnosticEntry &entry : live.entries)
    {
        if (entry.parent_id == 0) { continue; }
        const auto parent = std::ranges::find(
            live.entries, entry.parent_id, &GraphDiagnosticEntry::id);
        REQUIRE(parent != live.entries.end());
        CHECK(std::ranges::find(parent->children, entry.id) !=
              parent->children.end());
    }

    const GraphDiagnosticsSnapshot stopped = diagnostics.snapshot();
    CHECK(stopped.entries.size() == live.entries.size());
    CHECK(stopped.dynamic_live_bytes == 0);
    CHECK(stopped.dynamic_reserved_bytes == 0);
    CHECK(stopped.peak_dynamic_reserved_bytes > 0);
    CHECK(std::ranges::all_of(stopped.entries, [](const GraphDiagnosticEntry &entry) {
        return entry.stopped;
    }));

    const GraphDiagnosticsSnapshot repeated = diagnostics.snapshot();
    CHECK(repeated.entries.size() == stopped.entries.size());
    CHECK(repeated.peak_dynamic_reserved_bytes ==
          stopped.peak_dynamic_reserved_bytes);
}

TEST_CASE("diagnostics: copies share snapshots and reset clears owned history")
{
    stdlib::register_standard_operators();
    GraphDiagnostics diagnostics;
    GraphDiagnostics retained = diagnostics;

    GraphExecutorBuilder builder;
    builder.graph_builder(build_graph<DiagnosticsGraph>())
        .add_lifecycle_observer(&diagnostics);
    GraphExecutorValue executor = builder.make_executor();
    executor.view().run();

    REQUIRE_FALSE(retained.snapshot().entries.empty());
    retained.reset();
    CHECK(diagnostics.snapshot().entries.empty());
}

TEST_CASE("diagnostics: reset rejects active graph state")
{
    stdlib::register_standard_operators();
    GraphDiagnostics diagnostics;
    ActiveResetProbe reset_probe{diagnostics};

    GraphExecutorBuilder builder;
    builder.graph_builder(build_graph<DiagnosticsGraph>())
        .add_lifecycle_observer(&diagnostics)
        .add_lifecycle_observer(&reset_probe);
    GraphExecutorValue executor = builder.make_executor();
    executor.view().run();

    CHECK(reset_probe.rejected);
    CHECK_FALSE(diagnostics.snapshot().entries.empty());
}

TEST_CASE("diagnostics: endpoint value capture is explicit and owned")
{
    stdlib::register_standard_operators();
    GraphDiagnostics diagnostics{GraphDiagnosticsOptions{
        .recent_window = 4,
        .capture_values = true,
    }};

    GraphExecutorBuilder builder;
    builder.graph_builder(build_graph<DiagnosticsGraph>())
        .add_lifecycle_observer(&diagnostics);
    GraphExecutorValue executor = builder.make_executor();
    executor.view().run();

    const GraphDiagnosticsSnapshot snapshot = diagnostics.snapshot();
    const auto rendered = std::ranges::find_if(
        snapshot.entries, [](const GraphDiagnosticEntry &entry) {
            return entry.kind == GraphDiagnosticEntityKind::Node &&
                   entry.output.available && entry.output.valid &&
                   !entry.output.json.empty();
        });
    REQUIRE(rendered != snapshot.entries.end());
    CHECK(rendered->output.error.empty());
    CHECK_FALSE(rendered->output.schema_label.empty());
}

TEST_CASE("diagnostics: reference values render the referenced output")
{
    GraphDiagnostics diagnostics{GraphDiagnosticsOptions{
        .recent_window = 4,
        .capture_values = true,
    }};

    GraphExecutorBuilder builder;
    builder.graph_builder(build_graph<DiagnosticsRefGraph>())
        .add_lifecycle_observer(&diagnostics);
    GraphExecutorValue executor = builder.make_executor();
    executor.view().run();

    const GraphDiagnosticsSnapshot snapshot = diagnostics.snapshot();
    const GraphDiagnosticEntry &publisher = entry_containing(
        snapshot, "diagnostics_ref_publisher");
    const GraphDiagnosticEntry &source = entry_containing(
        snapshot, "diagnostics_ref_source");
    CHECK(publisher.output.available);
    CHECK(publisher.output.valid);
    CHECK(publisher.output.error.empty());
    CHECK(publisher.output.json == "42");
    CHECK(publisher.output.target_node_ids ==
          std::vector<std::uint64_t>{source.id});
}

TEST_CASE("diagnostics: reference values nested in containers render their targets")
{
    GraphDiagnostics diagnostics{GraphDiagnosticsOptions{
        .recent_window = 4,
        .capture_values = true,
    }};

    GraphExecutorBuilder builder;
    builder.graph_builder(build_graph<DiagnosticsNestedRefGraph>())
        .add_lifecycle_observer(&diagnostics);
    GraphExecutorValue executor = builder.make_executor();
    executor.view().run();

    const GraphDiagnosticsSnapshot snapshot = diagnostics.snapshot();
    const GraphDiagnosticEntry &publisher = entry_containing(
        snapshot, "diagnostics_nested_ref_publisher");
    const GraphDiagnosticEntry &source = entry_containing(
        snapshot, "diagnostics_ref_source");
    CHECK(publisher.output.available);
    CHECK(publisher.output.valid);
    CHECK(publisher.output.error.empty());
    CHECK(publisher.output.json == R"({"selected":42})");
    CHECK(publisher.output.target_node_ids ==
          std::vector<std::uint64_t>{source.id});
}

TEST_CASE("diagnostics: partial composite references preserve invalid fields")
{
    GraphDiagnostics diagnostics{GraphDiagnosticsOptions{
        .recent_window = 4,
        .capture_values = true,
    }};

    GraphExecutorBuilder builder;
    builder.graph_builder(build_graph<DiagnosticsBundleRefGraph>())
        .add_lifecycle_observer(&diagnostics);
    GraphExecutorValue executor = builder.make_executor();
    executor.view().run();

    const GraphDiagnosticsSnapshot snapshot = diagnostics.snapshot();
    const GraphDiagnosticEntry &publisher = entry_containing(
        snapshot, "diagnostics_bundle_ref_publisher");
    CHECK(publisher.output.available);
    CHECK(publisher.output.valid);
    CHECK(publisher.output.error.empty());
    CHECK(publisher.output.json == R"({"a":42,"b":null})");
}

TEST_CASE("diagnostics: frame capture retains an owned immutable table handle")
{
    GraphDiagnostics diagnostics{GraphDiagnosticsOptions{
        .recent_window = 4,
        .capture_values = true,
    }};

    GraphExecutorBuilder builder;
    builder.graph_builder(build_graph<DiagnosticsFrameGraph>())
        .add_lifecycle_observer(&diagnostics);
    GraphExecutorValue executor = builder.make_executor();
    executor.view().run();

    const GraphDiagnosticsSnapshot snapshot = diagnostics.snapshot();
    const GraphDiagnosticEntry &source = entry_containing(
        snapshot, "diagnostics_frame_source");
    CHECK(source.output.available);
    CHECK(source.output.valid);
    CHECK(source.output.error.empty());
    CHECK(source.output.json == R"("frame[2 x 1]")");
    REQUIRE(source.output.frame.has_value());
    CHECK(source.output.frame.table->num_rows() == 2);
    CHECK(source.output.frame.table->num_columns() == 1);
}
