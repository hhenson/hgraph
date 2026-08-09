#include <hgraph/lib/std/std_operators.h>
#include <hgraph/types/graph_wiring.h>
#include <hgraph/types/operator_dispatch.h>
#include <hgraph/types/static_node.h>
#include <hgraph/types/static_schema.h>
#include <hgraph/types/wiring_observer.h>
#include <hgraph/types/wired_fn.h>

#include <catch2/catch_test_macros.hpp>

#include <algorithm>
#include <array>
#include <string>
#include <utility>
#include <vector>

namespace
{
    using namespace hgraph;

    struct RecordingWiringObserver final : WiringObserver
    {
        struct ScopeExit
        {
            WiringScopeEvent event{};
            std::string      error{};
        };

        void on_enter_graph_wiring(const WiringScopeEvent &event) override
        {
            graph_entries.push_back(event);
        }

        void on_exit_graph_wiring(const WiringScopeEvent &event,
                                  std::string_view error) override
        {
            graph_exits.push_back({event, std::string{error}});
        }

        void on_enter_nested_graph_wiring(const WiringScopeEvent &event) override
        {
            nested_entries.push_back(event);
        }

        void on_exit_nested_graph_wiring(const WiringScopeEvent &event,
                                         std::string_view error) override
        {
            nested_exits.push_back({event, std::string{error}});
        }

        void on_enter_node_wiring(const WiringScopeEvent &event) override
        {
            node_entries.push_back(event);
        }

        void on_exit_node_wiring(const WiringScopeEvent &event,
                                 std::string_view error) override
        {
            node_exits.push_back({event, std::string{error}});
        }

        void on_overload_resolution(const WiringResolutionEvent &event) override
        {
            resolutions.push_back(event);
        }

        std::vector<WiringScopeEvent>      graph_entries{};
        std::vector<ScopeExit>             graph_exits{};
        std::vector<WiringScopeEvent>      nested_entries{};
        std::vector<ScopeExit>             nested_exits{};
        std::vector<WiringScopeEvent>      node_entries{};
        std::vector<ScopeExit>             node_exits{};
        std::vector<WiringResolutionEvent> resolutions{};
    };

    struct ObserverSource
    {
        static constexpr auto name = "observer_source";
        static constexpr bool schedule_on_start = true;
        static void eval(Out<TS<Int>> out) { out.set(Int{1}); }
    };

    struct observed_ : Operator<"observed", In<"ts", TsVar<"S">>, Out<TsVar<"S">>>
    {
    };

    struct ObservedInt
    {
        static constexpr auto name = "observed_int";
        static void eval(In<"ts", TS<Int>> ts, Out<TS<Int>> out)
        {
            out.set(ts.value());
        }
    };

    struct ObservedString
    {
        static constexpr auto name = "observed_string";
        static void eval(In<"ts", TS<Str>> ts, Out<TS<Str>> out)
        {
            out.set(ts.value());
        }
    };

    struct ObservedIntDuplicate
    {
        static constexpr auto name = "observed_int_duplicate";
        static void eval(In<"ts", TS<Int>> ts, Out<TS<Int>> out)
        {
            out.set(ts.value());
        }
    };

    struct ObserverNestedGraph
    {
        static constexpr auto name = "observer_nested";
        static Port<TS<Int>> compose(Wiring &wiring, Port<TS<Int>> input)
        {
            return wire<observed_, TS<Int>>(wiring, input);
        }
    };

    struct ObserverGraph
    {
        static constexpr auto name = "observer_graph";
        static void compose(Wiring &wiring)
        {
            wire<ObserverNestedGraph>(wiring, wire<ObserverSource>(wiring));
        }
    };

    struct ObserverMapSource
    {
        static constexpr auto name = "observer_map_source";
        static void eval(Out<TSD<Int, TS<Int>>>) {}
    };

    struct ObserverMapGraph
    {
        static constexpr auto name = "observer_map_graph";
        static void compose(Wiring &wiring)
        {
            auto source = wire<ObserverMapSource>(wiring);
            wire<stdlib::map_>(wiring, fn<ObserverNestedGraph>(), source);
        }
    };

    const WiringScopeEvent &node_event(const RecordingWiringObserver &observer,
                                       std::string_view label)
    {
        const auto found = std::ranges::find(observer.node_entries, label,
                                             &WiringScopeEvent::label);
        REQUIRE(found != observer.node_entries.end());
        return *found;
    }
}

TEST_CASE("wiring observers expose stable graph, node, and overload records")
{
    register_overload<observed_, ObservedString>();
    register_overload<observed_, ObservedInt>();

    RecordingWiringObserver observer;
    std::array<WiringObserver *, 1> observers{&observer};
    const GraphBuilder graph = build_graph_with_observers<ObserverGraph>(observers);

    REQUIRE(observer.graph_entries.size() == 1);
    CHECK(observer.graph_entries.front().path ==
          std::vector<std::string>{"observer_graph"});
    REQUIRE(observer.graph_exits.size() == 1);
    CHECK(observer.graph_exits.front().error.empty());

    REQUIRE(observer.nested_entries.size() == 1);
    CHECK(observer.nested_entries.front().path ==
          std::vector<std::string>{"observer_graph", "observer_nested"});

    const WiringScopeEvent &source = node_event(observer, "observer_source");
    CHECK(source.path ==
          std::vector<std::string>{"observer_graph", "observer_source"});
    CHECK(source.input_types.empty());
    CHECK(source.output_type.time_series_type() == ts_type<TS<Int>>());

    const WiringScopeEvent &selected = node_event(observer, "observed_int");
    REQUIRE(selected.input_types.size() == 1);
    CHECK(selected.input_types.front().time_series_type() == ts_type<TS<Int>>());
    CHECK(selected.output_type.time_series_type() == ts_type<TS<Int>>());
    CHECK(selected.path == std::vector<std::string>{
                               "observer_graph", "observer_nested", "observed_int"});

    REQUIRE(observer.resolutions.size() == 1);
    const WiringResolutionEvent &resolution = observer.resolutions.front();
    CHECK(resolution.path ==
          std::vector<std::string>{"observer_graph", "observer_nested"});
    REQUIRE(resolution.argument_types.size() == 1);
    CHECK(resolution.argument_types.front().time_series_type() == ts_type<TS<Int>>());
    REQUIRE(resolution.selected.has_value());
    CHECK(resolution.selected->label.starts_with("observed_int"));
    CHECK(resolution.selected->source == WiringCandidateSource::Cpp);
    REQUIRE(resolution.rejected.size() == 1);
    CHECK(resolution.rejected.front().label.starts_with("observed_string"));
    CHECK_FALSE(resolution.rejected.front().rejection_reason.empty());

    // Records own their text and retain only registry-interned type handles, so
    // they remain valid after the Wiring itself has been consumed.
    CHECK(std::string{observer.node_entries.back().output_type.name()} ==
          std::string{ts_type<TS<Int>>()->name()});
    CHECK(graph.node_count() == 2);
}

TEST_CASE("wiring observers report overload and enclosing graph failures")
{
    register_overload<observed_, ObservedString>();

    RecordingWiringObserver observer;
    std::array<WiringObserver *, 1> observers{&observer};
    REQUIRE_THROWS_AS(build_graph_with_observers<ObserverGraph>(observers),
                      OperatorResolutionError);

    REQUIRE(observer.resolutions.size() == 1);
    const WiringResolutionEvent &resolution = observer.resolutions.front();
    CHECK_FALSE(resolution.selected.has_value());
    REQUIRE(resolution.rejected.size() == 1);
    CHECK(resolution.rejected.front().label.starts_with("observed_string"));
    CHECK_FALSE(resolution.error.empty());

    REQUIRE(observer.nested_exits.size() == 1);
    CHECK_FALSE(observer.nested_exits.front().error.empty());
    REQUIRE(observer.graph_exits.size() == 1);
    CHECK_FALSE(observer.graph_exits.front().error.empty());
}

TEST_CASE("the native wiring tracer filters complete graph paths")
{
    register_overload<observed_, ObservedString>();
    register_overload<observed_, ObservedInt>();

    WiringTracer tracer{"observer_nested"};
    std::array<WiringObserver *, 1> observers{&tracer};
    static_cast<void>(build_graph_with_observers<ObserverGraph>(observers));

    REQUIRE_FALSE(tracer.lines().empty());
    CHECK(std::ranges::none_of(tracer.lines(), [](const std::string &line) {
        return line.contains("observer_source");
    }));
    CHECK(std::ranges::any_of(tracer.lines(), [](const std::string &line) {
        return line.contains("Resolved operator observed") &&
               line.contains("observed_int");
    }));
}

TEST_CASE("higher-order child compilation inherits wiring observers and paths")
{
    stdlib::register_standard_operators();
    register_overload<observed_, ObservedString>();
    register_overload<observed_, ObservedInt>();

    RecordingWiringObserver observer;
    std::array<WiringObserver *, 1> observers{&observer};
    static_cast<void>(build_graph_with_observers<ObserverMapGraph>(observers));

    REQUIRE(observer.nested_entries.size() == 2);
    const auto child_scope = std::ranges::find(
        observer.nested_entries, "observer_nested", &WiringScopeEvent::label);
    REQUIRE(child_scope != observer.nested_entries.end());
    CHECK(child_scope->path == std::vector<std::string>{
                                   "observer_map_graph", "map_impl", "observer_nested"});
    const WiringScopeEvent &child_node = node_event(observer, "observed_int");
    CHECK(child_node.path == std::vector<std::string>{
                                 "observer_map_graph", "map_impl", "observer_nested",
                                 "observed_int"});
    CHECK(std::ranges::any_of(
        observer.resolutions, [](const WiringResolutionEvent &event) {
            return event.operator_name == "observed" &&
                   event.path == std::vector<std::string>{
                                     "observer_map_graph", "map_impl", "observer_nested"};
        }));
}

TEST_CASE("wiring observers retain every ambiguous candidate")
{
    register_overload<observed_, ObservedInt>();
    register_overload<observed_, ObservedIntDuplicate>();

    RecordingWiringObserver observer;
    std::array<WiringObserver *, 1> observers{&observer};
    REQUIRE_THROWS_AS(build_graph_with_observers<ObserverGraph>(observers),
                      OperatorResolutionError);

    REQUIRE(observer.resolutions.size() == 1);
    const WiringResolutionEvent &resolution = observer.resolutions.front();
    CHECK_FALSE(resolution.selected.has_value());
    CHECK(resolution.rejected.empty());
    REQUIRE(resolution.ambiguous.size() == 2);
    CHECK(resolution.ambiguous[0].rank == resolution.ambiguous[1].rank);
    CHECK(resolution.ambiguous[0].label.starts_with("observed_int"));
    CHECK(resolution.ambiguous[1].label.starts_with("observed_int_duplicate"));
}
