#include <hgraph/lib/testing/check_output.h>
#include <hgraph/lib/testing/eval_node.h>
#include <hgraph/lib/testing/record_replay.h>
#include <hgraph/runtime/runtime.h>
#include <hgraph/types/adaptor_wiring.h>
#include <hgraph/types/graph_wiring.h>
#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/static_node.h>

#include <catch2/catch_test_macros.hpp>

#include <cstdint>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace
{
    using namespace hgraph;

    std::string node_name(const GraphBuilder &builder, std::size_t index)
    {
        const NodeBuilder &node = builder.nodes().at(index);
        if (!node.label().empty()) { return std::string{node.label()}; }
        const auto *meta = node.type().schema();
        return meta != nullptr && meta->display_name != nullptr ? std::string{meta->display_name} : std::string{};
    }

    std::size_t find_node(const GraphBuilder &builder, std::string_view needle)
    {
        for (std::size_t i = 0; i < builder.node_count(); ++i)
        {
            if (node_name(builder, i).find(needle) != std::string::npos) { return i; }
        }
        INFO("missing node containing: " << needle);
        REQUIRE(false);
        return static_cast<std::size_t>(-1);
    }

    struct OneTickSource
    {
        static constexpr auto name = "one_tick_source";
        static constexpr bool schedule_on_start = true;

        static void eval(Out<TS<Int>> out) { out.set(Int{41}); }
    };

    struct EchoNode
    {
        static constexpr auto name = "echo_node";

        static void eval(In<"ts", TS<Int>> ts, Out<TS<Int>> out) { out.set(ts.value()); }
    };

    struct LoopbackAdaptor : adaptor::interface
    {
        static constexpr std::string_view name{"loopback"};
        using input_schema = TS<Int>;
        using output_schema = TS<Int>;
    };

    struct LoopbackAdaptorImpl
    {
        static void compose(Wiring &w)
        {
            auto input = adaptor::from_graph<LoopbackAdaptor>(w);
            auto output = wire<EchoNode>(w, input);
            adaptor::to_graph<LoopbackAdaptor>(w, output);
        }
    };

    struct NamedInputAdaptor : adaptor::interface
    {
        static constexpr std::string_view name{"named_input"};
        using input_schema = TS<Int>;
        using output_schema = TS<Int>;
    };

    struct NamedInputAdaptorImpl
    {
        static void compose(Wiring &w, Scalar<"path", Str> path)
        {
            const auto custom = adaptor::path(path.value());
            auto incoming = adaptor::from_graph<NamedInputAdaptor>(w, custom);
            auto output = wire<EchoNode>(w, incoming);
            adaptor::to_graph<NamedInputAdaptor>(w, custom, output);
        }
    };

    struct FiveSource
    {
        static constexpr auto name = "five_source";
        static constexpr bool schedule_on_start = true;

        static void eval(Out<TS<Int>> out) { out.set(Int{5}); }
    };

    struct PathSource
    {
        static constexpr auto name = "path_source";
        static constexpr bool schedule_on_start = true;

        static void eval(Scalar<"path", Str> path, Out<TS<Int>> out)
        {
            out.set(path.value().find("secondary") != std::string::npos ? Int{22} : Int{11});
        }
    };

    struct SourceOnlyAdaptor : adaptor::interface
    {
        static constexpr std::string_view name{"source_only"};
        using output_schema = TS<Int>;
    };

    struct SourceOnlyAdaptorImpl
    {
        static void compose(Wiring &w)
        {
            auto output = wire<FiveSource>(w);
            adaptor::to_graph<SourceOnlyAdaptor>(w, output);
        }
    };

    struct TypedSourceAdaptor : adaptor::interface
    {
        static constexpr std::string_view name{"typed_source"};
        using output_schema = TS<Int>;
    };

    struct TypedSourceAdaptorImpl
    {
        static void compose(Wiring &w, Scalar<"path", Str> path)
        {
            const auto custom = adaptor::path(path.value());
            auto output = wire<PathSource>(w, arg<"path">(path.value()));
            adaptor::to_graph<TypedSourceAdaptor>(w, custom, output);
        }
    };

    struct GenericEchoAdaptor : adaptor::interface
    {
        static constexpr std::string_view name{"generic_echo"};
        using input_schema = TS<ScalarVar<"T">>;
        using output_schema = TS<ScalarVar<"T">>;
    };

    struct GenericEchoAdaptorImpl
    {
        static void compose(Wiring &w, Scalar<"path", Str> path)
        {
            const auto custom = adaptor::path(path.value());
            auto input = adaptor::from_graph<GenericEchoAdaptor>(w, custom);
            adaptor::to_graph<GenericEchoAdaptor>(w, custom, input);
        }
    };

    struct MissingOutputAdaptorImpl
    {
        static void compose(Wiring &w)
        {
            (void)adaptor::from_graph<LoopbackAdaptor>(w);
        }
    };

    struct SinkOnlyAdaptor : adaptor::interface
    {
        static constexpr std::string_view name{"sink_only"};
        using input_schema = TS<Int>;
    };

    struct SinkOnlyAdaptorImpl
    {
        static void compose(Wiring &w)
        {
            auto input = adaptor::from_graph<SinkOnlyAdaptor>(w);
            wire<stdlib::dense_record_impl>(w, input, std::string{"sink_out"});
        }
    };

    struct NamedSinkAdaptor : adaptor::interface
    {
        static constexpr std::string_view name{"named_sink"};
        using input_schema = TS<Int>;
    };

    struct NamedSinkAdaptorImpl
    {
        static void compose(Wiring &w, Scalar<"path", Str> path)
        {
            const auto custom = adaptor::path(path.value());
            auto input = adaptor::from_graph<NamedSinkAdaptor>(w, custom);
            wire<stdlib::dense_record_impl>(w, input, std::string{"named_sink_out"});
        }
    };

    struct NamedSinkGraph
    {
        static constexpr auto name = "named_sink_graph";

        static void compose(Wiring &w)
        {
            const auto custom = adaptor::path("named_custom");
            adaptor::register_adaptor<NamedSinkAdaptor, NamedSinkAdaptorImpl>(w, custom);
            auto input = wire<OneTickSource>(w);
            wire<NamedSinkAdaptor>(w, custom, input);
        }
    };

    struct MultiInAdaptor : adaptor::interface
    {
        static constexpr std::string_view name{"multi_in"};
        using input_schema = TS<Int>;
    };

    struct MultiOutAdaptor : adaptor::interface
    {
        static constexpr std::string_view name{"multi_out"};
        using output_schema = TS<Int>;
    };

    struct MultiAdaptorImpl
    {
        static void compose(Wiring &w, Scalar<"path", Str> path)
        {
            const auto custom = adaptor::path(path.value());
            auto input = adaptor::from_graph<MultiInAdaptor>(w, custom);
            auto output = wire<EchoNode>(w, input);
            adaptor::to_graph<MultiOutAdaptor>(w, custom, output);
        }
    };

    struct MultiAdaptorGraph
    {
        [[maybe_unused]] static constexpr auto name = "multi_adaptor_graph";

        static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> input)
        {
            const auto custom = adaptor::path("multi");
            adaptor::register_adaptors<MultiAdaptorImpl, MultiInAdaptor, MultiOutAdaptor>(w, custom);
            wire<MultiInAdaptor>(w, custom, input);
            return wire<MultiOutAdaptor>(w, custom);
        }
    };

    struct LoopbackGraph
    {
        static constexpr auto name = "loopback_graph";

        static Port<TS<Int>> compose(Wiring &w)
        {
            adaptor::register_adaptor<LoopbackAdaptor, LoopbackAdaptorImpl>(w);
            auto input = wire<OneTickSource>(w);
            return wire<LoopbackAdaptor>(w, input);
        }
    };

    struct AutomaticLoopbackAdaptorImpl
    {
        static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> input)
        {
            return wire<EchoNode>(w, input);
        }
    };

    struct AutomaticSourceAdaptorImpl
    {
        static Port<TS<Int>> compose(Wiring &w)
        {
            return wire<FiveSource>(w);
        }
    };

    struct AutomaticSinkAdaptorImpl
    {
        static void compose(Wiring &w, Port<TS<Int>> input)
        {
            wire<stdlib::dense_record_impl>(w, input, std::string{"automatic_sink_out"});
        }
    };

    struct AutomaticLoopbackGraph
    {
        static Port<TS<Int>> compose(Wiring &w)
        {
            adaptor::register_automatic_adaptor<
                LoopbackAdaptor, AutomaticLoopbackAdaptorImpl>(w);
            return wire<LoopbackAdaptor>(w, wire<OneTickSource>(w));
        }
    };

    struct AutomaticSourceGraph
    {
        static Port<TS<Int>> compose(Wiring &w)
        {
            adaptor::register_automatic_adaptor<
                SourceOnlyAdaptor, AutomaticSourceAdaptorImpl>(w);
            return wire<SourceOnlyAdaptor>(w);
        }
    };

    struct AutomaticSinkGraph
    {
        static Port<TS<Int>> compose(Wiring &w)
        {
            adaptor::register_automatic_adaptor<
                SinkOnlyAdaptor, AutomaticSinkAdaptorImpl>(w);
            auto input = wire<OneTickSource>(w);
            wire<SinkOnlyAdaptor>(w, input);
            return input;
        }
    };

    struct SecondLoopbackAdaptor : adaptor::interface
    {
        static constexpr std::string_view name{"second_loopback"};
        using input_schema = TS<Int>;
        using output_schema = TS<Int>;
    };

    struct SecondLoopbackAdaptorImpl
    {
        static void compose(Wiring &w)
        {
            auto input = adaptor::from_graph<SecondLoopbackAdaptor>(w);
            auto output = wire<EchoNode>(w, input);
            adaptor::to_graph<SecondLoopbackAdaptor>(w, output);
        }
    };

    // Two adaptors in series: the value crosses BOTH boundary relays. Adaptors
    // are rank-correct (captures rank before their paired sources, re-ranked
    // by Wiring::finish once all captures are known), so the whole chain
    // settles in a single engine cycle — no next-cycle workaround anywhere.
    struct ChainedAdaptorGraph
    {
        [[maybe_unused]] static constexpr auto name = "chained_adaptor_graph";

        static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> input)
        {
            adaptor::register_adaptor<LoopbackAdaptor, LoopbackAdaptorImpl>(w);
            adaptor::register_adaptor<SecondLoopbackAdaptor, SecondLoopbackAdaptorImpl>(w);
            auto first = wire<LoopbackAdaptor>(w, input);
            return wire<SecondLoopbackAdaptor>(w, first);
        }
    };

    struct ExplicitPathGraph
    {
        [[maybe_unused]] static constexpr auto name = "explicit_path_graph";

        static Port<TS<Int>> compose(Wiring &w)
        {
            const auto custom = adaptor::path("custom");
            adaptor::register_adaptor<NamedInputAdaptor, NamedInputAdaptorImpl>(w, custom);
            auto input = wire<OneTickSource>(w);
            return wire<NamedInputAdaptor>(w, custom, input);
        }
    };

    struct SourceOnlyGraph
    {
        [[maybe_unused]] static constexpr auto name = "source_only_graph";

        static Port<TS<Int>> compose(Wiring &w)
        {
            adaptor::register_adaptor<SourceOnlyAdaptor, SourceOnlyAdaptorImpl>(w);
            return wire<SourceOnlyAdaptor>(w);
        }
    };

    struct TypedPathAdaptorGraph
    {
        [[maybe_unused]] static constexpr auto name = "typed_path_adaptor_graph";

        static Port<TS<Int>> compose(Wiring &w, Scalar<"side", Str> side)
        {
            adaptor::register_adaptor<TypedSourceAdaptor, TypedSourceAdaptorImpl>(
                w, adaptor::path("typed", arg<"side">(Str{"primary"})));
            adaptor::register_adaptor<TypedSourceAdaptor, TypedSourceAdaptorImpl>(
                w, adaptor::path("typed", arg<"side">(Str{"secondary"})));
            adaptor::register_adaptor<TypedSourceAdaptor, TypedSourceAdaptorImpl>(
                w, adaptor::path("typed", arg<"side">(Str{"secondary/special, value"})));
            return wire<TypedSourceAdaptor>(w, adaptor::path("typed", arg<"side">(side.value())));
        }
    };

    struct GenericAdaptorGraph
    {
        [[maybe_unused]] static constexpr auto name = "generic_adaptor_graph";

        static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> input)
        {
            adaptor::register_adaptor<GenericEchoAdaptor, GenericEchoAdaptorImpl>(
                w, adaptor::path("generic", arg<"T">(scalar_type<Int>())));
            return wire<GenericEchoAdaptor>(w, adaptor::path("generic"), input).as<TS<Int>>();
        }
    };

    struct SinkOnlyGraph
    {
        static constexpr auto name = "sink_only_graph";

        static void compose(Wiring &w)
        {
            adaptor::register_adaptor<SinkOnlyAdaptor, SinkOnlyAdaptorImpl>(w);
            auto input = wire<OneTickSource>(w);
            wire<SinkOnlyAdaptor>(w, input);
        }
    };

    struct DuplicateAdaptorGraph
    {
        static constexpr auto name = "duplicate_adaptor_graph";

        static void compose(Wiring &w)
        {
            const auto custom = adaptor::path("duplicate");
            adaptor::register_adaptor<TypedSourceAdaptor, TypedSourceAdaptorImpl>(w, custom);
            adaptor::register_adaptor<TypedSourceAdaptor, TypedSourceAdaptorImpl>(w, custom);
        }
    };

    struct MissingAdaptorImplementationGraph
    {
        static constexpr auto name = "missing_adaptor_implementation_graph";

        static void compose(Wiring &w)
        {
            auto out = wire<SourceOnlyAdaptor>(w);
            wire<stdlib::dense_record_impl>(w, out, std::string{"missing_adaptor_out"});
        }
    };

    struct IllegalAdaptorStubGraph
    {
        static constexpr auto name = "illegal_adaptor_stub_graph";

        static void compose(Wiring &w)
        {
            (void)adaptor::from_graph<LoopbackAdaptor>(w);
        }
    };

    struct MissingAdaptorStubGraph
    {
        static constexpr auto name = "missing_adaptor_stub_graph";

        static void compose(Wiring &w)
        {
            adaptor::register_adaptor<LoopbackAdaptor, MissingOutputAdaptorImpl>(w);
        }
    };
}  // namespace

TEST_CASE("adaptor wiring ranks stubs around the implementation")
{
    using namespace hgraph;

    (void)TypeRegistry::instance().register_scalar<Int>("int");

    GraphBuilder builder = build_graph<LoopbackGraph>();

    const auto from_capture = find_node(
        builder, "shared_output_capture:adaptor://loopback_default/loopback/from_graph");
    const auto from_source = find_node(
        builder, "shared_output_source:adaptor://loopback_default/loopback/from_graph");
    const auto echo = find_node(builder, "echo_node");
    const auto to_capture = find_node(
        builder, "shared_output_capture:adaptor://loopback_default/loopback/to_graph");
    const auto to_source = find_node(
        builder, "shared_output_source:adaptor://loopback_default/loopback/to_graph");

    CHECK(from_capture < from_source);
    CHECK(from_source < echo);
    CHECK(echo < to_capture);
    CHECK(to_capture < to_source);
}

TEST_CASE("adaptor wiring round-trips a single-client input through an implementation")
{
    using namespace hgraph;

    (void)TypeRegistry::instance().register_scalar<Int>("int");

    CHECK_OUTPUT(testing::eval_node<LoopbackGraph>(), testing::values<Int>(41));
}

TEST_CASE("adaptor wiring relays same-cycle through chained adaptors")
{
    using namespace hgraph;

    (void)TypeRegistry::instance().register_scalar<Int>("int");

    // The value crosses two full adaptor boundaries in ONE engine cycle:
    // adaptors are rank-correct, so every capture schedules its paired source
    // for the current evaluation time.
    CHECK_OUTPUT(testing::eval_node<ChainedAdaptorGraph>(testing::values<Int>(7)),
                 testing::values<Int>(7));
}

TEST_CASE("adaptor wiring supports explicit paths through the adaptor call")
{
    using namespace hgraph;

    (void)TypeRegistry::instance().register_scalar<Int>("int");

    CHECK_OUTPUT(testing::eval_node<ExplicitPathGraph>(), testing::values<Int>(41));
}

TEST_CASE("adaptor wiring supports source-only and sink-only descriptors")
{
    using namespace hgraph;

    (void)TypeRegistry::instance().register_scalar<Int>("int");

    CHECK_OUTPUT(testing::eval_node<SourceOnlyGraph>(), testing::values<Int>(5));

    GraphExecutorBuilder sink_executor_builder;
    sink_executor_builder.graph_builder(build_graph<SinkOnlyGraph>())
        .start_time(MIN_ST)
        .end_time(MIN_ST + TimeDelta{6});

    GraphExecutorValue sink_executor = sink_executor_builder.make_executor();
    auto               sink_view = sink_executor.view();
    sink_view.run();

    const auto sink_values = testing::get_recorded_values<Int>(
        sink_view.graph().global_state(), "sink_out");
    REQUIRE(!sink_values.empty());
    CHECK(sink_values[0] == Int{41});

    GraphExecutorBuilder named_sink_executor_builder;
    named_sink_executor_builder.graph_builder(build_graph<NamedSinkGraph>())
        .start_time(MIN_ST)
        .end_time(MIN_ST + TimeDelta{6});

    GraphExecutorValue named_sink_executor = named_sink_executor_builder.make_executor();
    auto               named_sink_view = named_sink_executor.view();
    named_sink_view.run();

    const auto named_sink_values = testing::get_recorded_values<Int>(
        named_sink_view.graph().global_state(), "named_sink_out");
    REQUIRE(!named_sink_values.empty());
    CHECK(named_sink_values[0] == Int{41});
}

TEST_CASE("adaptor wiring supports multi-interface implementations")
{
    using namespace hgraph;

    (void)TypeRegistry::instance().register_scalar<Int>("int");

    CHECK_OUTPUT(testing::eval_node<MultiAdaptorGraph>(testing::values<Int>(41)), testing::values<Int>(41));
}

TEST_CASE("adaptor wiring supports scalar-qualified paths")
{
    using namespace hgraph;

    (void)TypeRegistry::instance().register_scalar<Int>("int");

    const auto primary_graph = build_graph<TypedPathAdaptorGraph>(Str{"primary"});
    std::vector<std::string> configured_paths;
    std::vector<std::string> output_sources;
    for (std::size_t index = 0; index < primary_graph.node_count(); ++index)
    {
        const auto &node = primary_graph.nodes()[index];
        const auto name = node_name(primary_graph, index);
        if (name == "path_source")
        {
            configured_paths.push_back(node.scalars().as_bundle().field("path").checked_as<Str>());
        }
        else if (name.starts_with("shared_output_source:adaptor://typed"))
        {
            output_sources.push_back(name);
        }
    }
    CHECK(configured_paths == std::vector<std::string>{"typed[side=primary]"});
    CHECK(output_sources == std::vector<std::string>{
                                "shared_output_source:adaptor://typed[side=primary]/typed_source/to_graph"});

    CHECK_OUTPUT(testing::eval_node<TypedPathAdaptorGraph>(Str{"primary"}), testing::values<Int>(11));
    CHECK_OUTPUT(testing::eval_node<TypedPathAdaptorGraph>(Str{"secondary"}), testing::values<Int>(22));
    CHECK_OUTPUT(testing::eval_node<TypedPathAdaptorGraph>(Str{"secondary/special, value"}), testing::values<Int>(22));
}

TEST_CASE("adaptor wiring resolves generic interface identities from client input")
{
    using namespace hgraph;

    (void)TypeRegistry::instance().register_scalar<Int>("int");

    CHECK_OUTPUT(testing::eval_node<GenericAdaptorGraph>(testing::values<Int>(41)), testing::values<Int>(41));
}

TEST_CASE("adaptor wiring supports automatic source sink and duplex implementations")
{
    using namespace hgraph;

    (void)TypeRegistry::instance().register_scalar<Int>("int");

    CHECK_OUTPUT(testing::eval_node<AutomaticLoopbackGraph>(), testing::values<Int>(41));
    CHECK_OUTPUT(testing::eval_node<AutomaticSourceGraph>(), testing::values<Int>(5));
    CHECK_OUTPUT(testing::eval_node<AutomaticSinkGraph>(), testing::values<Int>(41));
}

TEST_CASE("adaptor wiring rejects duplicate implementation registrations")
{
    using namespace hgraph;

    (void)TypeRegistry::instance().register_scalar<Int>("int");

    CHECK_THROWS_AS(build_graph<DuplicateAdaptorGraph>(), std::invalid_argument);
}

TEST_CASE("adaptor wiring validates requested implementations and ignores unused candidates")
{
    using namespace hgraph;

    (void)TypeRegistry::instance().register_scalar<Int>("int");

    CHECK_THROWS_AS(build_graph<MissingAdaptorImplementationGraph>(), std::invalid_argument);
    CHECK_THROWS_AS(build_graph<IllegalAdaptorStubGraph>(), std::invalid_argument);
    CHECK_NOTHROW(build_graph<MissingAdaptorStubGraph>());
}
