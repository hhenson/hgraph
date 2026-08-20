#include <hgraph/fabric/fabric.h>

#include <hgraph/lib/std/operators/registration.h>
#include <hgraph/lib/std/std_operators.h>
#include <hgraph/lib/std/std_nodes.h>
#include <hgraph/runtime/global_state.h>
#include <hgraph/types/graph_wiring.h>
#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/registry_reset.h>
#include <hgraph/types/subgraph_wiring.h>

#include <catch2/matchers/catch_matchers_string.hpp>
#include <catch2/catch_test_macros.hpp>

#include <algorithm>
#include <cctype>
#include <cstddef>
#include <fstream>
#include <iterator>
#include <span>
#include <string>
#include <utility>

namespace
{
    namespace hg  = hgraph;
    namespace hgf = hgraph::fabric;

    [[nodiscard]] hgraph::persistence::store::ObjectBytes notice(
        hgraph::Str data_id, hgraph::Int revision)
    {
        hgraph::Value value = hgf::make_data_revision(hgf::DataRevisionInput{
            .data_id = std::move(data_id),
            .revision = revision,
            .output_version = revision,
            .as_of = hg::DateTime{hg::TimeDelta{1'767'323'045'000'000 + revision}},
        });
        return hgf::encode_revision(value.view());
    }

    [[nodiscard]] std::string hex(
        std::span<const std::byte> value)
    {
        constexpr char digits[]{"0123456789abcdef"};
        std::string result;
        result.reserve(value.size() * 2);
        for (const std::byte item : value)
        {
            const auto raw = std::to_integer<unsigned char>(item);
            result.push_back(digits[raw >> 4U]);
            result.push_back(digits[raw & 0x0fU]);
        }
        return result;
    }

    [[nodiscard]] std::string fixture(std::string_view name)
    {
        std::ifstream input{std::string{HGRAPH_FABRIC_FIXTURE_DIR} + "/" +
                            std::string{name}};
        REQUIRE(input.good());
        std::string result{std::istreambuf_iterator<char>{input}, {}};
        std::erase_if(result, [](unsigned char value) {
            return std::isspace(value) != 0;
        });
        return result;
    }

    [[nodiscard]] hg::Value canonical_revision()
    {
        return hgf::make_data_revision(hgf::DataRevisionInput{
            .format_version = hgf::REVISION_FORMAT_VERSION,
            .data_id = "derived/α",
            .revision = 3,
            .output_version = 42,
            .dependencies = {{"input-b", 11}, {"input-a", 7}},
            .self_predecessor = 41,
            .as_of = hg::DateTime{hg::TimeDelta{1'767'323'045'006'007}},
        });
    }

    struct InstalledContractGraph
    {
        static constexpr auto name = "hgraph.fabric.test.contract_graph";

        static void compose(hg::Wiring &wiring)
        {
            auto source = hgf::subscribe_data(wiring, "input");
            auto dependency = hgf::dependency_handle(wiring, source);
            hgf::publish_data(wiring, "automatic", source);
            hgf::publish_data(
                wiring, "explicit", source,
                hgf::DependencySelection::explicit_dependencies(
                    {std::move(dependency)}));
        }
    };

    struct DuplicatePublisherGraph
    {
        static constexpr auto name = "hgraph.fabric.test.duplicate_publishers";

        static void compose(hg::Wiring &wiring)
        {
            auto first  = hgf::subscribe_data(wiring, "input-a");
            auto second = hgf::subscribe_data(wiring, "input-b");
            hgf::publish_data(wiring, "output", first);
            hgf::publish_data(wiring, "output", second);
        }
    };

    struct DuplicateExplicitDependencyGraph
    {
        static constexpr auto name =
            "hgraph.fabric.test.duplicate_explicit_dependencies";

        static void compose(hg::Wiring &wiring)
        {
            auto first  = hgf::subscribe_data(wiring, "input");
            auto second = hgf::subscribe_data(wiring, "input");
            hgf::publish_data(
                wiring, "output", first,
                hgf::DependencySelection::explicit_dependencies(
                    {hgf::dependency_handle(wiring, first),
                     hgf::dependency_handle(wiring, second)}));
        }
    };

    struct NeverFrameSource
    {
        static constexpr auto name = "hgraph.fabric.test.never_frame";
        static void eval(hg::Out<hg::TS<hg::Frame>>) {}
    };

    struct SharedDependencyGraph
    {
        static void compose(hg::Wiring &wiring)
        {
            auto source = hgf::subscribe_data(wiring, "shared");
            hgf::publish_data(wiring, "left", source);
            hgf::publish_data(wiring, "right", source);
        }
    };

    struct NestedSubscriptionGraph
    {
        static hg::Port<hg::TS<hg::Frame>> compose(hg::Wiring &wiring)
        {
            return hgf::subscribe_data(wiring, "nested");
        }
    };

    struct ConsumeFrameSink
    {
        static void eval(hg::In<"value", hg::TS<hg::Frame>>) {}
    };

    struct NestedSubscriptionWithSideEffectGraph
    {
        static hg::Port<hg::TS<hg::Frame>> compose(hg::Wiring &wiring)
        {
            auto returned = hgf::subscribe_data(wiring, "nested-returned");
            auto unrelated = hgf::subscribe_data(wiring, "nested-side-effect");
            hg::wire<ConsumeFrameSink>(wiring, unrelated);
            return returned;
        }
    };

    struct NestedDependencyGraph
    {
        static void compose(hg::Wiring &wiring)
        {
            auto source = hg::nested_<NestedSubscriptionGraph>(wiring);
            hgf::publish_data(wiring, "nested-output", source);
        }
    };

    struct NestedSideEffectDependencyGraph
    {
        static void compose(hg::Wiring &wiring)
        {
            auto source = hg::nested_<NestedSubscriptionWithSideEffectGraph>(wiring);
            hgf::publish_data(wiring, "nested-side-effect-output", source);
        }
    };

    struct ConditionalDependencyGraph
    {
        static void compose(hg::Wiring &wiring)
        {
            auto left = hgf::subscribe_data(wiring, "conditional-a");
            auto right = hgf::subscribe_data(wiring, "conditional-b");
            auto condition =
                hg::wire<hg::stdlib::const_, hg::TS<hg::Bool>>(wiring, true);
            auto selected = hg::wire<hg::stdlib::if_then_else>(
                                wiring, condition, left, right)
                                .as<hg::TS<hg::Frame>>();
            hgf::publish_data(wiring, "conditional-output", selected);
        }
    };

    struct ExplicitHiddenDependencyGraph
    {
        static void compose(hg::Wiring &wiring)
        {
            auto dependency = hgf::subscribe_data(wiring, "declared-only");
            auto value = hg::wire<NeverFrameSource>(wiring);
            hgf::publish_data(
                wiring, "explicit-output", value,
                hgf::DependencySelection::explicit_dependencies(
                    {hgf::dependency_handle(wiring, dependency)}));
        }
    };

    struct IndependentForestsGraph
    {
        static void compose(hg::Wiring &wiring)
        {
            auto a = hgf::subscribe_data(wiring, "a");
            auto b = hgf::subscribe_data(wiring, "b");
            auto x = hgf::subscribe_data(wiring, "x");
            auto condition =
                hg::wire<hg::stdlib::const_, hg::TS<hg::Bool>>(wiring, true);
            auto joined = hg::wire<hg::stdlib::if_then_else>(
                              wiring, condition, a, b)
                              .as<hg::TS<hg::Frame>>();
            hgf::publish_data(wiring, "ab-output", joined);
            hgf::publish_data(wiring, "x-output", x);
        }
    };

    struct NoDependencyGraph
    {
        static void compose(hg::Wiring &wiring)
        {
            hgf::publish_data(wiring, "independent-output",
                              hg::wire<NeverFrameSource>(wiring));
        }
    };

    template <typename Graph>
    [[nodiscard]] hgf::DependencyPlanInput plan_for()
    {
        hg::GraphBuilder graph = hg::build_graph<Graph>();
        return hgf::dependency_plan_input(
            graph.traits().get(hgf::DEPENDENCY_PLAN_TRAIT));
    }

    [[nodiscard]] std::size_t count_semantic_node(
        const hg::GraphBuilder &graph, std::string_view semantic_name)
    {
        return static_cast<std::size_t>(std::ranges::count_if(
            graph.nodes(), [&](const hg::NodeBuilder &node) {
                return node.type().record()->semantic_name() == semantic_name;
            }));
    }
}  // namespace

TEST_CASE("fabric public values are canonical and validate identity")
{
    hgf::register_fabric_types();
    hg::Value revision = canonical_revision();
    const auto input = hgf::data_revision_input(revision.view());
    REQUIRE(input.dependencies.size() == 2);
    CHECK(input.dependencies[0] == hgf::DataDependencyInput{"input-a", 7});
    CHECK(input.dependencies[1] == hgf::DataDependencyInput{"input-b", 11});

    CHECK_NOTHROW(hgf::require_data_id("desk/prices-α"));
    CHECK_NOTHROW(hgf::require_data_id(std::string(hgf::MAX_DATA_ID_BYTES, 'a')));
    CHECK_THROWS_AS(hgf::require_data_id(""), std::invalid_argument);
    CHECK_THROWS_AS(
        hgf::require_data_id(std::string(hgf::MAX_DATA_ID_BYTES + 1, 'a')),
        std::invalid_argument);
    CHECK_THROWS_AS(hgf::require_data_id(std::string{"bad\x01id", 6}),
                    std::invalid_argument);
    CHECK_THROWS_AS(hgf::require_data_id(std::string{"\xc0\x80", 2}),
                    std::invalid_argument);
    CHECK_THROWS_AS(hgf::make_data_revision(hgf::DataRevisionInput{
                        .data_id = "x",
                        .revision = 1,
                        .output_version = 1,
                        .dependencies = {{"x", 1}},
                        .as_of = hg::MIN_ST,
                    }),
                    std::invalid_argument);
    CHECK_THROWS_AS(hgf::make_data_revision(hgf::DataRevisionInput{
                        .data_id = "x",
                        .revision = 1,
                        .output_version = 1,
                    }),
                    std::invalid_argument);
}

TEST_CASE("fabric canonical metadata matches the shared golden fixtures")
{
    hg::Value revision = canonical_revision();
    const auto encoded = hgf::encode_revision(revision.view());
    CHECK(hex(encoded) == fixture("revision_v1.hex"));
    const auto decoded = hgf::decode_revision(encoded);
    CHECK(hgf::data_revision_input(decoded.view()) ==
          hgf::data_revision_input(revision.view()));

    const auto as_of = hgf::encode_revision_reference(
        hgf::MetadataObjectKind::AsOf, 3);
    const auto latest = hgf::encode_revision_reference(
        hgf::MetadataObjectKind::Latest, 3);
    CHECK(hex(as_of) == fixture("as_of_v1.hex"));
    CHECK(hex(latest) == fixture("latest_v1.hex"));
    CHECK(hgf::decode_revision_reference(hgf::MetadataObjectKind::AsOf,
                                         as_of) == 3);
    CHECK_THROWS_AS(hgf::decode_revision_reference(
                        hgf::MetadataObjectKind::Latest, as_of),
                    std::invalid_argument);

    auto malformed = encoded;
    malformed.push_back(std::byte{});
    CHECK_THROWS_AS(hgf::decode_revision(malformed), std::invalid_argument);
}

TEST_CASE("memory notifier fans out and conflates each data id")
{
    auto notifier = hgf::make_memory_notifier();
    auto first = notifier.subscribe();
    auto second = notifier.subscribe();

    CHECK(notifier.publish({"a", notice("a", 1)}).poll().status ==
          hgf::NotificationDeliveryStatus::Delivered);
    CHECK(notifier.publish({"b", notice("b", 1)}).poll().status ==
          hgf::NotificationDeliveryStatus::Delivered);
    CHECK(notifier.publish({"a", notice("a", 2)}).poll().status ==
          hgf::NotificationDeliveryStatus::Delivered);
    CHECK_THROWS_AS(notifier.publish({"a", notice("different", 1)}),
                    std::invalid_argument);

    REQUIRE(first.pending() == 2);
    REQUIRE(second.pending() == 2);
    CHECK(first.try_pop() == hgf::RevisionNotification{"a", notice("a", 2)});
    CHECK(first.try_pop() == hgf::RevisionNotification{"b", notice("b", 1)});
    CHECK(second.try_pop() == hgf::RevisionNotification{"a", notice("a", 2)});

    second.close();
    CHECK(notifier.publish({"c", notice("c", 1)}).poll().status ==
          hgf::NotificationDeliveryStatus::Delivered);
    CHECK(second.pending() == 0);
    CHECK(first.try_pop() == hgf::RevisionNotification{"c", notice("c", 1)});
}

TEST_CASE("fabric configuration is run scoped and validates resources")
{
    hg::GlobalContext context;
    auto state = context.state().view();
    CHECK_FALSE(hgf::fabric_config(state).has_value());
    hgf::set_fabric_config(state, hgf::make_memory_fabric_config("test/fabric"));
    const auto configured = hgf::fabric_config(state);
    REQUIRE(configured.has_value());
    CHECK(configured->prefix == "test/fabric");
    CHECK(configured->objects);
    CHECK(configured->frames);
    CHECK(configured->notifications);
    hgf::clear_fabric_config(state);
    CHECK_FALSE(hgf::fabric_config(state).has_value());

    auto invalid = hgf::make_memory_fabric_config("test/fabric");
    invalid.default_real_time = hgf::SubscriptionMode::Snapshot;
    CHECK_THROWS_WITH(hgf::require_valid_config(invalid),
                      "fabric real-time default must be Live or Replay");
    invalid.default_real_time = hgf::SubscriptionMode::Live;
    invalid.default_simulation = hgf::SubscriptionMode::Auto;
    CHECK_THROWS_WITH(hgf::require_valid_config(invalid),
                      "fabric simulation default must be Live or Replay");
}

TEST_CASE("fabric operator installer survives a registry rebuild")
{
    hg::stdlib::register_standard_operators();
    hgf::register_fabric_operators();
    CHECK(hg::build_graph<InstalledContractGraph>().node_count() > 3);

    hg::reset_all_registries();
    hg::stdlib::register_standard_operators();
    CHECK(hg::build_graph<InstalledContractGraph>().node_count() > 3);
}

TEST_CASE("fabric planner discovers direct shared nested conditional and explicit dependencies")
{
    hg::stdlib::register_standard_operators();
    hgf::register_fabric_operators();

    CHECK(plan_for<InstalledContractGraph>() == hgf::DependencyPlanInput{
        .roots = {"input"},
        .publishers = {{"automatic", {"input"}},
                       {"explicit", {"input"}}},
        .forests = {{{"input"}}},
    });
    CHECK(plan_for<SharedDependencyGraph>() == hgf::DependencyPlanInput{
        .roots = {"shared"},
        .publishers = {{"left", {"shared"}}, {"right", {"shared"}}},
        .forests = {{{"shared"}}},
    });
    CHECK(plan_for<NestedDependencyGraph>() == hgf::DependencyPlanInput{
        .roots = {"nested"},
        .publishers = {{"nested-output", {"nested"}}},
        .forests = {{{"nested"}}},
    });
    CHECK(plan_for<NestedSideEffectDependencyGraph>() ==
          hgf::DependencyPlanInput{
              .roots = {"nested-returned"},
              .publishers = {{"nested-side-effect-output",
                              {"nested-returned"}}},
              .forests = {{{"nested-returned"}}},
          });
    CHECK(plan_for<ConditionalDependencyGraph>() == hgf::DependencyPlanInput{
        .roots = {"conditional-a", "conditional-b"},
        .publishers = {{"conditional-output",
                        {"conditional-a", "conditional-b"}}},
        .forests = {{{"conditional-a"}}, {{"conditional-b"}}},
    });
    CHECK(plan_for<ExplicitHiddenDependencyGraph>() == hgf::DependencyPlanInput{
        .roots = {"declared-only"},
        .publishers = {{"explicit-output", {"declared-only"}}},
        .forests = {{{"declared-only"}}},
    });
}

TEST_CASE("fabric planner partitions independent consistency forests")
{
    hg::stdlib::register_standard_operators();
    hgf::register_fabric_operators();
    const auto plan = plan_for<IndependentForestsGraph>();
    CHECK(plan.roots == std::vector<hg::Str>{"a", "b", "x"});
    CHECK(plan.publishers ==
          std::vector<hgf::PlannedPublisherInput>{
              {"ab-output", {"a", "b"}}, {"x-output", {"x"}}});
    CHECK(plan.forests == std::vector<hgf::ConsistencyForestInput>{
                              {{"a"}}, {{"b"}}, {{"x"}}});
}

TEST_CASE("fabric planner wires one coordinator and hidden lineage cuts")
{
    hg::stdlib::register_standard_operators();
    hgf::register_fabric_operators();

    const auto dependent = hg::build_graph<IndependentForestsGraph>();
    CHECK(count_semantic_node(
              dependent, "hgraph.fabric.ingress_coordinator.contract") == 1);
    CHECK(count_semantic_node(
              dependent, "hgraph.fabric.publish_data.with_cut") == 2);
    CHECK(count_semantic_node(
              dependent, "hgraph.fabric.publish_data.no_dependencies") == 0);

    const auto independent = hg::build_graph<NoDependencyGraph>();
    CHECK(count_semantic_node(
              independent, "hgraph.fabric.ingress_coordinator.contract") == 0);
    CHECK(count_semantic_node(
              independent, "hgraph.fabric.publish_data.with_cut") == 0);
    CHECK(count_semantic_node(
              independent, "hgraph.fabric.publish_data.no_dependencies") == 1);
}

TEST_CASE("fabric dependency handles reject forwarded and unrelated values")
{
    hg::stdlib::register_standard_operators();
    hgf::register_fabric_operators();
    hg::Wiring wiring;
    auto source = hgf::subscribe_data(wiring, "direct");
    auto forwarded = hg::wire<hg::stdlib::pass_through_node>(wiring, source)
                         .as<hg::TS<hg::Frame>>();
    CHECK_THROWS_WITH(
        hgf::dependency_handle(wiring, forwarded),
        "fabric dependency handle requires a direct subscribe_data result");
    CHECK_THROWS_WITH(
        hgf::dependency_handle(wiring, hg::wire<NeverFrameSource>(wiring)),
        "fabric dependency handle requires a direct subscribe_data result");
}

TEST_CASE("fabric subscription mode validation rejects unknown enum values")
{
    hg::Wiring wiring;
    CHECK_THROWS_WITH(
        hgf::subscribe_data(wiring, "input",
                            static_cast<hgf::SubscriptionMode>(99)),
        "hgraph.fabric.subscribe_data: unsupported subscription mode");
}

TEST_CASE("fabric wiring rejects duplicate publisher and dependency data ids")
{
    hgf::register_fabric_operators();
    CHECK_THROWS_WITH(
        hg::build_graph<DuplicatePublisherGraph>(),
        Catch::Matchers::ContainsSubstring("data id already has a publisher"));
    CHECK_THROWS_WITH(
        hg::build_graph<DuplicateExplicitDependencyGraph>(),
        Catch::Matchers::ContainsSubstring("dependency data ids must be unique"));
}
