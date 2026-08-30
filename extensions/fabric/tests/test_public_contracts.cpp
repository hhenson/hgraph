#include <hgraph/fabric/fabric.h>
#include <hgraph/persistence/value_store.h>

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
#include <cstddef>
#include <string>
#include <utility>

namespace
{
    namespace hg   = hgraph;
    namespace hgf  = hgraph::fabric;
    namespace hgps = hgraph::persistence::store;

    /** A store for encoding fixtures, independent of any fabric config. */
    [[nodiscard]] const hgps::ValueStore &contract_values()
    {
        static const hgps::ValueStore store = [] {
            hgps::register_builtin_value_codecs();
            return hgps::make_value_store(
                hgps::ValueStoreConfig{.objects = hgps::make_object_store(
                                           hgps::ObjectStoreConfig{})});
        }();
        return store;
    }


    /** The bound codecs a configured fabric would carry.

        Deliberately NOT cached in a static: this suite resets the registries
        between cases, and a reset frees the interned converters a binding
        points at. Caching one across a reset dangles -- it crashed this suite
        with a bus error when first written, which is the hazard the
        BoundValueCodec docs describe, reproduced. Real callers bind with the
        config at wiring time and are torn down with the graph. */
    [[nodiscard]] hgps::BoundValueCodec contract_revision_codec()
    {
        return contract_values().bind(hgf::data_revision_meta());
    }

    [[nodiscard]] hgps::BoundValueCodec contract_reference_codec()
    {
        return contract_values().bind(hgf::revision_reference_meta());
    }

    [[nodiscard]] hgraph::persistence::store::ObjectBytes notice(
        hgraph::Str data_id, hgraph::Int revision)
    {
        hgraph::Value value = hgf::make_data_revision(hgf::DataRevisionInput{
            .data_id = std::move(data_id),
            .revision = revision,
            .output_version = revision,
            .as_of = hg::DateTime{hg::TimeDelta{1'767'323'045'000'000 + revision}},
        });
        return contract_values().encode(value.view());
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
            hgf::register_service(wiring);
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
            hgf::register_service(wiring);
            auto first = hgf::subscribe_data(wiring, "input-a");
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
            hgf::register_service(wiring);
            auto first = hgf::subscribe_data(wiring, "input");
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
            hgf::register_service(wiring);
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
            hgf::register_service(wiring);
            auto source = hg::nested_<NestedSubscriptionGraph>(wiring);
            hgf::publish_data(wiring, "nested-output", source);
        }
    };

    struct NestedSideEffectDependencyGraph
    {
        static void compose(hg::Wiring &wiring)
        {
            hgf::register_service(wiring);
            auto source = hg::nested_<NestedSubscriptionWithSideEffectGraph>(wiring);
            hgf::publish_data(wiring, "nested-side-effect-output", source);
        }
    };

    struct ConditionalDependencyGraph
    {
        static void compose(hg::Wiring &wiring)
        {
            hgf::register_service(wiring);
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
            hgf::register_service(wiring);
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
            hgf::register_service(wiring);
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
            hgf::register_service(wiring);
            hgf::publish_data(wiring, "independent-output",
                              hg::wire<NeverFrameSource>(wiring));
        }
    };

    struct MultipleFabricPathsGraph
    {
        static void compose(hg::Wiring &wiring)
        {
            const auto left_path = hg::service::path("left-fabric");
            const auto right_path = hg::service::path("right-fabric");
            hgf::register_service(wiring, left_path);
            hgf::register_service(wiring, right_path);
            auto left = hgf::subscribe_data(wiring, left_path, "input");
            auto right = hgf::subscribe_data(wiring, right_path, "input");
            hgf::publish_data(wiring, left_path, "output", left);
            hgf::publish_data(wiring, right_path, "output", right);
        }
    };

    struct CrossPathDependencyGraph
    {
        static void compose(hg::Wiring &wiring)
        {
            const auto left_path = hg::service::path("left-fabric");
            const auto right_path = hg::service::path("right-fabric");
            hgf::register_service(wiring, left_path);
            hgf::register_service(wiring, right_path);
            auto left = hgf::subscribe_data(wiring, left_path, "input");
            hgf::publish_data(wiring, right_path, "output", left);
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
    CHECK_THROWS_WITH(hgf::make_memory_fabric_config("test/invalid-limit", 0U),
                      "fabric notification request limit must be positive");
    CHECK_THROWS_AS(hgf::make_data_revision(hgf::DataRevisionInput{
                        .data_id = "x",
                        .revision = 1,
                        .output_version = 1,
                    }),
                    std::invalid_argument);
}

TEST_CASE("fabric metadata is a json document with the properties that matter")
{
    // The golden hex fixtures are gone with the hand-written codec they pinned:
    // the byte layout is now the library's business, covered by the value
    // store's own tests. What still belongs to fabric is behaviour.
    const auto &values = contract_values();

    hg::Value  revision = canonical_revision();
    const auto encoded  = values.encode(revision.view());

    // Readable outside this codebase: the stored object is a json document.
    const std::string text{reinterpret_cast<const char *>(encoded.data()),
                           encoded.size()};
    CHECK(text.front() == '{');
    CHECK(text.find("\"data_id\"") != std::string::npos);

    const auto decoded = values.decode(hgf::data_revision_meta(), encoded);
    CHECK(hgf::data_revision_input(decoded.view()) ==
          hgf::data_revision_input(revision.view()));

    // An index entry names the index it belongs to, so reading a latest entry
    // as an as-of entry is refused rather than silently answered.
    const auto as_of =
        hgf::encode_reference(contract_reference_codec(), hgf::MetadataObjectKind::AsOf, 3);
    CHECK(hgf::revision_reference_value(contract_reference_codec(), hgf::MetadataObjectKind::AsOf, as_of) == 3);
    CHECK_THROWS_AS(
        hgf::revision_reference_value(contract_reference_codec(), hgf::MetadataObjectKind::Latest, as_of),
        std::invalid_argument);

    // Malformed input still fails closed.
    auto malformed = encoded;
    malformed.push_back(std::byte{'!'});
    CHECK_THROWS_AS(values.decode(hgf::data_revision_meta(), malformed),
                    std::invalid_argument);
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

    auto left = hgf::make_memory_fabric_config("test/fabric/left");
    auto right = hgf::make_memory_fabric_config("test/fabric/right");
    hgf::set_fabric_config(state, "left-fabric", left);
    hgf::set_fabric_config(state, "right-fabric", right);
    REQUIRE(hgf::fabric_config(state, "left-fabric").has_value());
    REQUIRE(hgf::fabric_config(state, "right-fabric").has_value());
    CHECK(hgf::fabric_config(state, "left-fabric")->prefix == "test/fabric/left");
    CHECK(hgf::fabric_config(state, "right-fabric")->prefix == "test/fabric/right");
    hgf::clear_fabric_config(state, "left-fabric");
    CHECK_FALSE(hgf::fabric_config(state, "left-fabric").has_value());
    CHECK(hgf::fabric_config(state, "right-fabric").has_value());
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

TEST_CASE("fabric planner wires shared service clients and hidden lineage cuts")
{
    hg::stdlib::register_standard_operators();
    hgf::register_fabric_operators();

    const auto dependent = hg::build_graph<IndependentForestsGraph>();
    CHECK(count_semantic_node(
              dependent, "hgraph.fabric.service.lifecycle") == 1);
    CHECK(count_semantic_node(
              dependent, "hgraph.fabric.ingress_coordinator.contract") == 0);
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

TEST_CASE("fabric selects live or replay service topology during wiring")
{
    hg::stdlib::register_standard_operators();
    hgf::register_fabric_operators();

    const auto simulation = hg::build_graph<InstalledContractGraph>();
    CHECK(count_semantic_node(simulation, "hgraph.fabric.service.replay") == 1);
    CHECK(count_semantic_node(simulation, "hgraph.fabric.service.replay.planned") == 1);
    CHECK(count_semantic_node(simulation, "hgraph.fabric.service.live") == 0);
    CHECK(count_semantic_node(simulation, "hgraph.fabric.service.live.planned") == 0);
    CHECK(count_semantic_node(simulation, "hgraph.fabric.service.run_policy") == 0);

    const auto realtime = hg::build_graph<InstalledContractGraph>(
        hg::WiringOptions{.is_realtime = true});
    CHECK(count_semantic_node(realtime, "hgraph.fabric.service.replay") == 0);
    CHECK(count_semantic_node(realtime, "hgraph.fabric.service.replay.planned") == 0);
    CHECK(count_semantic_node(realtime, "hgraph.fabric.service.live") == 1);
    CHECK(count_semantic_node(realtime, "hgraph.fabric.service.live.planned") == 1);
    CHECK(count_semantic_node(realtime, "hgraph.fabric.service.run_policy") == 0);
}

TEST_CASE("fabric service paths isolate plans publishers and lineage")
{
    hg::stdlib::register_standard_operators();
    hgf::register_fabric_operators();

    auto graph = hg::build_graph<MultipleFabricPathsGraph>();
    CHECK(count_semantic_node(graph, "hgraph.fabric.service.lifecycle") == 2);
    CHECK(hgf::dependency_plan_input(
              graph.traits().get(hgf::dependency_plan_trait("left-fabric"))) ==
          hgf::DependencyPlanInput{
              .roots = {"input"},
              .publishers = {{"output", {"input"}}},
              .forests = {{{"input"}}},
          });
    CHECK(hgf::dependency_plan_input(
              graph.traits().get(hgf::dependency_plan_trait("right-fabric"))) ==
          hgf::DependencyPlanInput{
              .roots = {"input"},
              .publishers = {{"output", {"input"}}},
              .forests = {{{"input"}}},
          });

    CHECK_THROWS_WITH(
        hg::build_graph<CrossPathDependencyGraph>(),
        Catch::Matchers::ContainsSubstring("another Fabric service path"));
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

TEST_CASE("fabric rejects malformed metadata at the decode boundary")
{
    // The documents are externally readable, and therefore externally
    // editable: a hand-edited or corrupted file must be classified as
    // malformed here rather than reaching history and index logic. These
    // checks existed in the hand-written codec and are not the store's job,
    // so they live with the schema that owns their meaning.
    const auto &values = contract_values();

    SECTION("a non-positive revision reference is malformed")
    {
        CHECK_THROWS_AS(hgf::make_revision_reference(hgf::MetadataObjectKind::Latest, 0),
                        std::invalid_argument);
        CHECK_THROWS_AS(hgf::make_revision_reference(hgf::MetadataObjectKind::AsOf, -1),
                        std::invalid_argument);

        // ...including one that arrives already encoded, as an edited file would.
        hg::BundleBuilder builder{hg::ValuePlanFactory::instance().type_for(
            hgf::revision_reference_meta())};
        builder.set("kind", hg::Value{hg::Str{"latest"}}.view());
        builder.set("revision", hg::Value{hg::Int{0}}.view());
        const auto encoded = values.encode(builder.build().view());
        CHECK_THROWS_AS(hgf::revision_reference_value(
                            contract_reference_codec(),
                            hgf::MetadataObjectKind::Latest, encoded),
                        std::invalid_argument);
    }

    SECTION("a revision reference cannot claim the revision kind")
    {
        CHECK_THROWS_AS(
            hgf::make_revision_reference(hgf::MetadataObjectKind::Revision, 1),
            std::invalid_argument);
    }

    SECTION("non-positive ordinals in a revision are malformed")
    {
        auto encoded_with = [&](hgf::DataRevisionInput input) {
            return values.encode(hgf::make_data_revision(std::move(input)).view());
        };
        const hgf::DataRevisionInput valid{
            .data_id = "alpha", .revision = 1, .output_version = 1,
            .as_of = hg::MIN_ST};

        auto zero_revision = valid;
        zero_revision.revision = 0;
        CHECK_THROWS_AS(hgf::decode_data_revision(contract_revision_codec(), encoded_with(zero_revision)),
                        std::invalid_argument);

        auto zero_output = valid;
        zero_output.output_version = 0;
        CHECK_THROWS_AS(hgf::decode_data_revision(contract_revision_codec(), encoded_with(zero_output)),
                        std::invalid_argument);

        auto bad_dependency = valid;
        bad_dependency.dependencies = {{"input", 0}};
        CHECK_THROWS_AS(hgf::decode_data_revision(contract_revision_codec(), encoded_with(bad_dependency)),
                        std::invalid_argument);

        // A valid one still round trips, so the checks are not simply refusing.
        CHECK_NOTHROW(hgf::decode_data_revision(contract_revision_codec(), encoded_with(valid)));
    }

    SECTION("dependencies out of canonical order are malformed")
    {
        // Canonical order is what makes two equivalent revisions compare equal.
        // make_data_revision sorts, so this state cannot be reached through the
        // builder -- only by editing a stored document. The validator is
        // exported for exactly that reason, so drive it directly rather than
        // pretend the builder can produce the case.
        CHECK_THROWS_AS(hgf::validate_data_revision(hgf::DataRevisionInput{
                            .data_id = "alpha", .revision = 1,
                            .output_version = 1,
                            .dependencies = {{"b", 1}, {"a", 1}},
                            .as_of = hg::MIN_ST}),
                        std::invalid_argument);
        CHECK_NOTHROW(hgf::validate_data_revision(hgf::DataRevisionInput{
            .data_id = "alpha", .revision = 1, .output_version = 1,
            .dependencies = {{"a", 1}, {"b", 1}}, .as_of = hg::MIN_ST}));
    }

    SECTION("an oversized document is rejected before it is decoded")
    {
        const std::vector<std::byte> oversized(hgf::MAX_METADATA_BYTES + 1,
                                               std::byte{'{'});
        CHECK_THROWS_AS(hgf::decode_data_revision(contract_revision_codec(), oversized),
                        std::invalid_argument);
    }
}
