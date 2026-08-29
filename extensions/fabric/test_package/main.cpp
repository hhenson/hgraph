#include <hgraph/fabric/fabric.h>
#if defined(HGRAPH_FABRIC_CONSUMER_HAS_KAFKA)
#include <hgraph/fabric/kafka.h>
#include <hgraph/kafka/value_builders.h>
#endif

#include <hgraph/lib/std/operators/registration.h>
#include <hgraph/types/graph_wiring.h>

#include <span>

namespace
{
    namespace hg  = hgraph;
    namespace hgf = hgraph::fabric;

    struct InstalledFabricGraph
    {
        static constexpr auto name = "installed_hgraph_fabric_consumer";

        static void compose(hg::Wiring &wiring)
        {
            hgf::register_service(wiring);
            auto input = hgf::subscribe_data(wiring, "installed/input");
            hgf::publish_data(wiring, "installed/output", input);
            static_cast<void>(hgf::diagnostics(wiring));
        }
    };
}  // namespace

int main()
{
    hg::stdlib::register_standard_operators();
    hgf::register_fabric_operators();

    hg::GlobalContext context;
    auto state = context.state().view();
    hgf::set_fabric_config(
        state, hgf::make_memory_fabric_config("installed/fabric"));
    const auto config = hgf::fabric_config(state);
    if (!config.has_value() || config->prefix != "installed/fabric")
    {
        return 1;
    }
    if (hgf::load_data_as_of(
            *config, "installed/missing",
            hg::DateTime{hg::TimeDelta{1'767'323'045'006'007}})
            .has_value())
    {
        return 8;
    }

    hg::Value revision = hgf::make_data_revision(hgf::DataRevisionInput{
        .data_id = "installed/output",
        .revision = 1,
        .output_version = 7,
        .dependencies = {{"installed/input", 3}},
        .as_of = hg::MIN_ST,
    });
    const auto encoded = hgf::encode_revision(revision.view());
    const auto decoded = hgf::decode_revision(encoded);
    if (hgf::data_revision_input(decoded.view()) !=
        hgf::data_revision_input(revision.view()))
    {
        return 2;
    }

    if (hgf::decode_data_id_segment(
            hgf::encode_data_id_segment("installed/output")) !=
            "installed/output" ||
        hgf::revision_key(config->prefix, "installed/output", 1) !=
            "installed/fabric/baW5zdGFsbGVkL291dHB1dA/revision/"
            "0000000000000000001")
    {
        return 3;
    }

    hgf::PublisherStateMachine publisher{*config, "installed/output"};
    publisher.begin(hgf::PublicationInput{
        .system_time = hg::DateTime{hg::TimeDelta{1'767'323'045'006'007}},
    });
    if (publisher.advance() != hgf::PublicationState::AwaitingFirstOutput)
    {
        return 4;
    }

    hgf::ConsistencyResolver resolver{*config};
    if (resolver.resolve_forest({"installed/input"}).status !=
        hgf::ResolutionStatus::Pending)
    {
        return 5;
    }

#if defined(HGRAPH_FABRIC_CONSUMER_HAS_KAFKA)
    const auto kafka_config = hgraph::kafka::service_config()
                                  .bootstrap_servers({"broker:9092"})
                                  .build();
    hgf::require_fabric_kafka_profile(kafka_config);
    const auto subscription =
        hgf::fabric_kafka_subscription_key("installed-fabric", "installed-consumer");
    if (subscription.view().as_bundle().at("key_filter").data() != nullptr)
    {
        return 6;
    }
#endif

    auto graph = hg::build_graph<InstalledFabricGraph>();
    const auto plan = hgf::dependency_plan_input(
        graph.traits().get(hgf::DEPENDENCY_PLAN_TRAIT));
    return plan == hgf::DependencyPlanInput{
                       .roots = {"installed/input"},
                       .publishers = {{"installed/output", {"installed/input"}}},
                       .forests = {{{"installed/input"}}},
                   }
               ? 0
               : 7;
}
