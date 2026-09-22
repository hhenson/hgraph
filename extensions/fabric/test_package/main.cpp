#include <hgraph/fabric/fabric.h>

#include <hgraph/persistence/value_store.h>
#if defined(HGRAPH_FABRIC_CONSUMER_HAS_KAFKA)
#include <hgraph/fabric/kafka.h>
#include <hgraph/kafka/value_builders.h>
#endif

#include <hgraph/lib/std/operators/registration.h>
#include <hgraph/types/graph_wiring.h>

#include <cstddef>
#include <span>
#include <string_view>

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
    if (hgf::load_data(
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
    // Metadata is a declared value schema through a store assembled from the
    // Fabric object resource and codec policy. Run-local code binds this store
    // once; an installed consumer gets the same contract without a codec of
    // its own.
    const auto values = hg::persistence::store::make_value_store(
        {.objects = config->objects, .codec = config->metadata_codec});
    const auto revision_codec = values.bind(hgf::data_revision_meta());
    const auto encoded =
        hgf::encode_data_revision(revision_codec, revision.view());
    const auto decoded = hgf::decode_data_revision(revision_codec, encoded);
    if (hgf::data_revision_input(decoded.view()) !=
        hgf::data_revision_input(revision.view()))
    {
        return 2;
    }

    // Two promises worth asserting from outside the library, where they
    // actually matter (RFC 0040).
    //
    // Metadata fabric STORES is state, so by default it is the binary value
    // codec in a compression block, which begins with its codec byte: 0 to 2.
    if (encoded.empty() || std::to_integer<unsigned>(encoded.front()) > 2)
    {
        return 9;
    }
    // What fabric puts onto KAFKA is an external message format, because the
    // tools around a topic know nothing about hgraph: a readable document.
    hg::persistence::store::ObjectBytes message;
    hgf::notification_codec().encode(revision.view(), message);
    const std::string_view document(reinterpret_cast<const char *>(message.data()),
                                    message.size());
    if (!document.starts_with("{") || !document.ends_with("}") ||
        document.find("\"data_id\"") == std::string_view::npos)
    {
        return 12;
    }
    if (hgf::data_revision_input(
            hgf::notification_codec().decode(hgf::data_revision_meta(), message).view()) !=
        hgf::data_revision_input(revision.view()))
    {
        return 13;
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
