#include <hgraph/kafka/service.h>
#include <hgraph/kafka/value_builders.h>

#include <hgraph/lib/std/operators/conversion.h>
#include <hgraph/lib/std/operators/registration.h>
#include <hgraph/types/graph_wiring.h>

namespace
{
    using namespace hgraph;
    using namespace hgraph::kafka;

    struct InstalledConsumerGraph
    {
        static constexpr auto name = "installed_hgraph_kafka_consumer";

        static void compose(Wiring &w)
        {
            const auto path = service::path("installed-consumer");
            register_service(w, path, make_service_config({Str{"localhost:9092"}}));

            auto subscription_key = wire<stdlib::const_, TS<KafkaSubscriptionKey>>(
                w, make_subscription_key({Str{"installed-topic"}}, Str{"installed-group"}));
            static_cast<void>(subscribe(w, path, subscription_key));

            auto publish_record =
                wire<stdlib::const_, TS<KafkaProduceRecord>>(w, make_produce_record(Bytes{"payload"}));
            static_cast<void>(publish(w, path, publish_request(w, Str{"installed-topic"}, publish_record)));

            auto commit_cursor = wire<stdlib::const_, TS<KafkaCursor>>(
                w, make_cursor(Str{"installed-subscription"}, Int{1}, Str{"installed-topic"}, Int{0}, Int{1}));
            commit(w, path, commit_cursor);
            static_cast<void>(events(w, path));
        }
    };
}  // namespace

int main()
{
    stdlib::register_standard_operators();
    register_kafka_types();
    auto graph = build_graph<InstalledConsumerGraph>(
        WiringOptions{.is_realtime = true});
    return graph.node_count() == 0 ? 1 : 0;
}
