#ifndef HGRAPH_KAFKA_SERVICE_H
#define HGRAPH_KAFKA_SERVICE_H

#include <hgraph/kafka/export.h>
#include <hgraph/kafka/types.h>

#include <hgraph/types/graph_wiring.h>
#include <hgraph/types/service_wiring.h>

namespace hgraph::kafka
{
    struct KafkaSubscriptionService
    {
        static constexpr std::string_view name{"kafka_subscription"};
        using key_type     = KafkaSubscriptionKey;
        using value_schema = KafkaSubscriptionOutput;
    };

    struct KafkaPublishService
    {
        static constexpr std::string_view name{"kafka_publish"};
        using request_schema  = KafkaPublishRequest;
        using response_schema = TS<KafkaDeliveryReport>;
    };

    struct KafkaCommitService
    {
        static constexpr std::string_view name{"kafka_commit"};
        using request_schema = TS<KafkaCursor>;
    };

    struct KafkaEventService
    {
        static constexpr std::string_view name{"kafka_events"};
        using output_schema = TS<KafkaEvent>;
    };

    /** Register one lazy, path-bound production Kafka service implementation.
     * The wiring context selects the concrete execution-compatible graph:
     * real-time uses queue-owned root push sources; simulation preloads bounded
     * record-time history and emits it through ordinary scheduled nodes. No
     * Kafka client or worker thread is created until the graph is started. */
    HGRAPH_KAFKA_EXPORT void register_service(Wiring &w, service::ServicePath path, Value service_config);

    [[nodiscard]] HGRAPH_KAFKA_EXPORT Port<KafkaSubscriptionOutput> subscribe(Wiring &w, service::ServicePath path,
                                                                              Port<TS<KafkaSubscriptionKey>> key);

    [[nodiscard]] HGRAPH_KAFKA_EXPORT Port<KafkaPublishRequest> publish_request(Wiring &w, Port<TS<Str>> topic,
                                                                                Port<TS<KafkaProduceRecord>> record);

    [[nodiscard]] HGRAPH_KAFKA_EXPORT Port<KafkaPublishRequest> publish_request(Wiring &w, Str topic,
                                                                                Port<TS<KafkaProduceRecord>> record);

    [[nodiscard]] HGRAPH_KAFKA_EXPORT Port<TS<KafkaDeliveryReport>> publish(Wiring &w, service::ServicePath path,
                                                                            Port<KafkaPublishRequest> request);

    HGRAPH_KAFKA_EXPORT void commit(Wiring &w, service::ServicePath path, Port<TS<KafkaCursor>> cursor);

    [[nodiscard]] HGRAPH_KAFKA_EXPORT Port<TS<KafkaEvent>> events(Wiring &w, service::ServicePath path);
}  // namespace hgraph::kafka

#endif  // HGRAPH_KAFKA_SERVICE_H
