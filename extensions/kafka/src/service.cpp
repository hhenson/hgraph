#include <hgraph/kafka/service.h>

#include <hgraph/lib/std/operators/collection.h>
#include <hgraph/lib/std/operators/conversion.h>
#include <hgraph/lib/std/std_nodes.h>

namespace hgraph::kafka
{
    Port<KafkaSubscriptionOutput> subscribe(Wiring &w, service::ServicePath path, Port<TS<KafkaSubscriptionKey>> key) {
        return wire<KafkaSubscriptionService>(w, std::move(path), std::move(key)).as<KafkaSubscriptionOutput>();
    }

    Port<KafkaPublishRequest> publish_request(Wiring &w, Port<TS<Str>> topic, Port<TS<KafkaProduceRecord>> record) {
        return stdlib::to_tsb<KafkaPublishRequest>(w, topic, record);
    }

    Port<KafkaPublishRequest> publish_request(Wiring &w, Str topic, Port<TS<KafkaProduceRecord>> record) {
        auto topic_ts = wire<stdlib::const_, TS<Str>>(w, std::move(topic));
        return publish_request(w, std::move(topic_ts), std::move(record));
    }

    Port<TS<KafkaDeliveryReport>> publish(Wiring &w, service::ServicePath path, Port<KafkaPublishRequest> request) {
        return wire<KafkaPublishService>(w, std::move(path), std::move(request)).as<TS<KafkaDeliveryReport>>();
    }

    void commit(Wiring &w, service::ServicePath path, Port<TS<KafkaCursor>> cursor) {
        wire<KafkaCommitService>(w, std::move(path), std::move(cursor));
    }

    Port<TS<KafkaEvent>> events(Wiring &w, service::ServicePath path) {
        return wire<KafkaEventService>(w, std::move(path)).as<TS<KafkaEvent>>();
    }
}  // namespace hgraph::kafka
