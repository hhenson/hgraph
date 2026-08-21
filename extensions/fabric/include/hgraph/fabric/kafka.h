#ifndef HGRAPH_FABRIC_KAFKA_H
#define HGRAPH_FABRIC_KAFKA_H

#include <hgraph/fabric/kafka_export.h>
#include <hgraph/fabric/service.h>

#include <hgraph/types/value/value.h>

namespace hgraph::fabric {
inline constexpr std::string_view DEFAULT_KAFKA_SERVICE_PATH{"fabric-kafka"};

/** Validate the Kafka producer and queue policies required by RFC 0026.
    General Kafka services may use weaker/drop policies; Fabric rejects them
    before wiring its transport. */
HGRAPH_FABRIC_KAFKA_EXPORT void
require_fabric_kafka_profile(const Value &service_config);

/** Construct the unfiltered independent subscription used for one Fabric
    topic. It starts from committed offsets with earliest fallback and commits
    explicitly after the revision payload has crossed the decode edge. */
[[nodiscard]] HGRAPH_FABRIC_KAFKA_EXPORT Value
fabric_kafka_subscription_key(Str topic, Str identity);

/** Wire Fabric to an already registered Kafka service. This is the reusable
    composition surface for test services and externally registered Kafka
    hosts. Kafka remains the only real-time push-source owner. */
HGRAPH_FABRIC_KAFKA_EXPORT void
wire_kafka_transport(Wiring &wiring, service::ServicePath fabric_path,
                     service::ServicePath kafka_path, Str topic, Str identity);

/** Register the production Kafka service and compose the Fabric transport on
    top of it. Both implementations remain lazy graph singletons. */
HGRAPH_FABRIC_KAFKA_EXPORT void
register_kafka_transport(Wiring &wiring, service::ServicePath fabric_path,
                         service::ServicePath kafka_path, Str topic,
                         Str identity, Value kafka_service_config);

HGRAPH_FABRIC_KAFKA_EXPORT void
register_kafka_transport(Wiring &wiring, Str topic, Str identity,
                         Value kafka_service_config);
} // namespace hgraph::fabric

#endif // HGRAPH_FABRIC_KAFKA_H
