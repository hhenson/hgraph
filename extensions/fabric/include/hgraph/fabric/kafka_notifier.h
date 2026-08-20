#ifndef HGRAPH_FABRIC_KAFKA_NOTIFIER_H
#define HGRAPH_FABRIC_KAFKA_NOTIFIER_H

#include <hgraph/fabric/kafka_export.h>
#include <hgraph/fabric/notifier.h>

#include <hgraph/kafka/service.h>
#include <hgraph/types/value/value.h>

namespace hgraph::fabric
{
    /** Wiring-time configuration for the production Kafka wake-up path.

        One graph consumes the complete topic independently and explicitly
        commits only notices which Fabric has validated (or deliberately
        filtered/superseded). Queues remain bounded; persistence is always the
        authoritative state source. */
    struct KafkaNotifierConfig
    {
        Str topic{};
        Str group_id{};
        Str sharing_identity{};
        Int pending_data_id_limit{10'000};
        Int outbound_record_limit{10'000};
        Int commit_partition_limit{1'000};
    };

    /** Validate the strict RFC 0026 Kafka service profile. */
    HGRAPH_FABRIC_KAFKA_EXPORT void
    require_kafka_fabric_profile(ValueView service_config);

    /** Compose a Fabric notifier over an already registered public Kafka
        service. The returned owning handle must be installed in FabricConfig
        for this wiring root before graph execution.

        The adapter retains O(outbound_record_limit + pending_data_id_limit +
        commit_partition_limit) transport state. Callback work is O(log N) per
        affected data id/partition; each internal source evaluation emits at
        most one record or cursor and schedules another cycle for backlog. */
    [[nodiscard]] HGRAPH_FABRIC_KAFKA_EXPORT Notifier
    wire_kafka_notifier(Wiring &wiring, service::ServicePath service_path,
                        KafkaNotifierConfig config);

    /** Validate and register the public Kafka service, then compose the Fabric
        notifier over it. This is the normal production entry point. */
    [[nodiscard]] HGRAPH_FABRIC_KAFKA_EXPORT Notifier
    register_kafka_notifier(Wiring &wiring,
                            service::ServicePath service_path,
                            Value kafka_service_config,
                            KafkaNotifierConfig config);
}  // namespace hgraph::fabric

#endif  // HGRAPH_FABRIC_KAFKA_NOTIFIER_H
