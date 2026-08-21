#ifndef HGRAPH_FABRIC_SERVICE_H
#define HGRAPH_FABRIC_SERVICE_H

#include <hgraph/fabric/export.h>
#include <hgraph/fabric/types.h>

#include <hgraph/types/frame.h>
#include <hgraph/types/graph_wiring.h>
#include <hgraph/types/service_wiring.h>

#include <cstddef>
#include <string_view>

namespace hgraph::fabric
{
inline constexpr std::string_view DEFAULT_SERVICE_PATH{"fabric"};
inline constexpr std::size_t FABRIC_PUBLICATION_QUEUE_LIMIT_PER_DATA_ID{1024U};
inline constexpr std::size_t FABRIC_LIVE_NOTICE_LIMIT_PER_SESSION{4096U};
inline constexpr std::size_t FABRIC_NOTIFICATION_REQUEST_LIMIT{1024U};
inline constexpr std::size_t FABRIC_NOTIFICATION_RETRY_LIMIT{8U};
inline constexpr std::size_t FABRIC_DIAGNOSTIC_EVENT_LIMIT{256U};

/** Select where accepted revision notifications are delivered. Configured
    uses the Notifier stored in FabricConfig. GraphTransport exposes requests
    and accepts delivery reports through the services below, allowing Kafka or
    another graph-native transport to remain outside the Fabric runtime. */
enum class FabricNotificationMode
{
    Configured,
    GraphTransport,
};

/** One root update delivered by the shared Fabric service. A revision may
    advance without ticking ``frame``; hidden publisher lineage still sees
    the version/revision fields in that case. */
using FabricIngressSignal = TSB<"hgraph.fabric::IngressSignal", Field<"frame", TS<Frame>>, Field<"version", TS<Int>>,
                                Field<"revision", TS<Int>>>;

using FabricPublicationRequest =
    TSB<"hgraph.fabric::PublicationRequest", Field<"data_id", TS<Str>>, Field<"frame", TS<Frame>>,
        Field<"dependencies", TSD<Str, TS<Int>>>, Field<"self_predecessor", TS<Int>>>;

using FabricLoadResponse =
    TSB<"hgraph.fabric::LoadResponse", Field<"data_id", TS<Str>>, Field<"version", TS<Int>>, Field<"frame", TS<Frame>>>;

using FabricLoadRequest = TSB<"hgraph.fabric::LoadRequest", Field<"data_id", TS<Str>>, Field<"version", TS<Int>>>;

using FabricNotificationDelivery =
    TSB<"hgraph.fabric::NotificationDelivery", Field<"data_id", TS<Str>>, Field<"revision", TS<Int>>,
        Field<"delivered", TS<Bool>>, Field<"retriable", TS<Bool>>, Field<"message", TS<Str>>>;

/** Transport lifecycle projected onto Fabric without leaking transport-owned
    enum types into the base extension. ``ready`` means ingress is established;
    ``reconcile`` requests a durable-head pass for a new live generation. */
using FabricTransportControl =
    TSB<"hgraph.fabric::TransportControl", Field<"ready", TS<Bool>>, Field<"reconcile", TS<Bool>>,
        Field<"failed", TS<Bool>>, Field<"message", TS<Str>>>;

using FabricTransportEvent =
    TSB<"hgraph.fabric::TransportEvent", Field<"component", TS<Str>>, Field<"category", TS<Str>>,
        Field<"message", TS<Str>>, Field<"retriable", TS<Bool>>, Field<"fatal", TS<Bool>>>;

/** Stable metrics plus typed, path-addressed events. Metrics remain strings
    so counters and lifecycle values can share one map; events preserve
    component, category, severity and repeat count as native scalar fields. */
using FabricDiagnostics =
    TSB<"hgraph.fabric::Diagnostics", Field<"metrics", TSD<Str, TS<Str>>>,
        Field<"events", TSD<Str, TS<FabricDiagnosticEvent>>>>;

struct FabricLiveSubscriptionService
{
    static constexpr std::string_view name{"fabric_live_subscription"};
    using key_type = Str;
    using value_schema = FabricIngressSignal;
};

struct FabricReplaySubscriptionService
{
    static constexpr std::string_view name{"fabric_replay_subscription"};
    using key_type = Str;
    using value_schema = FabricIngressSignal;
};

struct FabricSnapshotSubscriptionService
{
    static constexpr std::string_view name{"fabric_snapshot_subscription"};
    using key_type = Str;
    using value_schema = FabricIngressSignal;
};

/** Replyless publication requests from every publish_data client. */
struct FabricPublicationService
{
    static constexpr std::string_view name{"fabric_publication"};
    using request_schema = FabricPublicationRequest;
};

/** Complete accepted revisions arriving on an ordinary graph edge. The
    optional Kafka adapter is the production implementation of this edge. */
struct FabricNoticeService
{
    static constexpr std::string_view name{"fabric_notice"};
    using request_schema = TS<DataRevision>;
};

/** One durable accepted revision awaiting graph-native transport. Requests
    are serialized onto one ordinary TS edge; the transport may have multiple
    broker deliveries in flight and correlates them by data id and revision. */
struct FabricNotificationRequestService
{
    static constexpr std::string_view name{"fabric_notification_request"};
    using output_schema = TS<DataRevision>;
};

struct FabricNotificationDeliveryService
{
    static constexpr std::string_view name{"fabric_notification_delivery"};
    using request_schema = FabricNotificationDelivery;
};

struct FabricTransportControlService
{
    static constexpr std::string_view name{"fabric_transport_control"};
    using request_schema = FabricTransportControl;
};

struct FabricTransportEventService
{
    static constexpr std::string_view name{"fabric_transport_event"};
    using request_schema = FabricTransportEvent;
};

/** Synchronous v1 load request/reply surface. The service owns persistence
    access; a later asynchronous strategy can retain this contract. */
struct FabricLoadService
{
    static constexpr std::string_view name{"fabric_load"};
    using request_schema = FabricLoadRequest;
    using response_schema = FabricLoadResponse;
};

struct FabricDiagnosticsService
{
    static constexpr std::string_view name{"fabric_diagnostics"};
    using output_schema = FabricDiagnostics;
};

/** Register one lazy root-graph FabricServiceImpl singleton. Configuration
    is read from GlobalState during service start. This host operation is
    intentionally separate from subscribe_data clients. */
HGRAPH_FABRIC_EXPORT void register_service(Wiring &wiring, service::ServicePath path);
HGRAPH_FABRIC_EXPORT void register_service(Wiring &wiring);
HGRAPH_FABRIC_EXPORT void register_service(Wiring &wiring, service::ServicePath path, FabricNotificationMode mode);

/** Feed a complete accepted revision into the registered service through
    the ordinary request edge. */
HGRAPH_FABRIC_EXPORT void submit_notice(Wiring &wiring, Port<TS<DataRevision>> notice, service::ServicePath path);
HGRAPH_FABRIC_EXPORT void submit_notice(Wiring &wiring, Port<TS<DataRevision>> notice);

[[nodiscard]] HGRAPH_FABRIC_EXPORT Port<TS<DataRevision>>
notification_requests(Wiring &wiring, service::ServicePath path);
[[nodiscard]] HGRAPH_FABRIC_EXPORT Port<TS<DataRevision>> notification_requests(Wiring &wiring);

HGRAPH_FABRIC_EXPORT void submit_notification_delivery(Wiring &wiring, Port<FabricNotificationDelivery> delivery,
                                                       service::ServicePath path);
HGRAPH_FABRIC_EXPORT void submit_transport_control(Wiring &wiring, Port<FabricTransportControl> control,
                                                   service::ServicePath path);
HGRAPH_FABRIC_EXPORT void submit_transport_event(Wiring &wiring, Port<FabricTransportEvent> event,
                                                 service::ServicePath path);

[[nodiscard]] HGRAPH_FABRIC_EXPORT Port<FabricLoadResponse> request_load(Wiring &wiring, Str data_id,
                                                                         DataVersion version,
                                                                         service::ServicePath path);
[[nodiscard]] HGRAPH_FABRIC_EXPORT Port<FabricLoadResponse> request_load(Wiring &wiring, Str data_id,
                                                                         DataVersion version);

[[nodiscard]] HGRAPH_FABRIC_EXPORT Port<FabricDiagnostics> diagnostics(Wiring &wiring,
                                                                        service::ServicePath path);
[[nodiscard]] HGRAPH_FABRIC_EXPORT Port<FabricDiagnostics> diagnostics(Wiring &wiring);
} // namespace hgraph::fabric

namespace hgraph::static_schema_detail
{
template <> struct scalar_name<fabric::FabricNotificationMode>
{
    static constexpr std::string_view value{"hgraph.fabric::FabricNotificationMode"};
};
} // namespace hgraph::static_schema_detail

#endif // HGRAPH_FABRIC_SERVICE_H
