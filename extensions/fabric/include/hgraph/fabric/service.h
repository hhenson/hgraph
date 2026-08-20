#ifndef HGRAPH_FABRIC_SERVICE_H
#define HGRAPH_FABRIC_SERVICE_H

#include <hgraph/fabric/export.h>
#include <hgraph/fabric/types.h>

#include <hgraph/types/frame.h>
#include <hgraph/types/graph_wiring.h>
#include <hgraph/types/service_wiring.h>

#include <string_view>

namespace hgraph::fabric
{
inline constexpr std::string_view DEFAULT_SERVICE_PATH{"fabric"};

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
    using output_schema = TSD<Str, TS<Str>>;
};

/** Register one lazy root-graph FabricServiceImpl singleton. Configuration
    is read from GlobalState during service start. This host operation is
    intentionally separate from subscribe_data clients. */
HGRAPH_FABRIC_EXPORT void register_service(Wiring &wiring, service::ServicePath path);
HGRAPH_FABRIC_EXPORT void register_service(Wiring &wiring);

/** Feed a complete accepted revision into the registered service through
    the ordinary request edge. */
HGRAPH_FABRIC_EXPORT void submit_notice(Wiring &wiring, Port<TS<DataRevision>> notice, service::ServicePath path);
HGRAPH_FABRIC_EXPORT void submit_notice(Wiring &wiring, Port<TS<DataRevision>> notice);

[[nodiscard]] HGRAPH_FABRIC_EXPORT Port<FabricLoadResponse> request_load(Wiring &wiring, Str data_id,
                                                                         DataVersion version,
                                                                         service::ServicePath path);
[[nodiscard]] HGRAPH_FABRIC_EXPORT Port<FabricLoadResponse> request_load(Wiring &wiring, Str data_id,
                                                                         DataVersion version);

[[nodiscard]] HGRAPH_FABRIC_EXPORT Port<TSD<Str, TS<Str>>> diagnostics(Wiring &wiring, service::ServicePath path);
[[nodiscard]] HGRAPH_FABRIC_EXPORT Port<TSD<Str, TS<Str>>> diagnostics(Wiring &wiring);
} // namespace hgraph::fabric

#endif // HGRAPH_FABRIC_SERVICE_H
