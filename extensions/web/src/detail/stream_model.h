#ifndef HGRAPH_WEB_DETAIL_STREAM_MODEL_H
#define HGRAPH_WEB_DETAIL_STREAM_MODEL_H

#include <hgraph/web/types.h>

#include <cstdint>
#include <string_view>

namespace hgraph::web::detail {
// Internal transport payloads. External tasks wrap these fully owned
// values in WebTransportEvent streams; burst push sources deliver the
// pending events to graph nodes for projection onto the public service
// outputs. A set `removed` field is retained for compatibility
// with persisted/test payloads, although graph-side key deltas now own
// route and connection removal.

using WebRequestEnvelope =
    Bundle<"hgraph.web.internal::WebRequestEnvelope", Field<"route", WebRoute>,
           Field<"request", HttpServerRequest>, Field<"state", WebRouteState>,
           Field<"generation", DateTime>, Field<"removed", Bool>>;

using WsIngressEnvelope =
    Bundle<"hgraph.web.internal::WsIngressEnvelope", Field<"route", WebRoute>,
           Field<"event", WsEvent>, Field<"frame", WsInboundFrame>,
           Field<"generation", DateTime>, Field<"removed", Bool>>;

using WsClientEnvelope =
    Bundle<"hgraph.web.internal::WsClientEnvelope", Field<"key", WsClientKey>,
           Field<"event", WsEvent>, Field<"frame", WsFrame>,
           Field<"generation", DateTime>, Field<"removed", Bool>>;

// Exactly one of response/failure is set: transport failure is never
// disguised as an HTTP status (RFC 0024).
using WebResponseEnvelope =
    Bundle<"hgraph.web.internal::WebResponseEnvelope", Field<"request_id", Int>,
           Field<"response", HttpResponse>,
           Field<"failure", WebTransportError>>;

using WebDeliveryEnvelope =
    Bundle<"hgraph.web.internal::WebDeliveryEnvelope", Field<"request_id", Int>,
           Field<"report", WebDeliveryReport>>;

using WebEventEnvelope =
    Bundle<"hgraph.web.internal::WebEventEnvelope", Field<"event", WebEvent>,
           Field<"stop_graph", Bool>>;

/** The scalar event admitted to one independently ordered web channel.
 *  Each channel owns a burst queue of this schema; the kind selects the
 *  populated payload field. */
enum class WebTransportEventKind : std::int64_t {
  ServerRequest,
  ServerWsIngress,
  ServerRespondDelivery,
  ServerWsSendDelivery,
  ServerEvent,
  ServerStats,
  ClientResponse,
  ClientWsIngress,
  ClientSendDelivery,
  ClientEvent,
  ClientStats,
};

} // namespace hgraph::web::detail

namespace hgraph::static_schema_detail {
template <> struct scalar_name<web::detail::WebTransportEventKind> {
  static constexpr std::string_view value{
      "hgraph.web.internal::WebTransportEventKind"};
};
} // namespace hgraph::static_schema_detail

namespace hgraph::web::detail {
using WebTransportEvent = Bundle<
    "hgraph.web.internal::WebTransportEvent",
    Field<"kind", WebTransportEventKind>, Field<"request", WebRequestEnvelope>,
    Field<"server_ws", WsIngressEnvelope>, Field<"client_ws", WsClientEnvelope>,
    Field<"response", WebResponseEnvelope>,
    Field<"delivery", WebDeliveryEnvelope>, Field<"event", WebEventEnvelope>,
    Field<"server_stats", WebServerStats>,
    Field<"client_stats", WebClientStats>, Field<"channel", Int>,
    Field<"retained_bytes", Int>, Field<"control", Bool>>;

using WebTransportEventBatch = HomogeneousTuple<WebTransportEvent>;

inline void register_internal_types() {
  static_cast<void>(scalar_descriptor<WebRequestEnvelope>::value_meta());
  static_cast<void>(scalar_descriptor<WsIngressEnvelope>::value_meta());
  static_cast<void>(scalar_descriptor<WsClientEnvelope>::value_meta());
  static_cast<void>(scalar_descriptor<WebResponseEnvelope>::value_meta());
  static_cast<void>(scalar_descriptor<WebDeliveryEnvelope>::value_meta());
  static_cast<void>(scalar_descriptor<WebEventEnvelope>::value_meta());
  static_cast<void>(scalar_descriptor<WebTransportEvent>::value_meta());
  static_cast<void>(scalar_descriptor<WebTransportEventBatch>::value_meta());
}
} // namespace hgraph::web::detail

#endif // HGRAPH_WEB_DETAIL_STREAM_MODEL_H
