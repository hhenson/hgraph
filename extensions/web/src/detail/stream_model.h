#ifndef HGRAPH_WEB_DETAIL_STREAM_MODEL_H
#define HGRAPH_WEB_DETAIL_STREAM_MODEL_H

#include <hgraph/web/types.h>

namespace hgraph::web::detail
{
    // Internal cross-thread envelopes (RFC 0024, runtime architecture).  The
    // transports move fully owned Values of these schemas through the bridge;
    // the drain nodes unpack them into the public service outputs.  A set
    // `removed` field is a control record erasing the enclosing key.

    using WebRequestEnvelope =
        Bundle<"hgraph.web.internal::WebRequestEnvelope", Field<"route", WebRoute>, Field<"request", HttpServerRequest>,
               Field<"state", WebRouteState>, Field<"removed", Bool>>;

    using WsIngressEnvelope =
        Bundle<"hgraph.web.internal::WsIngressEnvelope", Field<"route", WebRoute>, Field<"event", WsEvent>,
               Field<"frame", WsInboundFrame>, Field<"removed", Bool>>;

    using WsClientEnvelope =
        Bundle<"hgraph.web.internal::WsClientEnvelope", Field<"key", WsClientKey>, Field<"event", WsEvent>,
               Field<"frame", WsFrame>, Field<"removed", Bool>>;

    // Exactly one of response/failure is set: transport failure is never
    // disguised as an HTTP status (RFC 0024).
    using WebResponseEnvelope =
        Bundle<"hgraph.web.internal::WebResponseEnvelope", Field<"request_id", Int>, Field<"response", HttpResponse>,
               Field<"failure", WebTransportError>>;

    using WebDeliveryEnvelope =
        Bundle<"hgraph.web.internal::WebDeliveryEnvelope", Field<"request_id", Int>, Field<"report", WebDeliveryReport>>;

    using WebEventEnvelope =
        Bundle<"hgraph.web.internal::WebEventEnvelope", Field<"event", WebEvent>, Field<"stop_graph", Bool>>;

    inline void register_internal_types() {
        static_cast<void>(scalar_descriptor<WebRequestEnvelope>::value_meta());
        static_cast<void>(scalar_descriptor<WsIngressEnvelope>::value_meta());
        static_cast<void>(scalar_descriptor<WsClientEnvelope>::value_meta());
        static_cast<void>(scalar_descriptor<WebResponseEnvelope>::value_meta());
        static_cast<void>(scalar_descriptor<WebDeliveryEnvelope>::value_meta());
        static_cast<void>(scalar_descriptor<WebEventEnvelope>::value_meta());
    }
}  // namespace hgraph::web::detail

#endif  // HGRAPH_WEB_DETAIL_STREAM_MODEL_H
