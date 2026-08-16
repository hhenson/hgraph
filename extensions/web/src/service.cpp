#include <hgraph/web/service.h>
#include <hgraph/web/value_builders.h>

#include <hgraph/lib/std/operators/collection.h>
#include <hgraph/lib/std/operators/conversion.h>
#include <hgraph/lib/std/std_nodes.h>

namespace hgraph::web
{
    Port<WebRouteOutput> serve(Wiring &w, service::ServicePath path, Port<TS<WebRoute>> route) {
        return wire<HttpServeService>(w, std::move(path), std::move(route)).as<WebRouteOutput>();
    }

    Port<HttpRespondRequest> respond_request(Wiring &w, Port<TS<Int>> request_id, Port<TS<HttpResponse>> response) {
        return stdlib::to_tsb<HttpRespondRequest>(w, request_id, response);
    }

    Port<TS<WebDeliveryReport>> respond(Wiring &w, service::ServicePath path, Port<HttpRespondRequest> request) {
        return wire<HttpRespondService>(w, std::move(path), std::move(request)).as<TS<WebDeliveryReport>>();
    }

    Port<WsRouteOutput> ws_serve(Wiring &w, service::ServicePath path, Port<TS<WebRoute>> route) {
        return wire<WsServeService>(w, std::move(path), std::move(route)).as<WsRouteOutput>();
    }

    Port<WsSendRequest> ws_send_request(Wiring &w, Port<TS<Int>> connection_id, Port<TS<WsFrame>> frame) {
        return stdlib::to_tsb<WsSendRequest>(w, connection_id, frame);
    }

    Port<TS<WebDeliveryReport>> ws_send(Wiring &w, service::ServicePath path, Port<WsSendRequest> request) {
        return wire<WsSendService>(w, std::move(path), std::move(request)).as<TS<WebDeliveryReport>>();
    }

    Port<HttpClientCall> http_client_call(Wiring &w, Port<TS<HttpClientRequest>> request,
                                          Port<TS<HttpClientOptions>> options) {
        return stdlib::to_tsb<HttpClientCall>(w, request, options);
    }

    Port<HttpClientCall> http_client_call(Wiring &w, Port<TS<HttpClientRequest>> request) {
        auto options = wire<stdlib::const_, TS<HttpClientOptions>>(w, make_client_options());
        return http_client_call(w, std::move(request), std::move(options));
    }

    Port<HttpCallResult> http_request(Wiring &w, service::ServicePath path, Port<HttpClientCall> call) {
        return wire<HttpClientService>(w, std::move(path), std::move(call)).as<HttpCallResult>();
    }

    Port<WsClientOutput> ws_connect(Wiring &w, service::ServicePath path, Port<TS<WsClientKey>> key) {
        return wire<WsClientService>(w, std::move(path), std::move(key)).as<WsClientOutput>();
    }

    Port<WsClientSendRequest> ws_client_send_request(Wiring &w, Port<TS<WsClientKey>> key, Port<TS<WsFrame>> frame) {
        return stdlib::to_tsb<WsClientSendRequest>(w, key, frame);
    }

    Port<TS<WebDeliveryReport>> ws_client_send(Wiring &w, service::ServicePath path, Port<WsClientSendRequest> request) {
        return wire<WsClientSendService>(w, std::move(path), std::move(request)).as<TS<WebDeliveryReport>>();
    }

    Port<TS<WebEvent>> server_events(Wiring &w, service::ServicePath path) {
        return wire<WebServerEventService>(w, std::move(path)).as<TS<WebEvent>>();
    }

    Port<TS<WebEvent>> client_events(Wiring &w, service::ServicePath path) {
        return wire<WebClientEventService>(w, std::move(path)).as<TS<WebEvent>>();
    }

    Port<TS<WebServerStats>> server_stats(Wiring &w, service::ServicePath path) {
        return wire<WebServerStatsService>(w, std::move(path)).as<TS<WebServerStats>>();
    }

    Port<TS<WebClientStats>> client_stats(Wiring &w, service::ServicePath path) {
        return wire<WebClientStatsService>(w, std::move(path)).as<TS<WebClientStats>>();
    }
}  // namespace hgraph::web
