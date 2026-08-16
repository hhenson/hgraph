#ifndef HGRAPH_WEB_SERVICE_H
#define HGRAPH_WEB_SERVICE_H

#include <hgraph/web/export.h>
#include <hgraph/web/types.h>

#include <hgraph/types/graph_wiring.h>
#include <hgraph/types/service_wiring.h>

namespace hgraph::web
{
    // Server-side interfaces. Routes are subscription keys: the route set
    // reaches the transport as TSS deltas and requests come back keyed by
    // route (RFC 0024, routing section).
    struct HttpServeService
    {
        static constexpr std::string_view name{"web_http_serve"};
        using key_type     = WebRoute;
        using value_schema = WebRouteOutput;
    };

    struct HttpRespondService
    {
        static constexpr std::string_view name{"web_http_respond"};
        using request_schema  = HttpRespondRequest;
        using response_schema = TS<WebDeliveryReport>;
    };

    struct WsServeService
    {
        static constexpr std::string_view name{"web_ws_serve"};
        using key_type     = WebRoute;
        using value_schema = WsRouteOutput;
    };

    struct WsSendService
    {
        static constexpr std::string_view name{"web_ws_send"};
        using request_schema  = WsSendRequest;
        using response_schema = TS<WebDeliveryReport>;
    };

    struct WebServerEventService
    {
        static constexpr std::string_view name{"web_server_events"};
        using output_schema = TS<WebEvent>;
    };

    struct WebServerStatsService
    {
        static constexpr std::string_view name{"web_server_stats"};
        using output_schema = TS<WebServerStats>;
    };

    // Client-side interfaces.
    struct HttpClientService
    {
        static constexpr std::string_view name{"web_http_client"};
        using request_schema  = HttpClientCall;
        using response_schema = HttpCallResult;
    };

    struct WsClientService
    {
        static constexpr std::string_view name{"web_ws_client"};
        using key_type     = WsClientKey;
        using value_schema = WsClientOutput;
    };

    struct WsClientSendService
    {
        static constexpr std::string_view name{"web_ws_client_send"};
        using request_schema  = WsClientSendRequest;
        using response_schema = TS<WebDeliveryReport>;
    };

    struct WebClientEventService
    {
        static constexpr std::string_view name{"web_client_events"};
        using output_schema = TS<WebEvent>;
    };

    struct WebClientStatsService
    {
        static constexpr std::string_view name{"web_client_stats"};
        using output_schema = TS<WebClientStats>;
    };
}  // namespace hgraph::web

#endif  // HGRAPH_WEB_SERVICE_H
