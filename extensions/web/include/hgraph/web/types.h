#ifndef HGRAPH_WEB_TYPES_H
#define HGRAPH_WEB_TYPES_H

#include <hgraph/web/export.h>

#include <hgraph/types/primitive_types.h>
#include <hgraph/types/static_schema.h>

#include <cstdint>
#include <ostream>
#include <string_view>

namespace hgraph::web
{
    enum class HttpMethod : std::int64_t {
        Get,
        Head,
        Post,
        Put,
        Delete,
        Patch,
        Options,
        Trace,
    };

    enum class WebRouteState : std::int64_t {
        Starting,
        Serving,
        Removed,
        Failed,
    };

    enum class WsFrameKind : std::int64_t {
        Text,
        Binary,
        Ping,
        Pong,
        Close,
    };

    enum class WsConnectionState : std::int64_t {
        Open,
        Closing,
        Closed,
        Failed,
    };

    enum class WebDeliveryStatus : std::int64_t {
        EnqueueRejected,
        Dropped,
        Delivered,
        RetriableFailure,
        PermanentFailure,
    };

    enum class WebSeverity : std::int64_t {
        Debug,
        Info,
        Warning,
        Error,
        Fatal,
    };

    enum class WebInboundOverflow : std::int64_t {
        Backpressure,
        Reject,
    };

    enum class WebSlowConsumerPolicy : std::int64_t {
        Close,
        DropNewest,
    };

    enum class WebOverflowAction : std::int64_t {
        Fail,
        Drop,
        Stage,
    };

    enum class WebFailurePolicy : std::int64_t {
        Report,
        StopGraph,
    };

    enum class WebClientVerify : std::int64_t {
        None,
        Optional,
        Required,
    };

    enum class WebTlsVersion : std::int64_t {
        Tls1_2,
        Tls1_3,
    };

    enum class WebHttpVersionPolicy : std::int64_t {
        Auto,
        H1Only,
        H2Only,
    };

    namespace detail
    {
        [[nodiscard]] constexpr std::string_view enum_name(HttpMethod value) noexcept {
            switch (value) {
                case HttpMethod::Get: return "Get";
                case HttpMethod::Head: return "Head";
                case HttpMethod::Post: return "Post";
                case HttpMethod::Put: return "Put";
                case HttpMethod::Delete: return "Delete";
                case HttpMethod::Patch: return "Patch";
                case HttpMethod::Options: return "Options";
                case HttpMethod::Trace: return "Trace";
            }
            return "Unknown";
        }

        [[nodiscard]] constexpr std::string_view enum_name(WebRouteState value) noexcept {
            switch (value) {
                case WebRouteState::Starting: return "Starting";
                case WebRouteState::Serving: return "Serving";
                case WebRouteState::Removed: return "Removed";
                case WebRouteState::Failed: return "Failed";
            }
            return "Unknown";
        }

        [[nodiscard]] constexpr std::string_view enum_name(WsFrameKind value) noexcept {
            switch (value) {
                case WsFrameKind::Text: return "Text";
                case WsFrameKind::Binary: return "Binary";
                case WsFrameKind::Ping: return "Ping";
                case WsFrameKind::Pong: return "Pong";
                case WsFrameKind::Close: return "Close";
            }
            return "Unknown";
        }

        [[nodiscard]] constexpr std::string_view enum_name(WsConnectionState value) noexcept {
            switch (value) {
                case WsConnectionState::Open: return "Open";
                case WsConnectionState::Closing: return "Closing";
                case WsConnectionState::Closed: return "Closed";
                case WsConnectionState::Failed: return "Failed";
            }
            return "Unknown";
        }

        [[nodiscard]] constexpr std::string_view enum_name(WebDeliveryStatus value) noexcept {
            switch (value) {
                case WebDeliveryStatus::EnqueueRejected: return "EnqueueRejected";
                case WebDeliveryStatus::Dropped: return "Dropped";
                case WebDeliveryStatus::Delivered: return "Delivered";
                case WebDeliveryStatus::RetriableFailure: return "RetriableFailure";
                case WebDeliveryStatus::PermanentFailure: return "PermanentFailure";
            }
            return "Unknown";
        }

        [[nodiscard]] constexpr std::string_view enum_name(WebSeverity value) noexcept {
            switch (value) {
                case WebSeverity::Debug: return "Debug";
                case WebSeverity::Info: return "Info";
                case WebSeverity::Warning: return "Warning";
                case WebSeverity::Error: return "Error";
                case WebSeverity::Fatal: return "Fatal";
            }
            return "Unknown";
        }

        [[nodiscard]] constexpr std::string_view enum_name(WebInboundOverflow value) noexcept {
            switch (value) {
                case WebInboundOverflow::Backpressure: return "Backpressure";
                case WebInboundOverflow::Reject: return "Reject";
            }
            return "Unknown";
        }

        [[nodiscard]] constexpr std::string_view enum_name(WebSlowConsumerPolicy value) noexcept {
            switch (value) {
                case WebSlowConsumerPolicy::Close: return "Close";
                case WebSlowConsumerPolicy::DropNewest: return "DropNewest";
            }
            return "Unknown";
        }

        [[nodiscard]] constexpr std::string_view enum_name(WebOverflowAction value) noexcept {
            switch (value) {
                case WebOverflowAction::Fail: return "Fail";
                case WebOverflowAction::Drop: return "Drop";
                case WebOverflowAction::Stage: return "Stage";
            }
            return "Unknown";
        }

        [[nodiscard]] constexpr std::string_view enum_name(WebFailurePolicy value) noexcept {
            switch (value) {
                case WebFailurePolicy::Report: return "Report";
                case WebFailurePolicy::StopGraph: return "StopGraph";
            }
            return "Unknown";
        }

        [[nodiscard]] constexpr std::string_view enum_name(WebClientVerify value) noexcept {
            switch (value) {
                case WebClientVerify::None: return "None";
                case WebClientVerify::Optional: return "Optional";
                case WebClientVerify::Required: return "Required";
            }
            return "Unknown";
        }

        [[nodiscard]] constexpr std::string_view enum_name(WebTlsVersion value) noexcept {
            switch (value) {
                case WebTlsVersion::Tls1_2: return "Tls1_2";
                case WebTlsVersion::Tls1_3: return "Tls1_3";
            }
            return "Unknown";
        }

        [[nodiscard]] constexpr std::string_view enum_name(WebHttpVersionPolicy value) noexcept {
            switch (value) {
                case WebHttpVersionPolicy::Auto: return "Auto";
                case WebHttpVersionPolicy::H1Only: return "H1Only";
                case WebHttpVersionPolicy::H2Only: return "H2Only";
            }
            return "Unknown";
        }
    }  // namespace detail

    template <typename T>
        requires requires(T value) { detail::enum_name(value); }
    inline std::ostream &operator<<(std::ostream &stream, T value) {
        return stream << detail::enum_name(value);
    }
}  // namespace hgraph::web

namespace hgraph::static_schema_detail
{
    template <> struct scalar_name<web::HttpMethod>
    {
        static constexpr std::string_view value{"hgraph.web::HttpMethod"};
    };

    template <> struct scalar_name<web::WebRouteState>
    {
        static constexpr std::string_view value{"hgraph.web::WebRouteState"};
    };

    template <> struct scalar_name<web::WsFrameKind>
    {
        static constexpr std::string_view value{"hgraph.web::WsFrameKind"};
    };

    template <> struct scalar_name<web::WsConnectionState>
    {
        static constexpr std::string_view value{"hgraph.web::WsConnectionState"};
    };

    template <> struct scalar_name<web::WebDeliveryStatus>
    {
        static constexpr std::string_view value{"hgraph.web::WebDeliveryStatus"};
    };

    template <> struct scalar_name<web::WebSeverity>
    {
        static constexpr std::string_view value{"hgraph.web::WebSeverity"};
    };

    template <> struct scalar_name<web::WebInboundOverflow>
    {
        static constexpr std::string_view value{"hgraph.web::WebInboundOverflow"};
    };

    template <> struct scalar_name<web::WebSlowConsumerPolicy>
    {
        static constexpr std::string_view value{"hgraph.web::WebSlowConsumerPolicy"};
    };

    template <> struct scalar_name<web::WebOverflowAction>
    {
        static constexpr std::string_view value{"hgraph.web::WebOverflowAction"};
    };

    template <> struct scalar_name<web::WebFailurePolicy>
    {
        static constexpr std::string_view value{"hgraph.web::WebFailurePolicy"};
    };

    template <> struct scalar_name<web::WebClientVerify>
    {
        static constexpr std::string_view value{"hgraph.web::WebClientVerify"};
    };

    template <> struct scalar_name<web::WebTlsVersion>
    {
        static constexpr std::string_view value{"hgraph.web::WebTlsVersion"};
    };

    template <> struct scalar_name<web::WebHttpVersionPolicy>
    {
        static constexpr std::string_view value{"hgraph.web::WebHttpVersionPolicy"};
    };
}  // namespace hgraph::static_schema_detail

#if HGRAPH_ENABLE_PYTHON_USER_NODES
namespace hgraph
{
    #define HGRAPH_WEB_PYTHON_ENUM_CONVERSION(EnumType)                                                                            \
        template <> struct python_conversion_traits<web::EnumType>                                                                 \
        {                                                                                                                          \
            static nb::object    to_python(const web::EnumType &value) { return nb::cast(value); }                                 \
            static web::EnumType from_python(nb::handle source) {                                                                  \
                if (nb::hasattr(source, "value")) { source = source.attr("value"); }                                               \
                return static_cast<web::EnumType>(nb::cast<std::int64_t>(source));                                                 \
            }                                                                                                                      \
        }

    HGRAPH_WEB_PYTHON_ENUM_CONVERSION(HttpMethod);
    HGRAPH_WEB_PYTHON_ENUM_CONVERSION(WebRouteState);
    HGRAPH_WEB_PYTHON_ENUM_CONVERSION(WsFrameKind);
    HGRAPH_WEB_PYTHON_ENUM_CONVERSION(WsConnectionState);
    HGRAPH_WEB_PYTHON_ENUM_CONVERSION(WebDeliveryStatus);
    HGRAPH_WEB_PYTHON_ENUM_CONVERSION(WebSeverity);
    HGRAPH_WEB_PYTHON_ENUM_CONVERSION(WebInboundOverflow);
    HGRAPH_WEB_PYTHON_ENUM_CONVERSION(WebSlowConsumerPolicy);
    HGRAPH_WEB_PYTHON_ENUM_CONVERSION(WebOverflowAction);
    HGRAPH_WEB_PYTHON_ENUM_CONVERSION(WebFailurePolicy);
    HGRAPH_WEB_PYTHON_ENUM_CONVERSION(WebClientVerify);
    HGRAPH_WEB_PYTHON_ENUM_CONVERSION(WebTlsVersion);
    HGRAPH_WEB_PYTHON_ENUM_CONVERSION(WebHttpVersionPolicy);

    #undef HGRAPH_WEB_PYTHON_ENUM_CONVERSION
}  // namespace hgraph
#endif

namespace hgraph::web
{
    // Headers, query parameters, and path captures are ALWAYS ordered
    // sequences of name/value pairs, never maps: duplicate names and arrival
    // order are preserved by construction (RFC 0024, public value contract).
    using WebHeader = Bundle<"hgraph.web::WebHeader", Field<"name", Str>, Field<"value", Str>>;

    using WebParam = Bundle<"hgraph.web::WebParam", Field<"name", Str>, Field<"value", Str>>;

    using WebPeer = Bundle<"hgraph.web::WebPeer", Field<"remote_address", Str>, Field<"remote_port", Int>,
                           Field<"local_port", Int>, Field<"tls", Bool>, Field<"negotiated_protocol", Str>, Field<"sni", Str>,
                           Field<"client_cert_subject", Str>>;

    using WebRoute = Bundle<"hgraph.web::WebRoute", Field<"method", HttpMethod>, Field<"pattern", Str>, Field<"upgrade", Bool>>;

    using WebStaticFile =
        Bundle<"hgraph.web::WebStaticFile", Field<"url", Str>, Field<"file", Str>, Field<"content_type", Str>,
               Field<"cache_control", Str>>;

    using WebStaticDirectory =
        Bundle<"hgraph.web::WebStaticDirectory", Field<"url_prefix", Str>, Field<"directory", Str>,
               Field<"cache_control", Str>>;

    using HttpRequest = Bundle<"hgraph.web::HttpRequest", Field<"method", HttpMethod>, Field<"target", Str>, Field<"path", Str>,
                               Field<"query", HomogeneousTuple<WebParam>>, Field<"path_params", HomogeneousTuple<WebParam>>,
                               Field<"headers", HomogeneousTuple<WebHeader>>, Field<"body", Bytes>,
                               Field<"trailers", HomogeneousTuple<WebHeader>>>;

    using HttpServerRequest =
        Bundle<"hgraph.web::HttpServerRequest", Field<"request_id", Int>, Field<"connection_id", Int>, Field<"stream_id", Int>,
               Field<"request", HttpRequest>, Field<"peer", WebPeer>>;

    using HttpResponse = Bundle<"hgraph.web::HttpResponse", Field<"status", Int>, Field<"headers", HomogeneousTuple<WebHeader>>,
                                Field<"body", Bytes>, Field<"trailers", HomogeneousTuple<WebHeader>>>;

    using WebTransportError =
        Bundle<"hgraph.web::WebTransportError", Field<"error_code", Int>, Field<"message", Str>, Field<"retriable", Bool>>;

    using HttpClientRequest = Bundle<"hgraph.web::HttpClientRequest", Field<"method", HttpMethod>, Field<"url", Str>,
                                     Field<"headers", HomogeneousTuple<WebHeader>>, Field<"body", Bytes>>;

    using HttpClientOptions =
        Bundle<"hgraph.web::HttpClientOptions", Field<"connect_timeout_ms", Int>, Field<"request_timeout_ms", Int>,
               Field<"follow_redirects", Bool>, Field<"max_redirects", Int>, Field<"http_version", WebHttpVersionPolicy>>;

    using WsFrame = Bundle<"hgraph.web::WsFrame", Field<"kind", WsFrameKind>, Field<"text", Str>, Field<"data", Bytes>,
                           Field<"close_code", Int>, Field<"close_reason", Str>>;

    using WsInboundFrame = Bundle<"hgraph.web::WsInboundFrame", Field<"connection_id", Int>, Field<"frame", WsFrame>>;

    using WsEvent = Bundle<"hgraph.web::WsEvent", Field<"connection_id", Int>, Field<"state", WsConnectionState>,
                           Field<"request", HttpServerRequest>, Field<"close_code", Int>, Field<"close_reason", Str>>;

    using WsClientKey = Bundle<"hgraph.web::WsClientKey", Field<"url", Str>, Field<"headers", HomogeneousTuple<WebHeader>>,
                               Field<"subprotocols", HomogeneousTuple<Str>>>;

    using WebDeliveryReport =
        Bundle<"hgraph.web::WebDeliveryReport", Field<"request_id", Int>, Field<"sequence", Int>,
               Field<"status", WebDeliveryStatus>, Field<"error_code", Int>, Field<"retriable", Bool>, Field<"fatal", Bool>,
               Field<"message", Str>>;

    using WebEvent = Bundle<"hgraph.web::WebEvent", Field<"severity", WebSeverity>, Field<"component", Str>,
                            Field<"category", Str>, Field<"error_code", Int>, Field<"retriable", Bool>, Field<"fatal", Bool>,
                            Field<"service_path", Str>, Field<"connection_id", Int>, Field<"message", Str>>;

    using WebServerStats =
        Bundle<"hgraph.web::WebServerStats", Field<"listening_port", Int>, Field<"connection_count", Int>,
               Field<"ws_connection_count", Int>, Field<"pending_request_count", Int>, Field<"ingress_record_count", Int>,
               Field<"ingress_byte_count", Int>, Field<"outbound_byte_count", Int>, Field<"dropped_count", Int>>;

    using WebClientStats =
        Bundle<"hgraph.web::WebClientStats", Field<"in_flight_count", Int>, Field<"staged_count", Int>,
               Field<"ws_connection_count", Int>, Field<"ingress_record_count", Int>, Field<"ingress_byte_count", Int>,
               Field<"dropped_count", Int>>;

    using TlsServerConfig =
        Bundle<"hgraph.web::TlsServerConfig", Field<"cert_path", Str>, Field<"cert_pem", Str>, Field<"key_path", Str>,
               Field<"key_pem", Str>, Field<"key_password", Str>, Field<"ca_path", Str>, Field<"ca_pem", Str>,
               Field<"client_verify", WebClientVerify>, Field<"alpn", HomogeneousTuple<Str>>,
               Field<"min_version", WebTlsVersion>>;

    using TlsClientConfig =
        Bundle<"hgraph.web::TlsClientConfig", Field<"ca_path", Str>, Field<"ca_pem", Str>, Field<"cert_path", Str>,
               Field<"cert_pem", Str>, Field<"key_path", Str>, Field<"key_pem", Str>, Field<"sni", Str>,
               Field<"verify_peer", Bool>, Field<"verify_host", Bool>, Field<"alpn", HomogeneousTuple<Str>>,
               Field<"min_version", WebTlsVersion>>;

    // The h2_* fields are deliberately present before the server HTTP/2
    // session activates so activation is not a schema break (RFC 0024).
    // ws_max_frame_bytes caps OUTBOUND fragmentation; inbound reassembly is
    // bounded by ws_max_message_bytes (Beast exposes no per-frame inbound
    // cap — the HTTP/2 session owns its own framing when it activates).
    using WebServerConfig = Bundle<
        "hgraph.web::WebServerConfig", Field<"bind_address", Str>, Field<"port", Int>, Field<"tls", TlsServerConfig>,
        Field<"io_threads", Int>, Field<"max_connections", Int>, Field<"max_header_bytes", Int>, Field<"max_body_bytes", Int>,
        Field<"request_timeout_ms", Int>, Field<"idle_timeout_ms", Int>, Field<"keep_alive_timeout_ms", Int>,
        Field<"bind_deferred", Bool>, Field<"static_files", HomogeneousTuple<WebStaticFile>>,
        Field<"static_directories", HomogeneousTuple<WebStaticDirectory>>,
        Field<"ingress_record_limit", Int>, Field<"ingress_byte_limit", Int>, Field<"ws_ingress_record_limit", Int>,
        Field<"ws_ingress_byte_limit", Int>, Field<"watermark_high_pct", Int>, Field<"watermark_low_pct", Int>,
        Field<"inbound_overflow", WebInboundOverflow>, Field<"outbound_message_limit", Int>, Field<"outbound_byte_limit", Int>,
        Field<"slow_consumer_policy", WebSlowConsumerPolicy>, Field<"failure_policy", WebFailurePolicy>,
        Field<"shutdown_drain_timeout_ms", Int>, Field<"ws_max_frame_bytes", Int>, Field<"ws_max_message_bytes", Int>,
        Field<"ping_interval_ms", Int>, Field<"pong_timeout_ms", Int>, Field<"stats_interval_ms", Int>,
        Field<"h2_max_concurrent_streams", Int>, Field<"h2_initial_window_bytes", Int>>;

    using WebClientConfig = Bundle<
        "hgraph.web::WebClientConfig", Field<"http_version_policy", WebHttpVersionPolicy>,
        Field<"max_connections_per_host", Int>, Field<"max_total_connections", Int>, Field<"connect_timeout_ms", Int>,
        Field<"request_timeout_ms", Int>, Field<"keep_alive_ms", Int>, Field<"follow_redirects", Bool>,
        Field<"max_redirects", Int>, Field<"proxy", Str>, Field<"tls", TlsClientConfig>, Field<"max_response_bytes", Int>,
        Field<"ingress_record_limit", Int>, Field<"ingress_byte_limit", Int>, Field<"watermark_high_pct", Int>,
        Field<"watermark_low_pct", Int>, Field<"outbound_record_limit", Int>, Field<"outbound_byte_limit", Int>,
        Field<"overflow", WebOverflowAction>, Field<"stage_overflow", WebOverflowAction>,
        Field<"shutdown_drain_timeout_ms", Int>, Field<"failure_policy", WebFailurePolicy>,
        Field<"ws_ingress_record_limit", Int>, Field<"ws_ingress_byte_limit", Int>, Field<"ws_max_frame_bytes", Int>,
        Field<"ws_max_message_bytes", Int>, Field<"ping_interval_ms", Int>, Field<"pong_timeout_ms", Int>,
        Field<"stats_interval_ms", Int>>;

    using WebRouteOutput =
        TSB<"hgraph.web::WebRouteOutput", Field<"request", TS<HttpServerRequest>>, Field<"state", TS<WebRouteState>>>;

    using HttpRespondRequest =
        TSB<"hgraph.web::HttpRespondRequest", Field<"request_id", TS<Int>>, Field<"response", TS<HttpResponse>>>;

    using WsRouteOutput = TSB<"hgraph.web::WsRouteOutput", Field<"event", TS<WsEvent>>, Field<"frame", TS<WsInboundFrame>>>;

    using WsSendRequest = TSB<"hgraph.web::WsSendRequest", Field<"connection_id", TS<Int>>, Field<"frame", TS<WsFrame>>>;

    using HttpClientCall =
        TSB<"hgraph.web::HttpClientCall", Field<"request", TS<HttpClientRequest>>, Field<"options", TS<HttpClientOptions>>>;

    // Transport failure is never disguised as an HTTP status: the failure arm
    // is distinct from the response arm (RFC 0024).
    using HttpCallResult =
        TSB<"hgraph.web::HttpCallResult", Field<"response", TS<HttpResponse>>, Field<"failure", TS<WebTransportError>>>;

    using WsClientOutput = TSB<"hgraph.web::WsClientOutput", Field<"event", TS<WsEvent>>, Field<"frame", TS<WsFrame>>>;

    using WsClientSendRequest =
        TSB<"hgraph.web::WsClientSendRequest", Field<"key", TS<WsClientKey>>, Field<"frame", TS<WsFrame>>>;

    HGRAPH_WEB_EXPORT void register_web_types();
}  // namespace hgraph::web

#endif  // HGRAPH_WEB_TYPES_H
