#ifndef HGRAPH_WEB_VALUE_BUILDERS_H
#define HGRAPH_WEB_VALUE_BUILDERS_H

#include <hgraph/web/export.h>
#include <hgraph/web/types.h>
#include <hgraph/types/value/value.h>

#include <chrono>
#include <optional>
#include <string_view>
#include <utility>
#include <vector>

namespace hgraph::web
{
    using WebHeaderInput = std::pair<Str, Str>;
    using WebParamInput  = std::pair<Str, Str>;

    class HGRAPH_WEB_EXPORT TlsServerConfigBuilder
    {
      public:
        TlsServerConfigBuilder &cert_path(Str value);
        TlsServerConfigBuilder &cert_pem(Str value);
        TlsServerConfigBuilder &key_path(Str value);
        TlsServerConfigBuilder &key_pem(Str value);
        TlsServerConfigBuilder &key_password(Str value);
        TlsServerConfigBuilder &ca_path(Str value);
        TlsServerConfigBuilder &ca_pem(Str value);
        TlsServerConfigBuilder &client_verify(WebClientVerify value);
        TlsServerConfigBuilder &alpn(std::vector<Str> value);
        TlsServerConfigBuilder &min_version(WebTlsVersion value);

        [[nodiscard]] Value build() const;

      private:
        Str              cert_path_{};
        Str              cert_pem_{};
        Str              key_path_{};
        Str              key_pem_{};
        Str              key_password_{};
        Str              ca_path_{};
        Str              ca_pem_{};
        WebClientVerify  client_verify_{WebClientVerify::None};
        std::vector<Str> alpn_{};
        WebTlsVersion    min_version_{WebTlsVersion::Tls1_2};
    };

    [[nodiscard]] HGRAPH_WEB_EXPORT TlsServerConfigBuilder tls_server();

    class HGRAPH_WEB_EXPORT TlsClientConfigBuilder
    {
      public:
        TlsClientConfigBuilder &ca_path(Str value);
        TlsClientConfigBuilder &ca_pem(Str value);
        TlsClientConfigBuilder &cert_path(Str value);
        TlsClientConfigBuilder &cert_pem(Str value);
        TlsClientConfigBuilder &key_path(Str value);
        TlsClientConfigBuilder &key_pem(Str value);
        TlsClientConfigBuilder &sni(Str value);
        TlsClientConfigBuilder &verify_peer(bool value);
        TlsClientConfigBuilder &verify_host(bool value);
        TlsClientConfigBuilder &alpn(std::vector<Str> value);
        TlsClientConfigBuilder &min_version(WebTlsVersion value);

        [[nodiscard]] Value build() const;

      private:
        Str              ca_path_{};
        Str              ca_pem_{};
        Str              cert_path_{};
        Str              cert_pem_{};
        Str              key_path_{};
        Str              key_pem_{};
        Str              sni_{};
        bool             verify_peer_{true};
        bool             verify_host_{true};
        std::vector<Str> alpn_{};
        WebTlsVersion    min_version_{WebTlsVersion::Tls1_2};
    };

    [[nodiscard]] HGRAPH_WEB_EXPORT TlsClientConfigBuilder tls_client();

    class HGRAPH_WEB_EXPORT ServerConfigBuilder
    {
      public:
        ServerConfigBuilder &bind_address(Str value);
        ServerConfigBuilder &port(Int value);
        ServerConfigBuilder &tls(Value value);
        ServerConfigBuilder &io_threads(Int value);
        ServerConfigBuilder &max_connections(Int value);
        ServerConfigBuilder &max_header_bytes(Int value);
        ServerConfigBuilder &max_body_bytes(Int value);
        ServerConfigBuilder &request_timeout(std::chrono::milliseconds value);
        ServerConfigBuilder &idle_timeout(std::chrono::milliseconds value);
        ServerConfigBuilder &keep_alive_timeout(std::chrono::milliseconds value);
        ServerConfigBuilder &bind_deferred(bool value);
        ServerConfigBuilder &ingress_limits(Int records, Int bytes);
        ServerConfigBuilder &ws_ingress_limits(Int records, Int bytes);
        ServerConfigBuilder &watermarks(Int high_pct, Int low_pct);
        ServerConfigBuilder &inbound_overflow(WebInboundOverflow value);
        ServerConfigBuilder &outbound_limits(Int messages, Int bytes);
        ServerConfigBuilder &slow_consumer_policy(WebSlowConsumerPolicy value);
        ServerConfigBuilder &failure_policy(WebFailurePolicy value);
        ServerConfigBuilder &shutdown_drain_timeout(std::chrono::milliseconds value);
        ServerConfigBuilder &ws_frame_limits(Int frame_bytes, Int message_bytes);
        ServerConfigBuilder &ping_interval(std::chrono::milliseconds value);
        ServerConfigBuilder &pong_timeout(std::chrono::milliseconds value);
        ServerConfigBuilder &stats_interval(std::chrono::milliseconds value);

        [[nodiscard]] Value build() const;

      private:
        Str                   bind_address_{"0.0.0.0"};
        Int                   port_{0};
        Value                 tls_{};
        Int                   io_threads_{1};
        Int                   max_connections_{10'000};
        Int                   max_header_bytes_{64 * 1024};
        Int                   max_body_bytes_{16 * 1024 * 1024};
        Int                   request_timeout_ms_{30'000};
        Int                   idle_timeout_ms_{60'000};
        Int                   keep_alive_timeout_ms_{15'000};
        bool                  bind_deferred_{false};
        Int                   ingress_record_limit_{10'000};
        Int                   ingress_byte_limit_{64 * 1024 * 1024};
        Int                   ws_ingress_record_limit_{10'000};
        Int                   ws_ingress_byte_limit_{64 * 1024 * 1024};
        Int                   watermark_high_pct_{80};
        Int                   watermark_low_pct_{50};
        WebInboundOverflow    inbound_overflow_{WebInboundOverflow::Backpressure};
        Int                   outbound_message_limit_{1'000};
        Int                   outbound_byte_limit_{16 * 1024 * 1024};
        WebSlowConsumerPolicy slow_consumer_policy_{WebSlowConsumerPolicy::Close};
        WebFailurePolicy      failure_policy_{WebFailurePolicy::Report};
        Int                   shutdown_drain_timeout_ms_{5'000};
        Int                   ws_max_frame_bytes_{1024 * 1024};
        Int                   ws_max_message_bytes_{16 * 1024 * 1024};
        Int                   ping_interval_ms_{30'000};
        Int                   pong_timeout_ms_{10'000};
        Int                   stats_interval_ms_{0};
        Int                   h2_max_concurrent_streams_{100};
        Int                   h2_initial_window_bytes_{1024 * 1024};
    };

    [[nodiscard]] HGRAPH_WEB_EXPORT ServerConfigBuilder server_config();

    class HGRAPH_WEB_EXPORT ClientConfigBuilder
    {
      public:
        ClientConfigBuilder &http_version_policy(WebHttpVersionPolicy value);
        ClientConfigBuilder &max_connections_per_host(Int value);
        ClientConfigBuilder &max_total_connections(Int value);
        ClientConfigBuilder &connect_timeout(std::chrono::milliseconds value);
        ClientConfigBuilder &request_timeout(std::chrono::milliseconds value);
        ClientConfigBuilder &keep_alive(std::chrono::milliseconds value);
        ClientConfigBuilder &follow_redirects(bool value, Int max_redirects = 5);
        ClientConfigBuilder &proxy(Str value);
        ClientConfigBuilder &tls(Value value);
        ClientConfigBuilder &max_response_bytes(Int value);
        ClientConfigBuilder &ingress_limits(Int records, Int bytes);
        ClientConfigBuilder &watermarks(Int high_pct, Int low_pct);
        ClientConfigBuilder &outbound_limits(Int records, Int bytes);
        ClientConfigBuilder &outbound_overflow(WebOverflowAction value, WebOverflowAction stage_full = WebOverflowAction::Fail);
        ClientConfigBuilder &shutdown_drain_timeout(std::chrono::milliseconds value);
        ClientConfigBuilder &failure_policy(WebFailurePolicy value);
        ClientConfigBuilder &ws_ingress_limits(Int records, Int bytes);
        ClientConfigBuilder &ws_frame_limits(Int frame_bytes, Int message_bytes);
        ClientConfigBuilder &ping_interval(std::chrono::milliseconds value);
        ClientConfigBuilder &pong_timeout(std::chrono::milliseconds value);
        ClientConfigBuilder &stats_interval(std::chrono::milliseconds value);

        [[nodiscard]] Value build() const;

      private:
        WebHttpVersionPolicy http_version_policy_{WebHttpVersionPolicy::Auto};
        Int                  max_connections_per_host_{6};
        Int                  max_total_connections_{64};
        Int                  connect_timeout_ms_{10'000};
        Int                  request_timeout_ms_{30'000};
        Int                  keep_alive_ms_{60'000};
        bool                 follow_redirects_{true};
        Int                  max_redirects_{5};
        Str                  proxy_{};
        Value                tls_{};
        Int                  max_response_bytes_{16 * 1024 * 1024};
        Int                  ingress_record_limit_{10'000};
        Int                  ingress_byte_limit_{64 * 1024 * 1024};
        Int                  watermark_high_pct_{80};
        Int                  watermark_low_pct_{50};
        Int                  outbound_record_limit_{10'000};
        Int                  outbound_byte_limit_{64 * 1024 * 1024};
        WebOverflowAction    overflow_{WebOverflowAction::Stage};
        WebOverflowAction    stage_overflow_{WebOverflowAction::Fail};
        Int                  shutdown_drain_timeout_ms_{5'000};
        WebFailurePolicy     failure_policy_{WebFailurePolicy::Report};
        Int                  ws_ingress_record_limit_{10'000};
        Int                  ws_ingress_byte_limit_{64 * 1024 * 1024};
        Int                  ws_max_frame_bytes_{1024 * 1024};
        Int                  ws_max_message_bytes_{16 * 1024 * 1024};
        Int                  ping_interval_ms_{30'000};
        Int                  pong_timeout_ms_{10'000};
        Int                  stats_interval_ms_{0};
    };

    [[nodiscard]] HGRAPH_WEB_EXPORT ClientConfigBuilder client_config();

    [[nodiscard]] HGRAPH_WEB_EXPORT Value make_route(HttpMethod method, Str pattern, bool upgrade = false);

    [[nodiscard]] HGRAPH_WEB_EXPORT Value make_headers(std::vector<WebHeaderInput> values);

    [[nodiscard]] HGRAPH_WEB_EXPORT Value make_params(std::vector<WebParamInput> values);

    [[nodiscard]] HGRAPH_WEB_EXPORT Value make_request(HttpMethod method, Str target, Str path,
                                                       std::vector<WebParamInput>  query   = {},
                                                       std::vector<WebParamInput>  params  = {},
                                                       std::vector<WebHeaderInput> headers = {}, Bytes body = {},
                                                       std::vector<WebHeaderInput> trailers = {});

    [[nodiscard]] HGRAPH_WEB_EXPORT Value make_response(Int status, std::vector<WebHeaderInput> headers = {}, Bytes body = {},
                                                        std::vector<WebHeaderInput> trailers = {});

    [[nodiscard]] HGRAPH_WEB_EXPORT Value make_client_request(HttpMethod method, Str url,
                                                              std::vector<WebHeaderInput> headers = {}, Bytes body = {});

    [[nodiscard]] HGRAPH_WEB_EXPORT Value make_transport_error(Int error_code, Str message, Bool retriable = false);

    [[nodiscard]] HGRAPH_WEB_EXPORT Value make_text_frame(Str text);

    [[nodiscard]] HGRAPH_WEB_EXPORT Value make_binary_frame(Bytes data);

    [[nodiscard]] HGRAPH_WEB_EXPORT Value make_close_frame(Int close_code = 1000, Str close_reason = {});

    [[nodiscard]] HGRAPH_WEB_EXPORT Value make_ws_client_key(Str url, std::vector<WebHeaderInput> headers = {},
                                                             std::vector<Str> subprotocols = {});

    [[nodiscard]] HGRAPH_WEB_EXPORT Value make_delivery_report(Int request_id, Int sequence, WebDeliveryStatus status,
                                                               Int error_code = 0, Bool retriable = false, Bool fatal = false,
                                                               Str message = {});

    [[nodiscard]] HGRAPH_WEB_EXPORT Value make_event(WebSeverity severity, Str component, Str category, Str service_path,
                                                     Str message, Int error_code = 0, Bool retriable = false,
                                                     Bool fatal = false, Int connection_id = 0);
}  // namespace hgraph::web

#endif  // HGRAPH_WEB_VALUE_BUILDERS_H
