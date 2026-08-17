#include <hgraph/web/value_builders.h>

#include <hgraph/types/metadata/value_plan_factory.h>
#include <hgraph/types/value/value_builder.h>

#include <algorithm>
#include <initializer_list>
#include <stdexcept>
#include <string>
#include <utility>

namespace hgraph::web
{
    namespace
    {
        template <typename T> [[nodiscard]] Value atomic(T value) {
            static_cast<void>(scalar_descriptor<T>::value_meta());
            return Value{std::move(value)};
        }

        template <typename Schema> [[nodiscard]] Value bundle(std::vector<std::pair<std::string_view, Value>> fields) {
            const auto   *meta = scalar_descriptor<Schema>::value_meta();
            BundleBuilder builder{ValuePlanFactory::instance().type_for(meta)};
            for (auto &[name, field] : fields) {
                if (field.has_value()) { builder.set(name, std::move(field)); }
            }
            return builder.build();
        }

        template <typename Element> [[nodiscard]] Value homogeneous_tuple(std::vector<Value> values) {
            const auto element_binding = ValuePlanFactory::instance().type_for(scalar_descriptor<Element>::value_meta());
            const auto tuple_binding =
                ValuePlanFactory::instance().type_for(scalar_descriptor<HomogeneousTuple<Element>>::value_meta());
            if (!element_binding || !tuple_binding) { throw std::logic_error("Web tuple schema did not resolve"); }

            ListBuilder builder{element_binding};
            for (const Value &value : values) {
                if (value.schema() != element_binding.schema()) {
                    throw std::invalid_argument("Web tuple element schema mismatch");
                }
                builder.push_back_copy(value.view().data());
            }
            ListStorage storage = builder.build_storage();
            return Value{tuple_binding, &storage};
        }

        [[nodiscard]] Value strings(std::vector<Str> values) {
            static_cast<void>(scalar_descriptor<Str>::value_meta());
            std::vector<Value> erased;
            erased.reserve(values.size());
            for (auto &value : values) { erased.push_back(atomic(std::move(value))); }
            return homogeneous_tuple<Str>(std::move(erased));
        }

        template <typename Schema>
        [[nodiscard]] Value name_value_tuple(std::vector<std::pair<Str, Str>> values, std::string_view what) {
            std::vector<Value> erased;
            erased.reserve(values.size());
            for (auto &[name, value] : values) {
                if (name.empty()) { throw std::invalid_argument(std::string{"Web "} + std::string{what} + " names cannot be empty"); }
                erased.push_back(bundle<Schema>({
                    {"name", atomic(std::move(name))},
                    {"value", atomic(std::move(value))},
                }));
            }
            return homogeneous_tuple<Schema>(std::move(erased));
        }

        void require_path_xor_pem(const Str &path, const Str &pem, std::string_view what) {
            if (!path.empty() && !pem.empty()) {
                throw std::invalid_argument(std::string{"Web TLS "} + std::string{what} +
                                            " accepts a filesystem path or inline PEM, not both");
            }
        }

        void require_pct_watermarks(Int high, Int low) {
            if (low <= 0 || high >= 100 || low >= high) {
                throw std::invalid_argument("Web watermarks require 0 < low < high < 100");
            }
        }

        void require_positive(Int value, std::string_view what) {
            if (value <= 0) { throw std::invalid_argument(std::string{"Web "} + std::string{what} + " must be positive"); }
        }

        void require_non_negative(Int value, std::string_view what) {
            if (value < 0) { throw std::invalid_argument(std::string{"Web "} + std::string{what} + " cannot be negative"); }
        }

        void require_tls_schema(const Value &value, const void *expected, std::string_view what) {
            if (value.has_value() && value.schema() != expected) {
                throw std::invalid_argument(std::string{"Web "} + std::string{what} + " has the wrong schema");
            }
        }
    }  // namespace

    TlsServerConfigBuilder &TlsServerConfigBuilder::cert_path(Str value) {
        cert_path_ = std::move(value);
        return *this;
    }

    TlsServerConfigBuilder &TlsServerConfigBuilder::cert_pem(Str value) {
        cert_pem_ = std::move(value);
        return *this;
    }

    TlsServerConfigBuilder &TlsServerConfigBuilder::key_path(Str value) {
        key_path_ = std::move(value);
        return *this;
    }

    TlsServerConfigBuilder &TlsServerConfigBuilder::key_pem(Str value) {
        key_pem_ = std::move(value);
        return *this;
    }

    TlsServerConfigBuilder &TlsServerConfigBuilder::key_password(Str value) {
        key_password_ = std::move(value);
        return *this;
    }

    TlsServerConfigBuilder &TlsServerConfigBuilder::ca_path(Str value) {
        ca_path_ = std::move(value);
        return *this;
    }

    TlsServerConfigBuilder &TlsServerConfigBuilder::ca_pem(Str value) {
        ca_pem_ = std::move(value);
        return *this;
    }

    TlsServerConfigBuilder &TlsServerConfigBuilder::client_verify(WebClientVerify value) {
        client_verify_ = value;
        return *this;
    }

    TlsServerConfigBuilder &TlsServerConfigBuilder::alpn(std::vector<Str> value) {
        alpn_ = std::move(value);
        return *this;
    }

    TlsServerConfigBuilder &TlsServerConfigBuilder::min_version(WebTlsVersion value) {
        min_version_ = value;
        return *this;
    }

    Value TlsServerConfigBuilder::build() const {
        register_web_types();
        require_path_xor_pem(cert_path_, cert_pem_, "certificate");
        require_path_xor_pem(key_path_, key_pem_, "key");
        require_path_xor_pem(ca_path_, ca_pem_, "CA");
        if (cert_path_.empty() && cert_pem_.empty()) {
            throw std::invalid_argument("Web server TLS requires a certificate chain");
        }
        if (key_path_.empty() && key_pem_.empty()) { throw std::invalid_argument("Web server TLS requires a private key"); }
        if (client_verify_ != WebClientVerify::None && ca_path_.empty() && ca_pem_.empty()) {
            throw std::invalid_argument("Web client-certificate verification requires a CA");
        }
        // The HTTP/2 session is active behind the ALPN seam (RFC 0024,
        // activation plan): "h2" is a legal server protocol.  Only known
        // protocols may be advertised.
        if (std::ranges::any_of(alpn_, [](const Str &protocol) {
                return protocol != "h2" && protocol != "http/1.1";
            })) {
            throw std::invalid_argument("Web server ALPN accepts only \"h2\" and \"http/1.1\"");
        }
        std::vector<std::pair<std::string_view, Value>> fields{
            {"client_verify", atomic(client_verify_)},
            {"min_version", atomic(min_version_)},
            {"alpn", strings(alpn_.empty() ? std::vector<Str>{"http/1.1"} : alpn_)},
        };
        if (!cert_path_.empty()) { fields.emplace_back("cert_path", atomic(cert_path_)); }
        if (!cert_pem_.empty()) { fields.emplace_back("cert_pem", atomic(cert_pem_)); }
        if (!key_path_.empty()) { fields.emplace_back("key_path", atomic(key_path_)); }
        if (!key_pem_.empty()) { fields.emplace_back("key_pem", atomic(key_pem_)); }
        if (!key_password_.empty()) { fields.emplace_back("key_password", atomic(key_password_)); }
        if (!ca_path_.empty()) { fields.emplace_back("ca_path", atomic(ca_path_)); }
        if (!ca_pem_.empty()) { fields.emplace_back("ca_pem", atomic(ca_pem_)); }
        return bundle<TlsServerConfig>(std::move(fields));
    }

    TlsServerConfigBuilder tls_server() { return {}; }

    TlsClientConfigBuilder &TlsClientConfigBuilder::ca_path(Str value) {
        ca_path_ = std::move(value);
        return *this;
    }

    TlsClientConfigBuilder &TlsClientConfigBuilder::ca_pem(Str value) {
        ca_pem_ = std::move(value);
        return *this;
    }

    TlsClientConfigBuilder &TlsClientConfigBuilder::cert_path(Str value) {
        cert_path_ = std::move(value);
        return *this;
    }

    TlsClientConfigBuilder &TlsClientConfigBuilder::cert_pem(Str value) {
        cert_pem_ = std::move(value);
        return *this;
    }

    TlsClientConfigBuilder &TlsClientConfigBuilder::key_path(Str value) {
        key_path_ = std::move(value);
        return *this;
    }

    TlsClientConfigBuilder &TlsClientConfigBuilder::key_pem(Str value) {
        key_pem_ = std::move(value);
        return *this;
    }

    TlsClientConfigBuilder &TlsClientConfigBuilder::sni(Str value) {
        sni_ = std::move(value);
        return *this;
    }

    TlsClientConfigBuilder &TlsClientConfigBuilder::verify_peer(bool value) {
        verify_peer_ = value;
        return *this;
    }

    TlsClientConfigBuilder &TlsClientConfigBuilder::verify_host(bool value) {
        verify_host_ = value;
        return *this;
    }

    TlsClientConfigBuilder &TlsClientConfigBuilder::alpn(std::vector<Str> value) {
        alpn_ = std::move(value);
        return *this;
    }

    TlsClientConfigBuilder &TlsClientConfigBuilder::min_version(WebTlsVersion value) {
        min_version_ = value;
        return *this;
    }

    Value TlsClientConfigBuilder::build() const {
        register_web_types();
        require_path_xor_pem(ca_path_, ca_pem_, "CA");
        require_path_xor_pem(cert_path_, cert_pem_, "certificate");
        require_path_xor_pem(key_path_, key_pem_, "key");
        const bool has_cert = !cert_path_.empty() || !cert_pem_.empty();
        const bool has_key  = !key_path_.empty() || !key_pem_.empty();
        if (has_cert != has_key) {
            throw std::invalid_argument("Web client mTLS requires both a certificate and a private key");
        }
        std::vector<std::pair<std::string_view, Value>> fields{
            {"verify_peer", atomic(Bool{verify_peer_})},
            {"verify_host", atomic(Bool{verify_host_})},
            {"min_version", atomic(min_version_)},
        };
        if (!alpn_.empty()) { fields.emplace_back("alpn", strings(alpn_)); }
        if (!ca_path_.empty()) { fields.emplace_back("ca_path", atomic(ca_path_)); }
        if (!ca_pem_.empty()) { fields.emplace_back("ca_pem", atomic(ca_pem_)); }
        if (!cert_path_.empty()) { fields.emplace_back("cert_path", atomic(cert_path_)); }
        if (!cert_pem_.empty()) { fields.emplace_back("cert_pem", atomic(cert_pem_)); }
        if (!key_path_.empty()) { fields.emplace_back("key_path", atomic(key_path_)); }
        if (!key_pem_.empty()) { fields.emplace_back("key_pem", atomic(key_pem_)); }
        if (!sni_.empty()) { fields.emplace_back("sni", atomic(sni_)); }
        return bundle<TlsClientConfig>(std::move(fields));
    }

    TlsClientConfigBuilder tls_client() { return {}; }

    ServerConfigBuilder &ServerConfigBuilder::bind_address(Str value) {
        bind_address_ = std::move(value);
        return *this;
    }

    ServerConfigBuilder &ServerConfigBuilder::port(Int value) {
        port_ = value;
        return *this;
    }

    ServerConfigBuilder &ServerConfigBuilder::tls(Value value) {
        tls_ = std::move(value);
        return *this;
    }

    ServerConfigBuilder &ServerConfigBuilder::io_threads(Int value) {
        io_threads_ = value;
        return *this;
    }

    ServerConfigBuilder &ServerConfigBuilder::max_connections(Int value) {
        max_connections_ = value;
        return *this;
    }

    ServerConfigBuilder &ServerConfigBuilder::max_header_bytes(Int value) {
        max_header_bytes_ = value;
        return *this;
    }

    ServerConfigBuilder &ServerConfigBuilder::max_body_bytes(Int value) {
        max_body_bytes_ = value;
        return *this;
    }

    ServerConfigBuilder &ServerConfigBuilder::request_timeout(std::chrono::milliseconds value) {
        request_timeout_ms_ = value.count();
        return *this;
    }

    ServerConfigBuilder &ServerConfigBuilder::idle_timeout(std::chrono::milliseconds value) {
        idle_timeout_ms_ = value.count();
        return *this;
    }

    ServerConfigBuilder &ServerConfigBuilder::keep_alive_timeout(std::chrono::milliseconds value) {
        keep_alive_timeout_ms_ = value.count();
        return *this;
    }

    ServerConfigBuilder &ServerConfigBuilder::bind_deferred(bool value) {
        bind_deferred_ = value;
        return *this;
    }

    ServerConfigBuilder &ServerConfigBuilder::ingress_limits(Int records, Int bytes) {
        ingress_record_limit_ = records;
        ingress_byte_limit_   = bytes;
        return *this;
    }

    ServerConfigBuilder &ServerConfigBuilder::ws_ingress_limits(Int records, Int bytes) {
        ws_ingress_record_limit_ = records;
        ws_ingress_byte_limit_   = bytes;
        return *this;
    }

    ServerConfigBuilder &ServerConfigBuilder::watermarks(Int high_pct, Int low_pct) {
        watermark_high_pct_ = high_pct;
        watermark_low_pct_  = low_pct;
        return *this;
    }

    ServerConfigBuilder &ServerConfigBuilder::inbound_overflow(WebInboundOverflow value) {
        inbound_overflow_ = value;
        return *this;
    }

    ServerConfigBuilder &ServerConfigBuilder::outbound_limits(Int messages, Int bytes) {
        outbound_message_limit_ = messages;
        outbound_byte_limit_    = bytes;
        return *this;
    }

    ServerConfigBuilder &ServerConfigBuilder::slow_consumer_policy(WebSlowConsumerPolicy value) {
        slow_consumer_policy_ = value;
        return *this;
    }

    ServerConfigBuilder &ServerConfigBuilder::failure_policy(WebFailurePolicy value) {
        failure_policy_ = value;
        return *this;
    }

    ServerConfigBuilder &ServerConfigBuilder::shutdown_drain_timeout(std::chrono::milliseconds value) {
        shutdown_drain_timeout_ms_ = value.count();
        return *this;
    }

    ServerConfigBuilder &ServerConfigBuilder::ws_frame_limits(Int frame_bytes, Int message_bytes) {
        ws_max_frame_bytes_   = frame_bytes;
        ws_max_message_bytes_ = message_bytes;
        return *this;
    }

    ServerConfigBuilder &ServerConfigBuilder::ping_interval(std::chrono::milliseconds value) {
        ping_interval_ms_ = value.count();
        return *this;
    }

    ServerConfigBuilder &ServerConfigBuilder::pong_timeout(std::chrono::milliseconds value) {
        pong_timeout_ms_ = value.count();
        return *this;
    }

    ServerConfigBuilder &ServerConfigBuilder::stats_interval(std::chrono::milliseconds value) {
        stats_interval_ms_ = value.count();
        return *this;
    }

    Value ServerConfigBuilder::build() const {
        register_web_types();
        if (bind_address_.empty()) { throw std::invalid_argument("Web server bind address cannot be empty"); }
        if (port_ < 0 || port_ > 65'535) { throw std::invalid_argument("Web server port must be 0..65535"); }
        require_positive(io_threads_, "io_threads");
        require_positive(max_connections_, "max_connections");
        require_positive(max_header_bytes_, "max_header_bytes");
        require_positive(max_body_bytes_, "max_body_bytes");
        require_non_negative(request_timeout_ms_, "request timeout");
        require_non_negative(idle_timeout_ms_, "idle timeout");
        require_non_negative(keep_alive_timeout_ms_, "keep-alive timeout");
        require_positive(ingress_record_limit_, "ingress record limit");
        require_positive(ingress_byte_limit_, "ingress byte limit");
        require_positive(ws_ingress_record_limit_, "WS ingress record limit");
        require_positive(ws_ingress_byte_limit_, "WS ingress byte limit");
        require_pct_watermarks(watermark_high_pct_, watermark_low_pct_);
        require_positive(outbound_message_limit_, "outbound message limit");
        require_positive(outbound_byte_limit_, "outbound byte limit");
        require_non_negative(shutdown_drain_timeout_ms_, "shutdown drain timeout");
        require_positive(ws_max_frame_bytes_, "WS frame limit");
        require_positive(ws_max_message_bytes_, "WS message limit");
        require_non_negative(ping_interval_ms_, "ping interval");
        require_non_negative(pong_timeout_ms_, "pong timeout");
        require_non_negative(stats_interval_ms_, "stats interval");
        // A single maximal payload must always fit an empty ingress channel;
        // otherwise Backpressure would hold it forever (transport invariant,
        // mirrored in the runtime's config parse).
        if (ingress_byte_limit_ < max_body_bytes_ + max_header_bytes_ + 512) {
            throw std::invalid_argument(
                "Web ingress_byte_limit must cover one maximal request (max_body_bytes + max_header_bytes + 512)");
        }
        if (ws_ingress_byte_limit_ < ws_max_message_bytes_ + 256) {
            throw std::invalid_argument(
                "Web ws_ingress_byte_limit must cover one maximal message (ws_max_message_bytes + 256)");
        }
        require_tls_schema(tls_, scalar_descriptor<TlsServerConfig>::value_meta(), "server TLS config");

        std::vector<std::pair<std::string_view, Value>> fields{
            {"bind_address", atomic(bind_address_)},
            {"port", atomic(port_)},
            {"io_threads", atomic(io_threads_)},
            {"max_connections", atomic(max_connections_)},
            {"max_header_bytes", atomic(max_header_bytes_)},
            {"max_body_bytes", atomic(max_body_bytes_)},
            {"request_timeout_ms", atomic(request_timeout_ms_)},
            {"idle_timeout_ms", atomic(idle_timeout_ms_)},
            {"keep_alive_timeout_ms", atomic(keep_alive_timeout_ms_)},
            {"bind_deferred", atomic(Bool{bind_deferred_})},
            {"ingress_record_limit", atomic(ingress_record_limit_)},
            {"ingress_byte_limit", atomic(ingress_byte_limit_)},
            {"ws_ingress_record_limit", atomic(ws_ingress_record_limit_)},
            {"ws_ingress_byte_limit", atomic(ws_ingress_byte_limit_)},
            {"watermark_high_pct", atomic(watermark_high_pct_)},
            {"watermark_low_pct", atomic(watermark_low_pct_)},
            {"inbound_overflow", atomic(inbound_overflow_)},
            {"outbound_message_limit", atomic(outbound_message_limit_)},
            {"outbound_byte_limit", atomic(outbound_byte_limit_)},
            {"slow_consumer_policy", atomic(slow_consumer_policy_)},
            {"failure_policy", atomic(failure_policy_)},
            {"shutdown_drain_timeout_ms", atomic(shutdown_drain_timeout_ms_)},
            {"ws_max_frame_bytes", atomic(ws_max_frame_bytes_)},
            {"ws_max_message_bytes", atomic(ws_max_message_bytes_)},
            {"ping_interval_ms", atomic(ping_interval_ms_)},
            {"pong_timeout_ms", atomic(pong_timeout_ms_)},
            {"stats_interval_ms", atomic(stats_interval_ms_)},
            {"h2_max_concurrent_streams", atomic(h2_max_concurrent_streams_)},
            {"h2_initial_window_bytes", atomic(h2_initial_window_bytes_)},
        };
        if (tls_.has_value()) { fields.emplace_back("tls", tls_.clone()); }
        return bundle<WebServerConfig>(std::move(fields));
    }

    ServerConfigBuilder server_config() { return {}; }

    ClientConfigBuilder &ClientConfigBuilder::http_version_policy(WebHttpVersionPolicy value) {
        http_version_policy_ = value;
        return *this;
    }

    ClientConfigBuilder &ClientConfigBuilder::max_connections_per_host(Int value) {
        max_connections_per_host_ = value;
        return *this;
    }

    ClientConfigBuilder &ClientConfigBuilder::max_total_connections(Int value) {
        max_total_connections_ = value;
        return *this;
    }

    ClientConfigBuilder &ClientConfigBuilder::connect_timeout(std::chrono::milliseconds value) {
        connect_timeout_ms_ = value.count();
        return *this;
    }

    ClientConfigBuilder &ClientConfigBuilder::request_timeout(std::chrono::milliseconds value) {
        request_timeout_ms_ = value.count();
        return *this;
    }

    ClientConfigBuilder &ClientConfigBuilder::keep_alive(std::chrono::milliseconds value) {
        keep_alive_ms_ = value.count();
        return *this;
    }

    ClientConfigBuilder &ClientConfigBuilder::follow_redirects(bool value, Int max_redirects) {
        follow_redirects_ = value;
        max_redirects_    = max_redirects;
        return *this;
    }

    ClientConfigBuilder &ClientConfigBuilder::proxy(Str value) {
        proxy_ = std::move(value);
        return *this;
    }

    ClientConfigBuilder &ClientConfigBuilder::tls(Value value) {
        tls_ = std::move(value);
        return *this;
    }

    ClientConfigBuilder &ClientConfigBuilder::max_response_bytes(Int value) {
        max_response_bytes_ = value;
        return *this;
    }

    ClientConfigBuilder &ClientConfigBuilder::ingress_limits(Int records, Int bytes) {
        ingress_record_limit_ = records;
        ingress_byte_limit_   = bytes;
        return *this;
    }

    ClientConfigBuilder &ClientConfigBuilder::watermarks(Int high_pct, Int low_pct) {
        watermark_high_pct_ = high_pct;
        watermark_low_pct_  = low_pct;
        return *this;
    }

    ClientConfigBuilder &ClientConfigBuilder::outbound_limits(Int records, Int bytes) {
        outbound_record_limit_ = records;
        outbound_byte_limit_   = bytes;
        return *this;
    }

    ClientConfigBuilder &ClientConfigBuilder::outbound_overflow(WebOverflowAction value, WebOverflowAction stage_full) {
        overflow_       = value;
        stage_overflow_ = stage_full;
        return *this;
    }

    ClientConfigBuilder &ClientConfigBuilder::shutdown_drain_timeout(std::chrono::milliseconds value) {
        shutdown_drain_timeout_ms_ = value.count();
        return *this;
    }

    ClientConfigBuilder &ClientConfigBuilder::failure_policy(WebFailurePolicy value) {
        failure_policy_ = value;
        return *this;
    }

    ClientConfigBuilder &ClientConfigBuilder::ws_ingress_limits(Int records, Int bytes) {
        ws_ingress_record_limit_ = records;
        ws_ingress_byte_limit_   = bytes;
        return *this;
    }

    ClientConfigBuilder &ClientConfigBuilder::ws_frame_limits(Int frame_bytes, Int message_bytes) {
        ws_max_frame_bytes_   = frame_bytes;
        ws_max_message_bytes_ = message_bytes;
        return *this;
    }

    ClientConfigBuilder &ClientConfigBuilder::ping_interval(std::chrono::milliseconds value) {
        ping_interval_ms_ = value.count();
        return *this;
    }

    ClientConfigBuilder &ClientConfigBuilder::pong_timeout(std::chrono::milliseconds value) {
        pong_timeout_ms_ = value.count();
        return *this;
    }

    ClientConfigBuilder &ClientConfigBuilder::stats_interval(std::chrono::milliseconds value) {
        stats_interval_ms_ = value.count();
        return *this;
    }

    Value ClientConfigBuilder::build() const {
        register_web_types();
        require_positive(max_connections_per_host_, "max_connections_per_host");
        require_positive(max_total_connections_, "max_total_connections");
        require_non_negative(connect_timeout_ms_, "connect timeout");
        require_non_negative(request_timeout_ms_, "request timeout");
        require_non_negative(keep_alive_ms_, "keep-alive");
        require_non_negative(max_redirects_, "max_redirects");
        require_positive(max_response_bytes_, "max_response_bytes");
        // The 512-byte response envelope overhead is charged against this
        // limit; smaller values would make every response fail while
        // under-reporting the failure envelope's retained memory.
        if (max_response_bytes_ < 1024) {
            throw std::invalid_argument("Web max_response_bytes must be at least 1024");
        }
        require_positive(ingress_record_limit_, "ingress record limit");
        require_positive(ingress_byte_limit_, "ingress byte limit");
        require_pct_watermarks(watermark_high_pct_, watermark_low_pct_);
        require_positive(outbound_record_limit_, "outbound record limit");
        require_positive(outbound_byte_limit_, "outbound byte limit");
        require_non_negative(shutdown_drain_timeout_ms_, "shutdown drain timeout");
        require_positive(ws_ingress_record_limit_, "WS ingress record limit");
        require_positive(ws_ingress_byte_limit_, "WS ingress byte limit");
        require_positive(ws_max_frame_bytes_, "WS frame limit");
        require_positive(ws_max_message_bytes_, "WS message limit");
        // A single maximal message must always fit an empty WS ingress
        // channel (transport invariant, mirrored in the runtime parses).
        if (ws_ingress_byte_limit_ < ws_max_message_bytes_ + 256) {
            throw std::invalid_argument(
                "Web ws_ingress_byte_limit must cover one maximal message (ws_max_message_bytes + 256)");
        }
        require_non_negative(ping_interval_ms_, "ping interval");
        require_non_negative(pong_timeout_ms_, "pong timeout");
        require_non_negative(stats_interval_ms_, "stats interval");
        // The response reservation must fit the response channel, or no call
        // could ever be accepted (RFC 0024, flow control).
        if (max_response_bytes_ > ingress_byte_limit_) {
            throw std::invalid_argument("Web max_response_bytes cannot exceed the ingress byte limit");
        }
        require_tls_schema(tls_, scalar_descriptor<TlsClientConfig>::value_meta(), "client TLS config");

        std::vector<std::pair<std::string_view, Value>> fields{
            {"http_version_policy", atomic(http_version_policy_)},
            {"max_connections_per_host", atomic(max_connections_per_host_)},
            {"max_total_connections", atomic(max_total_connections_)},
            {"connect_timeout_ms", atomic(connect_timeout_ms_)},
            {"request_timeout_ms", atomic(request_timeout_ms_)},
            {"keep_alive_ms", atomic(keep_alive_ms_)},
            {"follow_redirects", atomic(Bool{follow_redirects_})},
            {"max_redirects", atomic(max_redirects_)},
            {"max_response_bytes", atomic(max_response_bytes_)},
            {"ingress_record_limit", atomic(ingress_record_limit_)},
            {"ingress_byte_limit", atomic(ingress_byte_limit_)},
            {"watermark_high_pct", atomic(watermark_high_pct_)},
            {"watermark_low_pct", atomic(watermark_low_pct_)},
            {"outbound_record_limit", atomic(outbound_record_limit_)},
            {"outbound_byte_limit", atomic(outbound_byte_limit_)},
            {"overflow", atomic(overflow_)},
            {"stage_overflow", atomic(stage_overflow_)},
            {"shutdown_drain_timeout_ms", atomic(shutdown_drain_timeout_ms_)},
            {"failure_policy", atomic(failure_policy_)},
            {"ws_ingress_record_limit", atomic(ws_ingress_record_limit_)},
            {"ws_ingress_byte_limit", atomic(ws_ingress_byte_limit_)},
            {"ws_max_frame_bytes", atomic(ws_max_frame_bytes_)},
            {"ws_max_message_bytes", atomic(ws_max_message_bytes_)},
            {"ping_interval_ms", atomic(ping_interval_ms_)},
            {"pong_timeout_ms", atomic(pong_timeout_ms_)},
            {"stats_interval_ms", atomic(stats_interval_ms_)},
        };
        if (!proxy_.empty()) { fields.emplace_back("proxy", atomic(proxy_)); }
        if (tls_.has_value()) { fields.emplace_back("tls", tls_.clone()); }
        return bundle<WebClientConfig>(std::move(fields));
    }

    ClientConfigBuilder client_config() { return {}; }

    void register_web_types() {
        static_cast<void>(scalar_descriptor<HttpMethod>::value_meta());
        static_cast<void>(scalar_descriptor<WebRouteState>::value_meta());
        static_cast<void>(scalar_descriptor<WsFrameKind>::value_meta());
        static_cast<void>(scalar_descriptor<WsConnectionState>::value_meta());
        static_cast<void>(scalar_descriptor<WebDeliveryStatus>::value_meta());
        static_cast<void>(scalar_descriptor<WebSeverity>::value_meta());
        static_cast<void>(scalar_descriptor<WebInboundOverflow>::value_meta());
        static_cast<void>(scalar_descriptor<WebSlowConsumerPolicy>::value_meta());
        static_cast<void>(scalar_descriptor<WebOverflowAction>::value_meta());
        static_cast<void>(scalar_descriptor<WebFailurePolicy>::value_meta());
        static_cast<void>(scalar_descriptor<WebClientVerify>::value_meta());
        static_cast<void>(scalar_descriptor<WebTlsVersion>::value_meta());
        static_cast<void>(scalar_descriptor<WebHttpVersionPolicy>::value_meta());

        static_cast<void>(scalar_descriptor<WebHeader>::value_meta());
        static_cast<void>(scalar_descriptor<WebParam>::value_meta());
        static_cast<void>(scalar_descriptor<WebPeer>::value_meta());
        static_cast<void>(scalar_descriptor<WebRoute>::value_meta());
        static_cast<void>(scalar_descriptor<HttpRequest>::value_meta());
        static_cast<void>(scalar_descriptor<HttpServerRequest>::value_meta());
        static_cast<void>(scalar_descriptor<HttpResponse>::value_meta());
        static_cast<void>(scalar_descriptor<WebTransportError>::value_meta());
        static_cast<void>(scalar_descriptor<HttpClientRequest>::value_meta());
        static_cast<void>(scalar_descriptor<HttpClientOptions>::value_meta());
        static_cast<void>(scalar_descriptor<WsFrame>::value_meta());
        static_cast<void>(scalar_descriptor<WsInboundFrame>::value_meta());
        static_cast<void>(scalar_descriptor<WsEvent>::value_meta());
        static_cast<void>(scalar_descriptor<WsClientKey>::value_meta());
        static_cast<void>(scalar_descriptor<WebDeliveryReport>::value_meta());
        static_cast<void>(scalar_descriptor<WebEvent>::value_meta());
        static_cast<void>(scalar_descriptor<WebServerStats>::value_meta());
        static_cast<void>(scalar_descriptor<WebClientStats>::value_meta());
        static_cast<void>(scalar_descriptor<TlsServerConfig>::value_meta());
        static_cast<void>(scalar_descriptor<TlsClientConfig>::value_meta());
        static_cast<void>(scalar_descriptor<WebServerConfig>::value_meta());
        static_cast<void>(scalar_descriptor<WebClientConfig>::value_meta());

        static_cast<void>(schema_descriptor<WebRouteOutput>::ts_meta());
        static_cast<void>(schema_descriptor<HttpRespondRequest>::ts_meta());
        static_cast<void>(schema_descriptor<WsRouteOutput>::ts_meta());
        static_cast<void>(schema_descriptor<WsSendRequest>::ts_meta());
        static_cast<void>(schema_descriptor<HttpClientCall>::ts_meta());
        static_cast<void>(schema_descriptor<HttpCallResult>::ts_meta());
        static_cast<void>(schema_descriptor<WsClientOutput>::ts_meta());
        static_cast<void>(schema_descriptor<WsClientSendRequest>::ts_meta());
    }

    Value make_route(HttpMethod method, Str pattern, bool upgrade) {
        register_web_types();
        if (pattern.empty() || pattern.front() != '/') {
            throw std::invalid_argument("Web route patterns must start with '/'");
        }
        return bundle<WebRoute>({
            {"method", atomic(method)},
            {"pattern", atomic(std::move(pattern))},
            {"upgrade", atomic(Bool{upgrade})},
        });
    }

    Value make_headers(std::vector<WebHeaderInput> values) {
        register_web_types();
        return name_value_tuple<WebHeader>(std::move(values), "header");
    }

    Value make_params(std::vector<WebParamInput> values) {
        register_web_types();
        return name_value_tuple<WebParam>(std::move(values), "parameter");
    }

    Value make_request(HttpMethod method, Str target, Str path, std::vector<WebParamInput> query,
                       std::vector<WebParamInput> params, std::vector<WebHeaderInput> headers, Bytes body,
                       std::vector<WebHeaderInput> trailers) {
        register_web_types();
        return bundle<HttpRequest>({
            {"method", atomic(method)},
            {"target", atomic(std::move(target))},
            {"path", atomic(std::move(path))},
            {"query", make_params(std::move(query))},
            {"path_params", make_params(std::move(params))},
            {"headers", make_headers(std::move(headers))},
            {"body", atomic(std::move(body))},
            {"trailers", make_headers(std::move(trailers))},
        });
    }

    Value make_response(Int status, std::vector<WebHeaderInput> headers, Bytes body, std::vector<WebHeaderInput> trailers) {
        register_web_types();
        if (status < 100 || status > 599) { throw std::invalid_argument("Web response status must be 100..599"); }
        return bundle<HttpResponse>({
            {"status", atomic(status)},
            {"headers", make_headers(std::move(headers))},
            {"body", atomic(std::move(body))},
            {"trailers", make_headers(std::move(trailers))},
        });
    }

    Value make_client_request(HttpMethod method, Str url, std::vector<WebHeaderInput> headers, Bytes body) {
        register_web_types();
        if (url.empty()) { throw std::invalid_argument("Web client requests require a URL"); }
        return bundle<HttpClientRequest>({
            {"method", atomic(method)},
            {"url", atomic(std::move(url))},
            {"headers", make_headers(std::move(headers))},
            {"body", atomic(std::move(body))},
        });
    }

    Value make_client_options(Int connect_timeout_ms, Int request_timeout_ms, Bool follow_redirects, Int max_redirects,
                              WebHttpVersionPolicy http_version) {
        register_web_types();
        require_non_negative(connect_timeout_ms, "connect timeout");
        require_non_negative(request_timeout_ms, "request timeout");
        require_non_negative(max_redirects, "max_redirects");
        return bundle<HttpClientOptions>({
            {"connect_timeout_ms", atomic(connect_timeout_ms)},
            {"request_timeout_ms", atomic(request_timeout_ms)},
            {"follow_redirects", atomic(follow_redirects)},
            {"max_redirects", atomic(max_redirects)},
            {"http_version", atomic(http_version)},
        });
    }

    Value make_transport_error(Int error_code, Str message, Bool retriable) {
        register_web_types();
        return bundle<WebTransportError>({
            {"error_code", atomic(error_code)},
            {"message", atomic(std::move(message))},
            {"retriable", atomic(retriable)},
        });
    }

    Value make_text_frame(Str text) {
        register_web_types();
        return bundle<WsFrame>({
            {"kind", atomic(WsFrameKind::Text)},
            {"text", atomic(std::move(text))},
        });
    }

    Value make_binary_frame(Bytes data) {
        register_web_types();
        return bundle<WsFrame>({
            {"kind", atomic(WsFrameKind::Binary)},
            {"data", atomic(std::move(data))},
        });
    }

    Value make_close_frame(Int close_code, Str close_reason) {
        register_web_types();
        if (close_code < 1000 || close_code > 4999) {
            throw std::invalid_argument("WebSocket close codes must be 1000..4999");
        }
        return bundle<WsFrame>({
            {"kind", atomic(WsFrameKind::Close)},
            {"close_code", atomic(close_code)},
            {"close_reason", atomic(std::move(close_reason))},
        });
    }

    Value make_ws_client_key(Str url, std::vector<WebHeaderInput> headers, std::vector<Str> subprotocols) {
        register_web_types();
        if (url.empty()) { throw std::invalid_argument("WebSocket client keys require a URL"); }
        return bundle<WsClientKey>({
            {"url", atomic(std::move(url))},
            {"headers", make_headers(std::move(headers))},
            {"subprotocols", strings(std::move(subprotocols))},
        });
    }

    Value make_delivery_report(Int request_id, Int sequence, WebDeliveryStatus status, Int error_code, Bool retriable,
                               Bool fatal, Str message) {
        register_web_types();
        return bundle<WebDeliveryReport>({
            {"request_id", atomic(request_id)},
            {"sequence", atomic(sequence)},
            {"status", atomic(status)},
            {"error_code", atomic(error_code)},
            {"retriable", atomic(retriable)},
            {"fatal", atomic(fatal)},
            {"message", atomic(std::move(message))},
        });
    }

    Value make_event(WebSeverity severity, Str component, Str category, Str service_path, Str message, Int error_code,
                     Bool retriable, Bool fatal, Int connection_id) {
        register_web_types();
        return bundle<WebEvent>({
            {"severity", atomic(severity)},
            {"component", atomic(std::move(component))},
            {"category", atomic(std::move(category))},
            {"error_code", atomic(error_code)},
            {"retriable", atomic(retriable)},
            {"fatal", atomic(fatal)},
            {"service_path", atomic(std::move(service_path))},
            {"connection_id", atomic(connection_id)},
            {"message", atomic(std::move(message))},
        });
    }
}  // namespace hgraph::web
