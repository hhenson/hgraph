// The libcurl client transport (RFC 0024, runtime architecture: "one
// curl-multi owner thread per client runtime").  This is the ONLY translation
// unit permitted to include <curl/curl.h>; everything curl-shaped stays behind
// WebClientRuntime and the public surface is register_client.
#include <hgraph/web/service.h>
#include <hgraph/web/value_builders.h>

#include "detail/service_bridge.h"

#include <hgraph/runtime/node_scheduler.h>
#include <hgraph/types/metadata/value_plan_factory.h>
#include <hgraph/types/static_node.h>
#include <hgraph/types/value/value_builder.h>

#include <curl/curl.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <compare>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <iterator>
#include <memory>
#include <mutex>
#include <optional>
#include <ostream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <tuple>
#include <utility>
#include <vector>

namespace hgraph::web::detail {

// Transport-level failure codes are CURLcode values, which are non-negative.
// Conditions the extension itself reports carry negative codes so a consumer
// can tell "curl said so" from "the client refused the work".
inline constexpr Int kIngressFullCode = -1;
inline constexpr Int kSimulationCode = -2;
inline constexpr Int kStoppedCode = -3;
inline constexpr Int kInvalidRequestCode = -4;
inline constexpr Int kNotConnectedCode = -5;
inline constexpr Int kQueueFullCode = -6;

/** Parsed TlsClientConfig; an absent sub-bundle means library defaults. */
struct TlsClientSettings {
  Str ca_path{};
  Str ca_pem{};
  Str cert_path{};
  Str cert_pem{};
  Str key_path{};
  Str key_pem{};
  Str sni{};
  bool verify_peer{true};
  bool verify_host{true};
  WebTlsVersion min_version{WebTlsVersion::Tls1_2};
};

/** Plain, thread-safe-to-read snapshot of WebClientConfig. */
struct ClientRuntimeConfig {
  WebHttpVersionPolicy http_version_policy{WebHttpVersionPolicy::Auto};
  Int max_connections_per_host{6};
  Int max_total_connections{64};
  Int connect_timeout_ms{10'000};
  Int request_timeout_ms{30'000};
  Int keep_alive_ms{60'000};
  bool follow_redirects{true};
  Int max_redirects{5};
  Str proxy{};
  TlsClientSettings tls{};
  std::size_t max_response_bytes{16 * 1024 * 1024};
  OutputLimits ingress{};
  OutputLimits ws_ingress{};
  OutputLimits outbound{};
  Int watermark_high_pct{80};
  Int watermark_low_pct{50};
  WebOverflowAction overflow{WebOverflowAction::Stage};
  WebOverflowAction stage_overflow{WebOverflowAction::Fail};
  std::chrono::milliseconds shutdown_drain_timeout{5'000};
  WebFailurePolicy failure_policy{WebFailurePolicy::Report};
  std::size_t ws_max_frame_bytes{1024 * 1024};
  std::size_t ws_max_message_bytes{16 * 1024 * 1024};
  Int ping_interval_ms{30'000};
  Int pong_timeout_ms{10'000};
  Int stats_interval_ms{0};
};

namespace {

// ---------------------------------------------------------------------------
// Value helpers.  value_builders.cpp keeps its equivalents private, and the
// public make_* builders validate user input (make_response rejects a status
// outside 100..599), so wire-derived values are assembled here directly.

using NameValues = std::vector<std::pair<Str, Str>>;

[[nodiscard]] ValueTypeRef resolve(const ValueTypeMetaData *meta) {
  auto binding = ValuePlanFactory::instance().type_for(meta);
  if (!binding) {
    throw std::logic_error("Web client schema did not resolve");
  }
  return binding;
}

template <typename Schema> [[nodiscard]] ValueTypeRef resolve_schema() {
  return resolve(scalar_descriptor<Schema>::value_meta());
}

/**
 * Every binding the owner thread builds Values from, resolved once on the
 * graph thread at start.
 *
 * Pre-warming alone is not enough: `scalar_descriptor<T>::value_meta()` calls
 * `TypeRegistry::register_scalar` and `Value(T)` calls
 * `TypeRegistry::scalar_type<T>()` on EVERY invocation, so the interning
 * lookup — not just the first synthesis — takes the counted type-system
 * mutex.  Holding the resolved `ValueTypeRef`s and building through
 * `Value(binding, source)` and `BundleBuilder(binding)` keeps the worker
 * thread off the registry entirely (CLAUDE.md conventions; measured at 331
 * acquisitions per response before this table existed).
 */
struct ValueBindings {
  ValueTypeRef integer{};
  ValueTypeRef boolean{};
  ValueTypeRef text{};
  ValueTypeRef binary{};
  ValueTypeRef connection_state{};
  ValueTypeRef frame_kind{};
  ValueTypeRef delivery_status{};
  ValueTypeRef severity{};

  ValueTypeRef header{};
  ValueTypeRef header_tuple{};
  ValueTypeRef http_response{};
  ValueTypeRef transport_error{};
  ValueTypeRef ws_event{};
  ValueTypeRef ws_frame{};
  ValueTypeRef delivery_report{};
  ValueTypeRef web_event{};
  ValueTypeRef client_stats{};

  ValueTypeRef response_envelope{};
  ValueTypeRef ws_client_envelope{};
  ValueTypeRef delivery_envelope{};
  ValueTypeRef event_envelope{};

  void resolve_all() {
    integer = resolve_schema<Int>();
    boolean = resolve_schema<Bool>();
    text = resolve_schema<Str>();
    binary = resolve_schema<Bytes>();
    connection_state = resolve_schema<WsConnectionState>();
    frame_kind = resolve_schema<WsFrameKind>();
    delivery_status = resolve_schema<WebDeliveryStatus>();
    severity = resolve_schema<WebSeverity>();

    header = resolve_schema<WebHeader>();
    header_tuple = resolve_schema<HomogeneousTuple<WebHeader>>();
    http_response = resolve_schema<HttpResponse>();
    transport_error = resolve_schema<WebTransportError>();
    ws_event = resolve_schema<WsEvent>();
    ws_frame = resolve_schema<WsFrame>();
    delivery_report = resolve_schema<WebDeliveryReport>();
    web_event = resolve_schema<WebEvent>();
    client_stats = resolve_schema<WebClientStats>();

    response_envelope = resolve_schema<WebResponseEnvelope>();
    ws_client_envelope = resolve_schema<WsClientEnvelope>();
    delivery_envelope = resolve_schema<WebDeliveryEnvelope>();
    event_envelope = resolve_schema<WebEventEnvelope>();
  }

  [[nodiscard]] static Value scalar(const ValueTypeRef &binding,
                                    const void *source) {
    return Value{binding, source};
  }

  [[nodiscard]] Value number(Int value) const {
    return scalar(integer, &value);
  }
  [[nodiscard]] Value flag(Bool value) const { return scalar(boolean, &value); }
  [[nodiscard]] Value string(const Str &value) const {
    return scalar(text, &value);
  }
  [[nodiscard]] Value bytes(const Bytes &value) const {
    return scalar(binary, &value);
  }

  /** Duplicate names and arrival order survive (RFC 0024, value contract). */
  [[nodiscard]] Value headers(const NameValues &values) const {
    ListBuilder list{header};
    for (const auto &[name, value] : values) {
      BundleBuilder entry{header};
      entry.set("name", string(name));
      entry.set("value", string(value));
      Value item = entry.build();
      list.push_back_copy(item.view().data());
    }
    ListStorage storage = list.build_storage();
    return Value{header_tuple, &storage};
  }
};

/** Assemble a bundle from the fields that are set, on a resolved binding. */
[[nodiscard]] Value
build(const ValueTypeRef &binding,
      std::vector<std::pair<std::string_view, Value>> fields) {
  BundleBuilder builder{binding};
  for (auto &[name, field] : fields) {
    if (field.has_value()) {
      builder.set(name, std::move(field));
    }
  }
  return builder.build();
}

[[nodiscard]] bool present(const ValueView &value) noexcept {
  return value.data() != nullptr;
}

[[nodiscard]] Str text_or(const BundleView &fields, std::string_view name,
                          Str fallback = {}) {
  const auto item = fields.at(name);
  return present(item) ? item.checked_as<Str>() : std::move(fallback);
}

[[nodiscard]] Int int_or(const BundleView &fields, std::string_view name,
                         Int fallback) {
  const auto item = fields.at(name);
  return present(item) ? item.checked_as<Int>() : fallback;
}

[[nodiscard]] bool bool_or(const BundleView &fields, std::string_view name,
                           bool fallback) {
  const auto item = fields.at(name);
  return present(item) ? item.checked_as<Bool>() : fallback;
}

template <typename T>
[[nodiscard]] T enum_or(const BundleView &fields, std::string_view name,
                        T fallback) {
  const auto item = fields.at(name);
  return present(item) ? item.checked_as<T>() : fallback;
}

[[nodiscard]] std::string bytes_or(const BundleView &fields,
                                   std::string_view name) {
  const auto item = fields.at(name);
  return present(item) ? item.checked_as<Bytes>().data : std::string{};
}

[[nodiscard]] Value response_envelope(const ValueBindings &b, Int client_id,
                                      Value response, Value failure) {
  return build(b.response_envelope, {
                                        {"request_id", b.number(client_id)},
                                        {"response", std::move(response)},
                                        {"failure", std::move(failure)},
                                    });
}

[[nodiscard]] Value ws_client_envelope(const ValueBindings &b, Value key,
                                       Value event, Value frame) {
  return build(b.ws_client_envelope, {
                                         {"key", std::move(key)},
                                         {"event", std::move(event)},
                                         {"frame", std::move(frame)},
                                     });
}

[[nodiscard]] Value delivery_envelope(const ValueBindings &b, Int client_id,
                                      Value report) {
  return build(b.delivery_envelope, {
                                        {"request_id", b.number(client_id)},
                                        {"report", std::move(report)},
                                    });
}

[[nodiscard]] Value event_envelope(const ValueBindings &b, Value event,
                                   bool stop_graph) {
  return build(b.event_envelope, {
                                     {"event", std::move(event)},
                                     {"stop_graph", b.flag(stop_graph)},
                                 });
}

[[nodiscard]] Value transport_error(const ValueBindings &b, Int error_code,
                                    const Str &message, bool retriable) {
  return build(b.transport_error, {
                                      {"error_code", b.number(error_code)},
                                      {"message", b.string(message)},
                                      {"retriable", b.flag(retriable)},
                                  });
}

[[nodiscard]] Value http_response(const ValueBindings &b, Int status,
                                  const NameValues &headers, std::string body) {
  return build(b.http_response, {
                                    {"status", b.number(status)},
                                    {"headers", b.headers(headers)},
                                    {"body", b.bytes(Bytes{std::move(body)})},
                                    {"trailers", b.headers({})},
                                });
}

/** The WsEvent `request` field is server-side only and stays unset here. */
[[nodiscard]] Value ws_event(const ValueBindings &b, Int connection_id,
                             WsConnectionState state, Int close_code = 0,
                             const Str &close_reason = {}) {
  return build(b.ws_event,
               {
                   {"connection_id", b.number(connection_id)},
                   {"state", ValueBindings::scalar(b.connection_state, &state)},
                   {"close_code", b.number(close_code)},
                   {"close_reason", b.string(close_reason)},
               });
}

[[nodiscard]] Value ws_text_frame(const ValueBindings &b, std::string text) {
  const auto kind = WsFrameKind::Text;
  return build(b.ws_frame,
               {
                   {"kind", ValueBindings::scalar(b.frame_kind, &kind)},
                   {"text", b.string(Str{std::move(text)})},
               });
}

[[nodiscard]] Value ws_binary_frame(const ValueBindings &b, std::string data) {
  const auto kind = WsFrameKind::Binary;
  return build(b.ws_frame,
               {
                   {"kind", ValueBindings::scalar(b.frame_kind, &kind)},
                   {"data", b.bytes(Bytes{std::move(data)})},
               });
}

/** The exported make_delivery_report / make_event go through the registry on
 * every call; the owner thread builds these two locally instead. */
[[nodiscard]] Value delivery_report(const ValueBindings &b, Int request_id,
                                    Int sequence, WebDeliveryStatus status,
                                    Int error_code, bool retriable,
                                    const Str &message) {
  return build(
      b.delivery_report,
      {
          {"request_id", b.number(request_id)},
          {"sequence", b.number(sequence)},
          {"status", ValueBindings::scalar(b.delivery_status, &status)},
          {"error_code", b.number(error_code)},
          {"retriable", b.flag(retriable)},
          {"fatal", b.flag(false)},
          {"message", b.string(message)},
      });
}

[[nodiscard]] Value web_event(const ValueBindings &b, WebSeverity severity,
                              const Str &component, const Str &category,
                              const Str &service_path, const Str &message,
                              Int error_code, bool retriable, bool fatal,
                              Int connection_id) {
  return build(b.web_event,
               {
                   {"severity", ValueBindings::scalar(b.severity, &severity)},
                   {"component", b.string(component)},
                   {"category", b.string(category)},
                   {"error_code", b.number(error_code)},
                   {"retriable", b.flag(retriable)},
                   {"fatal", b.flag(fatal)},
                   {"service_path", b.string(service_path)},
                   {"connection_id", b.number(connection_id)},
                   {"message", b.string(message)},
               });
}

[[nodiscard]] std::size_t header_bytes(const NameValues &headers) noexcept {
  std::size_t result{};
  for (const auto &[name, value] : headers) {
    result += name.size() + value.size() + 32;
  }
  return result;
}

// ---------------------------------------------------------------------------
// Configuration

[[nodiscard]] std::size_t positive_limit(const BundleView &fields,
                                         std::string_view name) {
  const Int value = fields.at(name).checked_as<Int>();
  if (value <= 0) {
    throw std::invalid_argument("Web " + std::string{name} +
                                " must be positive");
  }
  return static_cast<std::size_t>(value);
}

[[nodiscard]] TlsClientSettings parse_tls(const ValueView &value) {
  TlsClientSettings result;
  if (!present(value)) {
    return result;
  }
  const auto fields = value.as_bundle();
  result.ca_path = text_or(fields, "ca_path");
  result.ca_pem = text_or(fields, "ca_pem");
  result.cert_path = text_or(fields, "cert_path");
  result.cert_pem = text_or(fields, "cert_pem");
  result.key_path = text_or(fields, "key_path");
  result.key_pem = text_or(fields, "key_pem");
  result.sni = text_or(fields, "sni");
  result.verify_peer = bool_or(fields, "verify_peer", true);
  result.verify_host = bool_or(fields, "verify_host", true);
  result.min_version = enum_or(fields, "min_version", WebTlsVersion::Tls1_2);
  return result;
}

[[nodiscard]] ClientRuntimeConfig parse_client_config(const Value &value) {
  if (value.schema() != scalar_descriptor<WebClientConfig>::value_meta()) {
    throw std::invalid_argument("Web client requires WebClientConfig");
  }
  const auto root = value.view().as_bundle();

  ClientRuntimeConfig result;
  result.http_version_policy =
      root.at("http_version_policy").checked_as<WebHttpVersionPolicy>();
  result.max_connections_per_host =
      root.at("max_connections_per_host").checked_as<Int>();
  result.max_total_connections =
      root.at("max_total_connections").checked_as<Int>();
  result.connect_timeout_ms = root.at("connect_timeout_ms").checked_as<Int>();
  result.request_timeout_ms = root.at("request_timeout_ms").checked_as<Int>();
  result.keep_alive_ms = root.at("keep_alive_ms").checked_as<Int>();
  result.follow_redirects = root.at("follow_redirects").checked_as<Bool>();
  result.max_redirects = root.at("max_redirects").checked_as<Int>();
  result.proxy = text_or(root, "proxy");
  result.tls = parse_tls(root.at("tls"));
  result.max_response_bytes = positive_limit(root, "max_response_bytes");
  result.ingress = OutputLimits{positive_limit(root, "ingress_record_limit"),
                                positive_limit(root, "ingress_byte_limit")};
  result.ws_ingress =
      OutputLimits{positive_limit(root, "ws_ingress_record_limit"),
                   positive_limit(root, "ws_ingress_byte_limit")};
  result.outbound = OutputLimits{positive_limit(root, "outbound_record_limit"),
                                 positive_limit(root, "outbound_byte_limit")};
  result.watermark_high_pct = root.at("watermark_high_pct").checked_as<Int>();
  result.watermark_low_pct = root.at("watermark_low_pct").checked_as<Int>();
  result.overflow = root.at("overflow").checked_as<WebOverflowAction>();
  result.stage_overflow =
      root.at("stage_overflow").checked_as<WebOverflowAction>();
  const Int drain_ms = root.at("shutdown_drain_timeout_ms").checked_as<Int>();
  if (drain_ms < 0) {
    throw std::invalid_argument(
        "Web shutdown drain timeout must be non-negative");
  }
  result.shutdown_drain_timeout = std::chrono::milliseconds{drain_ms};
  result.failure_policy =
      root.at("failure_policy").checked_as<WebFailurePolicy>();
  result.ws_max_frame_bytes = positive_limit(root, "ws_max_frame_bytes");
  result.ws_max_message_bytes = positive_limit(root, "ws_max_message_bytes");
  result.ping_interval_ms = root.at("ping_interval_ms").checked_as<Int>();
  result.pong_timeout_ms = root.at("pong_timeout_ms").checked_as<Int>();
  result.stats_interval_ms = root.at("stats_interval_ms").checked_as<Int>();

  if (result.connect_timeout_ms < 0 || result.request_timeout_ms < 0 ||
      result.max_redirects < 0 || result.stats_interval_ms < 0) {
    throw std::invalid_argument("Web client timings cannot be negative");
  }
  if (result.stage_overflow == WebOverflowAction::Stage) {
    throw std::invalid_argument("Web stage overflow must be Fail or Drop");
  }
  if (result.max_response_bytes > result.ingress.bytes) {
    throw std::invalid_argument(
        "Web max_response_bytes cannot exceed the ingress byte limit");
  }
  return result;
}

// ---------------------------------------------------------------------------
// curl helpers

/** Process-lifetime global init.  curl_global_cleanup is never called: it is
 * unsafe while any other user of the process still holds curl state, and the
 * kafka extension sets the same precedent for its library global. */
void ensure_curl_initialized() {
  static const bool initialized = [] {
    const CURLcode result = curl_global_init(CURL_GLOBAL_DEFAULT);
    if (result != CURLE_OK) {
      throw std::runtime_error(std::string{"curl_global_init failed: "} +
                               curl_easy_strerror(result));
    }
    return true;
  }();
  static_cast<void>(initialized);
}

[[nodiscard]] const char *method_name(HttpMethod method) noexcept {
  switch (method) {
  case HttpMethod::Get:
    return "GET";
  case HttpMethod::Head:
    return "HEAD";
  case HttpMethod::Post:
    return "POST";
  case HttpMethod::Put:
    return "PUT";
  case HttpMethod::Delete:
    return "DELETE";
  case HttpMethod::Patch:
    return "PATCH";
  case HttpMethod::Options:
    return "OPTIONS";
  case HttpMethod::Trace:
    return "TRACE";
  }
  return "GET";
}

/** The curl errors worth retrying: connection setup and transient I/O. */
[[nodiscard]] bool retriable_curl_error(CURLcode error) noexcept {
  switch (error) {
  case CURLE_COULDNT_RESOLVE_HOST:
  case CURLE_COULDNT_RESOLVE_PROXY:
  case CURLE_COULDNT_CONNECT:
  case CURLE_OPERATION_TIMEDOUT:
  case CURLE_SEND_ERROR:
  case CURLE_RECV_ERROR:
  case CURLE_GOT_NOTHING:
  case CURLE_PARTIAL_FILE:
    return true;
  default:
    return false;
  }
}

[[nodiscard]] long http_version_option(WebHttpVersionPolicy policy) noexcept {
  switch (policy) {
  case WebHttpVersionPolicy::H1Only:
    return CURL_HTTP_VERSION_1_1;
  case WebHttpVersionPolicy::H2Only:
    return CURL_HTTP_VERSION_2_0;
  case WebHttpVersionPolicy::Auto:
    break;
  }
  return CURL_HTTP_VERSION_2TLS;
}

void apply_tls(CURL *easy, const TlsClientSettings &tls) {
  static_cast<void>(curl_easy_setopt(easy, CURLOPT_SSL_VERIFYPEER,
                                     tls.verify_peer ? 1L : 0L));
  static_cast<void>(curl_easy_setopt(easy, CURLOPT_SSL_VERIFYHOST,
                                     tls.verify_host ? 2L : 0L));
  static_cast<void>(
      curl_easy_setopt(easy, CURLOPT_SSLVERSION,
                       tls.min_version == WebTlsVersion::Tls1_3
                           ? static_cast<long>(CURL_SSLVERSION_TLSv1_3)
                           : static_cast<long>(CURL_SSLVERSION_TLSv1_2)));
  if (!tls.ca_path.empty()) {
    static_cast<void>(
        curl_easy_setopt(easy, CURLOPT_CAINFO, tls.ca_path.c_str()));
  }
  // Blobs are copied by curl (CURL_BLOB_COPY), so the configuration string
  // does not have to outlive this call.
  if (!tls.ca_pem.empty()) {
    curl_blob blob{const_cast<char *>(tls.ca_pem.data()), tls.ca_pem.size(),
                   CURL_BLOB_COPY};
    static_cast<void>(curl_easy_setopt(easy, CURLOPT_CAINFO_BLOB, &blob));
  }
  if (!tls.cert_path.empty()) {
    static_cast<void>(
        curl_easy_setopt(easy, CURLOPT_SSLCERT, tls.cert_path.c_str()));
  }
  if (!tls.cert_pem.empty()) {
    curl_blob blob{const_cast<char *>(tls.cert_pem.data()), tls.cert_pem.size(),
                   CURL_BLOB_COPY};
    static_cast<void>(curl_easy_setopt(easy, CURLOPT_SSLCERT_BLOB, &blob));
    static_cast<void>(curl_easy_setopt(easy, CURLOPT_SSLCERTTYPE, "PEM"));
  }
  if (!tls.key_path.empty()) {
    static_cast<void>(
        curl_easy_setopt(easy, CURLOPT_SSLKEY, tls.key_path.c_str()));
  }
  if (!tls.key_pem.empty()) {
    curl_blob blob{const_cast<char *>(tls.key_pem.data()), tls.key_pem.size(),
                   CURL_BLOB_COPY};
    static_cast<void>(curl_easy_setopt(easy, CURLOPT_SSLKEY_BLOB, &blob));
    static_cast<void>(curl_easy_setopt(easy, CURLOPT_SSLKEYTYPE, "PEM"));
  }
}

// ---------------------------------------------------------------------------
// Owner-thread transfer state

enum class TransferKind : std::uint8_t {
  Http,
  WebSocket,
};

/** The CURLOPT_PRIVATE payload; both transfer shapes derive from it so one
 * completion path can recover the owner from a finished easy handle. */
struct TransferTag {
  TransferKind kind{TransferKind::Http};
};

/** Owns its easy handle and header list.  The handle must already have been
 * removed from the multi when this is destroyed. */
struct HttpTransfer : TransferTag {
  HttpTransfer() { kind = TransferKind::Http; }
  HttpTransfer(const HttpTransfer &) = delete;
  HttpTransfer &operator=(const HttpTransfer &) = delete;

  ~HttpTransfer() {
    if (easy != nullptr) {
      curl_easy_cleanup(easy);
    }
    if (headers != nullptr) {
      curl_slist_free_all(headers);
    }
  }

  CURL *easy{};
  curl_slist *headers{};
  Int client_id{};
  std::size_t reserved_bytes{};
  std::size_t max_response_bytes{};
  std::string body{};
  NameValues response_headers{};
  bool truncated{};
  char error[CURL_ERROR_SIZE]{};
};

struct WsConnection : TransferTag {
  WsConnection() { kind = TransferKind::WebSocket; }
  WsConnection(const WsConnection &) = delete;
  WsConnection &operator=(const WsConnection &) = delete;

  ~WsConnection() {
    if (easy != nullptr) {
      curl_easy_cleanup(easy);
    }
    if (headers != nullptr) {
      curl_slist_free_all(headers);
    }
  }

  CURL *easy{};
  curl_slist *headers{};
  Value key{};
  Int connection_id{};
  bool handshaking{true};
  bool open{};
  bool closing{};
  curl_socket_t socket{CURL_SOCKET_BAD};
  std::string message{};
  bool message_active{};
  bool message_is_text{};
  char error[CURL_ERROR_SIZE]{};
};

struct HttpSubmission {
  Int client_id{};
  HttpMethod method{HttpMethod::Get};
  Str url{};
  NameValues headers{};
  std::string body{};
  Int connect_timeout_ms{};
  Int request_timeout_ms{};
  bool follow_redirects{};
  Int max_redirects{};
  WebHttpVersionPolicy http_version{WebHttpVersionPolicy::Auto};
  std::size_t reserved_bytes{};
};

struct WsSendItem {
  Int client_id{};
  Value key{};
  WsFrameKind kind{WsFrameKind::Text};
  std::string payload{};
  Int close_code{1000};
  Str close_reason{};
  std::size_t bytes{};
};

/** Key add/remove stays one ordered stream: a remove followed by a re-add in
 * the same engine cycle must not be reordered into an add that survives. */
struct WsCommand {
  Value key{};
  bool added{};
};

} // namespace

// ---------------------------------------------------------------------------
// The runtime

class WebClientRuntime;

struct ClientRuntimeConfigHandle {
  std::shared_ptr<const ClientRuntimeConfig> value{};

  friend bool operator==(const ClientRuntimeConfigHandle &,
                         const ClientRuntimeConfigHandle &) noexcept = default;
  friend std::strong_ordering
  operator<=>(const ClientRuntimeConfigHandle &lhs,
              const ClientRuntimeConfigHandle &rhs) noexcept {
    return reinterpret_cast<std::uintptr_t>(lhs.value.get()) <=>
           reinterpret_cast<std::uintptr_t>(rhs.value.get());
  }
};

inline std::ostream &operator<<(std::ostream &stream,
                                const ClientRuntimeConfigHandle &value) {
  return stream << "ClientRuntimeConfigHandle(" << value.value.get() << ')';
}

struct ClientRuntimeHandle {
  std::shared_ptr<WebClientRuntime> value{};

  friend bool operator==(const ClientRuntimeHandle &,
                         const ClientRuntimeHandle &) noexcept = default;
  friend std::strong_ordering
  operator<=>(const ClientRuntimeHandle &lhs,
              const ClientRuntimeHandle &rhs) noexcept {
    return reinterpret_cast<std::uintptr_t>(lhs.value.get()) <=>
           reinterpret_cast<std::uintptr_t>(rhs.value.get());
  }
};

inline std::ostream &operator<<(std::ostream &stream,
                                const ClientRuntimeHandle &value) {
  return stream << "ClientRuntimeHandle(" << value.value.get() << ')';
}
} // namespace hgraph::web::detail

namespace std {
template <> struct hash<hgraph::web::detail::ClientRuntimeConfigHandle> {
  size_t operator()(const hgraph::web::detail::ClientRuntimeConfigHandle &value)
      const noexcept {
    return hash<const void *>{}(value.value.get());
  }
};

template <> struct hash<hgraph::web::detail::ClientRuntimeHandle> {
  size_t operator()(
      const hgraph::web::detail::ClientRuntimeHandle &value) const noexcept {
    return hash<const void *>{}(value.value.get());
  }
};
} // namespace std

namespace hgraph::static_schema_detail {
template <> struct scalar_name<web::detail::ClientRuntimeConfigHandle> {
  static constexpr std::string_view value{
      "hgraph.web.internal::ClientRuntimeConfigHandle"};
};

template <> struct scalar_name<web::detail::ClientRuntimeHandle> {
  static constexpr std::string_view value{
      "hgraph.web.internal::ClientRuntimeHandle"};
};
} // namespace hgraph::static_schema_detail

namespace hgraph::web::detail {

/**
 * One curl-multi owner thread per client runtime (RFC 0024).  The graph thread
 * only stages owned Values under `mutex_` and wakes the loop; every curl call,
 * every completion, and every bridge push for inbound work happens on the
 * owner thread.
 */
class WebClientRuntime {
public:
  WebClientRuntime(ClientRuntimeConfig config, Str path,
                   ClientBridgeHandle bridge, bool simulation)
      : config_{std::move(config)}, path_{std::move(path)},
        bridge_{std::move(bridge)}, simulation_{simulation} {
    if (!bridge_.value) {
      throw std::invalid_argument(
          "Web client runtime requires an output bridge");
    }
  }

  WebClientRuntime(const WebClientRuntime &) = delete;
  WebClientRuntime &operator=(const WebClientRuntime &) = delete;

  ~WebClientRuntime() { stop(); }

  void start() {
    started_ = true;
    bindings_.resolve_all();
    bridge_.value->start();
    if (simulation_) {
      // Live transport is hard-rejected under a simulation executor, exactly
      // as kafka does; deterministic tests use the fake transport seam.  The
      // warning is emitted on the first rejected submission rather than here:
      // a value pushed during start marks a push update pending before the
      // first cycle, and the simulation executor then advances to
      // start_time + MIN_TD, skipping every node scheduled at start_time.
      return;
    }
    try {
      ensure_curl_initialized();
      multi_ = curl_multi_init();
      if (multi_ == nullptr) {
        throw std::runtime_error("curl_multi_init failed");
      }
      static_cast<void>(
          curl_multi_setopt(multi_, CURLMOPT_MAX_TOTAL_CONNECTIONS,
                            static_cast<long>(config_.max_total_connections)));
      static_cast<void>(curl_multi_setopt(
          multi_, CURLMOPT_MAX_HOST_CONNECTIONS,
          static_cast<long>(config_.max_connections_per_host)));
      accepting_.store(true, std::memory_order_release);
      thread_ = std::thread{[this] { run(); }};
    } catch (...) {
      accepting_.store(false, std::memory_order_release);
      if (multi_ != nullptr) {
        curl_multi_cleanup(multi_);
        multi_ = nullptr;
      }
      bridge_.value->stop();
      throw;
    }
  }

  /** Stop order (RFC 0024, lifecycle): stop intake, drain in-flight within
   * the budget, close WebSockets with 1001, join the owner thread, release
   * curl state, and only then stop the bridge. */
  void stop() noexcept {
    if (!started_ || stopped_.exchange(true)) {
      return;
    }
    accepting_.store(false, std::memory_order_release);
    stopping_.store(true, std::memory_order_release);
    if (thread_.joinable()) {
      if (multi_ != nullptr) {
        static_cast<void>(curl_multi_wakeup(multi_));
      }
      try {
        thread_.join();
      } catch (...) {
      }
    }
    if (multi_ != nullptr) {
      curl_multi_cleanup(multi_);
      multi_ = nullptr;
    }
    try {
      bridge_.value->stop();
    } catch (...) {
    }
  }

  // -------------------------------------------------------------------------
  // Graph-thread submission surface

  void call(Int client_id, Value request, Value options) {
    if (!live()) {
      warn_simulation_once();
      reject_call(client_id, simulation_ ? kSimulationCode : kStoppedCode,
                  simulation_ ? Str{"web client is real-time only"}
                              : Str{"web client is not accepting calls"});
      return;
    }
    // The response reservation is taken before curl sees the request, so a
    // completion can never be dropped for lack of queue space (RFC 0024,
    // flow control).
    if (!bridge_.value->reserve(index(ClientChannel::Response),
                                config_.max_response_bytes)) {
      reject_call(client_id, kIngressFullCode,
                  Str{"web client ingress is full"}, true);
      return;
    }
    HttpSubmission submission;
    try {
      submission = parse_submission(client_id, request, options);
    } catch (const std::exception &exception) {
      push_response(
          response_envelope(bindings_, client_id, Value{},
                            transport_error(bindings_, kInvalidRequestCode,
                                            Str{exception.what()}, false)),
          512, config_.max_response_bytes);
      return;
    }
    submission.reserved_bytes = config_.max_response_bytes;
    {
      std::lock_guard lock{mutex_};
      pending_calls_.push_back(std::move(submission));
    }
    wake();
  }

  void ws_key_added(Value key) {
    if (!live()) {
      warn_simulation_once();
      reject_ws_key(std::move(key), simulation_
                                        ? Str{"web client is real-time only"}
                                        : Str{"web client is stopping"});
      return;
    }
    {
      std::lock_guard lock{mutex_};
      pending_ws_commands_.push_back(WsCommand{std::move(key), true});
    }
    wake();
  }

  void ws_key_removed(Value key) {
    if (!live()) {
      return;
    }
    {
      std::lock_guard lock{mutex_};
      pending_ws_commands_.push_back(WsCommand{std::move(key), false});
    }
    wake();
  }

  void ws_send(Int client_id, Value key, Value frame) {
    if (!live()) {
      warn_simulation_once();
      report_send(client_id, WebDeliveryStatus::EnqueueRejected,
                  simulation_ ? kSimulationCode : kStoppedCode,
                  simulation_ ? Str{"web client is real-time only"}
                              : Str{"web client is not accepting sends"},
                  false, true);
      return;
    }
    WsSendItem item;
    try {
      item = parse_send(client_id, std::move(key), frame);
    } catch (const std::exception &exception) {
      report_send(client_id, WebDeliveryStatus::PermanentFailure,
                  kInvalidRequestCode, Str{exception.what()}, false, true);
      return;
    }
    bool overflowed{};
    {
      std::lock_guard lock{mutex_};
      const bool records_full =
          pending_sends_.size() >= config_.outbound.records;
      const bool bytes_full =
          item.bytes > config_.outbound.bytes -
                           std::min(staged_bytes_, config_.outbound.bytes);
      overflowed = records_full || bytes_full;
      if (!overflowed) {
        staged_bytes_ += item.bytes;
        pending_sends_.push_back(std::move(item));
      }
    }
    if (overflowed) {
      // Stage is the default: accept until the staging limit, then apply
      // stage_overflow (RFC 0024, configuration contract).
      const WebOverflowAction action =
          config_.overflow == WebOverflowAction::Stage ? config_.stage_overflow
                                                       : config_.overflow;
      const bool dropped = action == WebOverflowAction::Drop;
      if (dropped) {
        dropped_.fetch_add(1, std::memory_order_relaxed);
      }
      report_send(client_id,
                  dropped ? WebDeliveryStatus::Dropped
                          : WebDeliveryStatus::EnqueueRejected,
                  kQueueFullCode, Str{"web client outbound queue is full"},
                  true, true);
      emit_event(dropped ? WebSeverity::Warning : WebSeverity::Error,
                 Str{"client"}, Str{"queue_overflow"},
                 dropped ? Str{"outbound WebSocket frame was dropped because "
                               "the staging queue is full"}
                         : Str{"outbound WebSocket staging queue is full"},
                 kQueueFullCode, true,
                 !dropped &&
                     config_.failure_policy == WebFailurePolicy::StopGraph);
      return;
    }
    wake();
  }

private:
  using Clock = std::chrono::steady_clock;

  [[nodiscard]] bool live() const noexcept {
    return !simulation_ && accepting_.load(std::memory_order_acquire);
  }

  /** One warning per runtime, on the first submission a simulation refuses. */
  void warn_simulation_once() noexcept {
    if (!simulation_ || simulation_warned_) {
      return;
    }
    simulation_warned_ = true;
    emit_event(WebSeverity::Warning, Str{"client"}, Str{"simulation"},
               Str{"web client is real-time only; calls, connections, and "
                   "sends are rejected under a simulation executor"},
               kSimulationCode, false, false);
  }

  void wake() noexcept {
    if (multi_ != nullptr) {
      static_cast<void>(curl_multi_wakeup(multi_));
    }
  }

  // -------------------------------------------------------------------------
  // Graph-thread parsing

  [[nodiscard]] HttpSubmission parse_submission(Int client_id,
                                                const Value &request,
                                                const Value &options) const {
    if (!request.has_value()) {
      throw std::invalid_argument("web client call requires a request");
    }
    const auto fields = request.view().as_bundle();
    HttpSubmission result;
    result.client_id = client_id;
    result.method = enum_or(fields, "method", HttpMethod::Get);
    result.url = text_or(fields, "url");
    if (result.url.empty()) {
      throw std::invalid_argument("web client call requires a URL");
    }
    const auto headers = fields.at("headers");
    if (present(headers)) {
      for (const auto item : headers.as_list()) {
        const auto header = item.as_bundle();
        result.headers.emplace_back(header.at("name").checked_as<Str>(),
                                    header.at("value").checked_as<Str>());
      }
    }
    result.body = bytes_or(fields, "body");

    result.connect_timeout_ms = config_.connect_timeout_ms;
    result.request_timeout_ms = config_.request_timeout_ms;
    result.follow_redirects = config_.follow_redirects;
    result.max_redirects = config_.max_redirects;
    result.http_version = config_.http_version_policy;
    if (options.has_value()) {
      const auto option_fields = options.view().as_bundle();
      result.connect_timeout_ms = int_or(option_fields, "connect_timeout_ms",
                                         result.connect_timeout_ms);
      result.request_timeout_ms = int_or(option_fields, "request_timeout_ms",
                                         result.request_timeout_ms);
      result.follow_redirects =
          bool_or(option_fields, "follow_redirects", result.follow_redirects);
      result.max_redirects =
          int_or(option_fields, "max_redirects", result.max_redirects);
      result.http_version =
          enum_or(option_fields, "http_version", result.http_version);
    }
    return result;
  }

  [[nodiscard]] WsSendItem parse_send(Int client_id, Value key,
                                      const Value &frame) const {
    if (!key.has_value()) {
      throw std::invalid_argument("web client send requires a key");
    }
    if (!frame.has_value()) {
      throw std::invalid_argument("web client send requires a frame");
    }
    const auto fields = frame.view().as_bundle();
    WsSendItem result;
    result.client_id = client_id;
    result.key = std::move(key);
    result.kind = enum_or(fields, "kind", WsFrameKind::Text);
    switch (result.kind) {
    case WsFrameKind::Text:
      result.payload = text_or(fields, "text");
      break;
    case WsFrameKind::Close:
      result.close_code = int_or(fields, "close_code", 1000);
      result.close_reason = text_or(fields, "close_reason");
      break;
    case WsFrameKind::Binary:
    case WsFrameKind::Ping:
    case WsFrameKind::Pong:
      result.payload = bytes_or(fields, "data");
      break;
    }
    if (result.payload.size() > config_.ws_max_frame_bytes) {
      throw std::invalid_argument(
          "outbound WebSocket frame exceeds ws_max_frame_bytes");
    }
    result.bytes = result.payload.size() + result.close_reason.size() + 256;
    return result;
  }

  // -------------------------------------------------------------------------
  // Bridge pushes (safe from either thread)

  void push_response(Value envelope, std::size_t retained,
                     std::size_t reserved) noexcept {
    try {
      static_cast<void>(bridge_.value->push_reserved(
          index(ClientChannel::Response), std::move(envelope),
          std::min(retained, reserved), reserved));
    } catch (...) {
      bridge_.value->release_reservation(index(ClientChannel::Response),
                                         reserved);
    }
  }

  void reject_call(Int client_id, Int error_code, Str message,
                   bool retriable = false) noexcept {
    try {
      Value envelope =
          response_envelope(bindings_, client_id, Value{},
                            transport_error(bindings_, error_code,
                                            std::move(message), retriable));
      static_cast<void>(bridge_.value->push_control(
          index(ClientChannel::Response), std::move(envelope), 512));
    } catch (...) {
    }
  }

  void reject_ws_key(Value key, Str message) noexcept {
    try {
      Value envelope = ws_client_envelope(
          bindings_, std::move(key),
          ws_event(bindings_, 0, WsConnectionState::Failed, 0, message),
          Value{});
      static_cast<void>(bridge_.value->push_control(
          index(ClientChannel::WsIngress), std::move(envelope), 512));
      emit_event(WebSeverity::Warning, Str{"client"}, Str{"ws_connect"},
                 std::move(message),
                 simulation_ ? kSimulationCode : kStoppedCode, false, false);
    } catch (...) {
    }
  }

  void report_send(Int client_id, WebDeliveryStatus status, Int error_code,
                   Str message, bool retriable, bool control) noexcept {
    try {
      const Int sequence = ++sequence_;
      const std::size_t retained = message.size() + 512;
      Value report = delivery_report(bindings_, client_id, sequence, status,
                                     error_code, retriable, message);
      Value envelope =
          delivery_envelope(bindings_, client_id, std::move(report));
      if (control) {
        static_cast<void>(bridge_.value->push_control(
            index(ClientChannel::SendDelivery), std::move(envelope), retained));
        return;
      }
      if (!bridge_.value->push(index(ClientChannel::SendDelivery),
                               envelope.clone(), retained)) {
        // A delivery report the payload lane could not take still has to
        // reach its requester; the control lane is sized for exactly this.
        static_cast<void>(bridge_.value->push_control(
            index(ClientChannel::SendDelivery), std::move(envelope), retained));
      }
    } catch (...) {
    }
  }

  void emit_event(WebSeverity severity, Str component, Str category,
                  Str message, Int error_code = 0, bool retriable = false,
                  bool fatal = false, Int connection_id = 0) noexcept {
    try {
      const std::size_t retained = message.size() + component.size() +
                                   category.size() + path_.size() + 256;
      Value event = web_event(bindings_, severity, std::move(component),
                              std::move(category), path_, std::move(message),
                              error_code, retriable, fatal, connection_id);
      const bool stop_graph =
          fatal && config_.failure_policy == WebFailurePolicy::StopGraph;
      Value envelope = event_envelope(bindings_, std::move(event), stop_graph);
      if (!bridge_.value->push(index(ClientChannel::Event), envelope.clone(),
                               retained)) {
        static_cast<void>(bridge_.value->push_control(
            index(ClientChannel::Event), std::move(envelope), retained));
      }
    } catch (...) {
    }
  }

  // -------------------------------------------------------------------------
  // Owner thread

  void run() noexcept {
    std::optional<Clock::time_point> drain_deadline;
    if (!config_.tls.sni.empty()) {
      // libcurl derives SNI from the request URL and exposes no override, so
      // an explicit value cannot be honoured; say so rather than let it look
      // applied.  Emitted from the owner thread, never from start().
      emit_event(WebSeverity::Warning, Str{"client"}, Str{"tls"},
                 Str{"the configured TLS sni override is not supported by the "
                     "curl client transport and is ignored"},
                 0, false, false);
    }
    if (config_.stats_interval_ms > 0) {
      next_stats_ =
          Clock::now() + std::chrono::milliseconds{config_.stats_interval_ms};
    }
    while (true) {
      try {
        int running{};
        static_cast<void>(curl_multi_perform(multi_, &running));
        collect_completions();
        pump_connections();

        if (stopping_.load(std::memory_order_acquire)) {
          if (!drain_deadline.has_value()) {
            drain_deadline = Clock::now() + config_.shutdown_drain_timeout;
          }
          if (transfers_.empty() || Clock::now() >= *drain_deadline) {
            shutdown();
            return;
          }
        } else {
          apply_staged_work();
          maybe_emit_stats();
        }

        std::vector<curl_waitfd> waitfds;
        waitfds.reserve(connections_.size());
        for (const auto &connection : connections_) {
          if (connection->open && connection->socket != CURL_SOCKET_BAD) {
            waitfds.push_back(
                curl_waitfd{connection->socket, CURL_WAIT_POLLIN, 0});
          }
        }
        int ready{};
        static_cast<void>(
            curl_multi_poll(multi_, waitfds.empty() ? nullptr : waitfds.data(),
                            static_cast<unsigned int>(waitfds.size()),
                            poll_timeout_ms(), &ready));
      } catch (const std::exception &exception) {
        emit_event(WebSeverity::Fatal, Str{"client"}, Str{"worker"},
                   Str{exception.what()}, 0, false, true);
        stopping_.store(true, std::memory_order_release);
      } catch (...) {
        emit_event(WebSeverity::Fatal, Str{"client"}, Str{"worker"},
                   Str{"the web client owner thread failed"}, 0, false, true);
        stopping_.store(true, std::memory_order_release);
      }
    }
  }

  [[nodiscard]] int poll_timeout_ms() const noexcept {
    std::int64_t timeout =
        stopping_.load(std::memory_order_acquire) ? 25 : 1000;
    if (config_.stats_interval_ms > 0) {
      const auto remaining =
          std::chrono::duration_cast<std::chrono::milliseconds>(next_stats_ -
                                                                Clock::now())
              .count();
      timeout =
          std::min<std::int64_t>(timeout, std::max<std::int64_t>(1, remaining));
    }
    return static_cast<int>(timeout);
  }

  void apply_staged_work() {
    std::deque<HttpSubmission> submissions;
    std::deque<WsCommand> commands;
    std::deque<WsSendItem> sends;
    {
      std::lock_guard lock{mutex_};
      submissions.swap(pending_calls_);
      commands.swap(pending_ws_commands_);
      sends.swap(pending_sends_);
      staged_bytes_ = 0;
    }

    for (auto &submission : submissions) {
      begin_call(std::move(submission));
    }
    for (auto &command : commands) {
      if (command.added) {
        begin_connect(std::move(command.key));
      } else {
        close_connection_for(command.key, 1001,
                             Str{"graph removed the connection key"});
      }
    }

    std::deque<WsSendItem> deferred;
    for (auto &item : sends) {
      dispatch_send(std::move(item), deferred);
    }
    if (!deferred.empty()) {
      std::lock_guard lock{mutex_};
      for (const auto &item : deferred) {
        staged_bytes_ += item.bytes;
      }
      // Deferred work stays ahead of anything staged while it waited.
      pending_sends_.insert(pending_sends_.begin(),
                            std::make_move_iterator(deferred.begin()),
                            std::make_move_iterator(deferred.end()));
    }
  }

  void begin_call(HttpSubmission submission) {
    auto transfer = std::make_unique<HttpTransfer>();
    transfer->client_id = submission.client_id;
    transfer->reserved_bytes = submission.reserved_bytes;
    transfer->max_response_bytes = config_.max_response_bytes;
    transfer->easy = curl_easy_init();
    if (transfer->easy == nullptr) {
      fail_call(submission.client_id, submission.reserved_bytes,
                static_cast<Int>(CURLE_FAILED_INIT),
                Str{"curl_easy_init failed"}, false);
      return;
    }
    CURL *easy = transfer->easy;

    for (const auto &[name, value] : submission.headers) {
      const Str line = name + ": " + value;
      curl_slist *appended = curl_slist_append(transfer->headers, line.c_str());
      if (appended == nullptr) {
        fail_call(submission.client_id, submission.reserved_bytes,
                  static_cast<Int>(CURLE_OUT_OF_MEMORY),
                  Str{"the request header list could not be built"}, false);
        return;
      }
      transfer->headers = appended;
    }

    static_cast<void>(
        curl_easy_setopt(easy, CURLOPT_URL, submission.url.c_str()));
    if (transfer->headers != nullptr) {
      static_cast<void>(
          curl_easy_setopt(easy, CURLOPT_HTTPHEADER, transfer->headers));
    }
    switch (submission.method) {
    case HttpMethod::Head:
      static_cast<void>(curl_easy_setopt(easy, CURLOPT_NOBODY, 1L));
      break;
    case HttpMethod::Get:
      if (submission.body.empty()) {
        static_cast<void>(curl_easy_setopt(easy, CURLOPT_HTTPGET, 1L));
        break;
      }
      [[fallthrough]];
    default:
      static_cast<void>(curl_easy_setopt(easy, CURLOPT_CUSTOMREQUEST,
                                         method_name(submission.method)));
      break;
    }
    if (submission.method != HttpMethod::Head &&
        !(submission.method == HttpMethod::Get && submission.body.empty())) {
      // POSTFIELDSIZE first, then COPYPOSTFIELDS: the size decides how many
      // bytes curl copies, which is what makes a binary body safe.
      static_cast<void>(
          curl_easy_setopt(easy, CURLOPT_POSTFIELDSIZE_LARGE,
                           static_cast<curl_off_t>(submission.body.size())));
      static_cast<void>(curl_easy_setopt(
          easy, CURLOPT_COPYPOSTFIELDS,
          submission.body.empty() ? "" : submission.body.data()));
    }
    static_cast<void>(
        curl_easy_setopt(easy, CURLOPT_CONNECTTIMEOUT_MS,
                         static_cast<long>(submission.connect_timeout_ms)));
    static_cast<void>(
        curl_easy_setopt(easy, CURLOPT_TIMEOUT_MS,
                         static_cast<long>(submission.request_timeout_ms)));
    static_cast<void>(curl_easy_setopt(easy, CURLOPT_FOLLOWLOCATION,
                                       submission.follow_redirects ? 1L : 0L));
    static_cast<void>(curl_easy_setopt(
        easy, CURLOPT_MAXREDIRS, static_cast<long>(submission.max_redirects)));
    static_cast<void>(
        curl_easy_setopt(easy, CURLOPT_HTTP_VERSION,
                         http_version_option(submission.http_version)));
    if (config_.keep_alive_ms > 0) {
      static_cast<void>(curl_easy_setopt(
          easy, CURLOPT_MAXAGE_CONN,
          static_cast<long>(std::max<Int>(1, config_.keep_alive_ms / 1000))));
    }
    if (!config_.proxy.empty()) {
      static_cast<void>(
          curl_easy_setopt(easy, CURLOPT_PROXY, config_.proxy.c_str()));
    }
    apply_tls(easy, config_.tls);
    static_cast<void>(curl_easy_setopt(easy, CURLOPT_WRITEFUNCTION,
                                       &WebClientRuntime::on_body));
    static_cast<void>(
        curl_easy_setopt(easy, CURLOPT_WRITEDATA, transfer.get()));
    static_cast<void>(curl_easy_setopt(easy, CURLOPT_HEADERFUNCTION,
                                       &WebClientRuntime::on_header));
    static_cast<void>(
        curl_easy_setopt(easy, CURLOPT_HEADERDATA, transfer.get()));
    static_cast<void>(
        curl_easy_setopt(easy, CURLOPT_ERRORBUFFER, transfer->error));
    static_cast<void>(curl_easy_setopt(
        easy, CURLOPT_PRIVATE, static_cast<TransferTag *>(transfer.get())));

    if (curl_multi_add_handle(multi_, easy) != CURLM_OK) {
      fail_call(submission.client_id, submission.reserved_bytes,
                static_cast<Int>(CURLE_FAILED_INIT),
                Str{"the transfer could not be added to the curl multi loop"},
                false);
      return;
    }
    transfers_.push_back(std::move(transfer));
  }

  void fail_call(Int client_id, std::size_t reserved, Int error_code,
                 Str message, bool retriable) noexcept {
    push_response(
        response_envelope(bindings_, client_id, Value{},
                          transport_error(bindings_, error_code,
                                          std::move(message), retriable)),
        512, reserved);
  }

  static std::size_t on_body(char *data, std::size_t size, std::size_t nitems,
                             void *userdata) {
    auto *transfer = static_cast<HttpTransfer *>(userdata);
    const std::size_t total = size * nitems;
    if (transfer->body.size() + total > transfer->max_response_bytes) {
      transfer->truncated = true;
      return CURL_WRITEFUNC_ERROR;
    }
    transfer->body.append(data, total);
    return total;
  }

  /** Headers are captured in arrival order with duplicates preserved; a new
   * status line resets the block so only the FINAL response survives a
   * redirect chain or a 100-continue. */
  static std::size_t on_header(char *data, std::size_t size, std::size_t nitems,
                               void *userdata) {
    auto *transfer = static_cast<HttpTransfer *>(userdata);
    const std::size_t total = size * nitems;
    std::string_view line{data, total};
    while (!line.empty() && (line.back() == '\n' || line.back() == '\r')) {
      line.remove_suffix(1);
    }
    if (line.empty()) {
      return total;
    }
    const auto colon = line.find(':');
    if (colon == std::string_view::npos) {
      if (line.starts_with("HTTP/")) {
        transfer->response_headers.clear();
      }
      return total;
    }
    std::string_view name = line.substr(0, colon);
    std::string_view value = line.substr(colon + 1);
    if (!value.empty() && value.front() == ' ') {
      value.remove_prefix(1);
    }
    transfer->response_headers.emplace_back(Str{name}, Str{value});
    return total;
  }

  void collect_completions() {
    int remaining{};
    while (CURLMsg *message = curl_multi_info_read(multi_, &remaining)) {
      if (message->msg != CURLMSG_DONE) {
        continue;
      }
      CURL *easy = message->easy_handle;
      const CURLcode result = message->data.result;
      char *tag_data{};
      static_cast<void>(curl_easy_getinfo(easy, CURLINFO_PRIVATE, &tag_data));
      auto *tag = reinterpret_cast<TransferTag *>(tag_data);
      if (tag == nullptr) {
        static_cast<void>(curl_multi_remove_handle(multi_, easy));
        curl_easy_cleanup(easy);
        continue;
      }
      if (tag->kind == TransferKind::Http) {
        complete_call(static_cast<HttpTransfer *>(tag), result);
      } else {
        complete_handshake(static_cast<WsConnection *>(tag), result);
      }
    }
  }

  void complete_call(HttpTransfer *raw, CURLcode result) {
    auto owned = take_transfer(raw);
    if (!owned) {
      return;
    }
    static_cast<void>(curl_multi_remove_handle(multi_, owned->easy));
    const std::size_t reserved = owned->reserved_bytes;
    if (owned->truncated) {
      push_response(
          response_envelope(
              bindings_, owned->client_id, Value{},
              transport_error(
                  bindings_, static_cast<Int>(CURLE_FILESIZE_EXCEEDED),
                  Str{"response exceeded max_response_bytes"}, false)),
          512, reserved);
      return;
    }
    if (result != CURLE_OK) {
      // Transport failure is never disguised as an HTTP status (RFC 0024).
      Str message{curl_easy_strerror(result)};
      if (owned->error[0] != '\0') {
        message += ": ";
        message += owned->error;
      }
      push_response(
          response_envelope(bindings_, owned->client_id, Value{},
                            transport_error(bindings_, static_cast<Int>(result),
                                            std::move(message),
                                            retriable_curl_error(result))),
          512, reserved);
      return;
    }
    long status{};
    static_cast<void>(
        curl_easy_getinfo(owned->easy, CURLINFO_RESPONSE_CODE, &status));
    const std::size_t retained =
        owned->body.size() + header_bytes(owned->response_headers) + 256;
    push_response(
        response_envelope(bindings_, owned->client_id,
                          http_response(bindings_, static_cast<Int>(status),
                                        owned->response_headers,
                                        std::move(owned->body)),
                          Value{}),
        retained, reserved);
  }

  [[nodiscard]] std::unique_ptr<HttpTransfer> take_transfer(HttpTransfer *raw) {
    const auto found = std::ranges::find_if(
        transfers_, [raw](const auto &item) { return item.get() == raw; });
    if (found == transfers_.end()) {
      return {};
    }
    auto owned = std::move(*found);
    transfers_.erase(found);
    return owned;
  }

  // -------------------------------------------------------------------------
  // WebSocket connections

  void begin_connect(Value key) {
    auto connection = std::make_unique<WsConnection>();
    connection->key = std::move(key);
    const auto fields = connection->key.view().as_bundle();
    const Str url = text_or(fields, "url");
    if (url.empty()) {
      fail_connection(*connection, 0,
                      Str{"WebSocket client keys require a URL"});
      return;
    }
    connection->easy = curl_easy_init();
    if (connection->easy == nullptr) {
      fail_connection(*connection, static_cast<Int>(CURLE_FAILED_INIT),
                      Str{"curl_easy_init failed"});
      return;
    }
    CURL *easy = connection->easy;

    const auto headers = fields.at("headers");
    if (present(headers)) {
      for (const auto item : headers.as_list()) {
        const auto header = item.as_bundle();
        const Str line = header.at("name").checked_as<Str>() + ": " +
                         header.at("value").checked_as<Str>();
        curl_slist *appended =
            curl_slist_append(connection->headers, line.c_str());
        if (appended == nullptr) {
          fail_connection(*connection, static_cast<Int>(CURLE_OUT_OF_MEMORY),
                          Str{"the upgrade header list could not be built"});
          return;
        }
        connection->headers = appended;
      }
    }
    const auto subprotocols = fields.at("subprotocols");
    if (present(subprotocols)) {
      Str joined;
      for (const auto item : subprotocols.as_list()) {
        if (!joined.empty()) {
          joined += ", ";
        }
        joined += item.checked_as<Str>();
      }
      if (!joined.empty()) {
        curl_slist *appended = curl_slist_append(
            connection->headers, ("Sec-WebSocket-Protocol: " + joined).c_str());
        if (appended == nullptr) {
          fail_connection(*connection, static_cast<Int>(CURLE_OUT_OF_MEMORY),
                          Str{"the subprotocol header could not be built"});
          return;
        }
        connection->headers = appended;
      }
    }

    static_cast<void>(curl_easy_setopt(easy, CURLOPT_URL, url.c_str()));
    // CONNECT_ONLY=2 performs the WebSocket handshake and then hands the
    // connection back for curl_ws_send / curl_ws_recv.
    static_cast<void>(curl_easy_setopt(easy, CURLOPT_CONNECT_ONLY, 2L));
    if (connection->headers != nullptr) {
      static_cast<void>(
          curl_easy_setopt(easy, CURLOPT_HTTPHEADER, connection->headers));
    }
    static_cast<void>(
        curl_easy_setopt(easy, CURLOPT_CONNECTTIMEOUT_MS,
                         static_cast<long>(config_.connect_timeout_ms)));
    if (!config_.proxy.empty()) {
      static_cast<void>(
          curl_easy_setopt(easy, CURLOPT_PROXY, config_.proxy.c_str()));
    }
    apply_tls(easy, config_.tls);
    static_cast<void>(
        curl_easy_setopt(easy, CURLOPT_ERRORBUFFER, connection->error));
    static_cast<void>(curl_easy_setopt(
        easy, CURLOPT_PRIVATE, static_cast<TransferTag *>(connection.get())));

    if (curl_multi_add_handle(multi_, easy) != CURLM_OK) {
      fail_connection(*connection, static_cast<Int>(CURLE_FAILED_INIT),
                      Str{"the WebSocket handshake could not be started"});
      return;
    }
    connections_.push_back(std::move(connection));
  }

  void complete_handshake(WsConnection *raw, CURLcode result) {
    const auto found = std::ranges::find_if(
        connections_, [raw](const auto &item) { return item.get() == raw; });
    if (found == connections_.end()) {
      return;
    }
    raw->handshaking = false;
    if (result != CURLE_OK) {
      Str message{curl_easy_strerror(result)};
      if (raw->error[0] != '\0') {
        message += ": ";
        message += raw->error;
      }
      auto owned = std::move(*found);
      connections_.erase(found);
      fail_connection(*owned, static_cast<Int>(result), std::move(message),
                      retriable_curl_error(result));
      fail_staged_sends_for(owned->key, Str{"the WebSocket handshake failed"});
      return;
    }
    // The handshake finished.  The easy handle STAYS IN the multi: removing
    // a CONNECT_ONLY handle detaches its connection and curl_ws_recv then
    // fails with CURLE_BAD_FUNCTION_ARGUMENT.  The multi no longer monitors
    // the socket itself, so it also joins the poll set explicitly.
    curl_socket_t socket{CURL_SOCKET_BAD};
    static_cast<void>(
        curl_easy_getinfo(raw->easy, CURLINFO_ACTIVESOCKET, &socket));
    raw->socket = socket;
    raw->open = true;
    raw->connection_id = ++next_connection_id_;
    push_ws(ws_client_envelope(bindings_, raw->key.clone(),
                               ws_event(bindings_, raw->connection_id,
                                        WsConnectionState::Open),
                               Value{}),
            512);
  }

  void fail_connection(WsConnection &connection, Int error_code, Str message,
                       bool retriable = false) {
    push_ws(ws_client_envelope(bindings_, connection.key.clone(),
                               ws_event(bindings_, connection.connection_id,
                                        WsConnectionState::Failed, 0, message),
                               Value{}),
            message.size() + 512);
    emit_event(WebSeverity::Warning, Str{"client"}, Str{"ws_connect"},
               std::move(message), error_code, retriable, false,
               connection.connection_id);
  }

  /** Connection lifecycle is graph data (RFC 0024), so an Open/Failed/Closed
   * event falls back to the guaranteed control lane; frames do not. */
  void push_ws(Value envelope, std::size_t retained,
               bool lifecycle = true) noexcept {
    try {
      if (bridge_.value->push(
              index(ClientChannel::WsIngress),
              lifecycle ? envelope.clone() : std::move(envelope), retained)) {
        return;
      }
      if (lifecycle) {
        static_cast<void>(bridge_.value->push_control(
            index(ClientChannel::WsIngress), std::move(envelope), retained));
      }
    } catch (...) {
    }
  }

  void pump_connections() {
    for (std::size_t position = 0; position < connections_.size();) {
      WsConnection &connection = *connections_[position];
      if (!connection.open) {
        ++position;
        continue;
      }
      if (!pump_connection(connection)) {
        static_cast<void>(curl_multi_remove_handle(multi_, connection.easy));
        connections_.erase(connections_.begin() +
                           static_cast<std::ptrdiff_t>(position));
        continue;
      }
      ++position;
    }
  }

  /** Returns false when the connection has been torn down. */
  bool pump_connection(WsConnection &connection) {
    std::array<char, 16384> buffer;
    while (true) {
      std::size_t received{};
      const struct curl_ws_frame *meta{};
      const CURLcode result = curl_ws_recv(connection.easy, buffer.data(),
                                           buffer.size(), &received, &meta);
      if (result == CURLE_AGAIN) {
        return true;
      }
      if (result != CURLE_OK) {
        close_connection(connection, WsConnectionState::Closed, 1006,
                         Str{curl_easy_strerror(result)});
        return false;
      }
      if (meta == nullptr) {
        return true;
      }
      if ((meta->flags & CURLWS_CLOSE) != 0) {
        Int close_code{1005};
        Str close_reason;
        if (received >= 2) {
          close_code =
              static_cast<Int>((static_cast<unsigned char>(buffer[0]) << 8) |
                               static_cast<unsigned char>(buffer[1]));
          close_reason.assign(buffer.data() + 2, received - 2);
        }
        // Answer the peer's close so the socket shuts down cleanly, then
        // report the closure as data.
        send_frame(connection, nullptr, 0, CURLWS_CLOSE);
        close_connection(connection, WsConnectionState::Closed, close_code,
                         std::move(close_reason));
        return false;
      }
      if ((meta->flags & CURLWS_PING) != 0) {
        // CONNECT_ONLY leaves control frames to the caller.
        send_frame(connection, buffer.data(), received, CURLWS_PONG);
        continue;
      }
      if ((meta->flags & (CURLWS_TEXT | CURLWS_BINARY | CURLWS_CONT)) == 0) {
        continue;
      }
      if (!connection.message_active) {
        connection.message_active = true;
        connection.message_is_text = (meta->flags & CURLWS_TEXT) != 0;
        connection.message.clear();
      }
      if (connection.message.size() + received > config_.ws_max_message_bytes) {
        close_connection(connection, WsConnectionState::Failed, 1009,
                         Str{"inbound message exceeded ws_max_message_bytes"});
        return false;
      }
      connection.message.append(buffer.data(), received);
      if (meta->bytesleft != 0 || (meta->flags & CURLWS_CONT) != 0) {
        continue;
      }
      std::string message = std::move(connection.message);
      connection.message.clear();
      connection.message_active = false;
      if (!deliver_frame(connection, std::move(message))) {
        return false;
      }
    }
  }

  /** Returns false when the ingress lane forced the connection closed. */
  bool deliver_frame(WsConnection &connection, std::string payload) {
    const std::size_t retained = payload.size() + 256;
    Value frame = connection.message_is_text
                      ? ws_text_frame(bindings_, std::move(payload))
                      : ws_binary_frame(bindings_, std::move(payload));
    Value envelope = ws_client_envelope(bindings_, connection.key.clone(),
                                        Value{}, std::move(frame));
    if (bridge_.value->push(index(ClientChannel::WsIngress),
                            std::move(envelope), retained)) {
      return true;
    }
    // Client WS ingress has no read-pause seam in v1 (curl owns the socket
    // under CONNECT_ONLY), so a full lane closes the connection with 1013
    // rather than silently dropping graph-visible data (RFC 0024, flow
    // control: no silent unbounded mode).
    dropped_.fetch_add(1, std::memory_order_relaxed);
    close_connection(connection, WsConnectionState::Failed, 1013,
                     Str{"web client WS ingress is full"});
    return false;
  }

  void close_connection(WsConnection &connection, WsConnectionState state,
                        Int close_code, Str reason) {
    if (state != WsConnectionState::Closed) {
      send_close(connection, close_code, reason);
    }
    connection.open = false;
    connection.closing = true;
    push_ws(ws_client_envelope(bindings_, connection.key.clone(),
                               ws_event(bindings_, connection.connection_id,
                                        state, close_code, reason),
                               Value{}),
            reason.size() + 512);
    if (state == WsConnectionState::Failed) {
      emit_event(WebSeverity::Warning, Str{"client"}, Str{"ws"},
                 std::move(reason), close_code, false, false,
                 connection.connection_id);
    }
    fail_staged_sends_for(connection.key,
                          Str{"the WebSocket connection closed"});
  }

  void close_connection_for(const Value &key, Int close_code, Str reason) {
    const auto found =
        std::ranges::find_if(connections_, [&key](const auto &item) {
          return item->key.view().equals(key.view());
        });
    if (found == connections_.end()) {
      return;
    }
    WsConnection &connection = **found;
    if (connection.open) {
      send_close(connection, close_code, reason);
    }
    // The runtime node already erased the key from the output, so the
    // closure needs no further ingress record.
    fail_staged_sends_for(key, Str{"the WebSocket connection closed"});
    static_cast<void>(curl_multi_remove_handle(multi_, connection.easy));
    connections_.erase(found);
  }

  void send_close(WsConnection &connection, Int close_code, const Str &reason) {
    std::string payload;
    payload.push_back(static_cast<char>((close_code >> 8) & 0xFF));
    payload.push_back(static_cast<char>(close_code & 0xFF));
    payload.append(reason);
    send_frame(connection, payload.data(), payload.size(), CURLWS_CLOSE);
    connection.closing = true;
  }

  /** curl_ws_send accepts a frame in pieces; a partial write continues with
   * CURLWS_OFFSET, which is curl's documented continuation form. */
  CURLcode send_frame(WsConnection &connection, const char *data,
                      std::size_t size, unsigned int flags) {
    if (connection.easy == nullptr) {
      return CURLE_SEND_ERROR;
    }
    const auto deadline = Clock::now() + kSendDeadline;
    std::size_t offset{};
    while (true) {
      std::size_t sent{};
      const unsigned int call_flags =
          offset == 0 ? flags : (flags | CURLWS_OFFSET);
      const CURLcode result =
          curl_ws_send(connection.easy, data == nullptr ? "" : data + offset,
                       size - offset, &sent, 0, call_flags);
      offset += sent;
      if (result == CURLE_OK && offset >= size) {
        return CURLE_OK;
      }
      if (result != CURLE_OK && result != CURLE_AGAIN) {
        return result;
      }
      if (Clock::now() >= deadline) {
        return CURLE_AGAIN;
      }
      if (connection.socket != CURL_SOCKET_BAD) {
        curl_waitfd writable{connection.socket, CURL_WAIT_POLLOUT, 0};
        int ready{};
        static_cast<void>(curl_multi_poll(multi_, &writable, 1, 5, &ready));
      }
    }
  }

  void dispatch_send(WsSendItem item, std::deque<WsSendItem> &deferred) {
    const auto found =
        std::ranges::find_if(connections_, [&item](const auto &connection) {
          return connection->key.view().equals(item.key.view());
        });
    if (found == connections_.end()) {
      report_send(item.client_id, WebDeliveryStatus::PermanentFailure,
                  kNotConnectedCode, Str{"WebSocket is not connected"}, false,
                  true);
      return;
    }
    WsConnection &connection = **found;
    if (connection.handshaking) {
      // The key was accepted but the upgrade has not completed; holding the
      // frame is bounded by the same staging limits and avoids failing every
      // send a graph issues in the cycle it adds the key.
      deferred.push_back(std::move(item));
      return;
    }
    if (!connection.open) {
      report_send(item.client_id, WebDeliveryStatus::PermanentFailure,
                  kNotConnectedCode, Str{"WebSocket is not connected"}, false,
                  true);
      return;
    }

    CURLcode result{CURLE_OK};
    switch (item.kind) {
    case WsFrameKind::Text:
      result = send_frame(connection, item.payload.data(), item.payload.size(),
                          CURLWS_TEXT);
      break;
    case WsFrameKind::Binary:
      result = send_frame(connection, item.payload.data(), item.payload.size(),
                          CURLWS_BINARY);
      break;
    case WsFrameKind::Ping:
      result = send_frame(connection, item.payload.data(), item.payload.size(),
                          CURLWS_PING);
      break;
    case WsFrameKind::Pong:
      result = send_frame(connection, item.payload.data(), item.payload.size(),
                          CURLWS_PONG);
      break;
    case WsFrameKind::Close:
      // A graph-initiated close still reads until the peer echoes it, so the
      // Closed event carries the peer's code (RFC 0024, WS lifecycle is data).
      send_close(connection, item.close_code, item.close_reason);
      break;
    }
    if (result != CURLE_OK) {
      const bool retriable =
          result == CURLE_AGAIN || retriable_curl_error(result);
      report_send(item.client_id,
                  retriable ? WebDeliveryStatus::RetriableFailure
                            : WebDeliveryStatus::PermanentFailure,
                  static_cast<Int>(result), Str{curl_easy_strerror(result)},
                  retriable, true);
      return;
    }
    report_send(item.client_id, WebDeliveryStatus::Delivered, 0, Str{}, false,
                false);
  }

  void fail_staged_sends_for(const Value &key, Str message) {
    std::deque<WsSendItem> staged;
    {
      std::lock_guard lock{mutex_};
      auto item = pending_sends_.begin();
      while (item != pending_sends_.end()) {
        if (!item->key.view().equals(key.view())) {
          ++item;
          continue;
        }
        staged_bytes_ -= std::min(staged_bytes_, item->bytes);
        staged.push_back(std::move(*item));
        item = pending_sends_.erase(item);
      }
    }
    for (const auto &item : staged) {
      report_send(item.client_id, WebDeliveryStatus::PermanentFailure,
                  kNotConnectedCode, message, false, true);
    }
  }

  // -------------------------------------------------------------------------
  // Statistics and shutdown

  void maybe_emit_stats() {
    if (config_.stats_interval_ms <= 0 || Clock::now() < next_stats_) {
      return;
    }
    next_stats_ =
        Clock::now() + std::chrono::milliseconds{config_.stats_interval_ms};
    std::size_t staged{};
    {
      std::lock_guard lock{mutex_};
      staged = pending_sends_.size();
    }
    const auto open_connections = static_cast<Int>(std::ranges::count_if(
        connections_, [](const auto &connection) { return connection->open; }));
    Value stats = build(
        bindings_.client_stats,
        {
            {"in_flight_count",
             bindings_.number(static_cast<Int>(transfers_.size()))},
            {"staged_count", bindings_.number(static_cast<Int>(staged))},
            {"ws_connection_count", bindings_.number(open_connections)},
            {"ingress_record_count",
             bindings_.number(static_cast<Int>(bridge_.value->payload_pending(
                 index(ClientChannel::Response))))},
            {"ingress_byte_count", bindings_.number(static_cast<Int>(
                                       bridge_.value->payload_retained_bytes(
                                           index(ClientChannel::Response))))},
            {"dropped_count", bindings_.number(static_cast<Int>(
                                  dropped_.load(std::memory_order_relaxed)))},
        });
    bridge_.value->push_latest(index(ClientChannel::Stats), std::move(stats),
                               1);
  }

  void shutdown() noexcept {
    try {
      // A call staged in the engine cycle that preceded stop still holds its
      // response reservation; answering it here is what keeps "a request is
      // never silently dropped" true through teardown (RFC 0024).
      std::deque<HttpSubmission> staged;
      {
        std::lock_guard lock{mutex_};
        staged.swap(pending_calls_);
      }
      for (const auto &submission : staged) {
        fail_call(submission.client_id, submission.reserved_bytes, kStoppedCode,
                  Str{"the web client stopped before the request was sent"},
                  true);
      }
      for (auto &transfer : transfers_) {
        static_cast<void>(curl_multi_remove_handle(multi_, transfer->easy));
        // The reservation is consumed by the report, so nothing leaks even
        // when the bridge has already refused the push.
        push_response(
            response_envelope(
                bindings_, transfer->client_id, Value{},
                transport_error(bindings_, kStoppedCode,
                                Str{"the web client stopped before the "
                                    "response arrived"},
                                true)),
            512, transfer->reserved_bytes);
      }
      transfers_.clear();
      for (auto &connection : connections_) {
        if (connection->handshaking) {
          static_cast<void>(curl_multi_remove_handle(multi_, connection->easy));
          continue;
        }
        if (connection->open) {
          send_close(*connection, 1001, Str{"the web client is stopping"});
        }
      }
      connections_.clear();

      std::deque<WsSendItem> sends;
      {
        std::lock_guard lock{mutex_};
        sends.swap(pending_sends_);
        staged_bytes_ = 0;
      }
      for (const auto &item : sends) {
        report_send(item.client_id, WebDeliveryStatus::EnqueueRejected,
                    kStoppedCode,
                    Str{"the web client stopped before the frame was sent"},
                    true, true);
      }
    } catch (...) {
    }
  }

  static constexpr std::chrono::milliseconds kSendDeadline{250};

  ClientRuntimeConfig config_;
  ValueBindings bindings_{};
  Str path_;
  ClientBridgeHandle bridge_;
  bool simulation_{};
  bool started_{};
  bool simulation_warned_{};

  std::atomic<bool> accepting_{};
  std::atomic<bool> stopping_{};
  std::atomic<bool> stopped_{};
  std::atomic<std::size_t> dropped_{};
  std::thread thread_{};
  CURLM *multi_{};

  mutable std::mutex mutex_{};
  std::deque<HttpSubmission> pending_calls_{};
  std::deque<WsCommand> pending_ws_commands_{};
  std::deque<WsSendItem> pending_sends_{};
  std::size_t staged_bytes_{};
  std::atomic<Int> sequence_{};

  // Owner-thread only.
  std::vector<std::unique_ptr<HttpTransfer>> transfers_{};
  std::vector<std::unique_ptr<WsConnection>> connections_{};
  Int next_connection_id_{};
  Clock::time_point next_stats_{};
};
} // namespace hgraph::web::detail

namespace hgraph::web {
namespace {
namespace wd = ::hgraph::web::detail;

struct WebClientRuntimeNode {
  static constexpr auto name = "web_curl_client_runtime";
  using signature_args = std::tuple<
      In<"calls", TSD<Int, HttpClientCall>, InputValidity::Unchecked>,
      In<"ws_keys", TSS<WsClientKey>, InputValidity::Unchecked>,
      In<"ws_sends", TSD<Int, WsClientSendRequest>, InputValidity::Unchecked>,
      Scalar<"config", wd::ClientRuntimeConfigHandle>, Scalar<"path", Str>,
      Scalar<"bridge", wd::ClientBridgeHandle>, State<wd::ClientRuntimeHandle>>;

  static void start(Scalar<"config", wd::ClientRuntimeConfigHandle> config,
                    Scalar<"path", Str> path,
                    Scalar<"bridge", wd::ClientBridgeHandle> bridge,
                    State<wd::ClientRuntimeHandle> state,
                    EngineControlView engine) {
    auto runtime = std::make_shared<wd::WebClientRuntime>(
        *config.value().value, path.value(), bridge.value(),
        engine.mode() == GraphExecutorMode::Simulation);
    runtime->start();
    try {
      state.set(wd::ClientRuntimeHandle{runtime});
    } catch (...) {
      runtime->stop();
      throw;
    }
  }

  static void
  eval(In<"calls", TSD<Int, HttpClientCall>, InputValidity::Unchecked> calls,
       In<"ws_keys", TSS<WsClientKey>, InputValidity::Unchecked> ws_keys,
       In<"ws_sends", TSD<Int, WsClientSendRequest>, InputValidity::Unchecked>
           ws_sends,
       Scalar<"bridge", wd::ClientBridgeHandle> bridge,
       State<wd::ClientRuntimeHandle> state) {
    auto runtime = state.get().value;
    if (!runtime) {
      throw std::logic_error("Web client runtime evaluated before start");
    }

    if (calls.modified()) {
      for (const auto &[client_id, call] : calls.modified_items()) {
        auto request = call.template field<"request">();
        auto options = call.template field<"options">();
        if (!request.modified() || !request.valid()) {
          continue;
        }
        runtime->call(client_id.template checked_as<Int>(),
                      request.base().value().clone(),
                      options.valid() ? options.base().value().clone()
                                      : Value{});
      }
    }

    if (ws_keys.modified()) {
      const auto &erased = static_cast<const TSSInputView &>(ws_keys);
      for (const auto key : erased.removed()) {
        if (!wd::erase_keyed<wd::WsClientEnvelope>(
                *bridge.value().value, wd::index(wd::ClientChannel::WsIngress),
                "key", key.clone())) {
          throw std::overflow_error("Web client key-removal queue is full");
        }
        runtime->ws_key_removed(key.clone());
      }
      for (const auto key : erased.added()) {
        runtime->ws_key_added(key.clone());
      }
    }

    if (ws_sends.modified()) {
      for (const auto &[client_id, request] : ws_sends.modified_items()) {
        auto frame = request.template field<"frame">();
        auto key = request.template field<"key">();
        if (!frame.modified() || !frame.valid()) {
          continue;
        }
        if (!key.valid()) {
          throw std::invalid_argument(
              "Web WS client send requires a valid key");
        }
        runtime->ws_send(client_id.template checked_as<Int>(),
                         key.base().value().clone(),
                         frame.base().value().clone());
      }
    }
  }

  static void stop(State<wd::ClientRuntimeHandle> state) {
    if (auto runtime = state.get().value) {
      runtime->stop();
    }
    state.set(wd::ClientRuntimeHandle{});
  }
};

struct WebClientImpl {
  static constexpr auto name = "web_client_impl";

  static void compose(Wiring &w, Scalar<"config", Value> config,
                      Scalar<"path", Str> path) {
    register_web_types();
    wd::register_internal_types();
    // Wiring is parse/validate only (RFC 0024, lifecycle): a graph that wires
    // has a structurally valid configuration and has bound no socket.
    wd::ClientRuntimeConfigHandle runtime_config{
        std::make_shared<const wd::ClientRuntimeConfig>(
            wd::parse_client_config(config.value()))};

    const auto binding = service::path(path.value());
    auto calls = service::impl_input<HttpClientService>(w, binding);
    auto ws_keys = service::impl_input<WsClientService>(w, binding);
    auto ws_sends = service::impl_input<WsClientSendService>(w, binding);

    auto bridge = wd::make_client_bridge(config.value());
    auto outputs = wd::wire_client_outputs(w, bridge);

    static_cast<void>(wire<WebClientRuntimeNode>(
        w, calls, ws_keys, ws_sends, runtime_config, path.value(), bridge));

    service::impl_output<HttpClientService>(w, binding, outputs.responses);
    service::impl_output<WsClientService>(w, binding, outputs.ws);
    service::impl_output<WsClientSendService>(w, binding, outputs.send_reports);
    service::impl_output<WebClientEventService>(w, binding, outputs.events);
    service::impl_output<WebClientStatsService>(w, binding, outputs.stats);
  }
};
} // namespace

void register_client(Wiring &w, service::ServicePath path,
                     Value client_config) {
  service::register_services<WebClientImpl, HttpClientService, WsClientService,
                             WsClientSendService, WebClientEventService,
                             WebClientStatsService>(w, std::move(path),
                                                    std::move(client_config));
}
} // namespace hgraph::web
