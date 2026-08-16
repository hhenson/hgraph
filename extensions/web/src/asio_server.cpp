// The server transport translation unit: the ONLY file that includes
// Boost.Asio/Beast and server-side OpenSSL (RFC 0024, packaging).  It owns
// listener sockets, TLS contexts, connection strands, the compiled route
// tables, and the strict stop ordering.  The HTTP/2 seam is the ALPN
// protocol dispatch below: the h1 branch is the only one wired until
// nghttp2_session.cpp activates (config validation rejects advertising h2).

#include <hgraph/web/service.h>
#include <hgraph/web/value_builders.h>

#include "detail/route_table.h"
#include "detail/service_bridge.h"
#include "detail/stream_model.h"

#include <boost/asio.hpp>
#include <boost/asio/ssl.hpp>
#include <boost/beast/core.hpp>
#include <boost/beast/http.hpp>
#include <boost/beast/ssl.hpp>
#include <boost/beast/websocket.hpp>

#include <openssl/ssl.h>
#include <openssl/x509.h>


#include <atomic>
#include <chrono>
#include <compare>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <ostream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <tuple>
#include <typeindex>
#include <utility>
#include <variant>
#include <vector>

namespace asio = boost::asio;
namespace beast = boost::beast;
namespace http = boost::beast::http;
namespace websocket = boost::beast::websocket;
using tcp = asio::ip::tcp;

namespace hgraph::web::detail {
namespace {
// ---------------------------------------------------------------------------
// Value construction helpers (worker threads build only against schemas and
// plan bindings pre-warmed at compose/start; kafka precedent).

template <typename T> [[nodiscard]] Value atomic_value(T value) {
  static_cast<void>(scalar_descriptor<T>::value_meta());
  return Value{std::move(value)};
}

template <typename Schema>
[[nodiscard]] Value
bundle_value(std::vector<std::pair<std::string_view, Value>> fields) {
  BundleBuilder builder{ValuePlanFactory::instance().type_for(
      scalar_descriptor<Schema>::value_meta())};
  for (auto &[name, field] : fields) {
    if (field.has_value()) {
      builder.set(name, std::move(field));
    }
  }
  return builder.build();
}

template <typename Element>
[[nodiscard]] Value tuple_value(std::vector<Value> values) {
  const auto element_binding =
      ValuePlanFactory::instance().type_for(scalar_descriptor<Element>::value_meta());
  const auto tuple_binding = ValuePlanFactory::instance().type_for(
      scalar_descriptor<HomogeneousTuple<Element>>::value_meta());
  ListBuilder builder{element_binding};
  for (const Value &value : values) {
    builder.push_back_copy(value.view().data());
  }
  ListStorage storage = builder.build_storage();
  return Value{tuple_binding, &storage};
}

using NamedPairs = std::vector<std::pair<std::string, std::string>>;

template <typename Schema>
[[nodiscard]] Value name_value_tuple(const NamedPairs &pairs) {
  std::vector<Value> erased;
  erased.reserve(pairs.size());
  for (const auto &[name, value] : pairs) {
    erased.push_back(bundle_value<Schema>({
        {"name", atomic_value(Str{name})},
        {"value", atomic_value(Str{value})},
    }));
  }
  return tuple_value<Schema>(std::move(erased));
}

// ---------------------------------------------------------------------------
// Configuration

struct TlsServerSettings {
  bool enabled{};
  Str cert_path{};
  Str cert_pem{};
  Str key_path{};
  Str key_pem{};
  Str key_password{};
  Str ca_path{};
  Str ca_pem{};
  WebClientVerify client_verify{WebClientVerify::None};
  std::vector<Str> alpn{};
  WebTlsVersion min_version{WebTlsVersion::Tls1_2};
  Value raw{};
};

struct ServerRuntimeConfig {
  Value tls_identity{};
  Str bind_address{};
  std::uint16_t port{};
  TlsServerSettings tls{};
  std::size_t io_threads{1};
  std::size_t max_connections{};
  std::size_t max_header_bytes{};
  std::size_t max_body_bytes{};
  std::chrono::milliseconds request_timeout{};
  std::chrono::milliseconds idle_timeout{};
  std::chrono::milliseconds keep_alive_timeout{};
  bool bind_deferred{};
  OutputLimits ingress{};
  OutputLimits ws_ingress{};
  Int watermark_high_pct{};
  Int watermark_low_pct{};
  WebInboundOverflow inbound_overflow{WebInboundOverflow::Backpressure};
  std::size_t outbound_message_limit{};
  std::size_t outbound_byte_limit{};
  WebSlowConsumerPolicy slow_consumer_policy{WebSlowConsumerPolicy::Close};
  WebFailurePolicy failure_policy{WebFailurePolicy::Report};
  std::chrono::milliseconds shutdown_drain_timeout{};
  std::size_t ws_max_frame_bytes{};
  std::size_t ws_max_message_bytes{};
  std::chrono::milliseconds ping_interval{};
  std::chrono::milliseconds pong_timeout{};
  std::chrono::milliseconds stats_interval{};
};

[[nodiscard]] std::size_t positive_size(const ValueView &field,
                                        std::string_view name) {
  const Int value = field.checked_as<Int>();
  if (value <= 0) {
    throw std::invalid_argument("Web server " + std::string{name} +
                                " must be positive");
  }
  return static_cast<std::size_t>(value);
}

[[nodiscard]] std::chrono::milliseconds
milliseconds_field(const ValueView &field, std::string_view name) {
  const Int value = field.checked_as<Int>();
  if (value < 0) {
    throw std::invalid_argument("Web server " + std::string{name} +
                                " cannot be negative");
  }
  return std::chrono::milliseconds{value};
}

[[nodiscard]] ServerRuntimeConfig parse_server_config(const Value &value) {
  if (value.schema() != scalar_descriptor<WebServerConfig>::value_meta()) {
    throw std::invalid_argument("Web server requires WebServerConfig");
  }
  const auto root = value.view().as_bundle();

  ServerRuntimeConfig result;
  result.bind_address = root.at("bind_address").checked_as<Str>();
  const Int port = root.at("port").checked_as<Int>();
  if (port < 0 || port > 65'535) {
    throw std::invalid_argument("Web server port must be 0..65535");
  }
  result.port = static_cast<std::uint16_t>(port);
  result.io_threads = positive_size(root.at("io_threads"), "io_threads");
  result.max_connections =
      positive_size(root.at("max_connections"), "max_connections");
  result.max_header_bytes =
      positive_size(root.at("max_header_bytes"), "max_header_bytes");
  result.max_body_bytes =
      positive_size(root.at("max_body_bytes"), "max_body_bytes");
  result.request_timeout =
      milliseconds_field(root.at("request_timeout_ms"), "request timeout");
  result.idle_timeout =
      milliseconds_field(root.at("idle_timeout_ms"), "idle timeout");
  result.keep_alive_timeout = milliseconds_field(
      root.at("keep_alive_timeout_ms"), "keep-alive timeout");
  result.bind_deferred = root.at("bind_deferred").checked_as<Bool>();
  result.ingress = OutputLimits{
      positive_size(root.at("ingress_record_limit"), "ingress record limit"),
      positive_size(root.at("ingress_byte_limit"), "ingress byte limit"),
  };
  result.ws_ingress = OutputLimits{
      positive_size(root.at("ws_ingress_record_limit"),
                    "WS ingress record limit"),
      positive_size(root.at("ws_ingress_byte_limit"), "WS ingress byte limit"),
  };
  result.watermark_high_pct = root.at("watermark_high_pct").checked_as<Int>();
  result.watermark_low_pct = root.at("watermark_low_pct").checked_as<Int>();
  result.inbound_overflow =
      root.at("inbound_overflow").checked_as<WebInboundOverflow>();
  result.outbound_message_limit = positive_size(
      root.at("outbound_message_limit"), "outbound message limit");
  result.outbound_byte_limit =
      positive_size(root.at("outbound_byte_limit"), "outbound byte limit");
  result.slow_consumer_policy =
      root.at("slow_consumer_policy").checked_as<WebSlowConsumerPolicy>();
  result.failure_policy =
      root.at("failure_policy").checked_as<WebFailurePolicy>();
  result.shutdown_drain_timeout = milliseconds_field(
      root.at("shutdown_drain_timeout_ms"), "shutdown drain timeout");
  result.ws_max_frame_bytes =
      positive_size(root.at("ws_max_frame_bytes"), "WS frame limit");
  result.ws_max_message_bytes =
      positive_size(root.at("ws_max_message_bytes"), "WS message limit");
  result.ping_interval =
      milliseconds_field(root.at("ping_interval_ms"), "ping interval");
  result.pong_timeout =
      milliseconds_field(root.at("pong_timeout_ms"), "pong timeout");
  result.stats_interval =
      milliseconds_field(root.at("stats_interval_ms"), "stats interval");

  const auto tls = root.at("tls");
  if (tls.data() != nullptr) {
    result.tls_identity = tls.clone();
    const auto fields = tls.as_bundle();
    const auto text = [&](std::string_view name) {
      const auto field = fields.at(name);
      return field.data() != nullptr ? field.checked_as<Str>() : Str{};
    };
    result.tls.enabled = true;
    result.tls.cert_path = text("cert_path");
    result.tls.cert_pem = text("cert_pem");
    result.tls.key_path = text("key_path");
    result.tls.key_pem = text("key_pem");
    result.tls.key_password = text("key_password");
    result.tls.ca_path = text("ca_path");
    result.tls.ca_pem = text("ca_pem");
    result.tls.client_verify =
        fields.at("client_verify").checked_as<WebClientVerify>();
    result.tls.min_version =
        fields.at("min_version").checked_as<WebTlsVersion>();
    for (const auto protocol : fields.at("alpn").as_list()) {
      result.tls.alpn.push_back(protocol.checked_as<Str>());
    }
  }
  return result;
}

struct ServerConfigHandle {
  std::shared_ptr<const ServerRuntimeConfig> value{};

  friend bool operator==(const ServerConfigHandle &,
                         const ServerConfigHandle &) noexcept = default;
  friend std::strong_ordering
  operator<=>(const ServerConfigHandle &lhs,
              const ServerConfigHandle &rhs) noexcept {
    return reinterpret_cast<std::uintptr_t>(lhs.value.get()) <=>
           reinterpret_cast<std::uintptr_t>(rhs.value.get());
  }
};

inline std::ostream &operator<<(std::ostream &stream,
                                const ServerConfigHandle &value) {
  return stream << "ServerConfigHandle(" << value.value.get() << ')';
}

[[nodiscard]] std::optional<HttpMethod> method_from(http::verb verb) noexcept {
  switch (verb) {
  case http::verb::get:
    return HttpMethod::Get;
  case http::verb::head:
    return HttpMethod::Head;
  case http::verb::post:
    return HttpMethod::Post;
  case http::verb::put:
    return HttpMethod::Put;
  case http::verb::delete_:
    return HttpMethod::Delete;
  case http::verb::patch:
    return HttpMethod::Patch;
  case http::verb::options:
    return HttpMethod::Options;
  case http::verb::trace:
    return HttpMethod::Trace;
  default:
    return std::nullopt;
  }
}

// Query strings stay raw: decoding is a codec-tier decision (RFC 0024).
[[nodiscard]] NamedPairs parse_query(std::string_view query) {
  NamedPairs result;
  while (!query.empty()) {
    const auto amp = query.find('&');
    const auto item = query.substr(0, amp);
    if (!item.empty()) {
      const auto eq = item.find('=');
      if (eq == std::string_view::npos) {
        result.emplace_back(std::string{item}, std::string{});
      } else {
        result.emplace_back(std::string{item.substr(0, eq)},
                            std::string{item.substr(eq + 1)});
      }
    }
    if (amp == std::string_view::npos) {
      break;
    }
    query.remove_prefix(amp + 1);
  }
  return result;
}

class WebServerRuntime;

struct MatchedRoute {
  std::shared_ptr<WebServerRuntime> runtime{};
  Value route{};
  NamedPairs params{};
};

// ---------------------------------------------------------------------------
// Listener: owns the io_context pool and acceptor for one (address, port).
// Several server runtimes may attach (RFC 0024, routing/port sharing); the
// first to start binds, later starts attach after a TLS-identity check, and
// the last detach closes the listener.

class WebListener : public std::enable_shared_from_this<WebListener> {
public:
  WebListener(std::string address, std::uint16_t port, Value tls_identity,
              std::size_t io_threads)
      : address_{std::move(address)}, port_{port},
        tls_identity_{std::move(tls_identity)}, io_threads_{io_threads},
        acceptor_{io_context_} {}

  ~WebListener() { stop_io(); }

  [[nodiscard]] asio::io_context &io_context() noexcept { return io_context_; }

  [[nodiscard]] std::uint16_t bound_port() const noexcept {
    return bound_port_.load(std::memory_order_acquire);
  }

  [[nodiscard]] const Value &tls_identity() const noexcept {
    return tls_identity_;
  }

  void attach(std::shared_ptr<WebServerRuntime> runtime) {
    std::lock_guard lock{mutex_};
    runtimes_.push_back(std::move(runtime));
  }

  /** @return true when this was the last attached runtime. */
  bool detach(const WebServerRuntime *runtime) {
    std::lock_guard lock{mutex_};
    std::erase_if(runtimes_, [runtime](const auto &attached) {
      return attached.get() == runtime;
    });
    return runtimes_.empty();
  }

  void ensure_listening();
  void start_io();
  void stop_accepting();
  void stop_io();

  [[nodiscard]] std::optional<MatchedRoute>
  match(HttpMethod method, std::string_view path, bool upgrade);

  void connection_opened() noexcept { ++open_connections_; }
  void connection_closed() noexcept { --open_connections_; }
  [[nodiscard]] std::size_t open_connections() const noexcept {
    return open_connections_.load(std::memory_order_relaxed);
  }
  [[nodiscard]] bool at_connection_limit(std::size_t limit) const noexcept {
    return open_connections_.load(std::memory_order_relaxed) >= limit;
  }

  void set_reads_paused(bool paused);
  [[nodiscard]] bool reads_paused() const noexcept {
    return reads_paused_.load(std::memory_order_acquire);
  }
  void park_for_resume(std::function<void()> resume);

  [[nodiscard]] std::shared_ptr<WebServerRuntime> owner() {
    std::lock_guard lock{mutex_};
    return runtimes_.empty() ? nullptr : runtimes_.front();
  }

private:
  void accept_next();

  friend class ListenerRegistry;

  std::string address_{};
  std::uint16_t port_{};
  Value tls_identity_{};
  std::size_t io_threads_{1};
  asio::io_context io_context_{};
  tcp::acceptor acceptor_;
  std::optional<asio::executor_work_guard<asio::io_context::executor_type>>
      work_{};
  std::vector<std::thread> threads_{};
  std::atomic<std::uint16_t> bound_port_{0};
  std::atomic<bool> accepting_{false};
  std::atomic<bool> listening_{false};
  std::atomic<std::size_t> open_connections_{0};
  std::atomic<bool> reads_paused_{false};
  std::mutex mutex_{};
  // Owning (P1): a connection's asynchronous callbacks may outlive one
  // attachee's stop when the peer stalls its close handshake, so runtimes
  // stay alive until every reference (match results, WS connections, queued
  // frames) has drained.  detach() drops this reference at stop, breaking
  // the runtime->listener->runtime cycle.
  std::vector<std::shared_ptr<WebServerRuntime>> runtimes_{};
  std::vector<std::function<void()>> parked_{};
};

class ListenerRegistry {
public:
  static ListenerRegistry &instance() {
    static ListenerRegistry registry;
    return registry;
  }

  [[nodiscard]] std::shared_ptr<WebListener>
  acquire(const std::string &address, std::uint16_t port, const Value &tls,
          std::size_t io_threads, std::shared_ptr<WebServerRuntime> runtime) {
    std::lock_guard lock{mutex_};
    const auto key = std::make_pair(address, port);
    if (port != 0) {
      if (const auto found = listeners_.find(key); found != listeners_.end()) {
        if (auto existing = found->second.lock()) {
          const bool same_tls =
              existing->tls_identity().has_value() == tls.has_value() &&
              (!tls.has_value() ||
               existing->tls_identity().view().equals(tls.view()));
          if (!same_tls) {
            throw std::invalid_argument(
                "Web servers sharing " + address + ":" +
                std::to_string(port) +
                " must use an identical TLS configuration");
          }
          existing->attach(std::move(runtime));
          return existing;
        }
      }
    }
    auto listener = std::make_shared<WebListener>(
        address, port, tls.has_value() ? tls.clone() : Value{}, io_threads);
    listener->attach(std::move(runtime));
    if (port != 0) {
      listeners_[key] = listener;
    }
    return listener;
  }

  /** Detach; when this was the last attachee, accepting stops here and the
   * caller runs the drain before stopping the io pool itself — stopping io
   * first would strand the posted shutdown work (RFC 0024, lifecycle). */
  [[nodiscard]] bool release(const std::shared_ptr<WebListener> &listener,
                             WebServerRuntime *runtime) {
    bool last = false;
    {
      std::lock_guard lock{mutex_};
      last = listener->detach(runtime);
      if (last && listener->port_ != 0) {
        listeners_.erase(std::make_pair(listener->address_, listener->port_));
      }
    }
    if (last) {
      listener->stop_accepting();
    }
    return last;
  }

private:
  std::mutex mutex_{};
  std::map<std::pair<std::string, std::uint16_t>,
           std::weak_ptr<WebListener>>
      listeners_{};
};

// ---------------------------------------------------------------------------
// Runtime

struct CompiledRoutes {
  RouteTable table{RouteTable::build({})};
  std::vector<Value> routes{};
};

// Lock-free published-snapshot access for io threads.  Apple's libc++ does
// not provide std::atomic<std::shared_ptr>, and GCC deprecates the free
// functions; the deprecation is suppressed here and nowhere else.
#if defined(__GNUC__) && !defined(__clang__)
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wdeprecated-declarations"
#endif
[[nodiscard]] inline std::shared_ptr<const CompiledRoutes>
atomic_load_routes(const std::shared_ptr<const CompiledRoutes> *slot) {
  return std::atomic_load(slot);
}

inline void atomic_store_routes(std::shared_ptr<const CompiledRoutes> *slot,
                                std::shared_ptr<const CompiledRoutes> value) {
  std::atomic_store(slot, std::move(value));
}
#if defined(__GNUC__) && !defined(__clang__)
#pragma GCC diagnostic pop
#endif

class ServerConnection;

class WebServerRuntime
    : public std::enable_shared_from_this<WebServerRuntime> {
public:
  WebServerRuntime(std::shared_ptr<const ServerRuntimeConfig> config, Str path,
                   ServerBridgeHandle bridge, bool simulation)
      : config_{std::move(config)}, path_{std::move(path)},
        bridge_{std::move(bridge)}, simulation_{simulation} {}

  ~WebServerRuntime() { stop(); }

  void start();
  void stop() noexcept;

  void apply_http_routes(std::vector<Value> added, std::vector<Value> removed);
  void apply_ws_routes(std::vector<Value> added, std::vector<Value> removed);
  void respond(Int client_id, Int request_id, Value response);
  void ws_send(Int client_id, Int connection_id, Value frame);

  // --- transport-side entry points (io threads) ---
  [[nodiscard]] const ServerRuntimeConfig &config() const noexcept {
    return *config_;
  }
  [[nodiscard]] std::shared_ptr<const ServerRuntimeConfig>
  config_ptr() const noexcept {
    return config_;
  }
  [[nodiscard]] ServerBridgeHandle &bridge() noexcept { return bridge_; }
  [[nodiscard]] asio::ssl::context *tls_context() noexcept {
    return tls_context_ ? tls_context_.get() : nullptr;
  }
  [[nodiscard]] std::shared_ptr<const CompiledRoutes> http_routes() const {
    return atomic_load_routes(&http_routes_);
  }
  [[nodiscard]] std::shared_ptr<const CompiledRoutes> ws_routes() const {
    return atomic_load_routes(&ws_routes_);
  }

  [[nodiscard]] Int register_pending(const std::shared_ptr<ServerConnection> &connection);
  void unregister_pending(Int request_id) noexcept;
  [[nodiscard]] Int register_ws_connection(
      const std::shared_ptr<ServerConnection> &connection);
  void unregister_ws_connection(Int connection_id) noexcept;

  [[nodiscard]] bool push_request(Value route, Value request);
  void push_ws_event(Value route, Value event);
  void push_ws_frame(Value route, Value inbound_frame);
  void report(std::size_t channel, Int client_id, Value report_value);
  void emit_event(WebSeverity severity, Str component, Str category,
                  Str message, Int error_code = 0, bool retriable = false,
                  bool fatal = false, Int connection_id = 0);
  void count_drop() noexcept { ++dropped_; }

  [[nodiscard]] std::shared_ptr<WebListener> listener() const noexcept {
    return listener_;
  }

private:
  void sweep_expired_requests();
  void start_stats_timer();
  void rebuild(std::shared_ptr<const CompiledRoutes> &slot,
               std::vector<std::tuple<HttpMethod, std::string, Value>> &master,
               std::vector<Value> added, std::vector<Value> removed);

  friend class ServerConnection;

  std::shared_ptr<const ServerRuntimeConfig> config_{};
  Str path_{};
  ServerBridgeHandle bridge_{};
  bool simulation_{};
  bool started_{};
  std::shared_ptr<WebListener> listener_{};
  std::unique_ptr<asio::ssl::context> tls_context_{};
  std::shared_ptr<const CompiledRoutes> http_routes_{
      std::make_shared<CompiledRoutes>()};
  std::shared_ptr<const CompiledRoutes> ws_routes_{
      std::make_shared<CompiledRoutes>()};
  std::mutex routes_mutex_{};
  std::vector<std::tuple<HttpMethod, std::string, Value>> http_master_{};
  std::vector<std::tuple<HttpMethod, std::string, Value>> ws_master_{};

  std::mutex pending_mutex_{};
  std::atomic<Int> next_request_id_{0};
  struct Pending {
    // Owning: between dispatch and the graph's answer no async operation
    // holds the connection, so the pending registry keeps it alive until it
    // is answered, timed out, or shut down.
    std::shared_ptr<ServerConnection> connection{};
    std::chrono::steady_clock::time_point deadline{};
  };
  std::map<Int, Pending> pending_{};
  std::map<Int, std::weak_ptr<ServerConnection>> ws_connections_{};
  std::unique_ptr<asio::steady_timer> sweep_timer_{};
  std::unique_ptr<asio::steady_timer> stats_timer_{};
  std::atomic<Int> sequence_{0};
  std::atomic<Int> dropped_{0};
  std::atomic<bool> stopping_{false};
};

// ---------------------------------------------------------------------------
// Connection: one strand per socket; plain h1 and WebSocket.  The eval
// thread never touches a socket — respond/ws_send post owned data onto the
// strand (RFC 0024, threading).

class ServerConnection : public std::enable_shared_from_this<ServerConnection> {
public:
  ServerConnection(std::shared_ptr<WebListener> listener,
                   std::shared_ptr<const ServerRuntimeConfig> config,
                   tcp::socket socket, asio::ssl::context *tls)
      : listener_{std::move(listener)}, config_{std::move(config)},
        strand_{asio::make_strand(listener_->io_context())} {
    if (tls != nullptr) {
      tls_stream_.emplace(std::move(socket), *tls);
    } else {
      plain_stream_.emplace(std::move(socket));
    }
    listener_->connection_opened();
  }

  ~ServerConnection() { listener_->connection_closed(); }

  void run() {
    asio::post(strand_, [self = shared_from_this()] { self->handshake(); });
  }

  void deliver_response(Int request_id, Value response, Int client_id,
                        std::shared_ptr<WebServerRuntime> runtime) {
    asio::post(strand_, [self = shared_from_this(), request_id,
                         response = std::move(response), client_id, runtime] {
      self->write_response(request_id, response, client_id, runtime);
    });
  }

  void deliver_ws_frame(Value frame, Int client_id,
                        std::shared_ptr<WebServerRuntime> runtime) {
    asio::post(strand_, [self = shared_from_this(), frame = std::move(frame),
                         client_id, runtime] {
      self->queue_ws_frame(frame, client_id, runtime);
    });
  }

  void answer_timeout(Int request_id) {
    asio::post(strand_, [self = shared_from_this(), request_id] {
      if (self->pending_request_id_ == request_id && !self->writing_) {
        self->send_simple_response(http::status::service_unavailable,
                                   "the graph did not answer in time", false);
        self->pending_request_id_ = -1;
      }
    });
  }

  void begin_shutdown() {
    asio::post(strand_, [self = shared_from_this()] {
      self->shutting_down_ = true;
      if (self->ws_) {
        self->ws_close(websocket::close_code{1001}, "going away");
      } else if (self->pending_request_id_ >= 0 && !self->writing_) {
        self->send_simple_response(http::status::service_unavailable,
                                   "server shutting down", false);
        self->pending_request_id_ = -1;
      } else if (!self->writing_) {
        self->close();
      }
    });
  }

  void resume_reading() {
    asio::post(strand_, [self = shared_from_this()] {
      if (self->read_parked_) {
        self->read_parked_ = false;
        self->read_next();
      }
    });
  }

private:
  using PlainStream = beast::tcp_stream;
  using TlsStream = asio::ssl::stream<beast::tcp_stream>;
  using PlainWs = websocket::stream<beast::tcp_stream>;
  using TlsWs = websocket::stream<TlsStream>;

  void handshake() {
    if (tls_stream_.has_value()) {
      beast::get_lowest_layer(*tls_stream_)
          .expires_after(config_->idle_timeout);
      tls_stream_->async_handshake(
          asio::ssl::stream_base::server,
          asio::bind_executor(
              strand_, [self = shared_from_this()](beast::error_code ec) {
                if (ec) {
                  self->close();
                  return;
                }
                // The ALPN seam: h1 is the only wired protocol until the
                // HTTP/2 session lands (RFC 0024, activation plan).
                self->read_next();
              }));
    } else {
      read_next();
    }
  }


  void read_next();
  void on_request();
  void dispatch_http(MatchedRoute matched, bool keep_alive);
  void accept_ws(MatchedRoute matched);
  void ws_read_next();
  void write_response(Int request_id, const Value &response, Int client_id,
                      const std::shared_ptr<WebServerRuntime> &runtime);
  void finish_response(beast::error_code ec, Int client_id, Int request_id,
                       const std::shared_ptr<WebServerRuntime> &runtime,
                       bool keep_alive);
  void queue_ws_frame(const Value &frame, Int client_id,
                      const std::shared_ptr<WebServerRuntime> &runtime);
  void ws_write_next();
  void ws_send_fragment();
  void ws_close(websocket::close_code code, beast::string_view reason);
  void send_simple_response(http::status status, std::string_view body,
                            bool keep_alive);
  void close();
  void close_socket_only();
  [[nodiscard]] Value build_server_request(const MatchedRoute &matched,
                                           Int request_id);
  [[nodiscard]] Value peer_value();

  std::shared_ptr<WebListener> listener_;
  // Captured at accept: the owning runtime may detach from the listener
  // while this connection's handlers are still draining, so socket-level
  // settings are never fetched through the (possibly empty) attach list.
  std::shared_ptr<const ServerRuntimeConfig> config_;
  asio::strand<asio::io_context::executor_type> strand_;
  std::optional<PlainStream> plain_stream_{};
  std::optional<TlsStream> tls_stream_{};
  std::optional<PlainWs> plain_ws_{};
  std::optional<TlsWs> tls_ws_{};
  beast::flat_buffer buffer_{};
  std::optional<http::request_parser<http::string_body>> parser_{};
  http::request<http::string_body> request_{};
  bool ws_{};
  bool writing_{};
  bool shutting_down_{};
  bool read_parked_{};
  bool request_keep_alive_{};
  bool pending_is_head_{};
  std::string decoded_path_{};
  Int pending_request_id_{-1};
  Int ws_connection_id_{-1};
  std::shared_ptr<WebServerRuntime> ws_runtime_{};
  Value ws_route_{};
  std::optional<http::response<http::string_body>> outgoing_{};
  std::optional<http::response<http::empty_body>> chunk_head_{};
  std::optional<http::response_serializer<http::empty_body>> chunk_serializer_{};
  std::string chunk_body_{};
  http::fields chunk_trailers_{};
  struct QueuedWsFrame {
    Value frame{};
    Int client_id{};
    std::shared_ptr<WebServerRuntime> runtime{};
    std::size_t bytes{};
  };
  std::deque<QueuedWsFrame> ws_outbound_{};
  std::size_t ws_outbound_bytes_{};
  std::string ws_send_buffer_{};
  std::size_t ws_send_offset_{};
  beast::flat_buffer ws_read_buffer_{};
};

// ---------------------------------------------------------------------------
// Listener implementation

void WebListener::ensure_listening() {
  if (listening_.exchange(true)) {
    return;
  }
  tcp::endpoint endpoint{asio::ip::make_address(address_), port_};
  acceptor_.open(endpoint.protocol());
  acceptor_.set_option(asio::socket_base::reuse_address(true));
  acceptor_.bind(endpoint);
  acceptor_.listen(asio::socket_base::max_listen_connections);
  bound_port_.store(acceptor_.local_endpoint().port(),
                    std::memory_order_release);
  accepting_.store(true, std::memory_order_release);
  accept_next();
}

void WebListener::start_io() {
  if (!threads_.empty()) {
    return;
  }
  work_.emplace(asio::make_work_guard(io_context_));
  threads_.reserve(io_threads_);
  for (std::size_t index = 0; index != io_threads_; ++index) {
    threads_.emplace_back([this] { io_context_.run(); });
  }
}

void WebListener::stop_accepting() {
  accepting_.store(false, std::memory_order_release);
  asio::post(io_context_, [self = shared_from_this()] {
    beast::error_code ec;
    static_cast<void>(self->acceptor_.cancel(ec));
    static_cast<void>(self->acceptor_.close(ec));
  });
}

void WebListener::stop_io() {
  work_.reset();
  io_context_.stop();
  for (auto &thread : threads_) {
    if (thread.joinable()) {
      thread.join();
    }
  }
  threads_.clear();
}

void WebListener::accept_next() {
  if (!accepting_.load(std::memory_order_acquire)) {
    return;
  }
  acceptor_.async_accept([self = shared_from_this()](beast::error_code ec,
                                                     tcp::socket socket) {
    if (!ec && self->accepting_.load(std::memory_order_acquire)) {
      std::shared_ptr<WebServerRuntime> owner;
      {
        std::lock_guard lock{self->mutex_};
        owner = self->runtimes_.empty() ? nullptr : self->runtimes_.front();
      }
      if (owner != nullptr &&
          !self->at_connection_limit(owner->config().max_connections)) {
        auto connection = std::make_shared<ServerConnection>(
            self, owner->config_ptr(), std::move(socket),
            owner->tls_context());
        connection->run();
      }
    }
    self->accept_next();
  });
}

std::optional<MatchedRoute> WebListener::match(HttpMethod method,
                                               std::string_view path,
                                               bool upgrade) {
  std::vector<std::shared_ptr<WebServerRuntime>> runtimes;
  {
    std::lock_guard lock{mutex_};
    runtimes = runtimes_;
  }
  for (const std::shared_ptr<WebServerRuntime> &runtime : runtimes) {
    const auto snapshot = upgrade ? runtime->ws_routes() : runtime->http_routes();
    const auto matched = snapshot->table.match(method, path);
    if (matched.matched) {
      return MatchedRoute{runtime, snapshot->routes[matched.entry_index].clone(),
                          matched.params};
    }
  }
  return std::nullopt;
}

void WebListener::set_reads_paused(bool paused) {
  reads_paused_.store(paused, std::memory_order_release);
  if (!paused) {
    std::vector<std::function<void()>> parked;
    {
      std::lock_guard lock{mutex_};
      parked.swap(parked_);
    }
    for (auto &resume : parked) {
      resume();
    }
  }
}

void WebListener::park_for_resume(std::function<void()> resume) {
  bool run_now = false;
  {
    std::lock_guard lock{mutex_};
    if (reads_paused_.load(std::memory_order_acquire)) {
      parked_.push_back(std::move(resume));
    } else {
      run_now = true;
    }
  }
  if (run_now) {
    resume();
  }
}

// ---------------------------------------------------------------------------
// Runtime implementation

void WebServerRuntime::start() {
  register_web_types();
  register_internal_types();
  if (simulation_) {
    // Live transport is real-time only (RFC 0024, lifecycle): no socket is
    // bound and every sink command is rejected with a typed report.
    bridge_.value->start();
    started_ = true;
    emit_event(WebSeverity::Warning, Str{"server"}, Str{"simulation"},
               Str{"the web server transport is real-time only"});
    return;
  }

  if (config_->tls.enabled) {
    tls_context_ = std::make_unique<asio::ssl::context>(
        asio::ssl::context::tls_server);
    auto &context = *tls_context_;
    context.set_options(asio::ssl::context::default_workarounds |
                        asio::ssl::context::no_sslv2 |
                        asio::ssl::context::no_sslv3 |
                        asio::ssl::context::no_tlsv1 |
                        asio::ssl::context::no_tlsv1_1);
    if (config_->tls.min_version == WebTlsVersion::Tls1_3) {
      SSL_CTX_set_min_proto_version(context.native_handle(), TLS1_3_VERSION);
    }
    if (!config_->tls.key_password.empty()) {
      context.set_password_callback(
          [password = config_->tls.key_password](std::size_t,
                                                asio::ssl::context::password_purpose) {
            return password;
          });
    }
    if (!config_->tls.cert_path.empty()) {
      context.use_certificate_chain_file(config_->tls.cert_path);
    } else {
      context.use_certificate_chain(
          asio::buffer(config_->tls.cert_pem.data(), config_->tls.cert_pem.size()));
    }
    if (!config_->tls.key_path.empty()) {
      context.use_private_key_file(config_->tls.key_path,
                                   asio::ssl::context::pem);
    } else {
      context.use_private_key(
          asio::buffer(config_->tls.key_pem.data(), config_->tls.key_pem.size()),
          asio::ssl::context::pem);
    }
    if (!config_->tls.ca_path.empty()) {
      context.load_verify_file(config_->tls.ca_path);
    } else if (!config_->tls.ca_pem.empty()) {
      context.add_certificate_authority(
          asio::buffer(config_->tls.ca_pem.data(), config_->tls.ca_pem.size()));
    }
    switch (config_->tls.client_verify) {
    case WebClientVerify::None:
      context.set_verify_mode(asio::ssl::verify_none);
      break;
    case WebClientVerify::Optional:
      context.set_verify_mode(asio::ssl::verify_peer);
      break;
    case WebClientVerify::Required:
      context.set_verify_mode(asio::ssl::verify_peer |
                              asio::ssl::verify_fail_if_no_peer_cert);
      break;
    }
    // The ALPN callback is the HTTP/2 seam: only h1 is selectable until the
    // nghttp2 session activates (config validation rejects "h2").
    SSL_CTX_set_alpn_select_cb(
        context.native_handle(),
        [](SSL *, const unsigned char **out, unsigned char *out_length,
           const unsigned char *in, unsigned int in_length,
           void *) noexcept -> int {
          unsigned char *selected = nullptr;
          if (SSL_select_next_proto(
                  &selected, out_length,
                  reinterpret_cast<const unsigned char *>("\x08http/1.1"), 9,
                  in, in_length) == OPENSSL_NPN_NEGOTIATED) {
            *out = selected;
            return SSL_TLSEXT_ERR_OK;
          }
          return SSL_TLSEXT_ERR_NOACK;
        },
        nullptr);
  }

  listener_ = ListenerRegistry::instance().acquire(
      std::string{config_->bind_address}, config_->port,
      config_->tls_identity, config_->io_threads, shared_from_this());
  try {
    if (!config_->bind_deferred) {
      listener_->ensure_listening();
    }
    listener_->start_io();
    bridge_.value->start();
  } catch (...) {
    if (ListenerRegistry::instance().release(listener_, this)) {
      listener_->stop_io();
    }
    listener_.reset();
    throw;
  }
  started_ = true;

  // Watermarks pause socket reads while the graph catches up (RFC 0024,
  // flow control).  The callback runs on whichever thread crossed the
  // watermark; it only flips an atomic and posts resumes.
  const auto high = OutputLimits{
      config_->ingress.records * static_cast<std::size_t>(config_->watermark_high_pct) / 100,
      config_->ingress.bytes * static_cast<std::size_t>(config_->watermark_high_pct) / 100,
  };
  const auto low = OutputLimits{
      config_->ingress.records * static_cast<std::size_t>(config_->watermark_low_pct) / 100,
      config_->ingress.bytes * static_cast<std::size_t>(config_->watermark_low_pct) / 100,
  };
  bridge_.value->set_watermark(
      index(ServerChannel::Request),
      WatermarkConfig{high, low, [listener = listener_](bool paused) {
                        listener->set_reads_paused(paused);
                      }});

  sweep_timer_ = std::make_unique<asio::steady_timer>(listener_->io_context());
  sweep_expired_requests();
  if (config_->stats_interval.count() > 0) {
    stats_timer_ =
        std::make_unique<asio::steady_timer>(listener_->io_context());
    start_stats_timer();
  }
  if (!config_->bind_deferred) {
    emit_event(WebSeverity::Info, Str{"server"}, Str{"listening"},
               Str{"listening on port " +
                   std::to_string(listener_->bound_port())});
  }
}

void WebServerRuntime::stop() noexcept {
  if (!started_) {
    return;
  }
  started_ = false;
  stopping_.store(true, std::memory_order_release);
  try {
    if (listener_) {
      // (1) stop intake, (2) 503 pending + drain, (3) WS Close(1001),
      // (4) cancel + join IO, (5) bridge last (RFC 0024, lifecycle).
      const bool last =
          ListenerRegistry::instance().release(listener_, this);

      std::vector<std::shared_ptr<ServerConnection>> connections;
      {
        std::lock_guard lock{pending_mutex_};
        for (auto &[request_id, pending] : pending_) {
          connections.push_back(std::move(pending.connection));
        }
        for (auto &[connection_id, weak] : ws_connections_) {
          if (auto connection = weak.lock()) {
            connections.push_back(std::move(connection));
          }
        }
        pending_.clear();
        ws_connections_.clear();
      }
      for (const auto &connection : connections) {
        connection->begin_shutdown();
      }
      const auto deadline =
          std::chrono::steady_clock::now() + config_->shutdown_drain_timeout;
      while (std::chrono::steady_clock::now() < deadline &&
             listener_->open_connections() != 0) {
        std::this_thread::sleep_for(std::chrono::milliseconds{10});
      }
      if (last) {
        listener_->stop_io();
      }
      // The timers hold service pointers into the listener's io_context;
      // they must die while it is still alive (threads are joined, so no
      // handler can race the destruction).
      sweep_timer_.reset();
      stats_timer_.reset();
      listener_.reset();
    }
  } catch (...) {
  }
  bridge_.value->stop();
}

void WebServerRuntime::rebuild(
    std::shared_ptr<const CompiledRoutes> &slot,
    std::vector<std::tuple<HttpMethod, std::string, Value>> &master,
    std::vector<Value> added, std::vector<Value> removed) {
  std::lock_guard lock{routes_mutex_};
  for (const Value &route : removed) {
    std::erase_if(master, [&](const auto &entry) {
      return std::get<2>(entry).view().equals(route.view());
    });
  }
  for (Value &route : added) {
    const auto fields = route.view().as_bundle();
    master.emplace_back(fields.at("method").checked_as<HttpMethod>(),
                        std::string{fields.at("pattern").checked_as<Str>()},
                        std::move(route));
  }
  auto compiled = std::make_shared<CompiledRoutes>();
  std::vector<RouteTable::Entry> entries;
  entries.reserve(master.size());
  for (const auto &[method, pattern, route] : master) {
    entries.push_back(RouteTable::Entry{method, pattern});
    compiled->routes.push_back(route.clone());
  }
  compiled->table = RouteTable::build(std::move(entries));
  atomic_store_routes(&slot, compiled);
}

void WebServerRuntime::apply_http_routes(std::vector<Value> added,
                                         std::vector<Value> removed) {
  if (simulation_) {
    return;
  }
  std::vector<Value> announce;
  for (const Value &route : added) {
    announce.push_back(route.clone());
  }
  rebuild(http_routes_, http_master_, std::move(added), std::move(removed));
  for (Value &route : announce) {
    static_cast<void>(bridge_.value->push(
        index(ServerChannel::Request),
        bundle_value<WebRequestEnvelope>({
            {"route", std::move(route)},
            {"state", atomic_value(WebRouteState::Serving)},
        }),
        1));
  }
  if (config_->bind_deferred && listener_) {
    listener_->ensure_listening();
    emit_event(WebSeverity::Info, Str{"server"}, Str{"listening"},
               Str{"listening on port " +
                   std::to_string(listener_->bound_port())});
  }
}

void WebServerRuntime::apply_ws_routes(std::vector<Value> added,
                                       std::vector<Value> removed) {
  if (simulation_) {
    return;
  }
  rebuild(ws_routes_, ws_master_, std::move(added), std::move(removed));
  if (config_->bind_deferred && listener_) {
    listener_->ensure_listening();
  }
}

Int WebServerRuntime::register_pending(
    const std::shared_ptr<ServerConnection> &connection) {
  const Int request_id = ++next_request_id_;
  std::lock_guard lock{pending_mutex_};
  pending_[request_id] =
      Pending{connection, std::chrono::steady_clock::now() +
                              config_->request_timeout};
  return request_id;
}

void WebServerRuntime::unregister_pending(Int request_id) noexcept {
  std::lock_guard lock{pending_mutex_};
  pending_.erase(request_id);
}

Int WebServerRuntime::register_ws_connection(
    const std::shared_ptr<ServerConnection> &connection) {
  const Int connection_id = ++next_request_id_;
  std::lock_guard lock{pending_mutex_};
  ws_connections_[connection_id] = connection;
  return connection_id;
}

void WebServerRuntime::unregister_ws_connection(Int connection_id) noexcept {
  std::lock_guard lock{pending_mutex_};
  ws_connections_.erase(connection_id);
}

bool WebServerRuntime::push_request(Value route, Value request) {
  return bridge_.value->push(index(ServerChannel::Request),
                             bundle_value<WebRequestEnvelope>({
                                 {"route", std::move(route)},
                                 {"request", std::move(request)},
                             }),
                             1);
}

void WebServerRuntime::push_ws_event(Value route, Value event) {
  static_cast<void>(bridge_.value->push(
      index(ServerChannel::WsIngress),
      bundle_value<WsIngressEnvelope>({
          {"route", std::move(route)},
          {"event", std::move(event)},
      }),
      1));
}

void WebServerRuntime::push_ws_frame(Value route, Value inbound_frame) {
  if (!bridge_.value->push(index(ServerChannel::WsIngress),
                           bundle_value<WsIngressEnvelope>({
                               {"route", std::move(route)},
                               {"frame", std::move(inbound_frame)},
                           }),
                           1)) {
    count_drop();
  }
}

void WebServerRuntime::report(std::size_t channel, Int client_id,
                              Value report_value) {
  const Value envelope = bundle_value<WebDeliveryEnvelope>({
      {"request_id", atomic_value(client_id)},
      {"report", std::move(report_value)},
  });
  if (!bridge_.value->push(channel, envelope.clone(), 1)) {
    static_cast<void>(bridge_.value->push_control(channel, envelope.clone(), 512));
  }
}

void WebServerRuntime::emit_event(WebSeverity severity, Str component,
                                  Str category, Str message, Int error_code,
                                  bool retriable, bool fatal,
                                  Int connection_id) {
  const bool stop_graph =
      fatal && config_->failure_policy == WebFailurePolicy::StopGraph;
  Value event = make_event(severity, std::move(component), std::move(category),
                           path_, std::move(message), error_code,
                           Bool{retriable}, Bool{fatal}, connection_id);
  const Value envelope = bundle_value<WebEventEnvelope>({
      {"event", std::move(event)},
      {"stop_graph", atomic_value(Bool{stop_graph})},
  });
  if (!bridge_.value->push(index(ServerChannel::Event), envelope.clone(), 1)) {
    static_cast<void>(bridge_.value->push_control(index(ServerChannel::Event),
                                                  envelope.clone(), 512));
  }
}

void WebServerRuntime::sweep_expired_requests() {
  if (stopping_.load(std::memory_order_acquire)) {
    return;
  }
  std::vector<std::pair<Int, std::shared_ptr<ServerConnection>>> expired;
  {
    std::lock_guard lock{pending_mutex_};
    const auto now = std::chrono::steady_clock::now();
    for (auto item = pending_.begin(); item != pending_.end();) {
      if (item->second.deadline <= now) {
        expired.emplace_back(item->first, std::move(item->second.connection));
        item = pending_.erase(item);
      } else {
        ++item;
      }
    }
  }
  for (auto &[request_id, connection] : expired) {
    connection->answer_timeout(request_id);
  }
  sweep_timer_->expires_after(std::chrono::milliseconds{250});
  sweep_timer_->async_wait([this](beast::error_code ec) {
    if (!ec) {
      sweep_expired_requests();
    }
  });
}

void WebServerRuntime::start_stats_timer() {
  stats_timer_->expires_after(config_->stats_interval);
  stats_timer_->async_wait([this](beast::error_code ec) {
    if (ec || stopping_.load(std::memory_order_acquire)) {
      return;
    }
    std::size_t pending_count = 0;
    std::size_t ws_count = 0;
    {
      std::lock_guard lock{pending_mutex_};
      pending_count = pending_.size();
      ws_count = ws_connections_.size();
    }
    bridge_.value->push_latest(
        index(ServerChannel::Stats),
        bundle_value<WebServerStats>({
            {"listening_port",
             atomic_value(Int{listener_ ? listener_->bound_port() : 0})},
            {"connection_count",
             atomic_value(Int(listener_ ? listener_->open_connections() : 0))},
            {"ws_connection_count", atomic_value(Int(ws_count))},
            {"pending_request_count", atomic_value(Int(pending_count))},
            {"ingress_record_count",
             atomic_value(Int(bridge_.value->payload_pending(
                 index(ServerChannel::Request))))},
            {"ingress_byte_count",
             atomic_value(Int(bridge_.value->payload_retained_bytes(
                 index(ServerChannel::Request))))},
            {"outbound_byte_count", atomic_value(Int{0})},
            {"dropped_count", atomic_value(dropped_.load())},
        }),
        1);
    start_stats_timer();
  });
}

void WebServerRuntime::respond(Int client_id, Int request_id, Value response) {
  if (simulation_ || stopping_.load(std::memory_order_acquire)) {
    report(index(ServerChannel::RespondDelivery), client_id,
           make_delivery_report(request_id, ++sequence_,
                                WebDeliveryStatus::EnqueueRejected, 0, false,
                                false, Str{"web server is not serving"}));
    return;
  }
  std::shared_ptr<ServerConnection> connection;
  {
    std::lock_guard lock{pending_mutex_};
    const auto found = pending_.find(request_id);
    if (found != pending_.end()) {
      connection = std::move(found->second.connection);
      pending_.erase(found);
    }
  }
  if (!connection) {
    // Responding to an unknown or already-answered id is a reported error,
    // never silence (RFC 0024).
    report(index(ServerChannel::RespondDelivery), client_id,
           make_delivery_report(request_id, ++sequence_,
                                WebDeliveryStatus::PermanentFailure, 0, false,
                                false,
                                Str{"unknown or already-answered request id"}));
    return;
  }
  connection->deliver_response(request_id, std::move(response), client_id,
                               shared_from_this());
}

void WebServerRuntime::ws_send(Int client_id, Int connection_id, Value frame) {
  if (simulation_ || stopping_.load(std::memory_order_acquire)) {
    report(index(ServerChannel::WsSendDelivery), client_id,
           make_delivery_report(connection_id, ++sequence_,
                                WebDeliveryStatus::EnqueueRejected, 0, false,
                                false, Str{"web server is not serving"}));
    return;
  }
  std::shared_ptr<ServerConnection> connection;
  {
    std::lock_guard lock{pending_mutex_};
    const auto found = ws_connections_.find(connection_id);
    if (found != ws_connections_.end()) {
      connection = found->second.lock();
    }
  }
  if (!connection) {
    report(index(ServerChannel::WsSendDelivery), client_id,
           make_delivery_report(connection_id, ++sequence_,
                                WebDeliveryStatus::PermanentFailure, 0, false,
                                false, Str{"WebSocket is not connected"}));
    return;
  }
  connection->deliver_ws_frame(std::move(frame), client_id,
                               shared_from_this());
}

// ---------------------------------------------------------------------------
// Connection implementation

void ServerConnection::read_next() {
  if (shutting_down_) {
    close();
    return;
  }
  if (listener_->reads_paused()) {
    read_parked_ = true;
    listener_->park_for_resume(
        [self = shared_from_this()] { self->resume_reading(); });
    return;
  }
  const auto &config = *config_;
  parser_.emplace();
  parser_->header_limit(static_cast<std::uint32_t>(config.max_header_bytes));
  parser_->body_limit(config.max_body_bytes);
  auto &stream_timeout = tls_stream_.has_value()
                             ? beast::get_lowest_layer(*tls_stream_)
                             : *plain_stream_;
  stream_timeout.expires_after(config.idle_timeout);
  const auto on_read = asio::bind_executor(
      strand_, [self = shared_from_this()](beast::error_code ec, std::size_t) {
        if (ec) {
          self->close();
          return;
        }
        self->request_ = self->parser_->release();
        self->on_request();
      });
  if (tls_stream_.has_value()) {
    http::async_read(*tls_stream_, buffer_, *parser_, on_read);
  } else {
    http::async_read(*plain_stream_, buffer_, *parser_, on_read);
  }
}

void ServerConnection::on_request() {
  const auto method = method_from(request_.method());
  if (!method.has_value()) {
    send_simple_response(http::status::not_implemented,
                         "unsupported method", false);
    return;
  }
  const std::string_view target{request_.target().data(),
                                request_.target().size()};
  const auto query_start = target.find('?');
  const std::string_view path = target.substr(0, query_start);
  // Malformed percent-encoding is a client error, not an unmatched route
  // (RFC 0024, routing); decode once here so the graph-visible path and the
  // matcher agree on the same bytes.
  auto decoded_path = RouteTable::decode_path(path);
  if (!decoded_path.has_value()) {
    send_simple_response(http::status::bad_request,
                         "malformed percent-encoding", false);
    return;
  }
  decoded_path_ = std::move(*decoded_path);

  if (websocket::is_upgrade(request_)) {
    auto matched = listener_->match(*method, path, true);
    if (!matched.has_value()) {
      send_simple_response(http::status::not_found, "no such route", false);
      return;
    }
    accept_ws(std::move(*matched));
    return;
  }

  auto matched = listener_->match(*method, path, false);
  if (!matched.has_value() && *method == HttpMethod::Head) {
    // Standard HTTP: HEAD is GET without the body, so a GET route serves it
    // (the transport suppresses the body on the way out).
    matched = listener_->match(HttpMethod::Get, path, false);
  }
  if (!matched.has_value()) {
    send_simple_response(http::status::not_found, "no such route",
                         request_.keep_alive());
    return;
  }
  dispatch_http(std::move(*matched), request_.keep_alive());
}

Value ServerConnection::peer_value() {
  beast::error_code ec;
  auto &socket = tls_stream_.has_value()
                     ? beast::get_lowest_layer(*tls_stream_).socket()
                     : plain_stream_->socket();
  const auto remote = socket.remote_endpoint(ec);
  const auto local = socket.local_endpoint(ec);
  Str negotiated{};
  Str sni{};
  Str subject{};
  const bool tls = tls_stream_.has_value();
  if (tls) {
    SSL *ssl = tls_stream_->native_handle();
    const unsigned char *alpn = nullptr;
    unsigned int alpn_length = 0;
    SSL_get0_alpn_selected(ssl, &alpn, &alpn_length);
    if (alpn != nullptr && alpn_length != 0) {
      negotiated = Str{reinterpret_cast<const char *>(alpn), alpn_length};
    }
    if (const char *name =
            SSL_get_servername(ssl, TLSEXT_NAMETYPE_host_name)) {
      sni = Str{name};
    }
    if (X509 *cert = SSL_get_peer_certificate(ssl)) {
      char buffer[512];
      X509_NAME_oneline(X509_get_subject_name(cert), buffer, sizeof(buffer));
      subject = Str{buffer};
      X509_free(cert);
    }
  }
  return bundle_value<WebPeer>({
      {"remote_address",
       atomic_value(Str{ec ? std::string{} : remote.address().to_string()})},
      {"remote_port", atomic_value(Int{ec ? 0 : remote.port()})},
      {"local_port", atomic_value(Int{ec ? 0 : local.port()})},
      {"tls", atomic_value(Bool{tls})},
      {"negotiated_protocol", atomic_value(std::move(negotiated))},
      {"sni", atomic_value(std::move(sni))},
      {"client_cert_subject", atomic_value(std::move(subject))},
  });
}

Value ServerConnection::build_server_request(const MatchedRoute &matched,
                                             Int request_id) {
  const auto method = *method_from(request_.method());
  const std::string_view target{request_.target().data(),
                                request_.target().size()};
  const auto query_start = target.find('?');
  const std::string_view query =
      query_start == std::string_view::npos ? std::string_view{}
                                            : target.substr(query_start + 1);

  NamedPairs headers;
  for (const auto &field : request_) {
    headers.emplace_back(std::string{field.name_string()},
                         std::string{field.value()});
  }

  Value request = bundle_value<HttpRequest>({
      {"method", atomic_value(method)},
      {"target", atomic_value(Str{std::string{target}})},
      {"path", atomic_value(Str{decoded_path_})},
      {"query", name_value_tuple<WebParam>(parse_query(query))},
      {"path_params", name_value_tuple<WebParam>(matched.params)},
      {"headers", name_value_tuple<WebHeader>(headers)},
      {"body", atomic_value(Bytes{request_.body()})},
      {"trailers", name_value_tuple<WebHeader>({})},
  });
  return bundle_value<HttpServerRequest>({
      {"request_id", atomic_value(request_id)},
      {"connection_id", atomic_value(Int{0})},
      {"stream_id", atomic_value(Int{0})},
      {"request", std::move(request)},
      {"peer", peer_value()},
  });
}

void ServerConnection::dispatch_http(MatchedRoute matched, bool keep_alive) {
  const std::shared_ptr<WebServerRuntime> runtime = matched.runtime;
  const Int request_id = runtime->register_pending(shared_from_this());
  Value server_request = build_server_request(matched, request_id);
  if (!runtime->push_request(matched.route.clone(),
                             std::move(server_request))) {
    runtime->unregister_pending(request_id);
    if (runtime->config().inbound_overflow == WebInboundOverflow::Reject) {
      // Over the hard limit the transport answers itself; the graph never
      // sees the request (RFC 0024, flow control).
      send_simple_response(http::status::service_unavailable,
                           "server is at capacity", keep_alive);
    } else {
      // Backpressure: leave the request unanswered until capacity returns.
      auto self = shared_from_this();
      auto retry = std::make_shared<asio::steady_timer>(
          listener_->io_context());
      retry->expires_after(std::chrono::milliseconds{50});
      retry->async_wait(asio::bind_executor(
          strand_, [self, matched = std::move(matched), keep_alive,
                    retry](beast::error_code ec) mutable {
            if (!ec && !self->shutting_down_) {
              self->dispatch_http(std::move(matched), keep_alive);
            }
          }));
    }
    return;
  }
  pending_request_id_ = request_id;
  request_keep_alive_ = keep_alive;
  pending_is_head_ = request_.method() == http::verb::head;
}

void ServerConnection::write_response(
    Int request_id, const Value &response, Int client_id,
    const std::shared_ptr<WebServerRuntime> &runtime) {
  if (pending_request_id_ != request_id || writing_) {
    runtime->report(index(ServerChannel::RespondDelivery), client_id,
                    make_delivery_report(
                        request_id, ++runtime->sequence_,
                        WebDeliveryStatus::PermanentFailure, 0, false, false,
                        Str{"the request was already answered"}));
    return;
  }
  pending_request_id_ = -1;
  const auto fields = response.view().as_bundle();
  const bool keep_alive = request_keep_alive_ && !shutting_down_;
  const auto status =
      static_cast<unsigned>(fields.at("status").checked_as<Int>());
  std::string body;
  const auto body_field = fields.at("body");
  if (body_field.data() != nullptr) {
    body = std::string{body_field.checked_as<Bytes>().data};
  }
  chunk_trailers_.clear();
  const auto trailers = fields.at("trailers");
  const bool suppress_body = pending_is_head_;
  if (!suppress_body && trailers.data() != nullptr) {
    for (const auto trailer : trailers.as_list()) {
      const auto pair = trailer.as_bundle();
      chunk_trailers_.insert(std::string{pair.at("name").checked_as<Str>()},
                             std::string{pair.at("value").checked_as<Str>()});
    }
  }
  writing_ = true;

  if (chunk_trailers_.begin() != chunk_trailers_.end()) {
    // Trailer-bearing responses use chunked transfer (the only way h1.1
    // carries trailers); the header, one body chunk, and the trailer part
    // write in sequence on the strand (RFC 0024, value contract).
    chunk_head_.emplace();
    chunk_head_->version(11);
    chunk_head_->result(status);
    const auto headers = fields.at("headers");
    if (headers.data() != nullptr) {
      for (const auto header : headers.as_list()) {
        const auto pair = header.as_bundle();
        chunk_head_->insert(std::string{pair.at("name").checked_as<Str>()},
                            std::string{pair.at("value").checked_as<Str>()});
      }
    }
    std::string trailer_names;
    for (const auto &trailer : chunk_trailers_) {
      if (!trailer_names.empty()) {
        trailer_names += ", ";
      }
      trailer_names += std::string{trailer.name_string()};
    }
    chunk_head_->set(http::field::trailer, trailer_names);
    chunk_head_->keep_alive(keep_alive);
    chunk_head_->chunked(true);
    chunk_body_ = std::move(body);
    chunk_serializer_.emplace(*chunk_head_);
    const auto on_header = asio::bind_executor(
        strand_, [self = shared_from_this(), client_id, request_id, runtime,
                  keep_alive](beast::error_code ec, std::size_t) {
          if (ec) {
            self->finish_response(ec, client_id, request_id, runtime,
                                  keep_alive);
            return;
          }
          const auto on_body = asio::bind_executor(
              self->strand_,
              [self, client_id, request_id, runtime,
               keep_alive](beast::error_code body_ec, std::size_t) {
                if (body_ec) {
                  self->finish_response(body_ec, client_id, request_id,
                                        runtime, keep_alive);
                  return;
                }
                const auto on_last = asio::bind_executor(
                    self->strand_,
                    [self, client_id, request_id, runtime,
                     keep_alive](beast::error_code last_ec, std::size_t) {
                      self->finish_response(last_ec, client_id, request_id,
                                            runtime, keep_alive);
                    });
                if (self->tls_stream_.has_value()) {
                  asio::async_write(*self->tls_stream_,
                                    http::make_chunk_last(self->chunk_trailers_),
                                    on_last);
                } else {
                  asio::async_write(*self->plain_stream_,
                                    http::make_chunk_last(self->chunk_trailers_),
                                    on_last);
                }
              });
          if (self->chunk_body_.empty()) {
            asio::post(self->strand_, [on_body] {
              on_body.get()(beast::error_code{}, 0);
            });
            return;
          }
          if (self->tls_stream_.has_value()) {
            asio::async_write(*self->tls_stream_,
                              http::make_chunk(asio::buffer(self->chunk_body_)),
                              on_body);
          } else {
            asio::async_write(*self->plain_stream_,
                              http::make_chunk(asio::buffer(self->chunk_body_)),
                              on_body);
          }
        });
    if (tls_stream_.has_value()) {
      http::async_write_header(*tls_stream_, *chunk_serializer_, on_header);
    } else {
      http::async_write_header(*plain_stream_, *chunk_serializer_, on_header);
    }
    return;
  }

  outgoing_.emplace();
  outgoing_->version(11);
  outgoing_->result(status);
  const auto headers = fields.at("headers");
  if (headers.data() != nullptr) {
    for (const auto header : headers.as_list()) {
      const auto pair = header.as_bundle();
      outgoing_->insert(std::string{pair.at("name").checked_as<Str>()},
                        std::string{pair.at("value").checked_as<Str>()});
    }
  }
  const std::size_t body_size = body.size();
  if (!suppress_body) {
    outgoing_->body() = std::move(body);
  }
  outgoing_->keep_alive(keep_alive);
  outgoing_->prepare_payload();
  if (suppress_body) {
    // A HEAD response advertises the entity's Content-Length but carries no
    // body (RFC 0024 / RFC 9110).
    outgoing_->content_length(body_size);
  }
  const auto on_write = asio::bind_executor(
      strand_, [self = shared_from_this(), client_id, request_id, runtime,
                keep_alive](beast::error_code ec, std::size_t) {
        self->finish_response(ec, client_id, request_id, runtime, keep_alive);
      });
  if (tls_stream_.has_value()) {
    http::async_write(*tls_stream_, *outgoing_, on_write);
  } else {
    http::async_write(*plain_stream_, *outgoing_, on_write);
  }
}

void ServerConnection::finish_response(
    beast::error_code ec, Int client_id, Int request_id,
    const std::shared_ptr<WebServerRuntime> &runtime, bool keep_alive) {
  writing_ = false;
  outgoing_.reset();
  chunk_serializer_.reset();
  chunk_head_.reset();
  chunk_body_.clear();
  chunk_trailers_.clear();
  runtime->report(index(ServerChannel::RespondDelivery), client_id,
                  make_delivery_report(request_id, ++runtime->sequence_,
                                       ec ? WebDeliveryStatus::PermanentFailure
                                          : WebDeliveryStatus::Delivered,
                                       ec ? ec.value() : 0, false, false,
                                       ec ? Str{ec.message()} : Str{}));
  if (ec || !keep_alive) {
    close();
  } else {
    read_next();
  }
}

void ServerConnection::send_simple_response(http::status status,
                                            std::string_view body,
                                            bool keep_alive) {
  if (writing_) {
    return;
  }
  outgoing_.emplace();
  outgoing_->version(11);
  outgoing_->result(status);
  outgoing_->set(http::field::content_type, "text/plain");
  outgoing_->body() = std::string{body};
  outgoing_->keep_alive(keep_alive && !shutting_down_);
  outgoing_->prepare_payload();
  writing_ = true;
  const bool continue_reading = keep_alive && !shutting_down_;
  const auto on_write = asio::bind_executor(
      strand_,
      [self = shared_from_this(), continue_reading](beast::error_code ec,
                                                    std::size_t) {
        self->writing_ = false;
        self->outgoing_.reset();
        if (ec || !continue_reading) {
          self->close();
        } else {
          self->read_next();
        }
      });
  if (tls_stream_.has_value()) {
    http::async_write(*tls_stream_, *outgoing_, on_write);
  } else {
    http::async_write(*plain_stream_, *outgoing_, on_write);
  }
}

void ServerConnection::accept_ws(MatchedRoute matched) {
  const std::shared_ptr<WebServerRuntime> runtime = matched.runtime;
  ws_runtime_ = runtime;
  ws_route_ = matched.route.clone();
  ws_connection_id_ = runtime->register_ws_connection(shared_from_this());

  const auto &config = runtime->config();
  websocket::stream_base::timeout timeout{};
  timeout.handshake_timeout = config.request_timeout.count() > 0
                                  ? websocket::stream_base::duration{
                                        config.request_timeout}
                                  : websocket::stream_base::none();
  // Beast's built-in keep-alive pings implement the configured ping/pong
  // policy; a zero interval disables idle enforcement.
  timeout.idle_timeout =
      config.ping_interval.count() > 0
          ? websocket::stream_base::duration{config.ping_interval +
                                             config.pong_timeout}
          : websocket::stream_base::none();
  timeout.keep_alive_pings = config.ping_interval.count() > 0;

  const Value server_request =
      build_server_request(matched, ws_connection_id_);

  const auto on_accept = asio::bind_executor(
      strand_, [self = shared_from_this(), runtime,
                server_request = server_request.clone()](beast::error_code ec) {
        if (ec) {
          runtime->unregister_ws_connection(self->ws_connection_id_);
          self->close();
          return;
        }
        self->ws_ = true;
        runtime->push_ws_event(
            self->ws_route_.clone(),
            bundle_value<WsEvent>({
                {"connection_id", atomic_value(self->ws_connection_id_)},
                {"state", atomic_value(WsConnectionState::Open)},
                {"request", server_request.clone()},
            }));
        self->ws_read_next();
      });
  if (tls_stream_.has_value()) {
    tls_ws_.emplace(std::move(*tls_stream_));
    tls_stream_.reset();
    tls_ws_->set_option(timeout);
    tls_ws_->read_message_max(config.ws_max_message_bytes);
    tls_ws_->async_accept(request_, on_accept);
  } else {
    plain_ws_.emplace(std::move(*plain_stream_));
    plain_stream_.reset();
    plain_ws_->set_option(timeout);
    plain_ws_->read_message_max(config.ws_max_message_bytes);
    plain_ws_->async_accept(request_, on_accept);
  }
}

void ServerConnection::ws_read_next() {
  const auto on_read = asio::bind_executor(
      strand_, [self = shared_from_this()](beast::error_code ec, std::size_t) {
        const std::shared_ptr<WebServerRuntime> runtime = self->ws_runtime_;
        if (ec) {
          runtime->unregister_ws_connection(self->ws_connection_id_);
          const bool orderly = ec == websocket::error::closed;
          Int close_code = 1005;
          Str close_reason{};
          if (orderly) {
            const auto &reason = self->plain_ws_.has_value()
                                     ? self->plain_ws_->reason()
                                     : self->tls_ws_->reason();
            close_code = static_cast<Int>(reason.code);
            close_reason = Str{std::string{reason.reason.data(),
                                           reason.reason.size()}};
          }
          runtime->push_ws_event(
              self->ws_route_.clone(),
              bundle_value<WsEvent>({
                  {"connection_id", atomic_value(self->ws_connection_id_)},
                  {"state", atomic_value(orderly ? WsConnectionState::Closed
                                                 : WsConnectionState::Failed)},
                  {"close_code", atomic_value(close_code)},
                  {"close_reason", atomic_value(std::move(close_reason))},
              }));
          self->close();
          return;
        }
        const bool text = self->plain_ws_.has_value()
                              ? self->plain_ws_->got_text()
                              : self->tls_ws_->got_text();
        const auto data = self->ws_read_buffer_.data();
        std::string payload{static_cast<const char *>(data.data()),
                            data.size()};
        self->ws_read_buffer_.consume(self->ws_read_buffer_.size());
        Value frame =
            text ? bundle_value<WsFrame>({
                       {"kind", atomic_value(WsFrameKind::Text)},
                       {"text", atomic_value(Str{std::move(payload)})},
                   })
                 : bundle_value<WsFrame>({
                       {"kind", atomic_value(WsFrameKind::Binary)},
                       {"data", atomic_value(Bytes{std::move(payload)})},
                   });
        runtime->push_ws_frame(
            self->ws_route_.clone(),
            bundle_value<WsInboundFrame>({
                {"connection_id", atomic_value(self->ws_connection_id_)},
                {"frame", std::move(frame)},
            }));
        self->ws_read_next();
      });
  if (plain_ws_.has_value()) {
    plain_ws_->async_read(ws_read_buffer_, on_read);
  } else {
    tls_ws_->async_read(ws_read_buffer_, on_read);
  }
}

void ServerConnection::queue_ws_frame(
    const Value &frame, Int client_id,
    const std::shared_ptr<WebServerRuntime> &runtime) {
  if (!ws_) {
    runtime->report(index(ServerChannel::WsSendDelivery), client_id,
                    make_delivery_report(
                        ws_connection_id_, ++runtime->sequence_,
                        WebDeliveryStatus::PermanentFailure, 0, false, false,
                        Str{"WebSocket is not connected"}));
    return;
  }
  const auto fields = frame.view().as_bundle();
  const auto kind = fields.at("kind").checked_as<WsFrameKind>();
  if (kind == WsFrameKind::Close) {
    const auto code = fields.at("close_code");
    const auto reason = fields.at("close_reason");
    ws_close(websocket::close_code{static_cast<std::uint16_t>(
                 code.data() != nullptr ? code.checked_as<Int>() : 1000)},
             reason.data() != nullptr
                 ? std::string{reason.checked_as<Str>()}
                 : std::string{});
    runtime->report(index(ServerChannel::WsSendDelivery), client_id,
                    make_delivery_report(ws_connection_id_,
                                         ++runtime->sequence_,
                                         WebDeliveryStatus::Delivered));
    return;
  }
  if (kind == WsFrameKind::Ping || kind == WsFrameKind::Pong) {
    // Beast's keep-alive pings own the control-frame policy; graph-initiated
    // pings are not part of the v1 surface (RFC 0024).
    runtime->report(index(ServerChannel::WsSendDelivery), client_id,
                    make_delivery_report(
                        ws_connection_id_, ++runtime->sequence_,
                        WebDeliveryStatus::PermanentFailure, 0, false, false,
                        Str{"control frames are transport-managed"}));
    return;
  }

  const auto &config = runtime->config();
  const auto text_field = fields.at("text");
  const auto data_field = fields.at("data");
  const std::size_t bytes =
      kind == WsFrameKind::Text
          ? (text_field.data() != nullptr
                 ? std::string_view{text_field.checked_as<Str>()}.size()
                 : 0)
          : (data_field.data() != nullptr
                 ? data_field.checked_as<Bytes>().data.size()
                 : 0);
  if (ws_outbound_.size() >= config.outbound_message_limit ||
      ws_outbound_bytes_ + bytes > config.outbound_byte_limit) {
    // The explicit slow-consumer policy (RFC 0024, flow control): Close
    // (1013) or DropNewest; never an unbounded queue.
    if (config.slow_consumer_policy == WebSlowConsumerPolicy::Close) {
      for (const auto &queued : ws_outbound_) {
        queued.runtime->report(
            index(ServerChannel::WsSendDelivery), queued.client_id,
            make_delivery_report(ws_connection_id_, ++runtime->sequence_,
                                 WebDeliveryStatus::Dropped, 0, false, false,
                                 Str{"slow consumer"}));
      }
      ws_outbound_.clear();
      ws_outbound_bytes_ = 0;
      ws_close(websocket::close_code{1013}, "slow consumer");
    }
    runtime->report(index(ServerChannel::WsSendDelivery), client_id,
                    make_delivery_report(ws_connection_id_,
                                         ++runtime->sequence_,
                                         WebDeliveryStatus::Dropped, 0, false,
                                         false, Str{"slow consumer"}));
    runtime->count_drop();
    return;
  }
  ws_outbound_.push_back(QueuedWsFrame{frame.clone(), client_id, runtime, bytes});
  ws_outbound_bytes_ += bytes;
  if (!writing_) {
    ws_write_next();
  }
}

void ServerConnection::ws_write_next() {
  if (ws_outbound_.empty()) {
    writing_ = false;
    return;
  }
  writing_ = true;
  const QueuedWsFrame &next = ws_outbound_.front();
  const auto fields = next.frame.view().as_bundle();
  const auto kind = fields.at("kind").checked_as<WsFrameKind>();
  const bool text = kind == WsFrameKind::Text;
  const auto text_field = fields.at("text");
  const auto data_field = fields.at("data");
  const std::string payload =
      text ? (text_field.data() != nullptr
                  ? std::string{text_field.checked_as<Str>()}
                  : std::string{})
           : (data_field.data() != nullptr
                  ? std::string{data_field.checked_as<Bytes>().data}
                  : std::string{});
  ws_send_buffer_ = std::move(payload);
  ws_send_offset_ = 0;
  if (plain_ws_.has_value()) {
    plain_ws_->text(text);
  } else {
    tls_ws_->text(text);
  }
  ws_send_fragment();
}

void ServerConnection::ws_send_fragment() {
  // Outbound messages fragment at the advertised frame cap (RFC 0024,
  // configuration): Beast tracks continuation state across write_some calls.
  const std::size_t cap = std::max<std::size_t>(config_->ws_max_frame_bytes, 1);
  const std::size_t remaining = ws_send_buffer_.size() - ws_send_offset_;
  const std::size_t length = std::min(cap, remaining);
  const bool fin = ws_send_offset_ + length == ws_send_buffer_.size();
  const auto on_write = asio::bind_executor(
      strand_, [self = shared_from_this(), fin,
                length](beast::error_code ec, std::size_t) {
        if (!ec && !fin) {
          self->ws_send_offset_ += length;
          self->ws_send_fragment();
          return;
        }
        QueuedWsFrame sent = std::move(self->ws_outbound_.front());
        self->ws_outbound_.pop_front();
        self->ws_outbound_bytes_ -= sent.bytes;
        sent.runtime->report(
            index(ServerChannel::WsSendDelivery), sent.client_id,
            make_delivery_report(self->ws_connection_id_,
                                 ++sent.runtime->sequence_,
                                 ec ? WebDeliveryStatus::PermanentFailure
                                    : WebDeliveryStatus::Delivered,
                                 ec ? ec.value() : 0, false, false,
                                 ec ? Str{ec.message()} : Str{}));
        if (ec) {
          self->writing_ = false;
          self->close();
          return;
        }
        self->ws_write_next();
      });
  const auto fragment = asio::buffer(ws_send_buffer_.data() + ws_send_offset_,
                                     length);
  if (plain_ws_.has_value()) {
    plain_ws_->async_write_some(fin, fragment, on_write);
  } else {
    tls_ws_->async_write_some(fin, fragment, on_write);
  }
}

void ServerConnection::ws_close(websocket::close_code code,
                                beast::string_view reason) {
  const auto on_close =
      asio::bind_executor(strand_, [self = shared_from_this()](
                                       beast::error_code) { self->close(); });
  websocket::close_reason close{code};
  close.reason = reason;
  if (plain_ws_.has_value()) {
    plain_ws_->async_close(close, on_close);
  } else if (tls_ws_.has_value()) {
    tls_ws_->async_close(close, on_close);
  } else {
    close_socket_only();
  }
}

void ServerConnection::close() { close_socket_only(); }

void ServerConnection::close_socket_only() {
  beast::error_code ec;
  if (plain_ws_.has_value()) {
    static_cast<void>(
        beast::get_lowest_layer(*plain_ws_).socket().close(ec));
  } else if (tls_ws_.has_value()) {
    static_cast<void>(
        beast::get_lowest_layer(*tls_ws_).socket().close(ec));
  } else if (tls_stream_.has_value()) {
    static_cast<void>(beast::get_lowest_layer(*tls_stream_).socket().close(ec));
  } else if (plain_stream_.has_value()) {
    static_cast<void>(plain_stream_->socket().close(ec));
  }
}
} // namespace

// ---------------------------------------------------------------------------
// Runtime node + service implementation

namespace {
struct WebServerRuntimeHandle {
  std::shared_ptr<WebServerRuntime> value{};

  friend bool operator==(const WebServerRuntimeHandle &,
                         const WebServerRuntimeHandle &) noexcept = default;
  friend std::strong_ordering
  operator<=>(const WebServerRuntimeHandle &lhs,
              const WebServerRuntimeHandle &rhs) noexcept {
    return reinterpret_cast<std::uintptr_t>(lhs.value.get()) <=>
           reinterpret_cast<std::uintptr_t>(rhs.value.get());
  }
};

inline std::ostream &operator<<(std::ostream &stream,
                                const WebServerRuntimeHandle &value) {
  return stream << "WebServerRuntimeHandle(" << value.value.get() << ')';
}
} // namespace
} // namespace hgraph::web::detail

namespace std {
template <> struct hash<hgraph::web::detail::WebServerRuntimeHandle> {
  size_t operator()(const hgraph::web::detail::WebServerRuntimeHandle &value)
      const noexcept {
    return hash<const void *>{}(value.value.get());
  }
};

template <> struct hash<hgraph::web::detail::ServerConfigHandle> {
  size_t operator()(const hgraph::web::detail::ServerConfigHandle &value)
      const noexcept {
    return hash<const void *>{}(value.value.get());
  }
};
} // namespace std

namespace hgraph::static_schema_detail {
template <> struct scalar_name<web::detail::WebServerRuntimeHandle> {
  static constexpr std::string_view value{
      "hgraph.web.internal::WebServerRuntimeHandle"};
};

template <> struct scalar_name<web::detail::ServerConfigHandle> {
  static constexpr std::string_view value{
      "hgraph.web.internal::ServerConfigHandle"};
};
} // namespace hgraph::static_schema_detail

namespace hgraph::web {
namespace {
namespace wd = ::hgraph::web::detail;

struct WebServerRuntimeNode {
  static constexpr auto name = "web_server_runtime";
  using signature_args = std::tuple<
      In<"http_routes", TSS<WebRoute>, InputValidity::Unchecked>,
      In<"ws_routes", TSS<WebRoute>, InputValidity::Unchecked>,
      In<"responses", TSD<Int, HttpRespondRequest>, InputValidity::Unchecked>,
      In<"ws_sends", TSD<Int, WsSendRequest>, InputValidity::Unchecked>,
      Scalar<"config", wd::ServerConfigHandle>, Scalar<"path", Str>,
      Scalar<"bridge", wd::ServerBridgeHandle>,
      State<wd::WebServerRuntimeHandle>>;

  static void start(Scalar<"config", wd::ServerConfigHandle> config,
                    Scalar<"path", Str> path,
                    Scalar<"bridge", wd::ServerBridgeHandle> bridge,
                    State<wd::WebServerRuntimeHandle> state,
                    EngineControlView engine) {
    auto runtime = std::make_shared<wd::WebServerRuntime>(
        config.value().value, path.value(), bridge.value(),
        engine.mode() == GraphExecutorMode::Simulation);
    runtime->start();
    try {
      state.set(wd::WebServerRuntimeHandle{runtime});
    } catch (...) {
      runtime->stop();
      throw;
    }
  }

  static void
  eval(In<"http_routes", TSS<WebRoute>, InputValidity::Unchecked> http_routes,
       In<"ws_routes", TSS<WebRoute>, InputValidity::Unchecked> ws_routes,
       In<"responses", TSD<Int, HttpRespondRequest>, InputValidity::Unchecked>
           responses,
       In<"ws_sends", TSD<Int, WsSendRequest>, InputValidity::Unchecked>
           ws_sends,
       Scalar<"bridge", wd::ServerBridgeHandle> bridge,
       State<wd::WebServerRuntimeHandle> state) {
    auto runtime = state.get().value;
    if (!runtime) {
      throw std::logic_error("Web server runtime evaluated before start");
    }

    if (http_routes.modified()) {
      const auto &erased = static_cast<const TSSInputView &>(http_routes);
      std::vector<Value> removed;
      for (const auto route : erased.removed()) {
        if (!wd::erase_keyed<wd::WebRequestEnvelope>(
                *bridge.value().value, wd::index(wd::ServerChannel::Request),
                "route", route.clone())) {
          throw std::overflow_error("Web route-removal queue is full");
        }
        removed.push_back(route.clone());
      }
      std::vector<Value> added;
      for (const auto route : erased.added()) {
        added.push_back(route.clone());
      }
      runtime->apply_http_routes(std::move(added), std::move(removed));
    }

    if (ws_routes.modified()) {
      const auto &erased = static_cast<const TSSInputView &>(ws_routes);
      std::vector<Value> removed;
      for (const auto route : erased.removed()) {
        if (!wd::erase_keyed<wd::WsIngressEnvelope>(
                *bridge.value().value, wd::index(wd::ServerChannel::WsIngress),
                "route", route.clone())) {
          throw std::overflow_error("Web WS route-removal queue is full");
        }
        removed.push_back(route.clone());
      }
      std::vector<Value> added;
      for (const auto route : erased.added()) {
        added.push_back(route.clone());
      }
      runtime->apply_ws_routes(std::move(added), std::move(removed));
    }

    if (responses.modified()) {
      for (const auto &[client_id, request] : responses.modified_items()) {
        auto response = request.template field<"response">();
        auto request_id = request.template field<"request_id">();
        if (!response.modified() || !response.valid()) {
          continue;
        }
        if (!request_id.valid()) {
          throw std::invalid_argument(
              "Web respond requires a valid request id");
        }
        runtime->respond(client_id.template checked_as<Int>(),
                         request_id.value(),
                         response.base().value().clone());
      }
    }

    if (ws_sends.modified()) {
      for (const auto &[client_id, request] : ws_sends.modified_items()) {
        auto frame = request.template field<"frame">();
        auto connection_id = request.template field<"connection_id">();
        if (!frame.modified() || !frame.valid()) {
          continue;
        }
        if (!connection_id.valid()) {
          throw std::invalid_argument(
              "Web WS send requires a valid connection id");
        }
        runtime->ws_send(client_id.template checked_as<Int>(),
                         connection_id.value(),
                         frame.base().value().clone());
      }
    }
  }

  static void stop(State<wd::WebServerRuntimeHandle> state) {
    if (auto runtime = state.get().value) {
      runtime->stop();
    }
    state.set(wd::WebServerRuntimeHandle{});
  }
};

struct WebServerImpl {
  static constexpr auto name = "web_server_impl";

  static void compose(Wiring &w, Scalar<"config", Value> config,
                      Scalar<"path", Str> path) {
    register_web_types();
    wd::register_internal_types();
    // Fail every structural error at wiring time (RFC 0024, configuration).
    wd::ServerConfigHandle runtime_config{
        std::make_shared<const wd::ServerRuntimeConfig>(
            wd::parse_server_config(config.value()))};

    const auto binding = service::path(path.value());
    auto http_routes = service::impl_input<HttpServeService>(w, binding);
    auto ws_routes = service::impl_input<WsServeService>(w, binding);
    auto responses = service::impl_input<HttpRespondService>(w, binding);
    auto ws_sends = service::impl_input<WsSendService>(w, binding);

    auto bridge = wd::make_server_bridge(config.value());
    auto outputs = wd::wire_server_outputs(w, bridge);

    static_cast<void>(wire<WebServerRuntimeNode>(w, http_routes, ws_routes,
                                                 responses, ws_sends,
                                                 runtime_config, path.value(),
                                                 bridge));

    service::impl_output<HttpServeService>(w, binding, outputs.requests);
    service::impl_output<WsServeService>(w, binding, outputs.ws);
    service::impl_output<HttpRespondService>(w, binding,
                                             outputs.respond_reports);
    service::impl_output<WsSendService>(w, binding, outputs.ws_send_reports);
    service::impl_output<WebServerEventService>(w, binding, outputs.events);
    service::impl_output<WebServerStatsService>(w, binding, outputs.stats);
  }
};
} // namespace

void register_server(Wiring &w, service::ServicePath path,
                     Value server_config) {
  service::register_services<WebServerImpl, HttpServeService,
                             HttpRespondService, WsServeService, WsSendService,
                             WebServerEventService, WebServerStatsService>(
      w, std::move(path), std::move(server_config));
}
} // namespace hgraph::web
