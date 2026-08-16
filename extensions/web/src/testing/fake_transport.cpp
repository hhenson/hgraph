#include <hgraph/web/testing/fake_transport.h>

#include "../detail/service_bridge.h"

#include <condition_variable>
#include <cstdint>
#include <memory>
#include <mutex>
#include <ostream>
#include <stdexcept>
#include <utility>
#include <vector>

namespace hgraph::web::testing::detail {
struct FakeServerHandle {
  FakeWebServerPtr value{};

  friend bool operator==(const FakeServerHandle &,
                         const FakeServerHandle &) noexcept = default;
  friend std::strong_ordering
  operator<=>(const FakeServerHandle &lhs,
              const FakeServerHandle &rhs) noexcept {
    return reinterpret_cast<std::uintptr_t>(lhs.value.get()) <=>
           reinterpret_cast<std::uintptr_t>(rhs.value.get());
  }
};

inline std::ostream &operator<<(std::ostream &stream,
                                const FakeServerHandle &value) {
  return stream << "FakeServerHandle(" << value.value.get() << ')';
}

struct FakeClientHandle {
  FakeWebClientPtr value{};

  friend bool operator==(const FakeClientHandle &,
                         const FakeClientHandle &) noexcept = default;
  friend std::strong_ordering
  operator<=>(const FakeClientHandle &lhs,
              const FakeClientHandle &rhs) noexcept {
    return reinterpret_cast<std::uintptr_t>(lhs.value.get()) <=>
           reinterpret_cast<std::uintptr_t>(rhs.value.get());
  }
};

inline std::ostream &operator<<(std::ostream &stream,
                                const FakeClientHandle &value) {
  return stream << "FakeClientHandle(" << value.value.get() << ')';
}

class FakeServerRuntime;
class FakeClientRuntime;

struct FakeServerRuntimeHandle {
  std::shared_ptr<FakeServerRuntime> value{};

  friend bool operator==(const FakeServerRuntimeHandle &,
                         const FakeServerRuntimeHandle &) noexcept = default;
  friend std::strong_ordering
  operator<=>(const FakeServerRuntimeHandle &lhs,
              const FakeServerRuntimeHandle &rhs) noexcept {
    return reinterpret_cast<std::uintptr_t>(lhs.value.get()) <=>
           reinterpret_cast<std::uintptr_t>(rhs.value.get());
  }
};

inline std::ostream &operator<<(std::ostream &stream,
                                const FakeServerRuntimeHandle &value) {
  return stream << "FakeServerRuntimeHandle(" << value.value.get() << ')';
}

struct FakeClientRuntimeHandle {
  std::shared_ptr<FakeClientRuntime> value{};

  friend bool operator==(const FakeClientRuntimeHandle &,
                         const FakeClientRuntimeHandle &) noexcept = default;
  friend std::strong_ordering
  operator<=>(const FakeClientRuntimeHandle &lhs,
              const FakeClientRuntimeHandle &rhs) noexcept {
    return reinterpret_cast<std::uintptr_t>(lhs.value.get()) <=>
           reinterpret_cast<std::uintptr_t>(rhs.value.get());
  }
};

inline std::ostream &operator<<(std::ostream &stream,
                                const FakeClientRuntimeHandle &value) {
  return stream << "FakeClientRuntimeHandle(" << value.value.get() << ')';
}
} // namespace hgraph::web::testing::detail

namespace std {
template <> struct hash<hgraph::web::testing::detail::FakeServerHandle> {
  size_t operator()(const hgraph::web::testing::detail::FakeServerHandle
                        &value) const noexcept {
    return hash<const void *>{}(value.value.get());
  }
};

template <> struct hash<hgraph::web::testing::detail::FakeClientHandle> {
  size_t operator()(const hgraph::web::testing::detail::FakeClientHandle
                        &value) const noexcept {
    return hash<const void *>{}(value.value.get());
  }
};

template <>
struct hash<hgraph::web::testing::detail::FakeServerRuntimeHandle> {
  size_t operator()(const hgraph::web::testing::detail::FakeServerRuntimeHandle
                        &value) const noexcept {
    return hash<const void *>{}(value.value.get());
  }
};

template <>
struct hash<hgraph::web::testing::detail::FakeClientRuntimeHandle> {
  size_t operator()(const hgraph::web::testing::detail::FakeClientRuntimeHandle
                        &value) const noexcept {
    return hash<const void *>{}(value.value.get());
  }
};
} // namespace std

namespace hgraph::static_schema_detail {
template <> struct scalar_name<web::testing::detail::FakeServerHandle> {
  static constexpr std::string_view value{
      "hgraph.web.testing::FakeServerHandle"};
};

template <> struct scalar_name<web::testing::detail::FakeClientHandle> {
  static constexpr std::string_view value{
      "hgraph.web.testing::FakeClientHandle"};
};

template <> struct scalar_name<web::testing::detail::FakeServerRuntimeHandle> {
  static constexpr std::string_view value{
      "hgraph.web.testing::FakeServerRuntimeHandle"};
};

template <> struct scalar_name<web::testing::detail::FakeClientRuntimeHandle> {
  static constexpr std::string_view value{
      "hgraph.web.testing::FakeClientRuntimeHandle"};
};
} // namespace hgraph::static_schema_detail

namespace hgraph::web::testing {
namespace {
namespace wd = ::hgraph::web::detail;

template <typename T> [[nodiscard]] Value atomic(T value) {
  static_cast<void>(scalar_descriptor<T>::value_meta());
  return Value{std::move(value)};
}

template <typename Schema>
[[nodiscard]] Value
bundle(std::vector<std::pair<std::string_view, Value>> fields) {
  BundleBuilder builder{ValuePlanFactory::instance().type_for(
      scalar_descriptor<Schema>::value_meta())};
  for (auto &[name, field] : fields) {
    if (field.has_value()) {
      builder.set(name, std::move(field));
    }
  }
  return builder.build();
}

void require_schema(const Value &value, const void *expected,
                    std::string_view what) {
  if (value.schema() != expected) {
    throw std::invalid_argument(std::string{"Web fake "} + std::string{what} +
                                " has the wrong schema");
  }
}

[[nodiscard]] Value request_envelope(Value route, Value request) {
  return bundle<wd::WebRequestEnvelope>({
      {"route", std::move(route)},
      {"request", std::move(request)},
  });
}

[[nodiscard]] Value route_state_envelope(Value route, WebRouteState state) {
  return bundle<wd::WebRequestEnvelope>({
      {"route", std::move(route)},
      {"state", atomic(state)},
  });
}

[[nodiscard]] Value ws_ingress_envelope(Value route, Value event,
                                        Value frame) {
  return bundle<wd::WsIngressEnvelope>({
      {"route", std::move(route)},
      {"event", std::move(event)},
      {"frame", std::move(frame)},
  });
}

[[nodiscard]] Value ws_client_envelope(Value key, Value event, Value frame) {
  return bundle<wd::WsClientEnvelope>({
      {"key", std::move(key)},
      {"event", std::move(event)},
      {"frame", std::move(frame)},
  });
}

[[nodiscard]] Value delivery_envelope(Int client_id, Value report) {
  return bundle<wd::WebDeliveryEnvelope>({
      {"request_id", atomic(client_id)},
      {"report", std::move(report)},
  });
}

[[nodiscard]] Value response_envelope(Int client_id, Value response,
                                      Value failure) {
  return bundle<wd::WebResponseEnvelope>({
      {"request_id", atomic(client_id)},
      {"response", std::move(response)},
      {"failure", std::move(failure)},
  });
}

[[nodiscard]] Value event_envelope(Value event, bool stop_graph) {
  return bundle<wd::WebEventEnvelope>({
      {"event", std::move(event)},
      {"stop_graph", atomic(Bool{stop_graph})},
  });
}
} // namespace

struct FakeWebServer::Impl {
  mutable std::mutex mutex{};
  mutable std::condition_variable changed{};
  std::shared_ptr<wd::ServerBridge> bridge{};
  std::size_t attaches{};
  Int sequence{};
  std::vector<Value> http_routes{};
  std::vector<Value> removed_http_routes{};
  std::vector<Value> ws_routes{};
  std::vector<Value> removed_ws_routes{};
  std::vector<FakeResponse> responses{};
  std::vector<FakeWsSend> ws_sends{};
};

struct detail::FakeServerAccess {
  [[nodiscard]] static std::shared_ptr<wd::ServerBridge>
  bridge(const FakeWebServer &server) {
    std::lock_guard lock{server.impl_->mutex};
    if (!server.impl_->bridge) {
      throw std::logic_error(
          "Web fake server is not attached to a running graph");
    }
    return server.impl_->bridge;
  }

  static void attach(FakeWebServer &server,
                     std::shared_ptr<wd::ServerBridge> bridge) {
    {
      std::lock_guard lock{server.impl_->mutex};
      server.impl_->bridge = std::move(bridge);
      ++server.impl_->attaches;
    }
    server.impl_->changed.notify_all();
  }

  static void detach(FakeWebServer &server) noexcept {
    {
      std::lock_guard lock{server.impl_->mutex};
      server.impl_->bridge.reset();
    }
    server.impl_->changed.notify_all();
  }

  static void http_route_added(FakeWebServer &server, Value route) {
    std::shared_ptr<wd::ServerBridge> bridge;
    {
      std::lock_guard lock{server.impl_->mutex};
      bridge = server.impl_->bridge;
    }
    // Route activation is observable graph data: the serve output's key
    // materializes with a Serving state tick (RFC 0024).  The state envelope
    // is queued BEFORE waiters learn about the route, so a test emitting a
    // request on wake cannot overtake the activation tick.
    if (bridge && !bridge->push(wd::index(wd::ServerChannel::Request),
                                route_state_envelope(route.clone(),
                                                     WebRouteState::Serving),
                                1)) {
      throw std::overflow_error("Web fake request queue is full");
    }
    {
      std::lock_guard lock{server.impl_->mutex};
      server.impl_->http_routes.push_back(std::move(route));
    }
    server.impl_->changed.notify_all();
  }

  static void http_route_removed(FakeWebServer &server, Value route) {
    {
      std::lock_guard lock{server.impl_->mutex};
      server.impl_->removed_http_routes.push_back(std::move(route));
    }
    server.impl_->changed.notify_all();
  }

  static void ws_route_added(FakeWebServer &server, Value route) {
    {
      std::lock_guard lock{server.impl_->mutex};
      server.impl_->ws_routes.push_back(std::move(route));
    }
    server.impl_->changed.notify_all();
  }

  static void ws_route_removed(FakeWebServer &server, Value route) {
    {
      std::lock_guard lock{server.impl_->mutex};
      server.impl_->removed_ws_routes.push_back(std::move(route));
    }
    server.impl_->changed.notify_all();
  }

  static void respond(FakeWebServer &server, Int client_id, Int request_id,
                      Value response) {
    std::shared_ptr<wd::ServerBridge> bridge;
    Int sequence{};
    {
      std::lock_guard lock{server.impl_->mutex};
      if (!server.impl_->bridge) {
        throw std::logic_error(
            "Web fake server is not attached to a running graph");
      }
      sequence = ++server.impl_->sequence;
      server.impl_->responses.push_back(
          FakeResponse{client_id, request_id, response.clone()});
      bridge = server.impl_->bridge;
    }
    server.impl_->changed.notify_all();

    Value report = make_delivery_report(request_id, sequence,
                                        WebDeliveryStatus::Delivered);
    if (!bridge->push(wd::index(wd::ServerChannel::RespondDelivery),
                      delivery_envelope(client_id, std::move(report)), 1)) {
      throw std::overflow_error("Web fake respond-delivery queue is full");
    }
  }

  static void ws_send(FakeWebServer &server, Int client_id, Int connection_id,
                      Value frame) {
    std::shared_ptr<wd::ServerBridge> bridge;
    Int sequence{};
    {
      std::lock_guard lock{server.impl_->mutex};
      if (!server.impl_->bridge) {
        throw std::logic_error(
            "Web fake server is not attached to a running graph");
      }
      sequence = ++server.impl_->sequence;
      server.impl_->ws_sends.push_back(
          FakeWsSend{client_id, connection_id, frame.clone()});
      bridge = server.impl_->bridge;
    }
    server.impl_->changed.notify_all();

    Value report = make_delivery_report(connection_id, sequence,
                                        WebDeliveryStatus::Delivered);
    if (!bridge->push(wd::index(wd::ServerChannel::WsSendDelivery),
                      delivery_envelope(client_id, std::move(report)), 1)) {
      throw std::overflow_error("Web fake ws-send-delivery queue is full");
    }
  }
};

FakeWebServer::FakeWebServer() : impl_{std::make_unique<Impl>()} {}
FakeWebServer::~FakeWebServer() = default;

bool FakeWebServer::wait_until_attached(
    std::chrono::milliseconds timeout) const {
  std::unique_lock lock{impl_->mutex};
  return impl_->changed.wait_for(
      lock, timeout, [&] { return static_cast<bool>(impl_->bridge); });
}

bool FakeWebServer::wait_until_detached(
    std::chrono::milliseconds timeout) const {
  std::unique_lock lock{impl_->mutex};
  return impl_->changed.wait_for(lock, timeout,
                                 [&] { return !impl_->bridge; });
}

bool FakeWebServer::wait_for_http_routes(
    std::size_t count, std::chrono::milliseconds timeout) const {
  std::unique_lock lock{impl_->mutex};
  return impl_->changed.wait_for(
      lock, timeout, [&] { return impl_->http_routes.size() >= count; });
}

bool FakeWebServer::wait_for_ws_routes(
    std::size_t count, std::chrono::milliseconds timeout) const {
  std::unique_lock lock{impl_->mutex};
  return impl_->changed.wait_for(
      lock, timeout, [&] { return impl_->ws_routes.size() >= count; });
}

bool FakeWebServer::wait_for_responses(
    std::size_t count, std::chrono::milliseconds timeout) const {
  std::unique_lock lock{impl_->mutex};
  return impl_->changed.wait_for(
      lock, timeout, [&] { return impl_->responses.size() >= count; });
}

bool FakeWebServer::wait_for_ws_sends(
    std::size_t count, std::chrono::milliseconds timeout) const {
  std::unique_lock lock{impl_->mutex};
  return impl_->changed.wait_for(
      lock, timeout, [&] { return impl_->ws_sends.size() >= count; });
}

std::size_t FakeWebServer::attach_count() const {
  std::lock_guard lock{impl_->mutex};
  return impl_->attaches;
}

std::vector<Value> FakeWebServer::http_routes() const {
  std::lock_guard lock{impl_->mutex};
  return impl_->http_routes;
}

std::vector<Value> FakeWebServer::removed_http_routes() const {
  std::lock_guard lock{impl_->mutex};
  return impl_->removed_http_routes;
}

std::vector<Value> FakeWebServer::ws_routes() const {
  std::lock_guard lock{impl_->mutex};
  return impl_->ws_routes;
}

std::vector<Value> FakeWebServer::removed_ws_routes() const {
  std::lock_guard lock{impl_->mutex};
  return impl_->removed_ws_routes;
}

std::vector<FakeResponse> FakeWebServer::responses() const {
  std::lock_guard lock{impl_->mutex};
  return impl_->responses;
}

std::vector<FakeWsSend> FakeWebServer::ws_sends() const {
  std::lock_guard lock{impl_->mutex};
  return impl_->ws_sends;
}

namespace {
[[nodiscard]] std::shared_ptr<wd::ServerBridge>
attached_server_bridge(const FakeWebServer &server) {
  return detail::FakeServerAccess::bridge(server);
}
} // namespace

void FakeWebServer::emit_request(Value route, Value request) {
  require_schema(route, scalar_descriptor<WebRoute>::value_meta(), "route");
  require_schema(request, scalar_descriptor<HttpServerRequest>::value_meta(),
                 "request");
  auto bridge = attached_server_bridge(*this);
  if (!bridge->push(wd::index(wd::ServerChannel::Request),
                    request_envelope(std::move(route), std::move(request)),
                    1)) {
    throw std::overflow_error("Web fake request queue is full");
  }
}

void FakeWebServer::emit_route_state(Value route, WebRouteState state) {
  require_schema(route, scalar_descriptor<WebRoute>::value_meta(), "route");
  auto bridge = attached_server_bridge(*this);
  if (!bridge->push(wd::index(wd::ServerChannel::Request),
                    route_state_envelope(std::move(route), state), 1)) {
    throw std::overflow_error("Web fake request queue is full");
  }
}

void FakeWebServer::emit_ws_event(Value route, Value event) {
  require_schema(route, scalar_descriptor<WebRoute>::value_meta(), "route");
  require_schema(event, scalar_descriptor<WsEvent>::value_meta(), "WS event");
  auto bridge = attached_server_bridge(*this);
  if (!bridge->push(wd::index(wd::ServerChannel::WsIngress),
                    ws_ingress_envelope(std::move(route), std::move(event),
                                        Value{}),
                    1)) {
    throw std::overflow_error("Web fake WS ingress queue is full");
  }
}

void FakeWebServer::emit_ws_frame(Value route, Value inbound_frame) {
  require_schema(route, scalar_descriptor<WebRoute>::value_meta(), "route");
  require_schema(inbound_frame,
                 scalar_descriptor<WsInboundFrame>::value_meta(),
                 "inbound frame");
  auto bridge = attached_server_bridge(*this);
  if (!bridge->push(wd::index(wd::ServerChannel::WsIngress),
                    ws_ingress_envelope(std::move(route), Value{},
                                        std::move(inbound_frame)),
                    1)) {
    throw std::overflow_error("Web fake WS ingress queue is full");
  }
}

void FakeWebServer::emit_event(Value event, bool stop_graph) {
  require_schema(event, scalar_descriptor<WebEvent>::value_meta(), "event");
  auto bridge = attached_server_bridge(*this);
  if (!bridge->push(wd::index(wd::ServerChannel::Event),
                    event_envelope(std::move(event), stop_graph), 1)) {
    throw std::overflow_error("Web fake event queue is full");
  }
}

void FakeWebServer::emit_stats(Value stats) {
  require_schema(stats, scalar_descriptor<WebServerStats>::value_meta(),
                 "stats");
  auto bridge = attached_server_bridge(*this);
  bridge->push_latest(wd::index(wd::ServerChannel::Stats), std::move(stats),
                      1);
}

struct FakeWebClient::Impl {
  mutable std::mutex mutex{};
  mutable std::condition_variable changed{};
  std::shared_ptr<wd::ClientBridge> bridge{};
  std::size_t attaches{};
  Int sequence{};
  std::vector<FakeHttpCall> calls{};
  std::vector<Value> ws_keys{};
  std::vector<Value> removed_ws_keys{};
  std::vector<FakeClientWsSend> ws_sends{};
};

struct detail::FakeClientAccess {
  [[nodiscard]] static std::shared_ptr<wd::ClientBridge>
  bridge(const FakeWebClient &client) {
    std::lock_guard lock{client.impl_->mutex};
    if (!client.impl_->bridge) {
      throw std::logic_error(
          "Web fake client is not attached to a running graph");
    }
    return client.impl_->bridge;
  }

  static void attach(FakeWebClient &client,
                     std::shared_ptr<wd::ClientBridge> bridge) {
    {
      std::lock_guard lock{client.impl_->mutex};
      client.impl_->bridge = std::move(bridge);
      ++client.impl_->attaches;
    }
    client.impl_->changed.notify_all();
  }

  static void detach(FakeWebClient &client) noexcept {
    {
      std::lock_guard lock{client.impl_->mutex};
      client.impl_->bridge.reset();
    }
    client.impl_->changed.notify_all();
  }

  static void call(FakeWebClient &client, Int client_id, Value request,
                   Value options) {
    {
      std::lock_guard lock{client.impl_->mutex};
      if (!client.impl_->bridge) {
        throw std::logic_error(
            "Web fake client is not attached to a running graph");
      }
      client.impl_->calls.push_back(
          FakeHttpCall{client_id, std::move(request), std::move(options)});
    }
    client.impl_->changed.notify_all();
  }

  static void ws_key_added(FakeWebClient &client, Value key) {
    {
      std::lock_guard lock{client.impl_->mutex};
      client.impl_->ws_keys.push_back(std::move(key));
    }
    client.impl_->changed.notify_all();
  }

  static void ws_key_removed(FakeWebClient &client, Value key) {
    {
      std::lock_guard lock{client.impl_->mutex};
      client.impl_->removed_ws_keys.push_back(std::move(key));
    }
    client.impl_->changed.notify_all();
  }

  static void ws_send(FakeWebClient &client, Int client_id, Value key,
                      Value frame) {
    std::shared_ptr<wd::ClientBridge> bridge;
    Int sequence{};
    {
      std::lock_guard lock{client.impl_->mutex};
      if (!client.impl_->bridge) {
        throw std::logic_error(
            "Web fake client is not attached to a running graph");
      }
      sequence = ++client.impl_->sequence;
      client.impl_->ws_sends.push_back(
          FakeClientWsSend{client_id, std::move(key), frame.clone()});
      bridge = client.impl_->bridge;
    }
    client.impl_->changed.notify_all();

    Value report =
        make_delivery_report(client_id, sequence, WebDeliveryStatus::Delivered);
    if (!bridge->push(wd::index(wd::ClientChannel::SendDelivery),
                      delivery_envelope(client_id, std::move(report)), 1)) {
      throw std::overflow_error("Web fake send-delivery queue is full");
    }
  }
};

FakeWebClient::FakeWebClient() : impl_{std::make_unique<Impl>()} {}
FakeWebClient::~FakeWebClient() = default;

bool FakeWebClient::wait_until_attached(
    std::chrono::milliseconds timeout) const {
  std::unique_lock lock{impl_->mutex};
  return impl_->changed.wait_for(
      lock, timeout, [&] { return static_cast<bool>(impl_->bridge); });
}

bool FakeWebClient::wait_until_detached(
    std::chrono::milliseconds timeout) const {
  std::unique_lock lock{impl_->mutex};
  return impl_->changed.wait_for(lock, timeout,
                                 [&] { return !impl_->bridge; });
}

bool FakeWebClient::wait_for_calls(std::size_t count,
                                   std::chrono::milliseconds timeout) const {
  std::unique_lock lock{impl_->mutex};
  return impl_->changed.wait_for(
      lock, timeout, [&] { return impl_->calls.size() >= count; });
}

bool FakeWebClient::wait_for_ws_keys(std::size_t count,
                                     std::chrono::milliseconds timeout) const {
  std::unique_lock lock{impl_->mutex};
  return impl_->changed.wait_for(
      lock, timeout, [&] { return impl_->ws_keys.size() >= count; });
}

bool FakeWebClient::wait_for_ws_sends(std::size_t count,
                                      std::chrono::milliseconds timeout) const {
  std::unique_lock lock{impl_->mutex};
  return impl_->changed.wait_for(
      lock, timeout, [&] { return impl_->ws_sends.size() >= count; });
}

std::size_t FakeWebClient::attach_count() const {
  std::lock_guard lock{impl_->mutex};
  return impl_->attaches;
}

std::vector<FakeHttpCall> FakeWebClient::calls() const {
  std::lock_guard lock{impl_->mutex};
  return impl_->calls;
}

std::vector<Value> FakeWebClient::ws_keys() const {
  std::lock_guard lock{impl_->mutex};
  return impl_->ws_keys;
}

std::vector<Value> FakeWebClient::removed_ws_keys() const {
  std::lock_guard lock{impl_->mutex};
  return impl_->removed_ws_keys;
}

std::vector<FakeClientWsSend> FakeWebClient::ws_sends() const {
  std::lock_guard lock{impl_->mutex};
  return impl_->ws_sends;
}

namespace {
[[nodiscard]] std::shared_ptr<wd::ClientBridge>
attached_client_bridge(const FakeWebClient &client) {
  return detail::FakeClientAccess::bridge(client);
}
} // namespace

void FakeWebClient::respond(Int client_id, Value response) {
  require_schema(response, scalar_descriptor<HttpResponse>::value_meta(),
                 "response");
  auto bridge = attached_client_bridge(*this);
  if (!bridge->push(wd::index(wd::ClientChannel::Response),
                    response_envelope(client_id, std::move(response), Value{}),
                    1)) {
    throw std::overflow_error("Web fake response queue is full");
  }
}

void FakeWebClient::fail(Int client_id, Value transport_error) {
  require_schema(transport_error,
                 scalar_descriptor<WebTransportError>::value_meta(),
                 "transport error");
  auto bridge = attached_client_bridge(*this);
  if (!bridge->push(wd::index(wd::ClientChannel::Response),
                    response_envelope(client_id, Value{},
                                      std::move(transport_error)),
                    1)) {
    throw std::overflow_error("Web fake response queue is full");
  }
}

void FakeWebClient::emit_ws_event(Value key, Value event) {
  require_schema(key, scalar_descriptor<WsClientKey>::value_meta(), "WS key");
  require_schema(event, scalar_descriptor<WsEvent>::value_meta(), "WS event");
  auto bridge = attached_client_bridge(*this);
  if (!bridge->push(wd::index(wd::ClientChannel::WsIngress),
                    ws_client_envelope(std::move(key), std::move(event),
                                       Value{}),
                    1)) {
    throw std::overflow_error("Web fake client WS queue is full");
  }
}

void FakeWebClient::emit_ws_frame(Value key, Value frame) {
  require_schema(key, scalar_descriptor<WsClientKey>::value_meta(), "WS key");
  require_schema(frame, scalar_descriptor<WsFrame>::value_meta(), "frame");
  auto bridge = attached_client_bridge(*this);
  if (!bridge->push(wd::index(wd::ClientChannel::WsIngress),
                    ws_client_envelope(std::move(key), Value{},
                                       std::move(frame)),
                    1)) {
    throw std::overflow_error("Web fake client WS queue is full");
  }
}

void FakeWebClient::emit_event(Value event, bool stop_graph) {
  require_schema(event, scalar_descriptor<WebEvent>::value_meta(), "event");
  auto bridge = attached_client_bridge(*this);
  if (!bridge->push(wd::index(wd::ClientChannel::Event),
                    event_envelope(std::move(event), stop_graph), 1)) {
    throw std::overflow_error("Web fake event queue is full");
  }
}

void FakeWebClient::emit_stats(Value stats) {
  require_schema(stats, scalar_descriptor<WebClientStats>::value_meta(),
                 "stats");
  auto bridge = attached_client_bridge(*this);
  bridge->push_latest(wd::index(wd::ClientChannel::Stats), std::move(stats),
                      1);
}

namespace detail {
class FakeServerRuntime {
public:
  FakeServerRuntime(FakeWebServerPtr server, wd::ServerBridgeHandle bridge)
      : server_{std::move(server)}, bridge_{std::move(bridge)} {}

  void start() {
    bridge_.value->start();
    FakeServerAccess::attach(*server_, bridge_.value);
    attached_ = true;
  }

  void stop() noexcept {
    if (!attached_) {
      return;
    }
    bridge_.value->stop();
    FakeServerAccess::detach(*server_);
    attached_ = false;
  }

  void http_route_added(Value route) {
    FakeServerAccess::http_route_added(*server_, std::move(route));
  }

  void http_route_removed(Value route) {
    FakeServerAccess::http_route_removed(*server_, std::move(route));
  }

  void ws_route_added(Value route) {
    FakeServerAccess::ws_route_added(*server_, std::move(route));
  }

  void ws_route_removed(Value route) {
    FakeServerAccess::ws_route_removed(*server_, std::move(route));
  }

  void respond(Int client_id, Int request_id, Value response) {
    FakeServerAccess::respond(*server_, client_id, request_id,
                              std::move(response));
  }

  void ws_send(Int client_id, Int connection_id, Value frame) {
    FakeServerAccess::ws_send(*server_, client_id, connection_id,
                              std::move(frame));
  }

private:
  FakeWebServerPtr server_{};
  wd::ServerBridgeHandle bridge_{};
  bool attached_{};
};

class FakeClientRuntime {
public:
  FakeClientRuntime(FakeWebClientPtr client, wd::ClientBridgeHandle bridge)
      : client_{std::move(client)}, bridge_{std::move(bridge)} {}

  void start() {
    bridge_.value->start();
    FakeClientAccess::attach(*client_, bridge_.value);
    attached_ = true;
  }

  void stop() noexcept {
    if (!attached_) {
      return;
    }
    bridge_.value->stop();
    FakeClientAccess::detach(*client_);
    attached_ = false;
  }

  void call(Int client_id, Value request, Value options) {
    FakeClientAccess::call(*client_, client_id, std::move(request),
                           std::move(options));
  }

  void ws_key_added(Value key) {
    FakeClientAccess::ws_key_added(*client_, std::move(key));
  }

  void ws_key_removed(Value key) {
    FakeClientAccess::ws_key_removed(*client_, std::move(key));
  }

  void ws_send(Int client_id, Value key, Value frame) {
    FakeClientAccess::ws_send(*client_, client_id, std::move(key),
                              std::move(frame));
  }

private:
  FakeWebClientPtr client_{};
  wd::ClientBridgeHandle bridge_{};
  bool attached_{};
};
} // namespace detail

namespace {
struct FakeServerRuntimeNode {
  static constexpr auto name = "web_fake_server_runtime";
  using signature_args = std::tuple<
      In<"http_routes", TSS<WebRoute>, InputValidity::Unchecked>,
      In<"ws_routes", TSS<WebRoute>, InputValidity::Unchecked>,
      In<"responses", TSD<Int, HttpRespondRequest>, InputValidity::Unchecked>,
      In<"ws_sends", TSD<Int, WsSendRequest>, InputValidity::Unchecked>,
      Scalar<"server", detail::FakeServerHandle>,
      Scalar<"bridge", wd::ServerBridgeHandle>,
      State<detail::FakeServerRuntimeHandle>>;

  static void start(Scalar<"server", detail::FakeServerHandle> server,
                    Scalar<"bridge", wd::ServerBridgeHandle> bridge,
                    State<detail::FakeServerRuntimeHandle> state) {
    auto runtime = std::make_shared<detail::FakeServerRuntime>(
        server.value().value, bridge.value());
    runtime->start();
    try {
      state.set(detail::FakeServerRuntimeHandle{runtime});
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
       State<detail::FakeServerRuntimeHandle> state) {
    auto runtime = state.get().value;
    if (!runtime) {
      throw std::logic_error("Web fake server runtime evaluated before start");
    }

    if (http_routes.modified()) {
      const auto &erased = static_cast<const TSSInputView &>(http_routes);
      for (const auto route : erased.removed()) {
        if (!wd::erase_keyed<wd::WebRequestEnvelope>(
                *bridge.value().value, wd::index(wd::ServerChannel::Request),
                "route", route.clone())) {
          throw std::overflow_error("Web fake route-removal queue is full");
        }
        runtime->http_route_removed(route.clone());
      }
      for (const auto route : erased.added()) {
        runtime->http_route_added(route.clone());
      }
    }

    if (ws_routes.modified()) {
      const auto &erased = static_cast<const TSSInputView &>(ws_routes);
      for (const auto route : erased.removed()) {
        if (!wd::erase_keyed<wd::WsIngressEnvelope>(
                *bridge.value().value, wd::index(wd::ServerChannel::WsIngress),
                "route", route.clone())) {
          throw std::overflow_error(
              "Web fake WS route-removal queue is full");
        }
        runtime->ws_route_removed(route.clone());
      }
      for (const auto route : erased.added()) {
        runtime->ws_route_added(route.clone());
      }
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

  static void stop(State<detail::FakeServerRuntimeHandle> state) {
    if (auto runtime = state.get().value) {
      runtime->stop();
    }
    state.set(detail::FakeServerRuntimeHandle{});
  }
};

struct FakeWebServerImpl {
  static constexpr auto name = "web_fake_server_impl";

  static void compose(Wiring &w, Scalar<"config", Value> config,
                      Scalar<"server", detail::FakeServerHandle> server,
                      Scalar<"path", Str> path) {
    register_web_types();
    wd::register_internal_types();
    if (config.value().schema() !=
        scalar_descriptor<WebServerConfig>::value_meta()) {
      throw std::invalid_argument(
          "Web server implementation requires WebServerConfig");
    }

    const auto binding = service::path(path.value());
    auto http_routes = service::impl_input<HttpServeService>(w, binding);
    auto ws_routes = service::impl_input<WsServeService>(w, binding);
    auto responses = service::impl_input<HttpRespondService>(w, binding);
    auto ws_sends = service::impl_input<WsSendService>(w, binding);

    auto bridge = wd::make_server_bridge(config.value());
    auto outputs = wd::wire_server_outputs(w, bridge);

    static_cast<void>(wire<FakeServerRuntimeNode>(w, http_routes, ws_routes,
                                                  responses, ws_sends,
                                                  server.value(), bridge));

    service::impl_output<HttpServeService>(w, binding, outputs.requests);
    service::impl_output<WsServeService>(w, binding, outputs.ws);
    service::impl_output<HttpRespondService>(w, binding,
                                             outputs.respond_reports);
    service::impl_output<WsSendService>(w, binding, outputs.ws_send_reports);
    service::impl_output<WebServerEventService>(w, binding, outputs.events);
    service::impl_output<WebServerStatsService>(w, binding, outputs.stats);
  }
};

struct FakeClientRuntimeNode {
  static constexpr auto name = "web_fake_client_runtime";
  using signature_args = std::tuple<
      In<"calls", TSD<Int, HttpClientCall>, InputValidity::Unchecked>,
      In<"ws_keys", TSS<WsClientKey>, InputValidity::Unchecked>,
      In<"ws_sends", TSD<Int, WsClientSendRequest>, InputValidity::Unchecked>,
      Scalar<"client", detail::FakeClientHandle>,
      Scalar<"bridge", wd::ClientBridgeHandle>,
      State<detail::FakeClientRuntimeHandle>>;

  static void start(Scalar<"client", detail::FakeClientHandle> client,
                    Scalar<"bridge", wd::ClientBridgeHandle> bridge,
                    State<detail::FakeClientRuntimeHandle> state) {
    auto runtime = std::make_shared<detail::FakeClientRuntime>(
        client.value().value, bridge.value());
    runtime->start();
    try {
      state.set(detail::FakeClientRuntimeHandle{runtime});
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
       State<detail::FakeClientRuntimeHandle> state) {
    auto runtime = state.get().value;
    if (!runtime) {
      throw std::logic_error("Web fake client runtime evaluated before start");
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
          throw std::overflow_error("Web fake key-removal queue is full");
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

  static void stop(State<detail::FakeClientRuntimeHandle> state) {
    if (auto runtime = state.get().value) {
      runtime->stop();
    }
    state.set(detail::FakeClientRuntimeHandle{});
  }
};

struct FakeWebClientImpl {
  static constexpr auto name = "web_fake_client_impl";

  static void compose(Wiring &w, Scalar<"config", Value> config,
                      Scalar<"client", detail::FakeClientHandle> client,
                      Scalar<"path", Str> path) {
    register_web_types();
    wd::register_internal_types();
    if (config.value().schema() !=
        scalar_descriptor<WebClientConfig>::value_meta()) {
      throw std::invalid_argument(
          "Web client implementation requires WebClientConfig");
    }

    const auto binding = service::path(path.value());
    auto calls = service::impl_input<HttpClientService>(w, binding);
    auto ws_keys = service::impl_input<WsClientService>(w, binding);
    auto ws_sends = service::impl_input<WsClientSendService>(w, binding);

    auto bridge = wd::make_client_bridge(config.value());
    auto outputs = wd::wire_client_outputs(w, bridge);

    static_cast<void>(wire<FakeClientRuntimeNode>(w, calls, ws_keys, ws_sends,
                                                  client.value(), bridge));

    service::impl_output<HttpClientService>(w, binding, outputs.responses);
    service::impl_output<WsClientService>(w, binding, outputs.ws);
    service::impl_output<WsClientSendService>(w, binding,
                                              outputs.send_reports);
    service::impl_output<WebClientEventService>(w, binding, outputs.events);
    service::impl_output<WebClientStatsService>(w, binding, outputs.stats);
  }
};
} // namespace

void register_fake_server(Wiring &w, service::ServicePath path,
                          Value server_config, FakeWebServerPtr server) {
  if (!server) {
    throw std::invalid_argument("Web fake service requires a server");
  }
  service::register_services<FakeWebServerImpl, HttpServeService,
                             HttpRespondService, WsServeService, WsSendService,
                             WebServerEventService, WebServerStatsService>(
      w, std::move(path), std::move(server_config),
      detail::FakeServerHandle{std::move(server)});
}

void register_fake_client(Wiring &w, service::ServicePath path,
                          Value client_config, FakeWebClientPtr client) {
  if (!client) {
    throw std::invalid_argument("Web fake service requires a client");
  }
  service::register_services<FakeWebClientImpl, HttpClientService,
                             WsClientService, WsClientSendService,
                             WebClientEventService, WebClientStatsService>(
      w, std::move(path), std::move(client_config),
      detail::FakeClientHandle{std::move(client)});
}
} // namespace hgraph::web::testing
