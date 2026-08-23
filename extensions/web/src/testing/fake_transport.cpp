#include <hgraph/web/testing/fake_transport.h>

#include "../detail/service_transport.h"

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

// ---------------------------------------------------------------------------
// Retained-byte estimates mirroring the live transports (asio_server.cpp,
// curl_client.cpp).  The fake must account real payload sizes so byte-limit
// and watermark behaviour is observable in socketless tests instead of being
// masked by one-byte records.

[[nodiscard]] std::size_t str_bytes(const ValueView &field) {
  return field.data() != nullptr ? field.checked_as<Str>().size() : 0;
}

[[nodiscard]] std::size_t bytes_field_bytes(const ValueView &field) {
  return field.data() != nullptr ? field.checked_as<Bytes>().data.size() : 0;
}

[[nodiscard]] std::size_t name_value_bytes(const ValueView &tuple) {
  if (tuple.data() == nullptr) {
    return 0;
  }
  std::size_t total = 0;
  for (const auto entry : tuple.as_list()) {
    const auto pair = entry.as_bundle();
    total += str_bytes(pair.at("name")) + str_bytes(pair.at("value"));
  }
  return total;
}

[[nodiscard]] std::size_t http_request_bytes(const ValueView &request) {
  if (request.data() == nullptr) {
    return 0;
  }
  const auto fields = request.as_bundle();
  return str_bytes(fields.at("target")) +
         name_value_bytes(fields.at("headers")) +
         bytes_field_bytes(fields.at("body"));
}

[[nodiscard]] std::size_t server_request_bytes(const Value &server_request) {
  const auto fields = server_request.view().as_bundle();
  return http_request_bytes(fields.at("request")) + 512;
}

[[nodiscard]] std::size_t ws_frame_bytes(const ValueView &frame) {
  if (frame.data() == nullptr) {
    return 0;
  }
  const auto fields = frame.as_bundle();
  return str_bytes(fields.at("text")) + bytes_field_bytes(fields.at("data")) +
         str_bytes(fields.at("close_reason"));
}

[[nodiscard]] std::size_t inbound_frame_bytes(const Value &inbound_frame) {
  const auto fields = inbound_frame.view().as_bundle();
  return ws_frame_bytes(fields.at("frame")) + 256;
}

[[nodiscard]] std::size_t ws_event_bytes(const Value &event) {
  const auto fields = event.view().as_bundle();
  const auto request = fields.at("request");
  const std::size_t request_bytes =
      request.data() != nullptr
          ? http_request_bytes(request.as_bundle().at("request"))
          : 0;
  return request_bytes + str_bytes(fields.at("close_reason")) + 512;
}

[[nodiscard]] std::size_t http_response_bytes(const Value &response) {
  const auto fields = response.view().as_bundle();
  return name_value_bytes(fields.at("headers")) +
         bytes_field_bytes(fields.at("body")) + 512;
}

[[nodiscard]] std::size_t transport_error_bytes(const Value &failure) {
  const auto fields = failure.view().as_bundle();
  return str_bytes(fields.at("message")) + 512;
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
  std::shared_ptr<wd::ServerTransportOutput> output{};
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
  [[nodiscard]] static std::shared_ptr<wd::ServerTransportOutput>
  output(const FakeWebServer &server) {
    std::lock_guard lock{server.impl_->mutex};
    if (!server.impl_->output) {
      throw std::logic_error(
          "Web fake server is not attached to a running graph");
    }
    return server.impl_->output;
  }

  static void attach(FakeWebServer &server,
                     std::shared_ptr<wd::ServerTransportOutput> output) {
    {
      std::lock_guard lock{server.impl_->mutex};
      server.impl_->output = std::move(output);
      ++server.impl_->attaches;
    }
    server.impl_->changed.notify_all();
  }

  static void detach(FakeWebServer &server) noexcept {
    {
      std::lock_guard lock{server.impl_->mutex};
      server.impl_->output.reset();
    }
    server.impl_->changed.notify_all();
  }

  static void http_route_added(FakeWebServer &server, Value route) {
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
    std::shared_ptr<wd::ServerTransportOutput> output;
    Int sequence{};
    {
      std::lock_guard lock{server.impl_->mutex};
      if (!server.impl_->output) {
        throw std::logic_error(
            "Web fake server is not attached to a running graph");
      }
      sequence = ++server.impl_->sequence;
      server.impl_->responses.push_back(
          FakeResponse{client_id, request_id, response.clone()});
      output = server.impl_->output;
    }
    server.impl_->changed.notify_all();

    Value report = make_delivery_report(request_id, sequence,
                                        WebDeliveryStatus::Delivered);
    if (!output->send(wd::WebTransportEventKind::ServerRespondDelivery,
                      "delivery",
                      delivery_envelope(client_id, std::move(report)),
                      wd::index(wd::ServerChannel::RespondDelivery), 512)) {
      throw std::overflow_error("Web fake respond-delivery queue is full");
    }
  }

  static void ws_send(FakeWebServer &server, Int client_id, Int connection_id,
                      Value frame) {
    std::shared_ptr<wd::ServerTransportOutput> output;
    Int sequence{};
    {
      std::lock_guard lock{server.impl_->mutex};
      if (!server.impl_->output) {
        throw std::logic_error(
            "Web fake server is not attached to a running graph");
      }
      sequence = ++server.impl_->sequence;
      server.impl_->ws_sends.push_back(
          FakeWsSend{client_id, connection_id, frame.clone()});
      output = server.impl_->output;
    }
    server.impl_->changed.notify_all();

    Value report = make_delivery_report(connection_id, sequence,
                                        WebDeliveryStatus::Delivered);
    if (!output->send(wd::WebTransportEventKind::ServerWsSendDelivery,
                      "delivery",
                      delivery_envelope(client_id, std::move(report)),
                      wd::index(wd::ServerChannel::WsSendDelivery), 512)) {
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
      lock, timeout, [&] { return static_cast<bool>(impl_->output); });
}

bool FakeWebServer::wait_until_detached(
    std::chrono::milliseconds timeout) const {
  std::unique_lock lock{impl_->mutex};
  return impl_->changed.wait_for(lock, timeout,
                                 [&] { return !impl_->output; });
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
[[nodiscard]] std::shared_ptr<wd::ServerTransportOutput>
attached_server_output(const FakeWebServer &server) {
  return detail::FakeServerAccess::output(server);
}
} // namespace

void FakeWebServer::emit_request(Value route, Value request) {
  require_schema(route, scalar_descriptor<WebRoute>::value_meta(), "route");
  require_schema(request, scalar_descriptor<HttpServerRequest>::value_meta(),
                 "request");
  auto output = attached_server_output(*this);
  const std::size_t retained = server_request_bytes(request);
  if (!output->send(wd::WebTransportEventKind::ServerRequest, "request",
                    request_envelope(std::move(route), std::move(request)),
                    wd::index(wd::ServerChannel::Request), retained)) {
    throw std::overflow_error("Web fake request queue is full");
  }
}

void FakeWebServer::emit_route_state(Value route, WebRouteState state) {
  require_schema(route, scalar_descriptor<WebRoute>::value_meta(), "route");
  auto output = attached_server_output(*this);
  if (!output->send(wd::WebTransportEventKind::ServerRequest, "request",
                    route_state_envelope(std::move(route), state),
                    wd::index(wd::ServerChannel::Request), 256)) {
    throw std::overflow_error("Web fake request queue is full");
  }
}

void FakeWebServer::emit_ws_event(Value route, Value event) {
  require_schema(route, scalar_descriptor<WebRoute>::value_meta(), "route");
  require_schema(event, scalar_descriptor<WsEvent>::value_meta(), "WS event");
  auto output = attached_server_output(*this);
  const std::size_t retained = ws_event_bytes(event);
  if (!output->send(
          wd::WebTransportEventKind::ServerWsIngress, "server_ws",
          ws_ingress_envelope(std::move(route), std::move(event), Value{}),
          wd::index(wd::ServerChannel::WsIngress), retained)) {
    throw std::overflow_error("Web fake WS ingress queue is full");
  }
}

void FakeWebServer::emit_ws_frame(Value route, Value inbound_frame) {
  require_schema(route, scalar_descriptor<WebRoute>::value_meta(), "route");
  require_schema(inbound_frame,
                 scalar_descriptor<WsInboundFrame>::value_meta(),
                 "inbound frame");
  auto output = attached_server_output(*this);
  const std::size_t retained = inbound_frame_bytes(inbound_frame);
  if (!output->send(
          wd::WebTransportEventKind::ServerWsIngress, "server_ws",
          ws_ingress_envelope(std::move(route), Value{},
                              std::move(inbound_frame)),
          wd::index(wd::ServerChannel::WsIngress), retained)) {
    throw std::overflow_error("Web fake WS ingress queue is full");
  }
}

void FakeWebServer::emit_event(Value event, bool stop_graph) {
  require_schema(event, scalar_descriptor<WebEvent>::value_meta(), "event");
  auto output = attached_server_output(*this);
  if (!output->send(wd::WebTransportEventKind::ServerEvent, "event",
                    event_envelope(std::move(event), stop_graph),
                    wd::index(wd::ServerChannel::Event), 512)) {
    throw std::overflow_error("Web fake event queue is full");
  }
}

void FakeWebServer::emit_stats(Value stats) {
  require_schema(stats, scalar_descriptor<WebServerStats>::value_meta(),
                 "stats");
  auto output = attached_server_output(*this);
  if (!output->send(wd::WebTransportEventKind::ServerStats, "server_stats",
                    std::move(stats), wd::index(wd::ServerChannel::Stats),
                    256)) {
    // Fake statistics have the same best-effort, self-superseding contract as
    // the live periodic sample.
    return;
  }
}

struct FakeWebClient::Impl {
  mutable std::mutex mutex{};
  mutable std::condition_variable changed{};
  std::shared_ptr<wd::ClientTransportOutput> output{};
  std::size_t attaches{};
  Int sequence{};
  std::vector<FakeHttpCall> calls{};
  std::vector<Value> ws_keys{};
  std::vector<Value> removed_ws_keys{};
  std::vector<FakeClientWsSend> ws_sends{};
};

struct detail::FakeClientAccess {
  [[nodiscard]] static std::shared_ptr<wd::ClientTransportOutput>
  output(const FakeWebClient &client) {
    std::lock_guard lock{client.impl_->mutex};
    if (!client.impl_->output) {
      throw std::logic_error(
          "Web fake client is not attached to a running graph");
    }
    return client.impl_->output;
  }

  static void attach(FakeWebClient &client,
                     std::shared_ptr<wd::ClientTransportOutput> output) {
    {
      std::lock_guard lock{client.impl_->mutex};
      client.impl_->output = std::move(output);
      ++client.impl_->attaches;
    }
    client.impl_->changed.notify_all();
  }

  static void detach(FakeWebClient &client) noexcept {
    {
      std::lock_guard lock{client.impl_->mutex};
      client.impl_->output.reset();
    }
    client.impl_->changed.notify_all();
  }

  static void call(FakeWebClient &client, Int client_id, Value request,
                   Value options) {
    {
      std::lock_guard lock{client.impl_->mutex};
      if (!client.impl_->output) {
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
    std::shared_ptr<wd::ClientTransportOutput> output;
    Int sequence{};
    {
      std::lock_guard lock{client.impl_->mutex};
      if (!client.impl_->output) {
        throw std::logic_error(
            "Web fake client is not attached to a running graph");
      }
      sequence = ++client.impl_->sequence;
      client.impl_->ws_sends.push_back(
          FakeClientWsSend{client_id, std::move(key), frame.clone()});
      output = client.impl_->output;
    }
    client.impl_->changed.notify_all();

    Value report =
        make_delivery_report(client_id, sequence, WebDeliveryStatus::Delivered);
    if (!output->send(wd::WebTransportEventKind::ClientSendDelivery,
                      "delivery",
                      delivery_envelope(client_id, std::move(report)),
                      wd::index(wd::ClientChannel::SendDelivery), 512)) {
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
      lock, timeout, [&] { return static_cast<bool>(impl_->output); });
}

bool FakeWebClient::wait_until_detached(
    std::chrono::milliseconds timeout) const {
  std::unique_lock lock{impl_->mutex};
  return impl_->changed.wait_for(lock, timeout,
                                 [&] { return !impl_->output; });
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
[[nodiscard]] std::shared_ptr<wd::ClientTransportOutput>
attached_client_output(const FakeWebClient &client) {
  return detail::FakeClientAccess::output(client);
}
} // namespace

void FakeWebClient::respond(Int client_id, Value response) {
  require_schema(response, scalar_descriptor<HttpResponse>::value_meta(),
                 "response");
  auto output = attached_client_output(*this);
  const std::size_t retained = http_response_bytes(response);
  if (!output->send(
          wd::WebTransportEventKind::ClientResponse, "response",
          response_envelope(client_id, std::move(response), Value{}),
          wd::index(wd::ClientChannel::Response), retained)) {
    throw std::overflow_error("Web fake response queue is full");
  }
}

void FakeWebClient::fail(Int client_id, Value transport_error) {
  require_schema(transport_error,
                 scalar_descriptor<WebTransportError>::value_meta(),
                 "transport error");
  auto output = attached_client_output(*this);
  const std::size_t retained = transport_error_bytes(transport_error);
  if (!output->send(
          wd::WebTransportEventKind::ClientResponse, "response",
          response_envelope(client_id, Value{}, std::move(transport_error)),
          wd::index(wd::ClientChannel::Response), retained)) {
    throw std::overflow_error("Web fake response queue is full");
  }
}

void FakeWebClient::emit_ws_event(Value key, Value event) {
  require_schema(key, scalar_descriptor<WsClientKey>::value_meta(), "WS key");
  require_schema(event, scalar_descriptor<WsEvent>::value_meta(), "WS event");
  auto output = attached_client_output(*this);
  const std::size_t retained = ws_event_bytes(event);
  if (!output->send(
          wd::WebTransportEventKind::ClientWsIngress, "client_ws",
          ws_client_envelope(std::move(key), std::move(event), Value{}),
          wd::index(wd::ClientChannel::WsIngress), retained)) {
    throw std::overflow_error("Web fake client WS queue is full");
  }
}

void FakeWebClient::emit_ws_frame(Value key, Value frame) {
  require_schema(key, scalar_descriptor<WsClientKey>::value_meta(), "WS key");
  require_schema(frame, scalar_descriptor<WsFrame>::value_meta(), "frame");
  auto output = attached_client_output(*this);
  const std::size_t retained = ws_frame_bytes(frame.view()) + 256;
  if (!output->send(
          wd::WebTransportEventKind::ClientWsIngress, "client_ws",
          ws_client_envelope(std::move(key), Value{}, std::move(frame)),
          wd::index(wd::ClientChannel::WsIngress), retained)) {
    throw std::overflow_error("Web fake client WS queue is full");
  }
}

void FakeWebClient::emit_event(Value event, bool stop_graph) {
  require_schema(event, scalar_descriptor<WebEvent>::value_meta(), "event");
  auto output = attached_client_output(*this);
  if (!output->send(wd::WebTransportEventKind::ClientEvent, "event",
                    event_envelope(std::move(event), stop_graph),
                    wd::index(wd::ClientChannel::Event), 512)) {
    throw std::overflow_error("Web fake event queue is full");
  }
}

void FakeWebClient::emit_stats(Value stats) {
  require_schema(stats, scalar_descriptor<WebClientStats>::value_meta(),
                 "stats");
  auto output = attached_client_output(*this);
  if (!output->send(wd::WebTransportEventKind::ClientStats, "client_stats",
                    std::move(stats), wd::index(wd::ClientChannel::Stats),
                    256)) {
    return;
  }
}

namespace {
struct FakeServerTransportTag {};

[[nodiscard]] Port<TS<wd::WebTransportEvent>> wire_fake_server_transport(
    Wiring &w, detail::FakeServerHandle server,
    wd::ServerAdmissionHandle admission,
    wd::WebTransportBindingsHandle bindings) {
  return wd::wire_transport_source<FakeServerTransportTag>(
      w, admission.value->max_pending(),
      [server, admission, bindings](PushSourceSender sender, const NodeView &,
                                    DateTime) {
        admission.value->start();
        try {
          detail::FakeServerAccess::attach(
              *server.value,
              std::make_shared<wd::ServerTransportOutput>(
                  std::move(sender), admission, bindings));
        } catch (...) {
          admission.value->stop();
          throw;
        }
      },
      [server, admission](const NodeView &) {
        detail::FakeServerAccess::detach(*server.value);
        admission.value->stop();
      });
}

/** Captures server HTTP route deltas for the socketless task. Cost is O(A + R)
 * per modified tick. */
struct FakeServerHttpRouteSink {
  static constexpr auto name = "web_fake_server_http_routes";

  static void eval(
      In<"routes", TSS<WebRoute>, InputValidity::Unchecked> routes,
      Scalar<"server", detail::FakeServerHandle> server) {
    if (!routes.modified()) {
      return;
    }
    const auto &erased = static_cast<const TSSInputView &>(routes);
    for (const auto route : erased.removed()) {
      detail::FakeServerAccess::http_route_removed(*server.value().value,
                                                   route.clone());
    }
    for (const auto route : erased.added()) {
      detail::FakeServerAccess::http_route_added(*server.value().value,
                                                 route.clone());
    }
  }
};

/** Captures server WebSocket route deltas. Cost is O(A + R) per modified
 * tick. */
struct FakeServerWsRouteSink {
  static constexpr auto name = "web_fake_server_ws_routes";

  static void eval(
      In<"routes", TSS<WebRoute>, InputValidity::Unchecked> routes,
      Scalar<"server", detail::FakeServerHandle> server) {
    if (!routes.modified()) {
      return;
    }
    const auto &erased = static_cast<const TSSInputView &>(routes);
    for (const auto route : erased.removed()) {
      detail::FakeServerAccess::ws_route_removed(*server.value().value,
                                                 route.clone());
    }
    for (const auto route : erased.added()) {
      detail::FakeServerAccess::ws_route_added(*server.value().value,
                                               route.clone());
    }
  }
};

/** Captures graph HTTP responses and emits fake delivery. Cost is O(M) for M
 * modified responses. */
struct FakeServerRespondSink {
  static constexpr auto name = "web_fake_server_respond";

  static void eval(
      In<"responses", TSD<Int, HttpRespondRequest>, InputValidity::Unchecked>
          responses,
      Scalar<"server", detail::FakeServerHandle> server) {
    if (!responses.modified()) {
      return;
    }
    for (const auto &[client_id, request] : responses.modified_items()) {
      auto response = request.template field<"response">();
      auto request_id = request.template field<"request_id">();
      if (!response.modified() || !response.valid()) {
        continue;
      }
      if (!request_id.valid()) {
        throw std::invalid_argument("Web respond requires a valid request id");
      }
      detail::FakeServerAccess::respond(
          *server.value().value, client_id.template checked_as<Int>(),
          request_id.value(), response.base().value().clone());
    }
  }
};

/** Captures graph WebSocket sends and emits fake delivery. Cost is O(M) for M
 * modified sends. */
struct FakeServerWsSendSink {
  static constexpr auto name = "web_fake_server_ws_send";

  static void eval(
      In<"sends", TSD<Int, WsSendRequest>, InputValidity::Unchecked> sends,
      Scalar<"server", detail::FakeServerHandle> server) {
    if (!sends.modified()) {
      return;
    }
    for (const auto &[client_id, request] : sends.modified_items()) {
      auto frame = request.template field<"frame">();
      auto connection_id = request.template field<"connection_id">();
      if (!frame.modified() || !frame.valid()) {
        continue;
      }
      if (!connection_id.valid()) {
        throw std::invalid_argument("Web WS send requires a valid connection id");
      }
      detail::FakeServerAccess::ws_send(
          *server.value().value, client_id.template checked_as<Int>(),
          connection_id.value(), frame.base().value().clone());
    }
  }
};

struct FakeWebServerImpl {
  static constexpr auto name = "web_fake_server_impl";

  static void compose(Wiring &w, Scalar<"config", Value> config,
                      Scalar<"server", detail::FakeServerHandle> server,
                      Scalar<"path", Str> path) {
    if (!w.is_realtime()) {
      throw std::invalid_argument(
          "the fake web server requires a real-time graph");
    }
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

    auto admission = wd::make_server_admission(config.value());
    auto transport_bindings = wd::make_transport_bindings();
    auto transport = wire_fake_server_transport(
        w, server.value(), admission, transport_bindings);
    static_cast<void>(
        wire<FakeServerHttpRouteSink>(w, http_routes, server.value()));
    static_cast<void>(
        wire<FakeServerWsRouteSink>(w, ws_routes, server.value()));
    static_cast<void>(
        wire<FakeServerRespondSink>(w, responses, server.value()));
    static_cast<void>(
        wire<FakeServerWsSendSink>(w, ws_sends, server.value()));
    auto outputs = wd::wire_server_outputs(
        w, transport, http_routes, ws_routes, admission, transport_bindings);

    service::impl_output<HttpServeService>(w, binding, outputs.requests);
    service::impl_output<WsServeService>(w, binding, outputs.ws);
    service::impl_output<HttpRespondService>(w, binding,
                                             outputs.respond_reports);
    service::impl_output<WsSendService>(w, binding, outputs.ws_send_reports);
    service::impl_output<WebServerEventService>(w, binding, outputs.events);
    service::impl_output<WebServerStatsService>(w, binding, outputs.stats);
  }
};

struct FakeClientTransportTag {};

[[nodiscard]] Port<TS<wd::WebTransportEvent>> wire_fake_client_transport(
    Wiring &w, detail::FakeClientHandle client,
    wd::ClientAdmissionHandle admission,
    wd::WebTransportBindingsHandle bindings) {
  return wd::wire_transport_source<FakeClientTransportTag>(
      w, admission.value->max_pending(),
      [client, admission, bindings](PushSourceSender sender, const NodeView &,
                                    DateTime) {
        admission.value->start();
        try {
          detail::FakeClientAccess::attach(
              *client.value,
              std::make_shared<wd::ClientTransportOutput>(
                  std::move(sender), admission, bindings));
        } catch (...) {
          admission.value->stop();
          throw;
        }
      },
      [client, admission](const NodeView &) {
        detail::FakeClientAccess::detach(*client.value);
        admission.value->stop();
      });
}

/** Captures graph HTTP client calls. Cost is O(M) for M modified calls. */
struct FakeClientCallSink {
  static constexpr auto name = "web_fake_client_call";

  static void eval(
      In<"calls", TSD<Int, HttpClientCall>, InputValidity::Unchecked> calls,
      Scalar<"client", detail::FakeClientHandle> client) {
    if (!calls.modified()) {
      return;
    }
    for (const auto &[client_id, call] : calls.modified_items()) {
      auto request = call.template field<"request">();
      auto options = call.template field<"options">();
      if (!request.modified() || !request.valid()) {
        continue;
      }
      detail::FakeClientAccess::call(
          *client.value().value, client_id.template checked_as<Int>(),
          request.base().value().clone(),
          options.valid() ? options.base().value().clone() : Value{});
    }
  }
};

/** Captures client connection-key deltas. Cost is O(A + R) per modified
 * tick. */
struct FakeClientWsKeySink {
  static constexpr auto name = "web_fake_client_ws_keys";

  static void eval(
      In<"keys", TSS<WsClientKey>, InputValidity::Unchecked> keys,
      Scalar<"client", detail::FakeClientHandle> client) {
    if (!keys.modified()) {
      return;
    }
    const auto &erased = static_cast<const TSSInputView &>(keys);
    for (const auto key : erased.removed()) {
      detail::FakeClientAccess::ws_key_removed(*client.value().value,
                                               key.clone());
    }
    for (const auto key : erased.added()) {
      detail::FakeClientAccess::ws_key_added(*client.value().value,
                                             key.clone());
    }
  }
};

/** Captures graph WebSocket client sends. Cost is O(M) for M modified sends. */
struct FakeClientWsSendSink {
  static constexpr auto name = "web_fake_client_ws_send";

  static void eval(
      In<"sends", TSD<Int, WsClientSendRequest>, InputValidity::Unchecked>
          sends,
      Scalar<"client", detail::FakeClientHandle> client) {
    if (!sends.modified()) {
      return;
    }
    for (const auto &[client_id, request] : sends.modified_items()) {
      auto frame = request.template field<"frame">();
      auto key = request.template field<"key">();
      if (!frame.modified() || !frame.valid()) {
        continue;
      }
      if (!key.valid()) {
        throw std::invalid_argument("Web WS client send requires a valid key");
      }
      detail::FakeClientAccess::ws_send(
          *client.value().value, client_id.template checked_as<Int>(),
          key.base().value().clone(), frame.base().value().clone());
    }
  }
};

struct FakeWebClientImpl {
  static constexpr auto name = "web_fake_client_impl";

  static void compose(Wiring &w, Scalar<"config", Value> config,
                      Scalar<"client", detail::FakeClientHandle> client,
                      Scalar<"path", Str> path) {
    if (!w.is_realtime()) {
      throw std::invalid_argument(
          "the fake web client requires a real-time graph");
    }
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

    auto admission = wd::make_client_admission(config.value());
    auto transport_bindings = wd::make_transport_bindings();
    auto transport = wire_fake_client_transport(
        w, client.value(), admission, transport_bindings);
    static_cast<void>(wire<FakeClientCallSink>(w, calls, client.value()));
    static_cast<void>(wire<FakeClientWsKeySink>(w, ws_keys, client.value()));
    static_cast<void>(
        wire<FakeClientWsSendSink>(w, ws_sends, client.value()));
    auto outputs = wd::wire_client_outputs(
        w, transport, ws_keys, admission, transport_bindings);

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
