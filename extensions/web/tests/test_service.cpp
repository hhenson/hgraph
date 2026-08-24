// Service-boundary tests over the socketless fake transport. The fake uses
// the same standard channel push sources, graph projections, and graph sinks
// as the live adaptors; only the external task is replaced (RFC 0024/0027).

#include <hgraph/web/service.h>
#include <hgraph/web/testing/fake_transport.h>
#include <hgraph/web/value_builders.h>

#include <hgraph/lib/std/operators/conversion.h>
#include <hgraph/lib/std/operators/registration.h>
#include <hgraph/lib/std/std_nodes.h>
#include <hgraph/lib/testing/runtime_support.h>
#include <hgraph/runtime/push_source_node.h>
#include <hgraph/runtime/runtime.h>
#include <hgraph/util/scope.h>

#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <iostream>
#include <mutex>
#include <optional>
#include <span>
#include <stdexcept>
#include <string>
#include <typeindex>
#include <utility>
#include <vector>

namespace {
using namespace hgraph;
using namespace hgraph::web;
using namespace hgraph::web::testing;
using namespace hgraph::testing;
using namespace std::chrono_literals;

void require(bool condition, std::string message) {
  if (!condition) {
    throw std::runtime_error(std::move(message));
  }
}

template <typename Fn> void require_failure(Fn &&fn, std::string message) {
  bool rejected = false;
  try {
    std::forward<Fn>(fn)();
  } catch (const std::exception &) {
    rejected = true;
  }
  require(rejected, std::move(message));
}

[[nodiscard]] GraphExecutorValue start_realtime(GraphBuilder builder,
                                                TimeDelta duration = TimeDelta{
                                                    5'000'000}) {
  const DateTime start = wall_now();
  GraphExecutorBuilder executor_builder;
  executor_builder.graph_builder(std::move(builder))
      .mode(GraphExecutorMode::RealTime)
      .start_time(start)
      .end_time(start + duration);
  return executor_builder.make_executor();
}

[[nodiscard]] Int bundle_int(const Value &value, std::string_view field) {
  return value.view().as_bundle().at(field).checked_as<Int>();
}

inline FakeWebServerPtr serve_server{};
inline FakeWebServerPtr respond_server{};
inline FakeWebServerPtr ws_server{};
inline FakeWebServerPtr backlog_server{};
inline FakeWebServerPtr lifecycle_server{};
inline FakeWebClientPtr call_client{};
inline FakeWebClientPtr failure_client{};
inline FakeWebClientPtr ws_client{};

inline Value server_configuration{};
inline Value client_configuration{};
inline Value serve_route{};
inline Value alternate_route{};
inline Value ws_route{};
inline Value alternate_ws_route{};
inline Value client_ws_key{};
inline Value alternate_client_ws_key{};
inline Value emitted_request{};
inline Value respond_response{};
inline Value client_request{};

inline Value observed_request{};
inline std::size_t observed_request_count{};
inline bool observed_serving_state{};
inline Value observed_report{};
inline std::size_t observed_report_count{};
inline Value observed_response{};
inline std::size_t observed_response_count{};
inline Value observed_failure{};
inline std::size_t observed_failure_count{};
inline Value observed_ws_event{};
inline Value observed_ws_frame{};
inline Value observed_ws_report{};
inline Value observed_client_ws_frame{};
inline Value observed_client_ws_report{};
inline std::size_t backlog_request_count{};
inline std::vector<Int> backlog_request_ids{};
inline std::vector<std::pair<Str, DateTime>> observed_transport_progress{};
inline std::vector<Int> post_readd_request_ids{};
inline std::vector<Str> post_readd_ws_frames{};
inline std::vector<Str> post_readd_client_ws_frames{};

class SenderLatch {
public:
  void reset() {
    std::lock_guard lock{mutex_};
    sender_.reset();
  }

  void publish(PushSourceSender sender) {
    {
      std::lock_guard lock{mutex_};
      sender_ = std::move(sender);
    }
    changed_.notify_all();
  }

  [[nodiscard]] std::optional<PushSourceSender>
  await(std::chrono::milliseconds timeout) {
    std::unique_lock lock{mutex_};
    if (!changed_.wait_for(lock, timeout,
                           [&] { return sender_.has_value(); })) {
      return std::nullopt;
    }
    return sender_;
  }

private:
  std::mutex mutex_{};
  std::condition_variable changed_{};
  std::optional<PushSourceSender> sender_{};
};

class EvaluationGate {
public:
  void reset() {
    std::lock_guard lock{mutex_};
    blocked_ = false;
    released_ = false;
  }

  void block() {
    std::unique_lock lock{mutex_};
    blocked_ = true;
    changed_.notify_all();
    changed_.wait(lock, [&] { return released_; });
  }

  [[nodiscard]] bool await_blocked(std::chrono::milliseconds timeout) {
    std::unique_lock lock{mutex_};
    return changed_.wait_for(lock, timeout, [&] { return blocked_; });
  }

  void release() {
    {
      std::lock_guard lock{mutex_};
      released_ = true;
    }
    changed_.notify_all();
  }

private:
  std::mutex mutex_{};
  std::condition_variable changed_{};
  bool blocked_{};
  bool released_{};
};

inline SenderLatch route_sender{};
inline EvaluationGate route_generation_gate{};
inline EvaluationGate transport_channel_gate{};
inline std::size_t route_value_count{};

void release_test_state() {
  serve_server.reset();
  respond_server.reset();
  ws_server.reset();
  backlog_server.reset();
  lifecycle_server.reset();
  call_client.reset();
  failure_client.reset();
  ws_client.reset();
  server_configuration = Value{};
  client_configuration = Value{};
  serve_route = Value{};
  alternate_route = Value{};
  ws_route = Value{};
  alternate_ws_route = Value{};
  client_ws_key = Value{};
  alternate_client_ws_key = Value{};
  emitted_request = Value{};
  respond_response = Value{};
  client_request = Value{};
  observed_request = Value{};
  observed_report = Value{};
  observed_response = Value{};
  observed_failure = Value{};
  observed_ws_event = Value{};
  observed_ws_frame = Value{};
  observed_ws_report = Value{};
  observed_client_ws_frame = Value{};
  observed_client_ws_report = Value{};
  observed_transport_progress.clear();
  backlog_request_ids.clear();
  post_readd_request_ids.clear();
  post_readd_ws_frames.clear();
  post_readd_client_ws_frames.clear();
  route_sender.reset();
  route_generation_gate.release();
  transport_channel_gate.release();
}

[[nodiscard]] Value make_server_request(Int request_id) {
  BundleBuilder builder{ValuePlanFactory::instance().type_for(
      scalar_descriptor<HttpServerRequest>::value_meta())};
  builder.set("request_id", Value{request_id});
  builder.set("connection_id", Value{Int{1}});
  builder.set("stream_id", Value{Int{0}});
  builder.set("request",
              make_request(HttpMethod::Get, "/orders/7?verbose=1&verbose=2",
                           "/orders/7", {{"verbose", "1"}, {"verbose", "2"}},
                           {{"id", "7"}},
                           {{"Accept", "application/json"}, {"accept", "*/*"}},
                           Bytes{}));
  return builder.build();
}

[[nodiscard]] Value make_inbound_frame(Str text) {
  BundleBuilder inbound{ValuePlanFactory::instance().type_for(
      scalar_descriptor<WsInboundFrame>::value_meta())};
  inbound.set("connection_id", Value{Int{5}});
  inbound.set("frame", make_text_frame(std::move(text)));
  return inbound.build();
}

void initialize_values() {
  register_web_types();
  server_configuration = server_config().build();
  client_configuration = client_config().build();
  serve_route = make_route(HttpMethod::Get, "/orders/{id}");
  alternate_route = make_route(HttpMethod::Get, "/alternate");
  ws_route = make_route(HttpMethod::Get, "/live", true);
  alternate_ws_route = make_route(HttpMethod::Get, "/alternate-live", true);
  client_ws_key = make_ws_client_key("wss://feed/live");
  alternate_client_ws_key = make_ws_client_key("wss://feed/alternate");
  emitted_request = make_server_request(Int{41});
  respond_response =
      make_response(Int{201}, {{"Content-Type", "application/json"}},
                    Bytes{"{\"ok\":true}"});
  client_request = make_client_request(HttpMethod::Post, "https://api/orders",
                                       {{"Content-Type", "application/json"}},
                                       Bytes{"{}"});
}

struct ServeCapture {
  static constexpr auto name = "web_serve_capture";

  static void eval(NodeView node,
                   In<"routed", WebRouteOutput, InputValidity::Unchecked>
                       routed) {
    auto state = routed.template field<"state">();
    if (state.valid() && state.modified() &&
        state.value() == WebRouteState::Serving) {
      observed_serving_state = true;
    }
    auto request = routed.template field<"request">();
    if (!request.valid() || !request.modified()) {
      return;
    }
    observed_request = request.base().value().clone();
    ++observed_request_count;
    node.graph().executor().request_stop();
  }
};

struct ServeGraph {
  static constexpr auto name = "web_serve_test_graph";

  static void compose(Wiring &w) {
    const auto path = service::path("web-serve");
    register_fake_server(w, path, server_configuration.clone(), serve_server);
    auto route =
        wire<stdlib::const_, TS<WebRoute>>(w, serve_route.clone());
    static_cast<void>(wire<ServeCapture>(w, serve(w, path, route)));
  }
};

struct ReportCapture {
  static constexpr auto name = "web_report_capture";

  static void eval(NodeView node,
                   In<"report", TS<WebDeliveryReport>,
                      InputValidity::Unchecked>
                       report) {
    if (!report.valid() || !report.modified()) {
      return;
    }
    observed_report = report.base().value().clone();
    ++observed_report_count;
    node.graph().executor().request_stop();
  }
};

struct RespondGraph {
  static constexpr auto name = "web_respond_test_graph";

  static void compose(Wiring &w) {
    const auto path = service::path("web-respond");
    register_fake_server(w, path, server_configuration.clone(),
                         respond_server);
    auto request_id = wire<stdlib::const_, TS<Int>>(w, Int{41});
    auto response =
        wire<stdlib::const_, TS<HttpResponse>>(w, respond_response.clone());
    auto reports =
        respond(w, path, respond_request(w, request_id, response));
    static_cast<void>(wire<ReportCapture>(w, reports));
  }
};

struct CallCapture {
  static constexpr auto name = "web_call_capture";

  static void eval(NodeView node,
                   In<"result", HttpCallResult, InputValidity::Unchecked>
                       result) {
    auto response = result.template field<"response">();
    if (response.valid() && response.modified()) {
      observed_response = response.base().value().clone();
      ++observed_response_count;
      node.graph().executor().request_stop();
    }
    auto failure = result.template field<"failure">();
    if (failure.valid() && failure.modified()) {
      observed_failure = failure.base().value().clone();
      ++observed_failure_count;
      node.graph().executor().request_stop();
    }
  }
};

struct CallGraph {
  static constexpr auto name = "web_call_test_graph";

  static void compose(Wiring &w) {
    const auto path = service::path("web-call");
    register_fake_client(w, path, client_configuration.clone(), call_client);
    auto request =
        wire<stdlib::const_, TS<HttpClientRequest>>(w, client_request.clone());
    static_cast<void>(
        wire<CallCapture>(w, http_request(w, path, http_client_call(w, request))));
  }
};

struct FailureGraph {
  static constexpr auto name = "web_failure_test_graph";

  static void compose(Wiring &w) {
    const auto path = service::path("web-failure");
    register_fake_client(w, path, client_configuration.clone(),
                         failure_client);
    auto request =
        wire<stdlib::const_, TS<HttpClientRequest>>(w, client_request.clone());
    static_cast<void>(
        wire<CallCapture>(w, http_request(w, path, http_client_call(w, request))));
  }
};

struct WsCapture {
  static constexpr auto name = "web_ws_capture";

  static void eval(NodeView node,
                   In<"routed", WsRouteOutput, InputValidity::Unchecked>
                       routed) {
    auto event = routed.template field<"event">();
    if (event.valid() && event.modified()) {
      observed_ws_event = event.base().value().clone();
    }
    auto frame = routed.template field<"frame">();
    if (frame.valid() && frame.modified()) {
      observed_ws_frame = frame.base().value().clone();
      node.graph().executor().request_stop();
    }
  }
};

struct WsReportCapture {
  static constexpr auto name = "web_ws_report_capture";

  static void eval(In<"report", TS<WebDeliveryReport>,
                      InputValidity::Unchecked>
                       report) {
    if (report.valid() && report.modified()) {
      observed_ws_report = report.base().value().clone();
    }
  }
};

struct WsGraph {
  static constexpr auto name = "web_ws_test_graph";

  static void compose(Wiring &w) {
    const auto path = service::path("web-ws");
    register_fake_server(w, path, server_configuration.clone(), ws_server);
    auto route = wire<stdlib::const_, TS<WebRoute>>(w, ws_route.clone());
    static_cast<void>(wire<WsCapture>(w, ws_serve(w, path, route)));
    auto connection_id = wire<stdlib::const_, TS<Int>>(w, Int{5});
    auto frame =
        wire<stdlib::const_, TS<WsFrame>>(w, make_text_frame("hello"));
    auto reports =
        ws_send(w, path, ws_send_request(w, connection_id, frame));
    static_cast<void>(wire<WsReportCapture>(w, reports));
  }
};

struct ClientWsCapture {
  static constexpr auto name = "web_client_ws_capture";

  static void eval(NodeView node,
                   In<"output", WsClientOutput, InputValidity::Unchecked>
                       output) {
    auto frame = output.template field<"frame">();
    if (frame.valid() && frame.modified()) {
      observed_client_ws_frame = frame.base().value().clone();
      node.graph().executor().request_stop();
    }
  }
};

struct ClientWsReportCapture {
  static constexpr auto name = "web_client_ws_report_capture";

  static void eval(In<"report", TS<WebDeliveryReport>,
                      InputValidity::Unchecked>
                       report) {
    if (report.valid() && report.modified()) {
      observed_client_ws_report = report.base().value().clone();
    }
  }
};

struct ClientWsGraph {
  static constexpr auto name = "web_client_ws_test_graph";

  static void compose(Wiring &w) {
    const auto path = service::path("web-client-ws");
    register_fake_client(w, path, client_configuration.clone(), ws_client);
    auto key = wire<stdlib::const_, TS<WsClientKey>>(
        w, make_ws_client_key("wss://feed/live"));
    static_cast<void>(wire<ClientWsCapture>(w, ws_connect(w, path, key)));
    auto frame =
        wire<stdlib::const_, TS<WsFrame>>(w, make_text_frame("subscribe"));
    auto reports = ws_client_send(w, path, ws_client_send_request(w, key, frame));
    static_cast<void>(wire<ClientWsReportCapture>(w, reports));
  }
};

struct BacklogCapture {
  static constexpr auto name = "web_backlog_capture";

  static void eval(NodeView node,
                   In<"routed", WebRouteOutput, InputValidity::Unchecked>
                       routed) {
    auto request = routed.template field<"request">();
    if (!request.valid() || !request.modified()) {
      return;
    }
    ++backlog_request_count;
    backlog_request_ids.push_back(
        request.base().value().as_bundle().at("request_id").checked_as<Int>());
    if (backlog_request_count == 3) {
      node.graph().executor().request_stop();
    }
  }
};

struct BacklogGraph {
  static constexpr auto name = "web_backlog_test_graph";

  static void compose(Wiring &w) {
    const auto path = service::path("web-backlog");
    register_fake_server(w, path, server_configuration.clone(),
                         backlog_server);
    auto route =
        wire<stdlib::const_, TS<WebRoute>>(w, serve_route.clone());
    static_cast<void>(wire<BacklogCapture>(w, serve(w, path, route)));
  }
};

struct RouteGenerationSourceTag {};

struct RouteGenerationForward {
  static constexpr auto name = "web_route_generation_forward";

  static void eval(In<"route", TS<WebRoute>> route,
                   Out<TS<WebRoute>> out) {
    ++route_value_count;
    out.apply(route.base().value());
  }
};

struct RouteGenerationCapture {
  static constexpr auto name = "web_route_generation_capture";

  static void eval(
      NodeView node,
      In<"routed", WebRouteOutput, InputValidity::Unchecked> routed) {
    auto request = routed.template field<"request">();
    if (!request.valid() || !request.modified()) {
      return;
    }
    const Int request_id =
        request.base().value().as_bundle().at("request_id").checked_as<Int>();
    if (request_id == Int{100}) {
      route_generation_gate.block();
      return;
    }
    if (route_value_count >= 3) {
      post_readd_request_ids.push_back(request_id);
      if (request_id == Int{999}) {
        node.graph().executor().request_stop();
      }
    }
  }
};

struct RouteGenerationGraph {
  static constexpr auto name = "web_route_generation_test_graph";

  static void compose(Wiring &w) {
    const auto path = service::path("web-route-generation");
    register_fake_server(w, path, server_configuration.clone(),
                         lifecycle_server);
    const auto *route_schema = ts_type<TS<WebRoute>>();
    auto route = Port<TS<WebRoute>>{
        w, w.add_unique_node(
               std::type_index(typeid(RouteGenerationSourceTag)),
               make_push_source_node(
                   *route_schema,
                   make_push_source_queue_policy(*route_schema, 8),
                   [](PushSourceSender sender) {
                     route_sender.publish(std::move(sender));
                   }),
               std::span<const WiringPortRef>{}, Value{})};
    auto forwarded = wire<RouteGenerationForward>(w, route);
    static_cast<void>(wire<RouteGenerationCapture>(
        w, serve(w, path, forwarded.template as<TS<WebRoute>>())));
  }
};

struct WsRouteGenerationSourceTag {};

struct WsRouteGenerationCapture {
  static constexpr auto name = "web_ws_route_generation_capture";

  static void eval(
      NodeView node,
      In<"routed", WsRouteOutput, InputValidity::Unchecked> routed) {
    auto inbound = routed.template field<"frame">();
    if (!inbound.valid() || !inbound.modified()) {
      return;
    }
    const Str text = inbound.base()
                         .value()
                         .as_bundle()
                         .at("frame")
                         .as_bundle()
                         .at("text")
                         .checked_as<Str>();
    if (text == Str{"block"}) {
      route_generation_gate.block();
      return;
    }
    if (route_value_count >= 3) {
      post_readd_ws_frames.push_back(text);
      if (text == Str{"new"}) {
        node.graph().executor().request_stop();
      }
    }
  }
};

struct WsRouteGenerationGraph {
  static constexpr auto name = "web_ws_route_generation_test_graph";

  static void compose(Wiring &w) {
    const auto path = service::path("web-ws-route-generation");
    register_fake_server(w, path, server_configuration.clone(),
                         lifecycle_server);
    const auto *route_schema = ts_type<TS<WebRoute>>();
    auto route = Port<TS<WebRoute>>{
        w, w.add_unique_node(
               std::type_index(typeid(WsRouteGenerationSourceTag)),
               make_push_source_node(
                   *route_schema,
                   make_push_source_queue_policy(*route_schema, 8),
                   [](PushSourceSender sender) {
                     route_sender.publish(std::move(sender));
                   }),
               std::span<const WiringPortRef>{}, Value{})};
    auto forwarded = wire<RouteGenerationForward>(w, route);
    static_cast<void>(wire<WsRouteGenerationCapture>(
        w, ws_serve(w, path, forwarded.template as<TS<WebRoute>>())));
  }
};

struct ClientKeyGenerationSourceTag {};

struct ClientKeyGenerationForward {
  static constexpr auto name = "web_client_key_generation_forward";

  static void eval(In<"key", TS<WsClientKey>> key,
                   Out<TS<WsClientKey>> out) {
    ++route_value_count;
    out.apply(key.base().value());
  }
};

struct ClientKeyGenerationCapture {
  static constexpr auto name = "web_client_key_generation_capture";

  static void eval(
      NodeView node,
      In<"output", WsClientOutput, InputValidity::Unchecked> output) {
    auto frame = output.template field<"frame">();
    if (!frame.valid() || !frame.modified()) {
      return;
    }
    const Str text = frame.base()
                         .value()
                         .as_bundle()
                         .at("text")
                         .checked_as<Str>();
    if (text == Str{"block"}) {
      route_generation_gate.block();
      return;
    }
    if (route_value_count >= 3) {
      post_readd_client_ws_frames.push_back(text);
      if (text == Str{"new"}) {
        node.graph().executor().request_stop();
      }
    }
  }
};

struct ClientKeyGenerationGraph {
  static constexpr auto name = "web_client_key_generation_test_graph";

  static void compose(Wiring &w) {
    const auto path = service::path("web-client-key-generation");
    register_fake_client(w, path, client_configuration.clone(), ws_client);
    const auto *key_schema = ts_type<TS<WsClientKey>>();
    auto key = Port<TS<WsClientKey>>{
        w, w.add_unique_node(
               std::type_index(typeid(ClientKeyGenerationSourceTag)),
               make_push_source_node(
                   *key_schema, make_push_source_queue_policy(*key_schema, 8),
                   [](PushSourceSender sender) {
                     route_sender.publish(std::move(sender));
                   }),
               std::span<const WiringPortRef>{}, Value{})};
    auto forwarded = wire<ClientKeyGenerationForward>(w, key);
    static_cast<void>(wire<ClientKeyGenerationCapture>(
        w, ws_connect(w, path, forwarded.template as<TS<WsClientKey>>())));
  }
};

void capture_transport_progress(NodeView &node, Str kind,
                                DateTime evaluation_time) {
  observed_transport_progress.emplace_back(std::move(kind), evaluation_time);
  if (observed_transport_progress.size() == 3) {
    node.graph().executor().request_stop();
  }
}

struct IndependentEventCapture {
  static constexpr auto name = "web_independent_event_capture";

  static void eval(NodeView node, DateTime evaluation_time,
                   In<"event", TS<WebEvent>, InputValidity::Unchecked> event) {
    if (event.valid() && event.modified()) {
      capture_transport_progress(node, Str{"event"}, evaluation_time);
    }
  }
};

struct IndependentRequestCapture {
  static constexpr auto name = "web_independent_request_capture";

  static void eval(
      NodeView node, DateTime evaluation_time,
      In<"routed", WebRouteOutput, InputValidity::Unchecked> routed) {
    auto request = routed.template field<"request">();
    if (!request.valid() || !request.modified()) {
      return;
    }
    const Int request_id =
        request.base().value().as_bundle().at("request_id").checked_as<Int>();
    if (request_id == Int{70}) {
      transport_channel_gate.block();
    } else if (request_id == Int{71}) {
      capture_transport_progress(node, Str{"request"}, evaluation_time);
    }
  }
};

struct IndependentWsCapture {
  static constexpr auto name = "web_independent_ws_capture";

  static void eval(NodeView node, DateTime evaluation_time,
                   In<"routed", WsRouteOutput, InputValidity::Unchecked>
                       routed) {
    auto event = routed.template field<"event">();
    if (event.valid() && event.modified()) {
      capture_transport_progress(node, Str{"ws"}, evaluation_time);
    }
  }
};

struct IndependentTransportGraph {
  static constexpr auto name = "web_independent_transport_test_graph";

  static void compose(Wiring &w) {
    const auto path = service::path("web-independent-transport");
    register_fake_server(w, path, server_configuration.clone(),
                         lifecycle_server);
    auto http_route =
        wire<stdlib::const_, TS<WebRoute>>(w, serve_route.clone());
    auto websocket_route =
        wire<stdlib::const_, TS<WebRoute>>(w, ws_route.clone());
    static_cast<void>(
        wire<IndependentRequestCapture>(w, serve(w, path, http_route)));
    static_cast<void>(
        wire<IndependentWsCapture>(w, ws_serve(w, path, websocket_route)));
    static_cast<void>(
        wire<IndependentEventCapture>(w, server_events(w, path)));
  }
};

struct DuplicateRegistrationGraph {
  static constexpr auto name = "web_duplicate_registration_graph";

  static void compose(Wiring &w) {
    const auto path = service::path("web-duplicate");
    register_fake_server(w, path, server_configuration.clone(), serve_server);
    register_fake_server(w, path, server_configuration.clone(), serve_server);
    auto route =
        wire<stdlib::const_, TS<WebRoute>>(w, serve_route.clone());
    static_cast<void>(wire<ServeCapture>(w, serve(w, path, route)));
  }
};

template <typename G> GraphBuilder build_realtime() {
  return build_graph<G>(WiringOptions{.is_realtime = true});
}

[[nodiscard]] std::size_t count_nodes(const GraphBuilder &graph,
                                      NodeKind kind) {
  std::size_t count = 0;
  for (const auto &node : graph.nodes()) {
    const auto *schema = node.type().schema();
    if (schema != nullptr && schema->node_kind == kind) {
      ++count;
    }
  }
  return count;
}

[[nodiscard]] bool has_node(const GraphBuilder &graph,
                            std::string_view name) {
  for (const auto &node : graph.nodes()) {
    const auto *schema = node.type().schema();
    if (schema != nullptr && schema->display_name != nullptr &&
        std::string_view{schema->display_name} == name) {
      return true;
    }
  }
  return false;
}

void test_runtime_specific_wiring_shapes() {
  serve_server = std::make_shared<FakeWebServer>();
  const auto server = build_realtime<ServeGraph>();
  require(count_nodes(server, NodeKind::PushSource) == 6,
          "web server did not wire one standard push source per channel");
  for (const auto name :
       {std::string_view{"web_fake_server_http_routes"},
        std::string_view{"web_fake_server_respond"},
        std::string_view{"web_server_request_projection"},
        std::string_view{"web_transport_admission_release"}}) {
    require(has_node(server, name),
            "web server wiring is missing graph node " + Str{name});
  }

  ws_client = std::make_shared<FakeWebClient>();
  const auto client = build_realtime<ClientWsGraph>();
  require(count_nodes(client, NodeKind::PushSource) == 5,
          "web client did not wire one standard push source per channel");
  for (const auto name :
       {std::string_view{"web_fake_client_ws_keys"},
        std::string_view{"web_fake_client_ws_send"},
        std::string_view{"web_client_ws_projection"},
        std::string_view{"web_transport_admission_release"}}) {
    require(has_node(client, name),
            "web client wiring is missing graph node " + Str{name});
  }

  for (const auto retired :
       {std::string_view{"web_request_drain"},
        std::string_view{"web_ws_ingress_drain"},
        std::string_view{"web_response_drain"},
        std::string_view{"web_client_ws_drain"}}) {
    require(!has_node(server, retired) && !has_node(client, retired),
            "web wiring retained private bridge drain " + Str{retired});
  }
}

void test_simulation_rejects_live_push_adaptors() {
  serve_server = std::make_shared<FakeWebServer>();
  require_failure(
      [] { static_cast<void>(build_graph<ServeGraph>()); },
      "simulation wiring accepted a push-based web server");

  ws_client = std::make_shared<FakeWebClient>();
  require_failure(
      [] { static_cast<void>(build_graph<ClientWsGraph>()); },
      "simulation wiring accepted a push-based web client");
}

void test_serve_boundary() {
  serve_server = std::make_shared<FakeWebServer>();
  observed_request = Value{};
  observed_request_count = 0;
  observed_serving_state = false;

  auto executor = start_realtime(build_realtime<ServeGraph>());
  auto view = executor.view();
  AsyncGraphExecutorRun runner{view};

  require(serve_server->wait_until_attached(2s),
          "web server service did not attach");
  require(serve_server->wait_for_http_routes(1, 2s),
          "route did not reach the transport sink");
  const auto routes = serve_server->http_routes();
  require(routes.size() == 1, "unexpected route count");
  require(routes.front()
                  .view()
                  .as_bundle()
                  .at("pattern")
                  .checked_as<Str>() == Str{"/orders/{id}"},
          "route pattern was not preserved");
  serve_server->emit_request(serve_route.clone(), emitted_request.clone());
  runner.join();

  require(observed_request_count == 1,
          "request output did not tick exactly once");
  require(observed_serving_state,
          "route activation did not tick a Serving state");
  require(bundle_int(observed_request, "request_id") == Int{41},
          "request id was not preserved");
  const auto request =
      observed_request.view().as_bundle().at("request").as_bundle();
  require(request.at("path").checked_as<Str>() == Str{"/orders/7"},
          "request path was not preserved");
  require(request.at("query").as_list().size() == 2,
          "multi-value query parameters were collapsed");
  require(request.at("headers").as_list().size() == 2,
          "duplicate headers were collapsed");
  require(serve_server->attach_count() == 1,
          "service materialized more than once");
  require(serve_server->wait_until_detached(2s),
          "web server service did not detach");
}

void test_respond_boundary() {
  respond_server = std::make_shared<FakeWebServer>();
  observed_report = Value{};
  observed_report_count = 0;

  auto executor = start_realtime(build_realtime<RespondGraph>());
  auto view = executor.view();
  AsyncGraphExecutorRun runner{view};

  require(respond_server->wait_for_responses(1, 2s),
          "response did not reach the transport sink");
  runner.join();

  const auto responses = respond_server->responses();
  require(responses.size() == 1, "unexpected response count");
  require(responses.front().request_id == Int{41},
          "transport request id was not preserved");
  require(bundle_int(responses.front().response, "status") == Int{201},
          "response status was not preserved");
  require(observed_report_count == 1,
          "delivery report did not tick exactly once");
  require(observed_report.view()
                  .as_bundle()
                  .at("status")
                  .checked_as<WebDeliveryStatus>() ==
              WebDeliveryStatus::Delivered,
          "respond delivery was not reported Delivered");
}

void test_client_call_response_arm() {
  call_client = std::make_shared<FakeWebClient>();
  observed_response = Value{};
  observed_response_count = 0;
  observed_failure_count = 0;

  auto executor = start_realtime(build_realtime<CallGraph>());
  auto view = executor.view();
  AsyncGraphExecutorRun runner{view};

  require(call_client->wait_for_calls(1, 2s),
          "client call did not reach the transport sink");
  const auto calls = call_client->calls();
  require(calls.size() == 1, "unexpected call count");
  require(calls.front().request.view().as_bundle().at("url").checked_as<Str>() ==
              Str{"https://api/orders"},
          "client request url was not preserved");
  require(calls.front().options.has_value(),
          "client call options were not delivered");
  call_client->respond(calls.front().client_id,
                       make_response(Int{200}, {}, Bytes{"done"}));
  runner.join();

  require(observed_response_count == 1,
          "client response did not tick exactly once");
  require(observed_failure_count == 0,
          "transport failure arm ticked for an HTTP response");
  require(bundle_int(observed_response, "status") == Int{200},
          "client response status was not preserved");
}

void test_client_call_failure_arm() {
  failure_client = std::make_shared<FakeWebClient>();
  observed_failure = Value{};
  observed_response_count = 0;
  observed_failure_count = 0;

  auto executor = start_realtime(build_realtime<FailureGraph>());
  auto view = executor.view();
  AsyncGraphExecutorRun runner{view};

  require(failure_client->wait_for_calls(1, 2s),
          "client call did not reach the transport sink");
  failure_client->fail(failure_client->calls().front().client_id,
                       make_transport_error(Int{28}, "timeout", true));
  runner.join();

  require(observed_failure_count == 1,
          "transport failure did not tick exactly once");
  require(observed_response_count == 0,
          "response arm ticked for a transport failure");
  require(bundle_int(observed_failure, "error_code") == Int{28},
          "transport error code was not preserved");
}

void test_ws_server_boundary() {
  ws_server = std::make_shared<FakeWebServer>();
  observed_ws_event = Value{};
  observed_ws_frame = Value{};
  observed_ws_report = Value{};

  auto executor = start_realtime(build_realtime<WsGraph>());
  auto view = executor.view();
  AsyncGraphExecutorRun runner{view};

  require(ws_server->wait_until_attached(2s),
          "WS server service did not attach");
  require(ws_server->wait_for_ws_routes(1, 2s),
          "WS route did not reach the transport sink");
  require(ws_server->wait_for_ws_sends(1, 2s),
          "WS send did not reach the transport sink");

  BundleBuilder event{ValuePlanFactory::instance().type_for(
      scalar_descriptor<WsEvent>::value_meta())};
  event.set("connection_id", Value{Int{5}});
  event.set("state", Value{WsConnectionState::Open});
  ws_server->emit_ws_event(ws_route.clone(), event.build());

  BundleBuilder inbound{ValuePlanFactory::instance().type_for(
      scalar_descriptor<WsInboundFrame>::value_meta())};
  inbound.set("connection_id", Value{Int{5}});
  inbound.set("frame", make_text_frame("ping"));
  ws_server->emit_ws_frame(ws_route.clone(), inbound.build());
  runner.join();

  require(observed_ws_event.has_value(), "WS open event was not delivered");
  require(observed_ws_event.view()
                  .as_bundle()
                  .at("state")
                  .checked_as<WsConnectionState>() == WsConnectionState::Open,
          "WS connection state was not preserved");
  require(observed_ws_frame.has_value(), "WS frame was not delivered");
  require(bundle_int(observed_ws_frame, "connection_id") == Int{5},
          "WS frame connection id was not preserved");
  const auto sends = ws_server->ws_sends();
  require(sends.size() == 1, "unexpected WS send count");
  require(sends.front().connection_id == Int{5},
          "WS send connection id was not preserved");
  require(observed_ws_report.has_value() &&
              observed_ws_report.view()
                      .as_bundle()
                      .at("status")
                      .checked_as<WebDeliveryStatus>() ==
                  WebDeliveryStatus::Delivered,
          "WS send was not reported Delivered");
}

void test_ws_client_boundary() {
  ws_client = std::make_shared<FakeWebClient>();
  observed_client_ws_frame = Value{};
  observed_client_ws_report = Value{};

  auto executor = start_realtime(build_realtime<ClientWsGraph>());
  auto view = executor.view();
  AsyncGraphExecutorRun runner{view};

  require(ws_client->wait_for_ws_keys(1, 2s),
          "WS client key did not reach the transport sink");
  require(ws_client->wait_for_ws_sends(1, 2s),
          "WS client send did not reach the transport sink");
  const auto key = ws_client->ws_keys().front().clone();
  ws_client->emit_ws_frame(key.clone(), make_text_frame("tick"));
  runner.join();

  require(observed_client_ws_frame.has_value(),
          "WS client frame was not delivered");
  require(observed_client_ws_frame.view()
                  .as_bundle()
                  .at("text")
                  .checked_as<Str>() == Str{"tick"},
          "WS client frame text was not preserved");
  require(observed_client_ws_report.has_value(),
          "WS client send was not reported");
}

void test_push_backlogs_deliver_one_request_per_cycle() {
  backlog_server = std::make_shared<FakeWebServer>();
  backlog_request_count = 0;
  backlog_request_ids.clear();

  auto executor = start_realtime(build_realtime<BacklogGraph>());
  auto view = executor.view();
  AsyncGraphExecutorRun runner{view};

  require(backlog_server->wait_for_http_routes(1, 2s),
          "route did not reach the transport sink");
  for (Int request_id : {Int{1}, Int{2}, Int{3}}) {
    backlog_server->emit_request(serve_route.clone(),
                                 make_server_request(request_id));
  }
  runner.join();

  // Three same-route requests may never conflate into fewer ticks: each is
  // one engine cycle (RFC 0024, ordering and time).
  require(backlog_request_count == 3,
          "backlogged requests did not tick once each");
  require(backlog_request_ids == std::vector<Int>({Int{1}, Int{2}, Int{3}}),
          "backlogged requests were reordered");
}

void test_transport_channels_progress_independently() {
  lifecycle_server = std::make_shared<FakeWebServer>();
  observed_transport_progress.clear();
  transport_channel_gate.reset();

  auto executor = start_realtime(build_realtime<IndependentTransportGraph>());
  auto view = executor.view();
  AsyncGraphExecutorRun runner{view};
  const auto release_gate =
      hgraph::make_scope_exit([] { transport_channel_gate.release(); });

  require(lifecycle_server->wait_for_http_routes(1, 2s),
          "independent transport HTTP route did not reach its sink");
  require(lifecycle_server->wait_for_ws_routes(1, 2s),
          "independent transport WS route did not reach its sink");

  lifecycle_server->emit_request(serve_route.clone(),
                                 make_server_request(Int{70}));
  require(transport_channel_gate.await_blocked(2s),
          "transport channel capture did not block");
  for (Int request_id : {Int{71}, Int{72}, Int{73}, Int{74}}) {
    lifecycle_server->emit_request(serve_route.clone(),
                                   make_server_request(request_id));
  }
  lifecycle_server->emit_event(
      make_event(WebSeverity::Info, Str{"test"}, Str{"ordering"},
                 Str{"web-independent-transport"}, Str{"independent"}));
  BundleBuilder ws_event{ValuePlanFactory::instance().type_for(
      scalar_descriptor<WsEvent>::value_meta())};
  ws_event.set("connection_id", Value{Int{9}});
  ws_event.set("state", Value{WsConnectionState::Open});
  lifecycle_server->emit_ws_event(ws_route.clone(), ws_event.build());
  transport_channel_gate.release();
  runner.join();

  require(observed_transport_progress.size() == 3,
          "not every independent transport channel made progress");
  const DateTime progress_time = observed_transport_progress.front().second;
  for (const auto &[kind, evaluation_time] : observed_transport_progress) {
    require(evaluation_time == progress_time,
            "the " + kind +
                " channel was blocked behind another channel's backlog");
  }
}

void test_removed_route_drops_queued_events_after_readd() {
  lifecycle_server = std::make_shared<FakeWebServer>();
  route_sender.reset();
  route_generation_gate.reset();
  route_value_count = 0;
  post_readd_request_ids.clear();

  auto executor = start_realtime(build_realtime<RouteGenerationGraph>());
  auto view = executor.view();
  AsyncGraphExecutorRun runner{view};
  const auto release_gate =
      hgraph::make_scope_exit([] { route_generation_gate.release(); });

  const auto sender = route_sender.await(2s);
  require(sender.has_value(), "dynamic route sender did not start");
  require(sender->send_blocking(serve_route.clone()),
          "initial route was refused");
  require(lifecycle_server->wait_for_http_routes(1, 2s),
          "initial route did not reach the transport sink");

  // Hold the graph in one legitimate request evaluation. This makes the
  // reviewed race deterministic: all following requests match the first
  // route lifetime before its removal, while the two route changes wait in a
  // separate standard push-source FIFO.
  lifecycle_server->emit_request(serve_route.clone(),
                                 make_server_request(Int{100}));
  require(route_generation_gate.await_blocked(2s),
          "route-generation capture did not block");
  for (Int request_id : {Int{101}, Int{102}, Int{103}, Int{104}}) {
    lifecycle_server->emit_request(serve_route.clone(),
                                   make_server_request(request_id));
  }
  require(sender->send_blocking(alternate_route.clone()),
          "route removal was refused");
  require(sender->send_blocking(serve_route.clone()),
          "route re-addition was refused");
  route_generation_gate.release();

  require(lifecycle_server->wait_for_http_routes(3, 2s),
          "route remove/re-add did not reach the transport sink");
  lifecycle_server->emit_request(serve_route.clone(),
                                 make_server_request(Int{999}));
  runner.join();

  require(route_value_count == 3,
          "route lifetime did not process all three route values");
  require(post_readd_request_ids == std::vector<Int>{Int{999}},
          "an event queued for the removed route lifetime reached the "
          "re-added subscription");
}

void test_removed_ws_route_drops_queued_frames_after_readd() {
  lifecycle_server = std::make_shared<FakeWebServer>();
  route_sender.reset();
  route_generation_gate.reset();
  route_value_count = 0;
  post_readd_ws_frames.clear();

  auto executor = start_realtime(build_realtime<WsRouteGenerationGraph>());
  auto view = executor.view();
  AsyncGraphExecutorRun runner{view};
  const auto release_gate =
      hgraph::make_scope_exit([] { route_generation_gate.release(); });

  const auto sender = route_sender.await(2s);
  require(sender.has_value(), "dynamic WebSocket route sender did not start");
  require(sender->send_blocking(ws_route.clone()),
          "initial WebSocket route was refused");
  require(lifecycle_server->wait_for_ws_routes(1, 2s),
          "initial WebSocket route did not reach the transport sink");
  lifecycle_server->emit_ws_frame(ws_route.clone(),
                                  make_inbound_frame(Str{"block"}));
  require(route_generation_gate.await_blocked(2s),
          "WebSocket route-generation capture did not block");
  for (const auto &text : {Str{"old-1"}, Str{"old-2"}, Str{"old-3"}}) {
    lifecycle_server->emit_ws_frame(ws_route.clone(),
                                    make_inbound_frame(text));
  }
  require(sender->send_blocking(alternate_ws_route.clone()),
          "WebSocket route removal was refused");
  require(sender->send_blocking(ws_route.clone()),
          "WebSocket route re-addition was refused");
  route_generation_gate.release();

  require(lifecycle_server->wait_for_ws_routes(3, 2s),
          "WebSocket route remove/re-add did not reach the transport sink");
  lifecycle_server->emit_ws_frame(ws_route.clone(),
                                  make_inbound_frame(Str{"new"}));
  runner.join();

  require(route_value_count == 3,
          "WebSocket route lifetime did not process all three values");
  require(post_readd_ws_frames == std::vector<Str>{Str{"new"}},
          "a frame queued for the removed WebSocket route lifetime reached "
          "the re-added subscription");
}

void test_removed_client_key_drops_queued_frames_after_readd() {
  ws_client = std::make_shared<FakeWebClient>();
  route_sender.reset();
  route_generation_gate.reset();
  route_value_count = 0;
  post_readd_client_ws_frames.clear();

  auto executor = start_realtime(build_realtime<ClientKeyGenerationGraph>());
  auto view = executor.view();
  AsyncGraphExecutorRun runner{view};
  const auto release_gate =
      hgraph::make_scope_exit([] { route_generation_gate.release(); });

  const auto sender = route_sender.await(2s);
  require(sender.has_value(), "dynamic WebSocket client sender did not start");
  require(sender->send_blocking(client_ws_key.clone()),
          "initial WebSocket client key was refused");
  require(ws_client->wait_for_ws_keys(1, 2s),
          "initial WebSocket client key did not reach the transport sink");
  ws_client->emit_ws_frame(client_ws_key.clone(), make_text_frame("block"));
  require(route_generation_gate.await_blocked(2s),
          "WebSocket client generation capture did not block");
  for (const auto &text : {Str{"old-1"}, Str{"old-2"}, Str{"old-3"}}) {
    ws_client->emit_ws_frame(client_ws_key.clone(), make_text_frame(text));
  }
  require(sender->send_blocking(alternate_client_ws_key.clone()),
          "WebSocket client key removal was refused");
  require(sender->send_blocking(client_ws_key.clone()),
          "WebSocket client key re-addition was refused");
  route_generation_gate.release();

  require(ws_client->wait_for_ws_keys(3, 2s),
          "WebSocket client remove/re-add did not reach the transport sink");
  ws_client->emit_ws_frame(client_ws_key.clone(), make_text_frame("new"));
  runner.join();

  require(route_value_count == 3,
          "WebSocket client lifetime did not process all three values");
  require(post_readd_client_ws_frames == std::vector<Str>{Str{"new"}},
          "a frame queued for the removed WebSocket client lifetime reached "
          "the re-added subscription");
}

void test_duplicate_registration_fails_and_service_restarts() {
  serve_server = std::make_shared<FakeWebServer>();
  require_failure(
      [] {
        static_cast<void>(build_realtime<DuplicateRegistrationGraph>());
      },
      "duplicate web service registration was accepted");

  for (int run = 0; run != 2; ++run) {
    observed_request = Value{};
    observed_request_count = 0;
    observed_serving_state = false;
    auto executor = start_realtime(build_realtime<ServeGraph>());
    auto view = executor.view();
    AsyncGraphExecutorRun runner{view};
    require(serve_server->wait_for_http_routes(
                static_cast<std::size_t>(run + 1), 2s),
            "route did not reach the transport sink on restart");
    serve_server->emit_request(serve_route.clone(), emitted_request.clone());
    runner.join();
    require(observed_request_count == 1,
            "request did not tick after restart");
  }
  require(serve_server->attach_count() == 2,
          "restarted service did not attach once per run");
}
} // namespace

int main() {
  try {
    hgraph::stdlib::register_standard_operators();
    const auto release_state = hgraph::make_scope_exit(release_test_state);
    initialize_values();
    test_runtime_specific_wiring_shapes();
    test_simulation_rejects_live_push_adaptors();
    test_serve_boundary();
    test_respond_boundary();
    test_client_call_response_arm();
    test_client_call_failure_arm();
    test_ws_server_boundary();
    test_ws_client_boundary();
    test_push_backlogs_deliver_one_request_per_cycle();
    test_transport_channels_progress_independently();
    test_removed_route_drops_queued_events_after_readd();
    test_removed_ws_route_drops_queued_frames_after_readd();
    test_removed_client_key_drops_queued_frames_after_readd();
    test_duplicate_registration_fails_and_service_restarts();
    std::cout << "hgraph-web service tests passed\n";
    return 0;
  } catch (const std::exception &error) {
    std::cerr << "hgraph-web service test failed: " << error.what() << '\n';
    return 1;
  }
}
