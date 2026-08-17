// Service-boundary tests over the socketless fake transport: the same
// bridge, drains, and composition as the real transports with only the
// runtime node swapped (RFC 0024, implementation plan step 2).

#include <hgraph/web/service.h>
#include <hgraph/web/testing/fake_transport.h>
#include <hgraph/web/value_builders.h>

#include <hgraph/lib/std/operators/conversion.h>
#include <hgraph/lib/std/operators/registration.h>
#include <hgraph/lib/std/std_nodes.h>
#include <hgraph/lib/testing/runtime_support.h>
#include <hgraph/runtime/runtime.h>
#include <hgraph/util/scope.h>

#include <chrono>
#include <cstddef>
#include <iostream>
#include <stdexcept>
#include <string>
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
inline Value ws_route{};
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
  ws_route = Value{};
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
  backlog_request_ids.clear();
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

void initialize_values() {
  register_web_types();
  server_configuration = server_config().build();
  client_configuration = client_config().build();
  serve_route = make_route(HttpMethod::Get, "/orders/{id}");
  ws_route = make_route(HttpMethod::Get, "/live", true);
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

void test_serve_boundary() {
  serve_server = std::make_shared<FakeWebServer>();
  observed_request = Value{};
  observed_request_count = 0;
  observed_serving_state = false;

  auto executor = start_realtime(build_graph<ServeGraph>());
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

  auto executor = start_realtime(build_graph<RespondGraph>());
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

  auto executor = start_realtime(build_graph<CallGraph>());
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

  auto executor = start_realtime(build_graph<FailureGraph>());
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

  auto executor = start_realtime(build_graph<WsGraph>());
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

  auto executor = start_realtime(build_graph<ClientWsGraph>());
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

void test_push_backlogs_drain_one_request_per_cycle() {
  backlog_server = std::make_shared<FakeWebServer>();
  backlog_request_count = 0;
  backlog_request_ids.clear();

  auto executor = start_realtime(build_graph<BacklogGraph>());
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

void test_duplicate_registration_fails_and_service_restarts() {
  serve_server = std::make_shared<FakeWebServer>();
  require_failure(
      [] { static_cast<void>(build_graph<DuplicateRegistrationGraph>()); },
      "duplicate web service registration was accepted");

  for (int run = 0; run != 2; ++run) {
    observed_request = Value{};
    observed_request_count = 0;
    observed_serving_state = false;
    auto executor = start_realtime(build_graph<ServeGraph>());
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
    test_serve_boundary();
    test_respond_boundary();
    test_client_call_response_arm();
    test_client_call_failure_arm();
    test_ws_server_boundary();
    test_ws_client_boundary();
    test_push_backlogs_drain_one_request_per_cycle();
    test_duplicate_registration_fails_and_service_restarts();
    std::cout << "hgraph-web service tests passed\n";
    return 0;
  } catch (const std::exception &error) {
    std::cerr << "hgraph-web service test failed: " << error.what() << '\n';
    return 1;
  }
}
