// The RFC 0024 loopback contract: the extension tests its own client
// against its own server on 127.0.0.1 — the curl client transport calls the
// Asio/Beast server transport inside one graph, HTTP and WebSocket both.

#include <hgraph/web/service.h>
#include <hgraph/web/value_builders.h>

#include <hgraph/lib/std/operators/conversion.h>
#include <hgraph/lib/std/operators/registration.h>
#include <hgraph/lib/std/std_nodes.h>
#include <hgraph/lib/testing/runtime_support.h>
#include <hgraph/runtime/runtime.h>
#include <hgraph/util/scope.h>

#include <atomic>
#include <chrono>
#include <iostream>
#include <stdexcept>
#include <string>
#include <utility>

namespace {
using namespace hgraph;
using namespace hgraph::web;
using namespace hgraph::testing;
using namespace std::chrono_literals;

void require(bool condition, std::string message) {
  if (!condition) {
    throw std::runtime_error(std::move(message));
  }
}

inline std::atomic<bool> http_triggered{false};
inline std::atomic<bool> ws_triggered{false};
inline std::atomic<bool> http_response_seen{false};
inline std::atomic<bool> http_failure_seen{false};
inline std::atomic<bool> ws_welcome_seen{false};
inline std::atomic<bool> server_saw_client_ping{false};
inline Value observed_response{};
inline Value observed_failure{};

void release_test_state() {
  observed_response = Value{};
  observed_failure = Value{};
}

void maybe_stop(NodeView &node) {
  if ((http_response_seen.load() || http_failure_seen.load()) &&
      ws_welcome_seen.load() && server_saw_client_ping.load()) {
    node.graph().executor().request_stop();
  }
}

struct HttpCallTrigger {
  static constexpr auto name = "web_client_loopback_http_trigger";

  static void eval(In<"stats", TS<WebServerStats>, InputValidity::Unchecked>
                       stats,
                   Out<TS<HttpClientRequest>> out) {
    if (!stats.valid() || !stats.modified() || http_triggered.load()) {
      return;
    }
    const Int port = stats.base()
                         .value()
                         .as_bundle()
                         .at("listening_port")
                         .checked_as<Int>();
    if (port == 0) {
      return;
    }
    http_triggered.store(true);
    const Value request = make_client_request(
        HttpMethod::Get,
        Str{"http://127.0.0.1:" + std::to_string(port) + "/echo/client"});
    out.apply(request.view());
  }
};

struct WsKeyTrigger {
  static constexpr auto name = "web_client_loopback_ws_trigger";

  static void eval(In<"stats", TS<WebServerStats>, InputValidity::Unchecked>
                       stats,
                   Out<TS<WsClientKey>> out) {
    if (!stats.valid() || !stats.modified() || ws_triggered.load()) {
      return;
    }
    const Int port = stats.base()
                         .value()
                         .as_bundle()
                         .at("listening_port")
                         .checked_as<Int>();
    if (port == 0) {
      return;
    }
    ws_triggered.store(true);
    const Value key = make_ws_client_key(
        Str{"ws://127.0.0.1:" + std::to_string(port) + "/live"});
    out.apply(key.view());
  }
};

struct CallResultCapture {
  static constexpr auto name = "web_client_loopback_result_capture";

  static void eval(NodeView node,
                   In<"result", HttpCallResult, InputValidity::Unchecked>
                       result) {
    auto response = result.template field<"response">();
    if (response.valid() && response.modified()) {
      observed_response = response.base().value().clone();
      http_response_seen.store(true);
    }
    auto failure = result.template field<"failure">();
    if (failure.valid() && failure.modified()) {
      observed_failure = failure.base().value().clone();
      http_failure_seen.store(true);
    }
    maybe_stop(node);
  }
};

struct WsClientPing {
  static constexpr auto name = "web_client_loopback_ws_ping";

  static void eval(In<"output", WsClientOutput, InputValidity::Unchecked>
                       output,
                   Out<TS<WsFrame>> out) {
    auto frame = output.template field<"frame">();
    if (!frame.valid() || !frame.modified()) {
      return;
    }
    const auto fields = frame.base().value().as_bundle();
    if (fields.at("kind").checked_as<WsFrameKind>() == WsFrameKind::Text &&
        fields.at("text").checked_as<Str>() == Str{"welcome"}) {
      ws_welcome_seen.store(true);
      const Value ping = make_text_frame("ping-from-client");
      out.apply(ping.view());
    }
  }
};

// Server side: echo responder plus the WS welcome-and-capture pair.
struct EchoId {
  static constexpr auto name = "web_client_loopback_echo_id";

  static void eval(In<"routed", WebRouteOutput, InputValidity::Unchecked>
                       routed,
                   Out<TS<Int>> out) {
    auto request = routed.template field<"request">();
    if (request.valid() && request.modified()) {
      out.apply(request.base().value().as_bundle().at("request_id"));
    }
  }
};

struct EchoResponse {
  static constexpr auto name = "web_client_loopback_echo_response";

  static void eval(In<"routed", WebRouteOutput, InputValidity::Unchecked>
                       routed,
                   Out<TS<HttpResponse>> out) {
    auto request = routed.template field<"request">();
    if (request.valid() && request.modified()) {
      const Value response =
          make_response(Int{200}, {}, Bytes{"hello client"});
      out.apply(response.view());
    }
  }
};

struct WsWelcomeId {
  static constexpr auto name = "web_client_loopback_ws_welcome_id";

  static void eval(In<"routed", WsRouteOutput, InputValidity::Unchecked>
                       routed,
                   Out<TS<Int>> out) {
    auto event = routed.template field<"event">();
    if (event.valid() && event.modified()) {
      const auto fields = event.base().value().as_bundle();
      if (fields.at("state").checked_as<WsConnectionState>() ==
          WsConnectionState::Open) {
        out.apply(fields.at("connection_id"));
      }
    }
  }
};

struct WsWelcomeFrame {
  static constexpr auto name = "web_client_loopback_ws_welcome_frame";

  static void eval(In<"routed", WsRouteOutput, InputValidity::Unchecked>
                       routed,
                   Out<TS<WsFrame>> out) {
    auto event = routed.template field<"event">();
    if (event.valid() && event.modified() &&
        event.base().value().as_bundle().at("state").checked_as<
            WsConnectionState>() == WsConnectionState::Open) {
      const Value frame = make_text_frame("welcome");
      out.apply(frame.view());
    }
  }
};

struct ServerPingCapture {
  static constexpr auto name = "web_client_loopback_server_ping_capture";

  static void eval(NodeView node,
                   In<"routed", WsRouteOutput, InputValidity::Unchecked>
                       routed) {
    auto frame = routed.template field<"frame">();
    if (!frame.valid() || !frame.modified()) {
      return;
    }
    const auto inner =
        frame.base().value().as_bundle().at("frame").as_bundle();
    if (inner.at("text").checked_as<Str>() == Str{"ping-from-client"}) {
      server_saw_client_ping.store(true);
    }
    maybe_stop(node);
  }
};

struct ClientLoopbackGraph {
  static constexpr auto name = "web_client_loopback_test_graph";

  static void compose(Wiring &w) {
    const auto server_path = service::path("web-loopback-server");
    const auto client_path = service::path("web-loopback-client");
    register_server(w, server_path,
                    server_config()
                        .port(0)
                        .request_timeout(2'000ms)
                        .stats_interval(50ms)
                        .build());
    register_client(w, client_path, client_config().build());

    auto stats = server_stats(w, server_path);

    // Server: /echo/{name} responder.
    auto echo_route = wire<stdlib::const_, TS<WebRoute>>(
        w, make_route(HttpMethod::Get, "/echo/{name}"));
    auto echoed = serve(w, server_path, echo_route);
    auto request_id = wire<EchoId>(w, echoed).as<TS<Int>>();
    auto response = wire<EchoResponse>(w, echoed).as<TS<HttpResponse>>();
    static_cast<void>(
        respond(w, server_path, respond_request(w, request_id, response)));

    // Server: /live WS welcome + inbound capture.
    auto ws_route = wire<stdlib::const_, TS<WebRoute>>(
        w, make_route(HttpMethod::Get, "/live", true));
    auto ws_output = ws_serve(w, server_path, ws_route);
    auto connection_id = wire<WsWelcomeId>(w, ws_output).as<TS<Int>>();
    auto welcome = wire<WsWelcomeFrame>(w, ws_output).as<TS<WsFrame>>();
    static_cast<void>(
        ws_send(w, server_path, ws_send_request(w, connection_id, welcome)));
    static_cast<void>(wire<ServerPingCapture>(w, ws_output));

    // Client: one HTTP call and one WS session, triggered by the bound port.
    auto client_request = wire<HttpCallTrigger>(w, stats).as<TS<HttpClientRequest>>();
    auto result =
        http_request(w, client_path, http_client_call(w, client_request));
    static_cast<void>(wire<CallResultCapture>(w, result));

    auto ws_key = wire<WsKeyTrigger>(w, stats).as<TS<WsClientKey>>();
    auto ws_client_output = ws_connect(w, client_path, ws_key);
    auto ping = wire<WsClientPing>(w, ws_client_output).as<TS<WsFrame>>();
    static_cast<void>(
        ws_client_send(w, client_path, ws_client_send_request(w, ws_key, ping)));
  }
};
} // namespace

namespace {
void run_loopback_once() {
    http_triggered.store(false);
    ws_triggered.store(false);
    http_response_seen.store(false);
    http_failure_seen.store(false);
    ws_welcome_seen.store(false);
    server_saw_client_ping.store(false);
    observed_response = Value{};
    observed_failure = Value{};

    const DateTime start = wall_now();
    GraphExecutorBuilder executor_builder;
    executor_builder.graph_builder(build_graph<ClientLoopbackGraph>())
        .mode(GraphExecutorMode::RealTime)
        .start_time(start)
        .end_time(start + TimeDelta{20'000'000});
    auto executor = executor_builder.make_executor();
    auto view = executor.view();
    {
      AsyncGraphExecutorRun runner{view};
      runner.join();
    }

    require(http_response_seen.load(),
            std::string{"the client call did not complete"} +
                (http_failure_seen.load()
                     ? ": " + std::string{observed_failure.view()
                                              .as_bundle()
                                              .at("message")
                                              .checked_as<Str>()}
                     : ""));
    require(!http_failure_seen.load(),
            "the client call took the transport-failure arm");
    const auto response = observed_response.view().as_bundle();
    require(response.at("status").checked_as<Int>() == Int{200},
            "the loopback response status was not 200");
    require(response.at("body").checked_as<Bytes>().data == "hello client",
            "the loopback response body did not round-trip");
    require(ws_welcome_seen.load(),
            "the WS client did not receive the welcome frame");
    require(server_saw_client_ping.load(),
            "the server did not receive the WS client's frame");
}
} // namespace

int main() {
  try {
    hgraph::stdlib::register_standard_operators();
    const auto release_state = hgraph::make_scope_exit(release_test_state);
    register_web_types();
    // Two sequential runs in one process: the second run must survive the
    // first run's teardown (the shape the python suites execute).
    run_loopback_once();
    run_loopback_once();
    std::cout << "hgraph-web client loopback tests passed\n";
    return 0;
  } catch (const std::exception &error) {
    std::cerr << "hgraph-web client loopback test failed: " << error.what()
              << '\n';
    return 1;
  }
}
