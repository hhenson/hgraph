// Live loopback tier: the real Asio/Beast server transport serving a graph
// on 127.0.0.1, exercised by an independent Beast synchronous client so the
// transport is not trusted to test itself (RFC 0024, verification).  Port 0
// binds ephemerally; the bound port is discovered through the stats service.

#include <hgraph/web/service.h>
#include <hgraph/web/value_builders.h>

#include <hgraph/lib/std/operators/conversion.h>
#include <hgraph/lib/std/operators/registration.h>
#include <hgraph/lib/std/std_nodes.h>
#include <hgraph/lib/testing/runtime_support.h>
#include <hgraph/runtime/runtime.h>
#include <hgraph/util/scope.h>

#include <boost/asio/connect.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/beast/core.hpp>
#include <boost/beast/http.hpp>
#include <boost/beast/websocket.hpp>

#include <atomic>
#include <chrono>
#include <iostream>
#include <stdexcept>
#include <string>
#include <thread>
#include <utility>

namespace {
using namespace hgraph;
using namespace hgraph::web;
using namespace hgraph::testing;
using namespace std::chrono_literals;

namespace asio = boost::asio;
namespace beast = boost::beast;
namespace bhttp = boost::beast::http;
namespace bws = boost::beast::websocket;
using tcp = asio::ip::tcp;

void require(bool condition, std::string message) {
  if (!condition) {
    throw std::runtime_error(std::move(message));
  }
}

[[nodiscard]] GraphExecutorValue start_realtime(GraphBuilder builder,
                                                TimeDelta duration) {
  const DateTime start = wall_now();
  GraphExecutorBuilder executor_builder;
  executor_builder.graph_builder(std::move(builder))
      .mode(GraphExecutorMode::RealTime)
      .start_time(start)
      .end_time(start + duration);
  return executor_builder.make_executor();
}

inline std::atomic<int> listening_port{0};
inline std::atomic<int> observed_query_count{0};
inline std::atomic<int> observed_dup_header_count{0};
inline std::atomic<int> respond_delivered_count{0};
inline std::atomic<int> ws_open_count{0};
inline std::atomic<int> ws_frame_count{0};
inline std::atomic<int> ws_closed_count{0};
inline Value observed_ws_frame{};

void release_test_state() { observed_ws_frame = Value{}; }

struct StatsCapture {
  static constexpr auto name = "web_loopback_stats_capture";

  static void eval(In<"stats", TS<WebServerStats>, InputValidity::Unchecked>
                       stats) {
    if (!stats.valid() || !stats.modified()) {
      return;
    }
    listening_port.store(static_cast<int>(stats.base()
                                              .value()
                                              .as_bundle()
                                              .at("listening_port")
                                              .checked_as<Int>()));
  }
};

struct EchoId {
  static constexpr auto name = "web_loopback_echo_id";

  static void eval(In<"routed", WebRouteOutput, InputValidity::Unchecked>
                       routed,
                   Out<TS<Int>> out) {
    auto request = routed.template field<"request">();
    if (!request.valid() || !request.modified()) {
      return;
    }
    const auto fields = request.base().value().as_bundle();
    const auto inner = fields.at("request").as_bundle();
    observed_query_count.store(
        static_cast<int>(inner.at("query").as_list().size()));
    int duplicates = 0;
    for (const auto header : inner.at("headers").as_list()) {
      if (header.as_bundle().at("name").checked_as<Str>() == Str{"X-Dup"}) {
        ++duplicates;
      }
    }
    observed_dup_header_count.store(duplicates);
    out.apply(fields.at("request_id"));
  }
};

struct EchoResponse {
  static constexpr auto name = "web_loopback_echo_response";

  static void eval(In<"routed", WebRouteOutput, InputValidity::Unchecked>
                       routed,
                   Out<TS<HttpResponse>> out) {
    auto request = routed.template field<"request">();
    if (!request.valid() || !request.modified()) {
      return;
    }
    const auto inner =
        request.base().value().as_bundle().at("request").as_bundle();
    Str name_param{};
    for (const auto param : inner.at("path_params").as_list()) {
      const auto fields = param.as_bundle();
      if (fields.at("name").checked_as<Str>() == Str{"name"}) {
        name_param = fields.at("value").checked_as<Str>();
      }
    }
    const Value response = make_response(
        Int{200}, {{"X-Answered-By", "graph"}},
        Bytes{std::string{"hello "} + std::string{name_param}});
    out.apply(response.view());
  }
};

struct ReportCounter {
  static constexpr auto name = "web_loopback_report_counter";

  static void eval(In<"report", TS<WebDeliveryReport>,
                      InputValidity::Unchecked>
                       report) {
    if (report.valid() && report.modified() &&
        report.base().value().as_bundle().at("status").checked_as<
            WebDeliveryStatus>() == WebDeliveryStatus::Delivered) {
      ++respond_delivered_count;
    }
  }
};

struct TraileredId {
  static constexpr auto name = "web_loopback_trailered_id";

  static void eval(In<"routed", WebRouteOutput, InputValidity::Unchecked>
                       routed,
                   Out<TS<Int>> out) {
    auto request = routed.template field<"request">();
    if (request.valid() && request.modified()) {
      out.apply(request.base().value().as_bundle().at("request_id"));
    }
  }
};

struct TraileredResponse {
  static constexpr auto name = "web_loopback_trailered_response";

  static void eval(In<"routed", WebRouteOutput, InputValidity::Unchecked>
                       routed,
                   Out<TS<HttpResponse>> out) {
    auto request = routed.template field<"request">();
    if (request.valid() && request.modified()) {
      const Value response =
          make_response(Int{200}, {}, Bytes{"trailered-body"},
                        {{"X-Trail", "checksum-1"}});
      out.apply(response.view());
    }
  }
};

struct NullRouteSink {
  static constexpr auto name = "web_loopback_null_route_sink";

  static void eval(In<"routed", WebRouteOutput, InputValidity::Unchecked>) {}
};

struct WsWelcomeId {
  static constexpr auto name = "web_loopback_ws_welcome_id";

  static void eval(In<"routed", WsRouteOutput, InputValidity::Unchecked>
                       routed,
                   Out<TS<Int>> out) {
    auto event = routed.template field<"event">();
    if (!event.valid() || !event.modified()) {
      return;
    }
    const auto fields = event.base().value().as_bundle();
    const auto state = fields.at("state").checked_as<WsConnectionState>();
    if (state == WsConnectionState::Open) {
      ++ws_open_count;
      out.apply(fields.at("connection_id"));
    } else if (state == WsConnectionState::Closed) {
      ++ws_closed_count;
    }
  }
};

struct WsWelcomeFrame {
  static constexpr auto name = "web_loopback_ws_welcome_frame";

  static void eval(In<"routed", WsRouteOutput, InputValidity::Unchecked>
                       routed,
                   Out<TS<WsFrame>> out) {
    auto event = routed.template field<"event">();
    if (!event.valid() || !event.modified()) {
      return;
    }
    if (event.base().value().as_bundle().at("state").checked_as<
            WsConnectionState>() == WsConnectionState::Open) {
      const Value frame = make_text_frame("welcome");
      out.apply(frame.view());
    }
  }
};

struct WsFrameCapture {
  static constexpr auto name = "web_loopback_ws_frame_capture";

  static void eval(In<"routed", WsRouteOutput, InputValidity::Unchecked>
                       routed) {
    auto frame = routed.template field<"frame">();
    if (frame.valid() && frame.modified()) {
      observed_ws_frame = frame.base().value().clone();
      ++ws_frame_count;
    }
  }
};

struct LoopbackGraph {
  static constexpr auto name = "web_loopback_test_graph";

  static void compose(Wiring &w) {
    const auto path = service::path("web-loopback");
    register_server(w, path,
                    server_config()
                        .port(0)
                        .request_timeout(500ms)
                        .stats_interval(50ms)
                        .build());

    auto echo_route = wire<stdlib::const_, TS<WebRoute>>(
        w, make_route(HttpMethod::Get, "/echo/{name}"));
    auto echoed = serve(w, path, echo_route);
    auto request_id = wire<EchoId>(w, echoed).as<TS<Int>>();
    auto response = wire<EchoResponse>(w, echoed).as<TS<HttpResponse>>();
    auto reports = respond(w, path, respond_request(w, request_id, response));
    static_cast<void>(wire<ReportCounter>(w, reports));

    auto trailered_route = wire<stdlib::const_, TS<WebRoute>>(
        w, make_route(HttpMethod::Get, "/trailered"));
    auto trailered = serve(w, path, trailered_route);
    auto trailered_id = wire<TraileredId>(w, trailered).as<TS<Int>>();
    auto trailered_response =
        wire<TraileredResponse>(w, trailered).as<TS<HttpResponse>>();
    static_cast<void>(respond(
        w, path, respond_request(w, trailered_id, trailered_response)));

    auto black_hole = wire<stdlib::const_, TS<WebRoute>>(
        w, make_route(HttpMethod::Get, "/black-hole"));
    static_cast<void>(wire<NullRouteSink>(w, serve(w, path, black_hole)));

    auto ws_route = wire<stdlib::const_, TS<WebRoute>>(
        w, make_route(HttpMethod::Get, "/live", true));
    auto ws_output = ws_serve(w, path, ws_route);
    auto connection_id = wire<WsWelcomeId>(w, ws_output).as<TS<Int>>();
    auto welcome = wire<WsWelcomeFrame>(w, ws_output).as<TS<WsFrame>>();
    static_cast<void>(
        ws_send(w, path, ws_send_request(w, connection_id, welcome)));
    static_cast<void>(wire<WsFrameCapture>(w, ws_output));

    static_cast<void>(wire<StatsCapture>(w, server_stats(w, path)));
  }
};

[[nodiscard]] int await_listening_port() {
  const auto deadline = std::chrono::steady_clock::now() + 5s;
  while (std::chrono::steady_clock::now() < deadline) {
    if (const int port = listening_port.load(); port != 0) {
      return port;
    }
    std::this_thread::sleep_for(10ms);
  }
  throw std::runtime_error("the server did not report its listening port");
}

[[nodiscard]] tcp::endpoint loopback_endpoint(int port) {
  return tcp::endpoint{asio::ip::make_address("127.0.0.1"),
                       static_cast<std::uint16_t>(port)};
}

[[nodiscard]] bhttp::response<bhttp::string_body>
sync_get(asio::io_context &ioc, int port, std::string_view target,
         std::vector<std::pair<std::string, std::string>> headers = {},
         bhttp::verb method = bhttp::verb::get) {
  beast::tcp_stream stream{ioc};
  stream.expires_after(5s);
  stream.connect(loopback_endpoint(port));
  bhttp::request<bhttp::string_body> request{method, std::string{target}, 11};
  request.set(bhttp::field::host, "127.0.0.1");
  for (const auto &[name, value] : headers) {
    request.insert(name, value);
  }
  bhttp::write(stream, request);
  beast::flat_buffer buffer;
  bhttp::response_parser<bhttp::string_body> parser;
  // The parser cannot know the request verb; a HEAD response advertises a
  // length it will not send.
  parser.skip(method == bhttp::verb::head);
  bhttp::read(stream, buffer, parser);
  beast::error_code ec;
  static_cast<void>(
      stream.socket().shutdown(tcp::socket::shutdown_both, ec));
  return parser.release();
}
} // namespace

int main() {
  try {
    hgraph::stdlib::register_standard_operators();
    const auto release_state = hgraph::make_scope_exit(release_test_state);
    register_web_types();

    auto executor =
        start_realtime(build_graph<LoopbackGraph>(), TimeDelta{30'000'000});
    auto view = executor.view();
    AsyncGraphExecutorRun runner{view};

    const int port = await_listening_port();
    asio::io_context ioc;

    {
      const auto response =
          sync_get(ioc, port, "/echo/world?a=1&a=2",
                   {{"X-Dup", "one"}, {"X-Dup", "two"}});
      require(response.result_int() == 200,
              "echo route did not answer 200, got " +
                  std::to_string(response.result_int()));
      require(response.body() == "hello world",
              "path parameter did not round-trip: " + response.body());
      require(response["X-Answered-By"] == "graph",
              "response headers did not round-trip");
      require(observed_query_count.load() == 2,
              "multi-value query parameters were collapsed");
      require(observed_dup_header_count.load() == 2,
              "duplicate request headers were collapsed");
    }

    {
      const auto response = sync_get(ioc, port, "/missing");
      require(response.result_int() == 404,
              "an unmatched route did not answer 404");
    }

    {
      // HEAD advertises the entity's length but carries no body.
      const auto response = sync_get(ioc, port, "/echo/head", {},
                                     bhttp::verb::head);
      require(response.result_int() == 200, "HEAD did not answer 200");
      require(response.body().empty(), "a HEAD response carried a body");
      require(response["Content-Length"] == "10",
              "HEAD did not advertise the entity length");
    }

    {
      // Percent-encoded targets route AND surface decoded (%6C = 'l').
      const auto response = sync_get(ioc, port, "/echo/he%6Clo");
      require(response.result_int() == 200,
              "the percent-encoded target did not route");
      require(response.body() == "hello hello",
              "the path capture was not percent-decoded: " + response.body());
    }

    {
      // Malformed escapes are a client error, not an unmatched route.
      const auto response = sync_get(ioc, port, "/echo/%ZZ");
      require(response.result_int() == 400,
              "a malformed escape did not answer 400");
    }

    {
      // Graph-provided trailers arrive over chunked transfer.
      const auto response = sync_get(ioc, port, "/trailered");
      require(response.result_int() == 200, "trailered route did not answer");
      require(response.body() == "trailered-body",
              "the chunked body did not round-trip");
      require(response["X-Trail"] == "checksum-1",
              "the response trailer was not delivered");
    }

    {
      // No graph responder is wired for this route: the transport must
      // answer 503 at the request timeout, never hang (RFC 0024).
      const auto started = std::chrono::steady_clock::now();
      const auto response = sync_get(ioc, port, "/black-hole");
      const auto elapsed = std::chrono::steady_clock::now() - started;
      require(response.result_int() == 503,
              "an unanswered request did not receive the transport 503");
      require(elapsed < 4s, "the transport 503 took too long");
    }

    {
      bws::stream<beast::tcp_stream> ws{ioc};
      beast::get_lowest_layer(ws).expires_after(5s);
      beast::get_lowest_layer(ws).connect(loopback_endpoint(port));
      ws.handshake("127.0.0.1", "/live");
      beast::get_lowest_layer(ws).expires_never();

      beast::flat_buffer buffer;
      ws.read(buffer);
      require(ws.got_text(), "the welcome frame was not text");
      require(beast::buffers_to_string(buffer.data()) == "welcome",
              "the graph welcome frame did not arrive");

      ws.write(asio::buffer(std::string{"ping-from-client"}));
      const auto deadline = std::chrono::steady_clock::now() + 5s;
      while (ws_frame_count.load() == 0 &&
             std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(10ms);
      }
      require(ws_frame_count.load() == 1,
              "the client frame did not reach the graph");
      {
        const auto frame_fields = observed_ws_frame.view().as_bundle();
        require(frame_fields.at("frame")
                        .as_bundle()
                        .at("text")
                        .checked_as<Str>() == Str{"ping-from-client"},
                "the inbound WS frame payload was not preserved");
      }

      // A message larger than the transport's 64KB read chunk exercises
      // the incremental accounting path: several some-reads grow one
      // reservation and reassemble in accounted storage (review P1).
      std::string big(200'000, '\0');
      for (std::size_t i = 0; i != big.size(); ++i) {
        big[i] = static_cast<char>('a' + static_cast<char>(i % 23));
      }
      ws.binary(true);
      ws.write(asio::buffer(big));
      const auto big_deadline = std::chrono::steady_clock::now() + 5s;
      while (ws_frame_count.load() < 2 &&
             std::chrono::steady_clock::now() < big_deadline) {
        std::this_thread::sleep_for(10ms);
      }
      require(ws_frame_count.load() == 2,
              "the multi-chunk frame did not reach the graph");
      {
        const auto big_fields = observed_ws_frame.view().as_bundle();
        const auto received =
            big_fields.at("frame").as_bundle().at("data").checked_as<Bytes>();
        require(received.data == big,
                "the multi-chunk frame payload was not reassembled intact");
      }

      ws.close(bws::close_code::normal);
      const auto close_deadline = std::chrono::steady_clock::now() + 5s;
      while (ws_closed_count.load() == 0 &&
             std::chrono::steady_clock::now() < close_deadline) {
        std::this_thread::sleep_for(10ms);
      }
      require(ws_closed_count.load() == 1,
              "the orderly close did not reach the graph as a Closed event");
    }

    require(respond_delivered_count.load() >= 1,
            "the respond delivery report did not tick");
    require(ws_open_count.load() == 1,
            "the WS open event did not tick exactly once");

    view.request_stop();
    runner.join();
    std::cout << "hgraph-web loopback tests passed\n";
    return 0;
  } catch (const std::exception &error) {
    std::cerr << "hgraph-web loopback test failed: " << error.what() << '\n';
    return 1;
  }
}
