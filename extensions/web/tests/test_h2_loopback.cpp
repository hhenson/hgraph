// The HTTP/2 loopback oracle (RFC 0024, HTTP/2 activation plan): the
// extension's curl client — pinned H2Only — calls the extension's own
// server over TLS with ALPN ["h2", "http/1.1"] on 127.0.0.1.  A completed
// call therefore proves the ALPN dispatch, the nghttp2 session, header and
// trailer round-trips, and the reservation-mapped flow control end to end;
// the second call streams a body larger than the h2 stream window to
// exercise incremental admission.

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

// A 100-year self-signed certificate for 127.0.0.1/localhost, generated
// once for this suite; the client disables verification, so nothing
// depends on its trust chain.
constexpr const char *kTestCertPem = R"pem(-----BEGIN CERTIFICATE-----
MIIDJzCCAg+gAwIBAgIUcNava/ilBlFAAby854h6n3WklcYwDQYJKoZIhvcNAQEL
BQAwFDESMBAGA1UEAwwJMTI3LjAuMC4xMCAXDTI2MDgxNzEwNTEyM1oYDzIxMjYw
NzI0MTA1MTIzWjAUMRIwEAYDVQQDDAkxMjcuMC4wLjEwggEiMA0GCSqGSIb3DQEB
AQUAA4IBDwAwggEKAoIBAQCR/ul5caZPxNee1TPZRkgzwPgHpsJAMchsAmRnkRXm
YGnzIvP7VZHFODj5nud2j/GCqBAxSCCbkLPfobnndehiCx40XEgpePsEDI7wnH6g
bO51I2ENAXodKE0y9Lq8WtEFyzKFbD73HUoNX8xv+KQjDxoQUpvgnXLBo0U/WJJJ
aUO1n1pAkEOMrNIj1Oda0JL6El+TbHJznRcfNh/RSKMDDYXrVXzFXt02I6dYvMKO
FVwTrBC9/bKr2WAvUNU2ugzYueVAkm24buMawgqxMbFSokJmVVj6LQz5SVheUZjs
4Rg1ASSvDs9FYAyFcAQ5X5ps4SZJa6lWhnobupq7mXj7AgMBAAGjbzBtMB0GA1Ud
DgQWBBSxRx4oz39xyLrEtzTDhqwQCaaE/TAfBgNVHSMEGDAWgBSxRx4oz39xyLrE
tzTDhqwQCaaE/TAPBgNVHRMBAf8EBTADAQH/MBoGA1UdEQQTMBGHBH8AAAGCCWxv
Y2FsaG9zdDANBgkqhkiG9w0BAQsFAAOCAQEAf4R9Aekow3u747bg7c33t9rI0/7w
RXGDOwb8JifIE420J6miDiFoWHwXmb1/pu+Ti+VsDYUsj6nkTXLYnO9tdDp71G6T
wHa7e7bDA0fBfWvDGJMq+9SK76ZOf6gD26JEN86ejbY/YKymzRF5+Q/Njj+WjfGt
IozucryoCL/TPpQW4W846THj6Jb6ePhfb8J65vxhU5e05Nn0Zv3KmJIg854kKFzE
9DmMw+yRBc7+sgkidrC3Z43CJ2UqStxTeh7ObnTayPUUou6EEbZrAYfGW3XYI1xe
/uMQ3iaGE08aFv7q8mjRN73rcvvqabBfqDcHGf93rUbCyh8HrPC6eLgSKQ==
-----END CERTIFICATE-----
)pem";

constexpr const char *kTestKeyPem = R"pem(-----BEGIN PRIVATE KEY-----
MIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQCR/ul5caZPxNee
1TPZRkgzwPgHpsJAMchsAmRnkRXmYGnzIvP7VZHFODj5nud2j/GCqBAxSCCbkLPf
obnndehiCx40XEgpePsEDI7wnH6gbO51I2ENAXodKE0y9Lq8WtEFyzKFbD73HUoN
X8xv+KQjDxoQUpvgnXLBo0U/WJJJaUO1n1pAkEOMrNIj1Oda0JL6El+TbHJznRcf
Nh/RSKMDDYXrVXzFXt02I6dYvMKOFVwTrBC9/bKr2WAvUNU2ugzYueVAkm24buMa
wgqxMbFSokJmVVj6LQz5SVheUZjs4Rg1ASSvDs9FYAyFcAQ5X5ps4SZJa6lWhnob
upq7mXj7AgMBAAECggEAAL/wjZy+gSQrN3GR20vlq8LnXur0jjqTOLBhQY6BJ+vB
s824Hce/4rrm0d71vhTEKci2cArjFZUsOfninTm5OQbgUsz7dPTvMq31bl3FPvlV
krPUgswWguPjmEWynDTShlDb7jHy812ZxlLO/asE7IHV2O5YedrtrXGALV+JtMmI
uDviZONYad/xzVUYwdft77koeKjZGnJQJitTr86AMKOpKixwtSOo/8fMXBjC4dTT
GWgfcxXorTDEUiiX3GvPgSEqRBxAkZ2epe1hdmO5rhZcry11tetupOq7tsHroASp
3zhwPtd17te/vSeMoUvrwZcn0cBFP+wKDfKgOC4E7QKBgQDCxhJyXPnlGbJBCk6t
OVJA6moRRjH62CbsC2WPjiL0z7P2NBezCuYqOUPXXsh+2TrHKfwQuj9Mv8HTonJB
ZzyZVnhwiPchM/CC1hK1ps6eHbOeu9jrbXlmaDYVPUAHhRJzA+aBoo3RCB7eVUd4
6qnUyGszXTP63daWnYQu8MudvQKBgQC/444NAMPnUgZmPlbfoxAYq5BTNPmjxzXS
rmQu2hBpNQKLDMG+sIgpSMlK8ojn9Ko1Td61y/8Ub0qndPmkADeBWNMxxpoy/Cxv
EI9x0JBN8UfoPnG2TllQ9ZiM6qH4/4jaXxahq2SkJ1ZJ+TmehCsYiqQv8eeP5m2H
kaySEpPRFwKBgQCVQIrqL+0ebe52gJuBiidJr1fQHOY3vmM1BhaxRs3qoy7YP1rZ
zERLns4pv2wMKBIuhDGv78iJ23d/4T+EdsOtDOIF+i7FtrNazwhPQp+Z8lCuFmxH
HACnRLwM0n66RHK6yAZe2F2sDHj7DoZSViAF+f6LwaQPXOcPS2z7O3IMUQKBgCA7
2IPkqgP0qnCIbk149d4/C6p+jqTtdOQkOV4JcZJKvlefV/hxbR4KRQ4a+daFKgZ0
Q0Ikt3+2RkMlCj57bteClU+aPhLse4ZYsM/8qhD9xAeGXdGzDZvk9bBORdEvE80j
Bgk4YlqU5RDeFcjECP1BZN1M9Ioeui140hVjm4MXAoGAeVrFpOGVToOedj3SG+4k
UP85yV64d10flGkxbCCaEEV8gOjoG3YWh+r0oeIGVFUOi9TzbwI9xYJb154/+dOi
ljQAIYegDzbgnKbPvtbj35Dy07fljW3WYT3fzH70nB3YieivDx2JXqp+DZ8AAnSc
8n8PFd6yOYtIC1rQQ0aqWag=
-----END PRIVATE KEY-----
)pem";

constexpr std::size_t kBigBodyBytes = 2 * 1024 * 1024;

inline std::atomic<bool> get_triggered{false};
inline std::atomic<bool> post_triggered{false};
inline std::atomic<bool> status_triggered{false};
inline std::atomic<bool> get_response_seen{false};
inline std::atomic<bool> post_response_seen{false};
inline std::atomic<bool> status_response_seen{false};
inline std::atomic<bool> failure_seen{false};
inline std::atomic<Int> bound_port{0};
inline Value observed_get_response{};
inline Value observed_post_response{};
inline Value observed_status_response{};
inline Value observed_failure{};
inline Value observed_server_peer{};

void release_test_state() {
  observed_get_response = Value{};
  observed_post_response = Value{};
  observed_status_response = Value{};
  observed_failure = Value{};
  observed_server_peer = Value{};
}

void maybe_stop(NodeView &node) {
  if ((get_response_seen.load() && post_response_seen.load() &&
       status_response_seen.load()) ||
      failure_seen.load()) {
    node.graph().executor().request_stop();
  }
}

struct H2GetTrigger {
  static constexpr auto name = "web_h2_loopback_get_trigger";

  static void eval(In<"stats", TS<WebServerStats>, InputValidity::Unchecked>
                       stats,
                   Out<TS<HttpClientRequest>> out) {
    if (!stats.valid() || !stats.modified() || get_triggered.load()) {
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
    bound_port.store(port);
    get_triggered.store(true);
    const Value request = make_client_request(
        HttpMethod::Get,
        Str{"https://127.0.0.1:" + std::to_string(port) + "/h2/answer"});
    out.apply(request.view());
  }
};

struct H2GetCapture {
  static constexpr auto name = "web_h2_loopback_get_capture";

  static void eval(NodeView node,
                   In<"result", HttpCallResult, InputValidity::Unchecked>
                       result,
                   Out<TS<HttpClientRequest>> out) {
    auto failure = result.template field<"failure">();
    if (failure.valid() && failure.modified()) {
      observed_failure = failure.base().value().clone();
      failure_seen.store(true);
      maybe_stop(node);
      return;
    }
    auto response = result.template field<"response">();
    if (!response.valid() || !response.modified()) {
      return;
    }
    observed_get_response = response.base().value().clone();
    get_response_seen.store(true);
    if (!post_triggered.exchange(true)) {
      // Chain the streaming call: a body larger than the h2 stream window
      // exercises the consume-gated flow control end to end.
      const Value request = make_client_request(
          HttpMethod::Post,
          Str{"https://127.0.0.1:" + std::to_string(bound_port.load()) +
              "/h2-ingest"},
          {}, Bytes{std::string(kBigBodyBytes, 'z')});
      out.apply(request.view());
    }
    maybe_stop(node);
  }
};

struct H2PostCapture {
  static constexpr auto name = "web_h2_loopback_post_capture";

  static void eval(NodeView node,
                   In<"result", HttpCallResult, InputValidity::Unchecked>
                       result,
                   Out<TS<HttpClientRequest>> out) {
    auto failure = result.template field<"failure">();
    if (failure.valid() && failure.modified()) {
      observed_failure = failure.base().value().clone();
      failure_seen.store(true);
      maybe_stop(node);
      return;
    }
    auto response = result.template field<"response">();
    if (!response.valid() || !response.modified()) {
      return;
    }
    observed_post_response = response.base().value().clone();
    post_response_seen.store(true);
    if (!status_triggered.exchange(true)) {
      // A gRPC-shaped exchange: empty body, terminal status in trailers.
      const Value request = make_client_request(
          HttpMethod::Get,
          Str{"https://127.0.0.1:" + std::to_string(bound_port.load()) +
              "/h2-status"});
      out.apply(request.view());
    }
    maybe_stop(node);
  }
};

struct H2StatusCapture {
  static constexpr auto name = "web_h2_loopback_status_capture";

  static void eval(NodeView node,
                   In<"result", HttpCallResult, InputValidity::Unchecked>
                       result) {
    auto failure = result.template field<"failure">();
    if (failure.valid() && failure.modified()) {
      observed_failure = failure.base().value().clone();
      failure_seen.store(true);
      maybe_stop(node);
      return;
    }
    auto response = result.template field<"response">();
    if (response.valid() && response.modified()) {
      observed_status_response = response.base().value().clone();
      status_response_seen.store(true);
    }
    maybe_stop(node);
  }
};

struct H2StatusResponse {
  static constexpr auto name = "web_h2_loopback_status_response";

  static void eval(In<"routed", WebRouteOutput, InputValidity::Unchecked>
                       routed,
                   Out<TS<HttpResponse>> out) {
    auto request = routed.template field<"request">();
    if (request.valid() && request.modified()) {
      const Value response = make_response(
          Int{200}, {}, Bytes{},
          {{Str{"grpc-like-status"}, Str{"0"}}});
      out.apply(response.view());
    }
  }
};

struct H2AnswerId {
  static constexpr auto name = "web_h2_loopback_answer_id";

  static void eval(In<"routed", WebRouteOutput, InputValidity::Unchecked>
                       routed,
                   Out<TS<Int>> out) {
    auto request = routed.template field<"request">();
    if (request.valid() && request.modified()) {
      const auto fields = request.base().value().as_bundle();
      observed_server_peer = fields.at("peer").clone();
      out.apply(fields.at("request_id"));
    }
  }
};

struct H2AnswerResponse {
  static constexpr auto name = "web_h2_loopback_answer_response";

  static void eval(In<"routed", WebRouteOutput, InputValidity::Unchecked>
                       routed,
                   Out<TS<HttpResponse>> out) {
    auto request = routed.template field<"request">();
    if (request.valid() && request.modified()) {
      const auto name_param = request.base()
                                  .value()
                                  .as_bundle()
                                  .at("request")
                                  .as_bundle()
                                  .at("path_params");
      Str captured{};
      for (const auto param : name_param.as_list()) {
        captured = param.as_bundle().at("value").checked_as<Str>();
      }
      const Value response = make_response(
          Int{200}, {{Str{"x-served-by"}, Str{"h2-loopback"}}},
          Bytes{"hi " + std::string{captured}},
          {{Str{"x-trail"}, Str{"h2-checksum"}}});
      out.apply(response.view());
    }
  }
};

struct H2IngestId {
  static constexpr auto name = "web_h2_loopback_ingest_id";

  static void eval(In<"routed", WebRouteOutput, InputValidity::Unchecked>
                       routed,
                   Out<TS<Int>> out) {
    auto request = routed.template field<"request">();
    if (request.valid() && request.modified()) {
      out.apply(request.base().value().as_bundle().at("request_id"));
    }
  }
};

struct H2IngestResponse {
  static constexpr auto name = "web_h2_loopback_ingest_response";

  static void eval(In<"routed", WebRouteOutput, InputValidity::Unchecked>
                       routed,
                   Out<TS<HttpResponse>> out) {
    auto request = routed.template field<"request">();
    if (request.valid() && request.modified()) {
      const auto body = request.base()
                            .value()
                            .as_bundle()
                            .at("request")
                            .as_bundle()
                            .at("body")
                            .checked_as<Bytes>();
      const Value response = make_response(
          Int{200}, {}, Bytes{std::to_string(body.data.size())});
      out.apply(response.view());
    }
  }
};

struct H2LoopbackGraph {
  static constexpr auto name = "web_h2_loopback_test_graph";

  static void compose(Wiring &w) {
    const auto server_path = service::path("web-h2-server");
    const auto client_path = service::path("web-h2-client");
    register_server(w, server_path,
                    server_config()
                        .port(0)
                        .request_timeout(5'000ms)
                        .stats_interval(50ms)
                        .tls(tls_server()
                                 .cert_pem(Str{kTestCertPem})
                                 .key_pem(Str{kTestKeyPem})
                                 .alpn({Str{"h2"}, Str{"http/1.1"}})
                                 .build())
                        .build());
    register_client(w, client_path,
                    client_config()
                        .http_version_policy(WebHttpVersionPolicy::H2Only)
                        .tls(tls_client()
                                 .verify_peer(false)
                                 .verify_host(false)
                                 .build())
                        .build());

    auto stats = server_stats(w, server_path);

    auto answer_route = wire<stdlib::const_, TS<WebRoute>>(
        w, make_route(HttpMethod::Get, "/h2/{name}"));
    auto answered = serve(w, server_path, answer_route);
    auto answer_id = wire<H2AnswerId>(w, answered).as<TS<Int>>();
    auto answer_response =
        wire<H2AnswerResponse>(w, answered).as<TS<HttpResponse>>();
    static_cast<void>(respond(
        w, server_path, respond_request(w, answer_id, answer_response)));

    auto ingest_route = wire<stdlib::const_, TS<WebRoute>>(
        w, make_route(HttpMethod::Post, "/h2-ingest"));
    auto ingested = serve(w, server_path, ingest_route);
    auto ingest_id = wire<H2IngestId>(w, ingested).as<TS<Int>>();
    auto ingest_response =
        wire<H2IngestResponse>(w, ingested).as<TS<HttpResponse>>();
    static_cast<void>(respond(
        w, server_path, respond_request(w, ingest_id, ingest_response)));

    auto status_route = wire<stdlib::const_, TS<WebRoute>>(
        w, make_route(HttpMethod::Get, "/h2-status"));
    auto status_served = serve(w, server_path, status_route);
    auto status_id = wire<H2IngestId>(w, status_served).as<TS<Int>>();
    auto status_response =
        wire<H2StatusResponse>(w, status_served).as<TS<HttpResponse>>();
    static_cast<void>(respond(
        w, server_path, respond_request(w, status_id, status_response)));

    auto get_request =
        wire<H2GetTrigger>(w, stats).as<TS<HttpClientRequest>>();
    auto get_result =
        http_request(w, client_path, http_client_call(w, get_request));
    auto post_request =
        wire<H2GetCapture>(w, get_result).as<TS<HttpClientRequest>>();
    auto post_result =
        http_request(w, client_path, http_client_call(w, post_request));
    auto status_request =
        wire<H2PostCapture>(w, post_result).as<TS<HttpClientRequest>>();
    auto status_result =
        http_request(w, client_path, http_client_call(w, status_request));
    static_cast<void>(wire<H2StatusCapture>(w, status_result));
  }
};

} // namespace

int main() {
  try {
    hgraph::stdlib::register_standard_operators();
    const auto release_state = hgraph::make_scope_exit(release_test_state);
    register_web_types();

    const DateTime start = wall_now();
    GraphExecutorBuilder executor_builder;
    executor_builder.graph_builder(build_graph<H2LoopbackGraph>())
        .mode(GraphExecutorMode::RealTime)
        .start_time(start)
        .end_time(start + TimeDelta{30'000'000});
    auto executor = executor_builder.make_executor();
    auto view = executor.view();
    {
      AsyncGraphExecutorRun runner{view};
      runner.join();
    }

    require(!failure_seen.load(),
            std::string{"a call took the transport-failure arm"} +
                (observed_failure.has_value()
                     ? ": " + std::string{observed_failure.view()
                                              .as_bundle()
                                              .at("message")
                                              .checked_as<Str>()}
                     : ""));
    require(get_response_seen.load(), "the h2 GET did not complete");
    const auto response = observed_get_response.view().as_bundle();
    require(response.at("status").checked_as<Int>() == Int{200},
            "the h2 GET status was not 200");
    require(response.at("body").checked_as<Bytes>().data == "hi answer",
            "the h2 GET body (path capture) did not round-trip");
    bool served_by = false;
    for (const auto entry : response.at("headers").as_list()) {
      const auto pair = entry.as_bundle();
      served_by |= pair.at("name").checked_as<Str>() == Str{"x-served-by"} &&
                   pair.at("value").checked_as<Str>() == Str{"h2-loopback"};
    }
    require(served_by, "the h2 response header did not round-trip");
    // Trailers arrive on the TRAILERS field, distinct from headers — the
    // separation a gRPC-style terminal status depends on (review P1).
    bool trailer = false;
    const auto trailer_entries = response.at("trailers");
    require(trailer_entries.data() != nullptr,
            "the h2 response trailers field was not populated");
    for (const auto entry : trailer_entries.as_list()) {
      const auto pair = entry.as_bundle();
      trailer |= pair.at("name").checked_as<Str>() == Str{"x-trail"} &&
                 pair.at("value").checked_as<Str>() == Str{"h2-checksum"};
    }
    require(trailer, "the h2 response trailer did not arrive as a trailer");

    const auto peer = observed_server_peer.view().as_bundle();
    require(peer.at("negotiated_protocol").checked_as<Str>() == Str{"h2"},
            "the server-side peer did not record the h2 negotiation");
    require(peer.at("tls").checked_as<Bool>(),
            "the server-side peer did not record TLS");

    require(post_response_seen.load(), "the h2 POST did not complete");
    const auto post = observed_post_response.view().as_bundle();
    require(post.at("status").checked_as<Int>() == Int{200},
            "the h2 POST status was not 200");
    require(post.at("body").checked_as<Bytes>().data ==
                std::to_string(kBigBodyBytes),
            "the h2 POST body was not delivered intact");

    // The gRPC-shaped exchange: an EMPTY body must not fold the trailer
    // into the headers (review P1 — origin-based separation).
    require(status_response_seen.load(),
            "the trailers-only exchange did not complete");
    const auto status = observed_status_response.view().as_bundle();
    require(status.at("body").checked_as<Bytes>().data.empty(),
            "the trailers-only response grew a body");
    bool status_trailer = false;
    const auto status_trailers = status.at("trailers");
    require(status_trailers.data() != nullptr,
            "the trailers-only response has no trailers field");
    for (const auto entry : status_trailers.as_list()) {
      const auto pair = entry.as_bundle();
      status_trailer |=
          pair.at("name").checked_as<Str>() == Str{"grpc-like-status"} &&
          pair.at("value").checked_as<Str>() == Str{"0"};
    }
    require(status_trailer,
            "the empty-body trailer did not arrive as a trailer");
    for (const auto entry : status.at("headers").as_list()) {
      const auto pair = entry.as_bundle();
      require(pair.at("name").checked_as<Str>() != Str{"grpc-like-status"},
              "the empty-body trailer leaked into the headers");
    }

    release_test_state();
  } catch (const std::exception &error) {
    release_test_state();
    std::cerr << "hgraph_web_h2_loopback_tests failed: " << error.what()
              << "\n";
    return 1;
  }
  std::cout << "hgraph_web_h2_loopback_tests passed\n";
  return 0;
}
