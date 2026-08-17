// A standalone HTTP/2 server for h2spec conformance runs (RFC 0024, h2
// acceptance criteria).  Starts a real graph serving GET / over TLS with
// ALPN ["h2"] on the requested port (default 8443), prints
// "h2spec-server listening <port>" once bound, and runs until the
// duration expires or the process is killed:
//
//   hgraph_web_h2spec_server [port] [seconds]
//   h2spec http2 -t -k -h 127.0.0.1 -p <port>

#include <hgraph/web/service.h>
#include <hgraph/web/value_builders.h>

#include <hgraph/lib/std/operators/conversion.h>
#include <hgraph/lib/std/operators/registration.h>
#include <hgraph/lib/std/std_nodes.h>
#include <hgraph/lib/testing/runtime_support.h>
#include <hgraph/runtime/runtime.h>

#include <atomic>
#include <chrono>
#include <cstdlib>
#include <iostream>
#include <string>

namespace {
using namespace hgraph;
using namespace hgraph::web;
using namespace hgraph::testing;
using namespace std::chrono_literals;

// The same 100-year loopback certificate the h2 loopback suite embeds.
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

inline std::atomic<bool> announced{false};

struct SpecAnnounce {
  static constexpr auto name = "web_h2spec_announce";

  static void eval(In<"stats", TS<WebServerStats>, InputValidity::Unchecked>
                       stats) {
    if (!stats.valid() || !stats.modified() || announced.load()) {
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
    announced.store(true);
    std::cout << "h2spec-server listening " << port << std::endl;
  }
};

struct SpecId {
  static constexpr auto name = "web_h2spec_id";

  static void eval(In<"routed", WebRouteOutput, InputValidity::Unchecked>
                       routed,
                   Out<TS<Int>> out) {
    auto request = routed.template field<"request">();
    if (request.valid() && request.modified()) {
      out.apply(request.base().value().as_bundle().at("request_id"));
    }
  }
};

struct SpecResponse {
  static constexpr auto name = "web_h2spec_response";

  static void eval(In<"routed", WebRouteOutput, InputValidity::Unchecked>
                       routed,
                   Out<TS<HttpResponse>> out) {
    auto request = routed.template field<"request">();
    if (request.valid() && request.modified()) {
      const Value response = make_response(Int{200}, {}, Bytes{"conformant"});
      out.apply(response.view());
    }
  }
};

int requested_port = 8443;

struct H2SpecGraph {
  static constexpr auto name = "web_h2spec_graph";

  static void compose(Wiring &w) {
    const auto server_path = service::path("web-h2spec-server");
    register_server(w, server_path,
                    server_config()
                        .port(static_cast<Int>(requested_port))
                        .request_timeout(5'000ms)
                        .stats_interval(100ms)
                        .tls(tls_server()
                                 .cert_pem(Str{kTestCertPem})
                                 .key_pem(Str{kTestKeyPem})
                                 .alpn({Str{"h2"}})
                                 .build())
                        .build());
    auto stats = server_stats(w, server_path);
    static_cast<void>(wire<SpecAnnounce>(w, stats));

    // h2spec exercises GET and POST on "/"; the rest-capture route serves
    // every path so no case sees a transport 404.
    auto route = wire<stdlib::const_, TS<WebRoute>>(
        w, make_route(HttpMethod::Get, "/*rest"));
    auto served = serve(w, server_path, route);
    auto id = wire<SpecId>(w, served).as<TS<Int>>();
    auto response = wire<SpecResponse>(w, served).as<TS<HttpResponse>>();
    static_cast<void>(
        respond(w, server_path, respond_request(w, id, response)));

    auto post_route = wire<stdlib::const_, TS<WebRoute>>(
        w, make_route(HttpMethod::Post, "/*rest"));
    auto post_served = serve(w, server_path, post_route);
    auto post_id = wire<SpecId>(w, post_served).as<TS<Int>>();
    auto post_response =
        wire<SpecResponse>(w, post_served).as<TS<HttpResponse>>();
    static_cast<void>(respond(
        w, server_path, respond_request(w, post_id, post_response)));
  }
};

} // namespace

int main(int argc, char **argv) {
  try {
    if (argc > 1) {
      requested_port = std::atoi(argv[1]);
    }
    int seconds = 600;
    if (argc > 2) {
      seconds = std::atoi(argv[2]);
    }
    hgraph::stdlib::register_standard_operators();
    register_web_types();

    const DateTime start = wall_now();
    GraphExecutorBuilder executor_builder;
    executor_builder.graph_builder(build_graph<H2SpecGraph>())
        .mode(GraphExecutorMode::RealTime)
        .start_time(start)
        .end_time(start + TimeDelta{static_cast<std::int64_t>(seconds) *
                                    1'000'000});
    auto executor = executor_builder.make_executor();
    auto view = executor.view();
    {
      AsyncGraphExecutorRun runner{view};
      runner.join();
    }
    return 0;
  } catch (const std::exception &error) {
    std::cerr << "h2spec server failed: " << error.what() << "\n";
    return 1;
  }
}
