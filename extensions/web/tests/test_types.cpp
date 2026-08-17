#include <hgraph/web/service.h>
#include <hgraph/web/value_builders.h>

#include <iostream>
#include <stdexcept>
#include <string>
#include <utility>

namespace {
using namespace hgraph;
using namespace hgraph::web;

void require(bool condition, std::string message) {
  if (!condition) {
    throw std::runtime_error(std::move(message));
  }
}

template <typename Fn> void require_invalid(Fn &&fn, std::string message) {
  bool rejected = false;
  try {
    std::forward<Fn>(fn)();
  } catch (const std::invalid_argument &) {
    rejected = true;
  }
  require(rejected, std::move(message));
}

[[nodiscard]] Str bundle_string(const Value &value, std::string_view field) {
  return value.view().as_bundle().at(field).checked_as<Str>();
}

[[nodiscard]] Int bundle_int(const Value &value, std::string_view field) {
  return value.view().as_bundle().at(field).checked_as<Int>();
}

void test_registration_is_idempotent() {
  register_web_types();
  register_web_types();
}

void test_route_values() {
  const Value route = make_route(HttpMethod::Get, "/api/orders/{id}");
  require(bundle_string(route, "pattern") == "/api/orders/{id}",
          "route pattern round-trips");
  require(!route.view().as_bundle().at("upgrade").checked_as<Bool>(),
          "routes default to non-upgrade");
  require_invalid([] { static_cast<void>(make_route(HttpMethod::Get, "orders")); },
                  "route patterns must start with '/'");
}

void test_headers_preserve_duplicates_and_order() {
  const Value headers = make_headers({{"Set-Cookie", "a=1"}, {"set-cookie", "b=2"}, {"X-One", "1"}});
  const auto list = headers.view().as_list();
  require(list.size() == 3, "duplicate header names are preserved");
  require(list.at(0).as_bundle().at("value").checked_as<Str>() == "a=1",
          "header order is arrival order");
  require_invalid([] { static_cast<void>(make_headers({{"", "x"}})); },
                  "header names cannot be empty");
}

void test_response_values() {
  const Value response = make_response(204);
  require(bundle_int(response, "status") == 204, "response status round-trips");
  require_invalid([] { static_cast<void>(make_response(99)); },
                  "response status must be 100..599");
}

void test_frames() {
  const Value text = make_text_frame("hello");
  require(text.view().as_bundle().at("kind").checked_as<WsFrameKind>() == WsFrameKind::Text,
          "text frames carry the Text kind");
  require_invalid([] { static_cast<void>(make_close_frame(999)); },
                  "close codes must be 1000..4999");
}

void test_server_config_defaults_and_validation() {
  const Value config = server_config().port(8080).build();
  require(bundle_int(config, "port") == 8080, "server port round-trips");
  require(bundle_int(config, "watermark_high_pct") == 80, "high watermark defaults to 80");
  require(bundle_int(config, "watermark_low_pct") == 50, "low watermark defaults to 50");
  require(!config.view().as_bundle().at("tls").has_value(),
          "an absent TLS config means a plaintext listener");
  require_invalid([] { static_cast<void>(server_config().port(65'536).build()); },
                  "server ports beyond 65535 are rejected");
  require_invalid([] { static_cast<void>(server_config().watermarks(50, 80).build()); },
                  "inverted watermarks are rejected");
}

void test_tls_server_config_validation() {
  const Value tls = tls_server().cert_path("/tmp/cert.pem").key_path("/tmp/key.pem").build();
  require(bundle_string(tls, "cert_path") == "/tmp/cert.pem", "TLS cert path round-trips");
  require_invalid([] { static_cast<void>(tls_server().key_path("/tmp/key.pem").build()); },
                  "server TLS requires a certificate");
  require_invalid(
      [] {
        static_cast<void>(
            tls_server().cert_path("/tmp/cert.pem").cert_pem("inline").key_path("/tmp/key.pem").build());
      },
      "certificate path and inline PEM are mutually exclusive");
  require_invalid(
      [] {
        static_cast<void>(tls_server()
                              .cert_path("/tmp/cert.pem")
                              .key_path("/tmp/key.pem")
                              .client_verify(WebClientVerify::Required)
                              .build());
      },
      "client-certificate verification requires a CA");
  // Advertising h2 before the server HTTP/2 session activates is a protocol
  // violation and stays a wiring-time error (RFC 0024).
  require_invalid(
      [] {
        static_cast<void>(tls_server()
                              .cert_path("/tmp/cert.pem")
                              .key_path("/tmp/key.pem")
                              .alpn({"h2", "http/1.1"})
                              .build());
      },
      "server ALPN cannot advertise h2 before activation");
}

void test_client_config_validation() {
  const Value config = client_config().build();
  require(config.view().as_bundle().at("http_version_policy").checked_as<WebHttpVersionPolicy>() ==
              WebHttpVersionPolicy::Auto,
          "client HTTP version policy defaults to Auto");
  // The response reservation must fit the response channel (RFC 0024, flow
  // control): a max_response_bytes above the ingress byte limit could never
  // reserve.
  require_invalid(
      [] {
        static_cast<void>(
            client_config().max_response_bytes(128 * 1024 * 1024).ingress_limits(10'000, 64 * 1024 * 1024).build());
      },
      "the response reservation must fit the ingress byte limit");
}

void test_service_descriptor_names_are_distinct() {
  require(HttpServeService::name != HttpRespondService::name &&
              WsServeService::name != WsSendService::name &&
              HttpClientService::name != WsClientService::name,
          "service descriptor names are distinct");
}
}  // namespace

int main() {
  try {
    test_registration_is_idempotent();
    test_route_values();
    test_headers_preserve_duplicates_and_order();
    test_response_values();
    test_frames();
    test_server_config_defaults_and_validation();
    test_tls_server_config_validation();
    test_client_config_validation();
    test_service_descriptor_names_are_distinct();
  } catch (const std::exception &error) {
    std::cerr << "hgraph_web_tests failed: " << error.what() << "\n";
    return 1;
  }
  std::cout << "hgraph_web_tests passed\n";
  return 0;
}
