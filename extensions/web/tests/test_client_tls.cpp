#include "detail/ca_bundle.h"

#include <hgraph/lib/std/operators/registration.h>
#include <hgraph/lib/testing/eval_node.h>
#include <hgraph/lib/testing/runtime_support.h>
#include <hgraph/util/scope.h>
#include <hgraph/web/service.h>
#include <hgraph/web/value_builders.h>

#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <iterator>
#include <stdexcept>
#include <string>
#include <vector>

namespace
{
    using namespace hgraph;
    using namespace hgraph::web;
    using namespace hgraph::testing;
    using namespace std::chrono_literals;

    void require(bool condition, const char *message) {
        if (!condition) { throw std::runtime_error(message); }
    }

    const auto fixtures = std::filesystem::path{__FILE__}.parent_path() / "fixtures";

    void set_ca_environment(const std::optional<std::string> &value) {
#ifdef _WIN32
        require(_putenv_s("SSL_CERT_FILE", value ? value->c_str() : "") == 0, "could not set CA environment");
#else
        require((value ? setenv("SSL_CERT_FILE", value->c_str(), 1) : unsetenv("SSL_CERT_FILE")) == 0,
                "could not set CA environment");
#endif
    }

    void test_bundle_selection() {
        using hgraph::web::detail::select_ca_bundle;
        const std::vector<std::string> paths{"/etc/ssl/certs/ca-certificates.crt", "/etc/pki/tls/certs/ca-bundle.crt",
                                             "/etc/ssl/cert.pem"};
        for (std::size_t first = 0; first < paths.size(); ++first) {
            std::vector<std::string> visited;
            const auto               selected = select_ca_bundle("", [&](const char *path) {
                visited.emplace_back(path);
                return visited.size() > first;
            });
            require(selected == paths[first], "wrong platform fallback selected");
            require(visited == std::vector<std::string>(paths.begin(), paths.begin() + first + 1),
                    "fallback paths were not probed in order");
        }
        require(select_ca_bundle("", [](const char *) { return false; }).empty(), "no bundle must preserve curl defaults");
        require(select_ca_bundle("missing-custom.pem",
                                 [](const char *) -> bool {
                                     throw std::runtime_error("explicit environment probed system paths");
                                 }) == "missing-custom.pem",
                "environment override was discarded");
    }

    enum class Trust { Environment, Path, Pem };
    inline Trust trust{};
    inline bool  requested{};
    inline bool  succeeded{};
    inline bool  failed{};
    inline Int   failure_code{};

    struct TlsRequest
    {
        static void eval(In<"stats", TS<WebServerStats>> stats, Out<TS<HttpClientRequest>> out) {
            const auto port = stats.base().value().as_bundle().at("listening_port").checked_as<Int>();
            if (port != 0 && !requested) {
                requested = true;
                out.apply(make_client_request(HttpMethod::Get, "https://127.0.0.1:" + std::to_string(port) + "/cert").view());
            }
        }
    };

    struct CaptureTlsResult
    {
        static void eval(NodeView node, In<"result", HttpCallResult, InputValidity::Unchecked> result) {
            auto response = result.template field<"response">();
            auto failure  = result.template field<"failure">();
            if (response.valid()) {
                succeeded = response.base().value().as_bundle().at("status").checked_as<Int>() == 200;
                node.graph().executor().request_stop();
            }
            if (failure.valid()) {
                failed       = true;
                failure_code = failure.base().value().as_bundle().at("error_code").checked_as<Int>();
                node.graph().executor().request_stop();
            }
        }
    };

    struct TlsLoopback
    {
        static Port<HttpCallResult> compose(Wiring &w, Port<TS<Bool>>) {
            const auto server = service::path("tls-server");
            const auto client = service::path("tls-client");
            const auto cert   = (fixtures / "localhost-cert.pem").string();
            register_server(w, server,
                            server_config()
                                .port(0)
                                .stats_interval(10ms)
                                .tls(tls_server().cert_path(cert).key_path((fixtures / "localhost-key.pem").string()).build())
                                .static_file("/cert", cert)
                                .build());
            auto config = client_config();
            if (trust == Trust::Path) {
                config.tls(tls_client().ca_path(cert).build());
            } else if (trust == Trust::Pem) {
                std::ifstream     input{cert};
                const std::string pem{std::istreambuf_iterator<char>{input}, {}};
                config.tls(tls_client().ca_pem(pem).build());
            }
            register_client(w, client, config.build());
            auto request = wire<TlsRequest>(w, server_stats(w, server)).as<TS<HttpClientRequest>>();
            auto result  = http_request(w, client, http_client_call(w, request));
            static_cast<void>(wire<CaptureTlsResult>(w, result));
            return result;
        }
    };

    void test_verified_tls(Trust mode, bool valid_environment, bool expect_success) {
        trust     = mode;
        requested = succeeded = failed = false;
        set_ca_environment((fixtures / (valid_environment ? "localhost-cert.pem" : "missing.pem")).string());
        const auto start = wall_now();
        static_cast<void>(eval_node_with_options<TlsLoopback>(
            EvalNodeRunOptions{.start_time = start, .end_time = start + TimeDelta{5'000'000}, .mode = GraphExecutorMode::RealTime},
            std::vector<std::optional<Bool>>{true}));
        require(requested, "TLS request was never sent");
        require(succeeded == expect_success && failed != expect_success, "verified TLS did not respect CA precedence");
        // CURLE_SSL_CACERT_BADFILE: distinguish the intended configuration
        // error from an unrelated connection or certificate-verification error.
        if (!expect_success) { require(failure_code == 77, "missing CA file did not report a CA-file error"); }
    }
}  // namespace

int main() {
    try {
        const auto previous = environment_variable("SSL_CERT_FILE");
        const auto restore  = make_scope_exit([&] { set_ca_environment(previous); });
        stdlib::register_standard_operators();
        register_web_types();
        test_bundle_selection();
        test_verified_tls(Trust::Environment, true, true);
        test_verified_tls(Trust::Environment, false, false);
        test_verified_tls(Trust::Path, false, true);
        test_verified_tls(Trust::Pem, false, true);
        std::cout << "hgraph-web client TLS tests passed\n";
        return 0;
    } catch (const std::exception &error) {
        std::cerr << error.what() << '\n';
        return 1;
    }
}
