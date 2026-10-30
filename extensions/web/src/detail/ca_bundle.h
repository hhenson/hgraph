#pragma once

#include <hgraph/util/environment.h>

#include <array>
#include <filesystem>
#include <fstream>
#include <string>
#include <string_view>

namespace hgraph::web::detail
{

    // Resolve on the deployment host, not the host that compiled libcurl (for
    // example a manylinux wheel builder). An explicit environment override is
    // authoritative even when invalid: let curl report that configuration error.
    template <typename Readable> [[nodiscard]] std::string select_ca_bundle(std::string_view ssl_cert_file, Readable readable) {
        if (!ssl_cert_file.empty()) { return std::string{ssl_cert_file}; }
        constexpr std::array paths{
            "/etc/ssl/certs/ca-certificates.crt",  // Debian / Ubuntu
            "/etc/pki/tls/certs/ca-bundle.crt",    // RHEL
            "/etc/ssl/cert.pem",                   // macOS
        };
        for (const char *path : paths) {
            if (readable(path)) { return path; }
        }
        return {};  // Preserve libcurl's defaults on other installations/backends.
    }

    [[nodiscard]] inline std::string default_ca_bundle() {
        return select_ca_bundle(environment_variable("SSL_CERT_FILE").value_or(""), [](const char *path) {
            std::error_code error;
            return std::filesystem::is_regular_file(path, error) && std::ifstream{path}.good();
        });
    }

}  // namespace hgraph::web::detail
