#pragma once
#include <native-result.h>
#include <vector>
namespace checks::native_result_provider {
inline std::vector<hgraph::Int> diagnostic_arguments;
struct Implementation {
    static hgl::ordinary::Result<hgraph::Int, hgraph::CanonicalError> checked(hgraph::Int value) {
        diagnostic_arguments.push_back(value);
        using Outcome = hgl::ordinary::Result<hgraph::Int, hgraph::CanonicalError>;
        if (value < 0) { return Outcome::err(hgraph::CanonicalError{"value.missing_key", "native missing price", {}}); }
        return Outcome::ok(value);
    }
};
inline constexpr auto native = checks::native_result::native_interface::bind<Implementation>();
}
