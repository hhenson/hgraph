#pragma once
#include <native-result.h>
namespace checks::native_result_provider {
struct Implementation {
    static hgl::ordinary::Result<hgraph::Int, hgraph::CanonicalError> checked(hgraph::Int value) {
        using Outcome = hgl::ordinary::Result<hgraph::Int, hgraph::CanonicalError>;
        if (value < 0) { return Outcome::err(hgraph::CanonicalError{"value.missing_key", "native missing price", {}}); }
        return Outcome::ok(value);
    }
};
inline constexpr auto native = checks::native_result::native_interface::bind<Implementation>();
}
