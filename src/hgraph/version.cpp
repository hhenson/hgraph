#include <hgraph/version.h>

namespace hgraph {

std::string_view version() noexcept {
    return version_string;
}

std::string_view release_version() noexcept {
    return release_version_string;
}

}  // namespace hgraph
