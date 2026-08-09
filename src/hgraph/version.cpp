#include <hgraph/version.h>

namespace hgraph {

std::string_view version() noexcept {
    return version_string;
}

}  // namespace hgraph
