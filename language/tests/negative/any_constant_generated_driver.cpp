#include "driver/driver.h"
#include <any-constant-positive.h>
#include <hgraph/version.h>

#include <string_view>
#include <vector>

namespace
{
    void register_runtime() { static_cast<void>(examples::reject::any_constant_capability::register_operators()); }
}  // namespace

int main(int argc, char **argv) {
    std::vector<std::string_view> arguments;
    for (int index = 1; index < argc; ++index) { arguments.emplace_back(argv[index]); }
    return hgl::driver::run(arguments, hgraph::release_version_string, nullptr, register_runtime);
}
