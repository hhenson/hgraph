#include "driver/driver.h"
#include <standard.h>
#include <hgraph/version.h>

#include <string_view>
#include <vector>

namespace {
void register_runtime() { static_cast<void>(hgraph_::std_::register_operators()); }
}

int main(int argc, char **argv) {
    std::vector<std::string_view> arguments;
    for (int index = 1; index < argc; ++index) { arguments.emplace_back(argv[index]); }
    return hgl::driver::run(arguments, hgraph::release_version_string, nullptr, register_runtime);
}
