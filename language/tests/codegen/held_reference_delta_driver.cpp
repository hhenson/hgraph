#include "driver/driver.h"
#include <held-reference-delta.h>
#include <replay_record.h>
#include <hgraph/version.h>

#include <string_view>
#include <vector>

namespace {
void prepare_eval() { static_cast<void>(hgraph_::std_::register_operators()); }
void register_runtime() { static_cast<void>(checks::held_reference_delta::register_operators()); }
}

int main(int argc, char **argv) {
    std::vector<std::string_view> arguments;
    for (int index = 1; index < argc; ++index) { arguments.emplace_back(argv[index]); }
    return hgl::driver::run(arguments, hgraph::release_version_string, prepare_eval, register_runtime);
}
