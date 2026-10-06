#include "driver/driver.h"
#include "driver/rejection_source.h"
#include <hgraph/version.h>

#include <fstream>
#include <iostream>
#include <iterator>
#include <string>
#include <string_view>
#include <vector>

// Test infrastructure only: reuse the driver's parsed owner ranges both for
// frontend-only rejection runs and for building the untouched positive cases.
int main(int argc, char **argv) {
    if (argc < 3) { return 2; }
    const std::string mode{argv[1]};
    std::ifstream input{argv[2]};
    if (!input) { return 2; }
    std::string source{std::istreambuf_iterator<char>{input}, {}};
    const hgl::syntax::SourceFile file{argv[2], source};
    const auto inspected = hgl::driver::inspect_rejection_source(file, std::cerr);
    if (!inspected.valid || inspected.cases.empty()) { return 1; }
    if (mode == "prepare" && argc == 4) {
        for (const auto &owner : inspected.cases) {
            for (auto offset = owner.range.begin; offset < owner.range.end; ++offset) {
                if (source[offset] != '\n' && source[offset] != '\r') { source[offset] = ' '; }
            }
        }
        std::ofstream output{argv[3]};
        output << source;
        return output ? 0 : 2;
    }
    if (mode == "reject" && argc == 3) {
        std::vector<std::string_view> arguments{"test", argv[2]};
        for (const auto &owner : inspected.cases) {
            if (owner.named_test) { arguments.push_back(owner.name); }
        }
        // An empty selection would also execute positive tests. This fixture
        // always contains named rejection cases; fail closed if it changes.
        if (arguments.size() == 2) { return 2; }
        return hgl::driver::run(arguments, hgraph::release_version_string);
    }
    return 2;
}
