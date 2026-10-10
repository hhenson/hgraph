#include "driver/driver.h"
#include <standard.h>
#include <string_view>
#include <vector>

namespace
{
    void prepare() { static_cast<void>(hgraph_::std_::register_operators()); }
}  // namespace
int main(int argc, char **argv) {
    std::vector<std::string_view> arguments;
    for (int index = 1; index < argc; ++index) { arguments.emplace_back(argv[index]); }
    return hgl::driver::run(arguments, "native-atomic-fixture", &prepare, &prepare);
}
