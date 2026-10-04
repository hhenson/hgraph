#include "driver/driver.h"

#include <hgraph/version.h>

#include <string_view>
#include <vector>

#ifdef HGL_COMPILED_EVAL_LIBRARY
#include <replay_record.h>
namespace {
    void prepare_eval_library() { static_cast<void>(hgraph_::std_::register_operators()); }
}
#endif

int main(int argc, char **argv)
{
    std::vector<std::string_view> arguments;
    arguments.reserve(static_cast<std::size_t>(argc > 0 ? argc - 1 : 0));
    for (int i = 1; i < argc; ++i) { arguments.emplace_back(argv[i]); }
    #ifdef HGL_COMPILED_EVAL_LIBRARY
    return hgl::driver::run(arguments, hgraph::release_version_string, prepare_eval_library);
#else
    return hgl::driver::run(arguments, hgraph::release_version_string);
#endif
}
