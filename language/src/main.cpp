#include <hgraph/version.h>

#include <iostream>
#include <string_view>

namespace
{
    constexpr std::string_view language_version{HGRAPH_LANGUAGE_VERSION};

    void print_help()
    {
        std::cout << "hgl - experimental hgraph language toolchain\n\n"
                     "Usage:\n"
                     "  hgl --help\n"
                     "  hgl --version\n\n"
                     "The compiler frontend is not implemented yet. Planned commands are\n"
                     "check, emit-cpp, run, build, and repl.\n";
    }

    void print_version()
    {
        std::cout << "hgl " << language_version << " (hgraph " << hgraph::version_string << ")\n";
    }
}  // namespace

int main(int argc, char **argv)
{
    if (argc == 1)
    {
        print_help();
        return 0;
    }

    const std::string_view argument{argv[1]};
    if (argc == 2 && (argument == "--help" || argument == "-h"))
    {
        print_help();
        return 0;
    }
    if (argc == 2 && argument == "--version")
    {
        print_version();
        return 0;
    }

    std::cerr << "hgl: unsupported command; use --help for the current interface\n";
    return 2;
}
