// A worker program (RFC 0037).
//
// What a host application does to take part in a distributed run, and nothing
// else: register the types and operators it uses, register the children a
// worker may be asked to build, then hand ``argv`` to the runtime before doing
// its own work. Launched without the worker flags it serves nothing and says
// so, which is the same shape a real application's ``main`` has.
//
// It is a SEPARATE executable here only because the caller is a Catch2 binary
// whose ``main`` belongs to Catch2. An ordinary application is its own worker.

#include <hgraph/lib/std/std_operators.h>
#include <hgraph/runtime/distributed_worker.h>
#include <hgraph/types/metadata/type_registry.h>

#include "distributed_worker_recipes.h"

#include <cstdio>
#include <exception>
#include <chrono>
#include <thread>
#include <vector>
#ifdef _WIN32
#include <windows.h>
#endif

namespace hgraph_test { void register_spawn_test_recipes(); }

int run_host(int argc, char **argv)
{
    try
    {
        if (argc > 1 && (std::string_view{argv[1]} == "--test-hang-bootstrap" ||
                         std::string_view{argv[1]} == "--test-hang-reply"))
        {
            if (std::string_view{argv[1]} == "--test-hang-reply")
            {
                using namespace hgraph::distributed;
                std::int64_t read = -1, write = -1;
                for (int i = 2; i < argc; ++i)
                {
                    const std::string_view argument{argv[i]};
                    if (argument.starts_with(worker_read_flag)) read = std::stoll(std::string{argument.substr(worker_read_flag.size())});
                    if (argument.starts_with(worker_write_flag)) write = std::stoll(std::string{argument.substr(worker_write_flag.size())});
                }
                auto channel = PipeEndpoint::adopt(read, write);
                std::string message;
                (void)channel.receive(message);
                // Retain the channel while simulating a node that never returns.
                while (true) std::this_thread::sleep_for(std::chrono::seconds{30});
            }
            while (true) std::this_thread::sleep_for(std::chrono::seconds{30});
        }
        (void)hgraph::TypeRegistry::instance().register_scalar<hgraph::Int>("int");
        hgraph::stdlib::register_standard_operators();
        hgraph_test::register_distributed_test_recipes();
        hgraph_test::register_spawn_test_recipes();

        if (!hgraph::distributed::run_worker_if_requested(argc, argv))
        {
            std::fputs("hgraph distributed worker host: not launched as a worker\n", stderr);
            return 2;
        }
        return 0;
    }
    catch (const std::exception &error)
    {
        std::fprintf(stderr, "hgraph distributed worker host: %s\n", error.what());
        return 1;
    }
}

#ifdef _WIN32
// A native Windows embedding receives Unicode argv through wmain and passes
// UTF-8 to hgraph, matching the public process/recipe string contract.
int wmain(int argc, wchar_t **argv)
{
    std::vector<std::string> arguments;
    arguments.reserve(static_cast<std::size_t>(argc));
    for (int i = 0; i < argc; ++i)
    {
        const int length = ::WideCharToMultiByte(CP_UTF8, WC_ERR_INVALID_CHARS, argv[i], -1, nullptr, 0, nullptr, nullptr);
        if (length <= 0) return 1;
        std::string utf8(static_cast<std::size_t>(length), '\0');
        if (!::WideCharToMultiByte(CP_UTF8, WC_ERR_INVALID_CHARS, argv[i], -1, utf8.data(), length, nullptr, nullptr)) return 1;
        utf8.pop_back();
        arguments.push_back(std::move(utf8));
    }
    std::vector<char *> narrow;
    for (auto &argument : arguments) narrow.push_back(argument.data());
    narrow.push_back(nullptr);
    return run_host(argc, narrow.data());
}
#else
int main(int argc, char **argv) { return run_host(argc, argv); }
#endif
