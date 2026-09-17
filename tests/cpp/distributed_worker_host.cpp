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

int main(int argc, char **argv)
{
    try
    {
        (void)hgraph::TypeRegistry::instance().register_scalar<hgraph::Int>("int");
        hgraph::stdlib::register_standard_operators();
        hgraph_test::register_distributed_test_recipes();

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
