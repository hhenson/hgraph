#include <hgraph/web/types.h>

#include <hgraph/lib/std/operators/conversion.h>
#include <hgraph/lib/std/operators/registration.h>
#include <hgraph/types/graph_wiring.h>

namespace
{
    using namespace hgraph;
    using namespace hgraph::web;

    struct InstalledConsumerGraph
    {
        static constexpr auto name = "installed_hgraph_web_consumer";

        static void compose(Wiring &w)
        {
            // The serve, respond, connect, and call wiring surface lands with
            // the service implementation (RFC 0024). Until then the installed
            // package is exercised by registering the web schemas against a
            // graph built from the same SDK.
            static_cast<void>(
                wire<stdlib::const_, TS<Str>>(w, Str{"installed-consumer"}));
        }
    };
}  // namespace

int main()
{
    stdlib::register_standard_operators();
    register_web_types();
    auto graph = build_graph<InstalledConsumerGraph>();
    return graph.node_count() == 0 ? 1 : 0;
}
