#include <hgraph/web/types.h>
#include <hgraph/web/value_builders.h>

#include <hgraph/lib/std/operators/conversion.h>
#include <hgraph/lib/std/operators/registration.h>
#include <hgraph/types/graph_wiring.h>
#include <hgraph/types/static_node.h>

namespace
{
    using namespace hgraph;
    using namespace hgraph::web;

    struct ConsumeOptions
    {
        static void eval(In<"options", TS<HttpClientOptions>>) {}
    };

    struct InstalledConsumerGraph
    {
        static constexpr auto name = "installed_hgraph_web_consumer";

        static void compose(Wiring &w)
        {
            auto options = wire<stdlib::const_, TS<HttpClientOptions>>(
                w, make_client_options(1500, 2250, true, 5,
                    WebHttpVersionPolicy::Auto, true, "user", "password"));
            wire<ConsumeOptions>(w, options);
        }
    };
}  // namespace

int main()
{
    stdlib::register_standard_operators();
    register_web_types();
    const Value key = make_ws_client_key("ws://localhost/live", {}, {}, "independent");
    if (key.view().as_bundle().at("identity").checked_as<Str>() != "independent") { return 1; }
    auto graph = build_graph<InstalledConsumerGraph>();
    return graph.node_count() == 0 ? 1 : 0;
}
