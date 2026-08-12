#include <hgraph/analytics/operators.h>

#include <hgraph/lib/std/operators/conversion.h>
#include <hgraph/lib/std/operators/registration.h>
#include <hgraph/types/graph_wiring.h>

namespace
{
    namespace hg = hgraph;
    namespace hga = hgraph::analytics;

    struct InstalledConsumerGraph
    {
        static constexpr auto name = "installed_hgraph_analytics_consumer";

        static void compose(hg::Wiring &w)
        {
            auto input = hg::wire<hg::stdlib::const_, hg::TS<hg::Float>>(
                w, hg::Float{100.0});
            static_cast<void>(hg::wire<hga::pct_change, hg::TS<hg::Float>>(
                w, input, hg::Int{12}, hg::stdlib::DivideByZero::Nan));
        }
    };
}  // namespace

int main()
{
    hgraph::stdlib::register_standard_operators();
    hgraph::analytics::register_analytics_operators();
    auto graph = hgraph::build_graph<InstalledConsumerGraph>();
    return graph.node_count() == 0 ? 1 : 0;
}
