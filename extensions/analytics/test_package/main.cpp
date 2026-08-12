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
            auto reset = hg::wire<hg::stdlib::const_, hg::TS<hg::Bool>>(
                w, hg::Bool{false});
            static_cast<void>(hg::wire<hga::diff>(w, input));
            static_cast<void>(hg::wire<hga::count>(w, input));
            static_cast<void>(hg::wire<hga::count>(w, input, reset));
            static_cast<void>(hg::wire<hga::clip>(
                w, input, hg::Float{0.0}, hg::Float{200.0}));
            static_cast<void>(hg::wire<hga::ewma>(w, input, hg::Float{0.2}));
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
