#include <hgraph/analytics/operators.h>

#include <hgraph/lib/std/operators/conversion.h>
#include <hgraph/lib/std/operators/registration.h>
#include <hgraph/lib/std/operators/stream.h>
#include <hgraph/types/graph_wiring.h>
#include <hgraph/types/metadata/value_plan_factory.h>

#include <cstddef>
#include <utility>

namespace
{
    namespace hg = hgraph;
    namespace hga = hgraph::analytics;

    using RollingFloat4 = hg::TSB<
        "RollingWindowResult[float,4]",
        hg::Field<"buffer", hg::TS<hg::ArrayOf<hg::Float, 4>>>,
        hg::Field<"index", hg::TS<hg::ArrayOf<hg::DateTime, 4>>>>;

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
            static_cast<void>(hg::wire<hga::std_, hg::TS<hg::Float>>(w, input));
            static_cast<void>(hg::wire<hga::var_, hg::TS<hg::Float>>(w, input));
            static_cast<void>(hg::wire<hga::rolling_mean, hg::TS<hg::Float>>(
                w, input, hg::Int{4}, hg::Int{2}));
            static_cast<void>(hg::wire<hga::resample>(w, input, hg::MIN_TD));

            hg::Value array{hg::ValuePlanFactory::instance().type_for(
                hg::TypeRegistry::instance().array(
                    hg::scalar_descriptor<hg::Float>::value_meta(), 4))};
            auto items = array.as_list().begin_mutation();
            items.resize(4);
            for (std::size_t index = 0; index < 4; ++index)
            {
                hg::Value item{static_cast<hg::Float>(index + 1)};
                items.at(index).copy_from(item.view());
            }
            auto array_input = hg::wire<hg::stdlib::const_,
                                        hg::TS<hg::ArrayOf<hg::Float, 4>>>(
                w, std::move(array));
            auto q = hg::wire<hg::stdlib::const_, hg::TS<hg::Float>>(
                w, hg::Float{0.5});
            static_cast<void>(hg::wire<hga::quantile>(w, array_input, q));
            static_cast<void>(hg::wire<hga::array_std>(w, array_input, hg::Int{1}));

            auto window = hg::wire<hg::stdlib::to_window>(w, input, hg::Int{4})
                              .as<hg::TSW<hg::Float, 4, 4>>();
            static_cast<void>(
                hg::wire<hga::rolling_window, RollingFloat4>(w, window));
            static_cast<void>(hg::wire<hga::std_, hg::TS<hg::Float>>(
                w, window, hg::arg<"ddof">(hg::Int{1})));
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
