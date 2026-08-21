#include <hgraph/fabric/fabric.h>

#include <hgraph/lib/std/operators/registration.h>
#include <hgraph/types/graph_wiring.h>

#include <span>

namespace
{
    namespace hg  = hgraph;
    namespace hgf = hgraph::fabric;

    struct InstalledFabricGraph
    {
        static constexpr auto name = "installed_hgraph_fabric_consumer";

        static void compose(hg::Wiring &wiring)
        {
            auto input = hgf::subscribe_data(
                wiring, "installed/input", hgf::SubscriptionMode::Live);
            hgf::publish_data(wiring, "installed/output", input);
        }
    };
}  // namespace

int main()
{
    hg::stdlib::register_standard_operators();
    hgf::register_fabric_operators();

    hg::GlobalContext context;
    auto state = context.state().view();
    hgf::set_fabric_config(
        state, hgf::make_memory_fabric_config("installed/fabric"));
    const auto config = hgf::fabric_config(state);
    if (!config.has_value() || config->prefix != "installed/fabric")
    {
        return 1;
    }

    hg::Value revision = hgf::make_data_revision(hgf::DataRevisionInput{
        .data_id = "installed/output",
        .revision = 1,
        .output_version = 7,
        .dependencies = {{"installed/input", 3}},
        .as_of = hg::MIN_ST,
    });
    const auto encoded = hgf::encode_revision(revision.view());
    const auto decoded = hgf::decode_revision(encoded);
    if (hgf::data_revision_input(decoded.view()) !=
        hgf::data_revision_input(revision.view()))
    {
        return 2;
    }

    auto graph = hg::build_graph<InstalledFabricGraph>();
    return graph.node_count() == 2 ? 0 : 3;
}
