#include <hgraph/persistence/frame_store.h>
#include <hgraph/persistence/recording_store.h>

#include <hgraph/runtime/global_state.h>
#include <hgraph/types/operator_dispatch.h>

#include <iostream>
#include <stdexcept>

namespace
{
    namespace hg = hgraph;
    namespace hgp = hgraph::persistence;

    void require(bool condition, const char *what)
    {
        if (!condition)
        {
            throw std::runtime_error(what);
        }
    }
}  // namespace

int main()
{
    try
    {
        // The installed extension registers its backend through the shared
        // runtime's keyed-installer mechanism.
        hgp::register_frame_backend();
        require(!hg::OperatorRegistry::instance()
                     .overload_signatures("record")
                     .empty(),
                "durable record overload registered");

        // The GlobalState-scoped store round-trips a frame with immutable
        // keys through the installed SDK.
        hg::GlobalContext context;
        const auto        state = context.state().view();
        hgp::set_frame_store(state,
                             hgp::store::make_frame_store(hgp::store::FrameStoreConfig{}));
        hgp::store_write(state, "consumer/frame", hg::Frame{});
        require(hgp::store_contains(state, "consumer/frame"),
                "stored frame is retrievable");
        require(!hgp::store_contains(state, "consumer/absent"),
                "absent keys read as absent");

        // The segmented-recording protocol survives the installed boundary.
        const hg::Frame marker = hgp::segmented_recording_marker();
        require(hgp::is_segmented_recording(marker),
                "segmented-recording marker recognised");
        require(hgp::segment_key("k", 2) == "k.2", "segment key shape");

        std::cout << "hgraph-persistence installed consumer passed\n";
        return 0;
    }
    catch (const std::exception &error)
    {
        std::cerr << error.what() << '\n';
        return 1;
    }
}
