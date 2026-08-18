#include <hgraph/persistence/recording_store.h>

#include <hgraph/types/operator_dispatch.h>

#include "record_replay_frame_impl.h"

namespace hgraph::persistence
{
    namespace
    {
        void install_frame_backend()
        {
            register_record_replay_frame_operators();
            record_replay::register_seed_resolver(FRAME_BACKEND, &frame_seed_resolver);
        }
    }  // namespace

    void register_frame_backend()
    {
        // Keyed installer (RFC 0025 checkpoint 3): registration intent
        // survives registry resets, so one rebuild call replays this
        // extension exactly as it replays core. Idempotent between resets.
        auto &registry = OperatorRegistry::instance();
        registry.register_installer("hgraph.persistence", &install_frame_backend);
        registry.run_installers();
    }
}  // namespace hgraph::persistence
