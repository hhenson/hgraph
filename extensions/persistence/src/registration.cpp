#include <hgraph/persistence/recording_store.h>

#include <hgraph/types/operator_dispatch.h>

#include "record_replay_frame_impl.h"

#if HGRAPH_ENABLE_PYTHON_USER_NODES
#include <hgraph/python/native_scalar_registration.h>
#endif

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

#if HGRAPH_ENABLE_PYTHON_USER_NODES
    void register_recording_option_enums(nanobind::object record_as_of,
                                         nanobind::object record_removes)
    {
        // Captured by value in the installer so the association is replayed
        // after a registry reset, exactly as the overloads are.
        auto &registry = OperatorRegistry::instance();
        registry.register_installer(
            "hgraph.persistence.python_scalars",
            [record_as_of = std::move(record_as_of),
             record_removes = std::move(record_removes)] {
                python_bridge::register_native_scalar_type<RecordAsOf>(record_as_of);
                python_bridge::register_native_scalar_type<RecordRemoves>(record_removes);
            });
        registry.run_installers();
    }
#endif

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
