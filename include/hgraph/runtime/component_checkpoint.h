#ifndef HGRAPH_RUNTIME_COMPONENT_CHECKPOINT_H
#define HGRAPH_RUNTIME_COMPONENT_CHECKPOINT_H

#include <hgraph/runtime/global_state.h>
#include <hgraph/runtime/lifecycle_observer.h>
#include <hgraph/runtime/node_checkpoint.h>

#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <string>

namespace hgraph
{
    /** Owned image of one deterministic component at a completed run boundary. */
    struct HGRAPH_CLASS_EXPORT ComponentCheckpoint
    {
        static constexpr std::uint32_t current_version = 1;
        std::uint32_t version{current_version};
        std::string component_id{};
        std::string graph_signature{};
        DateTime cut{MIN_DT};
        DateTime completed_until{MIN_DT};
        GraphCheckpointImage graph{};
    };

    /** Explicit opt-in contract: input-only deterministic simulation, no
     * external effects within the component. Revision identifies application
     * code; runtime schema/topology validation cannot inspect function bodies.
     * Capture is automatic; commit is called only after successful teardown.
     */
    struct HGRAPH_CLASS_EXPORT ComponentRecoveryConfig
    {
        std::string component_id{};
        std::string revision{"1"};
        std::function<std::optional<ComponentCheckpoint>()> load{};
        std::function<void(const ComponentCheckpoint &)> commit{};
    };

    HGRAPH_EXPORT void configure_component_recovery(GlobalStateView state,
                                                     ComponentRecoveryConfig config);
    HGRAPH_EXPORT void clear_component_recovery(GlobalStateView state);
    [[nodiscard]] HGRAPH_EXPORT bool component_recovery_selected(
        GlobalStateView state, std::string_view component_id);

    /** Executor-owned completed-day policy over a ``GraphCheckpointCoordinator``
     * (``graph_checkpoint_coordinator.h``), which owns the image mechanics. All
     * operations are cold lifecycle paths; an unconfigured run installs no
     * observer and captures no endpoint.
     */
    class HGRAPH_CLASS_EXPORT ComponentRecoverySession final
    {
      public:
        ComponentRecoverySession(const GraphView &graph, DateTime start, DateTime end,
                                 bool simulation);
        ~ComponentRecoverySession();
        ComponentRecoverySession(const ComponentRecoverySession &) = delete;
        ComponentRecoverySession &operator=(const ComponentRecoverySession &) = delete;
        [[nodiscard]] bool active() const noexcept;
        void prepare(const GraphView &graph);
        /** Release preparation cleanup only after the complete root start succeeds. */
        void complete_start() noexcept;
        void capture(const GraphView &graph);
        void commit();
        /** The coordinator to register for the start phase; null when inactive. */
        [[nodiscard]] LifecycleObserver *observer() const noexcept;
      private:
        struct Impl;
        std::unique_ptr<Impl> impl_;
    };
}
#endif
