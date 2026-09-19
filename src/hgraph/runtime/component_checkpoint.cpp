#include <hgraph/runtime/component_checkpoint.h>
#include <hgraph/runtime/graph.h>
#include <hgraph/runtime/graph_checkpoint_coordinator.h>
#include <hgraph/types/metadata/type_registry.h>

#include <stdexcept>
#include <utility>

namespace hgraph
{
    namespace
    {
        constexpr std::string_view config_key = "__hgraph.component_recovery.config__";
    }

    void configure_component_recovery(GlobalStateView state, ComponentRecoveryConfig config)
    {
        if (!state.valid() || config.component_id.empty() || config.revision.empty() || !config.commit)
        {
            throw std::invalid_argument("component recovery requires GlobalState, component id, revision and commit callback");
        }
        (void)TypeRegistry::instance().register_scalar<ComponentRecoveryConfig>("__ComponentRecoveryConfig");
        state.set(config_key, Value{std::move(config)});
    }

    void clear_component_recovery(GlobalStateView state) { (void)state.erase(config_key); }

    bool component_recovery_selected(GlobalStateView state, std::string_view component_id)
    {
        if (!state.valid() || !state.contains(config_key)) { return false; }
        return GraphCheckpointSelection::owned_by(state.get_as<ComponentRecoveryConfig>(config_key).component_id)
            .selects(component_id);
    }

    std::optional<std::string> configured_recovery_component(GlobalStateView state)
    {
        if (!state.valid() || !state.contains(config_key)) { return std::nullopt; }
        return state.get_as<ComponentRecoveryConfig>(config_key).component_id;
    }

    // Completed-day policy only. The image mechanics are the coordinator's.
    struct ComponentRecoverySession::Impl
    {
        std::optional<ComponentRecoveryConfig> config{};
        DateTime start{};
        DateTime end{};
        std::optional<ComponentCheckpoint> loaded{};
        std::optional<ComponentCheckpoint> captured{};
        std::optional<GraphCheckpointCoordinator> coordinator{};
    };

    ComponentRecoverySession::ComponentRecoverySession(const GraphView &graph, DateTime start, DateTime end,
                                                       bool simulation)
        : impl_(std::make_unique<Impl>())
    {
        impl_->start = start;
        impl_->end = end;
        auto state = graph.global_state();
        if (state.contains(config_key))
        {
            if (!simulation) { throw std::runtime_error("component recovery currently requires simulation execution"); }
            if (end == MAX_ET) { throw std::invalid_argument("component recovery requires a finite completed-day end time"); }
            impl_->config = state.get_as<ComponentRecoveryConfig>(config_key);
            impl_->coordinator.emplace(GraphCheckpointSelection::owned_by(impl_->config->component_id));
        }
    }
    ComponentRecoverySession::~ComponentRecoverySession() = default;
    bool ComponentRecoverySession::active() const noexcept { return impl_->config.has_value(); }

    LifecycleObserver *ComponentRecoverySession::observer() const noexcept
    {
        return impl_->coordinator ? &*impl_->coordinator : nullptr;
    }

    void ComponentRecoverySession::prepare(const GraphView &graph)
    {
        if (!active()) { return; }
        const auto expected = impl_->coordinator->shape(graph);
        if (expected.nodes.empty()) { throw std::runtime_error("component checkpoint: configured component was not wired"); }
        if (impl_->config->load) { impl_->loaded = impl_->config->load(); }
        if (!impl_->loaded) { return; }
        const auto &saved = *impl_->loaded;
        if (saved.version != ComponentCheckpoint::current_version || saved.component_id != impl_->config->component_id ||
            saved.graph_signature != GraphCheckpointCoordinator::signature(expected, impl_->config->revision))
        {
            throw std::runtime_error("component checkpoint: incompatible component, revision or graph signature");
        }
        if (saved.cut >= impl_->start || saved.completed_until > impl_->start ||
            saved.cut >= saved.completed_until)
        {
            throw std::runtime_error("component checkpoint: recovery must start after the cut and completed interval");
        }
        impl_->coordinator->restore(graph, saved.graph, impl_->start, saved.cut);
    }

    void ComponentRecoverySession::complete_start() noexcept
    {
        if (impl_->coordinator) { impl_->coordinator->complete_start(); }
    }

    void ComponentRecoverySession::capture(const GraphView &graph)
    {
        if (!active()) { return; }
        ComponentCheckpoint saved;
        saved.component_id = impl_->config->component_id;
        saved.cut = graph.evaluation_time();
        saved.completed_until = impl_->end;
        saved.graph = impl_->coordinator->capture(graph);
        saved.graph_signature = GraphCheckpointCoordinator::signature(saved.graph, impl_->config->revision);
        impl_->captured.emplace(std::move(saved));
    }

    void ComponentRecoverySession::commit()
    {
        if (impl_->captured) { impl_->config->commit(*impl_->captured); }
    }
}
