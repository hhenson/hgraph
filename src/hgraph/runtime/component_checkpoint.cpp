#include <hgraph/runtime/component_checkpoint.h>
#include <hgraph/runtime/graph.h>
#include <hgraph/runtime/node.h>
#include <hgraph/manifest/canonical.h>
#include <hgraph/types/metadata/type_registry.h>

#include <algorithm>
#include <stdexcept>
#include <unordered_map>
#include <unordered_set>
#include <utility>

namespace hgraph
{
    namespace
    {
        constexpr std::string_view config_key = "__hgraph.component_recovery.config__";

        bool belongs(std::string_view owner, std::string_view component)
        {
            return owner == component || (owner.starts_with(component) &&
                owner.size() > component.size() && owner[component.size()] == '.');
        }

        std::string node_id(const NodeView &node)
        {
            const auto &identity = node.checkpoint_identity();
            return identity.component + ":" + identity.id;
        }

        bool aliased_output(const NodeView &node)
        {
            return node.has_output() && !node.owns_output();
        }

        void require_node(const NodeView &node)
        {
            const auto *schema = node.schema();
            if (schema->captures_errors)
            {
                throw std::runtime_error("component checkpoint: error capture is unsupported at '" +
                    node_id(node) + "'; failed evaluations must abort the completed day");
            }
            if (node.checkpoint_ops().supported) { return; }
            if (schema->node_kind != NodeKind::Compute || schema->state_schema != nullptr ||
                schema->uses_scheduler || schema->schedule_on_start || schema->uses_global_state ||
                schema->uses_evaluation_clock)
            {
                throw std::runtime_error("component checkpoint: node '" + node_id(node) +
                    "' (" + std::string{schema->name()} +
                    ") requires explicit checkpoint support (local state, scheduler, source, sink or runtime service)");
            }
        }

        std::string graph_signature(const GraphCheckpointImage &graph, std::string_view revision)
        {
            manifest::CanonicalWriter writer;
            writer.string_field(revision);
            writer.varint(graph.nodes.size());
            for (const auto &node : graph.nodes)
            {
                writer.string_field(node.id);
                writer.string_field(node.signature);
            }
            const auto &bytes = writer.bytes();
            return {reinterpret_cast<const char *>(bytes.data()), bytes.size()};
        }

        void validate_cut(const TSCheckpointImage &image, DateTime cut)
        {
            if (image.last_modified_time > cut || image.key_set_last_modified_time > cut)
            {
                throw std::runtime_error("component checkpoint: endpoint timestamp exceeds the completed cut");
            }
            for (const auto &child : image.children) { validate_cut(child, cut); }
        }
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
        return belongs(component_id, state.get_as<ComponentRecoveryConfig>(config_key).component_id);
    }

    struct ComponentRecoverySession::Impl
    {
        std::optional<ComponentRecoveryConfig> config{};
        DateTime start{};
        DateTime end{};
        std::optional<ComponentCheckpoint> loaded{};
        std::optional<ComponentCheckpoint> captured{};
        std::unordered_map<void *, const NodeCheckpointImage *> restoring{};

        bool selected(const NodeView &node) const
        {
            return config && belongs(node.checkpoint_identity().component, config->component_id);
        }

        NodeView ingress_source(const NodeView &node, DateTime time) const
        {
            if (!node.checkpoint_ops().boundary_input) { return {}; }
            auto input = node.input(time);
            auto source = input.indexed_child_at(0).bound_output();
            auto producer = source.owner_node();
            // Nested components already receive the enclosing component's
            // restored endpoints; those producers are part of the graph image.
            if (producer.valid() && selected(producer)) { return {}; }
            if (!producer.valid() || producer.schema()->node_kind != NodeKind::PullSource ||
                !producer.owns_output() ||
                !source.handle().same_as(producer.output(time).handle()) ||
                !ts_checkpoint_eligible(source.data_view()))
            {
                throw std::runtime_error("component checkpoint: input '" + node_id(node) +
                    "' requires a direct owned pull-source endpoint or an already managed producer");
            }
            return producer;
        }

        GraphCheckpointImage capture_graph(const GraphView &graph, bool shape_only = false)
        {
            GraphCheckpointImage image;
            std::unordered_set<void *> ingress_sources;
            for (std::size_t i = 0; i < graph.node_count(); ++i)
            {
                auto node = graph.node_at(i);
                if (!selected(node)) { continue; }
                require_node(node);
                if (aliased_output(node) && graph.is_root())
                {
                    throw std::runtime_error("component checkpoint: root output aliases require reference recovery support");
                }
                if (node.has_output() && node.checkpoint_ops().captures_output && !aliased_output(node) &&
                    !ts_checkpoint_eligible(node.output(graph.evaluation_time()).data_view()))
                {
                    throw std::runtime_error("component checkpoint: unsupported output " +
                        std::string{node.schema()->output_schema->name()} + " at '" + node_id(node) + "'");
                }
                if (node.has_recordable_state() &&
                    !ts_checkpoint_eligible(node.recordable_state(graph.evaluation_time()).data_view()))
                {
                    throw std::runtime_error("component checkpoint: unsupported recordable state at '" + node_id(node) + "'");
                }
                NodeCheckpointImage item;
                item.id = node_id(node);
                item.signature = node.checkpoint_identity().signature;
                const auto ingress = ingress_source(node, graph.evaluation_time());
                if (ingress.valid() && !ingress_sources.insert(ingress.data()).second)
                {
                    throw std::runtime_error("component checkpoint: sharing an ingress source between component inputs is unsupported");
                }
                if (shape_only) { image.nodes.push_back(std::move(item)); continue; }
                const auto time = graph.evaluation_time();
                const auto scheduled = graph.node_scheduled_time(i);
                if (scheduled != MAX_DT && scheduled > time)
                {
                    throw std::runtime_error("component checkpoint: pending schedule at '" + item.id + "'");
                }
                if (node.has_output() && node.checkpoint_ops().captures_output && !aliased_output(node))
                {
                    item.output.emplace(capture_ts_checkpoint(node.output(time).data_view()));
                }
                if (node.has_error_output())
                {
                    item.error.emplace(capture_ts_checkpoint(node.error_output(time).data_view()));
                }
                if (node.has_recordable_state())
                {
                    item.recordable_state.emplace(capture_ts_checkpoint(node.recordable_state(time).data_view()));
                }
                if (ingress.valid())
                {
                    item.ingress.emplace(capture_ts_checkpoint(ingress.output(time).data_view()));
                    validate_cut(*item.ingress, time);
                }
                if (node.has_input()) { item.input_activity = node.input(time).checkpoint_activity(); }
                item.custom = node.checkpoint_ops().capture_impl(node, [&](const GraphView &child) {
                    return std::make_shared<GraphCheckpointImage>(capture_graph(child));
                });
                image.nodes.push_back(std::move(item));
            }
            return image;
        }

        void restore_graph(const GraphView &graph, const GraphCheckpointImage &image, bool start_graph)
        {
            std::vector<std::pair<std::size_t, const NodeCheckpointImage *>> matched;
            std::size_t cursor = 0;
            // Validate the complete static graph before importing any endpoint.
            for (std::size_t i = 0; i < graph.node_count(); ++i)
            {
                auto node = graph.node_at(i);
                if (!selected(node)) { continue; }
                require_node(node);
                if (cursor == image.nodes.size()) { throw std::runtime_error("component checkpoint: missing node"); }
                const auto &saved = image.nodes[cursor++];
                if (saved.id != node_id(node) || saved.signature != node.checkpoint_identity().signature)
                {
                    throw std::runtime_error("component checkpoint: incompatible node '" + node_id(node) + "'");
                }
                const bool owns_output = node.has_output() && node.checkpoint_ops().captures_output && !aliased_output(node);
                const auto ingress = ingress_source(node, start);
                if (saved.output.has_value() != owns_output || saved.error.has_value() != node.has_error_output() ||
                    saved.recordable_state.has_value() != node.has_recordable_state() ||
                    saved.ingress.has_value() != ingress.valid())
                {
                    throw std::runtime_error("component checkpoint: endpoint inventory mismatch at '" + saved.id + "'");
                }
                if (saved.output) { validate_cut(*saved.output, loaded->cut); }
                if (saved.error) { validate_cut(*saved.error, loaded->cut); }
                if (saved.recordable_state) { validate_cut(*saved.recordable_state, loaded->cut); }
                if (saved.ingress) { validate_cut(*saved.ingress, loaded->cut); }
                for (const auto &child : saved.custom.children)
                {
                    if (child.key_last_modified_time > loaded->cut)
                    {
                        throw std::runtime_error("component checkpoint: child key timestamp exceeds the completed cut");
                    }
                }
                if (saved.output) { validate_ts_checkpoint(node.output(start).data_view(), *saved.output); }
                if (saved.error) { validate_ts_checkpoint(node.error_output(start).data_view(), *saved.error); }
                if (saved.recordable_state) { validate_ts_checkpoint(node.recordable_state(start).data_view(), *saved.recordable_state); }
                if (saved.ingress) { validate_ts_checkpoint(ingress.output(start).data_view(), *saved.ingress); }
                if (node.has_input()) { node.input(start).validate_checkpoint_activity(saved.input_activity); }
                else if (!saved.input_activity.empty())
                {
                    throw std::runtime_error("component checkpoint: activity saved for a node without inputs");
                }
                matched.emplace_back(i, &saved);
            }
            if (cursor != image.nodes.size()) { throw std::runtime_error("component checkpoint: unexpected saved nodes"); }
            for (const auto &[i, saved] : matched)
            {
                auto node = graph.node_at(i);
                if (saved->ingress)
                {
                    // The source still owns admission/cursor scheduling for the
                    // supplied future-only stream. Only its endpoint baseline
                    // is restored; it is never added to `restoring` below.
                    auto ingress = ingress_source(node, start);
                    restore_ts_checkpoint(ingress.output(start).data_view(), *saved->ingress);
                }
                if (saved->output) { restore_ts_checkpoint(node.output(start).data_view(), *saved->output); }
                if (saved->error) { restore_ts_checkpoint(node.error_output(start).data_view(), *saved->error); }
                if (saved->recordable_state) { restore_ts_checkpoint(node.recordable_state(start).data_view(), *saved->recordable_state); }
                restoring.emplace(node.data(), saved);
            }
            if (start_graph) { graph.start(start); }
        }
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
        }
    }
    ComponentRecoverySession::~ComponentRecoverySession() = default;
    bool ComponentRecoverySession::active() const noexcept { return impl_->config.has_value(); }

    void ComponentRecoverySession::prepare(const GraphView &graph)
    {
        if (!active()) { return; }
        auto expected = impl_->capture_graph(graph, true);
        if (expected.nodes.empty()) { throw std::runtime_error("component checkpoint: configured component was not wired"); }
        if (impl_->config->load) { impl_->loaded = impl_->config->load(); }
        if (!impl_->loaded) { return; }
        const auto &saved = *impl_->loaded;
        if (saved.version != 1 || saved.component_id != impl_->config->component_id ||
            saved.graph_signature != graph_signature(expected, impl_->config->revision))
        {
            throw std::runtime_error("component checkpoint: incompatible component, revision or graph signature");
        }
        if (saved.cut >= impl_->start || saved.completed_until > impl_->start ||
            saved.cut >= saved.completed_until)
        {
            throw std::runtime_error("component checkpoint: recovery must start after the cut and completed interval");
        }
        impl_->restore_graph(graph, saved.graph, false);
    }

    void ComponentRecoverySession::on_after_start_node(const NodeView &node)
    {
        auto it = impl_->restoring.find(node.data());
        if (it == impl_->restoring.end()) { return; }
        const auto *saved = it->second;
        impl_->restoring.erase(it);
        node.checkpoint_ops().restore_impl(node, saved->custom, impl_->start,
            [&](const GraphView &child, const GraphCheckpointImage &image, DateTime) {
                impl_->restore_graph(child, image, true);
            });
        const auto changed = [&](const std::optional<TSCheckpointImage> &image, auto endpoint) {
            return image && endpoint().last_modified_time() != image->last_modified_time;
        };
        if (changed(saved->output, [&] { return node.output(impl_->start); }) ||
            changed(saved->error, [&] { return node.error_output(impl_->start); }) ||
            changed(saved->recordable_state, [&] { return node.recordable_state(impl_->start); }))
        {
            throw std::runtime_error("component checkpoint: start hook modified restored endpoint at '" + saved->id + "'");
        }
        // Only a restored active observation may retain a newly admitted
        // input event. Fresh start defaults must not reactivate frozen inputs.
        const bool active_input_changed = node.has_input() &&
            node.input(impl_->start).restore_checkpoint_activity(saved->input_activity);
        if (!active_input_changed)
        {
            node.graph().clear_restored_schedule(node.node_index());
        }
    }

    void ComponentRecoverySession::capture(const GraphView &graph)
    {
        if (!active()) { return; }
        ComponentCheckpoint saved;
        saved.component_id = impl_->config->component_id;
        saved.cut = graph.evaluation_time();
        saved.completed_until = impl_->end;
        saved.graph = impl_->capture_graph(graph);
        saved.graph_signature = graph_signature(saved.graph, impl_->config->revision);
        impl_->captured.emplace(std::move(saved));
    }

    void ComponentRecoverySession::commit()
    {
        if (impl_->captured) { impl_->config->commit(*impl_->captured); }
    }
}
