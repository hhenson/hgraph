#include <hgraph/runtime/graph_checkpoint_coordinator.h>
#include <hgraph/runtime/graph.h>
#include <hgraph/runtime/node.h>
#include <hgraph/runtime/node_scheduler.h>
#include <hgraph/manifest/canonical.h>
#include <hgraph/types/time_series_reference.h>
#include <hgraph/types/time_series/ts_output.h>
#include <hgraph/util/scope.h>

#include <algorithm>
#include <map>
#include <stdexcept>
#include <unordered_map>
#include <unordered_set>
#include <utility>

namespace hgraph
{
    namespace
    {
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
            if (node.checkpoint_ops().supported || schema->checkpoints_without_ops()) { return; }
            throw std::runtime_error("component checkpoint: node '" + node_id(node) +
                "' (" + std::string{schema->name()} +
                ") requires explicit checkpoint support (local state, source or runtime service)");
        }

        void validate_cut(const TSCheckpointImage &image, DateTime cut)
        {
            if (image.last_modified_time > cut || image.key_set_last_modified_time > cut)
            {
                throw std::runtime_error("component checkpoint: endpoint timestamp exceeds the completed cut");
            }
            for (const auto time : image.window_times)
            {
                if (time > cut)
                {
                    throw std::runtime_error("component checkpoint: window sample timestamp exceeds the completed cut");
                }
            }
            for (const auto &child : image.children) { validate_cut(child, cut); }
        }
    }

    GraphCheckpointSelection GraphCheckpointSelection::whole_graph() { return {}; }

    GraphCheckpointSelection GraphCheckpointSelection::owned_by(std::string component)
    {
        if (component.empty()) { throw std::invalid_argument("component checkpoint: a component selection requires an id"); }
        if (reserved_checkpoint_scope(component))
            throw std::invalid_argument("component checkpoint: component id uses the reserved @hgraph. namespace");
        GraphCheckpointSelection selection;
        selection.roots_.push_back(std::move(component));
        return selection;
    }

    GraphCheckpointSelection GraphCheckpointSelection::hosted(std::string_view component)
    {
        if (component.empty()) { return whole_graph(); }
        auto selection = owned_by(std::string{component});
        selection.roots_.emplace_back(worker_boundary_checkpoint_scope);
        return selection;
    }

    bool GraphCheckpointSelection::selects(std::string_view owner) const noexcept
    {
        if (whole()) { return true; }
        for (const auto &root : roots_)
        {
            if (owner == root || (owner.starts_with(root) && owner.size() > root.size() && owner[root.size()] == '.'))
            {
                return true;
            }
        }
        return false;
    }

    bool GraphCheckpointSelection::contains_dependencies(const NodeCheckpointIdentity &identity) const noexcept
    {
        return std::all_of(identity.input_components.begin(), identity.input_components.end(),
                           [&](const auto &owner) { return selects(owner); });
    }

    struct GraphCheckpointCoordinator::Impl
    {
        GraphCheckpointSelection selection;
        DateTime start{};
        DateTime cut{MIN_DT};
        std::unordered_map<void *, const NodeCheckpointImage *> restoring{};
        TSCheckpointContext reference_context{};
        struct ReferenceFixup
        {
            TSDataStorageRef<> storage;
            TSReferenceCheckpointImage image;
        };
        std::vector<ReferenceFixup> references{};
        std::map<std::vector<std::size_t>, std::vector<NodePtr>> graph_nodes{};
        std::unordered_map<const GraphCheckpointImage *, GraphPtr> captured_children{};
        struct EndpointKey
        {
            const TSOutput *output;
            const void *data;
            const TypeRecord *type;
            bool operator==(const EndpointKey &) const = default;
        };
        struct EndpointHash
        {
            std::size_t operator()(const EndpointKey &key) const noexcept
            {
                return std::hash<const void *>{}(key.output) ^
                    (std::hash<const void *>{}(key.data) << 1) ^
                    (std::hash<const void *>{}(key.type) << 2);
            }
        };
        std::unordered_map<EndpointKey, TSCheckpointLocator, EndpointHash> endpoints{};
        std::unordered_map<EndpointKey, TSOutputAlternativeDescriptor, EndpointHash> adapter_endpoints{};
        std::unordered_map<const void *, std::unordered_map<std::size_t, TSOutputHandle>> custom_endpoints{};
        std::unordered_set<const TSOutput *> outputs{};
        bool endpoint_index_ready{true};
        DateTime endpoint_index_time{MIN_DT};
        std::unordered_set<const TSOutput *> captured_alternatives{};
        std::vector<std::pair<NodePtr, const NodeCheckpointImage *>> prepared{};
        std::vector<GraphPtr> preparing_graphs{};

        void discard_preparation() noexcept
        {
            // Failure observers run before ordinary graph rollback can erase
            // child allocations. Detach the entire closure exactly once while
            // all reference targets are alive, including unstarted siblings.
            auto graphs = std::move(preparing_graphs);
            preparing_graphs.clear();
            for (const auto pointer : graphs) { GraphView{pointer}.discard_checkpoint_preparation(start); }
        }

        explicit Impl(GraphCheckpointSelection chosen) : selection(std::move(chosen))
        {
            reference_context.capture_reference = [&](const TimeSeriesReference &reference) {
                return capture_reference(reference);
            };
            reference_context.restore_reference = [&](const TSDataView &view, const TSReferenceCheckpointImage &image) {
                references.push_back({view.storage_ref(), image});
            };
        }

        static EndpointKey endpoint_key(const TSOutputHandle &handle)
        {
            return {handle.output(), handle.data_view().data(), handle.storage_type().record()};
        }

        struct LocatorHash
        {
            std::size_t operator()(const TSCheckpointLocator &value) const noexcept
            {
                std::size_t result = 0;
                const auto mix = [&](std::size_t part) { result ^= part + 0x9e3779b9 + (result << 6) + (result >> 2); };
                const auto path = [&](const auto &parts) { mix(parts.size()); for (const auto part : parts) { mix(part); } };
                path(value.graph_path); mix(value.node); mix(value.endpoint); mix(value.custom_endpoint); path(value.endpoint_path);
                for (const auto &step : value.bindings) { mix(std::hash<const void *>{}(step.requested_schema)); path(step.path); }
                return result;
            }
        };

        // Enumerate live positions instead of walking an arbitrary REF's parent
        // pointers: an invalid dangling reference must never be dereferenced.
        void index_endpoint(const TSOutputView &view, TSCheckpointLocator locator)
        {
            if (!view.bound()) { return; }
            endpoints.try_emplace(endpoint_key(view.handle()), locator);
            outputs.insert(view.output());
            if (view.schema()->kind == TSTypeKind::TSD)
            {
                const auto dict = view.as_dict();
                auto key_locator = locator;
                key_locator.endpoint_path.push_back(ts_key_set_path_component);
                index_endpoint(dict.key_set(), std::move(key_locator));
                for (std::size_t slot = 0; slot < dict.slot_capacity(); ++slot)
                {
                    if (!dict.slot_live(slot)) { continue; }
                    auto child = locator;
                    child.endpoint_path.push_back(slot);
                    index_endpoint(dict.at_slot(slot), std::move(child));
                }
            }
            else if (view.schema()->kind == TSTypeKind::TSB || view.schema()->kind == TSTypeKind::TSL)
            {
                for (std::size_t index = 0; index < view.data_view().indexed_child_count(); ++index)
                {
                    auto child = locator;
                    child.endpoint_path.push_back(index);
                    index_endpoint(view.indexed_child_at(index), std::move(child));
                }
            }
        }

        std::optional<TSCheckpointLocator> find_locator(const TSOutputHandle &handle, std::size_t depth = 0)
        {
            ensure_endpoint_index();
            if (const auto it = endpoints.find(endpoint_key(handle)); it != endpoints.end()) { return it->second; }
            if (depth > 64 || !outputs.contains(handle.output())) { return std::nullopt; }
            const auto found = adapter_endpoints.find(endpoint_key(handle));
            if (found == adapter_endpoints.end()) { return std::nullopt; }
            const auto &descriptor = found->second;
            auto locator = find_locator(descriptor.source, depth + 1);
            if (locator)
            {
                locator->bindings.push_back({descriptor.requested_schema, descriptor.path});
                endpoints.try_emplace(endpoint_key(handle), *locator);
            }
            return locator;
        }

        TSCheckpointLocator capture_locator(const TSOutputHandle &handle)
        {
            auto locator = find_locator(handle);
            if (!locator)
                throw std::runtime_error("component checkpoint: reference target has no live ordinal path inside the component boundary");
            return std::move(*locator);
        }

        TSReferenceCheckpointImage capture_reference(const TimeSeriesReference &reference)
        {
            TSReferenceCheckpointImage image;
            image.kind = static_cast<TSReferenceCheckpointKind>(reference.kind());
            image.target_schema = reference.target_schema();
            if (reference.is_peered()) { image.target = capture_locator(reference.target_output()); }
            else if (reference.is_non_peered())
                for (const auto &item : reference.items()) { image.items.push_back(capture_reference(item)); }
            return image;
        }

        static TSOutputView project(TSOutputView view, const std::vector<std::size_t> &path)
        {
            for (const auto index : path)
            {
                if (!view.bound()) { throw std::invalid_argument("component checkpoint: reference path is unbound"); }
                if (index == ts_key_set_path_component) { view = view.as_dict().key_set(); }
                else if (view.schema()->kind == TSTypeKind::TSD)
                {
                    const auto dict = view.as_dict();
                    if (!dict.slot_live(index))
                        throw std::invalid_argument("component checkpoint: reference names a non-live dictionary slot");
                    view = dict.at_slot(index);
                }
                else
                {
                    if ((view.schema()->kind != TSTypeKind::TSB && view.schema()->kind != TSTypeKind::TSL) ||
                        index >= view.data_view().indexed_child_count())
                        throw std::invalid_argument("component checkpoint: reference structural path is invalid");
                    view = view.indexed_child_at(index);
                }
            }
            return view;
        }

        TSOutputHandle resolve_locator(const TSCheckpointLocator &locator)
        {
            const auto graph = graph_nodes.find(locator.graph_path);
            if (graph == graph_nodes.end() || locator.node >= graph->second.size())
                throw std::invalid_argument("component checkpoint: reference graph or node ordinal is invalid");
            const NodeView node{graph->second[locator.node]};
            TSOutputView root;
            switch (locator.endpoint)
            {
                case 0: if (node.has_output()) { root = node.output(start); } break;
                case 1: if (node.has_error_output()) { root = node.error_output(start); } break;
                case 2: if (node.has_recordable_state()) { root = node.recordable_state(start); } break;
                case 3:
                {
                    const auto source = ingress_source(node, start);
                    if (source.valid()) { root = source.output(start); }
                    break;
                }
                case 4:
                    if (const auto owner = custom_endpoints.find(node.data()); owner != custom_endpoints.end())
                        if (const auto endpoint = owner->second.find(locator.custom_endpoint); endpoint != owner->second.end())
                            root = endpoint->second.view(start);
                    break;
                default: throw std::invalid_argument("component checkpoint: reference endpoint role is invalid");
            }
            if (!root.bound()) { throw std::invalid_argument("component checkpoint: reference endpoint is absent"); }
            auto target = project(std::move(root), locator.endpoint_path);
            for (const auto &step : locator.bindings)
            {
                target = project(target.output()->checkpoint_binding_for(target, *step.requested_schema).view(start), step.path);
            }
            return target.handle();
        }

        TimeSeriesReference resolve_reference(const TSReferenceCheckpointImage &image)
        {
            switch (image.kind)
            {
                case TSReferenceCheckpointKind::Empty: return TimeSeriesReference::empty(image.target_schema);
                case TSReferenceCheckpointKind::Peered:
                    return TimeSeriesReference{resolve_locator(*image.target)}.with_target_schema_unchecked(image.target_schema);
                case TSReferenceCheckpointKind::NonPeered:
                {
                    std::vector<TimeSeriesReference> items;
                    for (const auto &child : image.items) { items.push_back(resolve_reference(child)); }
                    return TimeSeriesReference::non_peered(image.target_schema, std::move(items));
                }
            }
            throw std::invalid_argument("component checkpoint: invalid reference kind");
        }

        void fix_reference(const ReferenceFixup &pending)
        {
            auto value = Value{resolve_reference(pending.image)};
            auto target = TSDataView{pending.storage};
            const auto &ops = target.ops();
            (void)ops.copy_value_from_impl(ops.context, target.mutable_data(), value.view(), target.last_modified_time());
        }

        // A whole-graph image names its nodes by identity alone. Without one
        // every node is "" and the image would validate against any graph.
        void require(const NodeView &node) const
        {
            if (selection.whole() && node.checkpoint_identity().id.empty())
            {
                throw std::runtime_error("component checkpoint: node '" + std::string{node.schema()->name()} +
                    "' has no checkpoint identity; wire the whole graph inside a checkpoint scope");
            }
            // An image selected by component does not restore what feeds this
            // node from outside it; a whole-graph image does, so there it is fine.
            if (!selection.contains_dependencies(node.checkpoint_identity()))
            {
                throw std::runtime_error("component checkpoint: node '" + node_id(node) + "' (" +
                    std::string{node.schema()->name()} + ") is fed from outside its component by a node "
                    "this image does not restore; move what computes its input inside the component");
            }
            // Recorded by a worker-graph scope, where a component scope
            // would have refused to wire the node at all.
            if (const auto &refusal = node.checkpoint_identity().refusal; !refusal.empty())
            {
                throw std::runtime_error("component checkpoint: node '" + node_id(node) + "' (" +
                    std::string{node.schema()->name()} + ") cannot be checkpointed: " + refusal);
            }
            require_node(node);
        }

        bool selected(const NodeView &node) const
        {
            // A transient sink is inside the scope and outside the image.
            const auto &identity = node.checkpoint_identity();
            return !identity.transient && selection.selects(identity.component);
        }

        // Legacy images and sinks without NodeScheduler data retain their
        // ordinary startup schedule. New scheduler images replace that data.
        // A sink with explicit operations follows its owner's contract.
        static bool owns_its_schedule(const NodeView &node)
        {
            return node.schema()->node_kind == NodeKind::Sink && !node.checkpoint_ops().supported;
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

        GraphCheckpointImage capture_graph(const GraphView &graph, bool shape_only = false, bool inventory_only = false)
        {
            GraphCheckpointImage image;
            std::unordered_set<void *> ingress_sources;
            for (std::size_t i = 0; i < graph.node_count(); ++i)
            {
                auto node = graph.node_at(i);
                if (!selected(node)) { continue; }
                require(node);
                if (aliased_output(node) && graph.is_root() &&
                    !(node.checkpoint_ops().supported && !node.checkpoint_ops().captures_output))
                {
                    throw std::runtime_error("component checkpoint: root output aliases require reference recovery support");
                }
                if (node.has_output() && node.checkpoint_ops().captures_output && !aliased_output(node) &&
                    !ts_checkpoint_eligible(node.output(graph.evaluation_time()).data_view(), &reference_context))
                {
                    throw std::runtime_error("component checkpoint: unsupported output " +
                        std::string{node.schema()->output_schema->name()} + " at '" + node_id(node) + "'");
                }
                if (node.has_recordable_state() &&
                    !ts_checkpoint_eligible(node.recordable_state(graph.evaluation_time()).data_view(), &reference_context))
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
                if (!inventory_only && node.has_output() && node.checkpoint_ops().captures_output && !aliased_output(node))
                {
                    item.output.emplace(capture_ts_checkpoint(node.output(time).data_view(), &reference_context));
                }
                if (!inventory_only && node.has_error_output())
                {
                    item.error.emplace(capture_ts_checkpoint(node.error_output(time).data_view()));
                }
                if (!inventory_only && node.has_recordable_state())
                {
                    item.recordable_state.emplace(capture_ts_checkpoint(node.recordable_state(time).data_view(), &reference_context));
                }
                if (!inventory_only && ingress.valid())
                {
                    item.ingress.emplace(capture_ts_checkpoint(ingress.output(time).data_view()));
                    validate_cut(*item.ingress, time);
                }
                if (node.has_input()) { item.input_activity = node.input(time).checkpoint_activity(); }
                item.custom = node.checkpoint_ops().capture_impl(node, [&](const GraphView &child) {
                    auto result = std::make_shared<GraphCheckpointImage>(capture_graph(child, false, inventory_only));
                    captured_children.emplace(result.get(), child.pointer());
                    return result;
                });
                image.nodes.push_back(std::move(item));
            }
            return image;
        }

        void index_graph(const GraphView &graph, const GraphCheckpointImage &image,
                         const std::vector<std::size_t> &path)
        {
            auto &nodes = graph_nodes[path];
            nodes.clear();
            std::size_t ordinal = 0;
            for (std::size_t i = 0; i < graph.node_count(); ++i)
            {
                auto node = graph.node_at(i);
                if (!selected(node)) { continue; }
                nodes.push_back(node.pointer());
                for (const auto &child : image.nodes.at(ordinal).custom.children)
                {
                    auto child_path = path;
                    child_path.insert(child_path.end(), {ordinal, child.slot});
                    index_graph(GraphView{captured_children.at(child.graph.get())}, *child.graph, child_path);
                }
                ++ordinal;
            }
        }

        // The position index maps every live endpoint -- each dictionary
        // child, bundle field and list element -- to its ordinal locator, so
        // it costs as much as the state it describes. Only reference locators
        // and adapter inventories consult it. Resetting therefore records the
        // owning outputs alone, which is one entry per node endpoint, and the
        // positions are indexed on the first lookup. The graph does not change
        // between the two: both happen inside one capture or one preparation.
        template <typename Visit>
        void visit_root_endpoints(DateTime time, Visit &&visit)
        {
            for (const auto &[path, nodes] : graph_nodes)
            {
                for (std::size_t ordinal = 0; ordinal < nodes.size(); ++ordinal)
                {
                    const NodeView node{nodes[ordinal]};
                    TSCheckpointLocator locator{.graph_path = path, .node = ordinal};
                    if (node.has_output()) { visit(node.output(time), locator); }
                    locator.endpoint = 1;
                    if (node.has_error_output()) { visit(node.error_output(time), locator); }
                    locator.endpoint = 2;
                    if (node.has_recordable_state()) { visit(node.recordable_state(time), locator); }
                    locator.endpoint = 3;
                    const auto source = ingress_source(node, time);
                    if (source.valid()) { visit(source.output(time), locator); }
                    locator.endpoint = 4;
                    node.checkpoint_ops().visit_endpoints_impl(node, [&](std::size_t index, const TSOutputHandle &handle) {
                        locator.custom_endpoint = index;
                        visit(handle.view(time), locator);
                    });
                }
            }
        }

        void rebuild_endpoint_index(DateTime time)
        {
            endpoints.clear();
            adapter_endpoints.clear();
            outputs.clear();
            endpoint_index_time = time;
            endpoint_index_ready = false;
            visit_root_endpoints(time, [&](const TSOutputView &view, const TSCheckpointLocator &) {
                if (view.bound()) { outputs.insert(view.output()); }
            });
        }

        void ensure_endpoint_index()
        {
            if (endpoint_index_ready) { return; }
            endpoint_index_ready = true;
            visit_root_endpoints(endpoint_index_time, [&](const TSOutputView &view, const TSCheckpointLocator &locator) {
                index_endpoint(view, locator);
            });
            // Build the reverse adapter index once while storage is stable.
            // Do not chase source handles here: retired cache sources can be
            // stale, and find_locator deliberately handles them as identities.
            for (const auto *output : outputs)
                output->visit_checkpoint_alternative_endpoints(
                    [&](const TSOutputHandle &handle, const TSOutputAlternativeDescriptor &descriptor) {
                        adapter_endpoints.try_emplace(endpoint_key(handle), descriptor);
                    });
        }

        void fill_graph(const GraphView &graph, GraphCheckpointImage &image)
        {
            std::size_t ordinal = 0;
            const auto time = graph.evaluation_time();
            for (std::size_t i = 0; i < graph.node_count(); ++i)
            {
                auto node = graph.node_at(i);
                if (!selected(node)) { continue; }
                auto &item = image.nodes.at(ordinal++);
                if (node.has_output() && node.checkpoint_ops().captures_output && !aliased_output(node))
                    item.output = capture_ts_checkpoint(node.output(time).data_view(), &reference_context);
                if (node.has_error_output()) { item.error = capture_ts_checkpoint(node.error_output(time).data_view(), &reference_context); }
                if (node.has_recordable_state())
                    item.recordable_state = capture_ts_checkpoint(node.recordable_state(time).data_view(), &reference_context);
                if (node.has_scheduler())
                    item.scheduler = NodeScheduler{node.scheduler_state(), nullptr, i, time}.capture_checkpoint(time);
                auto ingress = ingress_source(node, time);
                if (ingress.valid()) { item.ingress = capture_ts_checkpoint(ingress.output(time).data_view()); }
                const auto adapters = [&](const TSOutputHandle &handle) {
                    if (!handle.bound() || !captured_alternatives.insert(handle.output()).second) { return; }
                    for (auto &entry : handle.output()->capture_checkpoint_alternatives(
                             [&](const TSOutputHandle &source) { return find_locator(source).has_value(); }))
                    {
                        auto locator = capture_locator(entry.binding.source);
                        locator.bindings.push_back({entry.binding.requested_schema, entry.binding.path});
                        item.alternatives.push_back({std::move(locator), std::move(entry.clocks)});
                    }
                };
                if (node.has_output()) { adapters(node.output(time).handle()); }
                if (node.has_error_output()) { adapters(node.error_output(time).handle()); }
                if (node.has_recordable_state()) { adapters(node.recordable_state(time).handle()); }
                if (ingress.valid()) { adapters(ingress.output(time).handle()); }
                node.checkpoint_ops().visit_endpoints_impl(node, [&](std::size_t, const TSOutputHandle &handle) { adapters(handle); });
                for (auto &child : item.custom.children)
                    fill_graph(GraphView{captured_children.at(child.graph.get())}, *child.graph);
            }
        }

        void restore_graph(const GraphView &graph, const GraphCheckpointImage &image,
                           const std::vector<std::size_t> &path)
        {
            preparing_graphs.push_back(graph.pointer());
            std::vector<std::pair<std::size_t, const NodeCheckpointImage *>> matched;
            auto &nodes = graph_nodes[path];
            std::size_t cursor = 0;
            // Validate the complete static graph before importing any endpoint.
            for (std::size_t i = 0; i < graph.node_count(); ++i)
            {
                auto node = graph.node_at(i);
                if (!selected(node)) { continue; }
                require(node);
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
                if (saved.scheduler)
                {
                    if (!node.has_scheduler())
                        throw std::runtime_error("component checkpoint: scheduler inventory mismatch at '" + saved.id + "'");
                    NodeScheduler::validate_checkpoint(*saved.scheduler, start);
                }
                if (saved.output) { validate_cut(*saved.output, cut); }
                if (saved.error) { validate_cut(*saved.error, cut); }
                if (saved.recordable_state) { validate_cut(*saved.recordable_state, cut); }
                if (saved.ingress) { validate_cut(*saved.ingress, cut); }
                for (const auto &endpoint : saved.custom.endpoints) { validate_cut(endpoint, cut); }
                for (const auto &binding : saved.alternatives) { validate_cut(binding.clocks, cut); }
                for (const auto &child : saved.custom.children)
                {
                    if (child.key_last_modified_time > cut)
                    {
                        throw std::runtime_error("component checkpoint: child key timestamp exceeds the completed cut");
                    }
                }
                if (saved.output) { validate_ts_checkpoint(node.output(start).data_view(), *saved.output, &reference_context); }
                if (saved.error) { validate_ts_checkpoint(node.error_output(start).data_view(), *saved.error, &reference_context); }
                if (saved.recordable_state) { validate_ts_checkpoint(node.recordable_state(start).data_view(), *saved.recordable_state, &reference_context); }
                if (saved.ingress) { validate_ts_checkpoint(ingress.output(start).data_view(), *saved.ingress); }
                if (node.has_input()) { node.input(start).validate_checkpoint_activity(saved.input_activity); }
                else if (!saved.input_activity.empty())
                {
                    throw std::runtime_error("component checkpoint: activity saved for a node without inputs");
                }
                matched.emplace_back(i, &saved);
                nodes.push_back(node.pointer());
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
                if (saved->output) { restore_ts_checkpoint(node.output(start).data_view(), *saved->output, &reference_context); }
                if (saved->error) { restore_ts_checkpoint(node.error_output(start).data_view(), *saved->error, &reference_context); }
                if (saved->recordable_state) { restore_ts_checkpoint(node.recordable_state(start).data_view(), *saved->recordable_state, &reference_context); }
                restoring.emplace(node.data(), saved);
            }
            for (std::size_t ordinal = 0; ordinal < matched.size(); ++ordinal)
            {
                const auto &[i, saved] = matched[ordinal];
                auto node = graph.node_at(i);
                // Value-only ingress aliases must exist before a nested owner
                // reads restored membership; all other finalization is deferred.
                if (node.checkpoint_ops().boundary_input)
                    node.checkpoint_ops().restore_impl(node, saved->custom, start, {});
                std::unordered_map<const GraphCheckpointImage *, std::size_t> child_slots;
                child_slots.reserve(saved->custom.children.size());
                for (const auto &child : saved->custom.children)
                    if (!child.graph || !child_slots.emplace(child.graph.get(), child.slot).second)
                        throw std::invalid_argument("component checkpoint: child image identity is missing or duplicated");
                node.checkpoint_ops().prepare_restore_impl(node, saved->custom, start,
                    [&](const GraphView &child, const GraphCheckpointImage &child_image, DateTime) {
                        const auto child_slot = child_slots.find(&child_image);
                        if (child_slot == child_slots.end())
                            throw std::invalid_argument("component checkpoint: prepared child is absent from saved membership");
                        auto child_path = path;
                        child_path.insert(child_path.end(), {ordinal, child_slot->second});
                        restore_graph(child, child_image, child_path);
                    });
                prepared.emplace_back(node.pointer(), saved);
            }
        }

        void finalize_graph(const GraphView &graph, const GraphCheckpointImage &image)
        {
            std::size_t ordinal = 0;
            for (std::size_t index = 0; index < graph.node_count(); ++index)
            {
                const auto node = graph.node_at(index);
                if (!selected(node)) { continue; }
                const auto &saved = image.nodes.at(ordinal++);
                if (node.checkpoint_ops().boundary_input) { continue; }
                node.checkpoint_ops().restore_impl(node, saved.custom, start,
                    [&](const GraphView &child, const GraphCheckpointImage &child_image, DateTime) {
                        finalize_graph(child, child_image);
                    });
            }
        }

        void fix_references_and_alternatives()
        {
            // All custom owner storage now exists. Index it once: resolving
            // one saved key/index reference must not re-enumerate every sibling.
            custom_endpoints.clear();
            for (const auto &[_, nodes] : graph_nodes)
                for (const auto pointer : nodes)
                {
                    const NodeView node{pointer};
                    node.checkpoint_ops().visit_endpoints_impl(node, [&](std::size_t ordinal, const TSOutputHandle &handle) {
                        if (!custom_endpoints[node.data()].emplace(ordinal, handle).second)
                            throw std::invalid_argument("component checkpoint: duplicate custom endpoint ordinal");
                    });
                }
            std::vector<const EndpointBindingCheckpoint *> bindings;
            std::unordered_set<TSCheckpointLocator, LocatorHash> identities;
            for (const auto &[_, image] : prepared)
                for (const auto &binding : image->alternatives)
                {
                    validate_ts_reference_checkpoint({
                        .kind = TSReferenceCheckpointKind::Peered,
                        .target_schema = binding.clocks.schema,
                        .target = binding.binding});
                    if (!identities.insert(binding.binding).second)
                        throw std::invalid_argument("component checkpoint: duplicate reference adapter image");
                    bindings.push_back(&binding);
                }
            std::stable_sort(bindings.begin(), bindings.end(), [](auto lhs, auto rhs) {
                return lhs->binding.bindings.size() < rhs->binding.bindings.size();
            });
            const auto restore_binding = [&](const EndpointBindingCheckpoint *binding)
            {
                auto source_locator = binding->binding;
                if (source_locator.bindings.empty() || !source_locator.bindings.back().path.empty())
                    throw std::invalid_argument("component checkpoint: alternative image requires a complete binding root");
                const auto step = source_locator.bindings.back();
                source_locator.bindings.pop_back();
                const auto source = resolve_locator(source_locator).view(start);
                source.output()->restore_checkpoint_alternative(source, *step.requested_schema, binding->clocks, start);
            };
            // A locator may project through another restored REF's adapter.
            // Complete available fixups first, then retry their dependants.
            // No evaluation or notification is allowed during this barrier.
            std::vector<bool> reference_done(references.size(), false), binding_done(bindings.size(), false);
            std::size_t remaining = references.size() + bindings.size();
            while (remaining != 0)
            {
                const auto previous = remaining;
                FirstExceptionRecorder errors;
                for (std::size_t index = 0; index < references.size(); ++index)
                {
                    if (reference_done[index]) { continue; }
                    errors.capture([&] { fix_reference(references[index]); reference_done[index] = true; --remaining; });
                }
                for (std::size_t index = 0; index < bindings.size(); ++index)
                {
                    if (binding_done[index]) { continue; }
                    errors.capture([&] { restore_binding(bindings[index]); binding_done[index] = true; --remaining; });
                }
                if (remaining == previous) { errors.rethrow_if_any(); }
            }
            references.clear();
        }

        void validate_alternative_inventory()
        {
            rebuild_endpoint_index(start);
            std::unordered_set<TSCheckpointLocator, LocatorHash> saved;
            for (const auto &[_, image] : prepared)
                for (const auto &binding : image->alternatives) { saved.insert(binding.binding); }
            for (const auto *output : outputs)
            {
                for (const auto &entry : output->capture_checkpoint_alternatives(
                         [&](const TSOutputHandle &source) { return find_locator(source).has_value(); }))
                {
                    auto locator = capture_locator(entry.binding.source);
                    locator.bindings.push_back({entry.binding.requested_schema, entry.binding.path});
                    // Known limit (RFC 0039, "Sinks"): an adapter lives on the PRODUCER
                    // and is made for whichever consumer asks, so one made for a
                    // transient sink counts here too, and adding or removing such a
                    // sink is refused rather than ignored. Telling a fresh consumer's
                    // adapter from a restored one's needs a walk of the selected
                    // nodes' input bindings; accepting every unsaved adapter would also
                    // accept an image that LOST a restored consumer's clocks.
                    if (saved.erase(locator) != 1)
                        throw std::invalid_argument("component checkpoint: reference adapter inventory mismatch");
                }
            }
            if (!saved.empty())
                throw std::invalid_argument("component checkpoint: missing reference adapter target");
        }
    };

    GraphCheckpointCoordinator::GraphCheckpointCoordinator(GraphCheckpointSelection selection)
        : impl_(std::make_unique<Impl>(std::move(selection)))
    {}
    GraphCheckpointCoordinator::~GraphCheckpointCoordinator() = default;

    std::string GraphCheckpointCoordinator::signature(const GraphCheckpointImage &image, std::string_view revision)
    {
        manifest::CanonicalWriter writer;
        writer.string_field(revision);
        writer.varint(image.nodes.size());
        for (const auto &node : image.nodes)
        {
            writer.string_field(node.id);
            writer.string_field(node.signature);
        }
        const auto &bytes = writer.bytes();
        return {reinterpret_cast<const char *>(bytes.data()), bytes.size()};
    }

    GraphCheckpointImage GraphCheckpointCoordinator::shape(const GraphView &graph)
    {
        return impl_->capture_graph(graph, true);
    }

    GraphCheckpointImage GraphCheckpointCoordinator::capture(const GraphView &graph)
    {
        impl_->captured_children.clear();
        impl_->graph_nodes.clear();
        impl_->endpoints.clear();
        impl_->outputs.clear();
        impl_->endpoint_index_ready = true;   // Nothing to index until the image shape is known.
        impl_->captured_alternatives.clear();
        auto image = impl_->capture_graph(graph, false, true);
        impl_->index_graph(graph, image, {});
        impl_->rebuild_endpoint_index(graph.evaluation_time());
        impl_->fill_graph(graph, image);
        return image;
    }

    void GraphCheckpointCoordinator::restore(const GraphView &graph, const GraphCheckpointImage &image,
                                             DateTime start, DateTime cut)
    {
        if (cut >= start) { throw std::invalid_argument("component checkpoint: a restored graph must start after its cut"); }
        impl_->start = start;
        impl_->cut = cut;
        auto discard = UnwindCleanupGuard([&] { impl_->discard_preparation(); });
        impl_->restore_graph(graph, image, {});
        impl_->fix_references_and_alternatives();
        impl_->finalize_graph(graph, image);
        impl_->validate_alternative_inventory();
        discard.release();
    }

    void GraphCheckpointCoordinator::complete_start() noexcept
    {
        impl_->preparing_graphs.clear();
    }

    void GraphCheckpointCoordinator::on_start_node_failed(const NodeView &)
    {
        impl_->discard_preparation();
    }

    void GraphCheckpointCoordinator::on_start_graph_failed(const GraphView &)
    {
        impl_->discard_preparation();
    }

    void GraphCheckpointCoordinator::on_after_start_node(const NodeView &node)
    {
        auto it = impl_->restoring.find(node.data());
        if (it == impl_->restoring.end()) { return; }
        const auto *saved = it->second;
        impl_->restoring.erase(it);
        node.checkpoint_ops().start_restored_impl(node, impl_->start);
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
        if (!active_input_changed && (saved->scheduler || !Impl::owns_its_schedule(node)))
        {
            node.graph().clear_restored_schedule(node.node_index());
            // A NodeScheduler holds the same bootstrap alarm in the node's own
            // state. Left behind, it is re-armed after the node's next
            // evaluation -- by then in the past, which the graph refuses.
            if (node.has_scheduler())
            {
                auto &scheduler = node.scheduler_state();
                scheduler.events.clear();
                scheduler.tags.clear();
            }
        }
        if (saved->scheduler)
            NodeScheduler{node.scheduler_state(), node.graph_value(), node.node_index(), impl_->start}
                .restore_checkpoint(*saved->scheduler);
        // What the restored start found still to do is not historical. Asked
        // whichever way the branch above went: an input stamped at the start
        // reads as modified without having scheduled anyone, and scheduling keeps
        // the earliest time, so asking twice costs nothing.
        const auto live = node.checkpoint_ops().live_schedule_impl(node);
        if (live != MAX_DT) { node.graph().schedule_node(node.node_index(), std::max(live, impl_->start)); }
    }
}
