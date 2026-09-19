#include <hgraph/runtime/distributed_map.h>

#include <hgraph/lib/std/component.h>
#include <hgraph/manifest/canonical.h>
#include <hgraph/runtime/child_graph_inspection.h>
#include <hgraph/runtime/component_checkpoint.h>
#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/value/value_builder.h>

#include <fmt/format.h>

#include <algorithm>
#include <limits>

namespace hgraph::distributed::worker_checkpoint
{
    namespace
    {
        // The owner node's own address: unique among live nodes, and the state
        // only has to survive from restore to the start that follows.
        std::string parking_key(const NodeView &node)
        {
            return fmt::format("__hgraph.worker_owner.restored.{}", fmt::ptr(node.data()));
        }

        // A worker scope records what a component scope would have refused.
        // Walk nested child templates too: a map_ inside the worker is
        // supported, and what it maps may not be.
        struct Hosted
        {
            std::string_view                owner;
            std::size_t                     index;
            const GraphCheckpointSelection *selection;
        };
        void require_recoverable(const GraphBuilder &graph, const Hosted &hosted)
        {
            for (const NodeBuilder &node : graph.nodes())
            {
                const auto &identity = node.checkpoint_identity();
                if (identity.transient || !hosted.selection->selects(identity.component)) { continue; }
                const bool stranded = !hosted.selection->contains_dependencies(identity);
                if (identity.component.empty() || !identity.refusal.empty() || stranded)
                {
                    throw std::invalid_argument(fmt::format(
                        "component checkpoint: {} worker {} hosts '{}', which cannot be recovered: {}", hosted.owner,
                        hosted.index, node.type().schema()->name(),
                        !identity.refusal.empty() ? identity.refusal
                        : stranded ? "it is fed from outside its component by a node the image does not restore; "
                                     "move what computes its input inside the component"
                                   : "it was wired outside a worker checkpoint scope"));
                }
                node.visit_child_graphs(const_cast<Hosted *>(&hosted), [](void *context, ChildGraphInspectionView child) {
                    if (child.graph != nullptr) { require_recoverable(*child.graph, *static_cast<const Hosted *>(context)); }
                });
            }
        }
    }

    NodeCheckpointState capture(const NodeView &node, const CaptureGraphCheckpoint &)
    {
        auto *pool = node.state().checked_as<DistributedMapState>().pool;
        if (pool == nullptr) { throw std::logic_error("component checkpoint: dmap_ has no workers to capture"); }
        return state_of(pool->capture(), pool->output_extents());
    }

    NodeCheckpointState state_of(std::vector<std::string> worker_images, std::span<const std::size_t> output_extents)
    {
        auto &registry = TypeRegistry::instance();
        ListBuilder images{registry.scalar_type<Bytes>()};
        for (auto &image : worker_images) { images.push_back(Bytes{std::move(image)}); }
        ListBuilder extents{registry.scalar_type<Int>()};
        for (const auto extent : output_extents)
        {
            if (extent > static_cast<std::size_t>(std::numeric_limits<Int>::max()))
                throw std::overflow_error("worker checkpoint exceeds the portable integer range");
            extents.push_back(static_cast<Int>(extent));
        }
        auto image_values  = images.build();
        auto extent_values = extents.build();
        BundleBuilder tuple{ValuePlanFactory::instance().type_for(
            registry.tuple({image_values.schema(), extent_values.schema()}))};
        tuple.set(0, std::move(image_values));
        tuple.set(1, std::move(extent_values));
        NodeCheckpointState state;
        state.payload = tuple.build();
        return state;
    }

    void sign_worker_graph(manifest::CanonicalWriter &writer, const GraphBuilder &graph,
                           std::string_view owner, std::size_t index, const GraphCheckpointSelection &selection)
    {
        require_recoverable(graph, Hosted{owner, index, &selection});
        std::size_t selected = 0;
        const auto in_contract = [&](const NodeCheckpointIdentity &identity) {
            return !identity.transient && selection.selects(identity.component);
        };
        for (const NodeBuilder &node : graph.nodes()) { selected += in_contract(node.checkpoint_identity()); }
        writer.varint(selected);
        for (const NodeBuilder &node : graph.nodes())
        {
            const auto &identity = node.checkpoint_identity();
            if (!in_contract(identity)) { continue; }
            writer.string_field(identity.component);
            writer.string_field(identity.id);
            writer.string_field(identity.signature);
        }
    }

    HostedComponentScope::HostedComponentScope(Wiring &wiring, std::optional<std::string> component)
        : wiring_(wiring), component_(std::move(component))
    {
        if (component_) { previous_ = wiring_.checkpoint_component(*component_); }
    }

    HostedComponentScope::~HostedComponentScope()
    {
        if (component_) { (void)wiring_.checkpoint_component(previous_); }
    }

    WiringPortRef HostedComponentScope::input(WiringPortRef source, std::string_view name)
    {
        if (!component_) { return source; }
        return stdlib::component_detail::checkpoint_boundary(wiring_, std::move(source), name);
    }

    namespace
    {
        struct HostSearch
        {
            const GraphCheckpointSelection *component;   // what is being looked for
            const GraphCheckpointSelection *image;       // what an image of it would take
            bool                            found{false};
        };
        void search_for_component(const GraphBuilder &graph, HostSearch &search)
        {
            for (const NodeBuilder &node : graph.nodes())
            {
                if (search.found) { return; }
                const auto &identity = node.checkpoint_identity();
                if (identity.transient) { continue; }
                if (search.component->selects(identity.component)) { search.found = true; return; }
                // A dmap_ worker's component is one level down, in the child
                // template its map_ wires -- and that map_ is the runtime's
                // own, so an image takes it. Descend ONLY through such a node:
                // the coordinator reaches a dynamic owner's children only when
                // the owner is selected. A component below a map_ the user
                // wrote would be found here and then never captured, and
                // recovery would quietly do nothing; not finding it is what
                // makes that case fail loudly as "not wired".
                if (!search.image->selects(identity.component)) { continue; }
                node.visit_child_graphs(&search, [](void *context, ChildGraphInspectionView child) {
                    if (child.graph != nullptr) { search_for_component(*child.graph, *static_cast<HostSearch *>(context)); }
                });
            }
        }
    }

    DateTime live_schedule(const NodeView &node)
    {
        const auto *pool = node.state().checked_as<DistributedMapState>().pool;
        return pool == nullptr ? MAX_DT : pool->restored_next();
    }

    std::optional<std::string> hosted_component(
        Wiring &wiring, const std::function<bool(std::string_view component)> &any_worker_hosts)
    {
        // Wired inside a component, the owner is that component's member and
        // its workers are saved whole; there is nothing to stand in for.
        if (!wiring.checkpoint_component().empty()) { return std::nullopt; }
        auto configured = configured_recovery_component(wiring.operator_state());
        if (!configured || !any_worker_hosts(*configured)) { return std::nullopt; }
        return configured;
    }

    std::optional<std::string> hosted_component(Wiring &wiring, std::span<const GraphBuilder> children)
    {
        return hosted_component(wiring, [&](std::string_view component) {
            return std::any_of(children.begin(), children.end(),
                               [&](const GraphBuilder &child) { return hosts_component(child, component); });
        });
    }

    bool hosts_component(const GraphBuilder &graph, std::string_view component)
    {
        const auto owned = GraphCheckpointSelection::owned_by(std::string{component});
        const auto image = GraphCheckpointSelection::hosted(component);
        HostSearch search{&owned, &image};
        search_for_component(graph, search);
        return search.found;
    }

    void restore(const NodeView &node, const NodeCheckpointState &image, DateTime, const RestoreGraphCheckpoint &)
    {
        auto &registry = TypeRegistry::instance();
        if (!image.payload.has_value() || !image.children.empty() || !image.endpoints.empty())
            throw std::invalid_argument("component checkpoint: invalid worker owner image");
        // Interned schemas: the payload has the owner's shape exactly when it
        // IS the schema capture builds.
        const auto *shape = registry.tuple({registry.list(registry.scalar_type<Bytes>().schema()),
                                            registry.list(registry.scalar_type<Int>().schema())});
        if (image.payload.view().schema() != shape)
            throw std::invalid_argument("component checkpoint: worker owner image has the wrong shape");
        // An owner always has at least one worker. Refused here, in the
        // validation phase, because downstream "no images" means "start
        // fresh": an empty inventory would discard the workers' state silently.
        if (image.payload.view().as_tuple().at(0).as_list().size() == 0)
            throw std::invalid_argument("component checkpoint: worker owner image holds no worker images");
        // Here rather than in ``claim``: by then the graph is starting, and a
        // refusal belongs to the phase that can still detach the preparation.
        for (const auto &extent : image.payload.view().as_tuple().at(1).as_list())
        {
            if (extent.checked_as<Int>() < 0)
                throw std::invalid_argument("component checkpoint: worker owner image has a negative extent");
        }
        // The workers are raised in the owner's start, which has not run.
        // GlobalState owns the parked state: a preparation that never reaches
        // start leaves nothing to free.
        node.global_state().set(parking_key(node), image.payload.view());
    }

    std::optional<Restored> claim(const NodeView &node)
    {
        auto       state = node.global_state();
        const auto key   = parking_key(node);
        const auto parked = state.get(key);
        if (!parked.valid()) { return std::nullopt; }
        // Claimed once, however the copy below ends: the images are the
        // largest thing a recovery holds, and the key is this node's address.
        auto       unpark = make_scope_exit([&] { (void)state.erase(key); });
        Restored   restored;
        const auto tuple = parked.as_tuple();
        for (const auto &image : tuple.at(0).as_list()) { restored.images.push_back(image.checked_as<Bytes>().data); }
        for (const auto &extent : tuple.at(1).as_list())
        {
            restored.extents.push_back(static_cast<std::size_t>(extent.checked_as<Int>()));
        }
        return restored;
    }

    std::string signature(std::span<const GraphBuilder> children, const WorkerPoolConfig &config)
    {
        manifest::CanonicalWriter writer;
        writer.varint(1);
        // Placement is hash % workers and is not stored, so another count is
        // another contract; the hosting mode is part of it by RFC 0039.
        writer.varint(config.workers);
        writer.varint(static_cast<std::uint8_t>(config.hosting));
        writer.string_field(config.hosted_component);
        const auto selection = GraphCheckpointSelection::hosted(config.hosted_component);
        writer.varint(children.size());
        for (std::size_t group = 0; group < children.size(); ++group)
        {
            sign_worker_graph(writer, children[group], "dmap_", group, selection);
        }
        const auto &bytes = writer.bytes();
        return {reinterpret_cast<const char *>(bytes.data()), bytes.size()};
    }
}
