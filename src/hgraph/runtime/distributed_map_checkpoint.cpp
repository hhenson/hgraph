#include <hgraph/runtime/distributed_map.h>

#include <hgraph/manifest/canonical.h>
#include <hgraph/runtime/child_graph_inspection.h>
#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/value/value_builder.h>

#include <fmt/format.h>

#include <limits>

namespace hgraph::distributed::dmap_checkpoint
{
    namespace
    {
        // The owner node's own address: unique among live nodes, and the state
        // only has to survive from restore to the start that follows.
        std::string parking_key(const NodeView &node)
        {
            return fmt::format("__hgraph.dmap_.restored.{}", fmt::ptr(node.data()));
        }

        // A worker scope records what a component scope would have refused.
        // Walk nested child templates too: a map_ inside the worker is
        // supported, and what it maps may not be.
        void require_recoverable(const GraphBuilder &graph, std::size_t group)
        {
            struct Walk { std::size_t group; };
            Walk walk{group};
            for (const NodeBuilder &node : graph.nodes())
            {
                const auto &identity = node.checkpoint_identity();
                if (identity.component.empty() || !identity.refusal.empty())
                {
                    throw std::invalid_argument(fmt::format(
                        "component checkpoint: dmap_ partition {} hosts '{}', which cannot be recovered: {}", group,
                        node.type().schema()->name(),
                        identity.refusal.empty() ? "it was wired outside a worker checkpoint scope" : identity.refusal));
                }
                node.visit_child_graphs(&walk, [](void *context, ChildGraphInspectionView child) {
                    if (child.graph != nullptr) { require_recoverable(*child.graph, static_cast<Walk *>(context)->group); }
                });
            }
        }
    }

    NodeCheckpointState capture(const NodeView &node, const CaptureGraphCheckpoint &)
    {
        auto *pool = node.state().checked_as<DistributedMapState>().pool;
        if (pool == nullptr) { throw std::logic_error("component checkpoint: dmap_ has no workers to capture"); }
        auto &registry = TypeRegistry::instance();
        ListBuilder images{registry.scalar_type<Bytes>()};
        for (auto &image : pool->capture()) { images.push_back(Bytes{std::move(image)}); }
        ListBuilder extents{registry.scalar_type<Int>()};
        for (const auto extent : pool->output_extents())
        {
            if (extent > static_cast<std::size_t>(std::numeric_limits<Int>::max()))
                throw std::overflow_error("dmap_ checkpoint exceeds the portable integer range");
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

    void restore(const NodeView &node, const NodeCheckpointState &image, DateTime, const RestoreGraphCheckpoint &)
    {
        auto &registry = TypeRegistry::instance();
        if (!image.payload.has_value() || !image.children.empty() || !image.endpoints.empty())
            throw std::invalid_argument("component checkpoint: invalid dmap_ owner image");
        // Interned schemas: the payload has the owner's shape exactly when it
        // IS the schema capture builds.
        const auto *shape = registry.tuple({registry.list(registry.scalar_type<Bytes>().schema()),
                                            registry.list(registry.scalar_type<Int>().schema())});
        if (image.payload.view().schema() != shape)
            throw std::invalid_argument("component checkpoint: dmap_ owner image has the wrong shape");
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
        Restored   restored;
        const auto tuple = parked.as_tuple();
        for (const auto &image : tuple.at(0).as_list()) { restored.images.push_back(image.checked_as<Bytes>().data); }
        for (const auto &extent : tuple.at(1).as_list())
        {
            const Int value = extent.checked_as<Int>();
            if (value < 0) { throw std::invalid_argument("component checkpoint: dmap_ owner image has a negative extent"); }
            restored.extents.push_back(static_cast<std::size_t>(value));
        }
        (void)state.erase(key);
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
        writer.varint(children.size());
        for (std::size_t group = 0; group < children.size(); ++group)
        {
            require_recoverable(children[group], group);
            writer.varint(children[group].node_count());
            for (const NodeBuilder &node : children[group].nodes())
            {
                const auto &identity = node.checkpoint_identity();
                writer.string_field(identity.id);
                writer.string_field(identity.signature);
            }
        }
        const auto &bytes = writer.bytes();
        return {reinterpret_cast<const char *>(bytes.data()), bytes.size()};
    }
}
