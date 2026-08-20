#ifndef HGRAPH_FABRIC_OPERATORS_H
#define HGRAPH_FABRIC_OPERATORS_H

#include <hgraph/fabric/export.h>
#include <hgraph/fabric/types.h>

#include <hgraph/types/frame.h>
#include <hgraph/types/graph_wiring.h>
#include <hgraph/types/operator_dispatch.h>

#include <cstdint>
#include <optional>
#include <utility>
#include <vector>

namespace hgraph::fabric
{
    /** Subscribe to one stable fabric data id.
        @param data_id Durable data identity fixed while wiring.
        @param mode Wiring-time subscription mode.
        @return Complete atomic Frame versions selected by the fabric.
        Snapshot calls additionally accept an ``as_of`` scalar in their
        concrete overload. */
    struct SubscribeData
        : Operator<"hgraph.fabric.subscribe_data", Scalar<"data_id", Str>,
                   Scalar<"mode", SubscriptionMode>,
                   Scalar<"as_of", DateTime>, Out<TS<Frame>>>
    {
    };

    /** Publish complete Frame values under one stable fabric data id.
        @param data_id Durable data identity fixed while wiring.
        @param value Complete atomic Frame values.
        This sink records automatic or explicitly selected fabric ancestry;
        publication side effects are idempotent by contract. */
    struct PublishData
        : Operator<"hgraph.fabric.publish_data", Scalar<"data_id", Str>,
                   In<"value", TS<Frame>>>
    {
    };

    /** Opaque proof that a dependency was obtained from subscribe_data in one
        wired root. It cannot be constructed from an arbitrary data-id string. */
    class HGRAPH_FABRIC_EXPORT DependencyHandle final
    {
      public:
        DependencyHandle() = delete;

        [[nodiscard]] std::uint64_t root_identity() const noexcept;
        [[nodiscard]] std::string_view data_id() const noexcept;
        [[nodiscard]] const WiringPortRef &source() const noexcept;

      private:
        friend DependencyHandle dependency_handle(Wiring &, Port<TS<Frame>>);

        DependencyHandle(std::uint64_t root_identity, Str data_id,
                         WiringPortRef source);

        std::uint64_t root_identity_{};
        Str           data_id_{};
        WiringPortRef source_{};
    };

    class HGRAPH_FABRIC_EXPORT DependencySelection final
    {
      public:
        [[nodiscard]] static DependencySelection automatic();
        [[nodiscard]] static DependencySelection
        explicit_dependencies(std::vector<DependencyHandle> dependencies);

        [[nodiscard]] bool is_automatic() const noexcept;
        [[nodiscard]] const std::vector<DependencyHandle> &dependencies() const noexcept;

      private:
        DependencySelection() = default;

        bool                          automatic_{true};
        std::vector<DependencyHandle> dependencies_{};
    };

    /** Recover an explicit lineage handle from a direct subscribe_data result.
        Forwarded or unrelated Frame ports are rejected. */
    [[nodiscard]] HGRAPH_FABRIC_EXPORT DependencyHandle
    dependency_handle(Wiring &wiring, Port<TS<Frame>> subscription);

    [[nodiscard]] HGRAPH_FABRIC_EXPORT Port<TS<Frame>>
    subscribe_data(Wiring &wiring, Str data_id, SubscriptionMode mode,
                   std::optional<DateTime> as_of = {});

    HGRAPH_FABRIC_EXPORT void publish_data(
        Wiring &wiring, Str data_id, Port<TS<Frame>> value,
        DependencySelection dependencies = DependencySelection::automatic());

    /** Install fabric type registrations and operator overloads. The keyed
        installer replays them after registry reset. */
    HGRAPH_FABRIC_EXPORT void register_fabric_operators();
}  // namespace hgraph::fabric

#endif  // HGRAPH_FABRIC_OPERATORS_H
