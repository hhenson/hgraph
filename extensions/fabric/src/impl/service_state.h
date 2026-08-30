#ifndef HGRAPH_FABRIC_IMPL_SERVICE_STATE_H
#define HGRAPH_FABRIC_IMPL_SERVICE_STATE_H

#include <hgraph/fabric/publication.h>
#include <hgraph/fabric/resolution.h>
#include <hgraph/fabric/service.h>

#include <hgraph/runtime/node_scheduler.h>
#include <hgraph/types/static_node.h>

#include <compare>
#include <cstdint>
#include <limits>
#include <memory>
#include <optional>
#include <ostream>
#include <string_view>
#include <vector>

namespace hgraph::fabric::detail
{
    struct SubscriptionSpec
    {
        Str key{};
        Str data_id{};

        friend bool operator==(const SubscriptionSpec &, const SubscriptionSpec &) = default;
    };

    struct DeliveredRoot
    {
        Str key{};
        Str data_id{};
        RevisionId revision{};
        DataVersion output_version{};
        std::optional<Frame> frame{};
    };

    struct DeliveryBatch
    {
        std::vector<DeliveredRoot> roots{};
    };

    struct PublicationRequestInput
    {
        Str data_id{};
        Frame output{};
        std::vector<DataDependencyInput> dependencies{};
        std::optional<DataVersion> self_predecessor{};
    };

    struct NotificationDeliveryInput
    {
        Str data_id{};
        RevisionId revision{};
        bool delivered{};
        bool retriable{};
        Str message{};
    };

    struct TransportControlInput
    {
        bool ready{};
        bool reconcile{};
        bool failed{};
        Str message{};

        friend bool operator==(const TransportControlInput &,
                               const TransportControlInput &) = default;
    };

    struct FabricWiringPlan
    {
        std::vector<SubscriptionSpec> subscriptions{};

        void add(SubscriptionSpec subscription);

        friend bool operator==(const FabricWiringPlan &, const FabricWiringPlan &) = default;
    };

    /** Immutable wiring metadata shared by declarations and lazy service
        materialisation. Planned nodes copy their subscription vectors into
        node State during start; execution cannot mutate this plan. Identity
        ordering and hashing exist only for scalar/container bookkeeping and
        must never determine graph output or any other semantic iteration. */
    struct FabricWiringPlanHandle
    {
        std::shared_ptr<const FabricWiringPlan> value{};

        friend bool operator==(const FabricWiringPlanHandle &,
                               const FabricWiringPlanHandle &) noexcept = default;
        friend std::strong_ordering operator<=>(const FabricWiringPlanHandle &lhs,
                                                const FabricWiringPlanHandle &rhs) noexcept
        {
            return reinterpret_cast<std::uintptr_t>(lhs.value.get()) <=>
                   reinterpret_cast<std::uintptr_t>(rhs.value.get());
        }
    };

    inline std::ostream &operator<<(std::ostream &stream, const FabricWiringPlanHandle &value)
    {
        return stream << "FabricWiringPlanHandle(" << value.value.get() << ')';
    }

    struct FabricPlannedSubscriptionService
    {
        static constexpr std::string_view name{"fabric_planned_subscription"};
        using output_schema = TSD<Str, FabricIngressSignal>;
    };

    [[nodiscard]] FabricWiringPlanHandle service_plan(Wiring &wiring, std::string_view path);
    void plan_subscription(Wiring &wiring, SubscriptionSpec subscription,
                           std::string_view path);

    struct FabricNodeDiagnostics
    {
        std::vector<std::pair<Str, Str>> metrics{};
        std::vector<std::pair<Str, FabricDiagnosticEventInput>> events{};
    };

    /** Node-local replay algorithm state. Its scheduler is supplied by the replay
        node, so durable history never schedules work outside the graph. */
    class ReplayNodeState final
    {
      public:
        ReplayNodeState();
        ~ReplayNodeState();
        ReplayNodeState(ReplayNodeState &&) noexcept;
        ReplayNodeState &operator=(ReplayNodeState &&) noexcept;
        ReplayNodeState(const ReplayNodeState &) = delete;
        ReplayNodeState &operator=(const ReplayNodeState &) = delete;

        void start(FabricConfig config, DateTime start_time, DateTime end_time,
                   std::vector<SubscriptionSpec> planned = {});
        void stop() noexcept;
        [[nodiscard]] std::optional<DeliveryBatch>
        evaluate(std::vector<SubscriptionSpec> subscriptions, DateTime now,
                 NodeScheduler scheduler);
        [[nodiscard]] std::optional<DeliveryBatch> evaluate_planned(DateTime now,
                                                                    NodeScheduler scheduler);
        [[nodiscard]] FabricNodeDiagnostics diagnostics() const;

      private:
        struct Impl;
        std::unique_ptr<Impl> impl_{};
    };

    /** Node-local live-resolution state. Notice caching and reconciliation are
        evaluated only when the live node is scheduled by its graph inputs. */
    class LiveNodeState final
    {
      public:
        LiveNodeState();
        ~LiveNodeState();
        LiveNodeState(LiveNodeState &&) noexcept;
        LiveNodeState &operator=(LiveNodeState &&) noexcept;
        LiveNodeState(const LiveNodeState &) = delete;
        LiveNodeState &operator=(const LiveNodeState &) = delete;

        void start(FabricConfig config, std::vector<SubscriptionSpec> planned = {});
        void stop() noexcept;
        [[nodiscard]] std::optional<DeliveryBatch>
        evaluate(std::vector<SubscriptionSpec> subscriptions,
                 std::vector<DataRevisionInput> revisions, DateTime now, bool reconcile = false);
        [[nodiscard]] std::optional<DeliveryBatch>
        evaluate_planned(std::vector<DataRevisionInput> revisions, DateTime now,
                         bool reconcile = false);
        [[nodiscard]] FabricNodeDiagnostics diagnostics() const;

      private:
        struct Impl;
        std::unique_ptr<Impl> impl_{};
    };

    /** Node-local publication state. Per-data-id queues and state machines belong
        to the publication node; notification candidates and completion remain
        ordinary graph edges outside this state object. */
    class PublicationNodeState final
    {
      public:
        PublicationNodeState();
        ~PublicationNodeState();
        PublicationNodeState(PublicationNodeState &&) noexcept;
        PublicationNodeState &operator=(PublicationNodeState &&) noexcept;
        PublicationNodeState(const PublicationNodeState &) = delete;
        PublicationNodeState &operator=(const PublicationNodeState &) = delete;

        void start(FabricConfig config, bool graph_notifications);
        void stop() noexcept;
        void enqueue(PublicationRequestInput request);
        [[nodiscard]] std::vector<DataRevisionInput>
        advance(std::size_t notification_capacity =
                    std::numeric_limits<std::size_t>::max());
        void complete(NotificationDeliveryInput delivery);
        [[nodiscard]] bool work_pending() const noexcept;
        [[nodiscard]] std::size_t notification_request_limit() const;
        [[nodiscard]] FabricNodeDiagnostics diagnostics() const;

      private:
        struct Impl;
        std::unique_ptr<Impl> impl_{};
    };

    [[nodiscard]] SubscriptionSpec decode_subscription_key(std::string_view key);
} // namespace hgraph::fabric::detail

namespace std
{
    template <> struct hash<hgraph::fabric::detail::FabricWiringPlanHandle>
    {
        // Identity hashing matches equality. As with ordering above, bucket
        // placement must never escape into observable graph semantics.
        size_t
        operator()(const hgraph::fabric::detail::FabricWiringPlanHandle &value) const noexcept
        {
            return hash<const void *>{}(value.value.get());
        }
    };

} // namespace std

namespace hgraph::static_schema_detail
{
    template <> struct scalar_name<fabric::detail::ReplayNodeState>
    {
        static constexpr std::string_view value{"hgraph.fabric.internal::ReplayNodeState"};
    };

    template <> struct scalar_name<fabric::detail::LiveNodeState>
    {
        static constexpr std::string_view value{"hgraph.fabric.internal::LiveNodeState"};
    };

    template <> struct scalar_name<fabric::detail::PublicationNodeState>
    {
        static constexpr std::string_view value{"hgraph.fabric.internal::PublicationNodeState"};
    };

    template <> struct scalar_name<fabric::detail::FabricWiringPlanHandle>
    {
        static constexpr std::string_view value{"hgraph.fabric.internal::WiringPlanHandle"};
    };

} // namespace hgraph::static_schema_detail

#endif // HGRAPH_FABRIC_IMPL_SERVICE_STATE_H
