#ifndef HGRAPH_FABRIC_IMPL_SERVICE_RUNTIME_H
#define HGRAPH_FABRIC_IMPL_SERVICE_RUNTIME_H

#include <hgraph/fabric/publication.h>
#include <hgraph/fabric/resolution.h>
#include <hgraph/fabric/service.h>

#include <hgraph/runtime/node_scheduler.h>
#include <hgraph/types/static_node.h>

#include <compare>
#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <optional>
#include <ostream>
#include <string_view>
#include <tuple>
#include <vector>

namespace hgraph::fabric::detail
{
struct SubscriptionSpec
{
    Str key{};
    Str data_id{};
    DateTime as_of{MIN_DT};

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

    friend bool operator==(const TransportControlInput &, const TransportControlInput &) = default;
};

struct TransportEventInput
{
    Str component{};
    Str category{};
    Str message{};
    bool retriable{};
    bool fatal{};
};

struct FabricServicePlan
{
    std::vector<SubscriptionSpec> live{};
    std::vector<SubscriptionSpec> replay{};
    std::vector<SubscriptionSpec> snapshot{};

    void add(SubscriptionSpec subscription, SubscriptionMode mode);
};

struct FabricServicePlanHandle
{
    std::shared_ptr<FabricServicePlan> value{};

    friend bool operator==(const FabricServicePlanHandle &, const FabricServicePlanHandle &) noexcept = default;
    friend std::strong_ordering operator<=>(const FabricServicePlanHandle &lhs,
                                            const FabricServicePlanHandle &rhs) noexcept
    {
        return reinterpret_cast<std::uintptr_t>(lhs.value.get()) <=> reinterpret_cast<std::uintptr_t>(rhs.value.get());
    }
};

inline std::ostream &operator<<(std::ostream &stream, const FabricServicePlanHandle &value)
{
    return stream << "FabricServicePlanHandle(" << value.value.get() << ')';
}

struct FabricPlannedLiveService
{
    static constexpr std::string_view name{"fabric_planned_live"};
    using output_schema = TSD<Str, FabricIngressSignal>;
};

struct FabricPlannedReplayService
{
    static constexpr std::string_view name{"fabric_planned_replay"};
    using output_schema = TSD<Str, FabricIngressSignal>;
};

struct FabricPlannedSnapshotService
{
    static constexpr std::string_view name{"fabric_planned_snapshot"};
    using output_schema = TSD<Str, FabricIngressSignal>;
};

[[nodiscard]] FabricServicePlanHandle service_plan(Wiring &wiring, std::string_view path);
void plan_subscription(Wiring &wiring, SubscriptionSpec subscription, SubscriptionMode mode, std::string_view path);

class FabricServiceRuntime final
{
  public:
    explicit FabricServiceRuntime(FabricServicePlanHandle plan, std::optional<Notifier> notification_override = {});
    ~FabricServiceRuntime();

    FabricServiceRuntime(const FabricServiceRuntime &) = delete;
    FabricServiceRuntime &operator=(const FabricServiceRuntime &) = delete;

    void start(GlobalStateView global_state);
    void stop() noexcept;
    void configure_replay_window(DateTime start_time, DateTime end_time);

    [[nodiscard]] std::optional<DeliveryBatch> snapshot(std::vector<SubscriptionSpec> subscriptions);
    [[nodiscard]] std::optional<DeliveryBatch> planned_snapshot();
    [[nodiscard]] std::optional<DeliveryBatch> replay(std::vector<SubscriptionSpec> subscriptions, DateTime now,
                                                      NodeScheduler scheduler);
    [[nodiscard]] std::optional<DeliveryBatch> planned_replay(DateTime now, NodeScheduler scheduler);
    [[nodiscard]] std::optional<DeliveryBatch> live(std::vector<SubscriptionSpec> subscriptions,
                                                    std::vector<DataRevisionInput> revisions, DateTime now,
                                                    bool reconcile = false);
    [[nodiscard]] std::optional<DeliveryBatch> planned_live(std::vector<DataRevisionInput> revisions, DateTime now,
                                                            bool reconcile = false);

    void publish(PublicationRequestInput request);
    [[nodiscard]] std::vector<DataRevisionInput> advance_publications();
    [[nodiscard]] bool publication_work_pending() const noexcept;

    [[nodiscard]] std::optional<std::tuple<Str, DataVersion, Frame>> load(std::string_view data_id,
                                                                          DataVersion version) const;
    [[nodiscard]] std::vector<std::pair<Str, Str>> diagnostics() const;
    void observe_transport_event(TransportEventInput event);

  private:
    struct Impl;
    std::unique_ptr<Impl> impl_;
};

class GraphNotificationBridge final
{
  public:
    GraphNotificationBridge();
    ~GraphNotificationBridge();

    GraphNotificationBridge(const GraphNotificationBridge &) = delete;
    GraphNotificationBridge &operator=(const GraphNotificationBridge &) = delete;

    [[nodiscard]] Notifier notifier() const;
    [[nodiscard]] std::optional<DataRevisionInput> take_request();
    void complete(NotificationDeliveryInput delivery);
    [[nodiscard]] bool request_pending() const noexcept;
    [[nodiscard]] std::vector<std::pair<Str, Str>> diagnostics() const;

  private:
    struct Impl;
    std::shared_ptr<Impl> impl_;
};

struct GraphNotificationBridgeHandle
{
    std::shared_ptr<GraphNotificationBridge> value{};

    friend bool operator==(const GraphNotificationBridgeHandle &, const GraphNotificationBridgeHandle &) noexcept = default;
    friend std::strong_ordering operator<=>(const GraphNotificationBridgeHandle &lhs,
                                            const GraphNotificationBridgeHandle &rhs) noexcept
    {
        return reinterpret_cast<std::uintptr_t>(lhs.value.get()) <=> reinterpret_cast<std::uintptr_t>(rhs.value.get());
    }
};

inline std::ostream &operator<<(std::ostream &stream, const GraphNotificationBridgeHandle &value)
{
    return stream << "GraphNotificationBridgeHandle(" << value.value.get() << ')';
}

struct FabricServiceRuntimeHandle
{
    std::shared_ptr<FabricServiceRuntime> value{};

    friend bool operator==(const FabricServiceRuntimeHandle &, const FabricServiceRuntimeHandle &) noexcept = default;
    friend std::strong_ordering operator<=>(const FabricServiceRuntimeHandle &lhs,
                                            const FabricServiceRuntimeHandle &rhs) noexcept
    {
        return reinterpret_cast<std::uintptr_t>(lhs.value.get()) <=> reinterpret_cast<std::uintptr_t>(rhs.value.get());
    }
};

inline std::ostream &operator<<(std::ostream &stream, const FabricServiceRuntimeHandle &value)
{
    return stream << "FabricServiceRuntimeHandle(" << value.value.get() << ')';
}

[[nodiscard]] Str subscription_key(Str data_id, SubscriptionMode mode, DateTime as_of);
[[nodiscard]] SubscriptionSpec decode_subscription_key(std::string_view key, SubscriptionMode mode);
} // namespace hgraph::fabric::detail

namespace std
{
template <> struct hash<hgraph::fabric::detail::FabricServiceRuntimeHandle>
{
    size_t operator()(const hgraph::fabric::detail::FabricServiceRuntimeHandle &value) const noexcept
    {
        return hash<const void *>{}(value.value.get());
    }
};

template <> struct hash<hgraph::fabric::detail::FabricServicePlanHandle>
{
    size_t operator()(const hgraph::fabric::detail::FabricServicePlanHandle &value) const noexcept
    {
        return hash<const void *>{}(value.value.get());
    }
};

template <> struct hash<hgraph::fabric::detail::GraphNotificationBridgeHandle>
{
    size_t operator()(const hgraph::fabric::detail::GraphNotificationBridgeHandle &value) const noexcept
    {
        return hash<const void *>{}(value.value.get());
    }
};
} // namespace std

namespace hgraph::static_schema_detail
{
template <> struct scalar_name<fabric::detail::FabricServiceRuntimeHandle>
{
    static constexpr std::string_view value{"hgraph.fabric.internal::ServiceRuntimeHandle"};
};

template <> struct scalar_name<fabric::detail::FabricServicePlanHandle>
{
    static constexpr std::string_view value{"hgraph.fabric.internal::ServicePlanHandle"};
};

template <> struct scalar_name<fabric::detail::GraphNotificationBridgeHandle>
{
    static constexpr std::string_view value{"hgraph.fabric.internal::GraphNotificationBridgeHandle"};
};
} // namespace hgraph::static_schema_detail

#endif // HGRAPH_FABRIC_IMPL_SERVICE_RUNTIME_H
