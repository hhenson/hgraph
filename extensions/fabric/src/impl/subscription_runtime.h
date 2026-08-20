#ifndef HGRAPH_FABRIC_IMPL_SUBSCRIPTION_RUNTIME_H
#define HGRAPH_FABRIC_IMPL_SUBSCRIPTION_RUNTIME_H

#include <hgraph/fabric/operators.h>
#include <hgraph/fabric/resolution.h>

#include <hgraph/runtime/node_scheduler.h>
#include <hgraph/types/static_node.h>

#include <compare>
#include <cstdint>
#include <memory>
#include <ostream>
#include <span>
#include <vector>

namespace hgraph::fabric::detail
{
    using IngressSignal =
        TSB<"hgraph.fabric::IngressSignal", Field<"frame", TS<Frame>>,
            Field<"version", TS<Int>>, Field<"revision", TS<Int>>>;
    using IngressSignals = TSD<Str, IngressSignal>;

    /** The marked source remains visible to the wiring-time dependency walker,
        while its evaluation path only projects a changed atomic Frame.

        Per tick: O(1) with one shared Arrow-table handle copy; no retained
        history and no REF allocation. */
    struct SubscribeDataRuntimeSource
    {
        static constexpr auto name = "hgraph.fabric.subscribe_data.planned";
        using signature_args =
            std::tuple<In<"signal", IngressSignal, InputActivity::Active,
                          InputValidity::Unchecked>,
                       Scalar<"data_id", Str>, Scalar<"mode", SubscriptionMode>,
                       Scalar<"as_of", DateTime>, Out<TS<Frame>>>;

        static void eval(
            In<"signal", IngressSignal, InputActivity::Active,
               InputValidity::Unchecked> signal,
            Out<TS<Frame>> out)
        {
            const auto frame = signal.template field<"frame">();
            if (frame.modified()) { out.set(frame.value()); }
        }
    };

    class IngressBridge;

    struct IngressBridgeHandle
    {
        std::shared_ptr<IngressBridge> value{};

        friend bool operator==(const IngressBridgeHandle &,
                               const IngressBridgeHandle &) noexcept = default;
        friend std::strong_ordering
        operator<=>(const IngressBridgeHandle &lhs,
                    const IngressBridgeHandle &rhs) noexcept
        {
            return reinterpret_cast<std::uintptr_t>(lhs.value.get()) <=>
                   reinterpret_cast<std::uintptr_t>(rhs.value.get());
        }
    };

    inline std::ostream &operator<<(std::ostream &stream,
                                    const IngressBridgeHandle &value)
    {
        return stream << "IngressBridgeHandle(" << value.value.get() << ')';
    }

    class IngressBridge final
        : public std::enable_shared_from_this<IngressBridge>
    {
      public:
        explicit IngressBridge(std::vector<Str> roots);
        ~IngressBridge();

        IngressBridge(const IngressBridge &) = delete;
        IngressBridge &operator=(const IngressBridge &) = delete;

        void start(SubscriptionMode requested_mode, DateTime as_of,
                   EngineControlView engine, GlobalStateView global_state,
                   AsyncNodeWakeSender wake_sender);
        void evaluate(DateTime now, NodeScheduler scheduler,
                      Out<IngressSignals> &out);
        void stop() noexcept;

        /** Thread-safe wake path used by concrete notification strategies. */
        void wake();

      private:
        struct Impl;
        std::unique_ptr<Impl> impl_;
    };

    template <SubscriptionMode Mode>
    struct IngressCoordinatorNode
    {
        /** Cold ingress node, scheduled only for startup, replay entries,
            notifier wakes, or a retained proposal retry. Per evaluation it
            performs O(P) pending-proposal work plus the resolver cost
            documented by ConsistencyResolver; retained storage is O(P + R)
            for pending ids and cached accepted history. */
        static constexpr auto name = [] {
            if constexpr (Mode == SubscriptionMode::Auto)
                return "hgraph.fabric.ingress.auto";
            if constexpr (Mode == SubscriptionMode::Live)
                return "hgraph.fabric.ingress.live";
            if constexpr (Mode == SubscriptionMode::Replay)
                return "hgraph.fabric.ingress.replay";
            return "hgraph.fabric.ingress.snapshot";
        }();

        static void start(Scalar<"bridge", IngressBridgeHandle> bridge,
                          Scalar<"as_of", DateTime> as_of,
                          EngineControlView engine, GlobalStateView global_state,
                          AsyncNodeWakeSender wake_sender,
                          SingleShotScheduler scheduler)
        {
            bridge.value().value->start(Mode, as_of.value(), engine,
                                        global_state, wake_sender);
            scheduler.schedule_now();
        }

        static void eval(
            Scalar<"bridge", IngressBridgeHandle> bridge, DateTime now,
            Scalar<"as_of", DateTime>, NodeScheduler scheduler,
            Out<IngressSignals> out)
        {
            bridge.value().value->evaluate(now, scheduler, out);
        }

        static void stop(Scalar<"bridge", IngressBridgeHandle> bridge)
        {
            bridge.value().value->stop();
        }
    };

    template <>
    struct IngressCoordinatorNode<SubscriptionMode::Replay>
    {
        static constexpr auto name = "hgraph.fabric.ingress.replay";

        static void start(Scalar<"bridge", IngressBridgeHandle> bridge,
                          Scalar<"as_of", DateTime> as_of,
                          EngineControlView engine,
                          GlobalStateView global_state,
                          SingleShotScheduler scheduler)
        {
            bridge.value().value->start(SubscriptionMode::Replay,
                                        as_of.value(), engine, global_state,
                                        AsyncNodeWakeSender{});
            scheduler.schedule_now();
        }

        static void eval(
            Scalar<"bridge", IngressBridgeHandle> bridge, DateTime now,
            Scalar<"as_of", DateTime>, NodeScheduler scheduler,
            Out<IngressSignals> out)
        {
            bridge.value().value->evaluate(now, scheduler, out);
        }

        static void stop(Scalar<"bridge", IngressBridgeHandle> bridge)
        {
            bridge.value().value->stop();
        }
    };

    template <>
    struct IngressCoordinatorNode<SubscriptionMode::Snapshot>
    {
        static constexpr auto name = "hgraph.fabric.ingress.snapshot";

        static void start(Scalar<"bridge", IngressBridgeHandle> bridge,
                          Scalar<"as_of", DateTime> as_of,
                          EngineControlView engine,
                          GlobalStateView global_state,
                          SingleShotScheduler scheduler)
        {
            bridge.value().value->start(SubscriptionMode::Snapshot,
                                        as_of.value(), engine, global_state,
                                        AsyncNodeWakeSender{});
            scheduler.schedule_now();
        }

        static void eval(Scalar<"bridge", IngressBridgeHandle> bridge,
                         DateTime now, Scalar<"as_of", DateTime>,
                         Out<IngressSignals> out)
        {
            bridge.value().value->evaluate(now, NodeScheduler{}, out);
        }

        static void stop(Scalar<"bridge", IngressBridgeHandle> bridge)
        {
            bridge.value().value->stop();
        }
    };

    [[nodiscard]] Port<IngressSignals>
    wire_ingress_group(Wiring &wiring, std::vector<Str> roots,
                       SubscriptionMode mode, DateTime as_of);
}  // namespace hgraph::fabric::detail

namespace std
{
    template <>
    struct hash<hgraph::fabric::detail::IngressBridgeHandle>
    {
        size_t operator()(
            const hgraph::fabric::detail::IngressBridgeHandle &value) const noexcept
        {
            return hash<const void *>{}(value.value.get());
        }
    };
}  // namespace std

namespace hgraph::static_schema_detail
{
    template <>
    struct scalar_name<fabric::detail::IngressBridgeHandle>
    {
        static constexpr std::string_view value{
            "hgraph.fabric.internal::IngressBridgeHandle"};
    };
}  // namespace hgraph::static_schema_detail

#endif  // HGRAPH_FABRIC_IMPL_SUBSCRIPTION_RUNTIME_H
