#include <hgraph/fabric/service.h>

#include "impl/service_runtime.h"

#include <hgraph/fabric/value_builders.h>

#include <hgraph/lib/std/operators/collection.h>
#include <hgraph/lib/std/operators/container.h>
#include <hgraph/lib/std/operators/conversion.h>
#include <hgraph/runtime/executor.h>
#include <hgraph/runtime/logger.h>
#include <hgraph/types/metadata/value_plan_factory.h>
#include <hgraph/types/static_node.h>
#include <hgraph/types/value/value_builder.h>
#include <hgraph/util/scope.h>

#include <algorithm>
#include <memory>
#include <stdexcept>
#include <tuple>
#include <utility>
#include <vector>

namespace hgraph::fabric
{
namespace
{
using detail::DeliveryBatch;
using detail::FabricServiceRuntimeHandle;
using detail::GraphNotificationBridgeHandle;
using detail::NotificationDeliveryInput;
using detail::PublicationRequestInput;
using detail::SubscriptionSpec;
using detail::TransportControlInput;
using detail::TransportEventInput;

template <typename ValueSchema>
using FabricServiceNodeResult =
    UnNamedTSB<Field<"value", ValueSchema>, Field<"diagnostics_changed", TS<Int>>>;

void emit_diagnostic_change(Int before, const FabricServiceRuntimeHandle &runtime,
                            const Out<TS<Int>> &diagnostics_changed)
{
    const Int after = runtime.value->diagnostic_revision();
    if (after != before)
    {
        diagnostics_changed.set(after);
    }
}

template <typename ValueSchema>
[[nodiscard]] Port<ValueSchema> service_result_value(Wiring &wiring,
                                                     Port<FabricServiceNodeResult<ValueSchema>> result)
{
    return wire<stdlib::getattr_>(wiring, result, Str{"value"}).template as<ValueSchema>();
}

template <typename ValueSchema>
[[nodiscard]] Port<TS<Int>> service_result_diagnostics(Wiring &wiring,
                                                       Port<FabricServiceNodeResult<ValueSchema>> result)
{
    return wire<stdlib::getattr_>(wiring, result, Str{"diagnostics_changed"}).template as<TS<Int>>();
}

[[nodiscard]] Value ingress_value(const detail::DeliveredRoot &root)
{
    BundleBuilder builder{
        ValuePlanFactory::instance().type_for(schema_descriptor<FabricIngressSignal>::ts_meta()->value_schema)};
    if (root.frame.has_value())
    {
        builder.set("frame", Value{*root.frame});
    }
    builder.set("version", Value{root.output_version});
    builder.set("revision", Value{root.revision});
    return builder.build();
}

void apply_delivery(DeliveryBatch delivery, Out<TSD<Str, FabricIngressSignal>> &out)
{
    auto mutation = out.begin_mutation(out.evaluation_time());
    for (const auto &root : delivery.roots)
    {
        Value key{root.key};
        Value update = ingress_value(root);
        mutation.set(key.view(), update.view());
    }
}

[[nodiscard]] std::vector<SubscriptionSpec> subscriptions(const TSSInputView &keys, SubscriptionMode mode)
{
    std::vector<SubscriptionSpec> result;
    if (!keys.valid())
    {
        return result;
    }
    result.reserve(keys.size());
    for (const ValueView key : keys.values())
    {
        result.push_back(detail::decode_subscription_key(key.checked_as<Str>(), mode));
    }
    return result;
}

template <typename Requests> void collect_revisions(const Requests &requests, std::vector<DataRevisionInput> &revisions)
{
    if (!requests.modified())
    {
        return;
    }
    for (const auto &[key, revision] : requests.modified_items())
    {
        static_cast<void>(key);
        if (!revision.valid() || !revision.modified())
        {
            continue;
        }
        revisions.push_back(data_revision_input(revision.base().value()));
    }
}

[[nodiscard]] std::optional<TransportControlInput>
transport_control(const In<"controls", TSD<Int, FabricTransportControl>, InputValidity::Unchecked> &controls)
{
    std::optional<TransportControlInput> result;
    for (const auto &[request_id, control] : controls.valid_items())
    {
        static_cast<void>(request_id);
        const auto ready = control.template field<"ready">();
        const auto reconcile = control.template field<"reconcile">();
        const auto failed = control.template field<"failed">();
        const auto message = control.template field<"message">();
        TransportControlInput next{
            .ready = ready.valid() && ready.value(),
            .reconcile = reconcile.valid() && reconcile.modified() && reconcile.value(),
            .failed = failed.valid() && failed.value(),
            .message = message.valid() ? message.value() : Str{},
        };
        if (result.has_value() && *result != next)
        {
            throw std::logic_error("fabric graph transport has conflicting lifecycle clients");
        }
        result = std::move(next);
    }
    return result;
}

void apply_delivery_reports(const In<"deliveries", TSD<Int, FabricNotificationDelivery>, InputValidity::Unchecked> &deliveries,
                            const GraphNotificationBridgeHandle &bridge)
{
    if (!bridge.value || !deliveries.modified())
    {
        return;
    }
    for (const auto &[request_id, delivery] : deliveries.modified_items())
    {
        static_cast<void>(request_id);
        if (!delivery.valid())
        {
            continue;
        }
        const auto data_id = delivery.template field<"data_id">();
        const auto revision = delivery.template field<"revision">();
        const auto delivered = delivery.template field<"delivered">();
        const auto retriable = delivery.template field<"retriable">();
        const auto message = delivery.template field<"message">();
        if (!data_id.valid() || !revision.valid() || !delivered.valid() || !retriable.valid())
        {
            throw std::invalid_argument("fabric notification delivery is incomplete");
        }
        bridge.value->complete(NotificationDeliveryInput{
            .data_id = data_id.value(),
            .revision = revision.value(),
            .delivered = delivered.value(),
            .retriable = retriable.value(),
            .message = message.valid() ? message.value() : Str{},
        });
    }
}

struct FabricLifecycleNode
{
    static constexpr auto name = "hgraph.fabric.service.lifecycle";
    static constexpr bool schedule_on_start = true;
    using signature_args =
        std::tuple<Scalar<"runtime", FabricServiceRuntimeHandle>, Scalar<"path", Str>, GlobalStateView, Out<TS<Bool>>>;

    static void start(Scalar<"runtime", FabricServiceRuntimeHandle> runtime, Scalar<"path", Str> path,
                      GlobalStateView global_state, LoggerView log)
    {
        runtime.value().value->start(global_state);
        log.info("hgraph.fabric service started path={}", path.value());
    }

    static void eval(Out<TS<Bool>> ready)
    {
        ready.set(true);
    }

    static void stop(Scalar<"runtime", FabricServiceRuntimeHandle> runtime, Scalar<"path", Str> path, LoggerView log)
    {
        runtime.value().value->stop();
        log.info("hgraph.fabric service stopped path={}", path.value());
    }
};

struct FabricSnapshotNode
{
    static constexpr auto name = "hgraph.fabric.service.snapshot";

    static void eval(In<"keys", TSS<Str>, InputValidity::Unchecked> keys,
                     In<"ready", TS<Bool>, InputValidity::Unchecked>,
                     Scalar<"runtime", FabricServiceRuntimeHandle> runtime,
                     Out<FabricServiceNodeResult<TSD<Str, FabricIngressSignal>>> result)
    {
        auto out = result.template field<"value">();
        auto diagnostics_changed = result.template field<"diagnostics_changed">();
        const Int before = runtime.value().value->diagnostic_revision();
        UnwindCleanupGuard diagnostic_change{
            [&] { emit_diagnostic_change(before, runtime.value(), diagnostics_changed); }};
        const auto &erased = static_cast<const TSSInputView &>(keys);
        if (auto delivery = runtime.value().value->snapshot(subscriptions(erased, SubscriptionMode::Snapshot));
            delivery.has_value())
        {
            apply_delivery(std::move(*delivery), out);
        }
        diagnostic_change.complete();
    }
};

/** Planned root snapshots are independent of the keyed subscription
    transport, so the initial image is produced at the exact graph
    start. One evaluation performs one durable consistency resolve. */
struct FabricPlannedSnapshotNode
{
    static constexpr auto name = "hgraph.fabric.service.snapshot.planned";

    static void eval(In<"ready", TS<Bool>, InputValidity::Unchecked>,
                     Scalar<"runtime", FabricServiceRuntimeHandle> runtime,
                     Out<FabricServiceNodeResult<TSD<Str, FabricIngressSignal>>> result)
    {
        auto out = result.template field<"value">();
        auto diagnostics_changed = result.template field<"diagnostics_changed">();
        const Int before = runtime.value().value->diagnostic_revision();
        UnwindCleanupGuard diagnostic_change{
            [&] { emit_diagnostic_change(before, runtime.value(), diagnostics_changed); }};
        if (auto delivery = runtime.value().value->planned_snapshot(); delivery.has_value())
        {
            apply_delivery(std::move(*delivery), out);
        }
        diagnostic_change.complete();
    }
};

struct FabricReplayNode
{
    static constexpr auto name = "hgraph.fabric.service.replay";

    static void start(Scalar<"runtime", FabricServiceRuntimeHandle> runtime, EngineControlView engine)
    {
        runtime.value().value->configure_replay_window(engine.start_time(), engine.end_time());
    }

    static void eval(In<"keys", TSS<Str>, InputValidity::Unchecked> keys,
                     In<"ready", TS<Bool>, InputValidity::Unchecked>, DateTime now, NodeScheduler scheduler,
                     Scalar<"runtime", FabricServiceRuntimeHandle> runtime,
                     Out<FabricServiceNodeResult<TSD<Str, FabricIngressSignal>>> result)
    {
        auto out = result.template field<"value">();
        auto diagnostics_changed = result.template field<"diagnostics_changed">();
        const Int before = runtime.value().value->diagnostic_revision();
        UnwindCleanupGuard diagnostic_change{
            [&] { emit_diagnostic_change(before, runtime.value(), diagnostics_changed); }};
        const auto &erased = static_cast<const TSSInputView &>(keys);
        if (auto delivery =
                runtime.value().value->replay(subscriptions(erased, SubscriptionMode::Replay), now, scheduler);
            delivery.has_value())
        {
            apply_delivery(std::move(*delivery), out);
        }
        diagnostic_change.complete();
    }
};

/** Planned root replay is an ordinary scheduled source. Per tick it
    resolves one equal-as-of batch and schedules the next durable
    history time; retained memory is the reachable revision history. */
struct FabricPlannedReplayNode
{
    static constexpr auto name = "hgraph.fabric.service.replay.planned";

    static void start(Scalar<"runtime", FabricServiceRuntimeHandle> runtime, EngineControlView engine)
    {
        runtime.value().value->configure_replay_window(engine.start_time(), engine.end_time());
    }

    static void eval(In<"ready", TS<Bool>, InputValidity::Unchecked>, DateTime now, NodeScheduler scheduler,
                     Scalar<"runtime", FabricServiceRuntimeHandle> runtime,
                     Out<FabricServiceNodeResult<TSD<Str, FabricIngressSignal>>> result)
    {
        auto out = result.template field<"value">();
        auto diagnostics_changed = result.template field<"diagnostics_changed">();
        const Int before = runtime.value().value->diagnostic_revision();
        UnwindCleanupGuard diagnostic_change{
            [&] { emit_diagnostic_change(before, runtime.value(), diagnostics_changed); }};
        if (auto delivery = runtime.value().value->planned_replay(now, scheduler); delivery.has_value())
        {
            apply_delivery(std::move(*delivery), out);
        }
        diagnostic_change.complete();
    }
};

struct FabricLiveNode
{
    static constexpr auto name = "hgraph.fabric.service.live";

    static void eval(In<"keys", TSS<Str>, InputValidity::Unchecked> keys,
                     In<"notices", TSD<Int, TS<DataRevision>>, InputValidity::Unchecked> notices,
                     In<"controls", TSD<Int, FabricTransportControl>, InputValidity::Unchecked> controls,
                     In<"ready", TS<Bool>, InputValidity::Unchecked>, DateTime now,
                     Scalar<"notification_mode", FabricNotificationMode> notification_mode,
                     Scalar<"runtime", FabricServiceRuntimeHandle> runtime,
                     Out<FabricServiceNodeResult<TSD<Str, FabricIngressSignal>>> result)
    {
        auto out = result.template field<"value">();
        auto diagnostics_changed = result.template field<"diagnostics_changed">();
        const Int before = runtime.value().value->diagnostic_revision();
        UnwindCleanupGuard diagnostic_change{
            [&] { emit_diagnostic_change(before, runtime.value(), diagnostics_changed); }};
        const auto control = transport_control(controls);
        if (notification_mode.value() == FabricNotificationMode::GraphTransport)
        {
            if (control.has_value() && control->failed)
            {
                throw std::runtime_error(control->message.empty() ? "fabric transport failed" : control->message);
            }
            if (!control.has_value() || !control->ready)
            {
                diagnostic_change.complete();
                return;
            }
        }
        std::vector<DataRevisionInput> revisions;
        collect_revisions(notices, revisions);
        const auto &erased = static_cast<const TSSInputView &>(keys);
        if (auto delivery = runtime.value().value->live(subscriptions(erased, SubscriptionMode::Live),
                                                        std::move(revisions), now,
                                                        control.has_value() && control->reconcile);
            delivery.has_value())
        {
            apply_delivery(std::move(*delivery), out);
        }
        diagnostic_change.complete();
    }
};

/** Planned root Live ingress performs one durable startup resolve and
    thereafter admits only complete revisions delivered by the notice
    edge. Work is O(conflated notices plus affected resolver search). */
struct FabricPlannedLiveNode
{
    static constexpr auto name = "hgraph.fabric.service.live.planned";

    static void eval(In<"notices", TSD<Int, TS<DataRevision>>, InputValidity::Unchecked> notices,
                     In<"controls", TSD<Int, FabricTransportControl>, InputValidity::Unchecked> controls,
                     In<"ready", TS<Bool>, InputValidity::Unchecked>, DateTime now,
                     Scalar<"notification_mode", FabricNotificationMode> notification_mode,
                     Scalar<"runtime", FabricServiceRuntimeHandle> runtime,
                     Out<FabricServiceNodeResult<TSD<Str, FabricIngressSignal>>> result)
    {
        auto out = result.template field<"value">();
        auto diagnostics_changed = result.template field<"diagnostics_changed">();
        const Int before = runtime.value().value->diagnostic_revision();
        UnwindCleanupGuard diagnostic_change{
            [&] { emit_diagnostic_change(before, runtime.value(), diagnostics_changed); }};
        const auto control = transport_control(controls);
        if (notification_mode.value() == FabricNotificationMode::GraphTransport)
        {
            if (control.has_value() && control->failed)
            {
                throw std::runtime_error(control->message.empty() ? "fabric transport failed" : control->message);
            }
            if (!control.has_value() || !control->ready)
            {
                diagnostic_change.complete();
                return;
            }
        }
        std::vector<DataRevisionInput> revisions;
        collect_revisions(notices, revisions);
        if (auto delivery = runtime.value().value->planned_live(std::move(revisions), now,
                                                                control.has_value() && control->reconcile);
            delivery.has_value())
        {
            apply_delivery(std::move(*delivery), out);
        }
        diagnostic_change.complete();
    }
};

[[nodiscard]] PublicationRequestInput publication_request(const TSBInputView &request)
{
    const auto data_id = request.field("data_id");
    if (!data_id.valid())
    {
        throw std::invalid_argument("fabric publication request requires data_id");
    }
    PublicationRequestInput result{.data_id = data_id.value().checked_as<Str>()};

    const auto frame = request.field("frame");
    if (frame.valid() && frame.modified())
    {
        result.output = frame.value().checked_as<Frame>();
    }

    const auto dependency_field = request.field("dependencies");
    const auto dependencies = dependency_field.as_dict();
    if (dependencies.valid())
    {
        for (const auto &[dependency_id, version] : dependencies.valid_items())
        {
            if (!version.valid())
            {
                continue;
            }
            result.dependencies.push_back(DataDependencyInput{
                .data_id = dependency_id.checked_as<Str>(),
                .version = version.value().checked_as<Int>(),
            });
        }
    }
    const auto predecessor = request.field("self_predecessor");
    if (predecessor.valid())
    {
        result.self_predecessor = predecessor.value().checked_as<Int>();
    }
    return result;
}

struct FabricPublicationNode
{
    static constexpr auto name = "hgraph.fabric.service.publication";

    static void eval(In<"requests", TSD<Int, FabricPublicationRequest>, InputValidity::Unchecked> requests,
                     In<"deliveries", TSD<Int, FabricNotificationDelivery>, InputValidity::Unchecked> deliveries,
                     In<"ready", TS<Bool>, InputValidity::Unchecked>, NodeScheduler scheduler,
                     Scalar<"runtime", FabricServiceRuntimeHandle> runtime,
                     Scalar<"bridge", GraphNotificationBridgeHandle> bridge,
                     Out<FabricServiceNodeResult<TS<DataRevision>>> result)
    {
        auto notifications = result.template field<"value">();
        auto diagnostics_changed = result.template field<"diagnostics_changed">();
        const Int before = runtime.value().value->diagnostic_revision();
        UnwindCleanupGuard diagnostic_change{
            [&] { emit_diagnostic_change(before, runtime.value(), diagnostics_changed); }};
        apply_delivery_reports(deliveries, bridge.value());
        if (requests.modified())
        {
            for (const auto &[request_id, request] : requests.modified_items())
            {
                static_cast<void>(request_id);
                if (!request.valid())
                {
                    continue;
                }
                runtime.value().value->publish(publication_request(request));
            }
        }

        static_cast<void>(runtime.value().value->advance_publications());
        if (bridge.value().value)
        {
            if (auto revision = bridge.value().value->take_request(); revision.has_value())
            {
                Value value = make_data_revision(std::move(*revision));
                notifications.apply(value.view());
            }
        }
        if (runtime.value().value->publication_work_pending() ||
            (bridge.value().value && bridge.value().value->request_pending()))
        {
            scheduler.schedule(MIN_TD);
        }
        diagnostic_change.complete();
    }
};

[[nodiscard]] Value load_response_value(Str data_id, DataVersion version, Frame frame)
{
    BundleBuilder builder{
        ValuePlanFactory::instance().type_for(schema_descriptor<FabricLoadResponse>::ts_meta()->value_schema)};
    builder.set("data_id", Value{std::move(data_id)});
    builder.set("version", Value{version});
    builder.set("frame", Value{std::move(frame)});
    return builder.build();
}

struct FabricLoadNode
{
    static constexpr auto name = "hgraph.fabric.service.load";

    static void eval(In<"requests", TSD<Int, FabricLoadRequest>, InputValidity::Unchecked> requests,
                     In<"ready", TS<Bool>, InputValidity::Unchecked>,
                     Scalar<"runtime", FabricServiceRuntimeHandle> runtime,
                     Out<FabricServiceNodeResult<TSD<Int, FabricLoadResponse>>> result)
    {
        auto responses = result.template field<"value">();
        auto diagnostics_changed = result.template field<"diagnostics_changed">();
        const Int before = runtime.value().value->diagnostic_revision();
        UnwindCleanupGuard diagnostic_change{
            [&] { emit_diagnostic_change(before, runtime.value(), diagnostics_changed); }};
        if (!requests.modified())
        {
            diagnostic_change.complete();
            return;
        }
        auto mutation = responses.begin_mutation(responses.evaluation_time());
        for (const auto &[request_id, request] : requests.modified_items())
        {
            if (!request.valid())
            {
                continue;
            }
            const auto requested_data_id = request.template field<"data_id">();
            const auto requested_version = request.template field<"version">();
            if (!requested_data_id.valid() || !requested_version.valid())
            {
                continue;
            }
            const auto loaded = runtime.value().value->load(requested_data_id.value(), requested_version.value());
            if (!loaded.has_value())
            {
                continue;
            }
            auto [data_id, version, frame] = *loaded;
            Value response = load_response_value(std::move(data_id), version, std::move(frame));
            mutation.set(request_id, response.view());
        }
        diagnostic_change.complete();
    }
};

struct FabricDiagnosticsNode
{
    static constexpr auto name = "hgraph.fabric.service.diagnostics";

    /** Runs only when service lifecycle, transport events, or explicit
        internal-diagnostic edges tick. Work is O(stable metric count +
        retained distinct event paths), with event paths bounded by
        ``FABRIC_DIAGNOSTIC_EVENT_LIMIT``. */
    static void eval(In<"events", TSD<Int, FabricTransportEvent>, InputValidity::Unchecked> events,
                     In<"ready", TS<Bool>>, In<"publication_change", TS<Int>, InputValidity::Unchecked>,
                     In<"snapshot_change", TS<Int>, InputValidity::Unchecked>,
                     In<"planned_snapshot_change", TS<Int>, InputValidity::Unchecked>,
                     In<"replay_change", TS<Int>, InputValidity::Unchecked>,
                     In<"planned_replay_change", TS<Int>, InputValidity::Unchecked>,
                     In<"live_change", TS<Int>, InputValidity::Unchecked>,
                     In<"planned_live_change", TS<Int>, InputValidity::Unchecked>,
                     In<"load_change", TS<Int>, InputValidity::Unchecked>,
                     Scalar<"runtime", FabricServiceRuntimeHandle> runtime,
                     Scalar<"bridge", GraphNotificationBridgeHandle> bridge,
                     Out<FabricDiagnostics> diagnostics)
    {
        if (events.modified())
        {
            for (const auto &[request_id, event] : events.modified_items())
            {
                static_cast<void>(request_id);
                if (!event.valid())
                {
                    continue;
                }
                const auto component = event.template field<"component">();
                const auto category = event.template field<"category">();
                const auto message = event.template field<"message">();
                const auto retriable = event.template field<"retriable">();
                const auto fatal = event.template field<"fatal">();
                if (!component.valid() || !category.valid() || !retriable.valid() || !fatal.valid())
                {
                    throw std::invalid_argument("fabric transport event is incomplete");
                }
                runtime.value().value->observe_transport_event(TransportEventInput{
                    .component = component.value(),
                    .category = category.value(),
                    .message = message.valid() ? message.value() : Str{},
                    .retriable = retriable.value(),
                    .fatal = fatal.value(),
                });
            }
        }
        auto metrics = diagnostics.template field<"metrics">();
        auto mutation = metrics.begin_mutation(metrics.evaluation_time());
        for (auto &[metric_name, value] : runtime.value().value->diagnostics())
        {
            Value key{std::move(metric_name)};
            Value item{std::move(value)};
            mutation.set(key.view(), item.view());
        }
        if (bridge.value().value)
        {
            for (auto &[metric_name, value] : bridge.value().value->diagnostics())
            {
                Value key{std::move(metric_name)};
                Value item{std::move(value)};
                mutation.set(key.view(), item.view());
            }
        }

        auto diagnostic_events = diagnostics.template field<"events">();
        auto event_mutation = diagnostic_events.begin_mutation(diagnostic_events.evaluation_time());
        for (auto &[path, event] : runtime.value().value->events())
        {
            BundleBuilder builder{ValuePlanFactory::instance().type_for(
                scalar_descriptor<FabricDiagnosticEvent>::value_meta())};
            builder.set("component", Value{std::move(event.component)});
            builder.set("category", Value{std::move(event.category)});
            builder.set("message", Value{std::move(event.message)});
            builder.set("retriable", Value{event.retriable});
            builder.set("fatal", Value{event.fatal});
            builder.set("occurrences", Value{event.occurrences});
            Value key{std::move(path)};
            Value item = builder.build();
            event_mutation.set(key.view(), item.view());
        }
    }
};

struct FabricServiceImpl
{
    static constexpr auto name = "hgraph.fabric.service_impl";

    static void compose(Wiring &wiring, Scalar<"plan", detail::FabricServicePlanHandle> plan,
                        Scalar<"notification_mode", FabricNotificationMode> notification_mode,
                        Scalar<"path", Str> path)
    {
        const auto binding = service::path(path.value());
        auto live_keys = service::impl_input<FabricLiveSubscriptionService>(wiring, binding);
        auto replay_keys = service::impl_input<FabricReplaySubscriptionService>(wiring, binding);
        auto snapshot_keys = service::impl_input<FabricSnapshotSubscriptionService>(wiring, binding);
        auto publications = service::impl_input<FabricPublicationService>(wiring, binding);
        auto notices = service::impl_input<FabricNoticeService>(wiring, binding);
        auto deliveries = service::impl_input<FabricNotificationDeliveryService>(wiring, binding);
        auto controls = service::impl_input<FabricTransportControlService>(wiring, binding);
        auto events = service::impl_input<FabricTransportEventService>(wiring, binding);
        auto loads = service::impl_input<FabricLoadService>(wiring, binding);

        GraphNotificationBridgeHandle bridge{};
        std::optional<Notifier> notification_override;
        if (notification_mode.value() == FabricNotificationMode::GraphTransport)
        {
            bridge.value = std::make_shared<detail::GraphNotificationBridge>();
            notification_override = bridge.value->notifier();
        }
        FabricServiceRuntimeHandle runtime{
            std::make_shared<detail::FabricServiceRuntime>(plan.value(), std::move(notification_override))};
        auto ready = wire<FabricLifecycleNode>(wiring, runtime, path.value());
        auto publication_result =
            wire<FabricPublicationNode>(wiring, publications, deliveries, ready, runtime, bridge);
        auto snapshot_result = wire<FabricSnapshotNode>(wiring, snapshot_keys, ready, runtime);
        auto replay_result = wire<FabricReplayNode>(wiring, replay_keys, ready, runtime);
        auto live_result =
            wire<FabricLiveNode>(wiring, live_keys, notices, controls, ready, notification_mode.value(), runtime);
        auto planned_snapshot_result = wire<FabricPlannedSnapshotNode>(wiring, ready, runtime);
        auto planned_replay_result = wire<FabricPlannedReplayNode>(wiring, ready, runtime);
        auto planned_live_result =
            wire<FabricPlannedLiveNode>(wiring, notices, controls, ready, notification_mode.value(), runtime);
        auto load_result = wire<FabricLoadNode>(wiring, loads, ready, runtime);

        auto notification_requests = service_result_value(wiring, publication_result);
        auto snapshot = service_result_value(wiring, snapshot_result);
        auto replay = service_result_value(wiring, replay_result);
        auto live = service_result_value(wiring, live_result);
        auto planned_snapshot = service_result_value(wiring, planned_snapshot_result);
        auto planned_replay = service_result_value(wiring, planned_replay_result);
        auto planned_live = service_result_value(wiring, planned_live_result);
        auto loaded = service_result_value(wiring, load_result);
        auto diagnostic_values = wire<FabricDiagnosticsNode>(
            wiring, events, ready, service_result_diagnostics(wiring, publication_result),
            service_result_diagnostics(wiring, snapshot_result),
            service_result_diagnostics(wiring, planned_snapshot_result),
            service_result_diagnostics(wiring, replay_result),
            service_result_diagnostics(wiring, planned_replay_result),
            service_result_diagnostics(wiring, live_result), service_result_diagnostics(wiring, planned_live_result),
            service_result_diagnostics(wiring, load_result), runtime, bridge);

        service::impl_output<FabricLiveSubscriptionService>(wiring, binding,
                                                            live.template as<TSD<Str, FabricIngressSignal>>());
        service::impl_output<FabricReplaySubscriptionService>(wiring, binding,
                                                              replay.template as<TSD<Str, FabricIngressSignal>>());
        service::impl_output<FabricSnapshotSubscriptionService>(wiring, binding,
                                                                snapshot.template as<TSD<Str, FabricIngressSignal>>());
        service::impl_output<detail::FabricPlannedLiveService>(
            wiring, binding, planned_live.template as<TSD<Str, FabricIngressSignal>>());
        service::impl_output<detail::FabricPlannedReplayService>(
            wiring, binding, planned_replay.template as<TSD<Str, FabricIngressSignal>>());
        service::impl_output<detail::FabricPlannedSnapshotService>(
            wiring, binding, planned_snapshot.template as<TSD<Str, FabricIngressSignal>>());
        service::impl_output<FabricLoadService>(wiring, binding, loaded.template as<TSD<Int, FabricLoadResponse>>());
        service::impl_output<FabricDiagnosticsService>(wiring, binding,
                                                       diagnostic_values.template as<FabricDiagnostics>());
        service::impl_output<FabricNotificationRequestService>(wiring, binding,
                                                               notification_requests.template as<TS<DataRevision>>());
    }
};
} // namespace

void register_service(Wiring &wiring, service::ServicePath path)
{
    register_service(wiring, std::move(path), FabricNotificationMode::Configured);
}

void register_service(Wiring &wiring, service::ServicePath path, FabricNotificationMode mode)
{
    const auto plan = detail::service_plan(wiring, path.value);
    service::register_services<FabricServiceImpl, FabricLiveSubscriptionService, FabricReplaySubscriptionService,
                               FabricSnapshotSubscriptionService, detail::FabricPlannedLiveService,
                               detail::FabricPlannedReplayService, detail::FabricPlannedSnapshotService,
                               FabricPublicationService, FabricNoticeService, FabricLoadService,
                               FabricDiagnosticsService, FabricNotificationRequestService,
                               FabricNotificationDeliveryService, FabricTransportControlService,
                               FabricTransportEventService>(wiring, std::move(path), plan, mode);
}

void register_service(Wiring &wiring)
{
    register_service(wiring, service::path(DEFAULT_SERVICE_PATH));
}

void submit_notice(Wiring &wiring, Port<TS<DataRevision>> notice, service::ServicePath path)
{
    wire<FabricNoticeService>(wiring, std::move(path), notice);
}

void submit_notice(Wiring &wiring, Port<TS<DataRevision>> notice)
{
    submit_notice(wiring, std::move(notice), service::path(DEFAULT_SERVICE_PATH));
}

Port<TS<DataRevision>> notification_requests(Wiring &wiring, service::ServicePath path)
{
    return wire<FabricNotificationRequestService>(wiring, std::move(path));
}

Port<TS<DataRevision>> notification_requests(Wiring &wiring)
{
    return notification_requests(wiring, service::path(DEFAULT_SERVICE_PATH));
}

void submit_notification_delivery(Wiring &wiring, Port<FabricNotificationDelivery> delivery, service::ServicePath path)
{
    wire<FabricNotificationDeliveryService>(wiring, std::move(path), delivery);
}

void submit_transport_control(Wiring &wiring, Port<FabricTransportControl> control, service::ServicePath path)
{
    wire<FabricTransportControlService>(wiring, std::move(path), control);
}

void submit_transport_event(Wiring &wiring, Port<FabricTransportEvent> event, service::ServicePath path)
{
    wire<FabricTransportEventService>(wiring, std::move(path), event);
}

Port<FabricLoadResponse> request_load(Wiring &wiring, Str data_id, DataVersion version, service::ServicePath path)
{
    require_data_id(data_id);
    if (version <= 0)
    {
        throw std::invalid_argument("fabric load version must be positive");
    }
    auto id = wire<stdlib::const_, TS<Str>>(wiring, std::move(data_id));
    auto requested_version = wire<stdlib::const_, TS<Int>>(wiring, version);
    auto request = stdlib::to_tsb<FabricLoadRequest>(wiring, id, requested_version);
    return wire<FabricLoadService>(wiring, std::move(path), request);
}

Port<FabricLoadResponse> request_load(Wiring &wiring, Str data_id, DataVersion version)
{
    return request_load(wiring, std::move(data_id), version, service::path(DEFAULT_SERVICE_PATH));
}

Port<FabricDiagnostics> diagnostics(Wiring &wiring, service::ServicePath path)
{
    return wire<FabricDiagnosticsService>(wiring, std::move(path));
}

Port<FabricDiagnostics> diagnostics(Wiring &wiring)
{
    return diagnostics(wiring, service::path(DEFAULT_SERVICE_PATH));
}
} // namespace hgraph::fabric
