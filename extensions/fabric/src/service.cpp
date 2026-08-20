#include <hgraph/fabric/service.h>

#include "impl/service_runtime.h"

#include <hgraph/fabric/value_builders.h>

#include <hgraph/lib/std/operators/collection.h>
#include <hgraph/lib/std/operators/conversion.h>
#include <hgraph/runtime/executor.h>
#include <hgraph/types/metadata/value_plan_factory.h>
#include <hgraph/types/static_node.h>
#include <hgraph/types/value/value_builder.h>

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
using detail::PublicationRequestInput;
using detail::SubscriptionSpec;

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

struct FabricLifecycleNode
{
    static constexpr auto name = "hgraph.fabric.service.lifecycle";
    static constexpr bool schedule_on_start = true;
    using signature_args = std::tuple<Scalar<"runtime", FabricServiceRuntimeHandle>, GlobalStateView, Out<TS<Bool>>>;

    static void start(Scalar<"runtime", FabricServiceRuntimeHandle> runtime, GlobalStateView global_state)
    {
        runtime.value().value->start(global_state);
    }

    static void eval(Out<TS<Bool>> ready)
    {
        ready.set(true);
    }

    static void stop(Scalar<"runtime", FabricServiceRuntimeHandle> runtime)
    {
        runtime.value().value->stop();
    }
};

struct FabricSnapshotNode
{
    static constexpr auto name = "hgraph.fabric.service.snapshot";

    static void eval(In<"keys", TSS<Str>, InputValidity::Unchecked> keys,
                     In<"ready", TS<Bool>, InputValidity::Unchecked>,
                     Scalar<"runtime", FabricServiceRuntimeHandle> runtime, Out<TSD<Str, FabricIngressSignal>> out)
    {
        const auto &erased = static_cast<const TSSInputView &>(keys);
        if (auto delivery = runtime.value().value->snapshot(subscriptions(erased, SubscriptionMode::Snapshot));
            delivery.has_value())
        {
            apply_delivery(std::move(*delivery), out);
        }
    }
};

/** Planned root snapshots are independent of the keyed subscription
    transport, so the initial image is produced at the exact graph
    start. One evaluation performs one durable consistency resolve. */
struct FabricPlannedSnapshotNode
{
    static constexpr auto name = "hgraph.fabric.service.snapshot.planned";

    static void eval(In<"ready", TS<Bool>, InputValidity::Unchecked>,
                     Scalar<"runtime", FabricServiceRuntimeHandle> runtime, Out<TSD<Str, FabricIngressSignal>> out)
    {
        if (auto delivery = runtime.value().value->planned_snapshot(); delivery.has_value())
        {
            apply_delivery(std::move(*delivery), out);
        }
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
                     Scalar<"runtime", FabricServiceRuntimeHandle> runtime, Out<TSD<Str, FabricIngressSignal>> out)
    {
        const auto &erased = static_cast<const TSSInputView &>(keys);
        if (auto delivery =
                runtime.value().value->replay(subscriptions(erased, SubscriptionMode::Replay), now, scheduler);
            delivery.has_value())
        {
            apply_delivery(std::move(*delivery), out);
        }
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
                     Scalar<"runtime", FabricServiceRuntimeHandle> runtime, Out<TSD<Str, FabricIngressSignal>> out)
    {
        if (auto delivery = runtime.value().value->planned_replay(now, scheduler); delivery.has_value())
        {
            apply_delivery(std::move(*delivery), out);
        }
    }
};

struct FabricLiveNode
{
    static constexpr auto name = "hgraph.fabric.service.live";

    static void eval(In<"keys", TSS<Str>, InputValidity::Unchecked> keys,
                     In<"notices", TSD<Int, TS<DataRevision>>, InputValidity::Unchecked> notices,
                     In<"ready", TS<Bool>, InputValidity::Unchecked>, DateTime now,
                     Scalar<"runtime", FabricServiceRuntimeHandle> runtime, Out<TSD<Str, FabricIngressSignal>> out)
    {
        std::vector<DataRevisionInput> revisions;
        collect_revisions(notices, revisions);
        const auto &erased = static_cast<const TSSInputView &>(keys);
        if (auto delivery =
                runtime.value().value->live(subscriptions(erased, SubscriptionMode::Live), std::move(revisions), now);
            delivery.has_value())
        {
            apply_delivery(std::move(*delivery), out);
        }
    }
};

/** Planned root Live ingress performs one durable startup resolve and
    thereafter admits only complete revisions delivered by the notice
    edge. Work is O(conflated notices plus affected resolver search). */
struct FabricPlannedLiveNode
{
    static constexpr auto name = "hgraph.fabric.service.live.planned";

    static void eval(In<"notices", TSD<Int, TS<DataRevision>>, InputValidity::Unchecked> notices,
                     In<"ready", TS<Bool>, InputValidity::Unchecked>, DateTime now,
                     Scalar<"runtime", FabricServiceRuntimeHandle> runtime, Out<TSD<Str, FabricIngressSignal>> out)
    {
        std::vector<DataRevisionInput> revisions;
        collect_revisions(notices, revisions);
        if (auto delivery = runtime.value().value->planned_live(std::move(revisions), now); delivery.has_value())
        {
            apply_delivery(std::move(*delivery), out);
        }
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
                     In<"ready", TS<Bool>, InputValidity::Unchecked>, NodeScheduler scheduler,
                     Scalar<"runtime", FabricServiceRuntimeHandle> runtime, Out<TSD<Str, TS<DataRevision>>> accepted)
    {
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

        auto mutation = accepted.begin_mutation(accepted.evaluation_time());
        for (auto &revision : runtime.value().value->advance_publications())
        {
            Value key{revision.data_id};
            Value value = make_data_revision(std::move(revision));
            mutation.set(key.view(), value.view());
        }
        if (runtime.value().value->publication_work_pending())
        {
            scheduler.schedule(MIN_TD);
        }
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
                     Scalar<"runtime", FabricServiceRuntimeHandle> runtime, Out<TSD<Int, FabricLoadResponse>> responses)
    {
        if (!requests.modified())
        {
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
    }
};

struct FabricDiagnosticsNode
{
    static constexpr auto name = "hgraph.fabric.service.diagnostics";

    static void eval(In<"ready", TS<Bool>>, Scalar<"runtime", FabricServiceRuntimeHandle> runtime,
                     Out<TSD<Str, TS<Str>>> diagnostics)
    {
        auto mutation = diagnostics.begin_mutation(diagnostics.evaluation_time());
        for (auto &[name, value] : runtime.value().value->diagnostics())
        {
            Value key{std::move(name)};
            Value item{std::move(value)};
            mutation.set(key.view(), item.view());
        }
    }
};

struct FabricServiceImpl
{
    static constexpr auto name = "hgraph.fabric.service_impl";

    static void compose(Wiring &wiring, Scalar<"plan", detail::FabricServicePlanHandle> plan, Scalar<"path", Str> path)
    {
        const auto binding = service::path(path.value());
        auto live_keys = service::impl_input<FabricLiveSubscriptionService>(wiring, binding);
        auto replay_keys = service::impl_input<FabricReplaySubscriptionService>(wiring, binding);
        auto snapshot_keys = service::impl_input<FabricSnapshotSubscriptionService>(wiring, binding);
        auto publications = service::impl_input<FabricPublicationService>(wiring, binding);
        auto notices = service::impl_input<FabricNoticeService>(wiring, binding);
        auto loads = service::impl_input<FabricLoadService>(wiring, binding);

        FabricServiceRuntimeHandle runtime{std::make_shared<detail::FabricServiceRuntime>(plan.value())};
        auto ready = wire<FabricLifecycleNode>(wiring, runtime);
        static_cast<void>(wire<FabricPublicationNode>(wiring, publications, ready, runtime));
        auto snapshot = wire<FabricSnapshotNode>(wiring, snapshot_keys, ready, runtime);
        auto replay = wire<FabricReplayNode>(wiring, replay_keys, ready, runtime);
        auto live = wire<FabricLiveNode>(wiring, live_keys, notices, ready, runtime);
        auto planned_snapshot = wire<FabricPlannedSnapshotNode>(wiring, ready, runtime);
        auto planned_replay = wire<FabricPlannedReplayNode>(wiring, ready, runtime);
        auto planned_live = wire<FabricPlannedLiveNode>(wiring, notices, ready, runtime);
        auto loaded = wire<FabricLoadNode>(wiring, loads, ready, runtime);
        auto diagnostic_values = wire<FabricDiagnosticsNode>(wiring, ready, runtime);

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
                                                       diagnostic_values.template as<TSD<Str, TS<Str>>>());
    }
};
} // namespace

void register_service(Wiring &wiring, service::ServicePath path)
{
    const auto plan = detail::service_plan(wiring, path.value);
    service::register_services<FabricServiceImpl, FabricLiveSubscriptionService, FabricReplaySubscriptionService,
                               FabricSnapshotSubscriptionService, detail::FabricPlannedLiveService,
                               detail::FabricPlannedReplayService, detail::FabricPlannedSnapshotService,
                               FabricPublicationService, FabricNoticeService, FabricLoadService,
                               FabricDiagnosticsService>(wiring, std::move(path), plan);
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

Port<TSD<Str, TS<Str>>> diagnostics(Wiring &wiring, service::ServicePath path)
{
    return wire<FabricDiagnosticsService>(wiring, std::move(path));
}

Port<TSD<Str, TS<Str>>> diagnostics(Wiring &wiring)
{
    return diagnostics(wiring, service::path(DEFAULT_SERVICE_PATH));
}
} // namespace hgraph::fabric
