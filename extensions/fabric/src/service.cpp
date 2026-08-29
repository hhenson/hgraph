#include <hgraph/fabric/service.h>

#include "impl/service_state.h"

#include <hgraph/fabric/keys.h>
#include <hgraph/fabric/value_builders.h>

#include <hgraph/lib/std/operators/collection.h>
#include <hgraph/lib/std/operators/container.h>
#include <hgraph/lib/std/operators/control.h>
#include <hgraph/lib/std/operators/conversion.h>
#include <hgraph/lib/std/value_util.h>
#include <hgraph/runtime/executor.h>
#include <hgraph/runtime/logger.h>
#include <hgraph/types/metadata/value_plan_factory.h>
#include <hgraph/types/static_node.h>
#include <hgraph/types/value/value_builder.h>
#include <hgraph/util/scope.h>

#include <algorithm>
#include <cstdint>
#include <iterator>
#include <limits>
#include <map>
#include <memory>
#include <stdexcept>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

namespace hgraph::fabric
{
    namespace
    {
        using detail::DeliveryBatch;
        using detail::FabricNodeDiagnostics;
        using detail::NotificationDeliveryInput;
        using detail::PublicationRequestInput;
        using detail::SubscriptionSpec;
        using detail::TransportControlInput;

        using FabricNotificationCompletion =
            TSB<"hgraph.fabric::NotificationCompletion", Field<"data_id", TS<Str>>,
                Field<"revision", TS<Int>>, Field<"delivered", TS<Bool>>,
                Field<"message", TS<Str>>>;

        using FabricNotificationFlowResult =
            UnNamedTSB<Field<"request", REF<TS<Shared<DataRevision>>>>,
                       Field<"active_data_id", TS<Str>>, Field<"active_revision", TS<Int>>,
                       Field<"active", TS<Bool>>, Field<"retry_count", TS<Int>>,
                       Field<"retry_pending", TS<Bool>>,
                       Field<"completions", TSD<Str, FabricNotificationCompletion>>,
                       Field<"delivered", TS<Int>>, Field<"retried", TS<Int>>,
                       Field<"failed", TS<Int>>, Field<"stale_reports", TS<Int>>>;

        template <typename ValueSchema>
        using FabricServiceNodeResult =
            UnNamedTSB<Field<"value", ValueSchema>, Field<"metrics", TSD<Str, TS<Str>>>,
                       Field<"events", TSD<Str, TS<FabricDiagnosticEvent>>>>;

        template <typename ValueSchema>
        [[nodiscard]] Port<ValueSchema>
        service_result_value(Wiring &wiring, Port<FabricServiceNodeResult<ValueSchema>> result)
        {
            return wire<stdlib::getattr_>(wiring, result, Str{"value"}).template as<ValueSchema>();
        }

        template <typename ValueSchema>
        [[nodiscard]] Port<TSD<Str, TS<Str>>>
        service_result_metrics(Wiring &wiring, Port<FabricServiceNodeResult<ValueSchema>> result)
        {
            return wire<stdlib::getattr_>(wiring, result, Str{"metrics"})
                .template as<TSD<Str, TS<Str>>>();
        }

        template <typename ValueSchema>
        [[nodiscard]] Port<TSD<Str, TS<FabricDiagnosticEvent>>>
        service_result_events(Wiring &wiring, Port<FabricServiceNodeResult<ValueSchema>> result)
        {
            return wire<stdlib::getattr_>(wiring, result, Str{"events"})
                .template as<TSD<Str, TS<FabricDiagnosticEvent>>>();
        }

        [[nodiscard]] FabricConfig service_config(GlobalStateView global_state)
        {
            auto config = fabric_config(global_state);
            if (!config.has_value())
            {
                throw std::logic_error(
                    "hgraph.fabric service requires FabricConfig in GlobalState");
            }
            require_valid_config(*config);
            return std::move(*config);
        }

        [[nodiscard]] Value diagnostic_event_value(const FabricDiagnosticEventInput &event)
        {
            BundleBuilder builder{ValuePlanFactory::instance().type_for(
                scalar_descriptor<FabricDiagnosticEvent>::value_meta())};
            builder.set("component", Value{event.component});
            builder.set("category", Value{event.category});
            builder.set("message", Value{event.message});
            builder.set("retriable", Value{event.retriable});
            builder.set("fatal", Value{event.fatal});
            builder.set("occurrences", Value{event.occurrences});
            return builder.build();
        }

        void record_diagnostic_event(const Out<TSD<Str, TS<FabricDiagnosticEvent>>> &events,
                                     Str component, Str category, Str message, Bool retriable,
                                     Bool fatal)
        {
            Str path = component + "." + category;
            if (!events.contains(path) && events.size() >= FABRIC_DIAGNOSTIC_EVENT_LIMIT - 1U)
            {
                path = "diagnostics.capacity";
                component = "diagnostics";
                category = "capacity";
                message = "additional Fabric diagnostic paths were conflated at the "
                          "configured limit";
                retriable = false;
                fatal = false;
            }
            Int occurrences{};
            if (events.contains(path))
            {
                const auto existing = events.at(path);
                if (existing.valid())
                {
                    occurrences = existing.value().as_bundle().at("occurrences").checked_as<Int>();
                }
            }
            if (occurrences < std::numeric_limits<Int>::max())
            {
                ++occurrences;
            }
            FabricDiagnosticEventInput event{
                .component = std::move(component),
                .category = std::move(category),
                .message = std::move(message),
                .retriable = retriable,
                .fatal = fatal,
                .occurrences = occurrences,
            };
            auto mutation = events.begin_mutation(events.evaluation_time());
            Value key{std::move(path)};
            Value item = diagnostic_event_value(event);
            mutation.set(key.view(), item.view());
        }

        template <typename ValueSchema>
        void emit_node_diagnostics(FabricNodeDiagnostics diagnostics,
                                   const Out<FabricServiceNodeResult<ValueSchema>> &result)
        {
            auto metrics = result.template field<"metrics">();
            auto metric_mutation = metrics.begin_mutation(metrics.evaluation_time());
            for (auto &[name, value] : diagnostics.metrics)
            {
                Value key{std::move(name)};
                Value item{std::move(value)};
                metric_mutation.set(key.view(), item.view());
            }

            auto events = result.template field<"events">();
            auto event_mutation = events.begin_mutation(events.evaluation_time());
            for (auto &[path, event] : diagnostics.events)
            {
                Value key{std::move(path)};
                Value item = diagnostic_event_value(event);
                event_mutation.set(key.view(), item.view());
            }
        }

        [[nodiscard]] Value ingress_value(const detail::DeliveredRoot &root)
        {
            BundleBuilder builder{ValuePlanFactory::instance().type_for(
                schema_descriptor<FabricIngressSignal>::ts_meta()->value_schema)};
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

        [[nodiscard]] std::vector<SubscriptionSpec> subscriptions(const TSSInputView &keys,
                                                                  SubscriptionMode mode)
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

        template <typename Requests>
        void collect_revisions(const Requests &requests, std::vector<DataRevisionInput> &revisions)
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
                revisions.push_back(data_revision_input(revision.base().value().concrete()));
            }
        }

        [[nodiscard]] std::optional<TransportControlInput> transport_control(
            const In<"controls", TSD<Int, FabricTransportControl>, InputValidity::Unchecked>
                &controls)
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
                    throw std::logic_error(
                        "fabric graph transport has conflicting lifecycle clients");
                }
                result = std::move(next);
            }
            return result;
        }

        struct FabricLifecycleNode
        {
            static constexpr auto name = "hgraph.fabric.service.lifecycle";
            static constexpr bool schedule_on_start = true;
            using signature_args = std::tuple<Scalar<"path", Str>, GlobalStateView, Out<TS<Str>>>;

            static void start(Scalar<"path", Str> path, GlobalStateView global_state,
                              LoggerView log)
            {
                static_cast<void>(service_config(global_state));
                log.info("hgraph.fabric service started path={}", path.value());
            }

            static void eval(Out<TS<Str>> lifecycle)
            {
                lifecycle.set("running");
            }

            static void stop(Scalar<"path", Str> path, LoggerView log)
            {
                log.info("hgraph.fabric service stopped path={}", path.value());
            }
        };

        struct FabricSnapshotNode
        {
            static constexpr auto name = "hgraph.fabric.service.snapshot";

            static void start(GlobalStateView global_state, State<detail::SnapshotNodeState> state)
            {
                state.modify().start(service_config(global_state));
            }

            static void eval(In<"keys", TSS<Str>, InputValidity::Unchecked> keys,
                             State<detail::SnapshotNodeState> state,
                             Out<FabricServiceNodeResult<TSD<Str, FabricIngressSignal>>> result)
            {
                auto out = result.template field<"value">();
                UnwindCleanupGuard diagnostic_change{
                    [&] { emit_node_diagnostics(state.ref().diagnostics(), result); }};
                const auto &erased = static_cast<const TSSInputView &>(keys);
                if (auto delivery =
                        state.modify().evaluate(subscriptions(erased, SubscriptionMode::Snapshot));
                    delivery.has_value())
                {
                    apply_delivery(std::move(*delivery), out);
                }
                diagnostic_change.complete();
            }

            static void stop(State<detail::SnapshotNodeState> state)
            {
                state.modify().stop();
            }
        };

        /** Planned root snapshots are independent of the keyed subscription
            transport, so the initial image is produced at the exact graph
            start. One evaluation performs one durable consistency resolve. */
        struct FabricPlannedSnapshotNode
        {
            static constexpr auto name = "hgraph.fabric.service.snapshot.planned";

            static void start(Scalar<"plan", detail::FabricWiringPlanHandle> plan,
                              GlobalStateView global_state, State<detail::SnapshotNodeState> state)
            {
                if (!plan.value().value)
                {
                    throw std::logic_error("fabric planned snapshot node requires a wiring plan");
                }
                state.modify().start(service_config(global_state), plan.value().value->snapshot);
            }

            static void eval(In<"lifecycle", TS<Str>>,
                             Scalar<"plan", detail::FabricWiringPlanHandle>,
                             State<detail::SnapshotNodeState> state,
                             Out<FabricServiceNodeResult<TSD<Str, FabricIngressSignal>>> result)
            {
                auto out = result.template field<"value">();
                UnwindCleanupGuard diagnostic_change{
                    [&] { emit_node_diagnostics(state.ref().diagnostics(), result); }};
                if (auto delivery = state.modify().evaluate_planned(); delivery.has_value())
                {
                    apply_delivery(std::move(*delivery), out);
                }
                diagnostic_change.complete();
            }

            static void stop(State<detail::SnapshotNodeState> state)
            {
                state.modify().stop();
            }
        };

        struct FabricReplayNode
        {
            static constexpr auto name = "hgraph.fabric.service.replay";

            static void start(GlobalStateView global_state, EngineControlView engine,
                              State<detail::ReplayNodeState> state)
            {
                state.modify().start(service_config(global_state), engine.start_time(),
                                     engine.end_time());
            }

            static void eval(In<"keys", TSS<Str>, InputValidity::Unchecked> keys, DateTime now,
                             NodeScheduler scheduler, State<detail::ReplayNodeState> state,
                             Out<FabricServiceNodeResult<TSD<Str, FabricIngressSignal>>> result)
            {
                auto out = result.template field<"value">();
                UnwindCleanupGuard diagnostic_change{
                    [&] { emit_node_diagnostics(state.ref().diagnostics(), result); }};
                const auto &erased = static_cast<const TSSInputView &>(keys);
                if (auto delivery = state.modify().evaluate(
                        subscriptions(erased, SubscriptionMode::Replay), now, scheduler);
                    delivery.has_value())
                {
                    apply_delivery(std::move(*delivery), out);
                }
                diagnostic_change.complete();
            }

            static void stop(State<detail::ReplayNodeState> state)
            {
                state.modify().stop();
            }
        };

        /** Planned root replay is an ordinary scheduled source. Per tick it
            resolves one equal-as-of batch and schedules the next durable
            history time; retained memory is the reachable revision history. */
        struct FabricPlannedReplayNode
        {
            static constexpr auto name = "hgraph.fabric.service.replay.planned";

            static void start(Scalar<"plan", detail::FabricWiringPlanHandle> plan,
                              GlobalStateView global_state, EngineControlView engine,
                              State<detail::ReplayNodeState> state)
            {
                if (!plan.value().value)
                {
                    throw std::logic_error("fabric planned replay node requires a wiring plan");
                }
                state.modify().start(service_config(global_state), engine.start_time(),
                                     engine.end_time(), plan.value().value->replay);
            }

            static void eval(In<"lifecycle", TS<Str>>,
                             Scalar<"plan", detail::FabricWiringPlanHandle>, DateTime now,
                             NodeScheduler scheduler, State<detail::ReplayNodeState> state,
                             Out<FabricServiceNodeResult<TSD<Str, FabricIngressSignal>>> result)
            {
                auto out = result.template field<"value">();
                UnwindCleanupGuard diagnostic_change{
                    [&] { emit_node_diagnostics(state.ref().diagnostics(), result); }};
                if (auto delivery = state.modify().evaluate_planned(now, scheduler);
                    delivery.has_value())
                {
                    apply_delivery(std::move(*delivery), out);
                }
                diagnostic_change.complete();
            }

            static void stop(State<detail::ReplayNodeState> state)
            {
                state.modify().stop();
            }
        };

        struct FabricLiveNode
        {
            static constexpr auto name = "hgraph.fabric.service.live";

            static void start(GlobalStateView global_state, State<detail::LiveNodeState> state)
            {
                state.modify().start(service_config(global_state));
            }

            static void eval(
                In<"keys", TSS<Str>, InputValidity::Unchecked> keys,
                In<"notices", TSD<Int, TS<Shared<DataRevision>>>, InputValidity::Unchecked> notices,
                In<"controls", TSD<Int, FabricTransportControl>, InputValidity::Unchecked> controls,
                DateTime now, Scalar<"notification_mode", FabricNotificationMode> notification_mode,
                State<detail::LiveNodeState> state,
                Out<FabricServiceNodeResult<TSD<Str, FabricIngressSignal>>> result)
            {
                auto out = result.template field<"value">();
                UnwindCleanupGuard diagnostic_change{
                    [&] { emit_node_diagnostics(state.ref().diagnostics(), result); }};
                const auto control = transport_control(controls);
                if (notification_mode.value() == FabricNotificationMode::GraphTransport)
                {
                    if (control.has_value() && control->failed)
                    {
                        throw std::runtime_error(control->message.empty()
                                                     ? "fabric transport failed"
                                                     : control->message);
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
                if (auto delivery = state.modify().evaluate(
                        subscriptions(erased, SubscriptionMode::Live), std::move(revisions), now,
                        control.has_value() && control->reconcile);
                    delivery.has_value())
                {
                    apply_delivery(std::move(*delivery), out);
                }
                diagnostic_change.complete();
            }

            static void stop(State<detail::LiveNodeState> state)
            {
                state.modify().stop();
            }
        };

        /** Planned root Live ingress performs one durable startup resolve and
            thereafter admits only complete revisions delivered by the notice
            edge. Work is O(conflated notices plus affected resolver search). */
        struct FabricPlannedLiveNode
        {
            static constexpr auto name = "hgraph.fabric.service.live.planned";

            static void start(Scalar<"plan", detail::FabricWiringPlanHandle> plan,
                              GlobalStateView global_state, State<detail::LiveNodeState> state)
            {
                if (!plan.value().value)
                {
                    throw std::logic_error("fabric planned live node requires a wiring plan");
                }
                state.modify().start(service_config(global_state), plan.value().value->live);
            }

            static void eval(
                In<"lifecycle", TS<Str>>,
                In<"notices", TSD<Int, TS<Shared<DataRevision>>>, InputValidity::Unchecked> notices,
                In<"controls", TSD<Int, FabricTransportControl>, InputValidity::Unchecked> controls,
                Scalar<"plan", detail::FabricWiringPlanHandle>, DateTime now,
                Scalar<"notification_mode", FabricNotificationMode> notification_mode,
                State<detail::LiveNodeState> state,
                Out<FabricServiceNodeResult<TSD<Str, FabricIngressSignal>>> result)
            {
                auto out = result.template field<"value">();
                UnwindCleanupGuard diagnostic_change{
                    [&] { emit_node_diagnostics(state.ref().diagnostics(), result); }};
                const auto control = transport_control(controls);
                if (notification_mode.value() == FabricNotificationMode::GraphTransport)
                {
                    if (control.has_value() && control->failed)
                    {
                        throw std::runtime_error(control->message.empty()
                                                     ? "fabric transport failed"
                                                     : control->message);
                    }
                    if (!control.has_value() || !control->ready)
                    {
                        diagnostic_change.complete();
                        return;
                    }
                }
                std::vector<DataRevisionInput> revisions;
                collect_revisions(notices, revisions);
                if (auto delivery = state.modify().evaluate_planned(
                        std::move(revisions), now, control.has_value() && control->reconcile);
                    delivery.has_value())
                {
                    apply_delivery(std::move(*delivery), out);
                }
                diagnostic_change.complete();
            }

            static void stop(State<detail::LiveNodeState> state)
            {
                state.modify().stop();
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

        void enqueue_publications(const In<"requests", TSD<Int, FabricPublicationRequest>,
                                           InputValidity::Unchecked> &requests,
                                  detail::PublicationNodeState &state)
        {
            if (!requests.modified())
            {
                return;
            }
            for (const auto &[request_id, request] : requests.modified_items())
            {
                static_cast<void>(request_id);
                if (request.valid())
                {
                    state.enqueue(publication_request(request));
                }
            }
        }

        struct FabricConfiguredPublicationNode
        {
            static constexpr auto name = "hgraph.fabric.service.publication.configured";

            static void start(GlobalStateView global_state,
                              State<detail::PublicationNodeState> state)
            {
                state.modify().start(service_config(global_state), false);
            }

            static void
            eval(In<"requests", TSD<Int, FabricPublicationRequest>, InputValidity::Unchecked>
                     requests,
                 NodeScheduler scheduler, State<detail::PublicationNodeState> state,
                 Out<FabricServiceNodeResult<TSD<Str, TS<Shared<DataRevision>>>>> result)
            {
                UnwindCleanupGuard diagnostic_change{
                    [&] { emit_node_diagnostics(state.ref().diagnostics(), result); }};
                auto &publication = state.modify();
                enqueue_publications(requests, publication);
                static_cast<void>(publication.advance());
                if (publication.work_pending())
                {
                    scheduler.schedule(MIN_TD);
                }
                diagnostic_change.complete();
            }

            static void stop(State<detail::PublicationNodeState> state)
            {
                state.modify().stop();
            }
        };

        struct FabricGraphPublicationNode
        {
            static constexpr auto name = "hgraph.fabric.service.publication.graph_transport";

            static void start(GlobalStateView global_state,
                              State<detail::PublicationNodeState> state)
            {
                state.modify().start(service_config(global_state), true);
            }

            static void
            eval(In<"requests", TSD<Int, FabricPublicationRequest>, InputValidity::Unchecked>
                     requests,
                 In<"completions", TSD<Str, FabricNotificationCompletion>, InputValidity::Unchecked>
                     completions,
                 NodeScheduler scheduler, State<detail::PublicationNodeState> state,
                 Out<FabricServiceNodeResult<TSD<Str, TS<Shared<DataRevision>>>>> result)
            {
                auto candidates = result.template field<"value">();
                UnwindCleanupGuard diagnostic_change{
                    [&] { emit_node_diagnostics(state.ref().diagnostics(), result); }};
                auto &publication = state.modify();
                enqueue_publications(requests, publication);

                bool completed = false;
                if (completions.modified())
                {
                    for (const auto &[key, completion] : completions.modified_items())
                    {
                        if (!completion.valid())
                        {
                            continue;
                        }
                        const auto data_id = completion.template field<"data_id">();
                        const auto revision = completion.template field<"revision">();
                        const auto delivered = completion.template field<"delivered">();
                        const auto message = completion.template field<"message">();
                        if (!data_id.valid() || !revision.valid() || !delivered.valid())
                        {
                            throw std::invalid_argument(
                                "fabric notification completion is incomplete");
                        }
                        if (key.checked_as<Str>() != data_id.value())
                        {
                            throw std::invalid_argument(
                                "fabric notification completion key does not match its data id");
                        }
                        if (!candidates.contains(data_id.value()))
                        {
                            continue;
                        }
                        publication.complete(NotificationDeliveryInput{
                            .data_id = data_id.value(),
                            .revision = revision.value(),
                            .delivered = delivered.value(),
                            .message = message.valid() ? message.value() : Str{},
                        });
                        static_cast<void>(candidates.erase(data_id.value()));
                        completed = true;
                    }
                }
                if (completed)
                {
                    diagnostic_change.complete();
                    return;
                }

                for (auto revision : publication.advance())
                {
                    if (candidates.contains(revision.data_id))
                    {
                        continue;
                    }
                    if (candidates.size() >= FABRIC_NOTIFICATION_REQUEST_LIMIT)
                    {
                        throw std::overflow_error(
                            "fabric graph notification candidate set is full");
                    }
                    const Str data_id = revision.data_id;
                    Value value = make_data_revision(std::move(revision));
                    candidates.apply(data_id, value.view());
                }
                if (publication.work_pending())
                {
                    scheduler.schedule(MIN_TD);
                }
                diagnostic_change.complete();
            }

            static void stop(State<detail::PublicationNodeState> state)
            {
                state.modify().stop();
            }
        };

        void increment_counter(const Out<TS<Int>> &counter)
        {
            const Int value = counter.valid() ? counter.value().checked_as<Int>() : Int{};
            counter.set(value == std::numeric_limits<Int>::max() ? value : value + 1);
        }

        [[nodiscard]] NotificationDeliveryInput notification_delivery(const TSBInputView &delivery)
        {
            const auto data_id = delivery.field("data_id");
            const auto revision = delivery.field("revision");
            const auto delivered = delivery.field("delivered");
            const auto retriable = delivery.field("retriable");
            const auto message = delivery.field("message");
            if (!data_id.valid() || !revision.valid() || !delivered.valid() || !retriable.valid())
            {
                throw std::invalid_argument("fabric notification delivery is incomplete");
            }
            NotificationDeliveryInput result{
                .data_id = data_id.value().checked_as<Str>(),
                .revision = revision.value().checked_as<Int>(),
                .delivered = delivered.value().checked_as<Bool>(),
                .retriable = retriable.value().checked_as<Bool>(),
                .message = message.valid() ? message.value().checked_as<Str>() : Str{},
            };
            require_data_id(result.data_id);
            if (result.revision <= 0)
            {
                throw std::invalid_argument(
                    "fabric notification delivery requires a positive revision");
            }
            return result;
        }

        void
        emit_notification_completion(const NotificationDeliveryInput &delivery,
                                     const Out<TSD<Str, FabricNotificationCompletion>> &completions)
        {
            auto completion = completions.at(delivery.data_id);
            completion.template field<"data_id">().set(delivery.data_id);
            completion.template field<"revision">().set(delivery.revision);
            completion.template field<"delivered">().set(delivery.delivered);
            completion.template field<"message">().set(delivery.message);
        }

        /** Graph-owned notification correlation. Durable candidates remain visible on
            the input TSD until completion feedback removes them. The active fields are
            ordinary output time series, and delivery reports are consumed in service
            request-id order. Retrying re-ticks the retained Shared revision without
            materialising its concrete value. */
        struct FabricNotificationFlowNode
        {
            static constexpr auto name = "hgraph.fabric.service.notification_flow";
            static constexpr bool schedule_on_start = true;

            static void
            eval(In<"candidates", TSD<Str, TS<Shared<DataRevision>>>, InputValidity::Unchecked>
                     candidates,
                 In<"deliveries", TSD<Int, FabricNotificationDelivery>, InputValidity::Unchecked>
                     deliveries,
                 NodeScheduler scheduler, Out<FabricNotificationFlowResult> result)
            {
                auto request = result.template field<"request">();
                auto active_data_id = result.template field<"active_data_id">();
                auto active_revision = result.template field<"active_revision">();
                auto active = result.template field<"active">();
                auto retry_count = result.template field<"retry_count">();
                auto retry_pending = result.template field<"retry_pending">();
                auto completions = result.template field<"completions">();
                auto delivered_count = result.template field<"delivered">();
                auto retried_count = result.template field<"retried">();
                auto failed_count = result.template field<"failed">();
                auto stale_count = result.template field<"stale_reports">();

                if (!delivered_count.valid())
                {
                    active.set(false);
                    retry_count.set(Int{});
                    retry_pending.set(false);
                    delivered_count.set(Int{});
                    retried_count.set(Int{});
                    failed_count.set(Int{});
                    stale_count.set(Int{});
                }
                if (!completions.empty())
                {
                    completions.clear();
                }

                bool is_active = active.valid() && active.value().checked_as<Bool>();
                if (retry_pending.valid() && retry_pending.value().checked_as<Bool>())
                {
                    if (!is_active || !active_data_id.valid())
                    {
                        throw std::logic_error("fabric notification retry has no active request");
                    }
                    const Str data_id = active_data_id.value().checked_as<Str>();
                    if (!candidates.contains(data_id) || !candidates.at(data_id).valid())
                    {
                        throw std::logic_error(
                            "fabric notification retry lost its durable candidate");
                    }
                    request.set(candidates.at(data_id).base().reference());
                    retry_pending.set(false);
                    increment_counter(retried_count);
                    return;
                }
                std::vector<Str> completed_data_ids;
                if (deliveries.modified())
                {
                    for (const auto &[request_id, delivery_view] : deliveries.modified_items())
                    {
                        static_cast<void>(request_id);
                        if (!delivery_view.valid())
                        {
                            continue;
                        }
                        NotificationDeliveryInput delivery = notification_delivery(delivery_view);
                        if (!is_active || !active_data_id.valid() || !active_revision.valid() ||
                            active_data_id.value().checked_as<Str>() != delivery.data_id ||
                            active_revision.value().checked_as<Int>() != delivery.revision)
                        {
                            increment_counter(stale_count);
                            continue;
                        }
                        if (delivery.delivered)
                        {
                            active.set(false);
                            is_active = false;
                            completed_data_ids.push_back(delivery.data_id);
                            increment_counter(delivered_count);
                            emit_notification_completion(delivery, completions);
                            continue;
                        }

                        const Int retries =
                            retry_count.valid() ? retry_count.value().checked_as<Int>() : Int{};
                        if (delivery.retriable &&
                            retries < static_cast<Int>(FABRIC_NOTIFICATION_RETRY_LIMIT))
                        {
                            retry_count.set(retries + 1);
                            request.set(TimeSeriesReference::empty(
                                schema_descriptor<TS<Shared<DataRevision>>>::ts_meta()));
                            retry_pending.set(true);
                            scheduler.schedule(MIN_TD);
                            continue;
                        }

                        delivery.retriable = false;
                        if (delivery.message.empty())
                        {
                            delivery.message = "fabric notification delivery failed";
                        }
                        active.set(false);
                        is_active = false;
                        completed_data_ids.push_back(delivery.data_id);
                        increment_counter(failed_count);
                        emit_notification_completion(delivery, completions);
                    }
                }

                if (candidates.modified())
                {
                    for (const auto &[key, candidate] : candidates.modified_items())
                    {
                        if (!candidate.valid())
                        {
                            continue;
                        }
                        const Str data_id = key.checked_as<Str>();
                        const DataRevisionInput revision =
                            data_revision_input(candidate.base().value().concrete());
                        if (revision.data_id != data_id)
                        {
                            throw std::invalid_argument("fabric notification request key does "
                                                        "not match its revision payload");
                        }
                    }
                }

                if (is_active)
                {
                    return;
                }
                for (const auto &[key, candidate] : candidates.valid_items())
                {
                    if (!candidate.valid())
                    {
                        continue;
                    }
                    const Str data_id = key.checked_as<Str>();
                    if (std::ranges::find(completed_data_ids, data_id) != completed_data_ids.end())
                    {
                        continue;
                    }
                    const DataRevisionInput revision =
                        data_revision_input(candidate.base().value().concrete());
                    request.set(candidate.base().reference());
                    active_data_id.set(data_id);
                    active_revision.set(revision.revision);
                    active.set(true);
                    retry_count.set(Int{});
                    retry_pending.set(false);
                    break;
                }
            }
        };

        struct FabricNotificationMetricsNode
        {
            static constexpr auto name = "hgraph.fabric.service.notification_metrics";

            static void eval(
                In<"pending", TSD<Str, TS<Shared<DataRevision>>>, InputValidity::Unchecked> pending,
                In<"delivered", TS<Int>, InputValidity::Unchecked> delivered,
                In<"retried", TS<Int>, InputValidity::Unchecked> retried,
                In<"failed", TS<Int>, InputValidity::Unchecked> failed,
                In<"stale_reports", TS<Int>, InputValidity::Unchecked> stale_reports,
                Out<TSD<Str, TS<Str>>> metrics)
            {
                metrics.set("transport.notification.pending", std::to_string(pending.size()));
                metrics.set("transport.notification.delivered",
                            std::to_string(delivered.valid() ? delivered.value() : 0));
                metrics.set("transport.notification.retried",
                            std::to_string(retried.valid() ? retried.value() : 0));
                metrics.set("transport.notification.failed",
                            std::to_string(failed.valid() ? failed.value() : 0));
                metrics.set("transport.notification.stale_reports",
                            std::to_string(stale_reports.valid() ? stale_reports.value() : 0));
            }
        };

        [[nodiscard]] Value load_response_value(Str data_id, DataVersion version, Frame frame)
        {
            BundleBuilder builder{ValuePlanFactory::instance().type_for(
                schema_descriptor<FabricLoadResponse>::ts_meta()->value_schema)};
            builder.set("data_id", Value{std::move(data_id)});
            builder.set("version", Value{version});
            builder.set("frame", Value{std::move(frame)});
            return builder.build();
        }

        struct FabricLoadNode
        {
            static constexpr auto name = "hgraph.fabric.service.load";

            static void
            eval(In<"requests", TSD<Int, FabricLoadRequest>, InputValidity::Unchecked> requests,
                 GlobalStateView global_state,
                 Out<FabricServiceNodeResult<TSD<Int, FabricLoadResponse>>> result)
            {
                auto responses = result.template field<"value">();
                if (!requests.modified())
                {
                    return;
                }
                const FabricConfig config = service_config(global_state);
                auto diagnostic_events = result.template field<"events">();
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
                    Str data_id = requested_data_id.value();
                    const DataVersion version = requested_version.value();
                    require_data_id(data_id);
                    if (version <= 0)
                    {
                        throw std::invalid_argument("fabric load version must be positive");
                    }
                    Frame frame;
                    try
                    {
                        frame =
                            config.frames.read(data_version_key(config.prefix, data_id, version));
                    }
                    catch (const std::exception &error)
                    {
                        record_diagnostic_event(diagnostic_events, "store", "frame.read",
                                                error.what(), false, true);
                        throw;
                    }
                    if (!frame.has_value())
                    {
                        record_diagnostic_event(diagnostic_events, "store", "frame.missing",
                                                "requested Fabric Frame is not present: " +
                                                    data_id + ":" + std::to_string(version),
                                                false, false);
                        continue;
                    }
                    Value response =
                        load_response_value(std::move(data_id), version, std::move(frame));
                    mutation.set(request_id, response.view());
                }
            }
        };

        struct FabricTransportEventsNode
        {
            static constexpr auto name = "hgraph.fabric.service.transport_events";

            static void
            eval(In<"events", TSD<Int, FabricTransportEvent>, InputValidity::Unchecked> events,
                 Out<TSD<Str, TS<FabricDiagnosticEvent>>> diagnostics)
            {
                if (!events.modified())
                {
                    return;
                }
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
                    if (!component.valid() || !category.valid() || !retriable.valid() ||
                        !fatal.valid())
                    {
                        throw std::invalid_argument("fabric transport event is incomplete");
                    }
                    record_diagnostic_event(diagnostics, component.value(), category.value(),
                                            message.valid() ? message.value() : Str{},
                                            retriable.value(), fatal.value());
                }
            }
        };

        struct FabricDiagnosticsNode
        {
            static constexpr auto name = "hgraph.fabric.service.diagnostics";

            /** Aggregate node-owned diagnostic edges. Additive resolver and live
                counters are reduced here; no mutable service facade is consulted. */
            static void eval(
                In<"lifecycle", TS<Str>, InputValidity::Unchecked> lifecycle,
                In<"publication_metrics", TSD<Str, TS<Str>>, InputValidity::Unchecked>
                    publication_metrics,
                In<"snapshot_metrics", TSD<Str, TS<Str>>, InputValidity::Unchecked>
                    snapshot_metrics,
                In<"planned_snapshot_metrics", TSD<Str, TS<Str>>, InputValidity::Unchecked>
                    planned_snapshot_metrics,
                In<"replay_metrics", TSD<Str, TS<Str>>, InputValidity::Unchecked> replay_metrics,
                In<"planned_replay_metrics", TSD<Str, TS<Str>>, InputValidity::Unchecked>
                    planned_replay_metrics,
                In<"live_metrics", TSD<Str, TS<Str>>, InputValidity::Unchecked> live_metrics,
                In<"planned_live_metrics", TSD<Str, TS<Str>>, InputValidity::Unchecked>
                    planned_live_metrics,
                In<"notification_metrics", TSD<Str, TS<Str>>, InputValidity::Unchecked>
                    notification_metrics,
                In<"publication_events", TSD<Str, TS<FabricDiagnosticEvent>>,
                   InputValidity::Unchecked>
                    publication_events,
                In<"snapshot_events", TSD<Str, TS<FabricDiagnosticEvent>>, InputValidity::Unchecked>
                    snapshot_events,
                In<"planned_snapshot_events", TSD<Str, TS<FabricDiagnosticEvent>>,
                   InputValidity::Unchecked>
                    planned_snapshot_events,
                In<"replay_events", TSD<Str, TS<FabricDiagnosticEvent>>, InputValidity::Unchecked>
                    replay_events,
                In<"planned_replay_events", TSD<Str, TS<FabricDiagnosticEvent>>,
                   InputValidity::Unchecked>
                    planned_replay_events,
                In<"live_events", TSD<Str, TS<FabricDiagnosticEvent>>, InputValidity::Unchecked>
                    live_events,
                In<"planned_live_events", TSD<Str, TS<FabricDiagnosticEvent>>,
                   InputValidity::Unchecked>
                    planned_live_events,
                In<"load_events", TSD<Str, TS<FabricDiagnosticEvent>>, InputValidity::Unchecked>
                    load_events,
                In<"transport_events", TSD<Str, TS<FabricDiagnosticEvent>>,
                   InputValidity::Unchecked>
                    transport_events,
                Out<FabricDiagnostics> diagnostics)
            {
                auto metrics = diagnostics.template field<"metrics">();
                std::map<Str, std::uint64_t> sums;
                std::map<Str, Str> direct;
                std::uint64_t maximum_backtracking_depth{};
                std::uint64_t backtracking_depth_sum{};
                const auto collect_metric_input = [&](const TSDInputView &input)
                {
                    for (const auto &[metric_name, value] : input.valid_items())
                    {
                        if (!value.valid())
                        {
                            continue;
                        }
                        const Str name = metric_name.checked_as<Str>();
                        const Str text = value.value().checked_as<Str>();
                        if (name == "resolution.backtracking_depth.average")
                        {
                            continue;
                        }
                        if (name == "__resolution.backtracking_depth.sum")
                        {
                            backtracking_depth_sum += std::stoull(text);
                            continue;
                        }
                        if (name == "resolution.backtracking_depth.maximum")
                        {
                            maximum_backtracking_depth =
                                std::max(maximum_backtracking_depth, std::stoull(text));
                            continue;
                        }
                        if (name.starts_with("resolution.") || name == "live.notices")
                        {
                            sums[name] += std::stoull(text);
                            continue;
                        }
                        direct.insert_or_assign(name, text);
                    }
                };
                collect_metric_input(static_cast<const TSDInputView &>(publication_metrics));
                collect_metric_input(static_cast<const TSDInputView &>(snapshot_metrics));
                collect_metric_input(static_cast<const TSDInputView &>(planned_snapshot_metrics));
                collect_metric_input(static_cast<const TSDInputView &>(replay_metrics));
                collect_metric_input(static_cast<const TSDInputView &>(planned_replay_metrics));
                collect_metric_input(static_cast<const TSDInputView &>(live_metrics));
                collect_metric_input(static_cast<const TSDInputView &>(planned_live_metrics));
                collect_metric_input(static_cast<const TSDInputView &>(notification_metrics));
                direct.insert_or_assign("lifecycle",
                                        lifecycle.valid() ? lifecycle.value() : Str{"starting"});
                direct.try_emplace("publishers", "0");
                direct.try_emplace("publication.queued", "0");
                direct.insert_or_assign("publication.queue_limit_per_data_id",
                                        std::to_string(FABRIC_PUBLICATION_QUEUE_LIMIT_PER_DATA_ID));
                direct.insert_or_assign("live.notice_limit_per_session",
                                        std::to_string(FABRIC_LIVE_NOTICE_LIMIT_PER_SESSION));
                sums.try_emplace("live.notices", 0U);
                sums.try_emplace("resolution.candidate_selections", 0U);
                const auto selections = sums.at("resolution.candidate_selections");
                direct.insert_or_assign(
                    "resolution.backtracking_depth.average",
                    std::to_string(selections == 0U ? 0.0
                                                    : static_cast<double>(backtracking_depth_sum) /
                                                          static_cast<double>(selections)));
                direct.insert_or_assign("resolution.backtracking_depth.maximum",
                                        std::to_string(maximum_backtracking_depth));

                auto mutation = metrics.begin_mutation(metrics.evaluation_time());
                for (auto &[metric_name, value] : direct)
                {
                    Value key{std::move(metric_name)};
                    Value item{std::move(value)};
                    mutation.set(key.view(), item.view());
                }
                for (auto &[metric_name, value] : sums)
                {
                    Value key{std::move(metric_name)};
                    Value item{std::to_string(value)};
                    mutation.set(key.view(), item.view());
                }

                auto diagnostic_events = diagnostics.template field<"events">();
                std::map<Str, FabricDiagnosticEventInput> combined_events;
                const auto collect_event_input = [&](const TSDInputView &input)
                {
                    for (const auto &[path_value, event] : input.valid_items())
                    {
                        if (!event.valid())
                        {
                            continue;
                        }
                        Str path = path_value.checked_as<Str>();
                        const auto fields = event.value().as_bundle();
                        FabricDiagnosticEventInput next{
                            .component = fields.at("component").checked_as<Str>(),
                            .category = fields.at("category").checked_as<Str>(),
                            .message = fields.at("message").checked_as<Str>(),
                            .retriable = fields.at("retriable").checked_as<Bool>(),
                            .fatal = fields.at("fatal").checked_as<Bool>(),
                            .occurrences = fields.at("occurrences").checked_as<Int>(),
                        };
                        auto found = combined_events.find(path);
                        if (found == combined_events.end() &&
                            combined_events.size() >= FABRIC_DIAGNOSTIC_EVENT_LIMIT)
                        {
                            path = "diagnostics.capacity";
                            next = FabricDiagnosticEventInput{
                                .component = "diagnostics",
                                .category = "capacity",
                                .message = "additional Fabric diagnostic paths were conflated at "
                                           "the configured limit",
                                .occurrences = next.occurrences,
                            };
                            found = combined_events.find(path);
                            if (found == combined_events.end())
                            {
                                auto displaced = std::prev(combined_events.end());
                                const Int room = std::numeric_limits<Int>::max() - next.occurrences;
                                next.occurrences += std::min(room, displaced->second.occurrences);
                                combined_events.erase(displaced);
                            }
                        }
                        if (found == combined_events.end())
                        {
                            combined_events.emplace(std::move(path), std::move(next));
                            continue;
                        }
                        found->second.message = std::move(next.message);
                        found->second.retriable = next.retriable;
                        found->second.fatal = next.fatal;
                        const Int room =
                            std::numeric_limits<Int>::max() - found->second.occurrences;
                        found->second.occurrences += std::min(room, next.occurrences);
                    }
                };
                collect_event_input(static_cast<const TSDInputView &>(publication_events));
                collect_event_input(static_cast<const TSDInputView &>(snapshot_events));
                collect_event_input(static_cast<const TSDInputView &>(planned_snapshot_events));
                collect_event_input(static_cast<const TSDInputView &>(replay_events));
                collect_event_input(static_cast<const TSDInputView &>(planned_replay_events));
                collect_event_input(static_cast<const TSDInputView &>(live_events));
                collect_event_input(static_cast<const TSDInputView &>(planned_live_events));
                collect_event_input(static_cast<const TSDInputView &>(load_events));
                collect_event_input(static_cast<const TSDInputView &>(transport_events));

                auto event_mutation =
                    diagnostic_events.begin_mutation(diagnostic_events.evaluation_time());
                for (auto &[path, event] : combined_events)
                {
                    Value key{std::move(path)};
                    Value item = diagnostic_event_value(event);
                    event_mutation.set(key.view(), item.view());
                }
            }
        };

        struct FabricServiceImpl
        {
            static constexpr auto name = "hgraph.fabric.service_impl";

            static void
            compose(Wiring &wiring, Scalar<"plan", detail::FabricWiringPlanHandle> plan,
                    Scalar<"notification_mode", FabricNotificationMode> notification_mode,
                    Scalar<"path", Str> path)
            {
                const auto binding = service::path(path.value());
                auto live_keys =
                    service::impl_input<FabricLiveSubscriptionService>(wiring, binding);
                auto replay_keys =
                    service::impl_input<FabricReplaySubscriptionService>(wiring, binding);
                auto snapshot_keys =
                    service::impl_input<FabricSnapshotSubscriptionService>(wiring, binding);
                auto publications = service::impl_input<FabricPublicationService>(wiring, binding);
                auto notices = service::impl_input<FabricNoticeService>(wiring, binding);
                auto deliveries =
                    service::impl_input<FabricNotificationDeliveryService>(wiring, binding);
                auto controls = service::impl_input<FabricTransportControlService>(wiring, binding);
                auto events = service::impl_input<FabricTransportEventService>(wiring, binding);
                auto loads = service::impl_input<FabricLoadService>(wiring, binding);

                if (!plan.value().value)
                {
                    throw std::logic_error("fabric service implementation requires a wiring plan");
                }
                auto lifecycle = wire<FabricLifecycleNode>(wiring, path.value());
                Port<FabricServiceNodeResult<TSD<Str, TS<Shared<DataRevision>>>>>
                    publication_result;
                Port<TS<Shared<DataRevision>>> notification_requests;
                Port<TSD<Str, TS<Str>>> notification_metrics;
                if (notification_mode.value() == FabricNotificationMode::GraphTransport)
                {
                    auto completion_feedback =
                        stdlib::feedback<TSD<Str, FabricNotificationCompletion>>(wiring);
                    publication_result = wire<FabricGraphPublicationNode>(wiring, publications,
                                                                          completion_feedback());
                    auto candidates = service_result_value(wiring, publication_result);
                    auto notification_flow =
                        wire<FabricNotificationFlowNode>(wiring, candidates, deliveries);
                    auto notification_request_ref =
                        wire<stdlib::getattr_>(wiring, notification_flow, Str{"request"})
                            .template as<REF<TS<Shared<DataRevision>>>>();
                    // REF transparency exposes the retained candidate endpoint without
                    // materialising another Shared<DataRevision> value.
                    notification_requests =
                        notification_request_ref.template as<TS<Shared<DataRevision>>>();
                    auto completions =
                        wire<stdlib::getattr_>(wiring, notification_flow, Str{"completions"})
                            .template as<TSD<Str, FabricNotificationCompletion>>();
                    completion_feedback(completions);
                    auto delivered =
                        wire<stdlib::getattr_>(wiring, notification_flow, Str{"delivered"})
                            .template as<TS<Int>>();
                    auto retried = wire<stdlib::getattr_>(wiring, notification_flow, Str{"retried"})
                                       .template as<TS<Int>>();
                    auto failed = wire<stdlib::getattr_>(wiring, notification_flow, Str{"failed"})
                                      .template as<TS<Int>>();
                    auto stale_reports =
                        wire<stdlib::getattr_>(wiring, notification_flow, Str{"stale_reports"})
                            .template as<TS<Int>>();
                    notification_metrics = wire<FabricNotificationMetricsNode>(
                        wiring, candidates, delivered, retried, failed, stale_reports);
                }
                else
                {
                    publication_result =
                        wire<FabricConfiguredPublicationNode>(wiring, publications);
                    notification_requests = wire<stdlib::nothing, TS<Shared<DataRevision>>>(wiring);
                    notification_metrics = wire<stdlib::const_, TSD<Str, TS<Str>>>(
                        wiring, stdlib::make_map<Str, Str>({}));
                }
                auto snapshot_result = wire<FabricSnapshotNode>(wiring, snapshot_keys);
                auto replay_result = wire<FabricReplayNode>(wiring, replay_keys);
                auto live_result = wire<FabricLiveNode>(wiring, live_keys, notices, controls,
                                                        notification_mode.value());
                auto planned_snapshot_result =
                    wire<FabricPlannedSnapshotNode>(wiring, lifecycle, plan.value());
                auto planned_replay_result =
                    wire<FabricPlannedReplayNode>(wiring, lifecycle, plan.value());
                auto planned_live_result = wire<FabricPlannedLiveNode>(
                    wiring, lifecycle, notices, controls, plan.value(), notification_mode.value());
                auto load_result = wire<FabricLoadNode>(wiring, loads);
                auto transport_diagnostic_events = wire<FabricTransportEventsNode>(wiring, events);

                auto snapshot = service_result_value(wiring, snapshot_result);
                auto replay = service_result_value(wiring, replay_result);
                auto live = service_result_value(wiring, live_result);
                auto planned_snapshot = service_result_value(wiring, planned_snapshot_result);
                auto planned_replay = service_result_value(wiring, planned_replay_result);
                auto planned_live = service_result_value(wiring, planned_live_result);
                auto loaded = service_result_value(wiring, load_result);
                auto diagnostic_values = wire<FabricDiagnosticsNode>(
                    wiring, lifecycle, service_result_metrics(wiring, publication_result),
                    service_result_metrics(wiring, snapshot_result),
                    service_result_metrics(wiring, planned_snapshot_result),
                    service_result_metrics(wiring, replay_result),
                    service_result_metrics(wiring, planned_replay_result),
                    service_result_metrics(wiring, live_result),
                    service_result_metrics(wiring, planned_live_result), notification_metrics,
                    service_result_events(wiring, publication_result),
                    service_result_events(wiring, snapshot_result),
                    service_result_events(wiring, planned_snapshot_result),
                    service_result_events(wiring, replay_result),
                    service_result_events(wiring, planned_replay_result),
                    service_result_events(wiring, live_result),
                    service_result_events(wiring, planned_live_result),
                    service_result_events(wiring, load_result), transport_diagnostic_events);

                service::impl_output<FabricLiveSubscriptionService>(
                    wiring, binding, live.template as<TSD<Str, FabricIngressSignal>>());
                service::impl_output<FabricReplaySubscriptionService>(
                    wiring, binding, replay.template as<TSD<Str, FabricIngressSignal>>());
                service::impl_output<FabricSnapshotSubscriptionService>(
                    wiring, binding, snapshot.template as<TSD<Str, FabricIngressSignal>>());
                service::impl_output<detail::FabricPlannedLiveService>(
                    wiring, binding, planned_live.template as<TSD<Str, FabricIngressSignal>>());
                service::impl_output<detail::FabricPlannedReplayService>(
                    wiring, binding, planned_replay.template as<TSD<Str, FabricIngressSignal>>());
                service::impl_output<detail::FabricPlannedSnapshotService>(
                    wiring, binding, planned_snapshot.template as<TSD<Str, FabricIngressSignal>>());
                service::impl_output<FabricLoadService>(
                    wiring, binding, loaded.template as<TSD<Int, FabricLoadResponse>>());
                service::impl_output<FabricDiagnosticsService>(
                    wiring, binding, diagnostic_values.template as<FabricDiagnostics>());
                service::impl_output<FabricNotificationRequestService>(wiring, binding,
                                                                       notification_requests);
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
        service::register_services<
            FabricServiceImpl, FabricLiveSubscriptionService, FabricReplaySubscriptionService,
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

    void submit_notice(Wiring &wiring, Port<TS<Shared<DataRevision>>> notice,
                       service::ServicePath path)
    {
        wire<FabricNoticeService>(wiring, std::move(path), notice);
    }

    void submit_notice(Wiring &wiring, Port<TS<Shared<DataRevision>>> notice)
    {
        submit_notice(wiring, std::move(notice), service::path(DEFAULT_SERVICE_PATH));
    }

    Port<TS<Shared<DataRevision>>> notification_requests(Wiring &wiring, service::ServicePath path)
    {
        return wire<FabricNotificationRequestService>(wiring, std::move(path));
    }

    Port<TS<Shared<DataRevision>>> notification_requests(Wiring &wiring)
    {
        return notification_requests(wiring, service::path(DEFAULT_SERVICE_PATH));
    }

    void submit_notification_delivery(Wiring &wiring, Port<FabricNotificationDelivery> delivery,
                                      service::ServicePath path)
    {
        wire<FabricNotificationDeliveryService>(wiring, std::move(path), delivery);
    }

    void submit_transport_control(Wiring &wiring, Port<FabricTransportControl> control,
                                  service::ServicePath path)
    {
        wire<FabricTransportControlService>(wiring, std::move(path), control);
    }

    void submit_transport_event(Wiring &wiring, Port<FabricTransportEvent> event,
                                service::ServicePath path)
    {
        wire<FabricTransportEventService>(wiring, std::move(path), event);
    }

    Port<FabricLoadResponse> request_load(Wiring &wiring, Str data_id, DataVersion version,
                                          service::ServicePath path)
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
        return request_load(wiring, std::move(data_id), version,
                            service::path(DEFAULT_SERVICE_PATH));
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
