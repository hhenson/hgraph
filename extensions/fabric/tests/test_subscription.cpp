#include <hgraph/fabric/fabric.h>

#include "../src/impl/service_state.h"

#include <hgraph/lib/std/operators/control.h>
#include <hgraph/lib/std/operators/conversion.h>
#include <hgraph/lib/std/operators/registration.h>
#include <hgraph/lib/testing/runtime_support.h>
#include <hgraph/runtime/logger.h>
#include <hgraph/runtime/runtime.h>
#include <hgraph/types/static_node.h>

#include <arrow/array.h>
#include <arrow/builder.h>
#include <arrow/table.h>

#include <catch2/catch_test_macros.hpp>
#include <spdlog/sinks/ringbuffer_sink.h>

#include <cstdint>
#include <map>
#include <memory>
#include <optional>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <tuple>
#include <utility>
#include <vector>

namespace
{
    namespace hg = hgraph;
    namespace hgf = hgraph::fabric;
    namespace hgps = hgraph::persistence::store;

    constexpr hg::DateTime BASE_TIME{hg::TimeDelta{1'800'000'000'000'000}};

    std::vector<std::pair<hg::DateTime, std::int64_t>> observed_frames{};
    std::vector<std::tuple<hg::DateTime, hg::Str, std::int64_t>> observed_tagged_frames{};
    std::map<hg::Str, hgf::FabricDiagnosticEventInput> observed_diagnostic_events{};
    std::vector<const void *> observed_notification_allocations{};
    std::map<hg::Str, hg::Str> observed_notification_metrics{};

    struct CapturedLog
    {
        std::shared_ptr<spdlog::sinks::ringbuffer_sink_mt> sink{
            std::make_shared<spdlog::sinks::ringbuffer_sink_mt>(16)};

        CapturedLog()
        {
            hg::log::set_logger(std::make_shared<spdlog::logger>("hgraph-fabric-test", sink));
        }

        ~CapturedLog()
        {
            hg::log::set_logger(nullptr);
        }

        [[nodiscard]] std::string joined() const
        {
            std::string result;
            for (const auto &line : sink->last_formatted())
            {
                result += line;
            }
            return result;
        }
    };

    struct IoCounters
    {
        std::size_t object_puts{};
        std::size_t object_gets{};
        std::size_t object_lists{};
        std::size_t object_compares{};
        std::size_t frame_writes{};
        std::size_t frame_reads{};
        std::size_t frame_contains{};

        void reset() noexcept
        {
            *this = {};
        }
    };

    struct CountingObjectContext
    {
        hgps::ObjectStore store{};
        std::shared_ptr<IoCounters> counters{};
    };

    struct CountingFrameContext
    {
        hgps::FrameStore store{};
        std::shared_ptr<IoCounters> counters{};
    };

    [[nodiscard]] const hgps::ObjectStoreOps &counting_object_ops()
    {
        static const hgps::ObjectStoreOps ops{
            [](void *context, std::string_view key, std::span<const std::byte> data)
            {
                auto &counting = *static_cast<CountingObjectContext *>(context);
                ++counting.counters->object_puts;
                return counting.store.put_immutable(key, data);
            },
            [](void *context, std::string_view key)
            {
                auto &counting = *static_cast<CountingObjectContext *>(context);
                ++counting.counters->object_gets;
                return counting.store.get(key);
            },
            [](void *context, std::string_view prefix, std::optional<std::string_view> start_after,
               std::size_t limit)
            {
                auto &counting = *static_cast<CountingObjectContext *>(context);
                ++counting.counters->object_lists;
                return counting.store.list(prefix, start_after, limit);
            },
            [](void *context, std::string_view key, std::optional<std::string_view> expected,
               std::span<const std::byte> desired)
            {
                auto &counting = *static_cast<CountingObjectContext *>(context);
                ++counting.counters->object_compares;
                return counting.store.compare_exchange_ref(key, expected, desired);
            },
            [](void *context) { static_cast<CountingObjectContext *>(context)->store.clear(); },
        };
        return ops;
    }

    [[nodiscard]] const hgps::FrameStoreOps &counting_frame_ops()
    {
        static const hgps::FrameStoreOps ops{
            [](void *context, std::string_view key, hg::Frame value,
               std::optional<hgps::Compression> compression)
            {
                auto &counting = *static_cast<CountingFrameContext *>(context);
                ++counting.counters->frame_writes;
                counting.store.write(key, std::move(value), compression);
            },
            [](void *context, std::string_view key)
            {
                auto &counting = *static_cast<CountingFrameContext *>(context);
                ++counting.counters->frame_reads;
                return counting.store.read(key);
            },
            [](void *context, std::string_view key)
            {
                auto &counting = *static_cast<CountingFrameContext *>(context);
                ++counting.counters->frame_contains;
                return counting.store.contains(key);
            },
            [](void *context) { static_cast<CountingFrameContext *>(context)->store.clear(); },
        };
        return ops;
    }

    [[nodiscard]] hgf::FabricConfig counting_config(const hgf::FabricConfig &base,
                                                    std::shared_ptr<IoCounters> counters)
    {
        hgf::FabricConfig result = base;
        result.objects =
            hgps::ObjectStore{std::make_shared<CountingObjectContext>(CountingObjectContext{
                                  .store = base.objects,
                                  .counters = counters,
                              }),
                              counting_object_ops()};
        result.frames =
            hgps::FrameStore{std::make_shared<CountingFrameContext>(CountingFrameContext{
                                 .store = base.frames,
                                 .counters = std::move(counters),
                             }),
                             counting_frame_ops()};
        return result;
    }

    [[nodiscard]] hg::Frame frame(std::int64_t value)
    {
        arrow::Int64Builder builder;
        if (!builder.Append(value).ok())
        {
            throw std::runtime_error("failed to append test Frame value");
        }
        auto array = builder.Finish();
        if (!array.ok())
        {
            throw std::runtime_error("failed to finish test Frame value");
        }
        return hg::Frame{arrow::Table::Make(arrow::schema({arrow::field("value", arrow::int64())}),
                                            {std::move(array).ValueOrDie()})};
    }

    [[nodiscard]] std::int64_t frame_value(const hg::Frame &value)
    {
        if (!value.has_value() || value.table->num_rows() != 1)
        {
            throw std::runtime_error("test Frame is not a one-row value");
        }
        const auto values =
            std::static_pointer_cast<arrow::Int64Array>(value.table->column(0)->chunk(0));
        return values->Value(0);
    }

    [[nodiscard]] hgf::DataRevisionInput
    seed(const hgf::FabricConfig &config, hg::Str data_id, hgf::RevisionId revision,
         hgf::DataVersion output_version, hg::DateTime as_of,
         std::vector<hgf::DataDependencyInput> dependencies = {})
    {
        const std::string data_key = hgf::data_version_key(config.prefix, data_id, output_version);
        if (!config.frames.contains(data_key))
        {
            config.frames.write(data_key, frame(output_version));
        }
        hg::Value value = hgf::make_data_revision(hgf::DataRevisionInput{
            .data_id = std::move(data_id),
            .revision = revision,
            .output_version = output_version,
            .dependencies = std::move(dependencies),
            .as_of = as_of,
        });
        const hgf::DataRevisionInput decoded = hgf::data_revision_input(value.view());
        const auto revision_result = config.objects.put_immutable(
            hgf::revision_key(config.prefix, decoded.data_id, revision),
            config.values.encode(value.view()));
        if (revision_result.status == hgps::ImmutableWriteStatus::Conflict)
        {
            throw std::runtime_error("test revision conflicted");
        }
        const auto as_of_result = config.objects.put_immutable(
            hgf::as_of_key(config.prefix, decoded.data_id, as_of),
            hgf::encode_reference(config.values, hgf::MetadataObjectKind::AsOf, revision));
        if (as_of_result.status == hgps::ImmutableWriteStatus::Conflict)
        {
            throw std::runtime_error("test as-of index conflicted");
        }
        const std::string latest_key = hgf::latest_key(config.prefix, decoded.data_id);
        const auto current = config.objects.get(latest_key);
        const auto latest = config.objects.compare_exchange_ref(
            latest_key,
            current.has_value() ? std::optional<std::string_view>{current->version_token}
                                : std::nullopt,
            hgf::encode_reference(config.values, hgf::MetadataObjectKind::Latest, revision));
        if (!latest.exchanged)
        {
            throw std::runtime_error("test latest index update lost a race");
        }
        return decoded;
    }

    struct CaptureFrame
    {
        static constexpr auto name = "hgraph.fabric.test.capture_frame";

        static void eval(hg::DateTime now, hg::In<"value", hg::TS<hg::Frame>> value)
        {
            observed_frames.emplace_back(now, frame_value(value.value()));
        }
    };

    struct CaptureTaggedFrame
    {
        static constexpr auto name = "hgraph.fabric.test.capture_tagged_frame";

        static void eval(hg::DateTime now, hg::In<"value", hg::TS<hg::Frame>> value,
                         hg::Scalar<"label", hg::Str> label)
        {
            observed_tagged_frames.emplace_back(now, label.value(), frame_value(value.value()));
        }
    };

    struct ReplayGraph
    {
        static constexpr auto name = "hgraph.fabric.test.replay";

        static void compose(hg::Wiring &wiring)
        {
            hgf::register_service(wiring);
            auto value = hgf::subscribe_data(wiring, "prices");
            static_cast<void>(hg::wire<CaptureFrame>(wiring, value));
        }
    };

    struct EqualTimeReplayGraph
    {
        static constexpr auto name = "hgraph.fabric.test.equal_time_replay";

        static void compose(hg::Wiring &wiring)
        {
            hgf::register_service(wiring);
            auto left = hgf::subscribe_data(wiring, "left");
            auto right = hgf::subscribe_data(wiring, "right");
            static_cast<void>(hg::wire<CaptureTaggedFrame>(wiring, left, hg::Str{"left"}));
            static_cast<void>(hg::wire<CaptureTaggedFrame>(wiring, right, hg::Str{"right"}));
        }
    };

    struct DynamicAncestryReplayGraph
    {
        static constexpr auto name = "hgraph.fabric.test.dynamic_ancestry_replay";

        static void compose(hg::Wiring &wiring)
        {
            hgf::register_service(wiring);
            auto value = hgf::subscribe_data(wiring, "derived");
            static_cast<void>(hg::wire<CaptureFrame>(wiring, value));
        }
    };

    struct LiveNoticeSource
    {
        static constexpr auto name = "hgraph.fabric.test.live_notice";
        static constexpr bool schedule_on_start = true;

        static void eval(hg::GlobalStateView global_state, hg::NodeScheduler scheduler,
                         hg::State<hg::Int> phase,
                         hg::Out<hg::TS<hg::Shared<hgf::DataRevision>>> out)
        {
            if (phase.get() == hg::Int{0})
            {
                phase.set(hg::Int{1});
                scheduler.schedule(hg::TimeDelta{5});
                return;
            }
            if (phase.get() != hg::Int{1})
            {
                return;
            }
            const auto config = hgf::fabric_config(global_state);
            if (!config.has_value())
            {
                throw std::logic_error("test live source has no FabricConfig");
            }
            const auto revision = seed(*config, "prices", 2, 2, BASE_TIME + hg::TimeDelta{2});
            hg::Value value = hgf::make_data_revision(revision);
            out.apply(value.view());
            phase.set(hg::Int{2});
        }
    };

    struct LiveGraph
    {
        static constexpr auto name = "hgraph.fabric.test.live";

        static void compose(hg::Wiring &wiring)
        {
            hgf::register_service(wiring);
            auto value = hgf::subscribe_data(wiring, "prices");
            auto notice = hg::wire<LiveNoticeSource>(wiring);
            hgf::submit_notice(wiring, notice, hg::service::path(hgf::DEFAULT_SERVICE_PATH));
            static_cast<void>(hg::wire<CaptureFrame>(wiring, value));
        }
    };

    struct LiveTransportFirstGraph
    {
        static constexpr auto name = "hgraph.fabric.test.live_transport_first";

        static void compose(hg::Wiring &wiring)
        {
            hgf::register_service(wiring);
            auto notice = hg::wire<LiveNoticeSource>(wiring);
            hgf::submit_notice(wiring, notice, hg::service::path(hgf::DEFAULT_SERVICE_PATH));
            auto value = hgf::subscribe_data(wiring, "prices");
            static_cast<void>(hg::wire<CaptureFrame>(wiring, value));
        }
    };

    std::optional<hgf::FabricConfig> notice_seed_config{};
    std::shared_ptr<IoCounters> notice_io_counters{};
    std::vector<hgf::DataRevisionInput> notice_specs{};

    template <std::size_t Index, std::int64_t Delay, bool ResetCounters, std::size_t SeedCount = 1>
    struct CountingNoticeSource
    {
        static constexpr auto name = "hgraph.fabric.test.counting_notice";
        static constexpr bool schedule_on_start = true;

        static void eval(hg::NodeScheduler scheduler, hg::State<hg::Int> phase,
                         hg::Out<hg::TS<hg::Shared<hgf::DataRevision>>> out)
        {
            if (phase.get() == hg::Int{0})
            {
                phase.set(hg::Int{1});
                scheduler.schedule(hg::TimeDelta{Delay});
                return;
            }
            if (phase.get() != hg::Int{1})
            {
                return;
            }
            if (!notice_seed_config.has_value() || Index + SeedCount > notice_specs.size())
            {
                throw std::logic_error("counting notice source is not configured");
            }
            std::optional<hgf::DataRevisionInput> revision;
            for (std::size_t offset = 0; offset < SeedCount; ++offset)
            {
                auto spec = notice_specs[Index + offset];
                revision = seed(*notice_seed_config, std::move(spec.data_id), spec.revision,
                                spec.output_version, spec.as_of, std::move(spec.dependencies));
            }
            if constexpr (ResetCounters)
            {
                if (!notice_io_counters)
                {
                    throw std::logic_error("counting notice source has no counters");
                }
                notice_io_counters->reset();
            }
            hg::Value value = hgf::make_data_revision(std::move(*revision));
            out.apply(value.view());
            phase.set(hg::Int{2});
        }
    };

    struct CompleteNoticeGraph
    {
        static constexpr auto name = "hgraph.fabric.test.complete_notice";

        static void compose(hg::Wiring &wiring)
        {
            hgf::register_service(wiring);
            auto value = hgf::subscribe_data(wiring, "prices");
            hgf::submit_notice(wiring, hg::wire<CountingNoticeSource<0, 5, true>>(wiring));
            static_cast<void>(hg::wire<CaptureFrame>(wiring, value));
        }
    };

    struct CachedAncestryNoticeGraph
    {
        static constexpr auto name = "hgraph.fabric.test.cached_ancestry_notice";

        static void compose(hg::Wiring &wiring)
        {
            hgf::register_service(wiring);
            auto value = hgf::subscribe_data(wiring, "z-root");
            hgf::submit_notice(wiring, hg::wire<CountingNoticeSource<0, 3, true>>(wiring));
            hgf::submit_notice(wiring, hg::wire<CountingNoticeSource<1, 5, false>>(wiring));
            static_cast<void>(hg::wire<CaptureFrame>(wiring, value));
        }
    };

    struct GapNoticeGraph
    {
        static constexpr auto name = "hgraph.fabric.test.gap_notice";

        static void compose(hg::Wiring &wiring)
        {
            hgf::register_service(wiring);
            auto value = hgf::subscribe_data(wiring, "prices");
            hgf::submit_notice(wiring, hg::wire<CountingNoticeSource<0, 5, true, 2>>(wiring));
            static_cast<void>(hg::wire<CaptureFrame>(wiring, value));
        }
    };

    struct PublishedFrameSource
    {
        static constexpr auto name = "hgraph.fabric.test.published_frame";
        static constexpr bool schedule_on_start = true;

        static void eval(hg::Out<hg::TS<hg::Frame>> out)
        {
            out.set(frame(71));
        }
    };

    struct PublicationGraph
    {
        static constexpr auto name = "hgraph.fabric.test.publication";

        static void compose(hg::Wiring &wiring)
        {
            hgf::register_service(wiring);
            hgf::publish_data(wiring, "published", hg::wire<PublishedFrameSource>(wiring));
        }
    };

    struct RetryThenDeliverNotification
    {
        static constexpr auto name = "hgraph.fabric.test.retry_then_deliver_notification";

        static void eval(hg::In<"revision", hg::TS<hg::Shared<hgf::DataRevision>>> revision,
                         hg::State<hg::Int> attempts,
                         hg::Out<hgf::FabricNotificationDelivery> delivery)
        {
            const hgf::DataRevisionInput decoded =
                hgf::data_revision_input(revision.base().value().concrete());
            observed_notification_allocations.push_back(revision.base().value().concrete().data());
            const bool delivered = attempts.get() != 0;
            delivery.template field<"data_id">().set(decoded.data_id);
            delivery.template field<"revision">().set(decoded.revision);
            delivery.template field<"delivered">().set(delivered);
            delivery.template field<"retriable">().set(!delivered);
            delivery.template field<"message">().set(delivered ? hg::Str{} : hg::Str{"retry once"});
            attempts.set(attempts.get() + 1);
        }
    };

    struct AlwaysRetryNotification
    {
        static void eval(hg::In<"revision", hg::TS<hg::Shared<hgf::DataRevision>>> revision,
                         hg::Out<hgf::FabricNotificationDelivery> delivery)
        {
            const auto decoded = hgf::data_revision_input(revision.base().value().concrete());
            delivery.template field<"data_id">().set(decoded.data_id);
            delivery.template field<"revision">().set(decoded.revision);
            delivery.template field<"delivered">().set(false);
            delivery.template field<"retriable">().set(true);
            delivery.template field<"message">().set("retry");
        }
    };

    struct DeliverOnRetryNotification
    {
        static void eval(hg::In<"revision", hg::TS<hg::Shared<hgf::DataRevision>>> revision,
                         hg::State<hg::Int> attempts,
                         hg::Out<hgf::FabricNotificationDelivery> delivery)
        {
            if (attempts.get() != 0)
            {
                const auto decoded = hgf::data_revision_input(revision.base().value().concrete());
                delivery.template field<"data_id">().set(decoded.data_id);
                delivery.template field<"revision">().set(decoded.revision);
                delivery.template field<"delivered">().set(true);
                delivery.template field<"retriable">().set(false);
            }
            attempts.set(attempts.get() + 1);
        }
    };

    struct CaptureNotificationMetrics
    {
        static constexpr auto name = "hgraph.fabric.test.capture_notification_metrics";

        static void eval(hg::In<"values", hgf::FabricDiagnostics> values)
        {
            const auto metrics = values.template field<"metrics">();
            for (const auto &[metric_name, value] : metrics.modified_items())
            {
                if (value.valid())
                {
                    observed_notification_metrics.insert_or_assign(
                        metric_name.checked_as<hg::Str>(), value.value());
                }
            }
        }
    };

    struct ReplayDiagnosticsGraph
    {
        static constexpr auto name = "hgraph.fabric.test.replay_diagnostics";

        static void compose(hg::Wiring &wiring)
        {
            hgf::register_service(wiring);
            auto value = hgf::subscribe_data(wiring, "prices");
            static_cast<void>(hg::wire<CaptureFrame>(wiring, value));
            static_cast<void>(
                hg::wire<CaptureNotificationMetrics>(wiring, hgf::diagnostics(wiring)));
        }
    };

    struct GraphNotificationPublicationGraph
    {
        static constexpr auto name = "hgraph.fabric.test.graph_notification_publication";

        static void compose(hg::Wiring &wiring)
        {
            const auto path = hg::service::path(hgf::DEFAULT_SERVICE_PATH);
            hgf::register_service(wiring, path, hgf::FabricNotificationMode::GraphTransport);
            hgf::publish_data(wiring, "published", hg::wire<PublishedFrameSource>(wiring));
            // Production delivery returns through Kafka's asynchronous push-source
            // edge. Feedback supplies the equivalent cycle break in this pure graph.
            auto delivery_feedback = hg::stdlib::feedback<hgf::FabricNotificationDelivery>(wiring);
            hgf::submit_notification_delivery(wiring, delivery_feedback(), path);
            auto delivery = hg::wire<RetryThenDeliverNotification>(
                wiring, hgf::notification_requests(wiring, path));
            delivery_feedback(delivery);
            static_cast<void>(
                hg::wire<CaptureNotificationMetrics>(wiring, hgf::diagnostics(wiring, path)));
        }
    };

    struct DuplicateRetryThenSuccessGraph
    {
        static void compose(hg::Wiring &wiring)
        {
            const auto path = hg::service::path(hgf::DEFAULT_SERVICE_PATH);
            hgf::register_service(wiring, path, hgf::FabricNotificationMode::GraphTransport);
            hgf::publish_data(wiring, "published", hg::wire<PublishedFrameSource>(wiring));
            auto request = hgf::notification_requests(wiring, path);
            // Request ids follow client wiring order, reproducing the failing
            // retry-before-success delivery ordering in one evaluation.
            auto retry_feedback = hg::stdlib::feedback<hgf::FabricNotificationDelivery>(wiring);
            auto success_feedback = hg::stdlib::feedback<hgf::FabricNotificationDelivery>(wiring);
            hgf::submit_notification_delivery(wiring, retry_feedback(), path);
            hgf::submit_notification_delivery(wiring, success_feedback(), path);
            retry_feedback(hg::wire<AlwaysRetryNotification>(wiring, request));
            success_feedback(hg::wire<DeliverOnRetryNotification>(wiring, request));
            static_cast<void>(
                hg::wire<CaptureNotificationMetrics>(wiring, hgf::diagnostics(wiring, path)));
        }
    };

    struct CaptureLoad
    {
        static constexpr auto name = "hgraph.fabric.test.capture_load";

        static void eval(hg::DateTime now, hg::In<"loaded", hgf::FabricLoadResponse> loaded)
        {
            const auto value = loaded.template field<"frame">();
            if (value.modified() && value.valid())
            {
                observed_frames.emplace_back(now, frame_value(value.value()));
            }
        }
    };

    struct LoadGraph
    {
        static constexpr auto name = "hgraph.fabric.test.load";

        static void compose(hg::Wiring &wiring)
        {
            hgf::register_service(wiring);
            static_cast<void>(
                hg::wire<CaptureLoad>(wiring, hgf::request_load(wiring, "prices", 2)));
        }
    };

    struct CaptureDiagnostics
    {
        static constexpr auto name = "hgraph.fabric.test.capture_diagnostics";

        static void eval(hg::In<"values", hgf::FabricDiagnostics> values)
        {
            const auto events = values.template field<"events">();
            for (const auto &[path, event] : events.modified_items())
            {
                if (!event.valid())
                {
                    continue;
                }
                const auto fields = event.base().value().as_bundle();
                observed_diagnostic_events.insert_or_assign(
                    path.checked_as<hg::Str>(),
                    hgf::FabricDiagnosticEventInput{
                        .component = fields.at("component").checked_as<hg::Str>(),
                        .category = fields.at("category").checked_as<hg::Str>(),
                        .message = fields.at("message").checked_as<hg::Str>(),
                        .retriable = fields.at("retriable").checked_as<hg::Bool>(),
                        .fatal = fields.at("fatal").checked_as<hg::Bool>(),
                        .occurrences = fields.at("occurrences").checked_as<hg::Int>(),
                    });
            }
        }
    };

    struct SingleTransportEventSource
    {
        static constexpr auto name = "hgraph.fabric.test.single_transport_event";
        static constexpr bool schedule_on_start = true;

        static void eval(hg::Out<hgf::FabricTransportEvent> event)
        {
            event.template field<"component">().set("kafka");
            event.template field<"category">().set("disconnect");
            event.template field<"message">().set("broker unavailable");
            event.template field<"retriable">().set(true);
            event.template field<"fatal">().set(false);
        }
    };

    struct BoundedTransportEventSource
    {
        static constexpr auto name = "hgraph.fabric.test.bounded_transport_events";
        static constexpr bool schedule_on_start = true;

        static void eval(hg::NodeScheduler scheduler, hg::State<hg::Int> index,
                         hg::Out<hgf::FabricTransportEvent> event)
        {
            const hg::Int current = index.get();
            if (current >= static_cast<hg::Int>(hgf::FABRIC_DIAGNOSTIC_EVENT_LIMIT + 20U))
            {
                return;
            }
            event.template field<"component">().set("kafka");
            event.template field<"category">().set("category-" + std::to_string(current));
            event.template field<"message">().set("failure");
            event.template field<"retriable">().set(true);
            event.template field<"fatal">().set(false);
            index.set(current + 1);
            scheduler.schedule(hg::MIN_TD);
        }
    };

    struct TransportDiagnosticsGraph
    {
        static constexpr auto name = "hgraph.fabric.test.transport_diagnostics";

        static void compose(hg::Wiring &wiring)
        {
            hgf::register_service(wiring);
            hgf::submit_transport_event(wiring, hg::wire<SingleTransportEventSource>(wiring),
                                        hg::service::path(hgf::DEFAULT_SERVICE_PATH));
            static_cast<void>(hg::wire<CaptureDiagnostics>(wiring, hgf::diagnostics(wiring)));
        }
    };

    struct BoundedTransportDiagnosticsGraph
    {
        static constexpr auto name = "hgraph.fabric.test.bounded_transport_diagnostics";

        static void compose(hg::Wiring &wiring)
        {
            hgf::register_service(wiring);
            hgf::submit_transport_event(wiring, hg::wire<BoundedTransportEventSource>(wiring),
                                        hg::service::path(hgf::DEFAULT_SERVICE_PATH));
            static_cast<void>(hg::wire<CaptureDiagnostics>(wiring, hgf::diagnostics(wiring)));
        }
    };

    struct MissingLoadDiagnosticsGraph
    {
        static constexpr auto name = "hgraph.fabric.test.missing_load_diagnostics";

        static void compose(hg::Wiring &wiring)
        {
            hgf::register_service(wiring);
            static_cast<void>(hgf::request_load(wiring, "missing", 1));
            static_cast<void>(hg::wire<CaptureDiagnostics>(wiring, hgf::diagnostics(wiring)));
        }
    };

    [[nodiscard]] hg::GraphExecutorValue
    run(hg::GraphBuilder graph, const hgf::FabricConfig &config, hg::DateTime start,
        hg::DateTime end, hg::GraphExecutorMode mode = hg::GraphExecutorMode::Simulation)
    {
        hgf::set_fabric_config(graph.global_state(), config);
        return hg::testing::run_graph(std::move(graph), start, end, mode);
    }

    template <typename Graph> [[nodiscard]] hg::GraphBuilder build_realtime_graph()
    {
        return hg::build_graph<Graph>(hg::WiringOptions{.is_realtime = true});
    }
} // namespace

TEST_CASE("service lifecycle logs its path once at start and stop")
{
    hg::stdlib::register_standard_operators();
    hgf::register_fabric_operators();
    auto config = hgf::make_memory_fabric_config("tests/subscription/lifecycle-log");
    static_cast<void>(seed(config, "prices", 1, 1, BASE_TIME + hg::TimeDelta{1}));
    observed_frames.clear();
    CapturedLog captured;

    static_cast<void>(
        run(hg::build_graph<ReplayGraph>(), config, BASE_TIME, BASE_TIME + hg::TimeDelta{2}));

    const auto output = captured.joined();
    CHECK(output.find("hgraph.fabric service started path=fabric") != std::string::npos);
    CHECK(output.find("hgraph.fabric service stopped path=fabric") != std::string::npos);
}

TEST_CASE("reusable graph builders create independent Fabric node state")
{
    hg::stdlib::register_standard_operators();
    hgf::register_fabric_operators();
    auto config = hgf::make_memory_fabric_config("tests/subscription/independent-node-state");
    auto graph = hg::build_graph<ReplayGraph>();
    hgf::set_fabric_config(graph.global_state(), config);

    hg::GraphExecutorBuilder builder;
    builder.graph_builder(std::move(graph))
        .start_time(BASE_TIME)
        .end_time(BASE_TIME + hg::TimeDelta{2});
    auto first = builder.make_executor();
    auto second = builder.make_executor();
    auto first_graph = first.view().graph();
    auto second_graph = second.view().graph();

    first_graph.start(BASE_TIME);
    try
    {
        second_graph.start(BASE_TIME);
    }
    catch (...)
    {
        first_graph.stop(BASE_TIME);
        throw;
    }
    second_graph.stop(BASE_TIME);
    first_graph.stop(BASE_TIME);
}

TEST_CASE("replay emits ordered as-of images over a half-open run interval")
{
    hg::stdlib::register_standard_operators();
    hgf::register_fabric_operators();
    auto config = hgf::make_memory_fabric_config("tests/subscription/replay");
    static_cast<void>(seed(config, "prices", 1, 1, BASE_TIME + hg::TimeDelta{10}));
    static_cast<void>(seed(config, "prices", 2, 2, BASE_TIME + hg::TimeDelta{20}));
    static_cast<void>(seed(config, "prices", 3, 3, BASE_TIME + hg::TimeDelta{30}));
    observed_frames.clear();

    static_cast<void>(run(hg::build_graph<ReplayGraph>(), config, BASE_TIME + hg::TimeDelta{15},
                          BASE_TIME + hg::TimeDelta{30}));

    REQUIRE(observed_frames.size() == 2);
    CHECK(observed_frames[0].first == BASE_TIME + hg::TimeDelta{15});
    CHECK(observed_frames[0].second == 1);
    CHECK(observed_frames[1].first == BASE_TIME + hg::TimeDelta{20});
    CHECK(observed_frames[1].second == 2);
}

TEST_CASE("replay from MIN_ST begins at the first durable publication")
{
    hg::stdlib::register_standard_operators();
    hgf::register_fabric_operators();
    auto config = hgf::make_memory_fabric_config("tests/subscription/replay-min-start");
    static_cast<void>(seed(config, "prices", 1, 1, hg::MIN_ST + hg::TimeDelta{2}));
    observed_frames.clear();

    static_cast<void>(
        run(hg::build_graph<ReplayGraph>(), config, hg::MIN_ST, hg::MIN_ST + hg::TimeDelta{5}));

    REQUIRE(observed_frames.size() == 1);
    CHECK(observed_frames.front().first == hg::MIN_ST + hg::TimeDelta{2});
    CHECK(observed_frames.front().second == 1);
}

TEST_CASE("replay batches equal as-of timestamps across direct roots")
{
    hg::stdlib::register_standard_operators();
    hgf::register_fabric_operators();
    auto config = hgf::make_memory_fabric_config("tests/subscription/replay-equal-time");
    const auto published_at = BASE_TIME + hg::TimeDelta{10};
    static_cast<void>(seed(config, "left", 1, 1, published_at));
    static_cast<void>(seed(config, "right", 1, 1, published_at));
    observed_tagged_frames.clear();

    static_cast<void>(run(hg::build_graph<EqualTimeReplayGraph>(), config,
                          BASE_TIME + hg::TimeDelta{1}, BASE_TIME + hg::TimeDelta{20}));

    REQUIRE(observed_tagged_frames.size() == 2);
    CHECK(std::get<0>(observed_tagged_frames[0]) == published_at);
    CHECK(std::get<0>(observed_tagged_frames[1]) == published_at);
    std::map<hg::Str, std::int64_t> values;
    for (const auto &[time, label, value] : observed_tagged_frames)
    {
        static_cast<void>(time);
        values.emplace(label, value);
    }
    CHECK((values == std::map<hg::Str, std::int64_t>{{"left", 1}, {"right", 1}}));
}

TEST_CASE("replay follows dynamically introduced transitive ancestry")
{
    hg::stdlib::register_standard_operators();
    hgf::register_fabric_operators();
    auto config = hgf::make_memory_fabric_config("tests/subscription/replay-ancestry");
    static_cast<void>(seed(config, "base", 1, 1, BASE_TIME + hg::TimeDelta{5}));
    static_cast<void>(seed(config, "middle", 1, 1, BASE_TIME + hg::TimeDelta{7}, {{"base", 1}}));
    static_cast<void>(
        seed(config, "derived", 1, 1, BASE_TIME + hg::TimeDelta{10}, {{"middle", 1}}));
    static_cast<void>(seed(config, "base", 2, 2, BASE_TIME + hg::TimeDelta{15}));
    static_cast<void>(seed(config, "middle", 2, 2, BASE_TIME + hg::TimeDelta{20}, {{"base", 2}}));
    static_cast<void>(
        seed(config, "derived", 2, 2, BASE_TIME + hg::TimeDelta{30}, {{"middle", 2}}));
    observed_frames.clear();

    static_cast<void>(run(hg::build_graph<DynamicAncestryReplayGraph>(), config,
                          BASE_TIME + hg::TimeDelta{1}, BASE_TIME + hg::TimeDelta{40}));

    REQUIRE(observed_frames.size() == 2);
    CHECK(observed_frames[0].first == BASE_TIME + hg::TimeDelta{10});
    CHECK(observed_frames[0].second == 1);
    CHECK(observed_frames[1].first == BASE_TIME + hg::TimeDelta{30});
    CHECK(observed_frames[1].second == 2);
}

TEST_CASE("live starts from durable state and advances only from notice ticks")
{
    hg::stdlib::register_standard_operators();
    hgf::register_fabric_operators();
    auto config = hgf::make_memory_fabric_config("tests/subscription/live");
    static_cast<void>(seed(config, "prices", 1, 1, BASE_TIME + hg::TimeDelta{1}));
    observed_frames.clear();
    const auto start = hg::testing::wall_now();

    static_cast<void>(run(build_realtime_graph<LiveGraph>(), config, start,
                          start + hg::TimeDelta{100}, hg::GraphExecutorMode::RealTime));

    REQUIRE(observed_frames.size() == 2);
    CHECK(observed_frames[0].first == start);
    CHECK(observed_frames[0].second == 1);
    CHECK(observed_frames[1].first == start + hg::TimeDelta{5});
    CHECK(observed_frames[1].second == 2);
}

TEST_CASE("live plan freezes after transport-first service materialization")
{
    hg::stdlib::register_standard_operators();
    hgf::register_fabric_operators();
    auto config = hgf::make_memory_fabric_config("tests/subscription/live-transport-first");
    static_cast<void>(seed(config, "prices", 1, 1, BASE_TIME + hg::TimeDelta{1}));
    observed_frames.clear();
    const auto start = hg::testing::wall_now();

    static_cast<void>(run(build_realtime_graph<LiveTransportFirstGraph>(), config, start,
                          start + hg::TimeDelta{100}, hg::GraphExecutorMode::RealTime));

    REQUIRE(observed_frames.size() == 2);
    CHECK(observed_frames[0].second == 1);
    CHECK(observed_frames[1].second == 2);
}

TEST_CASE("complete live notices avoid metadata reads and load only the "
          "selected Frame")
{
    hg::stdlib::register_standard_operators();
    hgf::register_fabric_operators();
    auto base = hgf::make_memory_fabric_config("tests/subscription/complete-notice");
    static_cast<void>(seed(base, "prices", 1, 1, BASE_TIME + hg::TimeDelta{1}));
    auto counters = std::make_shared<IoCounters>();
    auto observed_config = counting_config(base, counters);
    notice_seed_config = base;
    notice_io_counters = counters;
    notice_specs = {hgf::DataRevisionInput{
        .data_id = "prices",
        .revision = 2,
        .output_version = 2,
        .as_of = BASE_TIME + hg::TimeDelta{2},
    }};
    observed_frames.clear();
    const auto start = hg::testing::wall_now();

    static_cast<void>(run(build_realtime_graph<CompleteNoticeGraph>(), observed_config, start,
                          start + hg::TimeDelta{100}, hg::GraphExecutorMode::RealTime));

    REQUIRE(observed_frames.size() == 2);
    CHECK(observed_frames.back().second == 2);
    CHECK(counters->object_gets == 0);
    CHECK(counters->object_lists == 0);
    CHECK(counters->object_puts == 0);
    CHECK(counters->object_compares == 0);
    CHECK(counters->frame_reads == 1);
    notice_specs.clear();
    notice_seed_config.reset();
    notice_io_counters.reset();
}

TEST_CASE("live gap recovery reads only missing metadata and the selected Frame")
{
    hg::stdlib::register_standard_operators();
    hgf::register_fabric_operators();
    auto base = hgf::make_memory_fabric_config("tests/subscription/notice-gap");
    static_cast<void>(seed(base, "prices", 1, 1, BASE_TIME + hg::TimeDelta{1}));
    auto counters = std::make_shared<IoCounters>();
    auto observed_config = counting_config(base, counters);
    notice_seed_config = base;
    notice_io_counters = counters;
    notice_specs = {
        hgf::DataRevisionInput{
            .data_id = "prices",
            .revision = 2,
            .output_version = 2,
            .as_of = BASE_TIME + hg::TimeDelta{2},
        },
        hgf::DataRevisionInput{
            .data_id = "prices",
            .revision = 3,
            .output_version = 3,
            .as_of = BASE_TIME + hg::TimeDelta{3},
        },
    };
    observed_frames.clear();
    const auto start = hg::testing::wall_now();

    static_cast<void>(run(build_realtime_graph<GapNoticeGraph>(), observed_config, start,
                          start + hg::TimeDelta{100}, hg::GraphExecutorMode::RealTime));

    REQUIRE(observed_frames.size() == 2);
    CHECK(observed_frames.back().second == 3);
    CHECK(counters->object_gets == 1);
    CHECK(counters->object_lists == 0);
    CHECK(counters->object_puts == 0);
    CHECK(counters->object_compares == 0);
    CHECK(counters->frame_reads == 1);
    notice_specs.clear();
    notice_seed_config.reset();
    notice_io_counters.reset();
}

TEST_CASE("live uses durable fallback for dependency notices received before "
          "they are observed")
{
    hg::stdlib::register_standard_operators();
    hgf::register_fabric_operators();
    auto base = hgf::make_memory_fabric_config("tests/subscription/cached-ancestry");
    static_cast<void>(seed(base, "z-root", 1, 1, BASE_TIME + hg::TimeDelta{1}));
    auto counters = std::make_shared<IoCounters>();
    auto observed_config = counting_config(base, counters);
    notice_seed_config = base;
    notice_io_counters = counters;
    notice_specs = {
        hgf::DataRevisionInput{
            .data_id = "a-ancestor",
            .revision = 1,
            .output_version = 1,
            .as_of = BASE_TIME + hg::TimeDelta{2},
        },
        hgf::DataRevisionInput{
            .data_id = "z-root",
            .revision = 2,
            .output_version = 2,
            .dependencies = {{"a-ancestor", 1}},
            .as_of = BASE_TIME + hg::TimeDelta{3},
        },
    };
    observed_frames.clear();
    const auto start = hg::testing::wall_now();

    static_cast<void>(run(build_realtime_graph<CachedAncestryNoticeGraph>(), observed_config, start,
                          start + hg::TimeDelta{100}, hg::GraphExecutorMode::RealTime));

    REQUIRE(observed_frames.size() == 2);
    CHECK(observed_frames.back().second == 2);
    CHECK(counters->object_gets == 4);
    CHECK(counters->object_lists == 1);
    CHECK(counters->frame_reads == 2);
    notice_specs.clear();
    notice_seed_config.reset();
    notice_io_counters.reset();
}

TEST_CASE("simulation Fabric service graph contains no push sources")
{
    hg::stdlib::register_standard_operators();
    hgf::register_fabric_operators();
    const auto replay = hg::build_graph<ReplayGraph>();
    CHECK(replay.root_type().schema()->push_source_nodes_end == 0);
}

TEST_CASE("publish_data routes through the singleton service publication state")
{
    hg::stdlib::register_standard_operators();
    hgf::register_fabric_operators();
    auto config = hgf::make_memory_fabric_config("tests/subscription/publication");
    static_cast<void>(
        run(hg::build_graph<PublicationGraph>(), config, BASE_TIME, BASE_TIME + hg::TimeDelta{5}));

    const auto latest = config.objects.get(hgf::latest_key(config.prefix, "published"));
    REQUIRE(latest.has_value());
    CHECK(hgf::revision_reference_value(config.values, hgf::MetadataObjectKind::Latest, latest->data) == 1);
    const auto revision_object =
        config.objects.get(hgf::revision_key(config.prefix, "published", 1));
    REQUIRE(revision_object.has_value());
    const auto revision =
        hgf::data_revision_input(config.values.decode(hgf::data_revision_meta(), revision_object->data).view());
    const auto stored = config.frames.read(
        hgf::data_version_key(config.prefix, "published", revision.output_version));
    REQUIRE(stored.has_value());
    CHECK(frame_value(stored) == 71);
}

TEST_CASE("request_load executes in the graph load node")
{
    hg::stdlib::register_standard_operators();
    hgf::register_fabric_operators();
    auto config = hgf::make_memory_fabric_config("tests/subscription/load");
    static_cast<void>(seed(config, "prices", 1, 2, BASE_TIME + hg::TimeDelta{1}));
    observed_frames.clear();

    static_cast<void>(
        run(hg::build_graph<LoadGraph>(), config, BASE_TIME, BASE_TIME + hg::TimeDelta{5}));

    REQUIRE(observed_frames.size() == 1);
    CHECK(observed_frames.front().second == 2);
}

TEST_CASE("service diagnostics tick when an internal load records an event")
{
    hg::stdlib::register_standard_operators();
    hgf::register_fabric_operators();
    auto config = hgf::make_memory_fabric_config("tests/subscription/load-diagnostics");
    observed_diagnostic_events.clear();

    static_cast<void>(run(hg::build_graph<MissingLoadDiagnosticsGraph>(), config, BASE_TIME,
                          BASE_TIME + hg::TimeDelta{5}));

    REQUIRE(observed_diagnostic_events.contains("store.frame.missing"));
    const auto &event = observed_diagnostic_events.at("store.frame.missing");
    CHECK(event.component == "store");
    CHECK(event.category == "frame.missing");
    CHECK_FALSE(event.retriable);
    CHECK_FALSE(event.fatal);
    CHECK(event.occurrences == 1);
}

TEST_CASE("service diagnostics expose resolver work and bounded queue usage")
{
    hg::stdlib::register_standard_operators();
    hgf::register_fabric_operators();
    auto config = hgf::make_memory_fabric_config("tests/subscription/node-diagnostics");
    static_cast<void>(seed(config, "prices", 1, 1, BASE_TIME + hg::TimeDelta{1}));
    observed_notification_metrics.clear();

    static_cast<void>(run(hg::build_graph<ReplayDiagnosticsGraph>(), config, BASE_TIME,
                          BASE_TIME + hg::TimeDelta{5}));

    CHECK(observed_notification_metrics.at("lifecycle") == "running");
    CHECK(observed_notification_metrics.at("publication.queued") == "0");
    CHECK(observed_notification_metrics.at("publication.queue_limit_per_data_id") == "1024");
    CHECK(observed_notification_metrics.at("live.notice_limit_per_session") == "4096");
    CHECK(observed_notification_metrics.at("resolution.calls") == "2");
    CHECK(observed_notification_metrics.at("resolution.forests.ready") == "1");
    CHECK(std::stoull(observed_notification_metrics.at("resolution.revision_cache.misses")) >= 1);
    CHECK(std::stoull(observed_notification_metrics.at("resolution.frame_cache.misses")) >= 1);
}

TEST_CASE("service diagnostics retain typed transport and store events")
{
    hg::stdlib::register_standard_operators();
    hgf::register_fabric_operators();
    auto config = hgf::make_memory_fabric_config("tests/subscription/typed-failures");
    observed_diagnostic_events.clear();

    static_cast<void>(run(hg::build_graph<MissingLoadDiagnosticsGraph>(), config, BASE_TIME,
                          BASE_TIME + hg::TimeDelta{5}));
    static_cast<void>(run(hg::build_graph<TransportDiagnosticsGraph>(), config, BASE_TIME,
                          BASE_TIME + hg::TimeDelta{5}));

    REQUIRE(observed_diagnostic_events.contains("store.frame.missing"));
    CHECK(observed_diagnostic_events.at("store.frame.missing").component == "store");
    CHECK_FALSE(observed_diagnostic_events.at("store.frame.missing").retriable);
    CHECK_FALSE(observed_diagnostic_events.at("store.frame.missing").fatal);
    REQUIRE(observed_diagnostic_events.contains("kafka.disconnect"));
    CHECK(observed_diagnostic_events.at("kafka.disconnect").message == "broker unavailable");
    CHECK(observed_diagnostic_events.at("kafka.disconnect").retriable);
    CHECK_FALSE(observed_diagnostic_events.at("kafka.disconnect").fatal);
    CHECK(observed_diagnostic_events.at("kafka.disconnect").occurrences == 1);
}

TEST_CASE("stalled service queues apply backpressure or enforce hard bounds")
{
    SECTION("graph notification candidates apply configured backpressure")
    {
        auto config = hgf::make_memory_fabric_config("tests/subscription/notification-bound", 1U);
        hgf::detail::PublicationNodeState state;
        state.start(config, true);
        state.enqueue(hgf::detail::PublicationRequestInput{.data_id = "a", .output = frame(1)});
        state.enqueue(hgf::detail::PublicationRequestInput{.data_id = "b", .output = frame(2)});

        const auto first = state.advance(1U);
        REQUIRE(first.size() == 1U);
        CHECK(state.notification_request_limit() == 1U);
        CHECK(state.advance(0U).empty());
        CHECK(state.work_pending());

        state.complete(hgf::detail::NotificationDeliveryInput{
            .data_id = first.front().data_id,
            .revision = first.front().revision,
            .delivered = true,
        });
        const auto second = state.advance(1U);
        REQUIRE(second.size() == 1U);
        CHECK(second.front().data_id != first.front().data_id);

        const auto values = state.diagnostics().metrics;
        const std::map<hg::Str, hg::Str> metrics{values.begin(), values.end()};
        CHECK(metrics.at("transport.notification.request_limit") == "1");
    }

    SECTION("publication requests")
    {
        auto config = hgf::make_memory_fabric_config("tests/subscription/publication-bound");
        hgf::detail::PublicationNodeState state;
        state.start(config, false);

        state.enqueue(hgf::detail::PublicationRequestInput{.data_id = "stalled"});
        for (std::size_t index = 0; index < hgf::FABRIC_PUBLICATION_QUEUE_LIMIT_PER_DATA_ID;
             ++index)
        {
            state.enqueue(hgf::detail::PublicationRequestInput{.data_id = "stalled"});
        }
        CHECK_THROWS_AS(state.enqueue(hgf::detail::PublicationRequestInput{.data_id = "stalled"}),
                        std::overflow_error);
        const auto values = state.diagnostics().metrics;
        const std::map<hg::Str, hg::Str> metrics{values.begin(), values.end()};
        CHECK(std::stoull(metrics.at("publication.queued")) ==
              hgf::FABRIC_PUBLICATION_QUEUE_LIMIT_PER_DATA_ID);
    }

    SECTION("live notices")
    {
        auto config = hgf::make_memory_fabric_config("tests/subscription/live-bound");
        hgf::detail::LiveNodeState state;
        state.start(config);

        std::vector<hgf::detail::SubscriptionSpec> subscriptions;
        subscriptions.reserve(hgf::FABRIC_LIVE_NOTICE_LIMIT_PER_SESSION + 1U);
        std::vector<hgf::DataRevisionInput> revisions;
        revisions.reserve(hgf::FABRIC_LIVE_NOTICE_LIMIT_PER_SESSION);
        for (std::size_t index = 0; index <= hgf::FABRIC_LIVE_NOTICE_LIMIT_PER_SESSION; ++index)
        {
            const hg::Str data_id = "notice-" + std::to_string(index);
            subscriptions.push_back(
                hgf::detail::SubscriptionSpec{.key = data_id, .data_id = data_id});
            if (index == hgf::FABRIC_LIVE_NOTICE_LIMIT_PER_SESSION)
            {
                continue;
            }
            config.frames.write(hgf::data_version_key(config.prefix, data_id, 1), frame(1));
            revisions.push_back(hgf::DataRevisionInput{
                .data_id = data_id,
                .revision = 1,
                .output_version = 1,
                .as_of = BASE_TIME,
            });
        }
        CHECK(state.evaluate(subscriptions, std::move(revisions), BASE_TIME).has_value());
        CHECK_THROWS_AS(
            state.evaluate(subscriptions,
                           {hgf::DataRevisionInput{
                               .data_id = "notice-" +
                                          std::to_string(hgf::FABRIC_LIVE_NOTICE_LIMIT_PER_SESSION),
                               .revision = 1,
                               .output_version = 1,
                               .as_of = BASE_TIME,
                           }},
                           BASE_TIME),
            std::overflow_error);
        const auto values = state.diagnostics().metrics;
        const std::map<hg::Str, hg::Str> metrics{values.begin(), values.end()};
        CHECK(std::stoull(metrics.at("live.notices")) == hgf::FABRIC_LIVE_NOTICE_LIMIT_PER_SESSION);
    }

    SECTION("inactive live notices")
    {
        auto config = hgf::make_memory_fabric_config("tests/subscription/inactive-live");
        hgf::detail::LiveNodeState state;
        state.start(config);

        std::vector<hgf::DataRevisionInput> revisions;
        revisions.reserve(hgf::FABRIC_LIVE_NOTICE_LIMIT_PER_SESSION + 1U);
        for (std::size_t index = 0; index <= hgf::FABRIC_LIVE_NOTICE_LIMIT_PER_SESSION; ++index)
        {
            revisions.push_back(hgf::DataRevisionInput{
                .data_id = "unrelated-" + std::to_string(index),
                .revision = 1,
                .output_version = 1,
                .as_of = BASE_TIME,
            });
        }
        CHECK_FALSE(state.evaluate({}, std::move(revisions), BASE_TIME).has_value());
        const auto values = state.diagnostics().metrics;
        const std::map<hg::Str, hg::Str> metrics{values.begin(), values.end()};
        CHECK(metrics.at("live.notices") == "0");
    }

    SECTION("diagnostic paths")
    {
        hg::stdlib::register_standard_operators();
        hgf::register_fabric_operators();
        auto config = hgf::make_memory_fabric_config("tests/subscription/diagnostic-bound");
        observed_diagnostic_events.clear();

        static_cast<void>(run(hg::build_graph<BoundedTransportDiagnosticsGraph>(), config,
                              BASE_TIME, BASE_TIME + hg::TimeDelta{600}));

        CHECK(observed_diagnostic_events.size() == hgf::FABRIC_DIAGNOSTIC_EVENT_LIMIT);
        REQUIRE(observed_diagnostic_events.contains("diagnostics.capacity"));
        CHECK(observed_diagnostic_events.at("diagnostics.capacity").occurrences == 21);
    }
}

TEST_CASE("graph notification flow retries the retained shared revision on "
          "explicit edges")
{
    hg::stdlib::register_standard_operators();
    hgf::register_fabric_operators();
    auto config = hgf::make_memory_fabric_config("tests/subscription/graph-notification");
    observed_notification_allocations.clear();
    observed_notification_metrics.clear();

    static_cast<void>(run(hg::build_graph<GraphNotificationPublicationGraph>(), config, BASE_TIME,
                          BASE_TIME + hg::TimeDelta{20}));

    REQUIRE(observed_notification_allocations.size() == 2);
    CHECK(observed_notification_allocations[0] == observed_notification_allocations[1]);
    CHECK(observed_notification_metrics.at("transport.notification.pending") == "0");
    CHECK(observed_notification_metrics.at("transport.notification.retried") == "1");
    CHECK(observed_notification_metrics.at("transport.notification.delivered") == "1");
    const auto latest = config.objects.get(hgf::latest_key(config.prefix, "published"));
    REQUIRE(latest.has_value());
    CHECK(hgf::revision_reference_value(config.values, hgf::MetadataObjectKind::Latest, latest->data) == 1);
}

TEST_CASE("a success clears retry state when duplicate reports arrive first")
{
    hg::stdlib::register_standard_operators();
    hgf::register_fabric_operators();
    auto config = hgf::make_memory_fabric_config("tests/subscription/duplicate-retry-success");
    observed_notification_metrics.clear();

    CHECK_NOTHROW(static_cast<void>(run(hg::build_graph<DuplicateRetryThenSuccessGraph>(), config,
                                        BASE_TIME, BASE_TIME + hg::TimeDelta{20})));

    CHECK(observed_notification_metrics.at("transport.notification.delivered") == "1");
    const auto latest = config.objects.get(hgf::latest_key(config.prefix, "published"));
    REQUIRE(latest.has_value());
}
