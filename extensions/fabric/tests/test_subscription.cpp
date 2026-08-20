#include <hgraph/fabric/fabric.h>

#include "../src/impl/service_runtime.h"

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

struct CapturedLog
{
    std::shared_ptr<spdlog::sinks::ringbuffer_sink_mt> sink{
        std::make_shared<spdlog::sinks::ringbuffer_sink_mt>(16)};

    CapturedLog()
    {
        hg::log::set_logger(
            std::make_shared<spdlog::logger>("hgraph-fabric-test", sink));
    }

    ~CapturedLog() { hg::log::set_logger(nullptr); }

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
        [](void *context, std::string_view key, std::span<const std::byte> data) {
            auto &counting = *static_cast<CountingObjectContext *>(context);
            ++counting.counters->object_puts;
            return counting.store.put_immutable(key, data);
        },
        [](void *context, std::string_view key) {
            auto &counting = *static_cast<CountingObjectContext *>(context);
            ++counting.counters->object_gets;
            return counting.store.get(key);
        },
        [](void *context, std::string_view prefix, std::optional<std::string_view> start_after, std::size_t limit) {
            auto &counting = *static_cast<CountingObjectContext *>(context);
            ++counting.counters->object_lists;
            return counting.store.list(prefix, start_after, limit);
        },
        [](void *context, std::string_view key, std::optional<std::string_view> expected,
           std::span<const std::byte> desired) {
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
        [](void *context, std::string_view key, hg::Frame value, std::optional<hgps::Compression> compression) {
            auto &counting = *static_cast<CountingFrameContext *>(context);
            ++counting.counters->frame_writes;
            counting.store.write(key, std::move(value), compression);
        },
        [](void *context, std::string_view key) {
            auto &counting = *static_cast<CountingFrameContext *>(context);
            ++counting.counters->frame_reads;
            return counting.store.read(key);
        },
        [](void *context, std::string_view key) {
            auto &counting = *static_cast<CountingFrameContext *>(context);
            ++counting.counters->frame_contains;
            return counting.store.contains(key);
        },
        [](void *context) { static_cast<CountingFrameContext *>(context)->store.clear(); },
    };
    return ops;
}

[[nodiscard]] hgf::FabricConfig counting_config(const hgf::FabricConfig &base, std::shared_ptr<IoCounters> counters)
{
    hgf::FabricConfig result = base;
    result.objects = hgps::ObjectStore{std::make_shared<CountingObjectContext>(CountingObjectContext{
                                           .store = base.objects,
                                           .counters = counters,
                                       }),
                                       counting_object_ops()};
    result.frames = hgps::FrameStore{std::make_shared<CountingFrameContext>(CountingFrameContext{
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
    return hg::Frame{
        arrow::Table::Make(arrow::schema({arrow::field("value", arrow::int64())}), {std::move(array).ValueOrDie()})};
}

[[nodiscard]] std::int64_t frame_value(const hg::Frame &value)
{
    if (!value.has_value() || value.table->num_rows() != 1)
    {
        throw std::runtime_error("test Frame is not a one-row value");
    }
    const auto values = std::static_pointer_cast<arrow::Int64Array>(value.table->column(0)->chunk(0));
    return values->Value(0);
}

[[nodiscard]] hgf::DataRevisionInput seed(const hgf::FabricConfig &config, hg::Str data_id, hgf::RevisionId revision,
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
        hgf::revision_key(config.prefix, decoded.data_id, revision), hgf::encode_revision(value.view()));
    if (revision_result.status == hgps::ImmutableWriteStatus::Conflict)
    {
        throw std::runtime_error("test revision conflicted");
    }
    const auto as_of_result =
        config.objects.put_immutable(hgf::as_of_key(config.prefix, decoded.data_id, as_of),
                                     hgf::encode_revision_reference(hgf::MetadataObjectKind::AsOf, revision));
    if (as_of_result.status == hgps::ImmutableWriteStatus::Conflict)
    {
        throw std::runtime_error("test as-of index conflicted");
    }
    const std::string latest_key = hgf::latest_key(config.prefix, decoded.data_id);
    const auto current = config.objects.get(latest_key);
    const auto latest = config.objects.compare_exchange_ref(
        latest_key, current.has_value() ? std::optional<std::string_view>{current->version_token} : std::nullopt,
        hgf::encode_revision_reference(hgf::MetadataObjectKind::Latest, revision));
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

    static void eval(hg::DateTime now, hg::In<"value", hg::TS<hg::Frame>> value, hg::Scalar<"label", hg::Str> label)
    {
        observed_tagged_frames.emplace_back(now, label.value(), frame_value(value.value()));
    }
};

struct SnapshotGraph
{
    static constexpr auto name = "hgraph.fabric.test.snapshot";

    static void compose(hg::Wiring &wiring)
    {
        hgf::register_service(wiring);
        auto value =
            hgf::subscribe_data(wiring, "prices", hgf::SubscriptionMode::Snapshot, BASE_TIME + hg::TimeDelta{2});
        static_cast<void>(hg::wire<CaptureFrame>(wiring, value));
    }
};

struct ReplayGraph
{
    static constexpr auto name = "hgraph.fabric.test.replay";

    static void compose(hg::Wiring &wiring)
    {
        hgf::register_service(wiring);
        auto value = hgf::subscribe_data(wiring, "prices", hgf::SubscriptionMode::Replay);
        static_cast<void>(hg::wire<CaptureFrame>(wiring, value));
    }
};

struct EqualTimeReplayGraph
{
    static constexpr auto name = "hgraph.fabric.test.equal_time_replay";

    static void compose(hg::Wiring &wiring)
    {
        hgf::register_service(wiring);
        auto left = hgf::subscribe_data(wiring, "left", hgf::SubscriptionMode::Replay);
        auto right = hgf::subscribe_data(wiring, "right", hgf::SubscriptionMode::Replay);
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
        auto value = hgf::subscribe_data(wiring, "derived", hgf::SubscriptionMode::Replay);
        static_cast<void>(hg::wire<CaptureFrame>(wiring, value));
    }
};

struct RecursiveSnapshotGraph
{
    static constexpr auto name = "hgraph.fabric.test.recursive_snapshot";

    static void compose(hg::Wiring &wiring)
    {
        hgf::register_service(wiring);
        auto value =
            hgf::subscribe_data(wiring, "derived", hgf::SubscriptionMode::Snapshot, BASE_TIME + hg::TimeDelta{5});
        static_cast<void>(hg::wire<CaptureFrame>(wiring, value));
    }
};

struct LiveNoticeSource
{
    static constexpr auto name = "hgraph.fabric.test.live_notice";
    static constexpr bool schedule_on_start = true;

    static void eval(hg::GlobalStateView global_state, hg::NodeScheduler scheduler, hg::State<hg::Int> phase,
                     hg::Out<hg::TS<hgf::DataRevision>> out)
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
        auto value = hgf::subscribe_data(wiring, "prices", hgf::SubscriptionMode::Live);
        auto notice = hg::wire<LiveNoticeSource>(wiring);
        hgf::submit_notice(wiring, notice, hg::service::path(hgf::DEFAULT_SERVICE_PATH));
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

    static void eval(hg::NodeScheduler scheduler, hg::State<hg::Int> phase, hg::Out<hg::TS<hgf::DataRevision>> out)
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
            revision = seed(*notice_seed_config, std::move(spec.data_id), spec.revision, spec.output_version,
                            spec.as_of, std::move(spec.dependencies));
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
        auto value = hgf::subscribe_data(wiring, "prices", hgf::SubscriptionMode::Live);
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
        auto value = hgf::subscribe_data(wiring, "z-root", hgf::SubscriptionMode::Live);
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
        auto value = hgf::subscribe_data(wiring, "prices", hgf::SubscriptionMode::Live);
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
        static_cast<void>(hg::wire<CaptureLoad>(wiring, hgf::request_load(wiring, "prices", 2)));
    }
};

[[nodiscard]] hg::GraphExecutorValue run(hg::GraphBuilder graph, const hgf::FabricConfig &config, hg::DateTime start,
                                         hg::DateTime end)
{
    hgf::set_fabric_config(graph.global_state(), config);
    return hg::testing::run_graph(std::move(graph), start, end);
}
} // namespace

TEST_CASE("snapshot loads one consistent image at the requested as-of")
{
    hg::stdlib::register_standard_operators();
    hgf::register_fabric_operators();
    auto config = hgf::make_memory_fabric_config("tests/subscription/snapshot");
    static_cast<void>(seed(config, "prices", 1, 1, BASE_TIME + hg::TimeDelta{1}));
    static_cast<void>(seed(config, "prices", 2, 2, BASE_TIME + hg::TimeDelta{2}));
    static_cast<void>(seed(config, "prices", 3, 3, BASE_TIME + hg::TimeDelta{3}));
    observed_frames.clear();

    static_cast<void>(run(hg::build_graph<SnapshotGraph>(), config, BASE_TIME, BASE_TIME + hg::TimeDelta{5}));

    REQUIRE(observed_frames.size() == 1);
    CHECK(observed_frames.front().first == BASE_TIME);
    CHECK(observed_frames.front().second == 2);
}

TEST_CASE("service lifecycle logs its path once at start and stop")
{
    hg::stdlib::register_standard_operators();
    hgf::register_fabric_operators();
    auto config = hgf::make_memory_fabric_config(
        "tests/subscription/lifecycle-log");
    static_cast<void>(
        seed(config, "prices", 1, 1, BASE_TIME + hg::TimeDelta{1}));
    observed_frames.clear();
    CapturedLog captured;

    static_cast<void>(run(hg::build_graph<SnapshotGraph>(), config, BASE_TIME,
                          BASE_TIME + hg::TimeDelta{2}));

    const auto output = captured.joined();
    CHECK(output.find("hgraph.fabric service started path=fabric") !=
          std::string::npos);
    CHECK(output.find("hgraph.fabric service stopped path=fabric") !=
          std::string::npos);
}

TEST_CASE("snapshot applies its as-of bound through transitive ancestry")
{
    hg::stdlib::register_standard_operators();
    hgf::register_fabric_operators();
    auto config = hgf::make_memory_fabric_config("tests/subscription/snapshot-ancestry");
    static_cast<void>(seed(config, "base", 1, 1, BASE_TIME + hg::TimeDelta{1}));
    static_cast<void>(seed(config, "middle", 1, 1, BASE_TIME + hg::TimeDelta{2}, {{"base", 1}}));
    static_cast<void>(seed(config, "derived", 1, 1, BASE_TIME + hg::TimeDelta{3}, {{"middle", 1}}));
    static_cast<void>(seed(config, "derived", 2, 2, BASE_TIME + hg::TimeDelta{5}, {{"middle", 2}}));
    static_cast<void>(seed(config, "base", 2, 2, BASE_TIME + hg::TimeDelta{6}));
    static_cast<void>(seed(config, "middle", 2, 2, BASE_TIME + hg::TimeDelta{7}, {{"base", 2}}));
    observed_frames.clear();

    const auto graph_start = BASE_TIME + hg::TimeDelta{20};
    static_cast<void>(
        run(hg::build_graph<RecursiveSnapshotGraph>(), config, graph_start, graph_start + hg::TimeDelta{5}));

    REQUIRE(observed_frames.size() == 1);
    CHECK(observed_frames.front().first == graph_start);
    CHECK(observed_frames.front().second == 1);
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

    static_cast<void>(
        run(hg::build_graph<ReplayGraph>(), config, BASE_TIME + hg::TimeDelta{15}, BASE_TIME + hg::TimeDelta{30}));

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

    static_cast<void>(run(hg::build_graph<ReplayGraph>(), config, hg::MIN_ST, hg::MIN_ST + hg::TimeDelta{5}));

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

    static_cast<void>(run(hg::build_graph<EqualTimeReplayGraph>(), config, BASE_TIME + hg::TimeDelta{1},
                          BASE_TIME + hg::TimeDelta{20}));

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
    static_cast<void>(seed(config, "derived", 1, 1, BASE_TIME + hg::TimeDelta{10}, {{"middle", 1}}));
    static_cast<void>(seed(config, "base", 2, 2, BASE_TIME + hg::TimeDelta{15}));
    static_cast<void>(seed(config, "middle", 2, 2, BASE_TIME + hg::TimeDelta{20}, {{"base", 2}}));
    static_cast<void>(seed(config, "derived", 2, 2, BASE_TIME + hg::TimeDelta{30}, {{"middle", 2}}));
    observed_frames.clear();

    static_cast<void>(run(hg::build_graph<DynamicAncestryReplayGraph>(), config, BASE_TIME + hg::TimeDelta{1},
                          BASE_TIME + hg::TimeDelta{40}));

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

    static_cast<void>(
        run(hg::build_graph<LiveGraph>(), config, BASE_TIME + hg::TimeDelta{1}, BASE_TIME + hg::TimeDelta{10}));

    REQUIRE(observed_frames.size() == 2);
    CHECK(observed_frames[0].first == BASE_TIME + hg::TimeDelta{1});
    CHECK(observed_frames[0].second == 1);
    CHECK(observed_frames[1].first == BASE_TIME + hg::TimeDelta{6});
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

    static_cast<void>(run(hg::build_graph<CompleteNoticeGraph>(), observed_config, BASE_TIME + hg::TimeDelta{1},
                          BASE_TIME + hg::TimeDelta{10}));

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

    static_cast<void>(run(hg::build_graph<GapNoticeGraph>(), observed_config, BASE_TIME + hg::TimeDelta{1},
                          BASE_TIME + hg::TimeDelta{10}));

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

TEST_CASE("live admits cached dependency notices before durable fallback")
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

    static_cast<void>(run(hg::build_graph<CachedAncestryNoticeGraph>(), observed_config, BASE_TIME + hg::TimeDelta{1},
                          BASE_TIME + hg::TimeDelta{10}));

    REQUIRE(observed_frames.size() == 2);
    CHECK(observed_frames.back().second == 2);
    CHECK(counters->object_gets == 0);
    CHECK(counters->object_lists == 0);
    CHECK(counters->frame_reads == 1);
    notice_specs.clear();
    notice_seed_config.reset();
    notice_io_counters.reset();
}

TEST_CASE("snapshot and replay service graphs contain no push sources")
{
    hg::stdlib::register_standard_operators();
    hgf::register_fabric_operators();
    const auto snapshot = hg::build_graph<SnapshotGraph>();
    const auto replay = hg::build_graph<ReplayGraph>();
    CHECK(snapshot.root_type().schema()->push_source_nodes_end == 0);
    CHECK(replay.root_type().schema()->push_source_nodes_end == 0);
}

TEST_CASE("publish_data routes through the singleton service publication state")
{
    hg::stdlib::register_standard_operators();
    hgf::register_fabric_operators();
    auto config = hgf::make_memory_fabric_config("tests/subscription/publication");
    static_cast<void>(run(hg::build_graph<PublicationGraph>(), config, BASE_TIME, BASE_TIME + hg::TimeDelta{5}));

    const auto latest = config.objects.get(hgf::latest_key(config.prefix, "published"));
    REQUIRE(latest.has_value());
    CHECK(hgf::decode_revision_reference(hgf::MetadataObjectKind::Latest, latest->data) == 1);
    const auto revision_object = config.objects.get(hgf::revision_key(config.prefix, "published", 1));
    REQUIRE(revision_object.has_value());
    const auto revision = hgf::data_revision_input(hgf::decode_revision(revision_object->data).view());
    const auto stored = config.frames.read(hgf::data_version_key(config.prefix, "published", revision.output_version));
    REQUIRE(stored.has_value());
    CHECK(frame_value(stored) == 71);
}

TEST_CASE("request_load uses the service-owned persistence resource")
{
    hg::stdlib::register_standard_operators();
    hgf::register_fabric_operators();
    auto config = hgf::make_memory_fabric_config("tests/subscription/load");
    static_cast<void>(seed(config, "prices", 1, 2, BASE_TIME + hg::TimeDelta{1}));
    observed_frames.clear();

    static_cast<void>(run(hg::build_graph<LoadGraph>(), config, BASE_TIME, BASE_TIME + hg::TimeDelta{5}));

    REQUIRE(observed_frames.size() == 1);
    CHECK(observed_frames.front().second == 2);
}

TEST_CASE("service diagnostics expose resolver work and bounded queue usage")
{
    auto config = hgf::make_memory_fabric_config(
        "tests/subscription/runtime-diagnostics");
    static_cast<void>(
        seed(config, "prices", 1, 1, BASE_TIME + hg::TimeDelta{1}));

    hgf::detail::FabricServicePlanHandle plan{
        std::make_shared<hgf::detail::FabricServicePlan>()};
    hgf::detail::FabricServiceRuntime runtime{std::move(plan)};
    hg::GlobalContext context;
    hgf::set_fabric_config(context.state().view(), config);
    runtime.start(context.state().view());
    const auto delivery = runtime.snapshot({hgf::detail::SubscriptionSpec{
        .key = "prices",
        .data_id = "prices",
        .as_of = BASE_TIME + hg::TimeDelta{2},
    }});
    REQUIRE(delivery.has_value());

    const auto values = runtime.diagnostics();
    const std::map<hg::Str, hg::Str> diagnostics{values.begin(), values.end()};
    CHECK(diagnostics.at("lifecycle") == "running");
    CHECK(diagnostics.at("publication.queued") == "0");
    CHECK(diagnostics.at("publication.queue_limit_per_data_id") == "1024");
    CHECK(diagnostics.at("live.notice_limit_per_session") == "4096");
    CHECK(diagnostics.at("resolution.calls") == "1");
    CHECK(diagnostics.at("resolution.forests.ready") == "1");
    CHECK(std::stoull(diagnostics.at("resolution.revision_cache.misses")) >= 1);
    CHECK(std::stoull(diagnostics.at("resolution.frame_cache.misses")) >= 1);
}

TEST_CASE("service diagnostics retain typed transport and store events")
{
    auto config = hgf::make_memory_fabric_config("tests/subscription/typed-failures");
    hgf::detail::FabricServicePlanHandle plan{
        std::make_shared<hgf::detail::FabricServicePlan>()};
    hgf::detail::FabricServiceRuntime runtime{std::move(plan)};
    hg::GlobalContext context;
    hgf::set_fabric_config(context.state().view(), config);
    runtime.start(context.state().view());

    CHECK_FALSE(runtime.load("missing", 1).has_value());
    runtime.observe_transport_event(hgf::detail::TransportEventInput{
        .component = "kafka",
        .category = "disconnect",
        .message = "broker unavailable",
        .retriable = true,
    });

    const auto values = runtime.events();
    const std::map<hg::Str, hgf::FabricDiagnosticEventInput> events{values.begin(), values.end()};
    REQUIRE(events.contains("store.frame.missing"));
    CHECK(events.at("store.frame.missing").component == "store");
    CHECK_FALSE(events.at("store.frame.missing").retriable);
    CHECK_FALSE(events.at("store.frame.missing").fatal);
    REQUIRE(events.contains("kafka.disconnect"));
    CHECK(events.at("kafka.disconnect").message == "broker unavailable");
    CHECK(events.at("kafka.disconnect").retriable);
    CHECK_FALSE(events.at("kafka.disconnect").fatal);
    CHECK(events.at("kafka.disconnect").occurrences == 1);
}

TEST_CASE("stalled service queues and diagnostic paths enforce hard bounds")
{
    SECTION("publication requests")
    {
        auto config = hgf::make_memory_fabric_config("tests/subscription/publication-bound");
        hgf::detail::FabricServicePlanHandle plan{
            std::make_shared<hgf::detail::FabricServicePlan>()};
        hgf::detail::FabricServiceRuntime runtime{std::move(plan)};
        hg::GlobalContext context;
        hgf::set_fabric_config(context.state().view(), config);
        runtime.start(context.state().view());

        runtime.publish(hgf::detail::PublicationRequestInput{.data_id = "stalled"});
        for (std::size_t index = 0; index < hgf::FABRIC_PUBLICATION_QUEUE_LIMIT_PER_DATA_ID; ++index)
        {
            runtime.publish(hgf::detail::PublicationRequestInput{.data_id = "stalled"});
        }
        CHECK_THROWS_AS(runtime.publish(hgf::detail::PublicationRequestInput{.data_id = "stalled"}),
                        std::overflow_error);
        const auto values = runtime.diagnostics();
        const std::map<hg::Str, hg::Str> metrics{values.begin(), values.end()};
        CHECK(std::stoull(metrics.at("publication.queued")) ==
              hgf::FABRIC_PUBLICATION_QUEUE_LIMIT_PER_DATA_ID);
    }

    SECTION("live notices")
    {
        auto config = hgf::make_memory_fabric_config("tests/subscription/live-bound");
        hgf::detail::FabricServicePlanHandle plan{
            std::make_shared<hgf::detail::FabricServicePlan>()};
        hgf::detail::FabricServiceRuntime runtime{std::move(plan)};
        hg::GlobalContext context;
        hgf::set_fabric_config(context.state().view(), config);
        runtime.start(context.state().view());

        std::vector<hgf::DataRevisionInput> revisions;
        revisions.reserve(hgf::FABRIC_LIVE_NOTICE_LIMIT_PER_SESSION);
        for (std::size_t index = 0; index < hgf::FABRIC_LIVE_NOTICE_LIMIT_PER_SESSION; ++index)
        {
            revisions.push_back(hgf::DataRevisionInput{
                .data_id = "notice-" + std::to_string(index),
                .revision = 1,
                .output_version = 1,
                .as_of = BASE_TIME,
            });
        }
        CHECK_FALSE(runtime.live({}, std::move(revisions), BASE_TIME).has_value());
        CHECK_THROWS_AS(runtime.live({}, {hgf::DataRevisionInput{
                                              .data_id = "notice-overflow",
                                              .revision = 1,
                                              .output_version = 1,
                                              .as_of = BASE_TIME,
                                          }},
                                     BASE_TIME),
                        std::overflow_error);
        const auto values = runtime.diagnostics();
        const std::map<hg::Str, hg::Str> metrics{values.begin(), values.end()};
        CHECK(std::stoull(metrics.at("live.notices")) == hgf::FABRIC_LIVE_NOTICE_LIMIT_PER_SESSION);
    }

    SECTION("diagnostic paths")
    {
        auto config = hgf::make_memory_fabric_config("tests/subscription/diagnostic-bound");
        hgf::detail::FabricServicePlanHandle plan{
            std::make_shared<hgf::detail::FabricServicePlan>()};
        hgf::detail::FabricServiceRuntime runtime{std::move(plan)};
        hg::GlobalContext context;
        hgf::set_fabric_config(context.state().view(), config);
        runtime.start(context.state().view());

        for (std::size_t index = 0; index < hgf::FABRIC_DIAGNOSTIC_EVENT_LIMIT + 20U; ++index)
        {
            runtime.observe_transport_event(hgf::detail::TransportEventInput{
                .component = "kafka",
                .category = "category-" + std::to_string(index),
                .message = "failure",
                .retriable = true,
            });
        }
        const auto values = runtime.events();
        const std::map<hg::Str, hgf::FabricDiagnosticEventInput> events{values.begin(), values.end()};
        CHECK(events.size() == hgf::FABRIC_DIAGNOSTIC_EVENT_LIMIT);
        REQUIRE(events.contains("diagnostics.capacity"));
        CHECK(events.at("diagnostics.capacity").occurrences == 21);
    }
}

TEST_CASE("graph notification bridge retries with stable revision correlation")
{
    hgf::detail::GraphNotificationBridge bridge;
    auto notifier = bridge.notifier();
    hg::Value value = hgf::make_data_revision(hgf::DataRevisionInput{
        .data_id = "prices",
        .revision = 7,
        .output_version = 3,
        .as_of = BASE_TIME,
    });
    auto delivery = notifier.publish(hgf::RevisionNotification{"prices", hgf::encode_revision(value.view())});

    auto first = bridge.take_request();
    REQUIRE(first.has_value());
    CHECK(first->revision == 7);
    bridge.complete(hgf::detail::NotificationDeliveryInput{
        .data_id = "prices",
        .revision = 7,
        .retriable = true,
        .message = "retry",
    });
    CHECK(delivery.poll().status == hgf::NotificationDeliveryStatus::Pending);

    auto retry = bridge.take_request();
    REQUIRE(retry.has_value());
    CHECK(*retry == *first);
    bridge.complete(hgf::detail::NotificationDeliveryInput{
        .data_id = "prices",
        .revision = 7,
        .delivered = true,
    });
    CHECK(delivery.poll().status == hgf::NotificationDeliveryStatus::Delivered);
    CHECK_FALSE(bridge.request_pending());
    const auto values = bridge.diagnostics();
    const std::map<hg::Str, hg::Str> diagnostics{values.begin(), values.end()};
    CHECK(diagnostics.at("transport.notification.pending") == "0");
    CHECK(diagnostics.at("transport.notification.retried") == "1");
    CHECK(diagnostics.at("transport.notification.delivered") == "1");
}

TEST_CASE("stalled graph notification delivery queue enforces its hard bound")
{
    hgf::detail::GraphNotificationBridge bridge;
    auto notifier = bridge.notifier();
    for (std::size_t index = 0; index < hgf::FABRIC_NOTIFICATION_REQUEST_LIMIT; ++index)
    {
        const auto revision = hgf::make_data_revision(hgf::DataRevisionInput{
            .data_id = "stalled",
            .revision = static_cast<hgf::RevisionId>(index + 1U),
            .output_version = 1,
            .as_of = BASE_TIME + hg::TimeDelta{static_cast<hg::TimeDelta::rep>(index + 1U)},
        });
        static_cast<void>(notifier.publish(
            hgf::RevisionNotification{"stalled", hgf::encode_revision(revision.view())}));
    }
    const auto overflow = hgf::make_data_revision(hgf::DataRevisionInput{
        .data_id = "stalled",
        .revision = static_cast<hgf::RevisionId>(hgf::FABRIC_NOTIFICATION_REQUEST_LIMIT + 1U),
        .output_version = 1,
        .as_of = BASE_TIME + hg::TimeDelta{
                               static_cast<hg::TimeDelta::rep>(hgf::FABRIC_NOTIFICATION_REQUEST_LIMIT + 1U)},
    });
    CHECK_THROWS_AS(notifier.publish(
                        hgf::RevisionNotification{"stalled", hgf::encode_revision(overflow.view())}),
                    std::overflow_error);
    const auto values = bridge.diagnostics();
    const std::map<hg::Str, hg::Str> diagnostics{values.begin(), values.end()};
    CHECK(std::stoull(diagnostics.at("transport.notification.pending")) ==
          hgf::FABRIC_NOTIFICATION_REQUEST_LIMIT);
}
