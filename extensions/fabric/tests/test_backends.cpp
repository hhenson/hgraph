#include <hgraph/fabric/fabric.h>

#include <hgraph/persistence/frame_store.h>
#include <hgraph/persistence/object_store.h>
#include <hgraph/persistence/store_location.h>
#include <hgraph/util/environment.h>

#include <arrow/array.h>
#include <arrow/builder.h>
#include <arrow/table.h>

#include <catch2/catch_test_macros.hpp>

#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#if !defined(_WIN32)
#include <sys/wait.h>
#include <unistd.h>
#endif

namespace
{
namespace hg = hgraph;
namespace hgf = hgraph::fabric;
namespace hgps = hgraph::persistence::store;

constexpr hg::DateTime BASE_TIME{hg::TimeDelta{1'800'000'000'000'000}};

class TempFabricStore
{
  public:
    explicit TempFabricStore(std::string_view name)
        : root_(std::filesystem::temp_directory_path() /
                ("hgraph_fabric_" + std::string{name} + "_" +
                 std::to_string(std::chrono::steady_clock::now()
                                    .time_since_epoch()
                                    .count())))
    {
        std::filesystem::create_directories(root_);
    }

    TempFabricStore(const TempFabricStore &) = delete;
    TempFabricStore &operator=(const TempFabricStore &) = delete;

    ~TempFabricStore()
    {
        std::error_code ignored;
        std::filesystem::remove_all(root_, ignored);
    }

    [[nodiscard]] std::string objects() const
    {
        return (root_ / "objects").string();
    }

    [[nodiscard]] std::string frames() const
    {
        return (root_ / "frames").string();
    }

  private:
    std::filesystem::path root_;
};

[[nodiscard]] hg::Frame frame(std::int64_t value)
{
    arrow::Int64Builder builder;
    if (!builder.Append(value).ok())
    {
        throw std::runtime_error("failed to append Fabric backend test value");
    }
    auto result = builder.Finish();
    if (!result.ok())
    {
        throw std::runtime_error("failed to finish Fabric backend test value");
    }
    return hg::Frame{arrow::Table::Make(
        arrow::schema({arrow::field("value", arrow::int64())}),
        {std::move(result).ValueOrDie()})};
}

[[nodiscard]] std::int64_t frame_value(const hg::Frame &value)
{
    if (!value.has_value() || value.table->num_rows() != 1)
    {
        throw std::runtime_error("Fabric backend test Frame is not one row");
    }
    const auto values = std::static_pointer_cast<arrow::Int64Array>(
        value.table->column(0)->chunk(0));
    return values->Value(0);
}

[[nodiscard]] hgf::FabricConfig config_for(
    hgps::Location object_location, hgps::Location frame_location,
    hgps::Format format, std::string prefix = "fabric")
{
    auto config = hgf::make_memory_fabric_config(std::move(prefix));
    config.objects = hgps::make_object_store(
        hgps::ObjectStoreConfig{std::move(object_location)});
    config.frames = hgps::make_frame_store(hgps::FrameStoreConfig{
        .location = std::move(frame_location),
        .format = format,
    });
    return config;
}

[[nodiscard]] hgf::FabricConfig local_config(
    const TempFabricStore &store, hgps::Format format)
{
    return config_for(hgps::LocalLocation{store.objects()},
                      hgps::LocalLocation{store.frames()}, format);
}

[[nodiscard]] hgf::PublicationState drive(
    hgf::PublisherStateMachine &publisher)
{
    for (int step = 0;
         step < 16 && !hgf::publication_terminal(publisher.state()); ++step)
    {
        publisher.advance();
    }
    return publisher.state();
}

[[nodiscard]] hgf::DataRevisionInput publish(
    const hgf::FabricConfig &config, std::string data_id, std::int64_t value,
    hg::DateTime now,
    std::vector<hgf::DataDependencyInput> dependencies = {})
{
    hgf::PublisherStateMachine publisher{config, std::move(data_id)};
    publisher.begin(hgf::PublicationInput{
        .output = frame(value),
        .dependencies = std::move(dependencies),
        .system_time = now,
    });
    if (drive(publisher) != hgf::PublicationState::Published)
    {
        throw std::runtime_error("Fabric backend test publication did not win");
    }
    return *publisher.accepted_revision();
}

void check_backend_round_trip(const hgf::FabricConfig &writer,
                              hgf::FabricConfig reader)
{
    const auto base = publish(writer, "base", 41, BASE_TIME);
    const auto derived = publish(
        writer, "derived", 42, BASE_TIME + hg::TimeDelta{1'000},
        {{"base", base.output_version}});

    hgf::ConsistencyResolver resolver{std::move(reader)};
    const auto result = resolver.resolve_forest({"derived"});
    REQUIRE(result.status == hgf::ResolutionStatus::Ready);
    REQUIRE(result.cut.has_value());
    REQUIRE(result.cut->revisions.size() == 2);
    CHECK(result.cut->revisions[0].data_id == "base");
    CHECK(result.cut->revisions[0].output_version == base.output_version);
    CHECK(result.cut->revisions[1].data_id == "derived");
    CHECK(result.cut->revisions[1].output_version == derived.output_version);
    REQUIRE(result.changed_roots.size() == 1);
    CHECK(frame_value(result.changed_roots.front().frame) == 42);
}

[[nodiscard]] hgps::S3Location s3_location(std::string prefix)
{
    const auto endpoint = hg::environment_variable("HGRAPH_S3_TEST_ENDPOINT");
    if (!endpoint)
    {
        throw std::logic_error("missing HGRAPH_S3_TEST_ENDPOINT");
    }
    hgps::S3Location location;
    location.bucket =
        hg::environment_variable("HGRAPH_S3_TEST_BUCKET").value_or("hgraph-test");
    location.prefix = std::move(prefix);
    location.region = "us-east-1";
    location.endpoint_override = *endpoint;
    if (const auto key = hg::environment_variable("AWS_ACCESS_KEY_ID"))
    {
        const auto secret = hg::environment_variable("AWS_SECRET_ACCESS_KEY");
        if (!secret)
        {
            throw std::logic_error("missing AWS_SECRET_ACCESS_KEY");
        }
        location.credentials.source =
            hgps::Credentials::Explicit{*key, *secret, {}};
    }
    return location;
}
}  // namespace

TEST_CASE("Fabric behavior is identical over local Arrow IPC and Parquet")
{
    for (const auto format : {hgps::Format::ArrowIpc, hgps::Format::Parquet})
    {
        if (format == hgps::Format::Parquet && !hgps::parquet_available())
        {
            continue;
        }
        TempFabricStore store{
            format == hgps::Format::ArrowIpc ? "local_ipc" : "local_parquet"};
        auto writer = local_config(store, format);
        check_backend_round_trip(writer, local_config(store, format));
    }
}

TEST_CASE("Fabric behavior is identical over S3 Arrow IPC and Parquet", "[.s3]")
{
    if (!hg::environment_variable("HGRAPH_S3_TEST_ENDPOINT"))
    {
        SKIP("set HGRAPH_S3_TEST_ENDPOINT to run the Fabric S3 contract");
    }
    const auto unique = std::to_string(
        std::chrono::steady_clock::now().time_since_epoch().count());
    for (const auto format : {hgps::Format::ArrowIpc, hgps::Format::Parquet})
    {
        if (format == hgps::Format::Parquet && !hgps::parquet_available())
        {
            continue;
        }
        const std::string format_name =
            format == hgps::Format::ArrowIpc ? "ipc" : "parquet";
        auto writer = config_for(
            s3_location("/fabric-cp7/" + unique + "/" + format_name +
                        "/objects/"),
            s3_location("/fabric-cp7/" + unique + "/" + format_name +
                        "/frames/"),
            format);
        writer.objects.clear();
        writer.frames.clear();
        auto reader = config_for(
            s3_location("/fabric-cp7/" + unique + "/" + format_name +
                        "/objects/"),
            s3_location("/fabric-cp7/" + unique + "/" + format_name +
                        "/frames/"),
            format);
        check_backend_round_trip(writer, std::move(reader));
        writer.objects.clear();
        writer.frames.clear();
        writer.objects.reset();
        writer.frames.reset();
    }
}

#if !defined(_WIN32)
TEST_CASE("local Fabric publication has one accepted winner across processes")
{
    TempFabricStore store{"process_publication_race"};
    int ready_pipe[2]{};
    int start_pipe[2]{};
    REQUIRE(::pipe(ready_pipe) == 0);
    REQUIRE(::pipe(start_pipe) == 0);

    std::vector<pid_t> children;
    for (int index = 0; index < 2; ++index)
    {
        const pid_t child = ::fork();
        REQUIRE(child >= 0);
        if (child == 0)
        {
            try
            {
                ::close(ready_pipe[0]);
                ::close(start_pipe[1]);
                auto config = local_config(store, hgps::Format::ArrowIpc);
                hgf::PublisherStateMachine publisher{config, "race"};
                publisher.begin(hgf::PublicationInput{
                    .output = frame(100 + index),
                    .system_time = BASE_TIME + hg::TimeDelta{index * 1'000},
                });
                if (publisher.advance() != hgf::PublicationState::FrameDurable)
                {
                    ::_exit(3);
                }
                const char ready{'r'};
                if (::write(ready_pipe[1], &ready, 1) != 1)
                {
                    ::_exit(3);
                }
                char start{};
                if (::read(start_pipe[0], &start, 1) != 1)
                {
                    ::_exit(3);
                }
                const auto state = drive(publisher);
                ::_exit(state == hgf::PublicationState::Published
                            ? 0
                            : state == hgf::PublicationState::LostRace ? 1 : 3);
            }
            catch (...)
            {
                ::_exit(3);
            }
        }
        children.push_back(child);
    }

    ::close(ready_pipe[1]);
    ::close(start_pipe[0]);
    char ready{};
    REQUIRE(::read(ready_pipe[0], &ready, 1) == 1);
    REQUIRE(::read(ready_pipe[0], &ready, 1) == 1);
    REQUIRE(::write(start_pipe[1], "s", 1) == 1);
    REQUIRE(::write(start_pipe[1], "s", 1) == 1);
    ::close(ready_pipe[0]);
    ::close(start_pipe[1]);

    int published{};
    int lost_race{};
    for (const auto child : children)
    {
        int status{};
        REQUIRE(::waitpid(child, &status, 0) == child);
        REQUIRE(WIFEXITED(status));
        REQUIRE(WEXITSTATUS(status) != 3);
        published += WEXITSTATUS(status) == 0 ? 1 : 0;
        lost_race += WEXITSTATUS(status) == 1 ? 1 : 0;
    }
    CHECK(published == 1);
    CHECK(lost_race == 1);

    auto config = local_config(store, hgps::Format::ArrowIpc);
    const auto latest = config.objects.get(hgf::latest_key(config.prefix, "race"));
    REQUIRE(latest.has_value());
    CHECK(hgf::revision_reference_value(config.reference_codec, hgf::MetadataObjectKind::Latest, latest->data) == 1);
    const auto stored = config.objects.get(
        hgf::revision_key(config.prefix, "race", 1));
    REQUIRE(stored.has_value());
    const auto winner = hgf::data_revision_input(
        config.values.decode(hgf::data_revision_meta(), stored->data).view());
    CHECK(config.frames.contains(hgf::data_version_key(
        config.prefix, "race", winner.output_version)));
}
#endif
