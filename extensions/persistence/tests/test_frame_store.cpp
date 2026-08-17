// RFC 0016 — object-store frame persistence.
//
// The store is a keyed content store over memory, a local filesystem, or S3.
// These cover what the RFC commits to: key validation applied identically by
// every backend, immutable-by-default writes, both formats round-tripping a
// frame, and RFC 0001 frame metadata surviving persistence and decoding back
// to the typed value.

#include <hgraph/runtime/global_state.h>
#include <hgraph/persistence/frame_store.h>
#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/metadata/value_plan_factory.h>
#include <hgraph/persistence/recording_store.h>
#include <hgraph/types/record_replay.h>
#include <hgraph/types/static_schema.h>
#include <hgraph/types/value/value_builder.h>

#include <arrow/array.h>
#include <arrow/builder.h>
#include <arrow/table.h>
#include <arrow/util/key_value_metadata.h>

#if defined(HGRAPH_WITH_PARQUET)
#include <parquet/file_reader.h>
#endif

#include <catch2/catch_test_macros.hpp>

namespace test_detail
{
    /** The moved tests' store shims: the process-global store forms died at
        RFC 0025 checkpoint 4, so the no-state spellings resolve against the
        ambient GlobalContext state the test established. */
    [[nodiscard]] inline hgraph::GlobalStateView active_state()
    {
        return hgraph::GlobalContext::active_state()->view();
    }
    inline void store_write(std::string_view key, hgraph::Frame frame)
    {
        hgraph::persistence::store_write(active_state(), key, std::move(frame));
    }
    inline void store_write(hgraph::GlobalStateView state, std::string_view key,
                            hgraph::Frame frame)
    {
        hgraph::persistence::store_write(state, key, std::move(frame));
    }
    [[nodiscard]] inline hgraph::Frame store_read(std::string_view key)
    {
        return hgraph::persistence::store_read(active_state(), key);
    }
    [[nodiscard]] inline hgraph::Frame store_read(hgraph::GlobalStateView state,
                                                  std::string_view key)
    {
        return hgraph::persistence::store_read(state, key);
    }
    [[nodiscard]] inline bool store_contains(std::string_view key)
    {
        return hgraph::persistence::store_contains(active_state(), key);
    }
    [[nodiscard]] inline bool store_contains(hgraph::GlobalStateView state, std::string_view key)
    {
        return hgraph::persistence::store_contains(state, key);
    }
}  // namespace test_detail

#include <catch2/matchers/catch_matchers_string.hpp>

#include <cstdlib>
#include <filesystem>
#include <string>
#include <type_traits>

using namespace hgraph;
using namespace hgraph::persistence::store;

namespace
{
    using FrameStoreMetaDetails =
        Bundle<"tests.frame_store::Details", Field<"desk", Str>>;
    using FrameStoreMeta =
        Bundle<"tests.frame_store::Metadata", Field<"revision", Int>,
               Field<"details", FrameStoreMetaDetails>>;

    [[nodiscard]] Frame make_frame(std::int64_t first = 1)
    {
        arrow::Int64Builder builder;
        REQUIRE(builder.Append(first).ok());
        REQUIRE(builder.Append(first + 1).ok());
        std::shared_ptr<arrow::Array> array;
        REQUIRE(builder.Finish(&array).ok());
        auto schema = arrow::schema({arrow::field("value", arrow::int64())});
        return Frame{arrow::Table::Make(schema, {array})};
    }

    [[nodiscard]] Value make_metadata()
    {
        BundleBuilder details{ValuePlanFactory::instance().type_for(
            scalar_descriptor<FrameStoreMetaDetails>::value_meta())};
        details.set(0, Value{Str{"systematic"}});

        BundleBuilder metadata{ValuePlanFactory::instance().type_for(
            scalar_descriptor<FrameStoreMeta>::value_meta())};
        metadata.set(0, Value{Int{7}});
        metadata.set(1, details.build());
        return metadata.build();
    }

    [[nodiscard]] Frame make_metadata_frame()
    {
        return with_frame_metadata(make_frame(), make_metadata());
    }

    void check_frame_round_trip(const FrameStore &store, std::string_view key,
                                const Frame &expected)
    {
        store.write(key, expected);
        const Frame actual = store.read(key);
        REQUIRE(actual.has_value());
        CHECK(actual.table->Equals(*expected.table));
        CHECK(actual.table->schema()->Equals(*expected.table->schema(), true));
    }

    void check_typed_metadata(const Frame &frame)
    {
        REQUIRE(frame.has_metadata());
        CHECK(frame_metadata(frame, scalar_descriptor<FrameStoreMeta>::value_meta()) ==
              make_metadata());
    }

    /** A directory that removes itself, so a failing test leaves no litter. */
    class TempDir
    {
      public:
        explicit TempDir(const std::string &name)
            : path_(std::filesystem::temp_directory_path() / ("hgraph_frame_store_" + name))
        {
            std::filesystem::remove_all(path_);
        }
        ~TempDir() { std::filesystem::remove_all(path_); }
        [[nodiscard]] std::string string() const { return path_.string(); }

      private:
        std::filesystem::path path_;
    };

    [[nodiscard]] FrameStoreConfig local_config(const TempDir &dir, Format format)
    {
        FrameStoreConfig config;
        config.location = LocalLocation{dir.string()};
        config.format = format;
        return config;
    }

    struct ProbeStore
    {
        Frame frame{};
        int   writes{0};
    };

    [[nodiscard]] const FrameStoreOps &probe_store_ops()
    {
        static const FrameStoreOps ops{
            [](void *context, std::string_view, Frame frame, std::optional<Compression>) {
                auto &store = *static_cast<ProbeStore *>(context);
                store.frame = std::move(frame);
                ++store.writes;
            },
            [](void *context, std::string_view) {
                return static_cast<ProbeStore *>(context)->frame;
            },
            [](void *context, std::string_view) {
                return static_cast<ProbeStore *>(context)->frame.has_value();
            },
            [](void *context) { static_cast<ProbeStore *>(context)->frame = Frame{}; },
        };
        return ops;
    }
}  // namespace

TEST_CASE("frame store: the public contract is an owning type-erased handle")
{
    static_assert(!std::is_polymorphic_v<FrameStore>);

    auto                      context = std::make_shared<ProbeStore>();
    const std::weak_ptr<void> lifetime = context;
    FrameStore                store{context, probe_store_ops()};
    FrameStore                copy = store;
    CHECK_FALSE(store.supports_segmented_recordings());
    context.reset();

    copy.write("value", make_frame());
    CHECK(copy.contains("value"));
    CHECK(copy.read("value").table->num_rows() == 2);

    FrameStore moved = std::move(store);
    CHECK_FALSE(store);
    CHECK_FALSE(store.contains("value"));
    CHECK_FALSE(store.read("value").has_value());
    CHECK_NOTHROW(store.clear());
    CHECK_THROWS_AS(store.write("value", make_frame()), std::logic_error);

    CHECK(moved.contains("value"));
    copy.reset();
    CHECK_FALSE(lifetime.expired());
    moved.reset();
    CHECK(lifetime.expired());

    CHECK_THROWS_AS((FrameStore{std::shared_ptr<void>{}, probe_store_ops()}),
                    std::invalid_argument);
    const FrameStoreOps incomplete{};
    CHECK_THROWS_AS((FrameStore{std::make_shared<ProbeStore>(), incomplete}),
                    std::invalid_argument);
}

TEST_CASE("frame store: key validation is identical in every backend")
{
    // The point of validating in memory too: a key that would fail against S3
    // fails in a unit test rather than at deployment.
    const std::vector<std::string_view> rejected{
        "", "/leading", "trailing/", "double//segment", "..", "a/../b", "a/./b", "back\\slash",
    };
    for (const auto key : rejected)
    {
        CHECK(validate_key(key).has_value());
    }

    const std::vector<std::string_view> accepted{
        "calc.lhs",
        "run/2026-08-10/prices",
        "a-b_c.parquet",
        "nested/deeply/held/key",
    };
    for (const auto key : accepted)
    {
        CHECK_FALSE(validate_key(key).has_value());
    }

    TempDir dir{"validation"};
    auto    memory = make_frame_store(FrameStoreConfig{});
    auto    local = make_frame_store(local_config(dir, Format::ArrowIpc));
    for (auto &store : {memory, local})
    {
        CHECK_THROWS_AS(store.write("../escape", make_frame()), std::invalid_argument);
        CHECK_THROWS_AS(store.contains("/absolute"), std::invalid_argument);
    }
}

TEST_CASE("frame store: memory backend round-trips and honours immutability")
{
    auto store = make_frame_store(FrameStoreConfig{});
    CHECK(store.supports_segmented_recordings());

    CHECK_FALSE(store.contains("prices"));
    CHECK_FALSE(store.read("prices").has_value());  // absent reads as an empty Frame

    store.write("prices", make_frame());
    CHECK(store.contains("prices"));
    CHECK(store.read("prices").table->num_rows() == 2);

    // Immutable is the default (RFC 0016): a second write is rejected rather
    // than silently discarding the first.
    CHECK_THROWS(store.write("prices", make_frame(10)));

    FrameStoreConfig overwritable;
    overwritable.immutable = false;
    auto mutable_store = make_frame_store(overwritable);
    mutable_store.write("prices", make_frame());
    CHECK_NOTHROW(mutable_store.write("prices", make_frame(10)));
    CHECK(mutable_store.read("prices").table->column(0)->GetScalar(0).ValueOrDie()->ToString() ==
          "10");

    store.clear();
    CHECK_FALSE(store.contains("prices"));

    const Frame metadata = make_metadata_frame();
    check_frame_round_trip(store, "metadata", metadata);
    check_typed_metadata(store.read("metadata"));
}

TEST_CASE("frame store: a local store persists frames as files")
{
    TempDir dir{"local"};
    auto    store = make_frame_store(local_config(dir, Format::ArrowIpc));
    CHECK(store.supports_segmented_recordings());

    store.write("run/2026-08-10/prices", make_frame());
    CHECK_THROWS(store.write("run/2026-08-10/prices", make_frame(10)));

    // Keys nest as directories - the reason the RFC chose transparent paths.
    CHECK(std::filesystem::exists(std::filesystem::path{dir.string()} / "run" / "2026-08-10" /
                                  "prices"));

    CHECK(store.contains("run/2026-08-10/prices"));
    CHECK(store.read("run/2026-08-10/prices").table->num_rows() == 2);
    CHECK_FALSE(store.contains("run/2026-08-10/absent"));

    // A store built over the same root sees what the first one wrote: the
    // point of persisting at all.
    auto reopened = make_frame_store(local_config(dir, Format::ArrowIpc));
    CHECK(reopened.contains("run/2026-08-10/prices"));

    // Publication uses an atomic rename from a sibling staging file.  A
    // completed write must not leave implementation files visible beside the
    // immutable key.
    for (const auto &entry : std::filesystem::recursive_directory_iterator(dir.string()))
    {
        CHECK(entry.path().filename().string().find(".hgraph-tmp-") == std::string::npos);
    }
}

TEST_CASE("frame store: both formats round-trip a frame")
{
    TempDir dir{"formats"};

    const Frame expected = make_metadata_frame();
    auto        ipc = make_frame_store(local_config(dir, Format::ArrowIpc));
    check_frame_round_trip(ipc, "ipc", expected);
    check_typed_metadata(ipc.read("ipc"));

    if (parquet_available())
    {
        auto parquet = make_frame_store(local_config(dir, Format::Parquet));
        check_frame_round_trip(parquet, "parquet", expected);
        check_typed_metadata(parquet.read("parquet"));
    }
    else
    {
        FrameStoreConfig config = local_config(dir, Format::Parquet);
        CHECK_THROWS(make_frame_store(config));  // loud, not a silent fallback
    }
}

TEST_CASE("frame store: RFC 0001 frame metadata survives persistence")
{
    // The property the RFC leans on: a stored frame answers "what produced
    // this?" on its own, through the object rather than through the store.
    TempDir dir{"metadata"};

    for (const auto format : {Format::ArrowIpc, Format::Parquet})
    {
        if (format == Format::Parquet && !parquet_available())
        {
            continue;
        }

        auto frame = make_frame();
        auto keyed = frame.table->schema()->WithMetadata(arrow::key_value_metadata(
            {std::string{frame_metadata_version_key}, "hgraph.metadata.field.dataset",
             "hgraph.metadata.field.universe"},
            {"1", "eod_prices", R"(["CL", "NG"])"}));
        frame.table = frame.table->ReplaceSchemaMetadata(keyed->metadata());
        REQUIRE(frame.has_metadata());

        auto store = make_frame_store(local_config(dir, format));
        store.write("provenance", std::move(frame));

        const auto back = store.read("provenance");
        REQUIRE(back.has_value());
        CHECK(back.has_metadata());
        const auto metadata = back.table->schema()->metadata();
        REQUIRE(metadata != nullptr);
        CHECK(metadata->Get("hgraph.metadata.field.dataset").ValueOr("") == "eod_prices");
        CHECK(metadata->Get("hgraph.metadata.field.universe").ValueOr("") == R"(["CL", "NG"])");

        store.clear();
    }
}

#if defined(HGRAPH_WITH_PARQUET)
TEST_CASE("frame store: a per-write compression override wins over the store default")
{
    TempDir          dir{"compression"};
    FrameStoreConfig config = local_config(dir, Format::Parquet);
    config.compression = Compression::None;
    auto store = make_frame_store(config);

    store.write("default", make_frame());
    store.write("override", make_frame(), Compression::Zstd);

    const auto default_reader = parquet::ParquetFileReader::OpenFile(
        (std::filesystem::path{dir.string()} / "default").string());
    const auto override_reader = parquet::ParquetFileReader::OpenFile(
        (std::filesystem::path{dir.string()} / "override").string());
    REQUIRE(default_reader->metadata()->num_row_groups() == 1);
    REQUIRE(override_reader->metadata()->num_row_groups() == 1);
    CHECK(default_reader->metadata()->RowGroup(0)->ColumnChunk(0)->compression() ==
          parquet::Compression::UNCOMPRESSED);
    CHECK(override_reader->metadata()->RowGroup(0)->ColumnChunk(0)->compression() ==
          parquet::Compression::ZSTD);
}
#endif

TEST_CASE("frame store: GlobalState scopes independent stores and owns their lifetime")
{
    auto                      first_context = std::make_shared<ProbeStore>();
    auto                      second_context = std::make_shared<ProbeStore>();
    const std::weak_ptr<void> first_lifetime = first_context;
    const std::weak_ptr<void> second_lifetime = second_context;

    {
        GlobalState first_state;
        GlobalState second_state;
        persistence::set_frame_store(first_state.view(),
                                       FrameStore{first_context, probe_store_ops()});
        persistence::set_frame_store(second_state.view(),
                                       FrameStore{second_context, probe_store_ops()});
        first_context.reset();
        second_context.reset();

        test_detail::store_write(first_state.view(), "value", make_frame(10));
        test_detail::store_write(second_state.view(), "value", make_frame(20));
        CHECK(test_detail::store_read(first_state.view(), "value")
                  .table->column(0)->GetScalar(0).ValueOrDie()->ToString() == "10");
        CHECK(test_detail::store_read(second_state.view(), "value")
                  .table->column(0)->GetScalar(0).ValueOrDie()->ToString() == "20");
        CHECK_FALSE(first_lifetime.expired());
        CHECK_FALSE(second_lifetime.expired());
    }

    CHECK(first_lifetime.expired());
    CHECK(second_lifetime.expired());
}

TEST_CASE("frame store: an unbuildable configuration fails rather than degrading")
{
    // A store that quietly fell back to memory would turn a deployment error
    // into output that looks wrong much later.
    FrameStoreConfig no_root;
    no_root.location = LocalLocation{""};
    CHECK_THROWS_AS(make_frame_store(no_root), std::invalid_argument);

    FrameStoreConfig no_bucket;
    no_bucket.location = S3Location{};
    CHECK_THROWS(make_frame_store(no_bucket));

#if defined(HGRAPH_WITH_S3)
    FrameStoreConfig named_profile;
    S3Location      profile_location;
    profile_location.bucket = "unused";
    profile_location.credentials.source = Credentials::Profile{"research"};
    named_profile.location = std::move(profile_location);
    CHECK_THROWS_WITH(make_frame_store(named_profile),
                      Catch::Matchers::ContainsSubstring("set AWS_PROFILE"));
    finalize_s3();
#endif
}

// The S3 backend runs against any S3-compatible endpoint, so it needs no cloud
// account: point HGRAPH_S3_TEST_ENDPOINT at a local MinIO (or LocalStack) and
// this exercises the real Arrow S3 filesystem. Without it the test reports as
// skipped rather than silently passing, so a green run without S3 coverage is
// visible as such.
//
//   docker run -d --name hgraph-minio -p 9010:9000
//     -e MINIO_ROOT_USER=hgraphtest -e MINIO_ROOT_PASSWORD=hgraphtest123
//     quay.io/minio/minio:latest server /data
//   export HGRAPH_S3_TEST_ENDPOINT=http://127.0.0.1:9010
//   export HGRAPH_S3_TEST_BUCKET=hgraph-test
//   export AWS_ACCESS_KEY_ID=hgraphtest AWS_SECRET_ACCESS_KEY=hgraphtest123
//
// Hidden by default ([.]): it needs an endpoint, so it is opt-in rather than a
// failure for everyone else, and a Catch2 skip reports as a CTest failure
// because catch_discover_tests does not set SKIP_RETURN_CODE. Run it with:
//     ./hgraph_unit_tests "[s3]"
TEST_CASE("frame store: an S3 store round-trips against a local endpoint", "[.s3]")
{
    const char *endpoint = std::getenv("HGRAPH_S3_TEST_ENDPOINT");
    if (endpoint == nullptr)
    {
        SKIP("set HGRAPH_S3_TEST_ENDPOINT to run the S3 backend against a local "
             "endpoint");
    }

    const char *bucket_name = std::getenv("HGRAPH_S3_TEST_BUCKET");
    const char *key_id = std::getenv("AWS_ACCESS_KEY_ID");
    const char *secret = std::getenv("AWS_SECRET_ACCESS_KEY");

    S3Location location;
    location.bucket = bucket_name != nullptr ? bucket_name : "hgraph-test";
    location.prefix = "frame-store";
    location.region = "us-east-1";
    location.endpoint_override = endpoint;
    if (key_id != nullptr && secret != nullptr)
    {
        location.credentials.source = Credentials::Explicit{key_id, secret, {}};
    }

    const Frame expected = make_metadata_frame();
    for (const auto format : {Format::ArrowIpc, Format::Parquet})
    {
        if (format == Format::Parquet && !parquet_available())
        {
            continue;
        }

        FrameStoreConfig config;
        config.location = location;
        config.format = format;

        auto store = make_frame_store(config);
        CHECK(store.supports_segmented_recordings());
        const std::string_view key =
            format == Format::ArrowIpc ? "run/ipc" : "run/parquet";
        check_frame_round_trip(store, key, expected);
        check_typed_metadata(store.read(key));
        CHECK_FALSE(store.contains("run/absent"));
        CHECK_THROWS_AS(store.contains("../invalid"), std::invalid_argument);

        // Immutability holds against a real object store, where an overwrite
        // would usually destroy the previous version outright.
        CHECK_THROWS(store.write(key, make_frame(10)));
        store.reset();
    }

    FrameStoreConfig cleanup_config;
    cleanup_config.location = location;
    auto cleanup = make_frame_store(cleanup_config);
    cleanup.clear();
    cleanup.reset();
    // The application owns S3 shutdown; see finalize_s3's contract.
    finalize_s3();
}
