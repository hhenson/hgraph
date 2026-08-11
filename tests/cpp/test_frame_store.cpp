// RFC 0016 — object-store frame persistence.
//
// The store is a keyed content store over memory, a local filesystem, or S3.
// These cover what the RFC commits to: key validation applied identically by
// every backend, immutable-by-default writes, both formats round-tripping a
// frame, and RFC 0001 frame metadata surviving persistence and decoding back
// to the typed value.

#include <hgraph/types/frame_store.h>
#include <hgraph/types/metadata/type_registry.h>

#include <arrow/table.h>
#include <arrow/builder.h>
#include <arrow/array.h>
#include <arrow/util/key_value_metadata.h>

#include <catch2/catch_test_macros.hpp>

#include <cstdlib>
#include <filesystem>
#include <string>

using namespace hgraph;
using namespace hgraph::store;

namespace
{
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
        config.format   = format;
        return config;
    }
}  // namespace

TEST_CASE("frame store: key validation is identical in every backend")
{
    // The point of validating in memory too: a key that would fail against S3
    // fails in a unit test rather than at deployment.
    const std::vector<std::string_view> rejected{
        "", "/leading", "trailing/", "double//segment", "..", "a/../b", "a/./b", "back\\slash",
    };
    for (const auto key : rejected) { CHECK(validate_key(key).has_value()); }

    const std::vector<std::string_view> accepted{
        "calc.lhs", "run/2026-08-10/prices", "a-b_c.parquet", "nested/deeply/held/key",
    };
    for (const auto key : accepted) { CHECK_FALSE(validate_key(key).has_value()); }

    TempDir dir{"validation"};
    auto    memory = make_frame_store(FrameStoreConfig{});
    auto    local  = make_frame_store(local_config(dir, Format::ArrowIpc));
    for (auto &store : {memory, local})
    {
        CHECK_THROWS_AS(store->write("../escape", make_frame()), std::invalid_argument);
        CHECK_THROWS_AS(store->contains("/absolute"), std::invalid_argument);
    }
}

TEST_CASE("frame store: memory backend round-trips and honours immutability")
{
    auto store = make_frame_store(FrameStoreConfig{});

    CHECK_FALSE(store->contains("prices"));
    CHECK_FALSE(store->read("prices").has_value());  // absent reads as an empty Frame

    store->write("prices", make_frame());
    CHECK(store->contains("prices"));
    CHECK(store->read("prices").table->num_rows() == 2);

    // Immutable is the default (RFC 0016): a second write is rejected rather
    // than silently discarding the first.
    CHECK_THROWS(store->write("prices", make_frame(10)));

    FrameStoreConfig overwritable;
    overwritable.immutable = false;
    auto mutable_store     = make_frame_store(overwritable);
    mutable_store->write("prices", make_frame());
    CHECK_NOTHROW(mutable_store->write("prices", make_frame(10)));
    CHECK(mutable_store->read("prices").table->column(0)->GetScalar(0).ValueOrDie()->ToString() == "10");

    store->clear();
    CHECK_FALSE(store->contains("prices"));
}

TEST_CASE("frame store: a local store persists frames as files")
{
    TempDir dir{"local"};
    auto    store = make_frame_store(local_config(dir, Format::ArrowIpc));

    store->write("run/2026-08-10/prices", make_frame());

    // Keys nest as directories - the reason the RFC chose transparent paths.
    CHECK(std::filesystem::exists(std::filesystem::path{dir.string()} / "run" / "2026-08-10" / "prices"));

    CHECK(store->contains("run/2026-08-10/prices"));
    CHECK(store->read("run/2026-08-10/prices").table->num_rows() == 2);
    CHECK_FALSE(store->contains("run/2026-08-10/absent"));

    // A store built over the same root sees what the first one wrote: the
    // point of persisting at all.
    auto reopened = make_frame_store(local_config(dir, Format::ArrowIpc));
    CHECK(reopened->contains("run/2026-08-10/prices"));
}

TEST_CASE("frame store: both formats round-trip a frame")
{
    TempDir dir{"formats"};

    auto ipc = make_frame_store(local_config(dir, Format::ArrowIpc));
    ipc->write("ipc", make_frame());
    CHECK(ipc->read("ipc").table->num_rows() == 2);

    if (parquet_available())
    {
        auto parquet = make_frame_store(local_config(dir, Format::Parquet));
        parquet->write("parquet", make_frame());
        CHECK(parquet->read("parquet").table->num_rows() == 2);
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
        if (format == Format::Parquet && !parquet_available()) { continue; }

        auto frame = make_frame();
        auto keyed = frame.table->schema()->WithMetadata(arrow::key_value_metadata(
            {std::string{frame_metadata_version_key}, "hgraph.metadata.field.dataset",
             "hgraph.metadata.field.universe"},
            {"1", "eod_prices", R"(["CL", "NG"])"}));
        frame.table = frame.table->ReplaceSchemaMetadata(keyed->metadata());
        REQUIRE(frame.has_metadata());

        auto store = make_frame_store(local_config(dir, format));
        store->write("provenance", std::move(frame));

        const auto back = store->read("provenance");
        REQUIRE(back.has_value());
        CHECK(back.has_metadata());
        const auto metadata = back.table->schema()->metadata();
        REQUIRE(metadata != nullptr);
        CHECK(metadata->Get("hgraph.metadata.field.dataset").ValueOr("") == "eod_prices");
        CHECK(metadata->Get("hgraph.metadata.field.universe").ValueOr("") == R"(["CL", "NG"])");

        store->clear();
    }
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
        SKIP("set HGRAPH_S3_TEST_ENDPOINT to run the S3 backend against a local endpoint");
    }

    const char *bucket_name = std::getenv("HGRAPH_S3_TEST_BUCKET");
    const char *key_id      = std::getenv("AWS_ACCESS_KEY_ID");
    const char *secret      = std::getenv("AWS_SECRET_ACCESS_KEY");

    S3Location location;
    location.bucket            = bucket_name != nullptr ? bucket_name : "hgraph-test";
    location.prefix            = "frame-store";
    location.region            = "us-east-1";
    location.endpoint_override = endpoint;
    if (key_id != nullptr && secret != nullptr)
    {
        location.credentials.source = Credentials::Explicit{key_id, secret, {}};
    }

    FrameStoreConfig config;
    config.location = location;
    config.format   = Format::Parquet;

    auto store = make_frame_store(config);

    store->write("run/prices", make_frame());
    CHECK(store->contains("run/prices"));
    CHECK(store->read("run/prices").table->num_rows() == 2);
    CHECK_FALSE(store->contains("run/absent"));

    // Immutability holds against a real object store, where an overwrite would
    // usually destroy the previous version outright.
    CHECK_THROWS(store->write("run/prices", make_frame(10)));

    store->clear();
    store.reset();
    // The application owns S3 shutdown; see finalize_s3's contract.
    finalize_s3();
}
