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
