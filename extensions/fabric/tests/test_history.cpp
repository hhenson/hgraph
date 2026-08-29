#include <hgraph/fabric/fabric.h>

#include <arrow/array.h>
#include <arrow/builder.h>
#include <arrow/table.h>

#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers_string.hpp>

#include <cstdint>
#include <stdexcept>

namespace
{
    namespace hg = hgraph;
    namespace hgf = hgraph::fabric;
    namespace hgps = hgraph::persistence::store;

    constexpr hg::DateTime BASE_TIME{hg::TimeDelta{1'800'000'000'000'000}};

    [[nodiscard]] hg::Frame frame(std::int64_t value)
    {
        arrow::Int64Builder builder;
        REQUIRE(builder.Append(value).ok());
        auto array = builder.Finish();
        REQUIRE(array.ok());
        return hg::Frame{arrow::Table::Make(
            arrow::schema({arrow::field("value", arrow::int64())}),
            {std::move(array).ValueOrDie()})};
    }

    [[nodiscard]] std::int64_t scalar(const hg::Frame &value)
    {
        REQUIRE(value.has_value());
        REQUIRE(value.table->num_rows() == 1);
        const auto values = std::static_pointer_cast<arrow::Int64Array>(
            value.table->column(0)->chunk(0));
        return values->Value(0);
    }

    void seed(const hgf::FabricConfig &config, hgf::RevisionId revision,
              hgf::DataVersion output_version, hg::DateTime as_of)
    {
        config.frames.write(
            hgf::data_version_key(config.prefix, "prices", output_version),
            frame(output_version));
        hg::Value value = hgf::make_data_revision(hgf::DataRevisionInput{
            .data_id = "prices",
            .revision = revision,
            .output_version = output_version,
            .as_of = as_of,
        });
        REQUIRE(config.objects
                    .put_immutable(hgf::revision_key(config.prefix, "prices", revision),
                                   hgf::encode_revision(value.view()))
                    .status == hgps::ImmutableWriteStatus::Created);
        REQUIRE(config.objects
                    .put_immutable(
                        hgf::as_of_key(config.prefix, "prices", as_of),
                        hgf::encode_revision_reference(hgf::MetadataObjectKind::AsOf,
                                                       revision))
                    .status == hgps::ImmutableWriteStatus::Created);
    }
}  // namespace

TEST_CASE("load_data_as_of selects the newest point at or before the cutoff")
{
    auto config = hgf::make_memory_fabric_config("tests/history/select");
    seed(config, 1, 10, BASE_TIME + hg::TimeDelta{10});
    seed(config, 2, 20, BASE_TIME + hg::TimeDelta{20});
    seed(config, 3, 30, BASE_TIME + hg::TimeDelta{30});

    CHECK_FALSE(hgf::load_data_as_of(config, "prices", BASE_TIME).has_value());
    REQUIRE(hgf::load_data_as_of(config, "prices",
                                 BASE_TIME + hg::TimeDelta{20})
                .has_value());
    CHECK(scalar(*hgf::load_data_as_of(config, "prices",
                                       BASE_TIME + hg::TimeDelta{20})) == 20);
    CHECK(scalar(*hgf::load_data_as_of(config, "prices",
                                       BASE_TIME + hg::TimeDelta{29})) == 20);
    CHECK(scalar(*hgf::load_data_as_of(config, "prices",
                                       BASE_TIME + hg::TimeDelta{40})) == 30);
}

TEST_CASE("load_data_as_of fails on a missing referenced frame")
{
    auto config = hgf::make_memory_fabric_config("tests/history/missing-frame");
    hg::Value value = hgf::make_data_revision(hgf::DataRevisionInput{
        .data_id = "prices",
        .revision = 1,
        .output_version = 10,
        .as_of = BASE_TIME,
    });
    REQUIRE(config.objects
                .put_immutable(hgf::revision_key(config.prefix, "prices", 1),
                               hgf::encode_revision(value.view()))
                .status == hgps::ImmutableWriteStatus::Created);
    REQUIRE(config.objects
                .put_immutable(
                    hgf::as_of_key(config.prefix, "prices", BASE_TIME),
                    hgf::encode_revision_reference(hgf::MetadataObjectKind::AsOf, 1))
                .status == hgps::ImmutableWriteStatus::Created);

    CHECK_THROWS_WITH(hgf::load_data_as_of(config, "prices", BASE_TIME),
                      "fabric as-of revision references a missing frame");
}
