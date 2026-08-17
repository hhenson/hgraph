// RFC 0019 step 3 - the options, and the column set they project.
//
// A layout describes every column a row COULD carry; a recording chooses which
// of them it actually has. recording_columns is that projection, and `source`
// is the part worth testing hardest: omitting a column shifts every index after
// it, while the emitted row cells are still laid out by the layout. A `source`
// that drifts writes each cell one column to the left - which is silent, since
// the types next to each other are frequently compatible.

#include <hgraph/lib/std/operators/table_rows.h>
#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/registry_reset.h>
#include <hgraph/types/value/table_codec.h>

#include <arrow/type.h>
#include <arrow/util/key_value_metadata.h>

#include <catch2/catch_test_macros.hpp>

#include <stdexcept>
#include <string>
#include <vector>

namespace
{
    using namespace hgraph;
    using namespace hgraph::stdlib::table_ts_detail;

    constexpr std::string_view kDate  = "__date_time__";
    constexpr std::string_view kAsOf  = "__as_of__";

    /** ``TSD[str, TS[int]]`` - one level, one flattened key column. */
    [[nodiscard]] const TsTableLayout &one_level_layout()
    {
        auto &registry = TypeRegistry::instance();
        const auto *ts = registry.tsd(scalar_descriptor<Str>::value_meta(),
                                      registry.ts(scalar_descriptor<Int>::value_meta()));
        return ts_table_layout(ts, kDate, kAsOf);
    }

    /** ``TSD[str, TSD[int, TS[float]]]`` - two levels, so two removed flags. */
    [[nodiscard]] const TsTableLayout &two_level_layout()
    {
        auto &registry = TypeRegistry::instance();
        const auto *inner = registry.tsd(scalar_descriptor<Int>::value_meta(),
                                         registry.ts(scalar_descriptor<Float>::value_meta()));
        const auto *ts    = registry.tsd(scalar_descriptor<Str>::value_meta(), inner);
        return ts_table_layout(ts, kDate, kAsOf);
    }

    [[nodiscard]] std::vector<std::string> names_of(const RecordingColumns &columns)
    {
        return columns.names;
    }
}  // namespace

TEST_CASE("recording columns: the default projection drops the removed flags")
{
    const auto &layout  = one_level_layout();
    const auto  columns = recording_columns(layout, TableRecordingOptions{});

    // Removes::Omit is the default, so the level's removed flag is not a
    // column - matching the adaptor, whose override state leaves it off.
    CHECK(names_of(columns) == std::vector<std::string>{"__date_time__", "__as_of__", "__key_1__", "value"});

    // Every name still resolves to the layout column it was projected from.
    for (std::size_t i = 0; i < columns.size(); ++i)
    {
        CHECK(columns.names[i] == layout.keys[columns.source[i]]);
        CHECK(columns.metas[i] == layout.col_metas[columns.source[i]]);
    }
    // The removed flag sits at layout column 2, so the key and value columns
    // are 3 and 4 - not 2 and 3. This is the shift the source map exists for.
    CHECK(columns.source == std::vector<std::size_t>{0, 1, 3, 4});
}

TEST_CASE("recording columns: tracking removes restores the flag per level")
{
    const auto &layout = two_level_layout();
    TableRecordingOptions options;
    options.removes = TableRecordingOptions::Removes::Track;

    const auto columns = recording_columns(layout, options);
    CHECK(names_of(columns) == std::vector<std::string>{"__date_time__", "__as_of__",
                                                        "__key_1_removed__", "__key_1__",
                                                        "__key_2_removed__", "__key_2__", "value"});
    // With nothing omitted the projection is the layout's own row, in order.
    CHECK(columns.source == std::vector<std::size_t>{0, 1, 2, 3, 4, 5, 6});
}

TEST_CASE("recording columns: omitting as-of removes the column and shifts the rest")
{
    const auto &layout = one_level_layout();
    TableRecordingOptions options;
    options.as_of = TableRecordingOptions::AsOf::Omit;

    const auto columns = recording_columns(layout, options);
    CHECK(names_of(columns) == std::vector<std::string>{"__date_time__", "__key_1__", "value"});
    // Layout column 1 is the as-of; the columns after it keep their layout
    // indices while their own positions move down by one.
    CHECK(columns.source == std::vector<std::size_t>{0, 3, 4});
}

TEST_CASE("recording columns: a fixed as-of is still a column")
{
    const auto &layout = one_level_layout();
    TableRecordingOptions options;
    options.as_of      = TableRecordingOptions::AsOf::Fixed;
    options.as_of_value = MIN_ST;

    // Fixed changes what the cell CARRIES, not whether the column exists -
    // only Omit drops it.
    const auto columns = recording_columns(layout, options);
    CHECK(columns.names[1] == "__as_of__");
    CHECK(columns.source == std::vector<std::size_t>{0, 1, 3, 4});
}

TEST_CASE("recording columns: every column can be renamed")
{
    const auto &layout = two_level_layout();
    TableRecordingOptions options;
    options.removes         = TableRecordingOptions::Removes::Track;
    options.date_key        = "event_time";
    options.as_of_key       = "observed_at";
    options.partition_names = {"symbol", "leg"};
    options.removed_names   = {"symbol_gone", "leg_gone"};

    const auto columns = recording_columns(layout, options);
    CHECK(names_of(columns) == std::vector<std::string>{"event_time", "observed_at", "symbol_gone",
                                                        "symbol", "leg_gone", "leg", "value"});
    // Renaming maps names, not positions.
    CHECK(columns.source == std::vector<std::size_t>{0, 1, 2, 3, 4, 5, 6});
}

TEST_CASE("recording columns: partition names are per flattened key column, not per level")
{
    const auto &layout = two_level_layout();
    TableRecordingOptions options;
    // Two levels, two flattened key columns here - but the count that must
    // match is the flattened one, so a per-level list of the wrong length is
    // refused rather than silently applied to the first N columns.
    options.partition_names = {"only_one"};
    CHECK_THROWS_AS(recording_columns(layout, options), std::invalid_argument);

    options.partition_names = {"a", "b", "c"};
    CHECK_THROWS_AS(recording_columns(layout, options), std::invalid_argument);

    options.partition_names = {"a", "b"};
    CHECK_NOTHROW(recording_columns(layout, options));
}

TEST_CASE("recording columns: a removed-name list of the wrong length is refused")
{
    const auto &layout = two_level_layout();
    TableRecordingOptions options;
    options.removes       = TableRecordingOptions::Removes::Track;
    options.removed_names = {"only_one"};
    CHECK_THROWS_AS(recording_columns(layout, options), std::invalid_argument);
}

TEST_CASE("recording columns: a rename that collides is refused, not deduplicated")
{
    const auto &layout = one_level_layout();
    TableRecordingOptions options;
    // Two columns called "value" would make the recording unreadable by name,
    // which is how replay resolves the leaf.
    options.partition_names = {"value"};
    CHECK_THROWS_AS(recording_columns(layout, options), std::invalid_argument);

    options.partition_names = {};
    options.date_key        = "__as_of__";
    CHECK_THROWS_AS(recording_columns(layout, options), std::invalid_argument);
}

TEST_CASE("recording columns: a layout is not reused across a registry reset")
{
    // The layout cache interns by TS-schema POINTER, and a reset frees every
    // schema - so the next interning can be handed the same address for a
    // DIFFERENT type. Without the generation check, this returns the one-level
    // layout for a two-level type (or the reverse, depending on the allocator),
    // and the recording is silently shaped wrong.
    const auto one = recording_columns(one_level_layout(), TableRecordingOptions{}).names;
    CHECK(one.size() == 4);

    reset_all_registries();

    const auto two = recording_columns(two_level_layout(), TableRecordingOptions{}).names;
    CHECK(two == std::vector<std::string>{"__date_time__", "__as_of__", "__key_1__", "__key_2__", "value"});

    reset_all_registries();

    CHECK(recording_columns(one_level_layout(), TableRecordingOptions{}).names == one);
}

TEST_CASE("recording columns: the frame prefix applies only to an expanded frame")
{
    auto &registry = TypeRegistry::instance();

    // A plain value column has nothing to disambiguate from, so the prefix
    // does not touch it.
    const auto &plain = ts_table_layout(registry.ts(scalar_descriptor<Int>::value_meta()), kDate, kAsOf);
    TableRecordingOptions options;
    options.frame_prefix = "px_";
    CHECK(names_of(recording_columns(plain, options))
          == std::vector<std::string>{"__date_time__", "__as_of__", "value"});

    // An expanded frame contributes its own columns, which the prefix
    // qualifies - that is what it is for.
    const auto *row = registry.bundle("tests.recording_columns::Row",
                                      {{"value", scalar_descriptor<Int>::value_meta()}});
    const auto &framed = ts_table_layout(registry.ts(registry.frame(row)), kDate, kAsOf);
    CHECK(names_of(recording_columns(framed, options))
          == std::vector<std::string>{"__date_time__", "__as_of__", "px_value"});
    // Without it the frame's column keeps its own name.
    CHECK(names_of(recording_columns(framed, TableRecordingOptions{}))
          == std::vector<std::string>{"__date_time__", "__as_of__", "value"});
}

TEST_CASE("recording columns: a recorded schema carries the temporal metadata")
{
    // The reader REJECTS a table without this, so a writer that omits it
    // produces recordings its own replay refuses. TableRecorder omitted it,
    // and only atomic-leaf tests covered that path.
    const auto *when = scalar_descriptor<DateTime>::value_meta();
    const auto *zoned = scalar_descriptor<ZonedDateTime>::value_meta();

    const std::string              plain_names[] = {"value"};
    const ValueTypeMetaData *const plain_metas[] = {when};
    TableRecorder                  plain{plain_names, plain_metas};
    const auto                    &plain_schema = plain.arrow_schema()->metadata();
    REQUIRE(plain_schema != nullptr);
    CHECK(plain_schema->FindKey("hgraph.temporal.version") >= 0);
    // No ZonedDateTime column, so no TZDB version is claimed.
    CHECK(plain_schema->FindKey("hgraph.tzdb.version") < 0);

    const std::string              zoned_names[] = {"value"};
    const ValueTypeMetaData *const zoned_metas[] = {zoned};
    TableRecorder                  tz{zoned_names, zoned_metas};
    const auto                    &tz_schema = tz.arrow_schema()->metadata();
    REQUIRE(tz_schema != nullptr);
    CHECK(tz_schema->FindKey("hgraph.tzdb.version") >= 0);
}
