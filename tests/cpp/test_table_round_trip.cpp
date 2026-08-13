// RFC 0019 - the record/read round trip, for EVERY leaf the table codec
// supports.
//
// The coverage here is the point, not the individual cases. Table tests
// naturally stop at Int/Float/Str because those are what the operator under
// test happened to use, and the leaves that actually carry format risk - the
// temporal ones, with struct and fixed-size-list Arrow encodings and a version
// stamp the reader enforces - are exactly the ones that get skipped. The
// missing schema metadata on TableRecorder was that bug: it made every
// recording of a temporal column unreadable by its own replay, and no test
// noticed because none of them recorded one.
//
// So this suite sizes itself against `table_atomic_leaf_metas()` - the codec's
// own dispatch list. Adding a leaf to the codec without adding a sample here
// fails `covers every supported leaf type`, rather than quietly leaving the
// suite one type narrower than it claims to be.
//
// Both writers are exercised per type. TableRecorder (column-described, the
// partitioned path) and TableConverter (schema-described, the value path)
// produce frames that the SAME reader has to accept, and they build their
// Arrow schemas in different places - which is how the two drifted apart.

#include <hgraph/lib/std/value_util.h>
#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/temporal.h>
#include <hgraph/types/value/table_codec.h>

#include <arrow/array.h>
#include <arrow/table.h>
#include <arrow/type.h>
#include <arrow/util/key_value_metadata.h>

#include <catch2/catch_test_macros.hpp>

#include <algorithm>
#include <chrono>
#include <string>
#include <vector>

namespace
{
    using namespace hgraph;

    struct Sample
    {
        std::string              label;
        const ValueTypeMetaData *meta{nullptr};
        Value                    value{};
    };

    template <typename T> [[nodiscard]] Sample sample_of(std::string label, const T &value)
    {
        return Sample{std::move(label), scalar_descriptor<T>::value_meta(), Value{value}};
    }

    [[nodiscard]] CivilDate civil_date(int y, unsigned m, unsigned d)
    {
        return CivilDate{std::chrono::year{y}, std::chrono::month{m}, std::chrono::day{d}};
    }

    /** One non-default value per atomic leaf the codec dispatches on.
     *
     * Values are deliberately not zero/empty: a cell that never got written
     * reads back as a null, and a default-valued sample would make that
     * indistinguishable from a correct write.
     */
    [[nodiscard]] std::vector<Sample> samples()
    {
        using namespace std::chrono;

        const CivilDate day = civil_date(2025, 11, 2);

        const auto provider = make_time_zone_provider();
        const auto zoned    = resolve(CivilDateTime{day, 1, 30}, ZoneId{"America/New_York"}, *provider,
                                      AmbiguousTimePolicy::Latest);

        const auto instants_a = InstantRange::bounded(Instant{Duration{0}}, Instant{Duration{2}});
        const auto instants_b = InstantRange::bounded(Instant{Duration{8}}, Instant{Duration{10}});
        const auto dates_a    = CivilDateRange::bounded(civil_date(2025, 1, 1), civil_date(2025, 2, 1));
        const auto dates_b    = CivilDateRange::bounded(civil_date(2025, 6, 1), civil_date(2025, 7, 1));

        std::vector<Sample> result;
        result.push_back(sample_of<Bool>("Bool", true));
        result.push_back(sample_of<Int>("Int", 42));
        result.push_back(sample_of<Float>("Float", 1.5));
        result.push_back(sample_of<Str>("Str", Str{"hello"}));
        result.push_back(sample_of<Bytes>("Bytes", bytes_("raw-bytes")));
        result.push_back(sample_of<Date>("Date", day));
        result.push_back(sample_of<DateTime>("DateTime", Instant{microseconds{1'700'000'000'000'000}}));
        result.push_back(sample_of<TimeDelta>("TimeDelta", Duration{123'456}));
        result.push_back(sample_of<Time>("Time", time_of_day(13, 45, 30, 250'000)));
        result.push_back(sample_of<CivilDateTime>("CivilDateTime", CivilDateTime{day, 1, 30}));
        result.push_back(sample_of<Period>("Period", Period{1, -2, 3}));
        result.push_back(sample_of<ZoneId>("ZoneId", ZoneId{"America/New_York"}));
        result.push_back(sample_of<ZonedDateTime>("ZonedDateTime", zoned));
        result.push_back(sample_of<InstantRange>("InstantRange", instants_a));
        result.push_back(sample_of<CivilDateRange>("CivilDateRange", dates_a));
        result.push_back(sample_of<InstantRangeSet>("InstantRangeSet", InstantRangeSet{instants_a, instants_b}));
        result.push_back(
            sample_of<CivilDateRangeSet>("CivilDateRangeSet", CivilDateRangeSet{dates_a, dates_b}));
        return result;
    }

    /** Write ``value`` (row 0) and an untouched row (row 1) through the
        column-described writer, and read both back. */
    void check_recorder_round_trip(const Sample &sample)
    {
        const std::string              names[] = {"value"};
        const ValueTypeMetaData *const metas[] = {sample.meta};

        TableRecorder recorder{names, metas};
        recorder.append_cell(0, sample.value.view());
        recorder.end_row();
        recorder.end_row();   // nothing appended - the null case

        const Frame frame = recorder.finish();
        REQUIRE(frame.table->num_rows() == 2);

        // The reader REJECTS an unstamped table, so a writer that forgets the
        // metadata produces recordings its own replay cannot read.
        REQUIRE(frame.table->schema()->metadata() != nullptr);
        CHECK(frame.table->schema()->metadata()->Get("hgraph.temporal.version").ValueOr("") == "2");

        const auto chunked = frame.table->GetColumnByName("value");
        REQUIRE(chunked != nullptr);
        const arrow::Array &array = *chunked->chunk(0);

        CHECK(read_table_cell(sample.meta, array, *frame.table->schema(), 0) == sample.value);
        // A row that never wrote the cell is a null, not a default - the
        // difference between "removed" and "was zero".
        CHECK_FALSE(read_table_cell(sample.meta, array, *frame.table->schema(), 1).has_value());
    }

    /** The same value through the schema-described writer. */
    void check_converter_round_trip(const Sample &sample)
    {
        const auto &converter = table_converter(sample.meta);
        const Frame frame     = single_row_frame(converter, MIN_ST, MIN_ST, sample.value.view());
        REQUIRE(frame.table->num_rows() == 1);
        CHECK(read_row(converter, frame, 0) == sample.value);
    }
}  // namespace

TEST_CASE("table codec: every supported leaf type round-trips through both writers")
{
    for (const Sample &sample : samples())
    {
        // The label is what a failure names - without it the report is a row
        // index into an anonymous loop.
        INFO("leaf type: " << sample.label);
        REQUIRE(sample.meta != nullptr);
        check_recorder_round_trip(sample);
        check_converter_round_trip(sample);
    }
}

TEST_CASE("table codec: the round trip covers every supported leaf type")
{
    // The guard. `table_atomic_leaf_metas` is generated from the codec's own
    // dispatch list, so this fails the moment a leaf is added to the codec
    // without a sample above - which is the only thing stopping this suite
    // from silently going back to covering whatever it happened to cover.
    const auto           supported = table_atomic_leaf_metas();
    const auto           covered   = samples();
    std::vector<std::string> missing;
    for (const auto *meta : supported)
    {
        const bool found = std::any_of(covered.begin(), covered.end(),
                                       [meta](const Sample &s) { return s.meta == meta; });
        if (!found) { missing.push_back(std::string{meta->name()}); }
    }
    CAPTURE(missing);
    CHECK(missing.empty());
    // Catches the reverse too: a leaf REMOVED from the codec leaves a sample
    // behind that no longer proves anything.
    CHECK(covered.size() == supported.size());
}

TEST_CASE("table codec: a sequence leaf round-trips")
{
    // Sequences are derived rather than enumerated (a list of any supported
    // leaf), so they are covered once here rather than per element type.
    const Sample sample{"list[int]", nullptr, stdlib::make_list<Int>({1, 2, 3})};
    const auto  *meta = sample.value.schema();
    REQUIRE(meta != nullptr);

    const std::string              names[] = {"value"};
    const ValueTypeMetaData *const metas[] = {meta};

    TableRecorder recorder{names, metas};
    recorder.append_cell(0, sample.value.view());
    recorder.end_row();
    const Frame frame = recorder.finish();

    const auto chunked = frame.table->GetColumnByName("value");
    REQUIRE(chunked != nullptr);
    CHECK(read_table_cell(meta, *chunked->chunk(0), *frame.table->schema(), 0) == sample.value);
}
