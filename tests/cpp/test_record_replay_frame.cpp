#include <hgraph/lib/std/std_operators.h>
#include <hgraph/lib/testing/check_output.h>
#include <hgraph/lib/testing/eval_node.h>
#include <hgraph/types/graph_wiring.h>
#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/record_replay.h>
#include <hgraph/types/table_config.h>
#include <hgraph/types/value/table_codec.h>

#include <arrow/api.h>

#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers_string.hpp>

#include <algorithm>
#include <cstdint>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

// Step 4 of the record/replay/table design record: the Arrow data-frame
// record/replay backend (backend "hgraph.persistence.frame") over the registered
// frame store (P6), plus the replay_const read. Recording happens in one
// graph run and replays in a SECOND run - the store outlives the graph.

namespace
{
    using namespace hgraph;
    using namespace hgraph::testing;

    using ReplayBundle = UnNamedTSB<Field<"a", TS<Int>>, Field<"b", TS<Str>>>;

    void require_arrow(const arrow::Status &status)
    {
        if (!status.ok())
        {
            throw std::runtime_error(status.ToString());
        }
    }

    template <typename Builder>
    [[nodiscard]] std::shared_ptr<arrow::Array> finish(Builder &builder)
    {
        std::shared_ptr<arrow::Array> result;
        require_arrow(builder.Finish(&result));
        return result;
    }

    [[nodiscard]] std::int64_t timestamp_at(const Frame &frame, const std::string &column,
                                            std::int64_t row)
    {
        const auto chunked = frame.table->GetColumnByName(column);
        REQUIRE(chunked != nullptr);
        return static_cast<const arrow::TimestampArray &>(*chunked->chunk(0)).Value(row);
    }

    [[nodiscard]] Frame scalar_replay_frame()
    {
        arrow::TimestampBuilder date{arrow::timestamp(arrow::TimeUnit::MICRO),
                                     arrow::default_memory_pool()};
        arrow::TimestampBuilder as_of{arrow::timestamp(arrow::TimeUnit::MICRO),
                                      arrow::default_memory_pool()};
        arrow::Int64Builder     value;
        for (const auto &[when, revision, item] : std::vector<std::tuple<DateTime, DateTime, Int>>{
                 {MIN_ST, MIN_ST + TimeDelta{20}, 20},
                 {MIN_ST, MIN_ST + TimeDelta{10}, 10},
                 {MIN_ST + TimeDelta{1}, MIN_ST + TimeDelta{20}, 40},
                 {MIN_ST + TimeDelta{1}, MIN_ST + TimeDelta{10}, 30},
                 {MIN_ST - TimeDelta{1}, MIN_ST + TimeDelta{5}, 5}})
        {
            require_arrow(date.Append(when.time_since_epoch().count()));
            require_arrow(as_of.Append(revision.time_since_epoch().count()));
            require_arrow(value.Append(item));
        }
        return Frame{arrow::Table::Make(
            arrow::schema({arrow::field("__date_time__", arrow::timestamp(arrow::TimeUnit::MICRO)),
                           arrow::field("__as_of__", arrow::timestamp(arrow::TimeUnit::MICRO)),
                           arrow::field("value", arrow::int64())}),
            {finish(date), finish(as_of), finish(value)})};
    }

    [[nodiscard]] Frame bundle_replay_frame()
    {
        arrow::TimestampBuilder date{arrow::timestamp(arrow::TimeUnit::MICRO),
                                     arrow::default_memory_pool()};
        arrow::TimestampBuilder as_of{arrow::timestamp(arrow::TimeUnit::MICRO),
                                      arrow::default_memory_pool()};
        arrow::Int64Builder     a;
        arrow::StringBuilder    b;
        for (const auto when : {MIN_ST, MIN_ST + TimeDelta{1}})
        {
            require_arrow(date.Append(when.time_since_epoch().count()));
            require_arrow(as_of.Append((MIN_ST + TimeDelta{10}).time_since_epoch().count()));
        }
        require_arrow(a.Append(1));
        require_arrow(a.AppendNull());
        require_arrow(b.AppendValues({"one", "two"}));
        return Frame{arrow::Table::Make(
            arrow::schema({arrow::field("__date_time__", arrow::timestamp(arrow::TimeUnit::MICRO)),
                           arrow::field("__as_of__", arrow::timestamp(arrow::TimeUnit::MICRO)),
                           arrow::field("a", arrow::int64()), arrow::field("b", arrow::utf8())}),
            {finish(date), finish(as_of), finish(a), finish(b)})};
    }

    [[nodiscard]] Frame dict_replay_frame()
    {
        arrow::TimestampBuilder date{arrow::timestamp(arrow::TimeUnit::MICRO),
                                     arrow::default_memory_pool()};
        arrow::TimestampBuilder as_of{arrow::timestamp(arrow::TimeUnit::MICRO),
                                      arrow::default_memory_pool()};
        arrow::BooleanBuilder   removed;
        arrow::StringBuilder    key;
        arrow::Int64Builder     value;
        for (const auto &[when, revision, is_removed, item_key, item] :
             std::vector<std::tuple<DateTime, DateTime, Bool, Str, std::optional<Int>>>{
                 {MIN_ST, MIN_ST + TimeDelta{20}, false, "a", 2},
                 {MIN_ST, MIN_ST + TimeDelta{10}, false, "a", 1},
                 {MIN_ST, MIN_ST + TimeDelta{10}, false, "b", 3},
                 {MIN_ST + TimeDelta{1}, MIN_ST + TimeDelta{10}, true, "a", std::nullopt}})
        {
            require_arrow(date.Append(when.time_since_epoch().count()));
            require_arrow(as_of.Append(revision.time_since_epoch().count()));
            require_arrow(removed.Append(is_removed));
            require_arrow(key.Append(item_key));
            if (item.has_value())
            {
                require_arrow(value.Append(*item));
            }
            else
            {
                require_arrow(value.AppendNull());
            }
        }
        return Frame{arrow::Table::Make(
            arrow::schema({arrow::field("__date_time__", arrow::timestamp(arrow::TimeUnit::MICRO)),
                           arrow::field("__as_of__", arrow::timestamp(arrow::TimeUnit::MICRO)),
                           arrow::field("__key_1_removed__", arrow::boolean()),
                           arrow::field("__key_1__", arrow::utf8()),
                           arrow::field("value", arrow::int64())}),
            {finish(date), finish(as_of), finish(removed), finish(key), finish(value)})};
    }

    struct RecordGraph
    {
        [[maybe_unused]] static constexpr auto name = "record_frame_graph";

        static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> ts)
        {
            wire<stdlib::record>(w, ts, Str{"prices"}, arg<"recordable_id">(Str{"book"}));
            return ts;  // eval_node needs an output; the recording is the side effect
        }
    };

    struct SegmentedRecordGraph
    {
        [[maybe_unused]] static constexpr auto name = "segmented_record_frame_graph";

        static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> ts)
        {
            wire<stdlib::record>(w, ts, Str{"prices"}, arg<"recordable_id">(Str{"book"}),
                                 arg<"flush_rows">(Int{2}));
            return ts;
        }
    };

    struct IntervalSegmentedRecordGraph
    {
        [[maybe_unused]] static constexpr auto name = "interval_segmented_record_frame_graph";

        static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> ts)
        {
            wire<stdlib::record>(w, ts, Str{"prices"}, arg<"recordable_id">(Str{"book"}),
                                 arg<"flush_interval">(TimeDelta{2}));
            return ts;
        }
    };

    template <Int Mode>
    struct ModeRecordGraph
    {
        [[maybe_unused]] static constexpr auto name = "mode_record_frame_graph";

        static Port<TSD<Str, ReplayBundle>> compose(Wiring &w, Port<TSD<Str, ReplayBundle>> ts)
        {
            wire<stdlib::record>(w, ts,
                                 Str{Mode == 1   ? "tick"
                                     : Mode == 2 ? "sample"
                                                 : "snap"},
                                 arg<"recordable_id">(Str{"mode"}),
                                 arg<"mode">(static_cast<hgraph::stdlib::ToTableMode>(Mode)));
            return ts;
        }
    };

    struct SegmentEquivalenceRecordGraph
    {
        [[maybe_unused]] static constexpr auto name = "segment_equivalence_record_graph";

        static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> ts)
        {
            wire<stdlib::record>(w, ts, Str{"one"}, arg<"recordable_id">(Str{"segments"}),
                                 arg<"flush_rows">(Int{100}));
            wire<stdlib::record>(w, ts, Str{"many"}, arg<"recordable_id">(Str{"segments"}),
                                 arg<"flush_rows">(Int{2}));
            return ts;
        }
    };

    template <fixed_string Key>
    struct SegmentEquivalenceReplayGraph
    {
        [[maybe_unused]] static constexpr auto name = "segment_equivalence_replay_graph";

        static Port<TS<Int>> compose(Wiring &w)
        {
            return wire<stdlib::replay, TS<Int>>(w, Str{Key.sv()},
                                                 arg<"recordable_id">(Str{"segments"}))
                .as<TS<Int>>();
        }
    };

    struct ReplayGraph
    {
        [[maybe_unused]] static constexpr auto name = "replay_frame_graph";

        static Port<TS<Int>> compose(Wiring &w)
        {
            return wire<stdlib::replay, TS<Int>>(w, Str{"prices"},
                                                 arg<"recordable_id">(Str{"book"}))
                .as<TS<Int>>();
        }
    };

    struct RenamedTimeRecordGraph
    {
        [[maybe_unused]] static constexpr auto name = "renamed_time_record_graph";

        static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> ts)
        {
            wire<stdlib::record>(w, ts, Str{"renamed"}, arg<"recordable_id">(Str{"book"}),
                                 arg<"date_key">(Str{"event_time"}),
                                 arg<"as_of_key">(Str{"revision"}));
            return ts;
        }
    };

    struct RenamedTimeReplayGraph
    {
        [[maybe_unused]] static constexpr auto name = "renamed_time_replay_graph";

        static Port<TS<Int>> compose(Wiring &w)
        {
            return wire<stdlib::replay, TS<Int>>(
                       w, Str{"renamed"}, arg<"recordable_id">(Str{"book"}),
                       arg<"date_key">(Str{"event_time"}), arg<"as_of_key">(Str{"revision"}))
                .as<TS<Int>>();
        }
    };

    struct ReplaySinkGraph
    {
        [[maybe_unused]] static constexpr auto name = "replay_frame_sink_graph";

        static void compose(Wiring &w)
        {
            auto replayed =
                wire<stdlib::replay, TS<Int>>(w, Str{"prices"}, arg<"recordable_id">(Str{"book"}));
            wire<stdlib::dense_record_impl>(w, replayed, Str{"selected"});
        }
    };

    struct TraitRecordGraph
    {
        [[maybe_unused]] static constexpr auto name = "trait_record_frame_graph";

        static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> ts)
        {
            // No explicit recordable_id: the graph trait provides it at runtime
            // through the TraitsView injectable.
            w.set_trait(std::string{record_replay::RECORDABLE_ID_TRAIT}, Value{Str{"desk.fx"}});
            wire<stdlib::record>(w, ts, Str{"orders"});
            return ts;
        }
    };

    struct CapturingFrameStore
    {
        std::unordered_map<std::string, Frame> frames;
    };

    [[nodiscard]] const store::FrameStoreOps &capturing_frame_store_ops()
    {
        static const store::FrameStoreOps ops{
            [](void *context, std::string_view key, Frame frame,
               std::optional<store::Compression>) {
                static_cast<CapturingFrameStore *>(context)->frames[std::string{key}] =
                    std::move(frame);
            },
            [](void *context, std::string_view key) {
                const auto &frames = static_cast<CapturingFrameStore *>(context)->frames;
                const auto  item = frames.find(std::string{key});
                return item == frames.end() ? Frame{} : item->second;
            },
            [](void *context, std::string_view key) {
                return static_cast<CapturingFrameStore *>(context)->frames.contains(
                    std::string{key});
            },
            [](void *context) { static_cast<CapturingFrameStore *>(context)->frames.clear(); },
        };
        return ops;
    }

    [[nodiscard]] Frame scalar_segment(std::initializer_list<std::pair<DateTime, Int>> rows)
    {
        FrameRecorder recorder{table_converter(scalar_descriptor<Int>::value_meta())};
        for (const auto &[when, value] : rows)
        {
            const Value cell{value};
            recorder.append(when, when, cell.view());
        }
        return recorder.finish();
    }
}  // namespace

TEST_CASE("frame backend: record writes a bitemporal frame to the store; "
          "replay re-emits it")
{
    stdlib::register_standard_operators();
    GlobalContext context;
    record_replay::set_config(
        context.state().view(),
        record_replay::RecordReplayConfig{.backend = "hgraph.persistence.frame"});

    // Run 1: record (values at cycles 0, 2 and 3 - gaps preserved).
    (void)eval_node<RecordGraph>(values<Int>(10, none, 30, 40));

    REQUIRE(record_replay::store_contains("book.prices"));
    const Frame recorded = record_replay::store_read("book.prices");
    CHECK(frame_rows(recorded) == 3);

    // Run 2: replay - values re-emitted at the RECORDED times (cycle-aligned).
    CHECK_OUTPUT(eval_node<ReplayGraph>(), values<Int>(10, none, 30, 40));
}

TEST_CASE("frame backend: per-recording time-column names replay through the "
          "projection")
{
    stdlib::register_standard_operators();
    GlobalContext context;
    const auto    state = context.state().view();
    record_replay::set_config(
        state, record_replay::RecordReplayConfig{.backend = "hgraph.persistence.frame"});
    const auto expected = values<Int>(10, none, 30);

    (void)eval_node<RenamedTimeRecordGraph>(expected);
    const Frame recorded = record_replay::store_read(state, "book.renamed");
    REQUIRE(recorded.has_value());
    CHECK(recorded.table->GetColumnByName("event_time") != nullptr);
    CHECK(recorded.table->GetColumnByName("revision") != nullptr);
    CHECK(recorded.table->GetColumnByName("__date_time__") == nullptr);
    CHECK(recorded.table->GetColumnByName("__as_of__") == nullptr);
    CHECK_OUTPUT(eval_node<RenamedTimeReplayGraph>(), expected);
}

TEST_CASE("frame backend: row-count flushing writes immutable segments and "
          "replays them sequentially")
{
    stdlib::register_standard_operators();
    GlobalContext context;
    const auto    state = context.state().view();
    record_replay::set_config(
        state, record_replay::RecordReplayConfig{.backend = "hgraph.persistence.frame"});

    (void)eval_node<SegmentedRecordGraph>(values<Int>(10, 20, 30, 40, 50));

    const Frame marker = record_replay::store_read(state, "book.prices");
    REQUIRE(record_replay::is_segmented_recording(marker));
    CHECK(frame_rows(record_replay::store_read(state, "book.prices.0")) == 2);
    CHECK(frame_rows(record_replay::store_read(state, "book.prices.1")) == 2);
    CHECK(frame_rows(record_replay::store_read(state, "book.prices.2")) == 1);
    CHECK(record_replay::store_contains(state, "book.prices.complete"));

    CHECK_OUTPUT(eval_node<ReplayGraph>(), values<Int>(10, 20, 30, 40, 50));
    CHECK(record_replay::replay_const_value(state, "book.prices",
                                            scalar_descriptor<Int>::value_meta())
              .view()
              .checked_as<Int>() == Int{50});
}

TEST_CASE("frame backend: time flushing commits complete tick-aligned segments")
{
    stdlib::register_standard_operators();
    GlobalContext context;
    const auto    state = context.state().view();
    record_replay::set_config(
        state, record_replay::RecordReplayConfig{.backend = "hgraph.persistence.frame"});

    (void)eval_node<IntervalSegmentedRecordGraph>(values<Int>(10, 20, 30, 40, 50));

    CHECK(frame_rows(record_replay::store_read(state, "book.prices.0")) == 3);
    CHECK(frame_rows(record_replay::store_read(state, "book.prices.1")) == 2);
    CHECK_FALSE(record_replay::store_contains(state, "book.prices.2"));
    CHECK_OUTPUT(eval_node<ReplayGraph>(), values<Int>(10, 20, 30, 40, 50));
}

TEST_CASE("frame backend: Tick Sample and Snap use the to_table row semantics")
{
    stdlib::register_standard_operators();
    GlobalContext context;
    const auto    state = context.state().view();
    record_replay::set_config(
        state, record_replay::RecordReplayConfig{.backend = "hgraph.persistence.frame"});

    const Value first =
        dict_delta<Str, ReplayBundle>({{Str{"a"}, tsb_delta<ReplayBundle>(Int{1}, Str{"one"})},
                                       {Str{"b"}, tsb_delta<ReplayBundle>(Int{10}, Str{"ten"})}});
    const Value second =
        dict_delta<Str, ReplayBundle>({{Str{"a"}, tsb_delta<ReplayBundle>(Int{2}, std::nullopt)}});
    const auto input = values<Value>(first, second);

    (void)eval_node<ModeRecordGraph<1>>(input);
    (void)eval_node<ModeRecordGraph<2>>(input);
    (void)eval_node<ModeRecordGraph<3>>(input);

    const Frame tick = record_replay::store_read(state, "mode.tick");
    const Frame sample = record_replay::store_read(state, "mode.sample");
    const Frame snap = record_replay::store_read(state, "mode.snap");
    REQUIRE(frame_rows(tick) == 3);
    REQUIRE(frame_rows(sample) == 3);
    REQUIRE(frame_rows(snap) == 4);

    // Tick carries only the modified field; Sample carries the complete
    // current child at each modified key. Snap additionally visits key b on
    // the second tick even though that child did not change.
    CHECK(tick.table->GetColumnByName("b")->chunk(0)->IsNull(2));
    CHECK(
        frame_cell(sample, "b", scalar_descriptor<Str>::value_meta(), 2).view().checked_as<Str>() ==
        Str{"one"});
    std::size_t second_tick_rows = 0;
    for (std::int64_t row = 0; row < frame_rows(snap); ++row)
    {
        if (timestamp_at(snap, "__date_time__", row) ==
            (MIN_ST + TimeDelta{1}).time_since_epoch().count())
        {
            ++second_tick_rows;
        }
    }
    CHECK(second_tick_rows == 2);
}

TEST_CASE("frame backend: one and many immutable segments replay identically")
{
    stdlib::register_standard_operators();
    GlobalContext context;
    const auto    state = context.state().view();
    record_replay::set_config(
        state, record_replay::RecordReplayConfig{.backend = "hgraph.persistence.frame"});
    const auto expected = values<Int>(10, 20, 30, 40, 50);

    (void)eval_node<SegmentEquivalenceRecordGraph>(expected);
    CHECK(record_replay::store_contains(state, "segments.one.0"));
    CHECK_FALSE(record_replay::store_contains(state, "segments.one.1"));
    CHECK(record_replay::store_contains(state, "segments.many.2"));
    CHECK_OUTPUT((eval_node<SegmentEquivalenceReplayGraph<"one">>()), expected);
    CHECK_OUTPUT((eval_node<SegmentEquivalenceReplayGraph<"many">>()), expected);
}

TEST_CASE("frame backend: replay consumes completed segments without a "
          "completion manifest")
{
    stdlib::register_standard_operators();
    GlobalContext context;
    const auto    state = context.state().view();
    record_replay::set_config(
        state, record_replay::RecordReplayConfig{.backend = "hgraph.persistence.frame"});

    record_replay::store_write(state, "book.prices", record_replay::segmented_recording_marker());
    record_replay::store_write(
        state, "book.prices.0",
        scalar_segment({{MIN_ST, Int{10}}, {MIN_ST + TimeDelta{1}, Int{20}}}));
    record_replay::store_write(state, "book.prices.1",
                               scalar_segment({{MIN_ST + TimeDelta{2}, Int{30}}}));

    CHECK_FALSE(record_replay::store_contains(state, "book.prices.complete"));
    CHECK_OUTPUT(eval_node<ReplayGraph>(), values<Int>(10, 20, 30));
}

TEST_CASE("frame backend: a legacy segment key prevents an ambiguous flat "
          "recording")
{
    stdlib::register_standard_operators();
    GlobalContext context;
    const auto    state = context.state().view();
    record_replay::set_config(
        state, record_replay::RecordReplayConfig{.backend = "hgraph.persistence.frame"});
    record_replay::store_write(state, "book.prices.0", scalar_segment({{MIN_ST, Int{10}}}));

    CHECK_THROWS_WITH(
        (void)eval_node<RecordGraph>(values<Int>(20)),
        Catch::Matchers::ContainsSubstring("recording key already exists: book.prices"));
}

TEST_CASE("frame backend: a stale completion key prevents a segmented run")
{
    stdlib::register_standard_operators();
    GlobalContext context;
    const auto    state = context.state().view();
    record_replay::set_config(
        state, record_replay::RecordReplayConfig{.backend = "hgraph.persistence.frame"});
    record_replay::store_write(state, "book.prices.complete",
                               record_replay::segmented_recording_manifest(1, 1));

    CHECK_THROWS_WITH(
        (void)eval_node<SegmentedRecordGraph>(values<Int>(20)),
        Catch::Matchers::ContainsSubstring("recording key already exists: book.prices"));
    CHECK_FALSE(record_replay::store_contains(state, "book.prices"));
    CHECK_FALSE(record_replay::store_contains(state, "book.prices.0"));
}

TEST_CASE("frame backend: inherited recording uses the configured fixed as-of")
{
    stdlib::register_standard_operators();
    GlobalContext  context;
    const auto     state = context.state().view();
    const DateTime fixed_as_of = MIN_ST + TimeDelta{30};
    record_replay::set_config(
        state, record_replay::RecordReplayConfig{.backend = "hgraph.persistence.frame"});
    table::set_config(state, table::TableConfig{.as_of = fixed_as_of});

    (void)eval_node<RecordGraph>(values<Int>(10, none, 30, 40));

    const Frame recorded = record_replay::store_read(state, "book.prices");
    REQUIRE(recorded.has_value());
    REQUIRE(frame_rows(recorded) == 3);
    for (std::int64_t row = 0; row < frame_rows(recorded); ++row)
    {
        CHECK(timestamp_at(recorded, "__as_of__", row) == fixed_as_of.time_since_epoch().count());
    }
}

TEST_CASE("frame backend: a custom erased C++ store participates through "
          "public wiring")
{
    stdlib::register_standard_operators();
    GlobalContext context;
    const auto    state = context.state().view();
    auto          capture = std::make_shared<CapturingFrameStore>();
    record_replay::set_frame_store(state, store::FrameStore{capture, capturing_frame_store_ops()});
    record_replay::set_config(
        state, record_replay::RecordReplayConfig{.backend = "hgraph.persistence.frame"});

    // Even when the call requests flushing, a compatibility/custom store is
    // a whole-frame write seam and is never asked to manage segment keys.
    (void)eval_node<SegmentedRecordGraph>(values<Int>(10, none, 30));
    REQUIRE(capture->frames.contains("book.prices"));
    CHECK_FALSE(capture->frames.contains("book.prices.0"));
    CHECK(frame_rows(capture->frames.at("book.prices")) == 2);
    CHECK_OUTPUT(eval_node<ReplayGraph>(), values<Int>(10, none, 30));
}

TEST_CASE("frame backend: the recordable id resolves through graph traits at "
          "runtime")
{
    stdlib::register_standard_operators();
    GlobalContext context;
    record_replay::set_config(
        context.state().view(),
        record_replay::RecordReplayConfig{.backend = "hgraph.persistence.frame"});

    (void)eval_node<TraitRecordGraph>(values<Int>(7));
    CHECK(record_replay::store_contains("desk.fx.orders"));
}

TEST_CASE("frame backend: native recording keys are immutable within a store")
{
    stdlib::register_standard_operators();
    GlobalContext context;
    const auto    state = context.state().view();
    record_replay::set_config(
        state, record_replay::RecordReplayConfig{.backend = "hgraph.persistence.frame"});

    (void)eval_node<RecordGraph>(values<Int>(10));
    CHECK_THROWS_WITH((void)eval_node<RecordGraph>(values<Int>(20)),
                      Catch::Matchers::ContainsSubstring("key already exists"));

    const Frame original = record_replay::store_read(state, "book.prices");
    REQUIRE(original.has_value());
    CHECK(read_row(table_converter(scalar_descriptor<Int>::value_meta()), original, 0)
              .view()
              .checked_as<Int>() == Int{10});
}

TEST_CASE("frame backend: replay_const_value reads the last row at or before tm")
{
    stdlib::register_standard_operators();
    GlobalContext context;
    const auto    state = context.state().view();
    record_replay::set_config(
        state, record_replay::RecordReplayConfig{.backend = "hgraph.persistence.frame"});

    (void)eval_node<RecordGraph>(values<Int>(10, none, 30, 40));

    const auto *int_meta = scalar_descriptor<Int>::value_meta();
    // Everything <= MAX_DT: the last recorded value.
    CHECK(record_replay::replay_const_value(state, "book.prices", int_meta)
              .view()
              .checked_as<Int>() == Int{40});
    // Cut at the second recorded tick (cycle 2 = MIN_ST + 2).
    CHECK(record_replay::replay_const_value(state, "book.prices", int_meta, MIN_ST + TimeDelta{2})
              .view()
              .checked_as<Int>() == Int{30});
    // Before the first tick: nothing qualifies.
    CHECK_FALSE(
        record_replay::replay_const_value(state, "book.prices", int_meta, MIN_ST - TimeDelta{1})
            .has_value());
    // Unknown key: empty.
    CHECK_FALSE(record_replay::replay_const_value(state, "missing.key", int_meta).has_value());
}

TEST_CASE("raw frame replay selects revisions independently of input row order")
{
    stdlib::register_standard_operators();
    const auto result = eval_node<stdlib::replay_data_frame, TS<Int>>(scalar_replay_frame(),
                                                                      MIN_ST + TimeDelta{15});
    REQUIRE(result.size() == 2);
    REQUIRE(result[0].has_value());
    REQUIRE(result[1].has_value());
    CHECK(result[0]->view().checked_as<Int>() == Int{10});
    CHECK(result[1]->view().checked_as<Int>() == Int{30});
}

TEST_CASE("stored frame replay selects the visible revision")
{
    stdlib::register_standard_operators();
    GlobalContext context;
    const auto    state = context.state().view();
    record_replay::set_config(
        state, record_replay::RecordReplayConfig{.backend = "hgraph.persistence.frame"});
    table::set_config(state, table::TableConfig{.as_of = MIN_ST + TimeDelta{15}});
    record_replay::store_write(state, "book.prices", scalar_replay_frame());

    CHECK_OUTPUT(eval_node<ReplayGraph>(), values<Int>(10, 30));
}

TEST_CASE("stored frame replay discards rows before graph start")
{
    stdlib::register_standard_operators();
    GlobalContext context;
    const auto    state = context.state().view();
    record_replay::set_config(
        state, record_replay::RecordReplayConfig{.backend = "hgraph.persistence.frame"});
    record_replay::store_write(state, "book.prices", scalar_replay_frame());

    GraphBuilder         gb = build_graph<ReplaySinkGraph>();
    GraphExecutorBuilder eb;
    eb.graph_builder(std::move(gb))
        .start_time(MIN_ST + TimeDelta{1})
        .end_time(MIN_ST + TimeDelta{2});
    GraphExecutorValue executor = eb.make_executor();
    auto               view = executor.view();
    view.run();

    const auto replayed = get_recorded_values<Int>(view.graph().global_state(), "selected");
    REQUIRE_FALSE(replayed.empty());
    CHECK(replayed.back() == std::optional<Int>{40});
    CHECK(std::ranges::none_of(replayed,
                               [](const auto &value) { return value == std::optional<Int>{20}; }));
}

TEST_CASE("raw frame replay uses configured as-of and applies TSB rows")
{
    stdlib::register_standard_operators();
    GlobalContext context;
    table::set_config(context.state().view(),
                      table::TableConfig{.as_of = MIN_ST + TimeDelta{15}});

    const auto scalar =
        eval_node<stdlib::replay_data_frame, TS<Int>>(scalar_replay_frame(), MAX_DT);
    REQUIRE(scalar.size() == 2);
    CHECK(scalar[0]->view().checked_as<Int>() == Int{10});
    CHECK(scalar[1]->view().checked_as<Int>() == Int{30});

    const auto bundle =
        eval_node<stdlib::replay_data_frame, ReplayBundle>(bundle_replay_frame(), MAX_DT);
    REQUIRE(bundle.size() == 2);
    CHECK(bundle[0]->as_bundle().at(0).checked_as<Int>() == Int{1});
    CHECK(bundle[0]->as_bundle().at(1).checked_as<Str>() == Str{"one"});
    CHECK_FALSE(bundle[1]->as_bundle().at(0).has_value());
    CHECK(bundle[1]->as_bundle().at(1).checked_as<Str>() == Str{"two"});
}

TEST_CASE("raw frame replay selects revisions per TSD partition and applies "
          "removes")
{
    stdlib::register_standard_operators();
    const auto result = eval_node<stdlib::replay_data_frame, TSD<Str, TS<Int>>>(
        dict_replay_frame(), MIN_ST + TimeDelta{15});
    CHECK_OUTPUT(result, values<Value>(dict_delta<Str, TS<Int>>({{"a", 1}, {"b", 3}}),
                                       dict_delta<Str, TS<Int>>({}, {"a"})));
}

TEST_CASE("raw frame replay accepts an empty canonical frame")
{
    stdlib::register_standard_operators();
    auto frame = scalar_replay_frame();
    frame.table = frame.table->Slice(0, 0);
    CHECK(eval_node<stdlib::replay_data_frame, TS<Int>>(frame, MAX_DT).empty());
}

TEST_CASE("frame backend: the in-memory backend still resolves record/replay "
          "by default")
{
    stdlib::register_standard_operators();
    // Default config = "memory": the frame backend must NOT be selected and
    // the testing (GlobalState) backend continues to serve the names.
    CHECK(record_replay::backend_is({}, record_replay::MEMORY));
    CHECK_OUTPUT(eval_node<stdlib::to_json>(values<Int>(1)), values<Str>(Str{"1"}));
}
