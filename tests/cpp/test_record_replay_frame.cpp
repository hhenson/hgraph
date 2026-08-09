#include <hgraph/lib/std/std_operators.h>
#include <hgraph/lib/testing/check_output.h>
#include <hgraph/lib/testing/eval_node.h>
#include <hgraph/types/graph_wiring.h>
#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/record_replay.h>
#include <hgraph/types/value/table_codec.h>

#include <arrow/api.h>

#include <catch2/catch_test_macros.hpp>

#include <optional>
#include <string>
#include <vector>

// Step 4 of the record/replay/table design record: the Arrow data-frame
// record/replay backend (model record_replay::DATA_FRAME) over the registered
// frame store (P6), plus the replay_const read. Recording happens in one
// graph run and replays in a SECOND run - the store outlives the graph.

namespace
{
    using namespace hgraph;
    using namespace hgraph::testing;

    using ReplayBundle =
        UnNamedTSB<Field<"a", TS<Int>>, Field<"b", TS<Str>>>;

    void require_arrow(const arrow::Status &status)
    {
        if (!status.ok()) { throw std::runtime_error(status.ToString()); }
    }

    template <typename Builder>
    [[nodiscard]] std::shared_ptr<arrow::Array> finish(Builder &builder)
    {
        std::shared_ptr<arrow::Array> result;
        require_arrow(builder.Finish(&result));
        return result;
    }

    [[nodiscard]] Frame scalar_replay_frame()
    {
        arrow::TimestampBuilder date{arrow::timestamp(arrow::TimeUnit::MICRO),
                                     arrow::default_memory_pool()};
        arrow::TimestampBuilder as_of{arrow::timestamp(arrow::TimeUnit::MICRO),
                                      arrow::default_memory_pool()};
        arrow::Int64Builder value;
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
            arrow::schema({
                arrow::field("__date_time__", arrow::timestamp(arrow::TimeUnit::MICRO)),
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
        arrow::Int64Builder a;
        arrow::StringBuilder b;
        for (const auto when : {MIN_ST, MIN_ST + TimeDelta{1}})
        {
            require_arrow(date.Append(when.time_since_epoch().count()));
            require_arrow(as_of.Append((MIN_ST + TimeDelta{10}).time_since_epoch().count()));
        }
        require_arrow(a.AppendValues({1, 2}));
        require_arrow(b.AppendValues({"one", "two"}));
        return Frame{arrow::Table::Make(
            arrow::schema({
                arrow::field("__date_time__", arrow::timestamp(arrow::TimeUnit::MICRO)),
                arrow::field("__as_of__", arrow::timestamp(arrow::TimeUnit::MICRO)),
                arrow::field("a", arrow::int64()),
                arrow::field("b", arrow::utf8())}),
            {finish(date), finish(as_of), finish(a), finish(b)})};
    }

    [[nodiscard]] Frame dict_replay_frame()
    {
        arrow::TimestampBuilder date{arrow::timestamp(arrow::TimeUnit::MICRO),
                                     arrow::default_memory_pool()};
        arrow::TimestampBuilder as_of{arrow::timestamp(arrow::TimeUnit::MICRO),
                                      arrow::default_memory_pool()};
        arrow::BooleanBuilder removed;
        arrow::StringBuilder key;
        arrow::Int64Builder value;
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
            if (item.has_value()) { require_arrow(value.Append(*item)); }
            else { require_arrow(value.AppendNull()); }
        }
        return Frame{arrow::Table::Make(
            arrow::schema({
                arrow::field("__date_time__", arrow::timestamp(arrow::TimeUnit::MICRO)),
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
            return ts;   // eval_node needs an output; the recording is the side effect
        }
    };

    struct ReplayGraph
    {
        [[maybe_unused]] static constexpr auto name = "replay_frame_graph";

        static Port<TS<Int>> compose(Wiring &w)
        {
            return wire<stdlib::replay, TS<Int>>(w, Str{"prices"}, arg<"recordable_id">(Str{"book"}))
                .as<TS<Int>>();
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
}  // namespace

TEST_CASE("frame backend: record writes a bitemporal frame to the store; replay re-emits it")
{
    stdlib::register_standard_operators();
    GlobalContext context;
    record_replay::set_config(context.state().view(),
                              record_replay::Config{.model = std::string{record_replay::DATA_FRAME}});

    // Run 1: record (values at cycles 0, 2 and 3 - gaps preserved).
    (void)eval_node<RecordGraph>(values<Int>(10, none, 30, 40));

    REQUIRE(record_replay::store_contains("book.prices"));
    const Frame recorded = record_replay::store_read("book.prices");
    CHECK(frame_rows(recorded) == 3);

    // Run 2: replay - values re-emitted at the RECORDED times (cycle-aligned).
    CHECK_OUTPUT(eval_node<ReplayGraph>(), values<Int>(10, none, 30, 40));
}

TEST_CASE("frame backend: the recordable id resolves through graph traits at runtime")
{
    stdlib::register_standard_operators();
    GlobalContext context;
    record_replay::set_config(context.state().view(),
                              record_replay::Config{.model = std::string{record_replay::DATA_FRAME}});

    (void)eval_node<TraitRecordGraph>(values<Int>(7));
    CHECK(record_replay::store_contains("desk.fx.orders"));
}

TEST_CASE("frame backend: replay_const_value reads the last row at or before tm")
{
    stdlib::register_standard_operators();
    GlobalContext context;
    const auto state = context.state().view();
    record_replay::set_config(state,
                              record_replay::Config{.model = std::string{record_replay::DATA_FRAME}});

    (void)eval_node<RecordGraph>(values<Int>(10, none, 30, 40));

    const auto *int_meta = scalar_descriptor<Int>::value_meta();
    // Everything <= MAX_DT: the last recorded value.
    CHECK(record_replay::replay_const_value(state, "book.prices", int_meta).view().checked_as<Int>() == Int{40});
    // Cut at the second recorded tick (cycle 2 = MIN_ST + 2).
    CHECK(record_replay::replay_const_value(state, "book.prices", int_meta, MIN_ST + TimeDelta{2})
              .view()
              .checked_as<Int>() == Int{30});
    // Before the first tick: nothing qualifies.
    CHECK_FALSE(record_replay::replay_const_value(state, "book.prices", int_meta, MIN_ST - TimeDelta{1}).has_value());
    // Unknown key: empty.
    CHECK_FALSE(record_replay::replay_const_value(state, "missing.key", int_meta).has_value());
}

TEST_CASE("raw frame replay selects revisions independently of input row order")
{
    stdlib::register_standard_operators();
    const auto result = eval_node<stdlib::replay_data_frame, TS<Int>>(
        scalar_replay_frame(), MIN_ST + TimeDelta{15});
    REQUIRE(result.size() == 2);
    REQUIRE(result[0].has_value());
    REQUIRE(result[1].has_value());
    CHECK(result[0]->view().checked_as<Int>() == Int{10});
    CHECK(result[1]->view().checked_as<Int>() == Int{30});
}

TEST_CASE("raw frame replay uses configured as-of and applies TSB rows")
{
    stdlib::register_standard_operators();
    GlobalContext context;
    record_replay::set_config(
        context.state().view(),
        record_replay::Config{.as_of = MIN_ST + TimeDelta{15}});

    const auto scalar = eval_node<stdlib::replay_data_frame, TS<Int>>(
        scalar_replay_frame(), MAX_DT);
    REQUIRE(scalar.size() == 2);
    CHECK(scalar[0]->view().checked_as<Int>() == Int{10});
    CHECK(scalar[1]->view().checked_as<Int>() == Int{30});

    const auto bundle = eval_node<stdlib::replay_data_frame, ReplayBundle>(
        bundle_replay_frame(), MAX_DT);
    REQUIRE(bundle.size() == 2);
    CHECK(bundle[0]->as_bundle().at(0).checked_as<Int>() == Int{1});
    CHECK(bundle[0]->as_bundle().at(1).checked_as<Str>() == Str{"one"});
    CHECK(bundle[1]->as_bundle().at(0).checked_as<Int>() == Int{2});
    CHECK(bundle[1]->as_bundle().at(1).checked_as<Str>() == Str{"two"});
}

TEST_CASE("raw frame replay selects revisions per TSD partition and applies removes")
{
    stdlib::register_standard_operators();
    const auto result = eval_node<stdlib::replay_data_frame, TSD<Str, TS<Int>>>(
        dict_replay_frame(), MIN_ST + TimeDelta{15});
    CHECK_OUTPUT(result,
                 values<Value>(dict_delta<Str, TS<Int>>({{"a", 1}, {"b", 3}}),
                               dict_delta<Str, TS<Int>>({}, {"a"})));
}

TEST_CASE("raw frame replay accepts an empty canonical frame")
{
    stdlib::register_standard_operators();
    auto frame = scalar_replay_frame();
    frame.table = frame.table->Slice(0, 0);
    CHECK(eval_node<stdlib::replay_data_frame, TS<Int>>(frame, MAX_DT).empty());
}

TEST_CASE("frame backend: the in-memory model still resolves record/replay by default")
{
    stdlib::register_standard_operators();
    // Default config = IN_MEMORY: the frame backend must NOT be selected and
    // the testing (GlobalState) backend continues to serve the names.
    CHECK(record_replay::model_is({}, record_replay::IN_MEMORY));
    CHECK_OUTPUT(eval_node<stdlib::to_json>(values<Int>(1)), values<Str>(Str{"1"}));
}
