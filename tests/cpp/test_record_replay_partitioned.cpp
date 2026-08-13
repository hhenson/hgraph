// RFC 0019 - record/replay for the shapes where a row is not a whole value.
//
// The existing frame-backend tests round trip a TS and a TSB, where one
// recorded row is one value and replay reads it straight through the
// converter. The three shapes here broke that assumption, and each was
// previously either untested end to end or an outright throw:
//
//   - a partitioned recording (TSD), where a row is a KEY plus a value;
//   - a COMPOUND key, which occupies one column per leaf and so cannot be
//     read back as a single cell;
//   - a frame-valued leaf, where one tick is a RUN of recorded rows.

#include <hgraph/lib/std/operators/impl/table_impl.h>
#include <hgraph/lib/std/std_operators.h>
#include <hgraph/lib/std/value_util.h>
#include <hgraph/lib/testing/check_output.h>
#include <hgraph/lib/testing/eval_node.h>
#include <hgraph/types/graph_wiring.h>
#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/record_replay.h>
#include <hgraph/types/value/table_codec.h>
#include <hgraph/types/value/value_builder.h>

#include <arrow/api.h>

#include <catch2/catch_test_macros.hpp>

#include <string>
#include <vector>

namespace
{
    using namespace hgraph;
    using namespace hgraph::testing;
    using namespace std::string_literals;

    using PriceDict   = TSD<Str, TS<Int>>;
    using CompoundKey = Tuple<Int, Str>;
    using LegDict     = TSD<CompoundKey, TS<Int>>;
    using Row         = Bundle<"tests.replay_partitioned::Row", Field<"a", Int>, Field<"b", Int>>;

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

    [[nodiscard]] bool equals(const Frame &lhs, const Frame &rhs)
    {
        return lhs.has_value() && rhs.has_value() && lhs.table->Equals(*rhs.table);
    }

    /** A two-column frame - the payload of the frame-valued leaf. */
    [[nodiscard]] Frame row_frame(std::vector<std::int64_t> a, std::vector<std::int64_t> b)
    {
        arrow::Int64Builder a_builder;
        arrow::Int64Builder b_builder;
        require_arrow(a_builder.AppendValues(a));
        require_arrow(b_builder.AppendValues(b));
        return Frame{arrow::Table::Make(
            arrow::schema({arrow::field("a", arrow::int64()), arrow::field("b", arrow::int64())}),
            {finish(a_builder), finish(b_builder)})};
    }

    /** A recording of ``TSD[tuple[int, str], TS[int]]``: the key is spread
        across ``__key_1_0__`` and ``__key_1_1__``, one column per tuple leaf.
        Hand-built because the eval_node harness authors dict keys from a C++
        key type, and a tuple schema is a tag rather than a value type. */
    [[nodiscard]] Frame compound_key_frame()
    {
        arrow::TimestampBuilder date{arrow::timestamp(arrow::TimeUnit::MICRO), arrow::default_memory_pool()};
        arrow::TimestampBuilder as_of{arrow::timestamp(arrow::TimeUnit::MICRO), arrow::default_memory_pool()};
        arrow::Int64Builder     leg;
        arrow::StringBuilder    ccy;
        arrow::Int64Builder     value;
        for (const auto &[when, leg_no, currency, item] :
             std::vector<std::tuple<DateTime, std::int64_t, std::string, std::int64_t>>{
                 {MIN_ST, 1, "eur", 10}, {MIN_ST + TimeDelta{1}, 2, "usd", 20}})
        {
            require_arrow(date.Append(when.time_since_epoch().count()));
            require_arrow(as_of.Append(when.time_since_epoch().count()));
            require_arrow(leg.Append(leg_no));
            require_arrow(ccy.Append(currency));
            require_arrow(value.Append(item));
        }
        return Frame{arrow::Table::Make(
            arrow::schema({arrow::field("__date_time__", arrow::timestamp(arrow::TimeUnit::MICRO)),
                           arrow::field("__as_of__", arrow::timestamp(arrow::TimeUnit::MICRO)),
                           arrow::field("__key_1_0__", arrow::int64()),
                           arrow::field("__key_1_1__", arrow::utf8()),
                           arrow::field("value", arrow::int64())}),
            {finish(date), finish(as_of), finish(leg), finish(ccy), finish(value)},
            2)};
    }

    template <typename TS_> struct RecordGraph
    {
        [[maybe_unused]] static constexpr auto name = "partitioned_record_graph";

        static Port<TS_> compose(Wiring &w, Port<TS_> ts)
        {
            wire<stdlib::record>(w, ts, Str{"ticks"}, arg<"recordable_id">(Str{"book"}));
            return ts;   // eval_node needs an output; the recording is the side effect
        }
    };

    template <typename TS_> struct ReplayGraph
    {
        [[maybe_unused]] static constexpr auto name = "partitioned_replay_graph";

        static Port<TS_> compose(Wiring &w)
        {
            return wire<stdlib::replay, TS_>(w, Str{"ticks"}, arg<"recordable_id">(Str{"book"}))
                .template as<TS_>();
        }
    };

    /** Replay a recording and record what came back. The echo is the assertion:
        a key rebuilt wrongly re-records under different key columns, so the two
        recordings can only agree if the reassembly is the exact inverse of the
        flattening. */
    template <typename TS_> struct EchoGraph
    {
        [[maybe_unused]] static constexpr auto name = "partitioned_echo_graph";

        static Port<TS_> compose(Wiring &w)
        {
            auto out = wire<stdlib::replay, TS_>(w, Str{"ticks"}, arg<"recordable_id">(Str{"book"}))
                           .template as<TS_>();
            wire<stdlib::record>(w, out, Str{"echo"}, arg<"recordable_id">(Str{"book"}));
            return out;
        }
    };

    /** Records with a ``frame_prefix``, so the frame's columns land under
        qualified names. */
    template <typename TS_> struct PrefixedRecordGraph
    {
        [[maybe_unused]] static constexpr auto name = "prefixed_record_graph";

        static Port<TS_> compose(Wiring &w, Port<TS_> ts)
        {
            wire<stdlib::record>(w, ts, Str{"ticks"}, arg<"recordable_id">(Str{"book"}),
                                 arg<"frame_prefix">(Str{"px_"}));
            return ts;
        }
    };

    /** Both runs under the same DATA_FRAME configuration. */
    void use_frame_backend(GlobalContext &context)
    {
        stdlib::register_standard_operators();
        record_replay::set_config(context.state().view(),
                                  record_replay::Config{.model = std::string{record_replay::DATA_FRAME}});
    }
}  // namespace

TEST_CASE("partitioned record/replay: a TSD round-trips through the store")
{
    GlobalContext context;
    use_frame_backend(context);

    const Value first  = dict_delta<Str, TS<Int>>({{"a"s, 1}, {"b"s, 2}});
    const Value second = dict_delta<Str, TS<Int>>({{"a"s, 3}});
    const Value third  = dict_delta<Str, TS<Int>>({{"b"s, 4}});

    (void)eval_node<RecordGraph<PriceDict>>(values<Value>(first, second, third));

    REQUIRE(record_replay::store_contains("book.ticks"));
    const Frame recorded = record_replay::store_read("book.ticks");
    // One row per (key, tick): two keys on the first tick, one on each after.
    CHECK(frame_rows(recorded) == 4);

    CHECK_OUTPUT(eval_node<ReplayGraph<PriceDict>>(), values<Value>(first, second, third));
}

TEST_CASE("partitioned record/replay: a compound TSD key round-trips through its flattened columns")
{
    GlobalContext context;
    use_frame_backend(context);

    const Frame recorded = compound_key_frame();
    record_replay::store_write("book.ticks", recorded);

    // Replaying this threw outright until the key could be rebuilt from the
    // columns it was flattened into.
    (void)eval_node<EchoGraph<LegDict>>();

    REQUIRE(record_replay::store_contains("book.echo"));
    const Frame echo = record_replay::store_read("book.echo");
    REQUIRE(frame_rows(echo) == 2);

    // The key columns must come back exactly as they went in - a key rebuilt
    // with its components swapped or dropped would not survive this.
    for (const std::string &column : {"__key_1_0__"s, "__key_1_1__"s, "value"s})
    {
        const auto original = recorded.table->GetColumnByName(column);
        const auto returned = echo.table->GetColumnByName(column);
        REQUIRE(returned != nullptr);
        INFO("column: " << column);
        CHECK(returned->chunk(0)->Equals(*original->chunk(0)));
    }
}

TEST_CASE("partitioned record/replay: a frame-valued leaf replays whole frames, not rows")
{
    GlobalContext context;
    use_frame_backend(context);

    // A frame tick records one row PER FRAME ROW, so replay has to group the
    // run of rows sharing a value time back into a single frame. Reading them
    // one at a time would tick three times carrying one row each.
    const Frame first  = row_frame({1, 2, 3}, {10, 20, 30});
    const Frame second = row_frame({4}, {40});

    (void)eval_node<RecordGraph<TS<FrameOf<Row>>>>(values<Frame>(first, second));

    const Frame recorded = record_replay::store_read("book.ticks");
    // Four recorded rows - three from the first tick, one from the second.
    REQUIRE(frame_rows(recorded) == 4);

    const auto replayed = eval_node<ReplayGraph<TS<FrameOf<Row>>>>();
    REQUIRE(replayed.size() == 2);
    REQUIRE(replayed[0].has_value());
    REQUIRE(replayed[1].has_value());
    CHECK(equals(*replayed[0], first));
    CHECK(equals(*replayed[1], second));
}

TEST_CASE("partitioned record/replay: a prefixed frame recording still replays")
{
    GlobalContext context;
    use_frame_backend(context);

    const Frame first  = row_frame({1, 2}, {10, 20});
    const Frame second = row_frame({3}, {30});

    (void)eval_node<PrefixedRecordGraph<TS<FrameOf<Row>>>>(values<Frame>(first, second));

    const Frame recorded = record_replay::store_read("book.ticks");
    // The prefix is what keeps a frame's columns clear of the bitemporal and
    // key columns, so it has to actually reach the recording.
    REQUIRE(recorded.table->GetColumnByName("px_a") != nullptr);
    REQUIRE(recorded.table->GetColumnByName("px_b") != nullptr);
    CHECK(recorded.table->GetColumnByName("a") == nullptr);

    // And replay has to read it back WITHOUT being told the prefix: the
    // options are not stored with the recording, which is why the value
    // columns are resolved positionally rather than by name.
    const auto replayed = eval_node<ReplayGraph<TS<FrameOf<Row>>>>();
    REQUIRE(replayed.size() == 2);
    REQUIRE(replayed[0].has_value());
    REQUIRE(replayed[1].has_value());
    // Round-tripped under the frame's OWN names, not the prefixed ones.
    CHECK(equals(*replayed[0], first));
    CHECK(equals(*replayed[1], second));
}

TEST_CASE("assemble_from_paths: a nested key is rebuilt through the paths it was flattened down")
{
    // The unit behind the replay case above, exercised at a depth the
    // recording layer does not yet produce - the inverse has to be defined by
    // the paths, not by an assumed one level of nesting.
    auto       &registry = TypeRegistry::instance();
    const auto *int_meta = scalar_descriptor<Int>::value_meta();
    const auto *str_meta = scalar_descriptor<Str>::value_meta();
    const auto *inner    = registry.tuple({int_meta, str_meta});
    const auto *outer    = registry.tuple({inner, int_meta});

    // Paths as flatten_value emits them: depth-first, in field order.
    const std::vector<std::vector<std::size_t>> paths{{0, 0}, {0, 1}, {1}};
    const std::vector<Value> leaves{Value{Int{7}}, Value{Str{"eur"}}, Value{Int{9}}};

    const Value key = stdlib::table_ts_detail::assemble_from_paths(outer, paths, leaves);
    REQUIRE(key.has_value());
    CHECK(key.schema() == outer);

    const auto tuple = key.view().as_tuple();
    CHECK(tuple[0].as_tuple()[0].checked_as<Int>() == Int{7});
    CHECK(tuple[0].as_tuple()[1].checked_as<Str>() == Str{"eur"});
    CHECK(tuple[1].checked_as<Int>() == Int{9});

    // An empty cell is not a key component - building a partial key would
    // replay ticks under a key that never existed.
    const std::vector<Value> missing{Value{Int{7}}, Value{}, Value{Int{9}}};
    CHECK_THROWS_AS(stdlib::table_ts_detail::assemble_from_paths(outer, paths, missing),
                    std::invalid_argument);
}
