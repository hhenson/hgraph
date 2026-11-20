// Tests for the in-memory testing toolkit: the replay source and record sink,
// and their shared cycle-aligned List<Any> buffer over the GlobalState.
//
// The end-to-end test wires replay -> add_one -> record, seeds the input buffer
// on the builder, runs the executor, and reads the recorded per-cycle output back
// out of the graph's GlobalState.

#include <hgraph/lib/std/operators/impl/record_replay_memory_impl.h>
#include <hgraph/lib/std/std_operators.h>
#include <hgraph/lib/testing/check_output.h>
#include <hgraph/lib/testing/eval_node.h>
#include <hgraph/lib/testing/record_replay.h>
#include <hgraph/runtime/runtime.h>
#include <hgraph/types/graph_wiring.h>
#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/static_node.h>
#include <hgraph/types/utils/counted_mutex.h>

#include <catch2/catch_test_macros.hpp>

#include <cstdint>
#include <optional>
#include <string>
#include <vector>

namespace
{
    using namespace hgraph;

    struct ForwardAny
    {
        static void eval(In<"in", TS<AnyValue>> in, Out<TS<AnyValue>> out) { out.apply(in.base().value()); }
    };

    struct PublishSetEdits
    {
        static void eval(In<"step", TS<Int>> step, Out<TSS<Int>> out)
        {
            if (step.value() == 0) { out.add(7); }
            else if (step.value() == 2) { out.remove(7); }
            else { out.base().as_set().begin_mutation(out.evaluation_time()).touch(); }
        }
    };

    struct PublishListEdits
    {
        static void eval(In<"step", TS<Int>> step, Out<TSL<TS<Int>>> out)
        {
            if (step.value() == 0) { out[0].set(7); }
            else if (step.value() == 1) { out.resize(0); }
            else { out.base().data_view().begin_mutation(out.evaluation_time()).mark_empty_delta(); }
        }
    };

    using ReplayLeaves = TSB<"ReplayLeaves", Field<"members", TSS<Int>>,
                             Field<"prices", TSL<TS<Int>>>, Field<"stamp", TS<Int>>>;
    using ReplayPair = TSL<ReplayLeaves, 2>;
    using ReplayRows = TSL<ReplayPair>;
    using ReplayBook = TSD<Str, ReplayRows>;

    struct PublishNestedCollectionEdits
    {
        static void eval(In<"step", TS<Int>> step, Out<ReplayBook> out)
        {
            if (step.value() == 5) { out.begin_mutation(out.evaluation_time()).touch(); return; }
            auto rows = out[Str{"book"}];
            if (step.value() == 0) { rows.resize(1); }
            auto leaf = rows[0][0];
            auto members = leaf.field<"members">();
            auto prices = leaf.field<"prices">();
            switch (step.value()) {
                case 0:
                    members.add(7);
                    prices[0].set(11);
                    leaf.field<"stamp">().set(1);
                    break;
                case 1:
                    members.remove(7);
                    prices.resize(0);
                    break;
                case 2: members.begin_mutation(out.evaluation_time()).touch(); break;
                case 3:
                    members.begin_mutation(out.evaluation_time()).touch();
                    prices.base().data_view().begin_mutation(out.evaluation_time()).mark_empty_delta();
                    leaf.field<"stamp">().set(2);
                    break;
                case 4: leaf.field<"stamp">().set(3); break;
                case 6: rows.base().data_view().begin_mutation(out.evaluation_time()).mark_empty_delta(); break;
                case 7: rows[0].base().data_view().begin_mutation(out.evaluation_time()).mark_empty_delta(); break;
                case 8: leaf.base().data_view().begin_mutation(out.evaluation_time()).mark_empty_delta(); break;
            }
        }
    };

    template <typename Collection>
    struct ReplayCollection
    {
        static Port<Collection> compose(Wiring &w)
        {
            return wire<stdlib::replay_impl, Collection>(w, Str{"events"});
        }
    };

    struct ReplayAnyRecordGraph
    {
        static void compose(Wiring &w) {
            const auto source = wire<stdlib::replay_impl, TS<AnyValue>>(w, std::string{"in"});
            wire<stdlib::dense_record_impl>(w, source, std::string{"out"});
        }
    };

    struct AddOne
    {
        static constexpr auto name = "add_one";
        static void           eval(In<"in", TS<Int>> in, Out<TS<Int>> out) { out.set(in.value() + 1); }
    };

    struct ReplayRecordGraph
    {
        static constexpr auto name = "replay_record_graph";
        static void           compose(Wiring &w)
        {
            auto src = wire<stdlib::replay_impl, TS<Int>>(w, std::string{"in"});
            auto inc = wire<AddOne>(w, src);
            wire<stdlib::dense_record_impl>(w, inc, std::string{"out"});
        }
    };

    // A source that initiates its single tick 3 cycles after start using the
    // lightweight SingleShotScheduler (no per-node scheduler state).
    struct DelayedSource
    {
        static constexpr auto name = "delayed_source";
        static void           start(SingleShotScheduler sched) { sched.schedule(TimeDelta{3}); }
        static void           eval(Out<TS<Int>> out) { out.set(Int{99}); }
    };

    struct DelayedGraph
    {
        static constexpr auto name = "delayed_graph";
        static void           compose(Wiring &w)
        {
            auto src = wire<DelayedSource>(w);
            wire<stdlib::dense_record_impl>(w, src, std::string{"out"});
        }
    };

    struct RecordDurationWindow
    {
        static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> samples)
        {
            const auto window = wire<stdlib::to_window>(w, samples, 2 * MIN_TD, TimeDelta{});
            wire<stdlib::record>(w, window, Str{"samples"}, arg<"recordable_id">(Str{"memory_window"}));
            return samples;
        }
    };
}  // namespace

TEST_CASE("memory recovery seeds a window at the inclusive cutoff with original sample times")
{
    using namespace hgraph;
    using namespace hgraph::testing;
    using Window = TSWDuration<Int, 2, 0>;
    stdlib::register_standard_operators();
    GlobalContext context;
    const auto state = context.state().view();
    record_replay::set_config(state, {.backend = std::string{record_replay::MEMORY}});
    (void)eval_node<RecordDurationWindow>(std::vector<std::optional<Int>>{7, 7});

    // Both samples have the same value. The first departs at offset 3,
    // and the second at offset 4. Each recovery is a fresh output.
    const auto times_at = [&](int offset) {
        TSOutput target{schema_descriptor<Window>::ts_meta()};
        const auto output = target.view(MIN_ST + offset * MIN_TD);
        record_replay::publish_recorded_seed(state, "memory_window.samples", output);
        REQUIRE(output.valid());
        REQUIRE(output.modified());
        const auto window = output.as_window();
        std::vector<DateTime> times;
        for (std::size_t i = 0; i < window.size(); ++i) { times.push_back(window.time_at(i)); }
        return times;
    };
    CHECK(times_at(2) == std::vector<DateTime>{MIN_ST, MIN_ST + MIN_TD});
    CHECK(times_at(3) == std::vector<DateTime>{MIN_ST + MIN_TD});
    CHECK(times_at(4).empty());
}

TEST_CASE("testing helpers: set_replay_values / get_recorded_values round-trip")
{
    using namespace hgraph;

    GlobalState gs;
    testing::set_replay_values<Int>(gs.view(), "buf", {1, std::nullopt, 3});

    const auto back = testing::get_recorded_values<Int>(gs.view(), "buf");
    REQUIRE(back.size() == 3);
    CHECK(back[0] == std::optional<Int>{1});
    CHECK(back[1] == std::nullopt);
    CHECK(back[2] == std::optional<Int>{3});
}

TEST_CASE("testing: replay -> add_one -> record captures the per-cycle output")
{
    using namespace hgraph;

    GraphBuilder gb = build_graph<ReplayRecordGraph>();
    // Seed the input on the builder (carried onto the graph at make_graph):
    // tick 1 at cycle 0, no tick at cycle 1, tick 3 at cycle 2.
    testing::set_replay_values<Int>(gb.global_state(), "in", {1, std::nullopt, 3});

    GraphExecutorBuilder eb;
    eb.graph_builder(std::move(gb)).start_time(MIN_ST).end_time(MIN_ST + TimeDelta{10});

    GraphExecutorValue executor = eb.make_executor();
    auto               view     = executor.view();
    view.run();

    // add_one shifts each tick by one; the skipped cycle stays skipped.
    const auto out = testing::get_recorded_values<Int>(view.graph().global_state(), "out");
    REQUIRE(out.size() == 3);
    CHECK(out[0] == std::optional<Int>{2});
    CHECK(out[1] == std::nullopt);
    CHECK(out[2] == std::optional<Int>{4});
}

TEST_CASE("testing: SingleShotScheduler schedules a delayed first tick with no scheduler state")
{
    using namespace hgraph;

    GraphBuilder gb = build_graph<DelayedGraph>();

    GraphExecutorBuilder eb;
    eb.graph_builder(std::move(gb)).start_time(MIN_ST).end_time(MIN_ST + TimeDelta{10});
    GraphExecutorValue executor = eb.make_executor();
    auto               view     = executor.view();
    view.run();

    // The single tick lands at cycle offset 3 (start + 3); 0..2 are skipped.
    const auto out = testing::get_recorded_values<Int>(view.graph().global_state(), "out");
    REQUIRE(out.size() == 4);
    CHECK(out[0] == std::nullopt);
    CHECK(out[1] == std::nullopt);
    CHECK(out[2] == std::nullopt);
    CHECK(out[3] == std::optional<Int>{99});

    // SingleShotScheduler is stateless: the source carries no scheduler component.
    CHECK_FALSE(view.graph().node_at(0).has_scheduler());
}

TEST_CASE("testing: typed TS<AnyValue> recording preserves empty mixed equal and silent cycles") {
    using namespace hgraph;
    using namespace hgraph::testing;
    const auto                              empty   = empty_any();
    const auto                              integer = make_any(Value{Int{0}});
    const auto                              boolean = make_any(Value{false});
    const std::vector<std::optional<Value>> input{empty, integer, boolean, std::nullopt, boolean, empty};
    const auto                              output = eval_node<ForwardAny>(input);
    REQUIRE(output.size() == input.size());
    for (std::size_t i = 0; i < input.size(); ++i) {
        REQUIRE(output[i].has_value() == input[i].has_value());
        if (input[i]) {
            CHECK(output[i]->schema() == TypeRegistry::instance().any());
            CHECK(output[i]->equals(*input[i]));
        }
    }
    CHECK_FALSE(output[0]->as_any().has_value());
    CHECK(output[1]->as_any().get().checked_as<Int>() == 0);
    CHECK_FALSE(output[2]->as_any().get().checked_as<bool>());
}

TEST_CASE("testing: schema-free seeded envelopes retain canonical Any payloads and silence") {
    using namespace hgraph;
    using namespace hgraph::testing;
    GlobalState gs;
    const auto  empty   = empty_any();
    const auto  integer = make_any(Value{Int{7}});
    set_replay_deltas(gs.view(), "buf", {empty, integer, std::nullopt, empty});
    const auto output = get_recorded_deltas(gs.view(), "buf");
    REQUIRE(output.size() == 4);
    REQUIRE(output[0]);
    CHECK(output[0]->schema() == TypeRegistry::instance().any());
    CHECK_FALSE(output[0]->as_any().has_value());
    REQUIRE(output[1]);
    CHECK(output[1]->equals(integer));
    CHECK_FALSE(output[2]);
    REQUIRE(output[3]);
    CHECK_FALSE(output[3]->as_any().has_value());
}

TEST_CASE("observed empty collection publications replay while raw sparse no-op edits stay silent") {
    using namespace hgraph;
    using namespace hgraph::testing;
    auto check = []<typename Collection, typename Source>(const std::vector<std::optional<Value>> &expected) {
        GlobalContext context;
        auto state = context.state().view();
        CHECK_OUTPUT(eval_node<Source>(values<Int>(0, 1, 2, none, 3)), expected);
        state.set("events", Value{state.get("eval_node::out")});
        CHECK_OUTPUT(eval_node<ReplayCollection<Collection>>(), expected);

        // A captured seed keeps concrete entries boxed, including across state copies.
        set_replay_deltas(state, "events", expected, true);
        auto copied = Value{state.get("events")};
        CHECK(copied.schema() == TypeRegistry::instance().mutable_list(TypeRegistry::instance().any()));
        CHECK(dense_buffer_layout(copied.binding()) == DenseBufferLayout::ObservedSeed);
        CHECK_OUTPUT(eval_node<ReplayCollection<Collection>>(), expected);

        // The same empty delta, authored as a raw sparse edit, stays quiet.
        set_replay_deltas(state, "events", {expected.front(), std::nullopt, expected.back()});
        const auto edited = eval_node<ReplayCollection<Collection>>();
        REQUIRE(edited.size() == 1);
        REQUIRE(edited[0]);
        CHECK(edited[0]->equals(*expected.front()));
    };
    SECTION("set") {
        const auto added = set_delta<Int>({7}, {}), removed = set_delta<Int>({}, {7}), empty = set_delta<Int>({}, {});
        check.template operator()<TSS<Int>, PublishSetEdits>(values<Value>(added, empty, removed, none, empty));
    }
    SECTION("growing list") {
        const auto added = dynamic_list_delta<TS<Int>>({{0, 7}}, {}, 1);
        const auto removed = dynamic_list_delta<TS<Int>>({}, {0}, 0), empty = dynamic_list_delta<TS<Int>>({});
        check.template operator()<TSL<TS<Int>>, PublishListEdits>(values<Value>(added, removed, empty, none, empty));
    }
}

TEST_CASE("nested recorded children retain their own publications when a sibling also changes")
{
    using namespace hgraph;
    using namespace hgraph::testing;
    const auto row = [](const Value &leaf, std::optional<std::size_t> size = std::nullopt) {
        const auto pair = list_delta<ReplayLeaves>({{0, leaf}});
        return dict_delta<Str, ReplayRows>({{"book", dynamic_list_delta<ReplayPair>({{0, pair}}, {}, size)}});
    };
    const auto initial = row(tsb_delta<ReplayLeaves>(set_delta<Int>({7}, {}),
                                                     dynamic_list_delta<TS<Int>>({{0, 11}}, {}, 1), 1), 1);
    const auto cleared = row(tsb_delta<ReplayLeaves>(set_delta<Int>({}, {7}),
                                                     dynamic_list_delta<TS<Int>>({}, {0}, 0), none));
    const auto no_membership_change = [] { return set_delta<Int>({}, {}); };
    const auto set_tick = row(tsb_delta<ReplayLeaves>(no_membership_change(), none, none));
    const auto mixed = row(tsb_delta<ReplayLeaves>(no_membership_change(), dynamic_list_delta<TS<Int>>({}), 2));
    const auto stamp_only = [&](Int stamp) { return row(tsb_delta<ReplayLeaves>(none, none, stamp)); };
    // A parent may publish by itself at any level. Its held children stay quiet.
    const auto book_only = dict_delta<Str, ReplayRows>({});
    const auto rows_only = dict_delta<Str, ReplayRows>({{"book", dynamic_list_delta<ReplayPair>({})}});
    const auto pair_only = dict_delta<Str, ReplayRows>({
        {"book", dynamic_list_delta<ReplayPair>({{0, list_delta<ReplayLeaves>({})}})}});
    const auto leaf_only = row(tsb_delta<ReplayLeaves>(none, none, none));
    const auto recorded = values<Value>(initial, cleared, set_tick, none, mixed, book_only, rows_only,
                                        pair_only, leaf_only, stamp_only(3));

    GlobalContext context;
    auto state = context.state().view();
    CHECK_OUTPUT(eval_node<PublishNestedCollectionEdits>(values<Int>(0, 1, 2, none, 3, 5, 6, 7, 8, 4)), recorded);
    state.set("events", Value{state.get("eval_node::out")});
    CHECK_OUTPUT(eval_node<ReplayCollection<ReplayBook>>(), recorded);

    set_replay_deltas(state, "events", recorded, true);
    CHECK_OUTPUT(eval_node<ReplayCollection<ReplayBook>>(), recorded);
    // With no recording provenance the identical empty child edits are quiet,
    // including when their sibling still publishes.
    set_replay_deltas(state, "events", recorded);
    CHECK_OUTPUT(eval_node<ReplayCollection<ReplayBook>>(),
                 values<Value>(initial, cleared, none, none, stamp_only(2), none, none, none, none, stamp_only(3)));
}

TEST_CASE("testing: native Any dense recording replays complete boxes without losing empty ticks") {
    using namespace hgraph;
    using namespace hgraph::testing;
    const auto empty = empty_any(), integer = make_any(Value{Int{0}}), boolean = make_any(Value{false});
    const std::vector<std::optional<Value>> input{empty, integer, std::nullopt, boolean, boolean, empty};
    auto                                    run = [](GraphBuilder builder) {
        GraphExecutorBuilder executor;
        executor.graph_builder(std::move(builder)).start_time(MIN_ST).end_time(MIN_ST + TimeDelta{10});
        auto graph = executor.make_executor();
        graph.view().run();
        return Value{graph.view().graph().global_state().get("out")};
    };
    auto first_builder = build_graph<ReplayAnyRecordGraph>();
    set_replay_deltas(first_builder.global_state(), "in", input);
    const Value seeded{first_builder.global_state().get("in")};
    const auto  recording = run(std::move(first_builder));
    const Value copied{recording.view()};
    CHECK(dense_buffer_layout(copied.binding()) == DenseBufferLayout::Typed);
    CHECK(copied.schema() == TypeRegistry::instance().mutable_list(TypeRegistry::instance().any()));
    GlobalState stored;
    stored.view().set("in", copied);
    auto second_builder = build_graph<ReplayAnyRecordGraph>();
    second_builder.global_state().copy_from(stored.view());
    CHECK(dense_buffer_layout(second_builder.global_state().get("in").binding()) == DenseBufferLayout::Typed);
    const auto replayed = run(std::move(second_builder));
    auto       check    = [&](const Value &buffer) {
        const auto reader = dense_entry_reader(buffer.binding());
        const auto locks  = type_system_lock_count();
        REQUIRE(buffer.as_list().size() == input.size());
        for (std::size_t index = 0; index < input.size(); ++index) {
            const auto delta = reader(buffer.as_list(), index);
            REQUIRE(delta.has_value() == input[index].has_value());
            if (delta) { CHECK(delta->equals(*input[index])); }
        }
        CHECK(type_system_lock_count() == locks);
    };
    check(seeded);     // the prepared seeded reader is also registry-free
    check(recording);  // schema-free seeded replay preserved every original box
    check(replayed);   // bare replay preserved the typed recording through copies

    // Legacy buffers predate the private representation label. Their Any
    // entries are seeded envelopes, including envelopes of empty Any boxes.
    Value legacy{ValuePlanFactory::instance().type_for(TypeRegistry::instance().mutable_list(TypeRegistry::instance().any()))};
    auto  entries = legacy.as_list().begin_mutation();
    for (const auto &value : input) { entries.push_back(value ? make_any(*value).view() : empty_any().view()); }
    CHECK(legacy.binding().record()->implementation_name().empty());
    check(legacy);  // unlabelled envelopes use the same prepared reader
    auto legacy_builder = build_graph<ReplayAnyRecordGraph>();
    legacy_builder.global_state().set("in", legacy);
    check(run(std::move(legacy_builder)));

    // Ordinary unlabelled typed lists retain their original scalar elements.
    const auto  int_binding = ValuePlanFactory::instance().type_for(scalar_descriptor<Int>::value_meta());
    Value       plain{mutable_list_type(int_binding)};
    const Value seven{Int{7}};
    auto        plain_entries = plain.as_list().begin_mutation();
    plain_entries.push_back(seven.view());
    plain_entries.push_back_unset();
    const auto plain_reader = dense_entry_reader(plain.binding());
    const auto plain_locks  = type_system_lock_count();
    const auto plain_value  = plain_reader(plain.as_list(), 0);
    REQUIRE(plain_value);
    CHECK(plain_value->equals(seven));
    CHECK_FALSE(plain_reader(plain.as_list(), 1));
    CHECK(type_system_lock_count() == plain_locks);

    auto                 missing_builder = build_graph<ReplayAnyRecordGraph>();
    GraphExecutorBuilder missing;
    missing.graph_builder(std::move(missing_builder)).start_time(MIN_ST).end_time(MIN_ST + TimeDelta{10});
    auto absent = missing.make_executor();
    absent.view().run();
    CHECK_FALSE(absent.view().graph().global_state().get("out").valid());
}

TEST_CASE("testing: typed empty Bundle recording retains present ticks through copies and prepared reads") {
    using namespace hgraph;
    using namespace hgraph::testing;
    const auto *schema  = TypeRegistry::instance().bundle("EmptyRecordedBundle", {});
    const auto  binding = ValuePlanFactory::instance().type_for(schema);
    REQUIRE(binding.checked_plan().layout.size == 0);
    Value empty{binding};
    Value buffer  = make_dense_buffer(binding);
    auto  entries = buffer.as_list().begin_mutation();
    entries.push_back(empty.view());
    entries.push_back_unset();
    entries.push_back(empty.view());
    Value       copied{buffer.view()};
    GlobalState stored;
    stored.view().set("empty", copied);
    GlobalState restored;
    restored.view().copy_from(stored.view());
    const auto saved = restored.view().get("empty");
    REQUIRE(dense_buffer_layout(saved.binding()) == DenseBufferLayout::Typed);
    const auto reader = dense_entry_reader(saved.binding());
    const auto locks  = type_system_lock_count();
    for (std::size_t i = 0; i < 3; ++i) {
        const auto tick = reader(saved.as_list(), i);
        REQUIRE(tick.has_value() == (i != 1));
        if (tick) {
            CHECK(tick->schema() == schema);
            CHECK(tick->equals(empty));
        }
    }
    CHECK(type_system_lock_count() == locks);
}
