// Slice 1: In<TSS<T>> / Out<TSS<T>> authoring selectors, exercised end-to-end —
// a TS<Int> source feeds an accumulator that adds into a TSS<Int> output, whose
// size is read back through a TSS<Int> input.

#include <hgraph/lib/testing/check_output.h>
#include <hgraph/lib/testing/eval_node.h>
#include <hgraph/lib/testing/record_replay.h>
#include <hgraph/runtime/runtime.h>
#include <hgraph/types/graph_wiring.h>
#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/static_node.h>

#include <catch2/catch_test_macros.hpp>

#include <cstdint>

#include <string>

namespace
{
    using namespace hgraph;
    using namespace hgraph::testing;

    // Add each ticked input value into a TSS<Int> output.
    struct Accumulate
    {
        static constexpr auto name = "accumulate";
        static void           eval(In<"in", TS<Int>> in, Out<TSS<Int>> out) { out.add(in.value()); }
    };

    // Emit the size of a TSS<Int> input whenever it ticks.
    struct SetSize
    {
        static constexpr auto name = "set_size";
        static void           eval(In<"s", TSS<Int>> s, Out<TS<Int>> out) { out.set(static_cast<Int>(s.size())); }
    };

    // Emit how many elements were added this cycle, via the typed In<TSS> accessor.
    struct AddedCount
    {
        static constexpr auto name = "added_count";
        static void           eval(In<"s", TSS<Int>> s, Out<TS<Int>> out)
        {
            out.set(static_cast<Int>(s.added().size()));
        }
    };

    struct TupleSetSize
    {
        static constexpr auto name = "tuple_set_size";

        static void eval(In<"s", TSS<HomogeneousTuple<Int>>> s, Out<TS<Int>> out)
        {
            out.set(static_cast<Int>(s.size()));
        }
    };

    [[nodiscard]] Value int_tuple(std::initializer_list<Int> elements)
    {
        const auto element_binding =
            ValuePlanFactory::instance().type_for(scalar_descriptor<Int>::value_meta());
        const auto *tuple_schema =
            scalar_descriptor<HomogeneousTuple<Int>>::value_meta();
        ListBuilder builder{element_binding};
        for (const Int element : elements) { builder.push_back(element); }
        ListStorage storage = builder.build_storage();
        return Value{compact_list_type(element_binding, *tuple_schema), &storage};
    }

    [[nodiscard]] Value tuple_set_delta(
        std::initializer_list<std::initializer_list<Int>> added_values)
    {
        auto &registry = TypeRegistry::instance();
        const auto *tuple_schema =
            scalar_descriptor<HomogeneousTuple<Int>>::value_meta();
        const auto tuple_binding =
            ValuePlanFactory::instance().type_for(tuple_schema);
        const auto *set_schema = registry.set(tuple_schema);
        const auto *delta_schema = registry.un_named_bundle(
            {{"added", set_schema}, {"removed", set_schema}});
        const auto delta_binding =
            ValuePlanFactory::instance().type_for(delta_schema);

        SetBuilder added{tuple_binding};
        for (const auto elements : added_values)
        {
            const Value tuple = int_tuple(elements);
            static_cast<void>(added.insert_copy(tuple.view().data()));
        }
        SetBuilder removed{tuple_binding};
        BundleBuilder delta{delta_binding};
        delta.set("added", added.build());
        delta.set("removed", removed.build());
        return delta.build();
    }

    struct TssGraph
    {
        static constexpr auto name = "tss_graph";
        static void           compose(Wiring &w)
        {
            auto src = wire<stdlib::replay_impl, TS<Int>>(w, Str{"in"});
            auto acc = wire<Accumulate>(w, src);   // -> Port<TSS<Int>>
            auto sz  = wire<SetSize>(w, acc);       // In<TSS<Int>> -> Out<TS<Int>>
            wire<stdlib::dense_record_impl>(w, sz, Str{"out"});
        }
    };

    // replay_set feeds set deltas straight into record_set (delta round-trip).
    struct TssDeltaGraph
    {
        static constexpr auto name = "tss_delta_graph";
        static void           compose(Wiring &w)
        {
            auto src = wire<stdlib::replay_impl, TSS<Int>>(w, Str{"in"});
            wire<stdlib::dense_record_impl>(w, src, Str{"out"});
        }
    };

    // replay_set -> added_count -> record: reads the delta via In<TSS>::delta().
    struct TssAddedCountGraph
    {
        static constexpr auto name = "tss_added_count_graph";
        static void           compose(Wiring &w)
        {
            auto src = wire<stdlib::replay_impl, TSS<Int>>(w, Str{"in"});
            auto cnt = wire<AddedCount>(w, src);
            wire<stdlib::dense_record_impl>(w, cnt, Str{"out"});
        }
    };
}  // namespace

TEST_CASE("tss: Out<TSS> accumulates and In<TSS> reads the growing set")
{
    (void)TypeRegistry::instance().register_scalar<Int>("int");

    GraphBuilder gb = build_graph<TssGraph>();
    testing::set_replay_values<Int>(gb.global_state(), "in", {1, 2, 3});

    GraphExecutorBuilder eb;
    eb.graph_builder(std::move(gb)).start_time(MIN_ST).end_time(MIN_ST + TimeDelta{10});
    GraphExecutorValue ex = eb.make_executor();
    ex.view().run();

    // The set grows {1} -> {1,2} -> {1,2,3}; sizes 1, 2, 3.
    CHECK_OUTPUT(testing::get_recorded_values<Int>(ex.view().graph().global_state(), "out"), {1, 2, 3});
}

TEST_CASE("tss: replay<TSS> -> record<TSS> round-trips set deltas (added/removed)")
{
    (void)TypeRegistry::instance().register_scalar<Int>("int");

    const std::vector<std::optional<Value>> deltas{
        set_delta<Int>({1, 2}, {}),   // add 1,2          -> {1,2}
        set_delta<Int>({3}, {1}),      // add 3, remove 1  -> {2,3}
        set_delta<Int>({}, {2, 3}),    // remove 2,3       -> {}
    };

    GraphBuilder gb = build_graph<TssDeltaGraph>();
    testing::set_replay_deltas(gb.global_state(), "in", deltas);

    GraphExecutorBuilder eb;
    eb.graph_builder(std::move(gb)).start_time(MIN_ST).end_time(MIN_ST + TimeDelta{10});
    GraphExecutorValue ex = eb.make_executor();
    ex.view().run();

    CHECK_OUTPUT(testing::get_recorded_deltas(ex.view().graph().global_state(), "out"),
                 {set_delta<Int>({1, 2}, {}), set_delta<Int>({3}, {1}), set_delta<Int>({}, {2, 3})});
}

TEST_CASE("tss: In<TSS> typed added() exposes this cycle's added elements")
{
    (void)TypeRegistry::instance().register_scalar<Int>("int");

    const std::vector<std::optional<Value>> deltas{
        set_delta<Int>({1, 2}, {}),   // 2 added
        set_delta<Int>({3}, {1}),      // 1 added
        set_delta<Int>({}, {2, 3}),    // 0 added
    };

    GraphBuilder gb = build_graph<TssAddedCountGraph>();
    testing::set_replay_deltas(gb.global_state(), "in", deltas);

    GraphExecutorBuilder eb;
    eb.graph_builder(std::move(gb)).start_time(MIN_ST).end_time(MIN_ST + TimeDelta{10});
    GraphExecutorValue ex = eb.make_executor();
    ex.view().run();

    CHECK_OUTPUT(testing::get_recorded_values<Int>(ex.view().graph().global_state(), "out"), {2, 1, 0});
}

TEST_CASE("tss: tuple keys execute through the public concrete C++ schema")
{
    CHECK_OUTPUT(
        eval_node<TupleSetSize>(values<Value>(tuple_set_delta({{1, 2}, {3, 4}}))),
        values<Int>(2));
}
