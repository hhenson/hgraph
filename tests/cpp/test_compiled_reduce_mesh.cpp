#include <hgraph/lib/std/component.h>
#include <hgraph/lib/std/operators/impl/higher_order_impl.h>
#include <hgraph/runtime/component_checkpoint.h>
#include <hgraph/lib/std/std_operators.h>
#include <hgraph/lib/testing/check_output.h>
#include <hgraph/lib/testing/eval_node.h>
#include <hgraph/types/context_wiring.h>
#include <hgraph/types/generated.h>

#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers_string.hpp>

#include <chrono>
#include <iostream>

namespace {
using namespace hgraph;
using namespace hgraph::testing;
using N = TS<Int>;
using Book = TSD<Int, N>;

template <typename Op, typename Accumulator = N> struct Binary {
    static Port<Accumulator> compose(Wiring &w, Port<Accumulator> a, Port<N> b) {
        return wire<Op>(w, a, b).template as<Accumulator>();
    }
};

struct Append {
    static void eval(In<"lhs", TS<Str>> lhs, In<"rhs", N> rhs, Out<TS<Str>> out) {
        out.set(lhs.value() + std::to_string(rhs.value()));
    }
};

struct DecimalFold {
    static void eval(In<"accumulator", N> accumulator, In<"member", N> member, Out<N> out) {
        REQUIRE(member.modified());
        CHECK(member.delta_value().checked_as<Int>() == member.value());
        out.set(accumulator.value() * 10 + member.value());
    }
};

template <typename Collection, typename Accumulator, typename Op, compiled::ReduceOrder order, bool direct>
struct Reduction {
    static Port<Accumulator> compose(Wiring &w, Port<Collection> collection, Port<Accumulator> zero) {
        if constexpr (direct) {
            auto result = compiled::reduce_(w, collection.erased(), compile_subgraph<Binary<Op, Accumulator>>(w),
                                            zero.erased(), order);
            return Port<Accumulator>{w, std::move(result)};
        } else if constexpr (order == compiled::ReduceOrder::Associative) {
            return wire<stdlib::reduce_>(w, fn<Binary<Op, Accumulator>>(), collection, zero).template as<Accumulator>();
        } else {
            return wire<stdlib::reduce_>(w, fn<Binary<Op, Accumulator>>(), collection, zero,
                                         Bool{false}).template as<Accumulator>();
        }
    }
};

template <typename Collection, typename Op, compiled::ReduceOrder order>
struct ReduceSelectedCollection {
    static Port<N> compose(Wiring &w, Port<Collection> left, Port<Collection> right,
                           Port<TS<Bool>> select_right, Port<N> zero) {
        auto selected = wire<stdlib::if_then_else>(w, select_right, right, left).template as<REF<Collection>>();
        return Reduction<REF<Collection>, N, Op, order, true>::compose(w, selected, zero);
    }
};

template <typename Collection, typename Acc, typename Op, compiled::ReduceOrder order>
void check_reduce(const auto &collection, const auto &zero, const auto &expected) {
    CHECK_OUTPUT((eval_node<Reduction<Collection, Acc, Op, order, false>>(collection, zero)), expected);
    CHECK_OUTPUT((eval_node<Reduction<Collection, Acc, Op, order, true>>(collection, zero)), expected);
}

template <compiled::ReduceOrder order> struct CapturedCombiner {
    static Port<N> compose(Wiring &w, Port<N> accumulator, Port<N> member) {
        const auto bias = wire<stdlib::add_>(w, context::get<N>(w, "reduce_bias"),
                                            context::get<N>(w, "reduce_offset")).as<N>();
        if constexpr (order == compiled::ReduceOrder::Ordered) {
            return wire<stdlib::sub_>(w, wire<stdlib::sub_>(w, accumulator, member), bias).as<N>();
        } else {
            return wire<stdlib::add_>(w, wire<stdlib::add_>(w, accumulator, member), bias).as<N>();
        }
    }
};

template <typename Collection, compiled::ReduceOrder order> struct CapturedReduction {
    static Port<N> compose(Wiring &w, Port<Collection> collection, Port<N> bias) {
        return reduce(w, collection, bias, std::nullopt);
    }

    static Port<N> compose_seeded(Wiring &w, Port<Collection> collection, Port<N> bias, Port<N> zero) {
        return reduce(w, collection, bias, zero.erased());
    }

  private:
    static Port<N> reduce(Wiring &w, Port<Collection> collection, Port<N> bias,
                          std::optional<WiringPortRef> zero) {
        context::scope<"reduce_bias"> capture{w, bias};
        context::scope<"reduce_offset"> offset{w, wire<stdlib::const_, N>(w, Int{0})};
        return {w, compiled::reduce_(w, collection.erased(), compile_subgraph<CapturedCombiner<order>>(w),
                                     std::move(zero), order)};
    }
};

template <typename Collection, compiled::ReduceOrder order> struct SeededCapturedReduction {
    static Port<N> compose(Wiring &w, Port<Collection> collection, Port<N> bias, Port<N> zero) {
        return CapturedReduction<Collection, order>::compose_seeded(w, collection, bias, zero);
    }
};

template <typename Collection>
void check_captured_reduce(const auto &collection, Int first_ordered, Int second_ordered, Int third_ordered) {
    constexpr auto associative = compiled::ReduceOrder::Associative;
    constexpr auto ordered = compiled::ReduceOrder::Ordered;
    const auto bias = values<Int>(10, 20, 30, none);
    const auto zero = values<Int>(100, none, none, none);
    const auto totals = values<Int>(26, 46, 68, none);
    CHECK_OUTPUT((eval_node<CapturedReduction<Collection, associative>>(collection, bias)), totals);
    CHECK_OUTPUT((eval_node<SeededCapturedReduction<Collection, associative>>(collection, bias, zero)), totals);
    CHECK_OUTPUT((eval_node<SeededCapturedReduction<Collection, ordered>>(collection, bias, zero)),
                 values<Int>(first_ordered, second_ordered, third_ordered, none));
}

struct SelectedCapture {
    static void eval(In<"left", N> left, In<"right", N> right, In<"select_right", TS<Bool>> select_right,
                     Out<REF<N>> output) {
        output.set(select_right.value() ? right.reference() : left.reference());
    }
};

struct ReturnCapture {
    static Port<N> compose(Wiring &w, Port<N>, Port<N>) {
        return context::get<N>(w, "reduce_bias");
    }
};

template <compiled::ReduceOrder order> struct ReduceCapturedReference {
    static Port<N> compose(Wiring &w, Port<Book> collection, Port<N> left, Port<N> right,
                           Port<TS<Bool>> select_right, Port<N> zero) {
        const auto selected = wire<SelectedCapture>(w, left, right, select_right).as<REF<N>>();
        context::scope<"reduce_bias"> capture{w, selected};
        return {w, compiled::reduce_(w, collection.erased(), compile_subgraph<ReturnCapture>(w),
                                     zero.erased(), order)};
    }
};

struct StableCapture {
    static void eval(In<"target", N> target, Out<REF<N>> output) {
        if (!output.valid()) { output.set(target.reference()); }
    }
};

struct ReadCapture {
    static Port<N> compose(Wiring &w, Port<N> accumulator, Port<N> member) {
        const auto sum = wire<stdlib::add_>(w, accumulator, member).as<N>();
        return wire<stdlib::add_>(w, sum, context::get<N>(w, "reduce_bias")).as<N>();
    }
};

template <typename Collection> struct ReadCapturedReference {
    static Port<N> compose(Wiring &w, Port<Collection> collection, Port<N> target, Port<N> zero) {
        const auto ref = wire<StableCapture>(w, target).as<REF<N>>();
        context::scope<"reduce_bias"> capture{w, ref};
        return {w, compiled::reduce_(w, collection.erased(), compile_subgraph<ReadCapture>(w),
                                     zero.erased(), compiled::ReduceOrder::Associative)};
    }
};

template <bool direct> struct ReadPeer {
    static Port<N> compose(Wiring &w, NamedPort<"key", N> key, Port<N> target) {
        auto peer = [&]() -> Port<N> {
            if constexpr (direct) {
                return {w, compiled::mesh_peer(w, target.erased(), schema_descriptor<Book>::ts_meta())};
            } else {
                return stdlib::mesh_ref<N>(w, target);
            }
        }();
        auto fallback = wire<stdlib::const_, N>(w, Int{0});
        auto resolved = wire<stdlib::default_>(w, peer, fallback);
        return wire<stdlib::add_>(w, key, resolved).template as<N>();
    }
};

template <bool direct> struct Mesh {
    static Port<Book> compose(Wiring &w, Port<Book> links, Port<TSS<Int>> selected) {
        if constexpr (direct) {
            return {w, compiled::mesh_(w, links.erased(), {}, compile_subgraph<ReadPeer<true>>(w),
                                       MapKeyArg::First, selected.erased())};
        } else {
            return wire<stdlib::mesh_>(w, fn<ReadPeer<false>>(), links, arg<"__keys__">(selected)).template as<Book>();
        }
    }
};
struct Weighted {
    static Port<N> compose(Wiring &w, Port<N> left, Port<N> offset, Port<N> right) {
        auto sum = wire<stdlib::add_>(w, left, right);
        return wire<stdlib::add_>(w, sum, offset).as<N>();
    }
};

template <typename Owner, bool direct> struct Multiple {
    static Port<Book> compose(Wiring &w, Port<Book> left, Port<N> offset, Port<Book> right, Port<TSS<Int>> keys) {
        if constexpr (direct) {
            const std::array inputs{left.erased(), offset.erased(), right.erased()};
            constexpr std::array<std::size_t, 2> split{0, 2};
            if constexpr (std::is_same_v<Owner, stdlib::mesh_>) {
                return {w, compiled::mesh_(w, inputs, split, compile_subgraph<Weighted>(w), MapKeyArg::None, keys.erased())};
            } else {
                return {w, compiled::map_(w, inputs, split, compile_subgraph<Weighted>(w), MapKeyArg::None, keys.erased())};
            }
        } else {
            return wire<Owner>(w, fn<Weighted>(), left, offset, right, arg<"__keys__">(keys)).template as<Book>();
        }
    }
};
struct RecordPair {
    static inline std::vector<std::pair<Int, Int>> visits;
    static void eval(In<"key", N> key, In<"value", N> value) {
        visits.emplace_back(key.value(), value.value());
    }
};
struct VisitPair {
    static void compose(Wiring &w, NamedPort<"key", N> key, Port<N> left, Port<N> offset, Port<N> right) {
        wire<RecordPair>(w, key, Weighted::compose(w, left, offset, right));
    }
};
template <bool direct> struct MultipleSink {
    static Port<Book> compose(Wiring &w, Port<Book> left, Port<N> offset, Port<Book> right, Port<TSS<Int>> keys) {
        if constexpr (direct) {
            const std::array inputs{left.erased(), offset.erased(), right.erased()};
            compiled::map_sink_(w, inputs, std::array<std::size_t, 2>{0, 2}, compile_subgraph<VisitPair>(w),
                                MapKeyArg::First, keys.erased());
        } else {
            wire<stdlib::map_sink_>(w, fn<VisitPair>(), left, offset, right, arg<"__keys__">(keys));
        }
        return left;
    }
};
struct MultipleLists {
    static Port<TSL<N>> compose(Wiring &w, Port<TSL<N>> left, Port<N> offset, Port<TSL<N>> right) {
        const std::array inputs{left.erased(), offset.erased(), right.erased()};
        return {w, compiled::map_(w, inputs, std::array<std::size_t, 2>{0, 2}, compile_subgraph<Weighted>(w))};
    }
};
struct SavedMesh {
    static Port<Book> compose(Wiring &w, Port<Book> links, Port<TSS<Int>> keys) {
        return stdlib::component<Mesh<true>>(w, "mesh_strategy", links, keys);
    }
};

struct ShiftAdd {
    static void eval(In<"accumulator", N> accumulator, In<"member", N> member, Out<N> out) {
        out.set(accumulator.value() * 10 + member.value());
    }
};

struct ShiftAddGraph {
    static Port<N> compose(Wiring &w, Port<N> accumulator, Port<N> member) {
        const auto shifted = wire<stdlib::mul_>(w, accumulator, wire<stdlib::const_, N>(w, Int{10})).as<N>();
        return wire<stdlib::add_>(w, shifted, member).as<N>();
    }
};

struct OnTick {
    static void eval(In<"input", N> input, Out<N> out) {
        if (input.modified()) { out.set(input.value()); }
    }
};

/** Folds only what its arguments ticked, so an argument rebound without a sample is never seen. */
struct ShiftAddTicks {
    static Port<N> compose(Wiring &w, Port<N> accumulator, Port<N> member) {
        return wire<ShiftAdd>(w, wire<OnTick>(w, accumulator), wire<OnTick>(w, member)).as<N>();
    }
};

template <typename Accumulator = N> struct PassAccumulator {
    static Port<Accumulator> compose(Wiring &, Port<Accumulator> accumulator, Port<N>) { return accumulator; }
};

struct PassMember {
    static Port<N> compose(Wiring &, Port<N>, Port<N> member) { return member; }
};

/** A reference to ``left`` for choice 0, to ``right`` for 1, and to nothing otherwise. */
template <typename T> struct SelectTarget {
    static void eval(In<"choice", N> choice,
                     In<"left", REF<T>, InputActivity::Passive, InputValidity::Unchecked> left,
                     In<"right", REF<T>, InputActivity::Passive, InputValidity::Unchecked> right,
                     Out<REF<T>> out) {
        switch (choice.value()) {
        case 0: out.set(left.value()); break;
        case 1: out.set(right.value()); break;
        default: out.set(TimeSeriesReference::empty()); break;
        }
    }
};

template <typename Collection, typename Accumulator, typename Combiner, compiled::ReduceOrder order,
          bool direct = true>
struct ReduceSelected {
    static Port<Accumulator> compose(Wiring &w, Port<N> choice, Port<Collection> left, Port<Collection> right,
                                     Port<Accumulator> zero) {
        const auto selected = wire<SelectTarget<Collection>>(w, choice, left, right).template as<REF<Collection>>();
        if constexpr (direct) {
            return {w, compiled::reduce_(w, selected.erased(), compile_subgraph<Combiner>(w), zero.erased(), order)};
        } else if constexpr (order == compiled::ReduceOrder::Associative) {
            return wire<stdlib::reduce_>(w, fn<Combiner>(), selected, zero).template as<Accumulator>();
        } else {
            return wire<stdlib::reduce_>(w, fn<Combiner>(), selected, zero, Bool{false}).template as<Accumulator>();
        }
    }
};

template <typename Collection, bool direct> struct SumSelected {
    static Port<N> compose(Wiring &w, Port<N> choice, Port<Collection> left, Port<Collection> right) {
        const auto selected = wire<SelectTarget<Collection>>(w, choice, left, right).template as<REF<Collection>>();
        if constexpr (direct) {
            return {w, compiled::reduce_(w, selected.erased(), compile_subgraph<Binary<stdlib::add_>>(w), std::nullopt,
                                         compiled::ReduceOrder::Associative)};
        } else {
            return wire<stdlib::reduce_>(w, fn<Binary<stdlib::add_>>(), selected).template as<N>();
        }
    }
};

/** A = {0: 1, 1: 2} and B = {0: 10, 1: 20, 2: 30}. The reference moves A, B, A;
    B loses its last member while it is not selected; the reference moves B, A, B;
    then B's member 1 ticks while it is selected, and A's while it is not. */
template <typename Collection> void check_repointed_collection(const auto &a, const auto &b) {
    constexpr auto ordered = compiled::ReduceOrder::Ordered;
    constexpr auto associative = compiled::ReduceOrder::Associative;
    const auto choice = values<Int>(0, 1, 0, none, none, 1, 0, 1, none, none);
    const auto zero = values<Int>(100);
    const auto folds = values<Int>(10012, 101230, 10012, none, none, 10120, 10012, 10120, 10121, none);
    const auto sums = values<Int>(3, 60, 3, none, none, 30, 3, 30, 31, none);
    CHECK_OUTPUT((eval_node<ReduceSelected<Collection, N, Binary<ShiftAdd>, ordered>>(choice, a, b, zero)), folds);
    CHECK_OUTPUT((eval_node<ReduceSelected<Collection, N, Binary<ShiftAdd>, ordered, false>>(choice, a, b, zero)),
                 folds);
    CHECK_OUTPUT((eval_node<ReduceSelected<Collection, N, ShiftAddGraph, ordered>>(choice, a, b, zero)), folds);
    CHECK_OUTPUT((eval_node<ReduceSelected<Collection, N, ShiftAddTicks, ordered>>(choice, a, b, zero)), folds);
    CHECK_OUTPUT((eval_node<ReduceSelected<Collection, N, PassMember, ordered>>(choice, a, b, zero)),
                 values<Int>(2, 30, 2, none, none, 20, 2, 20, 21, none));
    CHECK_OUTPUT((eval_node<ReduceSelected<Collection, N, Binary<stdlib::add_>, associative>>(choice, a, b, zero)),
                 sums);
    CHECK_OUTPUT(
        (eval_node<ReduceSelected<Collection, N, Binary<stdlib::add_>, associative, false>>(choice, a, b, zero)),
        sums);
    CHECK_OUTPUT((eval_node<SumSelected<Collection, true>>(choice, a, b)), sums);
    CHECK_OUTPUT((eval_node<SumSelected<Collection, false>>(choice, a, b)), sums);
}

template <typename Zero, typename Combiner> struct ReduceFromSelectedZero {
    static Port<Zero> compose(Wiring &w, Port<Book> collection, Port<N> choice, Port<Zero> left, Port<Zero> right) {
        const auto zero = wire<SelectTarget<Zero>>(w, choice, left, right).template as<REF<Zero>>();
        return {w, compiled::reduce_(w, collection.erased(), compile_subgraph<Combiner>(w), zero.erased(),
                                     compiled::ReduceOrder::Ordered)};
    }
};

using Seed = UnNamedTSB<Field<"v", N>>;
using SeedPair = UnNamedTSB<Field<"a", N>, Field<"b", N>>;

template <typename Zero> struct ObserveOrderedZero {
    static void eval(In<"clock", N>, In<"value", Zero, InputActivity::Passive, InputValidity::Unchecked> value,
                     Out<TS<Str>> out) {
        const TSInputView &ts = value.base();
        if (!ts.valid()) {
            CHECK_FALSE(ts.modified());
            CHECK(ts.last_modified_time() == MIN_DT);
            out.set("invalid");
            return;
        }
        Str trace = ts.modified() ? "tick" : "quiet";
        for (std::size_t slot = 0; slot < ts.data_view().indexed_child_count(); ++slot) {
            const auto child = ts.indexed_child_at(slot);
            trace += child.valid() ? ":" + std::to_string(child.value().checked_as<Int>()) : ":-";
        }
        out.set(trace);
    }
};

template <typename Zero> struct ObserveSelectedZero {
    static Port<TS<Str>> compose(Wiring &w, Port<N> clock, Port<Book> book, Port<N> choice,
                               Port<Zero> left, Port<Zero> right) {
        auto result = ReduceFromSelectedZero<Zero, PassAccumulator<Zero>>::compose(w, book, choice, left, right);
        return wire<ObserveOrderedZero<Zero>>(w, clock, result);
    }
};

struct SelectSeedFields {
    static void eval(In<"choice", N> choice,
                     In<"a", REF<N>, InputActivity::Passive, InputValidity::Unchecked> a,
                     In<"b", REF<N>, InputActivity::Passive, InputValidity::Unchecked> b,
                     Out<REF<SeedPair>> out) {
        out.set(TimeSeriesReference::non_peered(schema_descriptor<SeedPair>::ts_meta(),
            {a.value(), choice.value() == 0 ? b.value() : TimeSeriesReference::empty()}));
    }
};

struct ObservePartialZero {
    static Port<TS<Str>> compose(Wiring &w, Port<N> clock, Port<Book> book, Port<N> choice, Port<N> a, Port<N> b) {
        const auto zero = wire<SelectSeedFields>(w, choice, a, b);
        const Port<SeedPair> result{w, compiled::reduce_(w, book.erased(), compile_subgraph<PassAccumulator<SeedPair>>(w),
                                                        zero.erased(), compiled::ReduceOrder::Ordered)};
        return wire<ObserveOrderedZero<SeedPair>>(w, clock, result);
    }
};

struct PositiveShiftAdd {
    static void eval(In<"accumulator", N> accumulator, In<"member", N> member, Out<N> out) {
        if (member.value() > 0) { out.set(accumulator.value() * 10 + member.value()); }
    }
};

struct DeduplicatedShiftAdd {
    static Port<N> compose(Wiring &w, Port<N> accumulator, Port<N> member) {
        return wire<stdlib::dedup>(w, wire<ShiftAdd>(w, accumulator, member)).as<N>();
    }
};

struct ShiftAddSeed {
    static Port<Seed> compose(Wiring &w, Port<Seed> accumulator, Port<N> member) {
        const auto v = wire<stdlib::getitem_>(w, accumulator, Str{"v"}).as<N>();
        const auto next = wire<ShiftAdd>(w, v, member).as<N>();
        return wire<stdlib::pass_through_node>(w, stdlib::to_tsb<Seed>(w, next)).as<Seed>();
    }
};

struct ReduceSelectedWithCapture {
    static Port<N> compose(Wiring &w, Port<N> choice, Port<Book> left, Port<Book> right, Port<N> bias,
                           Port<N> zero) {
        const auto selected = wire<SelectTarget<Book>>(w, choice, left, right).as<REF<Book>>();
        return CapturedReduction<REF<Book>, compiled::ReduceOrder::Ordered>::compose_seeded(w, selected, bias, zero);
    }
};

struct FirstBook {
    static Port<Book> compose(Wiring &w) {
        return wire<stdlib::const_, Book>(w, stdlib::make_map<Int, Int>({{Int{0}, Int{1}}, {Int{1}, Int{2}}}));
    }
};

struct SecondBook {
    static Port<Book> compose(Wiring &w) {
        return wire<stdlib::const_, Book>(
            w, stdlib::make_map<Int, Int>({{Int{0}, Int{10}}, {Int{1}, Int{20}}, {Int{2}, Int{30}}}));
    }
};

struct EmptyBook {
    static Port<Book> compose(Wiring &w) { return wire<stdlib::const_, Book>(w, stdlib::make_map<Int, Int>({})); }
};

/** Each branch owns the dictionary it publishes. */
struct ReduceSwitchedBook {
    static Port<N> compose(Wiring &w, Port<N> choice, Port<N> zero) {
        const auto book = wire<stdlib::switch_>(w, choice,
                                                stdlib::switch_cases({{Value{Int{0}}, fn<FirstBook>()},
                                                                      {Value{Int{1}}, fn<SecondBook>()},
                                                                      {Value{Int{2}}, fn<EmptyBook>()}}))
                              .as<Book>();
        return Reduction<Book, N, ShiftAdd, compiled::ReduceOrder::Ordered, true>::compose(w, book, zero);
    }
};

struct PassLeftBook {
    static Port<Book> compose(Wiring &, Port<Book> left, Port<Book>) { return left; }
};

struct PassRightBook {
    static Port<Book> compose(Wiring &, Port<Book>, Port<Book> right) { return right; }
};

struct NoBook {
    static void eval(Out<Book>) {}
};

struct PassNoBook {
    static Port<Book> compose(Wiring &w, Port<Book>, Port<Book>) { return wire<NoBook>(w); }
};

/** Each branch forwards an outer dictionary, or one that never ticks. */
struct ReduceSwitchedPassThrough {
    static Port<N> compose(Wiring &w, Port<N> choice, Port<Book> left, Port<Book> right, Port<N> zero) {
        std::vector<SwitchBranchGraph> cases{
            {Value{Int{0}}, compile_subgraph<PassLeftBook>(w)},
            {Value{Int{1}}, compile_subgraph<PassRightBook>(w)},
            {Value{Int{2}}, compile_subgraph<PassNoBook>(w)},
        };
        const std::array inputs{left.erased(), right.erased()};
        const Port<Book> book{w, compiled::switch_(w, choice.erased(), inputs, std::move(cases))};
        return Reduction<Book, N, ShiftAdd, compiled::ReduceOrder::Ordered, true>::compose(w, book, zero);
    }
};

struct SavedSelectedReduction {
    static Port<N> compose(Wiring &w, Port<N> choice, Port<Book> left, Port<Book> right, Port<N> zero) {
        return stdlib::component<ReduceSelected<Book, N, Binary<ShiftAdd>, compiled::ReduceOrder::Ordered>>(
            w, "selected_reduction", choice, left, right, zero);
    }
};

using Shelf = TSD<Int, Book>;

struct Union {
    static Port<Book> compose(Wiring &w, Port<Book> accumulator, Port<Book> member) {
        return wire<stdlib::bit_or>(w, accumulator, member).as<Book>();
    }
};

}

TEST_CASE("compiled reduce uses the operator's empty, singleton, ordered and fixed-list rules") {
    stdlib::register_standard_operators();
    constexpr auto ordered = compiled::ReduceOrder::Ordered;
    check_reduce<Book, N, stdlib::add_, compiled::ReduceOrder::Associative>(
        values<Value>(dict_delta<Int, N>({}), dict_delta<Int, N>({{0, 4}}), dict_delta<Int, N>({{1, 7}}),
                      none, dict_delta<Int, N>({}, {1}), dict_delta<Int, N>({}, {0}), dict_delta<Int, N>({{0, 2}})),
        values<Int>(10, none, none, 20), values<Int>(10, 14, 11, none, 24, 20, 22));
    const auto fixed = values<Value>(list_delta<N>({{0, 3}}), list_delta<N>({{2, 5}}), none);
    const auto totals = values<Int>(13, 8, none);
    CHECK_OUTPUT((eval_node<stdlib::reduce_, TSL<N, 3>>(fn<stdlib::add_>(), fixed, Int{10})), totals);
    CHECK_OUTPUT((eval_node<Reduction<TSL<N, 3>, N, stdlib::add_, compiled::ReduceOrder::Associative, true>>(
                      fixed, values<Int>(10, none, none))), totals);
    check_reduce<Book, TS<Str>, Append, ordered>(
        values<Value>(dict_delta<Int, N>({}), dict_delta<Int, N>({{0, 1}, {1, 2}}), none,
                      dict_delta<Int, N>({}, {1}), dict_delta<Int, N>({}, {0})),
        values<Str>("a", none, "b"), values<Str>("a", "a12", "b12", "b1", "b"));
    check_reduce<TSL<N>, N, stdlib::sub_, ordered>(
        values<Value>(dynamic_list_delta<N>({{0, 1}}, {}, 1), dynamic_list_delta<N>({{1, 2}}, {}, 2),
                      dynamic_list_delta<N>({{2, 3}}, {}, 3)), values<Int>(100), values<Int>(99, 97, 94));
    check_reduce<TSL<N, 4>, N, stdlib::sub_, ordered>(
        values<Value>(list_delta<N>({{0, 1}}), list_delta<N>({{1, 2}, {2, 3}, {3, 4}}),
                      none, list_delta<N>({{0, 10}})),
        values<Int>(100, none, 7), values<Int>(-299, -8, none, 1));
}

TEST_CASE("compiled reduce broadcasts a captured input through both reduction orders") {
    stdlib::register_standard_operators();
    check_captured_reduce<Book>(
        values<Value>(dict_delta<Int, N>({{0, 1}, {1, 2}, {2, 3}}), none,
                      dict_delta<Int, N>({{2, 5}}), none),
        64, 34, 2);
    check_captured_reduce<TSL<N>>(
        values<Value>(dynamic_list_delta<N>({{0, 1}, {1, 2}, {2, 3}}, {}, 3), none,
                      dynamic_list_delta<N>({{2, 5}}), none),
        64, 34, 2);
    check_captured_reduce<TSL<N, 3>>(
        values<Value>(list_delta<N>({{0, 1}, {1, 2}, {2, 3}}), none,
                      list_delta<N>({{2, 5}}), none),
        -24, -44, -66);
}

TEST_CASE("compiled reduce follows each retarget of a dictionary reference") {
    stdlib::register_standard_operators();
    const auto left = values<Value>(dict_delta<Int, N>({{0, 1}, {1, 2}}), none, none, none,
                                    none, none, none, dict_delta<Int, N>({{1, 3}}));
    const auto right = values<Value>(dict_delta<Int, N>({{0, 10}, {1, 20}, {2, 30}}), none, none,
                                     dict_delta<Int, N>({}, {2}), none, none, dict_delta<Int, N>({{1, 21}}));
    const auto select_right = values<Bool>(false, true, false, none, false, true, none, none);
    const auto zero = values<Int>(100);

    CHECK_OUTPUT((eval_node<ReduceSelectedCollection<Book, DecimalFold, compiled::ReduceOrder::Ordered>>(
                     left, right, select_right, zero)),
                 values<Int>(10012, 101230, 10012, none, none, 10120, 10121, none));
    CHECK_OUTPUT((eval_node<ReduceSelectedCollection<Book, stdlib::add_, compiled::ReduceOrder::Associative>>(
                     left, right, select_right, zero)),
                 values<Int>(3, 60, 3, none, none, 30, 31, none));
}

TEST_CASE("compiled ordered reduce follows same-size retargets of a dynamic list reference") {
    stdlib::register_standard_operators();
    using List = TSL<N>;
    const auto left = values<Value>(dynamic_list_delta<N>({{0, 1}, {1, 2}}, {}, 2));
    const auto right = values<Value>(dynamic_list_delta<N>({{0, 3}, {1, 4}}, {}, 2));

    CHECK_OUTPUT((eval_node<ReduceSelectedCollection<List, DecimalFold, compiled::ReduceOrder::Ordered>>(
                     left, right, values<Bool>(false, true, false, true), values<Int>(100))),
                 values<Int>(10012, 10034, 10012, 10034));
}

TEST_CASE("compiled ordered reduce samples a dictionary first selected after publication", "[ordered-reduce-review]") {
    stdlib::register_standard_operators();
    constexpr auto ordered = compiled::ReduceOrder::Ordered;
    const auto left = values<Value>(dict_delta<Int, N>({{0, 1}, {1, 2}}));
    const auto right = values<Value>(dict_delta<Int, N>({{0, 10}, {1, 20}}));
    const auto choice = values<Int>(none, 0, 1);
    const auto zero = values<Int>(none, 100);
    const auto expected = values<Int>(none, 10012, 10120);
    CHECK_OUTPUT((eval_node<ReduceSelected<Book, N, ShiftAddTicks, ordered>>(choice, left, right, zero)), expected);
    CHECK_OUTPUT((eval_node<ReduceSelected<Book, N, Binary<DecimalFold>, ordered>>(choice, left, right, zero)), expected);
}

TEST_CASE("compiled ordered reduce preserves quiet child outputs on collection retarget", "[ordered-reduce-review]") {
    stdlib::register_standard_operators();
    constexpr auto ordered = compiled::ReduceOrder::Ordered;
    const auto choice = values<Int>(0, 1, none);
    const auto zero = values<Int>(100);
    const auto list = values<Value>(dynamic_list_delta<N>({{0, 1}, {1, 2}}, {}, 2));
    const auto partial = values<Value>(dynamic_list_delta<N>({{0, 10}}, {}, 2), none, dynamic_list_delta<N>({{1, 20}}));
    CHECK_OUTPUT((eval_node<ReduceSelected<TSL<N>, N, Binary<ShiftAdd>, ordered>>(choice, list, partial, zero)),
                 values<Int>(10012, none, 10120));

    const auto positive = values<Value>(dict_delta<Int, N>({{0, 5}}));
    const auto negative = values<Value>(dict_delta<Int, N>({{0, -1}}), none, dict_delta<Int, N>({{0, 8}}));
    CHECK_OUTPUT((eval_node<ReduceSelected<Book, N, Binary<PositiveShiftAdd>, ordered>>(choice, positive, negative, zero)),
                 values<Int>(1005, none, 1008));

    const auto equal = values<Value>(dict_delta<Int, N>({{0, 1}, {1, 2}}));
    const auto then_different = values<Value>(dict_delta<Int, N>({{0, 1}, {1, 2}}), none,
                                               dict_delta<Int, N>({{1, 3}}));
    CHECK_OUTPUT((eval_node<ReduceSelected<Book, N, DeduplicatedShiftAdd, ordered>>(choice, equal, then_different, zero)),
                 values<Int>(10012, none, 10013));
}

TEST_CASE("compiled ordered reduce publishes zero when retargeting between empty dictionaries") {
    stdlib::register_standard_operators();
    const auto empty = values<Value>(dict_delta<Int, N>({}));

    CHECK_OUTPUT((eval_node<ReduceSelectedCollection<Book, DecimalFold, compiled::ReduceOrder::Ordered>>(
                     empty, empty, values<Bool>(false, true, true, false), values<Int>(100))),
                 values<Int>(100, 100, none, 100));
}

TEST_CASE("compiled reduce folds a re-pointed collection from the zero in the cycle it moves") {
    stdlib::register_standard_operators();
    check_repointed_collection<Book>(
        values<Value>(dict_delta<Int, N>({{0, 1}, {1, 2}}), none, none, none, none, none, none, none, none,
                      dict_delta<Int, N>({{1, 3}})),
        values<Value>(dict_delta<Int, N>({{0, 10}, {1, 20}, {2, 30}}), none, none, dict_delta<Int, N>({}, {2}),
                      none, none, none, none, dict_delta<Int, N>({{1, 21}})));
    check_repointed_collection<TSL<N>>(
        values<Value>(dynamic_list_delta<N>({{0, 1}, {1, 2}}, {}, 2), none, none, none, none, none, none, none,
                      none, dynamic_list_delta<N>({{1, 3}})),
        values<Value>(dynamic_list_delta<N>({{0, 10}, {1, 20}, {2, 30}}, {}, 3), none, none,
                      dynamic_list_delta<N>({}, {2}, 2), none, none, none, none, dynamic_list_delta<N>({{1, 21}})));
    CHECK_OUTPUT((eval_node<ReduceSelected<TSL<N, 3>, N, Binary<ShiftAdd>, compiled::ReduceOrder::Ordered>>(
                     values<Int>(0, 1, 0), values<Value>(list_delta<N>({1, 2, 3})),
                     values<Value>(list_delta<N>({10, 20, 30})), values<Int>(100))),
                 values<Int>(123, 1230, 123));
}

TEST_CASE("compiled reduce folds what a re-pointed collection changed while it was not selected") {
    stdlib::register_standard_operators();
    const auto left = values<Value>(dict_delta<Int, N>({{0, 1}, {1, 2}}), none, none, none, none, none,
                                    dict_delta<Int, N>({{0, 9}}));
    const auto right = values<Value>(dict_delta<Int, N>({{0, 10}, {1, 20}, {2, 30}}), none, none, none,
                                     dict_delta<Int, N>({}, {2}), none, none, dict_delta<Int, N>({{1, 21}}));
    const auto choice = values<Int>(0, 1, 0, none, none, 1, none, none, 0);
    const auto zero = values<Int>(100);
    CHECK_OUTPUT((eval_node<ReduceSelected<Book, N, Binary<ShiftAdd>, compiled::ReduceOrder::Ordered>>(
                     choice, left, right, zero)),
                 values<Int>(10012, 101230, 10012, none, none, 10120, none, 10121, 10092));
    CHECK_OUTPUT((eval_node<ReduceSelected<Book, N, Binary<stdlib::add_>, compiled::ReduceOrder::Associative>>(
                     choice, left, right, zero)),
                 values<Int>(3, 60, 3, none, none, 30, none, 31, 11));
}

TEST_CASE("compiled reduce publishes the zero while a re-pointed collection is empty or withdrawn") {
    stdlib::register_standard_operators();
    constexpr auto ordered = compiled::ReduceOrder::Ordered;
    const auto zero = values<Int>(100);
    const auto a = values<Value>(dict_delta<Int, N>({{0, 1}, {1, 2}}));
    const auto empty = values<Value>(dict_delta<Int, N>({}));

    // Withdrawn at first; B is empty until it grows while selected.
    const auto b = values<Value>(dict_delta<Int, N>({}), none, none, dict_delta<Int, N>({{0, 10}, {1, 20}}));
    const auto choice = values<Int>(2, 0, 1, none, 2, 0);
    CHECK_OUTPUT((eval_node<ReduceSelected<Book, N, Binary<ShiftAdd>, ordered>>(choice, a, b, zero)),
                 values<Int>(100, 10012, 100, 10120, 100, 10012));
    CHECK_OUTPUT((eval_node<ReduceSelected<Book, N, Binary<stdlib::add_>, compiled::ReduceOrder::Associative>>(
                     choice, a, b, zero)),
                 values<Int>(100, 3, 100, 30, 100, 3));

    // From a chain to an empty dictionary, and to no collection.
    CHECK_OUTPUT((eval_node<ReduceSelected<Book, N, Binary<ShiftAdd>, ordered>>(values<Int>(0, 1, 0, 2, 0), a,
                                                                                 empty, zero)),
                 values<Int>(10012, 100, 10012, 100, 10012));

    // The chain built when the reference leaves an empty collection reads the zero in that cycle.
    CHECK_OUTPUT((eval_node<ReduceSelected<Book, N, ShiftAddTicks, ordered>>(values<Int>(0, 1, 0, 1), empty, a,
                                                                              zero)),
                 values<Int>(100, 10012, 100, 10012));

    // Back from an empty collection, then B loses its last member while selected.
    const auto choices = values<Int>(0, 1, 0);
    CHECK_OUTPUT((eval_node<ReduceSelected<Book, N, Binary<ShiftAdd>, ordered>>(
                     choices,
                     values<Value>(dict_delta<Int, N>({{0, 10}, {1, 20}, {2, 30}}), none, none,
                                   dict_delta<Int, N>({}, {2})),
                     empty, zero)),
                 values<Int>(101230, 100, 101230, 10120));
    CHECK_OUTPUT((eval_node<ReduceSelected<TSL<N>, N, Binary<ShiftAdd>, ordered>>(
                     choices,
                     values<Value>(dynamic_list_delta<N>({{0, 10}, {1, 20}, {2, 30}}, {}, 3), none, none,
                                   dynamic_list_delta<N>({}, {2}, 2)),
                     values<Value>(dynamic_list_delta<N>({}, {}, 0)), zero)),
                 values<Int>(101230, 100, 101230, 10120));

    // Between lists of the same length, one of which empties.
    CHECK_OUTPUT((eval_node<ReduceSelected<TSL<N>, N, Binary<ShiftAdd>, ordered>>(
                     values<Int>(0, 1, 0, 1, none, 0, 1, 0),
                     values<Value>(dynamic_list_delta<N>({{0, 1}, {1, 2}}, {}, 2)),
                     values<Value>(dynamic_list_delta<N>({{0, 4}, {1, 5}}, {}, 2), none, none, none,
                                   dynamic_list_delta<N>({}, {}, 0)),
                     zero)),
                 values<Int>(10012, 10045, 10012, 10045, 100, 10012, 100, 10012));
}

TEST_CASE("compiled ordered reduce folds from a re-pointed zero") {
    stdlib::register_standard_operators();
    const auto book = values<Value>(dict_delta<Int, N>({{0, 1}, {1, 2}}));
    const auto empty = values<Value>(dict_delta<Int, N>({}));
    const auto choice = values<Int>(0, 1, 0);
    const auto first = values<Int>(100);
    const auto second = values<Int>(200);

    CHECK_OUTPUT((eval_node<ReduceFromSelectedZero<N, Binary<ShiftAdd>>>(
                     values<Value>(dict_delta<Int, N>({{0, 1}, {1, 2}}), none, none, none,
                                   dict_delta<Int, N>({{1, 3}})),
                     values<Int>(0, 1, 0, none, none), values<Int>(100, none, none, 101, none), second)),
                 values<Int>(10012, 20012, 10012, 10112, 10113));
    CHECK_OUTPUT((eval_node<ReduceFromSelectedZero<N, Binary<ShiftAdd>>>(empty, choice, first, second)),
                 values<Int>(100, 200, 100));
    CHECK_OUTPUT((eval_node<ReduceFromSelectedZero<N, PassAccumulator<>>>(book, choice, first, second)),
                 values<Int>(100, 200, 100));

    // The zero moves with a chain, the chain empties, and the zero moves while it is empty.
    const auto emptied = values<Value>(dict_delta<Int, N>({{0, 1}, {1, 2}}), none, dict_delta<Int, N>({}, {0, 1}),
                                       none, none, dict_delta<Int, N>({{0, 3}}));
    CHECK_OUTPUT((eval_node<ReduceFromSelectedZero<N, Binary<ShiftAdd>>>(
                     emptied, values<Int>(0, 1, none, 0, 1, none), first, second)),
                 values<Int>(10012, 20012, 200, 100, 200, 2003));

    // A bundle zero is followed field by field.
    const auto first_seed = values<Value>(tsb_delta<Seed>(Int{100}));
    const auto second_seed = values<Value>(tsb_delta<Seed>(Int{200}));
    CHECK_OUTPUT((eval_node<ReduceFromSelectedZero<Seed, ShiftAddSeed>>(
                     emptied, values<Int>(0, 1, none, 0, 1), first_seed, second_seed)),
                 values<Value>(tsb_delta<Seed>(Int{10012}), tsb_delta<Seed>(Int{20012}), tsb_delta<Seed>(Int{200}),
                               tsb_delta<Seed>(Int{100}), tsb_delta<Seed>(Int{200}), tsb_delta<Seed>(Int{2003})));
    CHECK_OUTPUT((eval_node<ReduceFromSelectedZero<Seed, PassAccumulator<Seed>>>(book, choice, first_seed,
                                                                                  second_seed)),
                 values<Value>(tsb_delta<Seed>(Int{100}), tsb_delta<Seed>(Int{200}), tsb_delta<Seed>(Int{100})));
}

TEST_CASE("compiled ordered reduce follows withdrawal of a structural reference zero", "[ordered-reduce-review]") {
    stdlib::register_standard_operators();
    const auto clock = values<Int>(0, 1, 2, 3);
    const auto withdraw = values<Int>(0, 2, none, 1);
    const auto empty = values<Value>(dict_delta<Int, N>({}));
    const auto quiet = values<Value>(dict_delta<Int, N>({{0, 1}}));
    const auto ticking = values<Value>(dict_delta<Int, N>({{0, 1}}), dict_delta<Int, N>({{0, 2}}));
    const auto left = values<Value>(tsb_delta<Seed>(Int{100}), none, tsb_delta<Seed>(Int{101}));
    const auto right = values<Value>(tsb_delta<Seed>(Int{200}));
    const auto expected = values<Str>("tick:100", "invalid", "invalid", "tick:200");
    CHECK_OUTPUT(eval_node<ObserveSelectedZero<Seed>>(clock, empty, withdraw, left, right), expected);
    CHECK_OUTPUT(eval_node<ObserveSelectedZero<Seed>>(clock, quiet, withdraw, left, right), expected);
    CHECK_OUTPUT(eval_node<ObserveSelectedZero<Seed>>(clock, ticking, withdraw, left, right), expected);
    CHECK_OUTPUT(eval_node<ObserveSelectedZero<Seed>>(
                     clock, quiet, values<Int>(0, 1), left, values<Value>(none, none, none, tsb_delta<Seed>(Int{200}))),
                 expected);

    using List = TSL<N, 2>;
    CHECK_OUTPUT(eval_node<ObserveSelectedZero<List>>(clock, quiet, withdraw,
                     values<Value>(list_delta<N>({10, 20}), none, list_delta<N>({11, 21})),
                     values<Value>(list_delta<N>({30, 40}))),
                 values<Str>("tick:10:20", "invalid", "invalid", "tick:30:40"));
    CHECK_OUTPUT(eval_node<ObservePartialZero>(clock, quiet, values<Int>(0, 1),
                     values<Int>(10, none, none, 11), values<Int>(20, none, 21)),
                 values<Str>("tick:10:20", "quiet:10:-", "quiet:10:-", "tick:11:-"));
}

TEST_CASE("compiled ordered reduce keeps a captured input across collection re-points") {
    stdlib::register_standard_operators();
    CHECK_OUTPUT(eval_node<ReduceSelectedWithCapture>(
                     values<Int>(0, 1, 0), values<Value>(dict_delta<Int, N>({{0, 1}, {1, 2}})),
                     values<Value>(dict_delta<Int, N>({{0, 10}, {1, 20}, {2, 30}})), values<Int>(10, none, 5),
                     values<Int>(100)),
                 values<Int>(77, 10, 87));
}

TEST_CASE("compiled ordered reduce follows a collection re-pointed through a switch") {
    stdlib::register_standard_operators();
    CHECK_OUTPUT(eval_node<ReduceSwitchedBook>(values<Int>(0, 1, 2, 0), values<Int>(100)),
                 values<Int>(10012, 101230, 100, 10012));
    CHECK_OUTPUT(eval_node<ReduceSwitchedPassThrough>(
                     values<Int>(0, 1, 2, 0, 1), values<Value>(dict_delta<Int, N>({{0, 1}, {1, 2}})),
                     values<Value>(dict_delta<Int, N>({{0, 10}, {1, 20}, {2, 30}}), none,
                                   dict_delta<Int, N>({}, {2})),
                     values<Int>(100)),
                 values<Int>(10012, 101230, 100, 10012, 10120));
}

TEST_CASE("compiled ordered reduce validates the keys of a re-pointed dictionary") {
    stdlib::register_standard_operators();
    CHECK_THROWS_WITH(
        (eval_node<ReduceSelected<Book, N, Binary<ShiftAdd>, compiled::ReduceOrder::Ordered>>(
            values<Int>(0, 1), values<Value>(dict_delta<Int, N>({{0, 1}, {1, 2}})),
            values<Value>(dict_delta<Int, N>({{0, 10}, {2, 30}})), values<Int>(100))),
        Catch::Matchers::ContainsSubstring("ordered reduce requires contiguous integer keys from zero"));
}

TEST_CASE("compiled ordered reduce restores a checkpoint across collection re-points") {
    stdlib::register_standard_operators();
    const auto a = dict_delta<Int, N>({{0, 1}, {1, 2}});
    const auto b = dict_delta<Int, N>({{0, 10}, {1, 20}, {2, 30}});
    const auto shrink = dict_delta<Int, N>({}, {2});
    const auto saved_reduction = [](auto &&run) {
        GlobalContext context;
        std::optional<ComponentCheckpoint> image;
        configure_component_recovery(context.state().view(), ComponentRecoveryConfig{
            .component_id = "selected_reduction",
            .load = [&] { return image; },
            .commit = [&](const ComponentCheckpoint &saved) { image = saved; },
        });
        run(image);
    };

    // Saved before the reference moves.
    saved_reduction([&](const std::optional<ComponentCheckpoint> &image) {
        const auto cut = MIN_ST + MIN_TD;
        CHECK_OUTPUT(eval_node_with_options<SavedSelectedReduction>({.start_time = MIN_ST, .end_time = cut},
                                                                    values<Int>(0), values<Value>(a),
                                                                    values<Value>(b), values<Int>(100)),
                     values<Int>(10012));
        REQUIRE(image.has_value());
        CHECK_OUTPUT(eval_node_with_options<SavedSelectedReduction>(
                         {.start_time = cut, .end_time = cut + 3 * MIN_TD}, values<Int>(none, 1, none),
                         values<Value>(none, none, none), values<Value>(none, none, shrink),
                         values<Int>(none, none, none)),
                     values<Int>(none, 101230, 10120));
    });

    // Saved after it moved.
    saved_reduction([&](const std::optional<ComponentCheckpoint> &image) {
        const auto cut = MIN_ST + 2 * MIN_TD;
        CHECK_OUTPUT(eval_node_with_options<SavedSelectedReduction>(
                         {.start_time = MIN_ST, .end_time = cut}, values<Int>(0, 1), values<Value>(a, none),
                         values<Value>(b, none), values<Int>(100, none)),
                     values<Int>(10012, 101230));
        REQUIRE(image.has_value());
        CHECK_OUTPUT(eval_node_with_options<SavedSelectedReduction>(
                         {.start_time = cut, .end_time = cut + 3 * MIN_TD}, values<Int>(none, 0, 1),
                         values<Value>(none, none, none),
                         values<Value>(dict_delta<Int, N>({{2, 35}}), none, shrink),
                         values<Int>(none, none, none)),
                     values<Int>(101235, 10012, 10120));
    });
}

TEST_CASE("compiled ordered reduce republishes a passed-through accumulator when the collection re-points") {
    stdlib::register_standard_operators();
    const auto held = dict_delta<Int, N>({{5, 42}});
    CHECK_OUTPUT((eval_node<ReduceSelected<Book, Book, PassAccumulator<Book>, compiled::ReduceOrder::Ordered>>(
                     values<Int>(0, 1, none, 0), values<Value>(dict_delta<Int, N>({{0, 1}})),
                     values<Value>(dict_delta<Int, N>({{0, 2}})), values<Value>(held))),
                 values<Value>(held, held, none, held));
}

TEST_CASE("compiled reduce presents a re-pointed collection's members as they are now") {
    stdlib::register_standard_operators();
    const auto choice = values<Int>(0, 1, 0);
    const auto a = values<Value>(
        dict_delta<Int, Book>({{0, dict_delta<Int, N>({{1, 1}})}, {1, dict_delta<Int, N>({{3, 3}})}}));
    const auto b = values<Value>(
        dict_delta<Int, Book>({{0, dict_delta<Int, N>({{2, 2}})}, {1, dict_delta<Int, N>({{4, 4}})}}));
    const auto zero = values<Value>(dict_delta<Int, N>({}));
    const auto expected = values<Value>(dict_delta<Int, N>({{1, 1}, {3, 3}}),
                                        dict_delta<Int, N>({{2, 2}, {4, 4}}, {1, 3}),
                                        dict_delta<Int, N>({{1, 1}, {3, 3}}, {2, 4}));
    CHECK_OUTPUT((eval_node<ReduceSelected<Shelf, Book, Union, compiled::ReduceOrder::Ordered>>(choice, a, b, zero)),
                 expected);
    CHECK_OUTPUT(
        (eval_node<ReduceSelected<Shelf, Book, Union, compiled::ReduceOrder::Associative>>(choice, a, b, zero)),
        expected);
}

// Explicitly selected, with the growth benchmark in test_reduce.cpp:
//   hgraph_unit_tests '[ordered-reduce-scaling]'
// The reference moves between two lists of the same length on every tick, so
// every link refolds once per tick. The per-element figure must stay flat as
// the lists get longer.
TEST_CASE("compiled ordered reduce refolds a re-pointed list at the same cost per element at any length",
          "[.][ordered-reduce-scaling]") {
    stdlib::register_standard_operators();
    constexpr std::size_t moves = 32;
    for (const std::size_t count : {500, 1000, 2000, 4000}) {
        std::vector<std::optional<Int>> choice;
        for (std::size_t tick = 0; tick < moves; ++tick) { choice.emplace_back(static_cast<Int>(tick % 2)); }
        const auto list_of = [count](Int value) {
            return std::vector<std::optional<Value>>{
                dynamic_list_delta<N>(std::vector<std::optional<Int>>(count, value), {}, count)};
        };
        const auto started = std::chrono::steady_clock::now();
        const auto result = eval_node<ReduceSelected<TSL<N>, N, Binary<stdlib::add_>, compiled::ReduceOrder::Ordered>>(
            choice, list_of(1), list_of(2), values<Int>(0));
        const auto elapsed_ms =
            std::chrono::duration<double, std::milli>(std::chrono::steady_clock::now() - started).count();
        REQUIRE(result.size() == moves);
        REQUIRE(result.back() == static_cast<Int>(2 * count));
        std::cout << "ordered_reduce_repoint count=" << count << " run_ms=" << elapsed_ms
                  << " ns_per_element=" << elapsed_ms * 1.0e6 / static_cast<double>(moves * count) << '\n';
    }
}

TEST_CASE("compiled reduce capture ticks keep the empty and singleton rules") {
    stdlib::register_standard_operators();
    constexpr auto associative = compiled::ReduceOrder::Associative;
    const auto book = values<Value>(dict_delta<Int, N>({}), dict_delta<Int, N>({{0, 4}}), none,
                                    dict_delta<Int, N>({{1, 7}}), dict_delta<Int, N>({}, {1}),
                                    dict_delta<Int, N>({}, {0}), none);
    const auto bias = values<Int>(10, none, 20, none, none, none, 30);
    const auto zero = values<Int>(100, none, none, none, none, none, none);
    CHECK_OUTPUT((eval_node<CapturedReduction<Book, associative>>(book, bias)),
                 values<Int>(none, 4, none, 31, 4, none, none));
    CHECK_OUTPUT((eval_node<SeededCapturedReduction<Book, associative>>(book, bias, zero)),
                 values<Int>(100, 114, 124, 31, 124, 100, none));
}

TEST_CASE("compiled reduce can return a captured reference that changes its source") {
    stdlib::register_standard_operators();
    const auto book = values<Value>(dict_delta<Int, N>({{0, 1}, {1, 2}}), none, none, none);
    const auto left = values<Int>(10, none, none, none);
    const auto right = values<Int>(20, none, 21, none);
    const auto select_right = values<Bool>(false, true, none, false);
    const auto zero = values<Int>(100, none, none, none);
    const auto expected = values<Int>(10, 20, 21, 10);
    CHECK_OUTPUT(eval_node<ReduceCapturedReference<compiled::ReduceOrder::Associative>>(
                     book, left, right, select_right, zero), expected);
    CHECK_OUTPUT(eval_node<ReduceCapturedReference<compiled::ReduceOrder::Ordered>>(
                     book, left, right, select_right, zero), expected);
}

TEST_CASE("compiled associative reduce reads every due captured reference target") {
    stdlib::register_standard_operators();
    const auto target = values<Int>(1000, 1001, 1002, none, none, 1005, 1006, none);
    const auto zero = values<Int>(100, none, none, none, none, none, 200, none);
    const auto expected = values<Int>(3010, 3013, 3020, none, 3022, 3031, 3034, none);
    CHECK_OUTPUT(eval_node<ReadCapturedReference<Book>>(
                     values<Value>(dict_delta<Int, N>({{0, 1}, {1, 2}, {2, 3}, {3, 4}}), none,
                                   dict_delta<Int, N>({{0, 5}}), none, dict_delta<Int, N>({{3, 6}})),
                     target, zero),
                 expected);
    CHECK_OUTPUT(eval_node<ReadCapturedReference<TSL<N>>>(
                     values<Value>(dynamic_list_delta<N>({{0, 1}, {1, 2}, {2, 3}, {3, 4}}, {}, 4), none,
                                   dynamic_list_delta<N>({{0, 5}}), none, dynamic_list_delta<N>({{3, 6}})),
                     target, zero),
                 expected);
}

TEST_CASE("compiled ordered reduce still requires zero for a captured combiner") {
    stdlib::register_standard_operators();
    constexpr auto ordered = compiled::ReduceOrder::Ordered;
    CHECK_THROWS_WITH(
        (eval_node<CapturedReduction<Book, ordered>>(
            values<Value>(dict_delta<Int, N>({{0, 1}, {1, 2}})), values<Int>(10))),
        "reduce_: an ordered fold requires zero");
}

TEST_CASE("compiled mesh follows peer changes and explicit removal and re-add with the ordinary timing") {
    stdlib::register_standard_operators();
    const auto links = values<Value>(dict_delta<Int, N>({{2, 1}}), dict_delta<Int, N>({{2, 0}}), none, none);
    const auto keys = values<Value>(set_delta<Int>({2}, {}), none, set_delta<Int>({}, {2}), set_delta<Int>({2}, {}));
    const auto expected = values<Value>(dict_delta<Int, N>({{1, 1}, {2, 3}}), dict_delta<Int, N>({{0, 0}, {2, 2}}, {1}),
                                        dict_delta<Int, N>({}, {0, 2}), dict_delta<Int, N>({{0, 0}, {2, 2}}));
    CHECK_OUTPUT(eval_node<Mesh<false>>(links, keys), expected);
    CHECK_OUTPUT(eval_node<Mesh<true>>(links, keys), expected);
}

TEST_CASE("a compiled mesh resumes a saved peer subscription without an extra tick") {
    stdlib::register_standard_operators();
    GlobalContext context;
    std::optional<ComponentCheckpoint> image;
    configure_component_recovery(context.state().view(), ComponentRecoveryConfig{
        .component_id = "mesh_strategy",
        .load = [&] { return image; },
        .commit = [&](const ComponentCheckpoint &saved) { image = saved; },
    });
    const auto resume = MIN_ST + MIN_TD;
    CHECK_OUTPUT(eval_node_with_options<SavedMesh>({.start_time = MIN_ST, .end_time = resume},
                     values<Value>(dict_delta<Int, N>({{2, 1}})), values<Value>(set_delta<Int>({2}, {}))),
                 values<Value>(dict_delta<Int, N>({{1, 1}, {2, 3}})));
    REQUIRE(image.has_value());
    CHECK_OUTPUT(eval_node_with_options<SavedMesh>({.start_time = resume, .end_time = resume + 3 * MIN_TD},
                     values<Value>(none, dict_delta<Int, N>({{2, 0}}), none),
                     values<Value>(none, none, set_delta<Int>({}, {2}))),
                 values<Value>(none, dict_delta<Int, N>({{0, 0}, {2, 2}}, {1}), dict_delta<Int, N>({}, {0, 2})));
}

TEST_CASE("compiled map and mesh bind multiple per-key collections in child argument order") {
    stdlib::register_standard_operators();
    const auto left = values<Value>(dict_delta<Int, N>({{1, 2}, {2, 3}}), none, none,
                                    dict_delta<Int, N>({}, {1}), none, dict_delta<Int, N>({{1, 8}}));
    const auto right = values<Value>(dict_delta<Int, N>({{1, 10}}), dict_delta<Int, N>({{2, 20}}), none, none, none, none);
    const auto offset = values<Int>(5, none, 6);
    const auto keys = values<Value>(set_delta<Int>({1}, {}), set_delta<Int>({2}, {}), none, none,
                                    set_delta<Int>({}, {1}), set_delta<Int>({1}, {}));
    const auto expected = values<Value>(dict_delta<Int, N>({{1, 17}}), dict_delta<Int, N>({{2, 28}}),
                                        dict_delta<Int, N>({{1, 18}, {2, 29}}), none, dict_delta<Int, N>({}, {1}),
                                        dict_delta<Int, N>({{1, 24}}));
    CHECK_OUTPUT((eval_node<Multiple<stdlib::map_, false>>(left, offset, right, keys)), expected);
    CHECK_OUTPUT((eval_node<Multiple<stdlib::map_, true>>(left, offset, right, keys)), expected);
    CHECK_OUTPUT((eval_node<Multiple<stdlib::mesh_, false>>(left, offset, right, keys)), expected);
    CHECK_OUTPUT((eval_node<Multiple<stdlib::mesh_, true>>(left, offset, right, keys)), expected);
    RecordPair::visits.clear();
    CHECK_OUTPUT(eval_node<MultipleSink<false>>(left, offset, right, keys), left);
    auto operator_visits = RecordPair::visits;
    std::ranges::sort(operator_visits);
    CHECK(operator_visits == std::vector<std::pair<Int, Int>>{{1, 17}, {1, 18}, {1, 24}, {2, 28}, {2, 29}});
    RecordPair::visits.clear();
    CHECK_OUTPUT(eval_node<MultipleSink<true>>(left, offset, right, keys), left);
    std::ranges::sort(RecordPair::visits);
    CHECK(RecordPair::visits == operator_visits);
    const auto a = values<Value>(dynamic_list_delta<N>({{0, 2}}, {}, 1), dynamic_list_delta<N>({{1, 4}}, {}, 2));
    const auto b = values<Value>(dynamic_list_delta<N>({{0, 3}}, {}, 1), dynamic_list_delta<N>({{1, 5}}, {}, 2));
    const auto listed = values<Value>(dynamic_list_delta<N>({{0, 15}}, {}, 1), dynamic_list_delta<N>({{1, 19}}, {}, 2));
    CHECK_OUTPUT(eval_node<MultipleLists>(a, values<Int>(10), b), listed);
}

TEST_CASE("compiled dictionary map validates its multiplexed indices, collection types and lifecycle keys") {
    stdlib::register_standard_operators();
    Wiring w;
    const auto book = WiringPortRef::null_source(schema_descriptor<Book>::ts_meta());
    const auto other = WiringPortRef::null_source(schema_descriptor<TSD<Str, N>>::ts_meta());
    const auto key_set = WiringPortRef::null_source(schema_descriptor<TSS<Int>>::ts_meta());
    const auto offset = WiringPortRef::null_source(schema_descriptor<N>::ts_meta());
    const auto child = compile_subgraph<Weighted>(w);
    const std::array inputs{book, offset, book};
    const auto wire_map = [&](std::span<const std::size_t> split, std::optional<WiringPortRef> keys) {
        return compiled::map_(w, inputs, split, child, MapKeyArg::None, keys);
    };
    CHECK_THROWS_AS(wire_map({}, key_set), std::invalid_argument);
    CHECK_THROWS_AS(wire_map(std::array<std::size_t, 1>{3}, key_set), std::invalid_argument);
    CHECK_THROWS_AS(wire_map(std::array<std::size_t, 2>{0, 0}, key_set), std::invalid_argument);
    CHECK_THROWS_AS(wire_map(std::array<std::size_t, 2>{0, 1}, key_set), std::invalid_argument);
    constexpr std::array<std::size_t, 2> split{0, 2};
    CHECK_THROWS_AS(wire_map(split, std::nullopt), std::invalid_argument);
    CHECK_THROWS_AS(wire_map(split, offset), std::invalid_argument);
    CHECK_THROWS_AS(compiled::mesh_(w, std::array{book, offset, other}, split, child, MapKeyArg::None, key_set),
                    std::invalid_argument);
}
