#include <hgraph/lib/std/component.h>
#include <hgraph/lib/std/operators/impl/higher_order_impl.h>
#include <hgraph/runtime/component_checkpoint.h>
#include <hgraph/lib/std/std_operators.h>
#include <hgraph/lib/testing/check_output.h>
#include <hgraph/lib/testing/eval_node.h>
#include <hgraph/types/generated.h>

#include <catch2/catch_test_macros.hpp>

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

template <typename Collection, typename Acc, typename Op, compiled::ReduceOrder order>
void check_reduce(const auto &collection, const auto &zero, const auto &expected) {
    CHECK_OUTPUT((eval_node<Reduction<Collection, Acc, Op, order, false>>(collection, zero)), expected);
    CHECK_OUTPUT((eval_node<Reduction<Collection, Acc, Op, order, true>>(collection, zero)), expected);
}

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
