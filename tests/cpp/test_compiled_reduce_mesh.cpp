#include <hgraph/lib/std/operators/impl/higher_order_impl.h>
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
}

TEST_CASE("compiled reduce uses the operator's empty, singleton, ordered and fixed-list rules") {
    stdlib::register_standard_operators();
    constexpr auto ordered = compiled::ReduceOrder::Ordered;
    check_reduce<Book, N, stdlib::add_, compiled::ReduceOrder::Associative>(
        values<Value>(dict_delta<Int,N>({}), dict_delta<Int,N>({{0, 4}}), dict_delta<Int,N>({{1, 7}}),
                      none, dict_delta<Int,N>({}, {1}), dict_delta<Int,N>({}, {0}), dict_delta<Int,N>({{0, 2}})),
        values<Int>(10, none, none, 20), values<Int>(10, 14, 11, none, 24, 20, 22));
    const auto fixed = values<Value>(list_delta<N>({{0, 3}}), list_delta<N>({{2, 5}}), none);
    const auto totals = values<Int>(13, 8, none);
    CHECK_OUTPUT((eval_node<stdlib::reduce_, TSL<N, 3>>(fn<stdlib::add_>(), fixed, Int{10})), totals);
    CHECK_OUTPUT((eval_node<Reduction<TSL<N, 3>, N, stdlib::add_, compiled::ReduceOrder::Associative, true>>(
                      fixed, values<Int>(10, none, none))), totals);
    check_reduce<Book, TS<Str>, Append, ordered>(
        values<Value>(dict_delta<Int,N>({}), dict_delta<Int,N>({{0, 1}, {1, 2}}), none,
                      dict_delta<Int,N>({}, {1}), dict_delta<Int,N>({}, {0})),
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
    const auto links = values<Value>(dict_delta<Int,N>({{2, 1}}), dict_delta<Int,N>({{2, 0}}), none, none);
    const auto keys = values<Value>(set_delta<Int>({2}, {}), none, set_delta<Int>({}, {2}), set_delta<Int>({2}, {}));
    const auto expected = values<Value>(dict_delta<Int,N>({{1, 1}, {2, 3}}), dict_delta<Int,N>({{0, 0}, {2, 2}}, {1}),
                                        dict_delta<Int,N>({}, {0, 2}), dict_delta<Int,N>({{0, 0}, {2, 2}}));
    CHECK_OUTPUT(eval_node<Mesh<false>>(links, keys), expected);
    CHECK_OUTPUT(eval_node<Mesh<true>>(links, keys), expected);
}
