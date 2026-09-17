// Public native wiring parity for the distributed map call shapes.
#include <hgraph/lib/std/std_operators.h>
#include <hgraph/lib/testing/check_output.h>
#include <hgraph/lib/testing/eval_node.h>
#include <hgraph/runtime/distributed_map_wiring.h>
#include <hgraph/types/static_node.h>
#include <hgraph/types/subgraph_wiring.h>
#include <hgraph/types/service_wiring.h>
#include <hgraph/runtime/push_source_node.h>

#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers_string.hpp>
#include "distributed_worker_recipes.h"

#include <algorithm>
#include <initializer_list>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

namespace
{
    using namespace hgraph;
    using namespace hgraph::distributed;
    using namespace hgraph::testing;
    using Dict = TSD<Str, TS<Int>>;
    using Tag = WiringPortRef::ArgTag;

    Port<void> distribute(Wiring &w, const WiredFn &child,
                          std::initializer_list<WiringPortRef> inputs,
                          std::initializer_list<Str> names = {},
                          std::optional<std::string> key_arg = {},
                          std::string_view recipe = {})
    {
        std::vector<WiringPortRef> ports{inputs};
        std::vector<DistributedMapInput> descriptions;
        descriptions.reserve(ports.size());
        auto name = names.begin();
        for (const auto &port : ports)
        {
            descriptions.push_back({port.schema, port.arg_tag,
                                    name == names.end() ? Str{} : *name++});
        }
        WorkerPoolConfig config;
        config.hosting = WorkerHosting::InProcess;
        config.workers = 3;
        if (!recipe.empty())
        {
            config.hosting = WorkerHosting::Process;
            config.program = HGRAPH_TEST_WORKER_PROGRAM;
        }
        auto plan = prepare_distributed_map_pool(child, descriptions, key_arg, config);
        if (!recipe.empty()) bind_distributed_map_recipe(plan, recipe);
        return wire_distributed_map(w, ports,
                                   std::make_shared<const DistributedMapPlan>(std::move(plan)));
    }

    struct Add
    {
        static void eval(In<"lhs", TS<Int>> lhs, In<"rhs", TS<Int>> rhs, Out<TS<Int>> out)
        { out.set(lhs.value() + rhs.value()); }
    };
    struct AddOne
    {
        static void eval(In<"value", TS<Int>> value, Out<TS<Int>> out)
        { out.set(value.value() + 1); }
    };
    struct ReturnRight
    {
        static Port<TS<Int>> compose(Wiring &, Port<TS<Int>>, Port<TS<Int>> rhs) { return rhs; }
    };
    struct WholeSize
    {
        static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>>, Port<Dict> whole)
        { return wire<stdlib::len_>(w, whole).as<TS<Int>>(); }
    };
    struct KeyOnly
    {
        static void eval(In<"key", TS<Str>> key, Out<TS<Str>> out)
        { out.set(key.value() + "!"); }
    };
    struct CustomKey
    {
        static void eval(In<"symbol", TS<Str>> key, In<"value", TS<Int>> value, Out<TS<Str>> out)
        { out.set(key.value() + ":" + std::to_string(value.value())); }
    };
    struct KeyIsValue
    {
        static void eval(In<"key", TS<Int>> value, Out<TS<Int>> out)
        { out.set(value.value() + 1); }
    };

    struct NamedInputs
    {
        static Port<Dict> compose(Wiring &w, Port<Dict> lhs, Port<Dict> rhs)
        { return distribute(w, fn<Add>(), {rhs.erased(), lhs.erased()}, {"rhs", "lhs"}).as<Dict>(); }
    };
    struct Broadcast
    {
        static Port<Dict> compose(Wiring &w, Port<Dict> lhs, Port<TS<Int>> rhs)
        { return distribute(w, fn<Add>(), {lhs.erased(), rhs.erased()}).as<Dict>(); }
    };
    struct NoKey
    {
        static Port<Dict> compose(Wiring &w, Port<Dict> lhs, Port<Dict> rhs)
        { return distribute(w, fn<ReturnRight>(), {lhs.erased(), rhs.erased().with_arg_tag(Tag::NoKey)}).as<Dict>(); }
    };
    struct PassThrough
    {
        static Port<Dict> compose(Wiring &w, Port<Dict> lhs, Port<Dict> whole)
        { return distribute(w, fn<WholeSize>(), {lhs.erased(), whole.erased().with_arg_tag(Tag::PassThrough)}).as<Dict>(); }
    };
    struct ExplicitKeys
    {
        static Port<Dict> compose(Wiring &w, Port<Dict> values, Port<TSS<Str>> keys)
        { return distribute(w, fn<AddOne>(), {values.erased(), keys.erased()}, {"", "__keys__"}).as<Dict>(); }
    };
    struct KeyOnlyMap
    {
        static Port<TSD<Str, TS<Str>>> compose(Wiring &w, Port<TSS<Str>> keys)
        { return distribute(w, fn<KeyOnly>(), {keys.erased()}, {"__keys__"}).as<TSD<Str, TS<Str>>>(); }
    };
    struct CustomKeyMap
    {
        static Port<TSD<Str, TS<Str>>> compose(Wiring &w, Port<Dict> values)
        { return distribute(w, fn<CustomKey>(), {values.erased()}, {}, "symbol").as<TSD<Str, TS<Str>>>(); }
    };
    struct DisabledKeyMap
    {
        static Port<Dict> compose(Wiring &w, Port<Dict> values)
        { return distribute(w, fn<KeyIsValue>(), {values.erased()}, {}, "").as<Dict>(); }
    };
    template <std::size_t N, typename Child = AddOne>
    struct ListMap
    {
        static Port<TSL<TS<Int>, N>> compose(Wiring &w, Port<TSL<TS<Int>, N>> values)
        { return distribute(w, fn<Child>(), {values.erased()}).template as<TSL<TS<Int>, N>>(); }
    };
    template <typename S>
    struct Identity
    {
        static Port<S> compose(Wiring &, Port<S> value) { return value; }
    };
    template <typename S>
    struct Structured
    {
        static Port<TSD<Str, S>> compose(Wiring &w, Port<TSD<Str, S>> values)
        { return distribute(w, fn<Identity<S>>(), {values.erased()}).template as<TSD<Str, S>>(); }
    };
    std::vector<Int> sink_values;
    struct Sink
    {
        static void eval(In<"value", TS<Int>> value) { sink_values.push_back(value.value()); }
    };
    struct SinkMap
    {
        static Port<Dict> compose(Wiring &w, Port<Dict> values)
        {
            (void)distribute(w, fn<Sink>(), {values.erased()});
            return values;
        }
    };
}

TEST_CASE("dmap shapes: named multiple inputs and scalar broadcast")
{
    stdlib::register_standard_operators();
    CHECK_OUTPUT(eval_node<NamedInputs>(
        values<Value>(dict_delta<Str, TS<Int>>({{"a", 1}, {"b", 2}}),
                      dict_delta<Str, TS<Int>>({{"b", 4}})),
        values<Value>(dict_delta<Str, TS<Int>>({{"a", 10}}),
                      dict_delta<Str, TS<Int>>({{"b", 20}}))),
        values<Value>(dict_delta<Str, TS<Int>>({{"a", 11}}),
                      dict_delta<Str, TS<Int>>({{"b", 24}})));
    CHECK_OUTPUT(eval_node<Broadcast>(
        values<Value>(dict_delta<Str, TS<Int>>({{"a", 1}, {"b", 2}}), none), values<Int>(10, 20)),
        values<Value>(dict_delta<Str, TS<Int>>({{"a", 11}, {"b", 12}}),
                      dict_delta<Str, TS<Int>>({{"a", 21}, {"b", 22}})));
}

TEST_CASE("dmap shapes: no_key and pass_through preserve classification")
{
    stdlib::register_standard_operators();
    CHECK_OUTPUT(eval_node<NoKey>(
        values<Value>(dict_delta<Str, TS<Int>>({{"x", 1}, {"y", 2}})),
        values<Value>(dict_delta<Str, TS<Int>>({{"x", 10}, {"z", 30}}))),
        values<Value>(dict_delta<Str, TS<Int>>({{"x", 10}})));
    CHECK_OUTPUT(eval_node<PassThrough>(
        values<Value>(dict_delta<Str, TS<Int>>({{"a", 1}, {"b", 2}}), none),
        values<Value>(dict_delta<Str, TS<Int>>({{"p", 10}, {"q", 20}}),
                      dict_delta<Str, TS<Int>>({}, {"p"}))),
        values<Value>(dict_delta<Str, TS<Int>>({{"a", 2}, {"b", 2}}),
                      dict_delta<Str, TS<Int>>({{"a", 1}, {"b", 1}})));
}

TEST_CASE("dmap shapes: explicit keys retain initially invalid children")
{
    stdlib::register_standard_operators();
    CHECK_OUTPUT(eval_node<ExplicitKeys>(
        values<Value>(dict_delta<Str, TS<Int>>({{"a", 1}, {"ignored", 9}}),
                      dict_delta<Str, TS<Int>>({{"late", 3}}), none),
        values<Value>(set_delta<Str>({"a", "late"}, {}), none, set_delta<Str>({}, {"a"}))),
        values<Value>(dict_delta<Str, TS<Int>>({{"a", 2}}),
                      dict_delta<Str, TS<Int>>({{"late", 4}}),
                      dict_delta<Str, TS<Int>>({}, {"a"})));
}

TEST_CASE("dmap shapes: key-only custom-key and disabled key injection")
{
    stdlib::register_standard_operators();
    CHECK_OUTPUT(eval_node<KeyOnlyMap>(values<Value>(set_delta<Str>({"a", "b"}, {}), set_delta<Str>({}, {"a"}))),
        values<Value>(dict_delta<Str, TS<Str>>({{"a", "a!"}, {"b", "b!"}}),
                      dict_delta<Str, TS<Str>>({}, {"a"})));
    CHECK_OUTPUT(eval_node<CustomKeyMap>(values<Value>(dict_delta<Str, TS<Int>>({{"a", 2}}))),
                 values<Value>(dict_delta<Str, TS<Str>>({{"a", "a:2"}})));
    CHECK_OUTPUT(eval_node<DisabledKeyMap>(values<Value>(dict_delta<Str, TS<Int>>({{"a", 4}}))),
                 values<Value>(dict_delta<Str, TS<Int>>({{"a", 5}})));
}

TEST_CASE("dmap shapes: fixed and dynamic lists preserve sparse indices")
{
    stdlib::register_standard_operators();
    CHECK_OUTPUT(eval_node<ListMap<2>>(values<Value>(list_delta<TS<Int>>({1, 2}),
                                                     list_delta<TS<Int>>({std::nullopt, 3}))),
                 values<Value>(list_delta<TS<Int>>({2, 3}), list_delta<TS<Int>>({std::nullopt, 4})));
    CHECK_OUTPUT(eval_node<ListMap<unbounded_tsl_size>>(values<Value>(
        dynamic_list_delta<TS<Int>>({{0, 1}}), dynamic_list_delta<TS<Int>>({{2, 3}}))),
        values<Value>(dynamic_list_delta<TS<Int>>({{0, 2}}), dynamic_list_delta<TS<Int>>({{2, 4}})));
}

TEST_CASE("dmap shapes: structured elements retain recursive deltas")
{
    stdlib::register_standard_operators();
    SECTION("set")
    {
        const auto inputs = values<Value>(dict_delta<Str, TSS<Int>>({{"a", set_delta<Int>({1, 2}, {})}}),
            dict_delta<Str, TSS<Int>>({{"a", set_delta<Int>({3}, {1})}}), dict_delta<Str, TSS<Int>>({}, {"a"}));
        CHECK_OUTPUT(eval_node<Structured<TSS<Int>>>(inputs), inputs);
    }
    SECTION("dictionary")
    {
        using S = TSD<Str, TS<Int>>;
        const auto inputs = values<Value>(dict_delta<Str, S>({{"a", dict_delta<Str, TS<Int>>({{"x", 1}})}}),
            dict_delta<Str, S>({{"a", dict_delta<Str, TS<Int>>({}, {"x"})}}), dict_delta<Str, S>({}, {"a"}));
        CHECK_OUTPUT(eval_node<Structured<S>>(inputs), inputs);
    }
    SECTION("fixed list")
    {
        using S = TSL<TS<Int>, 2>;
        const auto inputs = values<Value>(dict_delta<Str, S>({{"a", list_delta<TS<Int>>({1, std::nullopt})}}),
            dict_delta<Str, S>({{"a", list_delta<TS<Int>>({std::nullopt, 2})}}), dict_delta<Str, S>({}, {"a"}));
        CHECK_OUTPUT(eval_node<Structured<S>>(inputs), inputs);
    }
    SECTION("partial bundle")
    {
        using S = TSB<"DistributedRow", Field<"value", TS<Int>>, Field<"label", TS<Str>>>;
        const auto inputs = values<Value>(dict_delta<Str, S>({{"a", tsb_delta<S>(Int{1}, std::nullopt)}}),
            dict_delta<Str, S>({{"a", tsb_delta<S>(std::nullopt, Str{"one"})}}), dict_delta<Str, S>({}, {"a"}));
        CHECK_OUTPUT(eval_node<Structured<S>>(inputs), inputs);
    }
}

TEST_CASE("dmap shapes: outputless child executes and tears down")
{
    stdlib::register_standard_operators();
    sink_values.clear();
    const auto inputs = values<Value>(dict_delta<Str, TS<Int>>({{"a", 1}, {"b", 2}}),
                                     dict_delta<Str, TS<Int>>({{"a", 3}}), dict_delta<Str, TS<Int>>({}, {"b"}));
    CHECK_OUTPUT(eval_node<SinkMap>(inputs), inputs);
    std::sort(sink_values.begin(), sink_values.end());
    CHECK(sink_values == std::vector<Int>{1, 2, 3});
}


namespace
{
    template <typename T>
    void check_scalar_round_trip(const T &value)
    {
        const auto inputs = values<Value>(dict_delta<Str, TS<T>>({{"a", value}}), none,
                                          dict_delta<Str, TS<T>>({}, {"a"}),
                                          dict_delta<Str, TS<T>>({{"a", value}}));
        CHECK_OUTPUT(eval_node<Structured<TS<T>>>(inputs), inputs);
    }
    struct RefForward
    {
        static void eval(In<"value", REF<Dict>> value, Out<REF<Dict>> out) { out.set(value.value()); }
    };
    struct RefInputMap
    {
        static Port<Dict> compose(Wiring &w, Port<Dict> values)
        {
            auto reference = wire<RefForward>(w, values);
            return distribute(w, fn<AddOne>(), {reference.erased()}).as<Dict>();
        }
    };
    struct Select
    {
        static Port<REF<TS<Int>>> compose(Wiring &w, Port<TS<Str>> lookup, Port<Dict> rows)
        { return wire<stdlib::getitem_>(w, rows, lookup).as<REF<TS<Int>>>(); }
    };
    struct RefOutputMap
    {
        static Port<Dict> compose(Wiring &w, Port<TSD<Str, TS<Str>>> lookup, Port<Dict> rows)
        {
            return distribute(w, fn<Select>(), {lookup.erased(), rows.erased().with_arg_tag(Tag::PassThrough)}).as<Dict>();
        }
    };
    struct SumWindow
    {
        static Port<TS<Int>> compose(Wiring &w, Port<TSW<Int, 3, 1>> window)
        { return wire<stdlib::sum_>(w, window).as<TS<Int>>(); }
    };
    struct WithWindow
    {
        static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> value, Port<TSW<Int, 3, 1>> window)
        { return wire<stdlib::add_>(w, value, wire<SumWindow>(w, window)).as<TS<Int>>(); }
    };
    struct WindowInputMap
    {
        static Port<Dict> compose(Wiring &w, Port<Dict> values, Port<TS<Int>> ticks)
        {
            auto window = wire<stdlib::to_window>(w, ticks, Int{3}, Int{1});
            return distribute(w, fn<WithWindow>(), {values.erased(), window.erased()}).as<Dict>();
        }
    };
    struct MakeWindow
    {
        static Port<TSW<Int, 3, 1>> compose(Wiring &w, Port<TS<Int>> value)
        { return wire<stdlib::to_window>(w, value, Int{3}, Int{1}).as<TSW<Int, 3, 1>>(); }
    };
    struct WindowOutputMap
    {
        static Port<Dict> compose(Wiring &w, Port<Dict> values)
        {
            auto windows = distribute(w, fn<MakeWindow>(), {values.erased()});
            return wire<stdlib::map_>(w, fn<SumWindow>(), windows).as<Dict>();
        }
    };
}

TEST_CASE("dmap shapes: scalar and temporal families preserve values")
{
    stdlib::register_standard_operators();
    check_scalar_round_trip(Bool{false});
    check_scalar_round_trip(Int{-1099511627776});
    check_scalar_round_trip(Float{3.125});
    check_scalar_round_trip(Str{"hello\0unicode", 13});
    check_scalar_round_trip(Bytes{std::string{"\0\xff", 2}});
    check_scalar_round_trip(DateTime{TimeDelta{123456}});
    check_scalar_round_trip(TimeDelta{-9876});
    check_scalar_round_trip(CivilDateTime::from_epoch_microseconds(123456));
    check_scalar_round_trip(Period{0, 2, -3});
    check_scalar_round_trip(ZoneId{"Europe/London"});
    check_scalar_round_trip(InstantRange{MIN_ST, MIN_ST + TimeDelta{123}});
}

TEST_CASE("dmap shapes: REF input values follow target ticks and removals")
{
    stdlib::register_standard_operators();
    CHECK_OUTPUT(eval_node<RefInputMap>(values<Value>(dict_delta<Str, TS<Int>>({{"a", 1}}),
        dict_delta<Str, TS<Int>>({{"a", 2}, {"b", 3}}), dict_delta<Str, TS<Int>>({}, {"a"}))),
        values<Value>(dict_delta<Str, TS<Int>>({{"a", 2}}),
            dict_delta<Str, TS<Int>>({{"a", 3}, {"b", 4}}), dict_delta<Str, TS<Int>>({}, {"a"})));
}

TEST_CASE("dmap shapes: REF child results are materialized across rebinding")
{
    stdlib::register_standard_operators();
    CHECK_OUTPUT(eval_node<RefOutputMap>(
        values<Value>(dict_delta<Str, TS<Str>>({{"left", "a"}, {"right", "b"}}),
            dict_delta<Str, TS<Str>>({{"left", "b"}}), dict_delta<Str, TS<Str>>({}, {"right"})),
        values<Value>(dict_delta<Str, TS<Int>>({{"a", 1}, {"b", 2}}), none, dict_delta<Str, TS<Int>>({{"b", 3}}))),
        values<Value>(dict_delta<Str, TS<Int>>({{"left", 1}, {"right", 2}}),
            dict_delta<Str, TS<Int>>({{"left", 2}}), dict_delta<Str, TS<Int>>({{"left", 3}}, {"right"})));
}

TEST_CASE("dmap shapes: broadcast windows retain history for late children")
{
    stdlib::register_standard_operators();
    CHECK_OUTPUT(eval_node<WindowInputMap>(
        values<Value>(dict_delta<Str, TS<Int>>({{"a", 10}}), none, dict_delta<Str, TS<Int>>({{"b", 20}}), none),
        values<Int>(1, 2, 3, 4)),
        values<Value>(dict_delta<Str, TS<Int>>({{"a", 11}}), dict_delta<Str, TS<Int>>({{"a", 13}}),
            dict_delta<Str, TS<Int>>({{"a", 16}, {"b", 26}}), dict_delta<Str, TS<Int>>({{"a", 19}, {"b", 29}})));
}

TEST_CASE("dmap shapes: child windows retain parent-side history and eviction")
{
    stdlib::register_standard_operators();
    CHECK_OUTPUT(eval_node<WindowOutputMap>(values<Value>(dict_delta<Str, TS<Int>>({{"a", 1}}),
        dict_delta<Str, TS<Int>>({{"a", 2}}), dict_delta<Str, TS<Int>>({{"a", 3}}), dict_delta<Str, TS<Int>>({{"a", 4}}))),
        values<Value>(dict_delta<Str, TS<Int>>({{"a", 1}}), dict_delta<Str, TS<Int>>({{"a", 3}}),
            dict_delta<Str, TS<Int>>({{"a", 6}}), dict_delta<Str, TS<Int>>({{"a", 9}})));
}

namespace
{
    struct Index
    {
        static void eval(In<"ndx", TS<Int>> index, In<"value", TS<Int>> value, Out<TS<Int>> out)
        { out.set(index.value() * 100 + value.value()); }
    };
    template <std::size_t N>
    struct WholeList
    {
        static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> value, Port<TSL<TS<Int>, N>> whole)
        { return wire<stdlib::add_>(w, value, wire<stdlib::sum_>(w, whole)).template as<TS<Int>>(); }
    };
    template <std::size_t N>
    struct ListPairs
    {
        static Port<TSL<TS<Int>, N>> compose(Wiring &w, Port<TSL<TS<Int>, N>> lhs,
                                           Port<TSL<TS<Int>, N>> rhs, Port<TS<Int>> offset)
        {
            auto pairs = distribute(w, fn<Add>(), {lhs.erased(), rhs.erased()});
            return distribute(w, fn<Add>(), {pairs.erased(), offset.erased()}).template as<TSL<TS<Int>, N>>();
        }
    };
    template <std::size_t N>
    struct ListIndex
    {
        static Port<TSL<TS<Int>, N>> compose(Wiring &w, Port<TSL<TS<Int>, N>> values)
        { return distribute(w, fn<Index>(), {values.erased()}).template as<TSL<TS<Int>, N>>(); }
    };
    template <std::size_t N>
    struct ListPassThrough
    {
        static Port<TSL<TS<Int>, N>> compose(Wiring &w, Port<TSL<TS<Int>, N>> values,
                                           Port<TSL<TS<Int>, N>> whole)
        {
            return distribute(w, fn<WholeList<N>>(),
                {values.erased(), whole.erased().with_arg_tag(Tag::PassThrough)}).template as<TSL<TS<Int>, N>>();
        }
    };
    template <std::size_t N>
    Value list_change(Int left, Int right)
    {
        if constexpr (N == unbounded_tsl_size) return dynamic_list_delta<TS<Int>>({{0, left}, {1, right}});
        else return list_delta<TS<Int>>({left, right});
    }
    template <std::size_t N>
    void check_list_shapes()
    {
        CHECK_OUTPUT(eval_node<ListPairs<N>>(values<Value>(list_change<N>(1, 2)),
            values<Value>(list_change<N>(10, 20)), values<Int>(100)), values<Value>(list_change<N>(111, 122)));
        CHECK_OUTPUT(eval_node<ListIndex<N>>(values<Value>(list_change<N>(1, 2))), values<Value>(list_change<N>(1, 102)));
        CHECK_OUTPUT(eval_node<ListPassThrough<N>>(values<Value>(list_change<N>(1, 2)),
            values<Value>(list_change<N>(10, 20))), values<Value>(list_change<N>(31, 32)));
    }
}

TEST_CASE("dmap shapes: fixed and dynamic lists pair broadcast inject indices and pass whole")
{
    stdlib::register_standard_operators();
    check_list_shapes<2>();
    check_list_shapes<unbounded_tsl_size>();
}


namespace
{
    struct ProcessMultipleInputs
    {
        static Port<Dict> compose(Wiring &w, Port<Dict> lhs, Port<Dict> rhs, Port<TS<Int>> offset)
        {
            return distribute(w, fn<hgraph_test::PreparedAdd>(), {lhs.erased(), rhs.erased(), offset.erased()},
                              {}, {}, hgraph_test::prepared_add_name).as<Dict>();
        }
    };
    struct ProcessKeyOnly
    {
        static Port<TSD<Str, TS<Str>>> compose(Wiring &w, Port<TSS<Str>> keys)
        {
            return distribute(w, fn<hgraph_test::PreparedKeyOnly>(), {keys.erased()}, {"__keys__"},
                              {}, hgraph_test::prepared_keys_name).as<TSD<Str, TS<Str>>>();
        }
    };
    struct ProcessBundles
    {
        static Port<TSD<Str, hgraph_test::PreparedRow>> compose(Wiring &w, Port<TSD<Str, hgraph_test::PreparedRow>> values)
        {
            return distribute(w, fn<hgraph_test::PreparedBundleIdentity>(), {values.erased()}, {}, {},
                              hgraph_test::prepared_bundle_name).as<TSD<Str, hgraph_test::PreparedRow>>();
        }
    };
    PreparedWorkerPlan unused_factory(std::size_t, std::size_t) { return {}; }
    // Distinct behavior prevents linker identical-code folding on Windows.
    PreparedWorkerPlan other_unused_factory(std::size_t, std::size_t)
    { throw std::logic_error("the second registration fixture must not execute"); }
}

TEST_CASE("dmap shapes: native prepared recipes execute multiple inputs in worker processes")
{
    stdlib::register_standard_operators();
    hgraph_test::register_distributed_test_recipes();
    CHECK_OUTPUT(eval_node<ProcessMultipleInputs>(
        values<Value>(dict_delta<Str, TS<Int>>({{"a", 1}, {"b", 2}}), none),
        values<Value>(dict_delta<Str, TS<Int>>({{"a", 10}, {"b", 20}}), none), values<Int>(100, 200)),
        values<Value>(dict_delta<Str, TS<Int>>({{"a", 111}, {"b", 122}}),
                      dict_delta<Str, TS<Int>>({{"a", 211}, {"b", 222}})));
}

TEST_CASE("dmap shapes: native prepared recipes execute key-only worker graphs")
{
    stdlib::register_standard_operators();
    hgraph_test::register_distributed_test_recipes();
    CHECK_OUTPUT(eval_node<ProcessKeyOnly>(values<Value>(set_delta<Str>({"a", "b"}, {}), set_delta<Str>({}, {"a"}))),
        values<Value>(dict_delta<Str, TS<Str>>({{"a", "a!"}, {"b", "b!"}}), dict_delta<Str, TS<Str>>({}, {"a"})));
}

TEST_CASE("dmap shapes: native prepared recipes preserve partial bundles in worker processes")
{
    stdlib::register_standard_operators();
    hgraph_test::register_distributed_test_recipes();
    using S = hgraph_test::PreparedRow;
    const auto inputs = values<Value>(dict_delta<Str, S>({{"a", tsb_delta<S>(Int{1}, std::nullopt)}}),
        dict_delta<Str, S>({{"a", tsb_delta<S>(std::nullopt, Str{"one"})}}), dict_delta<Str, S>({}, {"a"}));
    CHECK_OUTPUT(eval_node<ProcessBundles>(inputs), inputs);
}

TEST_CASE("prepared worker recipes validate identities partition bounds and malformed encodings")
{
    using Catch::Matchers::ContainsSubstring;
    const Str name{"recipe: \"quoted\" factory"};
    register_prepared_worker_recipe(name, {&unused_factory});
    const auto *original = prepared_worker_recipe(name);
    REQUIRE(original != nullptr);
    register_prepared_worker_recipe(name, {&unused_factory});
    register_prepared_worker_recipe("another prepared recipe", {&other_unused_factory});
    CHECK(prepared_worker_recipe(name) == original);
    CHECK_THROWS_WITH(register_prepared_worker_recipe(name, {&other_unused_factory}), ContainsSubstring("different factory"));
    CHECK_THROWS(register_prepared_worker_recipe("", {&unused_factory}));
    CHECK_THROWS(register_prepared_worker_recipe("incomplete", {}));
    CHECK_THROWS(prepared_worker_recipe_key(name, 0, 0));
    CHECK_THROWS(prepared_worker_recipe_key(name, 3, 3));
    CHECK(prepared_worker_recipe_key(name, 1, 3).find(name) != Str::npos);
    for (const Str &encoded : {Str{"@hgraph-prepared:1:999:x:0:1"},
                               Str{"@hgraph-prepared:1:1:x:0:0"},
                               Str{"@hgraph-prepared:1:1:x:-1:3"},
                               Str{"@hgraph-prepared:1:1:x:1:1"},
                               Str{"@hgraph-prepared:1:1:x:0:1:trailing"}})
    {
        Str executable{"host"};
        Str argument = Str{worker_recipe_flag} + encoded;
        char *argv[]{executable.data(), argument.data()};
        CHECK_THROWS_WITH(run_worker_if_requested(2, argv), ContainsSubstring("prepared recipe"));
    }
}


TEST_CASE("dmap shapes: dynamic list truncation recreates child state")
{
    stdlib::register_standard_operators();
    CHECK_OUTPUT((eval_node<ListMap<unbounded_tsl_size, hgraph_test::RunningTotalNode>>(values<Value>(
        dynamic_list_delta<TS<Int>>({{0, 1}, {1, 2}, {2, 3}}),
        dynamic_list_delta<TS<Int>>({}, {1, 2}),
        dynamic_list_delta<TS<Int>>({{1, 4}, {2, 5}}),
        dynamic_list_delta<TS<Int>>({{0, 6}})))),
        values<Value>(dynamic_list_delta<TS<Int>>({{0, 1}, {1, 2}, {2, 3}}),
            dynamic_list_delta<TS<Int>>({}, {1, 2}),
            dynamic_list_delta<TS<Int>>({{1, 4}, {2, 5}}),
            dynamic_list_delta<TS<Int>>({{0, 7}})));
}

namespace
{
    struct InvalidMemberSource
    {
        static void eval(In<"event", TS<Int>> event, Out<Dict> out)
        {
            if (event.value() == 1) (void)out[Str{"a"}];
            else if (event.value() == 2) out[Str{"a"}].set(3);
            else (void)out.erase(Str{"a"});
        }
    };
    struct CountMembers
    {
        static void eval(In<"key", TS<Str>>, In<"whole", Dict> whole, Out<TS<Int>> out)
        { out.set(static_cast<Int>(whole.size())); }
    };
    struct InvalidMembershipMap
    {
        static Port<Dict> compose(Wiring &w, Port<TS<Int>> event)
        {
            auto whole = wire<InvalidMemberSource>(w, event);
            auto keys = wire<stdlib::const_, TSS<Str>>(w, stdlib::make_set<Str>({"only"}));
            return distribute(w, fn<CountMembers>(),
                {whole.erased().with_arg_tag(Tag::PassThrough), keys.erased()}, {"", "__keys__"}).as<Dict>();
        }
    };
}

TEST_CASE("dmap shapes: pass_through preserves initially invalid dictionary membership")
{
    stdlib::register_standard_operators();
    CHECK_OUTPUT(eval_node<InvalidMembershipMap>(values<Int>(1, 2, 3)),
        values<Value>(dict_delta<Str, TS<Int>>({{"only", 1}}), dict_delta<Str, TS<Int>>({{"only", 1}}),
                      dict_delta<Str, TS<Int>>({{"only", 0}})));
}

namespace
{
    Port<TS<Int>> captured_outer_port;
    struct CapturedChild
    {
        static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> value)
        { return wire<Add>(w, value, captured_outer_port); }
    };
    struct ForbiddenPushChild
    {
        static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> value)
        {
            auto source = make_push_source_node(*schema_descriptor<TS<Int>>::ts_meta());
            const auto index = w.add_node(std::type_index(typeid(ForbiddenPushChild)), std::move(source),
                                          std::span<const WiringPortRef>{}, Value{});
            return wire<Add>(w, value, Port<void>{w, index}).as<TS<Int>>();
        }
    };
    struct NestedPushChild
    {
        static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> value)
        { return nested_<ForbiddenPushChild>(w, value).as<TS<Int>>(); }
    };
    struct UnavailableService
    {
        static constexpr std::string_view name{"dmap_prepared_unavailable_service"};
        using output_schema = TS<Int>;
    };
    struct ServiceChild
    {
        static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> value)
        { (void)wire<UnavailableService>(w, service::path("parent-only")); return value; }
    };
    template <typename Child>
    struct InvalidWorkerMap
    {
        static Port<Dict> compose(Wiring &w, Port<Dict> values)
        {
            captured_outer_port = wire<stdlib::const_, TS<Int>>(w, Int{10});
            return distribute(w, fn<Child>(), {values.erased()}).template as<Dict>();
        }
    };
    struct IsolatedStateChild
    {
        static void eval(In<"value", TS<Int>> value, GlobalStateView state, Out<TS<Int>> out)
        {
            if (state.contains("parent_only")) throw std::logic_error("worker inherited parent runtime state");
            if (!state.get_as<bool>("worker_configured")) throw std::logic_error("worker configuration was not preserved");
            const Int previous = state.contains("out") ? state.get_as<Int>("out") : 0;
            state.set("out", Value{previous + value.value()});
            out.set(state.get_as<Int>("out"));
        }
    };
    struct ConfigureIsolatedState
    {
        static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> value)
        {
            if (w.operator_state().contains("parent_only")) throw std::logic_error("worker inherited parent wiring state");
            w.operator_state().set("worker_configured", Value{true});
            return wire<IsolatedStateChild>(w, value);
        }
    };
    struct IsolatedStateMap
    {
        static Port<Dict> compose(Wiring &w, Port<Dict> values)
        { return distribute(w, fn<ConfigureIsolatedState>(), {values.erased()}).as<Dict>(); }
    };
}

TEST_CASE("dmap shapes: prepared workers reject external dependencies before execution")
{
    stdlib::register_standard_operators();
    using Catch::Matchers::ContainsSubstring;
    auto input = values<Value>(dict_delta<Str, TS<Int>>({{"a", 1}}));
    const auto isolated = ContainsSubstring("dmap_: the child cannot be wired as an isolated worker");
    CHECK_THROWS_WITH(eval_node<InvalidWorkerMap<CapturedChild>>(input), isolated);
    CHECK_THROWS_WITH(eval_node<InvalidWorkerMap<ForbiddenPushChild>>(input), isolated && ContainsSubstring("push sources are disabled"));
    CHECK_THROWS_WITH(eval_node<InvalidWorkerMap<NestedPushChild>>(input), isolated && ContainsSubstring("push sources are disabled"));
    CHECK_THROWS_WITH(eval_node<InvalidWorkerMap<ServiceChild>>(input), isolated && ContainsSubstring("service"));
}

TEST_CASE("dmap shapes: worker state is isolated from parent and transport staging")
{
    stdlib::register_standard_operators();
    GlobalState parent;
    parent.view().set("parent_only", Value{Int{123}});
    parent.view().set("out", Value{Int{99}});
    GlobalContext context{parent};
    CHECK_OUTPUT(eval_node<IsolatedStateMap>(values<Value>(dict_delta<Str, TS<Int>>({{"a", 2}}),
                                                          dict_delta<Str, TS<Int>>({{"a", 3}}))),
                 values<Value>(dict_delta<Str, TS<Int>>({{"a", 2}}), dict_delta<Str, TS<Int>>({{"a", 5}})));
    CHECK(parent.view().get_as<Int>("out") == 99);
    CHECK_FALSE(parent.view().contains("worker_configured"));
}
