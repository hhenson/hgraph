// Operator call normalization: named arguments, defaults and variadic keyword
// inputs follow the same Python-style calling rules at the native wiring boundary.

#include <hgraph/lib/testing/check_output.h>
#include <hgraph/lib/testing/eval_node.h>
#include <hgraph/lib/testing/record_replay.h>
#include <hgraph/types/operator_type_resolution.h>
#include <hgraph/lib/std/std_nodes.h>
#include <hgraph/lib/std/std_operators.h>
#include <hgraph/lib/std/operators/convert_target.h>
#include <hgraph/lib/std/operators/impl/conversion_impl.h>
#include <hgraph/lib/std/value_util.h>
#include <hgraph/runtime/runtime.h>
#include <hgraph/types/graph_wiring.h>
#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/operator_dispatch.h>
#include <hgraph/types/static_node.h>
#include <hgraph/types/time_series/ts_delta.h>
#include <hgraph/types/type_pattern.h>
#include <hgraph/types/type_resolution.h>

#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers_string.hpp>

#include <array>
#include <cstdint>
#include <optional>
#include <span>
#include <string>
#include <vector>

namespace
{
    using namespace hgraph;
    using namespace hgraph::testing;

    struct add_ : Operator<"add", In<"lhs", TsVar<"S">>, In<"rhs", TsVar<"S">>, Out<TsVar<"S">>>
    {
    };

    struct add_ints
    {
        static constexpr auto name = "add_ints";

        static void eval(In<"lhs", TS<Int>> lhs, In<"rhs", TS<Int>> rhs, Out<TS<Int>> out)
        {
            out.set(lhs.value() + rhs.value());
        }
    };

    [[nodiscard]] WiringArg ts_arg(const TSValueTypeMetaData *schema)
    {
        WiringArg arg;
        arg.kind        = WiringArg::Kind::TimeSeries;
        arg.port.schema = schema;
        return arg;
    }
}  // namespace

// Positional fill -> *args overflow -> named -> defaults -> **kwargs.
namespace
{
    using namespace hgraph;

    struct scale_default_ : Operator<"scale_default", In<"ts", TS<Int>>, Scalar<"factor", Int>, Out<TS<Int>>>
    {
    };
    struct scale_node
    {
        static constexpr auto name = "scale_node";
        static void eval(In<"ts", TS<Int>> ts, Scalar<"factor", Int> factor, Out<TS<Int>> out)
        {
            out.set(ts.value() * factor.value());
        }
        static auto defaults()
        {
            return std::tuple{arg<"factor">(Int{3})};
        }
    };

    struct kw_sum_ : Operator<"kw_sum", In<"base", TS<Int>>, VarKwIn<"kwargs">, Out<TS<Int>>>
    {
    };
    // issue #247 pending-label probes
    struct label_probe_
    {
        static constexpr auto name = "label_probe";
        static void eval(In<"ts", TS<Int>> ts, Out<TS<Int>> out) { out.set(ts.value()); }
    };
    struct other_probe_
    {
        static constexpr auto name = "other_probe";
        static void eval(In<"ts", TS<Int>> ts, Out<TS<Int>> out) { out.set(ts.value()); }
    };
    // Issue #224 (review): VarKwIn<Name, Schema> retains the declared pack
    // pattern; dispatch matches it against the synthesized pack of the
    // supplied keywords and rejects on mismatch.
    struct typed_kw_ : Operator<"typed_kw",
                                VarKwIn<"kwargs", UnNamedTSB<Field<"x", TS<Int>>>>,
                                Out<TS<Int>>>
    {
    };
    struct typed_kw_impl
    {
        static constexpr auto name = "typed_kw_impl";
        static Port<TS<Int>>  compose(Wiring &w,
                                      VarKwIn<"kwargs", UnNamedTSB<Field<"x", TS<Int>>>> rest)
        {
            REQUIRE(rest.size() == 1);
            return Port<TS<Int>>{w, rest[0].second};
        }
    };
    struct kw_sum_impl
    {
        static constexpr auto name = "kw_sum_impl";
        static Port<TS<Int>>  compose(Wiring &, Port<TS<Int>> base, VarKwIn<"kwargs"> rest)
        {
            static_cast<void>(rest);
            return base;
        }
    };

    // Whether a port still carries a REF, using only the public registry.
    [[nodiscard]] inline bool is_reference_port(const WiringPortRef &port)
    {
        return port.schema != TypeRegistry::instance().dereference(port.schema);
    }

    // A TYPED **kwargs collector declares a pack schema, and a REF field in
    // that pack is an explicit request for the reference token. The deref rule
    // applies to what carries no such declaration, so it must not overrule one.
    struct ref_kw_ : Operator<"ref_kw",
                              VarKwIn<"kwargs", UnNamedTSB<Field<"x", REF<TS<Int>>>>>,
                              Out<TS<Int>>>
    {
    };
    struct ref_kw_impl
    {
        static constexpr auto name             = "ref_kw_impl";
        inline static bool    saw_reference    = false;
        static Port<TS<Int>>  compose(Wiring &w,
                                      VarKwIn<"kwargs", UnNamedTSB<Field<"x", REF<TS<Int>>>>> rest)
        {
            REQUIRE(rest.size() == 1);
            saw_reference = is_reference_port(rest[0].second);
            return Port<TS<Int>>{w, rest[0].second};
        }
    };

    // The untyped counterpart over the same source: nothing declares a
    // reference, so the collector receives the value.
    struct plain_kw_ : Operator<"plain_kw", VarKwIn<"kwargs">, Out<TS<Int>>>
    {
    };
    struct plain_kw_impl
    {
        static constexpr auto name          = "plain_kw_impl";
        inline static bool    saw_reference = true;
        static Port<TS<Int>>  compose(Wiring &w, VarKwIn<"kwargs"> rest)
        {
            REQUIRE(rest.size() == 1);
            saw_reference = is_reference_port(rest[0].second);
            return Port<TS<Int>>{w, rest[0].second};
        }
    };

    // if_then_else publishes a reference; each collector sees it per its own
    // declaration.
    struct RefKwGraph
    {
        [[maybe_unused]] static constexpr auto name = "ref_kw_graph";

        static Port<TS<Int>> compose(Wiring &w, Port<TS<Bool>> choose_lhs, Port<TS<Int>> lhs,
                                     Port<TS<Int>> rhs)
        {
            auto selected = wire<stdlib::if_then_else>(w, choose_lhs, lhs, rhs);
            return wire<ref_kw_>(w, arg<"x">(selected)).as<TS<Int>>();
        }
    };

    struct PlainKwGraph
    {
        [[maybe_unused]] static constexpr auto name = "plain_kw_graph";

        static Port<TS<Int>> compose(Wiring &w, Port<TS<Bool>> choose_lhs, Port<TS<Int>> lhs,
                                     Port<TS<Int>> rhs)
        {
            auto selected = wire<stdlib::if_then_else>(w, choose_lhs, lhs, rhs);
            return wire<plain_kw_>(w, arg<"x">(selected)).as<TS<Int>>();
        }
    };

    [[nodiscard]] inline WiringArg scalar_arg(Value value)
    {
        WiringArg arg;
        arg.kind         = WiringArg::Kind::Scalar;
        arg.scalar_value = std::move(value);
        arg.scalar_meta  = arg.scalar_value.schema();
        return arg;
    }

    [[nodiscard]] inline WiringArg named_ts_arg(std::string name, const TSValueTypeMetaData *schema)
    {
        WiringArg arg;
        arg.kind        = WiringArg::Kind::TimeSeries;
        arg.port.schema = schema;
        arg.name        = std::move(name);
        return arg;
    }

    [[nodiscard]] inline WiringArg named_scalar_arg(std::string name, Value value)
    {
        WiringArg arg;
        arg.kind         = WiringArg::Kind::Scalar;
        arg.scalar_value = std::move(value);
        arg.scalar_meta  = arg.scalar_value.schema();
        arg.name         = std::move(name);
        return arg;
    }
}  // namespace

TEST_CASE("operators: an omitted parameter takes its declared default, end to end")
{
    using namespace hgraph;
    (void)TypeRegistry::instance().register_scalar<Int>("int");
    register_overload<scale_default_, scale_node>();

    // factor omitted -> the defaults() hook fills 3.
    CHECK_OUTPUT(eval_node<scale_default_>(values<Int>(2, 5)), values<Int>(6, 15));
    // factor supplied positionally -> 4.
    CHECK_OUTPUT(eval_node<scale_default_>(values<Int>(2), Int{4}), values<Int>(8));
}

TEST_CASE("operators: named arguments target parameters by name, in any order")
{
    using namespace hgraph;
    (void)TypeRegistry::instance().register_scalar<Int>("int");
    register_overload<scale_default_, scale_node>();

    const auto *ts_int = ts_type<TS<Int>>();

    // scale(factor=7, ... ) is rejected: positional may not follow named.
    {
        std::array<WiringArg, 2> args{named_scalar_arg("factor", Value{Int{7}}), ts_arg(ts_int)};
        REQUIRE_THROWS_WITH(OperatorRegistry::instance().resolve("scale_default", std::span<const WiringArg>{args}),
                            Catch::Matchers::ContainsSubstring("positional argument follows"));
    }

    // scale(ts, factor=7): named scalar lands in declared position.
    {
        std::array<WiringArg, 2> args{ts_arg(ts_int), named_scalar_arg("factor", Value{Int{7}})};
        auto resolved = OperatorRegistry::instance().resolve("scale_default", std::span<const WiringArg>{args});
        REQUIRE(resolved.args.size() == 2);
        REQUIRE(resolved.args[1].scalar_value.try_as<Int>() != nullptr);
        CHECK(*resolved.args[1].scalar_value.try_as<Int>() == 7);
    }

    // Node-overload ports are named via their In<> declarations: add(rhs=, lhs=).
    register_overload<add_, add_ints>();
    {
        std::array<WiringArg, 2> args{named_ts_arg("rhs", ts_int), named_ts_arg("lhs", ts_int)};
        auto resolved = OperatorRegistry::instance().resolve("add", std::span<const WiringArg>{args});
        REQUIRE(resolved.args.size() == 2);
        CHECK(resolved.args[0].name == "lhs");
        CHECK(resolved.args[1].name == "rhs");
    }
}

TEST_CASE("operators: calling-rule violations report Python-style errors")
{
    using namespace hgraph;
    (void)TypeRegistry::instance().register_scalar<Int>("int");
    register_overload<scale_default_, scale_node>();

    const auto *ts_int = ts_type<TS<Int>>();

    // Duplicate: positional factor + named factor.
    {
        std::array<WiringArg, 3> args{ts_arg(ts_int), scalar_arg(Value{Int{2}}),
                                      named_scalar_arg("factor", Value{Int{7}})};
        REQUIRE_THROWS_WITH(OperatorRegistry::instance().resolve("scale_default", std::span<const WiringArg>{args}),
                            Catch::Matchers::ContainsSubstring("multiple values for argument 'factor'"));
    }
    // Unknown keyword with no **kwargs collector.
    {
        std::array<WiringArg, 2> args{ts_arg(ts_int), named_scalar_arg("nope", Value{Int{7}})};
        REQUIRE_THROWS_WITH(OperatorRegistry::instance().resolve("scale_default", std::span<const WiringArg>{args}),
                            Catch::Matchers::ContainsSubstring("unexpected keyword argument 'nope'"));
    }
    // Missing required argument (no default on ts).
    {
        std::array<WiringArg, 1> args{named_scalar_arg("factor", Value{Int{7}})};
        REQUIRE_THROWS_WITH(OperatorRegistry::instance().resolve("scale_default", std::span<const WiringArg>{args}),
                            Catch::Matchers::ContainsSubstring("missing required argument"));
    }
}

TEST_CASE("operators: unmatched keyword time-series collect into **kwargs")
{
    using namespace hgraph;
    (void)TypeRegistry::instance().register_scalar<Int>("int");
    register_graph_overload<kw_sum_, kw_sum_impl>();

    const auto *ts_int = ts_type<TS<Int>>();

    std::array<WiringArg, 3> args{ts_arg(ts_int), named_ts_arg("x", ts_int), named_ts_arg("y", ts_int)};
    auto resolved = OperatorRegistry::instance().resolve("kw_sum", std::span<const WiringArg>{args});
    REQUIRE(resolved.kwargs.size() == 2);
    CHECK(resolved.kwargs[0].first == "x");
    CHECK(resolved.kwargs[1].first == "y");
    CHECK(resolved.impl->has_kwargs);

    std::array<WiringArg, 3> duplicate_args{ts_arg(ts_int), named_ts_arg("x", ts_int), named_ts_arg("x", ts_int)};
    REQUIRE_THROWS_WITH(OperatorRegistry::instance().resolve("kw_sum", std::span<const WiringArg>{duplicate_args}),
                        Catch::Matchers::ContainsSubstring("multiple values for argument 'x'"));
}

TEST_CASE("wiring: a pending node label attaches to the matching operator only")
{
    using namespace hgraph;
    (void)TypeRegistry::instance().register_scalar<Int>("int");

    // Issue #247: the hint labels the next node whose schema name matches;
    // non-matching nodes (const lifts et al) pass through unlabeled, and the
    // hint is one-shot.
    Wiring w;
    w.set_pending_node_label("label_probe", "user_fn");

    NodeBuilder other;
    other.implementation<other_probe_>();
    struct other_tag {};
    const WiringPortRef other_ref = w.add_node(
        std::type_index(typeid(other_tag)), std::move(other),
        std::span<const WiringPortRef>{}, Value{});
    CHECK(other_ref.peered_node()->builder.label().empty());

    NodeBuilder target;
    target.implementation<label_probe_>();
    struct target_tag {};
    const WiringPortRef target_ref = w.add_node(
        std::type_index(typeid(target_tag)), std::move(target),
        std::span<const WiringPortRef>{}, Value{});
    CHECK(target_ref.peered_node()->builder.label() == "user_fn");

    // one-shot: a second matching node stays unlabeled
    NodeBuilder again;
    again.implementation<label_probe_>();
    struct again_tag {};
    const WiringPortRef again_ref = w.add_node(
        std::type_index(typeid(again_tag)), std::move(again),
        std::span<const WiringPortRef>{}, Value{});
    CHECK(again_ref.peered_node()->builder.label().empty());
}

TEST_CASE("wiring: a pending node label attaches to a deferred builder")
{
    using namespace hgraph;
    (void)TypeRegistry::instance().register_scalar<Int>("int");

    Wiring w;
    w.set_pending_node_label("label_probe", "app.map_");

    NodeBuilder schema_builder;
    schema_builder.implementation<label_probe_>();
    const WiringNodeSchema schema{
        .input = schema_builder.type().schema()->input_schema,
        .output = schema_builder.type().schema()->output_schema,
    };
    struct deferred_tag {};
    const WiringPortRef ref = w.add_node(
        std::type_index(typeid(deferred_tag)), schema,
        std::span<const WiringPortRef>{}, Value{}, [] {
            NodeBuilder builder;
            builder.implementation<label_probe_>();
            return builder;
        });

    CHECK(ref.peered_node()->builder.label() == "app.map_");
}

TEST_CASE("operators: a typed **kwargs collector gates candidates on its pack pattern")
{
    using namespace hgraph;
    (void)TypeRegistry::instance().register_scalar<Int>("int");
    (void)TypeRegistry::instance().register_scalar<Str>("str");

    register_graph_overload<typed_kw_, typed_kw_impl>();

    const auto *ts_int = ts_type<TS<Int>>();
    const auto *ts_str = ts_type<TS<Str>>();

    std::array<WiringArg, 1> matching{named_ts_arg("x", ts_int)};
    auto resolved = OperatorRegistry::instance().resolve(
        "typed_kw", std::span<const WiringArg>{matching});
    REQUIRE(resolved.impl->has_kwargs_pattern);
    CHECK(resolved.kwargs.size() == 1);

    std::array<WiringArg, 1> mismatched{named_ts_arg("x", ts_str)};
    REQUIRE_THROWS_WITH(
        OperatorRegistry::instance().resolve("typed_kw",
                                             std::span<const WiringArg>{mismatched}),
        Catch::Matchers::ContainsSubstring("**kwargs pattern"));
}

TEST_CASE("operators: a typed **kwargs pack schema decides whether REF survives")
{
    using namespace hgraph;
    (void)TypeRegistry::instance().register_scalar<Int>("int");
    stdlib::register_standard_operators();
    register_graph_overload<ref_kw_, ref_kw_impl>();
    register_graph_overload<plain_kw_, plain_kw_impl>();

    // The declared pack asks for REF<TS<Int>>, so the reference reaches the
    // implementation intact - dispatch matched against that schema, and
    // rewriting the port here would leave output resolution describing a
    // reference the implementation never receives.
    ref_kw_impl::saw_reference = false;
    CHECK_OUTPUT(eval_node<RefKwGraph>(values<Bool>(true), values<Int>(8), values<Int>(-6)),
                 values<Int>(8));
    CHECK(ref_kw_impl::saw_reference);

    // Nothing in a bare **kwargs can ask for a reference, so the same source
    // arrives dereferenced.
    plain_kw_impl::saw_reference = true;
    CHECK_OUTPUT(eval_node<PlainKwGraph>(values<Bool>(true), values<Int>(8), values<Int>(-6)),
                 values<Int>(8));
    CHECK(!plain_kw_impl::saw_reference);
}

TEST_CASE("operators: arg<\"name\">(...) flows named arguments through wire<Op>")
{
    using namespace hgraph;
    (void)TypeRegistry::instance().register_scalar<Int>("int");
    register_overload<scale_default_, scale_node>();

    // Named scalar at the typed call site: scale_default(ts, factor=5).
    CHECK_OUTPUT(eval_node<scale_default_>(values<Int>(2), arg<"factor">(Int{5})), values<Int>(10));
}

namespace
{
    using namespace hgraph;

    // TS-parameter defaults: a value default const-wraps; an EMPTY (None)
    // default leaves the input unwired (null source).
    struct offset_add_ : Operator<"offset_add", In<"a", TS<Int>>, In<"b", TS<Int>>, Out<TS<Int>>>
    {
    };
    struct offset_add_node
    {
        static constexpr auto name = "offset_add_node";
        static void eval(In<"a", TS<Int>> a, In<"b", TS<Int>> b, Out<TS<Int>> out)
        {
            out.set(a.value() + b.value());
        }
        static auto defaults()
        {
            return std::tuple{arg<"b">(Int{10})};
        }
    };

    struct opt_add_ : Operator<"opt_add", In<"a", TS<Int>>, In<"opt", TS<Int>>, Out<TS<Int>>>
    {
    };
    struct opt_add_node
    {
        static constexpr auto name = "opt_add_node";
        static void eval(In<"a", TS<Int>> a, In<"opt", TS<Int>, InputValidity::Unchecked> opt, Out<TS<Int>> out)
        {
            out.set(a.value() + (opt.valid() ? opt.value() : Int{0}));
        }
        static auto defaults()
        {
            return std::tuple{arg<"opt">(Value{})};   // Python None -> null source
        }
    };

    struct pick_ : Operator<"pick", In<"lhs", TS<Int>>, In<"rhs", TS<Int>>, Out<TS<Int>>>
    {
    };
    struct pick_lhs_graph
    {
        static constexpr auto name = "pick_lhs_graph";
        static Port<TS<Int>>  compose(Wiring &, NamedPort<"lhs", TS<Int>> lhs, NamedPort<"rhs", TS<Int>> rhs)
        {
            static_cast<void>(rhs);
            return lhs;
        }
    };
}  // namespace

TEST_CASE("operators: a value default on a time-series parameter const-wraps")
{
    using namespace hgraph;
    (void)TypeRegistry::instance().register_scalar<Int>("int");
    register_overload<offset_add_, offset_add_node>();

    // b omitted -> const(10); supplied scalar -> const(4).
    CHECK_OUTPUT(eval_node<offset_add_>(values<Int>(1, 2)), values<Int>(11, 12));
    CHECK_OUTPUT(eval_node<offset_add_>(values<Int>(1), Int{4}), values<Int>(5));
}

TEST_CASE("operators: a None default on a time-series parameter is a null source")
{
    using namespace hgraph;
    (void)TypeRegistry::instance().register_scalar<Int>("int");
    register_overload<opt_add_, opt_add_node>();

    // opt omitted -> unwired (never valid) -> a passes through.
    CHECK_OUTPUT(eval_node<opt_add_>(values<Int>(1, 2)), values<Int>(1, 2));
    // opt supplied -> sums.
    CHECK_OUTPUT(eval_node<opt_add_>(values<Int>(1, 2), values<Int>(100, none)), values<Int>(101, 102));
}

TEST_CASE("operators: NamedPort lets keyword arguments target graph-overload ports")
{
    using namespace hgraph;
    (void)TypeRegistry::instance().register_scalar<Int>("int");
    register_graph_overload<pick_, pick_lhs_graph>();

    const auto *ts_int = ts_type<TS<Int>>();

    std::array<WiringArg, 2> args{named_ts_arg("rhs", ts_int), named_ts_arg("lhs", ts_int)};
    auto resolved = OperatorRegistry::instance().resolve("pick", std::span<const WiringArg>{args});
    REQUIRE(resolved.args.size() == 2);
    CHECK(resolved.args[0].name == "lhs");
    CHECK(resolved.args[1].name == "rhs");
}
