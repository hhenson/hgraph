// Type arguments (RFC 0033): a ``TypeArg`` parameter receives a type — a
// time-series schema, a scalar schema or a size — as a ``TypeCarrier`` value,
// and matching it binds the variables of its carried pattern in the
// candidate's resolution map before the resolvers run. A deferred default
// materialises after the resolvers, at a fixed point with output resolution,
// and before ``requires_``. Design record: docs/source/developer_guide/
// operators.rst, "Type arguments"; docs/source/rfc/rfc_0033_*.rst.
#include <hgraph/lib/std/std_operators.h>
#include <hgraph/lib/std/operators/conversion.h>
#include <hgraph/lib/std/operators/impl/conversion_impl.h>
#include <hgraph/lib/testing/check_output.h>
#include <hgraph/lib/testing/eval_node.h>
#include <hgraph/runtime/runtime.h>
#include <hgraph/types/graph_wiring.h>
#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/operator_dispatch.h>
#include <hgraph/types/static_node.h>

#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers_string.hpp>

#include <optional>
#include <span>
#include <string>
#include <vector>

namespace
{
    using namespace hgraph;
    using namespace hgraph::testing;

    // ---- the operator family under test ------------------------------------

    struct ta_cast : Operator<"ta_cast", In<"x", TsVar<"X">>, TypeArg<"to", TS<ScalarVar<"T">>>, Out<TsVar<"O">>>
    {
    };

    /** ``to`` is required: it binds ``T`` and therefore the output. */
    struct ta_cast_generic
    {
        static constexpr auto name = "ta_cast";
        static void eval(In<"x", TS<Int>> x, TypeArg<"to", TS<ScalarVar<"T">>>, Out<TS<ScalarVar<"T">>> out)
        {
            static_cast<void>(x);
            static_cast<void>(out);
        }
    };

    /** A more specific sibling: ``type[TS[int]]`` outranks ``type[TS[T]]``. */
    struct ta_cast_int
    {
        static constexpr auto name = "ta_cast";
        static void eval(In<"x", TS<Int>> x, TypeArg<"to", TS<Int>>, Out<TS<Int>> out)
        {
            if (x.modified()) { out.set(x.value()); }
        }
    };

    struct ta_scalar : Operator<"ta_scalar", In<"x", TsVar<"X">>, TypeArg<"tp", ScalarVar<"T">>, Out<TsVar<"O">>>
    {
    };
    struct ta_scalar_impl
    {
        static constexpr auto name = "ta_scalar";
        static void eval(In<"x", TS<Int>> x, TypeArg<"tp", ScalarVar<"T">>, Out<TS<ScalarVar<"T">>> out)
        {
            static_cast<void>(x);
            static_cast<void>(out);
        }
    };

    struct ta_size : Operator<"ta_size", In<"xs", TsVar<"X">>, TypeArg<"n", SizeVar<"N">, AutoResolve>, Out<TsVar<"O">>>
    {
    };
    /** ``n`` is deferred: it materialises from the size the input bound. */
    struct ta_size_impl
    {
        static constexpr auto name = "ta_size";
        static void eval(In<"xs", TSL<TS<Int>, SIZE<"N">>> xs, TypeArg<"n", SizeVar<"N">, AutoResolve>, Out<TS<Int>> out)
        {
            static_cast<void>(xs);
            static_cast<void>(out);
        }
    };

    struct ta_deferred : Operator<"ta_deferred", In<"x", TsVar<"X">>, TypeArg<"tp", TsVar<"S">, AutoResolve>, Out<TsVar<"S">>>
    {
    };
    /** ``S`` is bound only by the resolver; ``tp`` materialises after it and
        ``requires_`` sees the materialised value. */
    struct ta_deferred_impl
    {
        static constexpr auto name = "ta_deferred";
        static void resolve_default_types(ResolutionMap &resolution)
        {
            if (resolution.find_ts("S") == nullptr)
            {
                resolution.bind_ts("S", TypeRegistry::instance().tss(resolution.find_ts("X")->value_schema));
            }
        }
        static bool requires_(const ResolutionMap &, OperatorCallContext context)
        {
            const auto *carrier = context.scalar_as<TypeCarrier>("tp");
            return carrier != nullptr && carrier->ts() != nullptr;
        }
        static void eval(In<"x", TsVar<"X">> x, TypeArg<"tp", TsVar<"S">, AutoResolve>, Out<TsVar<"S">> out)
        {
            static_cast<void>(x);
            static_cast<void>(out);
        }
    };

    struct ta_unresolved : Operator<"ta_unresolved", In<"x", TsVar<"X">>, TypeArg<"tp", TsVar<"S">, AutoResolve>, Out<TsVar<"X">>>
    {
    };
    /** Nothing binds ``S``: the deferred carrier cannot materialise. */
    struct ta_unresolved_impl
    {
        static constexpr auto name = "ta_unresolved";
        static void eval(In<"x", TsVar<"X">> x, TypeArg<"tp", TsVar<"S">, AutoResolve>, Out<TsVar<"X">> out)
        {
            static_cast<void>(x);
            static_cast<void>(out);
        }
    };

    struct ta_defaulted : Operator<"ta_defaulted", In<"x", TsVar<"X">>, TypeArg<"to", TS<ScalarVar<"T">>>, Out<TsVar<"O">>>
    {
    };
    /** A concrete default from ``defaults()`` binds ``T`` when ``to`` is omitted. */
    struct ta_defaulted_impl
    {
        static constexpr auto name = "ta_defaulted";
        static auto           defaults()
        {
            return std::tuple{arg<"to">(TypeCarrier::of_ts(ts_type<TS<Str>>()))};
        }
        static void eval(In<"x", TS<Int>> x, TypeArg<"to", TS<ScalarVar<"T">>>, Out<TS<ScalarVar<"T">>> out)
        {
            static_cast<void>(x);
            static_cast<void>(out);
        }
    };

    struct ta_graph : Operator<"ta_graph", In<"x", TsVar<"X">>, TypeArg<"to", TS<ScalarVar<"T">>>, Out<TsVar<"O">>>
    {
    };
    /** A graph overload receives the carrier in ``compose``. */
    struct ta_graph_impl
    {
        static constexpr auto name = "ta_graph";
        // An erased output binds ``__out__`` itself, from the carrier's ``T``.
        static void resolve_default_types(ResolutionMap &resolution)
        {
            if (const auto *scalar = resolution.find_scalar("T"))
            {
                resolution.bind_ts("__out__", TypeRegistry::instance().ts(scalar));
            }
        }
        static Port<void>     compose(Wiring &w, Port<TS<Int>> x, TypeArg<"to", TS<ScalarVar<"T">>> to)
        {
            static_cast<void>(x);
            REQUIRE(to.value().ts() != nullptr);
            return wire<stdlib::nothing>(w, to.value().ts());
        }
    };

    /** Disagrees with the family: ``to`` is a value here. Registration rejects it. */
    struct ta_cast_value
    {
        static constexpr auto name = "ta_cast";
        static void eval(In<"x", TS<Int>> x, Scalar<"to", Int>, Out<TS<Int>> out)
        {
            static_cast<void>(x);
            static_cast<void>(out);
        }
    };

    // The test harness resets the registries between test cases, so the
    // family registers through an installer (replayed by run_installers
    // after every reset), exactly as the standard library does.
    void install_type_argument_operators()
    {
        register_overload<ta_cast, ta_cast_generic>();
        register_overload<ta_cast, ta_cast_int>();
        register_overload<ta_scalar, ta_scalar_impl>();
        register_overload<ta_size, ta_size_impl>();
        register_overload<ta_deferred, ta_deferred_impl>();
        register_overload<ta_unresolved, ta_unresolved_impl>();
        register_overload<ta_defaulted, ta_defaulted_impl>();
        register_graph_overload<ta_graph, ta_graph_impl>();
    }
    void registered()
    {
        stdlib::register_standard_operators();   // const / nothing / combine around the family
        auto &registry = OperatorRegistry::instance();
        registry.register_installer("hgraph.tests.type_arguments", &install_type_argument_operators);
        registry.run_installers();
    }

    WiringArg ts_arg(WiringPortRef port)
    {
        WiringArg arg;
        arg.kind = WiringArg::Kind::TimeSeries;
        arg.port = std::move(port);
        return arg;
    }
    WiringArg type_arg(TypeCarrier carrier, std::string name = {})
    {
        WiringArg arg;
        arg.kind         = WiringArg::Kind::Scalar;
        arg.scalar_meta  = scalar_descriptor<TypeCarrier>::value_meta();
        arg.scalar_value = Value{carrier};
        arg.name         = std::move(name);
        return arg;
    }
    ResolvedOperatorCall resolve(Wiring &w, std::string_view name, std::vector<WiringArg> args)
    {
        return OperatorRegistry::instance().resolve(name, std::span<const WiringArg>{args.data(), args.size()},
                                                    std::nullopt, nullptr, {}, w.operator_state(), &w);
    }
    std::string resolution_error(Wiring &w, std::string_view name, std::vector<WiringArg> args)
    {
        try
        {
            static_cast<void>(resolve(w, name, std::move(args)));
        }
        catch (const OperatorResolutionError &error)
        {
            return error.what();
        }
        return {};
    }
}  // namespace

TEST_CASE("type arguments: a TypeArg is a wiring parameter but not a runtime field")
{
    using sig = StaticNodeSignature<ta_cast_generic>;
    STATIC_REQUIRE(sig::scalar_count() == 0);
    STATIC_REQUIRE(std::tuple_size_v<sig::wire_param_types> == 2);
    STATIC_REQUIRE(sig::input_count() == 1);
}

TEST_CASE("type arguments: a supplied time-series carrier binds the carried pattern's variables")
{
    registered();
    Wiring w;
    auto   x = wire<stdlib::const_, TS<Int>>(w, Int{1});

    const ResolvedOperatorCall call =
        resolve(w, "ta_cast", {ts_arg(x.erased()), type_arg(TypeCarrier::of_ts(ts_type<TS<Float>>()))});
    CHECK(call.impl->label == "ta_cast(TS[int], type[TS[~T]]) -> TS[~T]");
    CHECK(call.map.find_scalar("T") == scalar_descriptor<Float>::value_meta());
    CHECK(ts_pattern_resolve(call.impl->output, call.map) == ts_type<TS<Float>>());
}

TEST_CASE("type arguments: the more specific carried pattern wins at equal inputs")
{
    registered();
    Wiring w;
    auto   x = wire<stdlib::const_, TS<Int>>(w, Int{1});

    const ResolvedOperatorCall call =
        resolve(w, "ta_cast", {ts_arg(x.erased()), type_arg(TypeCarrier::of_ts(ts_type<TS<Int>>()))});
    CHECK(call.impl->label == "ta_cast(TS[int], type[TS[int]]) -> TS[int]");
}

TEST_CASE("type arguments: a carrier of the wrong form or type is rejected with the reason")
{
    registered();
    Wiring w;
    auto   x = wire<stdlib::const_, TS<Int>>(w, Int{1});

    const std::string form =
        resolution_error(w, "ta_cast", {ts_arg(x.erased()), type_arg(TypeCarrier::of_scalar(scalar_descriptor<Int>::value_meta()))});
    CHECK_THAT(form, Catch::Matchers::ContainsSubstring("expects a time-series type, got a scalar type"));

    const std::string shape =
        resolution_error(w, "ta_cast", {ts_arg(x.erased()), type_arg(TypeCarrier::of_ts(ts_type<TSS<Int>>()))});
    CHECK_THAT(shape, Catch::Matchers::ContainsSubstring("does not match type[TS[~T]]"));

    const std::string value = resolution_error(w, "ta_cast", {ts_arg(x.erased()), type_arg(TypeCarrier{}, {})});
    CHECK_FALSE(value.empty());
}

TEST_CASE("type arguments: a scalar carrier binds a scalar variable")
{
    registered();
    Wiring w;
    auto   x = wire<stdlib::const_, TS<Int>>(w, Int{1});

    const ResolvedOperatorCall call =
        resolve(w, "ta_scalar", {ts_arg(x.erased()), type_arg(TypeCarrier::of_scalar(scalar_descriptor<Str>::value_meta()))});
    CHECK(call.map.find_scalar("T") == scalar_descriptor<Str>::value_meta());
    CHECK(ts_pattern_resolve(call.impl->output, call.map) == ts_type<TS<Str>>());
}

TEST_CASE("type arguments: a deferred size carrier materialises from the size the input bound")
{
    registered();
    Wiring w;
    auto   a  = wire<stdlib::const_, TS<Int>>(w, Int{1});
    auto   b  = wire<stdlib::const_, TS<Int>>(w, Int{2});
    auto   xs = stdlib::to_tsl<TSL<TS<Int>, 2>>(w, a, b);

    const ResolvedOperatorCall call = resolve(w, "ta_size", {ts_arg(xs.erased())});
    REQUIRE(call.args.size() == 2);
    const auto *carrier = call.args[1].scalar_value.try_as<TypeCarrier>();
    REQUIRE(carrier != nullptr);
    CHECK(carrier->size() == std::optional<std::size_t>{2});
    CHECK(call.map.find_size("N") == std::optional<std::size_t>{2});
}

TEST_CASE("type arguments: a deferred carrier materialises after the resolver and before requires_")
{
    registered();
    Wiring w;
    auto   x = wire<stdlib::const_, TS<Int>>(w, Int{1});

    const ResolvedOperatorCall call = resolve(w, "ta_deferred", {ts_arg(x.erased())});
    const auto *carrier = call.args[1].scalar_value.try_as<TypeCarrier>();
    REQUIRE(carrier != nullptr);
    CHECK(carrier->ts() == ts_type<TSS<Int>>());   // the resolver's choice, not the input's
    CHECK(ts_pattern_resolve(call.impl->output, call.map) == ts_type<TSS<Int>>());

    // Supplied, the carrier binds S BEFORE the resolver, which then keeps it.
    const ResolvedOperatorCall supplied =
        resolve(w, "ta_deferred", {ts_arg(x.erased()), type_arg(TypeCarrier::of_ts(ts_type<TS<Str>>()))});
    CHECK(ts_pattern_resolve(supplied.impl->output, supplied.map) == ts_type<TS<Str>>());
}

TEST_CASE("type arguments: a deferred carrier nothing can resolve fails the candidate")
{
    registered();
    Wiring w;
    auto   x = wire<stdlib::const_, TS<Int>>(w, Int{1});

    const std::string message = resolution_error(w, "ta_unresolved", {ts_arg(x.erased())});
    CHECK_THAT(message, Catch::Matchers::ContainsSubstring("type argument 'tp' could not be resolved"));
}

TEST_CASE("type arguments: a concrete default from defaults() binds like a supplied carrier")
{
    registered();
    Wiring w;
    auto   x = wire<stdlib::const_, TS<Int>>(w, Int{1});

    const ResolvedOperatorCall call = resolve(w, "ta_defaulted", {ts_arg(x.erased())});
    CHECK(call.map.find_scalar("T") == scalar_descriptor<Str>::value_meta());
    CHECK(call.impl->label == "ta_defaulted(TS[int], type[TS[~T]]=…) -> TS[~T]");
}

TEST_CASE("type arguments: a graph overload's compose receives the carrier")
{
    registered();
    Wiring w;
    auto   x = wire<stdlib::const_, TS<Int>>(w, Int{1});

    const ResolvedOperatorCall call =
        resolve(w, "ta_graph", {ts_arg(x.erased()), type_arg(TypeCarrier::of_ts(ts_type<TS<Float>>()))});
    const OperatorWireResult wired = call.impl->wire(w, call.map, call.args, call.kwargs);
    REQUIRE(wired.has_output);
    CHECK(wired.output.erased().schema == ts_type<TS<Float>>());
}

TEST_CASE("type arguments: the registry reports a family's carrier parameters and keeps them consistent")
{
    registered();
    const auto carriers = OperatorRegistry::instance().carrier_parameters("ta_cast");
    CHECK(carriers.names == std::vector<std::string>{"to"});
    CHECK(carriers.positions == std::vector<std::size_t>{1});
    CHECK(OperatorRegistry::instance().carrier_parameters("add_").names.empty());

    CHECK_THROWS_AS((register_overload<ta_cast, ta_cast_value>()), std::invalid_argument);
}

TEST_CASE("type arguments: a carried REF is the type the caller names")
{
    registered();
    Wiring w;
    // Supplied: the carrier binds O to the reference schema verbatim.
    auto supplied = wire<stdlib::nothing>(w, ts_type<REF<TS<Int>>>());
    CHECK(supplied.erased().schema == ts_type<REF<TS<Int>>>());
    // Requested through the expected output: the deferred carrier
    // materialises from O and still matches the reference it names.
    auto requested = wire<stdlib::nothing, REF<TS<Int>>>(w);
    CHECK(requested.erased().schema == ts_type<REF<TS<Int>>>());
    // A structural carried pattern keeps REF transparency below the top
    // level: type[TS[int]] accepts a REF[TS[int]] carrier, so the concrete
    // candidate still wins and resolves the dereferenced output.
    auto x = wire<stdlib::const_, TS<Int>>(w, Int{1});
    const ResolvedOperatorCall call =
        resolve(w, "ta_cast", {ts_arg(x.erased()), type_arg(TypeCarrier::of_ts(ts_type<REF<TS<Int>>>()))});
    CHECK(call.impl->label == "ta_cast(TS[int], type[TS[int]]) -> TS[int]");
    CHECK(ts_pattern_resolve(call.impl->output, call.map) == ts_type<TS<Int>>());
}

TEST_CASE("type arguments: const and nothing declare their type argument")
{
    registered();
    Wiring w;
    // Supplied: the carrier is the output type.
    auto empty = wire<stdlib::nothing>(w, ts_type<TS<Str>>());
    CHECK(empty.erased().schema == ts_type<TS<Str>>());
    // Omitted: the resolver binds S from the value and tp materialises from it.
    auto value = wire<stdlib::const_>(w, Int{3});
    CHECK(value.erased().schema == ts_type<TS<Int>>());
    // Supplied on const: the value must still fit the requested output.
    auto typed = wire<stdlib::const_>(w, Int{3}, ts_type<TS<Int>>());
    CHECK(typed.erased().schema == ts_type<TS<Int>>());
    // delay is keyword-only after tp, as in the 0.5 signature.
    auto delayed = wire<stdlib::const_, TS<Int>>(w, Int{3}, arg<"delay">(TimeDelta{}));
    CHECK(delayed.erased().schema == ts_type<TS<Int>>());
    CHECK(OperatorRegistry::instance().carrier_parameters("const").names == std::vector<std::string>{"tp"});
}

namespace
{
    struct ta_cast_graph
    {
        static constexpr auto name = "ta_cast_graph";
        static Port<TS<Int>>  compose(Wiring &w, Port<TS<Int>> in)
        {
            registered();
            return wire<ta_cast>(w, in, ts_type<TS<Int>>()).as<TS<Int>>();
        }
    };
}  // namespace

TEST_CASE("type arguments: a node with a TypeArg evaluates with an empty placeholder in its slot")
{
    CHECK_OUTPUT(eval_node<ta_cast_graph>(values<Int>(1, none, 3)), {1, none, 3});
}
