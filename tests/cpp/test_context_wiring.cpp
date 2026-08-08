#include <hgraph/lib/std/std_operators.h>
#include <hgraph/lib/testing/check_output.h>
#include <hgraph/lib/testing/eval_node.h>
#include <hgraph/types/context_wiring.h>
#include <hgraph/types/graph_wiring.h>
#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/static_node.h>
#include <hgraph/types/subgraph_wiring.h>
#include <hgraph/types/wired_fn.h>

#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers_string.hpp>

// The user-facing context wiring surface (design record: services.rst,
// *Contexts*): context::scope<"name"> publishes a port for a wiring scope;
// Context<"name", S> signature inputs and context::get resolve the nearest
// publication. Approved 2026-07-04.

namespace
{
    using namespace hgraph;
    using namespace hgraph::testing;
    using namespace std::string_literals;

    struct DoubledConsumer
    {
        static constexpr auto name = "doubled_consumer";

        static void eval(Context<"price", TS<Int>> price, Out<TS<Int>> out)
        {
            out.set(price.value() * 2);
        }
    };

    // A consumer mixing a caller input with a context input: the caller passes
    // ONLY ``scale`` — ``price`` binds from the ambient context.
    struct ScaledConsumer
    {
        static constexpr auto name = "scaled_consumer";

        static void eval(In<"scale", TS<Int>> scale, Context<"price", TS<Int>> price, Out<TS<Int>> out)
        {
            out.set(scale.value() * price.value());
        }
    };

    struct context_scaled_ : Operator<"context_scaled", In<"scale", TS<Int>>, Out<TS<Int>>>
    {
    };

    struct ContextScaledOverload
    {
        static constexpr auto name = "context_scaled_overload";

        static void eval(Context<"price", TS<Int>> price, In<"scale", TS<Int>> scale,
                         Out<TS<Int>> out)
        {
            out.set(scale.value() * price.value());
        }
    };

    // A generic consumer: the context schema resolves from whatever is published.
    struct EchoConsumer
    {
        static constexpr auto name = "echo_consumer";

        static void eval(Context<"price", TS<ScalarVar<"T">>> price, Out<TS<ScalarVar<"T">>> out)
        {
            const Value delta = capture_delta(price.base());
            apply_delta(out, delta.view());
        }
    };

    struct ContextGraph
    {
        [[maybe_unused]] static constexpr auto name = "context_graph";

        static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> price)
        {
            context::scope<"price"> ctx{w, price};
            return wire<DoubledConsumer>(w);   // no args: price binds from the context
        }
    };

    struct MixedArgsGraph
    {
        [[maybe_unused]] static constexpr auto name = "mixed_args_graph";

        static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> scale, Port<TS<Int>> price)
        {
            context::scope<"price"> ctx{w, price};
            return wire<ScaledConsumer>(w, scale);   // positional arg maps to ``scale``, not ``price``
        }
    };

    struct RegisteredContextGraph
    {
        [[maybe_unused]] static constexpr auto name = "registered_context_graph";

        static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> scale, Port<TS<Int>> price)
        {
            context::scope<"price"> ctx{w, price};
            return wire<context_scaled_>(w, scale).as<TS<Int>>();
        }
    };

    struct ShadowingGraph
    {
        [[maybe_unused]] static constexpr auto name = "shadowing_graph";

        static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> outer, Port<TS<Int>> inner)
        {
            context::scope<"price"> outer_scope{w, outer};
            Port<TS<Int>>           result{};
            {
                context::scope<"price"> inner_scope{w, inner};
                result = wire<DoubledConsumer>(w);   // nearest wins: doubles ``inner``
            }
            // After the inner scope pops, the outer publication is visible again.
            auto again = context::get<TS<Int>>(w, "price");
            static_cast<void>(again);
            return result;
        }
    };

    struct OverrideGraph
    {
        [[maybe_unused]] static constexpr auto name = "override_graph";

        static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> ambient, Port<TS<Int>> explicit_price)
        {
            context::scope<"price"> ctx{w, ambient};
            // An explicit keyword argument overrides the ambient context.
            return wire<DoubledConsumer>(w, arg<"price">(explicit_price));
        }
    };

    struct GetFormGraph
    {
        [[maybe_unused]] static constexpr auto name = "get_form_graph";

        static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> price)
        {
            context::scope<"price"> ctx{w, price};
            REQUIRE(context::has(w, "price"));
            REQUIRE_FALSE(context::has(w, "volatility"));
            auto looked_up = context::get<TS<Int>>(w, "price");
            return wire<DoubledConsumer>(w, arg<"price">(looked_up));
        }
    };

    struct GenericContextGraph
    {
        [[maybe_unused]] static constexpr auto name = "generic_context_graph";

        static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> price)
        {
            context::scope<"price"> ctx{w, price};
            return wire<EchoConsumer>(w).as<TS<Int>>();
        }
    };

    struct MissingContextGraph
    {
        [[maybe_unused]] static constexpr auto name = "missing_context_graph";

        static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> price)
        {
            static_cast<void>(price);
            return wire<DoubledConsumer>(w);   // no scope published: wiring error
        }
    };

    struct CapturedContextValue
    {
        static constexpr auto name = "captured_context_value";

        static Port<TS<Int>> compose(Wiring &w)
        {
            return context::get<TS<Int>>(w, "price");
        }
    };

    struct NestedContextGraph
    {
        static constexpr auto name = "nested_context_graph";

        static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> price)
        {
            context::scope<"price"> ctx{w, price};
            return nested_<CapturedContextValue>(w);
        }
    };

    struct NestedContextMiddle
    {
        static constexpr auto name = "nested_context_middle";

        static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> price)
        {
            context::scope<"price"> ctx{w, price};
            return nested_<CapturedContextValue>(w);
        }
    };

    struct TwoLevelNestedContextGraph
    {
        static constexpr auto name = "two_level_nested_context_graph";

        static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> price)
        {
            return nested_<NestedContextMiddle>(w, price);
        }
    };

    using ContextBundle = UnNamedTSB<Field<"left", TS<Int>>, Field<"right", TS<Int>>>;

    struct BundleContextConsumer
    {
        static constexpr auto name = "bundle_context_consumer";

        static void eval(Context<"bundle", ContextBundle> bundle, Out<TS<Int>> out)
        {
            out.set(bundle.template field<"left">().value() +
                    bundle.template field<"right">().value());
        }
    };

    struct CapturedBundleContextGraph
    {
        static constexpr auto name = "captured_bundle_context_graph";

        static Port<TS<Int>> compose(Wiring &w)
        {
            return wire<BundleContextConsumer>(w);
        }
    };

    struct NestedStructuralContextGraph
    {
        static constexpr auto name = "nested_structural_context_graph";

        static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> left, Port<TS<Int>> right)
        {
            auto bundle = stdlib::to_tsb<ContextBundle>(w, left, right);
            context::scope<"bundle"> ctx{w, bundle};
            return nested_<CapturedBundleContextGraph>(w);
        }
    };

    struct AddCapturedContext
    {
        static constexpr auto name = "add_captured_context";

        static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> value)
        {
            using namespace hgraph::stdlib::syntax;
            return (value + context::get<TS<Int>>(w, "price")).as<TS<Int>>();
        }
    };

    struct SubtractCapturedContext
    {
        static constexpr auto name = "subtract_captured_context";

        static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> value)
        {
            using namespace hgraph::stdlib::syntax;
            return (value - context::get<TS<Int>>(w, "price")).as<TS<Int>>();
        }
    };

    struct SwitchContextGraph
    {
        static constexpr auto name = "switch_context_graph";

        static Port<TS<Int>> compose(Wiring &w, Port<TS<Str>> key,
                                     Port<TS<Int>> value, Port<TS<Int>> price)
        {
            context::scope<"price"> ctx{w, price};
            return wire<stdlib::switch_>(
                       w, key,
                       stdlib::switch_cases({
                           {Value{Str{"add"}}, fn<AddCapturedContext>()},
                           {Value{Str{"sub"}}, fn<SubtractCapturedContext>()},
                       }),
                       value)
                .as<TS<Int>>();
        }
    };

    struct MapContextGraph
    {
        static constexpr auto name = "map_context_graph";

        static Port<TSD<Str, TS<Int>>> compose(Wiring &w, Port<TSD<Str, TS<Int>>> values,
                                               Port<TS<Int>> price)
        {
            context::scope<"price"> ctx{w, price};
            return wire<stdlib::map_>(w, fn<AddCapturedContext>(), values)
                .as<TSD<Str, TS<Int>>>();
        }
    };

    struct MeshContextGraph
    {
        static constexpr auto name = "mesh_context_graph";

        static Port<TSD<Str, TS<Int>>> compose(Wiring &w, Port<TSD<Str, TS<Int>>> values,
                                               Port<TS<Int>> price)
        {
            context::scope<"price"> ctx{w, price};
            return wire<stdlib::mesh_>(w, fn<AddCapturedContext>(), values)
                .as<TSD<Str, TS<Int>>>();
        }
    };

    using TryIntResult = UnNamedTSB<Field<"exception", TS<NodeError>>, Field<"out", TS<Int>>>;

    struct TryOutValue
    {
        static constexpr auto name = "context_try_out_value";

        static void eval(In<"result", TryIntResult, InputValidity::Unchecked> result,
                         Out<TS<Int>> out)
        {
            auto value = result.template field<"out">();
            if (value.valid() && value.modified()) { out.set(value.value()); }
        }
    };

    struct InnerContextTryGraph
    {
        static constexpr auto name = "inner_context_try_graph";

        static Port<TS<Int>> compose(Wiring &w)
        {
            return context::get<TS<Int>>(w, "price");
        }
    };

    struct MiddleContextTryGraph
    {
        static constexpr auto name = "middle_context_try_graph";

        static Port<TS<Int>> compose(Wiring &w)
        {
            auto result = try_except_<InnerContextTryGraph>(w).as<TryIntResult>();
            return wire<TryOutValue>(w, result);
        }
    };

    struct StackedTryContextGraph
    {
        static constexpr auto name = "stacked_try_context_graph";

        static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> price)
        {
            context::scope<"price"> ctx{w, price};
            auto result = try_except_<MiddleContextTryGraph>(w).as<TryIntResult>();
            return wire<TryOutValue>(w, result);
        }
    };
}  // namespace

TEST_CASE("context wiring: a Context input binds from the enclosing scope")
{
    stdlib::register_standard_operators();
    CHECK_OUTPUT(eval_node<ContextGraph>(values<Int>(3, none, 5)), values<Int>(6, none, 10));
}

TEST_CASE("context wiring: positional caller args skip Context params")
{
    stdlib::register_standard_operators();
    CHECK_OUTPUT(eval_node<MixedArgsGraph>(values<Int>(2, 3), values<Int>(10, none)),
                 values<Int>(20, 30));
}

TEST_CASE("context wiring: registered C++ overloads inject Context inputs")
{
    register_overload<context_scaled_, ContextScaledOverload>();
    CHECK_OUTPUT(eval_node<RegisteredContextGraph>(values<Int>(2, 3), values<Int>(10, none)),
                 values<Int>(20, 30));
}

TEST_CASE("context wiring: nested scopes shadow, nearest publication wins")
{
    stdlib::register_standard_operators();
    CHECK_OUTPUT(eval_node<ShadowingGraph>(values<Int>(1), values<Int>(100)), values<Int>(200));
}

TEST_CASE("context wiring: an explicit keyword argument overrides the ambient context")
{
    stdlib::register_standard_operators();
    CHECK_OUTPUT(eval_node<OverrideGraph>(values<Int>(1), values<Int>(7)), values<Int>(14));
}

TEST_CASE("context wiring: context::get and context::has function forms")
{
    stdlib::register_standard_operators();
    CHECK_OUTPUT(eval_node<GetFormGraph>(values<Int>(4)), values<Int>(8));
}

TEST_CASE("context wiring: a generic Context input resolves from the published schema")
{
    stdlib::register_standard_operators();
    CHECK_OUTPUT(eval_node<GenericContextGraph>(values<Int>(9)), values<Int>(9));
}

TEST_CASE("context wiring: a missing context is a wiring error naming the context")
{
    stdlib::register_standard_operators();
    CHECK_THROWS_WITH((void)eval_node<MissingContextGraph>(values<Int>(1)),
                      Catch::Matchers::ContainsSubstring("price"));
}

TEST_CASE("context wiring: nested_ imports and directly returns an enclosing context")
{
    stdlib::register_standard_operators();
    CHECK_OUTPUT(eval_node<NestedContextGraph>(values<Int>(3, none, 5)),
                 values<Int>(3, none, 5));
}

TEST_CASE("context wiring: a nested child captures a context published from its parent boundary")
{
    stdlib::register_standard_operators();
    CHECK_OUTPUT(eval_node<TwoLevelNestedContextGraph>(values<Int>(3, none, 5)),
                 values<Int>(3, none, 5));
}

TEST_CASE("context wiring: nested_ imports a structurally assembled context leaf by leaf")
{
    stdlib::register_standard_operators();
    CHECK_OUTPUT(eval_node<NestedStructuralContextGraph>(values<Int>(1, 2, none),
                                                         values<Int>(10, none, 20)),
                 values<Int>(11, 12, 22));
}

TEST_CASE("context wiring: switch branches import context and react to its ticks")
{
    stdlib::register_standard_operators();
    CHECK_OUTPUT(eval_node<SwitchContextGraph>(
                     values<Str>(Str{"add"}, none, none, Str{"sub"}, none),
                     values<Int>(1, 2, none, none, 3),
                     values<Int>(10, none, 20, none, none)),
                 values<Int>(11, 12, 22, -18, -17));
}

TEST_CASE("context wiring: map children share context across key and value lifetimes")
{
    stdlib::register_standard_operators();
    CHECK_OUTPUT((eval_node<MapContextGraph>(
                     values<Value>(dict_delta<Str, TS<Int>>({{"a"s, 1}, {"b"s, 2}}),
                                   none,
                                   dict_delta<Str, TS<Int>>({{"a"s, 5}}),
                                   dict_delta<Str, TS<Int>>({}, {"b"s})),
                     values<Int>(10, 20, none, none))),
                 values<Value>(dict_delta<Str, TS<Int>>({{"a"s, 11}, {"b"s, 12}}),
                               dict_delta<Str, TS<Int>>({{"a"s, 21}, {"b"s, 22}}),
                               dict_delta<Str, TS<Int>>({{"a"s, 25}}),
                               dict_delta<Str, TS<Int>>({}, {"b"s})));
}

TEST_CASE("context wiring: mesh children share an enclosing context")
{
    stdlib::register_standard_operators();
    CHECK_OUTPUT((eval_node<MeshContextGraph>(
                     values<Value>(dict_delta<Str, TS<Int>>({{"a"s, 1}, {"b"s, 2}}), none),
                     values<Int>(10, 20))),
                 values<Value>(dict_delta<Str, TS<Int>>({{"a"s, 11}, {"b"s, 12}}),
                               dict_delta<Str, TS<Int>>({{"a"s, 21}, {"b"s, 22}})));
}

TEST_CASE("context wiring: stacked try_except_ boundaries import context")
{
    stdlib::register_standard_operators();
    CHECK_OUTPUT(eval_node<StackedTryContextGraph>(values<Int>(7, none, 9)),
                 values<Int>(7, none, 9));
}
