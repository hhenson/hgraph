#include <hgraph/lib/std/std_operators.h>
#include <hgraph/lib/testing/check_output.h>
#include <hgraph/lib/testing/eval_node.h>
#include <hgraph/types/graph_wiring.h>
#include <hgraph/types/lift.h>
#include <hgraph/types/wired_fn.h>

#include <catch2/catch_test_macros.hpp>

#include <array>
#include <limits>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>

namespace
{
    using namespace hgraph;
    using namespace hgraph::testing;

    [[nodiscard]] Int value_as_int(Value value)
    {
        return value.view().checked_as<Int>();
    }

    [[nodiscard]] Float value_as_float(Value value)
    {
        return value.view().checked_as<Float>();
    }

    [[nodiscard]] Bool value_as_bool(Value value)
    {
        return value.view().checked_as<Bool>();
    }

    [[nodiscard]] stdlib::CmpResult value_as_cmp(Value value)
    {
        return value.view().checked_as<stdlib::CmpResult>();
    }

    template <typename F, typename A, typename B>
    [[nodiscard]] Value eval_binary(A lhs_value, B rhs_value)
    {
        const WiredFn f = lift<F>();
        if (f.lifted == nullptr) { throw std::logic_error("lifted kernel metadata is missing"); }

        Value lhs{lhs_value};
        Value rhs{rhs_value};
        std::array<ValueView, 2> args{lhs.view(), rhs.view()};
        return f.lifted->eval(std::span<const ValueView>{args.data(), args.size()});
    }

    template <typename F, typename A>
    [[nodiscard]] Value eval_unary(A value)
    {
        const WiredFn f = lift<F>();
        if (f.lifted == nullptr) { throw std::logic_error("lifted kernel metadata is missing"); }

        Value arg{value};
        std::array<ValueView, 1> args{arg.view()};
        return f.lifted->eval(std::span<const ValueView>{args.data(), args.size()});
    }

    struct LiftedFormat
    {
        static constexpr const char *name = "lifted_format";
        static constexpr std::array<std::string_view, 2> parameter_names{"a", "b"};

        [[nodiscard]] static Str apply(Int a, const Str &b) { return std::to_string(a) + ":" + b; }
    };

    struct LiftedFormatGraph
    {
        static constexpr auto name = "lifted_format_graph";

        static Port<TS<Str>> compose(Wiring &w, Port<TS<Int>> a, Port<TS<Str>> b)
        {
            const WiredFn f = lift<LiftedFormat>();
            const std::array<WiringPortRef, 2> args{a.erased(), b.erased()};
            return Port<TS<Str>>{w, f.wire(w, std::span<const WiringPortRef>{args.data(), args.size()})};
        }
    };

    struct LiftedAddNoIdentity
    {
        static constexpr const char *name = "lifted_add_no_identity";
        static constexpr std::array<std::string_view, 2> parameter_names{"lhs", "rhs"};
        static constexpr bool associative = true;
        static constexpr bool commutative = true;

        [[nodiscard]] static Int apply(Int lhs, Int rhs) { return lhs + rhs; }
    };
}  // namespace

TEST_CASE("lift: a scalar function wires as a time-series compute node")
{
    using namespace hgraph;
    stdlib::register_standard_operators();

    CHECK_OUTPUT(eval_node<LiftedFormatGraph>(values<Int>(1, 2), values<Str>(Str{"x"}, Str{"y"})),
                 values<Str>(Str{"1:x"}, Str{"2:y"}));
}

TEST_CASE("lift: explicit identity is kernel metadata and part of function identity")
{
    using namespace hgraph;
    stdlib::register_standard_operators();

    const WiredFn no_identity = lift<LiftedAddNoIdentity>();
    REQUIRE(no_identity.lifted != nullptr);
    CHECK_FALSE(no_identity.lifted->has_identity());
    CHECK_THROWS_AS(no_identity.lifted->identity_value(), std::logic_error);

    const WiredFn explicit_zero = lift<LiftedAddNoIdentity, Int{0}>();
    REQUIRE(explicit_zero.lifted != nullptr);
    CHECK(explicit_zero.lifted->has_identity());
    CHECK(value_as_int(explicit_zero.lifted->identity_value()) == Int{0});

    const WiredFn explicit_five = lift<LiftedAddNoIdentity, Int{5}>();
    REQUIRE(explicit_five.lifted != nullptr);
    CHECK(explicit_five.lifted->has_identity());
    CHECK(value_as_int(explicit_five.lifted->identity_value()) == Int{5});

    CHECK(explicit_zero == lift<LiftedAddNoIdentity, Int{0}>());
    CHECK_FALSE(explicit_zero == no_identity);
    CHECK_FALSE(explicit_zero == explicit_five);

    Value lhs{Int{2}};
    Value rhs{Int{3}};
    std::array<ValueView, 2> args{lhs.view(), rhs.view()};
    CHECK(value_as_int(explicit_zero.lifted->eval(std::span<const ValueView>{args.data(), args.size()})) ==
          Int{5});
}

TEST_CASE("lift: explicit identity overrides a function-provided identity")
{
    using namespace hgraph;
    stdlib::register_standard_operators();

    const WiredFn function_identity = lift<stdlib::scalar_add<Int>>();
    REQUIRE(function_identity.lifted != nullptr);
    CHECK(function_identity.lifted->has_identity());
    CHECK(value_as_int(function_identity.lifted->identity_value()) == Int{0});

    const WiredFn explicit_identity = lift<stdlib::scalar_add<Int>, Int{7}>();
    REQUIRE(explicit_identity.lifted != nullptr);
    CHECK(explicit_identity.lifted->has_identity());
    CHECK(value_as_int(explicit_identity.lifted->identity_value()) == Int{7});
    CHECK_FALSE(explicit_identity == function_identity);
}

TEST_CASE("lift: standard arithmetic kernels expose scalar operator semantics")
{
    using namespace hgraph;
    stdlib::register_standard_operators();

    const WiredFn sub = lift<stdlib::scalar_sub<Int>>();
    REQUIRE(sub.lifted != nullptr);
    CHECK_FALSE(sub.lifted->has_identity());
    CHECK_FALSE(sub.lifted->associative);
    CHECK(value_as_int(eval_binary<stdlib::scalar_add<Int>>(Int{2}, Int{3})) == Int{5});
    CHECK(value_as_int(eval_binary<stdlib::scalar_sub<Int>>(Int{9}, Int{4})) == Int{5});

    const WiredFn mul = lift<stdlib::scalar_mul<Int>>();
    REQUIRE(mul.lifted != nullptr);
    CHECK(mul.lifted->has_identity());
    CHECK(value_as_int(mul.lifted->identity_value()) == Int{1});
    CHECK(mul.lifted->associative);
    CHECK(mul.lifted->commutative);
    CHECK(value_as_int(eval_binary<stdlib::scalar_mult<Int>>(Int{6}, Int{7})) == Int{42});

    const WiredFn div = lift<stdlib::scalar_div<Int>>();
    REQUIRE(div.lifted != nullptr);
    CHECK_FALSE(div.lifted->has_identity());
    CHECK(value_as_float(eval_binary<stdlib::scalar_div<Int>>(Int{7}, Int{2})) == Float{3.5});
    CHECK_THROWS_AS((eval_binary<stdlib::scalar_div<Int>>(Int{7}, Int{0})), std::domain_error);

    CHECK(value_as_int(eval_binary<stdlib::scalar_floordiv<Int>>(Int{-3}, Int{2})) == Int{-2});
    CHECK(value_as_int(eval_binary<stdlib::scalar_mod<Int>>(Int{-3}, Int{2})) == Int{1});
    CHECK(value_as_int(eval_binary<stdlib::scalar_pow<Int>>(Int{2}, Int{3})) == Int{8});
    CHECK(value_as_int(eval_binary<stdlib::scalar_pow<Int>>(Int{-2}, Int{3})) == Int{-8});
    CHECK(value_as_int(eval_binary<stdlib::scalar_pow<Int>>(Int{0}, Int{0})) == Int{1});
    CHECK(value_as_float(eval_binary<stdlib::scalar_pow<Int, Float>>(Int{4}, Float{0.5})) == Float{2.0});
    CHECK_THROWS_AS((eval_binary<stdlib::scalar_pow<Int>>(Int{2}, Int{-1})), std::domain_error);
    CHECK_THROWS_AS((eval_binary<stdlib::scalar_pow<Int>>(std::numeric_limits<Int>::max(), Int{2})),
                    std::overflow_error);
    CHECK(value_as_int(eval_unary<stdlib::scalar_neg<Int>>(Int{5})) == Int{-5});
    CHECK(value_as_int(eval_unary<stdlib::scalar_abs<Int>>(Int{-5})) == Int{5});
    CHECK(value_as_int(eval_unary<stdlib::scalar_sign<Int>>(Int{-5})) == Int{-1});
    CHECK(value_as_float(eval_unary<stdlib::scalar_ln>(Float{1.0})) == Float{0.0});
}

TEST_CASE("lift: standard min and max kernels can take explicit identities")
{
    using namespace hgraph;
    stdlib::register_standard_operators();

    const WiredFn min_no_identity = lift<stdlib::scalar_min<Int>>();
    REQUIRE(min_no_identity.lifted != nullptr);
    CHECK_FALSE(min_no_identity.lifted->has_identity());
    CHECK(min_no_identity.lifted->associative);
    CHECK(min_no_identity.lifted->commutative);
    CHECK(value_as_int(eval_binary<stdlib::scalar_min<Int>>(Int{9}, Int{4})) == Int{4});

    const WiredFn min_with_identity = lift<stdlib::scalar_min<Int>, std::numeric_limits<Int>::max()>();
    REQUIRE(min_with_identity.lifted != nullptr);
    CHECK(min_with_identity.lifted->has_identity());
    CHECK(value_as_int(min_with_identity.lifted->identity_value()) == std::numeric_limits<Int>::max());

    const WiredFn max_with_identity = lift<stdlib::scalar_max<Int>, std::numeric_limits<Int>::lowest()>();
    REQUIRE(max_with_identity.lifted != nullptr);
    CHECK(max_with_identity.lifted->has_identity());
    CHECK(value_as_int(max_with_identity.lifted->identity_value()) == std::numeric_limits<Int>::lowest());
    CHECK(value_as_int(eval_binary<stdlib::scalar_max<Int>>(Int{9}, Int{4})) == Int{9});
}

TEST_CASE("lift: standard comparison and logical kernels evaluate directly")
{
    using namespace hgraph;
    stdlib::register_standard_operators();

    CHECK(value_as_bool(eval_binary<stdlib::scalar_eq<Int>>(Int{5}, Int{5})));
    CHECK(value_as_bool(eval_binary<stdlib::scalar_lt<Int>>(Int{4}, Int{5})));
    CHECK(value_as_cmp(eval_binary<stdlib::scalar_cmp<Int>>(Int{7}, Int{5})) == stdlib::CmpResult::GT);

    const WiredFn all = lift<stdlib::scalar_and<Bool>>();
    REQUIRE(all.lifted != nullptr);
    CHECK(all.lifted->has_identity());
    CHECK(value_as_bool(all.lifted->identity_value()));
    CHECK_FALSE(value_as_bool(eval_binary<stdlib::scalar_and<Bool>>(true, false)));

    const WiredFn any = lift<stdlib::scalar_or<Bool>>();
    REQUIRE(any.lifted != nullptr);
    CHECK(any.lifted->has_identity());
    CHECK_FALSE(value_as_bool(any.lifted->identity_value()));
    CHECK(value_as_bool(eval_binary<stdlib::scalar_or<Bool>>(false, true)));
    CHECK(value_as_bool(eval_unary<stdlib::scalar_not<Int>>(Int{0})));

    CHECK(value_as_int(eval_binary<stdlib::scalar_bit_and<Int>>(Int{6}, Int{3})) == Int{2});
    CHECK(value_as_int(eval_binary<stdlib::scalar_bit_or<Int>>(Int{4}, Int{1})) == Int{5});
    CHECK(value_as_int(eval_binary<stdlib::scalar_bit_xor<Int>>(Int{6}, Int{3})) == Int{5});
    CHECK(value_as_int(eval_unary<stdlib::scalar_invert<Int>>(Int{0})) == Int{-1});
    CHECK(value_as_int(eval_binary<stdlib::scalar_lshift>(Int{3}, Int{2})) == Int{12});
    CHECK(value_as_int(eval_binary<stdlib::scalar_rshift>(Int{12}, Int{2})) == Int{3});
}

TEST_CASE("lift: standard operator overloads expose their lifted kernel metadata")
{
    using namespace hgraph;
    stdlib::register_standard_operators();

    CHECK(std::string{fn<stdlib::add_>().operator_name} == "add_");
    CHECK(fn<stdlib::min_>().arity == std::size_t{2});

    WiringArg lhs;
    lhs.kind = WiringArg::Kind::TimeSeries;
    lhs.port.schema = ts_type<TS<Int>>();

    WiringArg rhs = lhs;
    std::array<WiringArg, 2> args{lhs, rhs};

    ResolvedOperatorCall resolved = OperatorRegistry::instance().resolve(
        "add_", std::span<const WiringArg>{args.data(), args.size()}, true);
    REQUIRE(resolved.impl != nullptr);
    REQUIRE(resolved.impl->lifted_kernel != nullptr);
    CHECK(resolved.impl->lifted_kernel == lift<stdlib::scalar_add<Int>>().lifted);
}
