#include "hgraph_ir/lower.h"
#include "ir/lower.h"
#include "ir/type_check.h"
#include "semantics/resolve.h"
#include "syntax/parser.h"
#include "wiring/backend.h"
#include "wiring/delta_trace.h"
#include "wiring/operator_types.h"

#include <hgraph/lib/std/operators/collection.h>
#include <hgraph/lib/std/operators/conversion.h>
#include <hgraph/lib/std/operators/logical.h>
#include <hgraph/lib/std/value_util.h>
#include <hgraph/types/operator_dispatch.h>
#include <hgl/global_key_preflight.h>

#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers.hpp>

#include <algorithm>
#include <chrono>
#include <sstream>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

using namespace hgl::syntax;
using namespace hgl::semantics;
using namespace hgl::wiring;

namespace
{
    struct generic_preflight_bool_output {
        static constexpr auto name = "hgl_generic_preflight_bool_output";
        static void eval(hgraph::In<"value", hgraph::TsVar<"T">>, hgraph::Out<hgraph::TS<hgraph::Bool>>);
    };
    struct generic_preflight_variable_output {
        static constexpr auto name = "hgl_generic_preflight_variable_output";
        static void eval(hgraph::In<"value", hgraph::TsVar<"T">>, hgraph::Out<hgraph::TsVar<"T">>);
    };
    static_assert(std::is_same_v<decltype(hgl::ordinary::preflight_wire<generic_preflight_bool_output>(
        std::declval<hgraph::Wiring &>(), std::declval<hgraph::Port<hgraph::TS<hgraph::Int>>>())),
        hgraph::Port<hgraph::TS<hgraph::Bool>>>);
    static_assert(std::is_same_v<decltype(hgl::ordinary::preflight_wire<generic_preflight_variable_output>(
        std::declval<hgraph::Wiring &>(), std::declval<hgraph::Port<hgraph::TS<hgraph::Int>>>())), hgraph::Port<void>>);

    struct fixed_pair_operator
        : hgraph::Operator<"hgl_fixed_pair", hgraph::In<"a", hgraph::TS<hgraph::Float>>, hgraph::In<"b", hgraph::TS<hgraph::Float>>,
                           hgraph::Out<hgraph::TSL<hgraph::TS<hgraph::Float>, 2>>>
    {};

    struct fixed_pair_graph
    {
        static constexpr auto name = "hgl_fixed_pair_graph";

        static auto compose(hgraph::Wiring &w, hgraph::Port<hgraph::TS<hgraph::Float>> a,
                            hgraph::Port<hgraph::TS<hgraph::Float>> b) {
            return hgraph::stdlib::to_tsl(w, a, b);
        }
    };

    struct fixed_iteration_pair_operator
        : hgraph::Operator<"hgl_fixed_iteration_pair", hgraph::In<"a", hgraph::TS<hgraph::Float>>,
                           hgraph::In<"b", hgraph::TS<hgraph::Float>>, hgraph::Out<hgraph::TSL<hgraph::TS<hgraph::Float>, 2>>>
    {};

    struct fixed_iteration_pair_graph
    {
        static constexpr auto name = "hgl_fixed_iteration_pair_graph";

        static auto compose(hgraph::Wiring &w, hgraph::Port<hgraph::TS<hgraph::Float>> a,
                            hgraph::Port<hgraph::TS<hgraph::Float>> b) {
            return hgraph::stdlib::to_tsl(w, a, b);
        }
    };

    struct dynamic_iteration_book_operator
        : hgraph::Operator<"hgl_dynamic_iteration_book", hgraph::In<"trigger", hgraph::TS<hgraph::Float>>,
                           hgraph::Out<hgraph::TSD<hgraph::Str, hgraph::TS<hgraph::Float>>>>
    {};

    struct dynamic_iteration_book_graph
    {
        static constexpr auto name = "hgl_dynamic_iteration_book_graph";

        static auto compose(hgraph::Wiring &w, hgraph::Port<hgraph::TS<hgraph::Float>> trigger) {
            static_cast<void>(trigger);
            return hgraph::wire<hgraph::stdlib::const_, hgraph::TSD<hgraph::Str, hgraph::TS<hgraph::Float>>>(
                w, hgraph::stdlib::make_map<hgraph::Str, hgraph::Float>(
                       {{hgraph::Str{"A"}, hgraph::Float{1.0}}, {hgraph::Str{"B"}, hgraph::Float{2.0}}}));
        }
    };

    // Each test registers its own operator once; a shared one would register
    // twice and become an ambiguity.
    struct map_lambda_book_operator : hgraph::Operator<"hgl_map_lambda_book", hgraph::In<"trigger", hgraph::TS<hgraph::Float>>,
                                                       hgraph::Out<hgraph::TSD<hgraph::Str, hgraph::TS<hgraph::Float>>>>
    {};

    struct map_lambda_book_graph
    {
        static constexpr auto name = "hgl_map_lambda_book_graph";

        static auto compose(hgraph::Wiring &w, hgraph::Port<hgraph::TS<hgraph::Float>> trigger) {
            static_cast<void>(trigger);
            return hgraph::wire<hgraph::stdlib::const_, hgraph::TSD<hgraph::Str, hgraph::TS<hgraph::Float>>>(
                w, hgraph::stdlib::make_map<hgraph::Str, hgraph::Float>(
                       {{hgraph::Str{"A"}, hgraph::Float{1.0}}, {hgraph::Str{"B"}, hgraph::Float{2.0}}}));
        }
    };

    struct dynamic_iteration_list_operator
        : hgraph::Operator<"hgl_dynamic_iteration_list", hgraph::In<"trigger", hgraph::TS<hgraph::Float>>,
                           hgraph::Out<hgraph::TSL<hgraph::TS<hgraph::Float>>>>
    {};

    struct dynamic_iteration_list_graph
    {
        static constexpr auto name = "hgl_dynamic_iteration_list_graph";

        static auto compose(hgraph::Wiring &w, hgraph::Port<hgraph::TS<hgraph::Float>> trigger) {
            static_cast<void>(trigger);
            return hgraph::wire<hgraph::stdlib::const_, hgraph::TSL<hgraph::TS<hgraph::Float>>>(
                w, hgraph::stdlib::make_list<hgraph::Float>({hgraph::Float{1.0}, hgraph::Float{2.0}}));
        }
    };

    struct observed_condition_operator
        : hgraph::Operator<"hgl_observed_condition", hgraph::In<"condition", hgraph::TS<hgraph::Bool>>,
                           hgraph::Out<hgraph::TS<hgraph::Bool>>>
    {};

    struct observed_condition_graph
    {
        static constexpr auto name = "hgl_observed_condition_graph";
        static inline int     compose_calls{0};

        static auto compose(hgraph::Wiring &w, hgraph::Port<hgraph::TS<hgraph::Bool>> condition) {
            ++compose_calls;
            auto negated = hgraph::wire<hgraph::stdlib::not_>(w, condition).as<hgraph::TS<hgraph::Bool>>();
            return hgraph::wire<hgraph::stdlib::not_>(w, negated).as<hgraph::TS<hgraph::Bool>>();
        }
    };

    // A unit through the frontend against the live registry, ready for the
    // backend (developer guide, "Direct-wiring backend", "First pass").
    struct Unit
    {
        SourceFile             file;
        DiagnosticSink         diagnostics;
        ast::Module            module;
        ResolvedModule         resolved;
        hgl::ir::hir::Module   hir;
        hgl::hgraph_ir::Module graph_ir;

        explicit Unit(std::string text)
            : file{"test.hgl", std::move(text)}, module{parse(file, diagnostics)},
              resolved{resolve(file, module, has_operator, diagnostics)} {
            if (!diagnostics.has_errors()) { hir = hgl::ir::lower_to_hir(module, resolved, diagnostics); }
            if (!diagnostics.has_errors() && hgl::ir::complete_hir(hir, resolve_operator_types, diagnostics)) {
                graph_ir = hgl::hgraph_ir::lower(hir, diagnostics);
            }
        }

        [[nodiscard]] std::vector<TestResult> tests(std::vector<std::string> names = {}) {
            INFO(diagnostics.render(file));
            REQUIRE_FALSE(diagnostics.has_errors());
            TestOptions options;
            options.names = std::move(names);
            return run_tests(file, graph_ir, options, diagnostics);
        }

        [[nodiscard]] bool has(Category category, std::string_view fragment) const {
            return std::any_of(diagnostics.diagnostics().begin(), diagnostics.diagnostics().end(), [&](const Diagnostic &d) {
                return d.category == category && d.message.find(fragment) != std::string::npos;
            });
        }
    };

    TestResult only(std::vector<TestResult> results) {
        REQUIRE(results.size() == 1);
        return std::move(results.front());
    }
}  // namespace

TEST_CASE("constant expressions fold in a test body", "[wiring]") {
    Unit             unit{R"(
module t

fn third(const xs: list<i64>) -> i64 => xs[2]

test folding {
    let a = 1 + 2
    var b = a * 2.5
    b -= 0.5
    assert a == 3
    assert b == 7.0
    assert 7 / 2 == 3.5
    assert 7 % 3 == 1
    assert 1 == 1.0
    assert "ab" + "c" == "abc"
    assert @2026-09-03T10:30+01[Europe/London] != @2026-09-03T09:30Z[UTC]
    assert (1, 2.0)[1] == 2.0
    let xs: list<i64> = [10, 20, 30]
    assert xs[2] == 30
    assert third([1, 2, 3]) == 3
    assert 2s + 500ms == 2500ms
    assert !(a > b) && (a < b || false)
}
)"};
    const TestResult result = only(unit.tests());
    INFO(result.message);
    CHECK(result.passed);
}

TEST_CASE("folded integer constants retain full i64 precision", "[wiring][constants][integer]") {
    Unit unit{R"(
module t

test exact {
    9007199254740993 + 0
}
)"};
    INFO(unit.diagnostics.render(unit.file));
    REQUIRE_FALSE(unit.diagnostics.has_errors());
    TestOptions options;
    options.describe_tail   = true;
    const TestResult result = only(run_tests(unit.file, unit.graph_ir, options, unit.diagnostics));
    INFO(result.message);
    CHECK(result.passed);
    CHECK(result.tail == "9007199254740993");
}

TEST_CASE("activated constant arithmetic diagnoses overflow", "[wiring][constants][overflow]") {
    Unit             unit{R"(
module t

fn add_one(const value: duration) -> duration => value + 1us

test overflow {
    add_one(106751991d4h54s775ms807us)
}
)"};
    const TestResult result = only(unit.tests());
    CHECK_FALSE(result.passed);
    CHECK(unit.has(Category::Type, "overflow in a temporal constant expression"));
}

TEST_CASE("var assignment retains the initializer's static type", "[wiring]") {
    SECTION("an inferred i64 cannot be narrowed") {
        Unit unit{R"(
module t
test narrowing {
    var y = 1
    y = 2.5
    assert y == 2
}
)"};
        CHECK(unit.diagnostics.has_errors());
        CHECK(unit.has(Category::Type, "assignment has type f64, expected i64"));
    }
    SECTION("compound division cannot change i64 to f64") {
        Unit             unit{R"(
module t
test narrowing {
    var y = 4
    y /= 2
    assert y == 2
}
)"};
        CHECK(unit.diagnostics.has_errors());
        CHECK(unit.has(Category::Type, "assignment has type f64, expected i64"));
    }
    SECTION("i64 widens into an f64 var") {
        Unit             unit{R"(
module t
test widening {
    var y = 1.0
    y = 2
    assert y == 2.0
}
)"};
        const TestResult result = only(unit.tests());
        INFO(result.message);
        CHECK(result.passed);
    }
}

TEST_CASE("a typed var can receive its first value after declaration", "[wiring][locals][control-flow]") {
    Unit             unit{R"(
module t

test assigned {
    var result: i64
    if true {
        result = 4
    } else {
        result = 5
    }
    assert result == 4
}
)"};
    const TestResult result = only(unit.tests());
    INFO(unit.diagnostics.render(unit.file));
    INFO(result.message);
    CHECK(result.passed);
}

TEST_CASE("composition results preserve their runtime schema", "[wiring][types]") {
    Unit             unit{R"(
module t

fn widen(x: i64) -> f64 => x

test widening {
    assert eval(widen, x: [1, 2]) == [1.0, 2.0]
}
)"};
    const TestResult result = only(unit.tests());
    INFO(unit.diagnostics.render(unit.file));
    INFO(result.message);
    CHECK(result.passed);
}

TEST_CASE("a temporal if wires value-producing branches through switch", "[wiring][control-flow][conditional]") {
    Unit             unit{R"(
module t

fn choose(condition: bool, x: i64, y: i64) -> i64 {
    if condition {
        x + 1
    } else {
        y - 1
    }
}

test choose_ticks {
    assert eval(choose, condition: [true, true, false], x: [1, 2, 3], y: [10, 20, 30]) == [2, 3, 29]
}
)"};
    const TestResult result = only(unit.tests());
    INFO(unit.diagnostics.render(unit.file));
    INFO(result.message);
    CHECK(result.passed);
}

TEST_CASE("a value-producing temporal if without else uses a typed never-ticking branch", "[wiring][control-flow][conditional]") {
    Unit             unit{R"(
module t

fn choose(condition: bool, value: i64) -> i64 {
    if condition {
        value + 1
    }
}

test choose_ticks {
    assert eval(choose, condition: [false, true, false], value: [1, 2, 3]) == [_, 3, _]
}
)"};
    const TestResult result = only(unit.tests());
    INFO(unit.diagnostics.render(unit.file));
    INFO(result.message);
    CHECK(result.passed);
}

TEST_CASE("a temporal if evaluates its condition composition once", "[wiring][control-flow][conditional]") {
    ensure_session();
    hgraph::register_graph_overload<observed_condition_operator, observed_condition_graph>();
    observed_condition_graph::compose_calls = 0;
    Unit             unit{R"(
module t

use hgraph.std::{hgl_observed_condition}

fn choose(condition: bool, x: i64, y: i64) -> i64 {
    if hgl_observed_condition(condition) {
        x + 0
    } else {
        y + 0
    }
}

test choose_once {
    eval(choose, condition: [true], x: [1], y: [2])
}
)"};
    const TestResult result = only(unit.tests());
    INFO(unit.diagnostics.render(unit.file));
    INFO(result.message);
    CHECK(result.passed);
    CHECK(observed_condition_graph::compose_calls == 1);
}

TEST_CASE("a temporal early return assigns the remaining body to the falling branch",
          "[wiring][control-flow][conditional][continuation]") {
    Unit             unit{R"(
module t

fn choose(condition: bool, x: i64, y: i64) -> i64 {
    if condition {
        return x + 1
    }

    let r = y - 1
    return r * 2
}

test choose_ticks {
    assert eval(choose, condition: [true, true, false], x: [1, 2, 3], y: [10, 20, 30]) == [2, 3, 58]
}
)"};
    const TestResult result = only(unit.tests());
    INFO(unit.diagnostics.render(unit.file));
    INFO(result.message);
    CHECK(result.passed);
}

TEST_CASE("a temporal early return supports tail and outputless continuations",
          "[wiring][control-flow][conditional][continuation]") {
    Unit                          unit{R"(
module t

use hgraph.std::{null_sink}

fn choose(condition: bool, x: i64, y: i64) -> i64 {
    if condition {
        return x + 1
    }

    let r = y - 1
    r * 2
}

fn observe(enabled: bool, value: f64) {
    if enabled {
        return
    }

    null_sink(value)
}

test choose_ticks {
    assert eval(choose, condition: [true, false], x: [1, 2], y: [10, 20]) == [2, 38]
}

test observe_ticks {
    eval(observe, enabled: [true, false], value: [1.0, 2.0])
}
)"};
    const std::vector<TestResult> results = unit.tests();
    INFO(unit.diagnostics.render(unit.file));
    REQUIRE(results.size() == 2U);
    for (const TestResult &result : results) {
        INFO(result.message);
        CHECK(result.passed);
    }
}

TEST_CASE("a temporal early-return continuation can assign a predeclared variable",
          "[wiring][control-flow][conditional][continuation]") {
    Unit             unit{R"(
module t

fn choose(condition: bool, x: i64, y: i64) -> i64 {
    var result: i64
    if condition {
        return x + 1
    }
    result = y - 1
    return result * 2
}

test choose_ticks {
    assert eval(choose, condition: [true, false], x: [1, 2], y: [10, 20]) == [2, 38]
}
)"};
    const TestResult result = only(unit.tests());
    INFO(unit.diagnostics.render(unit.file));
    INFO(result.message);
    CHECK(result.passed);
}

TEST_CASE("nested temporal early returns retain every enclosing continuation",
          "[wiring][control-flow][conditional][continuation]") {
    Unit                          unit{R"(
module t

fn choose(outer: bool, inner: bool, x: i64, y: i64, z: i64) -> i64 {
    if outer {
        if inner {
            return x + 1
        }
        let r = y - 1
        return r * 2
    }
    return z * 3
}

fn choose_deep(outer: bool, middle: bool, inner: bool, x: i64, y: i64, z: i64, fallback: i64) -> i64 {
    if outer {
        return x
    }
    if middle {
        if inner {
            return y
        }
        return z
    }
    return fallback
}

test choose_ticks {
    assert eval(
        choose,
        outer: [true, true, false],
        inner: [true, false, false],
        x: [1, 2, 3],
        y: [10, 20, 30],
        z: [100, 200, 300],
    ) == [2, 38, 900]
}

test choose_deep_ticks {
    assert eval(
        choose_deep,
        outer: [true, false, false, false],
        middle: [false, false, true, true],
        inner: [false, false, true, false],
        x: [1, 2, 3, 4],
        y: [10, 20, 30, 40],
        z: [100, 200, 300, 400],
        fallback: [1000, 2000, 3000, 4000],
    ) == [1, 2000, 30, 400]
}
)"};
    const std::vector<TestResult> results = unit.tests();
    INFO(unit.diagnostics.render(unit.file));
    REQUIRE(results.size() == 2U);
    for (const TestResult &result : results) {
        INFO(result.message);
        CHECK(result.passed);
    }
}

TEST_CASE("an outputless temporal if wires a sink switch", "[wiring][control-flow][conditional]") {
    Unit             unit{R"(
module t

use hgraph.std::{null_sink}

fn observe(enabled: bool, value: f64) {
    if enabled {
        null_sink(value)
    } else {
        null_sink(value)
    }
}

test observe_sink {
    eval(observe, enabled: [false, true], value: [1.0, 2.0])
}
)"};
    const TestResult result = only(unit.tests());
    INFO(unit.diagnostics.render(unit.file));
    INFO(result.message);
    CHECK(result.passed);
}

TEST_CASE("an outputless temporal if is independent of the enclosing result", "[wiring][control-flow][conditional]") {
    Unit             unit{R"(
module t

use hgraph.std::{null_sink}

fn observe(enabled: bool, value: f64) -> f64 {
    if enabled {
        null_sink(value)
    }
    value
}

test observe_and_forward {
    assert eval(observe, enabled: [false, true], value: [1.0, 2.0]) == [1.0, 2.0]
}
)"};
    const TestResult result = only(unit.tests());
    INFO(unit.diagnostics.render(unit.file));
    INFO(result.message);
    CHECK(result.passed);
}

TEST_CASE("a temporal if remaps one assigned result into the enclosing graph", "[wiring][control-flow][conditional]") {
    Unit             unit{R"(
module t

fn adjusted(condition: bool, x: i64, y: i64) -> i64 {
    var result: i64
    if condition {
        result = x + 1
    } else {
        result = y - 1
    }
    return result * 2
}

test adjusted_ticks {
    assert eval(adjusted, condition: [true, true, false], x: [1, 2, 3], y: [10, 20, 30]) == [4, 6, 58]
}
)"};
    const TestResult result = only(unit.tests());
    INFO(unit.diagnostics.render(unit.file));
    INFO(result.message);
    CHECK(result.passed);
}

TEST_CASE("a temporal if can reuse a result assigned earlier in each branch", "[wiring][control-flow][conditional]") {
    Unit             unit{R"(
module t

fn adjusted(condition: bool, x: i64, y: i64) -> i64 {
    var result: i64
    if condition {
        result = x + 1
        result = result * 2
    } else {
        result = y - 1
        result = result * 3
    }
    result
}

test adjusted_ticks {
    assert eval(adjusted, condition: [true, false], x: [1, 2], y: [10, 20]) == [4, 57]
}
)"};
    const TestResult result = only(unit.tests());
    INFO(unit.diagnostics.render(unit.file));
    INFO(result.message);
    CHECK(result.passed);
}

TEST_CASE("a temporal if remaps several assigned results into the enclosing graph", "[wiring][control-flow][conditional]") {
    Unit             unit{R"(
module t

fn adjusted(condition: bool, x: i64, y: i64) -> i64 {
    var result: i64
    var offset: i64
    if condition {
        result = x + 1
        offset = x
    } else {
        result = y - 1
        offset = y
    }
    return result * 2 + offset
}

test adjusted_ticks {
    assert eval(adjusted, condition: [true, true, false], x: [1, 2, 3], y: [10, 20, 30]) == [5, 8, 88]
}
)"};
    const TestResult result = only(unit.tests());
    INFO(unit.diagnostics.render(unit.file));
    INFO(result.message);
    CHECK(result.passed);
}

TEST_CASE("a temporal if combines its expression result with an escaping assignment", "[wiring][control-flow][conditional]") {
    Unit             unit{R"(
module t

fn adjusted(condition: bool, x: i64, y: i64) -> i64 {
    var offset: i64
    let result = if condition {
        offset = x + 1
        x * 2
    } else {
        offset = y - 1
        y * 3
    }
    result + offset
}

test adjusted_ticks {
    assert eval(adjusted, condition: [true, true, false], x: [1, 2, 3], y: [10, 20, 30]) == [4, 7, 119]
}
)"};
    const TestResult result = only(unit.tests());
    INFO(unit.diagnostics.render(unit.file));
    INFO(result.message);
    CHECK(result.passed);
}

TEST_CASE("a temporal conditional forwards an existing binding on an unassigned branch", "[wiring][control-flow][conditional]") {
    Unit             unit{R"(
module t

fn adjusted(condition: bool, x: i64) -> i64 {
    var result: i64 = x
    if condition {
        result = result + 1
    }
    return result
}

test adjusted_ticks {
    assert eval(adjusted, condition: [false, true, false], x: [1, 2, 3]) == [1, 3, 3]
}
)"};
    const TestResult result = only(unit.tests());
    INFO(unit.diagnostics.render(unit.file));
    INFO(result.message);
    CHECK(result.passed);
}

TEST_CASE("conditional capture names do not opt into switch key binding", "[wiring][control-flow][conditional]") {
    Unit             unit{R"(
module t

fn adjusted(condition: bool, x: i64) -> i64 {
    var key: i64 = x
    if condition {
        key = key + 1
    }
    return key
}

test adjusted_ticks {
    assert eval(adjusted, condition: [false, true, false], x: [1, 2, 3]) == [1, 3, 3]
}
)"};
    const TestResult result = only(unit.tests());
    INFO(unit.diagnostics.render(unit.file));
    INFO(result.message);
    CHECK(result.passed);
}

TEST_CASE("temporal conditional forwarding is planned independently for each structural result field",
          "[wiring][control-flow][conditional]") {
    Unit             unit{R"(
module t

fn adjusted(condition: bool, x: i64, y: i64) -> i64 {
    var left: i64 = x
    var right: i64 = y
    if condition {
        left = left + 1
    } else {
        right = right + 1
    }
    return left + right
}

test adjusted_ticks {
    assert eval(adjusted, condition: [true, false], x: [1, 2], y: [10, 20]) == [12, 23]
}
)"};
    const TestResult result = only(unit.tests());
    INFO(unit.diagnostics.render(unit.file));
    INFO(result.message);
    CHECK(result.passed);
}

TEST_CASE("a temporal else-if fails instead of dropping its sink", "[wiring][control-flow][conditional]") {
    Unit unit{R"(
module t

use hgraph.std::{null_sink}

fn observe(first: bool, second: bool, value: f64) {
    if first {
        null_sink(value)
    } else if second {
        null_sink(value)
    }
}

test observe_sink {
    eval(observe, first: [false], second: [true], value: [1.0])
}
)"};
    // The shared control-flow analysis reports the rule during hgraph IR
    // lowering, before either backend runs (control_flow.h, PlanIssue).
    CHECK(unit.diagnostics.has_errors());
    CHECK(unit.has(Category::Backend, "temporal 'else if' is not supported"));
}

TEST_CASE("composition boundaries preserve compatible fixed list ports", "[wiring][types][list]") {
    ensure_session();
    hgraph::register_graph_overload<fixed_pair_operator, fixed_pair_graph>();
    Unit             unit{R"(
module t

use hgraph.std::{hgl_fixed_pair, sum}

fn fixed(a: f64, b: f64) -> list<f64, 2> => hgl_fixed_pair(a, b)
fn values(a: f64, b: f64) -> list<f64> => fixed(a, b)
fn total(a: f64, b: f64) -> f64 => sum(values(a, b))

test widening {
    assert eval(total, a: [1.0], b: [2.0]) == [3.0]
}
)"};
    const TestResult result = only(unit.tests());
    INFO(unit.diagnostics.render(unit.file));
    INFO(result.message);
    CHECK(result.passed);
}

TEST_CASE("outputless operators wire without a result schema", "[wiring][operators][sink]") {
    Unit             unit{R"(
module t

use hgraph.std::{null_sink}

fn discard(value: f64) {
    null_sink(value)
}

test sink {
    eval(discard, value: [1.0])
}
)"};
    const TestResult result = only(unit.tests());
    INFO(unit.diagnostics.render(unit.file));
    INFO(result.message);
    CHECK(result.passed);
}

TEST_CASE("direct wiring expands a homogeneous composition pack", "[wiring][parameter-pack]") {
    ensure_session();
    Unit             unit{R"(
module t

use hgraph.std::{all_}

fn all_inputs(inputs: ...bool) -> bool => all_(inputs)
fn all_values(values: ...bool) -> bool => all_inputs(values)
fn pair(a: bool, b: bool) -> bool => all_values(a, b)

test pack {
    assert eval(pair, a: [true, true], b: [true, false]) == [true, false]
}
)"};
    const TestResult result = only(unit.tests());
    INFO(unit.diagnostics.render(unit.file));
    INFO(result.message);
    CHECK(result.passed);
}

TEST_CASE("fixed temporal list graph iteration wires every child", "[wiring][iteration]") {
    ensure_session();
    hgraph::register_graph_overload<fixed_iteration_pair_operator, fixed_iteration_pair_graph>();
    Unit             unit{R"(
module t

use hgraph.std::{hgl_fixed_iteration_pair, null_sink}

fn discard(a: f64, b: f64) {
    let samples: list<f64, 2> = hgl_fixed_iteration_pair(a, b)
    for sample in elements(samples) {
        null_sink(sample)
    }
    for index, sample in items(samples) {
        null_sink(sample + index)
    }
}

test fixed_iteration {
    eval(discard, a: [1.0], b: [2.0])
}
)"};
    const TestResult result = only(unit.tests());
    INFO(unit.diagnostics.render(unit.file));
    INFO(result.message);
    CHECK(result.passed);
}

TEST_CASE("dynamic collection graph iteration compiles independent child graphs", "[wiring][iteration]") {
    ensure_session();
    hgraph::register_graph_overload<dynamic_iteration_book_operator, dynamic_iteration_book_graph>();
    hgraph::register_graph_overload<dynamic_iteration_list_operator, dynamic_iteration_list_graph>();
    Unit             unit{R"(
module t

use hgraph.std::{hgl_dynamic_iteration_book, hgl_dynamic_iteration_list, null_sink}

fn discard(trigger: f64, offset: f64) {
    let book: map<str, f64> = hgl_dynamic_iteration_book(trigger)
    let samples: list<f64> = hgl_dynamic_iteration_list(trigger)
    let peers: list<f64> = hgl_dynamic_iteration_list(trigger)
    for value in values(book) {
        null_sink(value + offset)
    }
    for key, value in items(book) {
        null_sink(value + offset)
    }
    for value in elements(samples) {
        null_sink(valid(peers))
    }
    for index, value in items(samples) {
        null_sink(value + index + offset)
    }
}

test dynamic_iteration {
    eval(discard, trigger: [1.0], offset: [10.0])
}
)"};
    const TestResult result = only(unit.tests());
    INFO(unit.diagnostics.render(unit.file));
    INFO(result.message);
    CHECK(result.passed);
}

TEST_CASE("an anonymous map function wires one value-producing child graph per key", "[wiring][map]") {
    ensure_session();
    hgraph::register_graph_overload<map_lambda_book_operator, map_lambda_book_graph>();
    Unit             unit{R"(
module t

use hgraph.std::{hgl_map_lambda_book, map, sum}

# The book is {A: 1.0, B: 2.0}; each key's child adds the value to itself.
fn doubled(trigger: f64) -> f64 {
    let book: map<str, f64> = hgl_map_lambda_book(trigger)
    let sums: map<str, f64> = map(book, book, fn(a, b) => a + b)
    sum(sums)
}

test doubled_ticks {
    assert eval(doubled, trigger: [1.0]) == [6.0]
}
)"};
    const TestResult result = only(unit.tests());
    INFO(unit.diagnostics.render(unit.file));
    INFO(result.message);
    CHECK(result.passed);
}

TEST_CASE("eval drives a composition through the harness", "[wiring]") {
    Unit unit{R"(
module t

fn midpoint(tob: tuple<f64, f64>) -> f64 => (tob[0] + tob[1]) / 2.0

fn scale(x: f64, const k: f64 = 2.0) -> f64 => x * k

test midpoint_ticks {
    assert eval(midpoint, tob: [(1.0, 2.0), _, (2.0, 3.0)]) == [1.5, _, 2.5]
}

test literals_take_the_parameter_type {
    assert eval(scale, x: [1, 2], k: 3) == [3.0, 6.0]
    assert [1.0, 2.0] != eval(scale, x: [1.0, 2.0])
}

test defaults_apply {
    assert eval(scale, x: [1.0, _]) == [2.0, _]
}
)"};
    for (const TestResult &result : unit.tests()) {
        INFO(result.name << ": " << result.message);
        INFO(unit.diagnostics.render(unit.file));
        CHECK(result.passed);
    }
}

TEST_CASE("eval rejects a target without a temporal sequence input", "[wiring][harness]") {
    Unit             unit{R"(
module t

use hgraph.std::{schedule}

fn heartbeat(const every: duration) -> datetime => last_modified(schedule(every))

test const_only {
    eval(heartbeat, every: 1us)
}
)"};
    CHECK(unit.diagnostics.has_errors());
    CHECK(unit.has(Category::Type, "at least one temporal sequence input"));
}

TEST_CASE("eval retains scheduled output beyond the harness input horizon", "[wiring][harness]") {
    Unit             unit{R"(
module t

use hgraph.std::{schedule}

fn heartbeat(trigger: f64, const every: duration) -> datetime => last_modified(schedule(every, max_ticks: 3))

test bounded {
    assert len(eval(heartbeat, trigger: [1.0, 2.0], every: 1us)) == 4
}
)"};
    const TestResult result = only(unit.tests());
    INFO(unit.diagnostics.render(unit.file));
    INFO(result.message);
    CHECK(result.passed);
}

TEST_CASE("nominal struct values apply defaults and inheritance", "[wiring]") {
    Unit             unit{R"(
module suite.struct_values

abstract struct Instrument {
    symbol: str
    venue: str = "ANY"
    alias: str = null
}

struct Future: Instrument {
    venue = "XEUR"
    expiry: date
}

struct Quote {
    bid: f64
    ask: f64
    venue: str = "XNAS"
    note: str = null
}

struct Snapshot {
    quote: Quote = Quote(bid: 1.0, ask: 2.0)
}

test values {
    let q = Quote(bid: 1.0, ask: 2.0)
    let f = Future(symbol: "ES", expiry: @2026-12-18)
    assert q.bid == 1.0
    assert q.ask == 2.0
    assert q.venue == "XNAS"
    assert f.symbol == "ES"
    assert f.venue == "XEUR"
    assert Snapshot().quote.ask == 2.0
}
)"};
    const TestResult result = only(unit.tests());
    INFO(result.message);
    CHECK(result.passed);
}

TEST_CASE("type-generic struct specializations retain nominal values", "[wiring]") {
    Unit unit{R"(
module suite.generic_structs

struct Box<T> {
    value: T
}

fn same_box(value: atomic<Box<f64>>) -> atomic<Box<f64>> => value
fn box_value(value: f64) -> f64 => same_box(Box<f64>(value: value)).value
fn inferred_box_value(value: f64) -> f64 => same_box(Box(value: value)).value

test generic_value {
    assert Box<f64>(value: 1.5).value == 1.5
}

test generic_atomic {
    assert eval(box_value, value: [1.5, _]) == [1.5, _]
    assert eval(inferred_box_value, value: [2.5, _]) == [2.5, _]
}
)"};
    for (const TestResult &result : unit.tests()) {
        INFO(result.name << ": " << result.message);
        INFO(unit.diagnostics.render(unit.file));
        CHECK(result.passed);
    }
}

TEST_CASE("temporal struct construction wires a structural bundle", "[wiring]") {
    Unit             unit{R"(
module suite.temporal_structs

struct Quote {
    bid: f64
    ask: f64
    venue: str = "XNAS"
}

fn quote_bid(bid: f64, ask: f64) -> f64 => Quote(bid: bid, ask: ask).bid

test fieldwise {
    assert eval(quote_bid, bid: [1.0, 2.0], ask: [3.0, 4.0]) == [1.0, 2.0]
}
)"};
    const TestResult result = only(unit.tests());
    INFO(result.message << "\n" << unit.diagnostics.render(unit.file));
    CHECK(result.passed);
}

TEST_CASE("a structured delta is sparse and does not apply defaults", "[wiring]") {
    Unit unit{R"(
module suite.struct_deltas

struct Quote {
    bid: f64
    ask: f64
    venue: str = "XNAS"
}

test sparse {
    delta<Quote>(bid: 1.5)
}
)"};
    INFO(unit.diagnostics.render(unit.file));
    REQUIRE_FALSE(unit.diagnostics.has_errors());
    TestOptions options;
    options.describe_tail   = true;
    const TestResult result = only(run_tests(unit.file, unit.graph_ir, options, unit.diagnostics));
    INFO(result.message);
    CHECK(result.passed);
    CHECK(result.tail.find("1.5") != std::string::npos);
    CHECK(result.tail.find("XNAS") == std::string::npos);
}

TEST_CASE("a structured delta preserves nested sparse deltas", "[wiring]") {
    Unit unit{R"(
module suite.nested_struct_deltas

struct Quote {
    bid: f64
    ask: f64
}

struct Book {
    best: Quote
    depth: i64
}

test nested_sparse {
    delta<Book>(best: delta<Quote>(bid: 100.5))
}
)"};
    INFO(unit.diagnostics.render(unit.file));
    REQUIRE_FALSE(unit.diagnostics.has_errors());
    TestOptions options;
    options.describe_tail   = true;
    const TestResult result = only(run_tests(unit.file, unit.graph_ir, options, unit.diagnostics));
    INFO(result.message);
    CHECK(result.passed);
    CHECK(result.tail.find("100.5") != std::string::npos);
}

TEST_CASE("unavailable structural operations are explicit diagnostics", "[wiring]") {
    SECTION("an optional clear needs a distinct native delta operation") {
        Unit unit{R"(
module suite.struct_clear
struct Quote { note: str = null }
test clear { delta<Quote>(note: null) }
)"};
        CHECK(unit.diagnostics.has_errors());
        CHECK(unit.has(Category::Backend, "distinct public hgraph clear-delta operation"));
    }

    SECTION("const generic identity needs native metadata") {
        Unit             unit{R"(
module suite.const_generic_structs
struct Vector<T, const size: i64> { values: list<T, size> }
test value { Vector<f64, 2>(values: [1.0, 2.0]) }
)"};
        const TestResult result = only(unit.tests());
        CHECK_FALSE(result.passed);
        CHECK(unit.has(Category::Backend, "const generic struct arguments require "
                                          "typed constant Bundle metadata"));
    }
}

TEST_CASE("a failing assert reports the observed sequence", "[wiring]") {
    Unit                          unit{R"(
module t

fn midpoint(tob: tuple<f64, f64>) -> f64 => (tob[0] + tob[1]) / 2.0

test wrong_value {
    assert eval(midpoint, tob: [(1.0, 2.0), (2.0, 3.0)]) == [1.5, 2.0]
}

test wrong_length {
    assert eval(midpoint, tob: [(1.0, 2.0), _, (2.0, 3.0)]) == [1.5, 2.5]
}

test plain {
    assert 1 == 2
}
)"};
    const std::vector<TestResult> results = unit.tests();
    REQUIRE(results.size() == 3);
    CHECK_FALSE(results[0].passed);
    CHECK(results[0].message.find("assert failed: eval(midpoint") != std::string::npos);
    CHECK(results[0].message.find("cycle 1: expected 2.0, observed 2.5 in [1.5, 2.5]") != std::string::npos);
    CHECK_FALSE(results[1].passed);
    CHECK(results[1].message.find("expected 2 cycles, observed 3: [1.5, _, 2.5]") != std::string::npos);
    CHECK_FALSE(results[2].passed);
    CHECK(results[2].message == "assert failed: 1 == 2");
}

TEST_CASE("selected tests run and describe their tail", "[wiring]") {
    Unit unit{R"(
module t

fn midpoint(tob: tuple<f64, f64>) -> f64 => (tob[0] + tob[1]) / 2.0

fn same(tob: tuple<f64, f64>) -> tuple<f64, f64> => tob

fn compound(a: f64, b: f64) -> f64 {
    var y = a
    y += b * 2.0
    y
}

test one { assert 1 == 1 }
test two { eval(midpoint, tob: [(1.0, 2.0), (2.0, 3.0)]) }
test three { eval(same, tob: [(1.0, 2.0), _]) }
test four { assert eval(compound, a: [1.0], b: [3.0]) == [7.0] }
)"};
    INFO(unit.diagnostics.render(unit.file));
    REQUIRE_FALSE(unit.diagnostics.has_errors());
    TestOptions options;
    options.names           = {"two"};
    options.describe_tail   = true;
    const TestResult result = only(run_tests(unit.file, unit.graph_ir, options, unit.diagnostics));
    CHECK(result.name == "two");
    CHECK(result.passed);
    CHECK(result.tail == "[1.5, 2.5]");

    options.names           = {"three"};
    const TestResult tuples = only(run_tests(unit.file, unit.graph_ir, options, unit.diagnostics));
    CHECK(tuples.passed);
    CHECK(tuples.tail.find("1.0") != std::string::npos);
    CHECK(tuples.tail.find("2.0") != std::string::npos);
    CHECK(tuples.tail.ends_with(", _]"));
}

TEST_CASE("first-pass limits are diagnostics, not crashes", "[wiring]") {
    Unit                          unit{R"(
module t

fn midpoint(tob: tuple<f64, f64>) -> f64 => (tob[0] + tob[1]) / 2.0

fn counter(x: f64) -> f64 {
    state total: f64 = 0.0
    inject out
    when modified(x) { total += x }
    out = total
}

test timed {
    assert eval(midpoint, tob: [@2026-01-01T00:00:00Z: (1.0, 2.0)]) == [1.5]
}

test runtime {
    assert eval(counter, x: [1.0]) == [1.0]
}

)"};
    const std::vector<TestResult> results = unit.tests();
    REQUIRE(results.size() == 2);
    for (const TestResult &result : results) { CHECK_FALSE(result.passed); }
    CHECK(unit.has(Category::Test, "timed sequences are not supported by the first pass"));
    CHECK(unit.has(Category::Operator, "t.counter"));

    Unit wrong_type{R"(
module t
fn midpoint(tob: tuple<f64, f64>) -> f64 => (tob[0] + tob[1]) / 2.0
test wrong_type { assert eval(midpoint, tob: ["a"]) == [1.5] }
)"};
    CHECK(wrong_type.diagnostics.has_errors());
    CHECK(wrong_type.has(Category::Type, "sequence elements have incompatible types"));
}

TEST_CASE("run_program prints one line per tick", "[wiring]") {
    Unit unit{R"(
module t

use hgraph.std::{schedule}

export fn heartbeat(const every: duration = 1s) -> datetime => last_modified(schedule(every))

export fn other(x: f64) -> f64 => x
)"};
    INFO(unit.diagnostics.render(unit.file));
    REQUIRE_FALSE(unit.diagnostics.has_errors());

    SECTION("the entry with only const parameters runs") {
        RunOptions options;
        options.start     = hgraph::DateTime{std::chrono::microseconds{1'700'000'000'000'000}};
        options.end_after = hgraph::TimeDelta{std::chrono::seconds{3}};
        options.settings.push_back(Setting{"every", hgraph::Value{hgraph::TimeDelta{std::chrono::milliseconds{1500}}}});
        std::ostringstream out;
        const bool         ok = run_program(unit.file, unit.graph_ir, options, unit.diagnostics, out);
        INFO(unit.diagnostics.render(unit.file));
        CHECK(ok);
        CHECK(out.str() == "2023-11-14T22:13:21.5Z 2023-11-14 22:13:21.500000\n");
    }

    SECTION("an entry with temporal parameters cannot run") {
        RunOptions options;
        options.entry = "other";
        std::ostringstream out;
        CHECK_FALSE(run_program(unit.file, unit.graph_ir, options, unit.diagnostics, out));
        CHECK(unit.has(Category::Backend, "entry parameter 'x' is not const"));
    }

    SECTION("settings are checked against the parameter type") {
        RunOptions options;
        options.settings.push_back(Setting{"every", hgraph::Value{hgraph::Int{3}}});
        std::ostringstream out;
        CHECK_FALSE(run_program(unit.file, unit.graph_ir, options, unit.diagnostics, out));
        CHECK(unit.has(Category::Type, "'every' expects timedelta, got int"));
    }
}

TEST_CASE("format_time spells the canonical datetime without the sigil", "[wiring]") {
    CHECK(format_time(hgraph::DateTime{std::chrono::microseconds{1'700'000'000'000'000}}) == "2023-11-14T22:13:20Z");
    CHECK(format_time(hgraph::DateTime{std::chrono::microseconds{1'700'000'000'500'000}}) == "2023-11-14T22:13:20.5Z");
}

TEST_CASE("ordinary generic helpers retain typed lists and independent copies", "[wiring][ordinary][generics]") {
    Unit unit{R"(
module t
struct TimedValue<T> {
    time: datetime
    value: delta<T>
}
const fn publications<T>(value: delta<T>) -> list<TimedValue<T>> {
    var entries: list<TimedValue<T>> = []
    push(entries, TimedValue<T>(time: @1970-01-01T00:00:00.000001Z, value: value))
    return entries
}
const fn forwarded<T>(value: delta<T>) -> list<TimedValue<T>> => publications(value)
test ordinary {
    let entries = forwarded(7)
    assert len(entries) == 1
    assert entries[0].value == 7
    assert entries[0].time == @1970-01-01T00:00:00.000001Z
    var values: list<i64> = []
    push(values, 3)
    let snapshot = values
    push(values, values[0])
    assert len(values) == 2
    assert len(snapshot) == 1
    var batches: list<list<i64>> = []
    push(batches, values)
    let before = batches
    push(batches[0], 9)
    assert len(batches[0]) == 3
    assert batches[0][0] == 3
    assert len(before[0]) == 2
    assert before[0][0] == 3
    var retained = entries
    retained[0].value = 8
    assert retained[0].value == 8
    assert entries[0].value == 7
}
)"};
    const auto result = only(unit.tests());
    INFO(unit.diagnostics.render(unit.file));
    INFO(result.message);
    CHECK(result.passed);
}

TEST_CASE("complete recursive constructors retain atomic adaptation at call boundaries", "[wiring][recursive]") {
    Unit unit{R"(
module checks.recursive_construction
struct Node { value: i64
    next: atomic<Node> = null }
struct Tree<T> { value: T
    left: atomic<Tree<T>> = null }
abstract struct Linked { next: atomic<Linked> = null }
struct Item: Linked { value: i64 }
struct Holder { first: Node
    count: i64 }
fn node_value(value: atomic<Node>) -> i64 => value.next.value
fn tree_value(value: atomic<Tree<i64>>) -> i64 => value.left.value
fn item_value(value: atomic<Item>) -> i64 => value.value
fn holder_value(value: atomic<Holder>) -> i64 => value.first.next.value
fn nested(value: i64) -> i64 => node_value(Node(value: value, next: Node(value: 7)))
fn generic(value: i64) -> i64 => tree_value(Tree<i64>(value: 1, left: Tree<i64>(value: value)))
fn inherited(value: i64) -> i64 => item_value(Item(value: value))
fn held_inline(value: i64) -> i64 => holder_value(Holder(first: Node(value: 1, next: Node(value: value)), count: 3))
test recursive_values {
    assert eval(nested, value: [1, 2]) == [7, 7]
    assert eval(generic, value: [3, 4]) == [3, 4]
    assert eval(inherited, value: [5, 6]) == [5, 6]
    assert eval(held_inline, value: [7, 8]) == [7, 8]
    # Supplied fields gate complete construction; omitted recursive defaults do not.
    assert eval(nested, value: [_, 2]) == [_, 7]
    assert eval(generic, value: [_, 4]) == [_, 4]
    assert eval(inherited, value: [_, 6]) == [_, 6]
    assert eval(held_inline, value: [_, 8]) == [_, 8]
}
)"};
    const auto result = only(unit.tests());
    INFO(unit.diagnostics.render(unit.file));
    INFO(result.message);
    CHECK(result.passed);
}

TEST_CASE("complete constructor atomic adaptation rejects a different nominal origin", "[wiring][recursive]") {
    Unit unit{R"(
module checks.recursive_origin
struct Node { value: i64
    next: atomic<Node> = null }
struct Other { value: i64
    next: atomic<Other> = null }
fn node_value(value: atomic<Node>) -> i64 => value.value
fn wrong(value: i64) -> i64 => node_value(Other(value: value))
)"};
    INFO(unit.diagnostics.render(unit.file));
    CHECK(unit.diagnostics.has_errors());
    CHECK(std::any_of(unit.diagnostics.diagnostics().begin(), unit.diagnostics.diagnostics().end(),
        [](const Diagnostic &diagnostic) { return diagnostic.category == Category::Type; }));
}

TEST_CASE("temporal publication literals preserve identity and validate before eval", "[wiring][temporal]") {
    Unit unit{R"(
module tests.temporal_publications
fn civil(value: civil_datetime) -> civil_datetime => value
fn zone(value: timezone) -> timezone => value
fn zoned(value: zoned_datetime) -> zoned_datetime => value
fn clock(value: zoned_time) -> zoned_time => value

test identity {
    let local = @2024-02-29T12:30:00.123456
    assert eval(civil, [local, local, _, @2024-02-29T12:30:00.123457]) == [local, local, _, @2024-02-29T12:30:00.123457]
    assert eval(zone, [@[US/Eastern], @[US/Eastern], _, @[America/New_York]]) == [@[US/Eastern], @[US/Eastern], _, @[America/New_York]]
    assert eval(zoned, [@2026-01-15T12:30Z[UTC], _, @2026-01-15T13:30+01[Europe/Paris]]) == [@2026-01-15T12:30Z[UTC], _, @2026-01-15T13:30+01[Europe/Paris]]
    assert @2026-01-15T12:30Z[UTC] != @2026-01-15T13:30+01[Europe/Paris]
    assert @[UTC] != @[Etc/UTC]
    let opening = @09:30:00.123456[US/Eastern]
    let alias = @09:30:00.123456[America/New_York]
    assert opening != alias
    assert eval(clock, [opening, opening, _, alias]) == [opening, opening, _, alias]
}
)"};
    const auto result = only(unit.tests());
    INFO(result.message);
    CHECK(result.passed);
    for (const std::string literal : {"@[utc]", "@[america/new_york]", "@[Etc/Unknown]", "@[Missing/Zone]",
                                      "@2026-01-15T12:30Z[utc]", "@2026-01-15T12:30Z[Missing/Zone]",
                                      "@2026-01-15T12:30+01[UTC]", "@09:30[utc]", "@09:30[america/new_york]",
                                      "@09:30[Etc/Unknown]", "@09:30[Missing/Zone]"}) {
        Unit invalid{"module tests.invalid_zone\ntest invalid { " + literal + "\nassert true }\n"};
        const auto rejected = only(invalid.tests());
        INFO(literal);
        INFO(rejected.message);
        CHECK_FALSE(rejected.passed);
    }
}

TEST_CASE("eval temporal recipes preserve written failure order and selected defaults", "[wiring][temporal]") {
    const std::string declarations = R"(
module tests.temporal_order
const fn ready(value: timezone) -> bool { return true }
fn target(tick: bool, const first: timezone = @[Missing/Default], const second: timezone = @[UTC]) -> bool => tick
)";
    for (const auto &call : {
        std::string{"eval(target, tick: [ready(@[Missing/First])], first: @[Missing/Second])"},
        std::string{"eval(target, second: @[Missing/First], tick: [true], first: @[Missing/Second])"}}) {
        Unit unit{declarations + "test order { " + call + " }\n"};
        const auto result = only(unit.tests());
        INFO(result.message);
        CHECK_FALSE(result.passed);
        CHECK(result.message.find("Missing/First") != std::string::npos);
    }
    Unit selected{declarations + "test defaults { eval(target, tick: [true]) }\n"};
    const auto failed = only(selected.tests());
    CHECK_FALSE(failed.passed);
    CHECK(failed.message.find("Missing/Default") != std::string::npos);
    Unit overridden{declarations + "test defaults { assert eval(target, tick: [true], first: @[Etc/UTC]) == [true] }\n"};
    const auto succeeded = only(overridden.tests());
    INFO(succeeded.message);
    CHECK(succeeded.passed);
    Unit skipped{R"(
module tests.temporal_skipped
const fn selected(value: bool) -> timezone {
    if value { return @[UTC] }
    return @[Missing/Skipped]
}
test skipped { assert selected(true) == @[UTC] }
)"};
    const auto branch = only(skipped.tests());
    INFO(branch.message);
    CHECK(branch.passed);
}

TEST_CASE("selected temporal default recipes retain short circuit reachability", "[wiring][temporal]") {
    for (const std::string expression : {"false && (@[Missing/Skipped] == @[UTC])", "true || (@[Missing/Skipped] == @[UTC])"}) {
        Unit unit{"module tests.default_short_circuit\nconst fn target(value: bool = " + expression +
                  ") -> bool { return value }\ntest selected { target()\nassert true }\n"};
        const auto result = only(unit.tests());
        INFO(result.message);
        CHECK(result.passed);
    }
}

TEST_CASE("ordinary calls materialize supplied temporal arguments before defaults", "[wiring][temporal]") {
    Unit unit{R"(
module tests.ordinary_default_order
fn target(first: timezone, const second: timezone = @[Missing/Second]) -> timezone => first
test ordered { target(@[Missing/First]) }
)"};
    const auto result = only(unit.tests());
    INFO(result.message);
    CHECK_FALSE(result.passed);
    CHECK(result.message.find("Missing/First") != std::string::npos);
}

TEST_CASE("direct enum publications preserve nominal members and nested deltas", "[wiring][enum]") {
    Unit unit{R"(
module tests.enum_publications
enum Mode { low = -9223372036854775808, first = -7, next, high = 9223372036854775807 }
struct Snapshot { mode: Mode = Mode::first }
fn scalar(value: Mode) -> Mode => value
fn fixed(value: list<Mode, 2>) -> list<Mode, 2> => value
fn snapshot(value: atomic<Snapshot>) -> atomic<Snapshot> => value
test identity {
    assert eval(scalar, [_, Mode::low, Mode::low, _, Mode::first, Mode::next, Mode::high]) == [_, Mode::low, Mode::low, _, Mode::first, Mode::next, Mode::high]
    assert eval(fixed, [delta<list<Mode, 2>>(items: [1: Mode::first]), _, delta<list<Mode, 2>>(items: [1: Mode::high])]) == [delta<list<Mode, 2>>(items: [1: Mode::first]), _, delta<list<Mode, 2>>(items: [1: Mode::high])]
    assert eval(snapshot, [Snapshot(), _, Snapshot(mode: Mode::high)]) == [Snapshot(mode: Mode::first), _, Snapshot(mode: Mode::high)]
}
)"};
    const auto result = only(unit.tests());
    INFO(unit.diagnostics.render(unit.file));
    INFO(result.message);
    CHECK(result.passed);
}

TEST_CASE("native publication admission rejects NaN collection keys", "[wiring][scalar-keys]") {
    using namespace hgraph;
    using namespace hgl::ordinary;
    auto &registry = TypeRegistry::instance();
    auto &factory = ValuePlanFactory::instance();
    const auto *floating = scalar_descriptor<Float>::value_meta();
    const auto *integer = scalar_descriptor<Int>::value_meta();
    const auto binding = factory.type_for(floating);
    const Value nan{std::numeric_limits<Float>::quiet_NaN()};
    SetBuilder nan_members{binding};
    nan_members.insert(nan.view());
    const auto present = nan_members.build();
    const auto empty = SetBuilder{binding}.build();
    const auto *set_shape = registry.tss(floating);
    const PreparedValuePlan set_plan{set_shape->delta_value_schema};
    for (bool removing : {false, true}) {
        const std::array fields{std::pair<std::size_t, ValueView>{0, removing ? empty.view() : present.view()},
            std::pair<std::size_t, ValueView>{1, removing ? present.view() : empty.view()}};
        const auto payload = set_plan.bundle(fields);
        hgl::wiring::DeltaTrace trace;
        CHECK_THROWS_WITH(trace.accept(set_shape, payload.view()), "NaN collection keys are outside the publication profile");
    }
    const auto *map_shape = registry.tsd(floating, registry.ts(integer));
    const PreparedValuePlan map_plan{map_shape->delta_value_schema};
    MapBuilder entries{binding, factory.type_for(integer)};
    entries.set_item(nan.view(), Value{Int{1}}.view());
    const auto modified = entries.build();
    const auto no_updates = MapBuilder{binding, factory.type_for(integer)}.build();
    for (bool removing : {false, true}) {
        const std::array fields{std::pair<std::size_t, ValueView>{0, removing ? present.view() : empty.view()},
            std::pair<std::size_t, ValueView>{1, removing ? no_updates.view() : modified.view()}};
        const auto payload = map_plan.bundle(fields);
        hgl::wiring::DeltaTrace trace;
        CHECK_THROWS_WITH(trace.accept(map_shape, payload.view()), "NaN collection keys are outside the publication profile");
    }
}

TEST_CASE("direct scalar keys compare cold values and preserve failure order", "[wiring][scalar-keys]") {
    Unit accepted{R"(module checks.scalar_keys
fn forward(value: map<zoned_time, i64>) -> map<zoned_time, i64> => value
test keys {
    let a = @09:30[US/Eastern]
    let b = @09:30[America/New_York]
    assert eval(forward, [delta<map<zoned_time, i64>>(upsert: [a: 1, b: 2]), _, delta<map<zoned_time, i64>>(remove: [a])]) == [delta<map<zoned_time, i64>>(upsert: [b: 2, a: 1]), _, delta<map<zoned_time, i64>>(remove: [a])]
})"};
    const auto passed = only(accepted.tests());
    INFO(passed.message);
    INFO(accepted.diagnostics.render(accepted.file));
    CHECK(passed.passed);
    for (const std::string recipe : {
        "delta<set<timezone>>(added: [@[UTC], @[UTC]])",
        "delta<set<zoned_time>>(added: [@09:30[UTC]], removed: [@09:30[UTC]])",
        "delta<map<timezone, i64>>(upsert: [@[UTC]: 1], remove: [@[UTC]])"
    }) {
        Unit duplicate{"module checks.duplicate_keys\nconst fn consume<T>(value: T) -> bool => true\ntest duplicate { assert consume(" + recipe + ") }\n"};
        const auto result = only(duplicate.tests());
        INFO(result.message);
        CHECK_FALSE(result.passed);
        CHECK(result.message.find("duplicate or overlapping") != std::string::npos);
    }
    Unit invalid{R"(module checks.key_order
const fn consume<T>(value: T) -> bool => true
test order { assert consume(delta<map<timezone, timezone>>(upsert: [@[Missing/Key]: @[Missing/Payload]])) }
)"};
    const auto failed = only(invalid.tests());
    CHECK_FALSE(failed.passed);
    CHECK(failed.message.find("Missing/Key") != std::string::npos);
    CHECK(failed.message.find("Missing/Payload") == std::string::npos);
}

TEST_CASE("ordinary map construction owns values and stops at duplicate keys", "[wiring][atomic-collections]") {
    Unit accepted{R"(module checks.atomic_collections
struct Snapshot { values: map<str, list<i64>> = map<str, list<i64>>(items: ["empty": []]) }
fn forward(value: atomic<Snapshot>) -> atomic<Snapshot> => value
test ownership {
    var row: list<i64> = [1]
    let saved = map<str, list<i64>>(items: ["row": row])
    push(row, 2)
    var source = Snapshot(values: saved)
    let captured = source
    source.values = map<str, list<i64>>(items: [])
    assert eval(forward, [captured, Snapshot()]) == [Snapshot(values: map<str, list<i64>>(items: ["row": [1]])), Snapshot(values: map<str, list<i64>>(items: ["empty": []]))]
})"};
    const auto passed = only(accepted.tests());
    INFO(passed.message);
    CHECK(passed.passed);
    Unit duplicate{R"(module checks.atomic_duplicate
const fn consume<T>(value: T) -> bool => true
test fail { assert consume(map<timezone, timezone>(items: [@[UTC]: @[UTC], @[UTC]: @[Missing/Payload]])) }
)"};
    const auto failed = only(duplicate.tests());
    CHECK_FALSE(failed.passed);
    INFO(failed.message);
    CHECK(failed.message.find("duplicate") != std::string::npos);
    CHECK(failed.message.find("Missing/Payload") == std::string::npos);
}

TEST_CASE("growing list traces reject gaps and non-tail removals before evaluation", "[wiring][growing-list]") {
    for (const std::string trace : {
        "delta<list<i64>>(items: [1: 1])",
        "delta<list<i64>>(items: [0: 1]), delta<list<i64>>(items: [2: 2])",
        "delta<list<i64>>(items: [0: 1, 1: 2, 2: 3]), delta<list<i64>>(remove: [0, 2])",
        "delta<list<i64>>(items: [0: 1]), delta<list<i64>>(remove: [1])",
        "delta<list<i64>>(items: [0: 1, 1: 2]), delta<list<i64>>(items: [2: 3], remove: [1])",
        "delta<list<i64>>()"
    }) {
        Unit unit{"module checks.growing_list\nfn forward(value: list<i64>) -> list<i64> => value\n"
            "test bad { eval(forward, [" + trace + "]) }\n"};
        const auto result = only(unit.tests());
        INFO(trace);
        INFO(result.message);
        CHECK_FALSE(result.passed);
        CHECK(result.message.find("input delta outside publication profile") != std::string::npos);
    }
    Unit nested{R"(module checks.growing_state
fn forward(value: list<set<str>>) -> list<set<str>> => value
test reset {
    assert eval(forward, [delta<list<set<str>>>(items: [0: delta<set<str>>(added: ["x"])]), delta<list<set<str>>>(remove: [0]), delta<list<set<str>>>(items: [0: delta<set<str>>(added: ["x"])])]) == [delta<list<set<str>>>(items: [0: delta<set<str>>(added: ["x"])]), delta<list<set<str>>>(remove: [0]), delta<list<set<str>>>(items: [0: delta<set<str>>(added: ["x"])])]
})"};
    const auto result = only(nested.tests());
    INFO(result.message);
    CHECK(result.passed);
}

TEST_CASE("ordinary tuple construction independently retains growing List children", "[wiring][ordinary-tuple][list-retention]") {
    Unit unit{R"hgl(module checks.tuple_list_retention
const fn retained_lists() -> bool {
    var values: list<i64> = []
    let empty_copies = (values, values)
    push(values, 7)
    let copies = (values, values)
    push(values, 9)
    var nested = ((values, values), values)
    push(nested[0][0], 11)
    return len(empty_copies[0]) == 0 && len(empty_copies[1]) == 0 &&
        len(copies[0]) == 1 && copies[0][0] == 7 &&
        len(copies[1]) == 1 && copies[1][0] == 7 &&
        len(values) == 2 && values[1] == 9 &&
        len(nested[0][0]) == 3 && nested[0][0][2] == 11 &&
        len(nested[0][1]) == 2 && len(nested[1]) == 2
}
test independent_ownership { assert retained_lists() }
)hgl"};
    const auto passed = only(unit.tests());
    INFO(passed.message);
    INFO(unit.diagnostics.render(unit.file));
    CHECK(passed.passed);
}
