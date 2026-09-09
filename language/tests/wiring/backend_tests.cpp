#include "hgraph_ir/lower.h"
#include "ir/lower.h"
#include "ir/type_check.h"
#include "semantics/resolve.h"
#include "syntax/parser.h"
#include "wiring/backend.h"
#include "wiring/operator_types.h"

#include <hgraph/lib/std/operators/collection.h>
#include <hgraph/lib/std/operators/conversion.h>
#include <hgraph/lib/std/operators/logical.h>
#include <hgraph/lib/std/value_util.h>
#include <hgraph/types/operator_dispatch.h>

#include <catch2/catch_test_macros.hpp>

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
    assert @2026-09-03T10:30+01[Europe/London] == @2026-09-03T09:30Z[UTC]
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
        const TestResult result = only(unit.tests());
        CHECK_FALSE(result.passed);
        CHECK(unit.has(Category::Type, "assignment to 'y' expects int, got float"));
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

use hgraph.std::{hgl_fixed_pair}

fn fixed(a: f64, b: f64) -> list<f64, 2> => hgl_fixed_pair(a, b)
fn values(a: f64, b: f64) -> list<f64> => fixed(a, b)

test widening {
    eval(values, a: [1.0], b: [2.0])
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

// The book is {A: 1.0, B: 2.0}; each key's child adds the value to itself.
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

fn midpoint(tob: atomic<tuple<f64, f64>>) -> f64 => (tob[0] + tob[1]) / 2.0

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
        CHECK(result.passed);
    }
}

TEST_CASE("eval rejects a const-only harness with no execution bound", "[wiring][harness]") {
    Unit             unit{R"(
module t

use hgraph.std::{schedule}

fn heartbeat(const every: duration) -> datetime => last_modified(schedule(every))

test const_only {
    eval(heartbeat, every: 1us)
}
)"};
    const TestResult result = only(unit.tests());
    CHECK_FALSE(result.passed);
    CHECK(unit.has(Category::Backend, "at least one time-series harness input to bound execution"));
}

TEST_CASE("eval bounds scheduled work by the longest harness input", "[wiring][harness]") {
    Unit             unit{R"(
module t

use hgraph.std::{schedule}

fn heartbeat(trigger: f64, const every: duration) -> datetime => last_modified(schedule(every))

test bounded {
    eval(heartbeat, trigger: [1.0, 2.0], every: 1us)
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

test generic_value {
    assert Box<f64>(value: 1.5).value == 1.5
}

test generic_atomic {
    assert eval(same_box, value: [Box<f64>(value: 1.5), _]) == [Box<f64>(value: 1.5), _]
    assert eval(same_box, value: [Box(value: 2.5), _]) == [Box<f64>(value: 2.5), _]
}
)"};
    for (const TestResult &result : unit.tests()) {
        INFO(result.name << ": " << result.message);
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
    CHECK(result.tail.starts_with("delta "));
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
    CHECK(result.tail.starts_with("delta "));
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

fn midpoint(tob: atomic<tuple<f64, f64>>) -> f64 => (tob[0] + tob[1]) / 2.0

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

fn midpoint(tob: atomic<tuple<f64, f64>>) -> f64 => (tob[0] + tob[1]) / 2.0

fn same(tob: atomic<tuple<f64, f64>>) -> atomic<tuple<f64, f64>> => tob

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
    CHECK(tuples.tail == "[(1.0, 2.0), _]");
}

TEST_CASE("first-pass limits are diagnostics, not crashes", "[wiring]") {
    Unit                          unit{R"(
module t

fn midpoint(tob: atomic<tuple<f64, f64>>) -> f64 => (tob[0] + tob[1]) / 2.0

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
fn midpoint(tob: atomic<tuple<f64, f64>>) -> f64 => (tob[0] + tob[1]) / 2.0
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
