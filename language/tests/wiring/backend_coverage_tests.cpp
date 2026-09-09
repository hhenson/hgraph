// Direct-wiring counterparts of behaviour that otherwise has only
// generated-C++ evidence (developer guide, "Direct-wiring backend and the
// harness"): reference routing, generic operator resolution over tick and
// duration windows, and dynamic collection traversal with recorded ticks.
// The harness mirrors `backend_tests.cpp`: a unit goes through the frontend
// against the live registry and its `test` blocks run on the backend.
//
// What the direct backend cannot express is said in place. `eval` drives only
// `TS`/`SIGNAL` parameters, so references, windows and collections are built
// inside the composition from registry operators. Runtime functions (the
// reference-routing example's `forward`/`route3`) and source-defined `impl fn`
// candidates need the scripted native image the file driver loads before
// wiring; this harness has no loader, so those remain generated-only.
#include "hgraph_ir/lower.h"
#include "ir/lower.h"
#include "ir/type_check.h"
#include "semantics/resolve.h"
#include "syntax/parser.h"
#include "wiring/backend.h"
#include "wiring/operator_types.h"

#include <hgraph/lib/std/operators/collection.h>
#include <hgraph/lib/std/operators/conversion.h>
#include <hgraph/lib/std/value_util.h>
#include <hgraph/types/operator_dispatch.h>

#include <catch2/catch_test_macros.hpp>

#include <algorithm>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

using namespace hgl::syntax;
using namespace hgl::semantics;
using namespace hgl::wiring;

namespace
{
    // Three sources packed into a fixed list: the `values: list<ref<T>, 3>`
    // input of the reference-routing example, which composition code cannot
    // spell as a literal (a time-series list literal is a first-pass limit).
    struct coverage_triple_operator
        : hgraph::Operator<"hgl_coverage_triple", hgraph::In<"a", hgraph::TS<hgraph::Float>>,
                           hgraph::In<"b", hgraph::TS<hgraph::Float>>, hgraph::In<"c", hgraph::TS<hgraph::Float>>,
                           hgraph::Out<hgraph::TSL<hgraph::TS<hgraph::Float>, 3>>>
    {};

    struct coverage_triple_graph
    {
        static constexpr auto name = "hgl_coverage_triple_graph";

        static auto compose(hgraph::Wiring &w, hgraph::Port<hgraph::TS<hgraph::Float>> a, hgraph::Port<hgraph::TS<hgraph::Float>> b,
                            hgraph::Port<hgraph::TS<hgraph::Float>> c) {
            return hgraph::stdlib::to_tsl(w, a, b, c);
        }
    };

    struct coverage_book_operator : hgraph::Operator<"hgl_coverage_book", hgraph::In<"trigger", hgraph::TS<hgraph::Float>>,
                                                     hgraph::Out<hgraph::TSD<hgraph::Str, hgraph::TS<hgraph::Float>>>>
    {};

    struct coverage_book_graph
    {
        static constexpr auto name = "hgl_coverage_book_graph";

        static auto compose(hgraph::Wiring &w, hgraph::Port<hgraph::TS<hgraph::Float>> trigger) {
            static_cast<void>(trigger);
            return hgraph::wire<hgraph::stdlib::const_, hgraph::TSD<hgraph::Str, hgraph::TS<hgraph::Float>>>(
                w, hgraph::stdlib::make_map<hgraph::Str, hgraph::Float>(
                       {{hgraph::Str{"A"}, hgraph::Float{1.0}}, {hgraph::Str{"B"}, hgraph::Float{2.0}}}));
        }
    };

    struct coverage_samples_operator : hgraph::Operator<"hgl_coverage_samples", hgraph::In<"trigger", hgraph::TS<hgraph::Float>>,
                                                        hgraph::Out<hgraph::TSL<hgraph::TS<hgraph::Float>>>>
    {};

    struct coverage_samples_graph
    {
        static constexpr auto name = "hgl_coverage_samples_graph";

        static auto compose(hgraph::Wiring &w, hgraph::Port<hgraph::TS<hgraph::Float>> trigger) {
            static_cast<void>(trigger);
            return hgraph::wire<hgraph::stdlib::const_, hgraph::TSL<hgraph::TS<hgraph::Float>>>(
                w, hgraph::stdlib::make_list<hgraph::Float>({hgraph::Float{1.0}, hgraph::Float{2.0}}));
        }
    };

    // A sink that records every value it is evaluated with, so a child graph
    // wired per key or index by a dynamic `for` body leaves observable ticks.
    struct coverage_observe_operator : hgraph::Operator<"hgl_coverage_observe", hgraph::In<"ts", hgraph::TS<hgraph::Float>>>
    {};

    struct coverage_observe_node
    {
        static inline std::vector<double> observed{};

        static void eval(hgraph::In<"ts", hgraph::TS<hgraph::Float>> ts) { observed.push_back(ts.value()); }
    };

    void register_coverage_operators() {
        static const bool registered = [] {
            ensure_session();
            hgraph::register_graph_overload<coverage_triple_operator, coverage_triple_graph>();
            hgraph::register_graph_overload<coverage_book_operator, coverage_book_graph>();
            hgraph::register_graph_overload<coverage_samples_operator, coverage_samples_graph>();
            hgraph::register_overload<coverage_observe_operator, coverage_observe_node>();
            return true;
        }();
        static_cast<void>(registered);
    }

    // A unit through the frontend against the live registry, ready for the
    // backend; the same shape as `backend_tests.cpp`.
    struct Unit
    {
        SourceFile             file;
        DiagnosticSink         diagnostics;
        ast::Module            module;
        ResolvedModule         resolved;
        hgl::ir::hir::Module   hir;
        hgl::hgraph_ir::Module graph_ir;

        explicit Unit(std::string text)
            : file{"coverage.hgl", std::move(text)}, module{parse(file, diagnostics)},
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

    void check_all_pass(Unit &unit) {
        for (const TestResult &result : unit.tests()) {
            INFO(result.name << ": " << result.message);
            CHECK(result.passed);
        }
    }
}  // namespace

TEST_CASE("reference-typed compositions forward a selected source to operator consumers", "[wiring][coverage][ref]") {
    Unit unit{R"(
module coverage.reference_routing

use hgraph.std::{abs_, add_, if_then_else}

// The composition form of the example's `forward`: a reference parameter
// passes through untouched. References are opaque to composition code, so
// the selected value is observed through operators that accept a reference
// input, not through arithmetic on the reference.
fn forward(value: ref<f64>) -> ref<f64> => value

fn select(condition: bool, a: f64, b: f64) -> f64 =>
    add_(forward(if_then_else(condition, a, b)), 0.0)

fn magnitude(condition: bool, a: f64, b: f64) -> f64 =>
    abs_(forward(if_then_else(condition, a, b)))

test select_ticks {
    assert eval(select, condition: [true, false, false], a: [1.0, 2.0, 3.0], b: [10.0, 20.0, 30.0]) == [1.0, 20.0, 30.0]
}

test magnitude_ticks {
    assert eval(magnitude, condition: [true, false, false], a: [-1.0, 2.0, 3.0], b: [10.0, -20.0, 30.0]) == [1.0, 20.0, 30.0]
}
)"};
    check_all_pass(unit);
}

TEST_CASE("fixed-list index routing follows the selected source", "[wiring][coverage][ref]") {
    register_coverage_operators();
    Unit unit{R"(
module coverage.reference_routing

use hgraph.std::{add_, hgl_coverage_triple}

// The shape of the example's `route3`: one element of a fixed list selected
// by a temporal index. Indexing a temporal list yields a reference to the
// selected element, consumed here by `add_`.
fn route(index: i64, a: f64, b: f64, c: f64) -> f64 {
    let values: list<f64, 3> = hgl_coverage_triple(a, b, c)
    add_(values[index], 0.0)
}

// The generated fixture's case: a constant index follows the first source.
test route_first_ticks {
    assert eval(route, index: [0], a: [1.0, 2.0], b: [10.0, 20.0], c: [100.0, 200.0]) == [1.0, 2.0]
}

test route_switching_ticks {
    assert eval(route, index: [0, 1, 2], a: [1.0, 2.0, 3.0], b: [10.0, 20.0, 30.0], c: [100.0, 200.0, 300.0]) == [1.0, 20.0, 300.0]
}
)"};
    check_all_pass(unit);
}

TEST_CASE("a reference result runs under eval and its adaptation to a plain result is a backend boundary",
          "[wiring][coverage][ref]") {
    SECTION("a ref<f64> composition wires and runs") {
        Unit             unit{R"(
module coverage.reference_result

use hgraph.std::{if_then_else}

fn pick(condition: bool, a: f64, b: f64) -> ref<f64> => if_then_else(condition, a, b)

// The harness records the reference itself, so there is no float sequence to
// assert against; the test passes when the graph wires and runs.
test pick_runs {
    eval(pick, condition: [true, false], a: [1.0, 2.0], b: [10.0, 20.0])
}
)"};
        const TestResult result = only(unit.tests());
        INFO(unit.diagnostics.render(unit.file));
        INFO(result.message);
        CHECK(result.passed);
    }

    SECTION("a reference is not adapted to a declared plain result") {
        // The frontend accepts a reference where its target type is declared;
        // the direct backend's result adaptation then resolves hgraph's
        // `convert` operator, which is ambiguous for a reference source, so
        // the test fails with a diagnostic instead of wiring-time
        // dereference. The generated fixture routes through a declared
        // `ref<T>` result instead. When the direct backend adapts a reference
        // at the result boundary this section becomes a tick assertion.
        Unit             unit{R"(
module coverage.reference_result

use hgraph.std::{if_then_else}

fn pick(condition: bool, a: f64, b: f64) -> f64 => if_then_else(condition, a, b)

test pick_ticks {
    assert eval(pick, condition: [true, false], a: [1.0, 2.0], b: [10.0, 20.0]) == [1.0, 20.0]
}
)"};
        const TestResult result = only(unit.tests());
        CHECK_FALSE(result.passed);
        CHECK(unit.diagnostics.has_errors());
    }
}

TEST_CASE("generic operator resolution over tick and duration windows", "[wiring][coverage][generic]") {
    Unit unit{R"(
module coverage.operators_and_generics

use hgraph.std::{mean, to_window}

// The example's exported windows: tick windows whose minimum defaults to the
// maximum, and a duration window. `mean` resolves to the window candidate
// for each concrete `rolling` shape.
fn summarize_full_window(window: rolling<f64, 20>) -> f64 => mean(window)

fn summarize_short_window(window: rolling<f64, 3>) -> f64 => mean(window)

fn summarize_recent(window: rolling<f64, 5m>) -> f64 => mean(window)

// `eval` drives only ts parameters, so each window is built inside the
// composition from `to_window`; the declared `rolling` type of the receiving
// parameter or local gives the operator its expected result shape.
fn full_window_mean(x: f64) -> f64 => summarize_full_window(to_window(x, 20, 20))

fn short_window_mean(x: f64) -> f64 => summarize_short_window(to_window(x, 3, 3))

fn partial_window_mean(x: f64) -> f64 {
    let window: rolling<f64, 3, 1> = to_window(x, 3, 1)
    mean(window)
}

fn recent_mean(x: f64) -> f64 => summarize_recent(to_window(x, 5m, 5m))

fn open_recent_mean(x: f64) -> f64 {
    let window: rolling<f64, 5m, 0s> = to_window(x, 5m, 0s)
    mean(window)
}

// The generated fixture's fixed-window case: the mean of 1..20 arrives once
// the window is full.
test full_window_ticks {
    assert eval(
        full_window_mean,
        x: [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0, 11.0, 12.0, 13.0, 14.0, 15.0, 16.0, 17.0, 18.0, 19.0, 20.0],
    ) == [_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, 10.5]
}

test short_window_ticks {
    assert eval(short_window_mean, x: [1.0, 2.0, 3.0, 4.0]) == [_, _, 2.0, 3.0]
}

test partial_window_ticks {
    assert eval(partial_window_mean, x: [1.0, 2.0, 3.0, 4.0]) == [1.0, 1.5, 2.0, 3.0]
}

// The generated fixture's duration case: one-microsecond harness cycles never
// span the five-minute minimum, so the window never becomes valid.
test recent_ticks {
    assert eval(recent_mean, x: [1.0, 2.0]) == [_, _]
}

test open_recent_ticks {
    assert eval(open_recent_mean, x: [1.0, 2.0]) == [1.0, 1.5]
}
)"};
    check_all_pass(unit);
}

TEST_CASE("a source-defined generic operator needs the scripted image in direct wiring", "[wiring][coverage][generic]") {
    // The example's `operator summarize` + generic `impl fn`. The file driver
    // compiles such a unit to a native image and loads its candidates before
    // wiring (`hgl test` on Unix); this harness has no loader, so the call is
    // an operator diagnostic rather than a silently wrong graph.
    Unit unit{R"(
module coverage.source_operator

use hgraph.std::{mean, to_window}

operator summarize<T, const max_size: i64, const min_size: i64>(window: rolling<T, max_size, min_size>) -> T

impl fn summarize<const max_size: i64, const min_size: i64>(window: rolling<f64, max_size, min_size>) -> f64 =>
    mean(window)

fn summarize_full_window(window: rolling<f64, 3>) -> f64 => summarize(window)

fn full_window_mean(x: f64) -> f64 => summarize_full_window(to_window(x, 3, 3))

test full_window_ticks {
    assert eval(full_window_mean, x: [1.0, 2.0, 3.0, 4.0]) == [_, _, 2.0, 3.0]
}
)"};
    if (!unit.diagnostics.has_errors()) {
        const TestResult result = only(unit.tests());
        CHECK_FALSE(result.passed);
    }
    INFO(unit.diagnostics.render(unit.file));
    CHECK(unit.diagnostics.has_errors());
    CHECK(unit.has(Category::Operator, "summarize"));
}

TEST_CASE("dynamic collection graph iteration ticks one child per key or index", "[wiring][coverage][iteration]") {
    register_coverage_operators();
    Unit unit{R"(
module coverage.dynamic_iteration

use hgraph.std::{hgl_coverage_book, hgl_coverage_observe, hgl_coverage_samples}

// `eval` drives only ts parameters, so the map and the unbounded list come
// from registered graphs. `offset` is a shared capture passed whole to every
// child; each child records what it computes on every cycle it evaluates.
fn observe_values(trigger: f64, offset: f64) {
    let book: map<str, f64> = hgl_coverage_book(trigger)
    for value in values(book) {
        hgl_coverage_observe(value + offset)
    }
}

fn observe_items(trigger: f64, offset: f64) {
    let book: map<str, f64> = hgl_coverage_book(trigger)
    for key, value in items(book) {
        hgl_coverage_observe(value + offset)
    }
}

fn observe_list_elements(trigger: f64, offset: f64) {
    let samples: list<f64> = hgl_coverage_samples(trigger)
    for value in elements(samples) {
        hgl_coverage_observe(value + offset)
    }
}

fn observe_list_items(trigger: f64, offset: f64) {
    let samples: list<f64> = hgl_coverage_samples(trigger)
    for index, value in items(samples) {
        hgl_coverage_observe(value + index + offset)
    }
}

test values_ticks {
    eval(observe_values, trigger: [1.0], offset: [10.0, 20.0])
}

test items_ticks {
    eval(observe_items, trigger: [1.0], offset: [10.0, 20.0])
}

test list_elements_ticks {
    eval(observe_list_elements, trigger: [1.0], offset: [10.0, 20.0])
}

test list_items_ticks {
    eval(observe_list_items, trigger: [1.0], offset: [10.0, 20.0])
}
)"};
    struct Expectation
    {
        const char         *name;
        std::vector<double> ticks;  // sorted: child order is not observable
    };
    const std::vector<Expectation> expectations{
        {"values_ticks", {11.0, 12.0, 21.0, 22.0}},
        {"items_ticks", {11.0, 12.0, 21.0, 22.0}},
        {"list_elements_ticks", {11.0, 12.0, 21.0, 22.0}},
        {"list_items_ticks", {11.0, 13.0, 21.0, 23.0}},
    };
    for (const Expectation &expectation : expectations) {
        coverage_observe_node::observed.clear();
        const TestResult result = only(unit.tests({expectation.name}));
        INFO(unit.diagnostics.render(unit.file));
        INFO(result.name << ": " << result.message);
        CHECK(result.passed);
        std::vector<double> observed = coverage_observe_node::observed;
        std::ranges::sort(observed);
        CHECK(observed == expectation.ticks);
    }
}
