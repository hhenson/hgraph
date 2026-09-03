#include "semantics/resolve.h"
#include "syntax/parser.h"
#include "wiring/backend.h"

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
    // A unit through the frontend against the live registry, ready for the
    // backend (developer guide, "Direct-wiring backend", "First pass").
    struct Unit
    {
        SourceFile     file;
        DiagnosticSink diagnostics;
        ast::Module    module;
        ResolvedModule resolved;

        explicit Unit(std::string text)
            : file{"test.hgl", std::move(text)}, module{parse(file, diagnostics)},
              resolved{resolve(file, module, has_operator, diagnostics)}
        {
        }

        [[nodiscard]] std::vector<TestResult> tests(std::vector<std::string> names = {})
        {
            INFO(diagnostics.render(file));
            REQUIRE_FALSE(diagnostics.has_errors());
            TestOptions options;
            options.names = std::move(names);
            return run_tests(file, module, resolved, options, diagnostics);
        }

        [[nodiscard]] bool has(Category category, std::string_view fragment) const
        {
            return std::any_of(diagnostics.diagnostics().begin(), diagnostics.diagnostics().end(),
                               [&](const Diagnostic &d) {
                                   return d.category == category && d.message.find(fragment) != std::string::npos;
                               });
        }
    };

    TestResult only(std::vector<TestResult> results)
    {
        REQUIRE(results.size() == 1);
        return std::move(results.front());
    }
}  // namespace

TEST_CASE("constant expressions fold in a test body", "[wiring]")
{
    Unit unit{R"(
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
    assert "ab" + "c" == "abc"
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

TEST_CASE("eval drives a composition through the harness", "[wiring]")
{
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
    for (const TestResult &result : unit.tests())
    {
        INFO(result.name << ": " << result.message);
        CHECK(result.passed);
    }
}

TEST_CASE("a failing assert reports the observed sequence", "[wiring]")
{
    Unit unit{R"(
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

TEST_CASE("selected tests run and describe their tail", "[wiring]")
{
    Unit unit{R"(
module t

fn midpoint(tob: atomic<tuple<f64, f64>>) -> f64 => (tob[0] + tob[1]) / 2.0

fn same(tob: atomic<tuple<f64, f64>>) -> atomic<tuple<f64, f64>> => tob

test one { assert 1 == 1 }
test two { eval(midpoint, tob: [(1.0, 2.0), (2.0, 3.0)]) }
test three { eval(same, tob: [(1.0, 2.0), _]) }
)"};
    INFO(unit.diagnostics.render(unit.file));
    REQUIRE_FALSE(unit.diagnostics.has_errors());
    TestOptions options;
    options.names         = {"two"};
    options.describe_tail = true;
    const TestResult result = only(run_tests(unit.file, unit.module, unit.resolved, options, unit.diagnostics));
    CHECK(result.name == "two");
    CHECK(result.passed);
    CHECK(result.tail == "[1.5, 2.5]");

    options.names = {"three"};
    const TestResult tuples = only(run_tests(unit.file, unit.module, unit.resolved, options, unit.diagnostics));
    CHECK(tuples.passed);
    CHECK(tuples.tail == "[(1.0, 2.0), _]");
}

TEST_CASE("first-pass limits are diagnostics, not crashes", "[wiring]")
{
    Unit unit{R"(
module t

fn midpoint(tob: atomic<tuple<f64, f64>>) -> f64 => (tob[0] + tob[1]) / 2.0

fn counter(x: f64) -> f64 {
    state total: f64 = 0.0
    inject out
    when x { total += x }
    out = total
}

test timed {
    assert eval(midpoint, tob: [@2026-01-01T00:00:00Z: (1.0, 2.0)]) == [1.5]
}

test runtime {
    assert eval(counter, x: [1.0]) == [1.0]
}

test wrong_type {
    assert eval(midpoint, tob: ["a"]) == [1.5]
}
)"};
    const std::vector<TestResult> results = unit.tests();
    REQUIRE(results.size() == 3);
    for (const TestResult &result : results) { CHECK_FALSE(result.passed); }
    CHECK(unit.has(Category::Test, "timed sequences are not supported by the first pass"));
    CHECK(unit.has(Category::Backend, "'counter' is a runtime function"));
    CHECK(unit.has(Category::Type, "the sequence element expects Tuple[float,float], got str"));
}

TEST_CASE("run_program prints one line per tick", "[wiring]")
{
    Unit unit{R"(
module t

use hgraph.std::{schedule}

export fn heartbeat(const every: duration = 1s) -> datetime => last_modified(schedule(every))

export fn other(x: f64) -> f64 => x
)"};
    INFO(unit.diagnostics.render(unit.file));
    REQUIRE_FALSE(unit.diagnostics.has_errors());

    SECTION("the entry with only const parameters runs")
    {
        RunOptions options;
        options.start     = hgraph::DateTime{std::chrono::microseconds{1'700'000'000'000'000}};
        options.end_after = hgraph::TimeDelta{std::chrono::seconds{3}};
        options.settings.push_back(Setting{"every", "1500ms"});
        std::ostringstream out;
        const bool ok = run_program(unit.file, unit.module, unit.resolved, options, unit.diagnostics, out);
        INFO(unit.diagnostics.render(unit.file));
        CHECK(ok);
        CHECK(out.str() == "2023-11-14T22:13:21.5Z 2023-11-14 22:13:21.500000\n");
    }

    SECTION("an entry with temporal parameters cannot run")
    {
        RunOptions options;
        options.entry = "other";
        std::ostringstream out;
        CHECK_FALSE(run_program(unit.file, unit.module, unit.resolved, options, unit.diagnostics, out));
        CHECK(unit.has(Category::Backend, "entry parameter 'x' is not const"));
    }

    SECTION("settings are checked against the parameter type")
    {
        RunOptions options;
        options.settings.push_back(Setting{"every", "3"});
        std::ostringstream out;
        CHECK_FALSE(run_program(unit.file, unit.module, unit.resolved, options, unit.diagnostics, out));
        CHECK(unit.has(Category::Type, "'every' expects timedelta, got int"));
    }
}

TEST_CASE("format_time spells the canonical datetime without the sigil", "[wiring]")
{
    CHECK(format_time(hgraph::DateTime{std::chrono::microseconds{1'700'000'000'000'000}}) == "2023-11-14T22:13:20Z");
    CHECK(format_time(hgraph::DateTime{std::chrono::microseconds{1'700'000'000'500'000}}) == "2023-11-14T22:13:20.5Z");
}
