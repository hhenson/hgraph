#include "codegen/cpp_emitter.h"
#include "semantics/resolve.h"
#include "syntax/parser.h"

#include <catch2/catch_test_macros.hpp>

#include <algorithm>
#include <fstream>
#include <iterator>
#include <string>
#include <string_view>

using namespace hgl::syntax;
using namespace hgl::semantics;
using namespace hgl::codegen;

namespace
{
    // The kernel names the fixtures import; the emitter itself is hgraph-free,
    // so the resolver's registry question is answered from this table exactly
    // as the resolver tests do (developer guide, "Frontend components").
    bool kernel_has(std::string_view name)
    {
        static constexpr std::string_view names[] = {"if_then_else", "add_", "mul_", "mean", "map_", "rolling_mean",
                                                     "hgraph.analytics.rolling_mean", "const"};
        return std::find(std::begin(names), std::end(names), name) != std::end(names);
    }

    struct Unit
    {
        SourceFile     file;
        DiagnosticSink diagnostics;
        ast::Module    module;
        ResolvedModule resolved;

        explicit Unit(std::string text, std::string path = "unit.hgl")
            : file{std::move(path), std::move(text)}, module{parse(file, diagnostics)},
              resolved{resolve(file, module, kernel_has, diagnostics)}
        {
        }

        [[nodiscard]] std::optional<EmittedModule> emit(EmitOptions options = {})
        {
            INFO(diagnostics.render(file));
            REQUIRE_FALSE(diagnostics.has_errors());
            if (options.header_name.empty()) { options.header_name = "unit.h"; }
            if (options.tool_version.empty()) { options.tool_version = "test"; }
            return emit_cpp(file, module, resolved, options, diagnostics);
        }

        [[nodiscard]] bool has(Category category, std::string_view fragment) const
        {
            return std::any_of(diagnostics.diagnostics().begin(), diagnostics.diagnostics().end(),
                               [&](const Diagnostic &d) {
                                   return d.category == category && d.message.find(fragment) != std::string::npos;
                               });
        }
    };

    std::string read_file(const std::string &path)
    {
        std::ifstream in{path, std::ios::binary};
        REQUIRE(in);
        return std::string{std::istreambuf_iterator<char>{in}, std::istreambuf_iterator<char>{}};
    }

    bool contains(const std::string &text, std::string_view fragment) { return text.find(fragment) != std::string::npos; }
}  // namespace

TEST_CASE("emit-cpp names the pair after the module and exports its functions", "[codegen]")
{
    Unit unit{read_file(std::string{HGL_CODEGEN_DIR} + "/parity.hgl"), "parity.hgl"};
    EmitOptions options;
    options.header_name = "parity.h";
    const auto emitted  = unit.emit(options);
    REQUIRE(emitted);

    CHECK(emitted->namespace_name == "hgl::codegen::parity");
    CHECK(emitted->module_name == "hgl.codegen.parity");
    CHECK(emitted->exports == std::vector<std::string>{"plus", "scaled_sum", "above", "maybe_double", "offset_by"});

    // The header declares the exported graphs and their operator markers.
    CHECK(contains(emitted->header, "#pragma once"));
    CHECK(contains(emitted->header, "namespace hgl::codegen::parity"));
    CHECK(contains(emitted->header, "struct plus : hgraph::Operator<\"hgl.codegen.parity.plus\", "
                                    "hgraph::In<\"a\", hgraph::TS<hgraph::Float>>, hgraph::In<\"b\", hgraph::TS<hgraph::Float>>, "
                                    "hgraph::Out<hgraph::TS<hgraph::Float>>> {};"));
    CHECK(contains(emitted->header, "static constexpr auto name = \"hgl.codegen.parity.plus\";"));
    CHECK(contains(emitted->header, "static hgraph::Port<hgraph::TS<hgraph::Float>> compose(hgraph::Wiring &, "
                                    "hgraph::Port<hgraph::TS<hgraph::Float>>, hgraph::Port<hgraph::TS<hgraph::Float>>);"));
    CHECK(contains(emitted->header, "hgraph::Scalar<\"k\", hgraph::Float>"));
    CHECK(contains(emitted->header, "static auto defaults() { return std::tuple{hgraph::arg<\"k\">(hgraph::Float{2.0})}; }"));
    CHECK(contains(emitted->header, "void register_operators();"));
    CHECK_FALSE(contains(emitted->header, "struct scale\n"));  // module-internal (scaled_sum is exported)

    // The source defines the exports out of line, keeps the helper internal,
    // wires operators by marker, and registers the exports by name.
    CHECK(contains(emitted->source, "#include \"parity.h\""));
    CHECK(contains(emitted->source, "namespace\n"));
    CHECK(contains(emitted->source, "struct scale\n"));
    CHECK(contains(emitted->source, "hgraph::Port<hgraph::TS<hgraph::Float>> plus::compose(hgraph::Wiring &w, "
                                    "hgraph::Port<hgraph::TS<hgraph::Float>> a, hgraph::Port<hgraph::TS<hgraph::Float>> b)"));
    CHECK(contains(emitted->source, "hgraph::wire<hgraph::stdlib::add_>(w, a, b).as<hgraph::TS<hgraph::Float>>()"));
    CHECK(contains(emitted->source, "hgraph::wire<hgraph::stdlib::gt_>(w, x, threshold.value()).as<hgraph::TS<hgraph::Bool>>()"));
    CHECK(contains(emitted->source, "hgraph::wire<scale>(w, hgraph::wire<plus>(w, a, b), k.value())"));
    CHECK(contains(emitted->source, "if (enabled.value())"));
    CHECK(contains(emitted->source, "const auto shift = (delta.value() * hgraph::Int{2});"));
    CHECK(contains(emitted->source, "register_installer(\"hgl.codegen.parity\""));
    CHECK(contains(emitted->source, "hgraph::register_graph_overload<ops::plus, plus>();"));
    CHECK(contains(emitted->source, "// parity.hgl:"));

    // Deterministic: the same input prints the same pair.
    Unit again{read_file(std::string{HGL_CODEGEN_DIR} + "/parity.hgl"), "parity.hgl"};
    const auto second = again.emit(options);
    REQUIRE(second);
    CHECK(second->header == emitted->header);
    CHECK(second->source == emitted->source);
}

TEST_CASE("emit-cpp writes a Python wrapper over the registered names", "[codegen]")
{
    Unit unit{read_file(std::string{HGL_CODEGEN_DIR} + "/parity.hgl"), "parity.hgl"};
    EmitOptions options;
    options.header_name          = "parity.h";
    options.python_native_module = "_parity";
    const auto emitted           = unit.emit(options);
    REQUIRE(emitted);
    CHECK(contains(emitted->python, "from . import _parity as _hgl_native"));
    CHECK(contains(emitted->python, "\"plus\": _hgl_operator_function(\"hgl.codegen.parity.plus\")"));
    CHECK(contains(emitted->python, "__all__ = [\"plus\", \"scaled_sum\", \"above\", \"maybe_double\", \"offset_by\"]"));
}

TEST_CASE("emit-cpp gives Python keyword exports a usable spelling", "[codegen]")
{
    Unit unit{R"(
module t
export fn class(x: f64) -> f64 => x
)"};
    EmitOptions options;
    options.python_native_module = "_t";
    const auto emitted = unit.emit(options);
    REQUIRE(emitted);
    CHECK(contains(emitted->python, "\"class_\": _hgl_operator_function(\"t.class\")"));
    CHECK(contains(emitted->python, "__all__ = [\"class_\"]"));
}

TEST_CASE("emit-cpp rejects ambiguous or invalid Python wrapper names", "[codegen]")
{
    SECTION("two exports map to one Python identifier")
    {
        Unit unit{R"(
module t
export fn def(x: f64) -> f64 => x
export fn def_(x: f64) -> f64 => x
)"};
        EmitOptions options;
        options.python_native_module = "_t";
        CHECK_FALSE(unit.emit(options));
        CHECK(unit.has(Category::Backend, "Python export 'def_' collides with 'def' as 'def_'"));
    }
    SECTION("the native module is one non-keyword identifier")
    {
        Unit unit{R"(
module t
export fn value(x: f64) -> f64 => x
)"};
        EmitOptions options;
        options.python_native_module = "bad-name";
        CHECK_FALSE(unit.emit(options));
        CHECK(unit.has(Category::Backend, "not a valid Python native-module identifier"));
    }
}

TEST_CASE("emit-cpp folds constants with the direct backend's rules", "[codegen]")
{
    Unit unit{R"(
module t

export fn f(x: f64, const n: i64, const s: str) -> f64 {
    let half = n / 2
    let label = s + "!"
    var total = n * 3
    total -= 1
    if total > 2 && label == "hi!" {
        return x * half
    }
    x
}
)"};
    const auto emitted = unit.emit();
    REQUIRE(emitted);
    CHECK(contains(emitted->source, "(static_cast<hgraph::Float>(n.value()) / static_cast<hgraph::Float>(hgraph::Int{2}))"));
    CHECK(contains(emitted->source, "const auto label = (s.value() + hgraph::Str{\"!\"});"));
    CHECK(contains(emitted->source, "auto total = (n.value() * hgraph::Int{3});"));
    CHECK(contains(emitted->source, "total = (total - hgraph::Int{1});"));
    CHECK(contains(emitted->source, "if (((total > hgraph::Int{2}) && (label == hgraph::Str{\"hi!\"})))"));
    CHECK(contains(emitted->source, "hgraph::wire<hgraph::stdlib::mul_>(w, x, half)"));
}

TEST_CASE("emit-cpp rejects type-changing var assignment", "[codegen]")
{
    SECTION("ordinary assignment cannot narrow an inferred i64")
    {
        Unit unit{R"(
module t
export fn f(x: f64) -> f64 {
    var y = 1
    y = 2.5
    x + y
}
)"};
        CHECK_FALSE(unit.emit());
        CHECK(unit.has(Category::Type, "assignment to 'y' expects hgraph::Int, got hgraph::Float"));
    }
    SECTION("compound division cannot change an inferred i64 to f64")
    {
        Unit unit{R"(
module t
export fn f(x: f64) -> f64 {
    var y = 4
    y /= 2
    x + y
}
)"};
        CHECK_FALSE(unit.emit());
        CHECK(unit.has(Category::Type, "assignment to 'y' expects hgraph::Int, got hgraph::Float"));
    }
    SECTION("i64 still widens into an f64 var")
    {
        Unit unit{R"(
module t
export fn f(x: f64) -> f64 {
    var y = 1.0
    y = 2
    x + y
}
)"};
        const auto emitted = unit.emit();
        REQUIRE(emitted);
        CHECK(contains(emitted->source, "y = static_cast<hgraph::Float>(hgraph::Int{2});"));
    }
}

TEST_CASE("emit-cpp rejects zero constant divisors", "[codegen]")
{
    SECTION("division")
    {
        Unit unit{R"(
module t
export fn f(x: f64) -> f64 => x + 1 / (2 - 2)
)"};
        CHECK_FALSE(unit.emit());
        CHECK(unit.has(Category::Type, "division by zero"));
    }
    SECTION("remainder")
    {
        Unit unit{R"(
module t
export fn f(x: f64) -> f64 => x + 1 % 0
)"};
        CHECK_FALSE(unit.emit());
        CHECK(unit.has(Category::Type, "division by zero"));
    }
}

TEST_CASE("emit-cpp wires constants at a temporal parameter and reads a kernel by marker", "[codegen]")
{
    Unit unit{R"(
module t

use hgraph.analytics::{rolling_mean}

fn midpoint(tob: atomic<tuple<f64, f64>>) -> f64 =>
    (tob[0] + tob[1]) / 2.0

export fn smooth(tob: atomic<tuple<f64, f64>>, const window: i64 = 20) -> f64 {
    rolling_mean(midpoint(tob), period: window)
}

export fn fixed(x: f64) -> f64 => plus_one(1)

fn plus_one(y: f64) -> f64 => y + 1.0
)"};
    const auto emitted = unit.emit();
    REQUIRE(emitted);
    CHECK(contains(emitted->header, "#include <hgraph/analytics/operators.h>"));
    CHECK(contains(emitted->header, "hgraph::Port<hgraph::TS<hgraph::Tuple<hgraph::Float, hgraph::Float>>>"));
    CHECK(contains(emitted->source, "hgraph::wire<hgraph::stdlib::getitem_>(w, tob, hgraph::Int{0})"));
    CHECK(contains(emitted->source, "hgraph::wire<hgraph::analytics::rolling_mean>(w, hgraph::wire<midpoint>(w, tob), "
                                    "hgraph::arg<\"period\">(window.value()))"));
    // A constant at a temporal parameter is wired through `const` at the
    // parameter's schema, converting int to float as the direct backend does.
    CHECK(contains(emitted->source, "hgraph::wire<plus_one>(w, hgraph::wire<hgraph::stdlib::const_, hgraph::TS<hgraph::Float>>(w, "
                                    "static_cast<hgraph::Float>(hgraph::Int{1})))"));
    // Helpers are defined before the functions that wire them.
    CHECK(emitted->source.find("struct plus_one") < emitted->source.find("fixed::compose"));
}

TEST_CASE("emit-cpp lowers scalar runtime functions to static nodes", "[codegen][runtime]")
{
    Unit unit{R"(
module t
export fn total(a: f64, b: f64) -> f64 {
    state sum: f64 = 0.0
    inject out
    when modified(a, b) && valid(a) {
        if valid(b) {
            sum += a + b
            out = sum
        }
    }
}

fn private_total(a: f64) -> f64 {
    state sum: f64 = 0.0
    when modified(a) && valid(a) {
        sum += a
        return sum
    }
}

export fn through_private(a: f64) -> f64 => private_total(a)
)"};
    const auto emitted = unit.emit();
    REQUIRE(emitted);

    CHECK(contains(emitted->header, "using recordable_state = hgraph::TSB<\"t.total.state\", "
                                    "hgraph::Field<\"sum\", hgraph::TS<hgraph::Float>>>;"));
    CHECK(contains(emitted->header, "hgraph::InputValidity::Unchecked"));
    CHECK(contains(emitted->header, "if (((a.modified() || b.modified()) && (a.valid())))"));
    CHECK(contains(emitted->header, "if (!sum.valid())"));
    CHECK(contains(emitted->header, "sum.set((sum.value().checked_as<hgraph::Float>() + (a.value() + b.value())));"));
    CHECK(contains(emitted->header, "hgl_output.set(sum.value().checked_as<hgraph::Float>());"));
    CHECK(contains(emitted->source, "hgraph::register_overload<ops::total, total>();"));
    CHECK_FALSE(contains(emitted->source, "register_graph_overload<ops::total"));
    CHECK_FALSE(contains(emitted->header, "private_total"));
    CHECK(contains(emitted->source, "struct hgl_internal_operator_"));
    CHECK(contains(emitted->source, "hgraph::register_overload<hgl_internal_operator_"));
    CHECK(contains(emitted->source, "hgraph::wire<private_total>(w, a)"));
}

TEST_CASE("emit-cpp requires validity to dominate runtime payload reads", "[codegen][runtime]")
{
    SECTION("a when and nested if establish validity for their bodies")
    {
        Unit unit{R"(
module t
export fn sampled(trigger: f64, sample: f64) -> f64 {
    when modified(trigger) && valid(trigger) {
        if valid(sample) && sample > 0.0 {
            return trigger + sample
        }
    }
}
)"};
        REQUIRE(unit.emit());
    }
    SECTION("an unchecked temporal payload read fails closed")
    {
        Unit unit{R"(
module t
export fn sampled(trigger: f64, sample: f64) -> f64 {
    when modified(trigger) {
        return sample
    }
}
)"};
        CHECK_FALSE(unit.emit());
        CHECK(unit.has(Category::Type, "temporal input 'sample' may be invalid here; guard the read with valid(sample)"));
    }
    SECTION("validity must precede a payload read in a short-circuit condition")
    {
        Unit unit{R"(
module t
export fn positive(value: f64) -> f64 {
    when modified(value) && value > 0.0 && valid(value) {
        return value
    }
}
)"};
        CHECK_FALSE(unit.emit());
        CHECK(unit.has(Category::Type, "temporal input 'value' may be invalid here; guard the read with valid(value)"));
    }
    SECTION("when blocks are function-level handlers")
    {
        Unit unit{R"(
module t
export fn sampled(value: f64) -> f64 {
    if valid(value) {
        when modified(value) { return value }
    }
}
)"};
        CHECK_FALSE(unit.emit());
        CHECK(unit.has(Category::Backend, "a 'when' block must be at function top level"));
    }
}

TEST_CASE("emit-cpp fails closed on what the first pass does not lower", "[codegen]")
{
    SECTION("a runtime call")
    {
        Unit unit{R"(
module t
fn twice(x: f64) -> f64 => x * 2.0
export fn sampled(x: f64) -> f64 {
    when modified(x) && valid(x) { return twice(x) }
}
)"};
        CHECK_FALSE(unit.emit());
        CHECK(unit.has(Category::Backend, "calls in a runtime function are not supported by emit-cpp yet"));
    }
    SECTION("an unsupported runtime injectable")
    {
        Unit unit{R"(
module t
export fn logged(x: f64) -> f64 {
    inject logger
    when modified(x) { return x }
}
)"};
        CHECK_FALSE(unit.emit());
        CHECK(unit.has(Category::Backend, "injectable 'logger' is not supported by emit-cpp yet"));
    }
    SECTION("runtime state without an explicit type")
    {
        Unit unit{R"(
module t
export fn total(x: f64) -> f64 {
    state sum = 0.0
    when modified(x) { return x }
}
)"};
        CHECK_FALSE(unit.emit());
        CHECK(unit.has(Category::Backend, "runtime state declaration needs an explicit scalar type"));
    }
    SECTION("a temporal input in a lifecycle block")
    {
        Unit unit{R"(
module t
export fn seeded(x: f64) -> f64 {
    state seed: f64 = 0.0
    start { seed = x }
    when modified(x) && valid(x) { return x }
}
)"};
        CHECK_FALSE(unit.emit());
        CHECK(unit.has(Category::Phase, "temporal parameters are not available in runtime lifecycle blocks"));
    }
    SECTION("a struct declaration")
    {
        Unit unit{R"(
module t
export struct Quote { bid: f64 }
export fn twice(x: f64) -> f64 => x * 2.0
)"};
        CHECK_FALSE(unit.emit());
        CHECK(unit.has(Category::Backend, "a struct declaration is not supported by emit-cpp yet"));
    }
    SECTION("a duration rolling window")
    {
        Unit unit{R"(
module t
use hgraph.std::{mean}
export fn recent(window: rolling<f64, 5m>) -> f64 => mean(window)
)"};
        CHECK_FALSE(unit.emit());
        CHECK(unit.has(Category::Backend, "duration rolling window"));
    }
    SECTION("a non-positive rolling size")
    {
        Unit unit{R"(
module t
export fn recent(window: rolling<f64, 0>) -> f64 => window
)"};
        CHECK_FALSE(unit.emit());
        CHECK(unit.has(Category::Type, "a rolling size is a positive i64 constant or a duration"));
    }
    SECTION("a negative rolling size")
    {
        Unit unit{R"(
module t
export fn recent(window: rolling<f64, -1>) -> f64 => window
)"};
        CHECK_FALSE(unit.emit());
        CHECK(unit.has(Category::Type, "a rolling size is a positive i64 constant or a duration"));
    }
    SECTION("a generic function")
    {
        Unit unit{R"(
module t
export fn same<U>(a: U, b: U) -> U => a
)"};
        CHECK_FALSE(unit.emit());
        CHECK(unit.has(Category::Backend, "a generic function is not supported by emit-cpp yet"));
    }
    SECTION("a missing module declaration")
    {
        // The front end rejects the unit before the emitter sees it; the
        // emitter's own guard is defensive.
        Unit unit{"export fn twice(x: f64) -> f64 => x * 2.0\n"};
        CHECK(unit.diagnostics.has_errors());
        CHECK(unit.has(Category::Module, "module declaration"));
    }
}

TEST_CASE("emit-cpp escapes C++ keywords and its own names", "[codegen]")
{
    Unit unit{R"(
module t.new
export fn w(delete: f64, const int: i64 = 1) -> f64 => delete * int
)"};
    const auto emitted = unit.emit();
    REQUIRE(emitted);
    CHECK(emitted->namespace_name == "t::new_");
    CHECK(contains(emitted->header, "struct w_ : hgraph::Operator<\"t.new.w\""));
    CHECK(contains(emitted->source, "hgraph::Port<hgraph::TS<hgraph::Float>> w_::compose(hgraph::Wiring &w, "
                                    "hgraph::Port<hgraph::TS<hgraph::Float>> delete_, hgraph::Scalar<\"int\", hgraph::Int> int_)"));
}

TEST_CASE("emit-cpp diagnoses escaped C++ name collisions", "[codegen]")
{
    SECTION("module functions")
    {
        Unit unit{R"(
module t
export fn class(x: f64) -> f64 => x
export fn class_(x: f64) -> f64 => x
)"};
        CHECK_FALSE(unit.emit());
        CHECK(unit.has(Category::Backend, "C++ function 'class_' collides with 'class' as 'class_'"));
    }
    SECTION("parameters")
    {
        Unit unit{R"(
module t
export fn value(class: f64, class_: f64) -> f64 => class + class_
)"};
        CHECK_FALSE(unit.emit());
        CHECK(unit.has(Category::Backend, "C++ parameter 'class_' collides with 'class' as 'class_'"));
    }
}

TEST_CASE("emit-cpp gives shadowing locals unique C++ names", "[codegen]")
{
    Unit unit{R"(
module t
export fn twice(x: f64) -> f64 {
    let x = x * 2.0
    x
}
)"};
    const auto emitted = unit.emit();
    REQUIRE(emitted);
    CHECK(contains(emitted->source, "const auto x_1 = hgraph::wire<hgraph::stdlib::mul_>(w, x, hgraph::Float{2.0});"));
    CHECK(contains(emitted->source, "return x_1.as<hgraph::TS<hgraph::Float>>();"));
}
