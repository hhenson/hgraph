#include "codegen/cpp_emitter.h"
#include "hgraph_ir/lower.h"
#include "ir/lower.h"
#include "ir/type_check.h"
#include "semantics/resolve.h"
#include "syntax/parser.h"

#include <catch2/catch_test_macros.hpp>

#include <algorithm>
#include <cstdint>
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
        hgl::ir::hir::Module   hir;
        hgl::hgraph_ir::Module graph;

        explicit Unit(std::string text, std::string path = "unit.hgl")
            : file{std::move(path), std::move(text)}, module{parse(file, diagnostics)} {
            resolved = resolve(file, module, kernel_has, diagnostics);
            if (diagnostics.has_errors()) { return; }
            hir                                       = hgl::ir::lower_to_hir(module, resolved, diagnostics);
            const hgl::ir::OperatorResolver operators = [](const hgl::ir::hir::Module &, const hgl::ir::OperatorQuery &query) {
                hgl::ir::OperatorSelection selected;
                selected.result = query.expected_result;
                if (!selected.result.valid() && !query.arguments.empty()) { selected.result = query.arguments.front().type; }
                selected.deferred = true;
                return selected;
            };
            hgl::ir::hir::Module        typed = hir;
            hgl::syntax::DiagnosticSink graph_diagnostics;
            if (hgl::ir::complete_hir(typed, operators, graph_diagnostics)) {
                hgl::hgraph_ir::Module lowered = hgl::hgraph_ir::lower(typed, graph_diagnostics);
                if (!graph_diagnostics.has_errors()) {
                    graph = std::move(lowered);
                    return;
                }
            }
            for (const Diagnostic &diagnostic : graph_diagnostics.diagnostics()) {
                Diagnostic &copy = diagnostics.report(diagnostic.category, diagnostic.range, diagnostic.message);
                copy.notes       = diagnostic.notes;
            }
        }

        [[nodiscard]] std::optional<EmittedModule> emit(EmitOptions options = {})
        {
            INFO(diagnostics.render(file));
            if (diagnostics.has_errors()) { return std::nullopt; }
            if (options.header_name.empty()) { options.header_name = "unit.h"; }
            if (options.tool_version.empty()) { options.tool_version = "test"; }
            return emit_cpp(file, graph, module, resolved, options, diagnostics);
        }

        [[nodiscard]] bool has(Category category, std::string_view fragment) const
        {
            const bool found =
                std::any_of(diagnostics.diagnostics().begin(), diagnostics.diagnostics().end(), [&](const Diagnostic &d) {
                    return d.category == category && d.message.find(fragment) != std::string::npos;
                });
            if (!found) { WARN(diagnostics.render(file)); }
            return found;
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

    // The header declares the exported graphs and transparent operator aliases.
    CHECK(contains(emitted->header, "#pragma once"));
    CHECK(contains(emitted->header, "namespace hgl::codegen::parity"));
    CHECK(contains(emitted->header, "using plus = hgraph::Operator<\"hgl.codegen.parity.plus\", "
                                    "hgraph::In<\"a\", hgraph::TS<hgraph::Float>>, hgraph::In<\"b\", hgraph::TS<hgraph::Float>>, "
                                    "hgraph::Out<hgraph::TS<hgraph::Float>>>;"));
    CHECK(contains(emitted->header, "[[maybe_unused]] static constexpr auto name = \"hgl.codegen.parity.plus\";"));
    CHECK(contains(emitted->header, "static hgraph::Port<hgraph::TS<hgraph::Float>> compose(hgraph::Wiring &, "
                                    "hgraph::Port<hgraph::TS<hgraph::Float>>, hgraph::Port<hgraph::TS<hgraph::Float>>);"));
    CHECK(contains(emitted->header, "hgraph::Scalar<\"k\", hgraph::Float>"));
    CHECK(contains(emitted->header, "static auto defaults() { return std::tuple{hgraph::arg<\"k\">(hgraph::Float{2.0})}; }"));
    CHECK(contains(emitted->header, "std::numeric_limits<hgraph::Float>::infinity()"));
    CHECK(contains(emitted->header, "std::numeric_limits<hgraph::Int>::min()"));
    CHECK(contains(emitted->source, "std::numeric_limits<hgraph::TimeDelta::rep>::min()"));
    CHECK(contains(emitted->header, "hgraph::OperatorProviderHandle register_operators();"));
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
    CHECK(contains(emitted->source, "#include <hgraph/util/scope.h>"));
    CHECK(contains(emitted->source, "auto rollback = hgraph::make_scope_exit<true>([&]"));
    CHECK(contains(emitted->source, "registry.activate_provider(provider);"));
    CHECK(contains(emitted->source, "(void)registry.remove_provider(provider);"));
    CHECK(contains(emitted->source, "rollback.release();"));
    CHECK(contains(emitted->source, "return provider;"));
    CHECK(contains(emitted->source, "hgraph::register_graph_overload<operators::plus, plus>();"));
    CHECK(contains(emitted->source, "// parity.hgl:"));

    // Deterministic: the same input prints the same pair.
    Unit again{read_file(std::string{HGL_CODEGEN_DIR} + "/parity.hgl"), "parity.hgl"};
    const auto second = again.emit(options);
    REQUIRE(second);
    CHECK(second->header == emitted->header);
    CHECK(second->source == emitted->source);
}

TEST_CASE("emit-cpp plans module and callable identity from hgraph IR", "[codegen][hgraph-ir]") {
    Unit unit{R"(
module old

fn hidden(value: f64) -> f64 => value
)"};
    REQUIRE_FALSE(unit.diagnostics.has_errors());
    REQUIRE(unit.graph.completion == hgl::hgraph_ir::Completion::Bodies);
    REQUIRE(unit.graph.callables.size() == 1);
    unit.graph.path                         = "planned.module";
    unit.graph.callables.front().identity   = "planned.module.exposed";
    unit.graph.callables.front().visibility = hgl::hgraph_ir::CallableVisibility::Export;
    const hgl::hgraph_ir::TypeId integer{static_cast<std::uint32_t>(unit.graph.types.size())};
    unit.graph.types.push_back({.kind   = hgl::ir::hir::TypeKind::Scalar,
                                .scalar = hgl::ir::hir::ScalarType::I64,
                                .range  = unit.graph.callables.front().range});
    unit.graph.callables.front().parameters.front().name = "amount";
    unit.graph.callables.front().parameters.front().type = integer;
    unit.graph.callables.front().result                  = integer;

    const auto emitted = unit.emit();
    REQUIRE(emitted);
    CHECK(emitted->module_name == "planned.module");
    CHECK(emitted->namespace_name == "planned::module");
    CHECK(emitted->exports == std::vector<std::string>{"exposed"});
    CHECK(contains(emitted->header, "namespace planned::module"));
    CHECK(contains(emitted->header, "struct exposed"));
    CHECK(contains(emitted->header, "static constexpr auto name = \"planned.module.exposed\""));
    CHECK(contains(emitted->header, "hgraph::In<\"amount\", hgraph::TS<hgraph::Int>>"));
    CHECK(contains(emitted->header, "hgraph::Out<hgraph::TS<hgraph::Int>>"));
    CHECK(contains(emitted->source, "hgraph::Port<hgraph::TS<hgraph::Int>> amount"));
    CHECK(contains(emitted->source, "hgraph::Port<hgraph::TS<hgraph::Int>> exposed::compose"));
    CHECK(contains(emitted->source, "return amount;"));
    CHECK_FALSE(contains(emitted->header, "hgraph::TS<hgraph::Float>"));
    CHECK(contains(emitted->source, "register_graph_overload<operators::exposed, exposed>()"));
}

TEST_CASE("emit-cpp rejects a mismatched syntax compatibility adapter", "[codegen][hgraph-ir]") {
    SECTION("a missing callable") {
        Unit unit{"module t\nexport fn value(x: f64) -> f64 => x\n"};
        REQUIRE(unit.graph.completion == hgl::hgraph_ir::Completion::Bodies);
        unit.graph.callables.clear();

        CHECK_FALSE(unit.emit());
        CHECK(unit.has(Category::Backend, "syntax body adapter disagree"));
    }
    SECTION("a changed parameter count") {
        Unit unit{"module t\nexport fn value(x: f64) -> f64 => x\n"};
        REQUIRE(unit.graph.completion == hgl::hgraph_ir::Completion::Bodies);
        unit.graph.callables.front().parameters.push_back(unit.graph.callables.front().parameters.front());

        CHECK_FALSE(unit.emit());
        CHECK(unit.has(Category::Backend, "syntax body adapter's signature shape"));
    }
    SECTION("a changed parameter role") {
        Unit unit{"module t\nexport fn value(x: f64) -> f64 => x\n"};
        REQUIRE(unit.graph.completion == hgl::hgraph_ir::Completion::Bodies);
        unit.graph.callables.front().parameters.front().is_const = true;

        CHECK_FALSE(unit.emit());
        CHECK(unit.has(Category::Backend, "syntax body adapter's parameter roles"));
    }
    SECTION("a changed default shape") {
        Unit unit{"module t\nexport fn value(x: f64, const factor: f64 = 2.0) -> f64 => x * factor\n"};
        REQUIRE(unit.graph.completion == hgl::hgraph_ir::Completion::Bodies);
        unit.graph.callables.front().parameters.back().default_value = {};

        CHECK_FALSE(unit.emit());
        CHECK(unit.has(Category::Backend, "syntax body adapter's default shape"));
    }
    SECTION("a missing struct") {
        Unit unit{"module t\nexport struct Value { amount: f64 }\n"};
        REQUIRE(unit.graph.completion == hgl::hgraph_ir::Completion::Bodies);
        unit.graph.structures.clear();

        CHECK_FALSE(unit.emit());
        CHECK(unit.has(Category::Backend, "syntax construction adapter disagree"));
    }
    SECTION("a changed struct field count") {
        Unit unit{"module t\nexport struct Value { amount: f64 }\n"};
        REQUIRE(unit.graph.completion == hgl::hgraph_ir::Completion::Bodies);
        unit.graph.structures.front().fields.clear();

        CHECK_FALSE(unit.emit());
        CHECK(unit.has(Category::Backend, "syntax construction adapter's declaration shape"));
    }
}

TEST_CASE("emit-cpp plans struct identity and layout from hgraph IR", "[codegen][hgraph-ir][structs]") {
    Unit unit{R"(
module old

export abstract struct Shape<T> {
    value: T
    label: str = null
}
)"};
    REQUIRE_FALSE(unit.diagnostics.has_errors());
    REQUIRE(unit.graph.completion == hgl::hgraph_ir::Completion::Bodies);
    REQUIRE(unit.graph.structures.size() == 1);

    auto &structure             = unit.graph.structures.front();
    structure.identity          = "planned.module.Record";
    structure.abstract          = false;
    structure.generics[0].name = "Item";
    structure.fields[0].name    = "amount";
    structure.fields[1].name    = "count";
    const hgl::hgraph_ir::TypeId integer{static_cast<std::uint32_t>(unit.graph.types.size())};
    unit.graph.types.push_back({.kind   = hgl::ir::hir::TypeKind::Scalar,
                                .scalar = hgl::ir::hir::ScalarType::I64,
                                .range  = structure.fields[1].range});
    structure.fields[1].type = integer;
    unit.graph.path           = "planned.module";

    const auto emitted = unit.emit();
    REQUIRE(emitted);
    CHECK(emitted->module_name == "planned.module");
    CHECK(contains(emitted->header, "namespace planned::module"));
    CHECK(contains(emitted->header, "template <typename Item>\n    struct Record"));
    CHECK(contains(emitted->header, "hgraph::NominalBundle<\"planned.module\", \"Record\", false"));
    CHECK(contains(emitted->header, "hgraph::Field<\"amount\", Item>"));
    CHECK(contains(emitted->header, "hgraph::Field<\"count\", hgraph::Int>"));
    CHECK(contains(emitted->header, "hgraph::Field<\"amount\", hgraph::TS<Item>>"));
    CHECK_FALSE(contains(emitted->header, "struct Shape"));
    CHECK_FALSE(contains(emitted->header, "hgraph::Field<\"value\""));
    CHECK_FALSE(contains(emitted->header, "hgraph::Field<\"label\""));
}

TEST_CASE("emit-cpp renders defaults and omitted arguments from hgraph IR", "[codegen][hgraph-ir][defaults]") {
    Unit unit{R"(
module planned_defaults

fn scaled(value: f64, const factor: f64 = 2.0) -> f64 => value * factor
export fn result(value: f64) -> f64 => scaled(value)
)"};
    REQUIRE_FALSE(unit.diagnostics.has_errors());
    REQUIRE(unit.graph.completion == hgl::hgraph_ir::Completion::Bodies);
    const auto scaled = std::find_if(unit.graph.callables.begin(), unit.graph.callables.end(),
                                     [](const auto &callable) { return callable.identity == "planned_defaults.scaled"; });
    REQUIRE(scaled != unit.graph.callables.end());
    REQUIRE(scaled->parameters.size() == 2);
    const hgl::hgraph_ir::ConstExprId default_value = scaled->parameters.back().default_value;
    REQUIRE(default_value.valid());
    REQUIRE(default_value.value < unit.graph.const_exprs.size());
    unit.graph.const_exprs[default_value.value].literal = std::int64_t{7};

    const auto emitted = unit.emit();
    REQUIRE(emitted);
    CHECK(contains(emitted->source, "hgraph::arg<\"factor\">(static_cast<hgraph::Float>(hgraph::Int{7}))"));
    CHECK(contains(emitted->source, "hgraph::wire<scaled>(w, value, static_cast<hgraph::Float>(hgraph::Int{7}))"));
    CHECK_FALSE(contains(emitted->source, "hgraph::Float{2.0}"));
}

TEST_CASE("emit-cpp gives operator implementations distinct readable C++ names", "[codegen][hgraph-ir][operators]") {
    Unit unit{R"(
module overloads

operator choose<T>(value: T) -> T
impl fn choose(value: f64) -> f64 => value
impl fn choose(value: i64) -> i64 => value
)"};
    REQUIRE_FALSE(unit.diagnostics.has_errors());
    REQUIRE(unit.graph.completion == hgl::hgraph_ir::Completion::Bodies);
    REQUIRE(unit.graph.callables.size() == 2);

    const auto emitted = unit.emit();
    REQUIRE(emitted);
    for (const auto &implementation : unit.graph.callables) {
        const std::size_t marker = implementation.identity.find_last_of('#');
        REQUIRE(marker != std::string::npos);
        const std::string concrete = "choose_impl_" + implementation.identity.substr(marker + 1U);
        CHECK(contains(emitted->source, "struct " + concrete));
        CHECK(contains(emitted->source, "register_graph_overload<operators::choose, " + concrete + ">()"));
    }
}

TEST_CASE("emit-cpp uses the hgraph IR identity for local operator calls", "[codegen][hgraph-ir][operators]") {
    Unit unit{R"(
module renamed_ops

operator choose<T>(value: T) -> T
impl fn choose(value: f64) -> f64 => value
export fn selected(value: f64) -> f64 => choose(value)
)"};
    REQUIRE_FALSE(unit.diagnostics.has_errors());
    REQUIRE(unit.graph.completion == hgl::hgraph_ir::Completion::Bodies);
    REQUIRE(unit.graph.operators.size() == 1);

    unit.graph.operators.front().identity                = "renamed_ops.pick";
    unit.graph.operators.front().parameters.front().name = "item";
    for (auto &callable : unit.graph.callables) {
        if (callable.visibility == hgl::hgraph_ir::CallableVisibility::Implementation) {
            callable.operator_identity = unit.graph.operators.front().identity;
        }
    }

    const auto emitted = unit.emit();
    REQUIRE(emitted);
    CHECK(contains(emitted->header, "using pick = hgraph::Operator<"));
    CHECK(contains(emitted->header, "hgraph::In<\"item\","));
    CHECK(contains(emitted->source, "hgraph::wire<operators::pick>(w, value)"));
    CHECK(contains(emitted->source, "register_graph_overload<operators::pick, pick_impl_"));
    CHECK_FALSE(contains(emitted->source, "operators::choose"));
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
        CHECK(unit.has(Category::Type, "assignment has type f64, expected i64"));
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
        CHECK(unit.has(Category::Type, "remainder by zero"));
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
    CHECK(contains(emitted->source, "hgraph::register_overload<operators::total, total>();"));
    CHECK_FALSE(contains(emitted->source, "register_graph_overload<operators::total"));
    CHECK_FALSE(contains(emitted->header, "private_total"));
    CHECK(contains(emitted->source, "namespace operator_contracts"));
    CHECK(contains(emitted->source, "using private_total = hgraph::Operator<"));
    CHECK(contains(emitted->source, "hgraph::register_overload<operator_contracts::private_total, private_total>()"));
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
export fn sampled(value: f64) {
    if valid(value) {
        when modified(value) { let sampled = value }
    }
}
)"};
        CHECK_FALSE(unit.emit());
        CHECK(unit.has(Category::Backend, "a 'when' block must be at function top level"));
    }
}

TEST_CASE("emit-cpp fails closed on constructs it does not lower", "[codegen]") {
    SECTION("a const-generic struct")
    {
        Unit unit{R"(
module t
export struct Tag<const n: i64> {
    value: i64
}
)"};
        CHECK_FALSE(unit.emit());
        CHECK(unit.has(Category::Backend,
                       "const generic struct arguments require typed constant Bundle metadata in hgraph"));
    }
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
    SECTION("a non-positive rolling size")
    {
        Unit unit{R"(
module t
export fn recent(window: rolling<f64, 0>) -> f64 => 1.0
)"};
        CHECK_FALSE(unit.emit());
        CHECK(unit.has(Category::Type, "a rolling size is a positive i64 constant or a duration"));
    }
    SECTION("a negative rolling size")
    {
        Unit unit{R"(
module t
export fn recent(window: rolling<f64, -1>) -> f64 => 1.0
)"};
        CHECK_FALSE(unit.emit());
        CHECK(unit.has(Category::Type, "a rolling size is a positive i64 constant or a duration"));
    }
    SECTION("a rolling minimum larger than its maximum") {
        Unit unit{R"(
module t
export fn recent(window: rolling<f64, 5, 6>) -> f64 => 1.0
)"};
        CHECK_FALSE(unit.emit());
        CHECK(unit.has(Category::Type, "minimum no larger than it"));
    }
    SECTION("a zero fixed list size") {
        Unit unit{R"(
module t
export fn recent(values: list<f64, 0>) -> f64 => 1.0
)"};
        CHECK_FALSE(unit.emit());
        CHECK(unit.has(Category::Type, "a fixed list size must be a positive i64 literal"));
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

TEST_CASE("emit-cpp lowers structural, generic, duration, and logger forms", "[codegen]") {
    Unit       unit{R"(
module t
use hgraph.std::{mean}

export struct Quote {
    bid: f64
    venue: str = null
}

export struct Box<T> {
    value: T
}

export fn same<U>(a: U, b: U) -> U => a

export fn recent(window: rolling<f64, 5m>) -> f64 => mean(window)

export fn logged(value: f64) -> f64 {
    inject logger
    when modified(value) && valid(value) {
        logger.info("value")
        return value
    }
}
)"};
    const auto emitted = unit.emit();
    REQUIRE(emitted);

    CHECK(contains(emitted->header, "hgraph::NominalBundle<\"t\", \"Quote\", "
                                    "false, hgraph::BundleParents<>"));
    CHECK(contains(emitted->header, "template <typename T>\n    struct Box"));
    CHECK(contains(emitted->header, "hgraph::ScalarVar<\"U\">"));
    CHECK(contains(emitted->header, "hgraph::TSWDuration<hgraph::Float, 300000000, 300000000>"));
    CHECK(contains(emitted->header, "hgraph::LoggerView logger"));
    CHECK(contains(emitted->header, "logger.log(2, hgraph::Str{\"value\"});"));
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
    CHECK(contains(emitted->header, "using w_ = hgraph::Operator<\"t.new.w\""));
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
    CHECK(contains(emitted->source, "hgraph::Float{2.0})"));
    CHECK(contains(emitted->source, "return x_1;"));
}
