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
    bool kernel_has(std::string_view name) {
        static constexpr std::string_view names[] = {"if_then_else", "add_",         "mul_",
                                                     "mean",         "map_",         "debug_print",
                                                     "null_sink",    "rolling_mean", "hgraph.analytics.rolling_mean",
                                                     "const",        "all_"};
        return std::find(std::begin(names), std::end(names), name) != std::end(names);
    }

    struct Unit
    {
        SourceFile             file;
        DiagnosticSink         diagnostics;
        ast::Module            module;
        ResolvedModule         resolved;
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

        Unit(std::string text, const ModuleCatalog &catalog, std::string path = "unit.hgl")
            : file{std::move(path), std::move(text)}, module{parse(file, diagnostics)} {
            resolved = resolve(file, module, catalog, kernel_has, diagnostics);
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

        [[nodiscard]] std::optional<EmittedModule> emit(EmitOptions options = {}) {
            INFO(diagnostics.render(file));
            if (diagnostics.has_errors()) { return std::nullopt; }
            if (options.header_name.empty()) { options.header_name = "unit.h"; }
            if (options.tool_version.empty()) { options.tool_version = "test"; }
            return emit_cpp(file, graph, options, diagnostics);
        }

        [[nodiscard]] bool has(Category category, std::string_view fragment) const {
            const bool found =
                std::any_of(diagnostics.diagnostics().begin(), diagnostics.diagnostics().end(), [&](const Diagnostic &d) {
                    return d.category == category && d.message.find(fragment) != std::string::npos;
                });
            if (!found) { WARN(diagnostics.render(file)); }
            return found;
        }
    };

    std::string read_file(const std::string &path) {
        std::ifstream in{path, std::ios::binary};
        REQUIRE(in);
        return std::string{std::istreambuf_iterator<char>{in}, std::istreambuf_iterator<char>{}};
    }

    bool contains(const std::string &text, std::string_view fragment) { return text.find(fragment) != std::string::npos; }

    std::size_t occurrences(std::string_view text, std::string_view fragment) {
        std::size_t result = 0;
        for (std::size_t offset = 0; (offset = text.find(fragment, offset)) != std::string_view::npos; offset += fragment.size()) {
            ++result;
        }
        return result;
    }

    ModuleCatalog native_catalog(std::string header = "acme/stats.h") {
        ModuleCatalog    catalog;
        ImportableModule module;
        module.identity = "acme.stats";
        module.functions.push_back(ImportedFunction{
            .module_identity        = module.identity,
            .name                   = "blend",
            .identity               = "acme.stats::blend",
            .cpp_symbol             = "acme::stats::blend",
            .parameters             = {{"value", hgl::semantics::ImportedScalarType::F64, false},
                                       {"window", hgl::semantics::ImportedScalarType::I64, true}},
            .result                 = hgl::semantics::ImportedScalarType::F64,
            .phases                 = {NativeCallPhase::Evaluation},
            .public_headers         = {std::move(header)},
            .cmake_packages         = {"acme_stats"},
            .imported_targets       = {"acme::stats"},
            .runtime_images         = {"libacme_stats.so"},
            .descriptor_fingerprint = "sha256:test",
        });
        REQUIRE_FALSE(catalog.add(std::move(module)));
        return catalog;
    }
}  // namespace

TEST_CASE("emit-cpp names the pair after the module and exports its functions", "[codegen]") {
    Unit        unit{read_file(std::string{HGL_CODEGEN_DIR} + "/parity.hgl"), "parity.hgl"};
    EmitOptions options;
    options.header_name = "parity.h";
    const auto emitted  = unit.emit(options);
    REQUIRE(emitted);

    CHECK(emitted->namespace_name == "hgl::codegen::parity");
    CHECK(emitted->module_name == "hgl.codegen.parity");
    CHECK(emitted->exports ==
          std::vector<std::string>{"plus", "scaled_sum", "above", "maybe_double", "offset_by", "choose", "choose_embedded"});
    CHECK(contains(emitted->descriptor, "\"format\": \"hgl.module\""));
    CHECK(contains(emitted->descriptor, "\"identity\": \"hgl.codegen.parity\""));
    CHECK(contains(emitted->descriptor, "\"signature\": {"));
    CHECK(contains(emitted->descriptor, "\"schema\": {"));
    CHECK(contains(emitted->descriptor, "\"kind\": \"scalar\""));
    CHECK(contains(emitted->descriptor, "\"public_headers\": [\n      \"parity.h\""));
    CHECK(contains(emitted->descriptor, "\"symbol\": \"hgl::codegen::parity::register_operators\""));

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
    CHECK(contains(emitted->source, "hgraph::Port<hgraph::TS<hgraph::Float>> plus::compose([[maybe_unused]] hgraph::Wiring &w, "
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
    Unit       again{read_file(std::string{HGL_CODEGEN_DIR} + "/parity.hgl"), "parity.hgl"};
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

TEST_CASE("emit-cpp writes readable direct native scalar calls and dependency closure", "[codegen][native]") {
    const ModuleCatalog catalog = native_catalog();
    Unit                unit{R"(
module checks.native
use acme.stats as stats

export fn smooth(value: f64) -> f64 {
    when modified(value) && valid(value) { return stats::blend(value, 3) }
}
)",
                             catalog, "native.hgl"};
    const auto          emitted = unit.emit();
    REQUIRE(emitted);
    CHECK(contains(emitted->header, "#include <acme/stats.h>"));
    CHECK(contains(emitted->header, "hgl_output.set(acme::stats::blend(value.value()"));
    CHECK_FALSE(contains(emitted->header, "struct blend"));
    CHECK(contains(emitted->descriptor, "\"acme/stats.h\""));
    CHECK(contains(emitted->descriptor, "\"acme_stats\""));
    CHECK(contains(emitted->descriptor, "\"acme::stats\""));
    CHECK(contains(emitted->descriptor, "\"libacme_stats.so\""));
}

TEST_CASE("emit-cpp writes source native functions as plain direct C++", "[codegen][native]") {
    Unit       unit{R"(
module checks.inline_native

cpp include <cstdint>
cpp include "native/helpers.h"
cpp include <cstdint>

native fn increment(value: f64) -> f64 {
    cpp(hgraph::Float value) {
        return value + 1.0;
    }
}

export fn incremented(value: f64) -> f64 {
    when {
        return increment(value)
    }
}
)"};
    const auto emitted = unit.emit();
    INFO(unit.diagnostics.render(unit.file));
    REQUIRE(emitted);
    CHECK(contains(emitted->header, "#include <cstdint>"));
    CHECK(contains(emitted->header, "#include \"native/helpers.h\""));
    CHECK(emitted->header.find("#include <cstdint>") == emitted->header.rfind("#include <cstdint>"));
    CHECK(emitted->header.find("#include <cstdint>") < emitted->header.find("#include \"native/helpers.h\""));
    CHECK_FALSE(contains(emitted->source, "native/helpers.h"));
    CHECK(contains(emitted->header, "namespace native"));
    CHECK(contains(emitted->header, "hgraph::Float increment(hgraph::Float value) noexcept;"));
    CHECK(contains(emitted->source, "hgraph::Float increment(hgraph::Float value) noexcept"));
    CHECK(contains(emitted->source, "return value + 1.0;"));
    CHECK(contains(emitted->header, "checks::inline_native::native::increment(value.value())"));
    CHECK_FALSE(contains(emitted->header, "struct increment\n"));
    CHECK(contains(emitted->descriptor, "\"identity\": \"checks.inline_native::increment\""));
    CHECK(contains(emitted->descriptor, "\"cpp_symbol\": \"checks::inline_native::native::increment\""));
}

TEST_CASE("emit-cpp preserves all parameter-pack shapes in public operator contracts", "[codegen][parameter-pack]") {
    Unit       unit{R"(
module packs
operator homogeneous<T>(values: ...T) -> T
operator positional<...Ts>(values: ...Ts) -> i64
operator keyword<...Fields>(values: ...{Fields}) -> i64
)"};
    const auto emitted = unit.emit();
    REQUIRE(emitted);
    CHECK(contains(emitted->header, "hgraph::VarIn<\"values\", hgraph::TsVar<\"T\">>, hgraph::Out<hgraph::TsVar<\"T\">>"));
    CHECK(contains(emitted->header, "hgraph::VarIn<\"values\", hgraph::TsVar<\"Ts\">>"));
    CHECK(contains(emitted->header, "hgraph::VarKwIn<\"values\">"));
}

TEST_CASE("emit-cpp forwards a homogeneous pack without exposing synthetic fields", "[codegen][parameter-pack]") {
    Unit       unit{R"(
module packs
use hgraph.std::{all_}
fn all_inputs(inputs: ...bool) -> bool => all_(inputs)
export fn all_values(values: ...bool) -> bool => all_inputs(values)
)"};
    const auto emitted = unit.emit();
    INFO(unit.diagnostics.render(unit.file));
    REQUIRE(emitted);
    CHECK(contains(emitted->header, "hgraph::VarIn<\"values\", hgraph::TS<hgraph::Bool>>"));
    CHECK(contains(emitted->source, "hgraph::VarIn<\"values\", hgraph::TS<hgraph::Bool>> values"));
    CHECK(contains(emitted->source, "hgraph::VarIn<\"inputs\", hgraph::TS<hgraph::Bool>>{values.ports}"));
    CHECK_FALSE(contains(emitted->source, "wire<all_inputs>(w, values)"));
    CHECK_FALSE(contains(emitted->header, "_0"));
    CHECK_FALSE(contains(emitted->source, "_0"));
}

TEST_CASE("emit-cpp traverses heterogeneous packs through tuple and bundle views", "[codegen][parameter-pack]") {
    Unit       unit{R"(
module packs
use hgraph.std::{null_sink}

export fn positional<...Ts>(values: ...Ts) {
    for value in elements(values) {
        null_sink(value)
    }
    for index, value in items(values) {
        null_sink(value)
    }
}

export fn keyword<...Fields>(values: ...{Fields}) {
    for name in keys(values) {
        let preserved = name
    }
    for name, value in items(values) {
        null_sink(value)
    }
}
)"};
    const auto emitted = unit.emit();
    INFO(unit.diagnostics.render(unit.file));
    REQUIRE(emitted);
    CHECK(contains(emitted->source, "for (std::size_t hgl_pack_first_"));
    CHECK(contains(emitted->source, "hgraph::Port<void>{w, values[hgl_pack_first_"));
    CHECK(contains(emitted->source, "for (const auto &[hgl_pack_first_"));
    CHECK(contains(emitted->source, "hgraph::Str{hgl_pack_first_"));
    CHECK_FALSE(contains(emitted->source, "_0"));
}

TEST_CASE("source native candidates have distinct plain C++ symbols", "[codegen][native][generics]") {
    Unit       unit{R"(
module checks.native_candidates

native fn len<T, const size: i64>(value: list<T, size>) -> i64 {
    cpp(const hgraph::TSLInputView &value) {
        return static_cast<hgraph::Int>(value.size());
    }
}

native fn len<T>(value: list<T, unbounded>) -> i64 {
    cpp(const hgraph::TSLInputView &value) {
        return static_cast<hgraph::Int>(value.size());
    }
}

native fn len<T, const max_size: i64, const min_size: i64>(
    value: rolling<T, max_size, min_size>
) -> i64 {
    cpp(const hgraph::TSWInputView &value) {
        return static_cast<hgraph::Int>(value.size());
    }
}

export fn fixed(value: list<i64, 2>) -> i64 {
    when { return len(value) }
}

export fn dynamic(value: list<i64, unbounded>) -> i64 {
    when { return len(value) }
}

export fn window(value: rolling<i64, 3, 1>) -> i64 {
    when { return len(value) }
}
)"};
    const auto emitted = unit.emit();
    INFO(unit.diagnostics.render(unit.file));
    REQUIRE(emitted);
    CHECK(contains(emitted->source, "hgraph::Int len(const hgraph::TSLInputView &value) noexcept"));
    CHECK(contains(emitted->source, "hgraph::Int len__candidate_2(const hgraph::TSLInputView &value) noexcept"));
    CHECK(contains(emitted->source, "hgraph::Int len__candidate_3(const hgraph::TSWInputView &value) noexcept"));
    CHECK(contains(emitted->header, "checks::native_candidates::native::len(value)"));
    CHECK(contains(emitted->header, "checks::native_candidates::native::len__candidate_2(value)"));
    CHECK(contains(emitted->header, "checks::native_candidates::native::len__candidate_3(value)"));
    CHECK(contains(emitted->descriptor, "\"cpp_symbol\": \"checks::native_candidates::native::len\""));
    CHECK(contains(emitted->descriptor, "\"cpp_symbol\": \"checks::native_candidates::native::len__candidate_2\""));
}

TEST_CASE("source native signal parameters receive an erased input view", "[codegen][native][signal]") {
    Unit       unit{R"(
module checks.native_signal

native fn endpoint_valid(value: signal) -> bool {
    cpp(const hgraph::TSInputView &value) {
        return value.valid();
    }
}

export fn valid_float(value: f64) -> bool {
    when { return endpoint_valid(value) }
}
)"};
    const auto emitted = unit.emit();
    INFO(unit.diagnostics.render(unit.file));
    REQUIRE(emitted);
    CHECK(contains(emitted->header, "hgraph::Bool endpoint_valid(const hgraph::TSInputView &value) noexcept;"));
    CHECK(contains(emitted->header, "checks::native_signal::native::endpoint_valid(value)"));
    CHECK(contains(emitted->descriptor, "\"kind\": \"signal\""));
    CHECK(contains(emitted->descriptor, "\"access\": \"input-view\""));
}

TEST_CASE("emit-cpp fails closed when a source native signature is outside the descriptor ABI", "[codegen][native]") {
    Unit unit{R"(
module checks.invalid_native

native fn inspect<T>(value: T) -> i64 {
    cpp(hgraph::Int value) { return value; }
}
)"};
    CHECK_FALSE(unit.emit());
    CHECK(unit.has(Category::Backend,
                   "generated module descriptor is invalid at '$.native.declarations[0].signature.parameters[0].type'"));
}

TEST_CASE("emit-cpp rejects unsafe native header metadata even in constructed IR", "[codegen][native]") {
    const ModuleCatalog catalog = native_catalog("acme/stats.h>\n#include <evil.h");
    Unit                unit{R"(
module checks.native_header
use acme.stats::{blend}
fn smooth(value: f64) -> f64 {
    when modified(value) && valid(value) { return blend(value, 3) }
}
)",
                             catalog};
    CHECK_FALSE(unit.emit());
    CHECK(unit.has(Category::Backend, "names an unsafe public header"));
}

TEST_CASE("emit-cpp rejects invalid source include metadata even in constructed IR", "[codegen][native][include]") {
    Unit unit{"module checks.native_include\nexport fn value(x: f64) -> f64 => x\n"};
    unit.graph.cpp_includes = {"<cstdint>\n#include <evil.h>"};
    CHECK_FALSE(unit.emit());
    CHECK(unit.has(Category::Backend, "module names an invalid C++ include header"));
}

TEST_CASE("emit-cpp rejects unsafe native symbols even in constructed IR", "[codegen][native]") {
    const ModuleCatalog catalog = native_catalog();
    Unit                unit{R"(
module checks.native_symbol
use acme.stats::{blend}
fn smooth(value: f64) -> f64 {
    when modified(value) && valid(value) { return blend(value, 3) }
}
)",
                             catalog};
    REQUIRE(unit.graph.native_functions.size() == 1U);
    unit.graph.native_functions.front().cpp_symbol = "acme::stats::blend(); injected";
    CHECK_FALSE(unit.emit());
    CHECK(unit.has(Category::Backend, "has an invalid exact C++ symbol"));
}

TEST_CASE("emit-cpp validates hgraph IR declaration order", "[codegen][hgraph-ir]") {
    SECTION("an invalid callable handle") {
        Unit unit{"module t\nexport fn value(x: f64) -> f64 => x\n"};
        REQUIRE(unit.graph.completion == hgl::hgraph_ir::Completion::Bodies);
        unit.graph.callables.clear();

        CHECK_FALSE(unit.emit());
        CHECK(unit.has(Category::Backend, "source order contains an invalid callable ID"));
    }
    SECTION("an omitted callable") {
        Unit unit{"module t\nexport fn value(x: f64) -> f64 => x\n"};
        REQUIRE(unit.graph.completion == hgl::hgraph_ir::Completion::Bodies);
        unit.graph.source_order.clear();

        CHECK_FALSE(unit.emit());
        CHECK(unit.has(Category::Backend, "source order omits a callable declaration"));
    }
    SECTION("a duplicate callable") {
        Unit unit{"module t\nexport fn value(x: f64) -> f64 => x\n"};
        REQUIRE(unit.graph.completion == hgl::hgraph_ir::Completion::Bodies);
        REQUIRE(unit.graph.source_order.size() == 1);
        unit.graph.source_order.push_back(unit.graph.source_order.front());

        CHECK_FALSE(unit.emit());
        CHECK(unit.has(Category::Backend, "source order contains a duplicate callable handle"));
    }
    SECTION("an invalid struct handle") {
        Unit unit{"module t\nexport struct Value { amount: f64 }\n"};
        REQUIRE(unit.graph.completion == hgl::hgraph_ir::Completion::Bodies);
        unit.graph.structures.clear();

        CHECK_FALSE(unit.emit());
        CHECK(unit.has(Category::Backend, "source order contains an invalid struct ID"));
    }
    SECTION("an imported operator handle") {
        Unit unit{"module t\nuse hgraph.std::{mean}\nexport fn value(x: f64) -> f64 => mean(x)\n"};
        REQUIRE(unit.graph.completion == hgl::hgraph_ir::Completion::Bodies);
        const auto imported =
            std::find_if(unit.graph.operators.begin(), unit.graph.operators.end(), [](const auto &item) { return item.imported; });
        REQUIRE(imported != unit.graph.operators.end());
        const auto index = static_cast<std::uint32_t>(std::distance(unit.graph.operators.begin(), imported));
        unit.graph.source_order.emplace_back(hgl::hgraph_ir::OperatorId{index});

        CHECK_FALSE(unit.emit());
        CHECK(unit.has(Category::Backend, "source order contains an imported operator handle"));
    }
}

TEST_CASE("emit-cpp validates hgraph IR body handles", "[codegen][hgraph-ir]") {
    SECTION("a missing planned body statement") {
        Unit unit{"module t\nexport fn value(x: f64) -> f64 {\n    let y = 1.0\n    x + y\n}\n"};
        REQUIRE(unit.graph.completion == hgl::hgraph_ir::Completion::Bodies);
        unit.graph.statements.clear();

        CHECK_FALSE(unit.emit());
        CHECK(unit.has(Category::Backend, "hgraph IR contains an invalid body statement ID"));
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

    auto &structure            = unit.graph.structures.front();
    structure.identity         = "planned.module.Record";
    structure.abstract         = false;
    structure.generics[0].name = "Item";
    structure.fields[0].name   = "amount";
    structure.fields[1].name   = "count";
    const hgl::hgraph_ir::TypeId integer{static_cast<std::uint32_t>(unit.graph.types.size())};
    unit.graph.types.push_back(
        {.kind = hgl::ir::hir::TypeKind::Scalar, .scalar = hgl::ir::hir::ScalarType::I64, .range = structure.fields[1].range});
    structure.fields[1].type = integer;
    unit.graph.path          = "planned.module";

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

TEST_CASE("emit-cpp constructs omitted struct fields from hgraph IR defaults", "[codegen][hgraph-ir][structs][defaults]") {
    Unit unit{R"(
module planned_struct_defaults

struct Record {
    amount: f64
    label: str = "source"
}

export fn make_record(amount: f64) -> Record => Record(amount: amount)
)"};
    REQUIRE_FALSE(unit.diagnostics.has_errors());
    REQUIRE(unit.graph.completion == hgl::hgraph_ir::Completion::Bodies);
    REQUIRE(unit.graph.structures.size() == 1);
    REQUIRE(unit.graph.structures.front().fields.size() == 2);

    hgl::hgraph_ir::StructField &label = unit.graph.structures.front().fields[1];
    REQUIRE(label.default_value.valid());
    REQUIRE(label.default_value.value < unit.graph.const_exprs.size());

    SECTION("the planned value is authoritative") {
        unit.graph.const_exprs[label.default_value.value].literal = std::string{"planned"};

        const auto emitted = unit.emit();
        REQUIRE(emitted);
        CHECK(contains(emitted->source, "hgraph::Str{\"planned\"}"));
        CHECK_FALSE(contains(emitted->source, "hgraph::Str{\"source\"}"));
    }

    SECTION("the planned presence is authoritative") {
        label.default_value = {};

        CHECK_FALSE(unit.emit());
        CHECK(unit.has(Category::Backend, "struct 'Record' needs field 'label'"));
    }
}

TEST_CASE("emit-cpp constructs nested hgraph IR struct defaults", "[codegen][hgraph-ir][structs][defaults]") {
    Unit unit{R"(
module nested_struct_defaults

struct Inner {
    amount: f64
    label: str = "inner"
}

struct Outer {
    inner: Inner = Inner(amount: 1.0)
}

export fn make_outer() -> Outer => Outer()
)"};
    REQUIRE_FALSE(unit.diagnostics.has_errors());

    const auto emitted = unit.emit();
    REQUIRE(emitted);
    CHECK(contains(emitted->source, "hgraph::Float{1.0}"));
    CHECK(contains(emitted->source, "hgraph::Str{\"inner\"}"));
    CHECK(contains(emitted->source, "hgraph::stdlib::to_tsb<typename Inner::time_series>"));
    CHECK(contains(emitted->source, "hgraph::stdlib::to_tsb<typename Outer::time_series>"));
}

TEST_CASE("emit-cpp constructs atomic hgraph IR struct defaults", "[codegen][hgraph-ir][structs][defaults]") {
    Unit unit{R"(
module atomic_struct_defaults

struct Inner {
    amount: f64
}

struct Outer {
    inner: atomic<Inner> = Inner(amount: 1.0)
}

export fn make_outer() -> Outer => Outer()
)"};
    REQUIRE_FALSE(unit.diagnostics.has_errors());

    const auto emitted = unit.emit();
    REQUIRE(emitted);
    CHECK(contains(emitted->source, "hgraph::stdlib::to_tsb<typename Inner::time_series>"));
    CHECK(contains(emitted->source, "hgraph::stdlib::combine_cs, hgraph::TS<typename Inner::value_type>"));
}

TEST_CASE("emit-cpp projects nested hgraph IR struct defaults", "[codegen][hgraph-ir][structs][defaults]") {
    Unit unit{R"(
module projected_struct_defaults

struct Inner {
    amount: f64
}

struct Outer {
    amount: f64 = Inner(amount: 1.0).amount
}

export fn make_outer() -> Outer => Outer()
)"};
    REQUIRE_FALSE(unit.diagnostics.has_errors());

    const auto emitted = unit.emit();
    REQUIRE(emitted);
    CHECK(contains(emitted->source, "hgraph::wire<hgraph::stdlib::getattr_>"));
    CHECK(contains(emitted->source, "hgraph::Str{\"amount\"}"));
}

TEST_CASE("emit-cpp preserves outer bindings in nested hgraph IR struct defaults", "[codegen][hgraph-ir][structs][defaults]") {
    Unit unit{R"(
module generic_struct_defaults

struct Leaf<U> {
    marker: i64 = 1
}

struct Middle<U> {
    leaf: Leaf<U>
}

struct Outer<U> {
    middle: Middle<U> = Middle<U>(leaf: Leaf<U>())
}

export fn make_outer() -> Outer<f64> => Outer<f64>()
)"};
    REQUIRE_FALSE(unit.diagnostics.has_errors());

    const auto emitted = unit.emit();
    REQUIRE(emitted);
    CHECK(contains(emitted->source, "typename Leaf<hgraph::Float>::time_series"));
    CHECK(contains(emitted->source, "typename Middle<hgraph::Float>::time_series"));
    CHECK_FALSE(contains(emitted->source, "ScalarVar<\"U\">"));
}

TEST_CASE("emit-cpp plans body-local bindings from hgraph IR", "[codegen][hgraph-ir][locals]") {
    Unit unit{R"(
module planned_locals

export fn adjusted(value: f64) -> f64 {
    let amount: f64 = 1
    value + amount
}
)"};
    REQUIRE_FALSE(unit.diagnostics.has_errors());
    REQUIRE(unit.graph.completion == hgl::hgraph_ir::Completion::Bodies);

    const auto statement = std::find_if(unit.graph.statements.begin(), unit.graph.statements.end(), [](const auto &candidate) {
        return std::holds_alternative<hgl::hgraph_ir::LocalBinding>(candidate.node);
    });
    REQUIRE(statement != unit.graph.statements.end());
    auto &local = std::get<hgl::hgraph_ir::LocalBinding>(statement->node);
    REQUIRE(local.binding.valid());
    REQUIRE(local.binding.value < unit.graph.bindings.size());

    const hgl::hgraph_ir::TypeId integer{static_cast<std::uint32_t>(unit.graph.types.size())};
    unit.graph.types.push_back(
        {.kind = hgl::ir::hir::TypeKind::Scalar, .scalar = hgl::ir::hir::ScalarType::I64, .range = statement->range});
    local.type    = integer;
    auto &binding = unit.graph.bindings[local.binding.value];
    binding.name  = "planned_amount";
    binding.kind  = hgl::hgraph_ir::BindingKind::LocalVar;
    binding.type  = integer;

    const auto emitted = unit.emit();
    REQUIRE(emitted);
    CHECK(contains(emitted->source, "auto planned_amount = hgraph::Int{1};"));
    CHECK_FALSE(contains(emitted->source, "const auto amount"));
    CHECK_FALSE(contains(emitted->source, "static_cast<hgraph::Float>(hgraph::Int{1})"));
}

TEST_CASE("emit-cpp validates local assignment mutability from hgraph IR", "[codegen][hgraph-ir][locals]") {
    Unit unit{R"(
module planned_local_assignment

export fn adjusted(value: f64) -> f64 {
    var amount = 1.0
    amount = 2.0
    value + amount
}
)"};
    REQUIRE_FALSE(unit.diagnostics.has_errors());

    const auto statement = std::find_if(unit.graph.statements.begin(), unit.graph.statements.end(), [](const auto &candidate) {
        return std::holds_alternative<hgl::hgraph_ir::LocalBinding>(candidate.node);
    });
    REQUIRE(statement != unit.graph.statements.end());
    const auto &local = std::get<hgl::hgraph_ir::LocalBinding>(statement->node);
    REQUIRE(local.binding.valid());
    REQUIRE(local.binding.value < unit.graph.bindings.size());

    SECTION("a planned let rejects assignment") {
        unit.graph.bindings[local.binding.value].kind = hgl::hgraph_ir::BindingKind::LocalLet;

        CHECK_FALSE(unit.emit());
        CHECK(unit.has(Category::Type, "'amount' is not a 'var'"));
    }

    SECTION("a planned var ignores stale syntax mutability") {
        const auto declaration = std::find_if(unit.module.stmts.begin(), unit.module.stmts.end(), [](auto &candidate) {
            return std::holds_alternative<ast::LocalDecl>(candidate.node);
        });
        REQUIRE(declaration != unit.module.stmts.end());
        std::get<ast::LocalDecl>(declaration->node).mutable_ = false;

        const auto emitted = unit.emit();
        REQUIRE(emitted);
        CHECK(contains(emitted->source, "auto amount = hgraph::Float{1.0};"));
        CHECK(contains(emitted->source, "amount = hgraph::Float{2.0};"));
    }
}

TEST_CASE("emit-cpp declares a typed var before conditional assignment", "[codegen][locals][control-flow]") {
    Unit unit{R"(
module planned_conditional_assignment

export fn selected(const condition: bool, value: i64) -> i64 {
    var result: i64
    if condition {
        result = value
    } else {
        result = value + 1
    }
    result
}
)"};
    REQUIRE_FALSE(unit.diagnostics.has_errors());

    const auto emitted = unit.emit();
    INFO(unit.diagnostics.render(unit.file));
    REQUIRE(emitted);
    CHECK(contains(emitted->source, "hgraph::Port<hgraph::TS<hgraph::Int>> result;"));
    CHECK(contains(emitted->source, "if (condition.value())"));
    CHECK(contains(emitted->source, "result = value;"));
    CHECK(contains(emitted->source, "return result;"));
}

TEST_CASE("emit-cpp lowers a temporal if to readable switch branch graphs", "[codegen][control-flow][conditional]") {
    Unit unit{R"(
module planned_temporal_conditional

export fn selected(condition: bool, x: i64, y: i64) -> i64 {
    if condition {
        let adjusted = x + 1
        adjusted
    } else {
        y - 1
    }
}
)"};
    REQUIRE_FALSE(unit.diagnostics.has_errors());

    const auto emitted = unit.emit();
    INFO(unit.diagnostics.render(unit.file));
    REQUIRE(emitted);
    CHECK(contains(emitted->source, "struct hgl_selected_if_1_then"));
    CHECK(contains(emitted->source, "struct hgl_selected_if_1_else"));
    CHECK(contains(emitted->source, "static hgraph::Port<hgraph::TS<hgraph::Int>> compose("));
    CHECK(contains(emitted->source, "hgraph::stdlib::switch_cases("));
    CHECK(contains(emitted->source, "hgraph::fn<hgl_selected_if_1_then>()"));
    CHECK_FALSE(contains(emitted->source, "if_then_else"));
}

TEST_CASE("emit-cpp lowers an outputless temporal if through switch_sink_", "[codegen][control-flow][conditional]") {
    Unit unit{R"(
module planned_temporal_sink
use hgraph.std::{debug_print}

export fn observe(enabled: bool, value: f64) {
    if enabled {
        debug_print("enabled", value)
    }
    debug_print("always", value)
}
)"};
    INFO(unit.diagnostics.render(unit.file));
    REQUIRE_FALSE(unit.diagnostics.has_errors());

    const auto emitted = unit.emit();
    REQUIRE(emitted);
    CHECK(contains(emitted->source, "struct hgl_observe_if_1_then"));
    CHECK(contains(emitted->source, "struct hgl_observe_if_1_else"));
    CHECK(occurrences(emitted->source, "static void compose(") == 2U);
    CHECK(contains(emitted->source, "hgraph::wire<hgraph::stdlib::switch_sink_>"));
    CHECK(contains(emitted->source, "hgraph::fn<hgl_observe_if_1_else>()"));
    CHECK(occurrences(emitted->source, "hgraph::wire<hgraph::stdlib::debug_print>") == 2U);
}

TEST_CASE("emit-cpp places a void callable suffix in the falling temporal branch",
          "[codegen][control-flow][conditional][continuation]") {
    Unit unit{R"(
module planned_temporal_sink_return
use hgraph.std::{null_sink}

export fn observe(enabled: bool, value: f64) {
    if enabled {
        return
    }
    null_sink(value)
}
)"};
    INFO(unit.diagnostics.render(unit.file));
    REQUIRE_FALSE(unit.diagnostics.has_errors());

    const auto emitted = unit.emit();
    REQUIRE(emitted);
    CHECK(contains(emitted->source, "struct hgl_observe_if_1_then"));
    CHECK(contains(emitted->source, "struct hgl_observe_if_1_else"));
    CHECK(contains(emitted->source, "hgraph::wire<hgraph::stdlib::switch_sink_>"));
    CHECK(occurrences(emitted->source, "hgraph::wire<hgraph::stdlib::null_sink>") == 1U);
}

TEST_CASE("emit-cpp materializes a continuation-assigned variable in its terminal branch",
          "[codegen][control-flow][conditional][continuation]") {
    Unit unit{R"(
module planned_temporal_assignment_return

export fn choose(condition: bool, x: i64, y: i64) -> i64 {
    var result: i64
    if condition {
        return x + 1
    }
    result = y - 1
    return result * 2
}
)"};
    INFO(unit.diagnostics.render(unit.file));
    REQUIRE_FALSE(unit.diagnostics.has_errors());

    const auto emitted = unit.emit();
    REQUIRE(emitted);
    CHECK(contains(emitted->source, "struct hgl_choose_if_1_else"));
    CHECK(contains(emitted->source, "hgraph::Port<hgraph::TS<hgraph::Int>> result;"));
    CHECK(contains(emitted->source, "result = hgraph::wire<hgraph::stdlib::sub_>"));
}

TEST_CASE("emit-cpp nests temporal continuation helpers inside their selected paths",
          "[codegen][control-flow][conditional][continuation]") {
    Unit unit{R"(
module planned_nested_temporal_return

export fn choose(outer: bool, inner: bool, x: i64, y: i64, z: i64) -> i64 {
    if outer {
        if !inner {
            return x + 1
        }
        return y + 2
    }
    return z + 3
}
)"};
    INFO(unit.diagnostics.render(unit.file));
    REQUIRE_FALSE(unit.diagnostics.has_errors());

    const auto emitted = unit.emit();
    INFO(unit.diagnostics.render(unit.file));
    REQUIRE(emitted);
    CHECK(contains(emitted->source, "struct hgl_choose_if_1_then"));
    CHECK(contains(emitted->source, "struct hgl_choose_if_2_then"));
    CHECK(occurrences(emitted->source, "hgraph::stdlib::switch_cases(") == 2U);
    CHECK(occurrences(emitted->source, "hgraph::wire<hgraph::stdlib::not_>") == 1U);
}

TEST_CASE("emit-cpp plans a returning temporal conditional at the callable block tail",
          "[codegen][control-flow][conditional][continuation]") {
    Unit unit{R"(
module planned_temporal_returning_tail

export fn choose(condition: bool, x: i64, y: i64) -> i64 {
    if condition {
        return x + 1
    } else {
        y - 1
    }
}
)"};
    INFO(unit.diagnostics.render(unit.file));
    REQUIRE_FALSE(unit.diagnostics.has_errors());

    const auto emitted = unit.emit();
    INFO(unit.diagnostics.render(unit.file));
    REQUIRE(emitted);
    CHECK(contains(emitted->source, "struct hgl_choose_if_1_then"));
    CHECK(contains(emitted->source, "struct hgl_choose_if_1_else"));
    CHECK(contains(emitted->source, "hgraph::stdlib::switch_cases("));
}

TEST_CASE("emit-cpp rejects temporal else-if before dropping a branch", "[codegen][control-flow][conditional]") {
    Unit unit{R"(
module planned_temporal_else_if
use hgraph.std::{debug_print}

export fn observe(first: bool, second: bool, value: f64) {
    if first {
        debug_print("first", value)
    } else if second {
        debug_print("second", value)
    }
}
)"};
    CHECK_FALSE(unit.emit());
    CHECK(unit.has(Category::Backend, "temporal 'else if' is not supported"));
}

TEST_CASE("emit-cpp remaps one temporal conditional assignment", "[codegen][control-flow][conditional]") {
    Unit unit{R"(
module planned_temporal_assignment

export fn adjusted(condition: bool, x: i64, y: i64) -> i64 {
    var result: i64
    if condition {
        result = x + 1
    } else {
        result = y - 1
    }
    return result * 2
}
)"};
    INFO(unit.diagnostics.render(unit.file));
    REQUIRE_FALSE(unit.diagnostics.has_errors());

    const auto emitted = unit.emit();
    REQUIRE(emitted);
    CHECK(contains(emitted->source, "struct hgl_adjusted_if_1_then"));
    CHECK(contains(emitted->source, "struct hgl_adjusted_if_1_else"));
    CHECK(occurrences(emitted->source, "hgraph::Port<hgraph::TS<hgraph::Int>> result;") == 3U);
    CHECK(occurrences(emitted->source, "return result;") == 2U);
    CHECK(contains(emitted->source, "result = hgraph::wire<hgraph::stdlib::switch_>"));
    CHECK(contains(emitted->source, "return hgraph::wire<hgraph::stdlib::mul_>"));
}

TEST_CASE("emit-cpp preserves a discarded branch tail before returning an escaping result",
          "[codegen][control-flow][conditional]") {
    Unit unit{R"(
module planned_temporal_assignment_tail
use hgraph.std::{null_sink}

export fn adjusted(condition: bool, x: i64, y: i64) -> i64 {
    var result: i64
    if condition {
        result = x + 1
        null_sink(x)
    } else {
        result = y - 1
        null_sink(y)
    }
    result
}
)"};
    INFO(unit.diagnostics.render(unit.file));
    REQUIRE_FALSE(unit.diagnostics.has_errors());

    const auto emitted = unit.emit();
    REQUIRE(emitted);
    CHECK(occurrences(emitted->source, "hgraph::wire<hgraph::stdlib::null_sink>") == 2U);
    const std::size_t first_sink    = emitted->source.find("hgraph::wire<hgraph::stdlib::null_sink>");
    const std::size_t first_return  = emitted->source.find("return result;", first_sink);
    const std::size_t second_sink   = emitted->source.find("hgraph::wire<hgraph::stdlib::null_sink>", first_return);
    const std::size_t second_return = emitted->source.find("return result;", second_sink);
    CHECK(first_sink < first_return);
    CHECK(first_return < second_sink);
    CHECK(second_sink < second_return);
}

TEST_CASE("emit-cpp remaps several temporal conditional assignments through a bundle", "[codegen][control-flow][conditional]") {
    Unit unit{R"(
module planned_multiple_temporal_assignments

export fn adjusted(condition: bool, x: i64, y: i64) -> i64 {
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
)"};
    INFO(unit.diagnostics.render(unit.file));
    REQUIRE_FALSE(unit.diagnostics.has_errors());

    const auto emitted = unit.emit();
    REQUIRE(emitted);
    CHECK(contains(emitted->source, "hgraph::UnNamedTSB<hgraph::Field<\"result\", hgraph::TS<hgraph::Int>>, "
                                    "hgraph::Field<\"offset\", hgraph::TS<hgraph::Int>>>"));
    CHECK(occurrences(emitted->source, "hgraph::stdlib::to_tsb<") == 2U);
    CHECK(contains(emitted->source, "auto hgl_adjusted_if_1_results = hgraph::wire<hgraph::stdlib::switch_,"));
    CHECK(contains(emitted->source, "hgraph::wire<hgraph::stdlib::getattr_>"));
    CHECK(contains(emitted->source, "hgraph::Str{\"result\"}"));
    CHECK(contains(emitted->source, "hgraph::Str{\"offset\"}"));
}

TEST_CASE("emit-cpp returns an expression result while remapping an escaping assignment", "[codegen][control-flow][conditional]") {
    Unit unit{R"(
module planned_mixed_temporal_results

export fn adjusted(condition: bool, x: i64, y: i64) -> i64 {
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
)"};
    INFO(unit.diagnostics.render(unit.file));
    REQUIRE_FALSE(unit.diagnostics.has_errors());

    const auto emitted = unit.emit();
    REQUIRE(emitted);
    CHECK(contains(emitted->source, "hgraph::UnNamedTSB<hgraph::Field<\"value\", hgraph::TS<hgraph::Int>>, "
                                    "hgraph::Field<\"offset\", hgraph::TS<hgraph::Int>>>"));
    CHECK(occurrences(emitted->source, "hgraph::stdlib::to_tsb<") == 2U);
    CHECK(contains(emitted->source, "auto hgl_adjusted_if_1_value = [&]()"));
    CHECK(contains(emitted->source, "offset = hgraph::wire<hgraph::stdlib::getattr_>"));
    CHECK(contains(emitted->source, "return hgraph::wire<hgraph::stdlib::getattr_>"));
    CHECK(contains(emitted->source, "hgraph::Str{\"value\"}"));
    CHECK(contains(emitted->source, "hgraph::Str{\"offset\"}"));
}

TEST_CASE("emit-cpp sequences mixed temporal projections before an enclosing expression", "[codegen][control-flow][conditional]") {
    Unit unit{R"(
module planned_inline_mixed_temporal_results

export fn adjusted(condition: bool, x: i64, y: i64) -> i64 {
    var offset: i64
    return (if condition {
        offset = x + 1
        x * 2
    } else {
        offset = y - 1
        y * 3
    }) + offset
}
)"};
    INFO(unit.diagnostics.render(unit.file));
    REQUIRE_FALSE(unit.diagnostics.has_errors());

    const auto emitted = unit.emit();
    REQUIRE(emitted);
    const std::size_t materialized = emitted->source.find("auto hgl_adjusted_if_1_value = [&]()");
    const std::size_t enclosing    = emitted->source.find("hgraph::wire<hgraph::stdlib::add_>", materialized);
    REQUIRE(materialized != std::string::npos);
    REQUIRE(enclosing != std::string::npos);
    CHECK(materialized < enclosing);
    CHECK(contains(emitted->source, "hgl_adjusted_if_1_value, offset"));
}

TEST_CASE("emit-cpp preserves reference access for a forwarded conditional binding", "[codegen][control-flow][conditional]") {
    Unit unit{R"(
module planned_temporal_forwarding

export fn adjusted(condition: bool, x: i64) -> i64 {
    var result: i64 = x
    if condition {
        result = result + 1
    }
    return result
}

export fn adjusted_pair(condition: bool, x: i64, y: i64) -> i64 {
    var left: i64 = x
    var right: i64 = y
    if condition {
        left = left + 1
    } else {
        right = right + 1
    }
    return left + right
}
)"};
    INFO(unit.diagnostics.render(unit.file));
    REQUIRE_FALSE(unit.diagnostics.has_errors());

    const auto emitted = unit.emit();
    REQUIRE(emitted);
    CHECK(contains(emitted->source, "hgraph::Port<hgraph::REF<hgraph::TS<hgraph::Int>>> result"));
    CHECK(contains(emitted->source, "return result.as<hgraph::TS<hgraph::Int>>()"));
    CHECK(contains(emitted->source, "result.as<hgraph::REF<hgraph::TS<hgraph::Int>>>()"));
    CHECK(contains(emitted->source, "hgraph::UnNamedTSB<hgraph::Field<\"left\", hgraph::TS<hgraph::Int>>, "
                                    "hgraph::Field<\"right\", hgraph::TS<hgraph::Int>>>"));
    CHECK(contains(emitted->source, "left.as<hgraph::REF<hgraph::TS<hgraph::Int>>>()"));
    CHECK(contains(emitted->source, "right.as<hgraph::REF<hgraph::TS<hgraph::Int>>>()"));
}

TEST_CASE("emit-cpp types an omitted temporal else with a never-ticking source", "[codegen][control-flow][conditional]") {
    Unit unit{R"(
module planned_temporal_omitted_else

export fn choose(condition: bool, value: i64) -> i64 {
    if condition {
        value + 1
    }
}
)"};
    INFO(unit.diagnostics.render(unit.file));
    REQUIRE_FALSE(unit.diagnostics.has_errors());

    const auto emitted = unit.emit();
    REQUIRE(emitted);
    CHECK(contains(emitted->source, "struct hgl_choose_if_1_else"));
    CHECK(contains(emitted->source, "return hgraph::wire<hgraph::stdlib::nothing, hgraph::TS<hgraph::Int>>(w);"));
    CHECK(contains(emitted->source, "return hgraph::wire<hgraph::stdlib::switch_>"));
}

TEST_CASE("emit-cpp promotes the first constant assignment to a typed composition var", "[codegen][locals][control-flow]") {
    Unit unit{R"(
module planned_constant_assignment

export fn selected(const condition: bool) -> i64 {
    var result: i64
    if condition {
        result = 1
    } else {
        result = 2
    }
    result
}
)"};
    REQUIRE_FALSE(unit.diagnostics.has_errors());

    const auto emitted = unit.emit();
    INFO(unit.diagnostics.render(unit.file));
    REQUIRE(emitted);
    CHECK(contains(emitted->source, "hgraph::Port<hgraph::TS<hgraph::Int>> result;"));
    CHECK(contains(emitted->source, "result = hgraph::wire<hgraph::stdlib::const_, hgraph::TS<hgraph::Int>>(w, hgraph::Int{1});"));
    CHECK(contains(emitted->source, "result = hgraph::wire<hgraph::stdlib::const_, hgraph::TS<hgraph::Int>>(w, hgraph::Int{2});"));
    CHECK(contains(emitted->source, "return result;"));
}

TEST_CASE("emit-cpp uses inferred hgraph IR state types", "[codegen][hgraph-ir][locals][runtime]") {
    Unit unit{R"(
module inferred_state

export fn total(value: f64) -> f64 {
    state sum = 0.0
    when modified(value) && valid(value) {
        sum += value
        return sum
    }
}
)"};
    REQUIRE_FALSE(unit.diagnostics.has_errors());

    const auto emitted = unit.emit();
    REQUIRE(emitted);
    CHECK(contains(emitted->header, "hgraph::Field<\"sum\", hgraph::TS<hgraph::Float>>"));
    CHECK(contains(emitted->header, "hgraph::Float{0.0}"));
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

TEST_CASE("emit-cpp emits and registers only requested generic operator materializations",
          "[codegen][hgraph-ir][operators][generics]") {
    Unit unit{R"(
module materialized_overloads

operator choose<T>(value: T) -> T
impl fn choose<T>(value: T) -> T
requires T in {i64, f64}
=> value

instantiate choose<i64>, choose<f64>
)"};
    REQUIRE_FALSE(unit.diagnostics.has_errors());
    REQUIRE(unit.graph.completion == hgl::hgraph_ir::Completion::Bodies);
    REQUIRE(unit.graph.callables.size() == 1);
    REQUIRE(unit.graph.materializations.size() == 2);

    const auto emitted = unit.emit();
    REQUIRE(emitted);
    const std::string &identity = unit.graph.callables.front().identity;
    const std::size_t  marker   = identity.find_last_of('#');
    REQUIRE(marker != std::string::npos);
    const std::string base = "choose_impl_" + identity.substr(marker + 1U);

    CHECK_FALSE(contains(emitted->source, "struct " + base + " {"));
    CHECK(contains(emitted->source, "struct " + base + "__i64__m0"));
    CHECK(contains(emitted->source, "struct " + base + "__f64__m1"));
    CHECK(contains(emitted->source, "register_graph_overload<operators::choose, " + base + "__i64__m0>()"));
    CHECK(contains(emitted->source, "register_graph_overload<operators::choose, " + base + "__f64__m1>()"));
    CHECK_FALSE(contains(emitted->header, base));
    for (const hgl::hgraph_ir::Materialization &materialization : unit.graph.materializations) {
        CHECK(contains(emitted->source, materialization.identity));
        CHECK(contains(emitted->descriptor, materialization.identity));
    }
}

TEST_CASE("emit-cpp preserves complete source-shape generics in operator contracts", "[codegen][hgraph-ir][operators][generics]") {
    Unit unit{R"(
module generic_source_contract

operator forward<S>(value: S) -> S
impl fn forward(value: i64) -> i64 => value
)"};
    REQUIRE_FALSE(unit.diagnostics.has_errors());

    const auto emitted = unit.emit();
    INFO(unit.diagnostics.render(unit.file));
    REQUIRE(emitted);
    CHECK(contains(emitted->header, "hgraph::In<\"value\", hgraph::TsVar<\"S\">>"));
    CHECK(contains(emitted->header, "hgraph::Out<hgraph::TsVar<\"S\">>"));
    CHECK_FALSE(contains(emitted->header, "hgraph::TS<hgraph::ScalarVar<\"S\">>"));
}

TEST_CASE("emit-cpp retains a size generic in a partially materialized list candidate",
          "[codegen][hgraph-ir][operators][generics]") {
    Unit unit{R"(
module partially_materialized_overloads

operator preserve<T, const size: i64>(value: list<T, size>) -> list<T, size>
impl fn preserve<T, const size: i64>(value: list<T, size>) -> list<T, size> => value

instantiate preserve<i64, _>
)"};
    REQUIRE_FALSE(unit.diagnostics.has_errors());
    REQUIRE(unit.graph.materializations.size() == 1);

    const auto emitted = unit.emit();
    INFO(unit.diagnostics.render(unit.file));
    REQUIRE(emitted);
    CHECK(contains(emitted->source, "struct preserve_impl_2__i64__any_size__m0"));
    CHECK(contains(emitted->source, "hgraph::TSL<hgraph::TS<hgraph::Int>, hgraph::SIZE<\"size\">>"));
    CHECK(contains(emitted->source, "register_graph_overload<operators::preserve, preserve_impl_2__i64__any_size__m0>()"));
    CHECK(contains(emitted->descriptor, "\"name\": \"size\""));
    CHECK(contains(emitted->descriptor, "\"kind\": \"const\""));
}

TEST_CASE("emit-cpp diagnoses value use of a retained generic separately from a signature marker",
          "[codegen][hgraph-ir][operators][generics]") {
    Unit concrete{R"(
module concrete_reification

operator extent<const size: i64>(value: list<i64, size>) -> i64
impl fn extent<const size: i64>(value: list<i64, size>) -> i64 => size

instantiate extent<3>
)"};
    REQUIRE_FALSE(concrete.diagnostics.has_errors());
    const auto concrete_emitted = concrete.emit();
    INFO(concrete.diagnostics.render(concrete.file));
    REQUIRE(concrete_emitted);
    CHECK(contains(concrete_emitted->source, "hgraph::Int{3}"));

    Unit unit{R"(
module reified_materialization

operator extent<const size: i64>(value: list<i64, size>) -> i64
impl fn extent<const size: i64>(value: list<i64, size>) -> i64 => size

instantiate extent<_>
)"};
    REQUIRE_FALSE(unit.diagnostics.has_errors());
    CHECK_FALSE(unit.emit());
    CHECK(contains(unit.diagnostics.render(unit.file),
                   "a retained generic used as a value; generic reification is not supported by emit-cpp yet"));
}

TEST_CASE("emit-cpp resolves a concrete duration generic before selecting a rolling shape",
          "[codegen][hgraph-ir][operators][generics][rolling]") {
    Unit unit{R"(
module materialized_duration_window

operator latest<const size: duration>(value: rolling<f64, size>) -> f64
impl fn latest<const size: duration>(value: rolling<f64, size>) -> f64 => 1.0

instantiate latest<5m>
)"};
    REQUIRE_FALSE(unit.diagnostics.has_errors());

    const auto emitted = unit.emit();
    INFO(unit.diagnostics.render(unit.file));
    REQUIRE(emitted);
    CHECK(contains(emitted->source, "hgraph::TSWDuration<hgraph::Float, 300000000, 300000000>"));
    CHECK_FALSE(contains(emitted->source, "hgraph::TSWAny<hgraph::Float>"));
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
    for (auto &value : unit.graph.values) {
        if (auto *reference = std::get_if<hgl::hgraph_ir::Reference>(&value.node);
            reference != nullptr && reference->kind == hgl::hgraph_ir::ReferenceKind::Operator &&
            reference->identity == "renamed_ops.choose") {
            reference->identity = unit.graph.operators.front().identity;
        }
        if (value.operation.kind == hgl::hgraph_ir::OperationKind::NominalOperator &&
            value.operation.identity == "renamed_ops.choose") {
            value.operation.identity = unit.graph.operators.front().identity;
        }
    }
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

TEST_CASE("emit-cpp writes a Python wrapper over the registered names", "[codegen]") {
    Unit        unit{read_file(std::string{HGL_CODEGEN_DIR} + "/parity.hgl"), "parity.hgl"};
    EmitOptions options;
    options.header_name          = "parity.h";
    options.python_native_module = "_parity";
    const auto emitted           = unit.emit(options);
    REQUIRE(emitted);
    CHECK(contains(emitted->python, "from . import _parity as _hgl_native"));
    CHECK(contains(emitted->python, "\"plus\": _hgl_operator_function(\"hgl.codegen.parity.plus\")"));
    CHECK(contains(
        emitted->python,
        "__all__ = [\"plus\", \"scaled_sum\", \"above\", \"maybe_double\", \"offset_by\", \"choose\", \"choose_embedded\"]"));
}

TEST_CASE("emit-cpp gives Python keyword exports a usable spelling", "[codegen]") {
    Unit        unit{R"(
module t
export fn class(x: f64) -> f64 => x
)"};
    EmitOptions options;
    options.python_native_module = "_t";
    const auto emitted           = unit.emit(options);
    REQUIRE(emitted);
    CHECK(contains(emitted->python, "\"class_\": _hgl_operator_function(\"t.class\")"));
    CHECK(contains(emitted->python, "__all__ = [\"class_\"]"));
}

TEST_CASE("emit-cpp rejects ambiguous or invalid Python wrapper names", "[codegen]") {
    SECTION("two exports map to one Python identifier") {
        Unit        unit{R"(
module t
export fn def(x: f64) -> f64 => x
export fn def_(x: f64) -> f64 => x
)"};
        EmitOptions options;
        options.python_native_module = "_t";
        CHECK_FALSE(unit.emit(options));
        CHECK(unit.has(Category::Backend, "Python export 'def_' collides with 'def' as 'def_'"));
    }
    SECTION("the native module is one non-keyword identifier") {
        Unit        unit{R"(
module t
export fn value(x: f64) -> f64 => x
)"};
        EmitOptions options;
        options.python_native_module = "bad-name";
        CHECK_FALSE(unit.emit(options));
        CHECK(unit.has(Category::Backend, "not a valid Python native-module identifier"));
    }
}

TEST_CASE("emit-cpp folds constants with the direct backend's rules", "[codegen]") {
    Unit       unit{R"(
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
    CHECK(contains(emitted->source, "hgraph::stdlib::scalar_div<hgraph::Int, hgraph::Int>::apply(n.value(), hgraph::Int{2})"));
    CHECK(contains(emitted->source, "const auto label = (s.value() + hgraph::Str{\"!\"});"));
    CHECK(contains(emitted->source, "auto total = (n.value() * hgraph::Int{3});"));
    CHECK(contains(emitted->source, "total = (total - hgraph::Int{1});"));
    CHECK(contains(emitted->source, "if (((total > hgraph::Int{2}) && (label == hgraph::Str{\"hi!\"})))"));
    CHECK(contains(emitted->source, "hgraph::wire<hgraph::stdlib::mul_>(w, x, half)"));
}

TEST_CASE("emit-cpp rejects type-changing var assignment", "[codegen]") {
    SECTION("ordinary assignment cannot narrow an inferred i64") {
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
    SECTION("compound division cannot change an inferred i64 to f64") {
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
    SECTION("i64 still widens into an f64 var") {
        Unit       unit{R"(
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

TEST_CASE("emit-cpp rejects zero constant divisors", "[codegen]") {
    SECTION("division") {
        Unit unit{R"(
module t
export fn f(x: f64) -> f64 => x + 1 / (2 - 2)
)"};
        CHECK_FALSE(unit.emit());
        CHECK(unit.has(Category::Type, "division by zero"));
    }
    SECTION("remainder") {
        Unit unit{R"(
module t
export fn f(x: f64) -> f64 => x + 1 % 0
)"};
        CHECK_FALSE(unit.emit());
        CHECK(unit.has(Category::Type, "remainder by zero"));
    }
}

TEST_CASE("emit-cpp wires constants at a temporal parameter and reads a kernel by marker", "[codegen]") {
    Unit       unit{R"(
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

TEST_CASE("emit-cpp orders internal dependencies from hgraph IR", "[codegen][hgraph-ir][dependencies]") {
    Unit unit{R"(
module planned_dependencies

fn second(value: f64) -> f64 => value
fn first(value: f64) -> f64 => second(value)
fn third(value: f64) -> f64 => value

export fn result(value: f64) -> f64 => first(value)
)"};
    REQUIRE_FALSE(unit.diagnostics.has_errors());

    const auto callable_id = [&](std::string_view identity) {
        const auto found = std::find_if(unit.graph.callables.begin(), unit.graph.callables.end(),
                                        [&](const auto &candidate) { return candidate.identity == identity; });
        REQUIRE(found != unit.graph.callables.end());
        return hgl::hgraph_ir::CallableId{static_cast<std::uint32_t>(std::distance(unit.graph.callables.begin(), found))};
    };
    const hgl::hgraph_ir::CallableId first  = callable_id("planned_dependencies.first");
    const hgl::hgraph_ir::CallableId second = callable_id("planned_dependencies.second");
    const hgl::hgraph_ir::CallableId third  = callable_id("planned_dependencies.third");
    const auto dependency = std::find_if(unit.graph.values.begin(), unit.graph.values.end(), [&](const auto &candidate) {
        return candidate.operation.kind == hgl::hgraph_ir::OperationKind::ExactFunction && candidate.operation.callable == second;
    });
    REQUIRE(dependency != unit.graph.values.end());

    SECTION("the planned dependency is authoritative") {
        dependency->operation.callable = third;
        auto &callee                   = unit.graph.values[std::get<hgl::hgraph_ir::Call>(dependency->node).callee.value];
        std::get<hgl::hgraph_ir::Reference>(callee.node).callable = third;

        const auto emitted = unit.emit();
        REQUIRE(emitted);
        const std::size_t second_pos = emitted->source.find("struct second");
        const std::size_t third_pos  = emitted->source.find("struct third");
        const std::size_t first_pos  = emitted->source.find("struct first");
        REQUIRE(second_pos != std::string::npos);
        REQUIRE(third_pos != std::string::npos);
        REQUIRE(first_pos != std::string::npos);
        CHECK(second_pos < third_pos);
        CHECK(third_pos < first_pos);
    }

    SECTION("an invalid planned dependency fails closed") {
        dependency->operation.callable = hgl::hgraph_ir::CallableId{static_cast<std::uint32_t>(unit.graph.callables.size())};

        CHECK_FALSE(unit.emit());
        CHECK(unit.has(Category::Backend, "hgraph IR contains an invalid callable dependency ID"));
    }

    SECTION("a missing planned dependency fails closed") {
        dependency->operation.callable = {};

        CHECK_FALSE(unit.emit());
        CHECK(unit.has(Category::Backend, "hgraph IR contains an invalid callable dependency ID"));
    }

    SECTION("a missing callable body fails closed") {
        unit.graph.callables[first.value].concise_body = {};

        CHECK_FALSE(unit.emit());
        CHECK(unit.has(Category::Backend, "'first' must have exactly one concise or block body"));
    }

    SECTION("a missing required value edge fails closed") {
        std::get<hgl::hgraph_ir::Call>(dependency->node).callee = {};

        CHECK_FALSE(unit.emit());
        CHECK(unit.has(Category::Backend, "hgraph IR contains an invalid body value ID"));
    }

    SECTION("a missing required block edge fails closed") {
        dependency->operation = {};
        dependency->node      = hgl::hgraph_ir::BlockValue{};

        CHECK_FALSE(unit.emit());
        CHECK(unit.has(Category::Backend, "hgraph IR contains an invalid body block ID"));
    }
}

TEST_CASE("emit-cpp renders concise bodies from hgraph IR", "[codegen][hgraph-ir][bodies]") {
    Unit unit{R"(
module planned_body

export fn combine(a: f64, b: f64) -> f64 => a + b
)"};
    REQUIRE_FALSE(unit.diagnostics.has_errors());
    REQUIRE(unit.graph.callables.size() == 1U);
    const hgl::hgraph_ir::ValueId body_id = unit.graph.callables.front().concise_body;
    REQUIRE(body_id.valid());
    REQUIRE(body_id.value < unit.graph.values.size());
    auto &body = unit.graph.values[body_id.value];
    auto &sum  = std::get<hgl::hgraph_ir::Binary>(body.node);

    SECTION("the planned operation is authoritative") {
        sum.op                       = hgl::ir::hir::BinaryOp::Mul;
        body.operation.identity      = "mul_";
        body.operation.registry_name = "mul_";

        const auto emitted = unit.emit();
        REQUIRE(emitted);
        CHECK(contains(emitted->source, "hgraph::wire<hgraph::stdlib::mul_>(w, a, b)"));
        CHECK_FALSE(contains(emitted->source, "hgraph::wire<hgraph::stdlib::add_>(w, a, b)"));
    }

    SECTION("planned lexical bindings are authoritative") {
        auto &lhs                                             = unit.graph.values[sum.lhs.value];
        auto &rhs                                             = unit.graph.values[sum.rhs.value];
        std::get<hgl::hgraph_ir::Reference>(lhs.node).binding = std::get<hgl::hgraph_ir::Reference>(rhs.node).binding;

        const auto emitted = unit.emit();
        REQUIRE(emitted);
        CHECK(contains(emitted->source, "hgraph::wire<hgraph::stdlib::add_>(w, b, b)"));
    }

    SECTION("an invalid planned body value fails closed") {
        unit.graph.callables.front().concise_body = hgl::hgraph_ir::ValueId{static_cast<std::uint32_t>(unit.graph.values.size())};

        CHECK_FALSE(unit.emit());
        CHECK(unit.has(Category::Backend, "hgraph IR contains an invalid body value ID"));
    }
}

TEST_CASE("emit-cpp renders composition blocks from hgraph IR", "[codegen][hgraph-ir][blocks]") {
    Unit unit{R"(
module planned_block

export fn adjusted(value: f64, const enabled: bool = true) -> f64 {
    var amount = 1.0
    amount += 2.0
    if enabled {
        return value + amount
    }
    if enabled {
        value - amount
    }
    value
}
)"};
    REQUIRE_FALSE(unit.diagnostics.has_errors());
    REQUIRE(unit.graph.callables.size() == 1U);
    const hgl::hgraph_ir::BlockId body_id = unit.graph.callables.front().block_body;
    REQUIRE(body_id.valid());
    REQUIRE(body_id.value < unit.graph.blocks.size());

    const auto local_statement =
        std::find_if(unit.graph.statements.begin(), unit.graph.statements.end(),
                     [](const auto &statement) { return std::holds_alternative<hgl::hgraph_ir::LocalBinding>(statement.node); });
    REQUIRE(local_statement != unit.graph.statements.end());
    const auto &local = std::get<hgl::hgraph_ir::LocalBinding>(local_statement->node);
    REQUIRE(local.init.valid());
    REQUIRE(local.init.value < unit.graph.values.size());

    const auto return_statement =
        std::find_if(unit.graph.statements.begin(), unit.graph.statements.end(),
                     [](const auto &statement) { return std::holds_alternative<hgl::hgraph_ir::Return>(statement.node); });
    REQUIRE(return_statement != unit.graph.statements.end());
    const auto &returned = std::get<hgl::hgraph_ir::Return>(return_statement->node);
    REQUIRE(returned.value.valid());
    REQUIRE(returned.value.value < unit.graph.values.size());

    SECTION("planned local initializers and return operations are authoritative") {
        auto &init    = unit.graph.values[local.init.value];
        init.node     = hgl::hgraph_ir::Literal{3.0};
        init.constant = 3.0;

        auto &result                                     = unit.graph.values[returned.value.value];
        std::get<hgl::hgraph_ir::Binary>(result.node).op = hgl::ir::hir::BinaryOp::Mul;
        result.operation.identity                        = "mul_";
        result.operation.registry_name                   = "mul_";

        const auto emitted = unit.emit();
        REQUIRE(emitted);
        CHECK(contains(emitted->source, "auto amount = hgraph::Float{3.0};"));
        CHECK_FALSE(contains(emitted->source, "auto amount = hgraph::Float{1.0};"));
        CHECK(contains(emitted->source, "hgraph::wire<hgraph::stdlib::mul_>(w, value, amount)"));
        CHECK_FALSE(contains(emitted->source, "hgraph::wire<hgraph::stdlib::add_>(w, value, amount)"));
        CHECK(contains(emitted->source, "(void)hgraph::wire<hgraph::stdlib::sub_>(w, value, amount)"));
    }

    SECTION("an invalid planned statement fails closed") {
        auto &body = unit.graph.blocks[body_id.value];
        REQUIRE_FALSE(body.statements.empty());
        body.statements.front() = hgl::hgraph_ir::StatementId{static_cast<std::uint32_t>(unit.graph.statements.size())};

        CHECK_FALSE(unit.emit());
        CHECK(unit.has(Category::Backend, "hgraph IR contains an invalid body statement ID"));
    }
}

TEST_CASE("emit-cpp unrolls fixed temporal list graph iteration", "[codegen][iteration]") {
    Unit       unit{R"(
module checks.fixed_iteration
use hgraph.std::{null_sink}

export fn observe(samples: list<f64, 3>) {
    for sample in elements(samples) {
        null_sink(sample)
    }
}

export fn observe_items(samples: list<f64, 3>) {
    for index, sample in items(samples) {
        null_sink(sample + index)
    }
}
)"};
    const auto emitted = unit.emit();
    REQUIRE(emitted);
    CHECK(occurrences(emitted->source, "hgraph::tsl_element(samples,") == 6U);
    CHECK(occurrences(emitted->source, "hgraph::wire<hgraph::stdlib::null_sink>") == 6U);
    CHECK(occurrences(emitted->source, "hgraph::wire<hgraph::stdlib::add_>") == 3U);

    SECTION("dynamic graph traversal lowers to independent sink child graphs") {
        Unit       dynamic{R"(
module checks.dynamic_iteration
use hgraph.std::{null_sink}
export fn observe(book: map<str, f64>, samples: list<f64>, peers: list<f64>, offset: f64) {
    for value in values(book) { null_sink(value + offset) }
    for key, value in items(book) {
        null_sink(value + offset)
    }
    for value in elements(samples) {
        null_sink(value + offset)
        null_sink(valid(peers))
    }
    for index, value in items(samples) { null_sink(value + index + offset) }
}
)"};
        const auto generated = dynamic.emit();
        REQUIRE(generated);
        CHECK(occurrences(generated->source, "hgraph::wire<hgraph::stdlib::map_sink_>") == 4U);
        CHECK(contains(generated->source, "hgraph::NamedPort<\"key\", hgraph::TS<hgraph::Str>> key"));
        CHECK(contains(generated->source, "hgraph::NamedPort<\"ndx\", hgraph::TS<hgraph::Int>> index"));
        CHECK(occurrences(generated->source, "[[maybe_unused]] hgraph::Port<hgraph::TS<hgraph::Float>> offset") == 4U);
        CHECK(contains(generated->source, "[[maybe_unused]] hgraph::Port<hgraph::TSL<hgraph::TS<hgraph::Float>>> peers"));
        CHECK(contains(generated->source, "hgraph::stdlib::pass_through(peers)"));
        CHECK_FALSE(contains(generated->source, "struct map_sink_"));
    }

    SECTION("graph iterator predicates remain a design boundary") {
        Unit predicate{R"(
module checks.predicate_iteration
use hgraph.std::{null_sink}
export fn observe(samples: list<f64, 3>) {
    for sample in elements(samples, modified) { null_sink(sample) }
}
)"};
        CHECK_FALSE(predicate.emit());
        CHECK(predicate.has(Category::Backend, "graph-phase iterator predicates are not defined yet"));
    }

    SECTION("scalar captures remain a deliberate boundary") {
        Unit scalar_capture{R"(
module checks.scalar_capture
use hgraph.std::{null_sink}
export fn observe(samples: list<f64>, const offset: f64) {
    for sample in elements(samples) { null_sink(sample + offset) }
}
)"};
        CHECK_FALSE(scalar_capture.emit());
        CHECK(scalar_capture.has(Category::Backend,
                                 "capturing scalar configuration in a dynamic graph 'for' body is not supported yet"));
    }

    SECTION("assignments cannot escape the traversal body") {
        Unit escaping{R"(
module checks.escaping_iteration
use hgraph.std::{null_sink}
export fn observe(samples: list<f64, 3>) {
    var selected: f64
    for sample in elements(samples) { selected = sample }
}
)"};
        CHECK_FALSE(escaping.emit());
        CHECK(escaping.has(Category::Backend, "assignment escaping a graph 'for' body is not defined yet"));
    }
}

TEST_CASE("emit-cpp renders runtime bodies from hgraph IR", "[codegen][hgraph-ir][runtime]") {
    Unit unit{R"(
module planned_runtime_body

export fn total(value: f64) -> f64 {
    state total = 1.0
    when modified(value) && valid(value) {
        var amount = 2.0
        total += value * amount
        return total
    }
}
)"};
    REQUIRE_FALSE(unit.diagnostics.has_errors());
    REQUIRE(unit.graph.callables.size() == 1U);
    const hgl::hgraph_ir::BlockId body_id = unit.graph.callables.front().block_body;
    REQUIRE(body_id.valid());
    REQUIRE(body_id.value < unit.graph.blocks.size());

    const auto state_statement =
        std::find_if(unit.graph.statements.begin(), unit.graph.statements.end(),
                     [](const auto &statement) { return std::holds_alternative<hgl::hgraph_ir::StateBinding>(statement.node); });
    REQUIRE(state_statement != unit.graph.statements.end());
    auto &state = std::get<hgl::hgraph_ir::StateBinding>(state_statement->node);
    REQUIRE(state.binding.valid());
    REQUIRE(state.binding.value < unit.graph.bindings.size());
    REQUIRE(state.init.valid());
    REQUIRE(state.init.value < unit.graph.values.size());

    const auto local_statement =
        std::find_if(unit.graph.statements.begin(), unit.graph.statements.end(),
                     [](const auto &statement) { return std::holds_alternative<hgl::hgraph_ir::LocalBinding>(statement.node); });
    REQUIRE(local_statement != unit.graph.statements.end());
    auto &local = std::get<hgl::hgraph_ir::LocalBinding>(local_statement->node);
    REQUIRE(local.binding.valid());
    REQUIRE(local.binding.value < unit.graph.bindings.size());
    REQUIRE(local.init.valid());
    REQUIRE(local.init.value < unit.graph.values.size());

    const auto product = std::find_if(unit.graph.values.begin(), unit.graph.values.end(), [](const auto &value) {
        return value.operation.kind == hgl::hgraph_ir::OperationKind::NominalOperator && value.operation.registry_name == "mul_";
    });
    REQUIRE(product != unit.graph.values.end());

    SECTION("planned state, locals, and operations are authoritative") {
        unit.graph.bindings[state.binding.value].name = "planned_total";
        unit.graph.bindings[local.binding.value].name = "planned_amount";

        auto &state_init    = unit.graph.values[state.init.value];
        state_init.node     = hgl::hgraph_ir::Literal{3.0};
        state_init.constant = 3.0;
        auto &local_init    = unit.graph.values[local.init.value];
        local_init.node     = hgl::hgraph_ir::Literal{4.0};
        local_init.constant = 4.0;

        auto &sum                        = std::get<hgl::hgraph_ir::Binary>(product->node);
        sum.op                           = hgl::ir::hir::BinaryOp::Add;
        product->operation.identity      = "add_";
        product->operation.registry_name = "add_";

        const auto emitted = unit.emit();
        REQUIRE(emitted);
        CHECK(contains(emitted->header, "hgraph::Field<\"planned_total\", hgraph::TS<hgraph::Float>>"));
        CHECK(contains(emitted->header, "planned_total.set(hgraph::Float{3.0})"));
        CHECK(contains(emitted->header, "auto planned_amount = hgraph::Float{4.0};"));
        CHECK(contains(emitted->header, "(value.value() + planned_amount)"));
        CHECK_FALSE(contains(emitted->header, "hgraph::Field<\"total\", hgraph::TS<hgraph::Float>>"));
        CHECK_FALSE(contains(emitted->header, "auto amount = hgraph::Float{2.0};"));
        CHECK_FALSE(contains(emitted->header, "(value.value() * planned_amount)"));
    }

    SECTION("an invalid planned runtime statement fails closed") {
        auto &body = unit.graph.blocks[body_id.value];
        REQUIRE_FALSE(body.statements.empty());
        body.statements.front() = hgl::hgraph_ir::StatementId{static_cast<std::uint32_t>(unit.graph.statements.size())};

        CHECK_FALSE(unit.emit());
        CHECK(unit.has(Category::Backend, "hgraph IR contains an invalid body statement ID"));
    }
}

TEST_CASE("emit-cpp lowers scalar runtime functions to static nodes", "[codegen][runtime]") {
    Unit       unit{R"(
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

TEST_CASE("emit-cpp lowers homogeneous runtime packs to Args input views", "[codegen][runtime][parameter-pack]") {
    Unit       unit{R"(
module runtime_packs

operator all_runtime(values: ...bool{2}) -> bool

impl fn all_runtime(values: ...bool) -> bool {
    when {
        var result = true
        for value in elements(values, valid) {
            result = result && value
        }
        return result
    }
}

)"};
    const auto emitted = unit.emit();
    INFO(unit.diagnostics.render(unit.file));
    REQUIRE(emitted);
    CHECK(contains(emitted->source, "hgraph::In<\"values\", hgraph::Args<hgraph::TS<hgraph::Bool>>"));
    CHECK(contains(emitted->source, "hgl_value_position < values.size()"));
    CHECK(contains(emitted->source, "const auto hgl_value_item = values[hgl_value_position]"));
    CHECK(contains(emitted->source, "if (hgl_value_item.valid())"));
    CHECK(contains(emitted->source, "result && hgl_value_item.value()"));
    CHECK(contains(emitted->source, "hgraph::register_overload<operators::all_runtime, all_runtime_impl_"));
    CHECK(contains(emitted->source, "hgraph::OperatorNodePack::Infer, hgraph::OperatorPackCardinality{2, 2}>"));
}

TEST_CASE("emit-cpp registers composition pack cardinality", "[codegen][parameter-pack][cardinality]") {
    Unit       unit{R"(
module composition_cardinality
operator bounded<T>(values: ...T{1:3}) -> i64
impl fn bounded<T>(values: ...T) -> i64 => 0
instantiate bounded<f64>
)"};
    const auto emitted = unit.emit();
    INFO(unit.diagnostics.render(unit.file));
    REQUIRE(emitted);
    CHECK(contains(emitted->source, "hgraph::OperatorPackCardinality{1, 3}, hgraph::OperatorPackCardinality{0, "
                                    "hgraph::OperatorPackCardinality::unbounded}>"));
}

TEST_CASE("emit-cpp rejects implementation cardinality disjoint from its operator contract",
          "[codegen][parameter-pack][cardinality]") {
    Unit unit{R"(
module disjoint_cardinality
operator bounded<T>(values: ...T{1:2}) -> i64
impl fn bounded<T>(values: ...T{3:*}) -> i64 => 0
instantiate bounded<f64>
)"};
    CHECK_FALSE(unit.emit());
    CHECK(contains(unit.diagnostics.render(unit.file), "pack cardinality does not overlap its contract"));
}

TEST_CASE("emit-cpp preserves heterogeneous runtime pack call style", "[codegen][runtime][parameter-pack]") {
    Unit       unit{R"(
module runtime_heterogeneous_packs

operator positional_count<...Ts>(values: ...Ts) -> i64
operator named_count<...Fields>(values: ...{Fields}) -> i64

impl fn positional_count<...Ts>(values: ...Ts) -> i64 {
    when modified(values) {
        return 0
    }
}

impl fn named_count<...Fields>(values: ...{Fields}) -> i64 {
    when {
        var count = 0
        for name in keys(values, modified) {
            if name == "a" || name == "b" {
                count += 1
            }
        }
        return count
    }
}

instantiate positional_count<_>, named_count<_>
)"};
    const auto emitted = unit.emit();
    INFO(unit.diagnostics.render(unit.file));
    REQUIRE(emitted);
    CHECK(contains(emitted->source, "hgraph::In<\"values\", hgraph::Kwargs<>"));
    CHECK(contains(emitted->source, "hgraph::OperatorNodePack::PositionalOnly>()"));
    CHECK(contains(emitted->source, "hgraph::OperatorNodePack::KeywordOnly>()"));
    CHECK(contains(emitted->source, "values.modified_keys()"));
}

TEST_CASE("emit-cpp expands default runtime activation and validity predicates", "[codegen][runtime]") {
    Unit       unit{R"(
module t

export fn implicit(a: f64, b: f64) -> f64 {
    when {
        return a + b
    }
}

export fn explicit(a: f64, b: f64) -> f64 {
    when modified() && valid() {
        return a + b
    }
}

export fn default_validity(a: f64, b: f64) -> f64 {
    when modified(a) {
        return a + b
    }
}

export fn default_activation(a: f64, b: f64) -> f64 {
    when valid(a) {
        return a
    }
}
)"};
    const auto emitted = unit.emit();
    INFO(unit.diagnostics.render(unit.file));
    REQUIRE(emitted);

    CHECK(contains(emitted->header, "(a.modified() || b.modified())"));
    CHECK(contains(emitted->header, "(a.valid() && b.valid())"));
    CHECK(contains(emitted->header, "hgraph::InputActivity::Passive"));
    CHECK_FALSE(unit.diagnostics.has_errors());
}

TEST_CASE("emit-cpp recognizes only top-level handler selectors as explicit policy", "[codegen][runtime]") {
    Unit       unit{R"(
module t

export fn residual(a: f64, b: f64) -> f64 {
    when modified(a) || valid(a) {
        return a + b
    }
}
)"};
    const auto emitted = unit.emit();
    INFO(unit.diagnostics.render(unit.file));
    REQUIRE(emitted);

    // Both selector calls are residual under `||`; neither suppresses the
    // corresponding all-input default. The default activation also keeps both
    // input selectors active in the generated node contract.
    CHECK(contains(emitted->header, "(a.modified() || b.modified())"));
    CHECK(contains(emitted->header, "(a.valid() && b.valid())"));
    CHECK(contains(emitted->header, "((a.modified()) || (a.valid()))"));
    CHECK_FALSE(contains(emitted->header, "hgraph::InputActivity::Passive"));
    CHECK_FALSE(unit.diagnostics.has_errors());
}

TEST_CASE("empty runtime metadata calls are contextual handler selectors", "[codegen][runtime]") {
    SECTION("modified and valid are rejected outside a when condition") {
        Unit unit{R"(
module t

export fn invalid(value: f64) -> f64 {
    inject out
    if valid() || modified() {
        out = value
    }
}
)"};
        CHECK_FALSE(unit.emit());
        CHECK(unit.has(Category::Type, "zero-argument 'valid' is only valid in a function-level 'when' condition"));
        CHECK(unit.has(Category::Type, "zero-argument 'modified' is only valid in a function-level 'when' condition"));
    }
    SECTION("all_valid always names at least one endpoint") {
        Unit unit{R"(
module t

export fn invalid(value: f64) -> f64 {
    when modified(value) && all_valid() {
        return value
    }
}
)"};
        CHECK_FALSE(unit.emit());
        CHECK(unit.has(Category::Type, "'all_valid' takes at least one argument"));
    }
}

TEST_CASE("emit-cpp requires validity to dominate runtime payload reads", "[codegen][runtime]") {
    SECTION("a when and nested if establish validity for their bodies") {
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
    SECTION("an unchecked temporal payload read fails closed") {
        Unit unit{R"(
module t
export fn sampled(trigger: f64, sample: f64) -> f64 {
    when modified(trigger) && valid(trigger) {
        return sample
    }
}
)"};
        CHECK_FALSE(unit.emit());
        CHECK(unit.has(Category::Type, "temporal input 'sample' may be invalid here; guard the read with valid(sample)"));
    }
    SECTION("validity must precede a payload read in a short-circuit condition") {
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
    SECTION("when blocks are function-level handlers") {
        Unit unit{R"(
module t
export fn sampled(value: f64) {
    if valid(value) {
        when modified(value) { let sampled = value }
    }
}
)"};
        CHECK_FALSE(unit.emit());
        // Placement is the checker's rule (#767 item 2); the emitter never
        // sees a nested handler.
        CHECK(unit.has(Category::FunctionKind, "'when' cannot be nested in another block"));
    }
}

TEST_CASE("emit-cpp fails closed on constructs it does not lower", "[codegen]") {
    SECTION("a const-generic struct") {
        Unit unit{R"(
module t
export struct Tag<const n: i64> {
    value: i64
}
)"};
        CHECK_FALSE(unit.emit());
        CHECK(unit.has(Category::Backend, "const generic struct arguments require typed constant Bundle metadata in hgraph"));
    }
    SECTION("a runtime call") {
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
    SECTION("a temporal input in a lifecycle block") {
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
    // The size rules below are typed HIR completion's (#767 item 2); they
    // reach the emitter as diagnostics of the unit, never as emitter guards.
    SECTION("a non-positive rolling size") {
        Unit unit{R"(
module t
export fn recent(window: rolling<f64, 0>) -> f64 => 1.0
)"};
        CHECK_FALSE(unit.emit());
        CHECK(unit.has(Category::Type, "a rolling tick size must be positive"));
    }
    SECTION("a negative rolling size") {
        Unit unit{R"(
module t
export fn recent(window: rolling<f64, -1>) -> f64 => 1.0
)"};
        CHECK_FALSE(unit.emit());
        CHECK(unit.has(Category::Type, "a rolling tick size must be positive"));
    }
    SECTION("a rolling minimum larger than its maximum") {
        Unit unit{R"(
module t
export fn recent(window: rolling<f64, 5, 6>) -> f64 => 1.0
)"};
        CHECK_FALSE(unit.emit());
        CHECK(unit.has(Category::Type, "a rolling minimum size must be positive and no larger than the maximum"));
    }
    SECTION("a zero fixed list size") {
        Unit       unit{R"(
module t
export fn recent(values: list<f64, 0>) -> f64 => 1.0
)"};
        const auto generated = unit.emit();
        REQUIRE(generated);
        CHECK(contains(generated->source, "hgraph::TSL<hgraph::TS<hgraph::Float>, 0>"));
    }
    SECTION("a missing module declaration") {
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
    CHECK(contains(emitted->header, "hgraph::TsVar<\"U\">"));
    CHECK(contains(emitted->header, "hgraph::TSWDuration<hgraph::Float, 300000000, 300000000>"));
    CHECK(contains(emitted->header, "hgraph::LoggerView logger"));
    CHECK(contains(emitted->header, "logger.log(2, hgraph::Str{\"value\"});"));
}

TEST_CASE("emit-cpp preserves explicit reference schemas", "[codegen][ref]") {
    Unit       unit{R"(
module t

export fn forward(value: ref<f64>) -> ref<f64> {
    when modified(value) && valid(value) {
        return value
    }
}
)"};
    const auto emitted = unit.emit();
    REQUIRE(emitted);

    CHECK(contains(emitted->descriptor, "\"kind\": \"ref\""));
    CHECK(contains(emitted->header, "hgraph::In<\"value\", hgraph::REF<hgraph::TS<hgraph::Float>>"));
    CHECK(contains(emitted->header, "hgraph::Out<hgraph::REF<hgraph::TS<hgraph::Float>>>"));
    CHECK(contains(emitted->header, "hgl_output.set(value.value());"));
}

TEST_CASE("emit-cpp proves selected reference validity by selector", "[codegen][ref][validity]") {
    SECTION("a different selected child is not covered") {
        Unit unit{R"(
module t
export fn wrong(values: list<ref<f64>, 3>) -> ref<f64> {
    when modified(values) && valid(values[0]) {
        return values[1]
    }
}
)"};
        CHECK_FALSE(unit.emit());
        CHECK(unit.has(Category::Type, "selected temporal input may be invalid here"));
    }

    SECTION("a selector index must itself be valid") {
        Unit unit{R"(
module t
export fn wrong(index: i64, values: list<ref<f64>, 3>) -> ref<f64> {
    when modified(values) && valid(values[index]) {
        return values[index]
    }
}
)"};
        CHECK_FALSE(unit.emit());
        CHECK(unit.has(Category::Type, "temporal input 'index' may be invalid here"));
    }

    SECTION("a dynamic selector index must be range guarded") {
        Unit unit{R"(
module t
export fn wrong(index: i64, values: list<ref<f64>, 3>) -> ref<f64> {
    when modified(index, values) && valid(index) && valid(values[index]) {
        return values[index]
    }
}
)"};
        CHECK_FALSE(unit.emit());
        CHECK(unit.has(Category::Type, "a dynamic fixed-list index must be guarded"));
    }

    SECTION("a folded constant proves the upper bound") {
        Unit unit{R"(
module t
export fn select(index: i64, values: list<ref<f64>, 3>) -> ref<f64> {
    when modified(index, values) && valid(index) && index >= 0 && index < 1 + 2 && valid(values[index]) {
        return values[index]
    }
}
)"};
        CHECK(unit.emit());
    }
}

TEST_CASE("emit-cpp escapes C++ keywords and its own names", "[codegen]") {
    Unit       unit{R"(
module t.new
export fn w(delete: f64, const int: i64 = 1) -> f64 => delete * int
)"};
    const auto emitted = unit.emit();
    REQUIRE(emitted);
    CHECK(emitted->namespace_name == "t::new_");
    CHECK(contains(emitted->header, "using w_ = hgraph::Operator<\"t.new.w\""));
    CHECK(contains(emitted->source, "hgraph::Port<hgraph::TS<hgraph::Float>> w_::compose([[maybe_unused]] hgraph::Wiring &w, "
                                    "hgraph::Port<hgraph::TS<hgraph::Float>> delete_, hgraph::Scalar<\"int\", hgraph::Int> int_)"));
}

TEST_CASE("emit-cpp diagnoses escaped C++ name collisions", "[codegen]") {
    SECTION("module functions") {
        Unit unit{R"(
module t
export fn class(x: f64) -> f64 => x
export fn class_(x: f64) -> f64 => x
)"};
        CHECK_FALSE(unit.emit());
        CHECK(unit.has(Category::Backend, "C++ function 'class_' collides with 'class' as 'class_'"));
    }
    SECTION("parameters") {
        Unit unit{R"(
module t
export fn value(class: f64, class_: f64) -> f64 => class + class_
)"};
        CHECK_FALSE(unit.emit());
        CHECK(unit.has(Category::Backend, "C++ parameter 'class_' collides with 'class' as 'class_'"));
    }
}

TEST_CASE("emit-cpp gives shadowing locals unique C++ names", "[codegen]") {
    Unit       unit{R"(
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
