#include <execution.h>
#include <cleanup.h>
#include "syntax/parser.h"
#include "semantics/resolve.h"
#include "ir/lower.h"
#include "ir/type_check.h"
#include "hgraph_ir/lower.h"
#include "wiring/backend.h"
#include "wiring/operator_types.h"
#include <catch2/catch_test_macros.hpp>
#include <fstream>
#include <iterator>
#include <map>
#include <string>

namespace {
std::vector<hgl::wiring::TestResult> run(const std::string &name) {
    std::ifstream input{std::string{HGL_NEGATIVE_SOURCE_DIR} + "/" + name + ".hgl"};
    REQUIRE(input.good());
    hgl::syntax::SourceFile file{name + ".hgl", {std::istreambuf_iterator<char>{input}, {}}};
    hgl::syntax::DiagnosticSink diagnostics;
    const auto ast = hgl::syntax::parse(file, diagnostics);
    const auto resolved = hgl::semantics::resolve(file, ast, hgl::wiring::has_operator, diagnostics);
    INFO(diagnostics.render(file));
    REQUIRE_FALSE(diagnostics.has_errors());
    auto hir = hgl::ir::lower_to_hir(ast, resolved, diagnostics);
    REQUIRE(hgl::ir::complete_hir(hir, hgl::wiring::resolve_operator_types, diagnostics));
    auto graph = hgl::hgraph_ir::lower(hir, diagnostics);
    REQUIRE_FALSE(diagnostics.has_errors());
    auto results = hgl::wiring::run_tests(file, graph, {}, diagnostics);
    INFO(diagnostics.render(file));
    REQUIRE_FALSE(diagnostics.has_errors());
    return results;
}
}
TEST_CASE("generated execution kernels retain exact negative test semantics", "[negative][generated]") {
    hgl::wiring::ensure_session();
    tests::negative_execution::register_operators();
    const auto results = run("execution");
    const std::map<std::string, bool> expected{{"expected",true},{"nested",true},{"normal",false},
        {"empty",false},{"wrong",false},{"assertion",false},{"inner_failure",false},{"continuation",true}};
    REQUIRE(results.size() == expected.size());
    for (const auto &result : results) {
        INFO(result.name << ": " << result.message);
        CHECK(result.passed == expected.at(result.name));
    }
}
TEST_CASE("generated cleanup failures never satisfy execution expectations", "[negative][generated]") {
    hgl::wiring::ensure_session();
    tests::negative_cleanup::register_operators();
    const auto results = run("cleanup");
    REQUIRE(results.size() == 5U);
    for (const auto &result : results) {
        INFO(result.name << ": " << result.message);
        CHECK(result.passed == (result.name == "continuation"));
        if (result.name == "combined_failure") {
            CHECK(result.message.find("negative duration") != std::string::npos);
            CHECK(result.message.find("cleanup sentinel") != std::string::npos);
        }
    }
}
