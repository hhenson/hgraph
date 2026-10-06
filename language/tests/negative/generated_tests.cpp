#include <execution.h>
#include <cleanup.h>
#include <delta-positive.h>
#include <eval-profile-errors.h>
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
std::vector<hgl::wiring::TestResult> run(const std::string &name, const std::string &path = {}) {
    std::ifstream input{path.empty() ? std::string{HGL_NEGATIVE_SOURCE_DIR} + "/" + name + ".hgl" : path};
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

TEST_CASE("generated shared eval admission and delta positive controls", "[negative][generated][delta]") {
    hgl::wiring::ensure_session();
    examples::eval_profile_errors::register_operators();
    examples::reject::delta_errors::register_operators();
    const auto profiles = run("eval-profile-errors", HGL_EVAL_PROFILE_FILE);
    REQUIRE(profiles.size() == 19U);
    for (const auto &result : profiles) {
        INFO(result.name << ": " << result.message);
        CHECK(result.passed);
    }
    const auto positives = run("delta-positive", HGL_DELTA_POSITIVE_FILE);
    REQUIRE(positives.size() == 1U);
    INFO(positives.front().message);
    CHECK(positives.front().passed);
}
