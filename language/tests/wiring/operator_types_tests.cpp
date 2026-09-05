#include "ir/lower.h"
#include "ir/type_check.h"
#include "semantics/resolve.h"
#include "syntax/parser.h"
#include "wiring/backend.h"
#include "wiring/operator_types.h"

#include <catch2/catch_test_macros.hpp>

#include <string>

namespace
{
    struct Unit
    {
        hgl::syntax::SourceFile        file;
        hgl::syntax::DiagnosticSink    diagnostics{};
        hgl::syntax::ast::Module       ast{};
        hgl::semantics::ResolvedModule resolved{};
        hgl::ir::hir::Module           hir{};

        explicit Unit(std::string text) : file{"test.hgl", std::move(text)} {
            hgl::wiring::ensure_session();
            ast = hgl::syntax::parse(file, diagnostics);
            if (diagnostics.has_errors()) { return; }
            resolved = hgl::semantics::resolve(file, ast, hgl::wiring::has_operator, diagnostics);
            if (diagnostics.has_errors()) { return; }
            hir = hgl::ir::lower_to_hir(ast, resolved, diagnostics);
        }

        bool complete() { return hgl::ir::complete_hir(hir, hgl::wiring::resolve_operator_types, diagnostics); }
    };
}  // namespace

TEST_CASE("native operator typing delegates concrete selection to hgraph", "[ir][operator-types]") {
    Unit unit{R"(
module checks.native_types
use hgraph.std::{mean}

fn average(window: rolling<f64, 20>) -> f64 => mean(window)
)"};
    INFO(unit.diagnostics.render(unit.file));
    REQUIRE_FALSE(unit.diagnostics.has_errors());
    REQUIRE(unit.complete());

    bool found = false;
    for (const hgl::ir::hir::Expr &expression : unit.hir.exprs) {
        if (expression.operation.identity != "hgraph.std.mean") { continue; }
        found = true;
        CHECK_FALSE(expression.operation.candidate_label.empty());
        CHECK_FALSE(expression.operation.deferred);
        CHECK_FALSE(expression.operation.substitutions.empty());
    }
    CHECK(found);
}

TEST_CASE("native operator typing recovers an inferred result for nested calls", "[ir][operator-types]") {
    Unit unit{R"(
module checks.nested_native_types
use hgraph.std::{mean}

fn scaled_average(window: rolling<f64, 20>) -> f64 => mean(window) * 2.0
)"};
    INFO(unit.diagnostics.render(unit.file));
    REQUIRE_FALSE(unit.diagnostics.has_errors());
    REQUIRE(unit.complete());

    bool found = false;
    for (const hgl::ir::hir::Expr &expression : unit.hir.exprs) {
        if (expression.operation.identity != "hgraph.std.mean") { continue; }
        found = true;
        CHECK(expression.type.valid());
        CHECK(unit.hir.type(expression.type).scalar == hgl::ir::hir::ScalarType::F64);
        CHECK_FALSE(expression.operation.deferred);
    }
    CHECK(found);
}

TEST_CASE("type-only const parameters still permit fixed native output inference", "[ir][operator-types]") {
    Unit unit{R"(
module checks.const_parameter_output
use hgraph.std::{schedule}

fn heartbeat(const every: duration) -> datetime => last_modified(schedule(every))
)"};
    INFO(unit.diagnostics.render(unit.file));
    REQUIRE_FALSE(unit.diagnostics.has_errors());
    REQUIRE(unit.complete());

    bool found = false;
    for (const hgl::ir::hir::Expr &expression : unit.hir.exprs) {
        if (expression.operation.identity != "hgraph.std.schedule") { continue; }
        found = true;
        REQUIRE(expression.type.valid());
        CHECK(unit.hir.type(expression.type).scalar == hgl::ir::hir::ScalarType::Bool);
        CHECK(expression.operation.deferred);
        CHECK(expression.operation.candidate_label.empty());
    }
    CHECK(found);
}

TEST_CASE("higher-order operator typing remains explicit and deferred", "[ir][operator-types]") {
    Unit unit{R"(
module checks.higher_order_types
use hgraph.std::{map}

fn double(values: map<str, f64>) -> map<str, f64> =>
    map(values, fn(value) => value * 2.0)
)"};
    INFO(unit.diagnostics.render(unit.file));
    REQUIRE_FALSE(unit.diagnostics.has_errors());
    REQUIRE(unit.complete());

    bool found = false;
    for (const hgl::ir::hir::Expr &expression : unit.hir.exprs) {
        if (expression.operation.kind != hgl::ir::hir::OperationKind::NominalOperator ||
            expression.operation.identity != "hgraph.std.map") {
            continue;
        }
        found = true;
        CHECK(expression.operation.deferred);
        CHECK(expression.operation.candidate_label.empty());
    }
    CHECK(found);
}

TEST_CASE("operator requirements use native registry viability", "[ir][operator-types][constraints]") {
    Unit unit{R"(
module checks.native_requirement
use hgraph.std::{add}

fn double<T>(value: T) -> T
requires add(T, T) -> T
=> value + value

fn apply_double(value: f64) -> f64 => double(value)
)"};
    INFO(unit.diagnostics.render(unit.file));
    REQUIRE_FALSE(unit.diagnostics.has_errors());
    REQUIRE(unit.complete());

    bool found_body_operation = false;
    for (const hgl::ir::hir::Expr &expression : unit.hir.exprs) {
        if (expression.operation.identity != "hgraph.std.add") { continue; }
        found_body_operation = true;
        CHECK(expression.operation.target.valid());
    }
    CHECK(found_body_operation);
}
