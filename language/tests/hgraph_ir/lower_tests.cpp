#include "hgraph_ir/complete.h"
#include "hgraph_ir/lower.h"
#include "hgraph_ir/printer.h"
#include "ir/lower.h"
#include "ir/type_check.h"
#include "semantics/resolve.h"
#include "syntax/parser.h"

#include <catch2/catch_test_macros.hpp>

#include <algorithm>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <optional>
#include <string>
#include <string_view>

namespace
{
    namespace hir = hgl::ir::hir;

    bool has_operator(std::string_view name) { return name != "map"; }

    struct Lowered
    {
        hgl::syntax::SourceFile               file;
        hgl::syntax::DiagnosticSink           diagnostics{};
        std::optional<hgl::hgraph_ir::Module> graph{};

        explicit Lowered(std::string text, std::string path = "test.hgl")
            : Lowered{std::move(text),
                      [](const hir::Module &, const hgl::ir::OperatorQuery &query) {
                          hgl::ir::OperatorSelection selected;
                          selected.result   = query.expected_result;
                          selected.deferred = true;
                          return selected;
                      },
                      std::move(path)} {}

        Lowered(std::string text, hgl::ir::OperatorResolver operators, std::string path = "test.hgl")
            : file{std::move(path), std::move(text)} {
            hgl::syntax::ast::Module ast = hgl::syntax::parse(file, diagnostics);
            if (diagnostics.has_errors()) { return; }
            hgl::semantics::ResolvedModule resolved = hgl::semantics::resolve(file, ast, has_operator, diagnostics);
            if (diagnostics.has_errors()) { return; }
            hir::Module language = hgl::ir::lower_to_hir(ast, resolved, diagnostics);
            if (!hgl::ir::complete_hir(language, operators, diagnostics)) { return; }
            graph = hgl::hgraph_ir::lower(language, diagnostics);
        }

        Lowered(std::string text, const hgl::semantics::ModuleCatalog &catalog, std::string path = "test.hgl")
            : file{std::move(path), std::move(text)} {
            hgl::syntax::ast::Module ast = hgl::syntax::parse(file, diagnostics);
            if (diagnostics.has_errors()) { return; }
            hgl::semantics::ResolvedModule resolved = hgl::semantics::resolve(file, ast, catalog, has_operator, diagnostics);
            if (diagnostics.has_errors()) { return; }
            hir::Module                     language  = hgl::ir::lower_to_hir(ast, resolved, diagnostics);
            const hgl::ir::OperatorResolver operators = [](const hir::Module &, const hgl::ir::OperatorQuery &query) {
                hgl::ir::OperatorSelection selected;
                selected.result   = query.expected_result;
                selected.deferred = true;
                return selected;
            };
            if (!hgl::ir::complete_hir(language, operators, diagnostics)) { return; }
            graph = hgl::hgraph_ir::lower(language, diagnostics);
        }
    };

    const hgl::hgraph_ir::Callable *callable(const hgl::hgraph_ir::Module &module, std::string_view identity) {
        for (const hgl::hgraph_ir::Callable &candidate : module.callables) {
            if (candidate.identity == identity) { return &candidate; }
        }
        return nullptr;
    }

    const hgl::hgraph_ir::StructContract *structure(const hgl::hgraph_ir::Module &module, std::string_view identity) {
        for (const hgl::hgraph_ir::StructContract &candidate : module.structures) {
            if (candidate.identity == identity) { return &candidate; }
        }
        return nullptr;
    }

    hgl::semantics::ModuleCatalog native_catalog() {
        hgl::semantics::ModuleCatalog    catalog;
        hgl::semantics::ImportableModule module;
        module.identity = "acme.stats";
        module.functions.push_back(hgl::semantics::ImportedFunction{
            .module_identity        = module.identity,
            .name                   = "blend",
            .identity               = "acme.stats::blend",
            .cpp_symbol             = "acme::stats::blend",
            .parameters             = {{"value", hgl::semantics::ImportedScalarType::F64, false},
                                       {"window", hgl::semantics::ImportedScalarType::I64, true}},
            .result                 = hgl::semantics::ImportedScalarType::F64,
            .phases                 = {hgl::semantics::NativeCallPhase::Evaluation},
            .public_headers         = {"acme/stats.h"},
            .cmake_packages         = {"acme"},
            .imported_targets       = {"acme::stats"},
            .descriptor_fingerprint = "sha256:test",
        });
        REQUIRE_FALSE(catalog.add(std::move(module)));
        return catalog;
    }
}  // namespace

TEST_CASE("every guide example lowers complete hgraph IR bodies", "[hgraph-ir][examples]") {
    const std::filesystem::path directory{HGL_EXAMPLES_DIR};
    REQUIRE(std::filesystem::is_directory(directory));

    std::size_t count = 0;
    for (const std::filesystem::directory_entry &entry : std::filesystem::directory_iterator{directory}) {
        if (entry.path().extension() != ".hgl") { continue; }
        ++count;
        std::ifstream input{entry.path()};
        REQUIRE(input.good());
        Lowered lowered{std::string{std::istreambuf_iterator<char>{input}, std::istreambuf_iterator<char>{}},
                        entry.path().string()};
        INFO(entry.path().filename().string());
        INFO(lowered.diagnostics.render(lowered.file));
        REQUIRE_FALSE(lowered.diagnostics.has_errors());
        REQUIRE(lowered.graph);
        CHECK(lowered.graph->completion == hgl::hgraph_ir::Completion::Bodies);
        CHECK_FALSE(lowered.graph->types.empty());
        for (const hgl::hgraph_ir::Callable &function : lowered.graph->callables) {
            CHECK_FALSE(function.identity.empty());
            CHECK(function.result.valid());
            CHECK((function.concise_body.valid() || function.block_body.valid()));
            for (const hgl::hgraph_ir::Parameter &parameter : function.parameters) { CHECK(parameter.type.valid()); }
        }
    }
    CHECK(count >= 6);
}

TEST_CASE("hgraph IR separates a contract identity from its native registry key", "[hgraph-ir][operators]") {
    Lowered lowered{R"(
module checks.operator_identity

use hgraph.std::{map}

fn map_values(values: map<str, f64>) -> map<str, f64> =>
    map(values, fn(value) => value)
)"};
    INFO(lowered.diagnostics.render(lowered.file));
    REQUIRE_FALSE(lowered.diagnostics.has_errors());
    REQUIRE(lowered.graph);

    REQUIRE(lowered.graph->operators.size() == 1);
    const hgl::hgraph_ir::OperatorContract &op = lowered.graph->operators.front();
    CHECK(op.imported);
    CHECK(op.identity == "hgraph.std.map");
    CHECK(op.registry_name == "map_");
}

TEST_CASE("hgraph IR retains typed declaration handles in source order", "[hgraph-ir][declarations]") {
    Lowered lowered{R"(
module checks.source_order

struct Box {
    value: f64
}

fn first(value: f64) -> f64 => value

operator scale(value: f64) -> f64

impl fn scale(value: f64) -> f64 => value

fn last(value: f64) -> f64 => first(value)

test last_ticks {
    assert eval(last, value: [1.0]) == [1.0]
}
)"};
    INFO(lowered.diagnostics.render(lowered.file));
    REQUIRE_FALSE(lowered.diagnostics.has_errors());
    REQUIRE(lowered.graph);

    const auto &order = lowered.graph->source_order;
    REQUIRE(order.size() == 6);
    CHECK(std::get<hgl::hgraph_ir::StructId>(order[0]).value == 0);
    CHECK(std::get<hgl::hgraph_ir::CallableId>(order[1]).value == 0);
    CHECK(std::get<hgl::hgraph_ir::OperatorId>(order[2]).value == 0);
    CHECK(std::get<hgl::hgraph_ir::CallableId>(order[3]).value == 1);
    CHECK(std::get<hgl::hgraph_ir::CallableId>(order[4]).value == 2);
    CHECK(std::get<hgl::hgraph_ir::TestId>(order[5]).value == 0);
    CHECK(lowered.file.slice(lowered.graph->declaration_range(order[0])).starts_with("struct Box"));
    CHECK(lowered.file.slice(lowered.graph->declaration_range(order[2])).starts_with("operator scale"));
    CHECK(lowered.file.slice(lowered.graph->declaration_range(order[5])).starts_with("test last_ticks"));

    const std::string dump = hgl::hgraph_ir::print(*lowered.graph);
    CHECK(dump.find("source-order [struct:s0, callable:f0, operator:o0, callable:f1, callable:f2, test:x0]") != std::string::npos);
}

TEST_CASE("hgraph IR classifies runtime nodes and copies capabilities", "[hgraph-ir][runtime]") {
    Lowered lowered{R"(
module checks.runtime

export fn total(value: f64) -> f64 {
    state current: f64 = 0.0
    inject out, logger
    when modified(value) {
        current += value
        out = current
    }
}
)"};
    INFO(lowered.diagnostics.render(lowered.file));
    REQUIRE_FALSE(lowered.diagnostics.has_errors());
    REQUIRE(lowered.graph);

    const hgl::hgraph_ir::Callable *total = callable(*lowered.graph, "checks.runtime.total");
    REQUIRE(total != nullptr);
    CHECK(total->visibility == hgl::hgraph_ir::CallableVisibility::Export);
    CHECK(total->kind == hgl::hgraph_ir::CallableKind::RuntimeNode);
    REQUIRE(total->capabilities.size() == 2);
    CHECK(total->capabilities[0].name == "out");
    CHECK(total->capabilities[0].type == total->result);
    CHECK(total->capabilities[1].name == "logger");
    CHECK(total->capabilities[1].type.valid());
    CHECK(hir::has_effect(total->effects, hir::Effect::WriteState));
    CHECK(hir::has_effect(total->effects, hir::Effect::WriteOutput));
    REQUIRE(total->block_body.valid());

    const hgl::hgraph_ir::Block &body = lowered.graph->blocks[total->block_body.value];
    CHECK(std::ranges::any_of(body.statements, [&](hgl::hgraph_ir::StatementId id) {
        return std::holds_alternative<hgl::hgraph_ir::StateBinding>(lowered.graph->statements[id.value].node);
    }));
    CHECK(std::ranges::any_of(body.statements, [&](hgl::hgraph_ir::StatementId id) {
        return std::holds_alternative<hgl::hgraph_ir::Activation>(lowered.graph->statements[id.value].node);
    }));
    CHECK(std::ranges::any_of(lowered.graph->statements, [](const hgl::hgraph_ir::Statement &statement) {
        return std::holds_alternative<hgl::hgraph_ir::Assignment>(statement.node);
    }));
}

TEST_CASE("hgraph IR bodies resolve exact calls and native operator identities", "[hgraph-ir][bodies][operations]") {
    Lowered lowered{R"(
module checks.calls

fn double(value: f64) -> f64 => value + value
fn adjusted(value: f64) -> f64 => double(value) - 1.0
)"};
    INFO(lowered.diagnostics.render(lowered.file));
    REQUIRE_FALSE(lowered.diagnostics.has_errors());
    REQUIRE(lowered.graph);
    REQUIRE(lowered.graph->completion == hgl::hgraph_ir::Completion::Bodies);
    REQUIRE(lowered.graph->callables.size() == 2);

    const auto exact = std::ranges::find_if(lowered.graph->values, [](const hgl::hgraph_ir::Value &value) {
        return value.operation.kind == hgl::hgraph_ir::OperationKind::ExactFunction;
    });
    REQUIRE(exact != lowered.graph->values.end());
    CHECK(exact->operation.identity == "checks.calls.double");
    REQUIRE(exact->operation.callable.valid());
    CHECK(lowered.graph->callables[exact->operation.callable.value].identity == "checks.calls.double");

    const auto add = std::ranges::find_if(lowered.graph->values, [](const hgl::hgraph_ir::Value &value) {
        return value.operation.kind == hgl::hgraph_ir::OperationKind::NominalOperator && value.operation.registry_name == "add_";
    });
    REQUIRE(add != lowered.graph->values.end());
    CHECK(add->operation.deferred);
}

TEST_CASE("hgraph IR owns descriptor-native exact calls and build metadata", "[hgraph-ir][native]") {
    const hgl::semantics::ModuleCatalog catalog = native_catalog();
    Lowered                             lowered{R"(
module checks.native
use acme.stats::{blend}
fn smooth(value: f64) -> f64 {
    when modified(value) { return blend(value, 3) }
}
)",
                                                catalog};
    INFO(lowered.diagnostics.render(lowered.file));
    REQUIRE_FALSE(lowered.diagnostics.has_errors());
    REQUIRE(lowered.graph);
    REQUIRE(lowered.graph->native_functions.size() == 1U);
    const hgl::hgraph_ir::NativeFunction &native = lowered.graph->native_functions.front();
    CHECK(native.identity == "acme.stats::blend");
    CHECK(native.cpp_symbol == "acme::stats::blend");
    CHECK(native.public_headers == std::vector<std::string>{"acme/stats.h"});

    const auto call = std::ranges::find_if(
        lowered.graph->values, [](const hgl::hgraph_ir::Value &value) { return value.operation.identity == "acme.stats::blend"; });
    REQUIRE(call != lowered.graph->values.end());
    CHECK(call->operation.kind == hgl::hgraph_ir::OperationKind::ExactFunction);
    CHECK_FALSE(call->operation.callable.valid());
    CHECK(call->operation.native_function == hgl::hgraph_ir::NativeFunctionId{0U});
    const auto *syntax_call = std::get_if<hgl::hgraph_ir::Call>(&call->node);
    REQUIRE(syntax_call != nullptr);
    const auto *reference = std::get_if<hgl::hgraph_ir::Reference>(&lowered.graph->values[syntax_call->callee.value].node);
    REQUIRE(reference != nullptr);
    CHECK(reference->kind == hgl::hgraph_ir::ReferenceKind::NativeFunction);
    CHECK(reference->native_function == hgl::hgraph_ir::NativeFunctionId{0U});
    CHECK(hgl::hgraph_ir::print(*lowered.graph).find("native-functions\n  z0 acme.stats::blend") != std::string::npos);
}

TEST_CASE("hgraph IR inventories concrete keyed operator providers deterministically", "[hgraph-ir][providers]") {
    const hgl::ir::OperatorResolver operators = [](const hir::Module &module, const hgl::ir::OperatorQuery &query) {
        hgl::ir::OperatorSelection selected;
        selected.result = query.expected_result;
        if (!selected.result.valid()) {
            const hir::Type &argument = module.type(query.arguments.front().type);
            selected.result           = argument.children.front();
        }
        selected.candidate_label = "selected " + query.identity;
        selected.provider_key    = query.identity == "total" ? "provider.alpha" : "provider.zeta";
        return selected;
    };
    Lowered lowered{R"(
module checks.providers
use hgraph.std::{mean, total}

fn combine(values: rolling<f64, 20>) -> f64 => mean(values) + total(values) + mean(values)
)",
                    operators};
    INFO(lowered.diagnostics.render(lowered.file));
    REQUIRE_FALSE(lowered.diagnostics.has_errors());
    REQUIRE(lowered.graph);

    CHECK(lowered.graph->completion == hgl::hgraph_ir::Completion::Bodies);
    CHECK(lowered.graph->provider_requirements == std::vector<std::string>{"provider.alpha", "provider.zeta"});
    const std::string printed = hgl::hgraph_ir::print(*lowered.graph);
    CHECK(printed.find("provider-requirements [\"provider.alpha\", \"provider.zeta\"]") != std::string::npos);
}

TEST_CASE("lowered hgraph IR completes against its locked provider universe", "[hgraph-ir][providers][completion]") {
    const hgl::ir::OperatorResolver operators = [](const hir::Module &, const hgl::ir::OperatorQuery &query) {
        return hgl::ir::OperatorSelection{
            .candidate_label = "selected " + query.identity,
            .provider_key    = "provider.analytics",
            .result          = query.expected_result,
        };
    };
    Lowered lowered{R"(
module checks.provider_completion
use hgraph.std::{mean}

fn average(values: rolling<f64, 20>) -> f64 => mean(values)
)",
                    operators};
    INFO(lowered.diagnostics.render(lowered.file));
    REQUIRE_FALSE(lowered.diagnostics.has_errors());
    REQUIRE(lowered.graph);

    CHECK(hgl::hgraph_ir::complete_execution(
        *lowered.graph, hgl::hgraph_ir::ProviderPlan{{"provider.analytics", "provider.unused"}}, lowered.diagnostics));
    INFO(lowered.diagnostics.render(lowered.file));
    CHECK_FALSE(lowered.diagnostics.has_errors());
    CHECK(lowered.graph->completion == hgl::hgraph_ir::Completion::Executable);
    CHECK(lowered.graph->provider_plan == hgl::hgraph_ir::ProviderPlan{{"provider.analytics", "provider.unused"}});
}

TEST_CASE("hgraph IR bodies preserve lifecycle and capability calls once", "[hgraph-ir][bodies][lifecycle]") {
    Lowered lowered{R"(
module checks.lifecycle

fn observed(value: f64) -> f64 {
    inject out, logger
    start { logger.info("start") }
    when modified(value) { out = value }
    stop { logger.info("stop") }
}
)"};
    INFO(lowered.diagnostics.render(lowered.file));
    REQUIRE_FALSE(lowered.diagnostics.has_errors());
    REQUIRE(lowered.graph);

    std::size_t starts = 0;
    std::size_t stops  = 0;
    for (const hgl::hgraph_ir::Statement &statement : lowered.graph->statements) {
        if (const auto *lifecycle = std::get_if<hgl::hgraph_ir::Lifecycle>(&statement.node)) {
            lifecycle->kind == hgl::hgraph_ir::LifecycleKind::Start ? ++starts : ++stops;
            const hgl::hgraph_ir::Block &block = lowered.graph->blocks[lifecycle->block.value];
            CHECK(block.statements.empty());
            CHECK(block.tail.valid());
        }
    }
    CHECK(starts == 1);
    CHECK(stops == 1);
    const auto capability = std::ranges::find_if(lowered.graph->values, [](const hgl::hgraph_ir::Value &value) {
        return value.operation.kind == hgl::hgraph_ir::OperationKind::Capability;
    });
    REQUIRE(capability != lowered.graph->values.end());
    CHECK(capability->operation.capability.valid());
    CHECK(capability->operation.identity == "logger.info");
}

TEST_CASE("hgraph IR bodies preserve traversal and predicate lambdas", "[hgraph-ir][bodies][collections]") {
    Lowered lowered{R"(
module checks.collections

fn recent(values: map<str, f64>, const cutoff: datetime) -> i64 {
    when modified(values) {
        var count = 0
        for key, value in items(values, fn(key, value) => last_modified(value) > cutoff) {
            count += 1
        }
        return count
    }
}
)"};
    INFO(lowered.diagnostics.render(lowered.file));
    REQUIRE_FALSE(lowered.diagnostics.has_errors());
    REQUIRE(lowered.graph);

    const auto traversal = std::ranges::find_if(lowered.graph->statements, [](const hgl::hgraph_ir::Statement &statement) {
        return std::holds_alternative<hgl::hgraph_ir::Traversal>(statement.node);
    });
    REQUIRE(traversal != lowered.graph->statements.end());
    const auto &loop = std::get<hgl::hgraph_ir::Traversal>(traversal->node);
    REQUIRE(loop.bindings.size() == 2);
    CHECK(loop.iterable.valid());
    CHECK(loop.block.valid());

    const auto lambda = std::ranges::find_if(lowered.graph->values, [](const hgl::hgraph_ir::Value &value) {
        return std::holds_alternative<hgl::hgraph_ir::Lambda>(value.node);
    });
    REQUIRE(lambda != lowered.graph->values.end());
    CHECK(std::get<hgl::hgraph_ir::Lambda>(lambda->node).parameters.size() == 2);
}

TEST_CASE("hgraph IR bodies own test harness plans", "[hgraph-ir][bodies][tests]") {
    Lowered lowered{R"(
module checks.tests

fn identity(value: f64) -> f64 => value
test identity_ticks {
    assert eval(identity, value: [1.0, _, 2.0]) == [1.0, _, 2.0]
}
)"};
    INFO(lowered.diagnostics.render(lowered.file));
    REQUIRE_FALSE(lowered.diagnostics.has_errors());
    REQUIRE(lowered.graph);
    REQUIRE(lowered.graph->tests.size() == 1);
    CHECK(lowered.graph->tests.front().identity == "checks.tests.identity_ticks");
    CHECK(lowered.graph->tests.front().body.valid());

    const auto evaluation = std::ranges::find_if(lowered.graph->values, [](const hgl::hgraph_ir::Value &value) {
        return value.operation.kind == hgl::hgraph_ir::OperationKind::HarnessEval;
    });
    REQUIRE(evaluation != lowered.graph->values.end());
    REQUIRE(evaluation->operation.callable.valid());
    CHECK(lowered.graph->callables[evaluation->operation.callable.value].identity == "checks.tests.identity");
}

TEST_CASE("hgraph IR preserves source implementation candidates", "[hgraph-ir][operators]") {
    Lowered lowered{R"(
module checks.implementation

operator choose<T>(value: T) -> T
impl fn choose(value: f64) -> f64 => value
fn selected(value: f64) -> f64 => choose(value)
)"};
    INFO(lowered.diagnostics.render(lowered.file));
    REQUIRE_FALSE(lowered.diagnostics.has_errors());
    REQUIRE(lowered.graph);

    REQUIRE(lowered.graph->operators.size() == 1);
    CHECK(lowered.graph->operators.front().identity == "checks.implementation.choose");
    REQUIRE(lowered.graph->callables.size() == 2);
    const hgl::hgraph_ir::Callable &implementation = lowered.graph->callables.front();
    CHECK(implementation.visibility == hgl::hgraph_ir::CallableVisibility::Implementation);
    CHECK(implementation.operator_identity == "checks.implementation.choose");
    CHECK(implementation.identity == "checks.implementation.choose#2");

    const auto call = std::ranges::find_if(lowered.graph->values,
                                           [](const hgl::hgraph_ir::Value &value) { return value.operation.candidate.valid(); });
    REQUIRE(call != lowered.graph->values.end());
    CHECK(call->operation.candidate_identity == implementation.identity);
    CHECK(lowered.graph->provider_requirements.empty());
}

TEST_CASE("hgraph IR owns explicit generic implementation materializations", "[hgraph-ir][operators][generics]") {
    Lowered lowered{R"(
module checks.materializations

operator sized<T, const N: i64>(value: list<T, N>) -> list<T, N>
impl fn sized<T, const N: i64>(value: list<T, N>) -> list<T, N> => value

instantiate sized<i64, 3>, sized<f64, 5>
)"};
    INFO(lowered.diagnostics.render(lowered.file));
    REQUIRE_FALSE(lowered.diagnostics.has_errors());
    REQUIRE(lowered.graph);
    REQUIRE(lowered.graph->callables.size() == 1);
    REQUIRE(lowered.graph->materializations.size() == 2);

    for (std::size_t index = 0; index < lowered.graph->materializations.size(); ++index) {
        const hgl::hgraph_ir::Materialization &materialization = lowered.graph->materializations[index];
        CHECK(materialization.implementation.value == 0);
        REQUIRE(materialization.substitutions.size() == 2);
        CHECK(materialization.substitutions[0].type.valid());
        REQUIRE(materialization.substitutions[1].constant);
        CHECK(std::get<std::int64_t>(*materialization.substitutions[1].constant) == (index == 0 ? 3 : 5));
    }

    const std::string printed = hgl::hgraph_ir::print(*lowered.graph);
    CHECK(printed.find("materializations") != std::string::npos);
    CHECK(printed.find("@instantiate:0 substitutions=[") != std::string::npos);
    CHECK(printed.find("=c") != std::string::npos);
}

TEST_CASE("hgraph IR prints constant-only operation substitutions", "[hgraph-ir][operators][printer]") {
    hgl::hgraph_ir::Module module;
    hgl::hgraph_ir::Value  value;
    value.operation.kind = hgl::hgraph_ir::OperationKind::NominalOperator;
    value.operation.substitutions.push_back(hgl::hgraph_ir::Substitution{
        .parameter_identity = "N",
        .constant           = hir::Constant{std::int64_t{42}},
    });
    module.values.push_back(std::move(value));

    CHECK(hgl::hgraph_ir::print(module).find("substitutions=[N=42]") != std::string::npos);
}

TEST_CASE("hgraph IR owns symbolic compile-time type arguments", "[hgraph-ir][types]") {
    Lowered lowered{R"(
module checks.const_types

operator recent<T, const N: i64>(values: rolling<T, N>) -> T
)"};
    INFO(lowered.diagnostics.render(lowered.file));
    REQUIRE_FALSE(lowered.diagnostics.has_errors());
    REQUIRE(lowered.graph);

    const auto rolling = std::ranges::find_if(lowered.graph->types,
                                              [](const hgl::hgraph_ir::Type &type) { return type.kind == hir::TypeKind::Rolling; });
    REQUIRE(rolling != lowered.graph->types.end());
    REQUIRE(rolling->size.valid());
    const hgl::hgraph_ir::ConstExpr &size = lowered.graph->const_exprs[rolling->size.value];
    CHECK(size.kind == hgl::hgraph_ir::ConstExprKind::Parameter);
    CHECK(size.parameter == "N");
    CHECK(size.parameter_binding.valid());
    CHECK(rolling->min_size == rolling->size);
    CHECK(hgl::hgraph_ir::print(*lowered.graph).find("parameter N binding=n") != std::string::npos);
}

TEST_CASE("hgraph IR preserves aggregate and temporal parameter defaults", "[hgraph-ir][defaults]") {
    Lowered lowered{R"(
module checks.defaults

fn configured<const N: i64>(
    value: f64,
    const sizes: list<i64> = [1, 2],
    const width: i64 = N,
    const venue: timezone = @[Europe/London]
) -> f64 => value
)"};
    INFO(lowered.diagnostics.render(lowered.file));
    REQUIRE_FALSE(lowered.diagnostics.has_errors());
    REQUIRE(lowered.graph);

    const hgl::hgraph_ir::Callable *configured = callable(*lowered.graph, "checks.defaults.configured");
    REQUIRE(configured != nullptr);
    REQUIRE(configured->parameters.size() == 4);
    const hgl::hgraph_ir::ConstExprId sizes = configured->parameters[1].default_value;
    REQUIRE(sizes.valid());
    const hgl::hgraph_ir::ConstExpr &sequence = lowered.graph->const_exprs[sizes.value];
    CHECK(sequence.kind == hgl::hgraph_ir::ConstExprKind::Sequence);
    CHECK(sequence.elements.size() == 2);
    const hgl::hgraph_ir::ConstExprId width = configured->parameters[2].default_value;
    REQUIRE(width.valid());
    CHECK(lowered.graph->const_exprs[width.value].kind == hgl::hgraph_ir::ConstExprKind::Parameter);
    CHECK(lowered.graph->const_exprs[width.value].parameter == "N");
    const std::string dump = hgl::hgraph_ir::print(*lowered.graph);
    CHECK(dump.find("@[Europe/London]") != std::string::npos);
}

TEST_CASE("hgraph IR owns effective struct contracts and requirements", "[hgraph-ir][structs][constraints]") {
    Lowered lowered{R"(
module checks.struct_contracts

export abstract struct Entity<T> {
    value: T
    label: str = "entity"
}

export struct Range<U>: Entity<U>
requires U in {i64, f64}
{
    label = "range"
    upper: U = null
}
)"};
    INFO(lowered.diagnostics.render(lowered.file));
    REQUIRE_FALSE(lowered.diagnostics.has_errors());
    REQUIRE(lowered.graph);

    const hgl::hgraph_ir::StructContract *entity = structure(*lowered.graph, "checks.struct_contracts.Entity");
    REQUIRE(entity != nullptr);
    CHECK(entity->exported);
    CHECK(entity->abstract);
    REQUIRE(entity->fields.size() == 2);
    CHECK(entity->fields[0].origin_identity == entity->identity);

    const hgl::hgraph_ir::StructContract *range = structure(*lowered.graph, "checks.struct_contracts.Range");
    REQUIRE(range != nullptr);
    CHECK(range->exported);
    CHECK_FALSE(range->abstract);
    REQUIRE(range->parents.size() == 1);
    REQUIRE(range->fields.size() == 3);
    CHECK(range->fields[0].name == "value");
    CHECK(range->fields[0].origin_identity == entity->identity);
    REQUIRE(range->fields[0].type.valid());
    CHECK(lowered.graph->types[range->fields[0].type.value].nominal_identity == "U");
    CHECK(range->fields[1].name == "label");
    CHECK(range->fields[1].origin_identity == entity->identity);
    CHECK(range->fields[1].default_value.valid());
    CHECK(range->fields[2].name == "upper");
    CHECK(range->fields[2].optional);
    CHECK(range->fields[2].origin_identity == range->identity);
    REQUIRE(range->requirements.valid());
    const hgl::hgraph_ir::Constraint &requirement = lowered.graph->constraints[range->requirements.value];
    const auto                       *relation    = std::get_if<hgl::hgraph_ir::ConstraintRelation>(&requirement.node);
    REQUIRE(relation != nullptr);
    CHECK(relation->op == hgl::hgraph_ir::ConstraintRelationOp::In);

    const std::string dump = hgl::hgraph_ir::print(*lowered.graph);
    CHECK(dump.find("constraints\n") != std::string::npos);
    CHECK(dump.find("export abstract struct checks.struct_contracts.Entity") != std::string::npos);
    CHECK(dump.find("export struct checks.struct_contracts.Range") != std::string::npos);
    CHECK(dump.find("requires=r") != std::string::npos);
    CHECK(dump.find("origin=checks.struct_contracts.Entity") != std::string::npos);
}

TEST_CASE("hgraph IR applies inherited type and const arguments", "[hgraph-ir][structs][generics]") {
    Lowered lowered{R"(
module checks.inherited_arguments

abstract struct Samples<T, const N: i64> {
    history: rolling<T, N>
}

struct Renamed<U, const M: i64>: Samples<U, M> {}

abstract struct Pair<A, B> {
    first: A
    second: B
}

struct Swap<X, Y>: Pair<Y, X> {}
)"};
    INFO(lowered.diagnostics.render(lowered.file));
    REQUIRE_FALSE(lowered.diagnostics.has_errors());
    REQUIRE(lowered.graph);

    const hgl::hgraph_ir::StructContract *renamed = structure(*lowered.graph, "checks.inherited_arguments.Renamed");
    REQUIRE(renamed != nullptr);
    REQUIRE(renamed->fields.size() == 1);
    const hgl::hgraph_ir::Type &history = lowered.graph->types[renamed->fields.front().type.value];
    REQUIRE(history.kind == hir::TypeKind::Rolling);
    REQUIRE(history.children.size() == 1);
    CHECK(lowered.graph->types[history.children.front().value].nominal_identity == "U");
    REQUIRE(history.size.valid());
    const hgl::hgraph_ir::ConstExpr &size = lowered.graph->const_exprs[history.size.value];
    CHECK(size.kind == hgl::hgraph_ir::ConstExprKind::Parameter);
    CHECK(size.parameter == "M");

    const hgl::hgraph_ir::StructContract *swap = structure(*lowered.graph, "checks.inherited_arguments.Swap");
    REQUIRE(swap != nullptr);
    REQUIRE(swap->fields.size() == 2);
    const hgl::hgraph_ir::Type &first  = lowered.graph->types[swap->fields[0].type.value];
    const hgl::hgraph_ir::Type &second = lowered.graph->types[swap->fields[1].type.value];
    CHECK(first.nominal_identity == "Y");
    CHECK(second.nominal_identity == "X");
}

TEST_CASE("hgraph IR preserves operator and implementation requirements", "[hgraph-ir][constraints][operators]") {
    Lowered lowered{R"(
module checks.requirements

operator ordered<T>(value: T) -> T
requires T in {i64, f64}

impl fn ordered<T>(value: T) -> T
requires T in {i64, f64}
=> value
)"};
    INFO(lowered.diagnostics.render(lowered.file));
    REQUIRE_FALSE(lowered.diagnostics.has_errors());
    REQUIRE(lowered.graph);

    REQUIRE(lowered.graph->operators.size() == 1);
    CHECK(lowered.graph->operators.front().requirements.valid());
    REQUIRE(lowered.graph->callables.size() == 1);
    CHECK(lowered.graph->callables.front().requirements.valid());
}

TEST_CASE("hgraph IR lowering rejects unresolved HIR", "[hgraph-ir][completion]") {
    hir::Module                  unresolved;
    hgl::syntax::DiagnosticSink  diagnostics;
    const hgl::hgraph_ir::Module graph = hgl::hgraph_ir::lower(unresolved, diagnostics);
    CHECK(diagnostics.has_errors());
    CHECK(graph.completion == hgl::hgraph_ir::Completion::Interfaces);
}
