#include "hgraph_ir/complete.h"
#include "hgraph_ir/printer.h"

#include <catch2/catch_test_macros.hpp>

#include <cstdint>
#include <string>
#include <utility>
#include <vector>

namespace
{
    hgl::hgraph_ir::Value native_call(std::string name, std::string provider, std::uint32_t begin = 0) {
        hgl::hgraph_ir::Value value;
        value.range                  = {begin, begin + 1};
        value.operation.kind         = hgl::hgraph_ir::OperationKind::NominalOperator;
        value.operation.identity     = std::move(name);
        value.operation.provider_key = std::move(provider);
        value.operation.deferred     = false;
        return value;
    }
}  // namespace

TEST_CASE("execution completion locks a normalized provider universe", "[hgraph-ir][providers][completion]") {
    hgl::hgraph_ir::Module module;
    module.path                  = "checks.executable";
    module.completion            = hgl::hgraph_ir::Completion::Bodies;
    module.provider_requirements = {"provider.alpha", "provider.zeta"};
    module.values.push_back(native_call("mean", "provider.zeta"));
    module.values.push_back(native_call("total", "provider.alpha"));
    module.values.push_back(native_call("mean", "provider.zeta"));

    hgl::syntax::DiagnosticSink diagnostics;
    CHECK(hgl::hgraph_ir::complete_execution(
        module, hgl::hgraph_ir::ProviderPlan{{"provider.zeta", "provider.unused", "provider.alpha", "provider.alpha"}},
        diagnostics));
    CHECK_FALSE(diagnostics.has_errors());
    CHECK(module.completion == hgl::hgraph_ir::Completion::Executable);
    REQUIRE(module.provider_plan);
    CHECK(module.provider_plan->universe == std::vector<std::string>{"provider.alpha", "provider.unused", "provider.zeta"});
    CHECK(hgl::hgraph_ir::print(module).find("provider-universe [\"provider.alpha\", \"provider.unused\", \"provider.zeta\"]") !=
          std::string::npos);
}

TEST_CASE("execution completion rejects providers outside the locked universe", "[hgraph-ir][providers][completion]") {
    hgl::hgraph_ir::Module module;
    module.completion            = hgl::hgraph_ir::Completion::Bodies;
    module.provider_requirements = {"provider.alpha"};
    module.values.push_back(native_call("total", "provider.alpha", 12));

    hgl::syntax::DiagnosticSink diagnostics;
    CHECK_FALSE(hgl::hgraph_ir::complete_execution(module, hgl::hgraph_ir::ProviderPlan{{"provider.zeta"}}, diagnostics));
    REQUIRE(diagnostics.size() == 1);
    CHECK(diagnostics.diagnostics().front().category == hgl::syntax::Category::Build);
    CHECK(diagnostics.diagnostics().front().range.begin == 12);
    CHECK(diagnostics.diagnostics().front().message.find("provider.alpha") != std::string::npos);
    CHECK(module.completion == hgl::hgraph_ir::Completion::Bodies);
    CHECK_FALSE(module.provider_plan);
}

TEST_CASE("execution completion rejects deferred and unkeyed operator calls", "[hgraph-ir][providers][completion]") {
    hgl::hgraph_ir::Module module;
    module.completion = hgl::hgraph_ir::Completion::Bodies;
    module.values.push_back(native_call("map", {}));
    module.values.push_back(native_call("add", "provider.alpha"));
    module.values.back().operation.deferred = true;

    hgl::syntax::DiagnosticSink diagnostics;
    CHECK_FALSE(hgl::hgraph_ir::complete_execution(module, {}, diagnostics));
    REQUIRE(diagnostics.size() == 2);
    CHECK(diagnostics.diagnostics()[0].category == hgl::syntax::Category::Operator);
    CHECK(diagnostics.diagnostics()[0].message.find("no keyed provider") != std::string::npos);
    CHECK(diagnostics.diagnostics()[1].category == hgl::syntax::Category::Operator);
    CHECK(diagnostics.diagnostics()[1].message.find("remains deferred") != std::string::npos);
}

TEST_CASE("execution completion accepts source implementations and folded constants", "[hgraph-ir][providers][completion]") {
    hgl::hgraph_ir::Module module;
    module.completion = hgl::hgraph_ir::Completion::Bodies;
    module.callables.emplace_back();
    module.callables.back().visibility        = hgl::hgraph_ir::CallableVisibility::Implementation;
    module.callables.back().operator_identity = "checks.local.choose";

    hgl::hgraph_ir::Value source = native_call("checks.local.choose", {});
    source.operation.candidate   = hgl::hgraph_ir::CallableId{0};
    module.values.push_back(std::move(source));

    hgl::hgraph_ir::Value folded = native_call("add", {});
    folded.constant              = hgl::ir::hir::Constant{std::int64_t{3}};
    module.values.push_back(std::move(folded));

    hgl::syntax::DiagnosticSink diagnostics;
    CHECK(hgl::hgraph_ir::complete_execution(module, {}, diagnostics));
    CHECK_FALSE(diagnostics.has_errors());
    CHECK(module.completion == hgl::hgraph_ir::Completion::Executable);
}

TEST_CASE("execution completion validates its input and requirement inventory", "[hgraph-ir][providers][completion]") {
    SECTION("requires bodies") {
        hgl::hgraph_ir::Module      module;
        hgl::syntax::DiagnosticSink diagnostics;
        CHECK_FALSE(hgl::hgraph_ir::complete_execution(module, {}, diagnostics));
        REQUIRE(diagnostics.size() == 1);
        CHECK(diagnostics.diagnostics().front().message == "execution completion requires hgraph IR bodies");
    }

    SECTION("rejects empty provider keys") {
        hgl::hgraph_ir::Module module;
        module.completion = hgl::hgraph_ir::Completion::Bodies;
        hgl::syntax::DiagnosticSink diagnostics;
        CHECK_FALSE(hgl::hgraph_ir::complete_execution(module, hgl::hgraph_ir::ProviderPlan{{""}}, diagnostics));
        REQUIRE(diagnostics.size() == 1);
        CHECK(diagnostics.diagnostics().front().message.find("empty provider key") != std::string::npos);
    }

    SECTION("rejects a stale inventory") {
        hgl::hgraph_ir::Module module;
        module.completion            = hgl::hgraph_ir::Completion::Bodies;
        module.provider_requirements = {"provider.stale"};
        hgl::syntax::DiagnosticSink diagnostics;
        CHECK_FALSE(hgl::hgraph_ir::complete_execution(module, {}, diagnostics));
        REQUIRE(diagnostics.size() == 1);
        CHECK(diagnostics.diagnostics().front().message.find("inventory is inconsistent") != std::string::npos);
    }

    SECTION("rejects an invalid source implementation") {
        hgl::hgraph_ir::Module module;
        module.completion            = hgl::hgraph_ir::Completion::Bodies;
        hgl::hgraph_ir::Value source = native_call("checks.local.choose", {});
        source.operation.candidate   = hgl::hgraph_ir::CallableId{4};
        module.values.push_back(std::move(source));
        hgl::syntax::DiagnosticSink diagnostics;
        CHECK_FALSE(hgl::hgraph_ir::complete_execution(module, {}, diagnostics));
        REQUIRE(diagnostics.size() == 1);
        CHECK(diagnostics.diagnostics().front().message.find("invalid source implementation") != std::string::npos);
    }

    SECTION("rejects an unrelated source callable") {
        hgl::hgraph_ir::Module module;
        module.completion = hgl::hgraph_ir::Completion::Bodies;
        module.callables.emplace_back();
        hgl::hgraph_ir::Value source = native_call("checks.local.choose", {});
        source.operation.candidate   = hgl::hgraph_ir::CallableId{0};
        module.values.push_back(std::move(source));
        hgl::syntax::DiagnosticSink diagnostics;
        CHECK_FALSE(hgl::hgraph_ir::complete_execution(module, {}, diagnostics));
        REQUIRE(diagnostics.size() == 1);
        CHECK(diagnostics.diagnostics().front().message.find("not its implementation") != std::string::npos);
    }
}
