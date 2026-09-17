#include "hgraph_ir/lower.h"
#include "hgraph_ir/uses.h"
#include "ir/lower.h"
#include "ir/type_check.h"
#include "semantics/resolve.h"
#include "syntax/parser.h"

#include <catch2/catch_test_macros.hpp>

#include <optional>
#include <string>
#include <string_view>

namespace
{
    namespace gir = hgl::hgraph_ir;
    namespace hir = hgl::ir::hir;

    bool has_operator(std::string_view) { return true; }

    struct Lowered
    {
        hgl::syntax::SourceFile     file;
        hgl::syntax::DiagnosticSink diagnostics{};
        std::optional<gir::Module>  graph{};

        explicit Lowered(std::string text) : file{"test.hgl", std::move(text)} {
            hgl::syntax::ast::Module ast = hgl::syntax::parse(file, diagnostics);
            if (diagnostics.has_errors()) { return; }
            hgl::semantics::ResolvedModule resolved = hgl::semantics::resolve(file, ast, has_operator, diagnostics);
            if (diagnostics.has_errors()) { return; }
            hir::Module language = hgl::ir::lower_to_hir(ast, resolved, diagnostics);
            if (diagnostics.has_errors()) { return; }
            const hgl::ir::OperatorResolver operators = [](const hir::Module &, const hgl::ir::OperatorQuery &query) {
                hgl::ir::OperatorSelection selected;
                selected.result   = query.expected_result;
                selected.deferred = true;
                return selected;
            };
            if (!hgl::ir::complete_hir(language, operators, diagnostics)) { return; }
            graph = gir::lower(language, diagnostics);
        }

        [[nodiscard]] const gir::Callable &callable(std::string_view name) const {
            for (const gir::Callable &item : graph->callables) {
                if (item.identity.ends_with(name)) { return item; }
            }
            FAIL("no callable " << name);
            return graph->callables.front();
        }

        /// The nth binding with this name and kind, in declaration order.
        [[nodiscard]] gir::BindingId binding(std::string_view name, gir::BindingKind kind, std::size_t occurrence = 0) const {
            std::size_t seen = 0;
            for (std::size_t index = 0; index < graph->bindings.size(); ++index) {
                const gir::Binding &item = graph->bindings[index];
                if (item.name != name || item.kind != kind) { continue; }
                if (seen++ == occurrence) { return gir::BindingId{static_cast<std::uint32_t>(index)}; }
            }
            FAIL("no binding " << name);
            return {};
        }
    };
}  // namespace

TEST_CASE("binding uses count the references a body reaches", "[hgraph-ir][uses]") {
    Lowered lowered{R"(
module checks.uses
use hgraph.std::{null_sink}

export fn forward(value: f64, other: f64) -> f64 => value

export fn count_items(values: list<f64>, const limit: i64) -> i64 {
    state total: i64 = 0
    state unused: i64 = 0
    when {
        for value in elements(values) {
            total += 1
        }
        if total > limit {
            return total
        }
    }
}

export fn sinks(book: map<str, f64>, offset: f64) {
    for key, value in items(book) {
        null_sink(value)
    }
}

export fn writes(value: f64) -> f64 {
    var latest: f64 = 0.0
    var running: f64 = 0.0
    when {
        latest = value
        running += value
        return running
    }
}
)"};
    INFO(lowered.diagnostics.render(lowered.file));
    REQUIRE(lowered.graph);

    SECTION("a concise composition reaches only the port it returns") {
        const gir::Callable    &forward = lowered.callable("forward");
        const gir::BindingUses  uses    = gir::binding_uses(*lowered.graph, forward.concise_body);
        CHECK(uses.uses(forward.parameters[0].binding));
        CHECK_FALSE(uses.uses(forward.parameters[1].binding));
    }

    SECTION("a runtime body reaches state through assignments, comparisons and returns, not through declarations") {
        const gir::Callable   &count = lowered.callable("count_items");
        const gir::BindingUses uses  = gir::binding_uses(*lowered.graph, count.block_body);
        CHECK(uses.count(lowered.binding("total", gir::BindingKind::State)) == 3);  // +=, >, return
        CHECK_FALSE(uses.uses(lowered.binding("unused", gir::BindingKind::State)));
        CHECK(uses.uses(count.parameters[0].binding));
        CHECK(uses.uses(count.parameters[1].binding));
    }

    SECTION("a plain assignment target is referenced but not read; a compound one is read") {
        const gir::Callable   &writes = lowered.callable("writes");
        const gir::BindingUses uses   = gir::binding_uses(*lowered.graph, writes.block_body);
        const gir::BindingId   latest = lowered.binding("latest", gir::BindingKind::LocalVar);
        CHECK(uses.count(latest) == 1);
        CHECK_FALSE(uses.is_read(latest));
        CHECK(uses.is_read(lowered.binding("running", gir::BindingKind::LocalVar)));
    }

    SECTION("loop bindings the body never reads are unreachable") {
        const gir::Callable   &sinks = lowered.callable("sinks");
        const gir::BindingUses uses  = gir::binding_uses(*lowered.graph, sinks.block_body);
        CHECK(uses.uses(sinks.parameters[0].binding));
        CHECK_FALSE(uses.uses(sinks.parameters[1].binding));
        CHECK_FALSE(uses.uses(lowered.binding("key", gir::BindingKind::LoopValue)));
        // The second loop binding named `value`: count_items declares the first.
        CHECK(uses.count(lowered.binding("value", gir::BindingKind::LoopValue, 1)) == 1);
    }
}
