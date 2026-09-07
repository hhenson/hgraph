#include "ir/definite_assignment.h"

#include <cstdint>
#include <type_traits>
#include <unordered_set>

namespace hgl::ir
{
    namespace
    {
        using namespace hir;

        struct Flow
        {
            std::unordered_set<std::uint32_t> assigned{};
            bool                              reaches{true};
        };

        class Analyzer
        {
          public:
            Analyzer(const Module &module, syntax::DiagnosticSink &diagnostics) : module_{module}, diagnostics_{diagnostics} {}

            void run() {
                for (const Stmt &statement : module_.stmts) {
                    if (const auto *local = std::get_if<LocalDecl>(&statement.node);
                        local != nullptr && !local->init.valid() && local->symbol.valid()) {
                        uninitialized_.insert(local->symbol.value);
                    }
                }
                if (uninitialized_.empty()) { return; }

                for (DeclarationId declaration : module_.source_order) {
                    const Declaration &item = module_.declaration(declaration);
                    if (const auto *fn = std::get_if<FunctionDecl>(&item.node)) {
                        Flow flow;
                        if (fn->concise_body.valid()) { expression(fn->concise_body, flow); }
                        if (fn->block_body.valid()) { block(fn->block_body, flow); }
                    } else if (const auto *test = std::get_if<TestDecl>(&item.node)) {
                        Flow flow;
                        block(test->block, flow);
                    }
                }
            }

          private:
            [[nodiscard]] SymbolId place_root(ExprId id) const noexcept {
                if (!id.valid()) { return {}; }
                const Expr &value = module_.expr(id);
                if (const auto *reference = std::get_if<SymbolRef>(&value.node)) { return reference->symbol; }
                if (const auto *index = std::get_if<Index>(&value.node)) { return place_root(index->target); }
                if (const auto *field = std::get_if<Field>(&value.node)) { return place_root(field->target); }
                return {};
            }

            void reference(SymbolId symbol, syntax::SourceRange range, const Flow &flow) {
                if (symbol.valid() && uninitialized_.contains(symbol.value) && !flow.assigned.contains(symbol.value)) {
                    diagnostics_.report(syntax::Category::Type, range,
                                        "'" + module_.symbol(symbol).name + "' may be used before it is assigned");
                }
            }

            static void merge(Flow &target, const Flow &then_flow, const Flow &else_flow) {
                if (!then_flow.reaches) {
                    target = else_flow;
                    return;
                }
                if (!else_flow.reaches) {
                    target = then_flow;
                    return;
                }
                target.reaches = true;
                target.assigned.clear();
                for (const std::uint32_t symbol : then_flow.assigned) {
                    if (else_flow.assigned.contains(symbol)) { target.assigned.insert(symbol); }
                }
            }

            void expression(ExprId id, Flow &flow) {
                if (!id.valid() || !flow.reaches) { return; }
                const Expr &value = module_.expr(id);
                std::visit(
                    [&](const auto &node) {
                        using T = std::decay_t<decltype(node)>;
                        if constexpr (std::is_same_v<T, SymbolRef>) {
                            reference(node.symbol, value.range, flow);
                        } else if constexpr (std::is_same_v<T, Unary>) {
                            expression(node.operand, flow);
                        } else if constexpr (std::is_same_v<T, Binary>) {
                            expression(node.lhs, flow);
                            expression(node.rhs, flow);
                        } else if constexpr (std::is_same_v<T, Call>) {
                            expression(node.callee, flow);
                            for (const Argument &argument : node.arguments) { expression(argument.value, flow); }
                        } else if constexpr (std::is_same_v<T, Index>) {
                            expression(node.target, flow);
                            expression(node.index, flow);
                        } else if constexpr (std::is_same_v<T, Field>) {
                            expression(node.target, flow);
                        } else if constexpr (std::is_same_v<T, Sequence>) {
                            for (const SequenceElement &element : node.elements) {
                                expression(element.key, flow);
                                expression(element.value, flow);
                            }
                        } else if constexpr (std::is_same_v<T, Tuple>) {
                            for (ExprId element : node.elements) { expression(element, flow); }
                        } else if constexpr (std::is_same_v<T, Lambda>) {
                            expression(node.body, flow);
                        } else if constexpr (std::is_same_v<T, If>) {
                            expression(node.condition, flow);
                            Flow then_flow = flow;
                            block(node.then_block, then_flow);
                            Flow else_flow = flow;
                            if (node.otherwise.valid()) { expression(node.otherwise, else_flow); }
                            merge(flow, then_flow, else_flow);
                        } else if constexpr (std::is_same_v<T, BlockExpr>) {
                            block(node.block, flow);
                        } else if constexpr (std::is_same_v<T, Eval>) {
                            expression(node.callee, flow);
                            for (const Argument &argument : node.arguments) { expression(argument.value, flow); }
                        } else if constexpr (std::is_same_v<T, Construct>) {
                            for (const Argument &argument : node.arguments) { expression(argument.value, flow); }
                        }
                    },
                    value.node);
            }

            void statement(StmtId id, Flow &flow) {
                if (!id.valid() || !flow.reaches) { return; }
                const Stmt &value = module_.stmt(id);
                std::visit(
                    [&](const auto &node) {
                        using T = std::decay_t<decltype(node)>;
                        if constexpr (std::is_same_v<T, LocalDecl>) {
                            if (node.init.valid()) {
                                expression(node.init, flow);
                                flow.assigned.insert(node.symbol.value);
                            } else {
                                flow.assigned.erase(node.symbol.value);
                            }
                        } else if constexpr (std::is_same_v<T, StateDecl>) {
                            expression(node.init, flow);
                        } else if constexpr (std::is_same_v<T, LifecycleBlock>) {
                            Flow nested = flow;
                            block(node.block, nested);
                        } else if constexpr (std::is_same_v<T, WhenStmt>) {
                            if (node.condition.valid()) { expression(node.condition, flow); }
                            Flow nested = flow;
                            block(node.block, nested);
                        } else if constexpr (std::is_same_v<T, ForStmt>) {
                            expression(node.iterable, flow);
                            Flow nested = flow;
                            block(node.block, nested);
                        } else if constexpr (std::is_same_v<T, AssignStmt>) {
                            const SymbolId root = place_root(node.place);
                            const bool     direct_initialization =
                                node.op == AssignOp::Assign && std::holds_alternative<SymbolRef>(module_.expr(node.place).node);
                            if (!direct_initialization) { expression(node.place, flow); }
                            expression(node.value, flow);
                            if (root.valid()) { flow.assigned.insert(root.value); }
                        } else if constexpr (std::is_same_v<T, ReturnStmt>) {
                            expression(node.value, flow);
                            flow.reaches = false;
                        } else if constexpr (std::is_same_v<T, AssertStmt>) {
                            expression(node.condition, flow);
                        } else if constexpr (std::is_same_v<T, ExprStmt>) {
                            expression(node.expr, flow);
                        }
                    },
                    value.node);
            }

            void block(BlockId id, Flow &flow) {
                if (!id.valid() || !flow.reaches) { return; }
                for (StmtId item : module_.block(id).statements) { statement(item, flow); }
            }

            const Module                     &module_;
            syntax::DiagnosticSink           &diagnostics_;
            std::unordered_set<std::uint32_t> uninitialized_{};
        };
    }  // namespace

    void check_definite_assignment(const hir::Module &module, syntax::DiagnosticSink &diagnostics) {
        Analyzer{module, diagnostics}.run();
    }
}  // namespace hgl::ir
