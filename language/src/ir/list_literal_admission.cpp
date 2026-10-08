#include "ir/list_literal_admission.h"

#include <algorithm>
#include <string>
#include <unordered_map>
#include <unordered_set>

namespace hgl::ir {
    namespace {
        using namespace hir;
        using Facts = std::unordered_map<std::uint32_t, bool>;

        class Admission {
          public:
            Admission(const Module &module, std::span<const Expr *const> literals,
                      const std::function<bool(ExprId)> &recipe, syntax::DiagnosticSink &diagnostics)
                : module_{module}, literals_{literals.begin(), literals.end()}, recipe_{recipe}, diagnostics_{diagnostics} {}

            void run() {
                for (const auto &declaration : module_.declarations) {
                    if (const auto *fn = std::get_if<FunctionDecl>(&declaration.node)) {
                        Facts facts;
                        for (const auto &parameter : fn->signature.parameters) { facts[parameter.symbol.value] = fn->is_const || parameter.is_const; }
                        bool returned = true;
                        return_facts_.push_back(true);
                        if (fn->concise_body.valid()) { (void)expression(fn->concise_body, facts); }
                        else { (void)block(fn->block_body, facts, true, returned); }
                        return_facts_.pop_back();
                    } else if (const auto *test = std::get_if<TestDecl>(&declaration.node)) {
                        Facts facts;
                        bool returned = true;
                        (void)block(test->block, facts, true, returned);
                    }
                }
            }

          private:
            const FunctionDecl *function(SymbolId symbol) const {
                if (!symbol.valid()) { return nullptr; }
                const auto owner = module_.symbol(symbol).owner;
                return owner.valid() ? std::get_if<FunctionDecl>(&module_.declaration(owner).node) : nullptr;
            }

            SymbolId root(ExprId id) const {
                if (!id.valid()) { return {}; }
                const auto &node = module_.expr(id).node;
                if (const auto *reference = std::get_if<SymbolRef>(&node)) { return reference->symbol; }
                if (const auto *field = std::get_if<Field>(&node)) { return root(field->target); }
                if (const auto *index = std::get_if<Index>(&node)) { return root(index->target); }
                return {};
            }

            bool invoke(const Expr &site, const Call &call, const FunctionDecl &fn, Facts &caller) {
                Facts facts;
                std::vector<bool> supplied(fn.signature.parameters.size());
                std::size_t positional = 0;
                for (const auto &argument : call.arguments) {
                    const bool value = expression(argument.value, caller);
                    std::size_t index = positional;
                    if (!argument.name.empty()) {
                        const auto found = std::ranges::find_if(fn.signature.parameters, [&](const auto &parameter) {
                            return module_.symbol(parameter.symbol).name == argument.name;
                        });
                        index = static_cast<std::size_t>(found - fn.signature.parameters.begin());
                        if (index == supplied.size()) {
                            const auto pack = std::ranges::find(fn.signature.parameters, ParameterPack::Keyword, &Parameter::pack);
                            index = static_cast<std::size_t>(pack - fn.signature.parameters.begin());
                        }
                    }
                    if (index >= supplied.size()) { continue; }
                    const auto &parameter = fn.signature.parameters[index];
                    auto [entry, inserted] = facts.emplace(parameter.symbol.value, value);
                    if (!inserted) { entry->second = entry->second && value; }
                    supplied[index] = true;
                    if (argument.name.empty() && parameter.pack == ParameterPack::None) { ++positional; }
                }
                std::string key = std::to_string(site.operation.target.value) + ":";
                for (std::size_t index = 0; index < supplied.size(); ++index) {
                    const auto &parameter = fn.signature.parameters[index];
                    if (!supplied[index]) { facts[parameter.symbol.value] = expression(parameter.default_value, facts); }
                    key += facts[parameter.symbol.value] ? '1' : '0';
                }
                if (const auto found = results_.find(key); found != results_.end()) { return found->second; }
                // A cycle with only constant inputs is provisionally cold;
                // every nonrecursive expression still has to prove that fact.
                // Do not memoize participants before the cycle is discharged.
                if (!active_.insert(key).second) {
                    recursive_.insert(active_.begin(), active_.end());
                    return fn.capabilities.empty() && std::ranges::all_of(facts, [](const auto &fact) { return fact.second; });
                }
                bool returned = true;
                return_facts_.push_back(true);
                const bool result = fn.concise_body.valid() ? expression(fn.concise_body, facts)
                    : block(fn.block_body, facts, true, returned) && returned;
                const bool complete = result && return_facts_.back() && fn.capabilities.empty();
                return_facts_.pop_back();
                active_.erase(key);
                if (!recursive_.contains(key)) { results_.emplace(std::move(key), complete); }
                return complete;
            }

            bool expression(ExprId id, Facts &facts, bool control = true) {
                if (!id.valid()) { return true; }
                const auto &value = module_.expr(id);
                return std::visit([&](const auto &node) -> bool {
                    using T = std::decay_t<decltype(node)>;
                    if constexpr (std::is_same_v<T, Literal>) { return true; }
                    else if constexpr (std::is_same_v<T, SymbolRef>) {
                        if (const auto found = facts.find(node.symbol.value); found != facts.end()) { return found->second; }
                        return recipe_(id);
                    } else if constexpr (std::is_same_v<T, Unary>) { return expression(node.operand, facts); }
                    else if constexpr (std::is_same_v<T, Binary>) {
                        const bool left = expression(node.lhs, facts);
                        return expression(node.rhs, facts) && left;
                    } else if constexpr (std::is_same_v<T, Index>) {
                        const bool target = expression(node.target, facts);
                        return expression(node.index, facts) && target;
                    } else if constexpr (std::is_same_v<T, Field>) {
                        return value.operation.kind != OperationKind::Capability && expression(node.target, facts);
                    } else if constexpr (std::is_same_v<T, Tuple> || std::is_same_v<T, Sequence>) {
                        bool constant = true;
                        for (const auto &element : node.elements) {
                            const ExprId child = [&] { if constexpr (std::is_same_v<T, Tuple>) { return element; } else { return element.value; } }();
                            constant = expression(child, facts) && constant;
                        }
                        if (literals_.contains(&value) && !constant && reported_.insert(&value).second) {
                            diagnostics_.report(syntax::Category::Phase, value.range,
                                "a nonempty ordinary list literal requires constant elements");
                        }
                        return constant;
                    } else if constexpr (std::is_same_v<T, Call> || std::is_same_v<T, Construct>) {
                        if constexpr (std::is_same_v<T, Call>) {
                            if (const auto *fn = function(value.operation.target); fn && fn->is_const) { return invoke(value, node, *fn, facts); }
                        }
                        bool constant = true;
                        for (const auto &argument : node.arguments) { constant = expression(argument.value, facts) && constant; }
                        if constexpr (std::is_same_v<T, Call>) {
                            if (value.operation.identity == "push" && !node.arguments.empty()) {
                                const auto target = root(node.arguments.front().value);
                                if (target.valid()) { facts[target.value] = control && constant; }
                            }
                            if (value.operation.kind == OperationKind::Capability || has_effect(value.effects, Effect::UseCapability)) { return false; }
                            if (const auto *fn = function(value.operation.target); fn && !fn->is_const) { return false; }
                            for (const auto &native : module_.native_functions) {
                                if ((native.symbol == value.operation.target ||
                                     std::ranges::find(value.operation.native_candidates, native.symbol) != value.operation.native_candidates.end()) &&
                                    (!native.capabilities.empty() ||
                                    std::ranges::find(native.phases, NativePhase::Wiring) == native.phases.end())) { return false; }
                            }
                        }
                        return constant;
                    } else if constexpr (std::is_same_v<T, If>) {
                        const bool condition = expression(node.condition, facts);
                        auto then_facts = facts;
                        bool returned = true;
                        const bool then_value = block(node.then_block, then_facts, control && condition, returned);
                        auto else_facts = facts;
                        const bool else_value = expression(node.otherwise, else_facts, control && condition);
                        for (auto &[symbol, constant] : facts) {
                            constant = then_facts[symbol] && else_facts[symbol];
                        }
                        return condition && then_value && else_value && returned;
                    } else if constexpr (std::is_same_v<T, BlockExpr>) {
                        bool returned = true;
                        return block(node.block, facts, control, returned) && returned;
                    } else if constexpr (std::is_same_v<T, Eval>) {
                        for (const auto &argument : node.arguments) { (void)expression(argument.value, facts); }
                        return false;
                    } else { return recipe_(id); }
                }, value.node);
            }

            bool block(BlockId id, Facts &facts, bool control, bool &returned) {
                if (!id.valid()) { return true; }
                const auto &body = module_.block(id);
                for (const auto statement_id : body.statements) {
                    std::visit([&](const auto &node) {
                        using T = std::decay_t<decltype(node)>;
                        if constexpr (std::is_same_v<T, LocalDecl> || std::is_same_v<T, StateDecl>) {
                            facts[node.symbol.value] = expression(node.init, facts);
                            if constexpr (std::is_same_v<T, StateDecl>) { facts[node.symbol.value] = false; }
                        } else if constexpr (std::is_same_v<T, AssignStmt>) {
                            const auto target = root(node.place);
                            const bool item = expression(node.value, facts);
                            const bool whole = std::holds_alternative<SymbolRef>(module_.expr(node.place).node);
                            if (target.valid()) { facts[target.value] = control && item &&
                                ((whole && node.op == AssignOp::Assign) || facts[target.value]); }
                        } else if constexpr (std::is_same_v<T, ReturnStmt>) {
                            returned = expression(node.value, facts) && control && returned;
                            if (!return_facts_.empty()) { return_facts_.back() = return_facts_.back() && returned; }
                        } else if constexpr (std::is_same_v<T, ExprStmt>) { (void)expression(node.expr, facts, control); }
                        else if constexpr (std::is_same_v<T, AssertStmt>) {
                            (void)expression(node.condition, facts);
                            (void)block(node.raises_block, facts, control, returned);
                        } else if constexpr (std::is_same_v<T, LifecycleBlock> || std::is_same_v<T, WhenStmt>) {
                            if constexpr (std::is_same_v<T, WhenStmt>) { (void)expression(node.condition, facts); }
                            (void)block(node.block, facts, control, returned);
                        } else if constexpr (std::is_same_v<T, ForStmt> || std::is_same_v<T, WhileStmt>) {
                            const bool condition = [&] { if constexpr (std::is_same_v<T, ForStmt>) { return expression(node.iterable, facts); }
                                else { return expression(node.condition, facts); } }();
                            if constexpr (std::is_same_v<T, ForStmt>) { for (auto symbol : node.bindings) { facts[symbol.value] = condition; } }
                            Facts before;
                            do {
                                before = facts;
                                const bool iteration = [&] {
                                    if constexpr (std::is_same_v<T, WhileStmt>) { return expression(node.condition, facts); }
                                    else { return condition; }
                                }();
                                (void)block(node.block, facts, control && iteration, returned);
                                for (const auto &[symbol, constant] : before) { facts[symbol] = facts[symbol] && constant; }
                            } while (facts != before);
                        } else if constexpr (std::is_same_v<T, YieldStmt>) {
                            (void)expression(node.time, facts); (void)expression(node.value, facts);
                        }
                    }, module_.stmt(statement_id).node);
                }
                return expression(body.tail, facts);
            }

            const Module &module_;
            std::unordered_set<const Expr *> literals_;
            const std::function<bool(ExprId)> &recipe_;
            syntax::DiagnosticSink &diagnostics_;
            std::unordered_set<const Expr *> reported_;
            std::unordered_set<std::string> active_;
            std::unordered_set<std::string> recursive_;
            std::unordered_map<std::string, bool> results_;
            std::vector<bool> return_facts_;
        };
    }
    void check_list_literal_admission(const hir::Module &module, std::span<const hir::Expr *const> literals,
                                     const std::function<bool(hir::ExprId)> &recipe, syntax::DiagnosticSink &diagnostics) {
        if (!literals.empty()) { Admission{module, literals, recipe, diagnostics}.run(); }
    }
}
