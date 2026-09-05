#include "ir/constraint_solver.h"

#include <algorithm>
#include <ranges>
#include <type_traits>
#include <utility>

namespace hgl::ir::detail
{
    using namespace hir;

    ConstraintSolver::ConstraintSolver(Module &module, CanonicalTypes &types, const OperatorResolver &resolve_operator,
                                       syntax::DiagnosticSink &diagnostics)
        : module_{module}, types_{types}, resolve_operator_{resolve_operator}, diagnostics_{diagnostics} {}

    void ConstraintSolver::fail(std::string message) {
        if (failure_detail_.empty()) { failure_detail_ = std::move(message); }
    }

    bool ConstraintSolver::same_symbolic_type(TypeId lhs, TypeId rhs) const noexcept {
        lhs = types_.canonical(lhs);
        rhs = types_.canonical(rhs);
        return lhs.valid() && rhs.valid() && lhs == rhs;
    }

    std::optional<std::string> ConstraintSolver::string_value(ExprId value) const {
        if (!value.valid()) { return std::nullopt; }
        const Expr &expression = module_.expr(value);
        if (expression.constant) {
            if (const auto *text = std::get_if<std::string>(&*expression.constant)) { return *text; }
        }
        if (const auto *literal = std::get_if<Literal>(&expression.node)) {
            if (const auto *text = std::get_if<std::string>(&literal->value)) { return *text; }
        }
        return std::nullopt;
    }

    bool ConstraintSolver::same_value(ExprId lhs, ExprId rhs) const {
        if (const auto left = string_value(lhs)) {
            const auto right = string_value(rhs);
            return right && *left == *right;
        }
        return types_.same_value(lhs, rhs);
    }

    bool ConstraintSolver::operand_equivalent(const Operand &lhs, const Operand &rhs) const {
        if (lhs.kind != rhs.kind) { return false; }
        switch (lhs.kind) {
            case OperandKind::Type: return types_.same(lhs.type, rhs.type);
            case OperandKind::Value:
                if (lhs.value.valid() && rhs.value.valid()) { return same_value(lhs.value, rhs.value); }
                if (lhs.variable.valid() && rhs.variable.valid()) { return lhs.variable == rhs.variable; }
                if (lhs.variable.valid() && rhs.value.valid()) {
                    const auto *reference = std::get_if<SymbolRef>(&module_.expr(rhs.value).node);
                    return reference != nullptr && reference->symbol == lhs.variable;
                }
                if (rhs.variable.valid() && lhs.value.valid()) {
                    const auto *reference = std::get_if<SymbolRef>(&module_.expr(lhs.value).node);
                    return reference != nullptr && reference->symbol == rhs.variable;
                }
                return false;
            case OperandKind::TypeSet:
                return lhs.types.size() == rhs.types.size() && std::ranges::all_of(lhs.types, [&](TypeId item) {
                           return std::ranges::any_of(rhs.types, [&](TypeId candidate) { return types_.same(item, candidate); });
                       });
            case OperandKind::ValueSet:
                return lhs.values.size() == rhs.values.size() && std::ranges::all_of(lhs.values, [&](ExprId item) {
                           return std::ranges::any_of(rhs.values, [&](ExprId candidate) { return same_value(item, candidate); });
                       });
            case OperandKind::FieldSet:
                {
                    std::vector<std::string> left  = lhs.fields;
                    std::vector<std::string> right = rhs.fields;
                    std::ranges::sort(left);
                    std::ranges::sort(right);
                    return left == right;
                }
            case OperandKind::Boolean: return lhs.known == rhs.known && (!lhs.known || lhs.boolean == rhs.boolean);
            case OperandKind::Invalid: return false;
        }
        return false;
    }

    bool ConstraintSolver::relation_implies(const ConstraintRelation &premise, GenericSubstitution &premise_substitution,
                                            const ConstraintRelation &goal, GenericSubstitution &goal_substitution) {
        if (premise.op != goal.op) { return false; }
        const Operand premise_lhs = operand(premise.lhs, premise_substitution);
        const Operand goal_lhs    = operand(goal.lhs, goal_substitution);
        if (premise.op == ConstraintRelationOp::Is) {
            return premise.category == goal.category && operand_equivalent(premise_lhs, goal_lhs);
        }
        const Operand premise_rhs = operand(premise.rhs, premise_substitution);
        const Operand goal_rhs    = operand(goal.rhs, goal_substitution);
        if (premise.op == ConstraintRelationOp::Equal) {
            return (operand_equivalent(premise_lhs, goal_lhs) && operand_equivalent(premise_rhs, goal_rhs)) ||
                   (operand_equivalent(premise_lhs, goal_rhs) && operand_equivalent(premise_rhs, goal_lhs));
        }
        if (!operand_equivalent(premise_lhs, goal_lhs)) { return false; }
        if (premise_rhs.kind == OperandKind::TypeSet && goal_rhs.kind == OperandKind::TypeSet) {
            return std::ranges::all_of(premise_rhs.types, [&](TypeId item) {
                return std::ranges::any_of(goal_rhs.types, [&](TypeId candidate) { return types_.same(item, candidate); });
            });
        }
        if (premise_rhs.kind == OperandKind::ValueSet && goal_rhs.kind == OperandKind::ValueSet) {
            return std::ranges::all_of(premise_rhs.values, [&](ExprId item) {
                return std::ranges::any_of(goal_rhs.values, [&](ExprId candidate) { return same_value(item, candidate); });
            });
        }
        if (premise_rhs.kind == OperandKind::FieldSet && goal_rhs.kind == OperandKind::FieldSet) {
            return std::ranges::all_of(premise_rhs.fields,
                                       [&](const std::string &item) { return std::ranges::contains(goal_rhs.fields, item); });
        }
        return operand_equivalent(premise_rhs, goal_rhs);
    }

    bool ConstraintSolver::atomic_equivalent(ConstraintId premise_id, GenericSubstitution &premise_substitution,
                                             ConstraintId goal_id, GenericSubstitution &goal_substitution) {
        const Constraint &premise = module_.constraint(premise_id);
        const Constraint &goal    = module_.constraint(goal_id);
        if (const auto *premise_relation = std::get_if<ConstraintRelation>(&premise.node)) {
            const auto *goal_relation = std::get_if<ConstraintRelation>(&goal.node);
            return goal_relation != nullptr &&
                   relation_implies(*premise_relation, premise_substitution, *goal_relation, goal_substitution);
        }
        if (const auto *premise_requirement = std::get_if<OperatorRequirement>(&premise.node)) {
            const auto *goal_requirement = std::get_if<OperatorRequirement>(&goal.node);
            if (goal_requirement == nullptr ||
                !identity_matches(operator_identity(premise_requirement->op), operator_identity(goal_requirement->op)) ||
                premise_requirement->arguments.size() != goal_requirement->arguments.size()) {
                return false;
            }
            for (std::size_t index = 0; index < premise_requirement->arguments.size(); ++index) {
                if (!operand_equivalent(operand(premise_requirement->arguments[index], premise_substitution),
                                        operand(goal_requirement->arguments[index], goal_substitution))) {
                    return false;
                }
            }
            if (premise_requirement->result.valid() != goal_requirement->result.valid()) { return false; }
            return !premise_requirement->result.valid() || types_.same(premise_substitution.apply(premise_requirement->result),
                                                                       goal_substitution.apply(goal_requirement->result));
        }
        if (const auto *premise_call = std::get_if<ConstraintCall>(&premise.node)) {
            const auto *goal_call = std::get_if<ConstraintCall>(&goal.node);
            if (goal_call == nullptr ||
                !identity_matches(operator_identity(premise_call->function), operator_identity(goal_call->function)) ||
                premise_call->arguments.size() != goal_call->arguments.size()) {
                return false;
            }
            for (std::size_t index = 0; index < premise_call->arguments.size(); ++index) {
                if (!operand_equivalent(operand(premise_call->arguments[index], premise_substitution),
                                        operand(goal_call->arguments[index], goal_substitution))) {
                    return false;
                }
            }
            return true;
        }
        if (const auto *premise_not = std::get_if<ConstraintNot>(&premise.node)) {
            const auto *goal_not = std::get_if<ConstraintNot>(&goal.node);
            return goal_not != nullptr &&
                   atomic_equivalent(premise_not->operand, premise_substitution, goal_not->operand, goal_substitution);
        }
        return operand_equivalent(operand(premise_id, premise_substitution), operand(goal_id, goal_substitution));
    }

    bool ConstraintSolver::premise_implies(ConstraintId premise_id, GenericSubstitution &premise_substitution, ConstraintId goal,
                                           GenericSubstitution &goal_substitution) {
        const Constraint &premise = module_.constraint(premise_id);
        if (const auto *logic = std::get_if<ConstraintLogic>(&premise.node)) {
            const bool lhs = premise_implies(logic->lhs, premise_substitution, goal, goal_substitution);
            const bool rhs = premise_implies(logic->rhs, premise_substitution, goal, goal_substitution);
            return logic->op == ConstraintLogicOp::And ? lhs || rhs : lhs && rhs;
        }
        return atomic_equivalent(premise_id, premise_substitution, goal, goal_substitution);
    }

    bool ConstraintSolver::premises_prove(ConstraintId goal_id, GenericSubstitution &goal_substitution,
                                          std::span<const ConstraintPremise> premises) {
        const Constraint &goal = module_.constraint(goal_id);
        if (const auto *logic = std::get_if<ConstraintLogic>(&goal.node)) {
            const bool lhs = premises_prove(logic->lhs, goal_substitution, premises);
            const bool rhs = premises_prove(logic->rhs, goal_substitution, premises);
            return logic->op == ConstraintLogicOp::And ? lhs && rhs : lhs || rhs;
        }
        GenericSubstitution empty{module_, types_};
        return std::ranges::any_of(premises, [&](const ConstraintPremise &premise) {
            if (!premise.requirement.valid()) { return false; }
            GenericSubstitution &bindings = premise.substitution == nullptr ? empty : *premise.substitution;
            return premise_implies(premise.requirement, bindings, goal_id, goal_substitution);
        });
    }

    ConstraintSolver::Operand ConstraintSolver::type_operand(TypeId type, GenericSubstitution &substitution) {
        type = types_.canonical(type);
        if (!type.valid()) { return {}; }
        const Type &source = module_.type(type);
        if (source.kind == TypeKind::Symbol && source.symbol.valid() &&
            module_.symbol(source.symbol).kind == SymbolKind::TypeParameter && !substitution.has_type(source.symbol)) {
            return Operand{.kind = OperandKind::Type, .known = false, .variable = source.symbol, .type = type};
        }
        return Operand{.kind = OperandKind::Type, .known = true, .type = substitution.apply(type)};
    }

    ConstraintSolver::Operand ConstraintSolver::value_operand(ExprId value, GenericSubstitution &substitution) {
        if (!value.valid()) { return {}; }
        const Expr &source = module_.expr(value);
        if (const auto *reference = std::get_if<SymbolRef>(&source.node);
            reference && reference->symbol.valid() && module_.symbol(reference->symbol).kind == SymbolKind::ConstParameter &&
            !substitution.has_value(reference->symbol)) {
            return Operand{.kind = OperandKind::Value, .known = false, .variable = reference->symbol, .value = value};
        }
        return Operand{.kind = OperandKind::Value, .known = true, .value = substitution.apply_value(value)};
    }

    bool ConstraintSolver::is_struct(TypeId type) const noexcept {
        type = types_.canonical(type);
        if (!type.valid()) { return false; }
        const Type &value = module_.type(type);
        if (value.kind != TypeKind::Symbol || !value.symbol.valid()) { return false; }
        const Symbol &symbol = module_.symbol(value.symbol);
        return symbol.kind == SymbolKind::Struct && symbol.owner.valid() &&
               std::holds_alternative<StructDecl>(module_.declaration(symbol.owner).node);
    }

    void ConstraintSolver::append_fields(TypeId type_id, std::vector<EffectiveField> &fields) {
        type_id = types_.canonical(type_id);
        if (!is_struct(type_id)) { return; }
        const Type       &applied = module_.type(type_id);
        const Symbol     &symbol  = module_.symbol(applied.symbol);
        const StructDecl &decl    = std::get<StructDecl>(module_.declaration(symbol.owner).node);

        GenericSubstitution substitution{module_, types_};
        for (std::size_t index = 0; index < decl.generics.size() && index < applied.arguments.size(); ++index) {
            const GenericParameter &generic  = decl.generics[index];
            const TypeArgument     &argument = applied.arguments[index];
            if (generic.is_const && argument.kind == TypeArgumentKind::Value) {
                (void)substitution.bind_value(generic.symbol, argument.value);
            } else if (!generic.is_const && argument.kind == TypeArgumentKind::Type) {
                (void)substitution.bind_type(generic.symbol, argument.type);
            }
        }

        for (TypeId parent : decl.parents) { append_fields(substitution.apply(parent), fields); }
        for (const StructField &field : decl.fields) {
            const auto   existing = std::ranges::find(fields, field.name, &EffectiveField::name);
            const TypeId resolved = substitution.apply(field.type);
            if (existing == fields.end()) {
                fields.push_back(EffectiveField{field.name, resolved});
            } else {
                existing->type = resolved;
            }
        }
    }

    std::vector<ConstraintSolver::EffectiveField> ConstraintSolver::effective_fields(TypeId type) {
        std::vector<EffectiveField> result;
        append_fields(type, result);
        return result;
    }

    ConstraintSolver::Operand ConstraintSolver::operand(ConstraintId id, GenericSubstitution &substitution) {
        if (!id.valid()) { return {}; }
        const Constraint &constraint = module_.constraint(id);
        return std::visit(
            [&](const auto &node) -> Operand {
                using T = std::decay_t<decltype(node)>;
                if constexpr (std::is_same_v<T, ConstraintSymbol>) {
                    if (!node.symbol.valid()) { return {}; }
                    const Symbol &symbol = module_.symbol(node.symbol);
                    if (symbol.kind == SymbolKind::TypeParameter) {
                        if (const auto bound = substitution.type_binding(node.symbol)) {
                            return Operand{.kind = OperandKind::Type, .known = true, .type = *bound};
                        }
                        const TypeId symbolic = types_.make(TypeKind::Symbol, {}, node.symbol);
                        return Operand{.kind = OperandKind::Type, .known = false, .variable = node.symbol, .type = symbolic};
                    }
                    if (symbol.kind == SymbolKind::ConstParameter) {
                        if (const auto bound = substitution.value_binding(node.symbol)) {
                            return Operand{.kind = OperandKind::Value, .known = true, .value = *bound};
                        }
                        return Operand{.kind = OperandKind::Value, .known = false, .variable = node.symbol};
                    }
                    if (symbol.kind == SymbolKind::Struct) {
                        return Operand{
                            .kind = OperandKind::Type, .known = true, .type = types_.make(TypeKind::Symbol, {}, node.symbol)};
                    }
                    return {};
                } else if constexpr (std::is_same_v<T, ConstraintType>) {
                    return type_operand(node.type, substitution);
                } else if constexpr (std::is_same_v<T, ConstraintValue>) {
                    return value_operand(node.value, substitution);
                } else if constexpr (std::is_same_v<T, ConstraintSet>) {
                    Operand result;
                    bool    all_types  = true;
                    bool    all_values = true;
                    result.known       = true;
                    for (ConstraintId element : node.elements) {
                        Operand item = operand(element, substitution);
                        result.known = result.known && item.known;
                        if (item.kind == OperandKind::Type) {
                            result.types.push_back(item.type);
                            all_values = false;
                        } else if (item.kind == OperandKind::Value) {
                            result.values.push_back(item.value);
                            all_types = false;
                        } else {
                            all_types  = false;
                            all_values = false;
                        }
                    }
                    if (all_types) {
                        result.kind = OperandKind::TypeSet;
                    } else if (all_values) {
                        result.kind = OperandKind::ValueSet;
                    }
                    return result;
                } else if constexpr (std::is_same_v<T, ConstraintCall>) {
                    if (!node.function.valid()) { return {}; }
                    const Symbol      &function = module_.symbol(node.function);
                    const std::string &name     = function.external_name.empty() ? function.name : function.external_name;
                    if (name == "schema" && node.arguments.size() == 1U) { return operand(node.arguments.front(), substitution); }
                    if ((name == "fields" || name == "keys") && node.arguments.size() == 1U) {
                        Operand source = operand(node.arguments.front(), substitution);
                        if (!source.known) { return Operand{.kind = OperandKind::FieldSet, .known = false}; }
                        if (source.kind != OperandKind::Type || !is_struct(source.type)) { return {}; }
                        Operand result{.kind = OperandKind::FieldSet, .known = true};
                        for (const EffectiveField &field : effective_fields(source.type)) { result.fields.push_back(field.name); }
                        return result;
                    }
                    if (name == "has_fields" && node.arguments.size() == 2U) {
                        Operand source = operand(node.arguments[0], substitution);
                        Operand names  = operand(node.arguments[1], substitution);
                        if (!source.known || !names.known) { return Operand{.kind = OperandKind::Boolean, .known = false}; }
                        if (source.kind != OperandKind::Type || !is_struct(source.type) || names.kind != OperandKind::ValueSet) {
                            return {};
                        }
                        std::vector<std::string> available;
                        for (const EffectiveField &field : effective_fields(source.type)) { available.push_back(field.name); }
                        bool result = true;
                        for (ExprId item : names.values) {
                            const auto name_value = string_value(item);
                            if (!name_value || !std::ranges::contains(available, *name_value)) { result = false; }
                        }
                        return Operand{.kind = OperandKind::Boolean, .known = true, .boolean = result};
                    }
                    if (name == "field_type" && node.arguments.size() == 2U) {
                        Operand source     = operand(node.arguments[0], substitution);
                        Operand name_value = operand(node.arguments[1], substitution);
                        if (!source.known || !name_value.known) { return Operand{.kind = OperandKind::Type, .known = false}; }
                        if (source.kind != OperandKind::Type || name_value.kind != OperandKind::Value) { return {}; }
                        const auto field_name = string_value(name_value.value);
                        if (!field_name || !is_struct(source.type)) { return {}; }
                        const auto fields = effective_fields(source.type);
                        const auto found  = std::ranges::find(fields, *field_name, &EffectiveField::name);
                        if (found == fields.end()) { return {}; }
                        return Operand{.kind = OperandKind::Type, .known = true, .type = found->type};
                    }
                    return {};
                } else {
                    return {};
                }
            },
            constraint.node);
    }

    ConstraintSolver::Truth ConstraintSolver::evaluate_relation(const ConstraintRelation &relation,
                                                                GenericSubstitution      &substitution) {
        Operand lhs = operand(relation.lhs, substitution);
        if (relation.op == ConstraintRelationOp::Is) {
            if (!lhs.known) { return Truth::Unresolved; }
            if (lhs.kind != OperandKind::Type || relation.category != "struct") { return Truth::False; }
            return is_struct(lhs.type) ? Truth::True : Truth::False;
        }
        Operand rhs = operand(relation.rhs, substitution);
        if (!lhs.known || !rhs.known) { return Truth::Unresolved; }
        if (relation.op == ConstraintRelationOp::Equal) {
            if (lhs.kind == OperandKind::Type && rhs.kind == OperandKind::Type) {
                return types_.same(lhs.type, rhs.type) ? Truth::True : Truth::False;
            }
            if (lhs.kind == OperandKind::Value && rhs.kind == OperandKind::Value) {
                return same_value(lhs.value, rhs.value) ? Truth::True : Truth::False;
            }
            if (lhs.kind == OperandKind::FieldSet && rhs.kind == OperandKind::FieldSet) {
                std::ranges::sort(lhs.fields);
                std::ranges::sort(rhs.fields);
                return lhs.fields == rhs.fields ? Truth::True : Truth::False;
            }
            return Truth::False;
        }
        if (lhs.kind == OperandKind::Type && rhs.kind == OperandKind::TypeSet) {
            return std::ranges::any_of(rhs.types, [&](TypeId candidate) { return types_.same(lhs.type, candidate); })
                       ? Truth::True
                       : Truth::False;
        }
        if (lhs.kind == OperandKind::Value && rhs.kind == OperandKind::ValueSet) {
            return std::ranges::any_of(rhs.values, [&](ExprId candidate) { return same_value(lhs.value, candidate); })
                       ? Truth::True
                       : Truth::False;
        }
        if (lhs.kind == OperandKind::Value && rhs.kind == OperandKind::FieldSet) {
            const auto name = string_value(lhs.value);
            return name && std::ranges::contains(rhs.fields, *name) ? Truth::True : Truth::False;
        }
        return Truth::False;
    }

    std::string ConstraintSolver::operator_identity(SymbolId symbol) const {
        if (!symbol.valid()) { return {}; }
        const Symbol &value = module_.symbol(symbol);
        return value.external_name.empty() ? value.name : value.external_name;
    }

    bool ConstraintSolver::identity_matches(std::string_view lhs, std::string_view rhs) noexcept {
        const auto trim = [](std::string_view value) { return value.ends_with('_') ? value.substr(0, value.size() - 1U) : value; };
        return lhs == rhs || trim(lhs) == trim(rhs);
    }

    ConstraintSolver::Truth ConstraintSolver::evaluate_operator(const OperatorRequirement &requirement,
                                                                GenericSubstitution &substitution, syntax::SourceRange range,
                                                                std::span<const ConstraintPremise> premises) {
        if (!requirement.op.valid()) { return Truth::False; }
        OperatorQuery        query;
        std::vector<Operand> arguments;
        query.identity = operator_identity(requirement.op);
        query.range    = range;
        for (ConstraintId argument : requirement.arguments) {
            Operand value = operand(argument, substitution);
            if (!value.known) { return Truth::Unresolved; }
            OperatorArgument query_argument;
            if (value.kind == OperandKind::Type) {
                query_argument.type       = value.type;
                query_argument.phase      = Phase::Wiring;
                query_argument.value_kind = ValueKind::Signal;
            } else if (value.kind == OperandKind::Value) {
                const Expr &expression    = module_.expr(value.value);
                query_argument.type       = substitution.apply(expression.type);
                query_argument.phase      = Phase::Constant;
                query_argument.value_kind = ValueKind::Constant;
                query_argument.constant   = expression.constant;
            } else {
                fail("operator requirement arguments must resolve to types or const values");
                return Truth::False;
            }
            arguments.push_back(value);
            query.arguments.push_back(std::move(query_argument));
        }
        if (requirement.result.valid()) {
            Operand result = type_operand(requirement.result, substitution);
            if (!result.known) { return Truth::Unresolved; }
            query.expected_result = result.type;
        }

        const Symbol &symbol = module_.symbol(requirement.op);
        if (symbol.kind == SymbolKind::ImportedOperator) {
            if (!resolve_operator_) {
                fail("native operator resolver is unavailable");
                return Truth::Unresolved;
            }
            OperatorSelection selection = resolve_operator_(module_, query);
            if (!selection.error.empty()) {
                fail(std::move(selection.error));
                return Truth::False;
            }
            return selection.deferred ? Truth::Unresolved : Truth::True;
        }

        if (symbol.kind != SymbolKind::Operator || !symbol.owner.valid()) { return Truth::False; }
        const auto *contract = std::get_if<OperatorDecl>(&module_.declaration(symbol.owner).node);
        if (contract == nullptr || contract->signature.parameters.size() != arguments.size()) { return Truth::False; }

        GenericSubstitution contract_substitution{module_, types_};
        for (std::size_t index = 0; index < arguments.size(); ++index) {
            const Parameter &parameter = contract->signature.parameters[index];
            const Operand   &argument  = arguments[index];
            if (parameter.is_const) {
                if (argument.kind != OperandKind::Value || !contract_substitution.bind_value(parameter.symbol, argument.value)) {
                    return Truth::False;
                }
            } else if (argument.kind != OperandKind::Type) {
                return Truth::False;
            }
            if (!contract_substitution.unify(parameter.type, query.arguments[index].type)) { return Truth::False; }
        }
        if (query.expected_result.valid() && !contract_substitution.unify(contract->signature.result, query.expected_result)) {
            return Truth::False;
        }
        if (!solve(contract->requirements, contract_substitution, range, "operator contract", false, premises)) {
            return Truth::False;
        }
        for (const GenericParameter &generic : contract->generics) {
            if (generic.is_const ? !contract_substitution.has_value(generic.symbol)
                                 : !contract_substitution.has_type(generic.symbol)) {
                fail("operator contract has an unresolved generic");
                return Truth::Unresolved;
            }
        }

        std::size_t admitted = 0;
        for (const Declaration &declaration : module_.declarations) {
            const auto *candidate = std::get_if<FunctionDecl>(&declaration.node);
            if (!candidate || candidate->visibility != Visibility::Implementation || !declaration.symbol.valid() ||
                module_.symbol(declaration.symbol).name != symbol.name ||
                candidate->signature.parameters.size() != query.arguments.size()) {
                continue;
            }
            GenericSubstitution candidate_substitution{module_, types_};
            bool                matches = true;
            for (std::size_t index = 0; index < query.arguments.size(); ++index) {
                const Parameter &parameter = candidate->signature.parameters[index];
                const Operand   &argument  = arguments[index];
                if (parameter.is_const) {
                    matches = argument.kind == OperandKind::Value &&
                              candidate_substitution.bind_value(parameter.symbol, argument.value) && matches;
                } else {
                    matches = argument.kind == OperandKind::Type && matches;
                }
                matches = candidate_substitution.unify(candidate->signature.parameters[index].type, query.arguments[index].type) &&
                          matches;
            }
            if (query.expected_result.valid()) {
                matches = candidate_substitution.unify(candidate->signature.result, query.expected_result) && matches;
            }
            for (std::size_t index = 0; matches && index < query.arguments.size(); ++index) {
                matches = types_.same(candidate_substitution.apply(candidate->signature.parameters[index].type),
                                      query.arguments[index].type);
            }
            if (matches && query.expected_result.valid()) {
                matches = types_.same(candidate_substitution.apply(candidate->signature.result), query.expected_result);
            }
            if (matches) {
                matches = solve(candidate->requirements, candidate_substitution, {}, "implementation", false, premises);
            }
            for (const GenericParameter &generic : candidate->generics) {
                if (generic.is_const ? !candidate_substitution.has_value(generic.symbol)
                                     : !candidate_substitution.has_type(generic.symbol)) {
                    matches = false;
                }
            }
            if (matches) { ++admitted; }
        }
        if (admitted == 1U) { return Truth::True; }
        if (admitted > 1U) {
            fail("multiple source implementations require native registry ranking");
            return Truth::Unresolved;
        }
        fail("operator requirement has no implementation");
        return Truth::False;
    }

    ConstraintSolver::Truth ConstraintSolver::evaluate(ConstraintId id, GenericSubstitution &substitution,
                                                       std::span<const ConstraintPremise> premises) {
        if (!id.valid()) { return Truth::True; }
        const Constraint &constraint = module_.constraint(id);
        return std::visit(
            [&](const auto &node) -> Truth {
                using T = std::decay_t<decltype(node)>;
                if constexpr (std::is_same_v<T, ConstraintRelation>) {
                    return evaluate_relation(node, substitution);
                } else if constexpr (std::is_same_v<T, OperatorRequirement>) {
                    return evaluate_operator(node, substitution, constraint.range, premises);
                } else if constexpr (std::is_same_v<T, ConstraintNot>) {
                    const std::string previous_failure = failure_detail_;
                    const Truth       value            = evaluate(node.operand, substitution, premises);
                    if (value == Truth::Unresolved) { return value; }
                    if (value == Truth::False) { failure_detail_ = previous_failure; }
                    return value == Truth::True ? Truth::False : Truth::True;
                } else if constexpr (std::is_same_v<T, ConstraintLogic>) {
                    if (node.op == ConstraintLogicOp::Or) {
                        const std::string previous_failure = failure_detail_;
                        const Truth       lhs              = evaluate(node.lhs, substitution, premises);
                        if (lhs == Truth::True) {
                            failure_detail_ = previous_failure;
                            return Truth::True;
                        }
                        const std::string lhs_failure = failure_detail_;
                        failure_detail_               = previous_failure;
                        const Truth rhs               = evaluate(node.rhs, substitution, premises);
                        if (rhs == Truth::True) {
                            failure_detail_ = previous_failure;
                            return Truth::True;
                        }
                        if (failure_detail_ == previous_failure && lhs_failure != previous_failure) {
                            failure_detail_ = lhs_failure;
                        }
                        if (lhs == Truth::False && rhs == Truth::False) { return Truth::False; }
                        return Truth::Unresolved;
                    }
                    const Truth lhs = evaluate(node.lhs, substitution, premises);
                    if (lhs == Truth::False) { return Truth::False; }
                    const Truth rhs = evaluate(node.rhs, substitution, premises);
                    if (rhs == Truth::False) { return Truth::False; }
                    return lhs == Truth::True && rhs == Truth::True ? Truth::True : Truth::Unresolved;
                } else {
                    const Operand value = operand(id, substitution);
                    if (!value.known) { return Truth::Unresolved; }
                    if (value.kind == OperandKind::Boolean) { return value.boolean ? Truth::True : Truth::False; }
                    return Truth::Unresolved;
                }
            },
            constraint.node);
    }

    bool ConstraintSolver::infer_equalities(ConstraintId id, GenericSubstitution &substitution, bool &changed) {
        if (!id.valid()) { return true; }
        const Constraint &constraint = module_.constraint(id);
        if (const auto *logic = std::get_if<ConstraintLogic>(&constraint.node)) {
            if (logic->op != ConstraintLogicOp::And) { return true; }
            return infer_equalities(logic->lhs, substitution, changed) && infer_equalities(logic->rhs, substitution, changed);
        }
        const auto *relation = std::get_if<ConstraintRelation>(&constraint.node);
        if (!relation || relation->op != ConstraintRelationOp::Equal) { return true; }
        Operand lhs = operand(relation->lhs, substitution);
        Operand rhs = operand(relation->rhs, substitution);
        if (!lhs.known && lhs.variable.valid() && rhs.known) {
            const bool already =
                lhs.kind == OperandKind::Type ? substitution.has_type(lhs.variable) : substitution.has_value(lhs.variable);
            const bool ok = lhs.kind == OperandKind::Type && rhs.kind == OperandKind::Type
                                ? substitution.bind_type(lhs.variable, rhs.type)
                            : lhs.kind == OperandKind::Value && rhs.kind == OperandKind::Value
                                ? substitution.bind_value(lhs.variable, rhs.value)
                                : false;
            changed       = changed || (ok && !already);
            if (!ok) { fail("constraint equality binds incompatible kinds or values"); }
            return ok;
        }
        if (!rhs.known && rhs.variable.valid() && lhs.known) {
            const bool already =
                rhs.kind == OperandKind::Type ? substitution.has_type(rhs.variable) : substitution.has_value(rhs.variable);
            const bool ok = rhs.kind == OperandKind::Type && lhs.kind == OperandKind::Type
                                ? substitution.bind_type(rhs.variable, lhs.type)
                            : rhs.kind == OperandKind::Value && lhs.kind == OperandKind::Value
                                ? substitution.bind_value(rhs.variable, lhs.value)
                                : false;
            changed       = changed || (ok && !already);
            if (!ok) { fail("constraint equality binds incompatible kinds or values"); }
            return ok;
        }
        return true;
    }

    bool ConstraintSolver::solve(ConstraintId requirement, GenericSubstitution &substitution, syntax::SourceRange use_range,
                                 std::string_view subject, bool report, std::span<const ConstraintPremise> premises) {
        if (!requirement.valid()) { return true; }
        if (evaluation_depth_ == 0U) { failure_detail_.clear(); }
        if (evaluation_depth_ > module_.constraints.size()) {
            fail("cyclic operator requirement");
            return false;
        }
        ++evaluation_depth_;
        struct DepthGuard
        {
            std::size_t &depth;
            ~DepthGuard() { --depth; }
        } depth_guard{evaluation_depth_};
        for (std::size_t pass = 0; pass <= module_.constraints.size(); ++pass) {
            bool changed = false;
            if (!infer_equalities(requirement, substitution, changed)) {
                if (report) {
                    diagnostics_.report(syntax::Category::Type, use_range,
                                        std::string{subject} + " requirements are inconsistent: " + failure_detail_);
                }
                return false;
            }
            if (!changed) { break; }
        }
        const std::string previous_failure = failure_detail_;
        const Truth       result           = evaluate(requirement, substitution, premises);
        if (result == Truth::True) { return true; }
        if (premises_prove(requirement, substitution, premises)) {
            failure_detail_ = previous_failure;
            return true;
        }
        if (!report) { return false; }
        const syntax::SourceRange range = use_range.empty() ? module_.constraint(requirement).range : use_range;
        if (result == Truth::False) {
            diagnostics_.report(syntax::Category::Type, range,
                                std::string{subject} + " requirements are not satisfied" +
                                    (failure_detail_.empty() ? std::string{} : ": " + failure_detail_));
        } else {
            diagnostics_.report(syntax::Category::Type, range,
                                std::string{subject} + " requirements could not be resolved" +
                                    (failure_detail_.empty() ? std::string{} : ": " + failure_detail_));
        }
        return false;
    }

    bool ConstraintSolver::proves_numeric(ConstraintId requirement, TypeId subject, GenericSubstitution *substitution) {
        if (!requirement.valid()) { return false; }
        const Constraint &constraint = module_.constraint(requirement);
        if (const auto *logic = std::get_if<ConstraintLogic>(&constraint.node)) {
            return logic->op == ConstraintLogicOp::And &&
                   (proves_numeric(logic->lhs, subject, substitution) || proves_numeric(logic->rhs, subject, substitution));
        }
        const auto *relation = std::get_if<ConstraintRelation>(&constraint.node);
        if (!relation || relation->op != ConstraintRelationOp::In) { return false; }
        GenericSubstitution  empty{module_, types_};
        GenericSubstitution &bindings = substitution == nullptr ? empty : *substitution;
        Operand              lhs      = operand(relation->lhs, bindings);
        Operand              rhs      = operand(relation->rhs, bindings);
        if (lhs.kind != OperandKind::Type || !same_symbolic_type(lhs.type, subject) || rhs.kind != OperandKind::TypeSet ||
            rhs.types.empty()) {
            return false;
        }
        return std::ranges::all_of(rhs.types, [&](TypeId item) { return types_.numeric(item); });
    }

    std::optional<RequiredOperation> ConstraintSolver::find_required_operation(ConstraintId requirement, std::string_view identity,
                                                                               const std::vector<TypeId> &arguments,
                                                                               GenericSubstitution       &substitution) {
        if (!requirement.valid()) { return std::nullopt; }
        const Constraint &constraint = module_.constraint(requirement);
        if (const auto *logic = std::get_if<ConstraintLogic>(&constraint.node)) {
            if (logic->op != ConstraintLogicOp::And) { return std::nullopt; }
            if (auto left = find_required_operation(logic->lhs, identity, arguments, substitution)) { return left; }
            return find_required_operation(logic->rhs, identity, arguments, substitution);
        }
        const auto *required = std::get_if<OperatorRequirement>(&constraint.node);
        if (!required || !identity_matches(operator_identity(required->op), identity) ||
            required->arguments.size() != arguments.size()) {
            return std::nullopt;
        }
        for (std::size_t index = 0; index < arguments.size(); ++index) {
            Operand pattern = operand(required->arguments[index], substitution);
            if (pattern.kind != OperandKind::Type || !same_symbolic_type(pattern.type, arguments[index])) { return std::nullopt; }
        }
        TypeId result = required->result.valid() ? substitution.apply(required->result) : TypeId{};
        return RequiredOperation{required->op, result, operator_identity(required->op)};
    }

    std::optional<RequiredOperation> ConstraintSolver::required_operation(ConstraintId requirement, std::string_view identity,
                                                                          const std::vector<TypeId> &arguments,
                                                                          GenericSubstitution       *substitution) {
        GenericSubstitution  empty{module_, types_};
        GenericSubstitution &bindings = substitution == nullptr ? empty : *substitution;
        return find_required_operation(requirement, identity, arguments, bindings);
    }

    std::optional<TypeId> ConstraintSolver::find_required_field(ConstraintId requirement, TypeId subject, std::string_view field,
                                                                GenericSubstitution &substitution) {
        if (!requirement.valid()) { return std::nullopt; }
        const Constraint &constraint = module_.constraint(requirement);
        if (const auto *logic = std::get_if<ConstraintLogic>(&constraint.node)) {
            if (logic->op != ConstraintLogicOp::And) { return std::nullopt; }
            if (auto left = find_required_field(logic->lhs, subject, field, substitution)) { return left; }
            return find_required_field(logic->rhs, subject, field, substitution);
        }
        const auto *relation = std::get_if<ConstraintRelation>(&constraint.node);
        if (!relation || relation->op != ConstraintRelationOp::Equal) { return std::nullopt; }

        const auto match = [&](ConstraintId call_id, ConstraintId type_id) -> std::optional<TypeId> {
            const auto *call = std::get_if<ConstraintCall>(&module_.constraint(call_id).node);
            if (!call || !call->function.valid() || call->arguments.size() != 2U ||
                module_.symbol(call->function).name != "field_type") {
                return std::nullopt;
            }
            Operand    source     = operand(call->arguments[0], substitution);
            Operand    name       = operand(call->arguments[1], substitution);
            Operand    result     = operand(type_id, substitution);
            const auto field_name = name.kind == OperandKind::Value ? string_value(name.value) : std::nullopt;
            if (source.kind != OperandKind::Type || !same_symbolic_type(source.type, subject) || !field_name ||
                *field_name != field || result.kind != OperandKind::Type) {
                return std::nullopt;
            }
            return result.type;
        };
        if (auto result = match(relation->lhs, relation->rhs)) { return result; }
        return match(relation->rhs, relation->lhs);
    }

    std::optional<TypeId> ConstraintSolver::field_type(ConstraintId requirement, TypeId subject, std::string_view field,
                                                       GenericSubstitution *substitution) {
        if (substitution != nullptr) { subject = substitution->apply(subject); }
        const auto concrete = effective_fields(subject);
        if (const auto found = std::ranges::find(concrete, field, &EffectiveField::name); found != concrete.end()) {
            return found->type;
        }
        GenericSubstitution  empty{module_, types_};
        GenericSubstitution &bindings = substitution == nullptr ? empty : *substitution;
        return find_required_field(requirement, subject, field, bindings);
    }
}  // namespace hgl::ir::detail
