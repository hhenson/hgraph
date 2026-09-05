#include "hgraph_ir/lower.h"

#include <cstddef>
#include <string>
#include <string_view>
#include <type_traits>
#include <unordered_map>
#include <unordered_set>
#include <utility>

namespace hgl::hgraph_ir
{
    namespace
    {
        namespace hir = ir::hir;

        class InterfaceLowerer
        {
          public:
            InterfaceLowerer(const hir::Module &source, syntax::DiagnosticSink &diagnostics)
                : source_{source}, diagnostics_{diagnostics} {
                result_.path = source.path;
            }

            Module run() {
                if (source_.completion != hir::Completion::Typed) {
                    diagnostics_.report(syntax::Category::Type, {}, "hgraph IR lowering requires typed HIR");
                    return std::move(result_);
                }

                lower_types();
                lower_constraints();
                lower_structures();
                lower_operators();
                lower_callables();
                return std::move(result_);
            }

          private:
            [[nodiscard]] hir::TypeId canonical(hir::TypeId source) const noexcept {
                if (!source.valid()) { return {}; }
                const hir::TypeId canonical = source_.type(source).canonical;
                return canonical.valid() ? canonical : source;
            }

            [[nodiscard]] ConstExprId lower_const_expr(hir::ExprId expression, syntax::SourceRange range, std::string_view role) {
                if (!expression.valid()) { return {}; }
                if (const auto found = const_exprs_.find(expression.value); found != const_exprs_.end()) { return found->second; }

                const ConstExprId id{static_cast<std::uint32_t>(result_.const_exprs.size())};
                const_exprs_.emplace(expression.value, id);
                result_.const_exprs.emplace_back();

                const hir::Expr &source = source_.expr(expression);
                ConstExpr        target;
                target.range = source.range;
                if (source.phase != hir::Phase::Constant) {
                    diagnostics_.report(syntax::Category::Type, range,
                                        "typed HIR has no compile-time expression for " + std::string{role});
                } else if (source.constant) {
                    target.literal = source.constant;
                } else if (const auto *reference = std::get_if<hir::SymbolRef>(&source.node)) {
                    if (!reference->symbol.valid() || source_.symbol(reference->symbol).kind != hir::SymbolKind::ConstParameter) {
                        diagnostics_.report(syntax::Category::Type, range,
                                            "typed HIR has no compile-time value for " + std::string{role});
                    } else {
                        target.kind      = ConstExprKind::Parameter;
                        target.parameter = source_.symbol(reference->symbol).name;
                    }
                } else if (const auto *unary = std::get_if<hir::Unary>(&source.node)) {
                    target.kind  = ConstExprKind::Unary;
                    target.unary = unary->op;
                    target.lhs   = lower_const_expr(unary->operand, source.range, role);
                } else if (const auto *binary = std::get_if<hir::Binary>(&source.node)) {
                    target.kind   = ConstExprKind::Binary;
                    target.binary = binary->op;
                    target.lhs    = lower_const_expr(binary->lhs, source.range, role);
                    target.rhs    = lower_const_expr(binary->rhs, source.range, role);
                } else if (const auto *index = std::get_if<hir::Index>(&source.node)) {
                    target.kind = ConstExprKind::Index;
                    target.lhs  = lower_const_expr(index->target, source.range, role);
                    target.rhs  = lower_const_expr(index->index, source.range, role);
                } else if (const auto *field = std::get_if<hir::Field>(&source.node)) {
                    target.kind   = ConstExprKind::Field;
                    target.lhs    = lower_const_expr(field->target, source.range, role);
                    target.member = field->name;
                } else if (const auto *sequence = std::get_if<hir::Sequence>(&source.node)) {
                    target.kind = ConstExprKind::Sequence;
                    for (const hir::SequenceElement &element : sequence->elements) {
                        target.elements.push_back(ConstElement{
                            .key   = lower_const_expr(element.key, source.range, role),
                            .value = lower_const_expr(element.value, source.range, role),
                        });
                    }
                } else if (const auto *tuple = std::get_if<hir::Tuple>(&source.node)) {
                    target.kind = ConstExprKind::Tuple;
                    for (hir::ExprId item : tuple->elements) { target.items.push_back(lower_const_expr(item, source.range, role)); }
                } else if (const auto *construct = std::get_if<hir::Construct>(&source.node)) {
                    target.kind             = ConstExprKind::Construct;
                    target.constructed_type = lower_type(construct->type);
                    target.delta            = construct->delta;
                    for (const hir::Argument &argument : construct->arguments) {
                        target.arguments.push_back(
                            ConstArgument{argument.name, lower_const_expr(argument.value, argument.range, role)});
                    }
                } else {
                    diagnostics_.report(syntax::Category::Type, range, "hgraph IR cannot represent " + std::string{role} + " yet");
                }
                result_.const_exprs[id.value] = std::move(target);
                return id;
            }

            [[nodiscard]] std::string symbol_identity(hir::SymbolId id) const {
                if (!id.valid()) { return {}; }
                const hir::Symbol &symbol = source_.symbol(id);
                if (!symbol.canonical_name.empty()) { return symbol.canonical_name; }
                if (symbol.kind == hir::SymbolKind::Struct || symbol.kind == hir::SymbolKind::Operator ||
                    symbol.kind == hir::SymbolKind::Function) {
                    return source_.path + "." + symbol.name;
                }
                return symbol.name;
            }

            [[nodiscard]] TypeId lower_type(hir::TypeId source_id) {
                source_id = canonical(source_id);
                if (!source_id.valid()) { return {}; }
                if (const auto found = types_.find(source_id.value); found != types_.end()) { return found->second; }

                const TypeId id{static_cast<std::uint32_t>(result_.types.size())};
                types_.emplace(source_id.value, id);
                result_.types.emplace_back();

                const hir::Type &source_type = source_.type(source_id);
                Type             target;
                target.kind             = source_type.kind;
                target.scalar           = source_type.scalar;
                target.nominal_identity = symbol_identity(source_type.symbol);
                target.unbounded        = source_type.unbounded;
                for (hir::TypeId child : source_type.children) { target.children.push_back(lower_type(child)); }
                for (const hir::TypeArgument &argument : source_type.arguments) {
                    TypeArgument lowered;
                    if (argument.kind == hir::TypeArgumentKind::Type) {
                        lowered.type = lower_type(argument.type);
                    } else {
                        lowered.value = lower_const_expr(argument.value, argument.range, "a generic type argument");
                    }
                    target.arguments.push_back(std::move(lowered));
                }
                target.size             = lower_const_expr(source_type.size, source_type.range, "a type size");
                target.min_size         = lower_const_expr(source_type.min_size, source_type.range, "a minimum type size");
                result_.types[id.value] = std::move(target);
                return id;
            }

            void lower_types() {
                for (std::uint32_t index = 0; index < source_.types.size(); ++index) { (void)lower_type(hir::TypeId{index}); }
            }

            [[nodiscard]] GenericParameter lower_generic(const hir::GenericParameter &source) {
                const hir::Symbol &symbol = source_.symbol(source.symbol);
                return GenericParameter{symbol.name, source.is_const, lower_type(source.type)};
            }

            [[nodiscard]] Parameter lower_parameter(const hir::Parameter &source) {
                const hir::Symbol &symbol = source_.symbol(source.symbol);
                Parameter          target;
                target.name          = symbol.name;
                target.is_const      = source.is_const;
                target.type          = lower_type(source.type);
                target.default_value = lower_const_expr(source.default_value, symbol.range, "a parameter default");
                return target;
            }

            [[nodiscard]] static ConstraintLogicOp lower_logic_op(hir::ConstraintLogicOp source) noexcept {
                return source == hir::ConstraintLogicOp::And ? ConstraintLogicOp::And : ConstraintLogicOp::Or;
            }

            [[nodiscard]] static ConstraintRelationOp lower_relation_op(hir::ConstraintRelationOp source) noexcept {
                switch (source) {
                    case hir::ConstraintRelationOp::Equal: return ConstraintRelationOp::Equal;
                    case hir::ConstraintRelationOp::In: return ConstraintRelationOp::In;
                    case hir::ConstraintRelationOp::Is: return ConstraintRelationOp::Is;
                }
                std::unreachable();
            }

            [[nodiscard]] ConstraintId lower_constraint(hir::ConstraintId source_id) {
                if (!source_id.valid()) { return {}; }
                if (const auto found = constraints_.find(source_id.value); found != constraints_.end()) { return found->second; }

                const ConstraintId id{static_cast<std::uint32_t>(result_.constraints.size())};
                constraints_.emplace(source_id.value, id);
                result_.constraints.emplace_back();

                const hir::Constraint &source = source_.constraint(source_id);
                Constraint             target;
                target.range = source.range;
                target.node  = std::visit(
                    [&](const auto &node) -> ConstraintNode {
                        using T = std::decay_t<decltype(node)>;
                        if constexpr (std::is_same_v<T, hir::ConstraintSymbol>) {
                            return ConstraintSymbol{symbol_identity(node.symbol)};
                        } else if constexpr (std::is_same_v<T, hir::ConstraintType>) {
                            return ConstraintType{lower_type(node.type)};
                        } else if constexpr (std::is_same_v<T, hir::ConstraintValue>) {
                            return ConstraintValue{lower_const_expr(node.value, source.range, "a constraint value")};
                        } else if constexpr (std::is_same_v<T, hir::ConstraintSet>) {
                            ConstraintSet lowered;
                            for (hir::ConstraintId element : node.elements) {
                                lowered.elements.push_back(lower_constraint(element));
                            }
                            return lowered;
                        } else if constexpr (std::is_same_v<T, hir::ConstraintCall>) {
                            ConstraintCall lowered;
                            lowered.function_identity = symbol_identity(node.function);
                            for (hir::ConstraintId argument : node.arguments) {
                                lowered.arguments.push_back(lower_constraint(argument));
                            }
                            return lowered;
                        } else if constexpr (std::is_same_v<T, hir::OperatorRequirement>) {
                            OperatorRequirement lowered;
                            lowered.operator_identity = symbol_identity(node.op);
                            if (node.op.valid()) { lowered.operator_registry_name = source_.symbol(node.op).external_name; }
                            for (hir::ConstraintId argument : node.arguments) {
                                lowered.arguments.push_back(lower_constraint(argument));
                            }
                            lowered.result = lower_type(node.result);
                            return lowered;
                        } else if constexpr (std::is_same_v<T, hir::ConstraintRelation>) {
                            return ConstraintRelation{lower_relation_op(node.op), lower_constraint(node.lhs),
                                                      lower_constraint(node.rhs), node.category};
                        } else if constexpr (std::is_same_v<T, hir::ConstraintNot>) {
                            return ConstraintNot{lower_constraint(node.operand)};
                        } else {
                            return ConstraintLogic{lower_logic_op(node.op), lower_constraint(node.lhs), lower_constraint(node.rhs)};
                        }
                    },
                    source.node);
                result_.constraints[id.value] = std::move(target);
                return id;
            }

            void lower_constraints() {
                for (std::uint32_t index = 0; index < source_.constraints.size(); ++index) {
                    (void)lower_constraint(hir::ConstraintId{index});
                }
            }

            [[nodiscard]] std::string declaration_identity(hir::DeclarationId id) const {
                if (!id.valid()) { return {}; }
                return symbol_identity(source_.declaration(id).symbol);
            }

            void lower_structures() {
                for (const hir::Declaration &declaration : source_.declarations) {
                    const auto *source = std::get_if<hir::StructDecl>(&declaration.node);
                    if (source == nullptr || !declaration.symbol.valid()) { continue; }

                    StructContract target;
                    target.identity     = symbol_identity(declaration.symbol);
                    target.exported     = source->exported;
                    target.abstract     = source->abstract;
                    target.requirements = lower_constraint(source->requirements);
                    target.range        = declaration.range;
                    for (const hir::GenericParameter &generic : source->generics) {
                        target.generics.push_back(lower_generic(generic));
                    }
                    for (hir::TypeId parent : source->parents) { target.parents.push_back(lower_type(parent)); }
                    for (const hir::StructField &field : source->fields) {
                        target.fields.push_back(StructField{
                            .name            = field.name,
                            .type            = lower_type(field.type),
                            .default_value   = lower_const_expr(field.default_value, field.range, "a struct field default"),
                            .origin_identity = declaration_identity(field.origin),
                            .optional        = field.optional,
                            .range           = field.range,
                        });
                    }
                    result_.structures.push_back(std::move(target));
                }
            }

            [[nodiscard]] static CallableVisibility lower_visibility(hir::Visibility source) noexcept {
                switch (source) {
                    case hir::Visibility::Internal: return CallableVisibility::Internal;
                    case hir::Visibility::Export: return CallableVisibility::Export;
                    case hir::Visibility::Implementation: return CallableVisibility::Implementation;
                }
                std::unreachable();
            }

            void lower_signature(const std::vector<hir::GenericParameter> &generics, const hir::Signature &signature,
                                 std::vector<GenericParameter> &target_generics, std::vector<Parameter> &target_parameters,
                                 TypeId &target_result) {
                for (const hir::GenericParameter &generic : generics) { target_generics.push_back(lower_generic(generic)); }
                for (const hir::Parameter &parameter : signature.parameters) {
                    target_parameters.push_back(lower_parameter(parameter));
                }
                target_result = lower_type(signature.result);
            }

            void lower_operators() {
                std::unordered_set<std::string> known;
                for (const hir::Declaration &declaration : source_.declarations) {
                    const auto *source = std::get_if<hir::OperatorDecl>(&declaration.node);
                    if (source == nullptr || !declaration.symbol.valid()) { continue; }
                    OperatorContract target;
                    target.identity = symbol_identity(declaration.symbol);
                    target.range    = declaration.range;
                    lower_signature(source->generics, source->signature, target.generics, target.parameters, target.result);
                    target.requirements = lower_constraint(source->requirements);
                    known.insert(target.identity);
                    result_.operators.push_back(std::move(target));
                }

                for (const hir::Symbol &symbol : source_.symbols) {
                    if (symbol.kind != hir::SymbolKind::ImportedOperator || symbol.canonical_name.empty() ||
                        !known.insert(symbol.canonical_name).second) {
                        continue;
                    }
                    result_.operators.push_back(OperatorContract{.identity      = symbol.canonical_name,
                                                                 .registry_name = symbol.external_name,
                                                                 .imported      = true,
                                                                 .range         = symbol.range});
                }
            }

            void lower_callables() {
                for (const hir::Declaration &declaration : source_.declarations) {
                    const auto *source = std::get_if<hir::FunctionDecl>(&declaration.node);
                    if (source == nullptr || !declaration.symbol.valid()) { continue; }
                    Callable target;
                    target.visibility = lower_visibility(source->visibility);
                    target.identity   = symbol_identity(declaration.symbol);
                    if (target.visibility == CallableVisibility::Implementation) {
                        target.identity += "#" + std::to_string(declaration.id.value);
                    }
                    target.kind =
                        source->kind == hir::FunctionKind::Composition ? CallableKind::Composition : CallableKind::RuntimeNode;
                    target.effects = source->effects;
                    target.range   = declaration.range;
                    if (source->operator_contract.valid()) {
                        const hir::Symbol &op         = source_.symbol(source->operator_contract);
                        target.operator_identity      = symbol_identity(source->operator_contract);
                        target.operator_registry_name = op.external_name;
                    }
                    lower_signature(source->generics, source->signature, target.generics, target.parameters, target.result);
                    target.requirements = lower_constraint(source->requirements);
                    for (hir::SymbolId capability : source->capabilities) {
                        const hir::Symbol &symbol = source_.symbol(capability);
                        target.capabilities.push_back(Capability{symbol.name, lower_type(symbol.type)});
                    }
                    result_.callables.push_back(std::move(target));
                }
            }

            const hir::Module                              &source_;
            syntax::DiagnosticSink                         &diagnostics_;
            Module                                          result_{};
            std::unordered_map<std::uint32_t, TypeId>       types_{};
            std::unordered_map<std::uint32_t, ConstExprId>  const_exprs_{};
            std::unordered_map<std::uint32_t, ConstraintId> constraints_{};
        };
    }  // namespace

    Module lower_interfaces(const ir::hir::Module &source, syntax::DiagnosticSink &diagnostics) {
        return InterfaceLowerer{source, diagnostics}.run();
    }
}  // namespace hgl::hgraph_ir
