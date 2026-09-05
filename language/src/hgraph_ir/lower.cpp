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

        class Lowerer
        {
          public:
            Lowerer(const hir::Module &source, syntax::DiagnosticSink &diagnostics) : source_{source}, diagnostics_{diagnostics} {
                result_.path = source.path;
            }

            Module run() {
                if (source_.completion != hir::Completion::Typed) {
                    diagnostics_.report(syntax::Category::Type, {}, "hgraph IR lowering requires typed HIR");
                    return std::move(result_);
                }

                lower_bindings();
                lower_types();
                lower_constraints();
                lower_structures();
                lower_operators();
                lower_callables();
                lower_tests();
                if (!diagnostics_.has_errors()) { result_.completion = Completion::Bodies; }
                return std::move(result_);
            }

          private:
            struct AppliedBindings
            {
                std::unordered_map<std::uint32_t, TypeId>      types{};
                std::unordered_map<std::uint32_t, ConstExprId> values{};

                [[nodiscard]] bool empty() const noexcept { return types.empty() && values.empty(); }
            };

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
                        target.kind              = ConstExprKind::Parameter;
                        target.parameter         = source_.symbol(reference->symbol).name;
                        target.parameter_binding = binding(reference->symbol);
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
                } else if (const auto *call = std::get_if<hir::Call>(&source.node);
                           call != nullptr && source.operation.kind == hir::OperationKind::Constructor) {
                    target.kind             = ConstExprKind::Construct;
                    target.constructed_type = lower_type(source.type);
                    for (const hir::Argument &argument : call->arguments) {
                        target.arguments.push_back(
                            ConstArgument{argument.name, lower_const_expr(argument.value, argument.range, role)});
                    }
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
                    symbol.kind == hir::SymbolKind::Function || symbol.kind == hir::SymbolKind::Test) {
                    return source_.path + "." + symbol.name;
                }
                return symbol.name;
            }

            [[nodiscard]] std::string declaration_identity(hir::DeclarationId id) const {
                if (!id.valid()) { return {}; }
                const hir::Declaration &declaration = source_.declaration(id);
                std::string             identity    = symbol_identity(declaration.symbol);
                if (const auto *function = std::get_if<hir::FunctionDecl>(&declaration.node);
                    function != nullptr && function->visibility == hir::Visibility::Implementation) {
                    identity += "#" + std::to_string(id.value);
                }
                return identity;
            }

            [[nodiscard]] static std::optional<BindingKind> lower_binding_kind(hir::SymbolKind kind) noexcept {
                switch (kind) {
                    case hir::SymbolKind::TypeParameter: return BindingKind::TypeParameter;
                    case hir::SymbolKind::ConstParameter: return BindingKind::ConstParameter;
                    case hir::SymbolKind::SignalParameter: return BindingKind::SignalParameter;
                    case hir::SymbolKind::LocalLet: return BindingKind::LocalLet;
                    case hir::SymbolKind::LocalVar: return BindingKind::LocalVar;
                    case hir::SymbolKind::State: return BindingKind::State;
                    case hir::SymbolKind::InjectedCapability: return BindingKind::Capability;
                    case hir::SymbolKind::LoopValue: return BindingKind::LoopValue;
                    case hir::SymbolKind::LambdaParameter: return BindingKind::LambdaParameter;
                    default: return std::nullopt;
                }
            }

            void lower_bindings() {
                for (std::uint32_t index = 0; index < source_.symbols.size(); ++index) {
                    const hir::Symbol               &symbol = source_.symbols[index];
                    const std::optional<BindingKind> kind   = lower_binding_kind(symbol.kind);
                    if (!kind) { continue; }
                    const BindingId id{static_cast<std::uint32_t>(result_.bindings.size())};
                    bindings_.emplace(index, id);
                    result_.bindings.push_back(Binding{.name           = symbol.name,
                                                       .kind           = *kind,
                                                       .owner_identity = declaration_identity(symbol.owner),
                                                       .index          = symbol.index,
                                                       .range          = symbol.range});
                }
                for (std::uint32_t index = 0; index < source_.symbols.size(); ++index) {
                    const auto found = bindings_.find(index);
                    if (found == bindings_.end()) { continue; }
                    const hir::Symbol &symbol                  = source_.symbols[index];
                    result_.bindings[found->second.value].type = lower_type(symbol.type, symbol.range);
                }
            }

            [[nodiscard]] BindingId binding(hir::SymbolId source_id) const noexcept {
                if (!source_id.valid()) { return {}; }
                const auto found = bindings_.find(source_id.value);
                return found == bindings_.end() ? BindingId{} : found->second;
            }

            [[nodiscard]] TypeId lower_type(hir::TypeId source_id, syntax::SourceRange fallback_range = {}) {
                syntax::SourceRange occurrence_range = source_id.valid() ? source_.type(source_id).range : syntax::SourceRange{};
                if (occurrence_range.end <= occurrence_range.begin) { occurrence_range = fallback_range; }
                source_id = canonical(source_id);
                if (!source_id.valid()) { return {}; }
                if (const auto found = types_.find(source_id.value); found != types_.end()) {
                    Type &existing = result_.types[found->second.value];
                    if (existing.range.end <= existing.range.begin && occurrence_range.end > occurrence_range.begin) {
                        existing.range = occurrence_range;
                    }
                    return found->second;
                }

                const TypeId id{static_cast<std::uint32_t>(result_.types.size())};
                types_.emplace(source_id.value, id);
                result_.types.emplace_back();

                const hir::Type &source_type = source_.type(source_id);
                Type             target;
                target.kind             = source_type.kind;
                target.scalar           = source_type.scalar;
                target.nominal_identity = symbol_identity(source_type.symbol);
                target.binding          = binding(source_type.symbol);
                target.unbounded        = source_type.unbounded;
                target.range            = occurrence_range;
                for (hir::TypeId child : source_type.children) { target.children.push_back(lower_type(child, occurrence_range)); }
                for (const hir::TypeArgument &argument : source_type.arguments) {
                    TypeArgument lowered;
                    if (argument.kind == hir::TypeArgumentKind::Type) {
                        lowered.type = lower_type(argument.type, argument.range);
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

            [[nodiscard]] ConstExprId lower_const_expr(hir::ExprId expression, const AppliedBindings &bindings,
                                                       syntax::SourceRange range, std::string_view role) {
                if (!expression.valid() || bindings.empty()) { return lower_const_expr(expression, range, role); }
                const hir::Expr &source = source_.expr(expression);
                if (source.constant) { return lower_const_expr(expression, range, role); }
                if (const auto *reference = std::get_if<hir::SymbolRef>(&source.node); reference && reference->symbol.valid()) {
                    if (const auto found = bindings.values.find(reference->symbol.value); found != bindings.values.end()) {
                        return found->second;
                    }
                    return lower_const_expr(expression, range, role);
                }

                ConstExpr target;
                target.range = source.range;
                if (source.phase != hir::Phase::Constant) {
                    diagnostics_.report(syntax::Category::Type, range,
                                        "typed HIR has no compile-time expression for " + std::string{role});
                } else if (const auto *unary = std::get_if<hir::Unary>(&source.node)) {
                    target.kind  = ConstExprKind::Unary;
                    target.unary = unary->op;
                    target.lhs   = lower_const_expr(unary->operand, bindings, source.range, role);
                } else if (const auto *binary = std::get_if<hir::Binary>(&source.node)) {
                    target.kind   = ConstExprKind::Binary;
                    target.binary = binary->op;
                    target.lhs    = lower_const_expr(binary->lhs, bindings, source.range, role);
                    target.rhs    = lower_const_expr(binary->rhs, bindings, source.range, role);
                } else if (const auto *index = std::get_if<hir::Index>(&source.node)) {
                    target.kind = ConstExprKind::Index;
                    target.lhs  = lower_const_expr(index->target, bindings, source.range, role);
                    target.rhs  = lower_const_expr(index->index, bindings, source.range, role);
                } else if (const auto *field = std::get_if<hir::Field>(&source.node)) {
                    target.kind   = ConstExprKind::Field;
                    target.lhs    = lower_const_expr(field->target, bindings, source.range, role);
                    target.member = field->name;
                } else if (const auto *sequence = std::get_if<hir::Sequence>(&source.node)) {
                    target.kind = ConstExprKind::Sequence;
                    for (const hir::SequenceElement &element : sequence->elements) {
                        target.elements.push_back(ConstElement{
                            .key   = lower_const_expr(element.key, bindings, source.range, role),
                            .value = lower_const_expr(element.value, bindings, source.range, role),
                        });
                    }
                } else if (const auto *tuple = std::get_if<hir::Tuple>(&source.node)) {
                    target.kind = ConstExprKind::Tuple;
                    for (hir::ExprId item : tuple->elements) {
                        target.items.push_back(lower_const_expr(item, bindings, source.range, role));
                    }
                } else if (const auto *call = std::get_if<hir::Call>(&source.node);
                           call != nullptr && source.operation.kind == hir::OperationKind::Constructor) {
                    target.kind             = ConstExprKind::Construct;
                    target.constructed_type = lower_type(source.type, bindings);
                    for (const hir::Argument &argument : call->arguments) {
                        target.arguments.push_back(
                            ConstArgument{argument.name, lower_const_expr(argument.value, bindings, argument.range, role)});
                    }
                } else if (const auto *construct = std::get_if<hir::Construct>(&source.node)) {
                    target.kind             = ConstExprKind::Construct;
                    target.constructed_type = lower_type(construct->type, bindings);
                    target.delta            = construct->delta;
                    for (const hir::Argument &argument : construct->arguments) {
                        target.arguments.push_back(
                            ConstArgument{argument.name, lower_const_expr(argument.value, bindings, argument.range, role)});
                    }
                } else {
                    diagnostics_.report(syntax::Category::Type, range, "hgraph IR cannot represent " + std::string{role} + " yet");
                }
                const ConstExprId id{static_cast<std::uint32_t>(result_.const_exprs.size())};
                result_.const_exprs.push_back(std::move(target));
                return id;
            }

            [[nodiscard]] TypeId intern_type(Type target) {
                for (std::uint32_t index = 0; index < result_.types.size(); ++index) {
                    if (result_.types[index] == target) { return TypeId{index}; }
                }
                const TypeId id{static_cast<std::uint32_t>(result_.types.size())};
                result_.types.push_back(std::move(target));
                return id;
            }

            [[nodiscard]] TypeId lower_type(hir::TypeId source_id, const AppliedBindings &bindings,
                                            syntax::SourceRange fallback_range = {}) {
                syntax::SourceRange occurrence_range = source_id.valid() ? source_.type(source_id).range : syntax::SourceRange{};
                if (occurrence_range.end <= occurrence_range.begin) { occurrence_range = fallback_range; }
                source_id = canonical(source_id);
                if (!source_id.valid() || bindings.empty()) { return lower_type(source_id, occurrence_range); }
                const hir::Type &source_type = source_.type(source_id);
                if (source_type.kind == hir::TypeKind::Symbol && source_type.symbol.valid()) {
                    if (const auto found = bindings.types.find(source_type.symbol.value); found != bindings.types.end()) {
                        return found->second;
                    }
                }

                Type target;
                target.kind             = source_type.kind;
                target.scalar           = source_type.scalar;
                target.nominal_identity = symbol_identity(source_type.symbol);
                target.binding          = binding(source_type.symbol);
                target.unbounded        = source_type.unbounded;
                target.range            = occurrence_range;
                for (hir::TypeId child : source_type.children) {
                    target.children.push_back(lower_type(child, bindings, occurrence_range));
                }
                for (const hir::TypeArgument &argument : source_type.arguments) {
                    TypeArgument lowered;
                    if (argument.kind == hir::TypeArgumentKind::Type) {
                        lowered.type = lower_type(argument.type, bindings, argument.range);
                    } else {
                        lowered.value = lower_const_expr(argument.value, bindings, argument.range, "a generic type argument");
                    }
                    target.arguments.push_back(std::move(lowered));
                }
                target.size     = lower_const_expr(source_type.size, bindings, source_type.range, "a type size");
                target.min_size = lower_const_expr(source_type.min_size, bindings, source_type.range, "a minimum type size");
                return intern_type(std::move(target));
            }

            [[nodiscard]] AppliedBindings parent_bindings(hir::TypeId parent_type, const AppliedBindings &outer) {
                AppliedBindings result;
                parent_type = canonical(parent_type);
                if (!parent_type.valid()) { return result; }
                const hir::Type &application = source_.type(parent_type);
                if (!application.symbol.valid()) { return result; }
                const hir::Symbol &parent_symbol = source_.symbol(application.symbol);
                if (!parent_symbol.owner.valid()) { return result; }
                const auto *parent = std::get_if<hir::StructDecl>(&source_.declaration(parent_symbol.owner).node);
                if (parent == nullptr) { return result; }
                for (std::size_t index = 0; index < parent->generics.size() && index < application.arguments.size(); ++index) {
                    const hir::GenericParameter &generic  = parent->generics[index];
                    const hir::TypeArgument     &argument = application.arguments[index];
                    if (generic.is_const && argument.kind == hir::TypeArgumentKind::Value) {
                        result.values.emplace(generic.symbol.value,
                                              lower_const_expr(argument.value, outer, argument.range, "an inherited argument"));
                    } else if (!generic.is_const && argument.kind == hir::TypeArgumentKind::Type) {
                        result.types.emplace(generic.symbol.value, lower_type(argument.type, outer));
                    }
                }
                return result;
            }

            [[nodiscard]] std::optional<AppliedBindings> bindings_for_origin(hir::DeclarationId current, hir::DeclarationId origin,
                                                                             const AppliedBindings &current_bindings) {
                if (!current.valid() || !origin.valid()) { return std::nullopt; }
                if (current == origin) { return current_bindings; }
                const auto *structure = std::get_if<hir::StructDecl>(&source_.declaration(current).node);
                if (structure == nullptr) { return std::nullopt; }
                for (hir::TypeId parent_type : structure->parents) {
                    const hir::Type &application = source_.type(canonical(parent_type));
                    if (!application.symbol.valid()) { continue; }
                    const hir::DeclarationId parent  = source_.symbol(application.symbol).owner;
                    const AppliedBindings    applied = parent_bindings(parent_type, current_bindings);
                    if (parent == origin) { return applied; }
                    if (auto inherited = bindings_for_origin(parent, origin, applied)) { return inherited; }
                }
                return std::nullopt;
            }

            void lower_types() {
                for (std::uint32_t index = 0; index < source_.types.size(); ++index) { (void)lower_type(hir::TypeId{index}); }
            }

            [[nodiscard]] GenericParameter lower_generic(const hir::GenericParameter &source) {
                const hir::Symbol &symbol = source_.symbol(source.symbol);
                return GenericParameter{symbol.name, source.is_const, lower_type(source.type, symbol.range),
                                        binding(source.symbol)};
            }

            [[nodiscard]] Parameter lower_parameter(const hir::Parameter &source) {
                const hir::Symbol &symbol = source_.symbol(source.symbol);
                Parameter          target;
                target.name          = symbol.name;
                target.is_const      = source.is_const;
                target.type          = lower_type(source.type, symbol.range);
                target.default_value = lower_const_expr(source.default_value, symbol.range, "a parameter default");
                target.binding       = binding(source.symbol);
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
                    for (hir::TypeId parent : source->parents) { target.parents.push_back(lower_type(parent, declaration.range)); }
                    std::unordered_map<std::uint32_t, AppliedBindings> origin_bindings;
                    for (const hir::StructField &field : source->fields) {
                        if (!origin_bindings.contains(field.origin.value)) {
                            std::optional<AppliedBindings> applied =
                                bindings_for_origin(declaration.id, field.origin, AppliedBindings{});
                            if (!applied) {
                                diagnostics_.report(syntax::Category::Type, field.range,
                                                    "cannot map an inherited field into the child generic scope");
                                continue;
                            }
                            origin_bindings.emplace(field.origin.value, std::move(*applied));
                        }
                        const AppliedBindings &applied = origin_bindings.at(field.origin.value);
                        target.fields.push_back(StructField{
                            .name          = field.name,
                            .type          = lower_type(field.type, applied, field.range),
                            .default_value = lower_const_expr(field.default_value, applied, field.range, "a struct field default"),
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

            [[nodiscard]] CallableId callable(hir::SymbolId source_id) const noexcept {
                if (!source_id.valid()) { return {}; }
                const auto found = callables_.find(source_id.value);
                return found == callables_.end() ? CallableId{} : found->second;
            }

            [[nodiscard]] Reference lower_reference(hir::SymbolId source_id) const {
                Reference target;
                if (!source_id.valid()) { return target; }
                const hir::Symbol &source = source_.symbol(source_id);
                if (const BindingId id = binding(source_id); id.valid()) {
                    target.kind    = ReferenceKind::Binding;
                    target.binding = id;
                    return target;
                }
                target.identity      = symbol_identity(source_id);
                target.registry_name = source.external_name;
                switch (source.kind) {
                    case hir::SymbolKind::Function:
                        target.kind     = ReferenceKind::Callable;
                        target.callable = callable(source_id);
                        break;
                    case hir::SymbolKind::Operator:
                    case hir::SymbolKind::ImportedOperator: target.kind = ReferenceKind::Operator; break;
                    case hir::SymbolKind::Struct: target.kind = ReferenceKind::Struct; break;
                    case hir::SymbolKind::Intrinsic: target.kind = ReferenceKind::Intrinsic; break;
                    default: target.kind = ReferenceKind::Binding; break;
                }
                return target;
            }

            [[nodiscard]] static OperationKind lower_operation_kind(hir::OperationKind kind) noexcept {
                switch (kind) {
                    case hir::OperationKind::None: return OperationKind::None;
                    case hir::OperationKind::ExactFunction: return OperationKind::ExactFunction;
                    case hir::OperationKind::NominalOperator: return OperationKind::NominalOperator;
                    case hir::OperationKind::Intrinsic: return OperationKind::Intrinsic;
                    case hir::OperationKind::Constructor: return OperationKind::Constructor;
                    case hir::OperationKind::Capability: return OperationKind::Capability;
                    case hir::OperationKind::Index: return OperationKind::Index;
                    case hir::OperationKind::Field: return OperationKind::Field;
                    case hir::OperationKind::HarnessEval: return OperationKind::HarnessEval;
                }
                std::unreachable();
            }

            [[nodiscard]] std::string binding_identity(hir::SymbolId source_id) const {
                if (!source_id.valid()) { return {}; }
                const hir::Symbol &symbol = source_.symbol(source_id);
                const std::string  owner  = declaration_identity(symbol.owner);
                return owner.empty() ? symbol.name : owner + "::" + symbol.name;
            }

            [[nodiscard]] Operation lower_operation(const hir::Operation &source, syntax::SourceRange range) {
                Operation target;
                target.kind            = lower_operation_kind(source.kind);
                target.callable        = callable(source.target);
                target.candidate       = callable(source.candidate);
                target.capability      = binding(source.target);
                target.identity        = source.identity;
                target.candidate_label = source.candidate_label;
                target.deferred        = source.deferred;
                if (source.target.valid()) {
                    const hir::Symbol &symbol = source_.symbol(source.target);
                    if (target.identity.empty()) { target.identity = symbol_identity(source.target); }
                    target.registry_name = symbol.external_name;
                }
                if (source.kind == hir::OperationKind::Index || source.kind == hir::OperationKind::Field) {
                    target.registry_name = source.identity;
                }
                if (source.kind == hir::OperationKind::NominalOperator && target.registry_name.empty() &&
                    !source.identity.empty() && !source.target.valid()) {
                    target.registry_name = source.identity;
                }
                if (source.candidate.valid()) {
                    target.candidate_identity = target.candidate.valid() ? result_.callables[target.candidate.value].identity
                                                                         : symbol_identity(source.candidate);
                }
                for (const hir::Substitution &substitution : source.substitutions) {
                    target.substitutions.push_back(Substitution{
                        .parameter = binding(substitution.parameter),
                        .parameter_identity =
                            substitution.parameter.valid() ? binding_identity(substitution.parameter) : substitution.name,
                        .type     = lower_type(substitution.type),
                        .value    = lower_const_expr(substitution.value, range, "an operation substitution"),
                        .constant = substitution.constant,
                    });
                }
                return target;
            }

            [[nodiscard]] std::vector<Argument> lower_arguments(const std::vector<hir::Argument> &source) {
                std::vector<Argument> target;
                target.reserve(source.size());
                for (const hir::Argument &argument : source) {
                    target.push_back(Argument{argument.name, lower_value(argument.value), argument.range});
                }
                return target;
            }

            [[nodiscard]] ValueId lower_value(hir::ExprId source_id) {
                if (!source_id.valid()) { return {}; }
                if (const auto found = values_.find(source_id.value); found != values_.end()) { return found->second; }

                const ValueId id{static_cast<std::uint32_t>(result_.values.size())};
                values_.emplace(source_id.value, id);
                result_.values.emplace_back();

                const hir::Expr &source = source_.expr(source_id);
                Value            target;
                target.range      = source.range;
                target.type       = lower_type(source.type);
                target.phase      = source.phase;
                target.value_kind = source.value_kind;
                target.effects    = source.effects;
                target.constant   = source.constant;
                target.operation  = lower_operation(source.operation, source.range);
                target.node       = std::visit(
                    [&](const auto &node) -> ValueNode {
                        using T = std::decay_t<decltype(node)>;
                        if constexpr (std::is_same_v<T, hir::Literal>) {
                            return Literal{node.value};
                        } else if constexpr (std::is_same_v<T, hir::SymbolRef>) {
                            return lower_reference(node.symbol);
                        } else if constexpr (std::is_same_v<T, hir::Unary>) {
                            return Unary{node.op, lower_value(node.operand)};
                        } else if constexpr (std::is_same_v<T, hir::Binary>) {
                            return Binary{node.op, lower_value(node.lhs), lower_value(node.rhs)};
                        } else if constexpr (std::is_same_v<T, hir::Call>) {
                            return Call{lower_value(node.callee), lower_arguments(node.arguments)};
                        } else if constexpr (std::is_same_v<T, hir::Index>) {
                            return Index{lower_value(node.target), lower_value(node.index)};
                        } else if constexpr (std::is_same_v<T, hir::Field>) {
                            return Field{lower_value(node.target), node.name, node.name_range};
                        } else if constexpr (std::is_same_v<T, hir::Sequence>) {
                            Sequence lowered;
                            for (const hir::SequenceElement &element : node.elements) {
                                lowered.elements.push_back(SequenceElement{lower_value(element.key), lower_value(element.value)});
                            }
                            return lowered;
                        } else if constexpr (std::is_same_v<T, hir::Tuple>) {
                            Tuple lowered;
                            for (hir::ExprId element : node.elements) { lowered.elements.push_back(lower_value(element)); }
                            return lowered;
                        } else if constexpr (std::is_same_v<T, hir::Lambda>) {
                            Lambda lowered;
                            for (hir::SymbolId parameter : node.parameters) { lowered.parameters.push_back(binding(parameter)); }
                            lowered.result = lower_type(node.result);
                            lowered.body   = lower_value(node.body);
                            return lowered;
                        } else if constexpr (std::is_same_v<T, hir::If>) {
                            return Conditional{lower_value(node.condition), lower_block(node.then_block),
                                               lower_value(node.otherwise)};
                        } else if constexpr (std::is_same_v<T, hir::BlockExpr>) {
                            return BlockValue{lower_block(node.block)};
                        } else if constexpr (std::is_same_v<T, hir::Eval>) {
                            return HarnessEval{lower_value(node.callee), lower_arguments(node.arguments)};
                        } else {
                            return Construct{lower_type(node.type), lower_arguments(node.arguments), node.delta};
                        }
                    },
                    source.node);
                result_.values[id.value] = std::move(target);
                return id;
            }

            [[nodiscard]] static AssignOp lower_assign_op(hir::AssignOp op) noexcept {
                switch (op) {
                    case hir::AssignOp::Assign: return AssignOp::Assign;
                    case hir::AssignOp::Add: return AssignOp::Add;
                    case hir::AssignOp::Sub: return AssignOp::Sub;
                    case hir::AssignOp::Mul: return AssignOp::Mul;
                    case hir::AssignOp::Div: return AssignOp::Div;
                }
                std::unreachable();
            }

            [[nodiscard]] StatementId lower_statement(hir::StmtId source_id) {
                if (!source_id.valid()) { return {}; }
                if (const auto found = statements_.find(source_id.value); found != statements_.end()) { return found->second; }

                const StatementId id{static_cast<std::uint32_t>(result_.statements.size())};
                statements_.emplace(source_id.value, id);
                result_.statements.emplace_back();

                const hir::Stmt &source = source_.stmt(source_id);
                Statement        target;
                target.range   = source.range;
                target.effects = source.effects;
                target.node    = std::visit(
                    [&](const auto &node) -> StatementNode {
                        using T = std::decay_t<decltype(node)>;
                        if constexpr (std::is_same_v<T, hir::LocalDecl>) {
                            return LocalBinding{binding(node.symbol), lower_type(node.type), lower_value(node.init)};
                        } else if constexpr (std::is_same_v<T, hir::StateDecl>) {
                            return StateBinding{binding(node.symbol), lower_type(node.type), lower_value(node.init)};
                        } else if constexpr (std::is_same_v<T, hir::InjectDecl>) {
                            Inject lowered;
                            for (hir::SymbolId symbol : node.symbols) { lowered.bindings.push_back(binding(symbol)); }
                            return lowered;
                        } else if constexpr (std::is_same_v<T, hir::LifecycleBlock>) {
                            return Lifecycle{node.is_stop ? LifecycleKind::Stop : LifecycleKind::Start, lower_block(node.block)};
                        } else if constexpr (std::is_same_v<T, hir::WhenStmt>) {
                            return Activation{lower_value(node.condition), lower_block(node.block)};
                        } else if constexpr (std::is_same_v<T, hir::ForStmt>) {
                            Traversal lowered;
                            for (hir::SymbolId symbol : node.bindings) { lowered.bindings.push_back(binding(symbol)); }
                            lowered.iterable = lower_value(node.iterable);
                            lowered.block    = lower_block(node.block);
                            return lowered;
                        } else if constexpr (std::is_same_v<T, hir::AssignStmt>) {
                            return Assignment{lower_assign_op(node.op), lower_value(node.place), lower_value(node.value)};
                        } else if constexpr (std::is_same_v<T, hir::ReturnStmt>) {
                            return Return{lower_value(node.value)};
                        } else if constexpr (std::is_same_v<T, hir::AssertStmt>) {
                            return Assert{lower_value(node.condition)};
                        } else {
                            return Evaluate{lower_value(node.expr)};
                        }
                    },
                    source.node);
                result_.statements[id.value] = std::move(target);
                return id;
            }

            [[nodiscard]] BlockId lower_block(hir::BlockId source_id) {
                if (!source_id.valid()) { return {}; }
                if (const auto found = blocks_.find(source_id.value); found != blocks_.end()) { return found->second; }

                const BlockId id{static_cast<std::uint32_t>(result_.blocks.size())};
                blocks_.emplace(source_id.value, id);
                result_.blocks.emplace_back();

                const hir::Block &source = source_.block(source_id);
                Block             target;
                target.range   = source.range;
                target.effects = source.effects;
                for (std::size_t index = 0; index < source.statements.size(); ++index) {
                    const hir::StmtId statement = source.statements[index];
                    if (source.tail.valid() && index + 1U == source.statements.size()) {
                        if (const auto *tail = std::get_if<hir::ExprStmt>(&source_.stmt(statement).node);
                            tail != nullptr && tail->expr == source.tail) {
                            continue;
                        }
                    }
                    target.statements.push_back(lower_statement(statement));
                }
                target.tail              = lower_value(source.tail);
                result_.blocks[id.value] = std::move(target);
                return id;
            }

            void lower_callables() {
                // Register all interfaces first so calls to later declarations
                // can name their target by stable CallableId.
                for (const hir::Declaration &declaration : source_.declarations) {
                    const auto *source = std::get_if<hir::FunctionDecl>(&declaration.node);
                    if (source == nullptr || !declaration.symbol.valid()) { continue; }
                    const CallableId id{static_cast<std::uint32_t>(result_.callables.size())};
                    callables_.emplace(declaration.symbol.value, id);
                    Callable target;
                    target.visibility = lower_visibility(source->visibility);
                    target.identity   = declaration_identity(declaration.id);
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
                        target.capabilities.push_back(Capability{symbol.name, lower_type(symbol.type), binding(capability)});
                    }
                    result_.callables.push_back(std::move(target));
                }

                for (const hir::Declaration &declaration : source_.declarations) {
                    const auto *source = std::get_if<hir::FunctionDecl>(&declaration.node);
                    if (source == nullptr || !declaration.symbol.valid()) { continue; }
                    Callable &target    = result_.callables[callable(declaration.symbol).value];
                    target.concise_body = lower_value(source->concise_body);
                    target.block_body   = lower_block(source->block_body);
                }
            }

            void lower_tests() {
                for (const hir::Declaration &declaration : source_.declarations) {
                    const auto *source = std::get_if<hir::TestDecl>(&declaration.node);
                    if (source == nullptr || !declaration.symbol.valid()) { continue; }
                    result_.tests.push_back(
                        TestPlan{declaration_identity(declaration.id), lower_block(source->block), declaration.range});
                }
            }

            const hir::Module                              &source_;
            syntax::DiagnosticSink                         &diagnostics_;
            Module                                          result_{};
            std::unordered_map<std::uint32_t, TypeId>       types_{};
            std::unordered_map<std::uint32_t, ConstExprId>  const_exprs_{};
            std::unordered_map<std::uint32_t, ConstraintId> constraints_{};
            std::unordered_map<std::uint32_t, BindingId>    bindings_{};
            std::unordered_map<std::uint32_t, CallableId>   callables_{};
            std::unordered_map<std::uint32_t, ValueId>      values_{};
            std::unordered_map<std::uint32_t, StatementId>  statements_{};
            std::unordered_map<std::uint32_t, BlockId>      blocks_{};
        };
    }  // namespace

    Module lower(const ir::hir::Module &source, syntax::DiagnosticSink &diagnostics) { return Lowerer{source, diagnostics}.run(); }
}  // namespace hgl::hgraph_ir
