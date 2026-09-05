#include "ir/type_check.h"

#include "ir/canonical_types.h"
#include "ir/constraint_solver.h"
#include "ir/generic_substitution.h"
#include "syntax/temporal.h"

#include <algorithm>
#include <cmath>
#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <type_traits>
#include <unordered_map>
#include <utility>

namespace hgl::ir
{
    namespace
    {
        using namespace hir;

        [[nodiscard]] Phase join_phase(Phase lhs, Phase rhs) noexcept {
            return static_cast<Phase>(std::max(static_cast<unsigned>(lhs), static_cast<unsigned>(rhs)));
        }

        [[nodiscard]] ValueKind value_kind_for_phase(Phase phase) noexcept {
            switch (phase) {
                case Phase::Constant: return ValueKind::Constant;
                case Phase::Wiring: return ValueKind::Signal;
                case Phase::Runtime: return ValueKind::RuntimeValue;
                case Phase::Unknown: return ValueKind::Unknown;
            }
            std::unreachable();
        }

        [[nodiscard]] std::string_view binary_identity(BinaryOp op) noexcept {
            switch (op) {
                case BinaryOp::Mul: return "mul_";
                case BinaryOp::Div: return "div_";
                case BinaryOp::Rem: return "mod_";
                case BinaryOp::Add: return "add_";
                case BinaryOp::Sub: return "sub_";
                case BinaryOp::Less: return "lt_";
                case BinaryOp::LessEqual: return "le_";
                case BinaryOp::Greater: return "gt_";
                case BinaryOp::GreaterEqual: return "ge_";
                case BinaryOp::Equal: return "eq_";
                case BinaryOp::NotEqual: return "ne_";
                case BinaryOp::And: return "and_";
                case BinaryOp::Or: return "or_";
            }
            std::unreachable();
        }

        [[nodiscard]] std::string_view unary_identity(UnaryOp op) noexcept { return op == UnaryOp::Negate ? "neg_" : "not_"; }

        class TypeChecker
        {
          public:
            TypeChecker(Module &module, const OperatorResolver &resolve_operator, syntax::DiagnosticSink &diagnostics)
                : module_{module}, resolve_operator_{resolve_operator}, diagnostics_{diagnostics},
                  canonical_types_{module, diagnostics},
                  constraint_solver_{module, canonical_types_, resolve_operator, diagnostics} {}

            bool run() {
                if (module_.completion != Completion::Resolved) {
                    diagnostics_.report(syntax::Category::Type, {}, "type completion requires resolved HIR");
                    return false;
                }

                expr_state_.resize(module_.exprs.size());
                check_type_expressions();
                canonical_types_.initialize();
                void_type_ = canonical_types_.void_type();
                for (DeclarationId declaration : module_.source_order) { check_declaration(declaration); }
                validate_completion();
                if (diagnostics_.has_errors()) { return false; }
                module_.completion = Completion::Typed;
                return true;
            }

          private:
            [[nodiscard]] const Type &type(TypeId id) const { return module_.type(id); }
            [[nodiscard]] Type       &type(TypeId id) { return module_.types[id.value]; }

            [[nodiscard]] TypeId canonical(TypeId id) const noexcept { return canonical_types_.canonical(id); }
            [[nodiscard]] TypeId intern(Type value) { return canonical_types_.intern(std::move(value)); }
            [[nodiscard]] TypeId make_type(TypeKind kind, std::vector<TypeId> children = {}, SymbolId symbol = {}) {
                return canonical_types_.make(kind, std::move(children), symbol);
            }
            [[nodiscard]] TypeId scalar(ScalarType value) { return canonical_types_.scalar(value); }
            [[nodiscard]] bool   same(TypeId lhs, TypeId rhs) const noexcept { return canonical_types_.same(lhs, rhs); }
            [[nodiscard]] bool   numeric(TypeId id) const noexcept { return canonical_types_.numeric(id); }
            [[nodiscard]] bool   boolean(TypeId id) const noexcept { return canonical_types_.boolean(id); }
            [[nodiscard]] bool   assignable(TypeId expected, TypeId actual) const noexcept {
                return canonical_types_.assignable(expected, actual);
            }

            void type_error(syntax::SourceRange range, std::string message) {
                diagnostics_.report(syntax::Category::Type, range, std::move(message));
            }

            [[nodiscard]] std::string type_name(TypeId id) const { return canonical_types_.name(id); }

            [[nodiscard]] const FunctionDecl *function(DeclarationId id) const noexcept {
                if (!id.valid() || id.value >= module_.declarations.size()) { return nullptr; }
                return std::get_if<FunctionDecl>(&module_.declaration(id).node);
            }

            [[nodiscard]] FunctionDecl *function(DeclarationId id) noexcept {
                if (!id.valid() || id.value >= module_.declarations.size()) { return nullptr; }
                return std::get_if<FunctionDecl>(&module_.declarations[id.value].node);
            }

            [[nodiscard]] const OperatorDecl *local_operator(std::string_view name) const noexcept {
                for (const Declaration &declaration : module_.declarations) {
                    const auto *op = std::get_if<OperatorDecl>(&declaration.node);
                    if (op && declaration.symbol.valid() && module_.symbol(declaration.symbol).name == name) { return op; }
                }
                return nullptr;
            }

            void check_implementation_conformance(const Declaration &declaration, const FunctionDecl &implementation,
                                                  const OperatorDecl &contract, detail::GenericSubstitution &substitution) {
                if (implementation.signature.parameters.size() != contract.signature.parameters.size()) {
                    type_error(declaration.range, "implementation parameter count does not match its operator contract");
                    return;
                }
                bool conforms = true;
                for (std::size_t index = 0; index < contract.signature.parameters.size(); ++index) {
                    const Parameter &expected = contract.signature.parameters[index];
                    const Parameter &actual   = implementation.signature.parameters[index];
                    if (expected.is_const != actual.is_const ||
                        module_.symbol(expected.symbol).name != module_.symbol(actual.symbol).name) {
                        conforms = false;
                    }
                    conforms = substitution.unify(expected.type, actual.type) && conforms;
                }
                conforms = substitution.unify(contract.signature.result, implementation.signature.result) && conforms;
                for (std::size_t index = 0; conforms && index < contract.signature.parameters.size(); ++index) {
                    conforms = same(substitution.apply(contract.signature.parameters[index].type),
                                    implementation.signature.parameters[index].type);
                }
                if (conforms) { conforms = same(substitution.apply(contract.signature.result), implementation.signature.result); }
                for (const GenericParameter &generic : contract.generics) {
                    conforms =
                        (generic.is_const ? substitution.has_value(generic.symbol) : substitution.has_type(generic.symbol)) &&
                        conforms;
                }
                if (!conforms) {
                    type_error(declaration.range, "implementation signature does not conform to its operator contract");
                }
            }

            [[nodiscard]] bool active_proves_numeric(TypeId type) {
                return constraint_solver_.proves_numeric(active_requirements_, type) ||
                       constraint_solver_.proves_numeric(inherited_requirements_, type,
                                                         inherited_substitution_ ? &*inherited_substitution_ : nullptr);
            }

            [[nodiscard]] std::optional<detail::RequiredOperation> active_required_operation(std::string_view           identity,
                                                                                             const std::vector<TypeId> &arguments) {
                if (auto required = constraint_solver_.required_operation(active_requirements_, identity, arguments)) {
                    return required;
                }
                return constraint_solver_.required_operation(inherited_requirements_, identity, arguments,
                                                             inherited_substitution_ ? &*inherited_substitution_ : nullptr);
            }

            [[nodiscard]] std::optional<TypeId> active_field_type(TypeId subject, std::string_view field) {
                if (auto result = constraint_solver_.field_type(active_requirements_, subject, field)) { return result; }
                return constraint_solver_.field_type(inherited_requirements_, subject, field,
                                                     inherited_substitution_ ? &*inherited_substitution_ : nullptr);
            }

            [[nodiscard]] bool runtime_owner(DeclarationId id) const noexcept {
                const FunctionDecl *fn = function(id);
                return fn != nullptr && fn->kind == FunctionKind::Runtime;
            }

            [[nodiscard]] TypeId callable_type(const Signature &signature) {
                std::vector<TypeId> children;
                children.reserve(signature.parameters.size() + 1U);
                for (const Parameter &parameter : signature.parameters) { children.push_back(parameter.type); }
                children.push_back(signature.result);
                return make_type(TypeKind::Callable, std::move(children));
            }

            [[nodiscard]] TypeId callable_type(SymbolId symbol) {
                if (!symbol.valid()) { return make_type(TypeKind::Callable); }
                const Symbol &target = module_.symbol(symbol);
                if (!target.owner.valid()) { return make_type(TypeKind::Callable); }
                const DeclarationNode &node = module_.declaration(target.owner).node;
                if (const auto *fn = std::get_if<FunctionDecl>(&node)) { return callable_type(fn->signature); }
                if (const auto *op = std::get_if<OperatorDecl>(&node)) { return callable_type(op->signature); }
                return make_type(TypeKind::Callable);
            }

            void check_declaration(DeclarationId id) {
                if (!id.valid()) { return; }
                Declaration &declaration = module_.declarations[id.value];
                std::visit(
                    [&](auto &node) {
                        using T = std::decay_t<decltype(node)>;
                        if constexpr (std::is_same_v<T, StructDecl>) {
                            for (StructField &field : node.fields) {
                                if (!field.default_value.valid()) { continue; }
                                Expr &value = check_expr(field.default_value, field.type);
                                if (value.constant && std::holds_alternative<NullValue>(*value.constant)) {
                                    if (!field.optional) { type_error(value.range, "null is only valid for an optional field"); }
                                } else {
                                    require_assignable(field.type, value, "field default");
                                }
                            }
                        } else if constexpr (std::is_same_v<T, OperatorDecl>) {
                            check_signature_defaults(node.signature);
                        } else if constexpr (std::is_same_v<T, FunctionDecl>) {
                            const ConstraintId previous_requirements = active_requirements_;
                            const ConstraintId previous_inherited    = inherited_requirements_;
                            active_requirements_                     = node.requirements;
                            inherited_requirements_                  = {};
                            inherited_substitution_.reset();
                            if (node.visibility == Visibility::Implementation && declaration.symbol.valid()) {
                                if (const OperatorDecl *contract = local_operator(module_.symbol(declaration.symbol).name)) {
                                    inherited_requirements_ = contract->requirements;
                                    inherited_substitution_.emplace(module_, canonical_types_);
                                    check_implementation_conformance(declaration, node, *contract, *inherited_substitution_);
                                }
                            }
                            check_signature_defaults(node.signature);
                            if (node.concise_body.valid()) {
                                Expr &body = check_expr(node.concise_body, node.signature.result);
                                require_assignable(node.signature.result, body, "function result");
                                node.effects = body.effects;
                            } else if (node.block_body.valid()) {
                                check_block(node.block_body, node.signature.result);
                                const Block &body = module_.block(node.block_body);
                                if (body.tail.valid()) {
                                    require_assignable(node.signature.result, module_.expr(body.tail), "function result");
                                }
                                node.effects = body.effects;
                            }
                            collect_capabilities(node, id);
                            active_requirements_    = previous_requirements;
                            inherited_requirements_ = previous_inherited;
                            inherited_substitution_.reset();
                        } else if constexpr (std::is_same_v<T, TestDecl>) {
                            check_block(node.block, void_type_);
                        }
                    },
                    declaration.node);
            }

            void check_signature_defaults(Signature &signature) {
                for (Parameter &parameter : signature.parameters) {
                    if (!parameter.default_value.valid()) { continue; }
                    Expr &value = check_expr(parameter.default_value, parameter.type);
                    require_assignable(parameter.type, value, "parameter default");
                    if (value.phase != Phase::Constant) {
                        diagnostics_.report(syntax::Category::Phase, value.range,
                                            "a parameter default must be a compile-time constant");
                    }
                }
            }

            void collect_capabilities(FunctionDecl &fn, DeclarationId owner) {
                fn.capabilities.clear();
                for (const Stmt &statement : module_.stmts) {
                    if (statement.owner != owner) { continue; }
                    if (const auto *inject = std::get_if<InjectDecl>(&statement.node)) {
                        fn.capabilities.insert(fn.capabilities.end(), inject->symbols.begin(), inject->symbols.end());
                    }
                }
            }

            void require_assignable(TypeId expected, const Expr &actual, std::string_view what) {
                if (!expected.valid() || !actual.type.valid()) { return; }
                if (!assignable(expected, actual.type)) {
                    type_error(actual.range,
                               std::string{what} + " has type " + type_name(actual.type) + ", expected " + type_name(expected));
                }
            }

            void check_type_expressions() {
                const std::size_t type_count = module_.types.size();
                for (std::size_t index = 0; index < type_count; ++index) {
                    const Type value          = module_.types[index];
                    const auto check_constant = [&](ExprId id) {
                        if (!id.valid()) { return; }
                        Expr &expression = check_expr(id);
                        if (expression.phase != Phase::Constant) {
                            diagnostics_.report(syntax::Category::Phase, expression.range,
                                                "a type argument requires a compile-time value");
                        }
                    };
                    check_constant(value.size);
                    check_constant(value.min_size);
                    for (const TypeArgument &argument : value.arguments) {
                        if (argument.kind == TypeArgumentKind::Value) { check_constant(argument.value); }
                    }
                }
            }

            Expr &check_expr(ExprId id, TypeId expected = {}) {
                if (!id.valid()) { return missing_expression_; }
                if (id.value >= expr_state_.size()) { expr_state_.resize(module_.exprs.size()); }
                Expr &expression = module_.exprs[id.value];
                if (expr_state_[id.value] == 2U) {
                    contextualize(expression, expected);
                    return expression;
                }
                if (expr_state_[id.value] == 1U) {
                    type_error(expression.range, "cyclic expression");
                    return expression;
                }
                expr_state_[id.value] = 1U;
                std::visit(
                    [&](auto &node) {
                        using T = std::decay_t<decltype(node)>;
                        if constexpr (std::is_same_v<T, Literal>) {
                            check_literal(expression, expected);
                        } else if constexpr (std::is_same_v<T, SymbolRef>) {
                            check_symbol(expression, node);
                        } else if constexpr (std::is_same_v<T, Unary>) {
                            check_unary(expression, node);
                        } else if constexpr (std::is_same_v<T, Binary>) {
                            check_binary(expression, node, expected);
                        } else if constexpr (std::is_same_v<T, Call>) {
                            check_call(expression, node, expected);
                        } else if constexpr (std::is_same_v<T, Index>) {
                            check_index(expression, node);
                        } else if constexpr (std::is_same_v<T, Field>) {
                            check_field(expression, node);
                        } else if constexpr (std::is_same_v<T, Sequence>) {
                            check_sequence(expression, node, expected);
                        } else if constexpr (std::is_same_v<T, Tuple>) {
                            check_tuple(expression, node, expected);
                        } else if constexpr (std::is_same_v<T, Lambda>) {
                            check_lambda(expression, node, expected);
                        } else if constexpr (std::is_same_v<T, If>) {
                            check_if(expression, node, expected);
                        } else if constexpr (std::is_same_v<T, BlockExpr>) {
                            check_block_expr(expression, node, expected);
                        } else if constexpr (std::is_same_v<T, Eval>) {
                            check_eval(expression, node);
                        } else if constexpr (std::is_same_v<T, Construct>) {
                            check_construct(expression, node, expected);
                        }
                    },
                    expression.node);
                contextualize(expression, expected);
                expr_state_[id.value] = 2U;
                return expression;
            }

            void contextualize(Expr &expression, TypeId expected) {
                if (!expected.valid()) { return; }
                expected = canonical(expected);
                if (!expression.type.valid() && expression.constant &&
                    (std::holds_alternative<NullValue>(*expression.constant) ||
                     std::holds_alternative<PlaceholderValue>(*expression.constant))) {
                    expression.type = expected;
                }
                if (expression.type.valid() && assignable(expected, expression.type) && type(expected).kind == TypeKind::Atomic &&
                    type(expression.type).kind != TypeKind::Atomic) {
                    expression.type = expected;
                }
            }

            void check_literal(Expr &expression, TypeId expected) {
                if (expression.type.valid()) { expression.type = canonical(expression.type); }
                contextualize(expression, expected);
                if (!expression.type.valid()) { type_error(expression.range, "null or '_' requires an expected type"); }
                expression.phase      = Phase::Constant;
                expression.value_kind = ValueKind::Constant;
            }

            void check_symbol(Expr &expression, const SymbolRef &reference) {
                if (!reference.symbol.valid()) { return; }
                Symbol &symbol = module_.symbols[reference.symbol.value];
                switch (symbol.kind) {
                    case SymbolKind::ConstParameter:
                        expression.type       = canonical(symbol.type);
                        expression.phase      = Phase::Constant;
                        expression.value_kind = ValueKind::Constant;
                        break;
                    case SymbolKind::SignalParameter:
                        expression.type = canonical(symbol.type);
                        if (runtime_owner(expression.owner)) {
                            expression.phase      = Phase::Runtime;
                            expression.value_kind = ValueKind::RuntimeValue;
                            expression.effects    = Effect::ReadRuntimeInput;
                        } else {
                            expression.phase      = Phase::Wiring;
                            expression.value_kind = ValueKind::Signal;
                        }
                        break;
                    case SymbolKind::LocalLet:
                    case SymbolKind::LocalVar:
                    case SymbolKind::LoopValue:
                    case SymbolKind::LambdaParameter:
                        expression.type = canonical(symbol.type);
                        if (const auto found = symbol_phase_.find(reference.symbol.value); found != symbol_phase_.end()) {
                            expression.phase = found->second;
                        } else {
                            expression.phase = runtime_owner(expression.owner) ? Phase::Runtime : Phase::Wiring;
                        }
                        expression.value_kind = value_kind_for_phase(expression.phase);
                        break;
                    case SymbolKind::State:
                        expression.type       = canonical(symbol.type);
                        expression.phase      = Phase::Runtime;
                        expression.value_kind = ValueKind::RuntimeValue;
                        expression.effects    = Effect::ReadState;
                        break;
                    case SymbolKind::InjectedCapability:
                        expression.type       = canonical(symbol.type);
                        expression.phase      = Phase::Runtime;
                        expression.value_kind = symbol.name == "out" ? ValueKind::RuntimeValue : ValueKind::Function;
                        expression.effects    = Effect::UseCapability;
                        break;
                    case SymbolKind::Function:
                        expression.type       = callable_type(reference.symbol);
                        expression.phase      = Phase::Constant;
                        expression.value_kind = ValueKind::Function;
                        break;
                    case SymbolKind::Operator:
                    case SymbolKind::ImportedOperator:
                        expression.type       = callable_type(reference.symbol);
                        expression.phase      = Phase::Constant;
                        expression.value_kind = ValueKind::Operator;
                        break;
                    case SymbolKind::Intrinsic:
                        expression.type       = make_type(TypeKind::Callable);
                        expression.phase      = Phase::Constant;
                        expression.value_kind = ValueKind::Function;
                        break;
                    case SymbolKind::Struct:
                    case SymbolKind::TypeParameter:
                        expression.type       = make_type(TypeKind::Symbol, {}, reference.symbol);
                        expression.phase      = Phase::Constant;
                        expression.value_kind = ValueKind::Type;
                        break;
                    case SymbolKind::Module:
                    case SymbolKind::Test: break;
                }
            }

            void check_unary(Expr &expression, const Unary &node) {
                Expr &operand        = check_expr(node.operand);
                expression.effects   = operand.effects;
                expression.phase     = operand.phase;
                expression.operation = Operation{.kind     = OperationKind::NominalOperator,
                                                 .identity = std::string{unary_identity(node.op)},
                                                 .deferred = operand.phase != Phase::Constant};
                const auto required  = active_required_operation(unary_identity(node.op), {operand.type});
                if (required) {
                    expression.type               = required->result.valid() ? required->result : operand.type;
                    expression.operation.target   = required->op;
                    expression.operation.identity = required->identity;
                    if (expression.phase == Phase::Wiring) { expression.effects |= Effect::WireGraph; }
                    expression.value_kind = value_kind_for_phase(expression.phase);
                    return;
                }
                if (node.op == UnaryOp::Not) {
                    if (!boolean(operand.type)) { type_error(operand.range, "'!' requires bool"); }
                    expression.type = scalar(ScalarType::Bool);
                    if (operand.constant && std::holds_alternative<bool>(*operand.constant)) {
                        expression.constant = Constant{!std::get<bool>(*operand.constant)};
                    }
                } else {
                    if (!numeric(operand.type) && !active_proves_numeric(operand.type)) {
                        type_error(operand.range, "unary '-' requires i64 or f64");
                    }
                    expression.type = operand.type;
                    if (operand.constant && std::holds_alternative<std::int64_t>(*operand.constant)) {
                        expression.constant = Constant{-std::get<std::int64_t>(*operand.constant)};
                    } else if (operand.constant && std::holds_alternative<double>(*operand.constant)) {
                        expression.constant = Constant{-std::get<double>(*operand.constant)};
                    }
                }
                if (expression.phase == Phase::Wiring) { expression.effects |= Effect::WireGraph; }
                expression.value_kind = value_kind_for_phase(expression.phase);
            }

            [[nodiscard]] TypeId arithmetic_result(BinaryOp op, TypeId lhs, TypeId rhs, syntax::SourceRange range) {
                if (op == BinaryOp::Add && same(lhs, scalar(ScalarType::Str)) && same(rhs, scalar(ScalarType::Str))) {
                    return scalar(ScalarType::Str);
                }
                const bool lhs_numeric = numeric(lhs) || active_proves_numeric(lhs);
                const bool rhs_numeric = numeric(rhs) || active_proves_numeric(rhs);
                if (!lhs_numeric || !rhs_numeric) {
                    type_error(range, "arithmetic operands must both be numeric");
                    return {};
                }
                if (op != BinaryOp::Div && same(lhs, rhs) && !numeric(lhs)) { return lhs; }
                if (op == BinaryOp::Div || same(lhs, scalar(ScalarType::F64)) || same(rhs, scalar(ScalarType::F64))) {
                    return scalar(ScalarType::F64);
                }
                return scalar(ScalarType::I64);
            }

            void fold_binary(Expr &expression, BinaryOp op, const Expr &lhs, const Expr &rhs) {
                if (!lhs.constant || !rhs.constant) { return; }
                const Constant &a         = *lhs.constant;
                const Constant &b         = *rhs.constant;
                const auto      as_double = [](const Constant &value) -> std::optional<double> {
                    if (const auto *integer = std::get_if<std::int64_t>(&value)) { return static_cast<double>(*integer); }
                    if (const auto *floating = std::get_if<double>(&value)) { return *floating; }
                    return std::nullopt;
                };
                if (op == BinaryOp::And || op == BinaryOp::Or) {
                    if (const auto *left = std::get_if<bool>(&a)) {
                        if (const auto *right = std::get_if<bool>(&b)) {
                            expression.constant = Constant{op == BinaryOp::And ? (*left && *right) : (*left || *right)};
                        }
                    }
                    return;
                }
                if (op == BinaryOp::Equal || op == BinaryOp::NotEqual) {
                    const bool equal    = a == b;
                    expression.constant = Constant{op == BinaryOp::Equal ? equal : !equal};
                    return;
                }
                const std::optional<double> left  = as_double(a);
                const std::optional<double> right = as_double(b);
                if (!left || !right) {
                    if (op == BinaryOp::Add) {
                        const auto *left_string  = std::get_if<std::string>(&a);
                        const auto *right_string = std::get_if<std::string>(&b);
                        if (left_string && right_string) { expression.constant = Constant{*left_string + *right_string}; }
                    }
                    return;
                }
                switch (op) {
                    case BinaryOp::Less: expression.constant = Constant{*left < *right}; return;
                    case BinaryOp::LessEqual: expression.constant = Constant{*left <= *right}; return;
                    case BinaryOp::Greater: expression.constant = Constant{*left > *right}; return;
                    case BinaryOp::GreaterEqual: expression.constant = Constant{*left >= *right}; return;
                    case BinaryOp::Add: expression.constant = Constant{*left + *right}; break;
                    case BinaryOp::Sub: expression.constant = Constant{*left - *right}; break;
                    case BinaryOp::Mul: expression.constant = Constant{*left * *right}; break;
                    case BinaryOp::Div:
                        if (*right == 0.0) {
                            type_error(expression.range, "division by zero in a constant expression");
                        } else {
                            expression.constant = Constant{*left / *right};
                        }
                        break;
                    case BinaryOp::Rem:
                        if (*right == 0.0) {
                            type_error(expression.range, "remainder by zero in a constant expression");
                        } else {
                            expression.constant = Constant{std::fmod(*left, *right)};
                        }
                        break;
                    case BinaryOp::Equal:
                    case BinaryOp::NotEqual:
                    case BinaryOp::And:
                    case BinaryOp::Or: return;
                }
                if (expression.constant && same(expression.type, scalar(ScalarType::I64)) &&
                    std::holds_alternative<double>(*expression.constant)) {
                    expression.constant = Constant{static_cast<std::int64_t>(std::get<double>(*expression.constant))};
                }
            }

            void check_binary(Expr &expression, const Binary &node, TypeId expected) {
                Expr &lhs = check_expr(node.lhs);
                Expr &rhs = check_expr(node.rhs, lhs.type.valid() ? lhs.type : expected);
                if (!lhs.type.valid() && rhs.type.valid()) { contextualize(lhs, rhs.type); }
                expression.phase     = join_phase(lhs.phase, rhs.phase);
                expression.effects   = lhs.effects | rhs.effects;
                expression.operation = Operation{.kind     = OperationKind::NominalOperator,
                                                 .identity = std::string{binary_identity(node.op)},
                                                 .deferred = expression.phase != Phase::Constant};
                const auto required  = active_required_operation(binary_identity(node.op), {lhs.type, rhs.type});
                if (required) {
                    expression.operation.target   = required->op;
                    expression.operation.identity = required->identity;
                    if (required->result.valid()) {
                        expression.type = required->result;
                    } else if (expected.valid()) {
                        expression.type = canonical(expected);
                    } else if (node.op == BinaryOp::Less || node.op == BinaryOp::LessEqual || node.op == BinaryOp::Greater ||
                               node.op == BinaryOp::GreaterEqual || node.op == BinaryOp::Equal || node.op == BinaryOp::NotEqual ||
                               node.op == BinaryOp::And || node.op == BinaryOp::Or) {
                        expression.type = scalar(ScalarType::Bool);
                    } else {
                        expression.type = lhs.type;
                    }
                    if (expression.phase == Phase::Wiring) { expression.effects |= Effect::WireGraph; }
                    expression.value_kind = value_kind_for_phase(expression.phase);
                    return;
                }
                switch (node.op) {
                    case BinaryOp::And:
                    case BinaryOp::Or:
                        if (!boolean(lhs.type) || !boolean(rhs.type)) {
                            type_error(expression.range, "logical operands must both be bool");
                        }
                        expression.type = scalar(ScalarType::Bool);
                        break;
                    case BinaryOp::Less:
                    case BinaryOp::LessEqual:
                    case BinaryOp::Greater:
                    case BinaryOp::GreaterEqual:
                        if (!assignable(lhs.type, rhs.type) && !assignable(rhs.type, lhs.type)) {
                            type_error(expression.range, "comparison operands have incompatible types");
                        }
                        expression.type = scalar(ScalarType::Bool);
                        break;
                    case BinaryOp::Equal:
                    case BinaryOp::NotEqual:
                        if (!assignable(lhs.type, rhs.type) && !assignable(rhs.type, lhs.type)) {
                            type_error(expression.range, "equality operands have incompatible types");
                        }
                        expression.type = scalar(ScalarType::Bool);
                        break;
                    case BinaryOp::Mul:
                    case BinaryOp::Div:
                    case BinaryOp::Rem:
                    case BinaryOp::Add:
                    case BinaryOp::Sub: expression.type = arithmetic_result(node.op, lhs.type, rhs.type, expression.range); break;
                }
                fold_binary(expression, node.op, lhs, rhs);
                if (expression.phase == Phase::Wiring) { expression.effects |= Effect::WireGraph; }
                expression.value_kind = value_kind_for_phase(expression.phase);
            }

            [[nodiscard]] std::vector<ExprId> bind_arguments(const Signature &signature, const std::vector<Argument> &arguments,
                                                             syntax::SourceRange range) {
                std::vector<ExprId> bound(signature.parameters.size());
                std::size_t         next = 0;
                for (const Argument &argument : arguments) {
                    if (argument.name.empty()) {
                        if (next >= bound.size()) {
                            type_error(argument.range, "too many positional arguments");
                            continue;
                        }
                        while (next < bound.size() && bound[next].valid()) { ++next; }
                        if (next >= bound.size()) {
                            type_error(argument.range, "too many positional arguments");
                            continue;
                        }
                        bound[next++] = argument.value;
                    } else {
                        const auto found =
                            std::find_if(signature.parameters.begin(), signature.parameters.end(), [&](const Parameter &parameter) {
                                return module_.symbol(parameter.symbol).name == argument.name;
                            });
                        if (found == signature.parameters.end()) {
                            diagnostics_.report(syntax::Category::Name, argument.range,
                                                "unknown parameter '" + argument.name + "'");
                            continue;
                        }
                        const std::size_t index = static_cast<std::size_t>(found - signature.parameters.begin());
                        if (bound[index].valid()) {
                            type_error(argument.range, "parameter '" + argument.name + "' is supplied twice");
                        } else {
                            bound[index] = argument.value;
                        }
                    }
                }
                for (std::size_t index = 0; index < bound.size(); ++index) {
                    if (!bound[index].valid()) { bound[index] = signature.parameters[index].default_value; }
                    if (!bound[index].valid()) {
                        type_error(range, "missing argument '" + module_.symbol(signature.parameters[index].symbol).name + "'");
                    }
                }
                return bound;
            }

            void require_complete_bindings(const std::vector<GenericParameter> &generics,
                                           const detail::GenericSubstitution &bindings, syntax::SourceRange range,
                                           std::string_view callable) {
                for (const GenericParameter &generic : generics) {
                    const bool bound = generic.is_const ? bindings.has_value(generic.symbol) : bindings.has_type(generic.symbol);
                    if (bound) { continue; }
                    type_error(range,
                               "cannot infer generic '" + module_.symbol(generic.symbol).name + "' for " + std::string{callable});
                }
            }

            void check_exact_call(Expr &expression, const Call &call, SymbolId target, const FunctionDecl &fn, TypeId expected) {
                const std::vector<ExprId>   bound = bind_arguments(fn.signature, call.arguments, expression.range);
                detail::GenericSubstitution bindings{module_, canonical_types_};
                for (std::size_t index = 0; index < bound.size(); ++index) {
                    if (!bound[index].valid()) { continue; }
                    Expr            &argument  = check_expr(bound[index]);
                    const Parameter &parameter = fn.signature.parameters[index];
                    if (parameter.is_const) {
                        if (argument.phase != Phase::Constant) {
                            diagnostics_.report(syntax::Category::Phase, argument.range,
                                                "a const parameter requires a compile-time value");
                        }
                        if (!bindings.bind_value(parameter.symbol, bound[index])) {
                            type_error(argument.range, "const parameter has an inconsistent value binding");
                        }
                    }
                    if (!bindings.unify(fn.signature.parameters[index].type, argument.type)) {
                        type_error(argument.range, "argument has type " + type_name(argument.type) + ", expected " +
                                                       type_name(fn.signature.parameters[index].type));
                    }
                }
                if (expected.valid()) { (void)bindings.unify(fn.signature.result, expected); }
                (void)constraint_solver_.solve(fn.requirements, bindings, expression.range, "function call");
                require_complete_bindings(fn.generics, bindings, expression.range, "function call");
                for (std::size_t index = 0; index < bound.size(); ++index) {
                    if (!bound[index].valid()) { continue; }
                    TypeId parameter = bindings.apply(fn.signature.parameters[index].type);
                    require_assignable(parameter, module_.expr(bound[index]), "argument");
                }
                expression.type    = bindings.apply(fn.signature.result);
                expression.phase   = Phase::Constant;
                expression.effects = Effect::None;
                for (ExprId argument : bound) {
                    if (!argument.valid()) { continue; }
                    const Expr &value = module_.expr(argument);
                    expression.phase  = join_phase(expression.phase, value.phase);
                    expression.effects |= value.effects;
                }
                if (runtime_owner(expression.owner)) {
                    expression.phase = Phase::Runtime;
                } else if (expression.type != void_type_) {
                    expression.phase = Phase::Wiring;
                    expression.effects |= Effect::WireGraph;
                }
                expression.value_kind = expression.type == void_type_ ? ValueKind::Void : value_kind_for_phase(expression.phase);
                expression.operation  = Operation{.kind          = OperationKind::ExactFunction,
                                                  .target        = target,
                                                  .identity      = module_.path + "." + module_.symbol(target).name,
                                                  .substitutions = bindings.materialize(fn.generics)};
            }

            [[nodiscard]] const OperatorDecl *operator_decl(SymbolId symbol) const noexcept {
                if (!symbol.valid()) { return nullptr; }
                const Symbol &target = module_.symbol(symbol);
                if (!target.owner.valid()) { return nullptr; }
                return std::get_if<OperatorDecl>(&module_.declaration(target.owner).node);
            }

            [[nodiscard]] bool local_candidate_matches(const FunctionDecl &candidate, const std::vector<ExprId> &arguments,
                                                       TypeId expected) {
                if (candidate.signature.parameters.size() != arguments.size()) { return false; }
                detail::GenericSubstitution bindings{module_, canonical_types_};
                for (std::size_t index = 0; index < arguments.size(); ++index) {
                    if (!arguments[index].valid()) { continue; }
                    const Expr      &argument  = module_.expr(arguments[index]);
                    const Parameter &parameter = candidate.signature.parameters[index];
                    if (parameter.is_const && argument.phase != Phase::Constant) { return false; }
                    if (parameter.is_const && !bindings.bind_value(parameter.symbol, arguments[index])) { return false; }
                    if (!bindings.unify(parameter.type, argument.type)) { return false; }
                }
                if (expected.valid() && !bindings.unify(candidate.signature.result, expected)) { return false; }
                if (!constraint_solver_.solve(candidate.requirements, bindings, {}, "implementation", false)) { return false; }
                for (const GenericParameter &generic : candidate.generics) {
                    if (generic.is_const ? !bindings.has_value(generic.symbol) : !bindings.has_type(generic.symbol)) {
                        return false;
                    }
                }
                for (std::size_t index = 0; index < arguments.size(); ++index) {
                    if (!arguments[index].valid()) { continue; }
                    if (!same(bindings.apply(candidate.signature.parameters[index].type), module_.expr(arguments[index]).type)) {
                        return false;
                    }
                }
                return true;
            }

            [[nodiscard]] SymbolId sole_local_candidate(SymbolId op, const std::vector<ExprId> &arguments, TypeId expected) {
                std::vector<SymbolId> candidates;
                const std::string     name = module_.symbol(op).name;
                for (const Declaration &declaration : module_.declarations) {
                    const auto *candidate = std::get_if<FunctionDecl>(&declaration.node);
                    if (!candidate || candidate->visibility != Visibility::Implementation || !declaration.symbol.valid() ||
                        module_.symbol(declaration.symbol).name != name) {
                        continue;
                    }
                    candidates.push_back(declaration.symbol);
                }
                // A sole implementation has an unambiguous stable identity;
                // two or more still require hgraph's TypePattern ranking. Do
                // not reproduce that ranking in the language pass.
                if (candidates.size() != 1U) { return {}; }
                const Symbol &symbol = module_.symbol(candidates.front());
                const auto   *candidate =
                    symbol.owner.valid() ? std::get_if<FunctionDecl>(&module_.declaration(symbol.owner).node) : nullptr;
                return candidate != nullptr && local_candidate_matches(*candidate, arguments, expected) ? candidates.front()
                                                                                                        : SymbolId{};
            }

            void check_local_operator_call(Expr &expression, const Call &call, SymbolId target, const OperatorDecl &op,
                                           TypeId expected) {
                const std::vector<ExprId>   bound = bind_arguments(op.signature, call.arguments, expression.range);
                detail::GenericSubstitution contract_bindings{module_, canonical_types_};
                for (std::size_t index = 0; index < bound.size(); ++index) {
                    if (!bound[index].valid()) { continue; }
                    Expr            &argument  = check_expr(bound[index]);
                    const Parameter &parameter = op.signature.parameters[index];
                    if (parameter.is_const) {
                        if (argument.phase != Phase::Constant) {
                            diagnostics_.report(syntax::Category::Phase, argument.range,
                                                "a const parameter requires a compile-time value");
                        }
                        if (!contract_bindings.bind_value(parameter.symbol, bound[index])) {
                            type_error(argument.range, "const parameter has an inconsistent value binding");
                        }
                    }
                    if (!contract_bindings.unify(op.signature.parameters[index].type, argument.type)) {
                        type_error(argument.range, "operator argument does not match its contract");
                    }
                }
                if (expected.valid()) { (void)contract_bindings.unify(op.signature.result, expected); }
                (void)constraint_solver_.solve(op.requirements, contract_bindings, expression.range, "operator call");
                require_complete_bindings(op.generics, contract_bindings, expression.range, "operator call");
                expression.type          = contract_bindings.apply(op.signature.result);
                const SymbolId candidate = sole_local_candidate(target, bound, expected);
                finish_call_semantics(expression, bound);
                expression.operation = Operation{.kind          = OperationKind::NominalOperator,
                                                 .target        = target,
                                                 .candidate     = candidate,
                                                 .identity      = module_.path + "." + module_.symbol(target).name,
                                                 .substitutions = contract_bindings.materialize(op.generics),
                                                 .deferred      = !candidate.valid()};
            }

            void finish_call_semantics(Expr &expression, const std::vector<ExprId> &arguments) {
                expression.phase   = Phase::Constant;
                expression.effects = Effect::None;
                for (ExprId argument : arguments) {
                    if (!argument.valid()) { continue; }
                    const Expr &value = module_.expr(argument);
                    expression.phase  = join_phase(expression.phase, value.phase);
                    expression.effects |= value.effects;
                }
                if (runtime_owner(expression.owner)) {
                    expression.phase = Phase::Runtime;
                } else if (expression.type != void_type_) {
                    expression.phase = Phase::Wiring;
                    expression.effects |= Effect::WireGraph;
                }
                expression.value_kind = expression.type == void_type_ ? ValueKind::Void : value_kind_for_phase(expression.phase);
            }

            void infer_map_lambda(const Call &call, TypeId expected) {
                if (call.arguments.empty()) { return; }
                const Argument &last   = call.arguments.back();
                auto           *lambda = std::get_if<Lambda>(&module_.exprs[last.value.value].node);
                if (!lambda) { return; }
                std::vector<TypeId> parameter_types;
                for (std::size_t index = 0; index + 1U < call.arguments.size(); ++index) {
                    Expr       &collection      = check_expr(call.arguments[index].value);
                    const Type &collection_type = type(canonical(collection.type));
                    if (collection_type.kind == TypeKind::Map && collection_type.children.size() == 2U) {
                        parameter_types.push_back(collection_type.children[1]);
                    }
                }
                TypeId result;
                if (expected.valid()) {
                    const Type &expected_type = type(canonical(expected));
                    if (expected_type.kind == TypeKind::Map && expected_type.children.size() == 2U) {
                        result = expected_type.children[1];
                    }
                }
                apply_lambda_context(last.value, parameter_types, result);
            }

            void check_imported_operator_call(Expr &expression, const Call &call, SymbolId target, TypeId expected) {
                const Symbol &symbol = module_.symbol(target);
                if (symbol.name == "map" || symbol.external_name == "map_") { infer_map_lambda(call, expected); }
                std::vector<ExprId> argument_ids;
                OperatorQuery       query;
                query.identity        = symbol.external_name;
                query.expected_result = canonical(expected);
                query.range           = expression.range;
                for (const Argument &argument : call.arguments) {
                    Expr &value = check_expr(argument.value);
                    argument_ids.push_back(argument.value);
                    query.arguments.push_back(
                        OperatorArgument{argument.name, value.type, value.phase, value.value_kind, value.constant});
                }
                OperatorSelection selection;
                if (resolve_operator_) {
                    selection = resolve_operator_(module_, query);
                } else {
                    selection.result   = query.expected_result;
                    selection.deferred = true;
                }
                if (!selection.error.empty()) {
                    diagnostics_.report(syntax::Category::Operator, expression.range, std::move(selection.error));
                }
                expression.type = selection.result.valid() ? canonical(selection.result) : canonical(expected);
                if (!expression.type.valid()) {
                    type_error(expression.range, "operator '" + symbol.name + "' needs an expected result type");
                    expression.type = void_type_;
                }
                finish_call_semantics(expression, argument_ids);
                expression.operation = Operation{.kind            = OperationKind::NominalOperator,
                                                 .target          = target,
                                                 .identity        = symbol.external_name,
                                                 .candidate_label = std::move(selection.candidate_label),
                                                 .substitutions   = std::move(selection.substitutions),
                                                 .deferred        = selection.deferred};
            }

            void check_call(Expr &expression, const Call &call, TypeId expected) {
                Expr       &callee    = check_expr(call.callee);
                const auto *reference = std::get_if<SymbolRef>(&callee.node);
                if (reference && reference->symbol.valid()) {
                    const Symbol &symbol = module_.symbol(reference->symbol);
                    if (symbol.kind == SymbolKind::Function) {
                        const auto *fn = function(symbol.owner);
                        if (fn) { check_exact_call(expression, call, reference->symbol, *fn, expected); }
                        return;
                    }
                    if (symbol.kind == SymbolKind::Operator) {
                        const OperatorDecl *op = operator_decl(reference->symbol);
                        if (op) { check_local_operator_call(expression, call, reference->symbol, *op, expected); }
                        return;
                    }
                    if (symbol.kind == SymbolKind::ImportedOperator) {
                        check_imported_operator_call(expression, call, reference->symbol, expected);
                        return;
                    }
                    if (symbol.kind == SymbolKind::Intrinsic) {
                        check_intrinsic_call(expression, call, reference->symbol, expected);
                        return;
                    }
                    if (symbol.kind == SymbolKind::Struct) {
                        check_struct_call(expression, call, reference->symbol, expected);
                        return;
                    }
                }
                if (const auto *field = std::get_if<Field>(&callee.node)) {
                    check_capability_call(expression, call, *field);
                    return;
                }
                type_error(callee.range, "expression is not callable");
            }

            [[nodiscard]] TypeId unwrap_atomic(TypeId id) const noexcept {
                id = canonical(id);
                if (!id.valid()) { return {}; }
                const Type &value = type(id);
                return value.kind == TypeKind::Atomic && !value.children.empty() ? value.children.front() : id;
            }

            void check_index(Expr &expression, const Index &node) {
                Expr  &target  = check_expr(node.target);
                Expr  &index   = check_expr(node.index);
                TypeId base_id = unwrap_atomic(target.type);
                if (!base_id.valid()) { return; }
                const Type &base = type(base_id);
                if (base.kind == TypeKind::Tuple) {
                    const auto *constant = index.constant ? std::get_if<std::int64_t>(&*index.constant) : nullptr;
                    if (!constant || *constant < 0 || static_cast<std::size_t>(*constant) >= base.children.size()) {
                        type_error(index.range, "tuple index must be a constant in range");
                    } else {
                        expression.type = base.children[static_cast<std::size_t>(*constant)];
                    }
                } else if (base.kind == TypeKind::List && !base.children.empty()) {
                    if (!same(index.type, scalar(ScalarType::I64))) { type_error(index.range, "list index must be i64"); }
                    expression.type = base.children.front();
                } else if (base.kind == TypeKind::Map && base.children.size() == 2U) {
                    require_assignable(base.children.front(), index, "map key");
                    expression.type = base.children[1];
                } else {
                    type_error(target.range, "this type cannot be indexed");
                }
                expression.phase      = join_phase(target.phase, index.phase);
                expression.value_kind = value_kind_for_phase(expression.phase);
                expression.effects    = target.effects | index.effects;
                expression.operation  = Operation{
                    .kind = OperationKind::Index, .identity = "getitem_", .deferred = expression.phase != Phase::Constant};
                if (expression.phase == Phase::Wiring) { expression.effects |= Effect::WireGraph; }
            }

            void check_field(Expr &expression, const Field &node) {
                Expr &target = check_expr(node.target);
                if (const auto *reference = std::get_if<SymbolRef>(&target.node);
                    reference && reference->symbol.valid() &&
                    module_.symbol(reference->symbol).kind == SymbolKind::InjectedCapability) {
                    expression.type       = make_type(TypeKind::Callable);
                    expression.phase      = Phase::Runtime;
                    expression.value_kind = ValueKind::Function;
                    expression.effects    = target.effects;
                    expression.operation  = Operation{.kind     = OperationKind::Capability,
                                                      .target   = reference->symbol,
                                                      .identity = module_.symbol(reference->symbol).name + "." + node.name};
                    return;
                }
                const TypeId base_id = unwrap_atomic(target.type);
                if (base_id.valid()) {
                    if (const auto field = active_field_type(base_id, node.name)) { expression.type = *field; }
                }
                if (!expression.type.valid()) { type_error(node.name_range, "type has no field '" + node.name + "'"); }
                expression.phase      = target.phase;
                expression.value_kind = value_kind_for_phase(expression.phase);
                expression.effects    = target.effects;
                expression.operation  = Operation{
                    .kind = OperationKind::Field, .identity = "getattr_", .deferred = expression.phase != Phase::Constant};
                if (expression.phase == Phase::Wiring) { expression.effects |= Effect::WireGraph; }
            }

            void check_sequence(Expr &expression, const Sequence &node, TypeId expected) {
                TypeId element_expected;
                if (expected.valid()) {
                    const Type &shape = type(canonical(expected));
                    if ((shape.kind == TypeKind::List || shape.kind == TypeKind::HarnessSequence) && !shape.children.empty()) {
                        element_expected = shape.children.front();
                    }
                }
                TypeId element_type = element_expected;
                Phase  phase        = Phase::Constant;
                for (const SequenceElement &element : node.elements) {
                    if (element.key.valid()) { (void)check_expr(element.key); }
                    Expr &value = check_expr(element.value, element_type);
                    if (!element_type.valid() && value.type.valid()) {
                        element_type = value.type;
                    } else if (value.type.valid() && !assignable(element_type, value.type)) {
                        type_error(value.range, "sequence elements have incompatible types");
                    }
                    phase = join_phase(phase, value.phase);
                    expression.effects |= value.effects;
                }
                if (!element_type.valid()) { element_type = void_type_; }
                const TypeKind kind   = expected.valid() && type(canonical(expected)).kind == TypeKind::HarnessSequence
                                            ? TypeKind::HarnessSequence
                                            : TypeKind::List;
                expression.type       = make_type(kind, {element_type});
                expression.phase      = phase;
                expression.value_kind = kind == TypeKind::HarnessSequence ? ValueKind::Constant : value_kind_for_phase(phase);
            }

            void check_tuple(Expr &expression, const Tuple &node, TypeId expected) {
                std::vector<TypeId> children;
                const Type         *expected_tuple = nullptr;
                if (expected.valid() && type(canonical(expected)).kind == TypeKind::Tuple) {
                    expected_tuple = &type(canonical(expected));
                }
                expression.phase = Phase::Constant;
                for (std::size_t index = 0; index < node.elements.size(); ++index) {
                    const TypeId item_expected =
                        expected_tuple && index < expected_tuple->children.size() ? expected_tuple->children[index] : TypeId{};
                    Expr &item = check_expr(node.elements[index], item_expected);
                    children.push_back(item.type);
                    expression.phase = join_phase(expression.phase, item.phase);
                    expression.effects |= item.effects;
                }
                expression.type       = make_type(TypeKind::Tuple, std::move(children));
                expression.value_kind = value_kind_for_phase(expression.phase);
            }

            void apply_lambda_context(ExprId id, const std::vector<TypeId> &parameters, TypeId result) {
                if (!id.valid()) { return; }
                Expr &expression = module_.exprs[id.value];
                auto *lambda     = std::get_if<Lambda>(&expression.node);
                if (!lambda) { return; }
                for (std::size_t index = 0; index < lambda->parameters.size() && index < parameters.size(); ++index) {
                    Symbol &symbol = module_.symbols[lambda->parameters[index].value];
                    if (!symbol.type.valid()) {
                        symbol.type = canonical(parameters[index]);
                    } else if (!same(parameters[index], symbol.type)) {
                        type_error(symbol.range, "lambda parameter type conflicts with its call context");
                    }
                    symbol_phase_[lambda->parameters[index].value] =
                        runtime_owner(expression.owner) ? Phase::Runtime : Phase::Wiring;
                }
                if (!lambda->result.valid() && result.valid()) {
                    lambda->result = canonical(result);
                } else if (lambda->result.valid() && result.valid() && !assignable(result, lambda->result)) {
                    type_error(expression.range, "lambda result type conflicts with its call context");
                }
            }

            void check_lambda(Expr &expression, Lambda &node, TypeId expected) {
                std::vector<TypeId> contextual_parameters;
                TypeId              result = node.result;
                if (expected.valid() && type(canonical(expected)).kind == TypeKind::Callable) {
                    const Type &callable = type(canonical(expected));
                    if (!callable.children.empty()) {
                        const std::size_t count = callable.children.size() - 1U;
                        for (std::size_t index = 0; index < count; ++index) {
                            contextual_parameters.push_back(callable.children[index]);
                        }
                        result = callable.children.back();
                    }
                }
                std::vector<TypeId> parameters;
                parameters.reserve(node.parameters.size() + 1U);
                for (std::size_t index = 0; index < node.parameters.size(); ++index) {
                    Symbol &symbol = module_.symbols[node.parameters[index].value];
                    if (!symbol.type.valid() && index < contextual_parameters.size()) {
                        symbol.type = canonical(contextual_parameters[index]);
                    }
                    if (!symbol.type.valid()) { type_error(symbol.range, "lambda parameter needs contextual type inference"); }
                    symbol_phase_[node.parameters[index].value] = runtime_owner(expression.owner) ? Phase::Runtime : Phase::Wiring;
                    parameters.push_back(symbol.type);
                }
                Expr &body = check_expr(node.body, result);
                if (!result.valid()) { result = body.type; }
                require_assignable(result, body, "lambda result");
                node.result = canonical(result);
                parameters.push_back(node.result);
                expression.type       = make_type(TypeKind::Callable, std::move(parameters));
                expression.phase      = Phase::Constant;
                expression.value_kind = ValueKind::Function;
                expression.effects    = body.effects;
            }

            void check_if(Expr &expression, const If &node, TypeId expected) {
                if (!expected.valid()) {
                    if (const FunctionDecl *fn = function(expression.owner)) { expected = fn->signature.result; }
                }
                Expr &condition = check_expr(node.condition, scalar(ScalarType::Bool));
                require_assignable(scalar(ScalarType::Bool), condition, "if condition");
                check_block(node.then_block, expected);
                expression.effects      = condition.effects | module_.block(node.then_block).effects;
                expression.phase        = condition.phase;
                const Block &then_block = module_.block(node.then_block);
                TypeId       then_type  = then_block.tail.valid() ? module_.expr(then_block.tail).type : void_type_;
                if (node.otherwise.valid()) {
                    Expr &otherwise = check_expr(node.otherwise, expected.valid() ? expected : then_type);
                    expression.effects |= otherwise.effects;
                    expression.phase = join_phase(expression.phase, otherwise.phase);
                    if (then_type == void_type_) { then_type = otherwise.type; }
                    if (!assignable(then_type, otherwise.type) && !assignable(otherwise.type, then_type)) {
                        type_error(expression.range, "if branches have incompatible result types");
                    }
                    expression.type = expected.valid() ? canonical(expected) : then_type;
                } else {
                    expression.type = void_type_;
                }
                expression.value_kind = expression.type == void_type_ ? ValueKind::Void : value_kind_for_phase(expression.phase);
            }

            void check_block_expr(Expr &expression, const BlockExpr &node, TypeId expected) {
                check_block(node.block, expected);
                const Block &block    = module_.block(node.block);
                expression.type       = block.tail.valid() ? module_.expr(block.tail).type : void_type_;
                expression.phase      = block.tail.valid() ? module_.expr(block.tail).phase : Phase::Constant;
                expression.effects    = block.effects;
                expression.value_kind = expression.type == void_type_ ? ValueKind::Void : value_kind_for_phase(expression.phase);
            }

            void check_eval(Expr &expression, const Eval &node) {
                Expr       &callee    = check_expr(node.callee);
                const auto *reference = std::get_if<SymbolRef>(&callee.node);
                if (!reference || !reference->symbol.valid() || module_.symbol(reference->symbol).kind != SymbolKind::Function) {
                    type_error(callee.range, "eval requires an exact HGL function");
                    return;
                }
                const FunctionDecl *fn = function(module_.symbol(reference->symbol).owner);
                if (!fn) { return; }
                const std::vector<ExprId> bound = bind_arguments(fn->signature, node.arguments, expression.range);
                for (std::size_t index = 0; index < bound.size(); ++index) {
                    const Parameter &parameter = fn->signature.parameters[index];
                    const TypeId     expected =
                        parameter.is_const ? parameter.type : make_type(TypeKind::HarnessSequence, {parameter.type});
                    Expr &value = check_expr(bound[index], expected);
                    require_assignable(expected, value, "eval input");
                }
                expression.type       = make_type(TypeKind::HarnessSequence, {fn->signature.result});
                expression.phase      = Phase::Constant;
                expression.value_kind = ValueKind::Constant;
                expression.effects    = Effect::TestHarness;
                expression.operation  = Operation{.kind     = OperationKind::HarnessEval,
                                                  .target   = reference->symbol,
                                                  .identity = module_.path + "." + module_.symbol(reference->symbol).name};
            }

            void bind_struct_arguments(TypeId applied, detail::GenericSubstitution &bindings) {
                applied = unwrap_atomic(applied);
                if (!applied.valid()) { return; }
                const Type &value = type(applied);
                if (value.kind != TypeKind::Symbol || !value.symbol.valid()) { return; }
                const Symbol &symbol = module_.symbol(value.symbol);
                if (!symbol.owner.valid()) { return; }
                const auto *structure = std::get_if<StructDecl>(&module_.declaration(symbol.owner).node);
                if (!structure) { return; }
                for (std::size_t index = 0; index < structure->generics.size() && index < value.arguments.size(); ++index) {
                    const GenericParameter &generic  = structure->generics[index];
                    const TypeArgument     &argument = value.arguments[index];
                    if (!generic.is_const && argument.kind == TypeArgumentKind::Type) {
                        (void)bindings.bind_type(generic.symbol, argument.type);
                    } else if (generic.is_const && argument.kind == TypeArgumentKind::Value) {
                        (void)bindings.bind_value(generic.symbol, argument.value);
                    }
                }
            }

            [[nodiscard]] TypeId apply_struct_field_type(TypeId field, TypeId applied) {
                detail::GenericSubstitution bindings{module_, canonical_types_};
                bind_struct_arguments(applied, bindings);
                return bindings.apply(field);
            }

            [[nodiscard]] TypeId infer_struct_application(TypeId applied, const StructDecl &structure,
                                                          const std::vector<Argument> &arguments, syntax::SourceRange range) {
                const TypeId unwrapped = unwrap_atomic(applied);
                if (!unwrapped.valid()) { return applied; }
                const Type nominal = type(unwrapped);
                if (nominal.arguments.size() >= structure.generics.size()) { return applied; }

                detail::GenericSubstitution bindings{module_, canonical_types_};
                bind_struct_arguments(unwrapped, bindings);
                std::size_t positional = 0;
                for (const Argument &argument : arguments) {
                    const StructField *field = nullptr;
                    if (argument.name.empty()) {
                        if (positional < structure.fields.size()) { field = &structure.fields[positional++]; }
                    } else {
                        const auto found = std::find_if(structure.fields.begin(), structure.fields.end(),
                                                        [&](const StructField &item) { return item.name == argument.name; });
                        if (found != structure.fields.end()) { field = &*found; }
                    }
                    if (!field) { continue; }
                    const Expr &source = module_.expr(argument.value);
                    if (source.constant && std::holds_alternative<NullValue>(*source.constant)) { continue; }
                    Expr &value = check_expr(argument.value);
                    (void)bindings.unify(field->type, value.type);
                }

                Type inferred = nominal;
                inferred.arguments.clear();
                bool complete = true;
                for (const GenericParameter &generic : structure.generics) {
                    TypeArgument argument;
                    argument.range = range;
                    if (generic.is_const) {
                        argument.kind    = TypeArgumentKind::Value;
                        const auto found = bindings.value_binding(generic.symbol);
                        if (!found) {
                            complete = false;
                            type_error(range,
                                       "cannot infer generic '" + module_.symbol(generic.symbol).name + "' for struct constructor");
                        } else {
                            argument.value = *found;
                        }
                    } else {
                        argument.kind    = TypeArgumentKind::Type;
                        const auto found = bindings.type_binding(generic.symbol);
                        if (!found) {
                            complete = false;
                            type_error(range,
                                       "cannot infer generic '" + module_.symbol(generic.symbol).name + "' for struct constructor");
                        } else {
                            argument.type = *found;
                        }
                    }
                    inferred.arguments.push_back(argument);
                }
                return complete ? intern(std::move(inferred)) : applied;
            }

            [[nodiscard]] TypeId check_constructor_arguments(Expr &expression, TypeId applied,
                                                             const std::vector<Argument> &arguments, bool delta) {
                TypeId unwrapped = unwrap_atomic(applied);
                if (!unwrapped.valid()) { return applied; }
                const Type &nominal = type(unwrapped);
                if (nominal.kind != TypeKind::Symbol || !nominal.symbol.valid()) {
                    type_error(expression.range, "constructor requires a struct type");
                    return applied;
                }
                const Symbol &symbol = module_.symbol(nominal.symbol);
                const auto   *structure =
                    symbol.owner.valid() ? std::get_if<StructDecl>(&module_.declaration(symbol.owner).node) : nullptr;
                if (!structure) { return applied; }
                applied   = infer_struct_application(applied, *structure, arguments, expression.range);
                unwrapped = unwrap_atomic(applied);
                detail::GenericSubstitution struct_substitution{module_, canonical_types_};
                bind_struct_arguments(unwrapped, struct_substitution);
                (void)constraint_solver_.solve(structure->requirements, struct_substitution, expression.range,
                                               "struct construction");
                std::size_t positional = 0;
                for (const Argument &argument : arguments) {
                    const StructField *field = nullptr;
                    if (argument.name.empty()) {
                        if (positional < structure->fields.size()) { field = &structure->fields[positional++]; }
                    } else {
                        const auto found = std::find_if(structure->fields.begin(), structure->fields.end(),
                                                        [&](const StructField &item) { return item.name == argument.name; });
                        if (found != structure->fields.end()) { field = &*found; }
                    }
                    if (!field) { continue; }
                    const TypeId expected = apply_struct_field_type(field->type, unwrapped);
                    Expr        &value    = check_expr(argument.value, expected);
                    if (value.constant && std::holds_alternative<NullValue>(*value.constant)) {
                        if (!delta && !field->optional) {
                            type_error(value.range, "null is only valid for an optional field or sparse delta");
                        }
                    } else {
                        require_assignable(expected, value, "constructor field");
                    }
                    expression.effects |= value.effects;
                }
                return applied;
            }

            void check_struct_call(Expr &expression, const Call &call, SymbolId target, TypeId expected) {
                TypeId applied = make_type(TypeKind::Symbol, {}, target);
                if (expected.valid()) {
                    const TypeId unwrapped = unwrap_atomic(expected);
                    if (type(unwrapped).kind == TypeKind::Symbol && type(unwrapped).symbol == target) { applied = expected; }
                }
                applied               = check_constructor_arguments(expression, applied, call.arguments, false);
                expression.type       = canonical(applied);
                expression.phase      = runtime_owner(expression.owner) ? Phase::Runtime : Phase::Wiring;
                expression.value_kind = value_kind_for_phase(expression.phase);
                if (expression.phase == Phase::Wiring) { expression.effects |= Effect::WireGraph; }
                expression.operation = Operation{.kind     = OperationKind::Constructor,
                                                 .target   = target,
                                                 .identity = module_.path + "." + module_.symbol(target).name};
            }

            void check_construct(Expr &expression, const Construct &node, TypeId expected) {
                TypeId applied = canonical(node.type);
                if (expected.valid() && assignable(expected, applied)) { applied = canonical(expected); }
                applied               = check_constructor_arguments(expression, applied, node.arguments, node.delta);
                expression.type       = applied;
                expression.phase      = runtime_owner(expression.owner) ? Phase::Runtime : Phase::Wiring;
                expression.value_kind = value_kind_for_phase(expression.phase);
                if (expression.phase == Phase::Wiring) { expression.effects |= Effect::WireGraph; }
                const TypeId nominal = unwrap_atomic(applied);
                expression.operation = Operation{.kind     = OperationKind::Constructor,
                                                 .target   = nominal.valid() ? type(nominal).symbol : SymbolId{},
                                                 .identity = node.delta ? "delta" : "construct"};
            }

            [[nodiscard]] std::vector<TypeId> collection_items(TypeId id) {
                id = unwrap_atomic(id);
                if (!id.valid()) { return {}; }
                const Type &value = type(id);
                if (value.kind == TypeKind::Map && value.children.size() == 2U) { return {value.children[0], value.children[1]}; }
                if (value.kind == TypeKind::List && !value.children.empty()) {
                    return {scalar(ScalarType::I64), value.children[0]};
                }
                if (value.kind == TypeKind::Set && !value.children.empty()) { return {value.children[0]}; }
                return {};
            }

            void check_intrinsic_call(Expr &expression, const Call &call, SymbolId target, TypeId expected) {
                const std::string  &name = module_.symbol(target).external_name;
                std::vector<ExprId> args;
                for (const Argument &argument : call.arguments) { args.push_back(argument.value); }
                if (name == "valid" || name == "modified" || name == "all_valid") {
                    if (args.empty()) { type_error(expression.range, "'" + name + "' takes at least one argument"); }
                    for (ExprId argument : args) { (void)check_expr(argument); }
                    expression.type = scalar(ScalarType::Bool);
                } else if (name == "last_modified") {
                    if (args.size() != 1U) { type_error(expression.range, "last_modified takes one argument"); }
                    for (ExprId argument : args) { (void)check_expr(argument); }
                    expression.type = scalar(ScalarType::DateTime);
                } else if (name == "key_set") {
                    if (args.size() != 1U) { type_error(expression.range, "key_set takes one map argument"); }
                    Expr        &value      = check_expr(args.empty() ? ExprId{} : args.front());
                    const TypeId collection = unwrap_atomic(value.type);
                    if (collection.valid() && type(collection).kind == TypeKind::Map) {
                        expression.type = make_type(TypeKind::Set, {type(collection).children.front()});
                    } else {
                        type_error(value.range, "key_set takes a map");
                    }
                } else if (name == "keys" || name == "values" || name == "items") {
                    Expr               &collection = check_expr(args.empty() ? ExprId{} : args.front());
                    std::vector<TypeId> items      = collection_items(collection.type);
                    if (items.empty()) { type_error(collection.range, "'" + name + "' takes a collection"); }
                    if (name == "keys" && !items.empty()) { items.resize(1U); }
                    if (name == "values" && items.size() == 2U) { items.erase(items.begin()); }
                    if (args.size() > 1U) {
                        Expr &predicate = module_.exprs[args[1].value];
                        if (std::holds_alternative<Lambda>(predicate.node)) {
                            apply_lambda_context(args[1], items, scalar(ScalarType::Bool));
                        }
                        (void)check_expr(args[1]);
                    }
                    expression.type    = make_type(TypeKind::Iterator, items);
                    expression.effects = Effect::IterateCollection;
                } else if (name == "added" || name == "removed") {
                    expression.type = make_type(TypeKind::Callable);
                } else {
                    type_error(expression.range, "unsupported intrinsic '" + name + "'");
                }
                finish_call_semantics(expression, args);
                if (expression.type.valid() && type(expression.type).kind == TypeKind::Iterator) {
                    expression.phase      = Phase::Runtime;
                    expression.value_kind = ValueKind::Iterator;
                    expression.effects |= Effect::IterateCollection;
                }
                expression.operation =
                    Operation{.kind = OperationKind::Intrinsic, .target = target, .identity = name, .deferred = false};
                contextualize(expression, expected);
            }

            void check_capability_call(Expr &expression, const Call &call, const Field &field) {
                Expr               &member    = module_.exprs[call.callee.value];
                const auto         *reference = std::get_if<SymbolRef>(&module_.expr(field.target).node);
                std::vector<ExprId> args;
                for (const Argument &argument : call.arguments) {
                    args.push_back(argument.value);
                    (void)check_expr(argument.value);
                }
                expression.type       = void_type_;
                expression.phase      = Phase::Runtime;
                expression.value_kind = ValueKind::Void;
                expression.effects    = member.effects | Effect::UseCapability;
                for (ExprId argument : args) { expression.effects |= module_.expr(argument).effects; }
                expression.operation = Operation{.kind     = OperationKind::Capability,
                                                 .target   = reference ? reference->symbol : SymbolId{},
                                                 .identity = member.operation.identity};
            }

            void check_stmt(StmtId id, TypeId expected_return, bool is_tail) {
                Stmt &statement = module_.stmts[id.value];
                std::visit(
                    [&](auto &node) {
                        using T = std::decay_t<decltype(node)>;
                        if constexpr (std::is_same_v<T, LocalDecl>) {
                            Expr &init = check_expr(node.init, node.type);
                            if (!node.type.valid()) { node.type = init.type; }
                            require_assignable(node.type, init, "local initializer");
                            Symbol &symbol                   = module_.symbols[node.symbol.value];
                            symbol.type                      = node.type;
                            symbol_phase_[node.symbol.value] = init.phase;
                            statement.effects                = init.effects;
                        } else if constexpr (std::is_same_v<T, StateDecl>) {
                            Expr &init = check_expr(node.init, node.type);
                            if (!node.type.valid()) { node.type = init.type; }
                            require_assignable(node.type, init, "state initializer");
                            module_.symbols[node.symbol.value].type = node.type;
                            symbol_phase_[node.symbol.value]        = Phase::Runtime;
                            statement.effects                       = init.effects | Effect::WriteState;
                        } else if constexpr (std::is_same_v<T, InjectDecl>) {
                            FunctionDecl *fn = function(statement.owner);
                            for (SymbolId symbol_id : node.symbols) {
                                Symbol &symbol = module_.symbols[symbol_id.value];
                                if (symbol.name == "out") {
                                    symbol.type = fn ? fn->signature.result : void_type_;
                                } else {
                                    symbol.type = make_type(TypeKind::Capability, {}, symbol_id);
                                }
                                symbol_phase_[symbol_id.value] = Phase::Runtime;
                            }
                        } else if constexpr (std::is_same_v<T, LifecycleBlock>) {
                            check_block(node.block, expected_return);
                            statement.effects = module_.block(node.block).effects;
                        } else if constexpr (std::is_same_v<T, WhenStmt>) {
                            Expr &condition = check_expr(node.condition, scalar(ScalarType::Bool));
                            require_assignable(scalar(ScalarType::Bool), condition, "when condition");
                            check_block(node.block, expected_return);
                            statement.effects = condition.effects | module_.block(node.block).effects;
                        } else if constexpr (std::is_same_v<T, ForStmt>) {
                            Expr       &iterable = check_expr(node.iterable);
                            const Type *iterator = iterable.type.valid() ? &type(canonical(iterable.type)) : nullptr;
                            if (iterator == nullptr || iterator->kind != TypeKind::Iterator ||
                                iterator->children.size() != node.bindings.size()) {
                                type_error(iterable.range, "for bindings do not match the iterator item shape");
                            } else {
                                for (std::size_t index = 0; index < node.bindings.size(); ++index) {
                                    module_.symbols[node.bindings[index].value].type = iterator->children[index];
                                    symbol_phase_[node.bindings[index].value]        = Phase::Runtime;
                                }
                            }
                            check_block(node.block, expected_return);
                            statement.effects = iterable.effects | module_.block(node.block).effects | Effect::IterateCollection;
                        } else if constexpr (std::is_same_v<T, AssignStmt>) {
                            Expr &place = check_expr(node.place);
                            Expr &value = check_expr(node.value, place.type);
                            require_assignable(place.type, value, "assignment");
                            statement.effects   = place.effects | value.effects;
                            const SymbolId root = place_root(node.place);
                            if (root.valid()) {
                                const Symbol &symbol = module_.symbol(root);
                                if (symbol.kind == SymbolKind::LocalVar) {
                                    statement.effects |= Effect::WriteLocal;
                                } else if (symbol.kind == SymbolKind::State) {
                                    statement.effects |= Effect::WriteState;
                                } else if (symbol.kind == SymbolKind::InjectedCapability && symbol.name == "out") {
                                    statement.effects |= Effect::WriteOutput;
                                } else {
                                    type_error(place.range, "assignment target is immutable");
                                }
                            }
                        } else if constexpr (std::is_same_v<T, ReturnStmt>) {
                            Expr &value = check_expr(node.value, expected_return);
                            require_assignable(expected_return, value, "return value");
                            statement.effects = value.effects;
                        } else if constexpr (std::is_same_v<T, AssertStmt>) {
                            Expr &condition = check_expr(node.condition, scalar(ScalarType::Bool));
                            require_assignable(scalar(ScalarType::Bool), condition, "assert condition");
                            statement.effects = condition.effects;
                            if (!function(statement.owner)) { statement.effects |= Effect::TestHarness; }
                        } else if constexpr (std::is_same_v<T, ExprStmt>) {
                            statement.effects = check_expr(node.expr, is_tail ? expected_return : TypeId{}).effects;
                        }
                    },
                    statement.node);
            }

            [[nodiscard]] SymbolId place_root(ExprId id) const noexcept {
                if (!id.valid()) { return {}; }
                const Expr &expression = module_.expr(id);
                if (const auto *reference = std::get_if<SymbolRef>(&expression.node)) { return reference->symbol; }
                if (const auto *index = std::get_if<Index>(&expression.node)) { return place_root(index->target); }
                if (const auto *field = std::get_if<Field>(&expression.node)) { return place_root(field->target); }
                return {};
            }

            void check_block(BlockId id, TypeId expected_return) {
                if (!id.valid()) { return; }
                Block &block = module_.blocks[id.value];
                if (checked_blocks_.contains(id.value)) { return; }
                checked_blocks_.emplace(id.value, true);
                block.effects = Effect::None;
                for (StmtId statement : block.statements) {
                    const auto *expression_statement = std::get_if<ExprStmt>(&module_.stmt(statement).node);
                    const bool  is_tail              = expression_statement && expression_statement->expr == block.tail;
                    check_stmt(statement, expected_return, is_tail);
                    block.effects |= module_.stmt(statement).effects;
                }
                if (block.tail.valid()) {
                    Expr &tail = check_expr(block.tail, expected_return);
                    block.effects |= tail.effects;
                }
            }

            void validate_completion() {
                for (const Expr &expression : module_.exprs) {
                    if (expression.value_kind == ValueKind::Unknown || expression.phase == Phase::Unknown) {
                        diagnostics_.report(syntax::Category::Type, expression.range,
                                            "expression did not receive complete type and phase information");
                        continue;
                    }
                    if (expression.value_kind != ValueKind::Function && expression.value_kind != ValueKind::Operator &&
                        expression.value_kind != ValueKind::Type && !expression.type.valid()) {
                        diagnostics_.report(syntax::Category::Type, expression.range, "expression has no canonical type");
                    }
                    if (std::holds_alternative<Call>(expression.node) && expression.operation.kind == OperationKind::None) {
                        diagnostics_.report(syntax::Category::Type, expression.range, "call has no semantic target");
                    }
                }
            }

            Module                                    &module_;
            const OperatorResolver                    &resolve_operator_;
            syntax::DiagnosticSink                    &diagnostics_;
            detail::CanonicalTypes                     canonical_types_;
            detail::ConstraintSolver                   constraint_solver_;
            std::vector<std::uint8_t>                  expr_state_{};
            std::unordered_map<std::uint32_t, Phase>   symbol_phase_{};
            std::unordered_map<std::uint32_t, bool>    checked_blocks_{};
            TypeId                                     void_type_{};
            ConstraintId                               active_requirements_{};
            ConstraintId                               inherited_requirements_{};
            std::optional<detail::GenericSubstitution> inherited_substitution_{};
            Expr                                       missing_expression_{};
        };
    }  // namespace

    bool complete_hir(hir::Module &module, const OperatorResolver &resolve_operator, syntax::DiagnosticSink &diagnostics) {
        return TypeChecker{module, resolve_operator, diagnostics}.run();
    }
}  // namespace hgl::ir
