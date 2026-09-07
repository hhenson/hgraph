#include "ir/type_check.h"

#include "ir/canonical_types.h"
#include "ir/constraint_solver.h"
#include "ir/definite_assignment.h"
#include "ir/generic_substitution.h"
#include "syntax/temporal.h"

#include <algorithm>
#include <cmath>
#include <cstdint>
#include <limits>
#include <optional>
#include <string>
#include <string_view>
#include <type_traits>
#include <unordered_map>
#include <unordered_set>
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

        [[nodiscard]] std::optional<std::int64_t> checked_add(std::int64_t lhs, std::int64_t rhs) noexcept {
            constexpr auto min = std::numeric_limits<std::int64_t>::min();
            constexpr auto max = std::numeric_limits<std::int64_t>::max();
            if ((rhs > 0 && lhs > max - rhs) || (rhs < 0 && lhs < min - rhs)) { return std::nullopt; }
            return lhs + rhs;
        }

        [[nodiscard]] std::optional<std::int64_t> checked_sub(std::int64_t lhs, std::int64_t rhs) noexcept {
            constexpr auto min = std::numeric_limits<std::int64_t>::min();
            constexpr auto max = std::numeric_limits<std::int64_t>::max();
            if ((rhs > 0 && lhs < min + rhs) || (rhs < 0 && lhs > max + rhs)) { return std::nullopt; }
            return lhs - rhs;
        }

        [[nodiscard]] std::optional<std::int64_t> checked_mul(std::int64_t lhs, std::int64_t rhs) noexcept {
            constexpr auto min = std::numeric_limits<std::int64_t>::min();
            constexpr auto max = std::numeric_limits<std::int64_t>::max();
            if (lhs == 0 || rhs == 0) { return 0; }
            if ((lhs == -1 && rhs == min) || (rhs == -1 && lhs == min)) { return std::nullopt; }
            if (lhs > 0) {
                if ((rhs > 0 && lhs > max / rhs) || (rhs < 0 && rhs < min / lhs)) { return std::nullopt; }
            } else if ((rhs > 0 && lhs < min / rhs) || (rhs < 0 && lhs < max / rhs)) {
                return std::nullopt;
            }
            return lhs * rhs;
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
                check_instantiations();
                for (DeclarationId declaration : module_.source_order) { check_declaration(declaration); }
                ir::check_definite_assignment(module_, diagnostics_);
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
            [[nodiscard]] bool   signal_marker(TypeId id) const noexcept {
                id = canonical(id);
                return id.valid() && type(id).kind == TypeKind::Signal;
            }
            [[nodiscard]] bool reference(TypeId id) const noexcept {
                id = canonical(id);
                return id.valid() && type(id).kind == TypeKind::Reference;
            }
            [[nodiscard]] bool assignable(TypeId expected, TypeId actual) const noexcept {
                return canonical_types_.assignable(expected, actual);
            }

            void type_error(syntax::SourceRange range, std::string message) {
                diagnostics_.report(syntax::Category::Type, range, std::move(message));
            }

            [[nodiscard]] std::string type_name(TypeId id) const { return canonical_types_.name(id); }

            [[nodiscard]] bool invalid_reference_shape(TypeId id) const {
                id = canonical(id);
                if (!id.valid()) { return false; }
                const Type &value = type(id);
                if (value.kind == TypeKind::Reference && value.children.size() == 1U) {
                    const TypeId child = canonical(value.children.front());
                    if (child.valid() && type(child).kind == TypeKind::Reference) { return true; }
                }
                if (value.kind == TypeKind::Map && value.children.size() == 2U) {
                    const TypeId mapped = canonical(value.children[1]);
                    if (mapped.valid() && type(mapped).kind == TypeKind::Reference) { return true; }
                }
                for (TypeId child : value.children) {
                    if (invalid_reference_shape(child)) { return true; }
                }
                for (const TypeArgument &argument : value.arguments) {
                    if (argument.kind == TypeArgumentKind::Type && invalid_reference_shape(argument.type)) { return true; }
                }
                return false;
            }

            [[nodiscard]] std::string operator_identity(SymbolId id) const {
                if (!id.valid()) { return {}; }
                const Symbol &symbol = module_.symbol(id);
                return symbol.canonical_name.empty() ? symbol.name : symbol.canonical_name;
            }

            [[nodiscard]] const FunctionDecl *function(DeclarationId id) const noexcept {
                if (!id.valid() || id.value >= module_.declarations.size()) { return nullptr; }
                return std::get_if<FunctionDecl>(&module_.declaration(id).node);
            }

            [[nodiscard]] FunctionDecl *function(DeclarationId id) noexcept {
                if (!id.valid() || id.value >= module_.declarations.size()) { return nullptr; }
                return std::get_if<FunctionDecl>(&module_.declarations[id.value].node);
            }

            [[nodiscard]] const NativeFunction *native_function(SymbolId symbol) const noexcept {
                const auto found = std::ranges::find(module_.native_functions, symbol, &NativeFunction::symbol);
                return found == module_.native_functions.end() ? nullptr : &*found;
            }

            [[nodiscard]] std::vector<const NativeFunction *> native_candidates(SymbolId symbol) const {
                std::vector<const NativeFunction *> result;
                for (const NativeFunction &function : module_.native_functions) {
                    if (function.symbol == symbol || function.family == symbol) { result.push_back(&function); }
                }
                return result;
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

            [[nodiscard]] bool bind_instantiation_arguments(const Instantiation &request, const FunctionDecl &implementation,
                                                            detail::GenericSubstitution &substitution) {
                if (implementation.generics.size() != request.arguments.size()) { return false; }
                for (std::size_t index = 0; index < implementation.generics.size(); ++index) {
                    const GenericParameter &generic  = implementation.generics[index];
                    const TypeArgument     &argument = request.arguments[index];
                    if (argument.retained) { continue; }
                    if (generic.is_const) {
                        if (argument.kind != TypeArgumentKind::Value || !argument.value.valid()) { return false; }
                        Expr &value = check_expr(argument.value, generic.type);
                        if (value.phase != Phase::Constant) { return false; }
                        if (!canonical_types_.assignable(generic.type, value.type)) { return false; }
                        if (!substitution.bind_value(generic.symbol, argument.value)) { return false; }
                    } else {
                        if (argument.kind != TypeArgumentKind::Type || !argument.type.valid()) { return false; }
                        const TypeId concrete = canonical(argument.type);
                        if (!concrete.valid() || !substitution.bind_type(generic.symbol, concrete)) { return false; }
                    }
                }
                return true;
            }

            [[nodiscard]] bool contract_accepts_materialization(const OperatorDecl &contract, const FunctionDecl &implementation,
                                                                detail::GenericSubstitution &implementation_bindings,
                                                                syntax::SourceRange          range) {
                detail::GenericSubstitution contract_bindings{module_, canonical_types_};
                if (contract.signature.parameters.size() != implementation.signature.parameters.size()) { return false; }
                for (std::size_t index = 0; index < contract.signature.parameters.size(); ++index) {
                    if (!contract_bindings.unify(contract.signature.parameters[index].type,
                                                 implementation_bindings.apply(implementation.signature.parameters[index].type))) {
                        return false;
                    }
                }
                if (!contract_bindings.unify(contract.signature.result,
                                             implementation_bindings.apply(implementation.signature.result))) {
                    return false;
                }
                return constraint_solver_.solve(contract.requirements, contract_bindings, range, "operator instantiation", false);
            }

            [[nodiscard]] bool same_materialization(const Materialization &lhs, const Materialization &rhs) {
                if (lhs.implementation != rhs.implementation || lhs.substitutions.size() != rhs.substitutions.size()) {
                    return false;
                }
                for (std::size_t index = 0; index < lhs.substitutions.size(); ++index) {
                    const Substitution &a = lhs.substitutions[index];
                    const Substitution &b = rhs.substitutions[index];
                    if (a.parameter != b.parameter || a.retained != b.retained || a.type.valid() != b.type.valid() ||
                        a.value.valid() != b.value.valid()) {
                        return false;
                    }
                    if (a.type.valid() && !same(a.type, b.type)) { return false; }
                    if (a.value.valid() && !canonical_types_.same_value(a.value, b.value)) { return false; }
                }
                return true;
            }

            /// A partially materialized candidate is a pattern: every
            /// concrete binding must agree with the call substitution, while
            /// a retained slot accepts the value inferred by the resolver.
            [[nodiscard]] bool materialization_accepts(const Materialization &candidate, const Materialization &requested) {
                if (candidate.implementation != requested.implementation ||
                    candidate.substitutions.size() != requested.substitutions.size()) {
                    return false;
                }
                for (std::size_t index = 0; index < candidate.substitutions.size(); ++index) {
                    const Substitution &published = candidate.substitutions[index];
                    const Substitution &actual    = requested.substitutions[index];
                    if (published.parameter != actual.parameter) { return false; }
                    if (published.retained) { continue; }
                    if (published.type.valid() != actual.type.valid() || published.value.valid() != actual.value.valid()) {
                        return false;
                    }
                    if (published.type.valid() && !same(published.type, actual.type)) { return false; }
                    if (published.value.valid() && !canonical_types_.same_value(published.value, actual.value)) { return false; }
                }
                return true;
            }

            void check_instantiation(Instantiation &request) {
                if (!request.operator_contract.valid()) { return; }
                const OperatorDecl *contract = operator_decl(request.operator_contract);
                if (contract == nullptr) {
                    type_error(request.range, "instantiate names no local operator contract");
                    return;
                }

                bool matched = false;
                for (Declaration &declaration : module_.declarations) {
                    auto *implementation = std::get_if<FunctionDecl>(&declaration.node);
                    if (implementation == nullptr || implementation->visibility != Visibility::Implementation ||
                        implementation->operator_contract != request.operator_contract || implementation->generics.empty()) {
                        continue;
                    }
                    detail::GenericSubstitution bindings{module_, canonical_types_};
                    if (!bind_instantiation_arguments(request, *implementation, bindings)) { continue; }
                    if (!constraint_solver_.solve(implementation->requirements, bindings, request.range,
                                                  "operator implementation instantiation", false)) {
                        continue;
                    }
                    bool retained_was_bound = false;
                    for (std::size_t index = 0; index < implementation->generics.size(); ++index) {
                        if (!request.arguments[index].retained) { continue; }
                        const GenericParameter &generic = implementation->generics[index];
                        retained_was_bound =
                            generic.is_const ? bindings.has_value(generic.symbol) : bindings.has_type(generic.symbol);
                        if (retained_was_bound) { break; }
                    }
                    if (retained_was_bound) { continue; }
                    if (!contract_accepts_materialization(*contract, *implementation, bindings, request.range)) { continue; }
                    matched = true;

                    Materialization materialization;
                    materialization.implementation = declaration.symbol;
                    materialization.substitutions  = bindings.materialize(implementation->generics);
                    materialization.range          = request.range;
                    for (std::size_t index = 0; index < materialization.substitutions.size(); ++index) {
                        Substitution &substitution = materialization.substitutions[index];
                        substitution.retained      = request.arguments[index].retained;
                        if (substitution.value.valid()) { substitution.constant = module_.expr(substitution.value).constant; }
                    }
                    const bool duplicate = std::ranges::any_of(materializations_, [&](const Materialization &existing) {
                        return same_materialization(existing, materialization);
                    });
                    if (duplicate) {
                        type_error(request.range, "operator implementation is instantiated more than once with the same arguments");
                        continue;
                    }
                    materializations_.push_back(materialization);
                    request.materializations.push_back(std::move(materialization));
                }
                if (!matched) { type_error(request.range, "instantiate matches no local generic operator implementation"); }
            }

            void check_instantiations() {
                for (DeclarationId declaration_id : module_.source_order) {
                    if (!declaration_id.valid()) { continue; }
                    auto *declaration = std::get_if<InstantiateDecl>(&module_.declarations[declaration_id.value].node);
                    if (declaration == nullptr) { continue; }
                    validate_owned_type_applications(declaration_id);
                    for (Instantiation &request : declaration->entries) { check_instantiation(request); }
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

            [[nodiscard]] std::vector<detail::ConstraintPremise> active_constraint_premises() {
                std::vector<detail::ConstraintPremise> premises;
                if (active_requirements_.valid()) { premises.push_back({active_requirements_, nullptr}); }
                if (inherited_requirements_.valid()) {
                    premises.push_back({inherited_requirements_, inherited_substitution_ ? &*inherited_substitution_ : nullptr});
                }
                return premises;
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
                if (target.kind == SymbolKind::ImportedFunction) {
                    const std::vector<const NativeFunction *> candidates = native_candidates(symbol);
                    if (candidates.size() != 1U) { return make_type(TypeKind::Callable); }
                    const NativeFunction *native = candidates.front();
                    std::vector<TypeId>   children;
                    children.reserve(native->parameters.size() + 1U);
                    for (const NativeParameter &parameter : native->parameters) { children.push_back(parameter.type); }
                    children.push_back(native->result);
                    return make_type(TypeKind::Callable, std::move(children));
                }
                if (!target.owner.valid()) { return make_type(TypeKind::Callable); }
                const DeclarationNode &node = module_.declaration(target.owner).node;
                if (const auto *fn = std::get_if<FunctionDecl>(&node)) { return callable_type(fn->signature); }
                if (const auto *op = std::get_if<OperatorDecl>(&node)) { return callable_type(op->signature); }
                return make_type(TypeKind::Callable);
            }

            [[nodiscard]] static std::uint64_t application_key(DeclarationId owner, TypeId application) noexcept {
                return (static_cast<std::uint64_t>(owner.value) << 32U) | application.value;
            }

            void validate_owned_type_applications(DeclarationId owner) {
                const std::size_t type_count = module_.types.size();
                for (std::uint32_t index = 0; index < type_count; ++index) {
                    const Type source = module_.types[index];
                    if (source.owner != owner) { continue; }
                    const TypeId application_id = canonical(TypeId{index});
                    if (!application_id.valid()) { continue; }
                    const Type &application = type(application_id);
                    if (application.kind != TypeKind::Symbol || !application.symbol.valid()) { continue; }
                    const Symbol &symbol = module_.symbol(application.symbol);
                    if (symbol.kind != SymbolKind::Struct || !symbol.owner.valid()) { continue; }
                    const auto *structure = std::get_if<StructDecl>(&module_.declaration(symbol.owner).node);
                    if (structure == nullptr || structure->generics.size() != application.arguments.size()) { continue; }

                    if (!checked_type_applications_.insert(application_key(owner, application_id)).second) { continue; }
                    if (!structure->requirements.valid()) { continue; }
                    detail::GenericSubstitution substitution{module_, canonical_types_};
                    bool                        complete = true;
                    for (std::size_t argument = 0; argument < structure->generics.size(); ++argument) {
                        const GenericParameter &generic = structure->generics[argument];
                        const TypeArgument     &value   = application.arguments[argument];
                        if (generic.is_const && value.kind == TypeArgumentKind::Value) {
                            complete = substitution.bind_value(generic.symbol, value.value) && complete;
                        } else if (!generic.is_const && value.kind == TypeArgumentKind::Type) {
                            complete = substitution.bind_type(generic.symbol, value.type) && complete;
                        } else {
                            complete = false;
                        }
                    }
                    if (!complete) { continue; }
                    const auto premises = active_constraint_premises();
                    (void)constraint_solver_.solve(structure->requirements, substitution, source.range,
                                                   "generic struct '" + symbol.name + "'", true, premises);
                }
            }

            void check_declaration(DeclarationId id) {
                if (!id.valid()) { return; }
                const ConstraintId previous_requirements = active_requirements_;
                const ConstraintId previous_inherited    = inherited_requirements_;
                const NativePhase  previous_native_phase = active_native_phase_;
                active_requirements_                     = {};
                inherited_requirements_                  = {};
                inherited_substitution_.reset();
                Declaration &declaration = module_.declarations[id.value];
                std::visit(
                    [&](auto &node) {
                        using T = std::decay_t<decltype(node)>;
                        if constexpr (std::is_same_v<T, StructDecl>) {
                            active_requirements_ = node.requirements;
                            validate_owned_type_applications(id);
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
                            active_requirements_ = node.requirements;
                            validate_owned_type_applications(id);
                            check_signature_defaults(node.signature);
                        } else if constexpr (std::is_same_v<T, InstantiateDecl>) {
                            // Materializations are checked as one module-level set
                            // before bodies so source order cannot affect availability.
                        } else if constexpr (std::is_same_v<T, FunctionDecl>) {
                            active_native_phase_ =
                                node.kind == FunctionKind::Runtime ? NativePhase::Evaluation : NativePhase::Wiring;
                            active_requirements_ = node.requirements;
                            if (node.visibility == Visibility::Implementation && node.operator_contract.valid()) {
                                if (const OperatorDecl *contract = operator_decl(node.operator_contract)) {
                                    inherited_requirements_ = contract->requirements;
                                    inherited_substitution_.emplace(module_, canonical_types_);
                                    check_implementation_conformance(declaration, node, *contract, *inherited_substitution_);
                                }
                            }
                            validate_owned_type_applications(id);
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
                            if (node.kind == FunctionKind::Runtime && node.block_body.valid()) {
                                check_runtime_layout(node.block_body);
                            }
                        } else if constexpr (std::is_same_v<T, TestDecl>) {
                            active_native_phase_ = NativePhase::Wiring;
                            validate_owned_type_applications(id);
                            check_block(node.block, void_type_);
                        }
                    },
                    declaration.node);
                active_requirements_    = previous_requirements;
                inherited_requirements_ = previous_inherited;
                active_native_phase_    = previous_native_phase;
                inherited_substitution_.reset();
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
                        // A repeated name is a resolver error ("declared twice
                        // in the block"), so the list is duplicate-free here.
                        fn.capabilities.insert(fn.capabilities.end(), inject->symbols.begin(), inject->symbols.end());
                    }
                }
            }

            /// The placement rules of syntax-and-semantics.md "Runtime function
            /// bodies": `state`, `inject`, `start`, `stop`, and `when` are
            /// function-level forms; `state` and `inject` precede the executable
            /// blocks; a function has at most one `start` and one `stop`; and
            /// `when` is never nested, because a nested handler cannot
            /// contribute to the node's activation policy. Semantic checks, so
            /// each diagnostic names the misplaced construct.
            void check_runtime_layout(BlockId body) {
                bool        executable_seen = false;
                std::size_t starts          = 0;
                std::size_t stops           = 0;
                for (StmtId id : module_.block(body).statements) {
                    const Stmt &statement = module_.stmt(id);
                    std::visit(
                        [&](const auto &node) {
                            using T = std::decay_t<decltype(node)>;
                            if constexpr (std::is_same_v<T, StateDecl> || std::is_same_v<T, InjectDecl>) {
                                if (executable_seen) {
                                    diagnostics_.report(syntax::Category::FunctionKind, statement.range,
                                                        std::string{"'"} + (std::is_same_v<T, StateDecl> ? "state" : "inject") +
                                                            "' must be declared before runtime handlers");
                                }
                                if constexpr (std::is_same_v<T, StateDecl>) { reject_nested_function_level_in(node.init); }
                            } else if constexpr (std::is_same_v<T, LifecycleBlock>) {
                                executable_seen    = true;
                                std::size_t &count = node.is_stop ? stops : starts;
                                if (++count > 1U) {
                                    diagnostics_.report(syntax::Category::FunctionKind, statement.range,
                                                        std::string{"a runtime function has at most one '"} +
                                                            (node.is_stop ? "stop" : "start") + "' block");
                                }
                                reject_nested_function_level(node.block);
                            } else if constexpr (std::is_same_v<T, WhenStmt>) {
                                executable_seen = true;
                                reject_nested_function_level_in(node.condition);
                                reject_nested_function_level(node.block);
                            } else {
                                reject_nested_function_level_in_statement(node);
                            }
                        },
                        statement.node);
                }
            }

            template <typename Node> [[nodiscard]] static ExprId expression_of(const Node &node) {
                if constexpr (std::is_same_v<Node, ReturnStmt>) {
                    return node.value;
                } else {
                    return node.expr;
                }
            }

            /// The ordinary statements' expressions and nested blocks.
            template <typename Node> void reject_nested_function_level_in_statement(const Node &node) {
                if constexpr (std::is_same_v<Node, ForStmt>) {
                    reject_nested_function_level_in(node.iterable);
                    reject_nested_function_level(node.block);
                } else if constexpr (std::is_same_v<Node, LocalDecl>) {
                    reject_nested_function_level_in(node.init);
                } else if constexpr (std::is_same_v<Node, AssignStmt>) {
                    reject_nested_function_level_in(node.place);
                    reject_nested_function_level_in(node.value);
                } else if constexpr (std::is_same_v<Node, AssertStmt>) {
                    reject_nested_function_level_in(node.condition);
                } else if constexpr (std::is_same_v<Node, ReturnStmt> || std::is_same_v<Node, ExprStmt>) {
                    reject_nested_function_level_in(expression_of(node));
                }
            }

            /// Every statement of a block nested inside a runtime body: the
            /// function-level forms are diagnosed, and everything is walked,
            /// including the expressions, so a block hidden inside a call
            /// argument, operand, element, or lambda cannot carry one past the
            /// checker.
            void reject_nested_function_level(BlockId id) {
                if (!id.valid()) { return; }
                const Block &block = module_.block(id);
                for (StmtId stmt_id : block.statements) {
                    const Stmt &statement = module_.stmt(stmt_id);
                    std::visit(
                        [&](const auto &node) {
                            using T = std::decay_t<decltype(node)>;
                            if constexpr (std::is_same_v<T, WhenStmt>) {
                                diagnostics_.report(syntax::Category::FunctionKind, statement.range,
                                                    "'when' cannot be nested in another block; it declares "
                                                    "node-level activation");
                                reject_nested_function_level_in(node.condition);
                                reject_nested_function_level(node.block);
                            } else if constexpr (std::is_same_v<T, LifecycleBlock>) {
                                diagnostics_.report(syntax::Category::FunctionKind, statement.range,
                                                    std::string{"'"} + (node.is_stop ? "stop" : "start") +
                                                        "' must be a function-level block, not nested in another block");
                                reject_nested_function_level(node.block);
                            } else if constexpr (std::is_same_v<T, StateDecl> || std::is_same_v<T, InjectDecl>) {
                                diagnostics_.report(syntax::Category::FunctionKind, statement.range,
                                                    std::string{"'"} + (std::is_same_v<T, StateDecl> ? "state" : "inject") +
                                                        "' must be declared at function level, not inside a block");
                                if constexpr (std::is_same_v<T, StateDecl>) { reject_nested_function_level_in(node.init); }
                            } else {
                                reject_nested_function_level_in_statement(node);
                            }
                        },
                        statement.node);
                }
                reject_nested_function_level_in(block.tail);
            }

            void reject_nested_function_level_in(ExprId id) {
                if (!id.valid()) { return; }
                const Expr &expression = module_.expr(id);
                std::visit(
                    [&](const auto &node) {
                        using T = std::decay_t<decltype(node)>;
                        if constexpr (std::is_same_v<T, Unary>) {
                            reject_nested_function_level_in(node.operand);
                        } else if constexpr (std::is_same_v<T, Binary>) {
                            reject_nested_function_level_in(node.lhs);
                            reject_nested_function_level_in(node.rhs);
                        } else if constexpr (std::is_same_v<T, Call> || std::is_same_v<T, Eval>) {
                            reject_nested_function_level_in(node.callee);
                            for (const Argument &argument : node.arguments) { reject_nested_function_level_in(argument.value); }
                        } else if constexpr (std::is_same_v<T, Construct>) {
                            for (const Argument &argument : node.arguments) { reject_nested_function_level_in(argument.value); }
                        } else if constexpr (std::is_same_v<T, Index>) {
                            reject_nested_function_level_in(node.target);
                            reject_nested_function_level_in(node.index);
                        } else if constexpr (std::is_same_v<T, Field>) {
                            reject_nested_function_level_in(node.target);
                        } else if constexpr (std::is_same_v<T, Sequence>) {
                            for (const SequenceElement &element : node.elements) {
                                reject_nested_function_level_in(element.key);
                                reject_nested_function_level_in(element.value);
                            }
                        } else if constexpr (std::is_same_v<T, Tuple>) {
                            for (ExprId element : node.elements) { reject_nested_function_level_in(element); }
                        } else if constexpr (std::is_same_v<T, Lambda>) {
                            reject_nested_function_level_in(node.body);
                        } else if constexpr (std::is_same_v<T, If>) {
                            reject_nested_function_level_in(node.condition);
                            reject_nested_function_level(node.then_block);
                            reject_nested_function_level_in(node.otherwise);
                        } else if constexpr (std::is_same_v<T, BlockExpr>) {
                            reject_nested_function_level(node.block);
                        }
                    },
                    expression.node);
            }

            void require_assignable(TypeId expected, const Expr &actual, std::string_view what) {
                if (!expected.valid() || !actual.type.valid()) { return; }
                if (!assignable(expected, actual.type)) {
                    type_error(actual.range,
                               std::string{what} + " has type " + type_name(actual.type) + ", expected " + type_name(expected));
                } else if (actual.phase == Phase::Wiring && requires_nested_conversion(expected, actual.type)) {
                    type_error(actual.range,
                               std::string{what} + " requires an implicit conversion inside a time-series collection");
                }
            }

            [[nodiscard]] bool requires_nested_conversion(TypeId expected, TypeId actual) const noexcept {
                expected = canonical(expected);
                actual   = canonical(actual);
                while (expected.valid() && type(expected).kind == TypeKind::Atomic && !type(expected).children.empty()) {
                    expected = type(expected).children.front();
                }
                while (actual.valid() && type(actual).kind == TypeKind::Atomic && !type(actual).children.empty()) {
                    actual = type(actual).children.front();
                }
                if (canonical_types_.same_ignoring_references(expected, actual)) { return false; }
                if (same(expected, actual) || !expected.valid() || !actual.valid()) { return false; }
                const Type &to            = type(expected);
                const Type &from          = type(actual);
                const bool  sequence_pair = (to.kind == TypeKind::List || to.kind == TypeKind::HarnessSequence) &&
                                            (from.kind == TypeKind::List || from.kind == TypeKind::HarnessSequence);
                const bool  structural    = to.kind == from.kind || sequence_pair;
                if (!structural ||
                    (to.kind != TypeKind::Tuple && to.kind != TypeKind::List && to.kind != TypeKind::Set &&
                     to.kind != TypeKind::Map && to.kind != TypeKind::HarnessSequence) ||
                    to.children.size() != from.children.size()) {
                    return false;
                }
                for (std::size_t index = 0; index < to.children.size(); ++index) {
                    if (!same(to.children[index], from.children[index])) { return true; }
                }
                return false;
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
                    check_type_shape(value);
                }
            }

            /// The size rules of syntax-and-semantics.md "Time-series
            /// collections": a fixed list size is a positive constant; a
            /// rolling window's sizes are both `i64` or both `duration`; tick
            /// sizes are positive, a duration minimum may be `0s`, and no
            /// minimum exceeds its maximum. A symbolic size (an in-scope
            /// `const` generic) carries its declared kind and has no value to
            /// range-check here.
            void check_type_shape(const Type &value) {
                if (value.kind == TypeKind::List) {
                    if (!value.size.valid()) { return; }
                    const Expr &size = module_.expr(value.size);
                    // A symbolic size (`list<T, n>` with `const n`) has no folded
                    // value but does have a declared type, which must be i64.
                    if (size.type.valid()) {
                        const Type &size_type = type(canonical(size.type));
                        if (size_type.kind != TypeKind::Scalar || size_type.scalar != ScalarType::I64) {
                            type_error(size.range, "a list size must be an i64 constant or 'unbounded'");
                            return;
                        }
                    }
                    if (!size.constant) { return; }
                    const auto *count = std::get_if<std::int64_t>(&*size.constant);
                    if (count == nullptr || *count <= 0) {
                        type_error(size.range, "list size must be a positive constant or 'unbounded'");
                    }
                    return;
                }
                if (value.kind != TypeKind::Rolling || !value.size.valid()) { return; }
                const Expr &maximum = module_.expr(value.size);
                const Expr *minimum = value.min_size.valid() ? &module_.expr(value.min_size) : nullptr;
                enum class SizeKind : std::uint8_t { Unknown, Tick, Duration, Other };
                // Source types are not yet canonicalized when this runs, so
                // classify by the scalar itself rather than by interned identity.
                const auto size_kind = [&](const Expr &expression) {
                    if (!expression.type.valid()) { return SizeKind::Unknown; }
                    const Type &size_type = type(canonical(expression.type));
                    if (size_type.kind != TypeKind::Scalar) { return SizeKind::Other; }
                    if (size_type.scalar == ScalarType::I64) { return SizeKind::Tick; }
                    if (size_type.scalar == ScalarType::Duration) { return SizeKind::Duration; }
                    return SizeKind::Other;
                };
                const SizeKind max_kind = size_kind(maximum);
                const SizeKind min_kind = minimum != nullptr ? size_kind(*minimum) : max_kind;
                if (max_kind == SizeKind::Other || min_kind == SizeKind::Other) {
                    type_error(max_kind == SizeKind::Other ? maximum.range : minimum->range,
                               "a rolling size must be an i64 or duration constant");
                    return;
                }
                if (max_kind != SizeKind::Unknown && min_kind != SizeKind::Unknown && max_kind != min_kind) {
                    type_error(minimum->range, "rolling sizes must both be i64 or both be duration");
                    return;
                }
                const auto tick = [](const Expr &expression) -> std::optional<std::int64_t> {
                    if (!expression.constant) { return std::nullopt; }
                    if (const auto *count = std::get_if<std::int64_t>(&*expression.constant)) { return *count; }
                    return std::nullopt;
                };
                const auto micros = [](const Expr &expression) -> std::optional<std::int64_t> {
                    if (!expression.constant) { return std::nullopt; }
                    const auto *temporal = std::get_if<syntax::TemporalValue>(&*expression.constant);
                    if (temporal == nullptr || temporal->kind != syntax::TemporalKind::Duration) { return std::nullopt; }
                    return temporal->micros;
                };
                const syntax::SourceRange minimum_range = minimum != nullptr ? minimum->range : maximum.range;
                if (max_kind == SizeKind::Tick) {
                    const std::optional<std::int64_t> max_value = tick(maximum);
                    const std::optional<std::int64_t> min_value = minimum != nullptr ? tick(*minimum) : max_value;
                    if (max_value && *max_value <= 0) {
                        type_error(maximum.range, "a rolling tick size must be positive");
                    } else if (min_value && (*min_value <= 0 || (max_value && *min_value > *max_value))) {
                        type_error(minimum_range, "a rolling minimum size must be positive and no larger than the maximum");
                    }
                } else if (max_kind == SizeKind::Duration) {
                    const std::optional<std::int64_t> max_value = micros(maximum);
                    const std::optional<std::int64_t> min_value = minimum != nullptr ? micros(*minimum) : max_value;
                    if (max_value && *max_value <= 0) {
                        type_error(maximum.range, "a rolling duration must be positive");
                    } else if (min_value && (*min_value < 0 || (max_value && *min_value > *max_value))) {
                        type_error(minimum_range, "a rolling minimum duration must be non-negative and no longer than the maximum");
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
                        // Output access during lifecycle hooks is an open
                        // question (language-model.md); the checker rejects it
                        // rather than leaving it to a backend.
                        if (symbol.name == "out" &&
                            (active_native_phase_ == NativePhase::Start || active_native_phase_ == NativePhase::Stop)) {
                            diagnostics_.report(syntax::Category::Phase, expression.range,
                                                std::string{"'out' is not available during "} +
                                                    (active_native_phase_ == NativePhase::Stop ? "stop" : "start"));
                        }
                        expression.type       = canonical(symbol.type);
                        expression.phase      = Phase::Runtime;
                        expression.value_kind = symbol.name == "out" ? ValueKind::RuntimeValue : ValueKind::Function;
                        expression.effects    = Effect::UseCapability;
                        break;
                    case SymbolKind::Function:
                    case SymbolKind::ImportedFunction:
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
                Expr &operand = check_expr(node.operand);
                if (runtime_owner(expression.owner) && reference(operand.type)) {
                    type_error(operand.range, "node evaluation cannot read through ref<T>");
                }
                expression.effects   = operand.effects;
                expression.phase     = operand.phase;
                expression.operation = Operation{.kind     = OperationKind::NominalOperator,
                                                 .identity = std::string{unary_identity(node.op)},
                                                 .deferred = operand.phase != Phase::Constant};
                if (signal_marker(operand.type)) {
                    type_error(operand.range, "'signal' has no payload and cannot be used with operators");
                    expression.type = node.op == UnaryOp::Not ? scalar(ScalarType::Bool) : operand.type;
                    if (expression.phase == Phase::Wiring) { expression.effects |= Effect::WireGraph; }
                    expression.value_kind = value_kind_for_phase(expression.phase);
                    return;
                }
                const auto required = active_required_operation(unary_identity(node.op), {operand.type});
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
                        const std::int64_t value = std::get<std::int64_t>(*operand.constant);
                        if (value == std::numeric_limits<std::int64_t>::min()) {
                            type_error(expression.range, "overflow in an integer constant expression");
                        } else {
                            expression.constant = Constant{-value};
                        }
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
                const TypeId duration = scalar(ScalarType::Duration);
                const TypeId datetime = scalar(ScalarType::DateTime);
                if (op == BinaryOp::Add && same(lhs, duration) && same(rhs, duration)) { return duration; }
                if (op == BinaryOp::Add && same(lhs, datetime) && same(rhs, duration)) { return datetime; }
                if (op == BinaryOp::Sub && same(lhs, duration) && same(rhs, duration)) { return duration; }
                if (op == BinaryOp::Sub && same(lhs, datetime) && same(rhs, duration)) { return datetime; }
                if (op == BinaryOp::Sub && same(lhs, datetime) && same(rhs, datetime)) { return duration; }
                if (op == BinaryOp::Mul && same(lhs, duration) && same(rhs, scalar(ScalarType::I64))) { return duration; }
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
                const auto *left_integer  = std::get_if<std::int64_t>(&a);
                const auto *right_integer = std::get_if<std::int64_t>(&b);
                if (left_integer && right_integer && op != BinaryOp::Div) {
                    std::optional<std::int64_t> result;
                    switch (op) {
                        case BinaryOp::Add: result = checked_add(*left_integer, *right_integer); break;
                        case BinaryOp::Sub: result = checked_sub(*left_integer, *right_integer); break;
                        case BinaryOp::Mul: result = checked_mul(*left_integer, *right_integer); break;
                        case BinaryOp::Rem:
                            if (*right_integer == 0) {
                                type_error(expression.range, "remainder by zero in a constant expression");
                                return;
                            }
                            result = *left_integer == std::numeric_limits<std::int64_t>::min() && *right_integer == -1
                                         ? 0
                                         : *left_integer % *right_integer;
                            break;
                        case BinaryOp::Less: expression.constant = Constant{*left_integer < *right_integer}; return;
                        case BinaryOp::LessEqual: expression.constant = Constant{*left_integer <= *right_integer}; return;
                        case BinaryOp::Greater: expression.constant = Constant{*left_integer > *right_integer}; return;
                        case BinaryOp::GreaterEqual: expression.constant = Constant{*left_integer >= *right_integer}; return;
                        case BinaryOp::Equal: expression.constant = Constant{*left_integer == *right_integer}; return;
                        case BinaryOp::NotEqual: expression.constant = Constant{*left_integer != *right_integer}; return;
                        case BinaryOp::Div:
                        case BinaryOp::And:
                        case BinaryOp::Or: std::unreachable();
                    }
                    if (!result) {
                        type_error(expression.range, "overflow in an integer constant expression");
                    } else {
                        expression.constant = Constant{*result};
                    }
                    return;
                }
                if (op == BinaryOp::Equal || op == BinaryOp::NotEqual) {
                    bool equal = false;
                    if (const std::optional<double> left = as_double(a), right = as_double(b); left && right) {
                        equal = *left == *right;
                    } else if (const auto *left = std::get_if<syntax::TemporalValue>(&a)) {
                        const auto *right = std::get_if<syntax::TemporalValue>(&b);
                        if (right && left->kind == right->kind &&
                            (left->kind == syntax::TemporalKind::DateTime || left->kind == syntax::TemporalKind::ZonedDateTime)) {
                            // Datetimes denote instants. Their source offset and
                            // zone metadata do not participate in equality.
                            equal = left->micros == right->micros;
                        } else {
                            equal = a == b;
                        }
                    } else {
                        equal = a == b;
                    }
                    expression.constant = Constant{op == BinaryOp::Equal ? equal : !equal};
                    return;
                }
                const auto *left_temporal  = std::get_if<syntax::TemporalValue>(&a);
                const auto *right_temporal = std::get_if<syntax::TemporalValue>(&b);
                if (left_temporal && right_temporal) {
                    syntax::TemporalValue               result = *left_temporal;
                    std::optional<std::int64_t>         micros;
                    std::optional<syntax::TemporalKind> result_kind;
                    bool                                supported = true;
                    if (op == BinaryOp::Add && left_temporal->kind == syntax::TemporalKind::Duration &&
                        right_temporal->kind == syntax::TemporalKind::Duration) {
                        micros = checked_add(left_temporal->micros, right_temporal->micros);
                    } else if (op == BinaryOp::Add && left_temporal->kind == syntax::TemporalKind::DateTime &&
                               right_temporal->kind == syntax::TemporalKind::Duration) {
                        micros = checked_add(left_temporal->micros, right_temporal->micros);
                    } else if (op == BinaryOp::Sub && left_temporal->kind == syntax::TemporalKind::Duration &&
                               right_temporal->kind == syntax::TemporalKind::Duration) {
                        micros = checked_sub(left_temporal->micros, right_temporal->micros);
                    } else if (op == BinaryOp::Sub && left_temporal->kind == syntax::TemporalKind::DateTime &&
                               right_temporal->kind == syntax::TemporalKind::Duration) {
                        micros = checked_sub(left_temporal->micros, right_temporal->micros);
                    } else if (op == BinaryOp::Sub && left_temporal->kind == syntax::TemporalKind::DateTime &&
                               right_temporal->kind == syntax::TemporalKind::DateTime) {
                        micros      = checked_sub(left_temporal->micros, right_temporal->micros);
                        result_kind = syntax::TemporalKind::Duration;
                    } else {
                        supported = false;
                    }
                    if (supported) {
                        if (!micros) {
                            type_error(expression.range, "overflow in a temporal constant expression");
                            return;
                        }
                        result.micros = *micros;
                        if (result_kind) { result.kind = *result_kind; }
                        expression.constant = Constant{result};
                        return;
                    }
                }
                if (left_temporal && left_temporal->kind == syntax::TemporalKind::Duration && op == BinaryOp::Mul) {
                    if (const auto *factor = std::get_if<std::int64_t>(&b)) {
                        syntax::TemporalValue result = *left_temporal;
                        const auto            micros = checked_mul(left_temporal->micros, *factor);
                        if (!micros) {
                            type_error(expression.range, "overflow in a temporal constant expression");
                            return;
                        }
                        result.micros       = *micros;
                        expression.constant = Constant{result};
                        return;
                    }
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
                if (runtime_owner(expression.owner) && (reference(lhs.type) || reference(rhs.type))) {
                    type_error(expression.range, "node evaluation cannot read through ref<T>");
                }
                if (!lhs.type.valid() && rhs.type.valid()) { contextualize(lhs, rhs.type); }
                expression.phase     = join_phase(lhs.phase, rhs.phase);
                expression.effects   = lhs.effects | rhs.effects;
                expression.operation = Operation{.kind     = OperationKind::NominalOperator,
                                                 .identity = std::string{binary_identity(node.op)},
                                                 .deferred = expression.phase != Phase::Constant};
                if (signal_marker(lhs.type) || signal_marker(rhs.type)) {
                    type_error(expression.range, "'signal' has no payload and cannot be used with operators");
                    expression.type = node.op == BinaryOp::Less || node.op == BinaryOp::LessEqual || node.op == BinaryOp::Greater ||
                                              node.op == BinaryOp::GreaterEqual || node.op == BinaryOp::Equal ||
                                              node.op == BinaryOp::NotEqual || node.op == BinaryOp::And || node.op == BinaryOp::Or
                                          ? scalar(ScalarType::Bool)
                                          : lhs.type;
                    if (expression.phase == Phase::Wiring) { expression.effects |= Effect::WireGraph; }
                    expression.value_kind = value_kind_for_phase(expression.phase);
                    return;
                }
                const auto required = active_required_operation(binary_identity(node.op), {lhs.type, rhs.type});
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

            [[nodiscard]] std::vector<ExprId> bind_native_arguments(const NativeFunction        &function,
                                                                    const std::vector<Argument> &arguments,
                                                                    syntax::SourceRange          range) {
                std::vector<ExprId> bound(function.parameters.size());
                std::size_t         next = 0U;
                for (const Argument &argument : arguments) {
                    if (argument.name.empty()) {
                        while (next < bound.size() && bound[next].valid()) { ++next; }
                        if (next >= bound.size()) {
                            type_error(argument.range, "too many positional arguments");
                        } else {
                            bound[next++] = argument.value;
                        }
                        continue;
                    }
                    const auto found = std::ranges::find(function.parameters, argument.name, &NativeParameter::name);
                    if (found == function.parameters.end()) {
                        diagnostics_.report(syntax::Category::Name, argument.range, "unknown parameter '" + argument.name + "'");
                        continue;
                    }
                    const std::size_t index = static_cast<std::size_t>(found - function.parameters.begin());
                    if (bound[index].valid()) {
                        type_error(argument.range, "parameter '" + argument.name + "' is supplied twice");
                    } else {
                        bound[index] = argument.value;
                    }
                }
                for (std::size_t index = 0; index < bound.size(); ++index) {
                    if (!bound[index].valid()) { type_error(range, "missing argument '" + function.parameters[index].name + "'"); }
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
                const auto premises = active_constraint_premises();
                (void)constraint_solver_.solve(fn.requirements, bindings, expression.range, "function call", true, premises);
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

            [[nodiscard]] bool try_bind_native_arguments(const NativeFunction &function, const std::vector<Argument> &arguments,
                                                         std::vector<ExprId> &bound) const {
                bound.assign(function.parameters.size(), {});
                std::size_t next = 0U;
                for (const Argument &argument : arguments) {
                    if (argument.name.empty()) {
                        while (next < bound.size() && bound[next].valid()) { ++next; }
                        if (next >= bound.size()) { return false; }
                        bound[next++] = argument.value;
                        continue;
                    }
                    const auto found = std::ranges::find(function.parameters, argument.name, &NativeParameter::name);
                    if (found == function.parameters.end()) { return false; }
                    const std::size_t index = static_cast<std::size_t>(found - function.parameters.begin());
                    if (bound[index].valid()) { return false; }
                    bound[index] = argument.value;
                }
                return std::ranges::all_of(bound, &ExprId::valid);
            }

            [[nodiscard]] bool native_candidate_matches(const NativeFunction &function, const std::vector<Argument> &arguments,
                                                        TypeId expected, std::vector<Substitution> *substitutions = nullptr) {
                if (std::ranges::find(function.phases, active_native_phase_) == function.phases.end()) { return false; }
                std::vector<ExprId> bound;
                if (!try_bind_native_arguments(function, arguments, bound)) { return false; }
                detail::GenericSubstitution bindings{module_, canonical_types_};
                for (std::size_t index = 0; index < bound.size(); ++index) {
                    const Expr            &argument  = module_.expr(bound[index]);
                    const NativeParameter &parameter = function.parameters[index];
                    if (parameter.is_const && argument.phase != Phase::Constant) { return false; }
                    if (active_native_phase_ == NativePhase::Wiring && argument.phase != Phase::Constant) { return false; }
                    if (!bindings.unify(parameter.type, argument.type)) { return false; }
                    if (!same(bindings.apply(parameter.type), argument.type)) { return false; }
                }
                if (expected.valid() && !bindings.unify(function.result, expected)) { return false; }
                for (const GenericParameter &generic : function.generics) {
                    if (generic.is_const) {
                        const std::optional<ExprId> value = bindings.value_binding(generic.symbol);
                        if (!value || !canonical_types_.assignable(generic.type, module_.expr(*value).type)) { return false; }
                    } else if (!bindings.has_type(generic.symbol)) {
                        return false;
                    }
                }
                if (substitutions != nullptr) {
                    *substitutions = bindings.materialize(function.generics);
                    for (Substitution &substitution : *substitutions) {
                        if (substitution.value.valid()) { substitution.constant = module_.expr(substitution.value).constant; }
                    }
                }
                return true;
            }

            void check_native_call(Expr &expression, const Call &call, SymbolId target, const NativeFunction &function,
                                   TypeId expected) {
                const std::vector<ExprId> bound = bind_native_arguments(function, call.arguments, expression.range);
                const bool phase_allowed        = std::ranges::find(function.phases, active_native_phase_) != function.phases.end();
                if (!phase_allowed) {
                    static constexpr std::string_view names[]{"wiring", "start", "evaluation", "stop"};
                    diagnostics_.report(syntax::Category::Phase, expression.range,
                                        "native function '" + function.identity + "' is not available during " +
                                            std::string{names[static_cast<std::size_t>(active_native_phase_)]});
                }

                expression.effects = Effect::None;
                detail::GenericSubstitution bindings{module_, canonical_types_};
                for (std::size_t index = 0; index < bound.size(); ++index) {
                    if (!bound[index].valid()) { continue; }
                    Expr &argument = check_expr(bound[index]);
                    expression.effects |= argument.effects;
                    if (!bindings.unify(function.parameters[index].type, argument.type) ||
                        !same(bindings.apply(function.parameters[index].type), argument.type)) {
                        type_error(argument.range, "native argument has type " + type_name(argument.type) + ", expected exactly " +
                                                       type_name(function.parameters[index].type));
                    }
                    if (function.parameters[index].is_const && argument.phase != Phase::Constant) {
                        diagnostics_.report(syntax::Category::Phase, argument.range,
                                            "a const native parameter requires a compile-time value");
                    }
                    if (active_native_phase_ == NativePhase::Wiring && argument.phase != Phase::Constant) {
                        diagnostics_.report(syntax::Category::Phase, argument.range,
                                            "a wiring-phase native value call requires compile-time arguments");
                    }
                }

                require_complete_bindings(function.generics, bindings, expression.range, "native function call");

                expression.type       = bindings.apply(function.result);
                expression.phase      = active_native_phase_ == NativePhase::Wiring ? Phase::Constant : Phase::Runtime;
                expression.value_kind = expression.type == void_type_ ? ValueKind::Void : value_kind_for_phase(expression.phase);
                std::vector<Substitution> substitutions = bindings.materialize(function.generics);
                for (Substitution &substitution : substitutions) {
                    if (substitution.value.valid()) { substitution.constant = module_.expr(substitution.value).constant; }
                }
                expression.operation = Operation{.kind          = OperationKind::ExactFunction,
                                                 .target        = target,
                                                 .identity      = function.identity,
                                                 .substitutions = std::move(substitutions)};
                contextualize(expression, expected);
            }

            [[nodiscard]] const OperatorDecl *operator_decl(SymbolId symbol) const noexcept {
                if (!symbol.valid()) { return nullptr; }
                const Symbol &target = module_.symbol(symbol);
                if (!target.owner.valid()) { return nullptr; }
                return std::get_if<OperatorDecl>(&module_.declaration(target.owner).node);
            }

            [[nodiscard]] bool local_candidate_matches(const FunctionDecl &candidate, const std::vector<ExprId> &arguments,
                                                       TypeId expected, std::vector<Substitution> *substitutions = nullptr) {
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
                const auto premises = active_constraint_premises();
                if (!constraint_solver_.solve(candidate.requirements, bindings, {}, "implementation", false, premises)) {
                    return false;
                }
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
                if (substitutions != nullptr) {
                    *substitutions = bindings.materialize(candidate.generics);
                    for (Substitution &substitution : *substitutions) {
                        if (substitution.value.valid()) { substitution.constant = module_.expr(substitution.value).constant; }
                    }
                }
                return true;
            }

            [[nodiscard]] SymbolId sole_local_candidate(SymbolId op, const std::vector<ExprId> &arguments, TypeId expected) {
                std::vector<SymbolId> candidates;
                for (const Declaration &declaration : module_.declarations) {
                    const auto *candidate = std::get_if<FunctionDecl>(&declaration.node);
                    if (!candidate || candidate->visibility != Visibility::Implementation || candidate->operator_contract != op) {
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
                if (candidate == nullptr) { return {}; }
                std::vector<Substitution> substitutions;
                if (!local_candidate_matches(*candidate, arguments, expected, &substitutions)) { return {}; }
                if (!candidate->generics.empty()) {
                    const Materialization requested{.implementation = candidates.front(),
                                                    .substitutions  = std::move(substitutions)};
                    if (!std::ranges::any_of(materializations_, [&](const Materialization &materialization) {
                            return materialization_accepts(materialization, requested);
                        })) {
                        return {};
                    }
                }
                return candidates.front();
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
                const auto premises = active_constraint_premises();
                (void)constraint_solver_.solve(op.requirements, contract_bindings, expression.range, "operator call", true,
                                               premises);
                require_complete_bindings(op.generics, contract_bindings, expression.range, "operator call");
                expression.type          = contract_bindings.apply(op.signature.result);
                const SymbolId candidate = sole_local_candidate(target, bound, expected);
                finish_call_semantics(expression, bound);
                expression.operation = Operation{.kind          = OperationKind::NominalOperator,
                                                 .target        = target,
                                                 .candidate     = candidate,
                                                 .identity      = operator_identity(target),
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
                                                 .identity        = operator_identity(target),
                                                 .candidate_label = std::move(selection.candidate_label),
                                                 .provider_key    = std::move(selection.provider_key),
                                                 .substitutions   = std::move(selection.substitutions),
                                                 .deferred        = selection.deferred};
            }

            void check_call(Expr &expression, const Call &call, TypeId expected) {
                Expr &callee    = check_expr(call.callee);
                auto *reference = std::get_if<SymbolRef>(&callee.node);
                if (reference && reference->symbol.valid()) {
                    const Symbol &symbol = module_.symbol(reference->symbol);
                    if (symbol.kind == SymbolKind::Function) {
                        const auto *fn = function(symbol.owner);
                        if (fn) { check_exact_call(expression, call, reference->symbol, *fn, expected); }
                        return;
                    }
                    if (symbol.kind == SymbolKind::ImportedFunction) {
                        for (const Argument &argument : call.arguments) { (void)check_expr(argument.value); }
                        const std::vector<const NativeFunction *> candidates = native_candidates(reference->symbol);
                        std::vector<const NativeFunction *>       matches;
                        for (const NativeFunction *candidate : candidates) {
                            if (native_candidate_matches(*candidate, call.arguments, expected)) { matches.push_back(candidate); }
                        }
                        if (matches.empty()) {
                            std::string arguments;
                            for (const Argument &argument : call.arguments) {
                                if (!arguments.empty()) { arguments += ", "; }
                                arguments += type_name(module_.expr(argument.value).type);
                            }
                            type_error(expression.range, "no native overload of '" + operator_identity(reference->symbol) +
                                                             "' accepts (" + arguments + ")");
                            if (!candidates.empty()) { matches.push_back(candidates.front()); }
                        } else if (matches.size() > 1U) {
                            type_error(expression.range, "native call to '" + operator_identity(reference->symbol) +
                                                             "' is ambiguous between " + std::to_string(matches.size()) +
                                                             " overloads");
                        }
                        if (!matches.empty()) {
                            const NativeFunction &native = *matches.front();
                            reference->symbol            = native.symbol;
                            callee.type                  = callable_type(native.symbol);
                            check_native_call(expression, call, native.symbol, native, expected);
                        }
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
                Expr &target = check_expr(node.target);
                Expr &index  = check_expr(node.index);
                if (runtime_owner(expression.owner) && reference(target.type)) {
                    type_error(target.range, "node evaluation cannot index through ref<T>");
                    return;
                }
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
                if (runtime_owner(expression.owner) && reference(target.type)) {
                    type_error(target.range, "node evaluation cannot access fields through ref<T>");
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
                bool   use_expected_list = false;
                if (expected.valid()) {
                    const Type &shape = type(canonical(expected));
                    if ((shape.kind == TypeKind::List || shape.kind == TypeKind::HarnessSequence) && !shape.children.empty()) {
                        element_expected = shape.children.front();
                    }
                    if (shape.kind == TypeKind::List && shape.size.valid() && !shape.unbounded) {
                        const Expr &size = module_.expr(shape.size);
                        if (size.constant) {
                            if (const auto *count = std::get_if<std::int64_t>(&*size.constant)) {
                                use_expected_list = *count >= 0 && static_cast<std::size_t>(*count) == node.elements.size();
                                if (!use_expected_list) {
                                    type_error(expression.range, "list literal has " + std::to_string(node.elements.size()) +
                                                                     " elements, expected " + std::to_string(*count));
                                }
                            }
                        }
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
                expression.type       = use_expected_list ? canonical(expected) : make_type(kind, {element_type});
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
                Expr &condition = check_expr(node.condition, scalar(ScalarType::Bool));
                if (runtime_owner(expression.owner) && reference(condition.type)) {
                    type_error(condition.range, "node evaluation cannot test a value through ref<T>");
                }
                require_assignable(scalar(ScalarType::Bool), condition, "if condition");
                TypeId expected_return = expected;
                if (const FunctionDecl *fn = function(expression.owner)) { expected_return = fn->signature.result; }
                check_block(node.then_block, expected_return, expected);
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
                } else if (condition.phase == Phase::Wiring && then_type != void_type_) {
                    // A consumed temporal conditional without `else` has the
                    // true branch's type. Its false branch is materialized as
                    // a typed never-ticking source by the execution backends.
                    // Discarded conditionals arrive with an expected void type
                    // and retain the outputless switch path.
                    expression.type = expected.valid() ? canonical(expected) : then_type;
                } else {
                    expression.type = void_type_;
                }
                expression.value_kind = expression.type == void_type_ ? ValueKind::Void : value_kind_for_phase(expression.phase);
            }

            void check_block_expr(Expr &expression, const BlockExpr &node, TypeId expected) {
                TypeId expected_return = expected;
                if (const FunctionDecl *fn = function(expression.owner)) { expected_return = fn->signature.result; }
                check_block(node.block, expected_return, expected);
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
                    const std::optional<TypeId> expected = constraint_solver_.field_type({}, unwrapped, field->name);
                    if (!expected) {
                        type_error(argument.range, "cannot resolve effective type for struct field '" + field->name + "'");
                        continue;
                    }
                    Expr &value = check_expr(argument.value);
                    (void)bindings.unify(*expected, value.type);
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
                if (!checked_type_applications_.contains(application_key(expression.owner, unwrapped))) {
                    const auto premises = active_constraint_premises();
                    (void)constraint_solver_.solve(structure->requirements, struct_substitution, expression.range,
                                                   "struct construction", true, premises);
                }
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
                    const std::optional<TypeId> expected = constraint_solver_.field_type({}, unwrapped, field->name);
                    if (!expected) {
                        type_error(argument.range, "cannot resolve effective type for struct field '" + field->name + "'");
                        continue;
                    }
                    Expr &value = check_expr(argument.value, *expected);
                    if (value.constant && std::holds_alternative<NullValue>(*value.constant)) {
                        if (!delta && !field->optional) {
                            type_error(value.range, "null is only valid for an optional field or sparse delta");
                        }
                    } else {
                        require_assignable(*expected, value, "constructor field");
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
                applied          = check_constructor_arguments(expression, applied, call.arguments, false);
                expression.type  = canonical(applied);
                expression.phase = Phase::Constant;
                for (const Argument &argument : call.arguments) {
                    expression.phase = join_phase(expression.phase, module_.expr(argument.value).phase);
                }
                if (runtime_owner(expression.owner)) { expression.phase = Phase::Runtime; }
                expression.value_kind = value_kind_for_phase(expression.phase);
                if (expression.phase == Phase::Wiring) { expression.effects |= Effect::WireGraph; }
                expression.operation = Operation{.kind     = OperationKind::Constructor,
                                                 .target   = target,
                                                 .identity = module_.path + "." + module_.symbol(target).name};
            }

            void check_construct(Expr &expression, const Construct &node, TypeId expected) {
                TypeId applied = canonical(node.type);
                if (expected.valid() && assignable(expected, applied)) { applied = canonical(expected); }
                applied          = check_constructor_arguments(expression, applied, node.arguments, node.delta);
                expression.type  = applied;
                expression.phase = Phase::Constant;
                for (const Argument &argument : node.arguments) {
                    expression.phase = join_phase(expression.phase, module_.expr(argument.value).phase);
                }
                if (runtime_owner(expression.owner)) { expression.phase = Phase::Runtime; }
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
                    expression.phase      = runtime_owner(expression.owner) ? Phase::Runtime : Phase::Wiring;
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

            void check_stmt(StmtId id, TypeId expected_return, TypeId expected_tail, bool is_tail) {
                Stmt &statement = module_.stmts[id.value];
                std::visit(
                    [&](auto &node) {
                        using T = std::decay_t<decltype(node)>;
                        if constexpr (std::is_same_v<T, LocalDecl>) {
                            Symbol &symbol = module_.symbols[node.symbol.value];
                            if (node.init.valid()) {
                                Expr &init = check_expr(node.init, node.type);
                                if (!node.type.valid()) { node.type = init.type; }
                                require_assignable(node.type, init, "local initializer");
                                symbol.type                      = node.type;
                                symbol_phase_[node.symbol.value] = init.phase;
                                statement.effects                = init.effects;
                            } else {
                                if (!node.type.valid()) {
                                    type_error(statement.range, "an uninitialized 'var' requires an explicit type");
                                }
                                symbol.type                      = canonical(node.type);
                                symbol_phase_[node.symbol.value] = runtime_owner(statement.owner) ? Phase::Runtime : Phase::Wiring;
                                statement.effects                = Effect::None;
                            }
                        } else if constexpr (std::is_same_v<T, StateDecl>) {
                            const NativePhase previous_phase = active_native_phase_;
                            active_native_phase_             = NativePhase::Start;
                            Expr &init                       = check_expr(node.init, node.type);
                            active_native_phase_             = previous_phase;
                            if (!node.type.valid()) { node.type = init.type; }
                            require_assignable(node.type, init, "state initializer");
                            module_.symbols[node.symbol.value].type = node.type;
                            symbol_phase_[node.symbol.value]        = Phase::Runtime;
                            statement.effects                       = init.effects | Effect::WriteState;
                        } else if constexpr (std::is_same_v<T, InjectDecl>) {
                            FunctionDecl *fn = function(statement.owner);
                            for (SymbolId symbol_id : node.symbols) {
                                Symbol &symbol = module_.symbols[symbol_id.value];
                                // `inject` names compiler-approved capabilities only
                                // (syntax-and-semantics.md, "Runtime state,
                                // injectables, and lifecycle"). `out` and `logger`
                                // lower today; `clock` and `scheduler` are agreed
                                // and fail closed until their selectors exist.
                                if (symbol.name == "out") {
                                    const TypeId result = fn != nullptr ? fn->signature.result : TypeId{};
                                    if (!result.valid() || same(result, void_type_)) {
                                        diagnostics_.report(syntax::Category::Injectable, symbol.range,
                                                            "'out' requires a function output");
                                    }
                                    symbol.type = fn ? fn->signature.result : void_type_;
                                } else {
                                    if (symbol.name == "clock" || symbol.name == "scheduler") {
                                        diagnostics_.report(syntax::Category::Injectable, symbol.range,
                                                            "the '" + symbol.name +
                                                                "' injectable is agreed but not implemented yet; "
                                                                "'out' and 'logger' are available");
                                    } else if (symbol.name != "logger") {
                                        diagnostics_.report(syntax::Category::Injectable, symbol.range,
                                                            "'" + symbol.name +
                                                                "' is not an approved runtime capability; the "
                                                                "injectables are out, logger, clock, and scheduler");
                                    }
                                    symbol.type = make_type(TypeKind::Capability, {}, symbol_id);
                                }
                                symbol_phase_[symbol_id.value] = Phase::Runtime;
                            }
                        } else if constexpr (std::is_same_v<T, LifecycleBlock>) {
                            const NativePhase previous_phase = active_native_phase_;
                            active_native_phase_             = node.is_stop ? NativePhase::Stop : NativePhase::Start;
                            check_block(node.block, expected_return);
                            active_native_phase_ = previous_phase;
                            statement.effects    = module_.block(node.block).effects;
                        } else if constexpr (std::is_same_v<T, WhenStmt>) {
                            Expr &condition = check_expr(node.condition, scalar(ScalarType::Bool));
                            require_assignable(scalar(ScalarType::Bool), condition, "when condition");
                            check_block(node.block, expected_return);
                            statement.effects = condition.effects | module_.block(node.block).effects;
                        } else if constexpr (std::is_same_v<T, ForStmt>) {
                            Expr       &iterable   = check_expr(node.iterable);
                            const Type *iterator   = iterable.type.valid() ? &type(canonical(iterable.type)) : nullptr;
                            const Phase loop_phase = runtime_owner(statement.owner) ? Phase::Runtime : Phase::Wiring;
                            if (iterator == nullptr || iterator->kind != TypeKind::Iterator ||
                                iterator->children.size() != node.bindings.size()) {
                                type_error(iterable.range, "for bindings do not match the iterator item shape");
                            } else {
                                for (std::size_t index = 0; index < node.bindings.size(); ++index) {
                                    module_.symbols[node.bindings[index].value].type = iterator->children[index];
                                    symbol_phase_[node.bindings[index].value]        = loop_phase;
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
                            if (active_native_phase_ == NativePhase::Start || active_native_phase_ == NativePhase::Stop) {
                                diagnostics_.report(syntax::Category::Phase, statement.range,
                                                    std::string{"'return' is not available during "} +
                                                        (active_native_phase_ == NativePhase::Stop ? "stop" : "start"));
                            }
                            Expr &value = check_expr(node.value, expected_return);
                            require_assignable(expected_return, value, "return value");
                            statement.effects = value.effects;
                        } else if constexpr (std::is_same_v<T, AssertStmt>) {
                            Expr &condition = check_expr(node.condition, scalar(ScalarType::Bool));
                            require_assignable(scalar(ScalarType::Bool), condition, "assert condition");
                            statement.effects = condition.effects;
                            if (!function(statement.owner)) { statement.effects |= Effect::TestHarness; }
                        } else if constexpr (std::is_same_v<T, ExprStmt>) {
                            TypeId expected = is_tail ? expected_tail : TypeId{};
                            if (!is_tail && std::holds_alternative<If>(module_.expr(node.expr).node)) { expected = void_type_; }
                            statement.effects = check_expr(node.expr, expected).effects;
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

            void check_block(BlockId id, TypeId expected_return) { check_block(id, expected_return, expected_return); }

            void check_block(BlockId id, TypeId expected_return, TypeId expected_tail) {
                if (!id.valid()) { return; }
                Block &block = module_.blocks[id.value];
                if (checked_blocks_.contains(id.value)) { return; }
                checked_blocks_.emplace(id.value, true);
                block.effects = Effect::None;
                for (StmtId statement : block.statements) {
                    const auto *expression_statement = std::get_if<ExprStmt>(&module_.stmt(statement).node);
                    const bool  is_tail              = expression_statement && expression_statement->expr == block.tail;
                    check_stmt(statement, expected_return, expected_tail, is_tail);
                    block.effects |= module_.stmt(statement).effects;
                }
                if (block.tail.valid()) {
                    Expr &tail = check_expr(block.tail, expected_tail);
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
                    if (expression.type.valid() && invalid_reference_shape(expression.type)) {
                        diagnostics_.report(syntax::Category::Type, expression.range,
                                            "generic substitution produces an unsupported reference shape");
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
            std::unordered_set<std::uint64_t>          checked_type_applications_{};
            TypeId                                     void_type_{};
            NativePhase                                active_native_phase_{NativePhase::Wiring};
            ConstraintId                               active_requirements_{};
            ConstraintId                               inherited_requirements_{};
            std::optional<detail::GenericSubstitution> inherited_substitution_{};
            std::vector<Materialization>               materializations_{};
            Expr                                       missing_expression_{};
        };
    }  // namespace

    bool complete_hir(hir::Module &module, const OperatorResolver &resolve_operator, syntax::DiagnosticSink &diagnostics) {
        return TypeChecker{module, resolve_operator, diagnostics}.run();
    }
}  // namespace hgl::ir
