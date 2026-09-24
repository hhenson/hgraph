#include "ir/lower.h"

#include <algorithm>
#include <optional>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <utility>

namespace hgl::ir
{
    namespace
    {
        namespace ast = syntax::ast;

        template <typename To, typename From> [[nodiscard]] To id(From value) noexcept {
            return value == ast::no_node ? To{} : To{value};
        }

        [[nodiscard]] std::string join_path(const std::vector<ast::Name> &path) {
            std::string result;
            for (const ast::Name &part : path) {
                if (!result.empty()) { result += '.'; }
                result += part.text;
            }
            return result;
        }

        [[nodiscard]] constexpr hir::ScalarType lower_scalar_type(ast::ScalarType type) noexcept {
            using ast::ScalarType;
            switch (type) {
                case ScalarType::Bool: return hir::ScalarType::Bool;
                case ScalarType::I64: return hir::ScalarType::I64;
                case ScalarType::F64: return hir::ScalarType::F64;
                case ScalarType::Str: return hir::ScalarType::Str;
                case ScalarType::Date: return hir::ScalarType::Date;
                case ScalarType::Time: return hir::ScalarType::Time;
                case ScalarType::DateTime: return hir::ScalarType::DateTime;
                case ScalarType::Duration: return hir::ScalarType::Duration;
                case ScalarType::CivilDateTime: return hir::ScalarType::CivilDateTime;
                case ScalarType::ZonedDateTime: return hir::ScalarType::ZonedDateTime;
                case ScalarType::ZonedTime: return hir::ScalarType::ZonedTime;
                case ScalarType::TimeZone: return hir::ScalarType::TimeZone;
            }
            std::unreachable();
        }

        [[nodiscard]] constexpr hir::ScalarType lower_scalar_type(semantics::ImportedScalarType type) noexcept {
            using semantics::ImportedScalarType;
            switch (type) {
                case ImportedScalarType::Bool: return hir::ScalarType::Bool;
                case ImportedScalarType::I64: return hir::ScalarType::I64;
                case ImportedScalarType::F64: return hir::ScalarType::F64;
                case ImportedScalarType::Str: return hir::ScalarType::Str;
                case ImportedScalarType::Date: return hir::ScalarType::Date;
                case ImportedScalarType::Time: return hir::ScalarType::Time;
                case ImportedScalarType::DateTime: return hir::ScalarType::DateTime;
                case ImportedScalarType::Duration: return hir::ScalarType::Duration;
                case ImportedScalarType::CivilDateTime: return hir::ScalarType::CivilDateTime;
                case ImportedScalarType::ZonedDateTime: return hir::ScalarType::ZonedDateTime;
                case ImportedScalarType::ZonedTime: return hir::ScalarType::ZonedTime;
                case ImportedScalarType::TimeZone: return hir::ScalarType::TimeZone;
            }
            std::unreachable();
        }

        [[nodiscard]] constexpr hir::NativePhase lower_native_phase(semantics::NativeCallPhase phase) noexcept {
            using semantics::NativeCallPhase;
            switch (phase) {
                case NativeCallPhase::Wiring: return hir::NativePhase::Wiring;
                case NativeCallPhase::Start: return hir::NativePhase::Start;
                case NativeCallPhase::Evaluation: return hir::NativePhase::Evaluation;
                case NativeCallPhase::Stop: return hir::NativePhase::Stop;
            }
            std::unreachable();
        }

        [[nodiscard]] constexpr hir::NativeParameterAccess lower_native_access(semantics::NativeParameterAccess access) noexcept {
            switch (access) {
                case semantics::NativeParameterAccess::Value: return hir::NativeParameterAccess::Value;
                case semantics::NativeParameterAccess::InputView: return hir::NativeParameterAccess::InputView;
            }
            std::unreachable();
        }

        [[nodiscard]] constexpr hir::TypeKind lower_imported_type_kind(semantics::ImportedTypeKind kind) noexcept {
            switch (kind) {
                case semantics::ImportedTypeKind::Scalar: return hir::TypeKind::Scalar;
                case semantics::ImportedTypeKind::Symbol: return hir::TypeKind::Symbol;
                case semantics::ImportedTypeKind::List: return hir::TypeKind::List;
                case semantics::ImportedTypeKind::Set: return hir::TypeKind::Set;
                case semantics::ImportedTypeKind::Map: return hir::TypeKind::Map;
                case semantics::ImportedTypeKind::Rolling: return hir::TypeKind::Rolling;
                case semantics::ImportedTypeKind::Signal: return hir::TypeKind::Signal;
                case semantics::ImportedTypeKind::Schema: return hir::TypeKind::Schema;
                case semantics::ImportedTypeKind::Atomic: return hir::TypeKind::Atomic;
            }
            std::unreachable();
        }

        [[nodiscard]] constexpr hir::TypeKind lower_type_kind(ast::TypeKind kind) noexcept {
            using ast::TypeKind;
            switch (kind) {
                case TypeKind::Scalar: return hir::TypeKind::Scalar;
                case TypeKind::Named: return hir::TypeKind::Symbol;
                case TypeKind::Tuple: return hir::TypeKind::Tuple;
                case TypeKind::List: return hir::TypeKind::List;
                case TypeKind::Set: return hir::TypeKind::Set;
                case TypeKind::Map: return hir::TypeKind::Map;
                case TypeKind::Rolling: return hir::TypeKind::Rolling;
                case TypeKind::Atomic: return hir::TypeKind::Atomic;
                case TypeKind::Reference: return hir::TypeKind::Reference;
                case TypeKind::Signal: return hir::TypeKind::Signal;
                case TypeKind::Schema: return hir::TypeKind::Schema;
            }
            std::unreachable();
        }

        [[nodiscard]] constexpr hir::UnaryOp lower_unary_op(ast::UnaryOp op) noexcept {
            switch (op) {
                case ast::UnaryOp::Negate: return hir::UnaryOp::Negate;
                case ast::UnaryOp::Not: return hir::UnaryOp::Not;
            }
            std::unreachable();
        }

        [[nodiscard]] constexpr hir::BinaryOp lower_binary_op(ast::BinaryOp op) noexcept {
            using ast::BinaryOp;
            switch (op) {
                case BinaryOp::Mul: return hir::BinaryOp::Mul;
                case BinaryOp::Div: return hir::BinaryOp::Div;
                case BinaryOp::FloorDiv: return hir::BinaryOp::FloorDiv;
                case BinaryOp::Rem: return hir::BinaryOp::Rem;
                case BinaryOp::Add: return hir::BinaryOp::Add;
                case BinaryOp::Sub: return hir::BinaryOp::Sub;
                case BinaryOp::Less: return hir::BinaryOp::Less;
                case BinaryOp::LessEqual: return hir::BinaryOp::LessEqual;
                case BinaryOp::Greater: return hir::BinaryOp::Greater;
                case BinaryOp::GreaterEqual: return hir::BinaryOp::GreaterEqual;
                case BinaryOp::Equal: return hir::BinaryOp::Equal;
                case BinaryOp::NotEqual: return hir::BinaryOp::NotEqual;
                case BinaryOp::And: return hir::BinaryOp::And;
                case BinaryOp::Or: return hir::BinaryOp::Or;
            }
            std::unreachable();
        }

        [[nodiscard]] constexpr hir::AssignOp lower_assign_op(ast::AssignOp op) noexcept {
            using ast::AssignOp;
            switch (op) {
                case AssignOp::Assign: return hir::AssignOp::Assign;
                case AssignOp::Add: return hir::AssignOp::Add;
                case AssignOp::Sub: return hir::AssignOp::Sub;
                case AssignOp::Mul: return hir::AssignOp::Mul;
                case AssignOp::Div: return hir::AssignOp::Div;
            }
            std::unreachable();
        }

        [[nodiscard]] constexpr hir::ConstraintRelationOp lower_constraint_relation_op(ast::ConstraintRelationOp op) noexcept {
            using ast::ConstraintRelationOp;
            switch (op) {
                case ConstraintRelationOp::Equal: return hir::ConstraintRelationOp::Equal;
                case ConstraintRelationOp::In: return hir::ConstraintRelationOp::In;
                case ConstraintRelationOp::Is: return hir::ConstraintRelationOp::Is;
            }
            std::unreachable();
        }

        /// A descriptor spells its relation and logic operators as text, so an
        /// imported requirement rebuilds from the spelling rather than an enum
        /// (ADR 0013). These are the spellings module_descriptor.cpp writes;
        /// the catalog converts only these, so the fallbacks are unreachable
        /// by construction rather than silent defaults.
        [[nodiscard]] inline hir::ConstraintRelationOp imported_relation_op(std::string_view spelling) noexcept {
            if (spelling == "in") { return hir::ConstraintRelationOp::In; }
            if (spelling == "is") { return hir::ConstraintRelationOp::Is; }
            return hir::ConstraintRelationOp::Equal;
        }

        [[nodiscard]] inline hir::ConstraintLogicOp imported_logic_op(std::string_view spelling) noexcept {
            return spelling == "or" ? hir::ConstraintLogicOp::Or : hir::ConstraintLogicOp::And;
        }

        [[nodiscard]] constexpr hir::ConstraintLogicOp lower_constraint_logic_op(ast::ConstraintLogicOp op) noexcept {
            switch (op) {
                case ast::ConstraintLogicOp::And: return hir::ConstraintLogicOp::And;
                case ast::ConstraintLogicOp::Or: return hir::ConstraintLogicOp::Or;
            }
            std::unreachable();
        }

        [[nodiscard]] constexpr hir::Visibility lower_visibility(ast::FunctionVisibility visibility) noexcept {
            switch (visibility) {
                case ast::FunctionVisibility::Internal: return hir::Visibility::Internal;
                case ast::FunctionVisibility::Export: return hir::Visibility::Export;
                case ast::FunctionVisibility::Impl: return hir::Visibility::Implementation;
            }
            std::unreachable();
        }

        [[nodiscard]] constexpr hir::FunctionKind lower_function_kind(semantics::FunctionKind kind) noexcept {
            switch (kind) {
                case semantics::FunctionKind::Composition: return hir::FunctionKind::Composition;
                case semantics::FunctionKind::Runtime: return hir::FunctionKind::Runtime;
            }
            std::unreachable();
        }

        class Lowerer
        {
          public:
            Lowerer(const ast::Module &module, const semantics::ResolvedModule &resolved, syntax::DiagnosticSink &diagnostics)
                : module_{module}, resolved_{resolved}, diagnostics_{diagnostics} {
                declaration_symbols_.resize(module_.decls.size());
                native_family_symbols_.resize(resolved_.native_families.size());
                generic_symbols_.resize(module_.decls.size());
                parameter_symbols_.resize(module_.decls.size());
                statement_symbols_.resize(module_.stmts.size());
                lambda_symbols_.resize(module_.exprs.size());
                constraint_symbols_.resize(module_.constraints.size());
                type_owners_.resize(module_.types.size(), ast::no_node);
                expr_owners_.resize(module_.exprs.size(), ast::no_node);
                stmt_owners_.resize(module_.stmts.size(), ast::no_node);
                block_owners_.resize(module_.blocks.size(), ast::no_node);
            }

            hir::Module run() {
                result_.path = resolved_.module_path;
                mark_owners();
                declare_symbols();

                result_.types.resize(module_.types.size());
                result_.exprs.resize(module_.exprs.size());
                result_.stmts.resize(module_.stmts.size());
                result_.blocks.resize(module_.blocks.size());
                result_.constraints.resize(module_.constraints.size());
                result_.declarations.resize(module_.decls.size());

                assign_symbol_types();
                for (ast::TypeId n = 0; n < module_.types.size(); ++n) { lower_type(n); }
                for (ast::ConstraintId n = 0; n < module_.constraints.size(); ++n) { lower_constraint(n); }
                for (ast::ExprId n = 0; n < module_.exprs.size(); ++n) { lower_expr(n); }
                for (ast::StmtId n = 0; n < module_.stmts.size(); ++n) { lower_stmt(n); }
                for (ast::BlockId n = 0; n < module_.blocks.size(); ++n) { lower_block(n); }
                for (ast::DeclId n = 0; n < module_.decls.size(); ++n) { lower_declaration(n); }
                for (ast::DeclId n : module_.declarations) { result_.source_order.push_back(id<hir::DeclarationId>(n)); }
                return std::move(result_);
            }

          private:
            [[nodiscard]] hir::SymbolId add_symbol(hir::SymbolKind kind, std::string_view name, syntax::SourceRange range,
                                                   ast::DeclId owner, std::uint32_t index = 0, std::string external_name = {},
                                                   std::string canonical_name = {}) {
                const hir::SymbolId symbol{static_cast<std::uint32_t>(result_.symbols.size())};
                result_.symbols.push_back(hir::Symbol{kind,
                                                      std::string{name},
                                                      std::move(external_name),
                                                      std::move(canonical_name),
                                                      id<hir::DeclarationId>(owner),
                                                      range,
                                                      {},
                                                      index});
                return symbol;
            }

            void mark_owners() {
                for (ast::DeclId decl = 0; decl < module_.decls.size(); ++decl) { mark_declaration(decl); }
            }

            void mark_type(ast::TypeId type, ast::DeclId owner) {
                if (type == ast::no_node) { return; }
                type_owners_[type]    = owner;
                const ast::Type &node = module_.type(type);
                for (ast::TypeId child : node.children) { mark_type(child, owner); }
                for (const ast::GenericArgument &argument : node.arguments) {
                    mark_type(argument.type, owner);
                    mark_expr(argument.value, owner);
                }
                mark_expr(node.size, owner);
                mark_expr(node.min_size, owner);
            }

            void mark_expr(ast::ExprId expression, ast::DeclId owner) {
                if (expression == ast::no_node) { return; }
                expr_owners_[expression] = owner;
                std::visit(
                    [&](const auto &node) {
                        using T = std::decay_t<decltype(node)>;
                        if constexpr (std::is_same_v<T, ast::Unary>) {
                            mark_expr(node.operand, owner);
                        } else if constexpr (std::is_same_v<T, ast::Binary>) {
                            mark_expr(node.lhs, owner);
                            mark_expr(node.rhs, owner);
                        } else if constexpr (std::is_same_v<T, ast::Call> || std::is_same_v<T, ast::Eval>) {
                            mark_expr(node.callee, owner);
                            for (const ast::Argument &argument : node.arguments) { mark_expr(argument.value, owner); }
                        } else if constexpr (std::is_same_v<T, ast::Index>) {
                            mark_expr(node.target, owner);
                            mark_expr(node.index, owner);
                        } else if constexpr (std::is_same_v<T, ast::Field>) {
                            mark_expr(node.target, owner);
                        } else if constexpr (std::is_same_v<T, ast::SequenceLiteral>) {
                            for (const ast::SequenceElement &element : node.elements) {
                                mark_expr(element.key, owner);
                                mark_expr(element.value, owner);
                            }
                        } else if constexpr (std::is_same_v<T, ast::TupleLiteral>) {
                            for (ast::ExprId element : node.elements) { mark_expr(element, owner); }
                        } else if constexpr (std::is_same_v<T, ast::AnonymousFn>) {
                            for (const ast::AnonymousParameter &parameter : node.parameters) { mark_type(parameter.type, owner); }
                            mark_type(node.result, owner);
                            mark_expr(node.body, owner);
                        } else if constexpr (std::is_same_v<T, ast::If>) {
                            mark_expr(node.condition, owner);
                            mark_block(node.then_block, owner);
                            mark_expr(node.otherwise, owner);
                        } else if constexpr (std::is_same_v<T, ast::BlockExpr>) {
                            mark_block(node.block, owner);
                        } else if constexpr (std::is_same_v<T, ast::Construct>) {
                            mark_type(node.type, owner);
                            for (const ast::Argument &argument : node.arguments) { mark_expr(argument.value, owner); }
                        }
                    },
                    module_.expr(expression).node);
            }

            void mark_stmt(ast::StmtId statement, ast::DeclId owner) {
                stmt_owners_[statement] = owner;
                std::visit(
                    [&](const auto &node) {
                        using T = std::decay_t<decltype(node)>;
                        if constexpr (std::is_same_v<T, ast::LocalDecl> || std::is_same_v<T, ast::StateDecl>) {
                            mark_type(node.type, owner);
                            mark_expr(node.init, owner);
                        } else if constexpr (std::is_same_v<T, ast::LifecycleBlock>) {
                            mark_block(node.block, owner);
                        } else if constexpr (std::is_same_v<T, ast::WhenStmt>) {
                            if (node.condition != ast::no_node) { mark_expr(node.condition, owner); }
                            mark_block(node.block, owner);
                        } else if constexpr (std::is_same_v<T, ast::ForStmt>) {
                            mark_expr(node.iterable, owner);
                            mark_block(node.block, owner);
                        } else if constexpr (std::is_same_v<T, ast::AssignStmt>) {
                            mark_expr(node.place, owner);
                            mark_expr(node.value, owner);
                        } else if constexpr (std::is_same_v<T, ast::ReturnStmt>) {
                            mark_expr(node.value, owner);
                        } else if constexpr (std::is_same_v<T, ast::AssertStmt>) {
                            mark_expr(node.condition, owner);
                        } else if constexpr (std::is_same_v<T, ast::ExprStmt>) {
                            mark_expr(node.expr, owner);
                        }
                    },
                    module_.stmt(statement).node);
            }

            void mark_block(ast::BlockId block, ast::DeclId owner) {
                if (block == ast::no_node) { return; }
                block_owners_[block] = owner;
                for (ast::StmtId statement : module_.block(block).statements) { mark_stmt(statement, owner); }
            }

            void mark_constraint(ast::ConstraintId constraint, ast::DeclId owner) {
                if (constraint == ast::no_node) { return; }
                std::visit(
                    [&](const auto &node) {
                        using T = std::decay_t<decltype(node)>;
                        if constexpr (std::is_same_v<T, ast::ConstraintType>) {
                            mark_type(node.type, owner);
                        } else if constexpr (std::is_same_v<T, ast::ConstraintValue>) {
                            mark_expr(node.value, owner);
                        } else if constexpr (std::is_same_v<T, ast::ConstraintSet>) {
                            for (ast::ConstraintId element : node.elements) { mark_constraint(element, owner); }
                        } else if constexpr (std::is_same_v<T, ast::ConstraintCall>) {
                            for (ast::ConstraintId argument : node.arguments) { mark_constraint(argument, owner); }
                        } else if constexpr (std::is_same_v<T, ast::ConstraintEach>) {
                            mark_constraint(node.source, owner);
                            mark_constraint(node.body, owner);
                        } else if constexpr (std::is_same_v<T, ast::OperatorRequirement>) {
                            for (ast::ConstraintId argument : node.arguments) { mark_constraint(argument, owner); }
                            mark_type(node.result, owner);
                        } else if constexpr (std::is_same_v<T, ast::ConstraintRelation> ||
                                             std::is_same_v<T, ast::ConstraintLogic>) {
                            mark_constraint(node.lhs, owner);
                            mark_constraint(node.rhs, owner);
                        } else if constexpr (std::is_same_v<T, ast::ConstraintNot>) {
                            mark_constraint(node.operand, owner);
                        }
                    },
                    module_.constraint(constraint).node);
            }

            void mark_signature(const ast::Signature &signature, ast::DeclId owner) {
                for (const ast::Parameter &parameter : signature.parameters) {
                    mark_type(parameter.type, owner);
                    mark_expr(parameter.default_value, owner);
                }
                mark_type(signature.result, owner);
            }

            void mark_generics(const std::vector<ast::GenericParameter> &generics, ast::DeclId owner) {
                for (const ast::GenericParameter &generic : generics) { mark_type(generic.type, owner); }
            }

            void mark_declaration(ast::DeclId declaration) {
                std::visit(
                    [&](const auto &node) {
                        using T = std::decay_t<decltype(node)>;
                        if constexpr (std::is_same_v<T, ast::StructDecl>) {
                            mark_generics(node.generics, declaration);
                            for (ast::TypeId parent : node.parents) { mark_type(parent, declaration); }
                            mark_constraint(node.requirements, declaration);
                            for (const ast::StructMember &member : node.members) {
                                std::visit(
                                    [&](const auto &item) {
                                        using M = std::decay_t<decltype(item)>;
                                        if constexpr (std::is_same_v<M, ast::StructField>) {
                                            mark_type(item.type, declaration);
                                            mark_expr(item.default_value, declaration);
                                        } else {
                                            mark_expr(item.value, declaration);
                                        }
                                    },
                                    member);
                            }
                        } else if constexpr (std::is_same_v<T, ast::OperatorDecl>) {
                            mark_generics(node.generics, declaration);
                            mark_signature(node.signature, declaration);
                            mark_constraint(node.requirements, declaration);
                            for (const ast::OperatorProperties &properties : node.properties) {
                                for (ast::TypeId domain : properties.domain) { mark_type(domain, declaration); }
                                for (const ast::OperatorProperty &property : properties.entries) {
                                    mark_expr(property.value, declaration);
                                }
                            }
                        } else if constexpr (std::is_same_v<T, ast::InstantiateDecl>) {
                            for (const ast::Instantiation &entry : node.entries) {
                                for (const ast::GenericArgument &argument : entry.arguments) {
                                    mark_type(argument.type, declaration);
                                    mark_expr(argument.value, declaration);
                                }
                            }
                        } else if constexpr (std::is_same_v<T, ast::FunctionDecl>) {
                            mark_generics(node.generics, declaration);
                            mark_signature(node.signature, declaration);
                            mark_constraint(node.requirements, declaration);
                            mark_expr(node.concise_body, declaration);
                            mark_block(node.block_body, declaration);
                        } else if constexpr (std::is_same_v<T, ast::NativeFunctionDecl>) {
                            mark_generics(node.generics, declaration);
                            mark_signature(node.signature, declaration);
                            mark_constraint(node.requirements, declaration);
                        } else if constexpr (std::is_same_v<T, ast::TestDecl>) {
                            mark_block(node.block, declaration);
                        }
                    },
                    module_.decl(declaration).node);
            }

            void declare_symbols() {
                for (std::size_t family = 0; family < resolved_.native_families.size(); ++family) {
                    if (resolved_.native_families[family].empty()) { continue; }
                    const auto &node =
                        std::get<ast::NativeFunctionDecl>(module_.decl(resolved_.native_families[family].front()).node);
                    native_family_symbols_[family] =
                        add_symbol(hir::SymbolKind::ImportedFunction, node.name.text, node.name.range, ast::no_node, 0, {},
                                   result_.path + "::" + std::string{node.name.text});
                    global_symbols_.emplace(std::string{node.name.text}, native_family_symbols_[family]);
                }
                for (ast::DeclId declaration = 0; declaration < module_.decls.size(); ++declaration) {
                    std::visit(
                        [&](const auto &node) {
                            using T = std::decay_t<decltype(node)>;
                            if constexpr (std::is_same_v<T, ast::ModuleDecl>) {
                                declaration_symbols_[declaration] = add_symbol(hir::SymbolKind::Module, join_path(node.path),
                                                                               module_.decl(declaration).range, declaration);
                            } else if constexpr (std::is_same_v<T, ast::StructDecl>) {
                                declaration_symbols_[declaration] =
                                    add_symbol(hir::SymbolKind::Struct, node.name.text, node.name.range, declaration);
                                global_symbols_.emplace(std::string{node.name.text}, declaration_symbols_[declaration]);
                                declare_generics(declaration, node.generics);
                            } else if constexpr (std::is_same_v<T, ast::OperatorDecl>) {
                                declaration_symbols_[declaration] =
                                    add_symbol(hir::SymbolKind::Operator, node.name.text, node.name.range, declaration, 0, {},
                                               result_.path + "." + std::string{node.name.text});
                                global_symbols_.emplace(std::string{node.name.text}, declaration_symbols_[declaration]);
                                declare_generics(declaration, node.generics);
                                declare_parameters(declaration, node.signature.parameters);
                            } else if constexpr (std::is_same_v<T, ast::FunctionDecl>) {
                                declaration_symbols_[declaration] = add_symbol(
                                    hir::SymbolKind::Function, node.name.text, node.name.range, declaration, 0, {},
                                    module_.decl(declaration).test_only ? result_.path + "." + std::string{node.name.text} + "$test"
                                                                        : "");
                                global_symbols_.emplace(std::string{node.name.text}, declaration_symbols_[declaration]);
                                declare_generics(declaration, node.generics);
                                declare_parameters(declaration, node.signature.parameters);
                            } else if constexpr (std::is_same_v<T, ast::NativeFunctionDecl>) {
                                std::size_t overload = 0;
                                for (const auto &family : resolved_.native_families) {
                                    const auto found = std::ranges::find(family, declaration);
                                    if (found != family.end()) {
                                        overload = static_cast<std::size_t>(found - family.begin());
                                        break;
                                    }
                                }
                                declaration_symbols_[declaration] = add_symbol(
                                    hir::SymbolKind::ImportedFunction, node.name.text, node.name.range, declaration, 0, {},
                                    result_.path + "::" + std::string{node.name.text} + "#" + std::to_string(overload));
                                declare_generics(declaration, node.generics);
                                declare_parameters(declaration, node.signature.parameters);
                            } else if constexpr (std::is_same_v<T, ast::TestDecl>) {
                                declaration_symbols_[declaration] =
                                    add_symbol(hir::SymbolKind::Test, node.name.text, node.name.range, declaration);
                                global_symbols_.emplace(std::string{node.name.text}, declaration_symbols_[declaration]);
                            }
                        },
                        module_.decl(declaration).node);
                }

                for (ast::StmtId statement = 0; statement < module_.stmts.size(); ++statement) {
                    const ast::DeclId owner = stmt_owners_[statement];
                    std::visit(
                        [&](const auto &node) {
                            using T = std::decay_t<decltype(node)>;
                            if constexpr (std::is_same_v<T, ast::LocalDecl>) {
                                statement_symbols_[statement].push_back(
                                    add_symbol(node.mutable_ ? hir::SymbolKind::LocalVar : hir::SymbolKind::LocalLet,
                                               node.name.text, node.name.range, owner));
                            } else if constexpr (std::is_same_v<T, ast::StateDecl>) {
                                statement_symbols_[statement].push_back(add_symbol(
                                    node.cache ? hir::SymbolKind::Cache : hir::SymbolKind::State, node.name.text, node.name.range, owner));
                            } else if constexpr (std::is_same_v<T, ast::InjectDecl>) {
                                for (const ast::Name &name : node.names) {
                                    statement_symbols_[statement].push_back(
                                        add_symbol(hir::SymbolKind::InjectedCapability, name.text, name.range, owner));
                                }
                            } else if constexpr (std::is_same_v<T, ast::ForStmt>) {
                                statement_symbols_[statement].push_back(
                                    add_symbol(hir::SymbolKind::LoopValue, node.first.text, node.first.range, owner, 0));
                                if (!node.second.empty()) {
                                    statement_symbols_[statement].push_back(
                                        add_symbol(hir::SymbolKind::LoopValue, node.second.text, node.second.range, owner, 1));
                                }
                            }
                        },
                        module_.stmt(statement).node);
                }

                for (ast::ExprId expression = 0; expression < module_.exprs.size(); ++expression) {
                    if (const auto *lambda = std::get_if<ast::AnonymousFn>(&module_.expr(expression).node)) {
                        for (std::size_t index = 0; index < lambda->parameters.size(); ++index) {
                            const ast::AnonymousParameter &parameter = lambda->parameters[index];
                            lambda_symbols_[expression].push_back(add_symbol(hir::SymbolKind::LambdaParameter, parameter.name.text,
                                                                             parameter.name.range, expr_owners_[expression],
                                                                             static_cast<std::uint32_t>(index)));
                        }
                    }
                }
            }

            void declare_generics(ast::DeclId owner, const std::vector<ast::GenericParameter> &generics) {
                generic_symbols_[owner].reserve(generics.size());
                for (std::size_t index = 0; index < generics.size(); ++index) {
                    const ast::GenericParameter &generic = generics[index];
                    generic_symbols_[owner].push_back(
                        add_symbol(generic.is_const ? hir::SymbolKind::ConstParameter : hir::SymbolKind::TypeParameter,
                                   generic.name.text, generic.name.range, owner, static_cast<std::uint32_t>(index)));
                }
            }

            void declare_parameters(ast::DeclId owner, const std::vector<ast::Parameter> &parameters) {
                const auto *function       = std::get_if<ast::FunctionDecl>(&module_.decl(owner).node);
                const bool  value_function = function && function->is_const;
                parameter_symbols_[owner].reserve(parameters.size());
                for (std::size_t index = 0; index < parameters.size(); ++index) {
                    const ast::Parameter &parameter = parameters[index];
                    parameter_symbols_[owner].push_back(add_symbol(parameter.is_const ? hir::SymbolKind::ConstParameter
                                                                   : value_function   ? hir::SymbolKind::ValueParameter
                                                                                      : hir::SymbolKind::SignalParameter,
                                                                   parameter.name.text, parameter.name.range, owner,
                                                                   static_cast<std::uint32_t>(index)));
                }
            }

            void assign_symbol_types() {
                for (ast::DeclId declaration = 0; declaration < module_.decls.size(); ++declaration) {
                    std::visit(
                        [&](const auto &node) {
                            using T = std::decay_t<decltype(node)>;
                            if constexpr (std::is_same_v<T, ast::StructDecl> || std::is_same_v<T, ast::OperatorDecl> ||
                                          std::is_same_v<T, ast::FunctionDecl> || std::is_same_v<T, ast::NativeFunctionDecl>) {
                                for (std::size_t index = 0; index < node.generics.size(); ++index) {
                                    result_.symbols[generic_symbols_[declaration][index].value].type =
                                        id<hir::TypeId>(node.generics[index].type);
                                }
                            }
                            if constexpr (std::is_same_v<T, ast::OperatorDecl> || std::is_same_v<T, ast::FunctionDecl> ||
                                          std::is_same_v<T, ast::NativeFunctionDecl>) {
                                for (std::size_t index = 0; index < node.signature.parameters.size(); ++index) {
                                    result_.symbols[parameter_symbols_[declaration][index].value].type =
                                        id<hir::TypeId>(node.signature.parameters[index].type);
                                }
                            }
                        },
                        module_.decl(declaration).node);
                }
                for (ast::StmtId statement = 0; statement < module_.stmts.size(); ++statement) {
                    std::visit(
                        [&](const auto &node) {
                            using T = std::decay_t<decltype(node)>;
                            if constexpr (std::is_same_v<T, ast::LocalDecl> || std::is_same_v<T, ast::StateDecl>) {
                                result_.symbols[statement_symbols_[statement].front().value].type = id<hir::TypeId>(node.type);
                            }
                        },
                        module_.stmt(statement).node);
                }
                for (ast::ExprId expression = 0; expression < module_.exprs.size(); ++expression) {
                    if (const auto *lambda = std::get_if<ast::AnonymousFn>(&module_.expr(expression).node)) {
                        for (std::size_t index = 0; index < lambda->parameters.size(); ++index) {
                            result_.symbols[lambda_symbols_[expression][index].value].type =
                                id<hir::TypeId>(lambda->parameters[index].type);
                        }
                    }
                }
            }

            /// A field's HIR type. A field this module declares has an AST type;
            /// one inherited from a struct another module exports does not --
            /// its type lives in the owner's layout (ADR 0013), so it is
            /// lowered from there rather than left absent, which would leave
            /// the field typeless in both IRs.
            [[nodiscard]] hir::TypeId imported_field_type(const semantics::StructField &field, syntax::SourceRange range) {
                if (!field.origin.is_imported()) { return id<hir::TypeId>(field.type); }
                const semantics::ImportedStruct &owner = resolved_.imported_structs[field.origin.imported];
                const auto found = std::ranges::find(owner.fields, field.name, &semantics::ImportedStructField::name);
                if (found == owner.fields.end()) { return hir::no_type; }
                if (!owner.generics.empty()) {
                    // A generic imported family needs its parameters mapped
                    // into this module's symbols, which applying an imported
                    // family will establish; until then the field would be
                    // typed against the wrong scope.
                    diagnostics_.report(syntax::Category::Type, range,
                                        "inheriting a generic imported struct is not supported yet: '" + owner.identity +
                                            "' declares generic parameters");
                    return hir::no_type;
                }
                static const std::unordered_map<std::string, hir::SymbolId> none;
                return imported_type(found->type, none, range);
            }

            [[nodiscard]] hir::SymbolId external_symbol(hir::SymbolKind kind, std::string_view name, std::string_view external_name,
                                                        std::string_view canonical_name, syntax::SourceRange range) {
                const std::string identity = canonical_name.empty() ? std::string{external_name} : std::string{canonical_name};
                const std::string key      = std::to_string(static_cast<unsigned>(kind)) + ':' + identity;
                if (const auto found = external_symbols_.find(key); found != external_symbols_.end()) { return found->second; }
                const hir::SymbolId symbol =
                    add_symbol(kind, name, range, ast::no_node, 0, std::string{external_name}, std::string{canonical_name});
                external_symbols_.emplace(key, symbol);
                return symbol;
            }

            [[nodiscard]] hir::ExprId imported_constant(const semantics::ImportedConstant                    &source,
                                                        const std::unordered_map<std::string, hir::SymbolId> &generics,
                                                        syntax::SourceRange                                   range) {
                using semantics::ImportedConstantKind;
                if (source.kind == ImportedConstantKind::None) { return {}; }
                if (source.kind == ImportedConstantKind::Parameter) {
                    const auto found = generics.find(source.binding_identity);
                    if (found == generics.end()) {
                        diagnostics_.report(syntax::Category::Name, range,
                                            "native type pattern names unknown const generic '" + source.binding_identity + "'");
                        return {};
                    }
                    return synthesized_ref(found->second, range);
                }
                hir::Expr expression;
                expression.range      = range;
                expression.type       = literal_type(hir::ScalarType::I64);
                expression.phase      = hir::Phase::Constant;
                expression.value_kind = hir::ValueKind::Constant;
                expression.constant   = hir::Constant{source.i64};
                expression.node       = hir::Literal{source.i64};
                const hir::ExprId id{static_cast<std::uint32_t>(result_.exprs.size())};
                result_.exprs.push_back(std::move(expression));
                return id;
            }

            [[nodiscard]] hir::TypeId imported_type(const semantics::ImportedType                        &source,
                                                    const std::unordered_map<std::string, hir::SymbolId> &generics,
                                                    syntax::SourceRange                                   range) {
                if (source.kind == semantics::ImportedTypeKind::Scalar) { return literal_type(lower_scalar_type(source.scalar)); }
                hir::Type target;
                target.kind           = lower_imported_type_kind(source.kind);
                target.range          = range;
                target.value_position = true;
                target.unbounded      = source.unbounded;
                if (source.kind == semantics::ImportedTypeKind::Symbol) {
                    if (source.binding_identity.empty() && !source.nominal_identity.empty()) {
                        // A layout's Symbol may name a STRUCT rather than a
                        // generic parameter (ADR 0013) -- a field type, or an
                        // ADR 0012 edge's target. It interns by identity, the
                        // way the struct itself does; the generic-only path
                        // would report an unknown generic and leave the type
                        // without a symbol.
                        target.symbol = external_symbol(hir::SymbolKind::ImportedStruct, source.nominal_identity,
                                                        source.nominal_identity, source.nominal_identity, range);
                    } else {
                        const auto found = generics.find(source.binding_identity);
                        if (found == generics.end()) {
                            diagnostics_.report(syntax::Category::Name, range,
                                                "native type pattern names unknown type generic '" + source.binding_identity +
                                                    "'");
                        } else {
                            target.symbol = found->second;
                        }
                    }
                }
                for (const semantics::ImportedType &child : source.children) {
                    target.children.push_back(imported_type(child, generics, range));
                }
                target.size     = imported_constant(source.size, generics, range);
                target.min_size = imported_constant(source.min_size, generics, range);
                const hir::TypeId id{static_cast<std::uint32_t>(result_.types.size())};
                result_.types.push_back(std::move(target));
                return id;
            }

            [[nodiscard]] hir::SymbolId imported_operator(const semantics::Binding &binding, syntax::SourceRange range,
                                                          std::string_view spelling) {
                const hir::SymbolId symbol = external_symbol(hir::SymbolKind::ImportedOperator, spelling, binding.registry_name,
                                                             binding.operator_identity, range);
                if (std::ranges::any_of(result_.imported_operators, [&](const auto &entry) { return entry.symbol == symbol; })) {
                    return symbol;
                }
                const auto source = std::ranges::find(resolved_.imported_contracts, binding.operator_identity,
                                                      &semantics::ImportedOperatorContract::identity);
                if (source == resolved_.imported_contracts.end()) { return symbol; }  // Legacy kernel-name import.
                hir::ImportedOperator target;
                target.symbol                 = symbol;
                target.descriptor_fingerprint = source->descriptor_fingerprint;
                std::unordered_map<std::string, hir::SymbolId> generics;
                for (std::size_t index = 0; index < source->generics.size(); ++index) {
                    const auto &generic = source->generics[index];
                    const auto  id = add_symbol(generic.is_const ? hir::SymbolKind::ConstParameter : hir::SymbolKind::TypeParameter,
                                                generic.name, range, ast::no_node, static_cast<std::uint32_t>(index), {},
                                                generic.binding_identity);
                    generics.emplace(generic.binding_identity, id);
                    target.contract.generics.push_back({id, generic.is_const, {}});
                }
                for (std::size_t index = 0; index < source->generics.size(); ++index) {
                    if (!source->generics[index].type) { continue; }
                    const auto type                      = imported_type(*source->generics[index].type, generics, range);
                    target.contract.generics[index].type = type;
                    result_.symbols[target.contract.generics[index].symbol.value].type = type;
                }
                for (std::size_t index = 0; index < source->parameters.size(); ++index) {
                    const auto &parameter = source->parameters[index];
                    const auto  id        = add_symbol(
                        parameter.is_const ? hir::SymbolKind::ConstParameter : hir::SymbolKind::SignalParameter, parameter.name,
                        range, ast::no_node, static_cast<std::uint32_t>(index), {}, parameter.binding_identity);
                    const auto type                = imported_type(parameter.type, generics, range);
                    result_.symbols[id.value].type = type;
                    target.contract.signature.parameters.push_back({id, parameter.is_const, type});
                }
                target.contract.signature.result = source->result ? imported_type(*source->result, generics, range) : void_type();
                result_.imported_operators.push_back(std::move(target));
                return symbol;
            }

            /// Rebuilds one node of an imported `where` as typed HIR (ADR 0013).
            /// `generics` maps the exporting module's binding identities to this
            /// module's symbols -- the same table `imported_type` uses -- so the
            /// requirement refers to the symbols an application binds.
            [[nodiscard]] hir::ConstraintId imported_constraint(const std::vector<semantics::ImportedConstraint> &arena,
                                                                std::uint32_t index,
                                                                const std::unordered_map<std::string, hir::SymbolId> &generics,
                                                                std::vector<hir::ConstraintId> &cache,
                                                                syntax::SourceRange range) {
                if (index == semantics::no_imported_constraint || index >= arena.size()) { return hir::no_constraint; }
                if (cache[index] != hir::no_constraint) { return cache[index]; }
                const semantics::ImportedConstraint &source = arena[index];
                const hir::ConstraintId reserved{static_cast<std::uint32_t>(result_.constraints.size())};
                result_.constraints.emplace_back();
                cache[index] = reserved;
                const auto child  = [&](std::uint32_t id) { return imported_constraint(arena, id, generics, cache, range); };
                const auto symbol = [&](const std::string &identity) {
                    const auto found = generics.find(identity);
                    return found == generics.end() ? hir::no_symbol : found->second;
                };
                hir::Constraint target;
                target.range = range;
                using K      = semantics::ImportedConstraintKind;
                switch (source.kind) {
                    case K::Symbol: target.node = hir::ConstraintSymbol{symbol(source.identity)}; break;
                    case K::Type:
                        target.node =
                            hir::ConstraintType{source.type ? imported_type(*source.type, generics, range) : hir::no_type};
                        break;
                    case K::Value: target.node = hir::ConstraintValue{imported_constant(source.value, generics, range)}; break;
                    case K::Set: {
                        hir::ConstraintSet set;
                        for (const std::uint32_t element : source.elements) { set.elements.push_back(child(element)); }
                        target.node = std::move(set);
                        break;
                    }
                    case K::Call: {
                        hir::ConstraintCall call{symbol(source.identity), {}};
                        for (const std::uint32_t argument : source.arguments) { call.arguments.push_back(child(argument)); }
                        target.node = std::move(call);
                        break;
                    }
                    case K::Each:
                        target.node = hir::ConstraintEach{symbol(source.identity), child(source.source), child(source.body)};
                        break;
                    case K::Operator: {
                        // An operator requirement names an OPERATOR, not one of
                        // the struct's generic parameters, so the generics table
                        // never holds it. It interns as an imported operator by
                        // its canonical identity, carrying the registry name the
                        // solver dispatches on -- looking it up among the
                        // generics yields no symbol, and the solver then refuses
                        // every application.
                        hir::SymbolId op = hir::no_symbol;
                        if (!source.identity.empty()) {
                            // `external_name` IS the registry key the solver
                            // dispatches on; `canonical_name` is the defining
                            // module's identity, independent of that spelling.
                            op = external_symbol(hir::SymbolKind::ImportedOperator, source.identity, source.registry_name,
                                                 source.identity, range);
                        }
                        hir::OperatorRequirement requirement{op, {}, hir::no_type};
                        for (const std::uint32_t argument : source.arguments) {
                            requirement.arguments.push_back(child(argument));
                        }
                        if (source.type) { requirement.result = imported_type(*source.type, generics, range); }
                        target.node = std::move(requirement);
                        break;
                    }
                    case K::Relation:
                        target.node = hir::ConstraintRelation{imported_relation_op(source.operator_spelling), child(source.lhs),
                                                              child(source.rhs), source.relation_category};
                        break;
                    case K::Not: target.node = hir::ConstraintNot{child(source.operand)}; break;
                    case K::Logic:
                        target.node = hir::ConstraintLogic{imported_logic_op(source.operator_spelling), child(source.lhs),
                                                           child(source.rhs)};
                        break;
                }
                result_.constraints[reserved.value] = std::move(target);
                return reserved;
            }


            /// Re-describes a struct another module exports into this module's
            /// HIR (ADR 0013), so hgraph IR can emit a contract both backends
            /// register from, and the EXISTING solver can check the family's
            /// requirements when it is applied -- not a second checker beside
            /// it (CLAUDE.md guardrail iii). Nothing here declares the struct:
            /// its identity stays the owner's.
            /// Lowers `source` and everything its layout reaches, ANCESTORS
            /// FIRST, without putting the closure's depth on the stack.
            ///
            /// This is the ONLY thing that orders ancestors before
            /// descendants: `lower_imported_struct` reaches for an ancestor's
            /// flattened fields and does not describe one itself, so an
            /// ordering slip here is a diagnostic there rather than a silent
            /// re-descent. A descriptor is an input: the chain `A0 -> A1 ->
            /// ...` is as long as the supplying module chose, and descending
            /// it per struct was the compiler's stack.
            void lower_imported_closure(const semantics::ImportedStruct &source, hir::SymbolId symbol,
                                        syntax::SourceRange range) {
                struct Pending
                {
                    semantics::ImportedStruct record{};
                    hir::SymbolId             symbol{};
                    std::size_t               parent{0};
                };
                const auto record_for = [&](const std::string &identity) -> const semantics::ImportedStruct * {
                    const auto found =
                        std::ranges::find(resolved_.imported_structs, identity, &semantics::ImportedStruct::identity);
                    return found == resolved_.imported_structs.end() ? nullptr : &*found;
                };
                // QUEUED IS NOT LOWERED. One set for both answers let a parent
                // that a sibling field had queued as a root be skipped by the
                // walk that inherits it, and the descendant was then flattened
                // against an ancestry nothing had described. So: `queued`
                // answers "is a root already waiting", and only
                // `described_imported_structs_` answers "is it lowered".
                std::unordered_set<std::string> queued{source.identity};
                std::vector<Pending>            roots{Pending{source, symbol, 0}};
                std::vector<std::string>        references;
                while (!roots.empty()) {
                    // Post-order over PARENTS: an ancestor's fields are read
                    // when its descendant is flattened, so it has to be
                    // recorded first.
                    std::vector<Pending> stack{std::move(roots.back())};
                    roots.pop_back();
                    // What stops THIS walk going round: an ancestry cycle is
                    // refused upstream, so this only guards a diamond whose
                    // two sides meet before either is lowered.
                    std::unordered_set<std::string> visiting{stack.back().record.identity};
                    while (!stack.empty()) {
                        if (stack.back().parent < stack.back().record.parents.size()) {
                            const semantics::ImportedType &parent =
                                stack.back().record.parents[stack.back().parent++];
                            if (parent.nominal_identity.empty()) { continue; }
                            if (described_imported_structs_.contains(parent.nominal_identity)) { continue; }
                            if (!visiting.insert(parent.nominal_identity).second) { continue; }
                            const semantics::ImportedStruct *ancestor = record_for(parent.nominal_identity);
                            if (ancestor == nullptr) { continue; }
                            const semantics::ImportedStruct copy = *ancestor;
                            const hir::SymbolId              ancestor_symbol =
                                external_symbol(hir::SymbolKind::ImportedStruct, copy.identity, copy.identity,
                                                copy.identity, range);
                            stack.push_back(Pending{copy, ancestor_symbol, 0});
                            continue;
                        }
                        const Pending done = std::move(stack.back());
                        stack.pop_back();
                        lower_imported_struct(done.record, done.symbol, range);
                        // Whatever its fields NAME becomes a root of its own;
                        // those are referred to by identity, so they need no
                        // ordering against this one.
                        references.clear();
                        for (const semantics::ImportedStructField &field : done.record.fields) {
                            layout_nominals(field.type, references);
                        }
                        for (const std::string &identity : references) {
                            if (described_imported_structs_.contains(identity)) { continue; }
                            if (!queued.insert(identity).second) { continue; }
                            const semantics::ImportedStruct *referenced = record_for(identity);
                            if (referenced == nullptr) { continue; }
                            const semantics::ImportedStruct copy = *referenced;
                            const hir::SymbolId              referenced_symbol =
                                external_symbol(hir::SymbolKind::ImportedStruct, copy.identity, copy.identity,
                                                copy.identity, range);
                            roots.push_back(Pending{copy, referenced_symbol, 0});
                        }
                    }
                }
            }

            /// The nominal identities a layout type names, at any depth.
            static void layout_nominals(const semantics::ImportedType &type, std::vector<std::string> &out) {
                if (!type.nominal_identity.empty()) { out.push_back(type.nominal_identity); }
                for (const semantics::ImportedType &child : type.children) { layout_nominals(child, out); }
            }

            void lower_imported_struct(const semantics::ImportedStruct &source, hir::SymbolId symbol,
                                       syntax::SourceRange range) {
                // The guard covers a struct still BEING described, not only
                // one already recorded: a recursive edge (ADR 0012) names its
                // own struct, and the record is pushed only once its fields
                // are lowered -- so a scan of `result_.imported_structs`
                // would never see it and the description would not terminate.
                if (!described_imported_structs_.insert(source.identity).second) { return; }
                hir::ImportedStructDecl target;
                target.identity       = source.identity;
                target.symbol         = symbol;
                target.abstract       = source.abstract;
                target.public_headers = source.public_headers;
                target.range          = range;

                std::unordered_map<std::string, hir::SymbolId> generics;
                for (std::size_t index = 0; index < source.generics.size(); ++index) {
                    const semantics::ImportedGeneric &generic = source.generics[index];
                    const hir::SymbolId               id      = add_symbol(
                        generic.is_const ? hir::SymbolKind::ConstParameter : hir::SymbolKind::TypeParameter, generic.name, range,
                        ast::no_node, static_cast<std::uint32_t>(index), {}, generic.binding_identity);
                    generics.emplace(generic.binding_identity, id);
                    target.generics.push_back({id, generic.is_const, {}});
                }
                for (std::size_t index = 0; index < source.generics.size(); ++index) {
                    if (!source.generics[index].type) { continue; }
                    const hir::TypeId type                                    = imported_type(*source.generics[index].type, generics, range);
                    target.generics[index].type                               = type;
                    result_.symbols[target.generics[index].symbol.value].type = type;
                }
                for (const semantics::ImportedType &parent : source.parents) {
                    target.parents.push_back(imported_type(parent, generics, range));
                    // An ancestor is named only through this parent, never in
                    // the source, and a backend cannot register a family whose
                    // ancestors it has no layout for. Describing it HERE is
                    // what put the chain's length on the stack, so this asks
                    // rather than descends: `lower_imported_closure` owns the
                    // ordering, and if it ever stops holding, the flattening
                    // below would quietly drop every inherited field instead.
                    if (parent.nominal_identity.empty()) { continue; }
                    if (described_imported_structs_.contains(parent.nominal_identity)) { continue; }
                    const auto ancestor = std::ranges::find(resolved_.imported_structs, parent.nominal_identity,
                                                            &semantics::ImportedStruct::identity);
                    if (ancestor == resolved_.imported_structs.end()) { continue; }
                    diagnostics_.report(syntax::Category::Name, range,
                                        "imported struct '" + source.identity + "' was described before its parent '" +
                                            parent.nominal_identity + "'");
                }
                // A catalog record holds only the fields it DECLARES, while
                // every consumer of `hir::StructField` -- the type checker,
                // hgraph IR, and through it both backends -- reads a struct's
                // fields as its WHOLE layout, the way a local declaration's
                // are (`seed_imported_fields` seeds a local child the same
                // way). hgraph's registry holds the same rule from the other
                // side: `bundle()` refuses a child that does not preserve its
                // parents' fields. So the ancestry is flattened here,
                // ancestors first, each field still naming the ancestor that
                // declares it. The ancestors were lowered just above, and
                // theirs are flattened already, so a diamond dedupes by name.
                std::unordered_set<std::string> present;
                for (const semantics::ImportedType &parent : source.parents) {
                    if (parent.nominal_identity.empty()) { continue; }
                    const auto ancestor = std::ranges::find(result_.imported_structs, parent.nominal_identity,
                                                            &hir::ImportedStructDecl::identity);
                    if (ancestor == result_.imported_structs.end()) { continue; }
                    // By value: lowering a field's type below can describe a
                    // further struct and grow the vector this points into.
                    const std::vector<hir::StructField> inherited = ancestor->fields;
                    for (const hir::StructField &field : inherited) {
                        // Indexed, not scanned: an ancestor's fields are
                        // already flattened, so a chain of N structs copies
                        // O(N) fields into each descendant and a linear dedupe
                        // per copied field makes the whole flattening cubic in
                        // the chain length (CLAUDE.md guardrail iv).
                        if (!present.insert(field.name).second) { continue; }
                        target.fields.push_back(field);
                    }
                }
                for (const semantics::ImportedStructField &field : source.fields) {
                    if (!present.insert(field.name).second) { continue; }
                    target.fields.push_back(hir::StructField{field.name, imported_type(field.type, generics, range),
                                                             hir::no_expr, hir::no_declaration, source.identity,
                                                             field.optional, range, field.recursive});
                    // A field's type may name another exported struct, and a
                    // recursive edge names its target (ADR 0012) -- those are
                    // described too, but by `lower_imported_closure`, which
                    // queues them as roots of their own. Descending into them
                    // from here put the chain's length on the stack, which is
                    // the whole reason that driver exists.
                }
                // The `where` the exporting module declared, rebuilt for the
                // solver that already checks a local family's.
                std::vector<hir::ConstraintId> cache(source.constraints.size(), hir::no_constraint);
                target.requirements = imported_constraint(source.constraints, source.requirements, generics, cache, range);
                result_.imported_structs.push_back(std::move(target));
            }

            [[nodiscard]] hir::SymbolId imported_function(const semantics::Binding &binding, syntax::SourceRange range,
                                                          std::string_view spelling) {
                const std::size_t count = binding.count == 0U ? 1U : binding.count;
                if (binding.index >= resolved_.imported_functions.size() ||
                    count > resolved_.imported_functions.size() - binding.index) {
                    diagnostics_.report(syntax::Category::Name, range,
                                        "imported function '" + std::string{spelling} + "' has no catalog record");
                    return {};
                }
                if (const auto found = imported_function_symbols_.find(binding.index); found != imported_function_symbols_.end()) {
                    return found->second;
                }

                const semantics::ImportedFunction &family_source = resolved_.imported_functions[binding.index];
                const hir::SymbolId                family =
                    external_symbol(hir::SymbolKind::ImportedFunction, spelling, {}, family_source.identity, range);
                for (std::size_t offset = 0; offset < count; ++offset) {
                    const semantics::ImportedFunction &source = resolved_.imported_functions[binding.index + offset];
                    const std::string   candidate_identity    = source.candidate_identity.empty()
                                                                    ? source.identity + "#native-" + std::to_string(offset)
                                                                    : source.candidate_identity;
                    const hir::SymbolId symbol =
                        external_symbol(hir::SymbolKind::ImportedFunction, spelling, source.cpp_symbol, candidate_identity, range);
                    std::unordered_map<std::string, hir::SymbolId> generic_symbols;
                    hir::NativeFunction                            target;
                    target.symbol                 = symbol;
                    target.family                 = family;
                    target.module_identity        = source.module_identity;
                    target.identity               = source.identity;
                    target.candidate_identity     = candidate_identity;
                    target.cpp_symbol             = source.cpp_symbol;
                    target.public_headers         = source.public_headers;
                    target.cmake_packages         = source.cmake_packages;
                    target.imported_targets       = source.imported_targets;
                    target.runtime_images         = source.runtime_images;
                    target.descriptor_fingerprint = source.descriptor_fingerprint;
                    for (std::size_t index = 0; index < source.generics.size(); ++index) {
                        const semantics::ImportedGeneric &generic        = source.generics[index];
                        const hir::SymbolId               generic_symbol = add_symbol(
                            generic.is_const ? hir::SymbolKind::ConstParameter : hir::SymbolKind::TypeParameter, generic.name,
                            range, ast::no_node, static_cast<std::uint32_t>(index), {}, candidate_identity + "::" + generic.name);
                        generic_symbols.emplace(generic.binding_identity, generic_symbol);
                        target.generics.push_back(hir::GenericParameter{generic_symbol, generic.is_const, {}});
                    }
                    for (std::size_t index = 0; index < source.generics.size(); ++index) {
                        if (!source.generics[index].type) { continue; }
                        const hir::TypeId type      = imported_type(*source.generics[index].type, generic_symbols, range);
                        target.generics[index].type = type;
                        result_.symbols[target.generics[index].symbol.value].type = type;
                    }
                    for (const semantics::ImportedParameter &parameter : source.parameters) {
                        target.parameters.push_back(
                            hir::NativeParameter{parameter.name, imported_type(parameter.type, generic_symbols, range),
                                                 parameter.is_const, lower_native_access(parameter.access)});
                    }
                    target.result = source.result ? imported_type(*source.result, generic_symbols, range) : void_type();
                    target.throws = source.throws;
                    for (semantics::NativeCallPhase phase : source.phases) { target.phases.push_back(lower_native_phase(phase)); }
                    result_.native_functions.push_back(std::move(target));
                }
                imported_function_symbols_.emplace(binding.index, family);
                return family;
            }

            [[nodiscard]] hir::SymbolId symbol_for(const semantics::Binding &binding, syntax::SourceRange range,
                                                   std::string_view spelling) {
                using semantics::BindingKind;
                switch (binding.kind) {
                    case BindingKind::Local:
                        if (binding.stmt < statement_symbols_.size()) {
                            const auto       &symbols = statement_symbols_[binding.stmt];
                            const std::size_t index   = binding.second ? 1U : binding.index;
                            if (index < symbols.size()) { return symbols[index]; }
                        }
                        break;
                    case BindingKind::Parameter:
                        if (binding.decl == ast::no_node && binding.stmt < lambda_symbols_.size() &&
                            binding.index < lambda_symbols_[binding.stmt].size()) {
                            return lambda_symbols_[binding.stmt][binding.index];
                        }
                        if (binding.decl < parameter_symbols_.size() && binding.index < parameter_symbols_[binding.decl].size()) {
                            return parameter_symbols_[binding.decl][binding.index];
                        }
                        break;
                    case BindingKind::Generic:
                        if (binding.decl < generic_symbols_.size() && binding.index < generic_symbols_[binding.decl].size()) {
                            return generic_symbols_[binding.decl][binding.index];
                        }
                        break;
                    case BindingKind::ConstraintLocal:
                        if (binding.constraint < constraint_symbols_.size()) {
                            hir::SymbolId &symbol = constraint_symbols_[binding.constraint];
                            if (!symbol.valid()) {
                                symbol = add_symbol(hir::SymbolKind::TypeParameter, spelling, range, binding.decl);
                            }
                            return symbol;
                        }
                        break;
                    case BindingKind::ImportedStruct: {
                        // A struct another module exports has no declaration
                        // here, so it is an external symbol interned by the
                        // owner's identity (ADR 0013) -- the same way an
                        // imported function or operator is. Re-describing it
                        // into this module's IR happens once, here, so hgraph
                        // IR can emit a contract the backends register from.
                        const semantics::ImportedStruct structure = resolved_.imported_structs[binding.index];
                        const hir::SymbolId             symbol =
                            external_symbol(hir::SymbolKind::ImportedStruct, spelling, structure.identity,
                                            structure.identity, range);
                        lower_imported_closure(structure, symbol, range);
                        return symbol;
                    }
                    case BindingKind::Struct:
                    case BindingKind::Function:
                    case BindingKind::LocalOperator:
                    case BindingKind::Test:
                        if (binding.decl < declaration_symbols_.size()) { return declaration_symbols_[binding.decl]; }
                        break;
                    case BindingKind::NativeFunction:
                        if (binding.index < native_family_symbols_.size()) { return native_family_symbols_[binding.index]; }
                        break;
                    case BindingKind::ImportedFunction: return imported_function(binding, range, spelling);
                    case BindingKind::Operator: return imported_operator(binding, range, spelling);
                    case BindingKind::Intrinsic:
                        return external_symbol(hir::SymbolKind::Intrinsic, spelling, binding.registry_name, {}, range);
                    case BindingKind::Unbound: break;
                }
                diagnostics_.report(syntax::Category::Name, range,
                                    "cannot form HIR identity for resolved name '" + std::string{spelling} + "'");
                return {};
            }

            [[nodiscard]] std::optional<hir::SymbolId> lexical_symbol(ast::DeclId owner, std::string_view name,
                                                                      bool want_const) const {
                if (owner != ast::no_node && owner < generic_symbols_.size()) {
                    const ast::DeclNode                      &declaration = module_.decl(owner).node;
                    const std::vector<ast::GenericParameter> *generics    = nullptr;
                    if (const auto *node = std::get_if<ast::StructDecl>(&declaration)) {
                        generics = &node->generics;
                    } else if (const auto *operation = std::get_if<ast::OperatorDecl>(&declaration)) {
                        generics = &operation->generics;
                    } else if (const auto *function = std::get_if<ast::FunctionDecl>(&declaration)) {
                        generics = &function->generics;
                    } else if (const auto *native = std::get_if<ast::NativeFunctionDecl>(&declaration)) {
                        generics = &native->generics;
                    }
                    if (generics) {
                        for (std::size_t index = 0; index < generics->size(); ++index) {
                            if ((*generics)[index].name.text == name && (*generics)[index].is_const == want_const) {
                                return generic_symbols_[owner][index];
                            }
                        }
                    }
                }
                if (!want_const) {
                    if (const auto found = global_symbols_.find(std::string{name}); found != global_symbols_.end()) {
                        return found->second;
                    }
                }
                return std::nullopt;
            }

            [[nodiscard]] hir::ExprId synthesized_ref(hir::SymbolId symbol, syntax::SourceRange range) {
                const hir::ExprId result{static_cast<std::uint32_t>(result_.exprs.size())};
                hir::ValueKind    value_kind = hir::ValueKind::Unknown;
                hir::Phase        phase      = hir::Phase::Unknown;
                if (symbol.valid()) {
                    const hir::SymbolKind kind = result_.symbol(symbol).kind;
                    if (kind == hir::SymbolKind::ConstParameter) {
                        value_kind = hir::ValueKind::Constant;
                        phase      = hir::Phase::Constant;
                    } else if (kind == hir::SymbolKind::TypeParameter || kind == hir::SymbolKind::Struct) {
                        value_kind = hir::ValueKind::Type;
                    }
                }
                hir::Expr expression;
                expression.range      = range;
                expression.type       = symbol.valid() ? result_.symbol(symbol).type : hir::no_type;
                expression.phase      = phase;
                expression.value_kind = value_kind;
                expression.node       = hir::SymbolRef{symbol};
                result_.exprs.push_back(std::move(expression));
                return result;
            }

            [[nodiscard]] hir::TypeId synthesized_symbol_type(hir::SymbolId symbol, syntax::SourceRange range, ast::DeclId owner) {
                const hir::TypeId result{static_cast<std::uint32_t>(result_.types.size())};
                hir::Type         type;
                type.kind           = hir::TypeKind::Symbol;
                type.range          = range;
                type.owner          = id<hir::DeclarationId>(owner);
                type.symbol         = symbol;
                type.value_position = true;
                result_.types.push_back(std::move(type));
                return result;
            }

            void lower_type(ast::TypeId index) {
                const ast::Type &source = module_.type(index);
                hir::Type        target;
                target.kind           = lower_type_kind(source.kind);
                target.range          = source.range;
                target.owner          = id<hir::DeclarationId>(type_owners_[index]);
                target.scalar         = lower_scalar_type(source.scalar);
                target.unbounded      = source.unbounded;
                target.value_position = source.value_position;
                for (ast::TypeId child : source.children) { target.children.push_back(id<hir::TypeId>(child)); }
                target.size     = id<hir::ExprId>(source.size);
                target.min_size = id<hir::ExprId>(source.min_size);
                if (source.kind == ast::TypeKind::Named) {
                    target.kind   = hir::TypeKind::Symbol;
                    target.symbol = symbol_for(resolved_.type_binding(index), source.name.range, source.name.text);
                }
                for (std::size_t argument_index = 0; argument_index < source.arguments.size(); ++argument_index) {
                    const ast::GenericArgument &argument = source.arguments[argument_index];
                    hir::TypeArgument           lowered;
                    lowered.range = argument.range;
                    if (argument.type != ast::no_node) {
                        lowered.kind = hir::TypeArgumentKind::Type;
                        lowered.type = id<hir::TypeId>(argument.type);
                    } else if (argument.value != ast::no_node) {
                        lowered.kind  = hir::TypeArgumentKind::Value;
                        lowered.value = id<hir::ExprId>(argument.value);
                    } else {
                        bool want_const = false;
                        if (source.kind == ast::TypeKind::Named) {
                            const semantics::Binding &binding = resolved_.type_binding(index);
                            if (binding.kind == semantics::BindingKind::Struct) {
                                const auto &generics = std::get<ast::StructDecl>(module_.decl(binding.decl).node).generics;
                                if (argument_index < generics.size()) { want_const = generics[argument_index].is_const; }
                            }
                        }
                        const std::optional<hir::SymbolId> symbol =
                            lexical_symbol(type_owners_[index], argument.name.text, want_const);
                        if (!symbol) {
                            diagnostics_.report(syntax::Category::Name, argument.name.range,
                                                "cannot form HIR identity for generic argument '" +
                                                    std::string{argument.name.text} + "'");
                        }
                        if (want_const) {
                            lowered.kind  = hir::TypeArgumentKind::Value;
                            lowered.value = synthesized_ref(symbol.value_or(hir::no_symbol), argument.range);
                        } else {
                            lowered.kind = hir::TypeArgumentKind::Type;
                            lowered.type =
                                synthesized_symbol_type(symbol.value_or(hir::no_symbol), argument.range, type_owners_[index]);
                        }
                    }
                    target.arguments.push_back(lowered);
                }
                result_.types[index] = std::move(target);
            }

            [[nodiscard]] hir::TypeId literal_type(hir::ScalarType scalar) {
                const auto key = static_cast<std::uint8_t>(scalar);
                if (const auto found = literal_types_.find(key); found != literal_types_.end()) { return found->second; }
                const hir::TypeId result{static_cast<std::uint32_t>(result_.types.size())};
                hir::Type         type;
                type.kind           = hir::TypeKind::Scalar;
                type.scalar         = scalar;
                type.value_position = true;
                result_.types.push_back(type);
                literal_types_.emplace(key, result);
                return result;
            }

            [[nodiscard]] hir::TypeId void_type() {
                if (void_type_.valid()) { return void_type_; }
                void_type_ = hir::TypeId{static_cast<std::uint32_t>(result_.types.size())};
                hir::Type type;
                type.kind           = hir::TypeKind::Void;
                type.value_position = true;
                result_.types.push_back(type);
                return void_type_;
            }

            [[nodiscard]] static hir::ScalarType temporal_type(syntax::TemporalKind kind) noexcept {
                using syntax::TemporalKind;
                switch (kind) {
                    case TemporalKind::Date: return hir::ScalarType::Date;
                    case TemporalKind::Time: return hir::ScalarType::Time;
                    case TemporalKind::DateTime: return hir::ScalarType::DateTime;
                    case TemporalKind::Duration: return hir::ScalarType::Duration;
                    case TemporalKind::CivilDateTime: return hir::ScalarType::CivilDateTime;
                    case TemporalKind::ZonedDateTime: return hir::ScalarType::ZonedDateTime;
                    case TemporalKind::ZonedTime: return hir::ScalarType::ZonedTime;
                    case TemporalKind::TimeZone: return hir::ScalarType::TimeZone;
                }
                std::unreachable();
            }

            [[nodiscard]] std::vector<hir::Argument> lower_arguments(const std::vector<ast::Argument> &arguments) const {
                std::vector<hir::Argument> result;
                result.reserve(arguments.size());
                for (const ast::Argument &argument : arguments) {
                    result.push_back(
                        hir::Argument{std::string{argument.name.text}, id<hir::ExprId>(argument.value),
                                      argument.name.empty() ? module_.expr(argument.value).range : argument.name.range});
                }
                return result;
            }

            void lower_expr(ast::ExprId index) {
                const ast::Expr &source = module_.expr(index);
                hir::Expr        target;
                target.range = source.range;
                target.owner = id<hir::DeclarationId>(expr_owners_[index]);
                std::visit(
                    [&](const auto &node) {
                        using T = std::decay_t<decltype(node)>;
                        if constexpr (std::is_same_v<T, ast::IntLiteral>) {
                            target.node       = hir::Literal{node.value};
                            target.type       = literal_type(hir::ScalarType::I64);
                            target.phase      = hir::Phase::Constant;
                            target.value_kind = hir::ValueKind::Constant;
                            target.constant   = hir::Constant{node.value};
                        } else if constexpr (std::is_same_v<T, ast::FloatLiteral>) {
                            target.node       = hir::Literal{node.value};
                            target.type       = literal_type(hir::ScalarType::F64);
                            target.phase      = hir::Phase::Constant;
                            target.value_kind = hir::ValueKind::Constant;
                            target.constant   = hir::Constant{node.value};
                        } else if constexpr (std::is_same_v<T, ast::StringLiteral>) {
                            target.node       = hir::Literal{node.value};
                            target.type       = literal_type(hir::ScalarType::Str);
                            target.phase      = hir::Phase::Constant;
                            target.value_kind = hir::ValueKind::Constant;
                            target.constant   = hir::Constant{node.value};
                        } else if constexpr (std::is_same_v<T, ast::BoolLiteral>) {
                            target.node       = hir::Literal{node.value};
                            target.type       = literal_type(hir::ScalarType::Bool);
                            target.phase      = hir::Phase::Constant;
                            target.value_kind = hir::ValueKind::Constant;
                            target.constant   = hir::Constant{node.value};
                        } else if constexpr (std::is_same_v<T, ast::NullLiteral>) {
                            target.node       = hir::Literal{hir::NullValue{}};
                            target.phase      = hir::Phase::Constant;
                            target.value_kind = hir::ValueKind::Constant;
                            target.constant   = hir::Constant{hir::NullValue{}};
                        } else if constexpr (std::is_same_v<T, ast::TemporalLiteral>) {
                            target.node       = hir::Literal{node.value};
                            target.type       = literal_type(temporal_type(node.value.kind));
                            target.phase      = hir::Phase::Constant;
                            target.value_kind = hir::ValueKind::Constant;
                            target.constant   = hir::Constant{node.value};
                        } else if constexpr (std::is_same_v<T, ast::Placeholder>) {
                            target.node       = hir::Literal{hir::PlaceholderValue{}};
                            target.phase      = hir::Phase::Constant;
                            target.value_kind = hir::ValueKind::Constant;
                            target.constant   = hir::Constant{hir::PlaceholderValue{}};
                        } else if constexpr (std::is_same_v<T, ast::NameRef>) {
                            target.node = hir::SymbolRef{symbol_for(resolved_.binding(index), node.name.range, node.name.text)};
                        } else if constexpr (std::is_same_v<T, ast::QualifiedRef>) {
                            target.node = hir::SymbolRef{symbol_for(resolved_.binding(index), node.name.range, node.name.text)};
                        } else if constexpr (std::is_same_v<T, ast::Unary>) {
                            target.node = hir::Unary{lower_unary_op(node.op), id<hir::ExprId>(node.operand)};
                        } else if constexpr (std::is_same_v<T, ast::Binary>) {
                            target.node =
                                hir::Binary{lower_binary_op(node.op), id<hir::ExprId>(node.lhs), id<hir::ExprId>(node.rhs)};
                        } else if constexpr (std::is_same_v<T, ast::Call>) {
                            target.node = hir::Call{id<hir::ExprId>(node.callee), lower_arguments(node.arguments)};
                        } else if constexpr (std::is_same_v<T, ast::Index>) {
                            target.node = hir::Index{id<hir::ExprId>(node.target), id<hir::ExprId>(node.index)};
                        } else if constexpr (std::is_same_v<T, ast::Field>) {
                            target.node = hir::Field{id<hir::ExprId>(node.target), std::string{node.field.text}, node.field.range};
                        } else if constexpr (std::is_same_v<T, ast::SequenceLiteral>) {
                            hir::Sequence sequence;
                            for (const ast::SequenceElement &element : node.elements) {
                                sequence.elements.push_back(
                                    hir::SequenceElement{id<hir::ExprId>(element.key), id<hir::ExprId>(element.value)});
                            }
                            target.node = std::move(sequence);
                        } else if constexpr (std::is_same_v<T, ast::TupleLiteral>) {
                            hir::Tuple tuple;
                            for (ast::ExprId element : node.elements) { tuple.elements.push_back(id<hir::ExprId>(element)); }
                            target.node = std::move(tuple);
                        } else if constexpr (std::is_same_v<T, ast::AnonymousFn>) {
                            target.node =
                                hir::Lambda{lambda_symbols_[index], id<hir::TypeId>(node.result), id<hir::ExprId>(node.body)};
                            target.value_kind = hir::ValueKind::Function;
                        } else if constexpr (std::is_same_v<T, ast::If>) {
                            target.node = hir::If{id<hir::ExprId>(node.condition), id<hir::BlockId>(node.then_block),
                                                  id<hir::ExprId>(node.otherwise)};
                        } else if constexpr (std::is_same_v<T, ast::BlockExpr>) {
                            target.node = hir::BlockExpr{id<hir::BlockId>(node.block)};
                        } else if constexpr (std::is_same_v<T, ast::Eval>) {
                            target.node = hir::Eval{id<hir::ExprId>(node.callee), lower_arguments(node.arguments)};
                        } else if constexpr (std::is_same_v<T, ast::Construct>) {
                            target.node = hir::Construct{id<hir::TypeId>(node.type), lower_arguments(node.arguments), node.delta};
                        }
                    },
                    source.node);
                if (const auto *reference = std::get_if<hir::SymbolRef>(&target.node); reference && reference->symbol.valid()) {
                    const hir::Symbol &symbol = result_.symbol(reference->symbol);
                    target.type               = symbol.type;
                    switch (symbol.kind) {
                        case hir::SymbolKind::ConstParameter:
                            target.phase      = hir::Phase::Constant;
                            target.value_kind = hir::ValueKind::Constant;
                            break;
                        case hir::SymbolKind::SignalParameter:
                            target.phase      = hir::Phase::Wiring;
                            target.value_kind = hir::ValueKind::Signal;
                            break;
                        case hir::SymbolKind::ValueParameter:
                            target.phase      = hir::Phase::Runtime;
                            target.value_kind = hir::ValueKind::RuntimeValue;
                            break;
                        case hir::SymbolKind::Function: target.value_kind = hir::ValueKind::Function; break;
                        case hir::SymbolKind::ImportedFunction: target.value_kind = hir::ValueKind::Function; break;
                        case hir::SymbolKind::Operator:
                        case hir::SymbolKind::ImportedOperator: target.value_kind = hir::ValueKind::Operator; break;
                        case hir::SymbolKind::Struct:
                        case hir::SymbolKind::TypeParameter: target.value_kind = hir::ValueKind::Type; break;
                        default: break;
                    }
                }
                result_.exprs[index] = std::move(target);
            }

            void lower_stmt(ast::StmtId index) {
                const ast::Stmt &source = module_.stmt(index);
                hir::Stmt        target;
                target.range = source.range;
                target.owner = id<hir::DeclarationId>(stmt_owners_[index]);
                std::visit(
                    [&](const auto &node) {
                        using T = std::decay_t<decltype(node)>;
                        if constexpr (std::is_same_v<T, ast::LocalDecl>) {
                            target.node = hir::LocalDecl{statement_symbols_[index].front(), id<hir::TypeId>(node.type),
                                                         id<hir::ExprId>(node.init)};
                        } else if constexpr (std::is_same_v<T, ast::StateDecl>) {
                            target.node = hir::StateDecl{statement_symbols_[index].front(), id<hir::TypeId>(node.type),
                                                         id<hir::ExprId>(node.init), node.cache};
                        } else if constexpr (std::is_same_v<T, ast::InjectDecl>) {
                            target.node = hir::InjectDecl{statement_symbols_[index]};
                        } else if constexpr (std::is_same_v<T, ast::LifecycleBlock>) {
                            target.node = hir::LifecycleBlock{node.is_stop, id<hir::BlockId>(node.block)};
                        } else if constexpr (std::is_same_v<T, ast::WhenStmt>) {
                            target.node = hir::WhenStmt{id<hir::ExprId>(node.condition), id<hir::BlockId>(node.block)};
                        } else if constexpr (std::is_same_v<T, ast::ForStmt>) {
                            target.node = hir::ForStmt{statement_symbols_[index], id<hir::ExprId>(node.iterable),
                                                       id<hir::BlockId>(node.block)};
                        } else if constexpr (std::is_same_v<T, ast::AssignStmt>) {
                            target.node =
                                hir::AssignStmt{lower_assign_op(node.op), id<hir::ExprId>(node.place), id<hir::ExprId>(node.value)};
                        } else if constexpr (std::is_same_v<T, ast::ReturnStmt>) {
                            target.node = hir::ReturnStmt{id<hir::ExprId>(node.value)};
                        } else if constexpr (std::is_same_v<T, ast::AssertStmt>) {
                            target.node = hir::AssertStmt{id<hir::ExprId>(node.condition)};
                        } else if constexpr (std::is_same_v<T, ast::ExprStmt>) {
                            target.node = hir::ExprStmt{id<hir::ExprId>(node.expr)};
                        }
                    },
                    source.node);
                result_.stmts[index] = std::move(target);
            }

            void lower_block(ast::BlockId index) {
                const ast::Block &source = module_.block(index);
                hir::Block        target;
                target.range = source.range;
                target.owner = id<hir::DeclarationId>(block_owners_[index]);
                for (ast::StmtId statement : source.statements) { target.statements.push_back(id<hir::StmtId>(statement)); }
                target.tail           = id<hir::ExprId>(source.tail);
                result_.blocks[index] = std::move(target);
            }

            void lower_constraint(ast::ConstraintId index) {
                const ast::Constraint &source = module_.constraint(index);
                hir::Constraint        target;
                target.range = source.range;
                std::visit(
                    [&](const auto &node) {
                        using T = std::decay_t<decltype(node)>;
                        if constexpr (std::is_same_v<T, ast::ConstraintName>) {
                            target.node = hir::ConstraintSymbol{
                                symbol_for(resolved_.constraint_binding(index), node.name.range, node.name.text)};
                        } else if constexpr (std::is_same_v<T, ast::ConstraintType>) {
                            target.node = hir::ConstraintType{id<hir::TypeId>(node.type)};
                        } else if constexpr (std::is_same_v<T, ast::ConstraintValue>) {
                            target.node = hir::ConstraintValue{id<hir::ExprId>(node.value)};
                        } else if constexpr (std::is_same_v<T, ast::ConstraintSet>) {
                            hir::ConstraintSet set;
                            for (ast::ConstraintId element : node.elements) {
                                set.elements.push_back(id<hir::ConstraintId>(element));
                            }
                            target.node = std::move(set);
                        } else if constexpr (std::is_same_v<T, ast::ConstraintCall>) {
                            hir::ConstraintCall call;
                            call.function = symbol_for(resolved_.constraint_binding(index), node.name.range, node.name.text);
                            for (ast::ConstraintId argument : node.arguments) {
                                call.arguments.push_back(id<hir::ConstraintId>(argument));
                            }
                            target.node = std::move(call);
                        } else if constexpr (std::is_same_v<T, ast::ConstraintEach>) {
                            target.node = hir::ConstraintEach{
                                symbol_for(resolved_.constraint_binding(index), node.binding.range, node.binding.text),
                                id<hir::ConstraintId>(node.source), id<hir::ConstraintId>(node.body)};
                        } else if constexpr (std::is_same_v<T, ast::OperatorRequirement>) {
                            hir::OperatorRequirement requirement;
                            requirement.op = symbol_for(resolved_.constraint_binding(index), node.name.range, node.name.text);
                            for (ast::ConstraintId argument : node.arguments) {
                                requirement.arguments.push_back(id<hir::ConstraintId>(argument));
                            }
                            requirement.result = id<hir::TypeId>(node.result);
                            target.node        = std::move(requirement);
                        } else if constexpr (std::is_same_v<T, ast::ConstraintRelation>) {
                            target.node =
                                hir::ConstraintRelation{lower_constraint_relation_op(node.op), id<hir::ConstraintId>(node.lhs),
                                                        id<hir::ConstraintId>(node.rhs), std::string{node.category.text}};
                        } else if constexpr (std::is_same_v<T, ast::ConstraintNot>) {
                            target.node = hir::ConstraintNot{id<hir::ConstraintId>(node.operand)};
                        } else if constexpr (std::is_same_v<T, ast::ConstraintLogic>) {
                            target.node = hir::ConstraintLogic{lower_constraint_logic_op(node.op), id<hir::ConstraintId>(node.lhs),
                                                               id<hir::ConstraintId>(node.rhs)};
                        }
                    },
                    source.node);
                result_.constraints[index] = std::move(target);
            }

            [[nodiscard]] std::vector<hir::GenericParameter>
            lower_generics(ast::DeclId owner, const std::vector<ast::GenericParameter> &generics) const {
                std::vector<hir::GenericParameter> result;
                result.reserve(generics.size());
                for (std::size_t index = 0; index < generics.size(); ++index) {
                    result.push_back(hir::GenericParameter{generic_symbols_[owner][index], generics[index].is_const,
                                                           id<hir::TypeId>(generics[index].type), generics[index].is_pack});
                }
                return result;
            }

            [[nodiscard]] hir::Signature lower_signature(ast::DeclId owner, const ast::Signature &signature) const {
                hir::Signature result;
                result.parameters.reserve(signature.parameters.size());
                for (std::size_t index = 0; index < signature.parameters.size(); ++index) {
                    const ast::Parameter &parameter = signature.parameters[index];
                    const auto            pack = parameter.pack == ast::ParameterPack::Positional ? hir::ParameterPack::Positional
                                                 : parameter.pack == ast::ParameterPack::Keyword  ? hir::ParameterPack::Keyword
                                                                                                  : hir::ParameterPack::None;
                    result.parameters.push_back(
                        hir::Parameter{parameter_symbols_[owner][index], parameter.is_const, id<hir::TypeId>(parameter.type),
                                       id<hir::ExprId>(parameter.default_value), pack,
                                       hir::PackCardinality{parameter.cardinality.minimum, parameter.cardinality.maximum}});
                }
                result.result = id<hir::TypeId>(signature.result);
                return result;
            }

            [[nodiscard]] syntax::SourceRange field_range(ast::DeclId origin, std::string_view name) const {
                if (origin == ast::no_node) { return {}; }
                const auto *structure = std::get_if<ast::StructDecl>(&module_.decl(origin).node);
                if (!structure) { return module_.decl(origin).range; }
                for (const ast::StructMember &member : structure->members) {
                    if (const auto *field = std::get_if<ast::StructField>(&member); field && field->name.text == name) {
                        return field->name.range;
                    }
                }
                return structure->name.range;
            }

            void lower_declaration(ast::DeclId index) {
                const ast::Decl &source = module_.decl(index);
                hir::Declaration target;
                target.id        = id<hir::DeclarationId>(index);
                target.symbol    = declaration_symbols_[index];
                target.range     = source.range;
                target.test_only = source.test_only;
                std::visit(
                    [&](const auto &node) {
                        using T = std::decay_t<decltype(node)>;
                        if constexpr (std::is_same_v<T, ast::ModuleDecl>) {
                            target.node = hir::ModuleDecl{};
                        } else if constexpr (std::is_same_v<T, ast::UseDecl>) {
                            hir::UseDecl use;
                            use.module = join_path(node.path);
                            use.alias  = std::string{node.alias.text};
                            for (const ast::Name &name : node.names) { use.names.emplace_back(name.text); }
                            target.node = std::move(use);
                        } else if constexpr (std::is_same_v<T, ast::CppIncludeDecl>) {
                            if (std::ranges::find(result_.cpp_includes, node.spelling) == result_.cpp_includes.end()) {
                                result_.cpp_includes.push_back(node.spelling);
                            }
                            target.node = hir::CppIncludeDecl{node.spelling};
                        } else if constexpr (std::is_same_v<T, ast::StructDecl>) {
                            hir::StructDecl structure;
                            structure.exported = node.exported;
                            structure.abstract = node.abstract;
                            structure.generics = lower_generics(index, node.generics);
                            for (ast::TypeId parent : node.parents) { structure.parents.push_back(id<hir::TypeId>(parent)); }
                            structure.requirements = id<hir::ConstraintId>(node.requirements);
                            if (index < resolved_.struct_info.size()) {
                                for (const semantics::StructField &field : resolved_.structure(index).fields) {
                                    structure.fields.push_back(
                                        // A field inherited from another module's struct has no
                                        // An inherited field keeps the struct that declares
                                        // it. When that is another module's struct there is
                                        // no declaration here to point at, so it travels as
                                        // an identity and its type comes from that owner's
                                        // layout (ADR 0013).
                                        hir::StructField{field.name,
                                                         imported_field_type(field, field_range(field.origin.decl, field.name)),
                                                         id<hir::ExprId>(field.default_value),
                                                         id<hir::DeclarationId>(field.origin.decl),
                                                         field.origin.is_imported()
                                                             ? resolved_.imported_structs[field.origin.imported].identity
                                                             : std::string{},
                                                         field.optional, field_range(field.origin.decl, field.name),
                                                         field.recursive});
                                }
                            }
                            target.node = std::move(structure);
                        } else if constexpr (std::is_same_v<T, ast::OperatorDecl>) {
                            hir::OperatorDecl operation{lower_generics(index, node.generics),
                                                        lower_signature(index, node.signature),
                                                        id<hir::ConstraintId>(node.requirements)};
                            for (const ast::OperatorProperties &clause : node.properties) {
                                hir::OperatorProperties properties;
                                properties.range = clause.range;
                                for (ast::TypeId domain : clause.domain) { properties.domain.push_back(id<hir::TypeId>(domain)); }
                                for (const ast::OperatorProperty &property : clause.entries) {
                                    properties.entries.push_back(
                                        {std::string{property.name.text}, id<hir::ExprId>(property.value), property.name.range});
                                }
                                operation.properties.push_back(std::move(properties));
                            }
                            target.node = std::move(operation);
                        } else if constexpr (std::is_same_v<T, ast::InstantiateDecl>) {
                            hir::InstantiateDecl                   instantiate;
                            const std::vector<semantics::Binding> &bindings = resolved_.instantiation_binding(index);
                            for (std::size_t entry_index = 0; entry_index < node.entries.size(); ++entry_index) {
                                const ast::Instantiation &source_entry = node.entries[entry_index];
                                hir::Instantiation        entry;
                                entry.range = source_entry.range;
                                if (entry_index < bindings.size() &&
                                    bindings[entry_index].kind != semantics::BindingKind::Unbound) {
                                    entry.operator_contract =
                                        symbol_for(bindings[entry_index], source_entry.name.range, source_entry.name.text);
                                }
                                for (const ast::GenericArgument &argument : source_entry.arguments) {
                                    hir::TypeArgument lowered;
                                    lowered.range    = argument.range;
                                    lowered.retained = argument.retained;
                                    if (argument.retained) {
                                        lowered.kind = hir::TypeArgumentKind::Type;
                                    } else if (argument.type != ast::no_node) {
                                        lowered.kind = hir::TypeArgumentKind::Type;
                                        lowered.type = id<hir::TypeId>(argument.type);
                                    } else {
                                        lowered.kind  = hir::TypeArgumentKind::Value;
                                        lowered.value = id<hir::ExprId>(argument.value);
                                    }
                                    entry.arguments.push_back(std::move(lowered));
                                }
                                instantiate.entries.push_back(std::move(entry));
                            }
                            target.node = std::move(instantiate);
                        } else if constexpr (std::is_same_v<T, ast::FunctionDecl>) {
                            hir::FunctionDecl function;
                            function.is_const   = node.is_const;
                            function.visibility = lower_visibility(node.visibility);
                            function.kind       = lower_function_kind(resolved_.kind(index));
                            if (function.visibility == hir::Visibility::Implementation) {
                                const semantics::Binding &binding = resolved_.implementation_binding(index);
                                if (binding.kind != semantics::BindingKind::Unbound) {
                                    function.operator_contract = symbol_for(binding, node.name.range, node.name.text);
                                }
                            }
                            function.generics     = lower_generics(index, node.generics);
                            function.signature    = lower_signature(index, node.signature);
                            function.requirements = id<hir::ConstraintId>(node.requirements);
                            function.concise_body = id<hir::ExprId>(node.concise_body);
                            function.block_body   = id<hir::BlockId>(node.block_body);
                            target.node           = std::move(function);
                        } else if constexpr (std::is_same_v<T, ast::NativeFunctionDecl>) {
                            hir::NativeFunction function;
                            function.symbol             = declaration_symbols_[index];
                            function.identity           = result_.path + "::" + std::string{node.name.text};
                            function.candidate_identity = result_.symbol(function.symbol).canonical_name;
                            for (std::size_t family = 0; family < resolved_.native_families.size(); ++family) {
                                if (std::ranges::find(resolved_.native_families[family], index) !=
                                    resolved_.native_families[family].end()) {
                                    function.family = native_family_symbols_[family];
                                    break;
                                }
                            }
                            function.generics              = lower_generics(index, node.generics);
                            const hir::Signature signature = lower_signature(index, node.signature);
                            for (std::size_t parameter = 0; parameter < signature.parameters.size(); ++parameter) {
                                const hir::Parameter &item       = signature.parameters[parameter];
                                const ast::TypeKind   kind       = module_.type(node.signature.parameters[parameter].type).kind;
                                const bool            input_view = kind == ast::TypeKind::Signal || kind == ast::TypeKind::List ||
                                                                   kind == ast::TypeKind::Set || kind == ast::TypeKind::Map ||
                                                                   kind == ast::TypeKind::Rolling;
                                function.parameters.push_back(hir::NativeParameter{
                                    std::string{node.signature.parameters[parameter].name.text}, item.type, item.is_const,
                                    input_view && !item.is_const && !node.is_const ? hir::NativeParameterAccess::InputView
                                                                                   : hir::NativeParameterAccess::Value});
                            }
                            // A native without `->` returns void, as an imported void native does.
                            function.result = signature.result.valid() ? signature.result : void_type();
                            // A value function has no live-input dependency, so it
                            // is available in every node hook; a view function needs
                            // the inputs and is evaluation-only.
                            const bool views = std::ranges::any_of(function.parameters, [](const hir::NativeParameter &parameter) {
                                return parameter.access == hir::NativeParameterAccess::InputView;
                            });
                            function.phases = views ? std::vector{hir::NativePhase::Evaluation}
                                                    : std::vector{hir::NativePhase::Start, hir::NativePhase::Evaluation,
                                                                  hir::NativePhase::Stop};
                            function.throws = node.throws;
                            function.is_const = node.is_const;
                            function.source_defined = true;
                            function.cpp_parameters = node.implementation.parameters;
                            function.cpp_body       = node.implementation.body;
                            function.range          = source.range;
                            result_.native_functions.push_back(std::move(function));
                            target.node = hir::NativeSourceDecl{};
                        } else if constexpr (std::is_same_v<T, ast::TestDecl>) {
                            target.node = hir::TestDecl{id<hir::BlockId>(node.block)};
                        }
                    },
                    source.node);
                result_.declarations[index] = std::move(target);
            }

            const ast::Module                               &module_;
            const semantics::ResolvedModule                 &resolved_;
            syntax::DiagnosticSink                          &diagnostics_;
            hir::Module                                      result_{};
            std::vector<hir::SymbolId>                       declaration_symbols_{};
            std::vector<hir::SymbolId>                       native_family_symbols_{};
            std::vector<std::vector<hir::SymbolId>>          generic_symbols_{};
            std::vector<std::vector<hir::SymbolId>>          parameter_symbols_{};
            std::vector<std::vector<hir::SymbolId>>          statement_symbols_{};
            std::vector<std::vector<hir::SymbolId>>          lambda_symbols_{};
            std::vector<hir::SymbolId>                       constraint_symbols_{};
            std::vector<ast::DeclId>                         type_owners_{};
            std::vector<ast::DeclId>                         expr_owners_{};
            std::vector<ast::DeclId>                         stmt_owners_{};
            std::vector<ast::DeclId>                         block_owners_{};
            std::unordered_map<std::string, hir::SymbolId>   global_symbols_{};
            std::unordered_map<std::string, hir::SymbolId>   external_symbols_{};
            std::unordered_map<std::uint32_t, hir::SymbolId> imported_function_symbols_{};
            /// Imported struct identities already described OR in progress
            /// (see `lower_imported_struct`).
            std::unordered_set<std::string>                  described_imported_structs_{};
            std::unordered_map<std::uint8_t, hir::TypeId>    literal_types_{};
            hir::TypeId                                      void_type_{};
        };
    }  // namespace

    hir::Module lower_to_hir(const ast::Module &module, const semantics::ResolvedModule &resolved,
                             syntax::DiagnosticSink &diagnostics) {
        return Lowerer{module, resolved, diagnostics}.run();
    }
}  // namespace hgl::ir
