#include "ir/lower.h"

#include <algorithm>
#include <optional>
#include <string_view>
#include <unordered_map>
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
                generic_symbols_.resize(module_.decls.size());
                parameter_symbols_.resize(module_.decls.size());
                statement_symbols_.resize(module_.stmts.size());
                lambda_symbols_.resize(module_.exprs.size());
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
                            mark_expr(node.condition, owner);
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
                        } else if constexpr (std::is_same_v<T, ast::FunctionDecl>) {
                            mark_generics(node.generics, declaration);
                            mark_signature(node.signature, declaration);
                            mark_constraint(node.requirements, declaration);
                            mark_expr(node.concise_body, declaration);
                            mark_block(node.block_body, declaration);
                        } else if constexpr (std::is_same_v<T, ast::TestDecl>) {
                            mark_block(node.block, declaration);
                        }
                    },
                    module_.decl(declaration).node);
            }

            void declare_symbols() {
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
                                declaration_symbols_[declaration] =
                                    add_symbol(hir::SymbolKind::Function, node.name.text, node.name.range, declaration);
                                global_symbols_.emplace(std::string{node.name.text}, declaration_symbols_[declaration]);
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
                                statement_symbols_[statement].push_back(
                                    add_symbol(hir::SymbolKind::State, node.name.text, node.name.range, owner));
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
                parameter_symbols_[owner].reserve(parameters.size());
                for (std::size_t index = 0; index < parameters.size(); ++index) {
                    const ast::Parameter &parameter = parameters[index];
                    parameter_symbols_[owner].push_back(
                        add_symbol(parameter.is_const ? hir::SymbolKind::ConstParameter : hir::SymbolKind::SignalParameter,
                                   parameter.name.text, parameter.name.range, owner, static_cast<std::uint32_t>(index)));
                }
            }

            void assign_symbol_types() {
                for (ast::DeclId declaration = 0; declaration < module_.decls.size(); ++declaration) {
                    std::visit(
                        [&](const auto &node) {
                            using T = std::decay_t<decltype(node)>;
                            if constexpr (std::is_same_v<T, ast::StructDecl> || std::is_same_v<T, ast::OperatorDecl> ||
                                          std::is_same_v<T, ast::FunctionDecl>) {
                                for (std::size_t index = 0; index < node.generics.size(); ++index) {
                                    result_.symbols[generic_symbols_[declaration][index].value].type =
                                        id<hir::TypeId>(node.generics[index].type);
                                }
                            }
                            if constexpr (std::is_same_v<T, ast::OperatorDecl> || std::is_same_v<T, ast::FunctionDecl>) {
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
                    case BindingKind::Struct:
                    case BindingKind::Function:
                    case BindingKind::LocalOperator:
                    case BindingKind::Test:
                        if (binding.decl < declaration_symbols_.size()) { return declaration_symbols_[binding.decl]; }
                        break;
                    case BindingKind::Operator:
                        return external_symbol(hir::SymbolKind::ImportedOperator, spelling, binding.registry_name,
                                               binding.operator_identity, range);
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
                    } else if (const auto *node = std::get_if<ast::OperatorDecl>(&declaration)) {
                        generics = &node->generics;
                    } else if (const auto *node = std::get_if<ast::FunctionDecl>(&declaration)) {
                        generics = &node->generics;
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
                        case hir::SymbolKind::Function: target.value_kind = hir::ValueKind::Function; break;
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
                                                         id<hir::ExprId>(node.init)};
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
                                                           id<hir::TypeId>(generics[index].type)});
                }
                return result;
            }

            [[nodiscard]] hir::Signature lower_signature(ast::DeclId owner, const ast::Signature &signature) const {
                hir::Signature result;
                result.parameters.reserve(signature.parameters.size());
                for (std::size_t index = 0; index < signature.parameters.size(); ++index) {
                    const ast::Parameter &parameter = signature.parameters[index];
                    result.parameters.push_back(hir::Parameter{parameter_symbols_[owner][index], parameter.is_const,
                                                               id<hir::TypeId>(parameter.type),
                                                               id<hir::ExprId>(parameter.default_value)});
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
                target.id     = id<hir::DeclarationId>(index);
                target.symbol = declaration_symbols_[index];
                target.range  = source.range;
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
                                        hir::StructField{field.name, id<hir::TypeId>(field.type),
                                                         id<hir::ExprId>(field.default_value), id<hir::DeclarationId>(field.origin),
                                                         field.optional, field_range(field.origin, field.name)});
                                }
                            }
                            target.node = std::move(structure);
                        } else if constexpr (std::is_same_v<T, ast::OperatorDecl>) {
                            target.node =
                                hir::OperatorDecl{lower_generics(index, node.generics), lower_signature(index, node.signature),
                                                  id<hir::ConstraintId>(node.requirements)};
                        } else if constexpr (std::is_same_v<T, ast::FunctionDecl>) {
                            hir::FunctionDecl function;
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
                        } else if constexpr (std::is_same_v<T, ast::TestDecl>) {
                            target.node = hir::TestDecl{id<hir::BlockId>(node.block)};
                        }
                    },
                    source.node);
                result_.declarations[index] = std::move(target);
            }

            const ast::Module                             &module_;
            const semantics::ResolvedModule               &resolved_;
            syntax::DiagnosticSink                        &diagnostics_;
            hir::Module                                    result_{};
            std::vector<hir::SymbolId>                     declaration_symbols_{};
            std::vector<std::vector<hir::SymbolId>>        generic_symbols_{};
            std::vector<std::vector<hir::SymbolId>>        parameter_symbols_{};
            std::vector<std::vector<hir::SymbolId>>        statement_symbols_{};
            std::vector<std::vector<hir::SymbolId>>        lambda_symbols_{};
            std::vector<ast::DeclId>                       type_owners_{};
            std::vector<ast::DeclId>                       expr_owners_{};
            std::vector<ast::DeclId>                       stmt_owners_{};
            std::vector<ast::DeclId>                       block_owners_{};
            std::unordered_map<std::string, hir::SymbolId> global_symbols_{};
            std::unordered_map<std::string, hir::SymbolId> external_symbols_{};
            std::unordered_map<std::uint8_t, hir::TypeId>  literal_types_{};
        };
    }  // namespace

    hir::Module lower_to_hir(const ast::Module &module, const semantics::ResolvedModule &resolved,
                             syntax::DiagnosticSink &diagnostics) {
        return Lowerer{module, resolved, diagnostics}.run();
    }
}  // namespace hgl::ir
