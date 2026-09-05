#include "syntax/ast_projection.h"

#include <algorithm>
#include <optional>
#include <stdexcept>
#include <string>
#include <utility>
#include <variant>
#include <vector>

namespace hgl::syntax
{
    namespace
    {
        class AstProjector
        {
          public:
            AstProjector(const SyntaxTree &tree, const LexResult &lexed, DiagnosticSink &diagnostics)
                : tree_{tree}, lexed_{lexed}, diagnostics_{diagnostics} {
                module_.comments = lexed.comments;
            }

            [[nodiscard]] ast::Module run() {
                require(tree_.has_root(), "syntax tree has no root");
                require(node(tree_.root).kind == SyntaxKind::Module, "syntax root is not a module");
                for (const SyntaxNodeId child : child_nodes(tree_.root)) {
                    if (node(child).kind == SyntaxKind::ModuleDecl) {
                        add_declaration(project_module_decl(child));
                    } else if (node(child).kind == SyntaxKind::DeclarationLine) {
                        if (const auto declaration = find_child(child, SyntaxKind::Declaration)) {
                            add_declaration(project_declaration(*declaration));
                        }
                    }
                }
                return std::move(module_);
            }

          private:
            [[noreturn]] static void malformed(std::string message) {
                throw std::logic_error{"malformed HGL syntax tree: " + std::move(message)};
            }

            static void require(bool condition, std::string message) {
                if (!condition) { malformed(std::move(message)); }
            }

            [[nodiscard]] const SyntaxNode &node(SyntaxNodeId id) const {
                require(id < tree_.nodes.size(), "node index is out of range");
                return tree_.nodes[id];
            }

            [[nodiscard]] const SyntaxToken &syntax_token(SyntaxTokenId id) const {
                require(id < tree_.tokens.size(), "token index is out of range");
                return tree_.tokens[id];
            }

            [[nodiscard]] const Token &source_token(SyntaxTokenId id) const {
                const SyntaxToken &syntax = syntax_token(id);
                require(syntax.source_token_index < lexed_.tokens.size(), "source token index is out of range");
                return lexed_.tokens[syntax.source_token_index];
            }

            [[nodiscard]] std::vector<SyntaxNodeId> child_nodes(SyntaxNodeId id) const {
                std::vector<SyntaxNodeId> result;
                for (const SyntaxChild child : node(id).children) {
                    if (child.kind == SyntaxChildKind::Node) { result.push_back(child.index); }
                }
                return result;
            }

            [[nodiscard]] std::vector<SyntaxNodeId> child_nodes(SyntaxNodeId id, SyntaxKind kind) const {
                std::vector<SyntaxNodeId> result;
                for (const SyntaxNodeId child : child_nodes(id)) {
                    if (node(child).kind == kind) { result.push_back(child); }
                }
                return result;
            }

            [[nodiscard]] std::vector<SyntaxTokenId> child_tokens(SyntaxNodeId id) const {
                std::vector<SyntaxTokenId> result;
                for (const SyntaxChild child : node(id).children) {
                    if (child.kind == SyntaxChildKind::Token) { result.push_back(child.index); }
                }
                return result;
            }

            [[nodiscard]] std::vector<SyntaxTokenId> child_tokens(SyntaxNodeId id, TokenKind kind) const {
                std::vector<SyntaxTokenId> result;
                for (const SyntaxTokenId child : child_tokens(id)) {
                    if (source_token(child).kind == kind) { result.push_back(child); }
                }
                return result;
            }

            void append_descendant_tokens(SyntaxNodeId id, std::vector<SyntaxTokenId> &result) const {
                for (const SyntaxChild child : node(id).children) {
                    if (child.kind == SyntaxChildKind::Token) {
                        if (source_token(child.index).kind != TokenKind::Newline) { result.push_back(child.index); }
                    } else {
                        append_descendant_tokens(child.index, result);
                    }
                }
            }

            [[nodiscard]] std::vector<SyntaxTokenId> descendant_tokens(SyntaxNodeId id) const {
                std::vector<SyntaxTokenId> result;
                append_descendant_tokens(id, result);
                return result;
            }

            [[nodiscard]] std::optional<SyntaxNodeId> find_child(SyntaxNodeId id, SyntaxKind kind) const {
                for (const SyntaxNodeId child : child_nodes(id)) {
                    if (node(child).kind == kind) { return child; }
                }
                return std::nullopt;
            }

            [[nodiscard]] SyntaxNodeId only_child(SyntaxNodeId id, SyntaxKind kind) const {
                const std::vector<SyntaxNodeId> matches = child_nodes(id, kind);
                require(matches.size() == 1, std::string{syntax_kind_name(node(id).kind)} + " does not have exactly one " +
                                                 std::string{syntax_kind_name(kind)} + " child");
                return matches.front();
            }

            [[nodiscard]] SyntaxNodeId semantic_child(SyntaxNodeId id) const {
                for (const SyntaxNodeId child : child_nodes(id)) {
                    switch (node(child).kind) {
                        case SyntaxKind::Newlines:
                        case SyntaxKind::LineEnd:
                        case SyntaxKind::CommaSeparator:
                        case SyntaxKind::ContinuedOperator: break;
                        default: return child;
                    }
                }
                malformed(std::string{syntax_kind_name(node(id).kind)} + " has no semantic child");
            }

            [[nodiscard]] ast::Name name(SyntaxTokenId id) const {
                const Token &token = source_token(id);
                require(token.kind == TokenKind::Identifier || is_keyword(token.kind), "name token is not an identifier");
                return ast::Name{token.text, token.range};
            }

            [[nodiscard]] std::vector<ast::Name> direct_names(SyntaxNodeId id, std::string_view role = {}) {
                std::vector<ast::Name> names;
                const auto             append_name = [&](SyntaxNodeId name_node) {
                    const std::vector<SyntaxTokenId> tokens = child_tokens(name_node);
                    require(tokens.size() == 1, "name production does not contain exactly one token");
                    const Token &token = source_token(tokens.front());
                    if (!role.empty() && is_keyword(token.kind)) {
                        diagnostics_.report(Category::Parse, token.range,
                                            "'" + std::string{token.text} + "' is a reserved word and cannot be used as " +
                                                std::string{role});
                    }
                    names.push_back(name(tokens.front()));
                };
                if (node(id).kind == SyntaxKind::Name) {
                    append_name(id);
                } else {
                    for (const SyntaxNodeId child : child_nodes(id, SyntaxKind::Name)) { append_name(child); }
                }
                return names;
            }

            [[nodiscard]] static std::optional<ast::ScalarType> scalar_type(TokenKind kind) noexcept {
                switch (kind) {
                    case TokenKind::KwBool: return ast::ScalarType::Bool;
                    case TokenKind::KwI64: return ast::ScalarType::I64;
                    case TokenKind::KwF64: return ast::ScalarType::F64;
                    case TokenKind::KwStr: return ast::ScalarType::Str;
                    case TokenKind::KwDate: return ast::ScalarType::Date;
                    case TokenKind::KwTime: return ast::ScalarType::Time;
                    case TokenKind::KwDateTime: return ast::ScalarType::DateTime;
                    case TokenKind::KwDuration: return ast::ScalarType::Duration;
                    case TokenKind::KwCivilDateTime: return ast::ScalarType::CivilDateTime;
                    case TokenKind::KwZonedDateTime: return ast::ScalarType::ZonedDateTime;
                    case TokenKind::KwZonedTime: return ast::ScalarType::ZonedTime;
                    case TokenKind::KwTimeZone: return ast::ScalarType::TimeZone;
                    default: return std::nullopt;
                }
            }

            [[nodiscard]] static ast::BinaryOp binary_op(TokenKind kind) {
                switch (kind) {
                    case TokenKind::Star: return ast::BinaryOp::Mul;
                    case TokenKind::Slash: return ast::BinaryOp::Div;
                    case TokenKind::Percent: return ast::BinaryOp::Rem;
                    case TokenKind::Plus: return ast::BinaryOp::Add;
                    case TokenKind::Minus: return ast::BinaryOp::Sub;
                    case TokenKind::Less: return ast::BinaryOp::Less;
                    case TokenKind::LessEqual: return ast::BinaryOp::LessEqual;
                    case TokenKind::Greater: return ast::BinaryOp::Greater;
                    case TokenKind::GreaterEqual: return ast::BinaryOp::GreaterEqual;
                    case TokenKind::EqualEqual: return ast::BinaryOp::Equal;
                    case TokenKind::NotEqual: return ast::BinaryOp::NotEqual;
                    case TokenKind::AndAnd: return ast::BinaryOp::And;
                    case TokenKind::OrOr: return ast::BinaryOp::Or;
                    default: malformed("unknown binary operator");
                }
            }

            [[nodiscard]] static ast::AssignOp assign_op(TokenKind kind) {
                switch (kind) {
                    case TokenKind::Assign: return ast::AssignOp::Assign;
                    case TokenKind::PlusAssign: return ast::AssignOp::Add;
                    case TokenKind::MinusAssign: return ast::AssignOp::Sub;
                    case TokenKind::StarAssign: return ast::AssignOp::Mul;
                    case TokenKind::SlashAssign: return ast::AssignOp::Div;
                    default: malformed("unknown assignment operator");
                }
            }

            [[nodiscard]] ast::TypeId project_type(SyntaxNodeId id, bool value_position) {
                const SyntaxNode &syntax = node(id);
                if (syntax.kind == SyntaxKind::Type) {
                    const std::vector<SyntaxTokenId> tokens = child_tokens(id);
                    if (!tokens.empty()) {
                        require(tokens.size() == 1, "scalar type contains multiple tokens");
                        const auto scalar = scalar_type(source_token(tokens.front()).kind);
                        require(scalar.has_value(), "type token is not scalar");
                        ast::Type type;
                        type.kind           = ast::TypeKind::Scalar;
                        type.range          = syntax.range;
                        type.scalar         = *scalar;
                        type.value_position = value_position;
                        return module_.add(std::move(type));
                    }
                    return project_type(semantic_child(id), value_position);
                }

                ast::Type type;
                type.range          = syntax.range;
                type.value_position = value_position;
                switch (syntax.kind) {
                    case SyntaxKind::NamedType:
                        {
                            type.kind                              = ast::TypeKind::Named;
                            const SyntaxNodeId           qualified = only_child(id, SyntaxKind::QualifiedName);
                            const std::vector<ast::Name> names     = direct_names(qualified);
                            require(names.size() == 1 || names.size() == 2, "named type has an invalid qualified name");
                            if (names.size() == 2) {
                                type.qualifier = names[0];
                                type.name      = names[1];
                            } else {
                                type.name = names[0];
                            }
                            if (const auto arguments = find_child(id, SyntaxKind::GenericArguments)) {
                                type.arguments = project_generic_arguments(*arguments);
                            }
                            break;
                        }
                    case SyntaxKind::TupleType:
                        type.kind = ast::TypeKind::Tuple;
                        for (const SyntaxNodeId child : child_nodes(id, SyntaxKind::Type)) {
                            type.children.push_back(project_type(child, value_position));
                        }
                        if (type.children.empty()) {
                            diagnostics_.report(Category::Parse, syntax.range, "a tuple type has at least one element");
                        }
                        break;
                    case SyntaxKind::ListType:
                        {
                            type.kind = ast::TypeKind::List;
                            type.children.push_back(project_type(only_child(id, SyntaxKind::Type), value_position));
                            const std::vector<SyntaxTokenId> tokens = descendant_tokens(id);
                            type.unbounded                          = std::ranges::any_of(tokens, [&](SyntaxTokenId token) {
                                return source_token(token).kind == TokenKind::Identifier && source_token(token).text == "unbounded";
                            });
                            if (const auto size = find_child(id, SyntaxKind::SizeExpression)) {
                                type.size = project_expression(*size);
                            }
                            break;
                        }
                    case SyntaxKind::SetType:
                        type.kind = ast::TypeKind::Set;
                        type.children.push_back(project_type(only_child(id, SyntaxKind::Type), true));
                        break;
                    case SyntaxKind::MapType:
                        {
                            type.kind                             = ast::TypeKind::Map;
                            const std::vector<SyntaxNodeId> types = child_nodes(id, SyntaxKind::Type);
                            require(types.size() == 2, "map type does not contain two types");
                            type.children.push_back(project_type(types[0], true));
                            type.children.push_back(project_type(types[1], value_position));
                            break;
                        }
                    case SyntaxKind::RollingType:
                        {
                            type.kind = ast::TypeKind::Rolling;
                            type.children.push_back(project_type(only_child(id, SyntaxKind::Type), true));
                            const std::vector<SyntaxNodeId> sizes = child_nodes(id, SyntaxKind::SizeExpression);
                            require(!sizes.empty() && sizes.size() <= 2, "rolling type has an invalid size list");
                            type.size = project_expression(sizes[0]);
                            if (sizes.size() == 2) { type.min_size = project_expression(sizes[1]); }
                            break;
                        }
                    case SyntaxKind::AtomicType:
                        type.kind = ast::TypeKind::Atomic;
                        type.children.push_back(project_type(only_child(id, SyntaxKind::Type), true));
                        break;
                    default: malformed("expected a type production");
                }
                return module_.add(std::move(type));
            }

            [[nodiscard]] std::vector<ast::GenericArgument> project_generic_arguments(SyntaxNodeId id) {
                std::vector<ast::GenericArgument> result;
                for (const SyntaxNodeId child : child_nodes(id, SyntaxKind::GenericArgument)) {
                    ast::GenericArgument argument;
                    argument.range                        = node(child).range;
                    const std::vector<SyntaxNodeId> nodes = child_nodes(child);
                    if (!nodes.empty()) {
                        const SyntaxNodeId value = semantic_child(child);
                        if (node(value).kind == SyntaxKind::SizeExpression) {
                            const std::vector<SyntaxTokenId> tokens = descendant_tokens(value);
                            if (tokens.size() == 1 && source_token(tokens.front()).kind == TokenKind::Identifier) {
                                argument.name = name(tokens.front());
                            } else {
                                argument.value = project_expression(value);
                            }
                        } else if (node(value).kind == SyntaxKind::Name) {
                            argument.name = direct_names(value).front();
                        } else {
                            argument.type = project_type(value, true);
                        }
                    } else {
                        const std::vector<SyntaxTokenId> tokens = child_tokens(child);
                        if (tokens.size() == 1) {
                            if (const auto scalar = scalar_type(source_token(tokens.front()).kind)) {
                                ast::Type type;
                                type.kind           = ast::TypeKind::Scalar;
                                type.range          = node(child).range;
                                type.scalar         = *scalar;
                                type.value_position = true;
                                argument.type       = module_.add(std::move(type));
                                result.push_back(std::move(argument));
                                continue;
                            }
                        }
                        const std::vector<ast::Name> names = direct_names(child);
                        require(names.size() == 1, "generic name argument has an invalid shape");
                        argument.name = names.front();
                    }
                    result.push_back(std::move(argument));
                }
                return result;
            }

            [[nodiscard]] ast::ExprId project_expression(SyntaxNodeId id) {
                switch (node(id).kind) {
                    case SyntaxKind::Expression:
                    case SyntaxKind::AndExpression:
                    case SyntaxKind::EqualityExpression:
                    case SyntaxKind::ComparisonExpression:
                    case SyntaxKind::SumExpression:
                    case SyntaxKind::ProductExpression: return project_binary_expression(id);
                    case SyntaxKind::SizeExpression:
                    case SyntaxKind::TupleElement: return project_expression(semantic_child(id));
                    case SyntaxKind::PrimaryExpression:
                        if (!child_nodes(id).empty()) { return project_expression(semantic_child(id)); }
                        break;
                    case SyntaxKind::UnaryExpression: return project_unary_expression(id);
                    case SyntaxKind::PostfixExpression: return project_postfix_expression(id);
                    case SyntaxKind::QualifiedName: return project_name_reference(id);
                    case SyntaxKind::TupleOrGroup: return project_tuple(id);
                    case SyntaxKind::SequenceLiteral: return project_sequence(id);
                    case SyntaxKind::AnonymousFunction: return project_anonymous_function(id);
                    case SyntaxKind::IfExpression: return project_if(id);
                    case SyntaxKind::EvalExpression: return project_eval(id);
                    case SyntaxKind::ExplicitConstruct: return project_construct(id);
                    case SyntaxKind::Block:
                        {
                            const ast::BlockId block = project_block(id);
                            return module_.add(ast::Expr{module_.block(block).range, ast::BlockExpr{block}});
                        }
                    default: break;
                }

                const std::vector<SyntaxTokenId> tokens = child_tokens(id);
                require(tokens.size() == 1, "expression leaf does not contain exactly one token");
                return project_literal(tokens.front());
            }

            [[nodiscard]] ast::ExprId project_binary_expression(SyntaxNodeId id) {
                ast::ExprId              result = ast::no_node;
                std::optional<TokenKind> operation;
                for (const SyntaxChild child : node(id).children) {
                    if (child.kind != SyntaxChildKind::Node) { continue; }
                    if (node(child.index).kind == SyntaxKind::ContinuedOperator) {
                        const std::vector<SyntaxTokenId> tokens = child_tokens(child.index);
                        const auto                       found  = std::ranges::find_if(
                            tokens, [&](SyntaxTokenId token) { return source_token(token).kind != TokenKind::Newline; });
                        require(found != tokens.end(), "continued operator contains no operator");
                        operation = source_token(*found).kind;
                        continue;
                    }
                    if (node(child.index).kind == SyntaxKind::Newlines) { continue; }
                    const ast::ExprId operand = project_expression(child.index);
                    if (result == ast::no_node) {
                        result = operand;
                        continue;
                    }
                    require(operation.has_value(), "binary operand has no operator");
                    const SourceRange range = module_.expr(result).range.join(module_.expr(operand).range);
                    result                  = module_.add(ast::Expr{range, ast::Binary{binary_op(*operation), result, operand}});
                    operation.reset();
                }
                require(result != ast::no_node && !operation.has_value(), "binary expression is incomplete");
                return result;
            }

            [[nodiscard]] ast::ExprId project_unary_expression(SyntaxNodeId id) {
                const std::vector<SyntaxTokenId> tokens = child_tokens(id);
                if (tokens.empty()) { return project_expression(semantic_child(id)); }
                require(tokens.size() == 1, "unary expression contains multiple operators");
                const Token &token = source_token(tokens.front());
                require(token.kind == TokenKind::Minus || token.kind == TokenKind::Bang, "invalid unary operator");
                const ast::ExprId  operand = project_expression(semantic_child(id));
                const ast::UnaryOp op      = token.kind == TokenKind::Minus ? ast::UnaryOp::Negate : ast::UnaryOp::Not;
                return module_.add(ast::Expr{token.range.join(module_.expr(operand).range), ast::Unary{op, operand}});
            }

            [[nodiscard]] ast::ExprId project_postfix_expression(SyntaxNodeId id) {
                ast::ExprId result = project_expression(only_child(id, SyntaxKind::PrimaryExpression));
                for (const SyntaxNodeId postfix : child_nodes(id, SyntaxKind::Postfix)) {
                    const SyntaxNodeId operation = semantic_child(postfix);
                    switch (node(operation).kind) {
                        case SyntaxKind::CallPostfix:
                            {
                                ast::Call call;
                                call.callee    = result;
                                call.arguments = project_arguments(only_child(operation, SyntaxKind::Arguments));
                                result         = module_.add(
                                    ast::Expr{{module_.expr(result).range.begin, node(operation).range.end}, std::move(call)});
                                break;
                            }
                        case SyntaxKind::IndexPostfix:
                            {
                                const ast::ExprId index = project_expression(only_child(operation, SyntaxKind::Expression));
                                result = module_.add(ast::Expr{{module_.expr(result).range.begin, node(operation).range.end},
                                                               ast::Index{result, index}});
                                break;
                            }
                        case SyntaxKind::FieldPostfix:
                            {
                                const std::vector<ast::Name> names = direct_names(operation);
                                require(names.size() == 1, "field postfix has an invalid name");
                                result = module_.add(ast::Expr{{module_.expr(result).range.begin, names.front().range.end},
                                                               ast::Field{result, names.front()}});
                                break;
                            }
                        default: malformed("invalid postfix production");
                    }
                }
                return result;
            }

            [[nodiscard]] ast::ExprId project_name_reference(SyntaxNodeId id) {
                const std::vector<ast::Name> names = direct_names(id);
                require(names.size() == 1 || names.size() == 2, "reference has an invalid qualified name");
                if (names.size() == 1) { return module_.add(ast::Expr{names[0].range, ast::NameRef{names[0]}}); }
                return module_.add(ast::Expr{names[0].range.join(names[1].range), ast::QualifiedRef{names[0], names[1]}});
            }

            [[nodiscard]] ast::ExprId project_literal(SyntaxTokenId id) {
                const Token &token = source_token(id);
                switch (token.kind) {
                    case TokenKind::IntLiteral: return module_.add(ast::Expr{token.range, ast::IntLiteral{token.int_value}});
                    case TokenKind::FloatLiteral: return module_.add(ast::Expr{token.range, ast::FloatLiteral{token.float_value}});
                    case TokenKind::StringLiteral:
                        return module_.add(ast::Expr{token.range, ast::StringLiteral{token.string_value}});
                    case TokenKind::TemporalLiteral:
                        {
                            ast::TemporalLiteral literal;
                            if (token.temporal_value) { literal.value = *token.temporal_value; }
                            return module_.add(ast::Expr{token.range, std::move(literal)});
                        }
                    case TokenKind::KwTrue:
                    case TokenKind::KwFalse:
                        return module_.add(ast::Expr{token.range, ast::BoolLiteral{token.kind == TokenKind::KwTrue}});
                    case TokenKind::KwNull: return module_.add(ast::Expr{token.range, ast::NullLiteral{}});
                    case TokenKind::Placeholder: return module_.add(ast::Expr{token.range, ast::Placeholder{}});
                    default: malformed("invalid expression token");
                }
            }

            [[nodiscard]] std::vector<ast::Argument> project_arguments(SyntaxNodeId id) {
                std::vector<ast::Argument> result;
                for (const SyntaxNodeId child : child_nodes(id, SyntaxKind::Argument)) {
                    ast::Argument argument;
                    if (!child_tokens(child, TokenKind::Colon).empty()) {
                        const std::vector<ast::Name> names = direct_names(child);
                        require(names.size() == 1, "named argument has an invalid name");
                        argument.name = names.front();
                    }
                    argument.value = project_expression(only_child(child, SyntaxKind::Expression));
                    result.push_back(std::move(argument));
                }
                return result;
            }

            [[nodiscard]] ast::ExprId project_tuple(SyntaxNodeId id) {
                const std::vector<SyntaxNodeId> elements = child_nodes(id, SyntaxKind::TupleElement);
                if (elements.empty()) {
                    diagnostics_.report(Category::Parse, node(id).range, "expected an expression; '()' is not a value");
                    return module_.add(ast::Expr{node(id).range, ast::TupleLiteral{}});
                }
                if (elements.size() == 1 && child_nodes(id, SyntaxKind::CommaSeparator).empty()) {
                    return project_expression(elements.front());
                }
                ast::TupleLiteral tuple;
                for (const SyntaxNodeId element : elements) { tuple.elements.push_back(project_expression(element)); }
                return module_.add(ast::Expr{node(id).range, std::move(tuple)});
            }

            [[nodiscard]] ast::ExprId project_sequence(SyntaxNodeId id) {
                ast::SequenceLiteral sequence;
                for (const SyntaxNodeId child : child_nodes(id, SyntaxKind::SequenceElement)) {
                    ast::SequenceElement             element;
                    const std::vector<SyntaxTokenId> timed = child_tokens(child, TokenKind::TemporalLiteral);
                    if (!timed.empty()) {
                        require(timed.size() == 1, "sequence element has multiple time keys");
                        element.key = project_literal(timed.front());
                    }
                    element.value = project_expression(only_child(child, SyntaxKind::Expression));
                    sequence.elements.push_back(element);
                }
                return module_.add(ast::Expr{node(id).range, std::move(sequence)});
            }

            [[nodiscard]] ast::ExprId project_anonymous_function(SyntaxNodeId id) {
                ast::AnonymousFn fn;
                for (const SyntaxNodeId child : child_nodes(id, SyntaxKind::AnonymousParameter)) {
                    ast::AnonymousParameter      parameter;
                    const std::vector<ast::Name> names = direct_names(child);
                    require(names.size() == 1, "anonymous parameter has an invalid name");
                    parameter.name = names.front();
                    if (const auto type = find_child(child, SyntaxKind::Type)) { parameter.type = project_type(*type, false); }
                    fn.parameters.push_back(std::move(parameter));
                }
                const std::vector<SyntaxNodeId> types = child_nodes(id, SyntaxKind::Type);
                if (!types.empty()) {
                    require(types.size() == 1, "anonymous function has multiple result types");
                    fn.result = project_type(types.front(), false);
                }
                fn.body = project_expression(only_child(id, SyntaxKind::Expression));
                return module_.add(ast::Expr{node(id).range, std::move(fn)});
            }

            [[nodiscard]] ast::ExprId project_if(SyntaxNodeId id) {
                ast::If result;
                result.condition                       = project_expression(only_child(id, SyntaxKind::Expression));
                const std::vector<SyntaxNodeId> blocks = child_nodes(id, SyntaxKind::Block);
                require(blocks.size() == 1, "if expression has an invalid then block");
                result.then_block = project_block(blocks.front());
                if (const auto arm = find_child(id, SyntaxKind::ElseArm)) {
                    if (const auto nested = find_child(*arm, SyntaxKind::IfExpression)) {
                        result.otherwise = project_if(*nested);
                    } else {
                        const ast::BlockId block = project_block(only_child(*arm, SyntaxKind::Block));
                        result.otherwise         = module_.add(ast::Expr{module_.block(block).range, ast::BlockExpr{block}});
                    }
                }
                return module_.add(ast::Expr{node(id).range, result});
            }

            [[nodiscard]] ast::ExprId project_eval(SyntaxNodeId id) {
                const ast::ExprId               callee    = project_expression(only_child(id, SyntaxKind::Expression));
                const std::vector<SyntaxNodeId> arguments = child_nodes(id, SyntaxKind::Argument);
                ast::Eval                       result;
                result.callee = callee;
                for (const SyntaxNodeId current : arguments) {
                    ast::Argument argument;
                    if (!child_tokens(current, TokenKind::Colon).empty()) {
                        const std::vector<ast::Name> names = direct_names(current);
                        require(names.size() == 1, "eval named argument has an invalid name");
                        argument.name = names.front();
                    }
                    argument.value = project_expression(only_child(current, SyntaxKind::Expression));
                    result.arguments.push_back(std::move(argument));
                }
                return module_.add(ast::Expr{node(id).range, std::move(result)});
            }

            [[nodiscard]] ast::ExprId project_construct(SyntaxNodeId id) {
                ast::Construct               result;
                const std::vector<ast::Name> names = direct_names(id);
                require(!names.empty(), "construct has no target name");
                result.delta = names.front().text == "delta";
                if (result.delta) {
                    result.type = project_type(only_child(id, SyntaxKind::Type), false);
                } else {
                    ast::Type type;
                    type.kind  = ast::TypeKind::Named;
                    type.range = names.front().range;
                    if (names.size() == 2) {
                        type.qualifier = names[0];
                        type.name      = names[1];
                        type.range     = names[0].range.join(names[1].range);
                    } else {
                        require(names.size() == 1, "construct has an invalid qualified name");
                        type.name = names[0];
                    }
                    if (const auto arguments = find_child(id, SyntaxKind::GenericArguments)) {
                        type.arguments = project_generic_arguments(*arguments);
                        type.range     = type.range.join(node(*arguments).range);
                    }
                    result.type = module_.add(std::move(type));
                }
                if (module_.type(result.type).kind != ast::TypeKind::Named) {
                    diagnostics_.report(Category::Parse, module_.type(result.type).range,
                                        "a struct constructor takes a named struct type");
                }
                result.arguments = project_arguments(only_child(id, SyntaxKind::Arguments));
                return module_.add(ast::Expr{node(id).range, std::move(result)});
            }

            [[nodiscard]] ast::BlockId project_block(SyntaxNodeId id) {
                ast::Block result;
                result.range = node(id).range;
                for (const SyntaxNodeId item : child_nodes(id, SyntaxKind::BlockItem)) {
                    if (const auto statement = find_child(item, SyntaxKind::Statement)) {
                        result.statements.push_back(project_statement(*statement));
                    }
                }
                if (!result.statements.empty()) {
                    const ast::Stmt &last = module_.stmt(result.statements.back());
                    if (const auto *expression = std::get_if<ast::ExprStmt>(&last.node)) { result.tail = expression->expr; }
                }
                return module_.add(std::move(result));
            }

            [[nodiscard]] ast::StmtId project_statement(SyntaxNodeId id) {
                const SyntaxNodeId statement = semantic_child(id);
                const SourceRange  range     = node(statement).range;
                switch (node(statement).kind) {
                    case SyntaxKind::LocalDecl:
                        {
                            ast::LocalDecl                   result;
                            const std::vector<SyntaxTokenId> tokens = child_tokens(statement);
                            require(!tokens.empty(), "local declaration has no introducer");
                            result.mutable_                    = source_token(tokens.front()).kind == TokenKind::KwVar;
                            const std::vector<ast::Name> names = direct_names(statement, "a variable name");
                            require(names.size() == 1, "local declaration has an invalid name");
                            result.name = names.front();
                            if (const auto type = find_child(statement, SyntaxKind::Type)) {
                                result.type = project_type(*type, false);
                            }
                            result.init = project_expression(only_child(statement, SyntaxKind::Expression));
                            return module_.add(ast::Stmt{range, std::move(result)});
                        }
                    case SyntaxKind::StateDecl:
                        {
                            ast::StateDecl               result;
                            const std::vector<ast::Name> names = direct_names(statement);
                            require(names.size() == 1, "state declaration has an invalid name");
                            result.name = names.front();
                            if (const auto type = find_child(statement, SyntaxKind::Type)) {
                                result.type = project_type(*type, true);
                            }
                            result.init = project_expression(only_child(statement, SyntaxKind::Expression));
                            return module_.add(ast::Stmt{range, std::move(result)});
                        }
                    case SyntaxKind::InjectDecl:
                        {
                            ast::InjectDecl result;
                            result.names = direct_names(statement);
                            require(!result.names.empty(), "inject declaration has no names");
                            return module_.add(ast::Stmt{range, std::move(result)});
                        }
                    case SyntaxKind::LifecycleStmt:
                        {
                            ast::LifecycleBlock              result;
                            const std::vector<SyntaxTokenId> tokens = child_tokens(statement);
                            require(tokens.size() == 1, "lifecycle statement has an invalid introducer");
                            result.is_stop = source_token(tokens.front()).kind == TokenKind::KwStop;
                            result.block   = project_block(only_child(statement, SyntaxKind::Block));
                            return module_.add(ast::Stmt{range, result});
                        }
                    case SyntaxKind::WhenStmt:
                        {
                            ast::WhenStmt result;
                            result.condition = project_expression(only_child(statement, SyntaxKind::Expression));
                            result.block     = project_block(only_child(statement, SyntaxKind::Block));
                            return module_.add(ast::Stmt{range, result});
                        }
                    case SyntaxKind::ForStmt:
                        {
                            ast::ForStmt           result;
                            std::vector<ast::Name> names = direct_names(statement);
                            // The grammar's contextual `in` separator is the
                            // final direct name. Earlier occurrences remain
                            // ordinary pattern names (`for key, in in xs`).
                            require(!names.empty() && names.back().text == "in", "for statement has no 'in' separator");
                            names.pop_back();
                            require(names.size() == 1 || names.size() == 2, "for statement has an invalid pattern");
                            result.first = names[0];
                            if (names.size() == 2) { result.second = names[1]; }
                            result.iterable = project_expression(only_child(statement, SyntaxKind::Expression));
                            result.block    = project_block(only_child(statement, SyntaxKind::Block));
                            return module_.add(ast::Stmt{range, result});
                        }
                    case SyntaxKind::ReturnStmt:
                        {
                            ast::ReturnStmt result;
                            if (const auto expression = find_child(statement, SyntaxKind::Expression)) {
                                result.value = project_expression(*expression);
                            }
                            return module_.add(ast::Stmt{range, result});
                        }
                    case SyntaxKind::AssertStmt:
                        {
                            const ast::ExprId condition = project_expression(only_child(statement, SyntaxKind::Expression));
                            return module_.add(ast::Stmt{range, ast::AssertStmt{condition}});
                        }
                    case SyntaxKind::AssignOrExpressionStmt:
                        {
                            const std::vector<SyntaxNodeId> expressions = child_nodes(statement, SyntaxKind::Expression);
                            require(expressions.size() == 1 || expressions.size() == 2,
                                    "expression statement has an invalid expression count");
                            const ast::ExprId first = project_expression(expressions.front());
                            if (expressions.size() == 1) { return module_.add(ast::Stmt{range, ast::ExprStmt{first}}); }

                            if (!is_place(first)) {
                                diagnostics_.report(Category::Parse, module_.expr(first).range,
                                                    "only a name, an index, or a field can be assigned to");
                            }
                            TokenKind operation = TokenKind::Error;
                            for (const SyntaxTokenId token : child_tokens(statement)) {
                                const TokenKind kind = source_token(token).kind;
                                if (kind == TokenKind::Assign || kind == TokenKind::PlusAssign || kind == TokenKind::MinusAssign ||
                                    kind == TokenKind::StarAssign || kind == TokenKind::SlashAssign) {
                                    operation = kind;
                                    break;
                                }
                            }
                            require(operation != TokenKind::Error, "assignment has no operator");
                            const ast::ExprId value = project_expression(expressions[1]);
                            if (!is_place(first)) { return module_.add(ast::Stmt{range, ast::ExprStmt{first}}); }
                            return module_.add(ast::Stmt{range, ast::AssignStmt{assign_op(operation), first, value}});
                        }
                    default: malformed("invalid statement production");
                }
            }

            [[nodiscard]] bool is_place(ast::ExprId id) const noexcept {
                const ast::ExprNode &value = module_.expr(id).node;
                if (std::holds_alternative<ast::NameRef>(value)) { return true; }
                if (const auto *index = std::get_if<ast::Index>(&value)) { return is_place(index->target); }
                if (const auto *field = std::get_if<ast::Field>(&value)) { return is_place(field->target); }
                return false;
            }

            [[nodiscard]] ast::ConstraintId project_constraint(SyntaxNodeId id) {
                switch (node(id).kind) {
                    case SyntaxKind::Constraint:
                        return project_constraint_logic(id, SyntaxKind::ConstraintAnd, ast::ConstraintLogicOp::Or);
                    case SyntaxKind::ConstraintAnd:
                        return project_constraint_logic(id, SyntaxKind::ConstraintTerm, ast::ConstraintLogicOp::And);
                    case SyntaxKind::ConstraintTerm: return project_constraint_term(id);
                    case SyntaxKind::ConstraintOperand: return project_constraint_operand(id);
                    default: malformed("invalid constraint production");
                }
            }

            [[nodiscard]] ast::ConstraintId project_constraint_logic(SyntaxNodeId id, SyntaxKind operand_kind,
                                                                     ast::ConstraintLogicOp operation) {
                const std::vector<SyntaxNodeId> operands = child_nodes(id, operand_kind);
                require(!operands.empty(), "constraint logic has no operands");
                ast::ConstraintId result = project_constraint(operands.front());
                for (auto operand = operands.begin() + 1; operand != operands.end(); ++operand) {
                    const ast::ConstraintId rhs = project_constraint(*operand);
                    result = module_.add(ast::Constraint{module_.constraint(result).range.join(module_.constraint(rhs).range),
                                                         ast::ConstraintLogic{operation, result, rhs}});
                }
                return result;
            }

            [[nodiscard]] ast::ConstraintId project_constraint_term(SyntaxNodeId id) {
                const std::vector<SyntaxTokenId> tokens = child_tokens(id);
                if (!tokens.empty() && source_token(tokens.front()).kind == TokenKind::Bang) {
                    const ast::ConstraintId operand = project_constraint(only_child(id, SyntaxKind::ConstraintTerm));
                    return module_.add(ast::Constraint{source_token(tokens.front()).range.join(module_.constraint(operand).range),
                                                       ast::ConstraintNot{operand}});
                }
                if (const auto grouped = find_child(id, SyntaxKind::Constraint)) { return project_constraint(*grouped); }

                const std::vector<SyntaxNodeId> operands = child_nodes(id, SyntaxKind::ConstraintOperand);
                require(!operands.empty(), "constraint term has no operand");
                ast::ConstraintId lhs = project_constraint(operands.front());
                const auto        arrow =
                    std::ranges::find_if(tokens, [&](SyntaxTokenId token) { return source_token(token).kind == TokenKind::Arrow; });
                if (arrow != tokens.end()) {
                    auto *call = std::get_if<ast::ConstraintCall>(&module_.constraints[lhs].node);
                    if (call == nullptr) {
                        diagnostics_.report(Category::Parse, module_.constraint(lhs).range,
                                            "the left side of an operator requirement is a call");
                        (void)project_type(only_child(id, SyntaxKind::Type), true);
                        return lhs;
                    }
                    ast::OperatorRequirement requirement;
                    requirement.qualifier    = call->qualifier;
                    requirement.name         = call->name;
                    requirement.arguments    = std::move(call->arguments);
                    requirement.result       = project_type(only_child(id, SyntaxKind::Type), true);
                    module_.constraints[lhs] = ast::Constraint{
                        module_.constraint(lhs).range.join(module_.type(requirement.result).range), std::move(requirement)};
                    return lhs;
                }

                std::optional<ast::ConstraintRelationOp> relation;
                for (const SyntaxTokenId token : tokens) {
                    const Token &source = source_token(token);
                    if (source.kind == TokenKind::EqualEqual) {
                        relation = ast::ConstraintRelationOp::Equal;
                    } else if (source.kind == TokenKind::KwIs) {
                        relation = ast::ConstraintRelationOp::Is;
                    } else if (!relation.has_value() && source.kind == TokenKind::Identifier && source.text == "in") {
                        relation = ast::ConstraintRelationOp::In;
                    }
                }
                if (!relation.has_value()) { return lhs; }

                ast::ConstraintRelation result;
                result.op  = *relation;
                result.lhs = lhs;
                if (*relation == ast::ConstraintRelationOp::Is) {
                    bool found = false;
                    for (const SyntaxNodeId category : child_nodes(id, SyntaxKind::Name)) {
                        const std::vector<ast::Name> names = direct_names(category);
                        require(names.size() == 1, "is relation category has an invalid name");
                        result.category = names.front();
                        found           = true;
                    }
                    for (const SyntaxTokenId token : tokens) {
                        const Token &source = source_token(token);
                        if (source.kind == TokenKind::Identifier || source.kind == TokenKind::KwStruct) {
                            result.category = name(token);
                            found           = true;
                        }
                    }
                    require(found, "is relation has no category");
                    return module_.add(
                        ast::Constraint{module_.constraint(lhs).range.join(result.category.range), std::move(result)});
                }
                require(operands.size() == 2, "constraint relation does not have two operands");
                result.rhs = project_constraint(operands[1]);
                return module_.add(
                    ast::Constraint{module_.constraint(lhs).range.join(module_.constraint(result.rhs).range), std::move(result)});
            }

            [[nodiscard]] ast::ConstraintId project_constraint_operand(SyntaxNodeId id) {
                if (const auto set = find_child(id, SyntaxKind::ConstraintSet)) {
                    ast::ConstraintSet result;
                    for (const SyntaxNodeId element : child_nodes(*set, SyntaxKind::ConstraintOperand)) {
                        result.elements.push_back(project_constraint(element));
                    }
                    return module_.add(ast::Constraint{node(*set).range, std::move(result)});
                }
                if (const auto call = find_child(id, SyntaxKind::ConstraintCall)) {
                    ast::ConstraintCall          result;
                    const SyntaxNodeId           qualified = only_child(*call, SyntaxKind::QualifiedName);
                    const std::vector<ast::Name> names     = direct_names(qualified);
                    require(names.size() == 1 || names.size() == 2, "constraint call has an invalid name");
                    if (names.size() == 2) {
                        result.qualifier = names[0];
                        result.name      = names[1];
                    } else {
                        result.name = names[0];
                    }
                    for (const SyntaxNodeId argument : child_nodes(*call, SyntaxKind::ConstraintOperand)) {
                        result.arguments.push_back(project_constraint(argument));
                    }
                    return module_.add(ast::Constraint{node(*call).range, std::move(result)});
                }

                const std::vector<SyntaxNodeId> nodes = child_nodes(id);
                if (!nodes.empty()) {
                    const SyntaxNodeId value = semantic_child(id);
                    switch (node(value).kind) {
                        case SyntaxKind::NamedType:
                        case SyntaxKind::TupleType:
                        case SyntaxKind::ListType:
                        case SyntaxKind::SetType:
                        case SyntaxKind::MapType:
                        case SyntaxKind::RollingType:
                        case SyntaxKind::AtomicType:
                            {
                                const ast::TypeId type = project_type(value, true);
                                return module_.add(ast::Constraint{module_.type(type).range, ast::ConstraintType{type}});
                            }
                        case SyntaxKind::SizeExpression:
                            {
                                const ast::ExprId expression = project_expression(value);
                                return module_.add(
                                    ast::Constraint{module_.expr(expression).range, ast::ConstraintValue{expression}});
                            }
                        case SyntaxKind::Name:
                            {
                                const std::vector<ast::Name> names = direct_names(value);
                                require(names.size() == 1, "constraint name has an invalid shape");
                                return module_.add(ast::Constraint{node(value).range, ast::ConstraintName{names.front()}});
                            }
                        default: malformed("invalid constraint operand child");
                    }
                }

                const std::vector<SyntaxTokenId> tokens = child_tokens(id);
                require(!tokens.empty(), "constraint operand has no value");
                if (const auto scalar = scalar_type(source_token(tokens.front()).kind)) {
                    ast::Type type;
                    type.kind                 = ast::TypeKind::Scalar;
                    type.range                = node(id).range;
                    type.scalar               = *scalar;
                    type.value_position       = true;
                    const ast::TypeId type_id = module_.add(std::move(type));
                    return module_.add(ast::Constraint{node(id).range, ast::ConstraintType{type_id}});
                }
                if (source_token(tokens.front()).kind == TokenKind::Identifier) {
                    return module_.add(ast::Constraint{node(id).range, ast::ConstraintName{name(tokens.front())}});
                }
                const ast::ExprId expression = project_literal(tokens.front());
                return module_.add(ast::Constraint{module_.expr(expression).range, ast::ConstraintValue{expression}});
            }

            [[nodiscard]] std::vector<ast::GenericParameter> project_generic_parameters(SyntaxNodeId id) {
                std::vector<ast::GenericParameter> result;
                for (const SyntaxNodeId child : child_nodes(id, SyntaxKind::GenericParameter)) {
                    ast::GenericParameter parameter;
                    parameter.is_const                 = !child_tokens(child, TokenKind::KwConst).empty();
                    const std::vector<ast::Name> names = direct_names(child, "a generic parameter name");
                    require(names.size() == 1, "generic parameter has an invalid name");
                    parameter.name = names.front();
                    if (parameter.is_const) { parameter.type = project_type(only_child(child, SyntaxKind::Type), true); }
                    result.push_back(std::move(parameter));
                }
                return result;
            }

            [[nodiscard]] ast::Signature project_signature(SyntaxNodeId id) {
                ast::Signature result;
                for (const SyntaxNodeId child : child_nodes(id, SyntaxKind::Parameter)) {
                    ast::Parameter parameter;
                    parameter.is_const                 = !child_tokens(child, TokenKind::KwConst).empty();
                    const std::vector<ast::Name> names = direct_names(child, "a parameter name");
                    require(names.size() == 1, "parameter has an invalid name");
                    parameter.name = names.front();
                    parameter.type = project_type(only_child(child, SyntaxKind::Type), parameter.is_const);
                    if (const auto expression = find_child(child, SyntaxKind::Expression)) {
                        parameter.default_value = project_expression(*expression);
                        if (!parameter.is_const) {
                            diagnostics_.report(Category::Parse, module_.expr(parameter.default_value).range,
                                                "only a const parameter may have a default");
                        }
                    }
                    result.parameters.push_back(std::move(parameter));
                }
                const std::vector<SyntaxNodeId> types = child_nodes(id, SyntaxKind::Type);
                if (!types.empty()) {
                    require(types.size() == 1, "signature has multiple result types");
                    result.result = project_type(types.front(), false);
                }
                return result;
            }

            [[nodiscard]] ast::ConstraintId project_optional_requires(SyntaxNodeId owner) {
                const auto optional = find_child(owner, SyntaxKind::OptionalRequiresClause);
                if (!optional.has_value()) { return ast::no_node; }
                const auto clause = find_child(*optional, SyntaxKind::RequiresClause);
                if (!clause.has_value()) { return ast::no_node; }
                return project_constraint(only_child(*clause, SyntaxKind::Constraint));
            }

            [[nodiscard]] ast::Decl project_module_decl(SyntaxNodeId id) {
                ast::ModuleDecl result;
                result.path = direct_names(only_child(id, SyntaxKind::ModulePath));
                require(!result.path.empty(), "module path is empty");
                return ast::Decl{node(id).range, std::move(result)};
            }

            [[nodiscard]] ast::Decl project_declaration(SyntaxNodeId id) {
                const SyntaxNodeId declaration = semantic_child(id);
                switch (node(declaration).kind) {
                    case SyntaxKind::UseDecl: return project_use_decl(declaration);
                    case SyntaxKind::FunctionDecl: return project_function_decl(declaration);
                    case SyntaxKind::OperatorDecl: return project_operator_decl(declaration);
                    case SyntaxKind::StructDecl: return project_struct_decl(declaration);
                    case SyntaxKind::TestDecl: return project_test_decl(declaration);
                    default: malformed("invalid declaration production");
                }
            }

            [[nodiscard]] ast::Decl project_use_decl(SyntaxNodeId id) {
                ast::UseDecl result;
                result.path                        = direct_names(only_child(id, SyntaxKind::ModulePath));
                const std::vector<ast::Name> names = direct_names(id, "an imported name");
                if (!child_tokens(id, TokenKind::KwAs).empty()) {
                    require(names.size() == 1, "aliased use declaration has an invalid alias");
                    result.alias = names.front();
                } else {
                    if (names.empty()) {
                        diagnostics_.report(Category::Parse, node(id).range, "an import set names at least one declaration");
                    }
                    result.names = names;
                }
                return ast::Decl{node(id).range, std::move(result)};
            }

            [[nodiscard]] ast::Decl project_function_decl(SyntaxNodeId id) {
                ast::FunctionDecl result;
                if (!child_tokens(id, TokenKind::KwExport).empty()) {
                    result.visibility = ast::FunctionVisibility::Export;
                } else if (!child_tokens(id, TokenKind::KwImpl).empty()) {
                    result.visibility = ast::FunctionVisibility::Impl;
                }
                const std::vector<ast::Name> names = direct_names(id, "a function name");
                require(names.size() == 1, "function has an invalid name");
                result.name = names.front();
                if (const auto generics = find_child(id, SyntaxKind::GenericParameters)) {
                    result.generics = project_generic_parameters(*generics);
                }
                result.signature    = project_signature(only_child(id, SyntaxKind::Signature));
                result.requirements = project_optional_requires(id);
                if (const auto expression = find_child(id, SyntaxKind::Expression)) {
                    result.concise_body = project_expression(*expression);
                } else {
                    result.block_body = project_block(only_child(id, SyntaxKind::Block));
                }
                return ast::Decl{node(id).range, std::move(result)};
            }

            [[nodiscard]] ast::Decl project_operator_decl(SyntaxNodeId id) {
                ast::OperatorDecl            result;
                const std::vector<ast::Name> names = direct_names(id);
                require(names.size() == 1, "operator has an invalid name");
                result.name = names.front();
                if (const auto generics = find_child(id, SyntaxKind::GenericParameters)) {
                    result.generics = project_generic_parameters(*generics);
                }
                result.signature    = project_signature(only_child(id, SyntaxKind::Signature));
                result.requirements = project_optional_requires(id);
                if (find_child(id, SyntaxKind::Expression) || find_child(id, SyntaxKind::Block)) {
                    diagnostics_.report(Category::Parse, node(id).range,
                                        "an operator declaration has no body; implement it with 'impl fn'");
                }
                return ast::Decl{node(id).range, std::move(result)};
            }

            [[nodiscard]] ast::Decl project_struct_decl(SyntaxNodeId id) {
                ast::StructDecl result;
                result.exported                    = !child_tokens(id, TokenKind::KwExport).empty();
                result.abstract                    = !child_tokens(id, TokenKind::KwAbstract).empty();
                const std::vector<ast::Name> names = direct_names(id);
                require(names.size() == 1, "struct has an invalid name");
                result.name = names.front();
                if (const auto generics = find_child(id, SyntaxKind::GenericParameters)) {
                    result.generics = project_generic_parameters(*generics);
                }
                for (const SyntaxNodeId parent : child_nodes(id, SyntaxKind::Type)) {
                    const ast::TypeId type = project_type(parent, true);
                    if (module_.type(type).kind != ast::TypeKind::Named) {
                        diagnostics_.report(Category::Parse, module_.type(type).range, "a struct parent is a named type");
                    }
                    result.parents.push_back(type);
                }
                result.requirements = project_optional_requires(id);
                for (const SyntaxNodeId item : child_nodes(id, SyntaxKind::StructBodyItem)) {
                    const SyntaxNodeId           member       = only_child(item, SyntaxKind::StructMember);
                    const std::vector<ast::Name> member_names = direct_names(member);
                    require(member_names.size() == 1, "struct member has an invalid name");
                    if (const auto type = find_child(member, SyntaxKind::Type)) {
                        ast::StructField field;
                        field.name = member_names.front();
                        field.type = project_type(*type, false);
                        if (const auto expression = find_child(member, SyntaxKind::Expression)) {
                            field.default_value = project_expression(*expression);
                        }
                        result.members.emplace_back(std::move(field));
                    } else {
                        ast::InheritedDefault inherited;
                        inherited.name  = member_names.front();
                        inherited.value = project_expression(only_child(member, SyntaxKind::Expression));
                        result.members.emplace_back(std::move(inherited));
                    }
                }
                return ast::Decl{node(id).range, std::move(result)};
            }

            [[nodiscard]] ast::Decl project_test_decl(SyntaxNodeId id) {
                ast::TestDecl                result;
                const std::vector<ast::Name> names = direct_names(id);
                require(names.size() == 1, "test has an invalid name");
                result.name  = names.front();
                result.block = project_block(only_child(id, SyntaxKind::Block));
                return ast::Decl{node(id).range, std::move(result)};
            }

            void add_declaration(ast::Decl declaration) {
                const bool is_use = std::holds_alternative<ast::UseDecl>(declaration.node);
                if (is_use && seen_ordinary_) {
                    diagnostics_.report(Category::Parse, declaration.range, "'use' declarations must precede other declarations");
                }
                const bool        ordinary = !is_use && !std::holds_alternative<ast::ModuleDecl>(declaration.node);
                const ast::DeclId id       = module_.add(std::move(declaration));
                module_.declarations.push_back(id);
                seen_ordinary_ = seen_ordinary_ || ordinary;
            }

            const SyntaxTree &tree_;
            const LexResult  &lexed_;
            DiagnosticSink   &diagnostics_;
            ast::Module       module_{};
            bool              seen_ordinary_{false};
        };
    }  // namespace

    ast::Module project_ast(const SyntaxTree &tree, const LexResult &lexed, DiagnosticSink &diagnostics) {
        return AstProjector{tree, lexed, diagnostics}.run();
    }
}  // namespace hgl::syntax
