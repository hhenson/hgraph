#include "syntax/parser.h"

#include "syntax/lexer.h"
#include "syntax/token.h"

#include <cstddef>
#include <optional>
#include <string>
#include <utility>
#include <variant>
#include <vector>

// A recursive-descent parser for the compilation-unit grammar of the
// developer guide ("Syntax and semantics"). Newlines are tokens: they end
// declarations and statements, and are skipped only inside `()`, `[]`, and
// `<>` lists, after commas and binary operators, after `=`, `=>`, `->`, and
// `:`, and before a line that begins with a binary operator, `->`, or `else`.
// Recovery is exception based and private to this file: a failed production
// throws `ParseError` after reporting, and the enclosing block or the file
// level resynchronises (developer guide, "AST requirements").
namespace hgl::syntax
{
    namespace
    {
        struct ParseError
        {
        };

        // Binary precedence, low to high (developer guide, "Blocks and
        // expressions"). `Additive` is the floor for type-size expressions
        // so that `>` closes the type argument list.
        enum Precedence : int
        {
            PrecOr             = 1,
            PrecAnd            = 2,
            PrecEquality       = 3,
            PrecComparison     = 4,
            PrecAdditive       = 5,
            PrecMultiplicative = 6,
        };

        struct BinaryInfo
        {
            ast::BinaryOp op;
            int           precedence;
        };

        [[nodiscard]] std::optional<BinaryInfo> binary_info(TokenKind kind) noexcept
        {
            switch (kind)
            {
                case TokenKind::Star: return BinaryInfo{ast::BinaryOp::Mul, PrecMultiplicative};
                case TokenKind::Slash: return BinaryInfo{ast::BinaryOp::Div, PrecMultiplicative};
                case TokenKind::Percent: return BinaryInfo{ast::BinaryOp::Rem, PrecMultiplicative};
                case TokenKind::Plus: return BinaryInfo{ast::BinaryOp::Add, PrecAdditive};
                case TokenKind::Minus: return BinaryInfo{ast::BinaryOp::Sub, PrecAdditive};
                case TokenKind::Less: return BinaryInfo{ast::BinaryOp::Less, PrecComparison};
                case TokenKind::LessEqual: return BinaryInfo{ast::BinaryOp::LessEqual, PrecComparison};
                case TokenKind::Greater: return BinaryInfo{ast::BinaryOp::Greater, PrecComparison};
                case TokenKind::GreaterEqual: return BinaryInfo{ast::BinaryOp::GreaterEqual, PrecComparison};
                case TokenKind::EqualEqual: return BinaryInfo{ast::BinaryOp::Equal, PrecEquality};
                case TokenKind::NotEqual: return BinaryInfo{ast::BinaryOp::NotEqual, PrecEquality};
                case TokenKind::AndAnd: return BinaryInfo{ast::BinaryOp::And, PrecAnd};
                case TokenKind::OrOr: return BinaryInfo{ast::BinaryOp::Or, PrecOr};
                default: return std::nullopt;
            }
        }

        [[nodiscard]] std::optional<ast::AssignOp> assign_op(TokenKind kind) noexcept
        {
            switch (kind)
            {
                case TokenKind::Assign: return ast::AssignOp::Assign;
                case TokenKind::PlusAssign: return ast::AssignOp::Add;
                case TokenKind::MinusAssign: return ast::AssignOp::Sub;
                case TokenKind::StarAssign: return ast::AssignOp::Mul;
                case TokenKind::SlashAssign: return ast::AssignOp::Div;
                default: return std::nullopt;
            }
        }

        [[nodiscard]] std::optional<ast::ScalarType> scalar_type(TokenKind kind) noexcept
        {
            switch (kind)
            {
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

        /// True for the tokens that begin a top-level declaration; `fn` only
        /// when a name follows, so an anonymous `fn(` inside a body is not a
        /// resynchronisation point.
        [[nodiscard]] bool starts_declaration(TokenKind kind, TokenKind next) noexcept
        {
            switch (kind)
            {
                case TokenKind::KwModule:
                case TokenKind::KwUse:
                case TokenKind::KwExport:
                case TokenKind::KwImpl:
                case TokenKind::KwOperator:
                case TokenKind::KwTest: return true;
                case TokenKind::KwFn: return next == TokenKind::Identifier || is_keyword(next);
                default: return false;
            }
        }

        class Parser
        {
          public:
            Parser(const SourceFile &file, DiagnosticSink &diagnostics) : diagnostics_{diagnostics}
            {
                LexResult lexed = lex(file, diagnostics);
                tokens_         = std::move(lexed.tokens);
                module_.comments = std::move(lexed.comments);
            }

            ast::Module run()
            {
                skip_newlines();
                if (at(TokenKind::KwModule))
                {
                    parse_top_level(false);
                }
                else
                {
                    diagnostics_.report(Category::Parse, {tok().range.begin, tok().range.begin},
                                        "expected a 'module' declaration at the start of the file");
                }
                bool seen_declaration = false;
                while (true)
                {
                    skip_newlines();
                    if (at_end()) { break; }
                    seen_declaration = parse_top_level(seen_declaration) || seen_declaration;
                }
                return std::move(module_);
            }

          private:
            // ------------------------------------------------------ tokens

            [[nodiscard]] const Token &tok() const noexcept { return tokens_[pos_]; }
            [[nodiscard]] const Token &tok_at(std::size_t index) const noexcept
            {
                return tokens_[index < tokens_.size() ? index : tokens_.size() - 1];
            }
            [[nodiscard]] const Token &next_tok() const noexcept { return tok_at(pos_ + 1); }
            [[nodiscard]] bool at(TokenKind kind) const noexcept { return tok().kind == kind; }
            [[nodiscard]] bool at_end() const noexcept { return at(TokenKind::EndOfFile); }
            [[nodiscard]] bool at_identifier(std::string_view text) const noexcept
            {
                return at(TokenKind::Identifier) && tok().text == text;
            }

            const Token &advance()
            {
                const Token &token = tok();
                if (token.kind != TokenKind::EndOfFile) { ++pos_; }
                if (token.kind != TokenKind::Newline) { last_end_ = token.range.end; }
                return token;
            }

            void skip_newlines()
            {
                while (at(TokenKind::Newline)) { ++pos_; }
            }

            /// A newline or the end of the file must follow. A lexer error
            /// token (for instance a `;`) has already been diagnosed and
            /// stands in for the terminator so the construct is not reported
            /// twice.
            void expect_terminator(std::string_view what)
            {
                if (at(TokenKind::Error))
                {
                    advance();
                    return;
                }
                if (!at(TokenKind::Newline) && !at_end()) { fail_expected(what); }
            }

            /// The kind of the next token, looking through one newline run.
            [[nodiscard]] const Token &tok_past_newline() const noexcept
            {
                return at(TokenKind::Newline) ? next_tok() : tok();
            }

            /// When the next token past an optional newline is `kind`, skip
            /// the newline and return true (continuation lines).
            bool continue_with(TokenKind kind)
            {
                if (tok_past_newline().kind != kind) { return false; }
                skip_newlines();
                return true;
            }

            [[nodiscard]] static std::string describe(const Token &token)
            {
                switch (token.kind)
                {
                    case TokenKind::EndOfFile: return "end of file";
                    case TokenKind::Newline: return "newline";
                    default: return "'" + std::string{token.text} + "'";
                }
            }

            [[nodiscard]] SourceRange here() const noexcept
            {
                const Token &token = tok();
                if (token.kind == TokenKind::Newline || token.kind == TokenKind::EndOfFile)
                {
                    return {token.range.begin, token.range.begin};
                }
                return token.range;
            }

            [[noreturn]] void fail(SourceRange range, std::string message)
            {
                diagnostics_.report(Category::Parse, range, std::move(message));
                throw ParseError{};
            }

            [[noreturn]] void fail_expected(std::string_view what)
            {
                fail(here(), "expected " + std::string{what} + ", found " + describe(tok()));
            }

            const Token &expect(TokenKind kind, std::string_view what)
            {
                if (!at(kind)) { fail_expected(what); }
                return advance();
            }

            /// An identifier; a reserved word is reported and still taken as
            /// the name so parsing continues.
            ast::Name expect_name(std::string_view what)
            {
                if (at(TokenKind::Identifier))
                {
                    const Token &token = advance();
                    return ast::Name{token.text, token.range};
                }
                if (is_keyword(tok().kind))
                {
                    const Token &token = advance();
                    diagnostics_.report(Category::Parse, token.range,
                                        "'" + std::string{token.text} + "' is a reserved word and cannot be used as " +
                                            std::string{what});
                    return ast::Name{token.text, token.range};
                }
                fail_expected(what);
            }

            [[nodiscard]] SourceRange range_from(std::uint32_t begin) const noexcept { return {begin, last_end_}; }

            // ---------------------------------------------------- top level

            /// Parse one declaration and its terminator; returns whether it
            /// was an ordinary declaration (not `module`/`use`). Recovers at
            /// the next declaration start on a line of its own.
            bool parse_top_level(bool seen_declaration)
            {
                bool ordinary = false;
                try
                {
                    ast::DeclId id = ast::no_node;
                    switch (tok().kind)
                    {
                        case TokenKind::KwModule: id = parse_module_decl(); break;
                        case TokenKind::KwUse:
                            if (seen_declaration)
                            {
                                diagnostics_.report(Category::Parse, tok().range,
                                                    "'use' declarations must precede other declarations");
                            }
                            id = parse_use_decl();
                            break;
                        case TokenKind::KwOperator: id = parse_operator_decl(); break;
                        case TokenKind::KwExport:
                        case TokenKind::KwImpl:
                        case TokenKind::KwFn: id = parse_function_decl(); break;
                        case TokenKind::KwTest: id = parse_test_decl(); break;
                        default: fail_expected("a declaration");
                    }
                    ordinary = !std::holds_alternative<ast::ModuleDecl>(module_.decl(id).node) &&
                               !std::holds_alternative<ast::UseDecl>(module_.decl(id).node);
                    module_.declarations.push_back(id);
                    expect_terminator("a newline after the declaration");
                }
                catch (const ParseError &)
                {
                    synchronize_declaration();
                }
                return ordinary;
            }

            void synchronize_declaration()
            {
                while (!at_end())
                {
                    if (at(TokenKind::Newline))
                    {
                        const Token &candidate = next_tok();
                        if (starts_declaration(candidate.kind, tok_at(pos_ + 2).kind)) { return; }
                    }
                    ++pos_;
                }
            }

            ast::DeclId parse_module_decl()
            {
                const std::uint32_t begin = tok().range.begin;
                if (seen_module_)
                {
                    diagnostics_.report(Category::Parse, tok().range, "a file has one 'module' declaration");
                }
                seen_module_ = true;
                advance();
                ast::ModuleDecl decl;
                decl.path = parse_module_path();
                return module_.add(ast::Decl{range_from(begin), std::move(decl)});
            }

            std::vector<ast::Name> parse_module_path()
            {
                std::vector<ast::Name> path;
                path.push_back(expect_name("a module name"));
                while (at(TokenKind::Dot))
                {
                    advance();
                    path.push_back(expect_name("a module name"));
                }
                return path;
            }

            ast::DeclId parse_use_decl()
            {
                const std::uint32_t begin = tok().range.begin;
                advance();
                ast::UseDecl decl;
                decl.path = parse_module_path();
                if (at(TokenKind::ColonColon))
                {
                    advance();
                    // The import set is always braced (`use a.b::{x}`); a
                    // bare `use a.b::x` is not a spelling of the language.
                    expect(TokenKind::LBrace, "'{' after '::'");
                    skip_newlines();
                    while (!at(TokenKind::RBrace))
                    {
                        decl.names.push_back(expect_name("an imported name"));
                        skip_newlines();
                        if (!at(TokenKind::Comma)) { break; }
                        advance();
                        skip_newlines();
                    }
                    expect(TokenKind::RBrace, "',' or '}'");
                    if (decl.names.empty())
                    {
                        fail(range_from(begin), "an import set names at least one declaration");
                    }
                }
                else if (at(TokenKind::KwAs))
                {
                    advance();
                    decl.alias = expect_name("a module alias");
                }
                else { fail_expected("'::' or 'as' after the module path"); }
                return module_.add(ast::Decl{range_from(begin), std::move(decl)});
            }

            ast::DeclId parse_operator_decl()
            {
                const std::uint32_t begin = tok().range.begin;
                advance();
                ast::OperatorDecl decl;
                decl.name = expect_name("an operator name");
                if (at(TokenKind::Less)) { decl.generics = parse_generic_parameters(); }
                decl.signature = parse_signature();
                if (at(TokenKind::FatArrow) || at(TokenKind::LBrace))
                {
                    fail(tok().range, "an operator declaration has no body; implement it with 'impl fn'");
                }
                return module_.add(ast::Decl{range_from(begin), std::move(decl)});
            }

            ast::DeclId parse_function_decl()
            {
                const std::uint32_t begin = tok().range.begin;
                ast::FunctionDecl   decl;
                if (at(TokenKind::KwExport))
                {
                    decl.visibility = ast::FunctionVisibility::Export;
                    advance();
                }
                else if (at(TokenKind::KwImpl))
                {
                    decl.visibility = ast::FunctionVisibility::Impl;
                    advance();
                }
                expect(TokenKind::KwFn, "'fn'");
                decl.name = expect_name("a function name");
                if (at(TokenKind::Less)) { decl.generics = parse_generic_parameters(); }
                decl.signature = parse_signature();
                if (at(TokenKind::FatArrow))
                {
                    advance();
                    skip_newlines();
                    decl.concise_body = parse_expression();
                }
                else if (at(TokenKind::LBrace)) { decl.block_body = parse_block(); }
                else { fail_expected("'=>' or '{' to begin the function body"); }
                return module_.add(ast::Decl{range_from(begin), std::move(decl)});
            }

            ast::DeclId parse_test_decl()
            {
                const std::uint32_t begin = tok().range.begin;
                advance();
                ast::TestDecl decl;
                decl.name  = expect_name("a test name");
                decl.block = parse_block();
                return module_.add(ast::Decl{range_from(begin), std::move(decl)});
            }

            std::vector<ast::GenericParameter> parse_generic_parameters()
            {
                std::vector<ast::GenericParameter> generics;
                expect(TokenKind::Less, "'<'");
                skip_newlines();
                while (!at(TokenKind::Greater))
                {
                    ast::GenericParameter generic;
                    if (at(TokenKind::KwConst))
                    {
                        advance();
                        generic.is_const = true;
                        generic.name     = expect_name("a const generic name");
                        expect(TokenKind::Colon, "':' and the const generic's type");
                        skip_newlines();
                        generic.type = parse_type(true);
                    }
                    else { generic.name = expect_name("a generic parameter name"); }
                    generics.push_back(std::move(generic));
                    skip_newlines();
                    if (!at(TokenKind::Comma)) { break; }
                    advance();
                    skip_newlines();
                }
                expect(TokenKind::Greater, "',' or '>'");
                if (generics.empty()) { fail(here(), "a generic parameter list names at least one parameter"); }
                return generics;
            }

            ast::Signature parse_signature()
            {
                ast::Signature signature;
                expect(TokenKind::LParen, "'('");
                skip_newlines();
                while (!at(TokenKind::RParen))
                {
                    ast::Parameter parameter;
                    if (at(TokenKind::KwConst))
                    {
                        advance();
                        parameter.is_const = true;
                    }
                    parameter.name = expect_name("a parameter name");
                    expect(TokenKind::Colon, "':' and the parameter's type");
                    skip_newlines();
                    parameter.type = parse_type(parameter.is_const);
                    if (at(TokenKind::Assign))
                    {
                        advance();
                        skip_newlines();
                        parameter.default_value = parse_expression();
                    }
                    signature.parameters.push_back(std::move(parameter));
                    skip_newlines();
                    if (!at(TokenKind::Comma)) { break; }
                    advance();
                    skip_newlines();
                }
                expect(TokenKind::RParen, "',' or ')'");
                if (continue_with(TokenKind::Arrow))
                {
                    advance();
                    skip_newlines();
                    signature.result = parse_type(false);
                }
                return signature;
            }

            // -------------------------------------------------------- types

            /// `value_position` marks the value-type grammar (`const`
            /// parameters, generics, set elements, map keys, rolling
            /// elements, atomic payloads); the restriction itself is a
            /// later type check.
            ast::TypeId parse_type(bool value_position)
            {
                ast::Type type;
                type.value_position       = value_position;
                const std::uint32_t begin = tok().range.begin;
                if (const auto scalar = scalar_type(tok().kind))
                {
                    type.kind   = ast::TypeKind::Scalar;
                    type.scalar = *scalar;
                    advance();
                }
                else if (at(TokenKind::Identifier))
                {
                    const std::string_view text = tok().text;
                    const bool             generic = next_tok().kind == TokenKind::Less;
                    if (generic && text == "tuple")
                    {
                        type.kind = ast::TypeKind::Tuple;
                        advance();
                        advance();
                        skip_newlines();
                        while (!at(TokenKind::Greater))
                        {
                            type.children.push_back(parse_type(value_position));
                            skip_newlines();
                            if (!at(TokenKind::Comma)) { break; }
                            advance();
                            skip_newlines();
                        }
                        expect(TokenKind::Greater, "',' or '>'");
                        if (type.children.empty()) { fail(range_from(begin), "a tuple type has at least one element"); }
                    }
                    else if (generic && text == "list")
                    {
                        type.kind = ast::TypeKind::List;
                        advance();
                        advance();
                        skip_newlines();
                        type.children.push_back(parse_type(value_position));
                        skip_newlines();
                        if (at(TokenKind::Comma))
                        {
                            advance();
                            skip_newlines();
                            if (at_identifier("unbounded") && next_tok().kind == TokenKind::Greater)
                            {
                                type.unbounded = true;
                                advance();
                            }
                            else { type.size = parse_size_expression(); }
                            skip_newlines();
                        }
                        expect(TokenKind::Greater, "'>'");
                    }
                    else if (generic && text == "set")
                    {
                        type.kind = ast::TypeKind::Set;
                        advance();
                        advance();
                        skip_newlines();
                        type.children.push_back(parse_type(true));
                        skip_newlines();
                        expect(TokenKind::Greater, "'>'");
                    }
                    else if (generic && text == "map")
                    {
                        type.kind = ast::TypeKind::Map;
                        advance();
                        advance();
                        skip_newlines();
                        type.children.push_back(parse_type(true));
                        skip_newlines();
                        expect(TokenKind::Comma, "',' and the map's value type");
                        skip_newlines();
                        type.children.push_back(parse_type(value_position));
                        skip_newlines();
                        expect(TokenKind::Greater, "'>'");
                    }
                    else if (generic && text == "rolling")
                    {
                        type.kind = ast::TypeKind::Rolling;
                        advance();
                        advance();
                        skip_newlines();
                        type.children.push_back(parse_type(true));
                        skip_newlines();
                        expect(TokenKind::Comma, "',' and the rolling window's size");
                        skip_newlines();
                        type.size = parse_size_expression();
                        skip_newlines();
                        if (at(TokenKind::Comma))
                        {
                            advance();
                            skip_newlines();
                            type.min_size = parse_size_expression();
                            skip_newlines();
                        }
                        expect(TokenKind::Greater, "'>'");
                    }
                    else if (generic && text == "atomic")
                    {
                        type.kind = ast::TypeKind::Atomic;
                        advance();
                        advance();
                        skip_newlines();
                        type.children.push_back(parse_type(true));
                        skip_newlines();
                        expect(TokenKind::Greater, "'>'");
                    }
                    else
                    {
                        type.kind = ast::TypeKind::Named;
                        type.name = ast::Name{text, tok().range};
                        advance();
                    }
                }
                else { fail_expected("a type"); }
                type.range = range_from(begin);
                return module_.add(std::move(type));
            }

            /// A size argument is a constant expression parsed above the
            /// comparison level, so `>` always closes the type argument
            /// list; parenthesise a comparison if one is ever needed.
            ast::ExprId parse_size_expression() { return parse_binary(PrecAdditive); }

            // -------------------------------------------------- expressions

            ast::ExprId parse_expression() { return parse_binary(PrecOr); }

            ast::ExprId parse_binary(int min_precedence)
            {
                ast::ExprId lhs = parse_unary();
                while (true)
                {
                    // A line that begins with a binary operator continues
                    // the expression of the previous line.
                    const std::size_t save = pos_;
                    skip_newlines();
                    const auto info = binary_info(tok().kind);
                    if (!info || info->precedence < min_precedence)
                    {
                        pos_ = save;
                        break;
                    }
                    advance();
                    skip_newlines();
                    ast::ExprId rhs = parse_binary(info->precedence + 1);
                    SourceRange range = module_.expr(lhs).range.join(module_.expr(rhs).range);
                    lhs = module_.add(ast::Expr{range, ast::Binary{info->op, lhs, rhs}});
                }
                return lhs;
            }

            ast::ExprId parse_unary()
            {
                if (at(TokenKind::Minus) || at(TokenKind::Bang))
                {
                    const Token &token = advance();
                    const auto   op    = token.kind == TokenKind::Minus ? ast::UnaryOp::Negate : ast::UnaryOp::Not;
                    ast::ExprId  operand = parse_unary();
                    return module_.add(
                        ast::Expr{token.range.join(module_.expr(operand).range), ast::Unary{op, operand}});
                }
                return parse_postfix();
            }

            ast::ExprId parse_postfix()
            {
                ast::ExprId expr = parse_primary();
                while (true)
                {
                    const std::uint32_t begin = module_.expr(expr).range.begin;
                    if (at(TokenKind::LParen))
                    {
                        ast::Call call;
                        call.callee    = expr;
                        call.arguments = parse_arguments();
                        expr           = module_.add(ast::Expr{range_from(begin), std::move(call)});
                    }
                    else if (at(TokenKind::LBracket))
                    {
                        advance();
                        skip_newlines();
                        ast::ExprId index = parse_expression();
                        skip_newlines();
                        expect(TokenKind::RBracket, "']'");
                        expr = module_.add(ast::Expr{range_from(begin), ast::Index{expr, index}});
                    }
                    else if (at(TokenKind::Dot))
                    {
                        advance();
                        ast::Name field = expect_name("a field name after '.'");
                        expr            = module_.add(ast::Expr{range_from(begin), ast::Field{expr, field}});
                    }
                    else { break; }
                }
                return expr;
            }

            /// `(` [argument {, argument} [,]] `)`; the opening paren is current.
            std::vector<ast::Argument> parse_arguments()
            {
                std::vector<ast::Argument> arguments;
                expect(TokenKind::LParen, "'('");
                skip_newlines();
                while (!at(TokenKind::RParen))
                {
                    arguments.push_back(parse_argument());
                    skip_newlines();
                    if (!at(TokenKind::Comma)) { break; }
                    advance();
                    skip_newlines();
                }
                expect(TokenKind::RParen, "',' or ')'");
                return arguments;
            }

            ast::Argument parse_argument()
            {
                ast::Argument argument;
                if (at(TokenKind::Identifier) && next_tok().kind == TokenKind::Colon)
                {
                    argument.name = expect_name("an argument name");
                    advance();
                    skip_newlines();
                }
                argument.value = parse_expression();
                return argument;
            }

            ast::ExprId parse_primary()
            {
                const Token &token = tok();
                switch (token.kind)
                {
                    case TokenKind::IntLiteral:
                        advance();
                        return module_.add(ast::Expr{token.range, ast::IntLiteral{token.int_value}});
                    case TokenKind::FloatLiteral:
                        advance();
                        return module_.add(ast::Expr{token.range, ast::FloatLiteral{token.float_value}});
                    case TokenKind::StringLiteral:
                        advance();
                        return module_.add(ast::Expr{token.range, ast::StringLiteral{token.string_value}});
                    case TokenKind::TemporalLiteral:
                    {
                        // An invalid literal already carries a diagnostic;
                        // its node holds the default value.
                        ast::TemporalLiteral literal;
                        if (token.temporal_value) { literal.value = *token.temporal_value; }
                        advance();
                        return module_.add(ast::Expr{token.range, std::move(literal)});
                    }
                    case TokenKind::KwTrue:
                    case TokenKind::KwFalse:
                        advance();
                        return module_.add(ast::Expr{token.range, ast::BoolLiteral{token.kind == TokenKind::KwTrue}});
                    case TokenKind::Placeholder: advance(); return module_.add(ast::Expr{token.range, ast::Placeholder{}});
                    case TokenKind::Identifier:
                    {
                        ast::Name name = expect_name("a name");
                        if (at(TokenKind::ColonColon))
                        {
                            advance();
                            ast::Name member = expect_name("a declaration name after '::'");
                            return module_.add(ast::Expr{name.range.join(member.range), ast::QualifiedRef{name, member}});
                        }
                        return module_.add(ast::Expr{name.range, ast::NameRef{name}});
                    }
                    case TokenKind::LParen: return parse_paren();
                    case TokenKind::LBracket: return parse_sequence();
                    case TokenKind::KwFn: return parse_anonymous_fn();
                    case TokenKind::KwIf: return parse_if();
                    case TokenKind::KwEval: return parse_eval();
                    case TokenKind::LBrace:
                    {
                        ast::BlockId block = parse_block();
                        return module_.add(ast::Expr{module_.block(block).range, ast::BlockExpr{block}});
                    }
                    default: fail_expected("an expression");
                }
            }

            /// Grouping or a tuple literal: `(e)` is `e`; `(e,)` and
            /// `(a, b)` are tuples.
            ast::ExprId parse_paren()
            {
                const std::uint32_t begin = tok().range.begin;
                advance();
                skip_newlines();
                if (at(TokenKind::RParen)) { fail(here(), "expected an expression; '()' is not a value"); }
                ast::ExprId first = parse_expression();
                skip_newlines();
                if (at(TokenKind::RParen))
                {
                    advance();
                    return first;
                }
                expect(TokenKind::Comma, "',' or ')'");
                ast::TupleLiteral tuple;
                tuple.elements.push_back(first);
                skip_newlines();
                while (!at(TokenKind::RParen))
                {
                    tuple.elements.push_back(parse_expression());
                    skip_newlines();
                    if (!at(TokenKind::Comma)) { break; }
                    advance();
                    skip_newlines();
                }
                expect(TokenKind::RParen, "')'");
                return module_.add(ast::Expr{range_from(begin), std::move(tuple)});
            }

            /// `[e, ...]` or `[key: e, ...]`; a timed key is a duration or
            /// datetime literal directly followed by `:`.
            ast::ExprId parse_sequence()
            {
                const std::uint32_t begin = tok().range.begin;
                advance();
                ast::SequenceLiteral sequence;
                skip_newlines();
                while (!at(TokenKind::RBracket))
                {
                    ast::SequenceElement element;
                    if (at(TokenKind::TemporalLiteral) && next_tok().kind == TokenKind::Colon)
                    {
                        element.key = parse_primary();
                        advance();
                        skip_newlines();
                    }
                    element.value = parse_expression();
                    sequence.elements.push_back(element);
                    skip_newlines();
                    if (!at(TokenKind::Comma)) { break; }
                    advance();
                    skip_newlines();
                }
                expect(TokenKind::RBracket, "',' or ']'");
                return module_.add(ast::Expr{range_from(begin), std::move(sequence)});
            }

            ast::ExprId parse_anonymous_fn()
            {
                const std::uint32_t begin = tok().range.begin;
                advance();
                ast::AnonymousFn fn;
                expect(TokenKind::LParen, "'(' after 'fn'");
                skip_newlines();
                while (!at(TokenKind::RParen))
                {
                    ast::AnonymousParameter parameter;
                    parameter.name = expect_name("a parameter name");
                    if (at(TokenKind::Colon))
                    {
                        advance();
                        skip_newlines();
                        parameter.type = parse_type(false);
                    }
                    fn.parameters.push_back(std::move(parameter));
                    skip_newlines();
                    if (!at(TokenKind::Comma)) { break; }
                    advance();
                    skip_newlines();
                }
                expect(TokenKind::RParen, "',' or ')'");
                if (continue_with(TokenKind::Arrow))
                {
                    advance();
                    skip_newlines();
                    fn.result = parse_type(false);
                }
                if (!continue_with(TokenKind::FatArrow)) { fail_expected("'=>' and the function's expression"); }
                advance();
                skip_newlines();
                fn.body = parse_expression();
                return module_.add(ast::Expr{range_from(begin), std::move(fn)});
            }

            /// `if c { } [else ({ } | if ...)]`; `else` may start the next line.
            ast::ExprId parse_if()
            {
                const std::uint32_t begin = tok().range.begin;
                advance();
                ast::If node;
                node.condition  = parse_expression();
                node.then_block = parse_block();
                if (continue_with(TokenKind::KwElse))
                {
                    advance();
                    if (at(TokenKind::KwIf)) { node.otherwise = parse_if(); }
                    else
                    {
                        ast::BlockId block = parse_block();
                        node.otherwise     = module_.add(ast::Expr{module_.block(block).range, ast::BlockExpr{block}});
                    }
                }
                return module_.add(ast::Expr{range_from(begin), node});
            }

            /// `eval(callee, argument, ...)`.
            ast::ExprId parse_eval()
            {
                const std::uint32_t begin = tok().range.begin;
                advance();
                ast::Eval eval;
                expect(TokenKind::LParen, "'(' after 'eval'");
                skip_newlines();
                eval.callee = parse_expression();
                skip_newlines();
                while (at(TokenKind::Comma))
                {
                    advance();
                    skip_newlines();
                    if (at(TokenKind::RParen)) { break; }
                    eval.arguments.push_back(parse_argument());
                    skip_newlines();
                }
                expect(TokenKind::RParen, "',' or ')'");
                return module_.add(ast::Expr{range_from(begin), std::move(eval)});
            }

            // ------------------------------------------------------- blocks

            ast::BlockId parse_block()
            {
                ast::Block          block;
                const std::uint32_t begin = tok().range.begin;
                expect(TokenKind::LBrace, "'{'");
                while (true)
                {
                    skip_newlines();
                    if (at(TokenKind::RBrace)) { break; }
                    if (at_end()) { fail(here(), "expected '}' to close the block, found end of file"); }
                    try
                    {
                        block.statements.push_back(parse_statement());
                        if (!at(TokenKind::RBrace)) { expect_terminator("a newline after the statement"); }
                    }
                    catch (const ParseError &)
                    {
                        synchronize_statement();
                    }
                }
                advance();
                block.range = range_from(begin);
                if (!block.statements.empty())
                {
                    const ast::Stmt &last = module_.stmt(block.statements.back());
                    if (const auto *expr_stmt = std::get_if<ast::ExprStmt>(&last.node)) { block.tail = expr_stmt->expr; }
                }
                return module_.add(std::move(block));
            }

            /// Skip to the end of the failed statement: the next newline or
            /// closing brace outside any bracket opened since the error.
            void synchronize_statement()
            {
                int depth = 0;
                while (!at_end())
                {
                    switch (tok().kind)
                    {
                        case TokenKind::Newline:
                            if (depth <= 0) { return; }
                            break;
                        case TokenKind::RBrace:
                            if (depth <= 0) { return; }
                            --depth;
                            break;
                        case TokenKind::LBrace:
                        case TokenKind::LParen:
                        case TokenKind::LBracket: ++depth; break;
                        case TokenKind::RParen:
                        case TokenKind::RBracket: --depth; break;
                        default: break;
                    }
                    ++pos_;
                }
            }

            ast::StmtId parse_statement()
            {
                const std::uint32_t begin = tok().range.begin;
                switch (tok().kind)
                {
                    case TokenKind::KwLet:
                    case TokenKind::KwVar:
                    {
                        ast::LocalDecl decl;
                        decl.mutable_ = advance().kind == TokenKind::KwVar;
                        decl.name     = expect_name("a variable name");
                        if (at(TokenKind::Colon))
                        {
                            advance();
                            skip_newlines();
                            decl.type = parse_type(false);
                        }
                        expect(TokenKind::Assign, "'=' and an initializer");
                        skip_newlines();
                        decl.init = parse_expression();
                        return module_.add(ast::Stmt{range_from(begin), std::move(decl)});
                    }
                    case TokenKind::KwState:
                    {
                        advance();
                        ast::StateDecl decl;
                        decl.name = expect_name("a state variable name");
                        if (at(TokenKind::Colon))
                        {
                            advance();
                            skip_newlines();
                            decl.type = parse_type(true);
                        }
                        expect(TokenKind::Assign, "'=' and an initializer");
                        skip_newlines();
                        decl.init = parse_expression();
                        return module_.add(ast::Stmt{range_from(begin), std::move(decl)});
                    }
                    case TokenKind::KwInject: return parse_inject();
                    case TokenKind::KwStart:
                    case TokenKind::KwStop:
                    {
                        ast::LifecycleBlock lifecycle;
                        lifecycle.is_stop = advance().kind == TokenKind::KwStop;
                        lifecycle.block   = parse_block();
                        return module_.add(ast::Stmt{range_from(begin), lifecycle});
                    }
                    case TokenKind::KwWhen:
                    {
                        advance();
                        ast::WhenStmt when;
                        when.condition = parse_expression();
                        when.block     = parse_block();
                        return module_.add(ast::Stmt{range_from(begin), when});
                    }
                    case TokenKind::KwFor:
                    {
                        advance();
                        ast::ForStmt loop;
                        loop.first = expect_name("a loop variable name");
                        if (at(TokenKind::Comma))
                        {
                            advance();
                            loop.second = expect_name("a loop variable name");
                        }
                        if (!at_identifier("in")) { fail_expected("'in' after the loop pattern"); }
                        advance();
                        loop.iterable = parse_expression();
                        loop.block    = parse_block();
                        return module_.add(ast::Stmt{range_from(begin), loop});
                    }
                    case TokenKind::KwReturn:
                    {
                        advance();
                        ast::ReturnStmt ret;
                        if (!at(TokenKind::Newline) && !at(TokenKind::RBrace) && !at_end())
                        {
                            ret.value = parse_expression();
                        }
                        return module_.add(ast::Stmt{range_from(begin), ret});
                    }
                    case TokenKind::KwAssert:
                    {
                        advance();
                        ast::AssertStmt assert_stmt;
                        assert_stmt.condition = parse_expression();
                        return module_.add(ast::Stmt{range_from(begin), assert_stmt});
                    }
                    default: break;
                }

                ast::ExprId expr = parse_expression();
                if (const auto op = assign_op(tok().kind))
                {
                    if (!is_place(expr))
                    {
                        fail(module_.expr(expr).range, "only a name, an index, or a field can be assigned to");
                    }
                    advance();
                    skip_newlines();
                    ast::ExprId value = parse_expression();
                    return module_.add(ast::Stmt{range_from(begin), ast::AssignStmt{*op, expr, value}});
                }
                return module_.add(ast::Stmt{range_from(begin), ast::ExprStmt{expr}});
            }

            /// `inject a, b` with newlines allowed after `inject` and
            /// around commas; a trailing comma is followed by a line that
            /// does not start with an identifier.
            ast::StmtId parse_inject()
            {
                const std::uint32_t begin = tok().range.begin;
                advance();
                ast::InjectDecl inject;
                skip_newlines();
                while (true)
                {
                    inject.names.push_back(expect_name("an injectable name"));
                    if (!at(TokenKind::Comma)) { break; }
                    advance();
                    if (tok_past_newline().kind != TokenKind::Identifier) { break; }
                    skip_newlines();
                }
                return module_.add(ast::Stmt{range_from(begin), std::move(inject)});
            }

            [[nodiscard]] bool is_place(ast::ExprId id) const noexcept
            {
                const ast::ExprNode &node = module_.expr(id).node;
                if (std::holds_alternative<ast::NameRef>(node)) { return true; }
                if (const auto *index = std::get_if<ast::Index>(&node)) { return is_place(index->target); }
                if (const auto *field = std::get_if<ast::Field>(&node)) { return is_place(field->target); }
                return false;
            }

            DiagnosticSink    &diagnostics_;
            std::vector<Token> tokens_;
            std::size_t        pos_{0};
            std::uint32_t      last_end_{0};
            bool               seen_module_{false};
            ast::Module        module_{};
        };
    }  // namespace

    ast::Module parse(const SourceFile &file, DiagnosticSink &diagnostics)
    {
        return Parser{file, diagnostics}.run();
    }
}  // namespace hgl::syntax
