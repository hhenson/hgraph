#include "syntax/syntax_diagnostics.h"

#include <optional>
#include <string>

namespace hgl::syntax
{
    namespace
    {
        [[nodiscard]] const Token &token_at(const LexResult &lexed, std::uint32_t offset) noexcept {
            for (const Token &token : lexed.tokens) {
                if ((token.range.begin <= offset && offset < token.range.end) || token.range.begin >= offset) { return token; }
            }
            return lexed.tokens.back();
        }

        [[nodiscard]] const Token *previous_token(const LexResult &lexed, const Token &token) noexcept {
            const Token *previous = nullptr;
            for (const Token &candidate : lexed.tokens) {
                if (&candidate == &token) { return previous; }
                if (candidate.kind != TokenKind::Newline) { previous = &candidate; }
            }
            return previous;
        }

        [[nodiscard]] const Token *immediately_previous_token(const LexResult &lexed, const Token &token) noexcept {
            const Token *previous = nullptr;
            for (const Token &candidate : lexed.tokens) {
                if (&candidate == &token) { return previous; }
                previous = &candidate;
            }
            return previous;
        }

        [[nodiscard]] std::string describe(const Token &token) {
            if (token.kind == TokenKind::EndOfFile) { return "end of file"; }
            if (token.kind == TokenKind::Newline) { return "newline"; }
            return "'" + std::string{token.text} + "'";
        }

        [[nodiscard]] std::string expected_token(TokenKind kind) {
            if (kind == TokenKind::Identifier) { return "a name"; }
            return std::string{token_kind_name(kind)};
        }

        [[nodiscard]] std::string missing_message(const SyntaxIssue &issue, const LexResult &lexed, const Token &found) {
            const std::string actual = describe(found);
            if (issue.context == SyntaxKind::ModuleDecl && issue.expected == TokenKind::KwModule) {
                return "expected a 'module' declaration at the start of the file";
            }
            if (issue.context == SyntaxKind::UseDecl && issue.expected == TokenKind::LBrace) {
                return "expected '{' after '::', found " + actual;
            }
            if ((issue.context == SyntaxKind::LocalDecl || issue.context == SyntaxKind::StateDecl) &&
                issue.expected == TokenKind::Assign) {
                return "expected '=' and an initializer, found " + actual;
            }
            if (issue.context == SyntaxKind::ForStmt && issue.expected == TokenKind::Identifier) {
                return "expected 'in' after the loop pattern, found " + actual;
            }
            if (issue.context == SyntaxKind::Block && issue.expected == TokenKind::RBrace) {
                return "expected '}' to close the block, found " + actual;
            }
            if (issue.context == SyntaxKind::Parameter && issue.expected == TokenKind::Colon) {
                return "expected ':' and the parameter's type, found " + actual;
            }
            if (issue.context == SyntaxKind::Signature && issue.expected == TokenKind::RParen) {
                if (is_keyword(found.kind)) {
                    return "'" + std::string{found.text} + "' is a reserved word and cannot be used as a parameter name";
                }
                const Token *previous = previous_token(lexed, found);
                if (previous != nullptr && previous->kind == TokenKind::LParen) {
                    return "expected a parameter name, found " + actual;
                }
                return "expected ',' or ')', found " + actual;
            }
            const std::string expected = issue.expected ? expected_token(*issue.expected) : "syntax";
            return "expected " + expected + ", found " + actual;
        }

        [[nodiscard]] std::string unexpected_message(const SyntaxIssue &issue, const LexResult &lexed, const Token &found) {
            const std::string actual = describe(found);
            switch (issue.context) {
                case SyntaxKind::Module:
                    if (found.kind == TokenKind::KwModule) { return "a file has one 'module' declaration"; }
                    return "expected a declaration, found " + actual;
                case SyntaxKind::Type: return "expected a type, found " + actual;
                case SyntaxKind::TupleType: return "a tuple type has at least one element";
                case SyntaxKind::TupleElement:
                case SyntaxKind::UnaryExpression:
                    if (found.kind == TokenKind::RParen) {
                        const Token *previous = previous_token(lexed, found);
                        if (previous != nullptr && previous->kind == TokenKind::LParen) {
                            return "expected an expression; '()' is not a value";
                        }
                    }
                    return "expected an expression, found " + actual;
                case SyntaxKind::FunctionDecl:
                    if (is_keyword(found.kind) && found.kind != TokenKind::Newline) {
                        return "'" + std::string{found.text} + "' is a reserved word and cannot be used as a function name";
                    }
                    return "expected '=>' or '{' to begin the function body, found " + actual;
                case SyntaxKind::LocalDecl:
                    if (is_keyword(found.kind)) {
                        return "'" + std::string{found.text} + "' is a reserved word and cannot be used as a variable name";
                    }
                    break;
                case SyntaxKind::UseDecl:
                    if (is_keyword(found.kind)) {
                        return "'" + std::string{found.text} + "' is a reserved word and cannot be used as an imported name";
                    }
                    break;
                case SyntaxKind::LineEnd:
                    if (found.kind == TokenKind::FatArrow || found.kind == TokenKind::LBrace) {
                        return "an operator declaration has no body; implement it with 'impl fn'";
                    }
                    return "expected a newline after the declaration, found " + actual;
                case SyntaxKind::BlockItem: return "expected a newline after the statement, found " + actual;
                case SyntaxKind::CommaSeparator: return "expected ',' and the map's value type, found " + actual;
                default: break;
            }
            return "unexpected " + actual + " while parsing " + std::string{syntax_kind_name(issue.context)};
        }
    }  // namespace

    void report_syntax_issues(const SyntaxTree &tree, const LexResult &lexed, DiagnosticSink &diagnostics) {
        std::optional<std::uint32_t> previous_offset;
        bool                         emitted = false;
        for (const SyntaxIssue &issue : tree.issues) {
            if (issue.context == SyntaxKind::DeclarationLine) { continue; }
            if (issue.context == SyntaxKind::Module && emitted) { continue; }
            if (previous_offset == issue.range.begin) { continue; }

            const Token *found = &token_at(lexed, issue.range.begin);
            if (issue.kind == SyntaxIssueKind::Unexpected && issue.context == SyntaxKind::FunctionDecl) {
                if (const Token *previous = immediately_previous_token(lexed, *found);
                    previous != nullptr && previous->kind == TokenKind::Newline) {
                    found = previous;
                }
            }
            if (found->kind == TokenKind::Error) { continue; }
            const std::string message = issue.kind == SyntaxIssueKind::Missing ? missing_message(issue, lexed, *found)
                                                                               : unexpected_message(issue, lexed, *found);
            const SourceRange range =
                found->kind == TokenKind::Newline
                    ? SourceRange{found->range.begin, found->range.begin}
                    : (issue.range.empty() ? SourceRange{found->range.begin, found->range.begin} : issue.range);
            diagnostics.report(Category::Parse, range, message);
            previous_offset = issue.range.begin;
            emitted         = true;
        }
    }
}  // namespace hgl::syntax
