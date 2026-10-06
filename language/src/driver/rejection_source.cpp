#include "driver/rejection_source.h"

#include "syntax/lexer.h"
#include "syntax/token_grammar.h"

#include <algorithm>
#include <map>
#include <ostream>
#include <regex>
#include <utility>

namespace hgl::driver
{
    namespace
    {
        using syntax::SyntaxKind;
        using syntax::TokenKind;

        struct Annotation
        {
            std::uint32_t        begin{}, end{};
            RejectionExpectation expected{};
        };

        bool opener(TokenKind kind) {
            return kind == TokenKind::LParen || kind == TokenKind::LBrace || kind == TokenKind::LBracket;
        }

        bool closer(TokenKind kind) {
            return kind == TokenKind::RParen || kind == TokenKind::RBrace || kind == TokenKind::RBracket;
        }

        bool pair(TokenKind left, TokenKind right) {
            return (left == TokenKind::LParen && right == TokenKind::RParen) ||
                   (left == TokenKind::LBrace && right == TokenKind::RBrace) ||
                   (left == TokenKind::LBracket && right == TokenKind::RBracket);
        }
    }  // namespace

    RejectionSource inspect_rejection_source(const syntax::SourceFile &file, std::ostream &errors) {
        RejectionSource result;
        auto            invalid = [&](std::uint32_t offset, std::string_view message) {
            result.valid = false;
            errors << file.path() << ':' << file.location(offset).line << ": " << message << '\n';
        };
        syntax::DiagnosticSink                        lexical_diagnostics;
        const auto                                    lexed = syntax::lex(file, lexical_diagnostics);
        const std::map<std::string, syntax::Category> codes{
            {"syntax.expected_token", syntax::Category::Parse}, {"rolling.size_kind", syntax::Category::Type},
            {"rolling.size_bounds", syntax::Category::Type},    {"yield.time_type", syntax::Category::Type},
            {"test.raises_code", syntax::Category::Type},       {"test.statement_phase", syntax::Category::Phase},
            {"delta.unsupported_shape", syntax::Category::Shape},
            {"delta.type_mismatch", syntax::Category::Type},
            {"delta.argument_name", syntax::Category::Name},
            {"delta.duplicate_argument", syntax::Category::Name},
            {"delta.entry_constant", syntax::Category::Type},
            {"delta.entry_type", syntax::Category::Type},
            {"delta.duplicate_entry", syntax::Category::Type},
            {"delta.index_bounds", syntax::Category::Type},
            {"delta.overlap", syntax::Category::Type},
        };
        const std::regex pattern{
            R"re(^#[ \t]*expect-error[ \t]*\([ \t]*([a-z]+(?:-[a-z]+)*)[ \t]*,[ \t]*("(?:[^"\\]|\\.)*")[ \t]*\)[ \t\r]*$)re"};
        std::vector<std::uint32_t> lines{0};
        for (std::uint32_t i = 0; i < file.text().size(); ++i) {
            if (file.text()[i] == '\n') { lines.push_back(i + 1U); }
        }
        std::vector<Annotation> annotations;
        for (const auto &fragment : lexed.fragments) {
            if (fragment.kind != syntax::SourceFragmentKind::LineComment) { continue; }
            const std::string comment{file.slice(fragment.range)};
            std::string_view  body{comment};
            body.remove_prefix(1);
            const auto first = body.find_first_not_of(" \t");
            if (first == std::string_view::npos || !body.substr(first).starts_with("expect-error")) { continue; }
            const auto  at     = file.location(fragment.range.begin);
            const auto  prefix = file.line_text(at.line).substr(0, at.column - 1U);
            std::smatch match;
            if (prefix.find_first_not_of(" \t") != std::string_view::npos || !std::regex_match(comment, match, pattern) ||
                at.line >= lines.size()) {
                invalid(fragment.range.begin, "invalid expect-error annotation");
                continue;
            }
            syntax::SourceFile     literal{file.path(), match[2].str()};
            syntax::DiagnosticSink literal_diagnostics;
            const auto             tokens = syntax::lex(literal, literal_diagnostics);
            const auto            &code   = tokens.tokens.front().string_value;
            const auto             known  = codes.find(code);
            if (literal_diagnostics.has_errors() || known == codes.end() ||
                syntax::category_name(known->second) != match[1].str()) {
                invalid(fragment.range.begin, "unknown or incompatible expected diagnostic category/code");
                continue;
            }
            const auto end = at.line + 1U < lines.size() ? lines[at.line + 1U] : static_cast<std::uint32_t>(file.text().size());
            annotations.push_back({lines[at.line], end, {at.line + 1U, known->second, code}});
        }
        if (!result.valid || annotations.empty()) { return result; }
        if (lexical_diagnostics.has_errors()) {
            errors << lexical_diagnostics.render(file);
            result.valid = false;
            return result;
        }

        const auto                 parsed = syntax::parse_source_syntax(file, lexed);
        std::vector<RejectionCase> owners;
        for (const auto &node : parsed.tree.nodes) {
            if (node.kind != SyntaxKind::DeclarationLine && node.kind != SyntaxKind::TestContextItem) { continue; }
            // An unnamed context is only a wrapper; its inner items own cases.
            const syntax::SyntaxNode *semantic = &node;
            while (semantic->kind == SyntaxKind::DeclarationLine || semantic->kind == SyntaxKind::Declaration ||
                   semantic->kind == SyntaxKind::TestContextItem) {
                const auto child = std::find_if(semantic->children.begin(), semantic->children.end(),
                                                [](const auto &item) { return item.kind == syntax::SyntaxChildKind::Node; });
                if (child == semantic->children.end()) { break; }
                semantic = &parsed.tree.nodes[child->index];
            }
            if (semantic->kind == SyntaxKind::TestContext) { continue; }
            auto begin = std::lower_bound(lexed.tokens.begin(), lexed.tokens.end(), node.range.begin,
                                          [](const auto &token, auto offset) { return token.range.begin < offset; });
            auto end   = std::lower_bound(begin, lexed.tokens.end(), node.range.end,
                                          [](const auto &token, auto offset) { return token.range.begin < offset; });
            while (end != begin && ((end - 1)->kind == TokenKind::Newline || (end - 1)->kind == TokenKind::EndOfFile)) { --end; }
            if (begin == end) { continue; }
            RejectionCase owner{{begin->range.begin, (end - 1)->range.end}, {}, begin->kind == TokenKind::KwTest, {}};
            for (const auto &child : semantic->children) {
                if (child.kind != syntax::SyntaxChildKind::Node) { continue; }
                const auto &name = parsed.tree.nodes[child.index];
                if (name.kind == SyntaxKind::Name) {
                    owner.name = file.slice(name.range);
                    break;
                }
            }
            if (owner.named_test && owner.name.empty() && begin + 1 != end && (begin + 1)->kind == TokenKind::Identifier) {
                owner.name = (begin + 1)->text;
            }
            owners.push_back(std::move(owner));
        }
        if (owners.empty() && !parsed.tree.has_root()) {
            // A fatal parse can discard its tree. A sole expression-bodied
            // declaration after the module header still ends reliably at EOF;
            // delimiter validation below rejects any possible hidden neighbour.
            auto begin = std::find_if(lexed.tokens.begin(), lexed.tokens.end(),
                                      [](const auto &token) { return token.kind == TokenKind::Newline; });
            if (begin != lexed.tokens.end()) { ++begin; }
            auto end = lexed.tokens.end();
            while (end != begin && ((end - 1)->kind == TokenKind::EndOfFile || (end - 1)->kind == TokenKind::Newline)) { --end; }
            if (begin != end && (begin->kind == TokenKind::KwFn || begin->kind == TokenKind::KwConst)) {
                owners.push_back({{begin->range.begin, (end - 1)->range.end}, {}, false, {}});
                auto name = begin;
                if (name->kind == TokenKind::KwConst) { ++name; }
                if (name != end && name->kind == TokenKind::KwFn && ++name != end && name->kind == TokenKind::Identifier) {
                    owners.back().name = name->text;
                }
            }
        }
        std::ranges::sort(owners, {}, [](const auto &owner) { return owner.range.begin; });
        for (const auto &annotation : annotations) {
            auto target = annotation.begin;
            while (target < annotation.end &&
                   (file.text()[target] == ' ' || file.text()[target] == '\t' || file.text()[target] == '\r')) {
                ++target;
            }
            // A declaration must begin the target line, or already enclose it.
            // Do not donate a context-header annotation to a later inner item
            // merely because both happen to occupy the same physical line.
            auto found = std::upper_bound(owners.begin(), owners.end(), target,
                                          [](auto offset, const auto &owner) { return offset < owner.range.begin; });
            if (found == owners.begin() || (--found)->range.end <= target) {
                invalid(annotation.begin, "expect-error annotation has no enclosing declaration or named test");
                continue;
            }
            found->expectations.push_back(annotation.expected);
        }

        // Each owner must be independently delimited. In particular, recovery
        // at a declaration keyword is not sufficient evidence of a boundary.
        for (auto &owner : owners) {
            if (owner.expectations.empty()) { continue; }
            std::vector<const syntax::Token *> stack;
            bool                               reliable = true;
            auto begin   = std::lower_bound(lexed.tokens.begin(), lexed.tokens.end(), owner.range.begin,
                                            [](const auto &token, auto offset) { return token.range.begin < offset; });
            auto end     = std::lower_bound(begin, lexed.tokens.end(), owner.range.end,
                                            [](const auto &token, auto offset) { return token.range.begin < offset; });
            bool seen_fn = false;
            for (auto token = begin; token != end; ++token) {
                if (opener(token->kind)) { stack.push_back(&*token); }
                if (closer(token->kind)) {
                    if (stack.empty() || !pair(stack.back()->kind, token->kind)) {
                        reliable = false;
                        break;
                    }
                    stack.pop_back();
                }
                // Named tests and declarations cannot be nested in an owner.
                // Anonymous `fn (` expressions are allowed by this guard.
                if (token != begin && token->kind == TokenKind::KwTest) {
                    reliable = false;
                    break;
                }
                if (token->kind == TokenKind::KwFn) {
                    if (seen_fn && token + 1 != end && (token + 1)->kind == TokenKind::Identifier) {
                        reliable = false;
                        break;
                    }
                    seen_fn = true;
                }
            }
            if (!stack.empty()) {
                // A terminal expression-bodied declaration with a missing
                // parameter close still has an unambiguous end at EOF.
                const bool terminal = std::all_of(end, lexed.tokens.end(), [](const auto &token) {
                    return token.kind == TokenKind::Newline || token.kind == TokenKind::EndOfFile;
                });
                const bool missing_parameter_close =
                    stack.size() == 1U && stack.front()->kind == TokenKind::LParen &&
                    std::any_of(begin, end, [](const auto &token) { return token.kind == TokenKind::FatArrow; });
                reliable = reliable && terminal && missing_parameter_close;
            }
            if (!reliable) { invalid(owner.range.begin, "ambiguous rejection declaration boundary"); }
            result.cases.push_back(std::move(owner));
        }
        return result;
    }
}  // namespace hgl::driver
