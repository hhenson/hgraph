#include "syntax/formatter.h"

#include "syntax/ast_projection.h"
#include "syntax/lexer.h"
#include "syntax/parser.h"
#include "syntax/syntax_diagnostics.h"
#include "syntax/token_grammar.h"

#include <algorithm>
#include <map>
#include <vector>

namespace hgl::syntax
{
    namespace
    {
        struct Edit
        {
            std::size_t begin, end;
            std::string text;
        };

        bool whitespace(std::string_view text) { return text.find_first_not_of(" \t\r\n") == std::string_view::npos; }

        std::size_t line_start(std::string_view text, std::size_t at) {
            const auto found = at == 0 ? std::string_view::npos : text.rfind('\n', at - 1);
            return found == std::string_view::npos ? 0 : found + 1;
        }

        std::string indent_at(std::string_view text, std::size_t at) {
            const auto start = line_start(text, at);
            auto       end   = start;
            while (end < at && (text[end] == ' ' || text[end] == '\t')) { ++end; }
            return std::string{text.substr(start, end - start)};
        }

        SourceRange significant_range(const SyntaxNode &node, const LexResult &lexed) {
            SourceRange range{node.range.end, node.range.begin};
            for (const auto &token : lexed.tokens) {
                if (token.range.begin < node.range.begin) { continue; }
                if (token.range.begin >= node.range.end) { break; }
                if (token.kind == TokenKind::Newline || token.kind == TokenKind::EndOfFile) { continue; }
                range.begin = std::min(range.begin, token.range.begin);
                range.end   = std::max(range.end, token.range.end);
            }
            return range;
        }

        SyntaxNodeId scope_of(const SyntaxTree &tree, SyntaxNodeId id) {
            for (auto parent = tree.nodes[id].parent; parent != no_syntax_node; parent = tree.nodes[parent].parent) {
                if (tree.nodes[parent].kind == SyntaxKind::TestContext) { return parent; }
            }
            return tree.root;
        }

        bool import(const SourceFile &file, SourceRange range) {
            const auto text = file.slice(range);
            return text.starts_with("use ") || text.starts_with("use\t") || text.starts_with("cpp include");
        }
    }  // namespace

    std::optional<std::string> format_declarations(const SourceFile &file, DiagnosticSink &diagnostics) {
        const auto lexed  = lex(file, diagnostics);
        const auto parsed = parse_source_syntax(file, lexed);
        report_syntax_issues(parsed.tree, lexed, diagnostics);
        if (parsed.tree.has_root()) { (void)project_ast(parsed.tree, lexed, diagnostics); }
        if (diagnostics.has_errors() || !parsed.grammar.accepted || parsed.grammar.recovered) { return std::nullopt; }

        const auto                                       text    = file.text();
        const auto                                      &tree    = parsed.tree;
        const std::string                                newline = text.find("\r\n") != std::string_view::npos ? "\r\n" : "\n";
        std::vector<Edit>                                edits;
        std::map<SyntaxNodeId, std::vector<SourceRange>> scopes;
        for (SyntaxNodeId id = 0; id < tree.nodes.size(); ++id) {
            const auto kind = tree.nodes[id].kind;
            if (kind == SyntaxKind::ModuleDecl || kind == SyntaxKind::Declaration || kind == SyntaxKind::TestContextItem) {
                const auto range = significant_range(tree.nodes[id], lexed);
                if (!range.empty()) { scopes[scope_of(tree, id)].push_back(range); }
            }
        }
        for (auto &[scope, definitions] : scopes) {
            (void)scope;
            std::ranges::sort(definitions, {}, &SourceRange::begin);
            for (std::size_t i = 1; i < definitions.size(); ++i) {
                const auto  previous = definitions[i - 1];
                const auto  next     = definitions[i];
                std::size_t begin = previous.end, end = next.begin;
                // A same-line comment trails the previous declaration. The first
                // standalone comment belongs to the next declaration's preamble.
                for (const auto &comment : lexed.comments) {
                    if (comment.range.begin < begin || comment.range.end > next.begin) { continue; }
                    if (text.substr(begin, comment.range.begin - begin).find('\n') == std::string_view::npos) {
                        begin = comment.range.end;
                    } else {
                        end = comment.range.begin;
                        break;
                    }
                }
                if (!whitespace(text.substr(begin, end - begin))) { continue; }
                const auto breaks = import(file, previous) && import(file, next) ? newline : newline + newline;
                edits.push_back({begin, end, breaks + indent_at(text, end)});
            }
        }
        for (SyntaxNodeId id = 0; id < tree.nodes.size(); ++id) {
            const auto kind = tree.nodes[id].kind;
            if (kind != SyntaxKind::RequiresClause && kind != SyntaxKind::OperatorProperties) { continue; }
            const auto range = significant_range(tree.nodes[id], lexed);
            if (range.empty()) { continue; }
            auto owner = tree.nodes[id].parent;
            while (owner != no_syntax_node && tree.nodes[owner].kind != SyntaxKind::Declaration &&
                   tree.nodes[owner].kind != SyntaxKind::TestContextItem) {
                owner = tree.nodes[owner].parent;
            }
            if (owner == no_syntax_node) { continue; }
            const auto  owner_range = significant_range(tree.nodes[owner], lexed);
            const auto  indent      = indent_at(text, owner_range.begin) + "    ";
            const auto  old_indent  = indent_at(text, range.begin);
            std::size_t begin       = range.begin;
            for (const auto &fragment : lexed.fragments) {
                if (fragment.range.end > range.begin) { break; }
                if (fragment.kind != SourceFragmentKind::Whitespace && fragment.kind != SourceFragmentKind::LineBreak) {
                    begin = fragment.range.end;
                }
            }
            if (whitespace(text.substr(begin, range.begin - begin))) { edits.push_back({begin, range.begin, newline + indent}); }
            // Shift continuation lines with their clause. Literal and block-comment
            // interiors are opaque: only lexical whitespace can be rewritten.
            for (const auto &fragment : lexed.fragments) {
                if (fragment.range.begin <= range.begin || fragment.range.begin >= range.end) { continue; }
                if (fragment.kind != SourceFragmentKind::Whitespace ||
                    line_start(text, fragment.range.begin) != fragment.range.begin) {
                    continue;
                }
                const auto existing = file.slice(fragment.range);
                if (existing.starts_with(old_indent)) {
                    edits.push_back(
                        {fragment.range.begin, fragment.range.end, indent + std::string{existing.substr(old_indent.size())}});
                }
            }
            // Unindented continuation tokens have no whitespace fragment.
            for (const auto &fragment : lexed.fragments) {
                if (fragment.range.begin <= range.begin || fragment.range.begin >= range.end) { continue; }
                if (fragment.kind != SourceFragmentKind::Token && fragment.kind != SourceFragmentKind::LineComment &&
                    fragment.kind != SourceFragmentKind::BlockComment) {
                    continue;
                }
                if (line_start(text, fragment.range.begin) == fragment.range.begin && old_indent.empty()) {
                    edits.push_back({fragment.range.begin, fragment.range.begin, indent});
                }
            }
        }
        // Keep a final line terminator, including for a comment-only file.
        if (!text.empty() && text.back() != '\n') { edits.push_back({text.size(), text.size(), newline}); }
        std::ranges::sort(edits, {}, &Edit::begin);
        std::string output;
        std::size_t cursor = 0;
        for (const auto &edit : edits) {
            if (edit.begin < cursor) {
                diagnostics.report(Category::Parse, {}, "overlapping formatter edits");
                return std::nullopt;
            }
            output.append(text.substr(cursor, edit.begin - cursor));
            output += edit.text;
            cursor = edit.end;
        }
        output.append(text.substr(cursor));
        const SourceFile formatted{file.path(), output};
        DiagnosticSink   verified;
        (void)parse(formatted, verified);
        if (verified.has_errors()) {
            diagnostics.report(Category::Parse, {}, "declaration formatting would change valid syntax");
            return std::nullopt;
        }
        return output;
    }
}  // namespace hgl::syntax
