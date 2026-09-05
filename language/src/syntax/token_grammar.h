#ifndef HGL_SYNTAX_TOKEN_GRAMMAR_H
#define HGL_SYNTAX_TOKEN_GRAMMAR_H

#include "syntax/lexer.h"
#include "syntax/syntax_tree.h"

#include <cstddef>
#include <limits>
#include <span>

namespace hgl::syntax
{
    /// Result of parsing the lexer's token stream with the declarative HGL
    /// grammar. This deliberately exposes no parser-library types.
    struct GrammarResult
    {
        bool        accepted{false};
        bool        recovered{false};
        std::size_t error_count{0};
        std::size_t syntax_node_count{0};
        std::size_t first_error_token{std::numeric_limits<std::size_t>::max()};
    };

    struct SyntaxParseResult
    {
        GrammarResult grammar{};
        SyntaxTree    tree{};
    };

    /// Parse a complete, EOF-terminated token stream without materializing a
    /// source tree. Intended for grammar conformance tests and probes.
    [[nodiscard]] GrammarResult parse_token_grammar(std::span<const Token> tokens);

    /// Parse the lossless lexer result and materialize the HGL-owned source
    /// tree. Parser-library storage is discarded before this call returns.
    [[nodiscard]] SyntaxParseResult parse_source_syntax(const SourceFile &file, const LexResult &lexed);
}  // namespace hgl::syntax

#endif  // HGL_SYNTAX_TOKEN_GRAMMAR_H
