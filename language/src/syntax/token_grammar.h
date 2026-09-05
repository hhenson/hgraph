#ifndef HGL_SYNTAX_TOKEN_GRAMMAR_H
#define HGL_SYNTAX_TOKEN_GRAMMAR_H

#include "syntax/token.h"

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

    /// Parse a complete, EOF-terminated token stream. The current AST parser
    /// remains the projection path while the lexy grammar is introduced in
    /// verified slices; this entry point is the shadow/conformance boundary.
    [[nodiscard]] GrammarResult parse_token_grammar(std::span<const Token> tokens);
}  // namespace hgl::syntax

#endif  // HGL_SYNTAX_TOKEN_GRAMMAR_H
