#ifndef HGL_SYNTAX_LEXER_H
#define HGL_SYNTAX_LEXER_H

#include "syntax/ast.h"
#include "syntax/diagnostic.h"
#include "syntax/source.h"
#include "syntax/token.h"

#include <vector>

namespace hgl::syntax
{
    struct LexResult
    {
        std::vector<Token>        tokens;    ///< ends with EndOfFile
        std::vector<ast::Comment> comments;  ///< `//` trivia in source order
    };

    /// Tokenize a whole file (developer guide, "Lexical rules" and
    /// "Literals"). Never throws; malformed input produces `Error` tokens or
    /// `TemporalLiteral` tokens without a value, with a diagnostic each.
    /// A run of line terminators (with any comments and blank lines between
    /// them) is one `Newline` token; the final token is `EndOfFile`.
    [[nodiscard]] LexResult lex(const SourceFile &file, DiagnosticSink &diagnostics);
}  // namespace hgl::syntax

#endif  // HGL_SYNTAX_LEXER_H
