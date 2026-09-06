#ifndef HGL_SYNTAX_SYNTAX_DIAGNOSTICS_H
#define HGL_SYNTAX_SYNTAX_DIAGNOSTICS_H

#include "syntax/diagnostic.h"
#include "syntax/lexer.h"
#include "syntax/syntax_tree.h"

namespace hgl::syntax
{
    /// Translate parser-independent syntax issues into source diagnostics.
    /// Recovery artifacts are suppressed here so callers see one useful
    /// message for each malformed construct rather than parser mechanics.
    void report_syntax_issues(const SyntaxTree &tree, const LexResult &lexed, DiagnosticSink &diagnostics);
}  // namespace hgl::syntax

#endif  // HGL_SYNTAX_SYNTAX_DIAGNOSTICS_H
