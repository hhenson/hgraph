#ifndef HGL_SYNTAX_PARSER_H
#define HGL_SYNTAX_PARSER_H

#include "syntax/ast.h"
#include "syntax/diagnostic.h"
#include "syntax/source.h"

namespace hgl::syntax
{
    /// Parse a whole file into an `ast::Module` (developer guide,
    /// "Compilation-unit grammar" and following). Recovers at closing
    /// braces, statements, and declaration boundaries so several diagnostics
    /// can be reported from one run. The returned module is complete for every
    /// declaration and statement that recovered structurally.
    [[nodiscard]] ast::Module parse(const SourceFile &file, DiagnosticSink &diagnostics);
}  // namespace hgl::syntax

#endif  // HGL_SYNTAX_PARSER_H
