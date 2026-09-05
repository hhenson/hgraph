#ifndef HGL_SYNTAX_AST_PROJECTION_H
#define HGL_SYNTAX_AST_PROJECTION_H

#include "syntax/ast.h"
#include "syntax/diagnostic.h"
#include "syntax/lexer.h"
#include "syntax/syntax_tree.h"

namespace hgl::syntax
{
    /// Lower one clean, source-accurate syntax tree into the compact semantic
    /// syntax arena consumed by the existing name and type resolver.
    ///
    /// The source tree is the grammatical authority. This pass does not parse
    /// tokens or recover syntax; it only converts explicit production shapes
    /// and performs the few context checks historically owned by the AST
    /// builder (for example, assignable places and const-only defaults).
    [[nodiscard]] ast::Module project_ast(const SyntaxTree &tree, const LexResult &lexed, DiagnosticSink &diagnostics);
}  // namespace hgl::syntax

#endif  // HGL_SYNTAX_AST_PROJECTION_H
