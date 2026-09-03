#ifndef HGL_SYNTAX_AST_PRINTER_H
#define HGL_SYNTAX_AST_PRINTER_H

#include "syntax/ast.h"

#include <string>

namespace hgl::syntax
{
    /// A deterministic, indentation-structured textual dump of the tree for
    /// snapshot tests and `hgl check --dump-ast`. One node per line:
    /// `Kind [begin..end) details`, children indented by two spaces.
    /// Temporal literals print their canonical spelling.
    [[nodiscard]] std::string print_ast(const ast::Module &module);
}  // namespace hgl::syntax

#endif  // HGL_SYNTAX_AST_PRINTER_H
