#ifndef HGL_SYNTAX_FORMATTER_H
#define HGL_SYNTAX_FORMATTER_H

#include "syntax/diagnostic.h"
#include "syntax/source.h"

#include <optional>
#include <string>

namespace hgl::syntax
{
    /// Format declaration boundaries and attached clauses without resolving imports.
    /// Comments, literals and body contents retain their source spelling.
    [[nodiscard]] std::optional<std::string> format_declarations(const SourceFile &file, DiagnosticSink &diagnostics);
}  // namespace hgl::syntax

#endif
