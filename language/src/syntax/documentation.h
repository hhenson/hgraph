#ifndef HGL_SYNTAX_DOCUMENTATION_H
#define HGL_SYNTAX_DOCUMENTATION_H
#include "syntax/diagnostic.h"
#include "syntax/source.h"
namespace hgl::syntax
{
    namespace ast
    {
        struct Module;
    }
    /// Attach and validate source documentation without interpreting reST directives.
    void capture_documentation(const SourceFile &, ast::Module &, DiagnosticSink &);
    /// Google-style section headings become reST rubrics; directive bodies remain intact.
    std::string documentation_rst(const std::vector<Documentation> &);
    /// Safe ordinary comments, including when a reST line ends with a backslash.
    std::string documentation_comments(const std::vector<Documentation> &);
}  // namespace hgl::syntax
#endif
