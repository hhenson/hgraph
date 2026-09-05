#include "syntax/parser.h"

#include "syntax/ast_projection.h"
#include "syntax/lexer.h"
#include "syntax/syntax_diagnostics.h"
#include "syntax/token_grammar.h"

namespace hgl::syntax
{
    ast::Module parse(const SourceFile &file, DiagnosticSink &diagnostics) {
        const LexResult         lexed  = lex(file, diagnostics);
        const SyntaxParseResult syntax = parse_source_syntax(file, lexed);
        report_syntax_issues(syntax.tree, lexed, diagnostics);
        if (syntax.tree.has_root()) { return project_ast(syntax.tree, lexed, diagnostics); }
        return {};
    }
}  // namespace hgl::syntax
