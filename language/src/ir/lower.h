#ifndef HGL_IR_LOWER_H
#define HGL_IR_LOWER_H

#include "ir/hir.h"
#include "semantics/resolve.h"
#include "syntax/ast.h"
#include "syntax/diagnostic.h"

namespace hgl::ir
{
    /// Project a successfully resolved syntax module into backend-independent
    /// HIR. This migration-stage pass resolves every name to a stable symbol
    /// identity and owns every body node. Type/effect completion is a later
    /// pass and is represented explicitly by Module::completion.
    [[nodiscard]] hir::Module lower_to_hir(const syntax::ast::Module &module, const semantics::ResolvedModule &resolved,
                                           syntax::DiagnosticSink &diagnostics);
}  // namespace hgl::ir

#endif  // HGL_IR_LOWER_H
