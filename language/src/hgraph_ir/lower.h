#ifndef HGL_HGRAPH_IR_LOWER_H
#define HGL_HGRAPH_IR_LOWER_H

#include "hgraph_ir/ir.h"
#include "syntax/diagnostic.h"

namespace hgl::hgraph_ir
{
    /// Lower typed HIR into self-contained hgraph contracts and bodies. The
    /// Bodies checkpoint owns all references and control flow but intentionally
    /// precedes overload-provider and native execution planning.
    [[nodiscard]] Module lower(const ir::hir::Module &source, syntax::DiagnosticSink &diagnostics);
}  // namespace hgl::hgraph_ir

#endif  // HGL_HGRAPH_IR_LOWER_H
