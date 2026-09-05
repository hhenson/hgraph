#ifndef HGL_HGRAPH_IR_LOWER_H
#define HGL_HGRAPH_IR_LOWER_H

#include "hgraph_ir/ir.h"
#include "syntax/diagnostic.h"

namespace hgl::hgraph_ir
{
    /// Lower the typed language interface into the execution-facing type and
    /// callable tables. This foundation intentionally returns Interfaces;
    /// body, activation, state, and lifecycle lowering advances it to
    /// Executable in the next hgraph-IR slices.
    [[nodiscard]] Module lower_interfaces(const ir::hir::Module &source, syntax::DiagnosticSink &diagnostics);
}  // namespace hgl::hgraph_ir

#endif  // HGL_HGRAPH_IR_LOWER_H
