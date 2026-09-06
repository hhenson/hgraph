#ifndef HGL_HGRAPH_IR_COMPLETE_H
#define HGL_HGRAPH_IR_COMPLETE_H

#include "hgraph_ir/ir.h"
#include "syntax/diagnostic.h"

namespace hgl::hgraph_ir
{
    /// Validate that every executable nominal operation has a concrete source
    /// implementation or a keyed native provider in the locked package target.
    /// On success the normalized provider plan is retained and the module
    /// advances from Bodies to Executable.
    [[nodiscard]] bool complete_execution(Module &module, ProviderPlan providers, syntax::DiagnosticSink &diagnostics);
}  // namespace hgl::hgraph_ir

#endif  // HGL_HGRAPH_IR_COMPLETE_H
