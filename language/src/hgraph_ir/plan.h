#ifndef HGL_HGRAPH_IR_PLAN_H
#define HGL_HGRAPH_IR_PLAN_H
#include "hgraph_ir/ir.h"
#include "syntax/diagnostic.h"
namespace hgl::hgraph_ir
{
    /// Identify a direct temporal parameter using binding identity.
    [[nodiscard]] std::optional<std::size_t> temporal_parameter(const Module &module, ValueId value, CallableId callable);
    /// Validate shared admission and prepare runtime activation/validity plans.
    void plan(Module &module, syntax::DiagnosticSink &diagnostics);
}  // namespace hgl::hgraph_ir
#endif
