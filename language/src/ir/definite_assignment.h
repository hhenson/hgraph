#ifndef HGL_IR_DEFINITE_ASSIGNMENT_H
#define HGL_IR_DEFINITE_ASSIGNMENT_H

#include "ir/hir.h"
#include "syntax/diagnostic.h"

namespace hgl::ir
{
    /// Check that every typed local without an initializer has been assigned
    /// on each path which reaches a read. This is a control-flow fact, not a
    /// type-inference or runtime-validity rule.
    void check_definite_assignment(const hir::Module &module, syntax::DiagnosticSink &diagnostics);
}  // namespace hgl::ir

#endif
