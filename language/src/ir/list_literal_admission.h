#pragma once

#include "ir/hir.h"
#include "syntax/diagnostic.h"
#include <functional>
#include <span>

namespace hgl::ir {
    // Cold checking only. Invocation-specific constant facts allow a value
    // helper's list literal at constant calls without admitting runtime payloads.
    void check_list_literal_admission(const hir::Module &module, std::span<const hir::Expr *const> literals,
                                     const std::function<bool(hir::ExprId)> &constant_recipe,
                                     syntax::DiagnosticSink &diagnostics);
}
