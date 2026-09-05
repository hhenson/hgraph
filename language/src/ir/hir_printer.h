#ifndef HGL_IR_HIR_PRINTER_H
#define HGL_IR_HIR_PRINTER_H

#include "ir/hir.h"

#include <string>

namespace hgl::ir
{
    /// Deterministic, source-ranged diagnostic rendering of the HGL HIR. This
    /// is a prototype debugging format, not a persisted compatibility format.
    [[nodiscard]] std::string print_hir(const hir::Module &module);
}  // namespace hgl::ir

#endif  // HGL_IR_HIR_PRINTER_H
