#ifndef HGL_HGRAPH_IR_PRINTER_H
#define HGL_HGRAPH_IR_PRINTER_H

#include "hgraph_ir/ir.h"

#include <string>

namespace hgl::hgraph_ir
{
    [[nodiscard]] std::string print(const Module &module);
}  // namespace hgl::hgraph_ir

#endif  // HGL_HGRAPH_IR_PRINTER_H
