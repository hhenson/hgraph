#ifndef HGL_WIRING_OPERATOR_TYPES_H
#define HGL_WIRING_OPERATOR_TYPES_H

#include "ir/type_check.h"

namespace hgl::wiring
{
    /// The hgraph-backed implementation of HIR's overload-resolution port.
    /// It constructs schema-only wiring arguments, delegates ranking to
    /// OperatorRegistry, and copies the result back into backend-independent
    /// data. Higher-order calls whose callable erasure is not yet available
    /// remain explicit nominal/deferred operations.
    [[nodiscard]] ir::OperatorSelection resolve_operator_types(const ir::hir::Module &module, const ir::OperatorQuery &query);
}  // namespace hgl::wiring

#endif  // HGL_WIRING_OPERATOR_TYPES_H
