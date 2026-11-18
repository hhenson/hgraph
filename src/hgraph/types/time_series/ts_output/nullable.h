#ifndef HGRAPH_TS_OUTPUT_NULLABLE_H
#define HGRAPH_TS_OUTPUT_NULLABLE_H

#include <hgraph/types/time_series/ts_type_ref.h>

namespace hgraph::detail
{
    /** Read-only TS[Optional[T]] representation over the source's TS[T] memory.
        The returned type is interned; no payload or clock is allocated. */
    [[nodiscard]] TSRoleTypeRef borrowed_nullable_type_for(
        TSRoleTypeRef source, const TSValueTypeMetaData &requested);
    HGRAPH_EXPORT void clear_borrowed_nullable_types() noexcept;
}

#endif
