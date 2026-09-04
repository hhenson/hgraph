#include <hgraph/lib/std/operators/impl/arithmetic_impl.h>

namespace hgraph::stdlib
{
    // min_ / max_ / sum_ / mean over one TS[map] / TS[set] / TS[list] value,
    // with and without a default for the empty container: twelve overloads
    // per container kind.
    void register_arithmetic_aggregate_overloads()
    {
        arithmetic_impl_detail::register_container_aggregates<ValueTypeKind::Map>();
        arithmetic_impl_detail::register_container_aggregates<ValueTypeKind::Set>();
        arithmetic_impl_detail::register_container_aggregates<ValueTypeKind::List>();
    }
}  // namespace hgraph::stdlib
