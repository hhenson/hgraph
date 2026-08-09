#ifndef HGRAPH_LIB_STD_OPERATORS_IMPL_COLLECTION_INPUT_SEMANTICS_H
#define HGRAPH_LIB_STD_OPERATORS_IMPL_COLLECTION_INPUT_SEMANTICS_H

#include <hgraph/types/time_series/ts_output/forwarding.h>

namespace hgraph::stdlib::collection_input_semantics
{
    /**
     * True when a collection input has an endpoint to observe.
     *
     * Transparent REF adaptation represents an empty reference as either an
     * empty reference or an unbound forwarding endpoint. Those states are
     * distinct from an ordinary bound collection which has not yet produced
     * its first value: upstream reference-backed feature operators stay silent
     * for the former, while some deliberately seed a value for the latter.
     */
    template <typename TInput>
    [[nodiscard]] bool has_bound_source(const TInput &input)
    {
        const auto reference = input.base().reference();
        if (reference.is_empty()) { return false; }
        if (!reference.is_peered() || !reference.has_output()) { return true; }

        const auto source = resolve_forwarding_source(
            reference.target_output().view(input.base().evaluation_time()));
        return source.bound() && !(source.forwarding() && !source.forwarding_bound());
    }
}  // namespace hgraph::stdlib::collection_input_semantics

#endif  // HGRAPH_LIB_STD_OPERATORS_IMPL_COLLECTION_INPUT_SEMANTICS_H
