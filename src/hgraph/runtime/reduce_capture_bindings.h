#ifndef HGRAPH_RUNTIME_REDUCE_CAPTURE_BINDINGS_H
#define HGRAPH_RUNTIME_REDUCE_CAPTURE_BINDINGS_H

#include <hgraph/runtime/nested_bindings.h>

namespace hgraph::runtime_detail
{
    /** A captured input is shared by every live combiner. A re-point can
        also change its projection routes without changing the root handle. */
    [[nodiscard]] inline bool refresh_reduce_captures(
        const TSInputView &inputs, std::size_t first_capture, std::vector<TSOutputHandle> &sources)
    {
        const std::size_t count = inputs.schema()->field_count() - first_capture;
        if (count == 0) { return false; }
        sources.resize(count);
        bool changed = false;
        for (std::size_t i = 0; i < count; ++i)
        {
            const auto input = inputs.indexed_child_at(first_capture + i);
            auto source = effective_output_handle(input.bound_output());
            changed = changed || input.modified() || !source.same_as(sources[i]);
            sources[i] = std::move(source);
        }
        return changed;
    }
}  // namespace hgraph::runtime_detail

#endif  // HGRAPH_RUNTIME_REDUCE_CAPTURE_BINDINGS_H
