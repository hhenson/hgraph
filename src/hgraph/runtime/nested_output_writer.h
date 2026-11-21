#ifndef HGRAPH_RUNTIME_NESTED_OUTPUT_WRITER_H
#define HGRAPH_RUNTIME_NESTED_OUTPUT_WRITER_H

#include <hgraph/runtime/nested_bindings.h>
#include <hgraph/types/time_series/ts_input/target_link.h>

namespace hgraph::runtime_detail
{
    // A nested writer shares storage with the parent output it represents.
    // Its explicit withdrawal must reach that storage just like a value write.
    inline void bind_nested_output_writer(const TSOutputView &terminal, const TSOutputView &destination)
    {
        bind_forwarding_output_to_source(terminal, destination);
        auto *writer = detail::mutable_target_link_storage(terminal.data_view());
        writer->state_.writes_parent_output = writer->bound();
    }
}

#endif
