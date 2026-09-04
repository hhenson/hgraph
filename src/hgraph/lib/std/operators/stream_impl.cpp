#include <hgraph/lib/std/operators/impl/stream_impl.h>

namespace hgraph::stdlib
{
    // The family registers through one group per translation unit; see
    // "Registration translation units" in the operators developer guide.
    void register_stream_operators()
    {
        register_stream_flow_overloads();
        register_stream_window_overloads();
    }
}  // namespace hgraph::stdlib
