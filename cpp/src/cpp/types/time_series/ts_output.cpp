#include <hgraph/hgraph_base.h>
#include <hgraph/types/time_series/ts_output.h>

namespace hgraph
{
    TSOutputView TSOutput::view()
    {
        const ViewContext context = view_context();
        return TSOutputView{context};
    }

}  // namespace hgraph
