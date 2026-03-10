#include <hgraph/hgraph_base.h>
#include <hgraph/types/time_series/ts_output.h>

namespace hgraph
{
    TSOutputView TSOutput::view()
    {
        return TSOutputView{};
    }

}  // namespace hgraph
