#include <hgraph/hgraph_base.h>
#include <hgraph/types/time_series/ts_output.h>

namespace hgraph
{
    TSOutputView TSOutput::view()
    {
        return TSOutputView{};
    }

    TimeSeriesStatePtr TSOutput::state_ptr() noexcept
    {
        return std::visit(
            [](auto &state_value) -> TimeSeriesStatePtr {
                return TimeSeriesStatePtr{&state_value};
            },
            state);
    }

}  // namespace hgraph
