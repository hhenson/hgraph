#include <hgraph/lib/std/operators/impl/stream_impl.h>
#include <hgraph/lib/std/operators/comparison.h>
#include <hgraph/lib/std/operators/arithmetic.h>

namespace hgraph::stdlib
{
    // TSW construction (to_window), the TSW reductions and the tick / time
    // window operators.
    void register_stream_window_overloads()
    {
        register_overload<to_window, stream_impl_detail::to_window_impl>();
        register_overload<to_window, stream_impl_detail::to_window_reset_impl>();
        register_overload<to_window, stream_impl_detail::to_window_duration_impl>();
        register_overload<to_window, stream_impl_detail::to_window_duration_reset_impl>();
        register_overload<abs_, stream_impl_detail::abs_tsw_impl<Int>>();
        register_overload<abs_, stream_impl_detail::abs_tsw_impl<Float>>();
        register_overload<sum_, stream_impl_detail::tsw_numeric_aggregate_impl<false, Int>>();
        register_overload<sum_, stream_impl_detail::tsw_numeric_aggregate_impl<false, Float>>();
        register_overload<mean, stream_impl_detail::tsw_numeric_aggregate_impl<true, Int>>();
        register_overload<mean, stream_impl_detail::tsw_numeric_aggregate_impl<true, Float>>();
        register_overload<min_, stream_impl_detail::tsw_extremum_impl<true>>();
        register_overload<max_, stream_impl_detail::tsw_extremum_impl<false>>();
        register_overload<min_, stream_impl_detail::tsw_extremum_default_impl<true>>();
        register_overload<max_, stream_impl_detail::tsw_extremum_default_impl<false>>();
        register_overload<window, window_tick_impl>();
        register_overload<window, window_time_impl>();
    }
}  // namespace hgraph::stdlib
