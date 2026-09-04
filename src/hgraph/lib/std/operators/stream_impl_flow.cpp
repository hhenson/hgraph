#include <hgraph/lib/std/operators/impl/stream_impl.h>
#include <hgraph/lib/std/operators/impl/tsb_itemwise_impl.h>

namespace hgraph::stdlib
{
    // Tick-flow control: sampling, filtering, lag, scheduling, gating,
    // take / drop / step / slice, dedup and batch.
    void register_stream_flow_overloads()
    {
        register_overload<sample, sample_impl>();
        register_overload<stream_impl_detail::tick_count, stream_impl_detail::tick_count_impl>();
        register_overload<filter_, filter_impl>();
        register_overload<lag, lag_tick_impl>();
        register_overload<lag, lag_time_impl>();
        register_overload<lag_proxy_node, lag_proxy_node_impl>();
        register_graph_overload<lag, lag_proxy_compose>();
        register_graph_overload<lag, lag_proxy_tsd_compose>();
        register_graph_overload<lag, lag_proxy_tsl_compose>();
        register_graph_overload<lag, lag_proxy_tsb_compose>();
        register_overload<schedule, schedule_impl>();
        register_overload<request_id, request_id_impl>();
        register_overload<schedule, schedule_ts_impl>();
        register_overload<schedule, schedule_ts_start_impl>();
        register_overload<until_true, until_true_bool_impl>();
        register_overload<until_true, until_true_value_callable_impl>();
        register_graph_overload<until_true, until_true_fn_compose>();
        register_overload<freeze, freeze_impl>();
        register_graph_overload<freeze, freeze_value_callable_compose>();
        register_graph_overload<freeze, freeze_fn_compose>();
        register_overload<gate, gate_impl>();
        register_overload<throttle, throttle_impl>();
        register_overload<take, take_scalar_impl>();
        register_overload<take, take_impl>();
        register_overload<take, take_reset_impl>();
        register_overload<drop, drop_scalar_impl>();
        register_overload<drop, drop_impl>();
        register_overload<drop, drop_time_impl>();
        register_overload<step, step_impl>();
        register_overload<slice_, slice_impl>();
        register_overload<dedup, dedup_scalar_impl>();
        register_overload<dedup, dedup_float_tol_impl>();
        register_graph_overload<dedup, dedup_tsd_map>();
        register_overload<dedup, dedup_tss_impl>();
        register_graph_overload<dedup, tsl_itemwise_impl_detail::tsl_unary_map<dedup>>();
        register_graph_overload<dedup, tsb_itemwise_impl_detail::tsb_unary_map<dedup>>();
        register_overload<lag, lag_time_ts_impl>();
        register_overload<batch, batch_impl>();
    }
}  // namespace hgraph::stdlib
