#include <hgraph/lib/std/operators/impl/operators_impl.h>
#include <hgraph/lib/std/operators/impl/series_impl.h>
#include <hgraph/lib/std/operators/registration.h>

namespace hgraph::stdlib
{
    void register_standard_operators()
    {
        register_arithmetic_operators();
        register_comparison_operators();
        register_logical_operators();
        register_container_operators();
        register_conversion_operators();
        register_collection_operators();
        register_control_operators();
        register_higher_order_operators();
        register_io_operators();
        register_record_replay_memory_operators();
        register_json_operators();
        register_table_operators();
        register_data_frame_operators();
        register_record_replay_frame_operators();
        register_stream_operators();
        register_series_operators();
        register_string_operators();
        register_temporal_operators();
    }
}  // namespace hgraph::stdlib
