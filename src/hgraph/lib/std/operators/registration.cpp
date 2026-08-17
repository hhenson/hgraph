#include <hgraph/lib/std/operators/impl/operators_impl.h>
#include <hgraph/lib/std/operators/impl/series_impl.h>
#include <hgraph/lib/std/operators/registration.h>
#include <hgraph/types/operator_dispatch.h>

namespace hgraph::stdlib
{
    namespace
    {
        void install_standard_operators()
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
    }  // namespace

    void register_standard_operators()
    {
        // Core's registration is the first installer (RFC 0025 checkpoint
        // 3): recorded once, replayed after every registry reset by
        // run_installers() — which also replays every extension installer
        // registered in this process, so one rebuild call restores the
        // whole overload table. Idempotent between resets.
        auto &registry = OperatorRegistry::instance();
        registry.register_installer("hgraph.stdlib", &install_standard_operators);
        registry.run_installers();
    }
}  // namespace hgraph::stdlib
