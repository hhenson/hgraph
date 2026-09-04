#include <hgraph/lib/std/operators/impl/conversion_impl.h>

namespace hgraph::stdlib
{
    // combine / collect / emit: building collection time-series from
    // scalars and flattening them back into ticks.
    void register_conversion_combine_overloads()
    {
        register_overload<combine, combine_date_impl>();
        register_overload<combine, combine_timedelta_impl<true>>();
        register_overload<combine, combine_timedelta_impl<false>>();
        register_overload<combine, combine_datetime_impl>();
        register_overload<combine, combine_tsb_strict_impl>();
        register_overload<collect, collect_collection_impl>();
        register_overload<collect, collect_map_impl>();
        register_overload<collect, collect_map_zip_impl>();
        register_graph_overload<combine, combine_tss_scalars_impl>();
        register_overload<combine_tss_from_tsl_marker, combine_tss_from_tsl_impl>();
        register_overload<combine, combine_tuple_impl<true>>();
        register_overload<combine, combine_tuple_impl<false>>();
        register_overload<collect, collect_tsd_impl>();
        register_overload<collect, collect_tss_impl>();
        register_overload<collect, collect_tsd_zip_impl>();
        register_overload<collect, collect_tsd_from_map_impl>();
        register_overload<collect, collect_tsd_from_tsd_impl>();
        register_overload<emit, emit_collection_impl>();
        register_overload<emit, emit_tsl_impl>();
        register_overload<emit, emit_map_impl>();
    }
}  // namespace hgraph::stdlib
