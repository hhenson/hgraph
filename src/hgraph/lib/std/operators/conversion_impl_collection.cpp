#include <hgraph/lib/std/operators/impl/collection_impl.h>
#include <hgraph/lib/std/operators/impl/conversion_impl.h>

namespace hgraph::stdlib
{
    // convert between collection time-series and their scalar collection
    // forms: TS <-> TSS / TSD / TSL, map and tuple projections, TSB <-> CS.
    void register_conversion_collection_overloads()
    {
        register_overload<convert, convert_ts_to_tss_impl>();
        register_overload<convert, convert_ts_to_collection_impl>();
        register_overload<convert, convert_collection_to_collection_impl>();
        register_overload<convert, convert_series_to_tuple_impl>();
        register_overload<convert, convert_tss_to_collection_impl>();
        register_overload<convert, convert_collection_to_tss_impl>();
        register_overload<convert, convert_tsd_to_map_impl>();
        register_overload<convert, convert_map_to_tsd_impl>();
        register_overload<convert, convert_kv_to_map_impl>();
        register_overload<convert, convert_kv_to_tsd_impl>();
        register_overload<convert, collection_impl_detail::convert_tsb_to_cs_impl>();
        register_overload<convert, collection_impl_detail::convert_tsb_to_cs_lenient_impl>();
        register_overload<convert, collection_impl_detail::convert_cs_to_tsb_impl>();
        register_overload<convert, convert_tsl_to_tuple_impl<true>>();
        register_overload<convert, convert_tsl_to_tuple_impl<false>>();
        register_graph_overload<convert, convert_tsl_to_tsd_impl>();
        register_overload<convert, convert_zip_to_tsd_impl>();
        register_overload<convert, convert_zip_to_map_impl>();
        register_overload<convert, convert_tsl_to_map_impl>();
        register_overload<convert, convert_tsb_to_map_impl>();
        register_overload<convert, convert_list_to_tsl_impl>();
        register_overload<convert, convert_tsb_to_bool_impl>();
        register_overload<convert, convert_tsb_to_tsd_impl<false>>();
        register_overload<convert, convert_tsb_to_tsd_impl<true>>();
        register_overload<convert, convert_list_to_enumerated_tsd_impl>();
        register_overload<str_, str_tsl_impl>();
    }
}  // namespace hgraph::stdlib
