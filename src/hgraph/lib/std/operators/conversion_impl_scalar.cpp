#include <hgraph/lib/std/operators/impl/conversion_impl.h>

namespace hgraph::stdlib
{
    // Sources (const_ / nothing / zero_ / default_), str_, and convert
    // between scalar time-series: identity, Any, bundle up/down casts,
    // numeric / text / bool and Date <-> DateTime.
    void register_conversion_scalar_overloads()
    {
        register_overload<const_, const_source>();    // const(value)         -> tick at start
        register_overload<const_, const_delayed>();   // const(value, delay)  -> tick at start + delay
        register_overload<nothing, nothing_source>(); // nothing              -> never ticks

        register_graph_overload<zero_, zero_int>();
        register_graph_overload<zero_, zero_float>();
        register_graph_overload<zero_, zero_str>();
        register_overload<zero_, zero_tsd>();

        register_graph_overload<default_, default_impl>();
        register_overload<str_, str_impl>();
        register_overload<convert, convert_identity_impl>();
        register_overload<convert, convert_to_any_impl>();
        register_overload<convert, convert_from_any_impl>();
        register_overload<convert, convert_bundle_upcast_impl>();
        register_overload<convert, convert_tsd_nominal_upcast_impl>();
        register_overload<convert, convert_opaque_downcast_impl>();
        register_overload<convert, downcast_bundle_impl>();
        register_overload<downcast_, downcast_bundle_impl>();
        register_overload<downcast_ref, downcast_ref_impl>();
        register_overload<convert, convert_numeric_impl<Int, Float>>();
        register_overload<convert, convert_numeric_impl<Float, Int>>();
        register_overload<convert, convert_numeric_impl<Int, Bool>>();
        register_overload<convert, convert_numeric_impl<Bool, Int>>();
        register_overload<convert, convert_numeric_impl<Float, Bool>>();
        register_overload<convert, convert_numeric_impl<Bool, Float>>();
        register_overload<convert, convert_text_bytes_impl<Str, Bytes>>();
        register_overload<convert, convert_text_bytes_impl<Bytes, Str>>();
        register_overload<convert, convert_to_str_impl<Int>>();
        register_overload<convert, convert_to_str_impl<Float>>();
        register_overload<convert, convert_to_str_impl<Bool>>();
        register_overload<convert, convert_list_to_str_impl>();
        register_overload<convert, convert_list_to_bool_impl>();
        register_overload<convert, convert_date_to_datetime_impl>();
        register_overload<convert, convert_datetime_to_date_impl>();
    }
}  // namespace hgraph::stdlib
