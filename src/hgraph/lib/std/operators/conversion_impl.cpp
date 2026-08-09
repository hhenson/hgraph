#include <hgraph/lib/std/operators/impl/collection_impl.h>
#include <hgraph/lib/std/operators/impl/conversion_impl.h>

#include <simdjson.h>

namespace hgraph::stdlib
{
    namespace
    {
        template <typename From, typename To>
        Value numeric_value_conversion(const ValueView &source)
        {
            const From value = source.checked_as<From>();
            if constexpr (std::same_as<To, Bool>) { return Value{value != From{}}; }
            else { return Value{static_cast<To>(value)}; }
        }

        Value str_to_bytes_value_conversion(const ValueView &source)
        {
            return Value{Bytes{source.checked_as<Str>()}};
        }

        Value bytes_to_str_value_conversion(const ValueView &source)
        {
            const auto &bytes = source.checked_as<Bytes>();
            if (!conversion_detail::valid_utf8(bytes.data))
            {
                throw std::invalid_argument("bytes value is not valid UTF-8");
            }
            return Value{Str{bytes.data}};
        }

        template <typename From>
        Value scalar_to_str_value_conversion(const ValueView &source)
        {
            if constexpr (std::same_as<From, Bool>)
            {
                return Value{Str{source.checked_as<Bool>() ? "True" : "False"}};
            }
            else if constexpr (std::same_as<From, Float>)
            {
                std::string text = fmt::format("{}", source.checked_as<Float>());
                if (text.find('.') == std::string::npos && text.find('e') == std::string::npos &&
                    text.find("inf") == std::string::npos && text.find("nan") == std::string::npos)
                {
                    text += ".0";
                }
                return Value{Str{std::move(text)}};
            }
            else { return Value{Str{fmt::format("{}", source.checked_as<From>())}}; }
        }

        Value date_to_datetime_value_conversion(const ValueView &source)
        {
            return Value{DateTime{std::chrono::sys_days{source.checked_as<Date>()}}};
        }

        Value datetime_to_date_value_conversion(const ValueView &source)
        {
            return Value{Date{std::chrono::floor<std::chrono::days>(source.checked_as<DateTime>())}};
        }

        template <typename From, typename To>
        void register_numeric_value_conversion()
        {
            ValueConversionRegistry::instance().register_converter(
                scalar_descriptor<From>::value_meta(), scalar_descriptor<To>::value_meta(),
                &numeric_value_conversion<From, To>);
        }

        void register_runtime_value_conversions()
        {
            register_numeric_value_conversion<Int, Float>();
            register_numeric_value_conversion<Float, Int>();
            register_numeric_value_conversion<Int, Bool>();
            register_numeric_value_conversion<Bool, Int>();
            register_numeric_value_conversion<Float, Bool>();
            register_numeric_value_conversion<Bool, Float>();

            auto &registry = ValueConversionRegistry::instance();
            registry.register_converter(scalar_descriptor<Str>::value_meta(),
                                        scalar_descriptor<Bytes>::value_meta(),
                                        &str_to_bytes_value_conversion);
            registry.register_converter(scalar_descriptor<Bytes>::value_meta(),
                                        scalar_descriptor<Str>::value_meta(),
                                        &bytes_to_str_value_conversion);
            registry.register_converter(scalar_descriptor<Int>::value_meta(),
                                        scalar_descriptor<Str>::value_meta(),
                                        &scalar_to_str_value_conversion<Int>);
            registry.register_converter(scalar_descriptor<Float>::value_meta(),
                                        scalar_descriptor<Str>::value_meta(),
                                        &scalar_to_str_value_conversion<Float>);
            registry.register_converter(scalar_descriptor<Bool>::value_meta(),
                                        scalar_descriptor<Str>::value_meta(),
                                        &scalar_to_str_value_conversion<Bool>);
            registry.register_converter(scalar_descriptor<Date>::value_meta(),
                                        scalar_descriptor<DateTime>::value_meta(),
                                        &date_to_datetime_value_conversion);
            registry.register_converter(scalar_descriptor<DateTime>::value_meta(),
                                        scalar_descriptor<Date>::value_meta(),
                                        &datetime_to_date_value_conversion);
        }
    }  // namespace

    bool conversion_detail::valid_utf8(std::string_view text) noexcept
    {
        return simdjson::validate_utf8(text);
    }

    void register_conversion_operators()
    {
        register_runtime_value_conversions();
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
        register_overload<combine, combine_date_impl>();
        register_overload<combine, combine_timedelta_impl<true>>();
        register_overload<combine, combine_timedelta_impl<false>>();
        register_overload<combine, combine_datetime_impl>();
        register_overload<combine, combine_tsb_strict_impl>();
        register_overload<collect, collect_collection_impl>();
        register_overload<collect, collect_map_impl>();
        register_overload<collect, collect_map_zip_impl>();
        register_overload<convert, convert_tsl_to_tuple_impl<true>>();
        register_overload<convert, convert_tsl_to_tuple_impl<false>>();
        register_graph_overload<convert, convert_tsl_to_tsd_impl>();
        register_overload<convert, convert_zip_to_tsd_impl>();
        register_graph_overload<combine, combine_tss_scalars_impl>();
        register_overload<combine_tss_from_tsl_marker, combine_tss_from_tsl_impl>();
        register_overload<convert, convert_zip_to_map_impl>();
        register_overload<convert, convert_tsl_to_map_impl>();
        register_overload<convert, convert_tsb_to_map_impl>();
        register_overload<combine, combine_tuple_impl<true>>();
        register_overload<combine, combine_tuple_impl<false>>();
        register_overload<convert, convert_list_to_tsl_impl>();
        register_overload<convert, convert_tsb_to_bool_impl>();
        register_overload<convert, convert_tsb_to_tsd_impl<false>>();
        register_overload<convert, convert_tsb_to_tsd_impl<true>>();
        register_overload<collect, collect_tsd_impl>();
        register_overload<collect, collect_tss_impl>();
        register_overload<convert, convert_list_to_enumerated_tsd_impl>();
        register_overload<collect, collect_tsd_zip_impl>();
        register_overload<collect, collect_tsd_from_map_impl>();
        register_overload<collect, collect_tsd_from_tsd_impl>();
        register_overload<emit, emit_collection_impl>();
        register_overload<emit, emit_tsl_impl>();
        register_overload<emit, emit_map_impl>();
        register_overload<str_, str_tsl_impl>();
    }
}  // namespace hgraph::stdlib
