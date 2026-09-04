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

    // The family registers through one group per translation unit; see
    // "Registration translation units" in the operators developer guide.
    // The runtime value conversions are registered first so the scalar
    // conversion operators can rely on them.
    void register_conversion_operators()
    {
        register_runtime_value_conversions();
        register_conversion_scalar_overloads();
        register_conversion_collection_overloads();
        register_conversion_combine_overloads();
    }
}  // namespace hgraph::stdlib
