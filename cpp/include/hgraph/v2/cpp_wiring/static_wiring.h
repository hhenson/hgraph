#pragma once

#include <hgraph/types/time_series/ts_type_registry.h>
#include <hgraph/types/value/type_registry.h>

#include <concepts>
#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>
#include <type_traits>

namespace hgraph::v2::cpp_wiring {

template<typename>
inline constexpr bool always_false_v = false;

template<size_t N>
struct fixed_string {
    char value[N];

    constexpr fixed_string(const char (&str)[N]) {
        for (size_t i = 0; i < N; ++i) {
            value[i] = str[i];
        }
    }

    [[nodiscard]] constexpr std::string_view view() const noexcept {
        return {value, N - 1};
    }
};

template<size_t N>
fixed_string(const char (&)[N]) -> fixed_string<N>;

template<fixed_string Name>
struct name {
    static constexpr auto literal = Name;

    [[nodiscard]] static constexpr const char* c_str() noexcept {
        return literal.value;
    }

    [[nodiscard]] static constexpr std::string_view view() noexcept {
        return literal.view();
    }
};

template<fixed_string Name, typename Schema>
struct field {
    using schema_type = Schema;
    static constexpr auto literal = Name;

    [[nodiscard]] static constexpr const char* c_str() noexcept {
        return literal.value;
    }

    [[nodiscard]] static constexpr std::string_view view() noexcept {
        return literal.view();
    }
};

namespace detail {

template<typename T>
using remove_cvref_t = std::remove_cv_t<std::remove_reference_t<T>>;

template<typename T>
concept ValueSchemaProvider = requires {
    { remove_cvref_t<T>::schema() } -> std::convertible_to<const value::TypeMeta*>;
};

template<typename T>
concept TimeSeriesSchemaProvider = requires {
    { remove_cvref_t<T>::schema() } -> std::convertible_to<const TSMeta*>;
};

template<typename T>
[[nodiscard]] const value::TypeMeta* value_schema() {
    using Type = remove_cvref_t<T>;

    if constexpr (ValueSchemaProvider<Type>) {
        return Type::schema();
    } else if constexpr (TimeSeriesSchemaProvider<Type>) {
        static_assert(always_false_v<Type>, "Time-series schemas cannot be used where a value schema is required");
    } else {
        return value::scalar_type_meta<Type>();
    }
}

template<typename T>
[[nodiscard]] const TSMeta* ts_schema() {
    using Type = remove_cvref_t<T>;

    if constexpr (TimeSeriesSchemaProvider<Type>) {
        return Type::schema();
    } else if constexpr (ValueSchemaProvider<Type>) {
        static_assert(always_false_v<Type>, "Value schemas cannot be used where a time-series schema is required");
    } else {
        static_assert(always_false_v<Type>, "A static time-series schema type is required");
    }
}

template<typename Spec>
struct bundle_field;

template<fixed_string Name, typename Schema>
struct bundle_field<field<Name, Schema>> {
    static void add(value::BundleBuilder& builder) {
        builder.add_field(field<Name, Schema>::c_str(), value_schema<Schema>());
    }
};

template<typename Spec>
struct ts_bundle_field;

template<fixed_string Name, typename Schema>
struct ts_bundle_field<field<Name, Schema>> {
    static void add(TSBBuilder& builder) {
        builder.add_field(field<Name, Schema>::c_str(), ts_schema<Schema>());
    }
};

} // namespace detail

template<typename... Specs>
struct Bundle;

template<fixed_string Name, typename... Fields>
struct Bundle<name<Name>, Fields...> {
    [[nodiscard]] static const value::TypeMeta* schema() {
        static const value::TypeMeta* meta = [] {
            auto builder = value::TypeRegistry::instance().bundle(std::string{name<Name>::view()});
            (detail::bundle_field<Fields>::add(builder), ...);
            return builder.build();
        }();
        return meta;
    }
};

template<typename Value>
struct TS {
    [[nodiscard]] static const TSMeta* schema() {
        static const TSMeta* meta = TSTypeRegistry::instance().ts(detail::value_schema<Value>());
        return meta;
    }
};

template<typename Element>
struct TSS {
    [[nodiscard]] static const TSMeta* schema() {
        static const TSMeta* meta = TSTypeRegistry::instance().tss(detail::value_schema<Element>());
        return meta;
    }
};

template<typename Key, typename ValueTS>
struct TSD {
    [[nodiscard]] static const TSMeta* schema() {
        static const TSMeta* meta = TSTypeRegistry::instance().tsd(detail::value_schema<Key>(), detail::ts_schema<ValueTS>());
        return meta;
    }
};

template<typename ElementTS, size_t FixedSize = 0>
struct TSL {
    [[nodiscard]] static const TSMeta* schema() {
        static const TSMeta* meta = TSTypeRegistry::instance().tsl(detail::ts_schema<ElementTS>(), FixedSize);
        return meta;
    }
};

template<typename Value, size_t Period, size_t MinPeriod = 0>
struct TSW {
    [[nodiscard]] static const TSMeta* schema() {
        static const TSMeta* meta = TSTypeRegistry::instance().tsw(detail::value_schema<Value>(), Period, MinPeriod);
        return meta;
    }
};

template<typename... Specs>
struct TSB;

template<fixed_string Name, typename... Fields>
struct TSB<name<Name>, Fields...> {
    [[nodiscard]] static const TSMeta* schema() {
        static const TSMeta* meta = [] {
            TSBBuilder builder;
            builder.set_name(std::string{name<Name>::view()});
            (detail::ts_bundle_field<Fields>::add(builder), ...);
            return builder.build();
        }();
        return meta;
    }
};

template<typename ReferencedTS>
struct REF {
    [[nodiscard]] static const TSMeta* schema() {
        static const TSMeta* meta = TSTypeRegistry::instance().ref(detail::ts_schema<ReferencedTS>());
        return meta;
    }
};

struct SIGNAL {
    [[nodiscard]] static const TSMeta* schema() {
        static const TSMeta* meta = TSTypeRegistry::instance().signal();
        return meta;
    }
};

} // namespace hgraph::v2::cpp_wiring
