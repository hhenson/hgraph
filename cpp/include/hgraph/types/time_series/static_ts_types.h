#pragma once

/**
 * @file static_ts_types.h
 * @brief Static template definitions for compile-time time-series type construction.
 *
 * Provides type aliases that enable declarative time-series definitions:
 *
 * @code
 * // Compile-time TS definitions
 * using PriceTS = TS<double>;
 * using FlagTS = TS<bool>;
 *
 * // Compile-time TSB definition
 * using QuoteSchema = TSB<
 *     name<"Quote">,
 *     field<"bid", TS<double>>,
 *     field<"ask", TS<double>>,
 *     field<"time", TS<engine_time_t>>
 * >;
 *
 * // Get schema at runtime
 * const TSMeta* schema = QuoteSchema::schema();
 * @endcode
 *
 * These templates use lazy initialization - schemas are only registered
 * when their schema() method is first called.
 */

#include <hgraph/types/time_series/ts_type_registry.h>
#include <hgraph/types/value/static_types.h>
#include <hgraph/util/date_time.h>

#include <type_traits>

namespace hgraph {

// Re-export value layer utilities for convenience
using value::fixed_string;
using value::name;
using value::field;

// ============================================================================
// Type Traits for TS Types
// ============================================================================

namespace detail {

/**
 * @brief Get TSMeta for a type.
 *
 * Specializations handle:
 * - Static TS type templates (have ts_schema() method)
 */
template<typename T, typename = void>
struct ts_meta_of {
    // If T has ts_schema(), use it; otherwise it's not a TS type
    static const TSMeta* get() {
        static_assert(sizeof(T) == 0, "Type is not a time-series type. "
            "Use TS<T> to wrap scalar types.");
        return nullptr;
    }
};

// Specialization for types with ts_schema() method (static TS templates)
template<typename T>
struct ts_meta_of<T, std::void_t<decltype(T::ts_schema())>> {
    static const TSMeta* get() {
        return T::ts_schema();
    }
};

template<typename T>
const TSMeta* get_ts_meta() {
    return ts_meta_of<T>::get();
}

// Check if type has ts_schema() method
template<typename T, typename = void>
struct has_ts_schema : std::false_type {};

template<typename T>
struct has_ts_schema<T, std::void_t<decltype(T::ts_schema())>> : std::true_type {};

template<typename T>
constexpr bool has_ts_schema_v = has_ts_schema<T>::value;

// Check if type is a name tag
template<typename T>
struct ts_is_name_tag : std::false_type {};

template<fixed_string Name>
struct ts_is_name_tag<name<Name>> : std::true_type {};

template<typename T>
constexpr bool ts_is_name_tag_v = ts_is_name_tag<T>::value;

// Check if type is a field
template<typename T>
struct ts_is_field : std::false_type {};

template<fixed_string Name, typename T>
struct ts_is_field<field<Name, T>> : std::true_type {};

template<typename T>
constexpr bool ts_is_field_v = ts_is_field<T>::value;

// Extract name from name<...> tag
template<typename T>
struct ts_extract_name {
    static constexpr auto value = fixed_string<1>{""};
};

template<fixed_string Name>
struct ts_extract_name<name<Name>> {
    static constexpr auto value = Name;
};

} // namespace detail

// ============================================================================
// TS[T] - Scalar Time-Series
// ============================================================================

/**
 * @brief Static scalar time-series type definition.
 *
 * Wraps a scalar value type with time-series semantics.
 *
 * @code
 * using PriceTS = TS<double>;
 * using FlagTS = TS<bool>;
 * using TimeTS = TS<engine_time_t>;
 *
 * // Also works with static value types
 * using PointSchema = Bundle<name<"Point">, field<"x", double>, field<"y", double>>;
 * using PointTS = TS<PointSchema>;
 * @endcode
 */
template<typename T>
struct TS {
    static const TSMeta* ts_schema() {
        static const TSMeta* s = []() {
            auto& registry = TSTypeRegistry::instance();
            return registry.ts(value::detail::get_type_meta<T>());
        }();
        return s;
    }
};

// ============================================================================
// TSS[T] - Set Time-Series
// ============================================================================

/**
 * @brief Static set time-series type definition.
 *
 * Tracks a set of scalar values that changes over time.
 *
 * @code
 * using ActiveIdsTS = TSS<int64_t>;
 * @endcode
 */
template<typename T>
struct TSS {
    static const TSMeta* ts_schema() {
        static const TSMeta* s = []() {
            auto& registry = TSTypeRegistry::instance();
            return registry.tss(value::detail::get_type_meta<T>());
        }();
        return s;
    }
};

// ============================================================================
// TSD[K, V] - Dict Time-Series
// ============================================================================

/**
 * @brief Static dict time-series type definition.
 *
 * Maps scalar keys to time-series values.
 *
 * @code
 * using PriceDictTS = TSD<int64_t, TS<double>>;
 * using OrderDictTS = TSD<std::string, TSB<...>>;
 * @endcode
 */
template<typename K, typename V>
struct TSD {
    static_assert(detail::has_ts_schema_v<V>,
        "TSD value type must be a time-series type (TS, TSB, TSL, etc.)");

    static const TSMeta* ts_schema() {
        static const TSMeta* s = []() {
            auto& registry = TSTypeRegistry::instance();
            return registry.tsd(
                value::detail::get_type_meta<K>(),
                detail::get_ts_meta<V>()
            );
        }();
        return s;
    }
};

// ============================================================================
// TSL[TS, Size] - List Time-Series
// ============================================================================

/**
 * @brief Static list time-series type definition.
 *
 * List of independent time-series elements.
 *
 * @code
 * // Fixed-size list of 10 float time-series
 * using PriceListTS = TSL<TS<double>, 10>;
 *
 * // Dynamic list
 * using DynamicListTS = TSL<TS<double>, 0>;
 * using DynamicListTS2 = TSL<TS<double>>;  // Same as above
 * @endcode
 */
template<typename TSType, size_t Size = 0>
struct TSL {
    static_assert(detail::has_ts_schema_v<TSType>,
        "TSL element type must be a time-series type (TS, TSB, TSL, etc.)");

    static const TSMeta* ts_schema() {
        static const TSMeta* s = []() {
            auto& registry = TSTypeRegistry::instance();
            return registry.tsl(detail::get_ts_meta<TSType>(), Size);
        }();
        return s;
    }
};

// ============================================================================
// TSW[T, Period, MinPeriod] - Window Time-Series (Tick-Based)
// ============================================================================

/**
 * @brief Static window time-series type definition (tick-based).
 *
 * Maintains a time-ordered window of values.
 *
 * @code
 * // Window of 10 most recent prices
 * using PriceWindowTS = TSW<double, 10>;
 *
 * // Window with minimum period
 * using MinWindowTS = TSW<double, 100, 5>;
 * @endcode
 */
template<typename T, size_t Period, size_t MinPeriod = 0>
struct TSW {
    static_assert(Period > 0, "TSW period must be > 0");

    static const TSMeta* ts_schema() {
        static const TSMeta* s = []() {
            auto& registry = TSTypeRegistry::instance();
            return registry.tsw(value::detail::get_type_meta<T>(), Period, MinPeriod);
        }();
        return s;
    }
};

// ============================================================================
// REF[TS] - Reference Time-Series
// ============================================================================

/**
 * @brief Static reference time-series type definition.
 *
 * Dynamic reference to another time-series.
 *
 * @code
 * using PriceRefTS = REF<TS<double>>;
 * using QuoteRefTS = REF<TSB<...>>;
 * @endcode
 */
template<typename TSType>
struct REF {
    static_assert(detail::has_ts_schema_v<TSType>,
        "REF target type must be a time-series type (TS, TSB, TSL, etc.)");

    static const TSMeta* ts_schema() {
        static const TSMeta* s = []() {
            auto& registry = TSTypeRegistry::instance();
            return registry.ref(detail::get_ts_meta<TSType>());
        }();
        return s;
    }
};

// ============================================================================
// SIGNAL - Signal Time-Series
// ============================================================================

/**
 * @brief Static signal time-series type definition.
 *
 * Tick notification with no data value.
 *
 * @code
 * using HeartbeatTS = SIGNAL;
 * @endcode
 */
struct SIGNAL {
    static const TSMeta* ts_schema() {
        static const TSMeta* s = []() {
            auto& registry = TSTypeRegistry::instance();
            return registry.signal();
        }();
        return s;
    }
};

// ============================================================================
// TSB[name<...>, field<...>, ...] - Bundle Time-Series
// ============================================================================

namespace detail {

// Helper to add a TSB field to the fields vector
template<typename Field>
void add_tsb_field_to_vector(std::vector<std::pair<std::string, const TSMeta*>>& fields) {
    static_assert(has_ts_schema_v<typename Field::type>,
        "TSB field type must be a time-series type (TS, TSB, TSL, etc.)");
    fields.emplace_back(
        std::string(Field::name.c_str()),
        get_ts_meta<typename Field::type>()
    );
}

} // namespace detail

/**
 * @brief Static bundle time-series type definition.
 *
 * Each field is an independently tracked time-series.
 *
 * @code
 * using QuoteSchema = TSB<
 *     name<"Quote">,
 *     field<"bid", TS<double>>,
 *     field<"ask", TS<double>>,
 *     field<"time", TS<engine_time_t>>
 * >;
 *
 * // Anonymous TSB (no name)
 * using AnonymousTSB = TSB<
 *     field<"x", TS<double>>,
 *     field<"y", TS<double>>
 * >;
 * @endcode
 */
template<typename NameTag, typename... Fields>
struct TSB {
    static_assert(detail::ts_is_name_tag_v<NameTag>,
        "First argument must be name<\"...\">");
    static_assert((detail::ts_is_field_v<Fields> && ...),
        "Remaining arguments must be field<\"...\", TSType>");

    static constexpr auto bundle_name = detail::ts_extract_name<NameTag>::value;

    static const TSMeta* ts_schema() {
        static const TSMeta* s = []() {
            auto& registry = TSTypeRegistry::instance();
            std::vector<std::pair<std::string, const TSMeta*>> fields;
            (detail::add_tsb_field_to_vector<Fields>(fields), ...);
            return registry.tsb(fields, std::string(bundle_name.c_str()));
        }();
        return s;
    }
};

// Anonymous TSB (no name tag, first arg is a field)
template<typename FirstField, typename... RestFields>
    requires (detail::ts_is_field_v<FirstField>)
struct TSB<FirstField, RestFields...> {
    static_assert((detail::ts_is_field_v<RestFields> && ...),
        "All arguments must be field<\"...\", TSType>");

    static const TSMeta* ts_schema() {
        static const TSMeta* s = []() {
            auto& registry = TSTypeRegistry::instance();
            std::vector<std::pair<std::string, const TSMeta*>> fields;
            detail::add_tsb_field_to_vector<FirstField>(fields);
            (detail::add_tsb_field_to_vector<RestFields>(fields), ...);
            return registry.tsb(fields, "");
        }();
        return s;
    }
};

} // namespace hgraph
