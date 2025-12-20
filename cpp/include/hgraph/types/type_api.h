//
// Created by Claude on 20/12/2025.
//
// Type API - Declarative type construction with automatic interning (C++20)
//
// This header provides clean APIs for constructing type metadata with automatic
// registry lookups and interning. Supports both compile-time and runtime usage.
//
// COMPILE-TIME API (template-based):
//
//   auto* ts_int = ts_type<TS<int>>();
//   auto* tss    = ts_type<TSS<int>>();
//   auto* tsl    = ts_type<TSL<TS<int>, 3>>();
//   auto* tsd    = ts_type<TSD<std::string, TS<int>>>();
//   auto* ref    = ts_type<REF<TS<int>>>();
//
//   // Bundles with C++20 string literal template parameters
//   auto* point = ts_type<TSB<
//       Field<"x", TS<int>>,
//       Field<"y", TS<float>>,
//       Name<"Point">
//   >>();
//
//   // Windows - count-based and time-based
//   auto* w1 = ts_type<TSW<float, 10>>();                      // Last 10 values
//   auto* w2 = ts_type<TSW_Time<float, Seconds<60>>>();        // 60-second window
//   auto* w3 = ts_type<TSW_Time<float, Minutes<5>, Count<3>>>(); // 5 min, min 3 values
//
// RUNTIME API (for dynamic type construction):
//
//   auto* ts_int = runtime::ts(type_of<int>());
//   auto* tsl    = runtime::tsl(ts_int, 3);
//   auto* point  = runtime::tsb({{"x", ts_int}, {"y", ts_float}}, "Point");
//   auto* window = runtime::tsw_time(type_of<float>(), 60'000'000, 0); // 60s in microseconds
//
// Both APIs return interned pointers - same type = same pointer.
//

#ifndef HGRAPH_TYPE_API_H
#define HGRAPH_TYPE_API_H

#include <hgraph/types/value/type_meta.h>
#include <hgraph/types/value/type_registry.h>
#include <hgraph/types/value/scalar_type.h>
#include <hgraph/types/value/set_type.h>
#include <hgraph/types/value/list_type.h>
#include <hgraph/types/value/bundle_type.h>
#include <hgraph/types/value/ref_type.h>
#include <hgraph/types/value/window_type.h>
#include <hgraph/types/value/dict_type.h>
#include <hgraph/types/time_series/ts_type_meta.h>
#include <hgraph/types/time_series/ts_type_registry.h>

// Conditionally include Python conversion support
// Define HGRAPH_TYPE_API_WITH_PYTHON before including to enable runtime_python namespace
#ifdef HGRAPH_TYPE_API_WITH_PYTHON
#include <hgraph/types/value/python_conversion.h>
#endif
#include <algorithm>
#include <cstddef>
#include <string>
#include <tuple>
#include <type_traits>

namespace hgraph::types {

// ============================================================================
// Compile-time String Support (C++20 NTTPs)
// ============================================================================

/**
 * StringLiteral - C++20 structural type for string literal template parameters
 *
 * Usage: Field<"name", TS<int>> - string literals work directly!
 */
template<std::size_t N>
struct StringLiteral {
    char value[N];

    constexpr StringLiteral(const char (&str)[N]) {
        std::copy_n(str, N, value);
    }

    [[nodiscard]] constexpr const char* c_str() const { return value; }
    [[nodiscard]] constexpr std::size_t size() const { return N - 1; }

    [[nodiscard]] constexpr std::size_t hash() const {
        std::size_t h = 0;
        for (std::size_t i = 0; i < N - 1; ++i) {
            h = h * 31 + static_cast<std::size_t>(value[i]);
        }
        return h;
    }
};

// ============================================================================
// Forward Declarations
// ============================================================================

template<typename T> struct TS;
template<typename T> struct TSS;
template<typename K, typename V> struct TSD;
template<typename V, int64_t Size> struct TSL;
template<typename... Args> struct TSB;
template<typename T, int64_t Size, int64_t MinSize = 0> struct TSW;
template<typename T, typename SizeSpec, typename MinSizeSpec> struct TSW_Count;
template<typename T, typename DurationSpec, typename MinSizeSpec> struct TSW_Time;
template<typename V> struct REF;

template<StringLiteral Name, typename TSType> struct Field;
template<StringLiteral N> struct Name;

// Window size/duration specifiers
template<int64_t N> struct Count;           // Count-based window
template<int64_t N> struct Microseconds;    // Time-based window (microseconds)
template<int64_t N> struct Milliseconds;    // Time-based window (milliseconds)
template<int64_t N> struct Seconds;         // Time-based window (seconds)
template<int64_t N> struct Minutes;         // Time-based window (minutes)
template<int64_t N> struct Hours;           // Time-based window (hours)

// ============================================================================
// Type Traits
// ============================================================================

namespace detail {

    // Check if a type is a time-series type descriptor
    template<typename T>
    struct is_ts_type : std::false_type {};

    template<typename T>
    struct is_ts_type<TS<T>> : std::true_type {};

    template<typename T>
    struct is_ts_type<TSS<T>> : std::true_type {};

    template<typename K, typename V>
    struct is_ts_type<TSD<K, V>> : std::true_type {};

    template<typename V, int64_t Size>
    struct is_ts_type<TSL<V, Size>> : std::true_type {};

    template<typename... Args>
    struct is_ts_type<TSB<Args...>> : std::true_type {};

    template<typename T, int64_t Size, int64_t MinSize>
    struct is_ts_type<TSW<T, Size, MinSize>> : std::true_type {};

    template<typename T, typename SizeSpec, typename MinSizeSpec>
    struct is_ts_type<TSW_Count<T, SizeSpec, MinSizeSpec>> : std::true_type {};

    template<typename T, typename DurationSpec, typename MinSizeSpec>
    struct is_ts_type<TSW_Time<T, DurationSpec, MinSizeSpec>> : std::true_type {};

    template<typename V>
    struct is_ts_type<REF<V>> : std::true_type {};

    template<typename T>
    inline constexpr bool is_ts_type_v = is_ts_type<T>::value;

    // Check if a type is a Field descriptor
    template<typename T>
    struct is_field : std::false_type {};

    template<StringLiteral N, typename TSType>
    struct is_field<Field<N, TSType>> : std::true_type {};

    template<typename T>
    inline constexpr bool is_field_v = is_field<T>::value;

    // Check if a type is a Name descriptor
    template<typename T>
    struct is_name : std::false_type {};

    template<StringLiteral N>
    struct is_name<Name<N>> : std::true_type {};

    template<typename T>
    inline constexpr bool is_name_v = is_name<T>::value;

    // Check if a type is a window size specifier
    template<typename T>
    struct is_window_size : std::false_type {};

    template<int64_t N> struct is_window_size<Count<N>> : std::true_type {};
    template<int64_t N> struct is_window_size<Microseconds<N>> : std::true_type {};
    template<int64_t N> struct is_window_size<Milliseconds<N>> : std::true_type {};
    template<int64_t N> struct is_window_size<Seconds<N>> : std::true_type {};
    template<int64_t N> struct is_window_size<Minutes<N>> : std::true_type {};
    template<int64_t N> struct is_window_size<Hours<N>> : std::true_type {};

    template<typename T>
    inline constexpr bool is_window_size_v = is_window_size<T>::value;

    // Check if a type is a count-based window size
    template<typename T>
    struct is_count_based : std::false_type {};

    template<int64_t N>
    struct is_count_based<Count<N>> : std::true_type {};

    template<typename T>
    inline constexpr bool is_count_based_v = is_count_based<T>::value;

    // Check if a type is a time-based window size
    template<typename T>
    struct is_time_based : std::false_type {};

    template<int64_t N> struct is_time_based<Microseconds<N>> : std::true_type {};
    template<int64_t N> struct is_time_based<Milliseconds<N>> : std::true_type {};
    template<int64_t N> struct is_time_based<Seconds<N>> : std::true_type {};
    template<int64_t N> struct is_time_based<Minutes<N>> : std::true_type {};
    template<int64_t N> struct is_time_based<Hours<N>> : std::true_type {};

    template<typename T>
    inline constexpr bool is_time_based_v = is_time_based<T>::value;

    // Hash combining utility
    inline size_t hash_combine(size_t h1, size_t h2) {
        return h1 ^ (h2 + 0x9e3779b9 + (h1 << 6) + (h1 >> 2));
    }

    // Hash seeds for different types
    constexpr size_t TS_SEED = 0x54530000;    // "TS\0\0"
    constexpr size_t TSS_SEED = 0x545353;     // "TSS"
    constexpr size_t TSD_SEED = 0x545344;     // "TSD"
    constexpr size_t TSL_SEED = 0x54534C;     // "TSL"
    constexpr size_t TSB_SEED = 0x545342;     // "TSB"
    constexpr size_t TSW_SEED = 0x545357;     // "TSW"
    constexpr size_t REF_SEED = 0x524546;     // "REF"

}  // namespace detail

// ============================================================================
// Scalar Type API
// ============================================================================

/**
 * Get the TypeMeta for a scalar type T.
 * Automatically uses the global registry for interning.
 */
template<typename T>
const value::TypeMeta* type_of() {
    return value::scalar_type_meta<T>();
}

// ============================================================================
// Time-Series Type Descriptors
// ============================================================================

/**
 * TS<T> - Single scalar time-series descriptor
 *
 * Represents a time-series that holds values of scalar type T.
 * Usage: ts_type<TS<int>>()
 */
template<typename T>
struct TS {
    using scalar_type = T;

    static const TimeSeriesTypeMeta* get() {
        const auto* scalar = type_of<T>();
        size_t key = detail::hash_combine(detail::TS_SEED, reinterpret_cast<size_t>(scalar));

        auto& registry = TimeSeriesTypeRegistry::global();
        if (auto* existing = registry.lookup_by_key(key)) {
            return existing;
        }

        auto meta = std::make_unique<TSTypeMeta>();
        meta->ts_kind = TimeSeriesKind::TS;
        meta->scalar_type = scalar;

        return registry.register_by_key(key, std::move(meta));
    }
};

/**
 * TSS<T> - Time-series set descriptor
 *
 * Represents a time-series that tracks additions/removals of elements of type T.
 * Usage: ts_type<TSS<int>>()
 */
template<typename T>
struct TSS {
    using element_type = T;

    static const TimeSeriesTypeMeta* get() {
        const auto* element = type_of<T>();
        size_t key = detail::hash_combine(detail::TSS_SEED, reinterpret_cast<size_t>(element));

        auto& registry = TimeSeriesTypeRegistry::global();
        if (auto* existing = registry.lookup_by_key(key)) {
            return existing;
        }

        auto meta = std::make_unique<TSSTypeMeta>();
        meta->ts_kind = TimeSeriesKind::TSS;
        meta->element_type = element;

        // Build and register SetTypeMeta via value registry
        size_t set_key = value::hash_combine(0x53455400, reinterpret_cast<size_t>(element));
        auto& value_registry = value::TypeRegistry::global();
        if (auto* existing_set = value_registry.lookup_by_key(set_key)) {
            meta->set_value_type = existing_set;
        } else {
            auto set_meta = value::SetTypeBuilder()
                .element_type(element)
                .build();
            meta->set_value_type = value_registry.register_by_key(set_key, std::move(set_meta));
        }

        return registry.register_by_key(key, std::move(meta));
    }
};

/**
 * TSD<K, V> - Time-series dict descriptor
 *
 * Represents a dictionary with scalar keys K and time-series values V.
 * Usage: ts_type<TSD<std::string, TS<int>>>()
 */
template<typename K, typename V>
struct TSD {
    static_assert(detail::is_ts_type_v<V>, "TSD value type must be a time-series type");

    using key_type = K;
    using value_ts_type = V;

    static const TimeSeriesTypeMeta* get() {
        const auto* key_meta = type_of<K>();
        const auto* value_ts = V::get();

        size_t key = detail::hash_combine(detail::TSD_SEED, reinterpret_cast<size_t>(key_meta));
        key = detail::hash_combine(key, reinterpret_cast<size_t>(value_ts));

        auto& registry = TimeSeriesTypeRegistry::global();
        if (auto* existing = registry.lookup_by_key(key)) {
            return existing;
        }

        auto meta = std::make_unique<TSDTypeMeta>();
        meta->ts_kind = TimeSeriesKind::TSD;
        meta->key_type = key_meta;
        meta->value_ts_type = value_ts;

        // Build DictTypeMeta via value registry
        if (key_meta && value_ts) {
            const value::TypeMeta* value_schema = value_ts->value_schema();
            if (value_schema) {
                size_t dict_key = value::hash_combine(0x44494354, reinterpret_cast<size_t>(key_meta));
                dict_key = value::hash_combine(dict_key, reinterpret_cast<size_t>(value_schema));

                auto& value_registry = value::TypeRegistry::global();
                if (auto* existing_dict = value_registry.lookup_by_key(dict_key)) {
                    meta->dict_value_type = existing_dict;
                } else {
                    auto dict_meta = value::DictTypeBuilder()
                        .key_type(key_meta)
                        .value_type(value_schema)
                        .build();
                    meta->dict_value_type = value_registry.register_by_key(dict_key, std::move(dict_meta));
                }
            }
        }

        return registry.register_by_key(key, std::move(meta));
    }
};

/**
 * TSL<V, Size> - Time-series list descriptor
 *
 * Represents a fixed-size list of time-series elements.
 * Usage: ts_type<TSL<TS<int>, 3>>()
 */
template<typename V, int64_t Size>
struct TSL {
    static_assert(detail::is_ts_type_v<V>, "TSL element type must be a time-series type");
    static_assert(Size >= -1, "TSL size must be >= -1 (use -1 for dynamic)");

    using element_ts_type = V;
    static constexpr int64_t size = Size;

    static const TimeSeriesTypeMeta* get() {
        const auto* element_ts = V::get();

        size_t key = detail::hash_combine(detail::TSL_SEED, reinterpret_cast<size_t>(element_ts));
        key = detail::hash_combine(key, static_cast<size_t>(Size + 1));

        auto& registry = TimeSeriesTypeRegistry::global();
        if (auto* existing = registry.lookup_by_key(key)) {
            return existing;
        }

        auto meta = std::make_unique<TSLTypeMeta>();
        meta->ts_kind = TimeSeriesKind::TSL;
        meta->element_ts_type = element_ts;
        meta->size = Size;

        // Build ListTypeMeta if element has value_schema and size is known
        if (element_ts && element_ts->value_schema() && Size > 0) {
            const auto* elem_value_schema = element_ts->value_schema();
            size_t list_key = value::hash_combine(0x4C495354, reinterpret_cast<size_t>(elem_value_schema));
            list_key = value::hash_combine(list_key, static_cast<size_t>(Size));

            auto& value_registry = value::TypeRegistry::global();
            if (auto* existing_list = value_registry.lookup_by_key(list_key)) {
                meta->list_value_type = existing_list;
            } else {
                auto list_meta = value::ListTypeBuilder()
                    .element_type(elem_value_schema)
                    .count(static_cast<size_t>(Size))
                    .build();
                meta->list_value_type = value_registry.register_by_key(list_key, std::move(list_meta));
            }
        }

        return registry.register_by_key(key, std::move(meta));
    }
};

// ============================================================================
// Window Size/Duration Types
// ============================================================================

/**
 * Count<N> - Count-based window size
 *
 * Specifies a window that holds the last N values.
 * Usage: TSW<float, Count<10>>
 */
template<int64_t N>
struct Count {
    static_assert(N >= 0, "Count must be non-negative");
    static constexpr int64_t value = N;
    static constexpr bool is_time_based = false;
    static constexpr int64_t microseconds() { return -1; }  // Not time-based
};

/**
 * Microseconds<N> - Time-based window (microseconds)
 */
template<int64_t N>
struct Microseconds {
    static_assert(N > 0, "Duration must be positive");
    static constexpr int64_t value = N;
    static constexpr bool is_time_based = true;
    static constexpr int64_t microseconds() { return N; }
};

/**
 * Milliseconds<N> - Time-based window (milliseconds)
 */
template<int64_t N>
struct Milliseconds {
    static_assert(N > 0, "Duration must be positive");
    static constexpr int64_t value = N;
    static constexpr bool is_time_based = true;
    static constexpr int64_t microseconds() { return N * 1000; }
};

/**
 * Seconds<N> - Time-based window (seconds)
 */
template<int64_t N>
struct Seconds {
    static_assert(N > 0, "Duration must be positive");
    static constexpr int64_t value = N;
    static constexpr bool is_time_based = true;
    static constexpr int64_t microseconds() { return N * 1'000'000; }
};

/**
 * Minutes<N> - Time-based window (minutes)
 */
template<int64_t N>
struct Minutes {
    static_assert(N > 0, "Duration must be positive");
    static constexpr int64_t value = N;
    static constexpr bool is_time_based = true;
    static constexpr int64_t microseconds() { return N * 60'000'000; }
};

/**
 * Hours<N> - Time-based window (hours)
 */
template<int64_t N>
struct Hours {
    static_assert(N > 0, "Duration must be positive");
    static constexpr int64_t value = N;
    static constexpr bool is_time_based = true;
    static constexpr int64_t microseconds() { return N * 3'600'000'000; }
};

// ============================================================================
// TSW - Time-Series Window (supports both count and time-based)
// ============================================================================

/**
 * TSW<T, Size, MinSize> - Time-series window descriptor (legacy int64_t form)
 *
 * Represents a count-based windowed view of scalar values.
 * Usage: ts_type<TSW<float, 10, 1>>()
 */
template<typename T, int64_t Size, int64_t MinSize>
struct TSW {
    using scalar_type = T;
    static constexpr int64_t size = Size;
    static constexpr int64_t min_size = MinSize;
    static constexpr bool is_time_based = false;

    static const TimeSeriesTypeMeta* get() {
        const auto* scalar = type_of<T>();

        size_t key = detail::hash_combine(detail::TSW_SEED, reinterpret_cast<size_t>(scalar));
        key = detail::hash_combine(key, static_cast<size_t>(Size + 1));
        key = detail::hash_combine(key, static_cast<size_t>(MinSize + 1));

        auto& registry = TimeSeriesTypeRegistry::global();
        if (auto* existing = registry.lookup_by_key(key)) {
            return existing;
        }

        auto meta = std::make_unique<TSWTypeMeta>();
        meta->ts_kind = TimeSeriesKind::TSW;
        meta->scalar_type = scalar;
        meta->size = Size;
        meta->min_size = MinSize;

        // Build WindowTypeMeta via value registry
        if (scalar && Size > 0) {
            size_t window_key = value::hash_combine(0x57494E44, reinterpret_cast<size_t>(scalar));
            window_key = value::hash_combine(window_key, static_cast<size_t>(Size));

            auto& value_registry = value::TypeRegistry::global();
            if (auto* existing_window = value_registry.lookup_by_key(window_key)) {
                meta->window_value_type = existing_window;
            } else {
                auto window_meta = value::WindowTypeBuilder()
                    .element_type(scalar)
                    .fixed_count(static_cast<size_t>(Size))
                    .build();
                meta->window_value_type = value_registry.register_by_key(window_key, std::move(window_meta));
            }
        }

        return registry.register_by_key(key, std::move(meta));
    }
};

/**
 * TSW_Count<T, Size, MinSize> - Count-based window using typed specifiers
 *
 * Usage: ts_type<TSW_Count<float, Count<10>, Count<1>>>()
 */
template<typename T, typename SizeSpec, typename MinSizeSpec = Count<0>>
struct TSW_Count {
    static_assert(detail::is_count_based_v<SizeSpec>, "SizeSpec must be Count<N>");
    static_assert(detail::is_count_based_v<MinSizeSpec>, "MinSizeSpec must be Count<N>");

    using scalar_type = T;
    static constexpr int64_t size = SizeSpec::value;
    static constexpr int64_t min_size = MinSizeSpec::value;

    static const TimeSeriesTypeMeta* get() {
        return TSW<T, size, min_size>::get();
    }
};

/**
 * TSW_Time<T, Duration, MinCount> - Time-based window
 *
 * Usage:
 *   ts_type<TSW_Time<float, Seconds<60>>>()           // 60-second window
 *   ts_type<TSW_Time<float, Minutes<5>, Count<10>>>() // 5-minute window, min 10 values
 */
template<typename T, typename DurationSpec, typename MinSizeSpec = Count<0>>
struct TSW_Time {
    static_assert(detail::is_time_based_v<DurationSpec>,
                  "DurationSpec must be Microseconds/Milliseconds/Seconds/Minutes/Hours<N>");
    static_assert(detail::is_count_based_v<MinSizeSpec>, "MinSizeSpec must be Count<N>");

    using scalar_type = T;
    static constexpr int64_t duration_us = DurationSpec::microseconds();
    static constexpr int64_t min_size = MinSizeSpec::value;

    static const TimeSeriesTypeMeta* get() {
        const auto* scalar = type_of<T>();

        // Use negative duration to indicate time-based, encode duration in key
        size_t key = detail::hash_combine(detail::TSW_SEED, reinterpret_cast<size_t>(scalar));
        key = detail::hash_combine(key, static_cast<size_t>(duration_us));
        key = detail::hash_combine(key, 0x54494D45);  // "TIME" marker
        key = detail::hash_combine(key, static_cast<size_t>(min_size + 1));

        auto& registry = TimeSeriesTypeRegistry::global();
        if (auto* existing = registry.lookup_by_key(key)) {
            return existing;
        }

        auto meta = std::make_unique<TSWTypeMeta>();
        meta->ts_kind = TimeSeriesKind::TSW;
        meta->scalar_type = scalar;
        meta->size = -duration_us;  // Negative indicates time-based, value is duration in microseconds
        meta->min_size = min_size;

        // Build WindowTypeMeta via value registry (time-based)
        if (scalar && duration_us > 0) {
            size_t window_key = value::hash_combine(0x57494E44, reinterpret_cast<size_t>(scalar));
            window_key = value::hash_combine(window_key, 0x54494D45);  // "TIME" marker
            window_key = value::hash_combine(window_key, static_cast<size_t>(duration_us));

            auto& value_registry = value::TypeRegistry::global();
            if (auto* existing_window = value_registry.lookup_by_key(window_key)) {
                meta->window_value_type = existing_window;
            } else {
                // Convert microseconds to nanoseconds for engine_time_delta_t
                engine_time_delta_t duration_ns{duration_us * 1000};
                auto window_meta = value::WindowTypeBuilder()
                    .element_type(scalar)
                    .time_duration(duration_ns)
                    .build();
                meta->window_value_type = value_registry.register_by_key(window_key, std::move(window_meta));
            }
        }

        return registry.register_by_key(key, std::move(meta));
    }
};

/**
 * REF<V> - Time-series reference descriptor
 *
 * Represents a reference to another time-series type.
 * Usage: ts_type<REF<TS<int>>>()
 */
template<typename V>
struct REF {
    static_assert(detail::is_ts_type_v<V>, "REF value type must be a time-series type");

    using value_ts_type = V;

    static const TimeSeriesTypeMeta* get() {
        const auto* value_ts = V::get();

        size_t key = detail::hash_combine(detail::REF_SEED, reinterpret_cast<size_t>(value_ts));

        auto& registry = TimeSeriesTypeRegistry::global();
        if (auto* existing = registry.lookup_by_key(key)) {
            return existing;
        }

        auto meta = std::make_unique<REFTypeMeta>();
        meta->ts_kind = TimeSeriesKind::REF;
        meta->value_ts_type = value_ts;

        // Build RefTypeMeta via value registry
        const value::TypeMeta* value_type = value_ts ? value_ts->value_schema() : nullptr;
        size_t ref_key = value::hash_combine(0x52454600, reinterpret_cast<size_t>(value_type));

        auto& value_registry = value::TypeRegistry::global();
        if (auto* existing_ref = value_registry.lookup_by_key(ref_key)) {
            meta->ref_value_type = existing_ref;
        } else {
            auto ref_meta = value::RefTypeBuilder()
                .value_type(value_type)
                .build();
            meta->ref_value_type = value_registry.register_by_key(ref_key, std::move(ref_meta));
        }

        return registry.register_by_key(key, std::move(meta));
    }
};

// ============================================================================
// Bundle Field and Name Descriptors
// ============================================================================

/**
 * Field<"name", TSType> - Bundle field descriptor
 *
 * Defines a named field in a TSB bundle.
 * Usage: Field<"x", TS<int>>
 */
template<StringLiteral FieldName, typename TSType>
struct Field {
    static_assert(detail::is_ts_type_v<TSType>, "Field type must be a time-series type");

    using ts_type = TSType;
    static constexpr auto name = FieldName;

    static const char* field_name() {
        static std::string stored_name{FieldName.c_str()};
        return stored_name.c_str();
    }

    static const TimeSeriesTypeMeta* field_type() {
        return TSType::get();
    }
};

/**
 * Name<"TypeName"> - Bundle type name descriptor
 *
 * Provides an optional name for a TSB type.
 * Usage: Name<"Point">
 */
template<StringLiteral N>
struct Name {
    static constexpr auto value = N;

    static const char* type_name() {
        static std::string stored_name{N.c_str()};
        return stored_name.c_str();
    }
};

// ============================================================================
// TSB Implementation
// ============================================================================

namespace detail {

    // Helper to extract fields from TSB args
    template<typename... Args>
    struct TSBHelper {
        // Count fields (non-Name types)
        static constexpr size_t field_count = (... + (is_field_v<Args> ? 1 : 0));

        // Check if Name is provided
        static constexpr bool has_name = (... || is_name_v<Args>);

        // Hash key extraction helper
        template<typename T, typename = void>
        struct KeyHasher {
            static void apply(size_t&) {}
        };

        template<StringLiteral N, typename TSType>
        struct KeyHasher<Field<N, TSType>, void> {
            static void apply(size_t& key) {
                key = hash_combine(key, N.hash());
                key = hash_combine(key, reinterpret_cast<size_t>(Field<N, TSType>::field_type()));
            }
        };

        // Build hash key from fields
        static size_t compute_key() {
            size_t key = TSB_SEED;
            (KeyHasher<Args>::apply(key), ...);
            return key;
        }

        // Get type name if provided - helper struct to extract name
        template<typename T, typename = void>
        struct NameExtractor {
            static const char* get() { return nullptr; }
        };

        template<StringLiteral N>
        struct NameExtractor<Name<N>, void> {
            static const char* get() { return Name<N>::type_name(); }
        };

        static const char* get_name() {
            const char* result = nullptr;
            ((void)(is_name_v<Args> ? (result = NameExtractor<Args>::get(), 0) : 0), ...);
            return result;
        }

        // Field builder helper
        template<typename T, typename = void>
        struct FieldBuilder {
            static void apply(std::vector<TSBTypeMeta::Field>&) {}
        };

        template<StringLiteral N, typename TSType>
        struct FieldBuilder<Field<N, TSType>, void> {
            static void apply(std::vector<TSBTypeMeta::Field>& fields) {
                fields.push_back({Field<N, TSType>::field_name(), Field<N, TSType>::field_type()});
            }
        };

        // Build fields vector
        static std::vector<TSBTypeMeta::Field> build_fields() {
            std::vector<TSBTypeMeta::Field> fields;
            fields.reserve(field_count);
            (FieldBuilder<Args>::apply(fields), ...);
            return fields;
        }
    };

}  // namespace detail

/**
 * TSB<Fields..., Name?> - Time-series bundle descriptor
 *
 * Represents a structured bundle of named time-series fields.
 * Usage:
 *   ts_type<TSB<Field<"x", TS<int>>, Field<"y", TS<float>>>>()
 *   ts_type<TSB<Field<"x", TS<int>>, Name<"Point">>>()
 */
template<typename... Args>
struct TSB {
    static_assert(sizeof...(Args) > 0, "TSB requires at least one field");
    static_assert(detail::TSBHelper<Args...>::field_count > 0, "TSB requires at least one Field");

    static const TimeSeriesTypeMeta* get() {
        using Helper = detail::TSBHelper<Args...>;

        size_t key = Helper::compute_key();

        auto& registry = TimeSeriesTypeRegistry::global();
        if (auto* existing = registry.lookup_by_key(key)) {
            return existing;
        }

        auto meta = std::make_unique<TSBTypeMeta>();
        meta->ts_kind = TimeSeriesKind::TSB;
        meta->fields = Helper::build_fields();
        meta->name = Helper::get_name();

        // Build BundleTypeMeta via value registry
        value::BundleTypeBuilder builder;
        bool all_fields_have_value_schema = true;
        for (const auto& field : meta->fields) {
            auto* field_value_schema = field.type->value_schema();
            if (field_value_schema) {
                builder.add_field(field.name, field_value_schema);
            } else {
                all_fields_have_value_schema = false;
                break;
            }
        }

        if (all_fields_have_value_schema && !meta->fields.empty()) {
            size_t bundle_key = value::hash_combine(0x42554E44, key);
            auto& value_registry = value::TypeRegistry::global();
            if (auto* existing_bundle = value_registry.lookup_by_key(bundle_key)) {
                meta->bundle_value_type = existing_bundle;
            } else {
                auto bundle_meta = builder.build(meta->name);
                meta->bundle_value_type = value_registry.register_by_key(bundle_key, std::move(bundle_meta));
            }
        }

        return registry.register_by_key(key, std::move(meta));
    }
};

// ============================================================================
// Main API Function
// ============================================================================

/**
 * ts_type<T>() - Get the TimeSeriesTypeMeta for a time-series type descriptor
 *
 * This is the main entry point for obtaining time-series type metadata.
 * The type is automatically interned in the global registry.
 *
 * Usage:
 *   auto* ts_int = ts_type<TS<int>>();
 *   auto* tsl = ts_type<TSL<TS<int>, 3>>();
 *   auto* point = ts_type<TSB<Field<"x", TS<int>>, Field<"y", TS<float>>, Name<"Point">>>();
 */
template<typename T>
const TimeSeriesTypeMeta* ts_type() {
    static_assert(detail::is_ts_type_v<T>, "ts_type<T>() requires a time-series type descriptor");
    return T::get();
}

// ============================================================================
// Runtime/Dynamic Type Construction API
// ============================================================================

/**
 * Runtime type factory functions.
 *
 * These provide a single-method approach for runtime type construction
 * with automatic interning. Useful when types are determined at runtime
 * (e.g., from Python, configuration, or reflection).
 */
namespace runtime {

    /**
     * Get or create a TS[scalar_type] metadata.
     */
    inline const TimeSeriesTypeMeta* ts(const value::TypeMeta* scalar_type) {
        size_t key = detail::hash_combine(detail::TS_SEED, reinterpret_cast<size_t>(scalar_type));

        auto& registry = TimeSeriesTypeRegistry::global();
        if (auto* existing = registry.lookup_by_key(key)) {
            return existing;
        }

        auto meta = std::make_unique<TSTypeMeta>();
        meta->ts_kind = TimeSeriesKind::TS;
        meta->scalar_type = scalar_type;

        return registry.register_by_key(key, std::move(meta));
    }

    /**
     * Get or create a TSS[element_type] metadata.
     */
    inline const TimeSeriesTypeMeta* tss(const value::TypeMeta* element_type) {
        size_t key = detail::hash_combine(detail::TSS_SEED, reinterpret_cast<size_t>(element_type));

        auto& registry = TimeSeriesTypeRegistry::global();
        if (auto* existing = registry.lookup_by_key(key)) {
            return existing;
        }

        auto meta = std::make_unique<TSSTypeMeta>();
        meta->ts_kind = TimeSeriesKind::TSS;
        meta->element_type = element_type;

        // Build and register SetTypeMeta
        size_t set_key = value::hash_combine(0x53455400, reinterpret_cast<size_t>(element_type));
        auto& value_registry = value::TypeRegistry::global();
        if (auto* existing_set = value_registry.lookup_by_key(set_key)) {
            meta->set_value_type = existing_set;
        } else {
            auto set_meta = value::SetTypeBuilder()
                .element_type(element_type)
                .build();
            meta->set_value_type = value_registry.register_by_key(set_key, std::move(set_meta));
        }

        return registry.register_by_key(key, std::move(meta));
    }

    /**
     * Get or create a TSD[key_type, value_ts_type] metadata.
     */
    inline const TimeSeriesTypeMeta* tsd(const value::TypeMeta* key_type,
                                          const TimeSeriesTypeMeta* value_ts_type) {
        size_t key = detail::hash_combine(detail::TSD_SEED, reinterpret_cast<size_t>(key_type));
        key = detail::hash_combine(key, reinterpret_cast<size_t>(value_ts_type));

        auto& registry = TimeSeriesTypeRegistry::global();
        if (auto* existing = registry.lookup_by_key(key)) {
            return existing;
        }

        auto meta = std::make_unique<TSDTypeMeta>();
        meta->ts_kind = TimeSeriesKind::TSD;
        meta->key_type = key_type;
        meta->value_ts_type = value_ts_type;

        // Build DictTypeMeta via value registry
        // Dict uses key_type and the value_ts_type's value_schema
        if (key_type && value_ts_type) {
            const value::TypeMeta* value_schema = value_ts_type->value_schema();
            if (value_schema) {
                size_t dict_key = value::hash_combine(0x44494354, reinterpret_cast<size_t>(key_type));
                dict_key = value::hash_combine(dict_key, reinterpret_cast<size_t>(value_schema));

                auto& value_registry = value::TypeRegistry::global();
                if (auto* existing_dict = value_registry.lookup_by_key(dict_key)) {
                    meta->dict_value_type = existing_dict;
                } else {
                    auto dict_meta = value::DictTypeBuilder()
                        .key_type(key_type)
                        .value_type(value_schema)
                        .build();
                    meta->dict_value_type = value_registry.register_by_key(dict_key, std::move(dict_meta));
                }
            }
        }

        return registry.register_by_key(key, std::move(meta));
    }

    /**
     * Get or create a TSL[element_ts_type, size] metadata.
     * Use size=-1 for dynamic/unresolved size.
     */
    inline const TimeSeriesTypeMeta* tsl(const TimeSeriesTypeMeta* element_ts_type,
                                          int64_t size) {
        size_t key = detail::hash_combine(detail::TSL_SEED, reinterpret_cast<size_t>(element_ts_type));
        key = detail::hash_combine(key, static_cast<size_t>(size + 1));

        auto& registry = TimeSeriesTypeRegistry::global();
        if (auto* existing = registry.lookup_by_key(key)) {
            return existing;
        }

        auto meta = std::make_unique<TSLTypeMeta>();
        meta->ts_kind = TimeSeriesKind::TSL;
        meta->element_ts_type = element_ts_type;
        meta->size = size;

        // Build ListTypeMeta if applicable
        if (element_ts_type && element_ts_type->value_schema() && size > 0) {
            const auto* elem_value_schema = element_ts_type->value_schema();
            size_t list_key = value::hash_combine(0x4C495354, reinterpret_cast<size_t>(elem_value_schema));
            list_key = value::hash_combine(list_key, static_cast<size_t>(size));

            auto& value_registry = value::TypeRegistry::global();
            if (auto* existing_list = value_registry.lookup_by_key(list_key)) {
                meta->list_value_type = existing_list;
            } else {
                auto list_meta = value::ListTypeBuilder()
                    .element_type(elem_value_schema)
                    .count(static_cast<size_t>(size))
                    .build();
                meta->list_value_type = value_registry.register_by_key(list_key, std::move(list_meta));
            }
        }

        return registry.register_by_key(key, std::move(meta));
    }

    /**
     * Get or create a TSB metadata from field definitions.
     *
     * @param fields Vector of (name, type) pairs
     * @param type_name Optional type name (nullptr for anonymous)
     */
    inline const TimeSeriesTypeMeta* tsb(
            const std::vector<std::pair<std::string, const TimeSeriesTypeMeta*>>& fields,
            const char* type_name = nullptr) {

        // Compute key
        size_t key = detail::TSB_SEED;
        for (const auto& [name, type] : fields) {
            key = detail::hash_combine(key, std::hash<std::string>{}(name));
            key = detail::hash_combine(key, reinterpret_cast<size_t>(type));
        }

        auto& registry = TimeSeriesTypeRegistry::global();
        if (auto* existing = registry.lookup_by_key(key)) {
            return existing;
        }

        auto meta = std::make_unique<TSBTypeMeta>();
        meta->ts_kind = TimeSeriesKind::TSB;

        // Store field definitions
        for (const auto& [name, type] : fields) {
            meta->fields.push_back({name, type});
        }

        // Store type name persistently if provided
        if (type_name) {
            static std::vector<std::string> stored_names;
            stored_names.emplace_back(type_name);
            meta->name = stored_names.back().c_str();
        }

        // Build BundleTypeMeta
        value::BundleTypeBuilder builder;
        bool all_fields_have_value_schema = true;
        for (const auto& field : meta->fields) {
            auto* field_value_schema = field.type->value_schema();
            if (field_value_schema) {
                builder.add_field(field.name, field_value_schema);
            } else {
                all_fields_have_value_schema = false;
                break;
            }
        }

        if (all_fields_have_value_schema && !meta->fields.empty()) {
            size_t bundle_key = value::hash_combine(0x42554E44, key);
            auto& value_registry = value::TypeRegistry::global();
            if (auto* existing_bundle = value_registry.lookup_by_key(bundle_key)) {
                meta->bundle_value_type = existing_bundle;
            } else {
                auto bundle_meta = builder.build(meta->name);
                meta->bundle_value_type = value_registry.register_by_key(bundle_key, std::move(bundle_meta));
            }
        }

        return registry.register_by_key(key, std::move(meta));
    }

    /**
     * Get or create a count-based TSW[scalar_type, size, min_size] metadata.
     */
    inline const TimeSeriesTypeMeta* tsw(const value::TypeMeta* scalar_type,
                                          int64_t size,
                                          int64_t min_size = 0) {
        size_t key = detail::hash_combine(detail::TSW_SEED, reinterpret_cast<size_t>(scalar_type));
        key = detail::hash_combine(key, static_cast<size_t>(size + 1));
        key = detail::hash_combine(key, static_cast<size_t>(min_size + 1));

        auto& registry = TimeSeriesTypeRegistry::global();
        if (auto* existing = registry.lookup_by_key(key)) {
            return existing;
        }

        auto meta = std::make_unique<TSWTypeMeta>();
        meta->ts_kind = TimeSeriesKind::TSW;
        meta->scalar_type = scalar_type;
        meta->size = size;
        meta->min_size = min_size;

        // Build WindowTypeMeta via value registry
        if (scalar_type && size > 0) {
            size_t window_key = value::hash_combine(0x57494E44, reinterpret_cast<size_t>(scalar_type));
            window_key = value::hash_combine(window_key, static_cast<size_t>(size));

            auto& value_registry = value::TypeRegistry::global();
            if (auto* existing_window = value_registry.lookup_by_key(window_key)) {
                meta->window_value_type = existing_window;
            } else {
                auto window_meta = value::WindowTypeBuilder()
                    .element_type(scalar_type)
                    .fixed_count(static_cast<size_t>(size))
                    .build();
                meta->window_value_type = value_registry.register_by_key(window_key, std::move(window_meta));
            }
        }

        return registry.register_by_key(key, std::move(meta));
    }

    /**
     * Get or create a time-based TSW metadata.
     *
     * @param scalar_type The scalar type for window elements
     * @param duration_us Window duration in microseconds
     * @param min_size Minimum number of values required (0 = no minimum)
     */
    inline const TimeSeriesTypeMeta* tsw_time(const value::TypeMeta* scalar_type,
                                               int64_t duration_us,
                                               int64_t min_size = 0) {
        size_t key = detail::hash_combine(detail::TSW_SEED, reinterpret_cast<size_t>(scalar_type));
        key = detail::hash_combine(key, static_cast<size_t>(duration_us));
        key = detail::hash_combine(key, 0x54494D45);  // "TIME" marker
        key = detail::hash_combine(key, static_cast<size_t>(min_size + 1));

        auto& registry = TimeSeriesTypeRegistry::global();
        if (auto* existing = registry.lookup_by_key(key)) {
            return existing;
        }

        auto meta = std::make_unique<TSWTypeMeta>();
        meta->ts_kind = TimeSeriesKind::TSW;
        meta->scalar_type = scalar_type;
        meta->size = -duration_us;  // Negative indicates time-based
        meta->min_size = min_size;

        // Build WindowTypeMeta via value registry (time-based)
        if (scalar_type && duration_us > 0) {
            size_t window_key = value::hash_combine(0x57494E44, reinterpret_cast<size_t>(scalar_type));
            window_key = value::hash_combine(window_key, 0x54494D45);  // "TIME" marker
            window_key = value::hash_combine(window_key, static_cast<size_t>(duration_us));

            auto& value_registry = value::TypeRegistry::global();
            if (auto* existing_window = value_registry.lookup_by_key(window_key)) {
                meta->window_value_type = existing_window;
            } else {
                // Convert microseconds to nanoseconds for engine_time_delta_t
                engine_time_delta_t duration_ns{duration_us * 1000};
                auto window_meta = value::WindowTypeBuilder()
                    .element_type(scalar_type)
                    .time_duration(duration_ns)
                    .build();
                meta->window_value_type = value_registry.register_by_key(window_key, std::move(window_meta));
            }
        }

        return registry.register_by_key(key, std::move(meta));
    }

    /**
     * Get or create a REF[value_ts_type] metadata.
     */
    inline const TimeSeriesTypeMeta* ref(const TimeSeriesTypeMeta* value_ts_type) {
        size_t key = detail::hash_combine(detail::REF_SEED, reinterpret_cast<size_t>(value_ts_type));

        auto& registry = TimeSeriesTypeRegistry::global();
        if (auto* existing = registry.lookup_by_key(key)) {
            return existing;
        }

        auto meta = std::make_unique<REFTypeMeta>();
        meta->ts_kind = TimeSeriesKind::REF;
        meta->value_ts_type = value_ts_type;

        // Build RefTypeMeta
        const value::TypeMeta* value_type = value_ts_type ? value_ts_type->value_schema() : nullptr;
        size_t ref_key = value::hash_combine(0x52454600, reinterpret_cast<size_t>(value_type));

        auto& value_registry = value::TypeRegistry::global();
        if (auto* existing_ref = value_registry.lookup_by_key(ref_key)) {
            meta->ref_value_type = existing_ref;
        } else {
            auto ref_meta = value::RefTypeBuilder()
                .value_type(value_type)
                .build();
            meta->ref_value_type = value_registry.register_by_key(ref_key, std::move(ref_meta));
        }

        return registry.register_by_key(key, std::move(meta));
    }

}  // namespace runtime

// ============================================================================
// Runtime API with Python Conversion Support
// ============================================================================

#ifdef HGRAPH_TYPE_API_WITH_PYTHON
/**
 * Python-aware runtime type factory functions.
 *
 * These are identical to the runtime:: functions but use builders that
 * include Python conversion ops (SetTypeBuilderWithPython, etc.).
 * Use these when types will be accessed from Python.
 *
 * To use this namespace, define HGRAPH_TYPE_API_WITH_PYTHON before including type_api.h
 */
namespace runtime_python {

    /**
     * Get or create a TS[scalar_type] metadata.
     * (Same as runtime::ts - TS doesn't need special Python handling)
     */
    inline const TimeSeriesTypeMeta* ts(const value::TypeMeta* scalar_type) {
        return runtime::ts(scalar_type);
    }

    /**
     * Get or create a TSS[element_type] metadata with Python conversion support.
     */
    inline const TimeSeriesTypeMeta* tss(const value::TypeMeta* element_type) {
        size_t key = detail::hash_combine(detail::TSS_SEED, reinterpret_cast<size_t>(element_type));

        auto& registry = TimeSeriesTypeRegistry::global();
        if (auto* existing = registry.lookup_by_key(key)) {
            return existing;
        }

        auto meta = std::make_unique<TSSTypeMeta>();
        meta->ts_kind = TimeSeriesKind::TSS;
        meta->element_type = element_type;

        // Use Python-aware builder
        size_t set_key = value::hash_combine(0x53455400, reinterpret_cast<size_t>(element_type));
        auto& value_registry = value::TypeRegistry::global();
        if (auto* existing_set = value_registry.lookup_by_key(set_key)) {
            meta->set_value_type = existing_set;
        } else {
            auto set_meta = value::SetTypeBuilderWithPython()
                .element_type(element_type)
                .build();
            meta->set_value_type = value_registry.register_by_key(set_key, std::move(set_meta));
        }

        return registry.register_by_key(key, std::move(meta));
    }

    /**
     * Get or create a TSD[key_type, value_ts_type] metadata.
     * (Same as runtime::tsd - TSD doesn't need special Python handling for the container)
     */
    inline const TimeSeriesTypeMeta* tsd(const value::TypeMeta* key_type,
                                          const TimeSeriesTypeMeta* value_ts_type) {
        return runtime::tsd(key_type, value_ts_type);
    }

    /**
     * Get or create a TSL[element_ts_type, size] metadata with Python conversion support.
     */
    inline const TimeSeriesTypeMeta* tsl(const TimeSeriesTypeMeta* element_ts_type,
                                          int64_t size) {
        size_t key = detail::hash_combine(detail::TSL_SEED, reinterpret_cast<size_t>(element_ts_type));
        key = detail::hash_combine(key, static_cast<size_t>(size + 1));

        auto& registry = TimeSeriesTypeRegistry::global();
        if (auto* existing = registry.lookup_by_key(key)) {
            return existing;
        }

        auto meta = std::make_unique<TSLTypeMeta>();
        meta->ts_kind = TimeSeriesKind::TSL;
        meta->element_ts_type = element_ts_type;
        meta->size = size;

        // Use Python-aware builder
        if (element_ts_type && element_ts_type->value_schema() && size > 0) {
            const auto* elem_value_schema = element_ts_type->value_schema();
            size_t list_key = value::hash_combine(0x4C495354, reinterpret_cast<size_t>(elem_value_schema));
            list_key = value::hash_combine(list_key, static_cast<size_t>(size));

            auto& value_registry = value::TypeRegistry::global();
            if (auto* existing_list = value_registry.lookup_by_key(list_key)) {
                meta->list_value_type = existing_list;
            } else {
                auto list_meta = value::ListTypeBuilderWithPython()
                    .element_type(elem_value_schema)
                    .count(static_cast<size_t>(size))
                    .build();
                meta->list_value_type = value_registry.register_by_key(list_key, std::move(list_meta));
            }
        }

        return registry.register_by_key(key, std::move(meta));
    }

    /**
     * Get or create a TSB metadata with Python conversion support.
     */
    inline const TimeSeriesTypeMeta* tsb(
            const std::vector<std::pair<std::string, const TimeSeriesTypeMeta*>>& fields,
            const char* type_name = nullptr) {

        size_t key = detail::TSB_SEED;
        for (const auto& [name, type] : fields) {
            key = detail::hash_combine(key, std::hash<std::string>{}(name));
            key = detail::hash_combine(key, reinterpret_cast<size_t>(type));
        }

        auto& registry = TimeSeriesTypeRegistry::global();
        if (auto* existing = registry.lookup_by_key(key)) {
            return existing;
        }

        auto meta = std::make_unique<TSBTypeMeta>();
        meta->ts_kind = TimeSeriesKind::TSB;

        for (const auto& [name, type] : fields) {
            meta->fields.push_back({name, type});
        }

        if (type_name) {
            static std::vector<std::string> stored_names;
            stored_names.emplace_back(type_name);
            meta->name = stored_names.back().c_str();
        }

        // Use Python-aware builder
        value::BundleTypeBuilderWithPython builder;
        bool all_fields_have_value_schema = true;
        for (const auto& field : meta->fields) {
            auto* field_value_schema = field.type->value_schema();
            if (field_value_schema) {
                builder.add_field(field.name, field_value_schema);
            } else {
                all_fields_have_value_schema = false;
                break;
            }
        }

        if (all_fields_have_value_schema && !meta->fields.empty()) {
            size_t bundle_key = value::hash_combine(0x42554E44, key);
            auto& value_registry = value::TypeRegistry::global();
            if (auto* existing_bundle = value_registry.lookup_by_key(bundle_key)) {
                meta->bundle_value_type = existing_bundle;
            } else {
                auto bundle_meta = builder.build(meta->name);
                meta->bundle_value_type = value_registry.register_by_key(bundle_key, std::move(bundle_meta));
            }
        }

        return registry.register_by_key(key, std::move(meta));
    }

    /**
     * Get or create a count-based TSW metadata.
     * (Same as runtime::tsw - TSW doesn't need special Python handling)
     */
    inline const TimeSeriesTypeMeta* tsw(const value::TypeMeta* scalar_type,
                                          int64_t size,
                                          int64_t min_size = 0) {
        return runtime::tsw(scalar_type, size, min_size);
    }

    /**
     * Get or create a time-based TSW metadata.
     * (Same as runtime::tsw_time - TSW doesn't need special Python handling)
     */
    inline const TimeSeriesTypeMeta* tsw_time(const value::TypeMeta* scalar_type,
                                               int64_t duration_us,
                                               int64_t min_size = 0) {
        return runtime::tsw_time(scalar_type, duration_us, min_size);
    }

    /**
     * Get or create a REF[value_ts_type] metadata with Python conversion support.
     */
    inline const TimeSeriesTypeMeta* ref(const TimeSeriesTypeMeta* value_ts_type) {
        size_t key = detail::hash_combine(detail::REF_SEED, reinterpret_cast<size_t>(value_ts_type));

        auto& registry = TimeSeriesTypeRegistry::global();
        if (auto* existing = registry.lookup_by_key(key)) {
            return existing;
        }

        auto meta = std::make_unique<REFTypeMeta>();
        meta->ts_kind = TimeSeriesKind::REF;
        meta->value_ts_type = value_ts_type;

        // Use Python-aware ops
        const value::TypeMeta* value_type = value_ts_type ? value_ts_type->value_schema() : nullptr;
        size_t ref_key = value::hash_combine(0x52454600, reinterpret_cast<size_t>(value_type));

        auto& value_registry = value::TypeRegistry::global();
        if (auto* existing_ref = value_registry.lookup_by_key(ref_key)) {
            meta->ref_value_type = existing_ref;
        } else {
            auto ref_meta = value::RefTypeBuilder()
                .value_type(value_type)
                .build();
            ref_meta->ops = &value::RefTypeOpsWithPython;
            meta->ref_value_type = value_registry.register_by_key(ref_key, std::move(ref_meta));
        }

        return registry.register_by_key(key, std::move(meta));
    }

}  // namespace runtime_python
#endif // HGRAPH_TYPE_API_WITH_PYTHON

}  // namespace hgraph::types

#endif // HGRAPH_TYPE_API_H
