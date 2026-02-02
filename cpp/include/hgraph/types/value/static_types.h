#pragma once

/**
 * @file static_types.h
 * @brief Static template definitions for compile-time type construction.
 *
 * Provides type aliases that enable declarative type definitions:
 *
 * @code
 * // Compile-time bundle definition
 * using PointSchema = Bundle<
 *     name<"Point">,
 *     field<"x", double>,
 *     field<"y", double>,
 *     field<"z", double>
 * >;
 *
 * // Get schema at runtime
 * const TypeMeta* schema = PointSchema::schema();
 * @endcode
 *
 * These templates use lazy initialization - schemas are only registered
 * when their schema() method is first called.
 */

#include <hgraph/types/value/type_registry.h>

#include <algorithm>
#include <array>
#include <cstddef>
#include <string_view>

namespace hgraph::value {

// ============================================================================
// Fixed String (Compile-Time String Literal)
// ============================================================================

/**
 * @brief Compile-time string literal holder.
 *
 * Enables using string literals as template parameters (C++20 feature):
 * @code
 * template<fixed_string Name>
 * struct my_type { ... };
 *
 * my_type<"hello"> x;  // Name = "hello"
 * @endcode
 */
template<size_t N>
struct fixed_string {
    char data[N]{};
    static constexpr size_t size = N - 1;  // Exclude null terminator

    constexpr fixed_string() = default;

    constexpr fixed_string(const char (&str)[N]) {
        std::copy_n(str, N, data);
    }

    constexpr operator std::string_view() const noexcept {
        return std::string_view(data, size);
    }

    constexpr const char* c_str() const noexcept { return data; }

    constexpr bool operator==(const fixed_string& other) const noexcept {
        for (size_t i = 0; i < N; ++i) {
            if (data[i] != other.data[i]) return false;
        }
        return true;
    }
};

// Deduction guide
template<size_t N>
fixed_string(const char (&)[N]) -> fixed_string<N>;

// ============================================================================
// Name Tag
// ============================================================================

/**
 * @brief Compile-time name tag for bundles.
 *
 * @code
 * using MyBundle = Bundle<name<"MyBundle">, ...>;
 * @endcode
 */
template<fixed_string Name>
struct name {
    static constexpr auto value = Name;
    static constexpr std::string_view str() { return Name; }
};

// ============================================================================
// Field Definition
// ============================================================================

/**
 * @brief Compile-time field definition for bundles.
 *
 * The type T can be:
 * - A C++ scalar type (int64_t, double, bool, etc.)
 * - A static type template (Bundle<...>, List<...>, etc.)
 *
 * @code
 * field<"x", double>           // Scalar field
 * field<"items", List<int64_t>> // List field
 * @endcode
 */
template<fixed_string Name, typename T>
struct field {
    static constexpr auto name = Name;
    using type = T;
};

// ============================================================================
// Type Traits for Static Types
// ============================================================================

namespace detail {

/**
 * @brief Get TypeMeta for a type.
 *
 * Specializations handle:
 * - Scalar types (registered via TypeRegistry)
 * - Static type templates (have schema() method)
 */
template<typename T, typename = void>
struct type_meta_of {
    // Primary template: assumes T is a scalar type registered with TypeRegistry
    static const TypeMeta* get() {
        return scalar_type_meta<T>();
    }
};

// Specialization for types with schema() method (static templates)
template<typename T>
struct type_meta_of<T, std::void_t<decltype(T::schema())>> {
    static const TypeMeta* get() {
        return T::schema();
    }
};

template<typename T>
const TypeMeta* get_type_meta() {
    return type_meta_of<T>::get();
}

// Check if type has schema() method
template<typename T, typename = void>
struct has_schema : std::false_type {};

template<typename T>
struct has_schema<T, std::void_t<decltype(T::schema())>> : std::true_type {};

template<typename T>
constexpr bool has_schema_v = has_schema<T>::value;

} // namespace detail

// ============================================================================
// Tuple (Heterogeneous, Positional Access)
// ============================================================================

/**
 * @brief Static tuple type definition.
 *
 * @code
 * using MyTuple = Tuple<int64_t, double, bool>;
 * const TypeMeta* schema = MyTuple::schema();
 * @endcode
 */
template<typename... Elements>
struct Tuple {
    static const TypeMeta* schema() {
        static const TypeMeta* s = []() {
            auto& registry = TypeRegistry::instance();
            auto builder = registry.tuple();
            (builder.element(detail::get_type_meta<Elements>()), ...);
            return builder.build();
        }();
        return s;
    }
};

// ============================================================================
// Bundle (Named Fields)
// ============================================================================

namespace detail {

// Extract name from name<...> tag
template<typename T>
struct extract_name {
    static constexpr auto value = fixed_string<1>{""};
};

template<fixed_string Name>
struct extract_name<name<Name>> {
    static constexpr auto value = Name;
};

// Check if type is a name tag
template<typename T>
struct is_name_tag : std::false_type {};

template<fixed_string Name>
struct is_name_tag<name<Name>> : std::true_type {};

template<typename T>
constexpr bool is_name_tag_v = is_name_tag<T>::value;

// Check if type is a field
template<typename T>
struct is_field : std::false_type {};

template<fixed_string Name, typename T>
struct is_field<field<Name, T>> : std::true_type {};

template<typename T>
constexpr bool is_field_v = is_field<T>::value;

// Add field to builder
template<typename Field>
void add_field_to_builder(BundleTypeBuilder& builder) {
    builder.field(Field::name.c_str(), get_type_meta<typename Field::type>());
}

} // namespace detail

/**
 * @brief Static bundle type definition.
 *
 * Can be used with or without a name:
 * - Named: First argument is name<"...">, followed by field<...> arguments
 * - Anonymous: All arguments are field<...> (no name tag)
 *
 * @code
 * // Named bundle
 * using PointSchema = Bundle<
 *     name<"Point">,
 *     field<"x", double>,
 *     field<"y", double>,
 *     field<"z", double>
 * >;
 *
 * // Anonymous bundle (no name)
 * using AnonymousPoint = Bundle<
 *     field<"x", double>,
 *     field<"y", double>
 * >;
 *
 * const TypeMeta* schema = PointSchema::schema();
 * @endcode
 */
template<typename NameTag, typename... Fields>
struct Bundle {
    static_assert(detail::is_name_tag_v<NameTag>, "First argument must be name<\"...\">");
    static_assert((detail::is_field_v<Fields> && ...), "Remaining arguments must be field<\"...\", Type>");

    static constexpr auto bundle_name = detail::extract_name<NameTag>::value;

    static const TypeMeta* schema() {
        static const TypeMeta* s = []() {
            auto& registry = TypeRegistry::instance();
            auto builder = registry.bundle(std::string(bundle_name.c_str()));
            (detail::add_field_to_builder<Fields>(builder), ...);
            return builder.build();
        }();
        return s;
    }
};

// Anonymous bundle (no name tag)
template<typename FirstField, typename... RestFields>
    requires (detail::is_field_v<FirstField>)
struct Bundle<FirstField, RestFields...> {
    static_assert((detail::is_field_v<RestFields> && ...), "All arguments must be field<\"...\", Type>");

    static const TypeMeta* schema() {
        static const TypeMeta* s = []() {
            auto& registry = TypeRegistry::instance();
            auto builder = registry.bundle();
            detail::add_field_to_builder<FirstField>(builder);
            (detail::add_field_to_builder<RestFields>(builder), ...);
            return builder.build();
        }();
        return s;
    }
};

// ============================================================================
// List (Homogeneous, Dynamic or Fixed Size)
// ============================================================================

/**
 * @brief Static list type definition.
 *
 * @code
 * // Dynamic list
 * using PriceList = List<double>;
 *
 * // Fixed-size list
 * using FixedList = List<double, 10>;
 * @endcode
 */
template<typename T, size_t Size = 0>
struct List {
    static const TypeMeta* schema() {
        static const TypeMeta* s = []() {
            auto& registry = TypeRegistry::instance();
            if constexpr (Size == 0) {
                return registry.list(detail::get_type_meta<T>()).build();
            } else {
                return registry.fixed_list(detail::get_type_meta<T>(), Size).build();
            }
        }();
        return s;
    }
};

// ============================================================================
// Set (Unique Elements)
// ============================================================================

/**
 * @brief Static set type definition.
 *
 * @code
 * using IdSet = Set<int64_t>;
 * @endcode
 */
template<typename T>
struct Set {
    static const TypeMeta* schema() {
        static const TypeMeta* s = []() {
            auto& registry = TypeRegistry::instance();
            return registry.set(detail::get_type_meta<T>()).build();
        }();
        return s;
    }
};

// ============================================================================
// Map (Key-Value Pairs)
// ============================================================================

/**
 * @brief Static map type definition.
 *
 * @code
 * using ScoreMap = Map<int64_t, double>;
 * @endcode
 */
template<typename K, typename V>
struct Map {
    static const TypeMeta* schema() {
        static const TypeMeta* s = []() {
            auto& registry = TypeRegistry::instance();
            return registry.map(
                detail::get_type_meta<K>(),
                detail::get_type_meta<V>()
            ).build();
        }();
        return s;
    }
};

// ============================================================================
// CyclicBuffer
// ============================================================================

/**
 * @brief Static cyclic buffer type definition.
 *
 * @code
 * using Buffer10 = CyclicBuffer<double, 10>;
 * @endcode
 */
template<typename T, size_t Capacity>
struct CyclicBuffer {
    static_assert(Capacity > 0, "CyclicBuffer capacity must be > 0");

    static const TypeMeta* schema() {
        static const TypeMeta* s = []() {
            auto& registry = TypeRegistry::instance();
            return registry.cyclic_buffer(detail::get_type_meta<T>(), Capacity).build();
        }();
        return s;
    }
};

// ============================================================================
// Queue
// ============================================================================

/**
 * @brief Static queue type definition.
 *
 * @code
 * // Unbounded queue
 * using UnboundedQueue = Queue<int64_t>;
 *
 * // Bounded queue
 * using BoundedQueue = Queue<int64_t, 100>;
 * @endcode
 */
template<typename T, size_t MaxCapacity = 0>
struct Queue {
    static const TypeMeta* schema() {
        static const TypeMeta* s = []() {
            auto& registry = TypeRegistry::instance();
            auto builder = registry.queue(detail::get_type_meta<T>());
            if constexpr (MaxCapacity > 0) {
                builder.max_capacity(MaxCapacity);
            }
            return builder.build();
        }();
        return s;
    }
};

} // namespace hgraph::value
