//
// Created by Howard Henson on 12/12/2025.
//

#ifndef HGRAPH_VALUE_SCALAR_TYPE_H
#define HGRAPH_VALUE_SCALAR_TYPE_H

#include <hgraph/types/value/type_meta.h>
#include <type_traits>
#include <concepts>
#include <cstring>
#include <sstream>
#include <iomanip>
#include <cmath>

namespace hgraph::value {

    /**
     * ScalarTypeOps - Generate TypeOps for a scalar type T
     */
    template<typename T>
    struct ScalarTypeOps {
        static void construct(void* dest, const TypeMeta*) {
            new (dest) T{};
        }

        static void destruct(void* dest, const TypeMeta*) {
            static_cast<T*>(dest)->~T();
        }

        static void copy_construct(void* dest, const void* src, const TypeMeta*) {
            new (dest) T(*static_cast<const T*>(src));
        }

        static void move_construct(void* dest, void* src, const TypeMeta*) {
            new (dest) T(std::move(*static_cast<T*>(src)));
        }

        static void copy_assign(void* dest, const void* src, const TypeMeta*) {
            *static_cast<T*>(dest) = *static_cast<const T*>(src);
        }

        static void move_assign(void* dest, void* src, const TypeMeta*) {
            *static_cast<T*>(dest) = std::move(*static_cast<T*>(src));
        }

        static bool equals(const void* a, const void* b, const TypeMeta*) {
            if constexpr (requires(const T& x, const T& y) { x == y; }) {
                return *static_cast<const T*>(a) == *static_cast<const T*>(b);
            } else {
                return false;
            }
        }

        static bool less_than(const void* a, const void* b, const TypeMeta*) {
            if constexpr (requires(const T& x, const T& y) { x < y; }) {
                return *static_cast<const T*>(a) < *static_cast<const T*>(b);
            } else {
                return false;
            }
        }

        static size_t hash(const void* v, const TypeMeta*) {
            if constexpr (requires(const T& x) { std::hash<T>{}(x); }) {
                return std::hash<T>{}(*static_cast<const T*>(v));
            } else {
                return 0;
            }
        }

        static std::string to_string(const void* v, const TypeMeta* meta) {
            const T& value = *static_cast<const T*>(v);
            if constexpr (std::is_same_v<T, bool>) {
                return value ? "true" : "false";
            } else if constexpr (std::is_integral_v<T>) {
                return std::to_string(value);
            } else if constexpr (std::is_floating_point_v<T>) {
                std::ostringstream oss;
                oss << std::setprecision(6) << value;
                return oss.str();
            } else if constexpr (std::is_same_v<T, std::string>) {
                return "\"" + value + "\"";
            } else if constexpr (requires { std::declval<std::ostream&>() << value; }) {
                // Handles engine_time_t, engine_date_t, engine_time_delta_t, etc.
                std::ostringstream oss;
                oss << value;
                return oss.str();
            } else {
                // Fallback: use type name if available
                return meta->name ? meta->name : "<unknown>";
            }
        }

        static std::string type_name(const TypeMeta* meta) {
            // Map C++ types to Python-style names
            if constexpr (std::is_same_v<T, bool>) {
                return "bool";
            } else if constexpr (std::is_same_v<T, int64_t> || (std::is_same_v<T, long> && sizeof(long) == 8)) {
                return "int";
            } else if constexpr (std::is_same_v<T, int32_t> || (std::is_same_v<T, int> && sizeof(int) == 4)) {
                return "int";  // Python doesn't distinguish int sizes
            } else if constexpr (std::is_same_v<T, int16_t>) {
                return "int";
            } else if constexpr (std::is_same_v<T, int8_t>) {
                return "int";
            } else if constexpr (std::is_same_v<T, uint64_t> || std::is_same_v<T, uint32_t> ||
                                 std::is_same_v<T, uint16_t> || std::is_same_v<T, uint8_t>) {
                return "int";  // Python doesn't have unsigned
            } else if constexpr (std::is_same_v<T, double> || std::is_same_v<T, float>) {
                return "float";
            } else if constexpr (std::is_same_v<T, std::string>) {
                return "str";
            } else {
                // For other types (datetime, date, timedelta, object), use the name if set
                if (meta->name) return meta->name;
                // Fallback to type_info
                if (meta->type_info) return meta->type_info->name();
                return "<unknown>";
            }
        }

        // =========================================================================
        // Arithmetic binary operations - C++ implementations for numeric types
        // =========================================================================

        static bool add(void* dest, const void* a, const void* b, const TypeMeta*) {
            // Check that T + T produces T (not some other type)
            if constexpr (requires(T x, T y) { { x + y } -> std::convertible_to<T>; }) {
                *static_cast<T*>(dest) = *static_cast<const T*>(a) + *static_cast<const T*>(b);
                return true;
            } else {
                return false;
            }
        }

        static bool subtract(void* dest, const void* a, const void* b, const TypeMeta*) {
            // Check that T - T produces T (not some other type like duration for time_points)
            if constexpr (requires(T x, T y) { { x - y } -> std::convertible_to<T>; }) {
                *static_cast<T*>(dest) = *static_cast<const T*>(a) - *static_cast<const T*>(b);
                return true;
            } else {
                return false;
            }
        }

        static bool multiply(void* dest, const void* a, const void* b, const TypeMeta*) {
            // Check that T * T produces T (not some other type)
            if constexpr (requires(T x, T y) { { x * y } -> std::convertible_to<T>; }) {
                *static_cast<T*>(dest) = *static_cast<const T*>(a) * *static_cast<const T*>(b);
                return true;
            } else {
                return false;
            }
        }

        static bool divide(void* dest, const void* a, const void* b, const TypeMeta*) {
            // Check that T / T produces T (not some other type like scalar for durations)
            if constexpr (requires(T x, T y) { { x / y } -> std::convertible_to<T>; }) {
                *static_cast<T*>(dest) = *static_cast<const T*>(a) / *static_cast<const T*>(b);
                return true;
            } else {
                return false;
            }
        }

        static bool floor_divide(void* dest, const void* a, const void* b, const TypeMeta*) {
            if constexpr (std::is_integral_v<T>) {
                // Integer division is already floor division for positive numbers
                // For negative, we need to adjust
                T av = *static_cast<const T*>(a);
                T bv = *static_cast<const T*>(b);
                if (bv == 0) return false;
                T result = av / bv;
                // Python-style floor division: round toward negative infinity
                if ((av < 0) != (bv < 0) && av % bv != 0) {
                    result -= 1;
                }
                *static_cast<T*>(dest) = result;
                return true;
            } else if constexpr (std::is_floating_point_v<T>) {
                *static_cast<T*>(dest) = std::floor(*static_cast<const T*>(a) / *static_cast<const T*>(b));
                return true;
            } else {
                return false;
            }
        }

        static bool modulo(void* dest, const void* a, const void* b, const TypeMeta*) {
            if constexpr (std::is_integral_v<T>) {
                T av = *static_cast<const T*>(a);
                T bv = *static_cast<const T*>(b);
                if (bv == 0) return false;
                // Python-style modulo: result has same sign as divisor
                T result = av % bv;
                if ((result < 0 && bv > 0) || (result > 0 && bv < 0)) {
                    result += bv;
                }
                *static_cast<T*>(dest) = result;
                return true;
            } else if constexpr (std::is_floating_point_v<T>) {
                *static_cast<T*>(dest) = std::fmod(*static_cast<const T*>(a), *static_cast<const T*>(b));
                return true;
            } else {
                return false;
            }
        }

        static bool power(void* dest, const void* a, const void* b, const TypeMeta*) {
            if constexpr (std::is_arithmetic_v<T>) {
                *static_cast<T*>(dest) = static_cast<T>(std::pow(
                    static_cast<double>(*static_cast<const T*>(a)),
                    static_cast<double>(*static_cast<const T*>(b))
                ));
                return true;
            } else {
                return false;
            }
        }

        // =========================================================================
        // Arithmetic unary operations
        // =========================================================================

        static bool negate(void* dest, const void* src, const TypeMeta*) {
            if constexpr (requires(T x) { -x; }) {
                *static_cast<T*>(dest) = -*static_cast<const T*>(src);
                return true;
            } else {
                return false;
            }
        }

        static bool absolute(void* dest, const void* src, const TypeMeta*) {
            if constexpr (std::is_arithmetic_v<T>) {
                T val = *static_cast<const T*>(src);
                *static_cast<T*>(dest) = val < 0 ? -val : val;
                return true;
            } else {
                return false;
            }
        }

        static bool invert(void* dest, const void* src, const TypeMeta*) {
            if constexpr (std::is_integral_v<T>) {
                *static_cast<T*>(dest) = ~(*static_cast<const T*>(src));
                return true;
            } else {
                return false;
            }
        }

        // =========================================================================
        // Boolean conversion
        // =========================================================================

        static bool to_bool(const void* v, const TypeMeta*) {
            if constexpr (std::is_arithmetic_v<T>) {
                return *static_cast<const T*>(v) != T{};
            } else if constexpr (std::is_same_v<T, std::string>) {
                return !static_cast<const T*>(v)->empty();
            } else {
                return true;  // Non-null objects are truthy by default
            }
        }

        // =========================================================================
        // Container operations (not supported for scalars)
        // =========================================================================

        static size_t length(const void*, const TypeMeta*) {
            return 0;  // Scalars have no length
        }

        static bool contains(const void*, const void*, const TypeMeta*) {
            return false;  // Scalars don't support 'in'
        }

        static constexpr TypeOps ops = {
            .construct = construct,
            .destruct = destruct,
            .copy_construct = copy_construct,
            .move_construct = move_construct,
            .copy_assign = copy_assign,
            .move_assign = move_assign,
            .equals = equals,
            .less_than = less_than,
            .hash = hash,
            .to_string = to_string,
            .type_name = type_name,
            .to_python = nullptr,
            .from_python = nullptr,
            .add = add,
            .subtract = subtract,
            .multiply = multiply,
            .divide = divide,
            .floor_divide = floor_divide,
            .modulo = modulo,
            .power = power,
            .negate = negate,
            .absolute = absolute,
            .invert = invert,
            .to_bool = to_bool,
            .length = nullptr,  // Not supported for scalars
            .contains = nullptr,  // Not supported for scalars
        };
    };

    /**
     * Compute TypeFlags for a type T
     */
    template<typename T>
    constexpr TypeFlags compute_flags() {
        TypeFlags flags = TypeFlags::None;

        if constexpr (std::is_trivially_default_constructible_v<T>) {
            flags = flags | TypeFlags::TriviallyConstructible;
        }
        if constexpr (std::is_trivially_destructible_v<T>) {
            flags = flags | TypeFlags::TriviallyDestructible;
        }
        if constexpr (std::is_trivially_copyable_v<T>) {
            flags = flags | TypeFlags::TriviallyCopyable;
            flags = flags | TypeFlags::BufferCompatible;
        }
        if constexpr (requires(const T& x, const T& y) { x == y; }) {
            flags = flags | TypeFlags::Equatable;
        }
        if constexpr (requires(const T& x, const T& y) { x < y; x == y; }) {
            flags = flags | TypeFlags::Comparable;
        }
        if constexpr (requires(const T& x) { std::hash<T>{}(x); }) {
            flags = flags | TypeFlags::Hashable;
        }
        // Arithmetic: types that support +, -, *, /
        if constexpr (std::is_arithmetic_v<T> && !std::is_same_v<T, bool>) {
            flags = flags | TypeFlags::Arithmetic;
        }
        // Integral: types that support //, %, ~
        if constexpr (std::is_integral_v<T> && !std::is_same_v<T, bool>) {
            flags = flags | TypeFlags::Integral;
        }

        return flags;
    }

    /**
     * Compile-time numpy format resolver
     * Returns the numpy dtype format character for buffer-compatible scalar types
     */
    template<typename T>
    constexpr const char* numpy_format_for() {
        if constexpr (std::is_same_v<T, bool>) return "?";
        else if constexpr (std::is_same_v<T, int8_t>) return "b";
        else if constexpr (std::is_same_v<T, uint8_t>) return "B";
        else if constexpr (std::is_same_v<T, int16_t>) return "h";
        else if constexpr (std::is_same_v<T, uint16_t>) return "H";
        else if constexpr (std::is_same_v<T, int32_t>) return "i";
        else if constexpr (std::is_same_v<T, uint32_t>) return "I";
        else if constexpr (std::is_same_v<T, int64_t>) return "q";
        else if constexpr (std::is_same_v<T, uint64_t>) return "Q";
        else if constexpr (std::is_same_v<T, float>) return "f";
        else if constexpr (std::is_same_v<T, double>) return "d";
        // Handle platform-specific int/long
        else if constexpr (std::is_same_v<T, int> && sizeof(int) == 4) return "i";
        else if constexpr (std::is_same_v<T, int> && sizeof(int) == 8) return "q";
        else if constexpr (std::is_same_v<T, long> && sizeof(long) == 4) return "l";
        else if constexpr (std::is_same_v<T, long> && sizeof(long) == 8) return "q";
        else if constexpr (std::is_same_v<T, unsigned int> && sizeof(unsigned int) == 4) return "I";
        else if constexpr (std::is_same_v<T, unsigned long> && sizeof(unsigned long) == 4) return "L";
        else if constexpr (std::is_same_v<T, unsigned long> && sizeof(unsigned long) == 8) return "Q";
        else return nullptr;  // Not numpy-compatible
    }

    /**
     * ScalarTypeMeta - TypeMeta for scalar types
     *
     * Usage:
     *   const TypeMeta* int_meta = ScalarTypeMeta<int>::get();
     */
    template<typename T>
    struct ScalarTypeMeta {
        static const TypeMeta instance;

        static const TypeMeta* get() { return &instance; }
    };

    template<typename T>
    const TypeMeta ScalarTypeMeta<T>::instance = {
        .size = sizeof(T),
        .alignment = alignof(T),
        .flags = compute_flags<T>(),
        .kind = TypeKind::Scalar,
        .ops = &ScalarTypeOps<T>::ops,
        .type_info = &typeid(T),
        .name = nullptr,
        .numpy_format = numpy_format_for<T>(),
    };

    /**
     * Helper to get TypeMeta for any scalar type
     */
    template<typename T>
    const TypeMeta* scalar_type_meta() {
        return ScalarTypeMeta<T>::get();
    }

    /**
     * TypedValue - Owns storage for a value with its TypeMeta
     *
     * This provides isolated access to a single value within
     * potentially larger storage (e.g., a field in a bundle).
     */
    struct TypedValue {
        void* _storage{nullptr};
        const TypeMeta* _meta{nullptr};
        bool _owns_storage{false};

        TypedValue() = default;

        // Create with external storage (non-owning)
        TypedValue(void* storage, const TypeMeta* meta)
            : _storage(storage), _meta(meta), _owns_storage(false) {}

        // Create with owned storage
        static TypedValue create(const TypeMeta* meta) {
            TypedValue tv;
            tv._meta = meta;
            tv._storage = ::operator new(meta->size, std::align_val_t{meta->alignment});
            tv._owns_storage = true;
            meta->construct_at(tv._storage);
            return tv;
        }

        ~TypedValue() {
            if (_owns_storage && _storage && _meta) {
                _meta->destruct_at(_storage);
                ::operator delete(_storage, std::align_val_t{_meta->alignment});
            }
        }

        // Move only
        TypedValue(TypedValue&& other) noexcept
            : _storage(other._storage)
            , _meta(other._meta)
            , _owns_storage(other._owns_storage) {
            other._storage = nullptr;
            other._owns_storage = false;
        }

        TypedValue& operator=(TypedValue&& other) noexcept {
            if (this != &other) {
                if (_owns_storage && _storage && _meta) {
                    _meta->destruct_at(_storage);
                    ::operator delete(_storage, std::align_val_t{_meta->alignment});
                }
                _storage = other._storage;
                _meta = other._meta;
                _owns_storage = other._owns_storage;
                other._storage = nullptr;
                other._owns_storage = false;
            }
            return *this;
        }

        TypedValue(const TypedValue&) = delete;
        TypedValue& operator=(const TypedValue&) = delete;

        [[nodiscard]] bool valid() const { return _storage && _meta; }
        [[nodiscard]] const TypeMeta* meta() const { return _meta; }

        [[nodiscard]] TypedPtr ptr() { return {_storage, _meta}; }
        [[nodiscard]] ConstTypedPtr ptr() const { return {_storage, _meta}; }

        template<typename T>
        [[nodiscard]] T& as() { return *static_cast<T*>(_storage); }

        template<typename T>
        [[nodiscard]] const T& as() const { return *static_cast<const T*>(_storage); }

        void copy_from(const TypedValue& other) {
            if (valid() && other.valid() && _meta == other._meta) {
                _meta->copy_assign_at(_storage, other._storage);
            }
        }

        void copy_from(ConstTypedPtr src) {
            if (valid() && src.valid() && _meta == src.meta) {
                _meta->copy_assign_at(_storage, src.ptr);
            }
        }

        [[nodiscard]] bool equals(const TypedValue& other) const {
            if (!valid() || !other.valid() || _meta != other._meta) return false;
            return _meta->equals_at(_storage, other._storage);
        }

        [[nodiscard]] size_t hash() const {
            return valid() ? _meta->hash_at(_storage) : 0;
        }
    };

} // namespace hgraph::value

#endif // HGRAPH_VALUE_SCALAR_TYPE_H
