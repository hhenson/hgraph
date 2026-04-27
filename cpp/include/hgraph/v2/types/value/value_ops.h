#ifndef HGRAPH_CPP_ROOT_VALUE_OPS_H
#define HGRAPH_CPP_ROOT_VALUE_OPS_H

#include <hgraph/v2/types/metadata/value_type_meta_data.h>

#include <concepts>
#include <cstddef>
#include <functional>
#include <new>
#include <sstream>
#include <stdexcept>
#include <string>
#include <type_traits>

namespace hgraph::v2
{
    /**
     * Behavior-only erased operations over a live value payload.
     *
     * `ValueOps` does not describe schema identity or storage layout. It only
     * knows how to operate on an already-constructed object at a raw memory
     * address.
     */
    struct ValueOps
    {
        using hash_fn      = size_t (*)(const void *);
        using equals_fn    = bool (*)(const void *, const void *);
        using to_string_fn = std::string (*)(const void *);

        hash_fn      hash{nullptr};
        equals_fn    equals{nullptr};
        to_string_fn to_string{nullptr};

        [[nodiscard]] constexpr bool can_hash() const noexcept { return hash != nullptr; }
        [[nodiscard]] constexpr bool can_equal() const noexcept { return equals != nullptr; }
        [[nodiscard]] constexpr bool can_to_string() const noexcept { return to_string != nullptr; }

        [[nodiscard]] size_t hash_of(const void *data) const {
            if (hash == nullptr) { throw std::logic_error("ValueOps is missing a hash hook"); }
            return hash(data);
        }

        [[nodiscard]] bool equals_of(const void *lhs, const void *rhs) const {
            if (equals == nullptr) { throw std::logic_error("ValueOps is missing an equality hook"); }
            return equals(lhs, rhs);
        }

        [[nodiscard]] std::string to_string_of(const void *data) const {
            if (to_string == nullptr) { throw std::logic_error("ValueOps is missing a string conversion hook"); }
            return to_string(data);
        }
    };

    namespace detail
    {
        template <typename T>
        concept HasAdlToString = requires(const T &value) {
            { to_string(value) } -> std::convertible_to<std::string>;
        };

        template <typename T>
        concept HasStdToString = requires(const T &value) {
            { std::to_string(value) } -> std::convertible_to<std::string>;
        };

        template <typename T>
        concept StreamInsertable = requires(std::ostream &stream, const T &value) {
            { stream << value } -> std::same_as<std::ostream &>;
        };

        template <typename T>
        concept ValueHashable = requires(const T &value) {
            { std::hash<T>{}(value) } -> std::convertible_to<size_t>;
        };

        template <typename T>
        concept ValueEquatable = requires(const T &lhs, const T &rhs) {
            { lhs == rhs } -> std::convertible_to<bool>;
        };

        template <typename T> [[nodiscard]] T *typed_value(void *data) noexcept {
            return std::launder(reinterpret_cast<T *>(data));
        }

        template <typename T> [[nodiscard]] const T *typed_value(const void *data) noexcept {
            return std::launder(reinterpret_cast<const T *>(data));
        }

        template <typename T> [[nodiscard]] std::string value_to_string(const T &value) {
            if constexpr (HasAdlToString<T>) {
                return to_string(value);
            } else if constexpr (HasStdToString<T>) {
                return std::to_string(value);
            } else if constexpr (StreamInsertable<T>) {
                std::ostringstream stream;
                stream << value;
                return stream.str();
            } else {
                throw std::logic_error("Value type does not support string conversion");
            }
        }

        template <typename T> [[nodiscard]] size_t scalar_hash(const void *data) { return std::hash<T>{}(*typed_value<T>(data)); }

        template <typename T> [[nodiscard]] bool scalar_equals(const void *lhs, const void *rhs) {
            return *typed_value<T>(lhs) == *typed_value<T>(rhs);
        }

        template <typename T> [[nodiscard]] std::string scalar_to_string(const void *data) {
            return value_to_string(*typed_value<T>(data));
        }

    }  // namespace detail

    template <typename T> [[nodiscard]] const ValueOps &scalar_value_ops() noexcept {
        using Type = std::remove_cv_t<std::remove_reference_t<T>>;
        static const ValueOps ops{
            .hash      = detail::ValueHashable<Type> ? &detail::scalar_hash<Type> : nullptr,
            .equals    = detail::ValueEquatable<Type> ? &detail::scalar_equals<Type> : nullptr,
            .to_string = (detail::HasAdlToString<Type> || detail::HasStdToString<Type> || detail::StreamInsertable<Type>)
                             ? &detail::scalar_to_string<Type>
                             : nullptr,
        };
        return ops;
    }
}  // namespace hgraph::v2

#endif  // HGRAPH_CPP_ROOT_VALUE_OPS_H
