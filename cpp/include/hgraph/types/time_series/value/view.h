#pragma once

#include <hgraph/hgraph_base.h>
#include <hgraph/types/value/type_meta.h>

#include <compare>
#include <concepts>
#include <functional>
#include <sstream>
#include <stdexcept>
#include <string>
#include <type_traits>
#include <utility>

namespace hgraph
{

    struct AtomicView;
    struct ListStateBase;
    struct ListView;

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
        concept Hashable = requires(const T &value) {
            { std::hash<T>{}(value) } -> std::convertible_to<size_t>;
        };

        template <typename T>
        concept EqualityComparable = requires(const T &lhs, const T &rhs) {
            { lhs == rhs } -> std::convertible_to<bool>;
        };

        template <typename T>
        concept PartiallyOrdered = requires(const T &lhs, const T &rhs) {
            { lhs <=> rhs } -> std::convertible_to<std::partial_ordering>;
        };

        template <typename T> [[nodiscard]] std::string value_to_string(const T &value) {
            if constexpr (HasAdlToString<T>) {
                return to_string(value);
            } else if constexpr (HasStdToString<T>) {
                return std::to_string(value);
            } else {
                static_assert(StreamInsertable<T>,
                              "View<T> requires either to_string(T), std::to_string(T), or stream insertion support");
                std::ostringstream stream;
                stream << value;
                return stream.str();
            }
        }

        /**
         * Pure abstract dispatch surface for erased value views.
         *
         * Concrete state types implement this interface once the schema and concrete
         * type are known. The raw `View` wrapper retains schema identity and performs
         * schema compatibility checks before delegating to these methods.
         *
         * Schema is intentionally not part of the dispatch surface. Keeping schema as
         * a view-level concern ensures the erased dispatch only models operations on
         * already-resolved state objects.
         */
        struct ViewDispatch
        {
            virtual ~ViewDispatch() = default;

            [[nodiscard]] virtual size_t hash() const = 0;
            /**
             * Equality is derived from the ordering result so there is a single source
             * of truth for comparison semantics.
             */
            [[nodiscard]] virtual bool                  eq(const ViewDispatch &other) const { return std::is_eq(*this <=> other); }
            [[nodiscard]] virtual std::string           to_string() const                            = 0;
            [[nodiscard]] virtual std::partial_ordering operator<=>(const ViewDispatch &other) const = 0;
            [[nodiscard]] virtual nb::object            to_python(const value::TypeMeta *schema) const = 0;
            virtual void                                from_python(const nb::object &src, const value::TypeMeta *schema) = 0;
            /**
             * Replace this stored value from another resolved value with the same
             * schema-selected dispatch implementation.
             */
            virtual void assign_from(const ViewDispatch &other) = 0;
            /**
             * Replace this stored value from a copied C++ object identified by
             * the supplied source schema.
             */
            virtual void set_from_cpp(const void *src, const value::TypeMeta *src_schema) = 0;
            /**
             * Replace this stored value from a moved C++ object identified by
             * the supplied source schema.
             */
            virtual void move_from_cpp(void *src, const value::TypeMeta *src_schema) = 0;
        };

    }  // namespace detail

    /**
     * Type-erased wrapper over a stored time-series value payload.
     *
     * `View` holds a non-owning pointer to a dispatch implementation supplied by a
     * concrete state object. The wrapper is responsible for generic schema checks
     * before delegating to the underlying dispatch surface.
     *
     * Mutation control is intentionally handled only through the constness of the
     * `View` instance itself. There is a single dispatch implementation pointer;
     * non-const `View` methods use that dispatch non-const, while const `View`
     * methods use it through const-qualified access.
     *
     * Comparison semantics were chosen to keep invalid views well-defined:
     * - schema is checked first, even for invalid views
     * - two invalid views compare equal only when they describe the same schema
     * - the same-schema invalid case compares as equivalent rather than unordered
     *
     * That preserves type identity even when the payload is invalid.
     */
    struct View
    {
        friend struct ListStateBase;
        friend struct ListView;

        /**
         * Construct an invalid view.
         *
         * Invalid views carry no dispatch target and are used as the empty state
         * for optional or unsupported erased value positions.
         */
        View() noexcept = default;
        /**
         * Bind this view to a concrete dispatch implementation and schema.
         *
         * The schema is stored on the wrapper rather than the dispatch so erased
         * compatibility checks can be performed before any operation is delegated.
         */
        View(detail::ViewDispatch *dispatch, const value::TypeMeta *schema) noexcept : m_dispatch(dispatch), m_schema(schema) {}

        /**
         * Return `true` when this view currently refers to a concrete stored value.
         */
        [[nodiscard]] bool valid() const noexcept { return m_dispatch != nullptr; }

        /**
         * Return `true` when this view currently refers to a concrete stored value.
         */
        explicit operator bool() const noexcept { return valid(); }

        /**
         * Return the schema that describes the value position represented by this
         * view, even when the current payload is invalid.
         */
        [[nodiscard]] const value::TypeMeta *schema() const noexcept { return m_schema; }

        /**
         * Return `true` when this view's schema matches the supplied schema.
         */
        [[nodiscard]] bool is_type(const value::TypeMeta *other) const noexcept { return schema() == other; }

        /**
         * Interpret this view as an atomic view.
         *
         * This conversion requires the schema kind to be atomic. Type
         * mismatches are enforced at runtime so a successfully created
         * `AtomicView` can rely on its cast safety thereafter.
         */
        [[nodiscard]] AtomicView as_atomic();

        /**
         * Interpret this view as an atomic view.
         *
         * This conversion requires the schema kind to be atomic. Type
         * mismatches are enforced at runtime so a successfully created
         * `AtomicView` can rely on its cast safety thereafter.
         */
        [[nodiscard]] AtomicView as_atomic() const;

        /**
         * Interpret this view as a list view.
         *
         * This conversion requires the schema kind to be list. Type
         * mismatches are enforced at runtime so a successfully created
         * `ListView` can rely on its cast safety thereafter.
         */
        [[nodiscard]] ListView as_list();

        /**
         * Interpret this view as a list view.
         *
         * This conversion requires the schema kind to be list. Type
         * mismatches are enforced at runtime so a successfully created
         * `ListView` can rely on its cast safety thereafter.
         */
        [[nodiscard]] ListView as_list() const;

        /**
         * Return the hash of the currently represented value.
         *
         * Hashing is only defined for valid views.
         */
        [[nodiscard]] size_t hash() const {
            if (!valid()) { throw std::runtime_error("View::hash() on invalid view"); }
            return m_dispatch->hash();
        }

        /**
         * Compare this view for equality with another erased view.
         *
         * Equality is schema-sensitive. Two invalid views are equal only when they
         * describe the same schema.
         */
        [[nodiscard]] bool eq(const View &other) const {
            if (schema() != other.schema()) { return false; }
            if (!valid() || !other.valid()) { return !valid() && !other.valid(); }
            return m_dispatch->eq(*other.m_dispatch);
        }

        /**
         * Return a string representation of the currently represented value.
         *
         * String conversion is only defined for valid views.
         */
        [[nodiscard]] std::string to_string() const {
            if (!valid()) { throw std::runtime_error("View::to_string() on invalid view"); }
            return m_dispatch->to_string();
        }

        /**
         * Convert the represented value to its Python representation using the
         * schema-bound conversion ops for this view.
         */
        [[nodiscard]] nb::object to_python() const {
            if (!valid()) { throw std::runtime_error("View::to_python() on invalid view"); }
            return m_dispatch->to_python(schema());
        }

        /**
         * Replace the represented value from a Python object using the
         * schema-bound conversion ops for this view.
         */
        void from_python(const nb::object &src) {
            if (!valid()) { throw std::runtime_error("View::from_python() on invalid view"); }
            m_dispatch->from_python(src, schema());
        }

        /**
         * Replace the represented value from a C++ object using the schema-bound
         * value-layer construction rules for this view.
         *
         * The generic entry point currently supports atomic schemas and is
         * intended to grow to collection schemas as their native set/build
         * surfaces are added.
         */
        template <typename T> void set(T &&value);

        /**
         * Compare this view with another erased view using partial ordering.
         *
         * Ordering is only defined when both views describe the same schema. Two
         * invalid same-schema views compare as equivalent.
         */
        [[nodiscard]] bool operator==(const View &other) const { return eq(other); }

        [[nodiscard]] std::partial_ordering operator<=>(const View &other) const {
            if (schema() != other.schema()) { return std::partial_ordering::unordered; }
            if (!valid() || !other.valid()) {
                return !valid() && !other.valid() ? std::partial_ordering::equivalent : std::partial_ordering::unordered;
            }
            return *m_dispatch <=> *other.m_dispatch;
        }

      protected:
        /**
         * Return the underlying dispatch implementation for non-const operations.
         */
        [[nodiscard]] detail::ViewDispatch *dispatch() noexcept { return m_dispatch; }

        /**
         * Return the underlying dispatch implementation for const operations.
         */
        [[nodiscard]] const detail::ViewDispatch *dispatch() const noexcept { return m_dispatch; }

      private:
        detail::ViewDispatch  *m_dispatch{nullptr};
        const value::TypeMeta *m_schema{nullptr};
    };

}  // namespace hgraph
