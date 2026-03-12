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
    struct TupleView;
    struct BundleView;
    struct ListView;
    struct SetView;
    struct MapView;
    struct CyclicBufferView;
    struct QueueView;

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
                              "Value types require either to_string(T), std::to_string(T), or stream insertion");
                std::ostringstream stream;
                stream << value;
                return stream.str();
            }
        }

        /**
         * Behavior-only dispatch over raw value storage.
         *
         * The dispatch does not own data and does not describe schema identity.
         * It only knows how to operate on a block of memory that is already known
         * to match the supplied schema.
         */
        struct ViewDispatch
        {
            virtual ~ViewDispatch() = default;

            [[nodiscard]] virtual size_t hash(const void *data) const = 0;
            [[nodiscard]] virtual std::string to_string(const void *data) const = 0;
            [[nodiscard]] virtual std::partial_ordering compare(const void *lhs, const void *rhs) const = 0;
            [[nodiscard]] virtual nb::object to_python(const void *data, const value::TypeMeta *schema) const = 0;
            virtual void from_python(void *dst, const nb::object &src, const value::TypeMeta *schema) const = 0;
            virtual void assign(void *dst, const void *src) const = 0;
            virtual void set_from_cpp(void *dst, const void *src, const value::TypeMeta *src_schema) const = 0;
            virtual void move_from_cpp(void *dst, void *src, const value::TypeMeta *src_schema) const = 0;
        };

    }  // namespace detail

    /**
     * Non-owning erased value view.
     *
     * The view is the readable, schema-aware surface for value behavior. It
     * carries:
     * - the schema that describes the value position
     * - the resolved behavior dispatcher for that schema
     * - a pointer to the raw storage for the represented value
     *
     * The data itself is plain storage. All behavior is delegated through the
     * dispatch object.
     */
    struct View
    {
        /**
         * Bind this view to raw storage and the schema-resolved behavior
         * dispatcher for that storage.
         */
        View(const detail::ViewDispatch *dispatch, void *data, const value::TypeMeta *schema) noexcept
            : m_dispatch(dispatch), m_data(data), m_schema(schema) {}

        /**
         * Construct an invalid view for a known schema.
         *
         * This is used for invalid child positions in collections where the slot
         * schema is still known even though the slot currently has no live
         * storage.
         */
        [[nodiscard]] static View invalid_for(const value::TypeMeta *schema) noexcept
        {
            return View{nullptr, nullptr, schema};
        }

        /**
         * Return `true` when this view currently refers to live storage.
         */
        [[nodiscard]] bool valid() const noexcept { return m_data != nullptr; }

        explicit operator bool() const noexcept { return valid(); }

        /**
         * Return the schema represented by this view.
         */
        [[nodiscard]] const value::TypeMeta *schema() const noexcept { return m_schema; }

        /**
         * Return `true` when this view's schema exactly matches the supplied
         * schema pointer.
         */
        [[nodiscard]] bool is_type(const value::TypeMeta *other) const noexcept { return m_schema == other; }

        [[nodiscard]] AtomicView as_atomic();
        [[nodiscard]] AtomicView as_atomic() const;
        [[nodiscard]] TupleView as_tuple();
        [[nodiscard]] TupleView as_tuple() const;
        [[nodiscard]] BundleView as_bundle();
        [[nodiscard]] BundleView as_bundle() const;
        [[nodiscard]] ListView   as_list();
        [[nodiscard]] ListView   as_list() const;
        [[nodiscard]] SetView    as_set();
        [[nodiscard]] SetView    as_set() const;
        [[nodiscard]] MapView    as_map();
        [[nodiscard]] MapView    as_map() const;
        [[nodiscard]] CyclicBufferView as_cyclic_buffer();
        [[nodiscard]] CyclicBufferView as_cyclic_buffer() const;
        [[nodiscard]] QueueView  as_queue();
        [[nodiscard]] QueueView  as_queue() const;

        /**
         * Return the hash of the represented value.
         */
        [[nodiscard]] size_t hash() const {
            if (!valid()) { throw std::runtime_error("View::hash() on invalid view"); }
            return m_dispatch->hash(m_data);
        }

        /**
         * Compare two views for equality.
         *
         * Equality is schema-sensitive. Two invalid views are equal only when
         * they represent the same schema.
         */
        [[nodiscard]] bool eq(const View &other) const {
            if (schema() != other.schema()) { return false; }
            if (!valid() || !other.valid()) { return !valid() && !other.valid(); }
            return std::is_eq(m_dispatch->compare(m_data, other.m_data));
        }

        /**
         * Return a string representation of the represented value.
         */
        [[nodiscard]] std::string to_string() const {
            if (!valid()) { throw std::runtime_error("View::to_string() on invalid view"); }
            return m_dispatch->to_string(m_data);
        }

        [[nodiscard]] nb::object to_python() const {
            if (!valid()) { throw std::runtime_error("View::to_python() on invalid view"); }
            return m_dispatch->to_python(m_data, schema());
        }

        void from_python(const nb::object &src) {
            if (!valid()) { throw std::runtime_error("View::from_python() on invalid view"); }
            m_dispatch->from_python(m_data, src, schema());
        }

        template <typename T> void set(T &&value);

        [[nodiscard]] bool operator==(const View &other) const { return eq(other); }

        [[nodiscard]] std::partial_ordering operator<=>(const View &other) const {
            if (schema() != other.schema()) { return std::partial_ordering::unordered; }
            if (!valid() || !other.valid()) {
                return !valid() && !other.valid() ? std::partial_ordering::equivalent : std::partial_ordering::unordered;
            }
            return m_dispatch->compare(m_data, other.m_data);
        }

      protected:
        /**
         * Return the schema-resolved behavior dispatcher.
         */
        [[nodiscard]] const detail::ViewDispatch *dispatch() const noexcept { return m_dispatch; }

        /**
         * Return the raw storage pointer represented by this view.
         *
         * This remains a protected helper so typed derived views can operate on
         * raw storage without exposing erased pointer access on the public API.
         */
        [[nodiscard]] void *data() const noexcept { return m_data; }

        /**
         * Return the raw storage pointer from another view.
         *
         * Derived view types use this when they need to combine their own typed
         * behavior with another erased `View`, for example during assignment from
         * another view of the same schema.
         */
        [[nodiscard]] static void *data_of(const View &view) noexcept { return view.m_data; }

      private:
        const detail::ViewDispatch *m_dispatch{nullptr};
        void                       *m_data{nullptr};
        const value::TypeMeta      *m_schema{nullptr};
    };

}  // namespace hgraph
