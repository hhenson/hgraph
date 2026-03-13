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
    struct Value;


    /**
     * Lightweight type-erased range over value-layer results.
     *
     * The range is storage-backed rather than materialized. It iterates over a
     * bounded index space and yields only the entries accepted by the supplied
     * predicate. This keeps delta-style APIs lazy and allocation-free while
     * still returning ordinary value-layer view objects.
     */
template <typename T>
struct Range
    {
        using Predicate = bool (*)(const void *context, size_t index);
        using Projector = T (*)(const void *context, size_t index);

        struct iterator
        {
            [[nodiscard]] T operator*() const { return range->project(context, index); }

            iterator &operator++()
            {
                ++index;
                advance_to_match();
                return *this;
            }

            [[nodiscard]] bool operator!=(const iterator &other) const
            {
                return context != other.context || index != other.index || range != other.range;
            }

          private:
            friend struct Range<T>;

            void advance_to_match()
            {
                while (index < range->m_limit && !range->includes(context, index)) {
                    ++index;
                }
            }

            const Range<T> *range{nullptr};
            const void     *context{nullptr};
            size_t          index{0};
        };

        [[nodiscard]] iterator begin() const
        {
            iterator it;
            it.range = this;
            it.context = m_context;
            it.index = 0;
            it.advance_to_match();
            return it;
        }

        [[nodiscard]] iterator end() const
        {
            iterator it;
            it.range = this;
            it.context = m_context;
            it.index = m_limit;
            return it;
        }

      private:
        friend struct iterator;

        [[nodiscard]] bool includes(const void *context, size_t index) const
        {
            return m_predicate == nullptr ? true : m_predicate(context, index);
        }

        [[nodiscard]] T project(const void *context, size_t index) const
        {
            return m_projector(context, index);
        }

        const void *m_context{nullptr};
        size_t      m_limit{0};
        Predicate   m_predicate{nullptr};
        Projector   m_projector{nullptr};

      public:
        Range(const void *context, size_t limit, Predicate predicate, Projector projector) noexcept
            : m_context(context), m_limit(limit), m_predicate(predicate), m_projector(projector)
        {
        }

        Range() noexcept = default;
    };

    /**
     * Lightweight type-erased range over storage-backed key/value results.
     *
     * This mirrors `Range<T>` but projects a logical key together with a
     * logical value. The range remains lazy and allocation-free.
     */
    template <typename K, typename V>
    struct KeyValueRange
    {
        using Value = std::pair<K, V>;
        using Predicate = bool (*)(const void *context, size_t index);
        using Projector = Value (*)(const void *context, size_t index);

        struct iterator
        {
            [[nodiscard]] Value operator*() const { return range->project(context, index); }

            iterator &operator++()
            {
                ++index;
                advance_to_match();
                return *this;
            }

            [[nodiscard]] bool operator!=(const iterator &other) const
            {
                return context != other.context || index != other.index || range != other.range;
            }

          private:
            friend struct KeyValueRange<K, V>;

            void advance_to_match()
            {
                while (index < range->m_limit && !range->includes(context, index)) {
                    ++index;
                }
            }

            const KeyValueRange<K, V> *range{nullptr};
            const void                *context{nullptr};
            size_t                     index{0};
        };

        [[nodiscard]] iterator begin() const
        {
            iterator it;
            it.range = this;
            it.context = m_context;
            it.index = 0;
            it.advance_to_match();
            return it;
        }

        [[nodiscard]] iterator end() const
        {
            iterator it;
            it.range = this;
            it.context = m_context;
            it.index = m_limit;
            return it;
        }

      private:
        friend struct iterator;

        [[nodiscard]] bool includes(const void *context, size_t index) const
        {
            return m_predicate == nullptr ? true : m_predicate(context, index);
        }

        [[nodiscard]] Value project(const void *context, size_t index) const
        {
            return m_projector(context, index);
        }

        const void *m_context{nullptr};
        size_t      m_limit{0};
        Predicate   m_predicate{nullptr};
        Projector   m_projector{nullptr};

      public:
        KeyValueRange(const void *context, size_t limit, Predicate predicate, Projector projector) noexcept
            : m_context(context), m_limit(limit), m_predicate(predicate), m_projector(projector)
        {
        }

        KeyValueRange() noexcept = default;
    };

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
         *
         * The value layer follows optional-style presence semantics. A view
         * either has a value or it does not; there is no separate notion of a
         * "valid" value in this API.
         */
        [[nodiscard]] bool has_value() const noexcept { return m_data != nullptr; }

        explicit operator bool() const noexcept { return has_value(); }

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
        [[nodiscard]] ListView as_list();
        [[nodiscard]] ListView as_list() const;
        [[nodiscard]] SetView as_set();
        [[nodiscard]] SetView as_set() const;
        [[nodiscard]] MapView as_map();
        [[nodiscard]] MapView as_map() const;
        [[nodiscard]] CyclicBufferView as_cyclic_buffer();
        [[nodiscard]] CyclicBufferView as_cyclic_buffer() const;
        [[nodiscard]] QueueView as_queue();
        [[nodiscard]] QueueView as_queue() const;

        /**
         * Return the hash of the represented value.
         */
        [[nodiscard]] size_t hash() const {
            if (!has_value()) { throw std::runtime_error("View::hash() on empty view"); }
            return m_dispatch->hash(m_data);
        }

        /**
         * Compare two views for equality.
         *
         * Equality is schema-sensitive. Two invalid views are equal only when
         * they represent the same schema.
         */
        [[nodiscard]] bool equals(const View &other) const {
            if (schema() != other.schema()) { return false; }
            if (!has_value() || !other.has_value()) { return !has_value() && !other.has_value(); }
            return std::is_eq(m_dispatch->compare(m_data, other.m_data));
        }

        /**
         * Return a string representation of the represented value.
         */
        [[nodiscard]] std::string to_string() const {
            if (!has_value()) { throw std::runtime_error("View::to_string() on empty view"); }
            return m_dispatch->to_string(m_data);
        }

        [[nodiscard]] nb::object to_python() const {
            if (!has_value()) { throw std::runtime_error("View::to_python() on empty view"); }
            return m_dispatch->to_python(m_data, schema());
        }

        void from_python(const nb::object &src) {
            if (!has_value()) { throw std::runtime_error("View::from_python() on empty view"); }
            m_dispatch->from_python(m_data, src, schema());
        }

        template <typename T> void set(T &&value);

        /**
         * Create an owning value by copying the storage currently represented
         * by this view.
         *
         * This preserves the represented schema and copies the current payload
         * when the view is valid.
         */
        [[nodiscard]] Value clone() const;

        /**
         * Copy the payload represented by another view into this view.
         *
         * Both views must be valid and must describe the same schema. This
         * copies payload state only; it does not rebind either view.
         */
        void copy_from(const View &other)
        {
            if (!has_value() || !other.has_value()) { throw std::runtime_error("View::copy_from requires non-empty views"); }
            if (schema() != other.schema()) {
                throw std::invalid_argument("View::copy_from requires matching schemas");
            }
            m_dispatch->assign(m_data, other.m_data);
        }

        template <typename T> [[nodiscard]] T *try_as() noexcept;
        template <typename T> [[nodiscard]] const T *try_as() const noexcept;
        template <typename T> [[nodiscard]] T &checked_as();
        template <typename T> [[nodiscard]] const T &checked_as() const;
        template <typename T> [[nodiscard]] T &as();
        template <typename T> [[nodiscard]] const T &as() const;
        template <typename T> [[nodiscard]] bool is_scalar_type() const noexcept;

        [[nodiscard]] bool operator==(const View &other) const { return equals(other); }

        [[nodiscard]] std::partial_ordering operator<=>(const View &other) const {
            if (schema() != other.schema()) { return std::partial_ordering::unordered; }
            if (!has_value() || !other.has_value()) {
                return !has_value() && !other.has_value() ? std::partial_ordering::equivalent
                                                          : std::partial_ordering::unordered;
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
