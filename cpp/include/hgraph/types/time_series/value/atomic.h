#pragma once

#include <hgraph/hgraph_base.h>
#include <hgraph/python/chrono.h>
#include <hgraph/types/time_series/value/view.h>
#include <hgraph/types/value/type_registry.h>

#include <cstddef>
#include <memory>
#include <stdexcept>
#include <string>
#include <type_traits>
#include <utility>

namespace hgraph
{

    struct ValueBuilder;

    template <typename T> struct AtomicState
    {
        /**
         * Raw atomic payload.
         *
         * Atomic data is intentionally plain storage. Behavior lives on the
         * schema-resolved dispatch object rather than inside each stored value.
         */
        T value{};
    };

    namespace detail
    {

        template <typename T>
        concept InlineValueEligible =
            sizeof(AtomicState<T>) <= sizeof(void *) && alignof(AtomicState<T>) <= alignof(void *) &&
            std::is_trivially_copyable_v<AtomicState<T>> && std::is_trivially_destructible_v<AtomicState<T>>;

        template <typename T>
        [[nodiscard]] nb::object atomic_to_python(const T &value)
        {
            return nb::cast(value);
        }

        template <typename T>
        void atomic_from_python(T &dst, const nb::object &src)
        {
            dst = nb::cast<T>(src);
        }

        template <typename T> struct AtomicDispatch final : ViewDispatch
        {
            static_assert(Hashable<T>, "AtomicState<T> requires std::hash<T>");
            static_assert(EqualityComparable<T>, "AtomicState<T> requires operator==");
            static_assert(PartiallyOrdered<T>,
                          "AtomicState<T> requires operator<=> returning an ordering convertible to std::partial_ordering");

            [[nodiscard]] size_t hash(const void *data) const override
            {
                return std::hash<T>{}(state(data)->value);
            }

            [[nodiscard]] std::string to_string(const void *data) const override
            {
                return value_to_string(state(data)->value);
            }

            [[nodiscard]] std::partial_ordering compare(const void *lhs, const void *rhs) const override
            {
                return state(lhs)->value <=> state(rhs)->value;
            }

            [[nodiscard]] nb::object to_python(const void *data, const value::TypeMeta *schema) const override
            {
                static_cast<void>(schema);
                return atomic_to_python(state(data)->value);
            }

            void from_python(void *dst, const nb::object &src, const value::TypeMeta *schema) const override
            {
                static_cast<void>(schema);
                atomic_from_python(state(dst)->value, src);
            }

            void assign(void *dst, const void *src) const override
            {
                state(dst)->value = state(src)->value;
            }

            void set_from_cpp(void *dst, const void *src, const value::TypeMeta *src_schema) const override
            {
                if (src_schema != value::scalar_type_meta<T>()) {
                    throw std::invalid_argument("AtomicDispatch::set_from_cpp requires matching source schema");
                }
                state(dst)->value = *static_cast<const T *>(src);
            }

            void move_from_cpp(void *dst, void *src, const value::TypeMeta *src_schema) const override
            {
                if (src_schema != value::scalar_type_meta<T>()) {
                    throw std::invalid_argument("AtomicDispatch::move_from_cpp requires matching source schema");
                }
                state(dst)->value = std::move(*static_cast<T *>(src));
            }

          private:
            [[nodiscard]] static AtomicState<T> *state(void *data) noexcept
            {
                return std::launder(reinterpret_cast<AtomicState<T> *>(data));
            }

            [[nodiscard]] static const AtomicState<T> *state(const void *data) noexcept
            {
                return std::launder(reinterpret_cast<const AtomicState<T> *>(data));
            }
        };

        template <typename T>
        [[nodiscard]] inline const ViewDispatch &atomic_view_dispatch() noexcept
        {
            static const AtomicDispatch<T> dispatch{};
            return dispatch;
        }

        /**
         * Return the cached builder for an atomic schema.
         *
         * Atomic storage layout and supported schema matching are owned by the
         * atomic implementation, so the generic builder factory delegates atomic
         * schema resolution to `atomic.cpp`.
         */
        [[nodiscard]] HGRAPH_EXPORT const ValueBuilder *atomic_builder_for(const value::TypeMeta *schema);

    }  // namespace detail

    /**
     * Non-owning typed wrapper over an atomic value position.
     *
     * The view performs schema validation once, then the typed accessors operate
     * directly on the plain atomic storage.
     */
    struct AtomicView : View
    {
        explicit AtomicView(const View &view)
            : View(view)
        {
            if (!view.valid()) { return; }
            if (view.schema() == nullptr || view.schema()->kind != value::TypeKind::Atomic) {
                throw std::runtime_error("AtomicView requires an atomic schema");
            }
        }

        template <typename T> [[nodiscard]] T *try_as() noexcept
        {
            return schema() == value::scalar_type_meta<T>() ? std::addressof(state<T>()->value) : nullptr;
        }

        template <typename T> [[nodiscard]] const T *try_as() const noexcept
        {
            return schema() == value::scalar_type_meta<T>() ? std::addressof(state<T>()->value) : nullptr;
        }

        template <typename T> [[nodiscard]] T &checked_as()
        {
            if (!valid()) { throw std::runtime_error("AtomicView::checked_as<T>() on invalid view"); }
            if (T *ptr = try_as<T>(); ptr != nullptr) { return *ptr; }
            throw std::runtime_error("AtomicView::checked_as<T>() type mismatch");
        }

        template <typename T> [[nodiscard]] const T &checked_as() const
        {
            if (!valid()) { throw std::runtime_error("AtomicView::checked_as<T>() on invalid view"); }
            if (const T *ptr = try_as<T>(); ptr != nullptr) { return *ptr; }
            throw std::runtime_error("AtomicView::checked_as<T>() type mismatch");
        }

        template <typename T> [[nodiscard]] T &as() { return checked_as<T>(); }
        template <typename T> [[nodiscard]] const T &as() const { return checked_as<T>(); }

        AtomicView &operator=(const View &other)
        {
            if (!valid()) { throw std::runtime_error("AtomicView::operator= on invalid view"); }
            if (!other.valid()) { throw std::runtime_error("AtomicView::operator= from invalid view"); }
            if (schema() != other.schema()) {
                throw std::runtime_error("AtomicView::operator= requires matching schema");
            }
            dispatch()->assign(data(), data_of(other));
            return *this;
        }

        AtomicView &operator=(const AtomicView &other) { return *this = static_cast<const View &>(other); }

        template <typename T>
            requires(!std::derived_from<std::remove_cvref_t<T>, View>)
        AtomicView &operator=(T &&value)
        {
            set(std::forward<T>(value));
            return *this;
        }

        template <typename T> void set(const T &value) { checked_as<T>() = value; }
        template <typename T> void set(T &&value) { checked_as<std::remove_cvref_t<T>>() = std::forward<T>(value); }

      private:
        template <typename T> [[nodiscard]] AtomicState<T> *state() noexcept
        {
            return std::launder(reinterpret_cast<AtomicState<T> *>(data()));
        }

        template <typename T> [[nodiscard]] const AtomicState<T> *state() const noexcept
        {
            return std::launder(reinterpret_cast<const AtomicState<T> *>(data()));
        }
    };

    inline AtomicView View::as_atomic()
    {
        return AtomicView{*this};
    }

    inline AtomicView View::as_atomic() const
    {
        return AtomicView{*this};
    }

}  // namespace hgraph
