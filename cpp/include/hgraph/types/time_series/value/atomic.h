#pragma once

#include <hgraph/hgraph_base.h>
#include <hgraph/python/chrono.h>
#include <hgraph/types/time_series/value/tracking.h>
#include <hgraph/types/time_series/value/view.h>
#include <hgraph/types/value/type_registry.h>
#include <hgraph/util/string_utils.h>

#include <cstddef>
#include <memory>
#include <stdexcept>
#include <string>
#include <type_traits>
#include <utility>

namespace hgraph
{
    template <typename T>
    [[nodiscard]] T atomic_default_value(std::type_identity<T>)
    {
        return T{};
    }

    template <typename T>
    [[nodiscard]] size_t atomic_hash(const T &value)
    {
        return std::hash<T>{}(value);
    }

    template <typename T>
    [[nodiscard]] std::partial_ordering atomic_compare(const T &lhs, const T &rhs)
    {
        if constexpr (requires { lhs <=> rhs; }) {
            return lhs <=> rhs;
        } else if constexpr (requires { lhs == rhs; lhs < rhs; }) {
            if (lhs == rhs) { return std::partial_ordering::equivalent; }
            return lhs < rhs ? std::partial_ordering::less : std::partial_ordering::greater;
        } else if constexpr (requires { lhs == rhs; }) {
            return lhs == rhs ? std::partial_ordering::equivalent : std::partial_ordering::unordered;
        } else {
            static_assert(requires { lhs == rhs; },
                          "Atomic values require either operator== or an atomic_compare overload");
        }
    }

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

        template <typename T> struct AtomicDispatch final : ViewDispatch
        {
            [[nodiscard]] size_t hash(const void *data) const override
            {
                return atomic_hash(state(data)->value);
            }

            [[nodiscard]] std::string to_string(const void *data) const override
            {
                return value_to_string(state(data)->value);
            }

            [[nodiscard]] std::partial_ordering compare(const void *lhs, const void *rhs) const override
            {
                return atomic_compare(state(lhs)->value, state(rhs)->value);
            }

            [[nodiscard]] nb::object to_python(const void *data, const value::TypeMeta *schema) const override
            {
                static_cast<void>(schema);
                return ::hgraph::atomic_to_python(state(data)->value);
            }

            void from_python(void *dst, const nb::object &src, const value::TypeMeta *schema) const override
            {
                static_cast<void>(schema);
                ::hgraph::atomic_from_python(state(dst)->value, src);
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

        /**
         * Python-object atomic dispatch.
         *
         * Unknown Python scalar types are represented as `nb::object` in the
         * value schema layer. They remain atomic from a storage perspective,
         * but their behavior must defer to Python rather than relying on C++
         * ordering and hashing concepts.
         */
        template <> struct AtomicDispatch<nb::object> final : ViewDispatch
        {
            [[nodiscard]] size_t hash(const void *data) const override
            {
                const auto &obj = state(data)->value;
                if (!obj.is_valid()) { return 0; }
                try {
                    return nb::hash(obj);
                } catch (...) {
                    return 0;
                }
            }

            [[nodiscard]] std::string to_string(const void *data) const override
            {
                const auto &obj = state(data)->value;
                if (!obj.is_valid()) { return "None"; }
                try {
                    return std::string(nb::str(nb::repr(obj)).c_str());
                } catch (...) {
                    return "<python-object>";
                }
            }

            [[nodiscard]] std::partial_ordering compare(const void *lhs, const void *rhs) const override
            {
                const auto &lhs_obj = state(lhs)->value;
                const auto &rhs_obj = state(rhs)->value;
                if (!lhs_obj.is_valid() && !rhs_obj.is_valid()) { return std::partial_ordering::equivalent; }
                if (!lhs_obj.is_valid() || !rhs_obj.is_valid()) { return std::partial_ordering::unordered; }
                try {
                    return lhs_obj.equal(rhs_obj) ? std::partial_ordering::equivalent : std::partial_ordering::unordered;
                } catch (...) {
                    return std::partial_ordering::unordered;
                }
            }

            [[nodiscard]] nb::object to_python(const void *data, const value::TypeMeta *schema) const override
            {
                static_cast<void>(schema);
                return state(data)->value;
            }

            void from_python(void *dst, const nb::object &src, const value::TypeMeta *schema) const override
            {
                static_cast<void>(schema);
                state(dst)->value = src;
            }

            void assign(void *dst, const void *src) const override
            {
                state(dst)->value = state(src)->value;
            }

            void set_from_cpp(void *dst, const void *src, const value::TypeMeta *src_schema) const override
            {
                if (src_schema != value::scalar_type_meta<nb::object>()) {
                    throw std::invalid_argument("AtomicDispatch<nb::object>::set_from_cpp requires matching source schema");
                }
                state(dst)->value = *static_cast<const nb::object *>(src);
            }

            void move_from_cpp(void *dst, void *src, const value::TypeMeta *src_schema) const override
            {
                if (src_schema != value::scalar_type_meta<nb::object>()) {
                    throw std::invalid_argument("AtomicDispatch<nb::object>::move_from_cpp requires matching source schema");
                }
                state(dst)->value = std::move(*static_cast<nb::object *>(src));
            }

          private:
            [[nodiscard]] static AtomicState<nb::object> *state(void *data) noexcept
            {
                return std::launder(reinterpret_cast<AtomicState<nb::object> *>(data));
            }

            [[nodiscard]] static const AtomicState<nb::object> *state(const void *data) noexcept
            {
                return std::launder(reinterpret_cast<const AtomicState<nb::object> *>(data));
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
        [[nodiscard]] HGRAPH_EXPORT const ValueBuilder *atomic_builder_for(
            const value::TypeMeta *schema, MutationTracking tracking);

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
            if (!view.has_value()) { return; }
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
            if (!has_value()) { throw std::runtime_error("AtomicView::checked_as<T>() on invalid view"); }
            if (T *ptr = try_as<T>(); ptr != nullptr) { return *ptr; }
            throw std::runtime_error("AtomicView::checked_as<T>() type mismatch");
        }

        template <typename T> [[nodiscard]] const T &checked_as() const
        {
            if (!has_value()) { throw std::runtime_error("AtomicView::checked_as<T>() on invalid view"); }
            if (const T *ptr = try_as<T>(); ptr != nullptr) { return *ptr; }
            throw std::runtime_error("AtomicView::checked_as<T>() type mismatch");
        }

        template <typename T> [[nodiscard]] T &as() { return checked_as<T>(); }
        template <typename T> [[nodiscard]] const T &as() const { return checked_as<T>(); }

        AtomicView &operator=(const View &other)
        {
            if (!has_value()) { throw std::runtime_error("AtomicView::operator= on invalid view"); }
            if (!other.has_value()) { throw std::runtime_error("AtomicView::operator= from invalid view"); }
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

    template <typename T> inline T *View::try_as() noexcept
    {
        return schema() != nullptr && schema()->kind == value::TypeKind::Atomic ? as_atomic().template try_as<T>() : nullptr;
    }

    template <typename T> inline const T *View::try_as() const noexcept
    {
        return schema() != nullptr && schema()->kind == value::TypeKind::Atomic ? as_atomic().template try_as<T>() : nullptr;
    }

    template <typename T> inline T &View::checked_as()
    {
        return as_atomic().template checked_as<T>();
    }

    template <typename T> inline const T &View::checked_as() const
    {
        return as_atomic().template checked_as<T>();
    }

    template <typename T> inline T &View::as()
    {
        return checked_as<T>();
    }

    template <typename T> inline const T &View::as() const
    {
        return checked_as<T>();
    }

    template <typename T> inline bool View::is_scalar_type() const noexcept
    {
        return schema() == value::scalar_type_meta<T>();
    }

}  // namespace hgraph
