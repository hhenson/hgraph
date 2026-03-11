#pragma once

#include <hgraph/hgraph_base.h>
#include <hgraph/python/chrono.h>
#include <hgraph/types/time_series/value/view.h>
#include <hgraph/types/value/type_registry.h>

#include <cstddef>
#include <cstdint>
#include <memory>
#include <stdexcept>
#include <string>
#include <type_traits>
#include <utility>

namespace hgraph
{

    template <typename T> struct AtomicState;
    struct AtomicView;

    namespace detail
    {

        struct AtomicViewDispatch : ViewDispatch
        {
            [[nodiscard]] virtual void *data() noexcept = 0;
            [[nodiscard]] virtual const void *data() const noexcept = 0;
        };

        inline const void *cast_atomic_data(const void *data, const value::TypeMeta *actual_schema,
                                            const value::TypeMeta *expected_schema) noexcept {
            return data != nullptr && actual_schema == expected_schema ? data : nullptr;
        }

        template <typename T>
        [[nodiscard]] const T *cast_atomic_data_as(const void *data, const value::TypeMeta *actual_schema) noexcept {
            return static_cast<const T *>(cast_atomic_data(data, actual_schema, value::scalar_type_meta<T>()));
        }

        template <typename T>
        [[nodiscard]] nb::object atomic_to_python(const T &value) {
            return nb::cast(value);
        }

        template <typename T>
        void atomic_from_python(T &dst, const nb::object &src) {
            dst = nb::cast<T>(src);
        }

    }  // namespace detail

    /**
     * Typed atomic state.
     *
     * `AtomicState<T>` owns the resolved raw atomic storage and exposes typed
     * operations directly against the resolved `T`.
     *
     * This split is deliberate: once execution reaches the state, the schema
     * has already been resolved and the implementation can operate on the
     * concrete `T` without repeating erased type checks.
     */
    template <typename T> struct AtomicState : detail::AtomicViewDispatch
    {
        static_assert(detail::Hashable<T>, "AtomicState<T> requires std::hash<T>");
        static_assert(detail::EqualityComparable<T>, "AtomicState<T> requires operator==");
        static_assert(detail::PartiallyOrdered<T>,
                      "AtomicState<T> requires operator<=> returning an ordering convertible to std::partial_ordering");

        /**
         * Atomic values up to pointer size are stored inline.
         */
        static constexpr bool stores_inline = sizeof(T) <= sizeof(void *);

        union Storage {
            Storage() noexcept : pointer(nullptr) {}
            ~Storage() {}

            T  value;
            T *pointer;
        };

        /**
         * Construct the stored value using the default construction of `T`.
         */
        AtomicState() {
            if constexpr (stores_inline) {
                std::construct_at(std::addressof(m_storage.value));
            } else {
                m_storage.pointer = new T{};
            }
        }

        /**
         * Construct the stored value from a copied `T`.
         */
        explicit AtomicState(const T &value) {
            if constexpr (stores_inline) {
                std::construct_at(std::addressof(m_storage.value), value);
            } else {
                m_storage.pointer = new T(value);
            }
        }

        /**
         * Construct the stored value from a moved `T`.
         */
        explicit AtomicState(T &&value) {
            if constexpr (stores_inline) {
                std::construct_at(std::addressof(m_storage.value), std::move(value));
            } else {
                m_storage.pointer = new T(std::move(value));
            }
        }

        /**
         * Copy the stored value from another atomic state with the same resolved
         * type.
         */
        AtomicState(const AtomicState &other) {
            if constexpr (stores_inline) {
                std::construct_at(std::addressof(m_storage.value), other.get());
            } else {
                m_storage.pointer = new T(other.get());
            }
        }

        /**
         * Move the stored value from another atomic state with the same resolved
         * type.
         */
        AtomicState(AtomicState &&other) noexcept(std::is_nothrow_move_constructible_v<T>) {
            if constexpr (stores_inline) {
                std::construct_at(std::addressof(m_storage.value), std::move(other.get()));
            } else {
                m_storage.pointer       = other.m_storage.pointer;
                other.m_storage.pointer = nullptr;
            }
        }

        /**
         * Replace the stored value by copying from another atomic state with the
         * same resolved type.
         */
        AtomicState &operator=(const AtomicState &other) {
            if (this != &other) {
                reset();
                if constexpr (stores_inline) {
                    std::construct_at(std::addressof(m_storage.value), other.get());
                } else {
                    m_storage.pointer = new T(other.get());
                }
            }
            return *this;
        }

        /**
         * Replace the stored value by moving from another atomic state with the
         * same resolved type.
         */
        AtomicState &operator=(AtomicState &&other) noexcept(std::is_nothrow_move_constructible_v<T>) {
            if (this != &other) {
                reset();
                if constexpr (stores_inline) {
                    std::construct_at(std::addressof(m_storage.value), std::move(other.get()));
                } else {
                    m_storage.pointer       = other.m_storage.pointer;
                    other.m_storage.pointer = nullptr;
                }
            }
            return *this;
        }

        /**
         * Destroy the stored value using the storage mode selected for `T`.
         */
        ~AtomicState() { reset(); }

        /**
         * Return mutable access to the resolved stored value.
         */
        [[nodiscard]] T &get() noexcept {
            if constexpr (stores_inline) {
                return m_storage.value;
            } else {
                return *m_storage.pointer;
            }
        }

        /**
         * Return const access to the resolved stored value.
         */
        [[nodiscard]] const T &get() const noexcept {
            if constexpr (stores_inline) {
                return m_storage.value;
            } else {
                return *m_storage.pointer;
            }
        }

        /**
         * Return the hash of the resolved stored value.
         */
        [[nodiscard]] size_t hash() const override { return std::hash<T>{}(get()); }

        [[nodiscard]] bool eq(const detail::ViewDispatch &other) const override {
            return get() == static_cast<const AtomicState<T> &>(other).get();
        }

        /**
         * Return the string representation of the resolved stored value.
         */
        [[nodiscard]] std::string to_string() const override { return detail::value_to_string(get()); }

        /**
         * Compare this resolved stored value with another atomic state of the same
         * resolved type.
         */
        [[nodiscard]] std::partial_ordering operator<=>(const AtomicState<T> &other) const;

        /**
         * Compare this resolved stored value through the erased dispatch surface.
         *
         * The caller is expected to have established schema compatibility before
         * dispatching here, so the cast to `AtomicState<T>` is a resolved-type
         * operation rather than a public erased cast.
         */
        [[nodiscard]] std::partial_ordering operator<=>(const detail::ViewDispatch &other) const override
        {
            return *this <=> static_cast<const AtomicState<T> &>(other);
        }

        /**
         * Return mutable access to the stored payload for internal dispatch use.
         */
        [[nodiscard]] void *data() noexcept override { return std::addressof(get()); }

        /**
         * Return const access to the stored payload for internal dispatch use.
         */
        [[nodiscard]] const void *data() const noexcept override { return std::addressof(get()); }

        /**
         * Convert the stored value to its Python representation using the atomic
         * value-layer conversion rules owned by this header.
         */
        [[nodiscard]] nb::object to_python(const value::TypeMeta *schema) const override
        {
            static_cast<void>(schema);
            return detail::atomic_to_python(get());
        }

        /**
         * Replace the stored value from a Python object using the atomic
         * value-layer conversion rules owned by this header.
         */
        void from_python(const nb::object &src, const value::TypeMeta *schema) override
        {
            static_cast<void>(schema);
            detail::atomic_from_python(get(), src);
        }

        /**
         * Replace the stored value from another resolved atomic state with the
         * same schema-selected type.
         */
        void assign_from(const detail::ViewDispatch &other) override
        {
            get() = static_cast<const AtomicState<T> &>(other).get();
        }

        /**
         * Replace the stored value from a copied C++ object whose schema has
         * already been identified by the caller.
         */
        void set_from_cpp(const void *src, const value::TypeMeta *src_schema) override
        {
            if (src_schema != value::scalar_type_meta<T>()) {
                throw std::invalid_argument("AtomicState::set_from_cpp requires matching source schema");
            }
            get() = *static_cast<const T *>(src);
        }

        /**
         * Replace the stored value from a moved C++ object whose schema has
         * already been identified by the caller.
         */
        void move_from_cpp(void *src, const value::TypeMeta *src_schema) override
        {
            if (src_schema != value::scalar_type_meta<T>()) {
                throw std::invalid_argument("AtomicState::move_from_cpp requires matching source schema");
            }
            get() = std::move(*static_cast<T *>(src));
        }

        /**
         * Return an erased atomic view over this state.
         */
        [[nodiscard]] AtomicView view() noexcept;

        /**
         * Return a const erased atomic view over this state.
         */
        [[nodiscard]] AtomicView view() const noexcept;

      private:
        /**
         * Destroy the currently stored payload and return this state to an empty
         * storage slot ready for reconstruction.
         */
        void reset() noexcept {
            if constexpr (stores_inline) {
                std::destroy_at(std::addressof(m_storage.value));
            } else {
                delete m_storage.pointer;
                m_storage.pointer = nullptr;
            }
        }

        Storage m_storage;
    };

    /**
     * Non-owning atomic view.
     *
     * `AtomicView` is the type-erased atomic surface. It performs schema checks
     * before dispatching comparisons or casts to the resolved typed state.
     *
     * The view is intentionally not templated. The state carries the resolved
     * type while the view remains the erased, schema-checking entry point.
     */
    struct AtomicView : View
    {
        /**
         * Construct an invalid atomic view.
         */
        AtomicView() = default;
        /**
         * Reinterpret an existing erased view as an atomic view.
         *
         * This conversion is only valid for views whose schema kind is atomic
         * and whose dispatch implements the atomic dispatch surface. Both
         * conditions are enforced here so a successfully created `AtomicView`
         * can rely on static dispatch casts thereafter.
         */
        explicit AtomicView(const View &view) : View(view)
        {
            if (!view.valid()) { return; }

            const value::TypeMeta *schema = view.schema();
            if (schema == nullptr || schema->kind != value::TypeKind::Atomic) {
                throw std::runtime_error("AtomicView requires an atomic schema");
            }
        }

        /**
         * Construct an atomic view from a mutable resolved atomic state.
         */
        template <typename T>
        explicit AtomicView(AtomicState<T> &state) noexcept
            : View(&state, value::scalar_type_meta<T>()) {}

        /**
         * Construct an atomic view from a const resolved atomic state.
         */
        template <typename T>
        explicit AtomicView(const AtomicState<T> &state) noexcept
            : View(const_cast<AtomicState<T> *>(&state), value::scalar_type_meta<T>()) {}

        /**
         * Return `true` when the wrapped erased view currently resolves to an
         * atomic dispatch implementation.
         */
        [[nodiscard]] bool valid() const noexcept
        {
            return View::valid();
        }

        /**
         * Replace the represented value from another erased view.
         *
         * Assignment on `AtomicView` mutates the represented value rather than
         * rebinding the wrapper itself. This keeps the assignment surface aligned
         * with the old mutable value-view API.
         */
        AtomicView &operator=(const View &other) {
            if (!valid()) { throw std::runtime_error("AtomicView::operator= on invalid view"); }
            if (!other.valid()) { throw std::runtime_error("AtomicView::operator= from invalid view"); }
            if (schema() != other.schema()) {
                throw std::runtime_error("AtomicView::operator= requires matching schema");
            }
            AtomicView other_atomic{other};
            if (!other_atomic.valid()) {
                throw std::runtime_error("AtomicView::operator= requires atomic source view");
            }
            atomic_dispatch()->assign_from(*other_atomic.atomic_dispatch());
            return *this;
        }

        /**
         * Replace the represented value from another atomic view.
         *
         * Assignment on `AtomicView` mutates the represented value rather than
         * rebinding the wrapper itself. This keeps the assignment surface aligned
         * with the old mutable value-view API.
         */
        AtomicView &operator=(const AtomicView &other) {
            return *this = static_cast<const View &>(other);
        }

        /**
         * Return a mutable typed pointer when this view currently holds the
         * requested atomic schema, otherwise return `nullptr`.
         */
        template <typename T> [[nodiscard]] T *try_as() noexcept {
            return const_cast<T *>(detail::cast_atomic_data_as<T>(data_ptr(), schema()));
        }

        /**
         * Return a const typed pointer when this view currently holds the
         * requested atomic schema, otherwise return `nullptr`.
         */
        template <typename T> [[nodiscard]] const T *try_as() const noexcept {
            return detail::cast_atomic_data_as<T>(data_ptr(), schema());
        }

        /**
         * Replace the represented value from a typed atomic payload.
         */
        template <typename T>
            requires(!std::derived_from<std::remove_cvref_t<T>, View>)
        AtomicView &operator=(const T &value) {
            set(value);
            return *this;
        }

        /**
         * Replace the represented value from a moved typed atomic payload.
         */
        template <typename T>
            requires(!std::derived_from<std::remove_cvref_t<T>, View>)
        AtomicView &operator=(T &&value) {
            set(std::forward<T>(value));
            return *this;
        }

        /**
         * Return mutable typed access to the represented atomic value.
         *
         * This requires the represented value to currently hold the requested
         * atomic schema and fails persistently when that requirement is not met.
         */
        template <typename T> [[nodiscard]] T &as() {
            return checked_as<T>();
        }

        /**
         * Return const typed access to the represented atomic value.
         *
         * This requires the represented value to currently hold the requested
         * atomic schema and fails persistently when that requirement is not met.
         */
        template <typename T> [[nodiscard]] const T &as() const {
            return checked_as<T>();
        }

        /**
         * Replace the represented value from a copied typed atomic payload.
         */
        template <typename T> void set(const T &value) {
            checked_as<T>() = value;
        }

        /**
         * Replace the represented value from a moved typed atomic payload.
         */
        template <typename T> void set(T &&value) {
            checked_as<std::remove_cvref_t<T>>() = std::forward<T>(value);
        }

        /**
         * Return mutable typed access to the represented atomic value, throwing on
         * invalid views or schema mismatches.
         */
        template <typename T> [[nodiscard]] T &checked_as() {
            if (!valid()) { throw std::runtime_error("AtomicView::checked_as<T>() on invalid view"); }
            if (T *ptr = try_as<T>(); ptr != nullptr) { return *ptr; }
            throw std::runtime_error("AtomicView::checked_as<T>() type mismatch");
        }

        /**
         * Return const typed access to the represented atomic value, throwing on
         * invalid views or schema mismatches.
         */
        template <typename T> [[nodiscard]] const T &checked_as() const {
            if (!valid()) { throw std::runtime_error("AtomicView::checked_as<T>() on invalid view"); }
            if (const T *ptr = try_as<T>(); ptr != nullptr) { return *ptr; }
            throw std::runtime_error("AtomicView::checked_as<T>() type mismatch");
        }

      private:
        /**
         * Return mutable erased payload access for the typed atomic view helpers.
         *
         * This remains private so callers cannot bypass the typed schema-checked
         * surface.
         */
        [[nodiscard]] void *data_ptr() noexcept
        {
            if (auto *dispatch = atomic_dispatch(); dispatch != nullptr) {
                return dispatch->data();
            }
            return nullptr;
        }

        /**
         * Return const erased payload access for the typed atomic view helpers.
         *
         * This remains private so callers cannot bypass the typed schema-checked
         * surface.
         */
        [[nodiscard]] const void *data_ptr() const noexcept
        {
            if (const auto *dispatch = atomic_dispatch(); dispatch != nullptr) {
                return dispatch->data();
            }
            return nullptr;
        }

        /**
         * Return the atomic-specific dispatch interface when this view actually
         * represents an atomic state.
         */
        [[nodiscard]] detail::AtomicViewDispatch *atomic_dispatch() noexcept
        {
            return valid() ? static_cast<detail::AtomicViewDispatch *>(dispatch()) : nullptr;
        }

        /**
         * Return the atomic-specific dispatch interface when this view actually
         * represents an atomic state.
         */
        [[nodiscard]] const detail::AtomicViewDispatch *atomic_dispatch() const noexcept
        {
            return valid() ? static_cast<const detail::AtomicViewDispatch *>(dispatch()) : nullptr;
        }
    };

    template <typename T> inline std::partial_ordering AtomicState<T>::operator<=>(const AtomicState<T> &other) const {
        return get() <=> other.get();
    }

    template <typename T> inline AtomicView AtomicState<T>::view() noexcept { return AtomicView{*this}; }

    template <typename T> inline AtomicView AtomicState<T>::view() const noexcept { return AtomicView{*this}; }

    inline AtomicView View::as_atomic()
    {
        return AtomicView{*this};
    }

    inline AtomicView View::as_atomic() const
    {
        return AtomicView{*this};
    }

}  // namespace hgraph
