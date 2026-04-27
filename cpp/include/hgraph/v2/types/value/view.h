#ifndef HGRAPH_CPP_ROOT_VIEW_H
#define HGRAPH_CPP_ROOT_VIEW_H

#include <hgraph/v2/types/value/value_builder.h>

#include <new>
#include <stdexcept>
#include <string>
#include <type_traits>
#include <utility>

namespace hgraph::v2
{
    struct Value;

    /**
     * Data holder for a non-owning erased scalar value reference.
     */
    struct ValueViewContext
    {
        const ValueTypeBinding *binding{nullptr};
        void                   *data{nullptr};
    };

    /**
     * Non-owning erased handle over a live scalar payload.
     *
     * `ValueView` carries:
     * - the schema/lifecycle/ops `ValueTypeBinding`
     * - a pointer to the underlying constructed object
     *
     * The first scalar slice intentionally mirrors the older value/view model:
     * a view is lightweight and copyable, and typed access is recovered by
     * comparing the bound schema pointer against `value::scalar_type_meta<T>()`.
     */
    struct ValueView
    {
        ValueViewContext context{};

        ValueView() = default;

        ValueView(const ValueTypeBinding *binding, void *data) noexcept : context{binding, data} {}
        ValueView(const ValueBuilder *builder, void *data) noexcept : context{builder ? builder->binding() : nullptr, data} {}

        [[nodiscard]] static ValueView invalid_for(const ValueTypeBinding &binding) noexcept {
            return ValueView{&binding, nullptr};
        }

        [[nodiscard]] static ValueView invalid_for(const ValueBuilder &builder) noexcept {
            return ValueView{builder.binding(), nullptr};
        }

        [[nodiscard]] bool has_value() const noexcept { return context.data != nullptr; }
        explicit           operator bool() const noexcept { return has_value(); }

        [[nodiscard]] const ValueTypeBinding  *binding() const noexcept { return context.binding; }
        [[nodiscard]] const ValueTypeMetaData *type() const noexcept {
            return context.binding ? context.binding->type_meta : nullptr;
        }
        [[nodiscard]] const MemoryUtils::LifecycleOps *lifecycle() const noexcept {
            return context.binding ? context.binding->lifecycle() : nullptr;
        }
        [[nodiscard]] const void *lifecycle_context() const noexcept {
            return context.binding ? context.binding->lifecycle_context() : nullptr;
        }
        [[nodiscard]] const ValueOps *ops() const noexcept { return context.binding ? context.binding->ops : nullptr; }
        [[nodiscard]] void           *data() noexcept { return context.data; }
        [[nodiscard]] const void     *data() const noexcept { return context.data; }

        template <typename T> [[nodiscard]] bool is_type() const noexcept {
            return type() == value::scalar_type_meta<std::remove_cv_t<std::remove_reference_t<T>>>();
        }

        template <typename T> [[nodiscard]] T *try_as() noexcept {
            using Type = std::remove_cv_t<std::remove_reference_t<T>>;
            if (!has_value() || !is_type<Type>()) { return nullptr; }
            return std::launder(reinterpret_cast<Type *>(context.data));
        }

        template <typename T> [[nodiscard]] const T *try_as() const noexcept {
            using Type = std::remove_cv_t<std::remove_reference_t<T>>;
            if (!has_value() || !is_type<Type>()) { return nullptr; }
            return std::launder(reinterpret_cast<const Type *>(context.data));
        }

        template <typename T> [[nodiscard]] T &checked_as() {
            if (!has_value()) { throw std::logic_error("ValueView::checked_as<T>() on an empty view"); }

            if (T *result = try_as<T>(); result != nullptr) { return *result; }

            throw std::logic_error("ValueView::checked_as<T>() type mismatch");
        }

        template <typename T> [[nodiscard]] const T &checked_as() const {
            if (!has_value()) { throw std::logic_error("ValueView::checked_as<T>() on an empty view"); }

            if (const T *result = try_as<T>(); result != nullptr) { return *result; }

            throw std::logic_error("ValueView::checked_as<T>() type mismatch");
        }

        template <typename T> [[nodiscard]] T &as() { return checked_as<T>(); }

        template <typename T> [[nodiscard]] const T &as() const { return checked_as<T>(); }

        template <typename T>
            requires(!std::same_as<std::remove_cv_t<std::remove_reference_t<T>>, ValueView> &&
                     !std::same_as<std::remove_cv_t<std::remove_reference_t<T>>, Value>)
        void set(T &&value) {
            using Type = std::remove_cv_t<std::remove_reference_t<T>>;

            if (!has_value()) { throw std::logic_error("ValueView::set<T>() on an empty view"); }
            if (!is_type<Type>()) { throw std::logic_error("ValueView::set<T>() type mismatch"); }

            if constexpr (std::is_assignable_v<Type &, T &&>) {
                *std::launder(reinterpret_cast<Type *>(context.data)) = std::forward<T>(value);
            } else {
                throw std::logic_error("ValueView::set<T>() requires an assignable scalar type");
            }
        }

        void copy_from(const ValueView &other) {
            if (!has_value()) { throw std::logic_error("ValueView::copy_from() on an empty destination"); }
            if (!other.has_value()) { throw std::logic_error("ValueView::copy_from() from an empty source"); }
            if (type() != other.type()) { throw std::logic_error("ValueView::copy_from() requires matching scalar schemas"); }

            checked_binding().copy_assign_at(context.data, other.context.data);
        }

        [[nodiscard]] size_t hash() const {
            if (!has_value()) { throw std::logic_error("ValueView::hash() on an empty view"); }
            return checked_ops().hash_of(context.data);
        }

        [[nodiscard]] bool equals(const ValueView &other) const {
            if (!has_value() || !other.has_value()) { return !has_value() && !other.has_value(); }
            if (type() != other.type()) { return false; }
            return checked_ops().equals_of(context.data, other.context.data);
        }

        [[nodiscard]] std::string to_string() const {
            if (!has_value()) { throw std::logic_error("ValueView::to_string() on an empty view"); }
            return checked_ops().to_string_of(context.data);
        }

        [[nodiscard]] bool operator==(const ValueView &other) const { return equals(other); }

      private:
        [[nodiscard]] const ValueTypeBinding &checked_binding() const {
            if (const ValueTypeBinding *value_binding = binding(); value_binding != nullptr) { return *value_binding; }

            throw std::logic_error("ValueView is not bound to a type binding");
        }

        [[nodiscard]] const ValueOps &checked_ops() const { return checked_binding().checked_ops(); }
    };
}  // namespace hgraph::v2

#endif  // HGRAPH_CPP_ROOT_VIEW_H
