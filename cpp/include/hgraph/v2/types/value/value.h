#ifndef HGRAPH_CPP_ROOT_VALUE_H
#define HGRAPH_CPP_ROOT_VALUE_H

#include <hgraph/v2/types/value/view.h>

#include <stdexcept>
#include <type_traits>
#include <utility>

namespace hgraph::v2
{
    /**
     * Thin owning scalar value wrapper over a v2 `StorageHandle`.
     *
     * `Value` is schema-bound at construction time. For the initial scalar
     * slice, the schema must be bridged through `value::scalar_type_meta<T>()`
     * so the corresponding `ValueBuilder` is available.
     */
    struct Value
    {
        using storage_type = MemoryUtils::StorageHandle<MemoryUtils::InlineStoragePolicy<>, ValueTypeBinding>;

        Value() noexcept = default;

        explicit Value(const ValueTypeMetaData &type, const MemoryUtils::AllocatorOps &allocator = MemoryUtils::allocator())
            : Value(ValueBuilder::checked(&type), allocator) {}

        explicit Value(const ValueBuilder &builder, const MemoryUtils::AllocatorOps &allocator = MemoryUtils::allocator())
            : m_storage(builder.checked_binding(), allocator) {}

        explicit Value(const ValueTypeBinding &binding, const MemoryUtils::AllocatorOps &allocator = MemoryUtils::allocator())
            : m_storage(binding, allocator) {}

        explicit Value(const ValueView &view, const MemoryUtils::AllocatorOps &allocator = MemoryUtils::allocator())
            : Value(checked_view_binding(view), allocator) {
            this->view().copy_from(view);
        }

        template <typename T>
            requires(!std::same_as<std::remove_cv_t<std::remove_reference_t<T>>, Value> &&
                     !std::same_as<std::remove_cv_t<std::remove_reference_t<T>>, ValueView>)
        explicit Value(T &&value, const MemoryUtils::AllocatorOps &allocator = MemoryUtils::allocator())
            : Value(*value::scalar_type_meta<std::remove_cv_t<std::remove_reference_t<T>>>(), allocator) {
            view().set(std::forward<T>(value));
        }

        Value(const Value &)                = default;
        Value(Value &&) noexcept            = default;
        Value &operator=(const Value &)     = default;
        Value &operator=(Value &&) noexcept = default;

        [[nodiscard]] bool has_value() const noexcept { return m_storage.has_value(); }
        explicit           operator bool() const noexcept { return has_value(); }

        [[nodiscard]] const ValueTypeBinding          *binding() const noexcept { return m_storage.binding(); }
        [[nodiscard]] const ValueTypeMetaData         *type() const noexcept { return binding() ? binding()->type_meta : nullptr; }
        [[nodiscard]] const MemoryUtils::StoragePlan  *plan() const noexcept { return m_storage.plan(); }
        [[nodiscard]] const MemoryUtils::AllocatorOps &allocator() const noexcept {
            if (const auto *allocator_ops = m_storage.allocator(); allocator_ops != nullptr) { return *allocator_ops; }
            return MemoryUtils::allocator();
        }

        [[nodiscard]] ValueView view() noexcept { return binding() ? ValueView{binding(), m_storage.data()} : ValueView{}; }

        [[nodiscard]] ValueView view() const noexcept {
            return binding() ? ValueView{binding(), const_cast<void *>(m_storage.data())} : ValueView{};
        }

        template <typename T> [[nodiscard]] T &as() { return view().template as<T>(); }

        template <typename T> [[nodiscard]] const T &as() const { return view().template as<T>(); }

        [[nodiscard]] size_t      hash() const { return view().hash(); }
        [[nodiscard]] bool        equals(const Value &other) const { return view().equals(other.view()); }
        [[nodiscard]] bool        equals(const ValueView &other) const { return view().equals(other); }
        [[nodiscard]] std::string to_string() const { return view().to_string(); }

        void reset() {
            if (m_storage.plan() != nullptr) { m_storage.reset_to_default(); }
        }

      private:
        storage_type m_storage{};

        [[nodiscard]] static const ValueTypeBinding &checked_view_binding(const ValueView &view) {
            if (!view.has_value()) { throw std::logic_error("Value cannot be copy-constructed from an empty ValueView"); }

            if (const ValueTypeBinding *binding = view.binding(); binding != nullptr) { return *binding; }

            throw std::logic_error("ValueView is missing type binding metadata");
        }
    };

    namespace value
    {
        template <typename T>
            requires(!std::same_as<std::remove_cv_t<std::remove_reference_t<T>>, Value> &&
                     !std::same_as<std::remove_cv_t<std::remove_reference_t<T>>, ValueView>)
        [[nodiscard]] Value value_for(T &&value, const MemoryUtils::AllocatorOps &allocator = MemoryUtils::allocator()) {
            return Value(std::forward<T>(value), allocator);
        }
    }  // namespace value
}  // namespace hgraph::v2

#endif  // HGRAPH_CPP_ROOT_VALUE_H
