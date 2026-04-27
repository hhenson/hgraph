#ifndef HGRAPH_CPP_ROOT_VALUE_BUILDER_H
#define HGRAPH_CPP_ROOT_VALUE_BUILDER_H

#include <hgraph/v2/types/metadata/type_registry.h>
#include <hgraph/v2/types/value/value_builder_ops.h>

#include <mutex>
#include <stdexcept>
#include <string>
#include <string_view>
#include <type_traits>
#include <unordered_map>

namespace hgraph::v2
{
    struct Value;
    struct ValueView;

    /**
     * Cached binder from a v2 value schema to owning storage and lightweight view behavior.
     *
     * For the current scalar slice, builders are registered through
     * `value::scalar_type_meta<T>()` and cached by schema pointer. A builder is
     * the one object that ties together:
     * - the interned `ValueTypeBinding` used by non-owning `ValueView`
     * - the bound storage plan used by owning `Value`
     */
    struct ValueBuilder
    {
        ValueBuilder() = default;

        explicit ValueBuilder(ValueBuilderOps ops) : m_ops(ops) {
            if (!m_ops.valid()) { throw std::logic_error("ValueBuilder requires a bound type binding"); }
        }

        [[nodiscard]] const ValueTypeBinding  *binding() const noexcept { return m_ops.binding; }
        [[nodiscard]] const ValueTypeMetaData *type() const noexcept { return m_ops.binding ? m_ops.binding->type_meta : nullptr; }
        [[nodiscard]] const MemoryUtils::StoragePlan *plan() const noexcept {
            return m_ops.binding ? m_ops.binding->plan() : nullptr;
        }
        [[nodiscard]] const MemoryUtils::LifecycleOps *lifecycle() const noexcept {
            return m_ops.binding ? m_ops.binding->lifecycle() : nullptr;
        }
        [[nodiscard]] const ValueOps *ops() const noexcept { return m_ops.binding ? m_ops.binding->ops : nullptr; }

        [[nodiscard]] const ValueTypeMetaData &checked_type() const {
            if (const ValueTypeMetaData *value_type = type(); value_type != nullptr) { return *value_type; }
            throw std::logic_error("ValueBuilder is not bound to a value type");
        }

        [[nodiscard]] const MemoryUtils::StoragePlan  &checked_plan() const { return checked_binding().checked_plan(); }
        [[nodiscard]] const ValueTypeBinding          &checked_binding() const { return m_ops.type_binding(); }
        [[nodiscard]] const MemoryUtils::LifecycleOps &checked_lifecycle() const { return checked_binding().checked_lifecycle(); }
        [[nodiscard]] const ValueOps                  &checked_ops() const { return checked_binding().checked_ops(); }

        [[nodiscard]] static const ValueBuilder *find(const ValueTypeMetaData *type) noexcept;
        [[nodiscard]] static const ValueBuilder &checked(const ValueTypeMetaData *type);

      private:
        ValueBuilderOps m_ops{};
    };

    namespace detail
    {
        template <typename T> using remove_cvref_t = std::remove_cv_t<std::remove_reference_t<T>>;

        template <typename T> struct ScalarTypeName
        {
            static constexpr std::string_view value{};
        };

        template <> struct ScalarTypeName<bool>
        {
            static constexpr std::string_view value{"bool"};
        };
        template <> struct ScalarTypeName<char>
        {
            static constexpr std::string_view value{"char"};
        };
        template <> struct ScalarTypeName<signed char>
        {
            static constexpr std::string_view value{"signed char"};
        };
        template <> struct ScalarTypeName<unsigned char>
        {
            static constexpr std::string_view value{"unsigned char"};
        };
        template <> struct ScalarTypeName<short>
        {
            static constexpr std::string_view value{"short"};
        };
        template <> struct ScalarTypeName<unsigned short>
        {
            static constexpr std::string_view value{"unsigned short"};
        };
        template <> struct ScalarTypeName<int>
        {
            static constexpr std::string_view value{"int"};
        };
        template <> struct ScalarTypeName<unsigned int>
        {
            static constexpr std::string_view value{"unsigned int"};
        };
        template <> struct ScalarTypeName<long>
        {
            static constexpr std::string_view value{"long"};
        };
        template <> struct ScalarTypeName<unsigned long>
        {
            static constexpr std::string_view value{"unsigned long"};
        };
        template <> struct ScalarTypeName<long long>
        {
            static constexpr std::string_view value{"long long"};
        };
        template <> struct ScalarTypeName<unsigned long long>
        {
            static constexpr std::string_view value{"unsigned long long"};
        };
        template <> struct ScalarTypeName<float>
        {
            static constexpr std::string_view value{"float"};
        };
        template <> struct ScalarTypeName<double>
        {
            static constexpr std::string_view value{"double"};
        };
        template <> struct ScalarTypeName<long double>
        {
            static constexpr std::string_view value{"long double"};
        };
        template <> struct ScalarTypeName<std::string>
        {
            static constexpr std::string_view value{"string"};
        };

        class ScalarValueBuilderRegistry
        {
          public:
            void register_builder(const ValueTypeMetaData *type, const ValueBuilder *builder) {
                if (type == nullptr || builder == nullptr) {
                    throw std::logic_error("ScalarValueBuilderRegistry requires non-null registrations");
                }

                std::lock_guard<std::mutex> lock(m_mutex);
                m_builders[type] = builder;
            }

            [[nodiscard]] const ValueBuilder *find(const ValueTypeMetaData *type) const noexcept {
                if (type == nullptr) { return nullptr; }

                std::lock_guard<std::mutex> lock(m_mutex);
                const auto                  it = m_builders.find(type);
                return it == m_builders.end() ? nullptr : it->second;
            }

          private:
            mutable std::mutex                                                  m_mutex;
            std::unordered_map<const ValueTypeMetaData *, const ValueBuilder *> m_builders{};
        };

        [[nodiscard]] inline ScalarValueBuilderRegistry &scalar_value_builder_registry() noexcept {
            static ScalarValueBuilderRegistry registry;
            return registry;
        }

        template <typename T> [[nodiscard]] const ValueBuilder &register_scalar_value_builder(std::string_view name) {
            using Type                                        = remove_cvref_t<T>;
            const ValueTypeMetaData               *meta       = TypeRegistry::instance().register_scalar<Type>(name);
            static const MemoryUtils::StoragePlan &plan       = MemoryUtils::plan_for<Type>();
            static const ValueTypeBinding         &binding    = ValueTypeBinding::intern(*meta, plan, scalar_value_ops<Type>());
            static const ValueBuilder              builder    = ValueBuilder(ValueBuilderOps{
                .binding = &binding,
            });
            static const bool                      registered = [] {
                scalar_value_builder_registry().register_builder(builder.type(), &builder);
                return true;
            }();
            static_cast<void>(registered);
            return builder;
        }
    }  // namespace detail

    inline const ValueBuilder *ValueBuilder::find(const ValueTypeMetaData *type) noexcept {
        return detail::scalar_value_builder_registry().find(type);
    }

    inline const ValueBuilder &ValueBuilder::checked(const ValueTypeMetaData *type) {
        if (const ValueBuilder *builder = find(type); builder != nullptr) { return *builder; }

        throw std::logic_error(
            "No v2 ValueBuilder is registered for this value type; bind scalars via hgraph::v2::value::scalar_type_meta<T>()");
    }

    namespace value
    {
        /**
         * Return the cached scalar builder for `T`.
         *
         * This is the bridge between the v2 metadata registry and the new
         * value/view layer. Callers that need a scalar schema for owning
         * `Value` or non-owning `ValueView` should prefer this path over
         * calling `TypeRegistry::register_scalar<T>()` directly.
         */
        template <typename T>
        [[nodiscard]] const ValueBuilder &
        scalar_value_builder(std::string_view name = detail::ScalarTypeName<detail::remove_cvref_t<T>>::value) {
            return detail::register_scalar_value_builder<detail::remove_cvref_t<T>>(name);
        }

        template <typename T>
        [[nodiscard]] const ValueTypeMetaData *
        scalar_type_meta(std::string_view name = detail::ScalarTypeName<detail::remove_cvref_t<T>>::value) {
            return scalar_value_builder<detail::remove_cvref_t<T>>(name).type();
        }
    }  // namespace value
}  // namespace hgraph::v2

#endif  // HGRAPH_CPP_ROOT_VALUE_BUILDER_H
