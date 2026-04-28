#ifndef HGRAPH_CPP_ROOT_TS_VALUE_BUILDER_H
#define HGRAPH_CPP_ROOT_TS_VALUE_BUILDER_H

#include <hgraph/v2/types/timeseries/ts_value_builder_ops.h>
#include <hgraph/v2/types/value/value_builder.h>

#include <memory>
#include <mutex>
#include <stdexcept>
#include <unordered_map>
#include <vector>

namespace hgraph::v2
{
    /**
     * Cached bridge from a TS schema to the underlying v2 value builder.
     *
     * The first pass keeps the TS runtime thin: the TS schema binds directly to
     * the value-layer storage plan for `TSValueTypeMetaData::value_type`. That
     * lets TS endpoints surface `ValueView` without embedding a `Value` object.
     */
    struct TsValueBuilder
    {
        TsValueBuilder() = default;

        explicit TsValueBuilder(TsValueBuilderOps ops) : m_ops(ops) {
            if (!m_ops.valid()) { throw std::logic_error("TsValueBuilder requires a TS type binding"); }
        }

        [[nodiscard]] const TsValueBuilderOps   &ops() const noexcept { return m_ops; }
        [[nodiscard]] const TsValueTypeBinding  *binding() const noexcept { return m_ops.binding; }
        [[nodiscard]] const TSValueTypeMetaData *type() const noexcept {
            return m_ops.binding != nullptr ? m_ops.binding->type_meta : nullptr;
        }
        [[nodiscard]] const ValueBuilder      *value_builder() const noexcept { return ValueBuilder::find(value_type()); }
        [[nodiscard]] const ValueTypeMetaData *value_type() const noexcept {
            return m_ops.binding != nullptr ? m_ops.binding->checked_ops().value_type() : nullptr;
        }
        [[nodiscard]] const ValueTypeBinding *value_binding() const noexcept {
            return m_ops.binding != nullptr ? m_ops.binding->checked_ops().value_binding : nullptr;
        }
        [[nodiscard]] const MemoryUtils::StoragePlan *value_plan() const noexcept {
            return m_ops.binding != nullptr ? m_ops.binding->plan() : nullptr;
        }

        [[nodiscard]] const TsValueTypeBinding  &checked_binding() const { return m_ops.checked_binding(); }
        [[nodiscard]] const TSValueTypeMetaData &checked_type() const { return checked_binding().checked_type(); }
        [[nodiscard]] const TsValueOps          &checked_ops() const { return checked_binding().checked_ops(); }
        [[nodiscard]] const ValueBuilder        &checked_value_builder() const { return ValueBuilder::checked(value_type()); }
        [[nodiscard]] const ValueTypeBinding    &checked_value_binding() const { return checked_ops().checked_value_binding(); }
        [[nodiscard]] const ValueTypeMetaData   &checked_value_type() const { return checked_value_binding().checked_type(); }
        [[nodiscard]] const MemoryUtils::StoragePlan &checked_value_plan() const { return checked_value_binding().checked_plan(); }

        [[nodiscard]] static const TsValueBuilder *find(const TSValueTypeMetaData *type);
        [[nodiscard]] static const TsValueBuilder &checked(const TSValueTypeMetaData *type);

      private:
        TsValueBuilderOps m_ops{};
    };

    namespace detail
    {
        class TsValueBuilderRegistry
        {
          public:
            [[nodiscard]] const TsValueBuilder *find(const TSValueTypeMetaData *type) const noexcept {
                if (type == nullptr) { return nullptr; }

                std::lock_guard<std::mutex> lock(m_mutex);
                const auto                  it = m_builders.find(type);
                return it == m_builders.end() ? nullptr : it->second;
            }

            [[nodiscard]] const TsValueBuilder &store_if_absent(const TSValueTypeMetaData &type, TsValueBuilderOps ops) {
                if (const TsValueBuilder *builder = find(&type); builder != nullptr) { return *builder; }

                auto                  builder = std::make_unique<TsValueBuilder>(ops);
                const TsValueBuilder *raw     = builder.get();

                std::lock_guard<std::mutex> lock(m_mutex);
                if (const auto it = m_builders.find(&type); it != m_builders.end()) { return *it->second; }

                m_storage.push_back(std::move(builder));
                m_builders.emplace(&type, raw);
                return *raw;
            }

          private:
            mutable std::mutex                                                      m_mutex{};
            std::unordered_map<const TSValueTypeMetaData *, const TsValueBuilder *> m_builders{};
            std::vector<std::unique_ptr<TsValueBuilder>>                            m_storage{};
        };

        [[nodiscard]] inline TsValueBuilderRegistry &ts_value_builder_registry() noexcept {
            static TsValueBuilderRegistry registry;
            return registry;
        }

        [[nodiscard]] inline const TsValueBuilder &register_ts_value_builder(const TSValueTypeMetaData &type) {
            if (const TsValueBuilder *builder = ts_value_builder_registry().find(&type); builder != nullptr) { return *builder; }
            if (type.value_type == nullptr) {
                throw std::logic_error("TsValueBuilder requires TS schemas with a bound value_type");
            }

            const ValueBuilder       &value_builder = ValueBuilder::checked(type.value_type);
            const TsValueOps         &ops           = ts_value_ops(value_builder.checked_binding());
            const TsValueTypeBinding &binding       = TsValueTypeBinding::intern(type, value_builder.checked_plan(), ops);
            return ts_value_builder_registry().store_if_absent(type, TsValueBuilderOps{
                                                                         .binding = &binding,
                                                                     });
        }
    }  // namespace detail

    inline const TsValueBuilder *TsValueBuilder::find(const TSValueTypeMetaData *type) {
        return detail::ts_value_builder_registry().find(type);
    }

    inline const TsValueBuilder &TsValueBuilder::checked(const TSValueTypeMetaData *type) {
        if (type == nullptr) { throw std::logic_error("TsValueBuilder::checked requires a non-null TS type"); }
        if (const TsValueBuilder *builder = find(type); builder != nullptr) { return *builder; }
        return detail::register_ts_value_builder(*type);
    }
}  // namespace hgraph::v2

#endif  // HGRAPH_CPP_ROOT_TS_VALUE_BUILDER_H
