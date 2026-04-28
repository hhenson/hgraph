#ifndef HGRAPH_CPP_ROOT_TS_OUPTUT_BUILDER_H
#define HGRAPH_CPP_ROOT_TS_OUPTUT_BUILDER_H

#include <hgraph/v2/types/timeseries/ts_output_builder_ops.h>
#include <hgraph/v2/types/timeseries/ts_value_builder.h>

#include <memory>
#include <mutex>
#include <stdexcept>
#include <unordered_map>
#include <vector>

namespace hgraph::v2
{
    /**
     * Cached output-endpoint builder wrapper.
     *
     * In the first pass this is intentionally thin: output construction uses a
     * dedicated output-side TS binding for the schema while still projecting
     * back to the shared `TsValueBuilder`.
     */
    struct TsOutputBuilder
    {
        TsOutputBuilder() = default;

        explicit TsOutputBuilder(TsOutputBuilderOps ops) : m_ops(ops) {
            if (!m_ops.valid()) { throw std::logic_error("TsOutputBuilder requires a TS output binding"); }
        }

        [[nodiscard]] const TsOutputBuilderOps  &ops() const noexcept { return m_ops; }
        [[nodiscard]] const TsOutputTypeBinding *binding() const noexcept { return m_ops.binding; }
        [[nodiscard]] const TsOutputTypeBinding &checked_binding() const { return m_ops.checked_binding(); }
        [[nodiscard]] const TSValueTypeMetaData *type() const noexcept {
            return m_ops.binding != nullptr ? m_ops.binding->type_meta : nullptr;
        }
        [[nodiscard]] const TsValueBuilder *ts_value_builder() const noexcept {
            return type() != nullptr ? TsValueBuilder::find(type()) : nullptr;
        }
        [[nodiscard]] const TsValueBuilder &checked_ts_value_builder() const { return TsValueBuilder::checked(type()); }

        [[nodiscard]] static const TsOutputBuilder *find(const TSValueTypeMetaData *type);
        [[nodiscard]] static const TsOutputBuilder &checked(const TSValueTypeMetaData *type);

      private:
        TsOutputBuilderOps m_ops{};
    };

    namespace detail
    {
        class TsOutputBuilderRegistry
        {
          public:
            [[nodiscard]] const TsOutputBuilder *find(const TSValueTypeMetaData *type) const noexcept {
                if (type == nullptr) { return nullptr; }

                std::lock_guard<std::mutex> lock(m_mutex);
                const auto                  it = m_builders.find(type);
                return it == m_builders.end() ? nullptr : it->second;
            }

            [[nodiscard]] const TsOutputBuilder &store_if_absent(const TSValueTypeMetaData &type, TsOutputBuilderOps ops) {
                if (const TsOutputBuilder *builder = find(&type); builder != nullptr) { return *builder; }

                auto                   builder = std::make_unique<TsOutputBuilder>(ops);
                const TsOutputBuilder *raw     = builder.get();

                std::lock_guard<std::mutex> lock(m_mutex);
                if (const auto it = m_builders.find(&type); it != m_builders.end()) { return *it->second; }

                m_storage.push_back(std::move(builder));
                m_builders.emplace(&type, raw);
                return *raw;
            }

          private:
            mutable std::mutex                                                       m_mutex{};
            std::unordered_map<const TSValueTypeMetaData *, const TsOutputBuilder *> m_builders{};
            std::vector<std::unique_ptr<TsOutputBuilder>>                            m_storage{};
        };

        [[nodiscard]] inline TsOutputBuilderRegistry &ts_output_builder_registry() noexcept {
            static TsOutputBuilderRegistry registry;
            return registry;
        }
    }  // namespace detail

    inline const TsOutputBuilder *TsOutputBuilder::find(const TSValueTypeMetaData *type) {
        return detail::ts_output_builder_registry().find(type);
    }

    inline const TsOutputBuilder &TsOutputBuilder::checked(const TSValueTypeMetaData *type) {
        if (type == nullptr) { throw std::logic_error("TsOutputBuilder::checked requires a non-null TS type"); }
        if (const TsOutputBuilder *builder = find(type); builder != nullptr) { return *builder; }

        const TsValueBuilder      &ts_value_builder = TsValueBuilder::checked(type);
        const TsOutputOps         &ops              = ts_output_ops(ts_value_builder.checked_value_binding());
        const TsOutputTypeBinding &binding = TsOutputTypeBinding::intern(*type, ts_value_builder.checked_value_plan(), ops);
        return detail::ts_output_builder_registry().store_if_absent(*type, TsOutputBuilderOps{
                                                                               .binding = &binding,
                                                                           });
    }
}  // namespace hgraph::v2

#endif  // HGRAPH_CPP_ROOT_TS_OUPTUT_BUILDER_H
