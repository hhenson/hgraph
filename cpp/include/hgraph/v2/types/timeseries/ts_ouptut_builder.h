#ifndef HGRAPH_CPP_ROOT_TS_OUPTUT_BUILDER_H
#define HGRAPH_CPP_ROOT_TS_OUPTUT_BUILDER_H

#include <hgraph/v2/types/timeseries/ts_output_builder_ops.h>

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
     * In the first pass this is intentionally thin: output construction uses
     * the same time-series schema-to-value binding as `TsValueBuilder`, with
     * endpoint behavior layered separately in `TsOutput`.
     */
    struct TsOutputBuilder
    {
        TsOutputBuilder() = default;

        explicit TsOutputBuilder(TsOutputBuilderOps ops) : m_ops(ops) {
            if (!m_ops.valid()) { throw std::logic_error("TsOutputBuilder requires a TS value builder"); }
        }

        [[nodiscard]] const TsOutputBuilderOps  &ops() const noexcept { return m_ops; }
        [[nodiscard]] const TsValueBuilder      *ts_value_builder() const noexcept { return m_ops.ts_value_builder; }
        [[nodiscard]] const TsValueBuilder      &checked_ts_value_builder() const { return m_ops.checked_ts_value_builder(); }
        [[nodiscard]] const TSValueTypeMetaData *type() const noexcept {
            return m_ops.ts_value_builder != nullptr ? m_ops.ts_value_builder->type() : nullptr;
        }

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

        const TsValueBuilder &ts_value_builder = TsValueBuilder::checked(type);
        return detail::ts_output_builder_registry().store_if_absent(*type, TsOutputBuilderOps{
                                                                               .ts_value_builder = &ts_value_builder,
                                                                           });
    }
}  // namespace hgraph::v2

#endif  // HGRAPH_CPP_ROOT_TS_OUPTUT_BUILDER_H
