#ifndef HGRAPH_CPP_ROOT_TS_INPUT_BUILDER_H
#define HGRAPH_CPP_ROOT_TS_INPUT_BUILDER_H

#include <hgraph/v2/types/timeseries/ts_input_builder_ops.h>
#include <hgraph/v2/types/timeseries/ts_value_builder.h>

#include <memory>
#include <mutex>
#include <stdexcept>
#include <unordered_map>
#include <vector>

namespace hgraph::v2
{
    /**
     * Cached input-endpoint builder wrapper.
     *
     * Inputs are schema-bound references in the first pass, so the builder is
     * responsible for recovering the input-side TS binding for a schema while
     * still projecting back to the shared `TsValueBuilder`.
     */
    struct TsInputBuilder
    {
        TsInputBuilder() = default;

        explicit TsInputBuilder(TsInputBuilderOps ops) : m_ops(ops) {
            if (!m_ops.valid()) { throw std::logic_error("TsInputBuilder requires a TS input binding"); }
        }

        [[nodiscard]] const TsInputBuilderOps   &ops() const noexcept { return m_ops; }
        [[nodiscard]] const TsInputTypeBinding  *binding() const noexcept { return m_ops.binding; }
        [[nodiscard]] const TsInputTypeBinding  &checked_binding() const { return m_ops.checked_binding(); }
        [[nodiscard]] const TSValueTypeMetaData *type() const noexcept {
            return m_ops.binding != nullptr ? m_ops.binding->type_meta : nullptr;
        }
        [[nodiscard]] const TsValueBuilder *ts_value_builder() const noexcept {
            return type() != nullptr ? TsValueBuilder::find(type()) : nullptr;
        }
        [[nodiscard]] const TsValueBuilder &checked_ts_value_builder() const { return TsValueBuilder::checked(type()); }

        [[nodiscard]] static const TsInputBuilder *find(const TSValueTypeMetaData *type);
        [[nodiscard]] static const TsInputBuilder &checked(const TSValueTypeMetaData *type);

      private:
        TsInputBuilderOps m_ops{};
    };

    namespace detail
    {
        class TsInputBuilderRegistry
        {
          public:
            [[nodiscard]] const TsInputBuilder *find(const TSValueTypeMetaData *type) const noexcept {
                if (type == nullptr) { return nullptr; }

                std::lock_guard<std::mutex> lock(m_mutex);
                const auto                  it = m_builders.find(type);
                return it == m_builders.end() ? nullptr : it->second;
            }

            [[nodiscard]] const TsInputBuilder &store_if_absent(const TSValueTypeMetaData &type, TsInputBuilderOps ops) {
                if (const TsInputBuilder *builder = find(&type); builder != nullptr) { return *builder; }

                auto                  builder = std::make_unique<TsInputBuilder>(ops);
                const TsInputBuilder *raw     = builder.get();

                std::lock_guard<std::mutex> lock(m_mutex);
                if (const auto it = m_builders.find(&type); it != m_builders.end()) { return *it->second; }

                m_storage.push_back(std::move(builder));
                m_builders.emplace(&type, raw);
                return *raw;
            }

          private:
            mutable std::mutex                                                      m_mutex{};
            std::unordered_map<const TSValueTypeMetaData *, const TsInputBuilder *> m_builders{};
            std::vector<std::unique_ptr<TsInputBuilder>>                            m_storage{};
        };

        [[nodiscard]] inline TsInputBuilderRegistry &ts_input_builder_registry() noexcept {
            static TsInputBuilderRegistry registry;
            return registry;
        }
    }  // namespace detail

    inline const TsInputBuilder *TsInputBuilder::find(const TSValueTypeMetaData *type) {
        return detail::ts_input_builder_registry().find(type);
    }

    inline const TsInputBuilder &TsInputBuilder::checked(const TSValueTypeMetaData *type) {
        if (type == nullptr) { throw std::logic_error("TsInputBuilder::checked requires a non-null TS type"); }
        if (const TsInputBuilder *builder = find(type); builder != nullptr) { return *builder; }

        const TsValueBuilder     &ts_value_builder = TsValueBuilder::checked(type);
        const TsInputOps         &ops              = ts_input_ops(*type, ts_value_builder.checked_value_binding());
        const TsInputTypeBinding &binding          = TsInputTypeBinding::intern(*type, ts_value_builder.checked_value_plan(), ops);
        return detail::ts_input_builder_registry().store_if_absent(*type, TsInputBuilderOps{
                                                                              .binding = &binding,
                                                                          });
    }
}  // namespace hgraph::v2

#endif  // HGRAPH_CPP_ROOT_TS_INPUT_BUILDER_H
