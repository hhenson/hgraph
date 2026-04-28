#ifndef HGRAPH_CPP_ROOT_TS_VIEW_H
#define HGRAPH_CPP_ROOT_TS_VIEW_H

#include <hgraph/util/date_time.h>
#include <hgraph/v2/types/timeseries/ts_value.h>

#include <utility>

namespace hgraph::v2
{
    struct TsViewContext
    {
        TsStorageViewHandle storage{};
        engine_time_t       evaluation_time{MIN_DT};

        TsViewContext() = default;

        TsViewContext(TsStorageViewHandle storage, engine_time_t evaluation_time = MIN_DT) noexcept
            : storage(std::move(storage)), evaluation_time(evaluation_time) {}

        TsViewContext(const TsViewContext &)                = delete;
        TsViewContext &operator=(const TsViewContext &)     = delete;
        TsViewContext(TsViewContext &&) noexcept            = default;
        TsViewContext &operator=(TsViewContext &&) noexcept = default;
    };

    /**
     * Thin erased view over a time-series endpoint value position.
     *
     * The view is backed by a borrowed `StorageHandle` and is move-only.
     * Future composite/path walking can therefore pass around non-owning
     * `TsView` objects without copying endpoint state. The current payload is
     * projected through `value()`. TS-specific structural adapters can layer
     * on top later without making `TsView` pretend to be a generic `ValueView`.
     */
    struct TsView
    {
        TsView() = default;

        explicit TsView(TsViewContext context) noexcept : m_context(std::move(context)) {}

        TsView(const TsView &)                = delete;
        TsView &operator=(const TsView &)     = delete;
        TsView(TsView &&) noexcept            = default;
        TsView &operator=(TsView &&) noexcept = default;

        [[nodiscard]] const TsViewContext       &context() const noexcept { return m_context; }
        [[nodiscard]] const TsStorageViewHandle &storage() const noexcept { return m_context.storage; }
        [[nodiscard]] const TsValueTypeBinding  *binding() const noexcept { return m_context.storage.binding(); }
        [[nodiscard]] const TsValueBuilder      *builder() const noexcept {
            return type() != nullptr ? TsValueBuilder::find(type()) : nullptr;
        }
        [[nodiscard]] const TSValueTypeMetaData *type() const noexcept {
            return binding() != nullptr ? binding()->type_meta : nullptr;
        }
        [[nodiscard]] const ValueTypeMetaData *value_type() const noexcept {
            return type() != nullptr ? type()->value_type : nullptr;
        }
        [[nodiscard]] engine_time_t evaluation_time() const noexcept { return m_context.evaluation_time; }
        [[nodiscard]] bool          has_value() const noexcept { return m_context.storage.data() != nullptr; }
        [[nodiscard]] explicit      operator bool() const noexcept { return has_value(); }

        [[nodiscard]] ValueView value() const noexcept {
            return detail::ts_value_view(binding(), const_cast<void *>(m_context.storage.data()));
        }

      protected:
        void refresh(TsStorageViewHandle storage) noexcept { m_context.storage = std::move(storage); }

      private:
        TsViewContext m_context{};
    };
}  // namespace hgraph::v2

#endif  // HGRAPH_CPP_ROOT_TS_VIEW_H
