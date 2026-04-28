#ifndef HGRAPH_CPP_ROOT_TS_OUTPUT_VIEW_H
#define HGRAPH_CPP_ROOT_TS_OUTPUT_VIEW_H

#include <hgraph/v2/types/timeseries/ts_output.h>
#include <hgraph/v2/types/timeseries/ts_view.h>

namespace hgraph::v2
{
    /**
     * Non-owning output endpoint view.
     *
     * This first pass is intentionally thin: it exposes the bound TS schema,
     * evaluation time, and the underlying `ValueView` for the output-owned
     * payload.
     */
    struct TsOutputView : BasicTsView<TsOutputTypeBinding>
    {
        using base_type    = BasicTsView<TsOutputTypeBinding>;
        using context_type = typename base_type::context_type;

        TsOutputView() = default;

        TsOutputView(TsOutput *output, engine_time_t evaluation_time = MIN_DT) noexcept
            : base_type(context_type{
                  output != nullptr ? detail::ts_storage_view(output->binding(), output->data(), output->allocator())
                                    : TsOutputStorageHandle{},
                  evaluation_time,
              }) {}

        [[nodiscard]] bool is_bound() const noexcept { return binding() != nullptr; }
    };

    inline TsOutputView TsOutput::view(engine_time_t evaluation_time) noexcept { return TsOutputView{this, evaluation_time}; }

    inline TsOutputView TsOutput::view(engine_time_t evaluation_time) const noexcept {
        return TsOutputView{const_cast<TsOutput *>(this), evaluation_time};
    }
}  // namespace hgraph::v2

#endif  // HGRAPH_CPP_ROOT_TS_OUTPUT_VIEW_H
