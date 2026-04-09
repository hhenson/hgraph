#pragma once

#include <hgraph/hgraph_base.h>
#include <hgraph/types/time_series/ts_view.h>

namespace hgraph {

/**
 * Output-specialized instantiation of the generic time-series view surface.
 *
 * `TSOutputView` is intended to expose output-facing operations without
 * transferring ownership of the underlying `TSOutput` state.
 */
struct HGRAPH_EXPORT TSOutputView : TSView<TSOutputView> {
    TSOutputView() = default;
    TSOutputView(TSViewContext context,
                 TSViewContext parent,
                 engine_time_t evaluation_time,
                 TSOutput *owning_output = nullptr,
                 const detail::TSOutputViewOps *output_view_ops = nullptr) noexcept
        : TSView<TSOutputView>(context, parent, evaluation_time),
          m_owning_output(owning_output),
          m_output_view_ops(output_view_ops)
    {
    }

    /**
     * Return the binding handle for the represented output position.
     */
    [[nodiscard]] LinkedTSContext linked_context() const noexcept
    {
        if (m_output_view_ops != nullptr) { return m_output_view_ops->linked_context(*this); }

        const TSViewContext resolved = this->m_context.resolved();
        return LinkedTSContext{
            resolved.schema,
            resolved.value_dispatch,
            resolved.ts_dispatch,
            resolved.value_data,
            this->m_context.ts_state,
            m_owning_output,
            m_output_view_ops,
        };
    }

    /**
     * Return the owning output endpoint for this logical output position.
     */
    [[nodiscard]] TSOutput *owning_output() const noexcept { return m_owning_output; }
    [[nodiscard]] const detail::TSOutputViewOps *output_view_ops() const noexcept { return m_output_view_ops; }

    [[nodiscard]] const TSViewContext &context_ref() const noexcept { return this->m_context; }

    /**
     * Internal helper used by collection wrappers to preserve output-specific
     * runtime state when navigating to a child TS position.
     */
    [[nodiscard]] TSOutputView make_child_view_impl(TSViewContext context,
                                                    TSViewContext parent,
                                                    engine_time_t evaluation_time) const noexcept
    {
        return TSOutputView{std::move(context), parent, evaluation_time, m_owning_output, m_output_view_ops};
    }

  private:
    TSOutput *m_owning_output{nullptr};
    const detail::TSOutputViewOps *m_output_view_ops{nullptr};
};

}  // namespace hgraph
