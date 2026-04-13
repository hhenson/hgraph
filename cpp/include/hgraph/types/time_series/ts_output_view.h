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
     * Apply a Python-facing value assignment to this output position using the
     * native TS semantics for the represented schema.
     */
    void from_python(nb::handle value) const;

    /**
     * Apply a Python node result. `None` is treated as "no update", matching
     * the Python output contract.
     */
    void apply_result(nb::handle value) const;

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

namespace detail {

[[nodiscard]] HGRAPH_EXPORT TSOutputView register_set_contains_output(const TSOutputView &view, const View &item);
HGRAPH_EXPORT void unregister_set_contains_output(const TSOutputView &view, const View &item);
[[nodiscard]] HGRAPH_EXPORT TSOutputView register_set_is_empty_output(const TSOutputView &view);
HGRAPH_EXPORT void unregister_set_is_empty_output(const TSOutputView &view);

}  // namespace detail

template <typename TView>
inline void TSDView<TView>::from_python(nb::handle value) const
    requires std::same_as<TView, TSOutputView>
{
    this->view_ref().from_python(value);
}

template <typename TView>
inline void TSDView<TView>::from_python(const View &key, nb::handle value) const
    requires std::same_as<TView, TSOutputView>
{
    const auto *dispatch = this->key_dispatch();
    if (dispatch == nullptr) {
        throw std::logic_error("TSDView::from_python(key, value) requires a dict dispatch");
    }
    dispatch->child_from_python(this->view_ref(), key, value);
}

template <typename TView>
inline void TSSView<TView>::from_python(nb::handle value) const
    requires std::same_as<TView, TSOutputView>
{
    this->view_ref().from_python(value);
}

template <typename TView>
inline TSOutputView TSSView<TView>::register_contains_output(const View &item) const
    requires std::same_as<TView, TSOutputView>
{
    return detail::register_set_contains_output(this->view_ref(), item);
}

template <typename TView>
inline void TSSView<TView>::unregister_contains_output(const View &item) const
    requires std::same_as<TView, TSOutputView>
{
    detail::unregister_set_contains_output(this->view_ref(), item);
}

template <typename TView>
inline TSOutputView TSSView<TView>::register_is_empty_output() const
    requires std::same_as<TView, TSOutputView>
{
    return detail::register_set_is_empty_output(this->view_ref());
}

template <typename TView>
inline void TSSView<TView>::unregister_is_empty_output() const
    requires std::same_as<TView, TSOutputView>
{
    detail::unregister_set_is_empty_output(this->view_ref());
}

}  // namespace hgraph
