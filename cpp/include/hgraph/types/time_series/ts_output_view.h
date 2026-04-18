#pragma once

#include <hgraph/hgraph_base.h>
#include <hgraph/types/time_series/ts_view.h>
#include <hgraph/types/time_series/ts_type_registry.h>

#include <memory>

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
                 const detail::TSOutputViewOps *output_view_ops = nullptr,
                 std::shared_ptr<const detail::TSOutputViewOps> output_view_ops_owner = {}) noexcept
        : TSView<TSOutputView>(context, parent, evaluation_time),
          m_owning_output(owning_output),
          m_output_view_ops(output_view_ops != nullptr ? output_view_ops : output_view_ops_owner.get()),
          m_output_view_ops_owner(std::move(output_view_ops_owner))
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
    [[nodiscard]] MutableTSSOutputView as_mutable_set() noexcept;
    [[nodiscard]] MutableTSSOutputView as_mutable_set() const noexcept;

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
     * Check whether another result can be applied in the current evaluation
     * tick without violating the output's mutation contract.
     */
    [[nodiscard]] bool can_apply_result(nb::handle value) const;

    /**
     * Clear the represented output position using schema-appropriate TS
     * semantics.
     */
    void clear() const;

    /**
     * Mark the represented output position invalid without routing through the
     * Python bridge.
     */
    void invalidate() const;

    /**
     * Internal helper used by collection wrappers to preserve output-specific
     * runtime state when navigating to a child TS position.
     */
    [[nodiscard]] TSOutputView make_child_view_impl(TSViewContext context,
                                                    TSViewContext parent,
                                                    engine_time_t evaluation_time,
                                                    std::shared_ptr<const detail::TSOutputViewOps> output_view_ops_owner = {}) const noexcept
    {
        return TSOutputView{
            std::move(context),
            parent,
            evaluation_time,
            m_owning_output,
            output_view_ops_owner != nullptr ? output_view_ops_owner.get() : m_output_view_ops,
            std::move(output_view_ops_owner)};
    }

  private:
    TSOutput *m_owning_output{nullptr};
    const detail::TSOutputViewOps *m_output_view_ops{nullptr};
    std::shared_ptr<const detail::TSOutputViewOps> m_output_view_ops_owner;
};

struct TSSOutputView : TSSReadView<TSOutputView>
{
    using TSSReadView<TSOutputView>::TSSReadView;
    TSSOutputView() = default;

    [[nodiscard]] LinkedTSContext linked_context() const noexcept { return this->view_ref().linked_context(); }
    [[nodiscard]] const TSOutputView &output_view() const noexcept { return this->view_ref(); }

    [[nodiscard]] TSOutputView register_contains_output(const View &item) const;
    void unregister_contains_output(const View &item) const;
    [[nodiscard]] TSOutputView register_is_empty_output() const;
    void unregister_is_empty_output() const;
};

struct MutableTSSOutputView : TSSOutputView
{
    using TSSOutputView::TSSOutputView;
    MutableTSSOutputView() = default;

    void from_python(nb::handle value) const;
    void add(const View &item) const;
    void remove(const View &item) const;
    void clear() const;
};

namespace detail {

[[nodiscard]] HGRAPH_EXPORT TSOutputView register_set_contains_output(const TSOutputView &view, const View &item);
HGRAPH_EXPORT void unregister_set_contains_output(const TSOutputView &view, const View &item);
[[nodiscard]] HGRAPH_EXPORT TSOutputView register_set_is_empty_output(const TSOutputView &view);
HGRAPH_EXPORT void unregister_set_is_empty_output(const TSOutputView &view);
HGRAPH_EXPORT void erase_dict_key(const TSOutputView &view, const View &key);
HGRAPH_EXPORT void add_set_item(const TSOutputView &view, const View &item);
HGRAPH_EXPORT void remove_set_item(const TSOutputView &view, const View &item);
HGRAPH_EXPORT void clear_set_items(const TSOutputView &view);
[[nodiscard]] HGRAPH_EXPORT TSOutputView make_missing_dict_child_output_view(const TSOutputView &view, const View &key);
[[nodiscard]] HGRAPH_EXPORT TSOutputView project_dict_key_set_output(const TSViewContext &source_context,
                                                                     engine_time_t      evaluation_time);

}  // namespace detail

inline MutableTSSOutputView TSOutputView::as_mutable_set() noexcept
{
    return MutableTSSOutputView{*this};
}

inline MutableTSSOutputView TSOutputView::as_mutable_set() const noexcept
{
    return MutableTSSOutputView{*this};
}

template <typename TView>
inline void TSDView<TView>::from_python(nb::handle value) const
    requires std::same_as<TView, TSOutputView>
{
    this->view_ref().from_python(value);
}

template <typename TView>
inline TSSetView<TView> TSDView<TView>::key_set() const
{
    TSOutputView projected_output =
        detail::project_dict_key_set_output(this->view_ref().context_ref(), this->view_ref().evaluation_time());
    if constexpr (std::same_as<TView, TSInputView>) {
        return TSSInputView{this->view_ref().make_child_view(projected_output.context_ref())};
    } else {
        return TSSOutputView{std::move(projected_output)};
    }
}

template <typename TView>
inline void TSDView<TView>::from_python(const View &key, nb::handle value) const
    requires std::same_as<TView, TSOutputView>
{
    this->key_dispatch()->child_from_python(this->view_ref(), key, value);
}

template <typename TView>
inline void TSDView<TView>::erase(const View &key) const
    requires std::same_as<TView, TSOutputView>
{
    detail::erase_dict_key(this->view_ref(), key);
}

inline TSOutputView TSSOutputView::register_contains_output(const View &item) const
{
    return detail::register_set_contains_output(this->view_ref(), item);
}

inline void TSSOutputView::unregister_contains_output(const View &item) const
{
    detail::unregister_set_contains_output(this->view_ref(), item);
}

inline TSOutputView TSSOutputView::register_is_empty_output() const
{
    return detail::register_set_is_empty_output(this->view_ref());
}

inline void TSSOutputView::unregister_is_empty_output() const
{
    detail::unregister_set_is_empty_output(this->view_ref());
}

inline void MutableTSSOutputView::from_python(nb::handle value) const
{
    this->view_ref().from_python(value);
}

inline void MutableTSSOutputView::add(const View &item) const
{
    detail::add_set_item(this->view_ref(), item);
}

inline void MutableTSSOutputView::remove(const View &item) const
{
    detail::remove_set_item(this->view_ref(), item);
}

inline void MutableTSSOutputView::clear() const
{
    detail::clear_set_items(this->view_ref());
}

}  // namespace hgraph
