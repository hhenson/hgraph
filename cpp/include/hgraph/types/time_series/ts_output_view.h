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

}  // namespace detail

template <typename TView>
inline void TSDView<TView>::from_python(nb::handle value) const
    requires std::same_as<TView, TSOutputView>
{
    this->view_ref().from_python(value);
}

template <typename TView>
inline TSOutputView TSDView<TView>::key_set() const
    requires std::same_as<TView, TSOutputView>
{
    const TSMeta *schema = this->view_ref().context_ref().schema;
    return this->view_ref().owning_output()->bindable_view(this->view_ref(), TSTypeRegistry::instance().tss(schema->key_type()));
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

template <typename TView>
inline void TSSView<TView>::from_python(nb::handle value) const
    requires std::same_as<TView, TSOutputView>
{
    this->view_ref().from_python(value);
}

template <typename TView>
inline void TSSView<TView>::add(const View &item) const
    requires std::same_as<TView, TSOutputView>
{
    detail::add_set_item(this->view_ref(), item);
}

template <typename TView>
inline void TSSView<TView>::remove(const View &item) const
    requires std::same_as<TView, TSOutputView>
{
    detail::remove_set_item(this->view_ref(), item);
}

template <typename TView>
inline void TSSView<TView>::clear() const
    requires std::same_as<TView, TSOutputView>
{
    detail::clear_set_items(this->view_ref());
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
