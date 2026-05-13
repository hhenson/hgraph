#pragma once

#include <hgraph/hgraph_base.h>
#include <hgraph/types/time_series/ts_type_registry.h>
#include <hgraph/types/time_series/ts_view.h>

namespace hgraph
{

    namespace detail
    {
        [[nodiscard]] HGRAPH_EXPORT TSOutputView output_view_from_pending_dict_child(const PendingDictChildContext &context,
                                                                                     engine_time_t evaluation_time);
    }

    /**
     * Output-specialized instantiation of the generic time-series view surface.
     *
     * `TSOutputView` is intended to expose output-facing operations without
     * transferring ownership of the underlying `TSOutput` state.
     */
    struct HGRAPH_EXPORT TSOutputView : TSView<TSOutputView>
    {
        TSOutputView() = default;
        TSOutputView(TSViewContext context, TSViewContext parent, engine_time_t evaluation_time, TSOutput *owning_output = nullptr,
                     const detail::TSOutputViewOps *output_view_ops = nullptr) noexcept
            : TSView<TSOutputView>(context, parent, evaluation_time), m_owning_output(owning_output),
              m_output_view_ops(output_view_ops) {}

        /**
         * Return the binding handle for the represented output position.
         */
        [[nodiscard]] LinkedTSContext linked_context() const noexcept {
            if (this->m_context.pending_dict_child.active()) {
                TSOutputView child =
                    detail::output_view_from_pending_dict_child(this->m_context.pending_dict_child, this->evaluation_time());
                child = detail::ensure_dict_child_output_view(child, this->m_context.pending_dict_child.key.view());
                if (!child.context_ref().pending_dict_child.active() && child.context_ref().is_bound()) {
                    return child.linked_context();
                }

                LinkedTSContext source    = this->m_context;
                source.owning_output      = source.owning_output != nullptr ? source.owning_output : m_owning_output;
                source.output_view_ops    = source.output_view_ops != nullptr ? source.output_view_ops : m_output_view_ops;
                source.notification_state = source.notification_state != nullptr ? source.notification_state : source.ts_state;
                return source;
            }

            if (m_output_view_ops != nullptr) { return m_output_view_ops->linked_context(*this); }

            const TSViewContext  resolved = this->m_context.resolved();
            const TSViewContext &source =
                this->m_context.is_bound() && this->m_context.value_data != nullptr ? this->m_context : resolved;
            return LinkedTSContext{
                source.schema,
                source.value_dispatch,
                source.ts_dispatch,
                source.value_data,
                source.ts_state,
                source.owning_output != nullptr ? source.owning_output : m_owning_output,
                source.output_view_ops != nullptr ? source.output_view_ops : m_output_view_ops,
                source.notification_state != nullptr ? source.notification_state : source.ts_state,
                source.pending_dict_child,
            };
        }

        /**
         * Return the owning output endpoint for this logical output position.
         */
        [[nodiscard]] TSOutput                      *owning_output() const noexcept { return m_owning_output; }
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
         * Copy the current input state into this output using time-series
         * semantics, including collection key removal without disturbing stable
         * slots that remain live.
         */
        void copy_from_input(const TSInputView &source) const;

        /**
         * Replace this output with another output using native TS semantics.
         */
        void copy_from_output(const TSOutputView &source) const;

        /**
         * Patch this output with another output without removing absent collection
         * members.
         */
        void patch_from_output(const TSOutputView &source) const;

        /**
         * Mark the represented output position invalid without routing through the
         * Python bridge.
         */
        void invalidate() const;

        /**
         * Internal helper used by collection wrappers to preserve output-specific
         * runtime state when navigating to a child TS position.
         */
        [[nodiscard]] TSOutputView make_child_view_impl(TSViewContext context, TSViewContext parent,
                                                        engine_time_t evaluation_time) const noexcept {
            if (context.owning_output == nullptr) { context.owning_output = m_owning_output; }
            if (context.output_view_ops == nullptr) { context.output_view_ops = m_output_view_ops; }
            if (context.notification_state == nullptr) {
                const TSViewContext resolved_parent = parent.resolved();
                context.notification_state =
                    resolved_parent.notification_state != nullptr ? resolved_parent.notification_state : resolved_parent.ts_state;
            }
            return TSOutputView{std::move(context), parent, evaluation_time, m_owning_output, m_output_view_ops};
        }

      private:
        TSOutput                      *m_owning_output{nullptr};
        const detail::TSOutputViewOps *m_output_view_ops{nullptr};
    };

    struct TSSOutputView : TSSReadView<TSOutputView>
    {
        using TSSReadView<TSOutputView>::TSSReadView;
        TSSOutputView() = default;

        [[nodiscard]] LinkedTSContext     linked_context() const noexcept { return this->view_ref().linked_context(); }
        [[nodiscard]] const TSOutputView &output_view() const noexcept { return this->view_ref(); }

        [[nodiscard]] TSOutputView register_contains_output(const View &item) const;
        void                       unregister_contains_output(const View &item) const;
        [[nodiscard]] TSOutputView register_is_empty_output() const;
        void                       unregister_is_empty_output() const;
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

    namespace detail
    {

        [[nodiscard]] HGRAPH_EXPORT TSOutputView register_set_contains_output(const TSOutputView &view, const View &item);
        HGRAPH_EXPORT void                       unregister_set_contains_output(const TSOutputView &view, const View &item);
        [[nodiscard]] HGRAPH_EXPORT TSOutputView register_set_is_empty_output(const TSOutputView &view);
        HGRAPH_EXPORT void                       unregister_set_is_empty_output(const TSOutputView &view);
        HGRAPH_EXPORT void                       erase_dict_key(const TSOutputView &view, const View &key);
        HGRAPH_EXPORT void                       add_set_item(const TSOutputView &view, const View &item);
        HGRAPH_EXPORT void                       remove_set_item(const TSOutputView &view, const View &item);
        HGRAPH_EXPORT void                       clear_set_items(const TSOutputView &view);
        [[nodiscard]] HGRAPH_EXPORT TSOutputView make_missing_dict_child_output_view(const TSOutputView &view, const View &key);
        [[nodiscard]] HGRAPH_EXPORT TSOutputView register_dict_key_set_output(BaseState             &owner_state,
                                                                              const LinkedTSContext &source_context,
                                                                              engine_time_t          evaluation_time);
        [[nodiscard]] HGRAPH_EXPORT TSOutputView project_dict_key_set_output(const TSOutputView &source_view,
                                                                             engine_time_t       evaluation_time);

    }  // namespace detail

    inline MutableTSSOutputView TSOutputView::as_mutable_set() noexcept { return MutableTSSOutputView{*this}; }

    inline MutableTSSOutputView TSOutputView::as_mutable_set() const noexcept { return MutableTSSOutputView{*this}; }

    template <typename TView>
    inline void TSDView<TView>::from_python(nb::handle value) const
        requires std::same_as<TView, TSOutputView>
    {
        this->view_ref().from_python(value);
    }

    template <typename TView> inline TSSetView<TView> TSDView<TView>::key_set() const {
        if constexpr (std::same_as<TView, TSInputView>) {
            const LinkedTSContext *target = this->view_ref().linked_target();
            if (target == nullptr || !target->is_bound() || target->owning_output == nullptr) {
                throw std::logic_error("TSDView::key_set requires a bound dict input");
            }

            BaseState *feature_state = this->view_ref().context_ref().ts_state;
            if (feature_state == nullptr) { throw std::logic_error("TSDView::key_set requires a live input state"); }

            TSOutputView projected_output =
                detail::register_dict_key_set_output(*feature_state, *target, this->view_ref().evaluation_time());
            return TSSInputView{this->view_ref().make_child_view(projected_output.context_ref())};
        } else {
            LinkedTSContext source_context = this->view_ref().linked_context();
            if (!source_context.is_bound()) {
                const TSMeta *source_schema = this->view_ref().ts_schema();
                if (source_schema == nullptr || source_schema->kind != TSKind::TSD || source_schema->key_type() == nullptr) {
                    throw std::logic_error("TSDView<TSOutputView>::key_set requires a TSD source schema");
                }
                TSViewContext deferred_context;
                deferred_context.schema = TSTypeRegistry::instance().tss(source_schema->key_type());
                TSOutputView deferred_output{deferred_context, TSViewContext::none(), this->view_ref().evaluation_time(),
                                             this->view_ref().owning_output(), &detail::default_output_view_ops()};
                return TSSOutputView{std::move(deferred_output)};
            }
            TSOutputView projected_output =
                detail::project_dict_key_set_output(this->view_ref(), this->view_ref().evaluation_time());
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

    inline TSOutputView TSSOutputView::register_contains_output(const View &item) const {
        return detail::register_set_contains_output(this->view_ref(), item);
    }

    inline void TSSOutputView::unregister_contains_output(const View &item) const {
        detail::unregister_set_contains_output(this->view_ref(), item);
    }

    inline TSOutputView TSSOutputView::register_is_empty_output() const {
        return detail::register_set_is_empty_output(this->view_ref());
    }

    inline void TSSOutputView::unregister_is_empty_output() const { detail::unregister_set_is_empty_output(this->view_ref()); }

    inline void MutableTSSOutputView::from_python(nb::handle value) const { this->view_ref().from_python(value); }

    inline void MutableTSSOutputView::add(const View &item) const { detail::add_set_item(this->view_ref(), item); }

    inline void MutableTSSOutputView::remove(const View &item) const { detail::remove_set_item(this->view_ref(), item); }

    inline void MutableTSSOutputView::clear() const { detail::clear_set_items(this->view_ref()); }

}  // namespace hgraph
