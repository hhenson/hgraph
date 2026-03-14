#pragma once

#include <hgraph/hgraph_base.h>
#include <hgraph/types/time_series/time_series_state.h>
#include <hgraph/types/time_series/ts_meta.h>
#include <hgraph/types/time_series/value/view.h>

#include <string_view>
#include <type_traits>
#include <utility>

namespace hgraph
{

    struct TSInput;
    struct TSOutput;

    template <typename TView>
    struct TSView;

    template <typename TView>
    struct BaseCollectionView;

    template <typename TView>
    struct TSBView;

    template <typename TView>
    struct TSDView;

    template <typename TView>
    struct TSLView;

    template <typename TView>
    struct TSSView;

    template <typename TView>
    struct TSWView;

    template <typename TView>
    struct SignalView;

    /**
     * Identifies the endpoint object at the root of a time-series state tree.
     *
     * A time-series view ultimately resolves to either an input endpoint or
     * an output endpoint by walking the TS state parent chain.
     */
    using ParentValue = std::variant<TSInput *, TSOutput *>;

    /**
     * Reusable TS view context for a specific logical time-series position.
     *
     * This carries the resolved TS-facing metadata for one logical position:
     * - the TS schema at that position
     * - the value-layer dispatch for that position
     * - the raw value pointer for that position
     * - the raw TS state pointer representing that position
     *
     * `TSView` uses one instance for its current position and another for its
     * parent position. That keeps the current position and the parent
     * position in the same lightweight carrier while preserving the key split
     * between pure value storage and TS extension state.
     */
    struct ViewContext
    {
        [[nodiscard]] static ViewContext none() noexcept
        {
            return ViewContext{
                nullptr,
                nullptr,
                nullptr,
                nullptr};
        }

        /**
         * Materialize the value-layer view for this logical TS position.
         *
         * The context stores the raw pieces directly so TS code can carry the
         * resolved value and TS pointers explicitly. The erased `View` is
         * reconstructed on demand from those pieces.
         */
        [[nodiscard]] View value() const noexcept
        {
            const value::TypeMeta *value_schema = schema != nullptr ? schema->value_type : nullptr;
            if (dispatch == nullptr || value_data == nullptr) { return View::invalid_for(value_schema); }
            return View{dispatch, value_data, value_schema};
        }

        const TSMeta                 *schema{nullptr};
        const detail::ViewDispatch   *dispatch{nullptr};
        void                         *value_data{nullptr};
        void                        *ts_state{nullptr};
    };

    /**
     * Lightweight time-series view over combined value and TS storage.
     *
     * This mirrors the shape of the new value-layer `View`: a TS view is a
     * non-owning wrapper over schema-resolved behavior and raw storage.
     *
     * The owning `TSValue` keeps one combined allocation. The TS-facing view
     * keeps:
     * - a `ViewContext` for the current logical TS position
     * - a `ViewContext` for the parent logical TS position
     *
     * That preserves the raw split between:
     * - the value-layer dispatch and value pointer
     * - the TS state pointer
     *
     * The raw TS region remains encapsulated inside `TSValue` until the TS
     * extension storage moves from the prototype `TimeSeriesStateV` model to
     * its final raw-layout form.
     *
     * `REF[...]` is treated as `TS[TimeSeriesReference]`, so reference
     * semantics remain part of this generic TS view contract rather than a
     * separate view family.
     */
    template <typename TView>
    struct TSView
    {
        TSView() = default;

        TSView(ViewContext context,
               ViewContext parent = ViewContext::none(),
               engine_time_t evaluation_time = MIN_DT) noexcept
            : m_context(context), m_parent(parent), m_evaluation_time(evaluation_time)
        {
        }

        /**
         * Return the engine time at which this view is being evaluated.
         *
         * When a view is created outside of an evaluation context, this stays
         * at `MIN_DT`. That deliberately means `modified()` cannot report a
         * current-cycle modification until the runtime wires in the real
         * evaluation time.
         */
        [[nodiscard]] engine_time_t evaluation_time() const noexcept { return m_evaluation_time; }

        /**
         * Return the current point-in-time value for this view.
         */
        [[nodiscard]] View value() const noexcept { return m_context.value(); }

        /**
         * Return the current delta value for this view.
         *
         * Collection-specific delta surfaces are still exposed through
         * `TSValue`-side helpers. Until TS collection view navigation is
         * completed, the generic TS view returns its current value surface
         * here.
         */
        [[nodiscard]] View delta_value() const noexcept { return value(); }

        /**
         * Return whether this view was modified in the current engine cycle.
         */
        [[nodiscard]] bool modified() const noexcept
        {
            return m_evaluation_time != MIN_DT && last_modified_time() == m_evaluation_time;
        }

        /**
         * Return whether this view currently holds a value.
         *
         * TS validity follows the sentinel carried by the TS extension region:
         * `MIN_DT` means the logical time-series position has no value.
         */
        [[nodiscard]] bool valid() const noexcept { return last_modified_time() != MIN_DT; }

        /**
         * Return whether this view and its required descendants are valid.
         *
         * The collection-specific recursive validity rules will be layered on
         * later. For the current root-oriented integration step, this uses the
         * same sentinel rule as `valid()`.
         */
        [[nodiscard]] bool all_valid() const noexcept { return valid(); }

        /**
         * Return the last modification time associated to this view.
         */
        [[nodiscard]] engine_time_t last_modified_time() const noexcept
        {
            if (const BaseState *state = base_state_from(m_context.ts_state, m_context.schema); state != nullptr) {
                return state->last_modified_time;
            }
            return MIN_DT;
        }

        /**
         * Interpret this view as a bundle view when the runtime kind matches.
         */
        [[nodiscard]] TSBView<TView> as_bundle() noexcept { return TSBView<TView>{*this}; }

        /**
         * Interpret this view as a bundle view when the runtime kind matches.
         */
        [[nodiscard]] TSBView<TView> as_bundle() const noexcept { return TSBView<TView>{*this}; }

        /**
         * Interpret this view as a list view when the runtime kind matches.
         */
        [[nodiscard]] TSLView<TView> as_list() noexcept { return TSLView<TView>{*this}; }

        /**
         * Interpret this view as a list view when the runtime kind matches.
         */
        [[nodiscard]] TSLView<TView> as_list() const noexcept { return TSLView<TView>{*this}; }

        /**
         * Interpret this view as a dictionary view when the runtime kind matches.
         */
        [[nodiscard]] TSDView<TView> as_dict() noexcept { return TSDView<TView>{*this}; }

        /**
         * Interpret this view as a dictionary view when the runtime kind matches.
         */
        [[nodiscard]] TSDView<TView> as_dict() const noexcept { return TSDView<TView>{*this}; }

        /**
         * Interpret this view as a set view when the runtime kind matches.
         */
        [[nodiscard]] TSSView<TView> as_set() noexcept { return TSSView<TView>{*this}; }

        /**
         * Interpret this view as a set view when the runtime kind matches.
         */
        [[nodiscard]] TSSView<TView> as_set() const noexcept { return TSSView<TView>{*this}; }

        /**
         * Interpret this view as a window view when the runtime kind matches.
         */
        [[nodiscard]] TSWView<TView> as_window() noexcept { return TSWView<TView>{*this}; }

        /**
         * Interpret this view as a window view when the runtime kind matches.
         */
        [[nodiscard]] TSWView<TView> as_window() const noexcept { return TSWView<TView>{*this}; }

        /**
         * Interpret this view as a signal view when the runtime kind matches.
         */
        [[nodiscard]] SignalView<TView> as_signal() noexcept { return SignalView<TView>{*this}; }

        /**
         * Interpret this view as a signal view when the runtime kind matches.
         */
        [[nodiscard]] SignalView<TView> as_signal() const noexcept { return SignalView<TView>{*this}; }

    protected:
        /**
         * Return the endpoint at the root of this view's TS state tree.
         */
        [[nodiscard]] ParentValue parent() const noexcept
        {
            const BaseState *state = base_state_from(m_context.ts_state, m_context.schema);

            while (state != nullptr) {
                ParentValue resolved{};
                bool        found = false;

                std::visit(
                    [&state, &resolved, &found](auto *ptr) {
                        using T = std::remove_pointer_t<decltype(ptr)>;

                        if (ptr == nullptr) {
                            state = nullptr;
                            return;
                        }

                        if constexpr (std::is_same_v<T, TSInput> || std::is_same_v<T, TSOutput>) {
                            resolved = ptr;
                            found = true;
                            state = nullptr;
                        } else {
                            state = ptr;
                        }
                    },
                    state->parent);

                if (found) { return resolved; }
            }

            return static_cast<TSInput *>(nullptr);
        }

        /**
         * Return the state node associated to the represented time-series
         * position.
         */
        [[nodiscard]] void *ts_state() const noexcept { return m_context.ts_state; }

        /**
         * Return the logical TS schema represented by this view.
         */
        [[nodiscard]] const TSMeta *schema() const noexcept { return m_context.schema; }
        [[nodiscard]] ViewContext parent_context() const noexcept { return m_parent; }

        ViewContext m_context{ViewContext::none()};
        ViewContext m_parent{ViewContext::none()};
        engine_time_t m_evaluation_time{MIN_DT};

      private:
        [[nodiscard]] static const BaseState *base_state_from(const void *ts_state, const TSMeta *schema) noexcept
        {
            if (ts_state == nullptr || schema == nullptr) { return nullptr; }

            switch (schema->kind) {
                case TSKind::TSValue: return static_cast<const TSState *>(ts_state);
                case TSKind::TSS: return static_cast<const TSSState *>(ts_state);
                case TSKind::TSD: return static_cast<const TSDState *>(ts_state);
                case TSKind::TSL: return static_cast<const TSLState *>(ts_state);
                case TSKind::TSW: return static_cast<const TSWState *>(ts_state);
                case TSKind::TSB: return static_cast<const TSBState *>(ts_state);
                case TSKind::REF: return static_cast<const RefLinkState *>(ts_state);
                case TSKind::SIGNAL: return static_cast<const SignalState *>(ts_state);
            }

            return nullptr;
        }
    };

    /**
     * Base for collection-oriented time-series views.
     */
    template <typename TView>
    struct BaseCollectionView : TSView<TView>
    {
        using TSView<TView>::TSView;
        BaseCollectionView() = default;
        BaseCollectionView(const TSView<TView> &other) noexcept : TSView<TView>(other) {}

        [[nodiscard]] size_t size() const noexcept;
        [[nodiscard]] TView at(size_t index) const noexcept;
        [[nodiscard]] TView operator[](size_t index) const noexcept;
    };

    template <typename TView>
    struct TSLView : BaseCollectionView<TView>
    {
        using BaseCollectionView<TView>::BaseCollectionView;

        [[nodiscard]] Range<TView> values() const noexcept;
        [[nodiscard]] Range<TView> valid_values() const noexcept;
        [[nodiscard]] Range<TView> modified_values() const noexcept;
        [[nodiscard]] KeyValueRange<size_t, TView> items() const noexcept;
        [[nodiscard]] KeyValueRange<size_t, TView> valid_items() const noexcept;
        [[nodiscard]] KeyValueRange<size_t, TView> modified_items() const noexcept;
    };

    template <typename TView>
    struct TSBView : BaseCollectionView<TView>
    {
        using BaseCollectionView<TView>::BaseCollectionView;

        [[nodiscard]] TView field(std::string_view name) const noexcept;
        [[nodiscard]] Range<std::string_view> keys() const noexcept;
        [[nodiscard]] Range<TView> values() const noexcept;
        [[nodiscard]] KeyValueRange<std::string_view, TView> items() const noexcept;
        [[nodiscard]] KeyValueRange<std::string_view, TView> valid_items() const noexcept;
        [[nodiscard]] KeyValueRange<std::string_view, TView> modified_items() const noexcept;
    };

    template <typename TView>
    struct TSDView : BaseCollectionView<TView>
    {
        using BaseCollectionView<TView>::BaseCollectionView;
        using BaseCollectionView<TView>::at;
        using BaseCollectionView<TView>::operator[];

        [[nodiscard]] TView at(const View &key) const noexcept;
        [[nodiscard]] TView operator[](const View &key) const noexcept;
        [[nodiscard]] Range<View> keys() const noexcept;
        [[nodiscard]] Range<TView> values() const noexcept;
        [[nodiscard]] KeyValueRange<View, TView> items() const noexcept;
        [[nodiscard]] KeyValueRange<View, TView> valid_items() const noexcept;
        [[nodiscard]] KeyValueRange<View, TView> modified_items() const noexcept;
    };

    template <typename TView>
    struct TSSView : TSView<TView>
    {
        using TSView<TView>::TSView;
        TSSView() = default;
        TSSView(const TSView<TView> &other) noexcept : TSView<TView>(other) {}

        [[nodiscard]] size_t size() const noexcept;
        [[nodiscard]] Range<View> values() const noexcept;
        [[nodiscard]] Range<View> added_values() const noexcept;
        [[nodiscard]] Range<View> removed_values() const noexcept;
        [[nodiscard]] TView is_empty() const noexcept;
    };

    template <typename TView>
    struct TSWView : TSView<TView>
    {
        using TSView<TView>::TSView;
        TSWView() = default;
        TSWView(const TSView<TView> &other) noexcept : TSView<TView>(other) {}

        [[nodiscard]] size_t size() const noexcept;
        [[nodiscard]] Range<View> values() const noexcept;
        [[nodiscard]] Range<engine_time_t> value_times() const noexcept;
    };

    template <typename TView>
    struct SignalView : TSView<TView>
    {
        using TSView<TView>::TSView;
        SignalView() = default;
        SignalView(const TSView<TView> &other) noexcept : TSView<TView>(other) {}
    };

}  // namespace hgraph
