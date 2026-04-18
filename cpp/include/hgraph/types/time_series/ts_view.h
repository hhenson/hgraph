#pragma once

#include <hgraph/hgraph_base.h>
#include <hgraph/types/time_series/time_series_state.h>
#include <hgraph/types/time_series/ts_meta.h>
#include <hgraph/types/time_series/value/value.h>
#include <hgraph/types/time_series/value/view.h>

#include <string_view>
#include <type_traits>
#include <utility>

namespace hgraph
{

    struct TSInput;
    struct TSOutput;
    struct TSInputView;
    struct TSOutputView;

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
    struct TSSReadView;

    struct TSSInputView;
    struct TSSOutputView;
    struct MutableTSSOutputView;

    template <typename TView>
    struct TSWView;

    template <typename TView>
    struct SignalView;

    struct TSViewContext;

    template <typename TView>
    struct TSSetViewType;

    template <>
    struct TSSetViewType<TSInputView>
    {
        using type = TSSInputView;
    };

    template <>
    struct TSSetViewType<TSOutputView>
    {
        using type = TSSOutputView;
    };

    template <typename TView>
    using TSSetView = typename TSSetViewType<TView>::type;

    namespace detail
    {
        struct TSCollectionDispatch;
        struct TSFieldDispatch;
        struct TSKeyDispatch;
        struct TSSetDispatch;
        struct TSWindowDispatch;
        struct TSInputViewOps;
        struct TSOutputViewOps;

        /**
         * Behavior-only dispatch over a logical TS position.
         *
         * The dispatch mirrors the value-layer builder pattern: it caches the
         * schema-expanded behavior for one time-series schema and uses those
         * cached facts to resolve TS operations from raw value and TS-state
         * pointers. Navigation is intentionally factored into collection-only
         * refinements so non-collection schemas do not implement child lookup
         * semantics they can never support.
         */
        struct TSDispatch
        {
            virtual ~TSDispatch() = default;
            [[nodiscard]] virtual nb::object to_python(const TSViewContext &context, engine_time_t evaluation_time) const;
            [[nodiscard]] virtual nb::object delta_to_python(const TSViewContext &context,
                                                             engine_time_t evaluation_time) const;
            [[nodiscard]] virtual bool can_apply_result(const TSOutputView &view, nb::handle value) const;
            virtual void apply_result(const TSOutputView &view, nb::handle value) const;
            virtual void from_python(const TSOutputView &view, nb::handle value) const;
            virtual void clear(const TSOutputView &view) const;
            [[nodiscard]] virtual View delta_value(const TSViewContext &context) const noexcept;
            [[nodiscard]] virtual engine_time_t last_modified_time(const TSViewContext &context) const noexcept;
            [[nodiscard]] virtual bool valid(const TSViewContext &context) const noexcept;
            [[nodiscard]] virtual bool all_valid(const TSViewContext &context) const noexcept;
            [[nodiscard]] virtual const TSCollectionDispatch *as_collection() const noexcept { return nullptr; }
            [[nodiscard]] virtual const TSSetDispatch *as_set() const noexcept { return nullptr; }
            [[nodiscard]] virtual const TSWindowDispatch *as_window() const noexcept { return nullptr; }
        };

        /**
         * Navigation dispatch for collection-like time-series schemas.
         */
        struct TSCollectionDispatch : TSDispatch
        {
            [[nodiscard]] const TSCollectionDispatch *as_collection() const noexcept override { return this; }
            [[nodiscard]] virtual const TSFieldDispatch *as_fields() const noexcept { return nullptr; }
            [[nodiscard]] virtual const TSKeyDispatch *as_keys() const noexcept { return nullptr; }
            [[nodiscard]] virtual size_t size(const TSViewContext &context) const noexcept                           = 0;
            [[nodiscard]] virtual bool child_modified(const TSViewContext &context, size_t index) const noexcept    = 0;
            /**
             * Resolve a child by schema-defined slot.
             *
             * For positional collections the slot is the logical index. For
             * slot-backed collections such as maps it is the backing storage
             * slot rather than the nth live element.
             */
            [[nodiscard]] virtual TSViewContext child_at(const TSViewContext &context, size_t index) const = 0;
        };

        /**
         * Navigation dispatch for named child collections such as bundles.
         */
        struct TSFieldDispatch : TSCollectionDispatch
        {
            [[nodiscard]] const TSFieldDispatch *as_fields() const noexcept override { return this; }
            [[nodiscard]] virtual TSViewContext child_field(const TSViewContext &context,
                                                            std::string_view name) const = 0;
            [[nodiscard]] virtual std::string_view child_name(size_t index) const noexcept                           = 0;
        };

        /**
         * Navigation dispatch for keyed child collections such as dicts.
         */
        struct TSKeyDispatch : TSCollectionDispatch
        {
            [[nodiscard]] const TSKeyDispatch *as_keys() const noexcept override { return this; }
            [[nodiscard]] virtual TSViewContext child_key(const TSViewContext &context, const View &key) const = 0;
            [[nodiscard]] virtual size_t iteration_limit(const TSViewContext &context) const noexcept = 0;
            [[nodiscard]] virtual bool slot_is_live(const TSViewContext &context, size_t slot) const noexcept = 0;
            [[nodiscard]] virtual View key_at_slot(const TSViewContext &context, size_t slot) const = 0;
            virtual void child_from_python(const TSOutputView &view, const View &key, nb::handle value) const;
        };

        struct TSSetDispatch : TSDispatch
        {
            [[nodiscard]] const TSSetDispatch *as_set() const noexcept override { return this; }
            [[nodiscard]] virtual size_t size(const TSViewContext &context) const noexcept = 0;
            [[nodiscard]] virtual bool empty(const TSViewContext &context) const noexcept = 0;
            [[nodiscard]] virtual Range<View> values(const TSViewContext &context) const noexcept = 0;
            [[nodiscard]] virtual Range<View> added_values(const TSViewContext &context) const noexcept = 0;
            [[nodiscard]] virtual Range<View> removed_values(const TSViewContext &context) const noexcept = 0;
        };

        struct TSWindowDispatch : TSDispatch
        {
            [[nodiscard]] const TSWindowDispatch *as_window() const noexcept override { return this; }
            [[nodiscard]] virtual size_t size(const TSViewContext &context, engine_time_t evaluation_time) const noexcept = 0;
            [[nodiscard]] virtual Range<View> values(const TSViewContext &context, engine_time_t evaluation_time) const noexcept = 0;
            [[nodiscard]] virtual Range<engine_time_t> value_times(const TSViewContext &context,
                                                                   engine_time_t       evaluation_time) const noexcept = 0;
        };

        struct TSInputViewOps
        {
            virtual ~TSInputViewOps() = default;
            virtual void bind_output(TSInputView &view, const TSOutputView &output) const = 0;
            virtual void unbind_output(TSInputView &view) const = 0;
            virtual void make_active(TSInputView &view) const = 0;
            virtual void make_passive(TSInputView &view) const = 0;
            [[nodiscard]] virtual bool active(const TSInputView &view) const noexcept = 0;
        };

        struct TSOutputViewOps
        {
            virtual ~TSOutputViewOps() = default;
            [[nodiscard]] virtual LinkedTSContext linked_context(const TSOutputView &view) const noexcept = 0;
            virtual bool from_python(const TSOutputView &view, nb::handle value) const
            {
                static_cast<void>(view);
                static_cast<void>(value);
                return false;
            }
        };

        [[nodiscard]] HGRAPH_EXPORT const TSInputViewOps &default_input_view_ops() noexcept;
        [[nodiscard]] HGRAPH_EXPORT const TSOutputViewOps &default_output_view_ops() noexcept;
        [[nodiscard]] HGRAPH_EXPORT TSOutputView make_missing_dict_child_output_view(const TSOutputView &view, const View &key);
        [[nodiscard]] HGRAPH_EXPORT TSOutputView ensure_dict_child_output_view(const TSOutputView &view, const View &key);
        [[nodiscard]] HGRAPH_EXPORT nb::object to_python(const TSViewContext &context, engine_time_t evaluation_time);
        [[nodiscard]] HGRAPH_EXPORT nb::object delta_to_python(const TSViewContext &context,
                                                               engine_time_t evaluation_time);
    }  // namespace detail

    /**
     * View-specialized TS context for a specific logical time-series position.
     *
     * `TSViewContext` extends the shared raw `TSContext` carrier with the
     * view-only helpers needed to resolve link-backed storage and materialize
     * the corresponding erased value view.
     */
    struct TSViewContext : TSContext
    {
        TSViewContext() noexcept = default;

        TSViewContext(const TSMeta *schema_,
                      const detail::ViewDispatch *value_dispatch_,
                      const detail::TSDispatch *ts_dispatch_,
                      void *value_data_,
                      BaseState *ts_state_) noexcept
            : TSContext(schema_, value_dispatch_, ts_dispatch_, value_data_, ts_state_)
        {
        }

        TSViewContext(const TSContext &context) noexcept
            : TSContext(context)
        {
        }

        [[nodiscard]] static TSViewContext none() noexcept { return {}; }

        /**
         * Materialize the value-layer view for this logical TS position.
         *
         * The context stores the raw pieces directly so TS code can carry the
         * resolved value and TS pointers explicitly. The erased `View` is
         * reconstructed on demand from those pieces.
         */
        [[nodiscard]] TSViewContext resolved() const noexcept
        {
            TSViewContext resolved_context = *this;
            const auto *state = ts_state;
            if (state == nullptr) { return resolved_context; }

            const auto apply_target = [&resolved_context](LinkedTSContext target) noexcept {
                if (!target.is_bound()) {
                    resolved_context.value_data = nullptr;
                    return;
                }

                // Link-backed positions can chain: a REF->TS alternative is a
                // local RefLinkState whose visible target may itself be another
                // link-backed state. Follow that chain until we reach the live
                // represented position or discover that one of the links is
                // currently unbound.
                while (target.ts_state != nullptr) {
                    const LinkedTSContext *nested = target.ts_state->linked_target();
                    if (nested == nullptr) { break; }

                    if (!nested->is_bound()) {
                        resolved_context.schema = target.schema != nullptr ? target.schema : resolved_context.schema;
                        resolved_context.value_dispatch =
                            target.value_dispatch != nullptr ? target.value_dispatch : resolved_context.value_dispatch;
                        resolved_context.ts_dispatch = target.ts_dispatch != nullptr ? target.ts_dispatch : resolved_context.ts_dispatch;
                        resolved_context.value_data = nullptr;
                        return;
                    }

                    target = *nested;
                }

                TSViewContext target_context{
                    target.schema != nullptr ? target.schema : resolved_context.schema,
                    target.value_dispatch != nullptr ? target.value_dispatch : resolved_context.value_dispatch,
                    target.ts_dispatch != nullptr ? target.ts_dispatch : resolved_context.ts_dispatch,
                    target.value_data,
                    target.ts_state,
                };

                resolved_context.schema = target_context.schema;
                resolved_context.value_dispatch = target_context.value_dispatch;
                resolved_context.ts_dispatch = target_context.ts_dispatch;
                resolved_context.value_data = target_context.value_data;
                resolved_context.ts_state = target_context.ts_state;
                resolved_context.owning_output = target.owning_output != nullptr ? target.owning_output : resolved_context.owning_output;
                resolved_context.output_view_ops = target.output_view_ops != nullptr ? target.output_view_ops : resolved_context.output_view_ops;
                resolved_context.notification_state =
                    target.notification_state != nullptr ? target.notification_state : resolved_context.notification_state;
            };

            if (const LinkedTSContext *target = state->linked_target(); target != nullptr) { apply_target(*target); }

            return detail::refresh_native_context(resolved_context);
        }

        [[nodiscard]] View value() const noexcept
        {
            const TSViewContext resolved_context = resolved();
            const value::TypeMeta *value_schema = resolved_context.schema != nullptr ? resolved_context.schema->value_type : nullptr;
            if (const Value *materialized = detail::materialized_target_link_value(resolved_context); materialized != nullptr) {
                return materialized->view();
            }
            if (const Value *materialized = detail::materialized_reference_value(resolved_context); materialized != nullptr) {
                return materialized->view();
            }
            if (resolved_context.value_dispatch == nullptr || resolved_context.value_data == nullptr) {
                return View::invalid_for(value_schema);
            }
            return View{resolved_context.value_dispatch, resolved_context.value_data, value_schema};
        }

    };

    /**
     * Lightweight time-series view over combined value and TS storage.
     *
     * This mirrors the shape of the new value-layer `View`: a TS view is a
     * non-owning wrapper over schema-resolved behavior and raw storage.
     *
     * The owning `TSValue` keeps one combined allocation. The TS-facing view
     * keeps:
     * - a `TSViewContext` for the current logical TS position
     * - a `TSViewContext` for the parent logical TS position
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

        TSView(TSViewContext context,
               TSViewContext parent = TSViewContext::none(),
               engine_time_t evaluation_time = MIN_DT) noexcept
            : m_context(context),
              m_parent(parent),
              m_evaluation_time(evaluation_time)
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
         * Materialize the current TS value using the native Python-facing TS
         * semantics for this schema.
         */
        [[nodiscard]] nb::object to_python() const
        {
            return detail::to_python(m_context, m_evaluation_time);
        }

        /**
         * Return the logical TS schema represented by this view.
         */
        [[nodiscard]] const TSMeta *ts_schema() const noexcept { return m_context.resolved().schema; }

        /**
         * Return the bound target for the represented link-backed position.
         *
         * Native storage positions return `nullptr`.
         */
        [[nodiscard]] const LinkedTSContext *linked_target() const noexcept
        {
            return m_context.ts_state != nullptr ? m_context.ts_state->linked_target() : nullptr;
        }

        /**
         * Return the current delta value for this view.
         */
        [[nodiscard]] View delta_value() const noexcept
        {
            const auto *dispatch = ts_dispatch();
            const auto *resolved_schema = schema();
            const auto *value_schema = resolved_schema != nullptr ? resolved_schema->value_type : nullptr;
            return dispatch != nullptr ? dispatch->delta_value(m_context) : View::invalid_for(value_schema);
        }

        /**
         * Materialize the current TS delta using the native Python-facing TS
         * semantics for this schema.
         */
        [[nodiscard]] nb::object delta_to_python() const
        {
            return detail::delta_to_python(m_context, m_evaluation_time);
        }

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
        [[nodiscard]] bool valid() const noexcept
        {
            const auto *dispatch = ts_dispatch();
            return dispatch != nullptr ? dispatch->valid(m_context) : false;
        }

        /**
         * Return whether this view and its required descendants are valid.
         *
         * The collection-specific recursive validity rules are implemented by
         * the collection view refinements. Leaf views follow the same sentinel
         * rule as `valid()`.
         */
        [[nodiscard]] bool all_valid() const noexcept
        {
            const auto *dispatch = ts_dispatch();
            return dispatch != nullptr ? dispatch->all_valid(m_context) : false;
        }

        /**
         * Return the last modification time associated to this view.
         */
        [[nodiscard]] engine_time_t last_modified_time() const noexcept
        {
            const auto *dispatch = ts_dispatch();
            return dispatch != nullptr ? dispatch->last_modified_time(m_context) : MIN_DT;
        }

        /**
         * Interpret this view as a bundle view when the runtime kind matches.
         */
        [[nodiscard]] TSBView<TView> as_bundle() noexcept { return TSBView<TView>{*static_cast<TView *>(this)}; }

        /**
         * Interpret this view as a bundle view when the runtime kind matches.
         */
        [[nodiscard]] TSBView<TView> as_bundle() const noexcept { return TSBView<TView>{*static_cast<const TView *>(this)}; }

        /**
         * Interpret this view as a list view when the runtime kind matches.
         */
        [[nodiscard]] TSLView<TView> as_list() noexcept { return TSLView<TView>{*static_cast<TView *>(this)}; }

        /**
         * Interpret this view as a list view when the runtime kind matches.
         */
        [[nodiscard]] TSLView<TView> as_list() const noexcept { return TSLView<TView>{*static_cast<const TView *>(this)}; }

        /**
         * Interpret this view as a dictionary view when the runtime kind matches.
         */
        [[nodiscard]] TSDView<TView> as_dict() noexcept { return TSDView<TView>{*static_cast<TView *>(this)}; }

        /**
         * Interpret this view as a dictionary view when the runtime kind matches.
         */
        [[nodiscard]] TSDView<TView> as_dict() const noexcept { return TSDView<TView>{*static_cast<const TView *>(this)}; }

        /**
         * Interpret this view as a set view when the runtime kind matches.
         */
        [[nodiscard]] TSSetView<TView> as_set() noexcept
        {
            if constexpr (std::same_as<TView, TSInputView>) {
                return TSSInputView{*static_cast<TView *>(this)};
            } else {
                return TSSOutputView{*static_cast<TView *>(this)};
            }
        }

        /**
         * Interpret this view as a set view when the runtime kind matches.
         */
        [[nodiscard]] TSSetView<TView> as_set() const noexcept
        {
            if constexpr (std::same_as<TView, TSInputView>) {
                return TSSInputView{*static_cast<const TView *>(this)};
            } else {
                return TSSOutputView{*static_cast<const TView *>(this)};
            }
        }

        /**
         * Interpret this view as a window view when the runtime kind matches.
         */
        [[nodiscard]] TSWView<TView> as_window() noexcept { return TSWView<TView>{*static_cast<TView *>(this)}; }

        /**
         * Interpret this view as a window view when the runtime kind matches.
         */
        [[nodiscard]] TSWView<TView> as_window() const noexcept { return TSWView<TView>{*static_cast<const TView *>(this)}; }

        /**
         * Interpret this view as a signal view when the runtime kind matches.
         */
        [[nodiscard]] SignalView<TView> as_signal() noexcept { return SignalView<TView>{*static_cast<TView *>(this)}; }

        /**
         * Interpret this view as a signal view when the runtime kind matches.
         */
        [[nodiscard]] SignalView<TView> as_signal() const noexcept { return SignalView<TView>{*static_cast<const TView *>(this)}; }

        [[nodiscard]] TView make_child_view(TSViewContext child_context) const
        {
            return static_cast<const TView *>(this)->make_child_view_impl(std::move(child_context), m_context, m_evaluation_time);
        }

        [[nodiscard]] TSViewContext &context_mutable() noexcept { return m_context; }
        [[nodiscard]] const TSViewContext &context_ref() const noexcept { return m_context; }
        [[nodiscard]] const TSViewContext &parent_context_ref() const noexcept { return m_parent; }

    protected:

        /**
         * Return the state node associated to the represented time-series
         * position.
         */
        [[nodiscard]] BaseState *ts_state() const noexcept { return m_context.ts_state; }

        /**
         * Return the logical TS schema represented by this view.
         */
        [[nodiscard]] const TSMeta *schema() const noexcept { return m_context.resolved().schema; }
        [[nodiscard]] const detail::TSDispatch *ts_dispatch() const noexcept { return m_context.resolved().ts_dispatch; }

        TSViewContext m_context{TSViewContext::none()};
        TSViewContext m_parent{TSViewContext::none()};
        engine_time_t m_evaluation_time{MIN_DT};
    };

    template <typename TView>
    struct TSViewFacade
    {
        TSViewFacade() = default;
        explicit TSViewFacade(const TView &view) noexcept : m_view(view) {}
        explicit TSViewFacade(TView &&view) noexcept : m_view(std::move(view)) {}

        [[nodiscard]] engine_time_t evaluation_time() const noexcept { return m_view.evaluation_time(); }
        [[nodiscard]] View value() const noexcept { return m_view.value(); }
        [[nodiscard]] nb::object to_python() const { return m_view.to_python(); }
        [[nodiscard]] const TSMeta *ts_schema() const noexcept { return m_view.ts_schema(); }
        [[nodiscard]] const LinkedTSContext *linked_target() const noexcept { return m_view.linked_target(); }
        [[nodiscard]] View delta_value() const noexcept { return m_view.delta_value(); }
        [[nodiscard]] nb::object delta_to_python() const { return m_view.delta_to_python(); }
        [[nodiscard]] bool modified() const noexcept { return m_view.modified(); }
        [[nodiscard]] bool valid() const noexcept { return m_view.valid(); }
        [[nodiscard]] bool all_valid() const noexcept { return m_view.all_valid(); }
        [[nodiscard]] engine_time_t last_modified_time() const noexcept { return m_view.last_modified_time(); }
        [[nodiscard]] TSBView<TView> as_bundle() noexcept { return m_view.as_bundle(); }
        [[nodiscard]] TSBView<TView> as_bundle() const noexcept { return m_view.as_bundle(); }
        [[nodiscard]] TSLView<TView> as_list() noexcept { return m_view.as_list(); }
        [[nodiscard]] TSLView<TView> as_list() const noexcept { return m_view.as_list(); }
        [[nodiscard]] TSDView<TView> as_dict() noexcept { return m_view.as_dict(); }
        [[nodiscard]] TSDView<TView> as_dict() const noexcept { return m_view.as_dict(); }
        [[nodiscard]] TSSetView<TView> as_set() noexcept { return m_view.as_set(); }
        [[nodiscard]] TSSetView<TView> as_set() const noexcept { return m_view.as_set(); }
        [[nodiscard]] TSWView<TView> as_window() noexcept { return m_view.as_window(); }
        [[nodiscard]] TSWView<TView> as_window() const noexcept { return m_view.as_window(); }
        [[nodiscard]] SignalView<TView> as_signal() noexcept { return m_view.as_signal(); }
        [[nodiscard]] SignalView<TView> as_signal() const noexcept { return m_view.as_signal(); }
        [[nodiscard]] TView &view() noexcept { return m_view; }
        [[nodiscard]] const TView &view() const noexcept { return m_view; }

      protected:
        [[nodiscard]] TView &view_mutable() noexcept { return m_view; }
        [[nodiscard]] const TView &view_ref() const noexcept { return m_view; }

      private:
        TView m_view{};
    };

    /**
     * Base for collection-oriented time-series views.
     */
    template <typename TView>
    struct BaseCollectionView : TSViewFacade<TView>
    {
        using TSViewFacade<TView>::TSViewFacade;
        BaseCollectionView() = default;

        [[nodiscard]] size_t size() const noexcept;
        [[nodiscard]] TView at(size_t index) const;
        [[nodiscard]] TView operator[](size_t index) const;

      protected:
        [[nodiscard]] const detail::TSCollectionDispatch *collection_dispatch() const noexcept;
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

        [[nodiscard]] TView field(std::string_view name) const;
        [[nodiscard]] Range<std::string_view> keys() const noexcept;
        [[nodiscard]] Range<TView> values() const noexcept;
        [[nodiscard]] KeyValueRange<std::string_view, TView> items() const noexcept;
        [[nodiscard]] KeyValueRange<std::string_view, TView> valid_items() const noexcept;
        [[nodiscard]] KeyValueRange<std::string_view, TView> modified_items() const noexcept;

      protected:
        [[nodiscard]] const detail::TSFieldDispatch *field_dispatch() const noexcept;
    };

    template <typename TView>
    struct TSDView : BaseCollectionView<TView>
    {
        using BaseCollectionView<TView>::BaseCollectionView;
        using BaseCollectionView<TView>::at;
        using BaseCollectionView<TView>::operator[];

        [[nodiscard]] TView at(const View &key) const;
        [[nodiscard]] TView operator[](const View &key) const;
        [[nodiscard]] Range<View> keys() const noexcept;
        [[nodiscard]] Range<TView> values() const noexcept;
        [[nodiscard]] KeyValueRange<View, TView> items() const noexcept;
        [[nodiscard]] KeyValueRange<View, TView> valid_items() const noexcept;
        [[nodiscard]] KeyValueRange<View, TView> modified_items() const noexcept;
        [[nodiscard]] TSSetView<TView> key_set() const;
        void from_python(nb::handle value) const requires std::same_as<TView, TSOutputView>;
        void from_python(const View &key, nb::handle value) const requires std::same_as<TView, TSOutputView>;
        void erase(const View &key) const requires std::same_as<TView, TSOutputView>;

      protected:
        [[nodiscard]] const detail::TSKeyDispatch *key_dispatch() const noexcept;
    };

    template <typename TView>
    struct TSSReadView : TSViewFacade<TView>
    {
        using TSViewFacade<TView>::TSViewFacade;
        TSSReadView() = default;

        [[nodiscard]] size_t size() const noexcept;
        [[nodiscard]] Range<View> values() const noexcept;
        [[nodiscard]] Range<View> added_values() const noexcept;
        [[nodiscard]] Range<View> removed_values() const noexcept;
        [[nodiscard]] bool empty() const noexcept;

      protected:
        [[nodiscard]] const detail::TSSetDispatch *set_dispatch() const noexcept;
    };

    template <typename TView>
    struct TSWView : TSViewFacade<TView>
    {
        using TSViewFacade<TView>::TSViewFacade;
        TSWView() = default;

        [[nodiscard]] size_t size() const noexcept;
        [[nodiscard]] Range<View> values() const noexcept;
        [[nodiscard]] Range<engine_time_t> value_times() const noexcept;
    };

    template <typename TView>
    struct SignalView : TSViewFacade<TView>
    {
        using TSViewFacade<TView>::TSViewFacade;
        SignalView() = default;
    };

    namespace detail
    {
        [[nodiscard]] inline const BaseCollectionState *collection_state(const BaseState *state) noexcept
        {
            return static_cast<const BaseCollectionState *>(state);
        }

        [[nodiscard]] inline bool dict_slot_is_live(const MapDeltaView &delta, size_t slot) noexcept
        {
            return delta.slot_occupied(slot) && !delta.slot_removed(slot);
        }

    }  // namespace detail

    inline View detail::TSDispatch::delta_value(const TSViewContext &context) const noexcept
    {
        return context.value();
    }

    inline engine_time_t detail::TSDispatch::last_modified_time(const TSViewContext &context) const noexcept
    {
        return context.ts_state != nullptr ? context.ts_state->last_modified_time : MIN_DT;
    }

    inline bool detail::TSDispatch::valid(const TSViewContext &context) const noexcept
    {
        if (context.schema != nullptr && context.schema->kind != TSKind::REF && context.ts_state != nullptr) {
            if (detail::materialized_target_link_value(context) != nullptr) {
                return last_modified_time(context) != MIN_DT && context.value().has_value();
            }

            if (const LinkedTSContext *target = context.ts_state->linked_target(); target != nullptr) {
                return detail::linked_context_valid(*target);
            }
        }

        return last_modified_time(context) != MIN_DT && context.value().has_value();
    }

    inline bool detail::TSDispatch::all_valid(const TSViewContext &context) const noexcept
    {
        if (context.schema != nullptr && context.schema->kind != TSKind::REF && context.ts_state != nullptr) {
            if (detail::materialized_target_link_value(context) != nullptr) { return valid(context); }

            if (const LinkedTSContext *target = context.ts_state->linked_target(); target != nullptr) {
                return detail::linked_context_all_valid(*target);
            }
        }

        return valid(context);
    }

    template <typename TView>
    size_t BaseCollectionView<TView>::size() const noexcept
    {
        const auto *dispatch = collection_dispatch();
        return dispatch != nullptr ? dispatch->size(this->view_ref().context_ref()) : 0;
    }

    template <typename TView>
    TView BaseCollectionView<TView>::at(size_t index) const
    {
        const auto *dispatch = collection_dispatch();
        if (dispatch == nullptr) { return this->view_ref().make_child_view(TSViewContext::none()); }

        const TSViewContext child = dispatch->child_at(this->view_ref().context_ref(), index);
        return this->view_ref().make_child_view(child);
    }

    template <typename TView>
    TView BaseCollectionView<TView>::operator[](size_t index) const
    {
        return at(index);
    }

    template <typename TView>
    const detail::TSCollectionDispatch *BaseCollectionView<TView>::collection_dispatch() const noexcept
    {
        const auto *dispatch = this->view_ref().context_ref().resolved().ts_dispatch;
        return dispatch != nullptr ? dispatch->as_collection() : nullptr;
    }

    template <typename TView>
    Range<TView> TSLView<TView>::values() const noexcept
    {
        return Range<TView>{this, this->size(), nullptr,
                            [](const void *context, size_t index) {
                                return static_cast<const TSLView *>(context)->at(index);
                            }};
    }

    template <typename TView>
    Range<TView> TSLView<TView>::valid_values() const noexcept
    {
        return Range<TView>{this, this->size(),
                            [](const void *context, size_t index) {
                                return static_cast<const TSLView *>(context)->at(index).valid();
                            },
                            [](const void *context, size_t index) {
                                return static_cast<const TSLView *>(context)->at(index);
                            }};
    }

    template <typename TView>
    Range<TView> TSLView<TView>::modified_values() const noexcept
    {
        const auto *dispatch = this->collection_dispatch();
        return Range<TView>{this, this->size(),
                            [](const void *context, size_t index) {
                                const auto *self = static_cast<const TSLView *>(context);
                                const auto *dispatch = self->collection_dispatch();
                                return dispatch != nullptr && dispatch->child_modified(self->view_ref().context_ref(), index);
                            },
                            [](const void *context, size_t index) {
                                return static_cast<const TSLView *>(context)->at(index);
                            }};
    }

    template <typename TView>
    KeyValueRange<size_t, TView> TSLView<TView>::items() const noexcept
    {
        return KeyValueRange<size_t, TView>{this, this->size(), nullptr,
                                            [](const void *context, size_t index) {
                                                return std::pair<size_t, TView>{index, static_cast<const TSLView *>(context)->at(index)};
                                            }};
    }

    template <typename TView>
    KeyValueRange<size_t, TView> TSLView<TView>::valid_items() const noexcept
    {
        return KeyValueRange<size_t, TView>{this, this->size(),
                                            [](const void *context, size_t index) {
                                                return static_cast<const TSLView *>(context)->at(index).valid();
                                            },
                                            [](const void *context, size_t index) {
                                                return std::pair<size_t, TView>{index, static_cast<const TSLView *>(context)->at(index)};
                                            }};
    }

    template <typename TView>
    KeyValueRange<size_t, TView> TSLView<TView>::modified_items() const noexcept
    {
        return KeyValueRange<size_t, TView>{this, this->size(),
                                            [](const void *context, size_t index) {
                                                const auto *self = static_cast<const TSLView *>(context);
                                                const auto *dispatch = self->collection_dispatch();
                                                return dispatch != nullptr && dispatch->child_modified(self->view_ref().context_ref(), index);
                                            },
                                            [](const void *context, size_t index) {
                                                return std::pair<size_t, TView>{index, static_cast<const TSLView *>(context)->at(index)};
                                            }};
    }

    template <typename TView>
    TView TSBView<TView>::field(std::string_view name) const
    {
        const auto *field_dispatch = this->field_dispatch();
        if (field_dispatch == nullptr) { return this->view_ref().make_child_view(TSViewContext::none()); }

        const TSViewContext child = field_dispatch->child_field(this->view_ref().context_ref(), name);
        return this->view_ref().make_child_view(child);
    }

    template <typename TView>
    Range<std::string_view> TSBView<TView>::keys() const noexcept
    {
        const auto *field_dispatch = this->field_dispatch();
        return Range<std::string_view>{field_dispatch, field_dispatch != nullptr ? this->size() : 0, nullptr,
                                       [](const void *context, size_t index) {
                                           return static_cast<const detail::TSFieldDispatch *>(context)->child_name(index);
                                       }};
    }

    template <typename TView>
    Range<TView> TSBView<TView>::values() const noexcept
    {
        return Range<TView>{this, this->size(), nullptr,
                            [](const void *context, size_t index) {
                                return static_cast<const TSBView *>(context)->at(index);
                            }};
    }

    template <typename TView>
    KeyValueRange<std::string_view, TView> TSBView<TView>::items() const noexcept
    {
        const auto *field_dispatch = this->field_dispatch();
        return KeyValueRange<std::string_view, TView>{this, field_dispatch != nullptr ? this->size() : 0, nullptr,
                                                      [](const void *context, size_t index) {
                                                          const auto *self = static_cast<const TSBView *>(context);
                                                          const auto *field_dispatch = self->field_dispatch();
                                                          const std::string_view name =
                                                              field_dispatch != nullptr ? field_dispatch->child_name(index) : std::string_view{};
                                                          return std::pair<std::string_view, TView>{name, self->at(index)};
                                                      }};
    }

    template <typename TView>
    KeyValueRange<std::string_view, TView> TSBView<TView>::valid_items() const noexcept
    {
        const auto *field_dispatch = this->field_dispatch();
        return KeyValueRange<std::string_view, TView>{this, field_dispatch != nullptr ? this->size() : 0,
                                                      [](const void *context, size_t index) {
                                                          return static_cast<const TSBView *>(context)->at(index).valid();
                                                      },
                                                      [](const void *context, size_t index) {
                                                          const auto *self = static_cast<const TSBView *>(context);
                                                          const auto *field_dispatch = self->field_dispatch();
                                                          const std::string_view name =
                                                              field_dispatch != nullptr ? field_dispatch->child_name(index) : std::string_view{};
                                                          return std::pair<std::string_view, TView>{name, self->at(index)};
                                                      }};
    }

    template <typename TView>
    KeyValueRange<std::string_view, TView> TSBView<TView>::modified_items() const noexcept
    {
        const auto *field_dispatch = this->field_dispatch();
        return KeyValueRange<std::string_view, TView>{this, field_dispatch != nullptr ? this->size() : 0,
                                                      [](const void *context, size_t index) {
                                                          const auto *self = static_cast<const TSBView *>(context);
                                                          const auto *dispatch = self->collection_dispatch();
                                                          return dispatch != nullptr &&
                                                                 dispatch->child_modified(self->view_ref().context_ref(), index);
                                                      },
                                                      [](const void *context, size_t index) {
                                                          const auto *self = static_cast<const TSBView *>(context);
                                                          const auto *field_dispatch = self->field_dispatch();
                                                          const std::string_view name =
                                                              field_dispatch != nullptr ? field_dispatch->child_name(index) : std::string_view{};
                                                          return std::pair<std::string_view, TView>{name, self->at(index)};
                                                      }};
    }

    template <typename TView>
    const detail::TSFieldDispatch *TSBView<TView>::field_dispatch() const noexcept
    {
        const auto *dispatch = this->collection_dispatch();
        return dispatch != nullptr ? dispatch->as_fields() : nullptr;
    }

    template <typename TView>
    TView TSDView<TView>::at(const View &key) const
    {
        const auto *key_dispatch = this->key_dispatch();
        const View map_value = this->value();
        if (key_dispatch == nullptr || !map_value.has_value() || !key.has_value()) {
            return this->view_ref().make_child_view(TSViewContext::none());
        }

        const MapView map = map_value.as_map();
        if (key.schema() != map.key_schema()) { return this->view_ref().make_child_view(TSViewContext::none()); }

        const TSViewContext child = key_dispatch->child_key(this->view_ref().context_ref(), key);
        if constexpr (std::same_as<TView, TSOutputView>) {
            if (!child.is_bound()) { return detail::ensure_dict_child_output_view(this->view_ref(), key); }
        }
        return this->view_ref().make_child_view(child);
    }

    template <typename TView>
    TView TSDView<TView>::operator[](const View &key) const
    {
        return at(key);
    }

    template <typename TView>
    const detail::TSKeyDispatch *TSDView<TView>::key_dispatch() const noexcept
    {
        const auto *dispatch = this->collection_dispatch();
        return dispatch != nullptr ? dispatch->as_keys() : nullptr;
    }

    template <typename TView>
    Range<View> TSDView<TView>::keys() const noexcept
    {
        const auto *dispatch = this->key_dispatch();
        return Range<View>{this, dispatch != nullptr ? dispatch->iteration_limit(this->view_ref().context_ref()) : 0,
                           [](const void *context, size_t slot) {
                               const auto *self = static_cast<const TSDView *>(context);
                               const auto *dispatch = self->key_dispatch();
                               return dispatch != nullptr && dispatch->slot_is_live(self->view_ref().context_ref(), slot);
                           },
                           [](const void *context, size_t slot) {
                               const auto *self = static_cast<const TSDView *>(context);
                               return self->key_dispatch()->key_at_slot(self->view_ref().context_ref(), slot);
                           }};
    }

    template <typename TView>
    Range<TView> TSDView<TView>::values() const noexcept
    {
        const auto *dispatch = this->key_dispatch();
        return Range<TView>{this, dispatch != nullptr ? dispatch->iteration_limit(this->view_ref().context_ref()) : 0,
                            [](const void *context, size_t slot) {
                                const auto *self = static_cast<const TSDView *>(context);
                                const auto *dispatch = self->key_dispatch();
                                return dispatch != nullptr && dispatch->slot_is_live(self->view_ref().context_ref(), slot);
                            },
                            [](const void *context, size_t slot) {
                                const auto *self = static_cast<const TSDView *>(context);
                                return self->at(self->key_dispatch()->key_at_slot(self->view_ref().context_ref(), slot));
                            }};
    }

    template <typename TView>
    KeyValueRange<View, TView> TSDView<TView>::items() const noexcept
    {
        const auto *dispatch = this->key_dispatch();
        return KeyValueRange<View, TView>{this, dispatch != nullptr ? dispatch->iteration_limit(this->view_ref().context_ref()) : 0,
                                          [](const void *context, size_t slot) {
                                              const auto *self = static_cast<const TSDView *>(context);
                                              const auto *dispatch = self->key_dispatch();
                                              return dispatch != nullptr && dispatch->slot_is_live(self->view_ref().context_ref(), slot);
                                          },
                                          [](const void *context, size_t slot) {
                                              const auto *self = static_cast<const TSDView *>(context);
                                              const View key = self->key_dispatch()->key_at_slot(self->view_ref().context_ref(), slot);
                                              return std::pair<View, TView>{key, self->at(key)};
                                          }};
    }

    template <typename TView>
    KeyValueRange<View, TView> TSDView<TView>::valid_items() const noexcept
    {
        const auto *dispatch = this->key_dispatch();
        return KeyValueRange<View, TView>{this, dispatch != nullptr ? dispatch->iteration_limit(this->view_ref().context_ref()) : 0,
                                          [](const void *context, size_t slot) {
                                              const auto *self = static_cast<const TSDView *>(context);
                                              const auto *dispatch = self->key_dispatch();
                                              if (dispatch == nullptr ||
                                                  !dispatch->slot_is_live(self->view_ref().context_ref(), slot)) {
                                                  return false;
                                              }
                                              return self->at(dispatch->key_at_slot(self->view_ref().context_ref(), slot)).valid();
                                          },
                                          [](const void *context, size_t slot) {
                                              const auto *self = static_cast<const TSDView *>(context);
                                              const View key = self->key_dispatch()->key_at_slot(self->view_ref().context_ref(), slot);
                                              return std::pair<View, TView>{key, self->at(key)};
                                          }};
    }

    template <typename TView>
    KeyValueRange<View, TView> TSDView<TView>::modified_items() const noexcept
    {
        const auto *dispatch = this->key_dispatch();
        return KeyValueRange<View, TView>{this, dispatch != nullptr ? dispatch->iteration_limit(this->view_ref().context_ref()) : 0,
                                          [](const void *context, size_t slot) {
                                              const auto *self = static_cast<const TSDView *>(context);
                                              const auto *dispatch = self->key_dispatch();
                                              if (dispatch == nullptr ||
                                                  !dispatch->slot_is_live(self->view_ref().context_ref(), slot)) {
                                                  return false;
                                              }
                                              const View key = dispatch->key_at_slot(self->view_ref().context_ref(), slot);
                                              return self->at(key).modified();
                                          },
                                          [](const void *context, size_t slot) {
                                              const auto *self = static_cast<const TSDView *>(context);
                                              const View key = self->key_dispatch()->key_at_slot(self->view_ref().context_ref(), slot);
                                              return std::pair<View, TView>{key, self->at(key)};
                                          }};
    }

    template <typename TView>
    const detail::TSSetDispatch *TSSReadView<TView>::set_dispatch() const noexcept
    {
        const auto *dispatch = this->view_ref().context_ref().resolved().ts_dispatch;
        return dispatch != nullptr ? dispatch->as_set() : nullptr;
    }

    template <typename TView>
    size_t TSSReadView<TView>::size() const noexcept
    {
        const auto *set_dispatch = this->set_dispatch();
        return set_dispatch != nullptr ? set_dispatch->size(this->view_ref().context_ref()) : 0;
    }

    template <typename TView>
    Range<View> TSSReadView<TView>::values() const noexcept
    {
        const auto *set_dispatch = this->set_dispatch();
        return set_dispatch != nullptr ? set_dispatch->values(this->view_ref().context_ref()) : Range<View>{nullptr, 0, nullptr, nullptr};
    }

    template <typename TView>
    Range<View> TSSReadView<TView>::added_values() const noexcept
    {
        if (!this->view_ref().modified()) { return Range<View>{nullptr, 0, nullptr, nullptr}; }
        const auto *set_dispatch = this->set_dispatch();
        return set_dispatch != nullptr ? set_dispatch->added_values(this->view_ref().context_ref())
                                       : Range<View>{nullptr, 0, nullptr, nullptr};
    }

    template <typename TView>
    Range<View> TSSReadView<TView>::removed_values() const noexcept
    {
        if (!this->view_ref().modified()) { return Range<View>{nullptr, 0, nullptr, nullptr}; }
        const auto *set_dispatch = this->set_dispatch();
        return set_dispatch != nullptr ? set_dispatch->removed_values(this->view_ref().context_ref())
                                       : Range<View>{nullptr, 0, nullptr, nullptr};
    }

    template <typename TView>
    bool TSSReadView<TView>::empty() const noexcept
    {
        const auto *set_dispatch = this->set_dispatch();
        return set_dispatch == nullptr || set_dispatch->empty(this->view_ref().context_ref());
    }

    template <typename TView>
    size_t TSWView<TView>::size() const noexcept
    {
        const auto *dispatch = this->view_ref().context_ref().resolved().ts_dispatch;
        const auto *window_dispatch = dispatch != nullptr ? dispatch->as_window() : nullptr;
        return window_dispatch != nullptr ? window_dispatch->size(this->view_ref().context_ref(), this->view_ref().evaluation_time()) : 0;
    }

    template <typename TView>
    Range<View> TSWView<TView>::values() const noexcept
    {
        const auto *dispatch = this->view_ref().context_ref().resolved().ts_dispatch;
        const auto *window_dispatch = dispatch != nullptr ? dispatch->as_window() : nullptr;
        return window_dispatch != nullptr ? window_dispatch->values(this->view_ref().context_ref(), this->view_ref().evaluation_time())
                                          : Range<View>{nullptr, 0, nullptr, nullptr};
    }

    template <typename TView>
    Range<engine_time_t> TSWView<TView>::value_times() const noexcept
    {
        const auto *dispatch = this->view_ref().context_ref().resolved().ts_dispatch;
        const auto *window_dispatch = dispatch != nullptr ? dispatch->as_window() : nullptr;
        return window_dispatch != nullptr ? window_dispatch->value_times(this->view_ref().context_ref(), this->view_ref().evaluation_time())
                                          : Range<engine_time_t>{nullptr, 0, nullptr, nullptr};
    }

}  // namespace hgraph
