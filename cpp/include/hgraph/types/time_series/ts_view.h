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

    struct TSViewContext;

    namespace detail
    {
        struct TSCollectionDispatch;
        struct TSFieldDispatch;
        struct TSKeyDispatch;

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
            [[nodiscard]] virtual const TSCollectionDispatch *as_collection() const noexcept { return nullptr; }
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
        };
    }  // namespace detail

    /**
     * Identifies the endpoint object at the root of a time-series state tree.
     *
     * A time-series view ultimately resolves to either an input endpoint or
     * an output endpoint by walking the TS state parent chain.
     */
    using ParentValue = pointer_aligned_discriminated_ptr<TSInput, TSOutput>;

    /**
     * View-specialized TS context for a specific logical time-series position.
     *
     * `TSViewContext` extends the shared raw `TSContext` carrier with the
     * view-only helpers needed to resolve link-backed storage and materialize
     * the corresponding erased value view.
     */
    struct TSViewContext : TSContext
    {
        TSViewContext() noexcept
            : active_state(View::invalid_for(nullptr))
        {
        }

        TSViewContext(const TSMeta *schema_,
                      const detail::ViewDispatch *value_dispatch_,
                      const detail::TSDispatch *ts_dispatch_,
                      void *value_data_,
                      BaseState *ts_state_) noexcept
            : TSContext(schema_, value_dispatch_, ts_dispatch_, value_data_, ts_state_),
              active_state(View::invalid_for(nullptr))
        {
        }

        TSViewContext(const TSContext &context) noexcept
            : TSContext(context),
              active_state(View::invalid_for(nullptr))
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

            const auto apply_target = [&resolved_context](const LinkedTSContext &target) noexcept {
                if (!target.is_bound()) {
                    resolved_context.value_data = nullptr;
                    return;
                }

                resolved_context.schema = target.schema != nullptr ? target.schema : resolved_context.schema;
                resolved_context.value_dispatch =
                    target.value_dispatch != nullptr ? target.value_dispatch : resolved_context.value_dispatch;
                resolved_context.ts_dispatch = target.ts_dispatch != nullptr ? target.ts_dispatch : resolved_context.ts_dispatch;
                resolved_context.value_data = target.value_data;
            };

            if (const LinkedTSContext *target = state->linked_target(); target != nullptr) { apply_target(*target); }

            return resolved_context;
        }

        [[nodiscard]] View value() const noexcept
        {
            const TSViewContext resolved_context = resolved();
            const value::TypeMeta *value_schema = resolved_context.schema != nullptr ? resolved_context.schema->value_type : nullptr;
            if (resolved_context.value_dispatch == nullptr || resolved_context.value_data == nullptr) {
                return View::invalid_for(value_schema);
            }
            return View{resolved_context.value_dispatch, resolved_context.value_data, value_schema};
        }

        View active_state;
        Notifiable *scheduling_notifier{nullptr};
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
            if (const BaseState *state = m_context.ts_state; state != nullptr) {
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
            const BaseState *state = m_context.ts_state;

            while (state != nullptr) {
                ParentValue resolved{};
                bool found = false;

                hgraph::visit(
                    state->parent,
                    [&state, &resolved, &found](auto *ptr) {
                        using T = std::remove_pointer_t<decltype(ptr)>;

                        if constexpr (std::same_as<T, TSInput> || std::same_as<T, TSOutput>) {
                            resolved = ptr;
                            found = true;
                        } else {
                            state = ptr;
                        }
                    },
                    [&state] { state = nullptr; });

                if (found) { return resolved; }
            }

            return {};
        }

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
        [[nodiscard]] TSViewContext parent_context() const noexcept { return m_parent; }

        TSViewContext m_context{TSViewContext::none()};
        TSViewContext m_parent{TSViewContext::none()};
        engine_time_t m_evaluation_time{MIN_DT};
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

      protected:
        [[nodiscard]] const detail::TSKeyDispatch *key_dispatch() const noexcept;
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

    namespace detail
    {
        [[nodiscard]] inline const BaseCollectionState *collection_state(const BaseState *state) noexcept
        {
            return static_cast<const BaseCollectionState *>(state);
        }

        [[nodiscard]] inline bool child_is_modified(const BaseState *state, size_t index) noexcept
        {
            const auto *collection = collection_state(state);
            return collection != nullptr && collection->modified_children.contains(index);
        }

        [[nodiscard]] inline bool dict_slot_is_live(const MapDeltaView &delta, size_t slot) noexcept
        {
            return delta.slot_occupied(slot) && !delta.slot_removed(slot);
        }

    }  // namespace detail

    template <typename TView>
    size_t BaseCollectionView<TView>::size() const noexcept
    {
        const auto *dispatch = collection_dispatch();
        return dispatch != nullptr ? dispatch->size(this->m_context) : 0;
    }

    template <typename TView>
    TView BaseCollectionView<TView>::at(size_t index) const
    {
        const auto *dispatch = collection_dispatch();
        if (dispatch == nullptr) { return TView{TSViewContext::none(), this->m_context, this->m_evaluation_time}; }

        const TSViewContext child = dispatch->child_at(this->m_context, index);
        return TView{child, this->m_context, this->m_evaluation_time};
    }

    template <typename TView>
    TView BaseCollectionView<TView>::operator[](size_t index) const
    {
        return at(index);
    }

    template <typename TView>
    const detail::TSCollectionDispatch *BaseCollectionView<TView>::collection_dispatch() const noexcept
    {
        const auto *dispatch = this->ts_dispatch();
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
        return Range<TView>{this, this->size(),
                            [](const void *context, size_t index) {
                                return detail::child_is_modified(static_cast<const TSLView *>(context)->ts_state(), index);
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
                                                return detail::child_is_modified(static_cast<const TSLView *>(context)->ts_state(), index);
                                            },
                                            [](const void *context, size_t index) {
                                                return std::pair<size_t, TView>{index, static_cast<const TSLView *>(context)->at(index)};
                                            }};
    }

    template <typename TView>
    TView TSBView<TView>::field(std::string_view name) const
    {
        const auto *field_dispatch = this->field_dispatch();
        if (field_dispatch == nullptr) { return TView{TSViewContext::none(), this->m_context, this->m_evaluation_time}; }

        const TSViewContext child = field_dispatch->child_field(this->m_context, name);
        return TView{child, this->m_context, this->m_evaluation_time};
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
                                                          return detail::child_is_modified(static_cast<const TSBView *>(context)->ts_state(), index);
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
            return TView{TSViewContext::none(), this->m_context, this->m_evaluation_time};
        }

        const MapView map = map_value.as_map();
        if (key.schema() != map.key_schema()) { return TView{TSViewContext::none(), this->m_context, this->m_evaluation_time}; }

        const TSViewContext child = key_dispatch->child_key(this->m_context, key);
        return TView{child, this->m_context, this->m_evaluation_time};
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
        return Range<View>{this, this->value().as_map().delta().slot_capacity(),
                           [](const void *context, size_t slot) {
                               const auto delta = static_cast<const TSDView *>(context)->value().as_map().delta();
                               return detail::dict_slot_is_live(delta, slot);
                           },
                           [](const void *context, size_t slot) {
                               return static_cast<const TSDView *>(context)->value().as_map().delta().key_at_slot(slot);
                           }};
    }

    template <typename TView>
    Range<TView> TSDView<TView>::values() const noexcept
    {
        return Range<TView>{this, this->value().as_map().delta().slot_capacity(),
                            [](const void *context, size_t slot) {
                                const auto delta = static_cast<const TSDView *>(context)->value().as_map().delta();
                                return detail::dict_slot_is_live(delta, slot);
                            },
                            [](const void *context, size_t slot) {
                                const auto key = static_cast<const TSDView *>(context)->value().as_map().delta().key_at_slot(slot);
                                return static_cast<const TSDView *>(context)->at(key);
                            }};
    }

    template <typename TView>
    KeyValueRange<View, TView> TSDView<TView>::items() const noexcept
    {
        return KeyValueRange<View, TView>{this, this->value().as_map().delta().slot_capacity(),
                                          [](const void *context, size_t slot) {
                                              const auto delta = static_cast<const TSDView *>(context)->value().as_map().delta();
                                              return detail::dict_slot_is_live(delta, slot);
                                          },
                                          [](const void *context, size_t slot) {
                                              const auto *self = static_cast<const TSDView *>(context);
                                              const View key = self->value().as_map().delta().key_at_slot(slot);
                                              return std::pair<View, TView>{key, self->at(key)};
                                          }};
    }

    template <typename TView>
    KeyValueRange<View, TView> TSDView<TView>::valid_items() const noexcept
    {
        return KeyValueRange<View, TView>{this, this->value().as_map().delta().slot_capacity(),
                                          [](const void *context, size_t slot) {
                                              const auto *self = static_cast<const TSDView *>(context);
                                              const auto delta = self->value().as_map().delta();
                                              if (!detail::dict_slot_is_live(delta, slot)) { return false; }
                                              return self->at(delta.key_at_slot(slot)).valid();
                                          },
                                          [](const void *context, size_t slot) {
                                              const auto *self = static_cast<const TSDView *>(context);
                                              const View key = self->value().as_map().delta().key_at_slot(slot);
                                              return std::pair<View, TView>{key, self->at(key)};
                                          }};
    }

    template <typename TView>
    KeyValueRange<View, TView> TSDView<TView>::modified_items() const noexcept
    {
        return KeyValueRange<View, TView>{this, this->value().as_map().delta().slot_capacity(),
                                          [](const void *context, size_t slot) {
                                              const auto *self = static_cast<const TSDView *>(context);
                                              const auto delta = self->value().as_map().delta();
                                              return detail::dict_slot_is_live(delta, slot) &&
                                                     detail::child_is_modified(self->ts_state(), slot);
                                          },
                                          [](const void *context, size_t slot) {
                                              const auto *self = static_cast<const TSDView *>(context);
                                              const View key = self->value().as_map().delta().key_at_slot(slot);
                                              return std::pair<View, TView>{key, self->at(key)};
                                          }};
    }

    template <typename TView>
    size_t TSSView<TView>::size() const noexcept
    {
        return this->value().as_set().size();
    }

    template <typename TView>
    Range<View> TSSView<TView>::values() const noexcept
    {
        return this->value().as_set().values();
    }

    template <typename TView>
    Range<View> TSSView<TView>::added_values() const noexcept
    {
        return this->value().as_set().delta().added();
    }

    template <typename TView>
    Range<View> TSSView<TView>::removed_values() const noexcept
    {
        return this->value().as_set().delta().removed();
    }

    template <typename TView>
    TView TSSView<TView>::is_empty() const noexcept
    {
        return TView{};
    }

    template <typename TView>
    size_t TSWView<TView>::size() const noexcept
    {
        return this->value().as_cyclic_buffer().size();
    }

    template <typename TView>
    Range<View> TSWView<TView>::values() const noexcept
    {
        return Range<View>{this, this->size(), nullptr,
                           [](const void *context, size_t index) {
                               return static_cast<const TSWView *>(context)->value().as_cyclic_buffer().at(index);
                           }};
    }

    template <typename TView>
    Range<engine_time_t> TSWView<TView>::value_times() const noexcept
    {
        return Range<engine_time_t>{nullptr, 0, nullptr, nullptr};
    }

}  // namespace hgraph
