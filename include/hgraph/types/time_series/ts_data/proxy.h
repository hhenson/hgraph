#ifndef HGRAPH_CPP_TS_DATA_PROXY_H
#define HGRAPH_CPP_TS_DATA_PROXY_H

#include <hgraph/types/notifiable.h>
#include <hgraph/types/time_series/ts_data/dict_view.h>
#include <hgraph/types/utils/slot_observer.h>
#include <hgraph/types/utils/value_slot_store.h>

#include <cstddef>
#include <vector>

namespace hgraph
{
    class TSDProxy;

    struct TSDProxyValueOps
    {
        using BuildFn = void (*)(TSDProxy &, std::size_t, const TSDataView &, const TSDataView &,
                                 DateTime, const void *);
        using SourceIdentityMatchesFn = bool (*)(const TSDProxy &, std::size_t, const TSDataView &,
                                                 const TSDataView &, const void *);

        BuildFn build{nullptr};
        SourceIdentityMatchesFn source_identity_matches{nullptr};
    };

    /** Slot-event adapter that drives one proxy storage from its source TSD. */
    class TSDProxySlotSync final : public SlotObserver, public Notifiable
    {
      public:
        explicit TSDProxySlotSync(TSDProxy &owner) noexcept;
        TSDProxySlotSync(const TSDProxySlotSync &) = delete;
        TSDProxySlotSync &operator=(const TSDProxySlotSync &) = delete;
        TSDProxySlotSync(TSDProxySlotSync &&) = delete;
        TSDProxySlotSync &operator=(TSDProxySlotSync &&) = delete;
        ~TSDProxySlotSync() override;

        void on_capacity(std::size_t old_capacity, std::size_t new_capacity) override;
        void on_insert(std::size_t slot) override;
        void on_remove(std::size_t slot) override;
        void on_erase(std::size_t slot) override;
        void on_clear() override;
        void notify(DateTime modified_time) override;
        void source_invalidated(const TSDataTracking *source) noexcept override;

      private:
        TSDProxy *owner_{nullptr};
    };

    /**
     * Read-only proxy over a source ``TSD``.
     *
     * ``TSDProxy`` mirrors the source dictionary's key-set and stable slot ids,
     * but owns its value slots. A caller-provided static value-ops table
     * constructs each proxy value from the source child at the same slot and
     * may compare source identity for equal-time refreshes. Structural slot
     * callbacks construct and destroy the parallel value slots; source TSData
     * modification notification supplies the evaluation time used to materialise
     * added/removed proxy children.
     */
    /** How a proxy reacts to a LIVE source child recording a value tick.
        To-REF proxies materialise identities - a child value tick does not
        change them (StructureOnly). From-REF proxies hold links bound FROM
        child reference values - a retarget must re-run the builder
        (OnChildTick). */
    enum class TSDProxyChildRefresh : std::uint8_t
    {
        StructureOnly,
        OnChildTick,
        Invalidating,  // Internal guard while source invalidation is running.
    };

    class TSDProxy final
    {
      public:
        TSDProxy() noexcept;
        TSDProxy(const TSDProxy &) = delete;
        TSDProxy &operator=(const TSDProxy &) = delete;
        TSDProxy(TSDProxy &&) = delete;
        TSDProxy &operator=(TSDProxy &&) = delete;
        ~TSDProxy();

        /**
         * Bind this proxy storage to ``source``.
         *
         * ``self_type`` describes the proxy storage itself. ``element_type``
         * describes each constructed proxy value. ``value_ops->build`` is
         * called when a slot value first needs to be materialised.
         */
        void bind(TSRoleTypeRef     self_type,
                  TSRoleTypeRef     element_type,
                  const TSDDataView   &source,
                  const TSDProxyValueOps *value_ops,
                  const void          *builder_context,
                  DateTime        modified_time,
                  TSDProxyChildRefresh child_refresh = TSDProxyChildRefresh::StructureOnly);

        [[nodiscard]] TSDataView source_view() const noexcept;
        [[nodiscard]] bool source_available() const noexcept { return source_storage_.valid(); }
        [[nodiscard]] TSDDataView source_dict() const;
        [[nodiscard]] TSDataView source_child_at_slot(std::size_t slot) const;

        [[nodiscard]] TSDataTracking &tracking() noexcept;
        [[nodiscard]] const TSDataTracking &tracking() const noexcept;
        [[nodiscard]] TSDataTracking &key_set_tracking() noexcept;
        [[nodiscard]] const TSDataTracking &key_set_tracking() const noexcept;
        [[nodiscard]] bool has_child(std::size_t slot) const noexcept;
        [[nodiscard]] bool child_updated(std::size_t slot) const noexcept;
        [[nodiscard]] const void *child_at_slot(std::size_t slot) const;
        [[nodiscard]] void *child_at_slot(std::size_t slot);
        [[nodiscard]] void *owned_child_memory(std::size_t slot) noexcept
        {
            return has_child(slot) ? values_.value_memory(slot) : nullptr;
        }

        /**
         * LAZY re-materialisation (single-threaded runtime): a key inserted
         * and value-written within ONE source mutation notifies observers at
         * insert time - the builder may run before the value lands, and the
         * once-per-time notification contract suppresses a second wake. Child
         * reads therefore re-run the builder when the source child recorded
         * after the slot was last built.
         */
        void refresh_stale_child(std::size_t slot) const;

        /** Ops hooks used by the proxy TSData binding. */
        void record_child_modified(std::size_t slot, DateTime modified_time);
        void subscribe_slot_observer(SlotObserver *observer);
        void unsubscribe_slot_observer(SlotObserver *observer);

        [[nodiscard]] TSRoleTypeRef element_type() const noexcept { return element_type_; }
        [[nodiscard]] std::size_t child_capacity() const noexcept { return values_.slot_capacity(); }
        [[nodiscard]] bool source_identities_match() const;
        void stop() noexcept;

      private:
        friend class TSDProxySlotSync;

        /** Source slot lifecycle callbacks, invoked by ``TSDProxySlotSync``. */
        void on_slot_capacity(std::size_t old_capacity, std::size_t new_capacity);
        void on_slot_inserted(std::size_t slot);
        void on_slot_removed(std::size_t slot);
        void on_slot_erased(std::size_t slot);
        void on_slots_cleared();
        void on_source_modified(DateTime modified_time);
        void on_source_invalidated(const TSDataTracking *source) noexcept;

        void subscribe_source();
        void unsubscribe_source(bool strict = true) noexcept;
        void sync_from_source(DateTime modified_time, bool force_modified);
        void construct_child_at_slot(std::size_t slot);
        bool retry_pending_child_at_slot(std::size_t slot, DateTime modified_time);
        [[nodiscard]] DateTime current_lifecycle_time(std::size_t slot) const;
        void refresh_child_at_slot(std::size_t slot, DateTime modified_time);
        void stamp_built(std::size_t slot, DateTime modified_time);
        void mark_modified(DateTime modified_time);
        void mark_structure_modified(DateTime modified_time);
        [[nodiscard]] bool has_constructed_children() const noexcept;

        TSRoleTypeRef              self_type_{};
        TSRoleTypeRef              element_type_{};
        TSDProxySlotSync              source_sync_;
        TSDDataStorageRef             source_storage_{};
        const TSDProxyValueOps        *value_ops_{nullptr};
        const void                   *value_builder_context_{nullptr};
        ValueSlotStore                values_{};
        std::vector<DateTime>         built_times_{};
        DateTime                      updated_window_{MIN_DT};   // lazy delta-window roll
        TSDProxyChildRefresh          child_refresh_{TSDProxyChildRefresh::StructureOnly};
        bool                          subscribed_{false};
        bool                          structure_pending_{false};
        TSDataTracking                tracking_{};
        TSDataTracking                key_set_tracking_{};
        SlotObserverList              slot_observers_{};
    };

    /** Return the proxy role type for ``schema`` and ``element_type``. */
    [[nodiscard]] TSDataTypeRef tsd_proxy_data_type_for(const TSValueTypeMetaData &schema,
                                                        TSRoleTypeRef element_type);
    [[nodiscard]] TSOutputTypeRef tsd_proxy_output_type_for(const TSValueTypeMetaData &schema,
                                                            TSRoleTypeRef element_type);

    /** Clear interned proxy type contexts that borrow schema and role-record pointers. */
    void clear_tsd_proxy_contexts() noexcept;

    /** Bind a live proxy TSData view to a source dictionary. */
    void bind_tsd_proxy(const TSDataView       &proxy,
                        const TSDDataView      &source,
                        const TSDProxyValueOps *value_ops,
                        const void             *builder_context,
                        DateTime           modified_time,
                        TSDProxyChildRefresh    child_refresh = TSDProxyChildRefresh::StructureOnly);
}  // namespace hgraph

#endif  // HGRAPH_CPP_TS_DATA_PROXY_H
