#ifndef HGRAPH_CPP_TS_DATA_TYPES_H
#define HGRAPH_CPP_TS_DATA_TYPES_H

#include <hgraph/runtime/node_fwd.h>
#include <hgraph/types/metadata/ts_value_type_meta_data.h>
#include <hgraph/types/notifiable.h>
#include <hgraph/types/storage_metrics.h>
#include <hgraph/types/time_series/endpoint_owner.h>
#include <hgraph/types/time_series/ts_type_ref.h>
#include <hgraph/types/value/value_ops.h>
#include <hgraph/util/date_time.h>
#include <hgraph/util/tagged_ptr.h>
#include <cstddef>
#include <cstdint>
#include <stdexcept>
#include <type_traits>
#include <vector>

namespace hgraph
{
    struct TSDataOps;

    class GraphView;
    struct TSDataTracking;
    struct TSDataParent;
    class TSInput;
    class TSOutput;
    class TSDataView;
    class TSDataMutationView;
    class IndexedTSDataView;
    class TSSDataView;
    class TSSDataMutationView;
    class TSDDataView;
    class TSDDataMutationView;
    class TSBDataView;
    class TSLDataView;
    class TSWDataView;
    class TSWDataMutationView;

    inline constexpr std::size_t TS_DATA_NO_CHILD_ID = static_cast<std::size_t>(-1);

    /**
     * Terminal parent for a TSData bubble-up chain.
     *
     * Non-TSData endpoint owners, such as ``TSOutput``, implement this small
     * interface so the root TSData node can use the same parent-notification
     * path as nested TSData children.
     */
    struct TSDataParent
    {
        virtual ~TSDataParent() = default;

        /** Record that ``child_id`` modified at ``mutation_time``. */
        virtual void record_child_modified(std::size_t child_id, DateTime mutation_time) = 0;
    };

    enum class TSParentLinkKind : std::uintptr_t
    {
        None           = 0,
        TSData         = 1,
        InputEndpoint  = 2,
        OutputEndpoint = 3,
        NodeEndpoint   = 4,
    };

    /**
     * Stable parent identity for one time-series node.
     *
     * This is stored in the child node's value-owned tracking region. It
     * carries a type-erased handle plus a small tag that says which view type
     * can interpret that handle. TSData and node parents both use the same
     * ``TSRoleTypeRef + data`` pattern as the rest of the runtime.
     */
    struct TSParentLink
    {
        using ParentIdentity = tagged_void_ptr<3, TSParentLinkKind>;

        union ParentPayload
        {
            const void *ts_data;
            void       *node_data;
            TSInput    *input;
            TSOutput   *output;

            constexpr ParentPayload() noexcept : ts_data(nullptr) {}
            constexpr explicit ParentPayload(const void *data) noexcept : ts_data(data) {}
            constexpr explicit ParentPayload(void *data) noexcept : node_data(data) {}
            constexpr explicit ParentPayload(TSInput *parent) noexcept : input(parent) {}
            constexpr explicit ParentPayload(TSOutput *parent) noexcept : output(parent) {}
        };

      private:
        ParentIdentity parent_{};
        ParentPayload  payload_{};

      public:
        std::size_t child_id{TS_DATA_NO_CHILD_ID};

        constexpr TSParentLink() noexcept = default;
        constexpr TSParentLink(TSRoleTypeRef type,
                               const void   *data,
                               std::size_t   parent_child_id) noexcept
            : parent_(type.record(), TSParentLinkKind::TSData),
              payload_(data),
              child_id(parent_child_id)
        {
        }
        TSParentLink(NodePtr node, TSEndpointOwnerPort port) noexcept
            : parent_(node.record(), TSParentLinkKind::NodeEndpoint),
              payload_(const_cast<void *>(node.data())),
              child_id(static_cast<std::size_t>(port))
        {
        }
        constexpr TSParentLink(TSInput &endpoint,
                               std::size_t parent_child_id = TS_DATA_NO_CHILD_ID) noexcept
            : parent_(nullptr, TSParentLinkKind::InputEndpoint),
              payload_(&endpoint),
              child_id(parent_child_id)
        {
        }
        constexpr TSParentLink(TSOutput &endpoint,
                               std::size_t parent_child_id = TS_DATA_NO_CHILD_ID) noexcept
            : parent_(nullptr, TSParentLinkKind::OutputEndpoint),
              payload_(&endpoint),
              child_id(parent_child_id)
        {
        }

        /** Parent kind encoded into the parent identity pointer. */
        [[nodiscard]] TSParentLinkKind kind() const noexcept;

        /** True when this child has either a TSData parent or an endpoint parent. */
        [[nodiscard]] bool has_parent() const noexcept;

        /** True when this child has a recorded parent TSData allocation. */
        [[nodiscard]] bool has_ts_data_parent() const noexcept;

        /** True when this child bubbles directly to an endpoint parent. */
        [[nodiscard]] bool has_endpoint_parent() const noexcept;

        /** True when this child bubbles directly to an input endpoint parent. */
        [[nodiscard]] bool has_input_endpoint_parent() const noexcept;

        /** True when this child bubbles directly to an output endpoint parent. */
        [[nodiscard]] bool has_output_endpoint_parent() const noexcept;

        /** True when this child bubbles directly to a node-owned endpoint parent. */
        [[nodiscard]] bool has_node_endpoint_parent() const noexcept;

        /** Canonical parent TSData type record, or an unbound reference. */
        [[nodiscard]] TSRoleTypeRef parent_storage_type() const noexcept;

        /** Parent TSData memory, or null when this link does not target TSData. */
        [[nodiscard]] const void *parent_data() const noexcept;

        /** Endpoint parent, or null when this link targets TSData or is empty. */
        [[nodiscard]] TSDataParent *parent_endpoint() const noexcept;

        /** Input endpoint parent, or null when this link targets a different parent kind. */
        [[nodiscard]] TSInput *parent_input() const noexcept;

        /** Output endpoint parent, or null when this link targets a different parent kind. */
        [[nodiscard]] TSOutput *parent_output() const noexcept;

        /** Typed node pointer for a node-owned endpoint parent, or unbound otherwise. */
        [[nodiscard]] NodePtr parent_node_ptr() const noexcept;

        /** Endpoint port for this endpoint parent, or Input for an empty/non-endpoint link. */
        [[nodiscard]] TSEndpointOwnerPort port() const noexcept;

        /** True when this parent link points at a runtime node allocation. */
        [[nodiscard]] bool node_owned() const noexcept;

        /** Node view for a node-owned endpoint parent, or an empty view otherwise. */
        [[nodiscard]] NodeView parent_node() const;

        /** Graph view for a node-owned endpoint parent, or an empty view otherwise. */
        [[nodiscard]] GraphView parent_graph() const;

        /**
         * Record a child modification against the parent and bubble that
         * modification towards the root. No-op for a root link.
         */
        void notify_child_modified(DateTime mutation_time) const;

        /**
         * Return parent-relative navigation ids from the root to this child.
         * Field, index, and slot ids are all represented as integer ids.
         */
        [[nodiscard]] std::vector<std::size_t> path_from_root() const;

        /** Resolve the root TSData view reached by repeatedly following links. */
        [[nodiscard]] TSDataView root_view() const;

      private:
        [[nodiscard]] const TSDataTracking &parent_tracking() const;
        [[nodiscard]] TSDataTracking &mutable_parent_tracking() const;
    };

    static_assert(sizeof(TSParentLink) <= sizeof(void *) * 3,
                  "TSParentLink should remain a compact three-word navigation handle");

    /**
     * Compact per-level observer set for TSData modification notifications.
     *
     * The common case stores no payload at all: an empty set is a null tagged
     * pointer. A single observer is stored directly in the tagged pointer. The
     * allocation for a vector is only introduced once a second observer is
     * registered. Observers are not copied with TSData payload copies.
     */
    class TSDataObserverSet
    {
      public:
        TSDataObserverSet() noexcept = default;
        TSDataObserverSet(const TSDataObserverSet &) noexcept;
        TSDataObserverSet &operator=(const TSDataObserverSet &) noexcept;
        TSDataObserverSet(TSDataObserverSet &&other) noexcept;
        TSDataObserverSet &operator=(TSDataObserverSet &&other) noexcept;
        ~TSDataObserverSet() noexcept;

        /** True when this level has no registered observers. */
        [[nodiscard]] bool empty() const noexcept;

        /** True when ``observer`` is currently registered. */
        [[nodiscard]] bool contains(const Notifiable *observer) const noexcept;

        /** Number of observers registered at this level. */
        [[nodiscard]] std::size_t size() const noexcept;

        /** Exact occupied/retained heap bytes for multi-observer storage. */
        [[nodiscard]] DynamicStorageMetrics dynamic_storage_metrics() const noexcept;

        /** Register an observer pointer; null is ignored and duplicates assert. */
        void subscribe(Notifiable *observer);

        /** Remove an observer pointer; null is ignored and missing observers assert. */
        void unsubscribe(Notifiable *observer);

        /** Replace a registered observer pointer without changing list shape. */
        void replace(Notifiable *observer, Notifiable *replacement) noexcept;

        /** Notify all registered observers for ``modified_time``.
            Header-inline fast path (issue #192 stage 1): the measured call
            distribution is 60-78% EMPTY sets and most of the rest a single
            observer (tick 700k/0/200k, subscription 150k/60k/40k, dense map
            1004k/201k/203k empty/single/many) — both dispatch without the
            out-of-line call; only the vector path with its re-entrancy
            depth/compaction bookkeeping stays out-of-line. */
        void notify(DateTime modified_time) const
        {
            if (!observers_) { return; }
            if (auto *entry = observers_.get<Notifiable>(); entry != nullptr)
            {
                entry->notify(modified_time);
                return;
            }
            notify_many(modified_time);
        }

        /** Detach and invalidate every observer before ``source`` is destroyed. */
        void invalidate(const TSDataTracking *source) noexcept;

        /** Clear all observer registrations without notifying them. */
        void clear() noexcept;

      private:
        struct ObserverList
        {
            std::vector<Notifiable *> entries{};
            std::size_t               notify_depth{0};
            bool                      compact_pending{false};
        };
        using ObserverStorage = discriminated_ptr<Notifiable, ObserverList>;

        [[nodiscard]] Notifiable *single() const noexcept;
        [[nodiscard]] ObserverList *many() const noexcept;
        void set_single(Notifiable *observer) noexcept;
        void set_many(ObserverList *observers) noexcept;
        void compact_many(ObserverList &observers) noexcept;
        void notify_many(DateTime modified_time) const;

        ObserverStorage observers_{};
    };

    static_assert(sizeof(TSDataObserverSet) <= sizeof(void *),
                  "TSDataObserverSet should remain a one-word tagged observer handle");

    /**
     * Common per-TSData tracking state.
     *
     * Every TSData shape, including compact atomic TSData, stores this in its
     * value-owned memory. Projected child values use ``parent`` for local
     * bubble-up; root values may carry a terminal endpoint parent such as the
     * owning ``TSOutput``. ``observers`` owns the compact per-level
     * notification set, while ``last_modified_time`` decides whether this
     * node's local delta view belongs to a given evaluation time.
     */
    struct TSDataTracking
    {
        TSDataTracking() noexcept = default;
        TSDataTracking(const TSDataTracking &) = default;
        TSDataTracking &operator=(const TSDataTracking &) = default;
        TSDataTracking(TSDataTracking &&) noexcept = default;
        TSDataTracking &operator=(TSDataTracking &&) noexcept = default;
        ~TSDataTracking() noexcept = default;

        /**
         * Record the first modification for ``modified_time`` and notify local
         * observers once. Returns false when this level was already marked for
         * the same or a later evaluation time — delta clocks are monotonic, so
         * a record replaying an older source timestamp never rewinds state.
         */
        [[nodiscard]] bool record_modified(DateTime modified_time);

        DateTime last_modified_time{MIN_DT};
        TSParentLink parent{};
        TSDataObserverSet observers{};
    };

    /**
     * Memory offsets for one TSData implementation.
     *
     * Current value bytes and tracking bytes are separate plan regions. Some
     * implementations also expose a separate delta memory region; compact
     * atomic TSData aliases ``delta_value(t)`` to the current value region
     * when ``last_modified_time == t``.
     */
    struct TSDataLayout
    {
        ValueTypeRef value_binding{nullptr};
        ValueTypeRef delta_binding{nullptr};
        std::size_t             value_offset{0};
        std::size_t             tracking_offset{0};
    };

    /**
     * Field layout entry for fixed-size bundle TSData.
     *
     * The child storage type owns its own TSData ops/layout. The parent layout only
     * records where that field's data pointer starts inside the parent
     * allocation.
     */
    struct FixedTSDataFieldLayout
    {
        TSRoleTypeRef     type{};
        const TSDataLayout  *layout{nullptr};
        std::size_t          data_offset{0};

        /** Current-value binding for this field, or null when the child layout is absent. */
        [[nodiscard]] ValueTypeRef value_binding() const noexcept;

        /** Delta-value binding for this field, or null when the child layout is absent. */
        [[nodiscard]] ValueTypeRef delta_binding() const noexcept;
    };

    /**
     * Layout for fixed-size bundle TSData.
     *
     * TSB fields may have different TSData schemas and therefore different
     * embedded offsets/layouts.
     */
    struct FixedTSBDataLayout : TSDataLayout
    {
        std::vector<FixedTSDataFieldLayout> fields{};

        /** Number of statically planned fields. */
        [[nodiscard]] std::size_t size() const noexcept;

        /** Layout entry for one field; throws when index is out of range. */
        [[nodiscard]] const FixedTSDataFieldLayout &field(std::size_t index) const;
    };

    /**
     * Layout for fixed-size list TSData.
     *
     * All elements have the same TSData schema. The current value bytes live in
     * a fixed value-layer array and the auxiliary element state lives in a
     * matching fixed array, so element offsets are computed from strides rather
     * than stored as one layout entry per element.
     */
    struct FixedTSLDataLayout : TSDataLayout
    {
        TSRoleTypeRef     element_type{};
        const TSDataLayout  *element_layout{nullptr};
        std::size_t          element_count{0};
        std::size_t          element_value_stride{0};
        std::size_t          element_auxiliary_offset{0};
        std::size_t          element_auxiliary_stride{0};

        /** Number of statically planned list elements. */
        [[nodiscard]] std::size_t size() const noexcept;

        /** Byte offset for one element's current value storage. */
        [[nodiscard]] std::size_t element_value_offset(std::size_t index) const;

        /** Byte offset for one element's auxiliary TSData state. */
        [[nodiscard]] std::size_t element_auxiliary_offset_at(std::size_t index) const;
    };

    struct TSSDataLayout : TSDataLayout
    {
        ValueTypeRef key_binding{nullptr};
    };

    struct TSDDataLayout : TSSDataLayout
    {
        TSRoleTypeRef        element_type{};
        const TSDataLayout     *element_layout{nullptr};
        TSRoleTypeRef        key_set_type{};
        ValueTypeRef element_value_binding{nullptr};
        ValueTypeRef element_delta_binding{nullptr};
    };

    static_assert(sizeof(TSSDataLayout) == sizeof(void *) * 5);
    static_assert(sizeof(TSDDataLayout) == sizeof(void *) * 10);

    struct SlotTSDataMutationResult
    {
        std::size_t slot{TS_DATA_NO_CHILD_ID};
        bool        changed{false};
        bool        constructed{false};
    };

    struct TSWDataLayout : TSDataLayout
    {
        ValueTypeRef element_binding{nullptr};
        ValueTypeRef time_binding{nullptr};
    };

    struct SizeTSWDataLayout : TSWDataLayout
    {
        std::size_t period{0};
        std::size_t min_period{0};
    };

    struct TimeTSWDataLayout : TSWDataLayout
    {
        TimeDelta     time_range{};
        TimeDelta     min_time_range{};
    };
}  // namespace hgraph

#endif  // HGRAPH_CPP_TS_DATA_TYPES_H
