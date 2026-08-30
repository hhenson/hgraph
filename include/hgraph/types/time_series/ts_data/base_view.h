#ifndef HGRAPH_CPP_TS_DATA_BASE_VIEW_H
#define HGRAPH_CPP_TS_DATA_BASE_VIEW_H

#include <hgraph/types/time_series/ts_data/ops.h>
#include <hgraph/types/time_series/endpoint_owner.h>
#include <hgraph/types/utils/memory_utils.h>
#include <hgraph/types/value/value_view.h>
#include <hgraph/util/date_time.h>
#include <cassert>
#include <concepts>
#include <cstddef>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

namespace hgraph
{
    class GraphView;
    class NodeView;
    class TSDataView;

    namespace detail
    {
        class TSOutputAlternativeStore;
        struct TSInputChildProjection;
        [[nodiscard]] TSInputChildProjection input_child_projection(const TSDataView &parent, std::size_t index);
        void attach_owned_ts_data_parents(TSDataView root);
        void attach_owned_ts_data_parent(TSDataView child, const TSDataView &parent, std::size_t child_id);
        void invalidate_owned_ts_data_tree(TSDataView root) noexcept;
    }

    template <typename DataOps = TSDataOps>
    class TSDataStorageRef
    {
      public:
        constexpr TSDataStorageRef() noexcept = default;

        constexpr TSDataStorageRef(TSRoleTypeRef type, void *data) noexcept
            requires std::same_as<DataOps, TSDataOps>
            : type_(type), data_(data)
        {
        }

        constexpr TSDataStorageRef(TSRoleTypeRef type, const void *data) noexcept
            requires std::same_as<DataOps, TSDataOps>
            : TSDataStorageRef(type, const_cast<void *>(data))
        {
        }

        TSDataStorageRef(TSDataStorageRef<> storage, TSTypeKind expected_kind)
            requires(!std::same_as<DataOps, TSDataOps>)
            : type_(storage.type_), data_(storage.data_)
        {
            validate_kind(storage, expected_kind);
        }

        /** True when both the binding and data pointer are present. */
        [[nodiscard]] constexpr bool has_value() const noexcept { return type_.bound() && data_ != nullptr; }
        [[nodiscard]] constexpr bool valid() const noexcept
        {
            return has_value();
        }
        [[nodiscard]] constexpr explicit operator bool() const noexcept { return valid(); }

        /** True when the reference carries a binding, even without data. */
        [[nodiscard]] constexpr bool bound() const noexcept { return type_.bound(); }

        /** Bound TSData type record, schema, and memory. */
        [[nodiscard]] constexpr TSRoleTypeRef storage_type() const noexcept { return type_; }
        [[nodiscard]] constexpr TSRoleTypeRef type_ref() const noexcept { return type_; }
        [[nodiscard]] const TSValueTypeMetaData *schema() const noexcept { return type_.schema(); }
        [[nodiscard]] constexpr void *data() const noexcept { return data_; }

        /** Generic storage identity over the same binding and memory. */
        [[nodiscard]] constexpr TSDataStorageRef<> storage_ref() const noexcept
        {
            return TSDataStorageRef<>{type_, data_};
        }

        /** Checked access to the ops table with the ref's requested type. */
        [[nodiscard]] const DataOps &ops() const
        {
            if constexpr (std::same_as<DataOps, TSDataOps>)
            {
                return has_value() ? type_.ops_ref() : ts_data_detail::default_ts_data_ops();
            }
            else
            {
                if (!has_value())
                {
                    // Unbound dispatches into the family's no-value sentinel:
                    // reads answer empty, mutations throw (audit sentinel
                    // design, 2026-08-16). Order matters: TSDDataOps derives
                    // TSSDataOps.
                    if constexpr (std::same_as<DataOps, TSDDataOps>)
                    {
                        return ts_data_detail::default_tsd_data_ops();
                    }
                    else if constexpr (std::same_as<DataOps, TSSDataOps>)
                    {
                        return ts_data_detail::default_tss_data_ops();
                    }
                    else if constexpr (std::same_as<DataOps, TSWDataOps>)
                    {
                        return ts_data_detail::default_tsw_data_ops();
                    }
                    else { return ts_data_detail::default_indexed_ts_data_ops(); }
                }
                return static_cast<const DataOps &>(type_.ops_ref());
            }
        }

        void reset() noexcept
        {
            type_ = {};
            data_ = nullptr;
        }

      private:
        static void validate_kind(TSDataStorageRef<> storage, TSTypeKind expected_kind)
            requires(!std::same_as<DataOps, TSDataOps>)
        {
            // A wholly unbound ref (no type) dispatches into the family's
            // no-value sentinel. A ref that CARRIES a type — even with null
            // data, e.g. a dict's bound element type for an absent key —
            // validates against that type: casting a bound atomic child
            // as_dict() is a programming error whether or not data is
            // present (review finding on #492).
            const auto type = storage.storage_type();
            if (!type) { return; }
            if (type.ops_ref().kind != expected_kind)
            {
                throw std::invalid_argument("TSDataStorageRef requires the matching TSData ops kind");
            }
        }

        TSRoleTypeRef type_{};
        void         *data_{nullptr};

        template <typename>
        friend class TSDataStorageRef;
    };

    using IndexedTSDataStorageRef = TSDataStorageRef<IndexedTSDataOps>;
    using TSSDataStorageRef       = TSDataStorageRef<TSSDataOps>;
    using TSDDataStorageRef       = TSDataStorageRef<TSDDataOps>;
    using TSWDataStorageRef       = TSDataStorageRef<TSWDataOps>;

    static_assert(sizeof(TSDataStorageRef<>) == sizeof(void *) * 2);
    static_assert(sizeof(IndexedTSDataStorageRef) == sizeof(void *) * 2);
    static_assert(std::is_trivially_copyable_v<TSDataStorageRef<>>);

    class HGRAPH_CLASS_EXPORT TSDataView
    {
      public:
        constexpr TSDataView() noexcept = default;

        TSDataView(TSRoleTypeRef type, void *data) noexcept : storage_(type, data) {}
        TSDataView(TSRoleTypeRef type, const void *data) noexcept : storage_(type, data) {}
        explicit TSDataView(TSDataStorageRef<> storage) noexcept : storage_(storage) {}

        TSDataView(const TSDataView &) = delete;
        TSDataView &operator=(const TSDataView &) = delete;
        TSDataView(TSDataView &&) noexcept = default;
        TSDataView &operator=(TSDataView &&) noexcept = default;

        /** Explicitly recreate a transient cursor over the same TSData storage. */
        [[nodiscard]] TSDataView borrowed_ref() const noexcept { return TSDataView{storage_}; }

        /** Copyable borrowed storage identity used by handles and owner links. */
        [[nodiscard]] TSDataStorageRef<> storage_ref() const noexcept { return storage_; }

        /** True when the view has both a binding and a live TSData memory pointer. */
        [[nodiscard]] bool valid() const noexcept { return storage_.has_value(); }
        explicit operator bool() const noexcept { return valid(); }

        /** Canonical type record that describes this TSData memory region. */
        [[nodiscard]] TSRoleTypeRef storage_type() const noexcept { return storage_.storage_type(); }
        [[nodiscard]] TSRoleTypeRef type_ref() const noexcept { return storage_.type_ref(); }

        /** Time-series schema associated with the binding, or null when unbound. */
        [[nodiscard]] const TSValueTypeMetaData *schema() const noexcept { return storage_.schema(); }

        /** Borrowed pointer to the underlying TSData memory. */
        [[nodiscard]] const void *data() const noexcept { return storage_.data(); }

        /** Parent link stored in this TSData node's tracking region. */
        [[nodiscard]] const TSParentLink &parent_link() const;

        /** Parent-relative id for this node, or ``TS_DATA_NO_CHILD_ID`` for roots. */
        [[nodiscard]] std::size_t child_id() const;

        /** True when this view's tracking record carries a parent link. */
        [[nodiscard]] bool has_parent() const;

        /** Integer path from the root TSData node to this node. */
        [[nodiscard]] std::vector<std::size_t> path_from_root() const;

        /** Root TSData view reached by following parent links. */
        [[nodiscard]] TSDataView root_view() const;

        /** Endpoint parent handle reached from this TSData view's root. */
        [[nodiscard]] TSParentLink root_endpoint_owner() const noexcept;

        /** Owning node for node-owned endpoint TSData, or an empty view when detached. */
        [[nodiscard]] NodeView owner_node() const;

        /** Owning graph for node-owned endpoint TSData, or an empty view when detached. */
        [[nodiscard]] GraphView owner_graph() const;

        /** Writable memory pointer; throws unless the ops table allows mutation. */
        [[nodiscard]] void *mutable_data() const;

        /** Checked access to the type-erased operation table. */
        [[nodiscard]] const TSDataOps &ops() const;

        /** Common TSData layout prefix for this node. */
        [[nodiscard]] const TSDataLayout &layout() const;

        /** Local tracking record for modification time and parent link. */
        [[nodiscard]] const TSDataTracking &tracking() const;

        /** Current value view for this TSData node. */
        [[nodiscard]] ValueView value() const;

        /**
         * Payload memory for typed in-place reads, or null when the fast
         * path does not apply. Non-null only when the installed ops declare
         * ``direct_native_value``, take the plain layout+memory route (no
         * ``value_view_impl`` override), and the value binding's ops table
         * is exactly ``expected_value_ops`` — the same address identity
         * ``checked_as`` relies on, so a non-null result may be cast
         * straight to the native type. Reads the identical memory the
         * erased ``value()`` path exposes; the read-side twin of
         * ``TSDataMutationView::mutable_value``.
         */
        [[nodiscard]] const void *try_native_value_memory(const void *expected_value_ops) const noexcept;

        /** Delta value for ``evaluation_time``, or a typed null view when unchanged. */
        [[nodiscard]] ValueView delta_value(DateTime evaluation_time) const;

#if HGRAPH_ENABLE_PYTHON_USER_NODES
        /**
         * Export the current value through this live representation's
         * type-erased TSDataOps table.
         *
         * Structural implementations must recursively invoke child TSDataOps
         * and produce the complete public Python shape. Callers must not
         * switch on TSTypeKind and reconstruct that shape themselves.
         */
        [[nodiscard]] nb::object value_to_python() const;

        /**
         * Export the delta for ``evaluation_time`` through this live
         * representation's type-erased TSDataOps table.
         *
         * As with value_to_python(), recursive shape conversion belongs to
         * the concrete TSData strategy, not its Python-facing caller.
         */
        [[nodiscard]] nb::object delta_value_to_python(DateTime evaluation_time) const;
#endif

        /** Last evaluation time that modified this TSData node, or ``MIN_DT`` if never valid. */
        [[nodiscard]] DateTime last_modified_time() const;

        /** True when this node was modified at ``evaluation_time``. */
        [[nodiscard]] bool modified(DateTime evaluation_time) const;

        /** Register / remove a per-level modification observer. */
        void subscribe(Notifiable *observer) const;
        void unsubscribe(Notifiable *observer) const;
        void replace_observer(Notifiable *observer, Notifiable *replacement) const;

        /** True when this level currently has one or more observers. */
        [[nodiscard]] bool has_observers() const;

        /** Number of observers currently registered at this level. */
        [[nodiscard]] std::size_t observer_count() const;

        /** True when the node currently holds a valid time-series value. */
        [[nodiscard]] bool has_current_value() const;

        /** True when this node and all required descendants are valid. */
        [[nodiscard]] bool all_valid() const;

        /** Occupied and retained heap bytes owned by this TSData root and its owned descendants. */
        [[nodiscard]] DynamicStorageMetrics dynamic_storage_metrics() const noexcept;

        /** Shape-erased indexed child projection for TSB/TSL-like data. */
        [[nodiscard]] std::size_t indexed_child_count() const;
        [[nodiscard]] TSDataView indexed_child_at(std::size_t index) const;
        [[nodiscard]] TSDataView ensure_indexed_child_at(std::size_t index) const;

        /** Clear collection-shaped TSData; returns false for non-collection shapes. */
        [[nodiscard]] bool clear_collection(DateTime modified_time) const;

        [[nodiscard]] TSSDataView as_set() &;
        [[nodiscard]] TSSDataView as_set() const &;
        void as_set() && = delete;
        void as_set() const && = delete;
        [[nodiscard]] TSDDataView as_dict() &;
        [[nodiscard]] TSDDataView as_dict() const &;
        void as_dict() && = delete;
        void as_dict() const && = delete;
        [[nodiscard]] TSBDataView as_bundle() &;
        [[nodiscard]] TSBDataView as_bundle() const &;
        void as_bundle() && = delete;
        void as_bundle() const && = delete;
        [[nodiscard]] TSLDataView as_list() &;
        [[nodiscard]] TSLDataView as_list() const &;
        void as_list() && = delete;
        void as_list() const && = delete;
        [[nodiscard]] TSWDataView as_window() &;
        [[nodiscard]] TSWDataView as_window() const &;
        void as_window() && = delete;
        void as_window() const && = delete;

        [[nodiscard]] TSDataMutationView begin_mutation(DateTime evaluation_time) const;

      private:
        friend class TSDataMutationView;
        friend class IndexedTSDataView;
        friend class TSInput;
        friend class TSOutput;
        friend class TSInputView;
        friend class detail::TSOutputAlternativeStore;
        friend detail::TSInputChildProjection detail::input_child_projection(
            const TSDataView &parent,
            std::size_t index);
        friend void detail::attach_owned_ts_data_parents(TSDataView root);
        friend void detail::attach_owned_ts_data_parent(
            TSDataView child, const TSDataView &parent, std::size_t child_id);
        friend void detail::invalidate_owned_ts_data_tree(TSDataView root) noexcept;
        friend class TSDProxy;
        friend class TSDDataView;
        friend class TSDDataMutationView;

        TSDataView(TSRoleTypeRef type, void *data, const TSDataView &parent, std::size_t child_id);
        TSDataView(TSRoleTypeRef type, const void *data, const TSDataView &parent, std::size_t child_id);

        void require_live(const char *what) const;
        [[nodiscard]] TSDataTracking &mutable_tracking() const;
        void bind_parent(const TSDataView &parent, std::size_t child_id) const;
        void bind_parent(const NodeView &parent, TSEndpointOwnerPort port) const;
        void bind_parent(TSInput &parent, std::size_t child_id) const;
        void bind_parent(TSOutput &parent, std::size_t child_id) const;

        TSDataStorageRef<> storage_{};
    };

    class HGRAPH_CLASS_EXPORT TSDataMutationView
    {
      public:
        /** Begin a mutation-capable projection of a live TSData view. */
        TSDataMutationView(TSDataView view, DateTime evaluation_time);

        TSDataMutationView(const TSDataMutationView &) = delete;
        TSDataMutationView &operator=(const TSDataMutationView &) = delete;

        TSDataMutationView(TSDataMutationView &&other) noexcept;

        TSDataMutationView &operator=(TSDataMutationView &&) = delete;

        ~TSDataMutationView() noexcept;

        /** Transient projection of the storage being mutated. */
        [[nodiscard]] TSDataView view() const noexcept;

        /** Operation table used by the underlying TSData binding. */
        [[nodiscard]] const TSDataOps &ops() const;

        /** Writable TSData memory; throws if this mutation view has been moved from. */
        [[nodiscard]] void *mutable_data() const;

        /** Current value view after any mutations applied through this scope. */
        [[nodiscard]] ValueView value() const;

        /**
         * Writable view over the current value for typed in-place assignment.
         * Returns an UNBOUND view unless the installed ops declare
         * ``direct_native_value`` — storages with representation side rules
         * (python caches, structural kinds) must write through
         * ``copy_value_from`` instead. A successful write is committed with
         * ``mark_modified()``; that pair is the typed fast path behind
         * ``Out<TS<T>>::set``.
         */
        [[nodiscard]] ValueView mutable_value() const;

        /** Delta value for ``evaluation_time`` using the underlying TSData semantics. */
        [[nodiscard]] ValueView delta_value(DateTime evaluation_time) const;

        /** Runtime time associated with this mutation scope. */
        [[nodiscard]] DateTime current_mutation_time() const { return mutation_time_; }

        /** True when the underlying TSData was modified at ``evaluation_time``. */
        [[nodiscard]] bool modified(DateTime evaluation_time) const;

        /** True when this scope's storage was modified at the scope's own
            mutation time (the niladic form the root-mutation API carried). */
        [[nodiscard]] bool modified() const { return modified(mutation_time_); }

        /**
         * Mark this TSData node as modified and notify its parent once.
         *
         * Repeated calls in the same evaluation time are coalesced by
         * ``last_modified_time`` and therefore do not re-notify ancestors.
         */
        void mark_modified();

        /** Copy a value-layer view into this TSData node using the binding's type-erased copy op. */
        [[nodiscard]] bool copy_value_from(const ValueView &source);

        /** Move an owned value into this TSData node using the binding's type-erased move op. */
        [[nodiscard]] bool move_value_from(Value &&source);

        /** Move from externally owned writable storage without fabricating an owner. */
        [[nodiscard]] bool move_value_from(ValueView source);

        /**
         * Invalidate this value and its statically indexed descendants.
         *
         * Observers are notified at the mutation time before the local
         * validity timestamp is cleared. Repeating the operation on an
         * already-invalid value is a no-op.
         */
        [[nodiscard]] bool invalidate();

#if HGRAPH_ENABLE_PYTHON_USER_NODES
        /** Apply a Python object through the TSData binding's type-erased conversion op. */
        [[nodiscard]] bool from_python(nb::handle source);
#endif

      private:
        friend class TSWDataMutationView;

        [[nodiscard]] void *mutable_data(const TSDataOps &table) const
        {
            require_active_mutation();
            assert(table.allows_mutation);
            static_cast<void>(table);
            return storage_.data();
        }

        [[nodiscard]] bool modified(const TSDataOps &table, DateTime evaluation_time) const
        {
            require_active_mutation();
            return evaluation_time != MIN_DT &&
                   table.tracking_impl(table.context, storage_.data())->last_modified_time == evaluation_time;
        }

        void mark_modified(const TSDataOps &table)
        {
            // ONE tracking fetch for the whole commit: recording and the
            // parent notify read the same record, but the split helpers
            // fetched it twice per write (deferred audit item, unblocked
            // once #488 merged).
            require_active_mutation();
            auto &state = *table.mutable_tracking_impl(table.context, storage_.data());
            if (state.record_modified(mutation_time_))
            {
                state.parent.notify_child_modified(mutation_time_);
            }
        }

        void require_active_mutation() const
        {
            // Scope LIVENESS stays a runtime guard: the move constructor
            // clears storage_ and mutation_time_, so a moved-from mutation
            // view must keep its documented exception in release builds too
            // (review finding on #488) — two register compares. The
            // type/mutability validation, by contrast, is immutable for the
            // scope and trusted after construction (validate_mutation_view).
            if (mutation_time_ == MIN_DT || !storage_.has_value())
            {
                throw std::logic_error("TSData mutation requires an active mutation scope");
            }
        }

        void validate_mutation_view() const;
        [[nodiscard]] bool record_modified_local() const;
        [[nodiscard]] bool record_modified_local(const TSDataOps &table) const
        {
            require_active_mutation();
            auto &state = *table.mutable_tracking_impl(table.context, storage_.data());
            return state.record_modified(mutation_time_);
        }

        void notify_parent_modified() const;
        void notify_parent_modified(const TSDataOps &table) const
        {
            table.tracking_impl(table.context, storage_.data())->parent.notify_child_modified(mutation_time_);
        }

        TSDataStorageRef<> storage_{};
        DateTime      mutation_time_{MIN_DT};
    };

    /** Apply a slot mutation result to the owning mutation view's timestamp state. */
    void apply_slot_mutation_result(TSDataMutationView &mutation, const SlotTSDataMutationResult &result);
}  // namespace hgraph

#endif  // HGRAPH_CPP_TS_DATA_BASE_VIEW_H
