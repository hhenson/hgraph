/**
 * Prepared input routes (RFC 0008 stage 5) — the shared helpers for node
 * front-ends that plan a per-slot route array (static nodes, lifted
 * kernels). One planned array per node with inputs; acquired by a
 * framework start callback AFTER the user start hook (input activation
 * precedes both), cleared by the framework stop callback after the user
 * stop hook (deactivation follows both). Non-owning throughout — validity
 * is re-checked per use via the trie handle, and the array's offset is
 * resolved through the node's OWN runtime layout
 * (``NodeView::prepared_input_routes``), never captured, so derived-type
 * rebuilds (error capture, passive inputs) relay or drop the field and
 * every consumer follows automatically.
 */
#ifndef HGRAPH_RUNTIME_PREPARED_INPUT_ROUTES_H
#define HGRAPH_RUNTIME_PREPARED_INPUT_ROUTES_H

#include <hgraph/runtime/node.h>
#include <hgraph/types/time_series/ts_input/detail.h>
#include <hgraph/types/time_series/ts_input/target_link.h>
#include <hgraph/types/time_series/ts_output.h>
#include <hgraph/types/time_series/ts_output/base_view.h>

#include <array>
#include <cstddef>

namespace hgraph
{
    template <std::size_t SlotCount>
    using PreparedRoutesStorage = std::array<detail::PreparedInputSlotRoute, SlotCount>;

    [[nodiscard]] inline detail::PreparedInputSlotRoute *
    prepared_input_routes_for(const NodeView &view) noexcept
    {
        return static_cast<detail::PreparedInputSlotRoute *>(view.prepared_input_routes());
    }

    /** The planned field for ``SlotCount`` canonical input slots; pass to
        ``node_storage_plan_for`` as an extra field. */
    template <std::size_t SlotCount>
    [[nodiscard]] NodeStorageField prepared_routes_storage_field()
    {
        return NodeStorageField{
            .name = node_prepared_inputs_field,
            .plan = &MemoryUtils::plan_for<PreparedRoutesStorage<SlotCount>>(),
        };
    }

    template <std::size_t SlotCount>
    void acquire_prepared_input_routes(const NodeView &view, DateTime evaluation_time)
    {
        auto *routes = prepared_input_routes_for(view);
        if (routes == nullptr) { return; }
        TSInputView root = view.input(evaluation_time);
        // Only a projectable (owned, child-carrying) root prepares; anything
        // else leaves the routes empty and every slot falls back to the
        // per-tick projection.
        if (!detail::has_input_children(root.data_view())) { return; }
        for (std::size_t slot = 0; slot < SlotCount; ++slot)
        {
            routes[slot] = root.prepare_child_route(slot);
        }
    }

    template <std::size_t SlotCount>
    void clear_prepared_input_routes(const NodeView &view) noexcept
    {
        auto *routes = prepared_input_routes_for(view);
        if (routes == nullptr) { return; }
        for (std::size_t slot = 0; slot < SlotCount; ++slot) { routes[slot] = {}; }
    }

    /**
     * Prepared output route (RFC 0008 stage 6): the node's own output and,
     * when it is a direct native atomic, its value memory and tracking
     * record. The output is embedded in the node's storage, so these are
     * fixed from start to stop; a typed ``Out<TS<T>>::set`` whose ops match
     * stores and records the modification directly (the ``mark_modified``
     * commit) instead of re-deriving the output view, validating a mutation
     * scope and locating the slot per tick. Non-native outputs keep
     * ``ready()`` (the output pointer) and fall back for the write.
     */
    struct PreparedOutputRoute
    {
        TSOutput       *output{nullptr};
        void           *native_value{nullptr};
        TSDataTracking *tracking{nullptr};
        const ValueOps *value_ops{nullptr};
        /** The native value's binding, for a writer that converts through the
            value ops' registered strategy (the Python bridge's result apply). */
        ValueTypeRef    value_binding{};
        /** A forwarding terminal (a map/mesh child's output into the parent's
            keyed slot) routed THROUGH its link to the bound target's native
            slot: the link, the target the route resolved — re-checked per use
            against the link's current target, since a parent can re-point a
            terminal between start and stop — and the link's own tracking
            record, which the write-through marks after the target's. */
        const TSOutputHandle *link_target_ref{nullptr};
        TSOutputHandle        link_target{};
        TSDataTracking       *link_tracking{nullptr};

        [[nodiscard]] bool ready() const noexcept { return output != nullptr; }
        [[nodiscard]] bool native() const noexcept
        {
            return native_value != nullptr && tracking != nullptr &&
                   (link_target_ref == nullptr || link_target_ref->same_as(link_target));
        }

        /** The ``mark_modified`` commit of a native store: record the slot's
            modification and bubble it once; for a link-routed terminal the
            link's own record follows, in the order the erased write-through
            keeps (the target's mutation, then the link's). */
        void commit(DateTime time) const
        {
            if (tracking->record_modified(time)) { tracking->parent.notify_child_modified(time); }
            if (link_tracking != nullptr && link_tracking->record_modified(time))
            {
                link_tracking->parent.notify_child_modified(time);
            }
        }
    };

    [[nodiscard]] inline PreparedOutputRoute *prepared_output_route_for(const NodeView &view) noexcept
    {
        return static_cast<PreparedOutputRoute *>(view.prepared_output());
    }

    /** The planned field for the output route; pass to ``node_storage_plan_for``. */
    [[nodiscard]] inline NodeStorageField prepared_output_storage_field()
    {
        return NodeStorageField{
            .name = node_prepared_output_field,
            .plan = &MemoryUtils::plan_for<PreparedOutputRoute>(),
        };
    }

    namespace prepared_output_detail
    {
        [[nodiscard]] inline bool direct_native_atomic(const TSDataOps &table) noexcept
        {
            return table.direct_native_value && table.value_view_impl == nullptr;
        }

        inline void resolve_native_slot(PreparedOutputRoute &route, const TSDataView &data, const TSDataOps &table)
        {
            const auto *layout  = table.layout_impl(table.context);
            route.value_ops     = layout->value_binding.ops();
            route.value_binding = layout->value_binding;
            route.native_value  = table.mutable_value_memory_impl(table.context, data.mutable_data());
            route.tracking      = table.mutable_tracking_impl(table.context, data.mutable_data());
        }

        /** Resolve a forwarding terminal's route through its bound link to a
            direct native target (the runtime library owns the link storage,
            so this step is out of line). Leaves ``route`` non-native when the
            output is not a link, the link is unbound, or the target is not a
            direct native atomic. */
        HGRAPH_EXPORT void resolve_forwarding_terminal(PreparedOutputRoute &route, const TSDataView &data);
    }  // namespace prepared_output_detail

    inline void acquire_prepared_output_route(const NodeView &view, DateTime evaluation_time)
    {
        auto *route = prepared_output_route_for(view);
        if (route == nullptr) { return; }
        *route = {};
        if (!view.has_output()) { return; }
        const TSOutputView output = view.output(evaluation_time);
        const TSDataView  &data   = output.data_view();
        if (!data.valid()) { return; }
        route->output = const_cast<TSOutput *>(output.output());
        const auto &table = data.ops();
        if (prepared_output_detail::direct_native_atomic(table))
        {
            prepared_output_detail::resolve_native_slot(*route, data, table);
            return;
        }
        // A forwarding terminal — a map/mesh child's output bound into the
        // parent's keyed slot — is written THROUGH to its target by the
        // target's own mutation, so a direct native target is routed exactly
        // as the node's own slot would be. The parent binds the terminal
        // before the child starts and may re-point it later; the route keeps
        // the target it resolved and ``native()`` re-checks it per use. A
        // target that is itself a link (a nested forwarding chain) keeps the
        // resolving path.
        prepared_output_detail::resolve_forwarding_terminal(*route, data);
    }

    inline void clear_prepared_output_route(const NodeView &view) noexcept
    {
        if (auto *route = prepared_output_route_for(view); route != nullptr) { *route = {}; }
    }
}  // namespace hgraph

#endif  // HGRAPH_RUNTIME_PREPARED_INPUT_ROUTES_H
