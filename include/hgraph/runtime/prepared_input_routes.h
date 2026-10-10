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

        [[nodiscard]] bool ready() const noexcept { return output != nullptr; }
        [[nodiscard]] bool native() const noexcept { return native_value != nullptr && tracking != nullptr; }
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
        if (table.direct_native_value && table.value_view_impl == nullptr)
        {
            const auto *layout   = table.layout_impl(table.context);
            route->value_ops     = layout->value_binding.ops();
            route->value_binding = layout->value_binding;
            route->native_value  = table.mutable_value_memory_impl(table.context, data.mutable_data());
            route->tracking      = table.mutable_tracking_impl(table.context, data.mutable_data());
        }
    }

    inline void clear_prepared_output_route(const NodeView &view) noexcept
    {
        if (auto *route = prepared_output_route_for(view); route != nullptr) { *route = {}; }
    }
}  // namespace hgraph

#endif  // HGRAPH_RUNTIME_PREPARED_INPUT_ROUTES_H
