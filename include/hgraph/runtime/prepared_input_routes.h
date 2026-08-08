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
}  // namespace hgraph

#endif  // HGRAPH_RUNTIME_PREPARED_INPUT_ROUTES_H
