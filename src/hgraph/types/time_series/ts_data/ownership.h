#ifndef HGRAPH_CPP_TS_DATA_OWNERSHIP_H
#define HGRAPH_CPP_TS_DATA_OWNERSHIP_H

#include <hgraph/types/time_series/ts_data/types.h>

#include <cstddef>

namespace hgraph::detail
{
    struct TSDataOwnedChild
    {
        TSRoleTypeRef type{};
        void            *data{nullptr};
        std::size_t      parent_child_id{TS_DATA_NO_CHILD_ID};
        bool             attach_parent{true};
    };

    struct TSDataOwnershipOps
    {
        using child_count_fn = std::size_t (*)(const void *context, const void *memory) noexcept;
        using child_at_fn = TSDataOwnedChild (*)(const void *context, void *memory, std::size_t index) noexcept;
        using stop_fn = void (*)(const void *context, void *memory) noexcept;
        using auxiliary_metrics_fn = DynamicStorageMetrics (*)(
            const void *context, const void *memory) noexcept;

        child_count_fn child_count{nullptr};
        child_at_fn    child_at{nullptr};
        stop_fn        stop{nullptr};
        auxiliary_metrics_fn auxiliary_dynamic_storage{nullptr};
    };

    void attach_owned_ts_data_parents(TSDataView root);
    void attach_owned_ts_data_parent(TSDataView child, const TSDataView &parent, std::size_t child_id);
    void stop_owned_ts_data_tree(TSDataView root) noexcept;
    void invalidate_owned_ts_data_tree(TSDataView root) noexcept;
    /** Auxiliary heap storage reachable through an owned endpoint tree. */
    [[nodiscard]] DynamicStorageMetrics owned_ts_data_auxiliary_dynamic_storage(
        TSDataView root) noexcept;
}  // namespace hgraph::detail

#endif  // HGRAPH_CPP_TS_DATA_OWNERSHIP_H
