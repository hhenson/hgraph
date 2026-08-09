#ifndef HGRAPH_RUNTIME_REGISTRY_SNAPSHOT_DETAIL_H
#define HGRAPH_RUNTIME_REGISTRY_SNAPSHOT_DETAIL_H

#include <cstddef>

namespace hgraph::detail
{
    [[nodiscard]] std::size_t node_runtime_type_count() noexcept;
    [[nodiscard]] std::size_t graph_program_count() noexcept;
    [[nodiscard]] std::size_t graph_runtime_type_count() noexcept;
    [[nodiscard]] std::size_t executor_runtime_type_count() noexcept;
}  // namespace hgraph::detail

#endif  // HGRAPH_RUNTIME_REGISTRY_SNAPSHOT_DETAIL_H
