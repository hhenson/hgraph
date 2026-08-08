#ifndef HGRAPH_RUNTIME_RACE_TSD_NODE_H
#define HGRAPH_RUNTIME_RACE_TSD_NODE_H

#include <hgraph/hgraph_export.h>
#include <hgraph/runtime/node.h>

namespace hgraph
{
    /**
     * The keyed race node (hgraph's ``reduce_tsd_with_race``): over a
     * ``TSD<K, REF<OUT>>`` the output forwards the reference of the key whose
     * TARGET was valid first; a winner whose target goes invalid (or whose
     * key is removed) triggers a re-race. Candidates with a valid reference
     * but an invalid target are PENDING: the node subscribes to their targets
     * directly (the from-REF alternative machinery is fixed-tree only, so the
     * dynamic keyed values input is managed by hand - hgraph's ``_values``).
     *
     * ``tsd_schema`` is the resolved ``TSD<K, REF<OUT>>``; the node output is
     * ``REF<OUT>``.
     */
    [[nodiscard]] HGRAPH_EXPORT NodeBuilder make_race_tsd_node(const TSValueTypeMetaData &tsd_schema);
}  // namespace hgraph

#endif  // HGRAPH_RUNTIME_RACE_TSD_NODE_H
