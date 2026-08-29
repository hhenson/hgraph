#ifndef HGRAPH_FABRIC_HISTORY_H
#define HGRAPH_FABRIC_HISTORY_H

#include <hgraph/fabric/config.h>
#include <hgraph/fabric/export.h>

#include <hgraph/types/frame.h>

#include <optional>

namespace hgraph::fabric
{
    /** Load the newest stored value for ``data_id`` whose revision timestamp
        is at or before ``as_of``.

        This is a synchronous, non-graph point lookup. It does not coordinate
        dependency versions or construct a consistency forest. Absence before
        the requested instant returns ``std::nullopt``; corrupt metadata or a
        missing referenced frame fails explicitly. */
    [[nodiscard]] HGRAPH_FABRIC_EXPORT std::optional<Frame>
    load_data_as_of(const FabricConfig &config, Str data_id, DateTime as_of);

}  // namespace hgraph::fabric

#endif  // HGRAPH_FABRIC_HISTORY_H
