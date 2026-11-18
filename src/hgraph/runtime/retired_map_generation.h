#ifndef HGRAPH_RUNTIME_RETIRED_MAP_GENERATION_H
#define HGRAPH_RUNTIME_RETIRED_MAP_GENERATION_H

#include <hgraph/util/date_time.h>

#include <optional>

namespace hgraph::runtime_detail
{
    // A retired bank stays readable for its removal cycle. Once drained, it
    // keeps its capacity for reuse, so checking its slots on later ticks is costly.
    template <typename EntryBank>
    void destroy_retired_map_entries_before(EntryBank &bank, std::optional<DateTime> &retired_at,
                                            DateTime evaluation_time) noexcept
    {
        if (retired_at && *retired_at < evaluation_time)
        {
            bank.destroy_all();
            retired_at.reset();
        }
    }
}  // namespace hgraph::runtime_detail

#endif  // HGRAPH_RUNTIME_RETIRED_MAP_GENERATION_H
