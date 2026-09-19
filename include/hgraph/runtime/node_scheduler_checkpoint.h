#ifndef HGRAPH_RUNTIME_NODE_SCHEDULER_CHECKPOINT_H
#define HGRAPH_RUNTIME_NODE_SCHEDULER_CHECKPOINT_H

#include <hgraph/hgraph_export.h>
#include <hgraph/util/date_time.h>
#include <string>
#include <utility>
#include <vector>

namespace hgraph
{
    /** Pending logical-time alarms, independent of user recordable state.
     * Entries are ordered by (time, tag); nonempty tags are unique. The live
     * scheduler rebuilds its tag index when restoring this owned image.
     * SingleShotScheduler has no checkpoint representation.
     */
    struct HGRAPH_CLASS_EXPORT NodeSchedulerCheckpoint
    {
        std::vector<std::pair<DateTime, std::string>> events{};
    };
}

#endif
