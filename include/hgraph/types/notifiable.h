#ifndef HGRAPH_CPP_NOTIFIABLE_H
#define HGRAPH_CPP_NOTIFIABLE_H

#include <hgraph/hgraph_export.h>
#include <hgraph/util/date_time.h>

namespace hgraph
{
    struct TSDataTracking;

    /**
     * Runtime notification target used by time-series observers.
     *
     * Time-series data does not own observers. A subscriber registers a
     * ``Notifiable`` pointer at the level it wants to observe and remains
     * responsible for unregistering before either side is destroyed.
     * ``notify`` is called once per observed level and evaluation time when the
     * level first records a modification.
     */
    struct HGRAPH_EXPORT Notifiable
    {
        virtual ~Notifiable() = default;

        /** Receive a time-series modification notification. */
        virtual void notify(DateTime modified_time) = 0;

        /** The observed TSData tracking record is about to be destroyed. */
        virtual void source_invalidated(const TSDataTracking *) noexcept {}
    };
}  // namespace hgraph

#endif  // HGRAPH_CPP_NOTIFIABLE_H
