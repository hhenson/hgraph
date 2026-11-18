#ifndef HGRAPH_TYPES_KEYED_NOTIFIABLE_H
#define HGRAPH_TYPES_KEYED_NOTIFIABLE_H

#include <hgraph/types/notifiable.h>

namespace hgraph
{
    class ValueView;

    /** Membership relay between structural projections. A concrete key change
        is routed separately from a whole-source replacement (notify). */
    struct HGRAPH_CLASS_EXPORT KeyedNotifiable : Notifiable
    {
        virtual void keys_replaced(DateTime modified_time) = 0;
        virtual void key_modified(const ValueView &key, DateTime modified_time) = 0;
    };
}
#endif
