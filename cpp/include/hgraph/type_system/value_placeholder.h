#ifndef HGRAPH_VALUE_PLACEHOLDER_H
#define HGRAPH_VALUE_PLACEHOLDER_H

#include <hgraph/hgraph_base.h>

namespace hgraph {
    /**
     * placeholder for value storage, getting the complier to ork out sizes and alignments
     */
    struct ValuePlaceholder {
        union {
            int64_t i;
            uint64_t u;
            float f;
            engine_time_t t;
            engine_time_delta_t d;
            void* p;
            uintptr_t up;
            std::byte b;
        };        
    };
} // namespace hgraph

#endif // HGRAPH_VALUE_PLACEHOLDER_H