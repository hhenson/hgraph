
#pragma once

#include <hgraph/hgraph_base.h>

namespace hgraph
{
    struct Notifiable {
        virtual ~Notifiable() = default;

        virtual void notify(engine_time_t et) = 0;
    };
}
