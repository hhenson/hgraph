
#pragma once

#include <hgraph/hgraph_base.h>

namespace hgraph
{
    struct Notifiable {
        static constexpr uint32_t ALIVE_SENTINEL = 0xBEEF'CAFE;
        uint32_t sentinel_ = ALIVE_SENTINEL;

        virtual ~Notifiable() { sentinel_ = 0xDEAD'DEAD; }

        bool is_alive() const { return sentinel_ == ALIVE_SENTINEL; }

        virtual void notify(engine_time_t et) = 0;
    };
}
