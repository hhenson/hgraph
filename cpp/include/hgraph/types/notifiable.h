
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

        /**
         * @brief Called by ObserverList::~ObserverList() when the source is being destroyed.
         *
         * Allows subscribers to clear their back-references to the (now-dying) observer list,
         * preventing use-after-free when unbind() is later called.
         *
         * Default implementation is a no-op. Override in LinkTarget and ActiveNotifier
         * to clear observer_data / owning_input pointers.
         */
        virtual void on_source_destroyed() {}
    };
}
