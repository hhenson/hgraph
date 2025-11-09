//
// Created by Howard Henson on 02/11/2025.
//

#ifndef HGRAPH_CPP_ROOT_TS_TRAITS_H
#define HGRAPH_CPP_ROOT_TS_TRAITS_H

#include <hgraph/runtime/evaluation_engine.h>
#include <hgraph/util/date_time.h>

namespace hgraph
{
    /**
     * Interface for objects that can be notified of time-based events.
     * This is used throughout the hgraph system for propagating value changes.
     */
    struct Notifiable
    {
        virtual ~Notifiable() = default;

        /**
         * Notify this object of an event at the specified engine time.
         *
         * @param et The engine time at which the event occurred
         */
        virtual void notify(engine_time_t et) = 0;
    };

    struct CurrentTimeProvider
    {
        virtual ~CurrentTimeProvider()                                  = default;
        [[nodiscard]] virtual engine_time_t current_engine_time() const = 0;
    };

    struct NotifiableContext : Notifiable, CurrentTimeProvider, EvaluationScheduler
    {};
}  // namespace hgraph

#endif  // HGRAPH_CPP_ROOT_TS_TRAITS_H
