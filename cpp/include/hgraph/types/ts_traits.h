//
// Created by Howard Henson on 02/11/2025.
//

#ifndef HGRAPH_CPP_ROOT_TS_TRAITS_H
#define HGRAPH_CPP_ROOT_TS_TRAITS_H

#include <hgraph/types/notifiable.h>
#include <hgraph/util/date_time.h>
#include <functional>

namespace hgraph
{
    // Forward declare EvaluationEngineApi to avoid circular includes
    struct EvaluationEngineApi;

    /**
     * Interface for objects that can provide the current engine time.
     * Used by TSOutput/TSInput to determine timestamps for events.
     */
    struct CurrentTimeProvider
    {
        virtual ~CurrentTimeProvider()                                  = default;
        [[nodiscard]] virtual engine_time_t current_engine_time() const = 0;
    };

    /**
     * Interface for objects that can schedule notifications before/after evaluation.
     * This is used by reference time-series to update bindings at appropriate times.
     */
    struct EvaluationScheduler
    {
        virtual ~EvaluationScheduler() = default;
        virtual void add_before_evaluation_notification(std::function<void()> &&fn) = 0;
        virtual void add_after_evaluation_notification(std::function<void()> &&fn) = 0;
    };

    /**
     * Composite interface combining Notifiable, CurrentTimeProvider, and EvaluationScheduler.
     * This is typically implemented by Node to provide all context needed by time-series.
     */
    struct NotifiableContext : Notifiable, CurrentTimeProvider, EvaluationScheduler
    {};
}  // namespace hgraph

#endif  // HGRAPH_CPP_ROOT_TS_TRAITS_H
