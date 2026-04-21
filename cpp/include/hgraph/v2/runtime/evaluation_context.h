//
// Created by Howard Henson on 20/04/2026.
//

#ifndef HGRAPH_CPP_ROOT_GRAPHEXECUTIONCONTEXT_H
#define HGRAPH_CPP_ROOT_GRAPHEXECUTIONCONTEXT_H

#include <hgraph/runtime/evaluation_engine.h>

namespace hgraph::v2
{

    struct HGRAPH_EXPORT EvaluationClock
    {
        [[nodiscard]] engine_time_t evaluation_time() const;

        [[nodiscard]] engine_time_t now() const;

        [[nodiscard]] engine_time_t next_cycle_evaluation_time() const { return evaluation_time() + MIN_TD; }

        [[nodiscard]] engine_time_delta_t cycle_time();
    };

    struct HGRAPH_EXPORT EvaluationEngineApi
    {

        [[nodiscard]] EvaluationMode evaluation_mode() const;

        [[nodiscard]] engine_time_t start_time() const;

        [[nodiscard]] engine_time_t end_time() const;

        [[nodiscard]] EvaluationClock evaluation_clock() const;

        [[nodiscard]] EvaluationClock evaluation_clock();

        void request_engine_stop();

        [[nodiscard]] bool is_stop_requested();

        void add_before_evaluation_notification(std::function<void()> &&fn);

        void add_after_evaluation_notification(std::function<void()> &&fn);

        void add_life_cycle_observer(EvaluationLifeCycleObserver::s_ptr observer);

        void remove_life_cycle_observer(const EvaluationLifeCycleObserver::s_ptr &observer);
    };

}  // namespace hgraph::v2

#endif  // HGRAPH_CPP_ROOT_GRAPHEXECUTIONCONTEXT_H
