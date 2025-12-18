//
// Created by Howard Henson on 02/08/2025.
//

#ifndef NESTED_EVALUATION_ENGINE_H
#define NESTED_EVALUATION_ENGINE_H

#include <hgraph/runtime/evaluation_engine.h>

#include <chrono>
#include <limits>
#include <utility>

namespace hgraph {
    struct NestedNode;
    using nested_node_ptr = NestedNode*;

    struct NestedEngineEvaluationClock : EngineEvaluationClockDelegate {
        NestedEngineEvaluationClock(EngineEvaluationClock::ptr engine_evaluation_clock, nested_node_ptr nested_node);

        nested_node_ptr node() const;

        engine_time_t next_scheduled_evaluation_time() const override;

        void reset_next_scheduled_evaluation_time();

        void update_next_scheduled_evaluation_time(engine_time_t next_time) override;

    protected:
        // Protected to allow derived classes (e.g., MeshNestedEngineEvaluationClock) to access
        nested_node_ptr _nested_node;
        engine_time_t _nested_next_scheduled_evaluation_time{MAX_DT};
    };


    struct NestedEvaluationEngine : EvaluationEngineDelegate {
        NestedEvaluationEngine(EvaluationEngine::s_ptr engine, EngineEvaluationClock::s_ptr evaluation_clock);

        [[nodiscard]] engine_time_t start_time() const override;

        [[nodiscard]] EvaluationClock::s_ptr evaluation_clock() override;

        [[nodiscard]] const EngineEvaluationClock::s_ptr& engine_evaluation_clock() override;

    private:
        EngineEvaluationClock::s_ptr _engine_evaluation_clock;
        engine_time_t _nested_start_time;
    };
} // namespace hgraph

#endif  // NESTED_EVALUATION_ENGINE_H