#pragma once

#include <hgraph/util/date_time.h>

namespace hgraph
{
    class GraphView;

    /** Tracks the caller's inspection interval independently of the clock:
        a looping executor can reach its end time without evaluating it. */
    enum class StepInspection { Unavailable, Evaluating, Paused, Completed, Releasing, Released };
    struct RootStepInspection
    {
        DateTime time{MIN_DT};
        StepInspection status{StepInspection::Unavailable};
    };

    /** Encloses the entire root evaluation, including its before/after callbacks.
        A failed evaluation remains available for diagnostics and cannot be released. */
    class RootEvaluationStep
    {
      public:
        RootEvaluationStep(const GraphView &root, DateTime now);
        ~RootEvaluationStep()
        {
            if (inspection_.status == StepInspection::Evaluating)
                inspection_.status = StepInspection::Unavailable;
        }
        RootEvaluationStep(const RootEvaluationStep &) = delete;
        RootEvaluationStep &operator=(const RootEvaluationStep &) = delete;

        /** Run under the evaluation phase's embedding lock, before callbacks. */
        void retire_previous() const;
        void finish(bool completed) noexcept
        {
            inspection_.status = completed ? StepInspection::Completed : StepInspection::Paused;
        }

      private:
        RootStepInspection &inspection_;
        const GraphView &root_;
        DateTime time_;
    };
}
