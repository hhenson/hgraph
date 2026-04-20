//
// Created by Howard Henson on 20/04/2026.
//

#ifndef HGRAPH_CPP_ROOT_EVALUATION_ENGINE_BUILDER_H
#define HGRAPH_CPP_ROOT_EVALUATION_ENGINE_BUILDER_H

#include <hgraph/runtime/graph_executor.h>
#include <hgraph/util/date_time.h>

#include <hgraph/v2/runtime/graph_executor.h>
#include <hgraph/v2/types/value/view.h>
#include <hgraph/v2/runtime/evaluation_life_cycle_observer.h>

namespace hgraph::v2
{
    /**
     * This allows for building an evaluation engine.
     * Once all configuration properties are set the build() method can be called and it will
     * construct an EvaluationEngine instance based on the provided configuration.
     */
    struct GraphExecutorBuilder
    {
        GraphExecutorBuilder &graph_builder(const GraphExecutorBuilder &graph_builder);
        GraphExecutorBuilder &start_time(engine_date_t start_time);
        GraphExecutorBuilder &end_time(engine_date_t end_time);
        GraphExecutorBuilder &run_mode(EvaluationMode run_mode);

        GraphExecutorBuilder &observer(EvaluationLifeCycleObserver &&observer);
        GraphExecutorBuilder &observers(EvaluationLifeCycleObserver &&observer...);

        /**
         * Add positional arg; once a string arg is provided, it is no longer possible to add positional args.
         * @param view
         * @return
         */
        GraphExecutorBuilder &arg(ValueView view);
        GraphExecutorBuilder &args(ValueView view...);
        GraphExecutorBuilder &arg(const std::string &arg, ValueView view);
        GraphExecutorBuilder &args(std::pair<const std::string &, ValueView> args...);

        /**
         * Constructs an instance of the evaluation engine based on the current configuration.
         * @return The constructed EvaluationEngine.
         */
        GraphExecutor build() const;
    };
}  // namespace hgraph::v2

#endif  // HGRAPH_CPP_ROOT_EVALUATION_ENGINE_BUILDER_H
