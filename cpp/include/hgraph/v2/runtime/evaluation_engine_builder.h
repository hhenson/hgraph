//
// Created by Howard Henson on 20/04/2026.
//

#ifndef HGRAPH_CPP_ROOT_EVALUATION_ENGINE_BUILDER_H
#define HGRAPH_CPP_ROOT_EVALUATION_ENGINE_BUILDER_H

#include "hgraph/runtime/graph_executor.h"
#include "hgraph/util/date_time.h"

#include <hgraph/v2/runtime/evaluation_engine.h>
#include <hgraph/v2/types/value/view.h>

namespace hgraph::v2
{
    /**
     * This allows for building an evaluation engine.
     * Once all configuration properties are set the build() method can be called and it will
     * construct an EvaluationEngine instance based on the provided configuration.
     */
    struct EvaluationEngineBuilder
    {
        EvaluationEngineBuilder &graph_builder(const GraphExecutorBuilder &graph_builder);
        EvaluationEngineBuilder &start_time(engine_date_t start_time);
        EvaluationEngineBuilder &end_time(engine_date_t end_time);
        EvaluationEngineBuilder &run_mode(EvaluationMode run_mode);

        /**
         * Add positional arg; once a string arg is provided, it is no longer possible to add positional args.
         * @param view
         * @return
         */
        EvaluationEngineBuilder &arg(ValueView view);
        EvaluationEngineBuilder &args(ValueView view...);
        EvaluationEngineBuilder &arg(const std::string &arg, ValueView view);
        EvaluationEngineBuilder &args(std::pair<const std::string &, ValueView> args...);

        /**
         * Constructs an instance of the evaluation engine based on the current configuration.
         * @return The constructed EvaluationEngine.
         */
        EvaluationEngine build() const;
    };
}  // namespace hgraph::v2

#endif  // HGRAPH_CPP_ROOT_EVALUATION_ENGINE_BUILDER_H
