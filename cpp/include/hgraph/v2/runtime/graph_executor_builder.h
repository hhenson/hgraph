//
// Created by Howard Henson on 20/04/2026.
//

#ifndef HGRAPH_CPP_ROOT_EVALUATION_ENGINE_BUILDER_H
#define HGRAPH_CPP_ROOT_EVALUATION_ENGINE_BUILDER_H

#include <functional>
#include <hgraph/runtime/graph_executor.h>
#include <hgraph/util/date_time.h>

#include <hgraph/v2/runtime/graph_executor.h>
#include <hgraph/v2/runtime/evaluation_life_cycle_observer.h>
#include <hgraph/v2/types/value/view.h>

#include <initializer_list>
#include <string_view>
#include <utility>

namespace hgraph::v2
{
    struct GraphBuilder;

    /**
     * This allows for building a graph executor.
     * Once all configuration properties are set the build() method can be called and it will
     * construct a GraphExecutor instance based on the provided configuration.
     */
    struct GraphExecutorBuilder
    {
        using ObserverSPtr = EvaluationLifeCycleObserver::s_ptr;
        using NamedArg = std::pair<std::string_view, ValueView>;

        GraphExecutorBuilder &graph_builder(const GraphBuilder &graph_builder);
        GraphExecutorBuilder &start_time(engine_date_t start_time);
        GraphExecutorBuilder &end_time(engine_date_t end_time);
        GraphExecutorBuilder &run_mode(EvaluationMode run_mode);

        GraphExecutorBuilder &observer(ObserverSPtr observer);
        GraphExecutorBuilder &observers(std::initializer_list<ObserverSPtr> observers);

        /**
         * Add positional arg; once a string arg is provided, it is no longer possible to add positional args.
         * @param view
         * @return
         */
        GraphExecutorBuilder &arg(ValueView view);
        GraphExecutorBuilder &args(std::initializer_list<ValueView> views);
        GraphExecutorBuilder &arg(std::string_view arg, ValueView view);
        GraphExecutorBuilder &args(std::initializer_list<NamedArg> args);

        /**
         * Constructs an instance of the graph executor based on the current configuration.
         * @return The constructed GraphExecutor.
         */
        [[nodiscard]] GraphExecutor build() const;
    };
}  // namespace hgraph::v2

#endif  // HGRAPH_CPP_ROOT_EVALUATION_ENGINE_BUILDER_H
