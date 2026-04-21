#include <hgraph/v2/runtime/graph_executor_builder.h>

namespace hgraph::v2
{
    GraphExecutorBuilder &GraphExecutorBuilder::graph_builder(const GraphBuilder &graph_builder)
    {
        static_cast<void>(graph_builder);
        return *this;
    }

    GraphExecutorBuilder &GraphExecutorBuilder::start_time(engine_date_t start_time)
    {
        static_cast<void>(start_time);
        return *this;
    }

    GraphExecutorBuilder &GraphExecutorBuilder::end_time(engine_date_t end_time)
    {
        static_cast<void>(end_time);
        return *this;
    }

    GraphExecutorBuilder &GraphExecutorBuilder::run_mode(EvaluationMode run_mode)
    {
        static_cast<void>(run_mode);
        return *this;
    }

    GraphExecutorBuilder &GraphExecutorBuilder::observer(ObserverSPtr observer)
    {
        static_cast<void>(observer);
        return *this;
    }

    GraphExecutorBuilder &GraphExecutorBuilder::observers(std::initializer_list<ObserverSPtr> observers)
    {
        static_cast<void>(observers);
        return *this;
    }

    GraphExecutorBuilder &GraphExecutorBuilder::arg(ValueView view)
    {
        static_cast<void>(view);
        return *this;
    }

    GraphExecutorBuilder &GraphExecutorBuilder::args(std::initializer_list<ValueView> views)
    {
        static_cast<void>(views);
        return *this;
    }

    GraphExecutorBuilder &GraphExecutorBuilder::arg(std::string_view arg, ValueView view)
    {
        static_cast<void>(arg);
        static_cast<void>(view);
        return *this;
    }

    GraphExecutorBuilder &GraphExecutorBuilder::args(std::initializer_list<NamedArg> args)
    {
        static_cast<void>(args);
        return *this;
    }

    GraphExecutor GraphExecutorBuilder::build() const { return {{},{}}; }

}

