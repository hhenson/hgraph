#include "spawn_test_graphs.h"

namespace
{
    template <typename Graph, typename... Inputs>
    void register_plain()
    {
        register_spawn_worker_recipe(typeid(Graph).name(), +[](std::string_view) {
            const std::array<const TSValueTypeMetaData *, sizeof...(Inputs)> schemas{schema_descriptor<Inputs>::ts_meta()...};
            return prepare_spawn_worker(fn<Graph>(), schemas);
        });
    }
    template <typename Graph, typename... Inputs>
    void register_trace()
    {
        register_spawn_worker_recipe(typeid(Graph).name(), +[](std::string_view bootstrap) {
            static Trace trace;
            trace.remote = true;
            trace.path = bootstrap;
            const std::array<const TSValueTypeMetaData *, sizeof...(Inputs)> schemas{schema_descriptor<Inputs>::ts_meta()...};
            const auto stage = spawn_fn<Graph>(arg<"trace">(&trace));
            return prepare_spawn_worker(stage.function, schemas);
        });
    }
    template <typename S>
    void register_boundary()
    {
        register_plain<Identity<S>, S>();
        register_trace<Sink<S>, S>();
    }
}

namespace hgraph_test
{
    void register_spawn_test_recipes()
    {
        prepare();
        register_boundary<TS<Int>>();
        register_boundary<SIGNAL>();
        register_boundary<TSS<Int>>();
        register_boundary<Dict>();
        register_boundary<TSL<TS<Int>, 2>>();
        register_boundary<TSL<TS<Int>>>();
        register_boundary<TSB<"SpawnTestRow", Field<"value", TS<Int>>, Field<"label", TS<Str>>>>();
        register_boundary<TSD<Str, TSS<Int>>>();
        register_boundary<TSW<Int, 3, 1>>();
        register_boundary<TSWDuration<Int, 3, 0>>();
        register_trace<TimerGraph>();
        register_trace<InputTimer, TS<Int>>();
        register_plain<Silent, TS<Int>>();
        register_plain<Failure, TS<Int>>();
        register_plain<Crash, TS<Int>>();
        register_plain<Hang, TS<Int>>();
        register_plain<StartFailure, TS<Int>>();
        register_plain<StopFailure, TS<Int>>();
        register_plain<RequestStop, TS<Int>>();
        register_plain<StopOnStart, TS<Int>>();
        register_plain<Nested<NestedKind::Map>, Dict>();
        register_plain<Nested<NestedKind::Reduce>, Dict>();
        register_plain<Nested<NestedKind::Mesh>, Dict>();
        register_spawn_worker_recipe(typeid(AddGraph).name(), +[](std::string_view config) {
            const auto stage = spawn_fn<AddGraph>(arg<"factor">(static_cast<Int>(std::stoll(std::string{config}))));
            const std::array schemas{schema_descriptor<TS<Int>>::ts_meta(), schema_descriptor<TS<Int>>::ts_meta()};
            return prepare_spawn_worker(stage.function, schemas);
        });
    }
}
