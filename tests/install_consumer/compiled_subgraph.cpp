#include <hgraph/types/generated.h>

namespace {
using namespace hgraph;

struct Counter {
    static constexpr bool schedule_on_start = true;
    static void eval(Out<TS<Int>> out) { out.set(Int{42}); }
};

struct Pass {
    static Port<TS<Int>> compose(Wiring &, Port<TS<Int>> value) { return value; }
};

struct Probe {
    static inline Int observed{};
    static void eval(In<"value", TS<Int>> value) { observed = value.value(); }
};
}

int main()
{
    using namespace hgraph;
    Wiring wiring;
    auto value = wire<Counter>(wiring);
    auto result = compiled::switch_(wiring, value, std::array{value.erased()},
                          {{Value{Int{42}}, compile_subgraph<Pass>(wiring)}});
    wire<Probe>(wiring, Port<void>{wiring, std::move(result)});

    GraphExecutorBuilder exec;
    exec.graph_builder(std::move(wiring).finish()).start_time(MIN_ST).end_time(MIN_ST + MIN_TD);
    auto executor = exec.make_executor();
    executor.view().run();
    return Probe::observed == Int{42} ? 0 : 1;
}
