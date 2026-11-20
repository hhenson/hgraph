#include <hgraph/types/generated.h>

#include <catch2/catch_test_macros.hpp>

namespace
{
    using namespace hgraph;
    using Number = TS<Int>;
    using Numbers = TSD<Int, Number>;

    struct Constant
    {
        static constexpr bool schedule_on_start = true;
        static void eval(Scalar<"value", Int> value, Out<Number> out) { out.set(value.value()); }
    };

    struct Dictionary
    {
        static constexpr bool schedule_on_start = true;
        static void eval(Out<Numbers> out)
        {
            out.set(3, Int{7});
            out.set(5, Int{11});
        }
    };

    struct Sum
    {
        static void eval(In<"a", Number> a, In<"b", Number> b, Out<Number> out, State<Int> state,
                         LoggerView logger, NodeScheduler, SingleShotScheduler, EvaluationClockView,
                         EngineControlView, GlobalStateView, DateTime)
        {
            state.set(a.value() + b.value());
            logger.debug("sum {}", state.get());
            out.set(state.get());
        }
    };

    struct PlusConstant
    {
        static Port<Number> compose(Wiring &w, Port<Number> value, Scalar<"extra", Int> extra)
        {
            return wire<Sum>(w, value, wire<Constant>(w, extra));
        }
    };

    struct AddKeyAndOffset
    {
        static Port<Number> compose(Wiring &w, Port<Number> key, Port<Number> element, Port<Number> offset)
        {
            return wire<Sum>(w, wire<Sum>(w, key, element), offset);
        }
    };

    struct Observe
    {
        static inline std::array<Int, 2> observed{};
        static void eval(In<"values", Numbers> values) { observed = {values[3].value(), values[5].value()}; }
    };
}

TEST_CASE("generated header alone wires and runs concrete nodes, a switch and a keyed map")
{
    Wiring wiring;
    auto selector = wire<Constant>(wiring, Int{1});
    auto offset = compiled::switch_(wiring, selector, std::array{selector.erased()},
                          {{Value{Int{1}}, compile_subgraph<PlusConstant>(wiring, Int{2})}});
    auto collection = wire<Dictionary>(wiring);
    auto result = compiled::map_(wiring, collection, std::array{std::move(offset)},
                       compile_subgraph<AddKeyAndOffset>(wiring), MapKeyArg::First);
    wire<Observe>(wiring, Port<void>{wiring, std::move(result)}.as<Numbers>());

    GraphExecutorBuilder builder;
    builder.graph_builder(std::move(wiring).finish()).start_time(MIN_ST).end_time(MIN_ST + MIN_TD);
    auto executor = builder.make_executor();
    executor.view().run();
    REQUIRE(Observe::observed == std::array<Int, 2>{13, 19});
}
