#include <hgraph/lib/std/std_operators.h>
#include <hgraph/lib/testing/eval_node.h>
#include <hgraph/runtime/spawn.h>
#include <hgraph/types/static_node.h>

#include <array>
#include <stdexcept>
#include <thread>
#include <vector>

namespace
{
    using namespace hgraph;
    struct Result
    {
        std::vector<Int> values;
        std::thread::id worker;
    };
}
namespace hgraph::static_schema_detail
{
    template <> struct scalar_name<Result *>
    { static constexpr std::string_view value{"InstalledSpawnResult"}; };
}
namespace
{
    struct Consume
    {
        static void eval(In<"value", TS<Int>> value, Scalar<"result", Result *> result)
        {
            result.value()->values.push_back(value.value());
            result.value()->worker = std::this_thread::get_id();
        }
    };
    struct Offset
    {
        static void eval(In<"value", TS<Int>> value, In<"offset", TS<Int>> offset, Out<TS<Int>> out)
        { out.set(value.value() + offset.value()); }
    };
    struct Probe
    {
        static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> value, Scalar<"result", Result *> result)
        {
            auto offset = wire<stdlib::const_>(w, Int{10}).as<TS<Int>>();
            auto stage = bind_(fn<Offset>(), {{"offset", offset.erased()}});
            auto sink = spawn_fn<Consume>(arg<"result">(result.value()));
            WiringArg input;
            input.port = value.erased();
            std::array arguments{input};
            wire_spawn(w, pipeline_({std::move(stage), std::move(sink)}), arguments);
            return value;
        }
    };
}

void check_spawn_consumer()
{
    hgraph::stdlib::register_standard_operators();
    hgraph::TypeRegistry::instance().register_scalar<Result *>("InstalledSpawnResult");
    Result result;
    (void)hgraph::testing::eval_node<Probe>(std::vector<std::optional<hgraph::Int>>{1, 2, 3},
                                           hgraph::arg<"result">(&result));
    if (result.values != std::vector<hgraph::Int>{11, 12, 13} ||
        result.worker == std::this_thread::get_id())
        throw std::runtime_error("installed spawn pipeline did not run its typed sink asynchronously");
}
