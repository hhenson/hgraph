#include <hgraph/lib/std/std_operators.h>
#include <hgraph/lib/testing/check_output.h>
#include <hgraph/lib/testing/eval_node.h>
#include <hgraph/types/graph_wiring.h>
#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/operator_dispatch.h>
#include <hgraph/types/record_replay.h>
#include <hgraph/types/value/table_codec.h>

#include <catch2/catch_test_macros.hpp>

// P1 of the record/replay/table design record: const-evaluable operators.
// The eager kernel reproduces Python's dual-mode @const_fn without a node
// class: OperatorRegistry::evaluate_const resolves the overload as usual and
// invokes its kernel directly - the exact entry the Python bridge exposes.

namespace
{
    using namespace hgraph;
    using namespace hgraph::testing;

    WiringArg scalar_arg(Value value, const ValueTypeMetaData *meta, std::string name = {})
    {
        WiringArg arg;
        arg.kind         = WiringArg::Kind::Scalar;
        arg.scalar_value = std::move(value);
        arg.scalar_meta  = meta;
        arg.name         = std::move(name);
        return arg;
    }

    struct RecordGraph
    {
        [[maybe_unused]] static constexpr auto name = "const_eval_record_graph";

        static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> ts)
        {
            wire<stdlib::record>(w, ts, Str{"prices"}, arg<"recordable_id">(Str{"book"}));
            return ts;
        }
    };

    struct ReplayConstGraph
    {
        [[maybe_unused]] static constexpr auto name = "replay_const_graph";

        static Port<TS<Int>> compose(Wiring &w)
        {
            return wire<stdlib::replay_const, TS<Int>>(w, Str{"prices"}, arg<"recordable_id">(Str{"book"}))
                .as<TS<Int>>();
        }
    };
}  // namespace

TEST_CASE("const eval: replay_const evaluates eagerly through the registry")
{
    stdlib::register_standard_operators();
    GlobalContext context;
    const auto state = context.state().view();
    record_replay::set_config(state,
                              record_replay::Config{.model = std::string{record_replay::DATA_FRAME}});
    auto &registry = TypeRegistry::instance();
    const auto *str_meta  = registry.register_scalar<Str>("str");
    const auto *int_meta  = registry.register_scalar<Int>("int");
    const auto *ts_int    = registry.ts(int_meta);
    const auto *dt_meta   = registry.register_scalar<DateTime>("datetime");

    (void)eval_node<RecordGraph>(values<Int>(10, none, 30, 40));

    std::vector<WiringArg> args{scalar_arg(Value{Str{"prices"}}, str_meta),
                                scalar_arg(Value{Str{"book"}}, str_meta, "recordable_id")};
    Value latest = OperatorRegistry::instance().evaluate_const(
        "replay_const", std::span<const WiringArg>{args.data(), args.size()}, ts_int, state);
    CHECK(latest.view().checked_as<Int>() == Int{40});

    // A tm cut selects the value recorded at or before it (cycle 2).
    std::vector<WiringArg> cut{scalar_arg(Value{Str{"prices"}}, str_meta),
                               scalar_arg(Value{Str{"book"}}, str_meta, "recordable_id"),
                               scalar_arg(Value{MIN_ST + TimeDelta{2}}, dt_meta, "tm")};
    Value at_cut = OperatorRegistry::instance().evaluate_const(
        "replay_const", std::span<const WiringArg>{cut.data(), cut.size()}, ts_int, state);
    CHECK(at_cut.view().checked_as<Int>() == Int{30});

    // The eager call requires an explicit recordable_id (no graph traits here).
    std::vector<WiringArg> no_id{scalar_arg(Value{Str{"prices"}}, str_meta)};
    CHECK_THROWS_AS((void)OperatorRegistry::instance().evaluate_const(
                        "replay_const", std::span<const WiringArg>{no_id.data(), no_id.size()}, ts_int, state),
                    std::invalid_argument);
}

TEST_CASE("const eval: from_table_const extracts a frame's last row eagerly")
{
    stdlib::register_standard_operators();
    auto &registry = TypeRegistry::instance();
    const auto *int_meta   = registry.register_scalar<Int>("int");
    const auto *frame_meta = registry.register_scalar<Frame>("frame");
    const auto *ts_int     = registry.ts(int_meta);

    const auto &converter = table_converter(int_meta);
    FrameRecorder recorder{converter};
    recorder.append(MIN_ST, MIN_ST, Value{Int{5}}.view());
    recorder.append(MIN_ST + TimeDelta{1}, MIN_ST, Value{Int{9}}.view());

    std::vector<WiringArg> args{scalar_arg(Value{recorder.finish()}, frame_meta)};
    Value value = OperatorRegistry::instance().evaluate_const(
        "from_table_const", std::span<const WiringArg>{args.data(), args.size()}, ts_int);
    CHECK(value.view().checked_as<Int>() == Int{9});
}

TEST_CASE("const eval: the wired replay_const form emits the recovered value at start")
{
    stdlib::register_standard_operators();
    GlobalContext context;
    record_replay::set_config(context.state().view(),
                              record_replay::Config{.model = std::string{record_replay::DATA_FRAME}});

    (void)eval_node<RecordGraph>(values<Int>(10, none, 30, 40));

    // The wired form recovers the value at or before the START time: only
    // the cycle-0 recording (10) qualifies at MIN_ST.
    CHECK_OUTPUT(eval_node<ReplayConstGraph>(), values<Int>(10));
}

TEST_CASE("const eval: a non-const-evaluable overload is a resolution error")
{
    stdlib::register_standard_operators();
    auto &registry = TypeRegistry::instance();
    const auto *int_meta = registry.register_scalar<Int>("int");

    std::vector<WiringArg> args{scalar_arg(Value{Int{1}}, int_meta)};
    CHECK_THROWS_AS((void)OperatorRegistry::instance().evaluate_const(
                        "const", std::span<const WiringArg>{args.data(), args.size()},
                        registry.ts(int_meta)),
                    OperatorResolutionError);
}
