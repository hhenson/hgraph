#pragma once

#include <hgraph/types/v2/graph.h>
#include <hgraph/types/v2/node.h>
#include <hgraph/types/v2/static_schema.h>
#include <hgraph/util/string_utils.h>

#include <string>

namespace hgraph::nodes::v2
{
    struct ConstNode
    {
        ConstNode() = delete;
        ~ConstNode() = delete;

        static constexpr auto name = "const";
        static constexpr auto node_type = hgraph::v2::NodeTypeEnum::PULL_SOURCE_NODE;

        static void start(hgraph::v2::Graph &graph,
                          hgraph::v2::Node &node,
                          hgraph::v2::State<bool> emitted,
                          hgraph::v2::ScalarArg<"delay", engine_time_delta_t> delay,
                          hgraph::v2::EvaluationClock clock)
        {
            emitted.view().set_scalar(false);
            graph.schedule_node(node.node_index(), clock.evaluation_time() + delay.value(), true);
        }

        static void eval(hgraph::v2::State<bool> emitted,
                         hgraph::v2::PythonScalarArg<"value"> value,
                         TSOutputView out,
                         engine_time_t evaluation_time)
        {
            auto &already_emitted = emitted.view().template checked_as<bool>();
            if (already_emitted) { return; }

            out.value().from_python(value.object());
            hgraph::v2::detail::mark_ts_output_modified(out, evaluation_time);
            already_emitted = true;
        }
    };

    struct NothingNode
    {
        NothingNode() = delete;
        ~NothingNode() = delete;

        static constexpr auto name = "nothing";
        static constexpr auto node_type = hgraph::v2::NodeTypeEnum::PULL_SOURCE_NODE;

        static void eval() {}
    };

    struct NullSinkNode
    {
        NullSinkNode() = delete;
        ~NullSinkNode() = delete;

        static constexpr auto name = "null_sink";
        static constexpr auto node_type = hgraph::v2::NodeTypeEnum::SINK_NODE;
        using TimeSeriesType = hgraph::v2::TsVar<"TIME_SERIES_TYPE">;

        static void eval(hgraph::v2::In<"ts", TimeSeriesType> ts)
        {
            static_cast<void>(ts);
        }
    };

    struct DebugPrintNode
    {
        DebugPrintNode() = delete;
        ~DebugPrintNode() = delete;

        static constexpr auto name = "debug_print";
        static constexpr auto node_type = hgraph::v2::NodeTypeEnum::SINK_NODE;
        using TimeSeriesType = hgraph::v2::TsVar<"TIME_SERIES_TYPE">;

        static void start(hgraph::v2::State<int> state)
        {
            state.view().set_scalar(0);
        }

        static void eval(hgraph::v2::In<"ts", TimeSeriesType> ts,
                         hgraph::v2::State<int> state,
                         hgraph::v2::EvaluationClock clock,
                         hgraph::v2::ScalarArg<"label", std::string> label,
                         hgraph::v2::ScalarArg<"print_delta", bool> print_delta,
                         hgraph::v2::ScalarArg<"sample", int> sample)
        {
            auto &count = state.view().template checked_as<int>();
            ++count;

            const int sample_value = sample.value();
            if (sample_value >= 2 && (count % sample_value) != 0) { return; }

            const View value = print_delta.value() ? ts.delta_value() : ts.view().value();
            const std::string count_prefix = sample_value > 1 ? "[" + std::to_string(count) + "]" : "";
            const std::string message = "[" + hgraph::to_string(clock.now()) + "][" +
                                        hgraph::to_string(clock.evaluation_time()) + "]" + count_prefix + " " +
                                        label.value() + ": " + value.to_string();

            [[maybe_unused]] const auto printed = hgraph::v2::detail::py_call(
                nb::module_::import_("builtins").attr("print"), nb::make_tuple(nb::str(message.c_str())));
        }
    };
}  // namespace hgraph::nodes::v2
