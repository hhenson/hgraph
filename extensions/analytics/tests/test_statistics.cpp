#include <hgraph/analytics/operators.h>

#include <hgraph/lib/std/operators/collection.h>
#include <hgraph/types/record_replay.h>
#include <hgraph/lib/std/operators/registration.h>
#include <hgraph/lib/std/operators/stream.h>
#include <hgraph/lib/testing/eval_node.h>
#include <hgraph/lib/testing/record_replay.h>
#include <hgraph/runtime/executor.h>

#include <cmath>
#include <cstddef>
#include <iostream>
#include <optional>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

namespace
{
    using namespace hgraph;
    using namespace hgraph::analytics;
    using namespace hgraph::testing;

    inline constexpr std::nullopt_t none = std::nullopt;

    template <typename T, typename U> [[nodiscard]] std::optional<T> optional_value(U &&value) { return T{std::forward<U>(value)}; }

    template <typename T> [[nodiscard]] std::optional<T> optional_value(std::nullopt_t) { return std::nullopt; }

    template <typename T, typename... Args> [[nodiscard]] std::vector<std::optional<T>> values(Args &&...args) {
        std::vector<std::optional<T>> output;
        output.reserve(sizeof...(Args));
        (output.push_back(optional_value<T>(std::forward<Args>(args))), ...);
        return output;
    }

    void require(bool condition, std::string message) {
        if (!condition) { throw std::runtime_error(std::move(message)); }
    }

    void require_float_output(const std::vector<std::optional<Float>> &actual, const std::vector<std::optional<Float>> &expected,
                              const std::string &label) {
        require(actual.size() == expected.size(), label + ": output size");
        for (std::size_t index = 0; index < expected.size(); ++index) {
            require(actual[index].has_value() == expected[index].has_value(),
                    label + ": readiness at index " + std::to_string(index));
            if (!expected[index].has_value()) { continue; }
            if (std::isnan(*expected[index])) {
                require(std::isnan(*actual[index]), label + ": NaN at index " + std::to_string(index));
            } else {
                require(std::abs(*actual[index] - *expected[index]) <= 1.0e-12,
                        label + ": value at index " + std::to_string(index));
            }
        }
    }

    void require_erased_float_output(const std::vector<std::optional<Value>> &actual,
                                     const std::vector<std::optional<Float>> &expected, const std::string &label) {
        require(actual.size() == expected.size(), label + ": output size");
        for (std::size_t index = 0; index < expected.size(); ++index) {
            require(actual[index].has_value() == expected[index].has_value(),
                    label + ": readiness at index " + std::to_string(index));
            if (!expected[index].has_value()) { continue; }
            const Float value = actual[index]->view().checked_as<Float>();
            require(std::abs(value - *expected[index]) <= 1.0e-12, label + ": value at index " + std::to_string(index));
        }
    }

    struct RunningStdGraph
    {
        static constexpr auto name = "analytics_running_std";

        static Port<TS<Float>> compose(Wiring &w, Port<TS<Int>> ts) { return wire<std_, TS<Float>>(w, ts); }
    };

    struct RunningVarGraph
    {
        static constexpr auto name = "analytics_running_var";

        static Port<TS<Float>> compose(Wiring &w, Port<TS<Int>> ts) { return wire<var_, TS<Float>>(w, ts); }
    };

    struct BinaryStdGraph
    {
        static constexpr auto name = "analytics_binary_std";

        static Port<TS<Float>> compose(Wiring &w, Port<TS<Int>> lhs, Port<TS<Int>> rhs) {
            return wire<std_, TS<Float>>(w, lhs, rhs);
        }
    };

    struct BinaryVarGraph
    {
        static constexpr auto name = "analytics_binary_var";

        static Port<TS<Float>> compose(Wiring &w, Port<TS<Int>> lhs, Port<TS<Int>> rhs) {
            return wire<var_, TS<Float>>(w, lhs, rhs);
        }
    };

    struct WindowStdGraph
    {
        static constexpr auto name = "analytics_window_std";

        static Port<TS<Float>> compose(Wiring &w, Port<TS<Int>> ts) {
            auto window = wire<hgraph::stdlib::to_window>(w, ts, Int{5}, Int{3});
            return wire<std_, TS<Float>>(w, window);
        }
    };

    struct WindowSampleStdGraph
    {
        static constexpr auto name = "analytics_window_sample_std";

        static Port<TS<Float>> compose(Wiring &w, Port<TS<Int>> ts) {
            auto window = wire<hgraph::stdlib::to_window>(w, ts, Int{5}, Int{3});
            return wire<std_, TS<Float>>(w, window, arg<"ddof">(Int{1}));
        }
    };

    struct RollingMeanGraph
    {
        static constexpr auto name = "analytics_rolling_mean";

        static Port<TS<Float>> compose(Wiring &w, Port<TS<Int>> ts) { return wire<rolling_mean, TS<Float>>(w, ts, Int{3}, Int{2}); }
    };

    using HomogeneousIntBundle = UnNamedTSB<Field<"a", TS<Int>>, Field<"b", TS<Int>>, Field<"c", TS<Int>>>;

    struct BundleStdGraph
    {
        static constexpr auto name = "analytics_bundle_std";

        static Port<TS<Float>> compose(Wiring &w, Port<TS<Int>> a, Port<TS<Int>> b, Port<TS<Int>> c) {
            return wire<std_, TS<Float>>(w, hgraph::stdlib::to_tsb<HomogeneousIntBundle>(w, a, b, c));
        }
    };

    struct BundleVarGraph
    {
        static constexpr auto name = "analytics_bundle_var";

        static Port<TS<Float>> compose(Wiring &w, Port<TS<Int>> a, Port<TS<Int>> b, Port<TS<Int>> c) {
            return wire<var_, TS<Float>>(w, hgraph::stdlib::to_tsb<HomogeneousIntBundle>(w, a, b, c));
        }
    };

    void test_running_and_binary_dispersion() {
        require_float_output(eval_node<RunningStdGraph>(values<Int>(1, 2, 3, 5)),
                             values<Float>(0.0, 0.5, 0.8164965809277263, 1.479019945774904), "running std");
        require_float_output(eval_node<RunningVarGraph>(values<Int>(1, 2, 3)), values<Float>(0.0, 0.25, 2.0 / 3.0), "running var");
        require_float_output(eval_node<BinaryStdGraph>(values<Int>(1, 2), values<Int>(2, 3)),
                             values<Float>(std::sqrt(0.5), std::sqrt(0.5)), "binary std");
        require_float_output(eval_node<BinaryVarGraph>(values<Int>(1, 2), values<Int>(2, 3)), values<Float>(0.5, 0.5),
                             "binary var");
    }

    void test_window_dispersion_and_rolling_mean() {
        require_float_output(eval_node<WindowStdGraph>(values<Int>(1, 2, 3, 4, 5)),
                             values<Float>(none, none, std::sqrt(2.0 / 3.0), std::sqrt(1.25), std::sqrt(2.0)),
                             "window population std");
        require_float_output(eval_node<WindowSampleStdGraph>(values<Int>(1, 2, 3, 4, 5)),
                             values<Float>(none, none, 1.0, std::sqrt(5.0 / 3.0), std::sqrt(2.5)), "window sample std");
        require_float_output(eval_node<RollingMeanGraph>(values<Int>(1, 2, 3, 4, 5)), values<Float>(none, 1.5, 2.0, 3.0, 4.0),
                             "rolling mean");
    }

    void test_collection_dispersion() {
        require_erased_float_output(
            eval_node<std_, TSS<Int>>(values<Value>(set_delta<Int>({1}, {}), set_delta<Int>({2}, {}), set_delta<Int>({-1, 3}, {}))),
            values<Float>(0.0, std::sqrt(0.5), std::sqrt(35.0 / 12.0)), "set std");
        require_erased_float_output(
            eval_node<var_, TSD<Int, TS<Int>>>(values<Value>(dict_delta<Int, TS<Int>>({{1, 1}}), dict_delta<Int, TS<Int>>({{2, 2}}),
                                                             dict_delta<Int, TS<Int>>({{3, -1}, {4, 3}}))),
            values<Float>(0.0, 0.5, 35.0 / 12.0), "dictionary var");
        require_erased_float_output(eval_node<std_, TSL<TS<Int>, 5>>(values<Value>(list_delta<TS<Int>>({{0, 1}, {1, 2}}),
                                                                                   list_delta<TS<Int>>({{2, 3}, {3, 4}, {4, 5}}),
                                                                                   list_delta<TS<Int>>({{0, 10}}))),
                                    values<Float>(std::sqrt(0.5), std::sqrt(2.5), std::sqrt(9.7)), "fixed-list std");
        require_float_output(eval_node<BundleStdGraph>(values<Int>(1), values<Int>(2), values<Int>(3)),
                             values<Float>(1.0), "homogeneous bundle std");
        require_float_output(eval_node<BundleVarGraph>(values<Int>(1), values<Int>(2), values<Int>(3)),
                             values<Float>(1.0), "homogeneous bundle var");
    }

    template <typename Period, typename Minimum>
    void require_invalid_rolling_mean(Period period, Minimum minimum, const std::string &expected) {
        try {
            static_cast<void>(eval_node<rolling_mean>(values<Int>(1), period, minimum));
        } catch (const std::exception &error) {
            require(std::string{error.what()}.find(expected) != std::string::npos,
                    "rolling mean validation message");
            return;
        }
        require(false, "rolling mean invalid parameters must fail while wiring");
    }

    void test_rolling_mean_validation() {
        require_invalid_rolling_mean(Int{0}, Int{0}, "period must be positive");
        require_invalid_rolling_mean(Int{3}, Int{-1}, "min_window_period must be between zero and period");
        require_invalid_rolling_mean(Int{3}, Int{4}, "min_window_period must be between zero and period");
        require_invalid_rolling_mean(TimeDelta{}, TimeDelta{}, "period must be positive");
        require_invalid_rolling_mean(MIN_TD * 3, MIN_TD * 4,
                                     "min_window_period must be between zero and period");
    }

    void test_resample_schedule() {
        Wiring wiring;
        // Wired through the PUBLIC operator markers (RFC 0025 checkpoint 3):
        // an extension test selects the dense testing backend and lets the
        // registry resolve the implementations — no impl headers.
        hgraph::record_replay::set_config(
            wiring.global_state(),
            hgraph::record_replay::RecordReplayConfig{
                .backend = std::string{hgraph::record_replay::TESTING}});
        auto   input  = wire<hgraph::stdlib::replay, TS<Int>>(wiring, Str{"analytics_resample_in"});
        auto   output = wire<resample>(wiring, input, MIN_TD * 2);
        wire<hgraph::stdlib::record>(wiring, output, Str{"analytics_resample_out"});

        GraphBuilder graph_builder = std::move(wiring).finish();
        set_replay_values<Int>(graph_builder.global_state(), "analytics_resample_in", values<Int>(1, none, 3));
        GraphExecutorBuilder executor_builder;
        executor_builder.graph_builder(std::move(graph_builder)).start_time(MIN_ST).end_time(MIN_ST + MIN_TD * 6);
        GraphExecutorValue executor = executor_builder.make_executor();
        auto               view     = executor.view();
        view.run();

        const auto actual   = get_recorded_values<Int>(view.graph().global_state(), "analytics_resample_out");
        const auto expected = values<Int>(none, none, 3, none, 3);
        require(actual == expected, "resample schedule");
    }
}  // namespace

int main() {
    try {
        hgraph::stdlib::register_standard_operators();
        hgraph::analytics::register_analytics_operators();
        test_running_and_binary_dispersion();
        test_window_dispersion_and_rolling_mean();
        test_collection_dispersion();
        test_rolling_mean_validation();
        test_resample_schedule();
        return 0;
    } catch (const std::exception &error) {
        std::cerr << error.what() << '\n';
        return 1;
    }
}
