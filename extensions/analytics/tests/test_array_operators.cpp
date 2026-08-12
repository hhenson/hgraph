#include <hgraph/analytics/operators.h>

#include <hgraph/lib/std/operators/conversion.h>
#include <hgraph/lib/std/operators/registration.h>
#include <hgraph/lib/std/operators/stream.h>
#include <hgraph/lib/testing/eval_node.h>
#include <hgraph/types/metadata/value_plan_factory.h>

#include <cmath>
#include <cstddef>
#include <iostream>
#include <limits>
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

    template <typename T, typename... Args>
    [[nodiscard]] std::vector<std::optional<T>> values(Args &&...args)
    {
        std::vector<std::optional<T>> output;
        output.reserve(sizeof...(Args));
        (output.emplace_back(T{std::forward<Args>(args)}), ...);
        return output;
    }

    void require(bool condition, std::string message)
    {
        if (!condition) { throw std::runtime_error(std::move(message)); }
    }

    template <typename T>
    [[nodiscard]] Value array_1d(std::vector<T> items,
                                 std::size_t capacity = 0)
    {
        if (capacity == 0) { capacity = items.size(); }
        require(items.size() <= capacity, "array value exceeds its capacity");
        const auto *meta = TypeRegistry::instance().array(
            scalar_descriptor<T>::value_meta(), capacity);
        Value result{ValuePlanFactory::instance().type_for(meta)};
        auto output = result.as_list().begin_mutation();
        output.resize(items.size());
        for (std::size_t index = 0; index < items.size(); ++index)
        {
            Value item{items[index]};
            output.at(index).copy_from(item.view());
        }
        return result;
    }

    template <typename T>
    [[nodiscard]] Value array_2d(std::vector<std::vector<T>> rows)
    {
        const std::size_t columns = rows.empty() ? 0 : rows.front().size();
        const auto *meta = TypeRegistry::instance().array(
            scalar_descriptor<T>::value_meta(),
            std::vector<std::size_t>{rows.size(), columns});
        Value result{ValuePlanFactory::instance().type_for(meta)};
        auto output = result.as_list().begin_mutation();
        output.resize(rows.size());
        for (std::size_t index = 0; index < rows.size(); ++index)
        {
            require(rows[index].size() == columns, "array value is not rectangular");
            Value row = array_1d<T>(std::move(rows[index]));
            output.at(index).copy_from(row.view());
        }
        return result;
    }

    struct WindowValuesGraph
    {
        static constexpr auto name = "analytics_window_values_graph";

        static Port<TS<ArrayOf<Int, 3>>> compose(Wiring &w, Port<TS<Int>> input)
        {
            auto window = wire<stdlib::to_window>(w, input, Int{3}, Int{3})
                              .as<TSW<Int, 3, 3>>();
            return wire<window_values, TS<ArrayOf<Int, 3>>>(w, window);
        }
    };

    struct PaddedWindowValuesGraph
    {
        static constexpr auto name = "analytics_padded_window_values_graph";

        static Port<TS<ArrayOf<Int, 3>>> compose(Wiring &w, Port<TS<Int>> input)
        {
            auto window = wire<stdlib::to_window>(w, input, Int{3}, Int{2})
                              .as<TSW<Int, 3, 2>>();
            auto zero = wire<stdlib::const_, TS<Int>>(w, Int{0});
            return wire<window_values, TS<ArrayOf<Int, 3>>>(w, window, zero);
        }
    };

    struct ScalarPaddedWindowValuesGraph
    {
        static constexpr auto name = "analytics_scalar_padded_window_values_graph";

        static Port<TS<ArrayOf<Int, 3>>> compose(Wiring &w, Port<TS<Int>> input)
        {
            auto window = wire<stdlib::to_window>(w, input, Int{3}, Int{2})
                              .as<TSW<Int, 3, 2>>();
            return wire<window_values, TS<ArrayOf<Int, 3>>>(w, window, Int{-1});
        }
    };

    struct MatrixRowGraph
    {
        static constexpr auto name = "analytics_matrix_row_graph";

        static Port<TS<ArrayOf<Int, 2>>> compose(
            Wiring &w, Port<TS<ArrayOf<Int, 3, 2>>> input)
        {
            return wire<array_get_item>(w, input, Int{1})
                .as<TS<ArrayOf<Int, 2>>>();
        }
    };

    struct MatrixItemGraph
    {
        static constexpr auto name = "analytics_matrix_item_graph";

        static Port<TS<Int>> compose(
            Wiring &w, Port<TS<ArrayOf<Int, 3, 2>>> input)
        {
            const auto *tuple_meta = TypeRegistry::instance().tuple(
                {scalar_descriptor<Int>::value_meta(),
                 scalar_descriptor<Int>::value_meta()});
            Value index{ValuePlanFactory::instance().type_for(tuple_meta)};
            auto tuple = index.as_tuple().begin_mutation();
            Value row{Int{1}};
            Value column{Int{0}};
            tuple.at(0).copy_from(row.view());
            tuple.at(1).copy_from(column.view());
            return wire<array_get_item>(w, input, std::move(index)).as<TS<Int>>();
        }
    };

    struct CumulativeSumAxisGraph
    {
        static constexpr auto name = "analytics_cumulative_sum_axis_graph";

        static Port<TS<ArrayOf<Int, 2, 3>>> compose(
            Wiring &w, Port<TS<ArrayOf<Int, 2, 3>>> input)
        {
            return wire<cumulative_sum, TS<ArrayOf<Int, 2, 3>>>(
                w, input, Int{0});
        }
    };

    struct CumulativeSumLastAxisGraph
    {
        static constexpr auto name = "analytics_cumulative_sum_last_axis_graph";

        static Port<TS<ArrayOf<Int, 2, 3>>> compose(
            Wiring &w, Port<TS<ArrayOf<Int, 2, 3>>> input)
        {
            return wire<cumulative_sum, TS<ArrayOf<Int, 2, 3>>>(
                w, input, Int{-1});
        }
    };

    struct CorrelationGraph
    {
        static constexpr auto name = "analytics_correlation_graph";

        static Port<TS<ArrayOf<Float, 2, 2>>> compose(
            Wiring &w, Port<TS<ArrayOf<Float, 2, 4>>> input)
        {
            return wire<correlation, TS<ArrayOf<Float, 2, 2>>>(
                w, input, Bool{true});
        }
    };

    struct ScalarCorrelationGraph
    {
        static constexpr auto name = "analytics_scalar_correlation_graph";

        static Port<TS<Float>> compose(
            Wiring &w, Port<TS<ArrayOf<Float, 4>>> input)
        {
            return wire<correlation, TS<Float>>(w, input, Bool{true});
        }
    };

    struct PairedCorrelationGraph
    {
        static constexpr auto name = "analytics_paired_correlation_graph";

        static Port<TS<ArrayOf<Float, 2, 2>>> compose(
            Wiring &w, Port<TS<ArrayOf<Float, 4>>> input)
        {
            return wire<correlation, TS<ArrayOf<Float, 2, 2>>>(
                w, input, input, Bool{true});
        }
    };

    struct ColumnCorrelationGraph
    {
        static constexpr auto name = "analytics_column_correlation_graph";

        static Port<TS<ArrayOf<Float, 2, 2>>> compose(
            Wiring &w, Port<TS<ArrayOf<Float, 3, 2>>> input)
        {
            return wire<correlation, TS<ArrayOf<Float, 2, 2>>>(
                w, input, Bool{false});
        }
    };

    struct QuantileGraph
    {
        static constexpr auto name = "analytics_quantile_graph";

        static Port<TS<Float>> compose(Wiring &w,
                                       Port<TS<ArrayOf<Int, 4>>> input)
        {
            auto q = wire<stdlib::const_, TS<Float>>(w, Float{0.5});
            return wire<quantile>(w, input, q).as<TS<Float>>();
        }
    };

    template <fixed_string Method>
    struct QuantileMethodGraph
    {
        static constexpr auto name = "analytics_quantile_method_graph";

        static Port<TS<Float>> compose(Wiring &w,
                                       Port<TS<ArrayOf<Int, 4>>> input)
        {
            auto q = wire<stdlib::const_, TS<Float>>(w, Float{0.625});
            return wire<quantile>(w, input, q, Str{Method.sv()})
                .template as<TS<Float>>();
        }
    };

    struct WindowQuantileGraph
    {
        static constexpr auto name = "analytics_window_quantile_graph";

        static Port<TS<Float>> compose(Wiring &w, Port<TS<Int>> input)
        {
            auto window = wire<stdlib::to_window>(w, input, Int{4});
            auto q = wire<stdlib::const_, TS<Float>>(w, Float{0.5});
            return wire<quantile>(w, window, q).as<TS<Float>>();
        }
    };

    struct SampleArrayStdGraph
    {
        static constexpr auto name = "analytics_sample_array_std_graph";

        static Port<TS<Float>> compose(Wiring &w,
                                       Port<TS<ArrayOf<Int, 4>>> input)
        {
            return wire<array_std>(w, input, Int{1}).as<TS<Float>>();
        }
    };

    using RollingInt3 = TSB<"RollingWindowResult[int,3]",
                            Field<"buffer", TS<ArrayOf<Int, 3>>>,
                            Field<"index", TS<ArrayOf<DateTime, 3>>>>;
    using RollingInt3Min2 = TSB<"RollingWindowResult[int,3,2]",
                                Field<"buffer", TS<ArrayOf<Int>>>,
                                Field<"index", TS<ArrayOf<DateTime>>>>;

    struct RollingWindowGraph
    {
        static constexpr auto name = "analytics_rolling_window_graph";

        static Port<RollingInt3> compose(Wiring &w, Port<TS<Int>> input)
        {
            auto window = wire<stdlib::to_window>(w, input, Int{3})
                              .as<TSW<Int, 3, 3>>();
            return wire<rolling_window, RollingInt3>(w, window);
        }
    };

    struct RollingWindowMinGraph
    {
        static constexpr auto name = "analytics_rolling_window_min_graph";

        static Port<RollingInt3Min2> compose(Wiring &w, Port<TS<Int>> input)
        {
            auto window = wire<stdlib::to_window>(w, input, Int{3}, Int{2})
                              .as<TSW<Int, 3, 2>>();
            return wire<rolling_window, RollingInt3Min2>(w, window);
        }
    };

    void test_window_values()
    {
        const auto full = eval_node<WindowValuesGraph>(values<Int>(1, 2, 3));
        require(full.size() == 3 && !full[0].has_value() &&
                    !full[1].has_value() && full[2].has_value(),
                "window values readiness");
        require(full[2]->equals(array_1d<Int>({1, 2, 3})),
                "window values materialization");

        const auto padded =
            eval_node<PaddedWindowValuesGraph>(values<Int>(1, 2, 3));
        require(padded.size() == 3 && !padded[0].has_value() &&
                    padded[1].has_value() && padded[2].has_value(),
                "padded window values readiness");
        require(padded[1]->equals(array_1d<Int>({1, 2, 0})),
                "live zero window padding");
        require(padded[2]->equals(array_1d<Int>({1, 2, 3})),
                "complete padded window values");

        const auto scalar_padded =
            eval_node<ScalarPaddedWindowValuesGraph>(values<Int>(1, 2));
        require(scalar_padded[1].has_value() &&
                    scalar_padded[1]->equals(array_1d<Int>({1, 2, -1})),
                "scalar zero window padding");
    }

    void test_array_get_item()
    {
        const Value matrix = array_2d<Int>({{1, 2}, {3, 4}, {5, 6}});
        const auto row = eval_node<MatrixRowGraph>(values<Value>(matrix));
        require(row.size() == 1 && row[0].has_value() &&
                    row[0]->equals(array_1d<Int>({3, 4})),
                "array row selection");
        require(eval_node<MatrixItemGraph>(values<Value>(matrix)) ==
                    std::vector<std::optional<Int>>{Int{3}},
                "array scalar selection");
    }

    void test_cumulative_sum()
    {
        const Value matrix = array_2d<Int>({{1, 2, 3}, {4, 5, 6}});
        const auto flattened =
            eval_node<cumulative_sum, TS<ArrayOf<Int, 2, 3>>>(values<Value>(matrix));
        require(flattened.size() == 1 && flattened[0].has_value() &&
                    flattened[0]->equals(
                        array_1d<Int>({1, 3, 6, 10, 15, 21})),
                "flattened cumulative sum");

        const auto axis = eval_node<CumulativeSumAxisGraph>(values<Value>(matrix));
        require(axis[0].has_value() && axis[0]->equals(
                    array_2d<Int>({{1, 2, 3}, {5, 7, 9}})),
                "axis cumulative sum");

        const auto last_axis =
            eval_node<CumulativeSumLastAxisGraph>(values<Value>(matrix));
        require(last_axis[0].has_value() && last_axis[0]->equals(
                    array_2d<Int>({{1, 3, 6}, {4, 9, 15}})),
                "negative-axis cumulative sum");

        const Value overflowing = array_1d<Int>(
            {std::numeric_limits<Int>::max(), Int{1}});
        const auto wrapped =
            eval_node<cumulative_sum, TS<ArrayOf<Int, 2>>>(
                values<Value>(overflowing));
        require(wrapped[0].has_value() && wrapped[0]->equals(array_1d<Int>(
                    {std::numeric_limits<Int>::max(),
                     std::numeric_limits<Int>::min()})),
                "integer cumulative sum wrapping");
    }

    void test_correlation()
    {
        const Value matrix = array_2d<Float>(
            {{1.0, 2.0, 3.0, 4.0}, {1.0, 2.0, 3.0, 4.0}});
        const auto matrix_result =
            eval_node<CorrelationGraph>(values<Value>(matrix));
        require(matrix_result.size() == 1 && matrix_result[0].has_value() &&
                    matrix_result[0]->equals(
                        array_2d<Float>({{1.0, 1.0}, {1.0, 1.0}})),
                "matrix correlation");

        const Value vector = array_1d<Float>({1.0, 2.0, 3.0, 4.0});
        const auto pair_result =
            eval_node<PairedCorrelationGraph>(values<Value>(vector));
        require(pair_result[0].has_value() && pair_result[0]->equals(
                    array_2d<Float>({{1.0, 1.0}, {1.0, 1.0}})),
                "paired vector correlation");

        const Value columns =
            array_2d<Float>({{1.0, 2.0}, {2.0, 4.0}, {3.0, 6.0}});
        const auto column_result =
            eval_node<ColumnCorrelationGraph>(values<Value>(columns));
        require(column_result[0].has_value() && column_result[0]->equals(
                    array_2d<Float>({{1.0, 1.0}, {1.0, 1.0}})),
                "column correlation");

        const Value constant = array_1d<Float>({2.0, 2.0, 2.0, 2.0});
        const auto scalar_result =
            eval_node<ScalarCorrelationGraph>(values<Value>(constant));
        require(scalar_result[0].has_value() && std::isnan(*scalar_result[0]),
                "constant-vector correlation");
    }

    void test_quantile()
    {
        const Value input = array_1d<Int>({1, 2, 3, 4});
        const auto median = eval_node<QuantileGraph>(values<Value>(input));
        require(median.size() == 1 && median[0].has_value() &&
                    std::abs(*median[0] - 2.5) <= 1.0e-12,
                "array quantile");

        const std::vector<std::pair<std::string, Float>> methods{
            {"linear", 2.875}, {"lower", 2.0}, {"higher", 3.0},
            {"midpoint", 2.5}, {"nearest", 3.0}};
        const auto linear =
            eval_node<QuantileMethodGraph<"linear">>(values<Value>(input));
        const auto lower =
            eval_node<QuantileMethodGraph<"lower">>(values<Value>(input));
        const auto higher =
            eval_node<QuantileMethodGraph<"higher">>(values<Value>(input));
        const auto midpoint =
            eval_node<QuantileMethodGraph<"midpoint">>(values<Value>(input));
        const auto nearest =
            eval_node<QuantileMethodGraph<"nearest">>(values<Value>(input));
        const std::vector<std::vector<std::optional<Float>>> results{
            linear, lower, higher, midpoint, nearest};
        for (std::size_t index = 0; index < methods.size(); ++index)
        {
            require(results[index][0].has_value() &&
                        std::abs(*results[index][0] - methods[index].second) <= 1.0e-12,
                    "quantile method " + methods[index].first);
        }

        const auto window = eval_node<WindowQuantileGraph>(values<Int>(1, 2, 3, 4));
        require(window.size() == 4 && !window[0].has_value() &&
                    !window[1].has_value() && !window[2].has_value() &&
                    window[3].has_value() &&
                    std::abs(*window[3] - 2.5) <= 1.0e-12,
                "window quantile readiness");
    }

    void test_array_standard_deviation()
    {
        const Value input = array_1d<Int>({1, 2, 3, 4});
        const auto population =
            eval_node<array_std, TS<ArrayOf<Int, 4>>>(values<Value>(input));
        require(population[0].has_value() &&
                    std::abs(population[0]->view().checked_as<Float>() -
                             std::sqrt(1.25)) <= 1.0e-12,
                "population array std");

        const auto sample =
            eval_node<SampleArrayStdGraph>(values<Value>(input));
        require(sample[0].has_value() &&
                    std::abs(*sample[0] - std::sqrt(5.0 / 3.0)) <= 1.0e-12,
                "sample array std");

        const Value offset = array_1d<Float>(
            {1.0e16, 1.0e16, 1.0e16 + 2.0, 1.0e16 + 4.0});
        const auto stable =
            eval_node<array_std, TS<ArrayOf<Float, 4>>>(values<Value>(offset));
        require(stable[0].has_value() &&
                    std::abs(stable[0]->view().checked_as<Float>() -
                             std::sqrt(5.0)) <= 1.0e-12,
                "stable array std");
    }

    void test_rolling_window()
    {
        const auto full = eval_node<RollingWindowGraph>(values<Int>(1, 2, 3));
        require(full.size() == 3 && !full[0].has_value() &&
                    !full[1].has_value() && full[2].has_value(),
                "full rolling window readiness");
        const auto full_bundle = full[2]->as_bundle();
        require(full_bundle["buffer"].equals(
                    array_1d<Int>({1, 2, 3}).view()),
                "full rolling window values");
        require(full_bundle["index"].equals(
                    array_1d<DateTime>(
                        {MIN_ST, MIN_ST + MIN_TD, MIN_ST + MIN_TD * 2})
                        .view()),
                "full rolling window timestamps");

        const auto partial =
            eval_node<RollingWindowMinGraph>(values<Int>(1, 2, 3));
        require(partial.size() == 3 && !partial[0].has_value() &&
                    partial[1].has_value() && partial[2].has_value(),
                "partial rolling window readiness");
        require(partial[1]->as_bundle()["buffer"].equals(
                    array_1d<Int>({1, 2}).view()),
                "partial rolling window values");
    }
}  // namespace

int main()
{
    try
    {
        hgraph::stdlib::register_standard_operators();
        hgraph::analytics::register_analytics_operators();
        test_window_values();
        test_array_get_item();
        test_cumulative_sum();
        test_correlation();
        test_quantile();
        test_array_standard_deviation();
        test_rolling_window();
        return 0;
    }
    catch (const std::exception &error)
    {
        std::cerr << error.what() << '\n';
        return 1;
    }
}
