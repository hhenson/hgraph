#include <hgraph/lib/std/std_operators.h>
#include <hgraph/lib/testing/check_output.h>
#include <hgraph/lib/testing/eval_node.h>
#include <hgraph/types/metadata/value_plan_factory.h>
#include <hgraph/types/value/value.h>

#include <catch2/catch_approx.hpp>
#include <catch2/catch_test_macros.hpp>

#include <cmath>
#include <limits>
#include <optional>
#include <vector>

namespace
{
    using namespace hgraph;
    using namespace hgraph::testing;

    template <typename T>
    [[nodiscard]] Value array_1d(std::vector<T> values,
                                 std::size_t capacity = 0)
    {
        if (capacity == 0) { capacity = values.size(); }
        REQUIRE(values.size() <= capacity);
        const auto *meta = TypeRegistry::instance().array(
            scalar_descriptor<T>::value_meta(), capacity);
        const auto binding = ValuePlanFactory::instance().type_for(meta);
        Value result{binding};
        auto output = result.as_list().begin_mutation();
        output.resize(values.size());
        for (std::size_t index = 0; index < values.size(); ++index)
        {
            Value item{values[index]};
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
        const auto binding = ValuePlanFactory::instance().type_for(meta);
        Value result{binding};
        auto output = result.as_list().begin_mutation();
        output.resize(rows.size());
        for (std::size_t index = 0; index < rows.size(); ++index)
        {
            REQUIRE(rows[index].size() == columns);
            Value row = array_1d<T>(std::move(rows[index]));
            output.at(index).copy_from(row.view());
        }
        return result;
    }

    struct AsArrayGraph
    {
        static constexpr auto name = "as_array_graph";

        static Port<TS<ArrayOf<Int, 3>>> compose(Wiring &w, Port<TS<Int>> ts)
        {
            auto window = wire<stdlib::to_window>(w, ts, Int{3}, Int{3})
                              .as<TSW<Int, 3, 3>>();
            return wire<stdlib::numpy::as_array, TS<ArrayOf<Int, 3>>>(w, window);
        }
    };

    struct AsArrayPaddingGraph
    {
        static constexpr auto name = "as_array_padding_graph";

        static Port<TS<ArrayOf<Int, 3>>> compose(Wiring &w, Port<TS<Int>> ts)
        {
            auto window = wire<stdlib::to_window>(w, ts, Int{3}, Int{2})
                              .as<TSW<Int, 3, 2>>();
            auto zero = wire<stdlib::const_, TS<Int>>(w, Int{0});
            return wire<stdlib::numpy::as_array, TS<ArrayOf<Int, 3>>>(w, window, zero);
        }
    };

    struct AsArrayScalarPaddingGraph
    {
        static constexpr auto name = "as_array_scalar_padding_graph";

        static Port<TS<ArrayOf<Int, 3>>> compose(Wiring &w, Port<TS<Int>> ts)
        {
            auto window = wire<stdlib::to_window>(w, ts, Int{3}, Int{2})
                              .as<TSW<Int, 3, 2>>();
            return wire<stdlib::numpy::as_array, TS<ArrayOf<Int, 3>>>(
                w, window, Int{-1});
        }
    };

    struct MatrixRowGraph
    {
        static constexpr auto name = "matrix_row_graph";

        static Port<TS<ArrayOf<Int, 2>>> compose(
            Wiring &w, Port<TS<ArrayOf<Int, 3, 2>>> input)
        {
            return wire<stdlib::numpy::get_item>(w, input, Int{1})
                .as<TS<ArrayOf<Int, 2>>>();
        }
    };

    struct MatrixItemGraph
    {
        static constexpr auto name = "matrix_item_graph";

        static Port<TS<Int>> compose(Wiring &w,
                                     Port<TS<ArrayOf<Int, 3, 2>>> input)
        {
            const auto *tuple_meta = TypeRegistry::instance().tuple(
                {scalar_descriptor<Int>::value_meta(), scalar_descriptor<Int>::value_meta()});
            Value index{ValuePlanFactory::instance().type_for(tuple_meta)};
            auto tuple = index.as_tuple().begin_mutation();
            Value row{Int{1}};
            Value column{Int{0}};
            tuple.at(0).copy_from(row.view());
            tuple.at(1).copy_from(column.view());
            return wire<stdlib::numpy::get_item>(w, input, std::move(index)).as<TS<Int>>();
        }
    };

    struct CumsumAxisGraph
    {
        static constexpr auto name = "cumsum_axis_graph";

        static Port<TS<ArrayOf<Int, 2, 3>>> compose(
            Wiring &w, Port<TS<ArrayOf<Int, 2, 3>>> input)
        {
            return wire<stdlib::numpy::cumsum, TS<ArrayOf<Int, 2, 3>>>(
                w, input, Int{0});
        }
    };

    struct CumsumLastAxisGraph
    {
        static constexpr auto name = "cumsum_last_axis_graph";

        static Port<TS<ArrayOf<Int, 2, 3>>> compose(
            Wiring &w, Port<TS<ArrayOf<Int, 2, 3>>> input)
        {
            return wire<stdlib::numpy::cumsum, TS<ArrayOf<Int, 2, 3>>>(
                w, input, Int{-1});
        }
    };

    struct CorrcoefGraph
    {
        static constexpr auto name = "corrcoef_graph";

        static Port<TS<ArrayOf<Float, 2, 2>>> compose(
            Wiring &w, Port<TS<ArrayOf<Float, 2, 4>>> input)
        {
            return wire<stdlib::numpy::corrcoef, TS<ArrayOf<Float, 2, 2>>>(
                w, input, Bool{true});
        }
    };

    struct CorrcoefScalarGraph
    {
        static constexpr auto name = "corrcoef_scalar_graph";

        static Port<TS<Float>> compose(
            Wiring &w, Port<TS<ArrayOf<Float, 4>>> input)
        {
            return wire<stdlib::numpy::corrcoef, TS<Float>>(
                w, input, Bool{true});
        }
    };

    struct CorrcoefPairGraph
    {
        static constexpr auto name = "corrcoef_pair_graph";

        static Port<TS<ArrayOf<Float, 2, 2>>> compose(
            Wiring &w, Port<TS<ArrayOf<Float, 4>>> input)
        {
            return wire<stdlib::numpy::corrcoef, TS<ArrayOf<Float, 2, 2>>>(
                w, input, input, Bool{true});
        }
    };

    struct CorrcoefColumnsGraph
    {
        static constexpr auto name = "corrcoef_columns_graph";

        static Port<TS<ArrayOf<Float, 2, 2>>> compose(
            Wiring &w, Port<TS<ArrayOf<Float, 3, 2>>> input)
        {
            return wire<stdlib::numpy::corrcoef, TS<ArrayOf<Float, 2, 2>>>(
                w, input, Bool{false});
        }
    };

    struct QuantileGraph
    {
        static constexpr auto name = "quantile_graph";

        static Port<TS<Float>> compose(Wiring &w, Port<TS<ArrayOf<Int, 4>>> input)
        {
            auto q = wire<stdlib::const_, TS<Float>>(w, Float{0.5});
            return wire<stdlib::numpy::quantile>(w, input, q, Str{"linear"}, Bool{false})
                .as<TS<Float>>();
        }
    };

    template <fixed_string Method>
    struct QuantileMethodGraph
    {
        static constexpr auto name = "quantile_method_graph";

        static Port<TS<Float>> compose(Wiring &w, Port<TS<ArrayOf<Int, 4>>> input)
        {
            auto q = wire<stdlib::const_, TS<Float>>(w, Float{0.625});
            return wire<stdlib::numpy::quantile>(
                       w, input, q, Str{Method.sv()}, Bool{false})
                .template as<TS<Float>>();
        }
    };

    using RollingInt3 = TSB<"NpRollingWindowResult[int,3]",
                            Field<"buffer", TS<ArrayOf<Int, 3>>>,
                            Field<"index", TS<ArrayOf<DateTime, 3>>>>;
    using RollingInt3Min2 = TSB<"NpRollingWindowResult[int,3,2]",
                                Field<"buffer", TS<ArrayOf<Int>>>,
                                Field<"index", TS<ArrayOf<DateTime>>>>;

    struct RollingWindowGraph
    {
        static constexpr auto name = "rolling_window_array_graph";

        static Port<RollingInt3> compose(Wiring &w, Port<TS<Int>> input)
        {
            auto window = wire<stdlib::to_window>(w, input, Int{3}, Int{3})
                              .as<TSW<Int, 3, 3>>();
            return wire<stdlib::numpy::rolling_window_arrays, RollingInt3>(w, window);
        }
    };

    struct RollingWindowMinGraph
    {
        static constexpr auto name = "rolling_window_array_min_graph";

        static Port<RollingInt3Min2> compose(Wiring &w, Port<TS<Int>> input)
        {
            auto window = wire<stdlib::to_window>(w, input, Int{3}, Int{2})
                              .as<TSW<Int, 3, 2>>();
            return wire<stdlib::numpy::rolling_window_arrays, RollingInt3Min2>(w, window);
        }
    };

    struct StandardDeviationGraph
    {
        static constexpr auto name = "standard_deviation_graph";

        static Port<TS<Float>> compose(Wiring &w, Port<TS<ArrayOf<Int, 4>>> input)
        {
            return wire<stdlib::numpy::standard_deviation>(w, input, Int{1})
                .as<TS<Float>>();
        }
    };

    struct PercentageChangeGraph
    {
        static constexpr auto name = "percentage_change_graph";

        static Port<TS<Float>> compose(Wiring &w, Port<TS<Int>> input)
        {
            return wire<stdlib::pct_change>(w, input).as<TS<Float>>();
        }
    };
}  // namespace

TEST_CASE("numpy operators: as_array preserves readiness and pads warm windows")
{
    stdlib::register_standard_operators();
    const auto full = eval_node<AsArrayGraph>(values<Int>(1, 2, 3));
    REQUIRE(full.size() == 3);
    CHECK_FALSE(full[0].has_value());
    CHECK_FALSE(full[1].has_value());
    REQUIRE(full[2].has_value());
    CHECK(full[2]->equals(array_1d<Int>({1, 2, 3})));

    const auto padded = eval_node<AsArrayPaddingGraph>(values<Int>(1, 2, 3));
    REQUIRE(padded.size() == 3);
    CHECK_FALSE(padded[0].has_value());
    REQUIRE(padded[1].has_value());
    REQUIRE(padded[2].has_value());
    CHECK(padded[1]->equals(array_1d<Int>({1, 2, 0})));
    CHECK(padded[2]->equals(array_1d<Int>({1, 2, 3})));

    const auto scalar_padded = eval_node<AsArrayScalarPaddingGraph>(values<Int>(1, 2));
    REQUIRE(scalar_padded[1].has_value());
    CHECK(scalar_padded[1]->equals(array_1d<Int>({1, 2, -1})));
}

TEST_CASE("numpy operators: fixed shaped arrays retain inline capacity with a logical extent")
{
    const Value partial = array_1d<Int>({1, 2}, 4);
    CHECK(partial.schema()->fixed_size == 4);
    CHECK(partial.as_list().size() == 2);
    CHECK(partial.as_list().at(0).checked_as<Int>() == 1);
    CHECK(partial.as_list().at(1).checked_as<Int>() == 2);

    Value copy{partial.binding()};
    copy.begin_mutation().copy_from(partial.view());
    CHECK(copy.equals(partial));
    CHECK(copy.as_list().size() == 2);

    auto resized = copy.as_list().begin_mutation();
    resized.resize(4);
    CHECK(resized.size() == 4);
    CHECK_THROWS_AS(resized.resize(5), std::out_of_range);
}

TEST_CASE("numpy operators: get_item resolves scalar and sliced outputs")
{
    stdlib::register_standard_operators();
    const Value matrix = array_2d<Int>({{1, 2}, {3, 4}, {5, 6}});

    const auto row = eval_node<MatrixRowGraph>(values<Value>(matrix));
    REQUIRE(row.size() == 1);
    REQUIRE(row[0].has_value());
    CHECK(row[0]->equals(array_1d<Int>({3, 4})));

    CHECK(eval_node<MatrixItemGraph>(values<Value>(matrix)) ==
          std::vector<std::optional<Int>>{Int{3}});
}

TEST_CASE("numpy operators: cumsum supports flattened and axis accumulation")
{
    stdlib::register_standard_operators();
    const Value matrix = array_2d<Int>({{1, 2, 3}, {4, 5, 6}});

    const auto flattened = eval_node<stdlib::numpy::cumsum,
                                     TS<ArrayOf<Int, 2, 3>>>(values<Value>(matrix));
    REQUIRE(flattened.size() == 1);
    REQUIRE(flattened[0].has_value());
    CHECK(flattened[0]->equals(array_1d<Int>({1, 3, 6, 10, 15, 21})));

    const auto axis = eval_node<CumsumAxisGraph>(values<Value>(matrix));
    REQUIRE(axis.size() == 1);
    REQUIRE(axis[0].has_value());
    CHECK(axis[0]->equals(array_2d<Int>({{1, 2, 3}, {5, 7, 9}})));

    const auto last_axis = eval_node<CumsumLastAxisGraph>(values<Value>(matrix));
    REQUIRE(last_axis[0].has_value());
    CHECK(last_axis[0]->equals(array_2d<Int>({{1, 3, 6}, {4, 9, 15}})));

    const Value overflowing = array_1d<Int>(
        {std::numeric_limits<Int>::max(), Int{1}});
    const auto wrapped = eval_node<stdlib::numpy::cumsum,
                                   TS<ArrayOf<Int, 2>>>(values<Value>(overflowing));
    REQUIRE(wrapped[0].has_value());
    CHECK(wrapped[0]->equals(array_1d<Int>(
        {std::numeric_limits<Int>::max(), std::numeric_limits<Int>::min()})));
}

TEST_CASE("numpy operators: corrcoef covers matrix and paired vector forms")
{
    stdlib::register_standard_operators();
    const Value matrix = array_2d<Float>({{1.0, 2.0, 3.0, 4.0},
                                         {1.0, 2.0, 3.0, 4.0}});
    const auto matrix_result = eval_node<CorrcoefGraph>(values<Value>(matrix));
    REQUIRE(matrix_result.size() == 1);
    REQUIRE(matrix_result[0].has_value());
    CHECK(matrix_result[0]->equals(array_2d<Float>({{1.0, 1.0}, {1.0, 1.0}})));

    const Value vector = array_1d<Float>({1.0, 2.0, 3.0, 4.0});
    const auto pair_result = eval_node<CorrcoefPairGraph>(values<Value>(vector));
    REQUIRE(pair_result.size() == 1);
    REQUIRE(pair_result[0].has_value());
    CHECK(pair_result[0]->equals(array_2d<Float>({{1.0, 1.0}, {1.0, 1.0}})));

    const Value columns = array_2d<Float>({{1.0, 2.0}, {2.0, 4.0}, {3.0, 6.0}});
    const auto column_result = eval_node<CorrcoefColumnsGraph>(values<Value>(columns));
    REQUIRE(column_result[0].has_value());
    CHECK(column_result[0]->equals(array_2d<Float>({{1.0, 1.0}, {1.0, 1.0}})));

    const Value constant = array_1d<Float>({2.0, 2.0, 2.0, 2.0});
    const auto scalar_result = eval_node<CorrcoefScalarGraph>(values<Value>(constant));
    REQUIRE(scalar_result[0].has_value());
    CHECK(std::isnan(*scalar_result[0]));
}

TEST_CASE("numpy operators: quantile delegates interpolation to Arrow Compute")
{
    stdlib::register_standard_operators();
    const Value input = array_1d<Int>({1, 2, 3, 4});
    const auto result = eval_node<QuantileGraph>(values<Value>(input));
    REQUIRE(result.size() == 1);
    REQUIRE(result[0].has_value());
    CHECK(*result[0] == Catch::Approx(2.5));

    CHECK(eval_node<QuantileMethodGraph<"linear">>(values<Value>(input))[0] ==
          Catch::Approx(2.875));
    CHECK(eval_node<QuantileMethodGraph<"lower">>(values<Value>(input))[0] ==
          Catch::Approx(2.0));
    CHECK(eval_node<QuantileMethodGraph<"higher">>(values<Value>(input))[0] ==
          Catch::Approx(3.0));
    CHECK(eval_node<QuantileMethodGraph<"midpoint">>(values<Value>(input))[0] ==
          Catch::Approx(2.5));
    CHECK(eval_node<QuantileMethodGraph<"nearest">>(values<Value>(input))[0] ==
          Catch::Approx(3.0));
}

TEST_CASE("numpy operators: rolling windows expose native value and timestamp arrays")
{
    stdlib::register_standard_operators();
    const auto full = eval_node<RollingWindowGraph>(values<Int>(1, 2, 3));
    REQUIRE(full.size() == 3);
    CHECK_FALSE(full[0].has_value());
    CHECK_FALSE(full[1].has_value());
    REQUIRE(full[2].has_value());
    const auto full_bundle = full[2]->as_bundle();
    CHECK(full_bundle["buffer"].equals(array_1d<Int>({1, 2, 3}).view()));
    CHECK(full_bundle["index"].equals(
        array_1d<DateTime>({MIN_ST, MIN_ST + MIN_TD, MIN_ST + MIN_TD * 2}).view()));

    const auto partial = eval_node<RollingWindowMinGraph>(values<Int>(1, 2, 3));
    REQUIRE(partial.size() == 3);
    CHECK_FALSE(partial[0].has_value());
    REQUIRE(partial[1].has_value());
    CHECK(partial[1]->as_bundle()["buffer"].equals(array_1d<Int>({1, 2}).view()));
    CHECK(partial[2]->as_bundle()["buffer"].equals(array_1d<Int>({1, 2, 3}).view()));
}

TEST_CASE("numpy operators: standard deviation honors degrees of freedom")
{
    stdlib::register_standard_operators();
    const Value input = array_1d<Int>({1, 2, 3, 4});
    const auto population = eval_node<stdlib::numpy::standard_deviation,
                                      TS<ArrayOf<Int, 4>>>(values<Value>(input));
    REQUIRE(population[0].has_value());
    CHECK(population[0]->view().checked_as<Float>() == Catch::Approx(std::sqrt(1.25)));

    const auto sample = eval_node<StandardDeviationGraph>(values<Value>(input));
    REQUIRE(sample[0].has_value());
    CHECK(sample[0].value() == Catch::Approx(std::sqrt(5.0 / 3.0)));

    const Value offset = array_1d<Float>(
        {1.0e16, 1.0e16, 1.0e16 + 2.0, 1.0e16 + 4.0});
    const auto stable = eval_node<stdlib::numpy::standard_deviation,
                                  TS<ArrayOf<Float, 4>>>(values<Value>(offset));
    REQUIRE(stable[0].has_value());
    CHECK(stable[0]->view().checked_as<Float>() == Catch::Approx(std::sqrt(5.0)));
}

TEST_CASE("analytical operators: percentage change is a native graph")
{
    stdlib::register_standard_operators();
    CHECK_OUTPUT(eval_node<PercentageChangeGraph>(values<Int>(1, 2, 3)),
                 values<Float>(none, 1.0, 0.5));
}
