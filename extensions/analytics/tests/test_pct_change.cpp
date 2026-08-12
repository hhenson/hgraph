#include <hgraph/analytics/operators.h>

#include <hgraph/lib/std/operators/registration.h>
#include <hgraph/lib/testing/eval_node.h>

#include <cmath>
#include <cstddef>
#include <iostream>
#include <optional>
#include <stdexcept>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

namespace
{
    using namespace hgraph;
    using namespace hgraph::analytics;
    using namespace hgraph::testing;

    inline constexpr std::nullopt_t none = std::nullopt;

    template <typename T, typename U>
    [[nodiscard]] std::optional<T> optional_value(U &&value)
    {
        return T{std::forward<U>(value)};
    }

    template <typename T>
    [[nodiscard]] std::optional<T> optional_value(std::nullopt_t)
    {
        return std::nullopt;
    }

    template <typename T, typename... Args>
    [[nodiscard]] std::vector<std::optional<T>> values(Args &&...args)
    {
        std::vector<std::optional<T>> output;
        output.reserve(sizeof...(Args));
        (output.push_back(optional_value<T>(std::forward<Args>(args))), ...);
        return output;
    }

    void require(bool condition, std::string message)
    {
        if (!condition) { throw std::runtime_error(std::move(message)); }
    }

    void require_output(
        const std::vector<std::optional<Float>> &actual,
        const std::vector<std::optional<Float>> &expected,
        const std::string &label)
    {
        require(actual.size() == expected.size(), label + ": output size");
        for (std::size_t index = 0; index < expected.size(); ++index)
        {
            require(actual[index].has_value() == expected[index].has_value(),
                    label + ": readiness at index " + std::to_string(index));
            if (expected[index].has_value())
            {
                require(std::abs(*actual[index] - *expected[index]) <= 1.0e-12,
                        label + ": value at index " + std::to_string(index));
            }
        }
    }

    struct DefaultIntChange
    {
        static constexpr auto name = "analytics_default_int_change";

        static Port<TS<Float>> compose(Wiring &w, Port<TS<Int>> ts)
        {
            return wire<pct_change, TS<Float>>(w, ts);
        }
    };

    struct TwoPeriodFloatChange
    {
        static constexpr auto name = "analytics_two_period_float_change";

        static Port<TS<Float>> compose(Wiring &w, Port<TS<Float>> ts)
        {
            return wire<pct_change, TS<Float>>(w, ts, Int{2});
        }
    };

    template <stdlib::DivideByZero Policy>
    struct ZeroPolicyChange
    {
        static constexpr auto name = "analytics_zero_policy_change";

        static Port<TS<Float>> compose(Wiring &w, Port<TS<Float>> ts)
        {
            return wire<pct_change, TS<Float>>(w, ts, Int{1}, Policy);
        }
    };

    struct InvalidPeriodChange
    {
        static constexpr auto name = "analytics_invalid_period_change";

        static Port<TS<Float>> compose(Wiring &w, Port<TS<Float>> ts)
        {
            return wire<pct_change, TS<Float>>(w, ts, Int{0});
        }
    };

    void test_default_and_sparse_observations()
    {
        require_output(eval_node<DefaultIntChange>(values<Int>(1, 2, 3)),
                       {std::nullopt, 1.0, 0.5}, "default period");

        require_output(eval_node<DefaultIntChange>(values<Int>(1, none, 2)),
                       {std::nullopt, std::nullopt, 1.0}, "sparse observations");
    }

    void test_period_and_float_input()
    {
        require_output(
            eval_node<TwoPeriodFloatChange>(
                values<Float>(10.0, 11.0, 12.0, 15.0)),
            {std::nullopt, std::nullopt, 0.2, 4.0 / 11.0},
            "two-period change");
    }

    void test_zero_policies()
    {
        const auto nan = eval_node<ZeroPolicyChange<stdlib::DivideByZero::Nan>>(
            values<Float>(0.0, 1.0));
        require(nan.size() == 2 && !nan[0].has_value() && nan[1].has_value(),
                "NaN policy output readiness");
        require(std::isnan(*nan[1]),
                "NaN policy result");

        require_output(
            eval_node<ZeroPolicyChange<stdlib::DivideByZero::Zero>>(
                values<Float>(0.0, 1.0)),
            {std::nullopt, 0.0}, "zero policy");
        require_output(
            eval_node<ZeroPolicyChange<stdlib::DivideByZero::One>>(
                values<Float>(0.0, 1.0)),
            {std::nullopt, 1.0}, "one policy");
        require_output(
            eval_node<ZeroPolicyChange<stdlib::DivideByZero::NoTick>>(
                values<Float>(0.0, 1.0)),
            {std::nullopt, std::nullopt}, "no-tick policy");

        bool raised = false;
        try
        {
            static_cast<void>(
                eval_node<ZeroPolicyChange<stdlib::DivideByZero::Error>>(
                    values<Float>(0.0, 1.0)));
        }
        catch (const std::exception &)
        {
            raised = true;
        }
        require(raised, "Error policy rejects a zero prior value");
    }

    void test_invalid_period()
    {
        bool raised = false;
        try
        {
            static_cast<void>(eval_node<InvalidPeriodChange>(values<Float>(1.0)));
        }
        catch (const std::invalid_argument &)
        {
            raised = true;
        }
        require(raised, "non-positive period is rejected while wiring");
    }
}  // namespace

int main()
{
    try
    {
        hgraph::stdlib::register_standard_operators();
        hgraph::analytics::register_analytics_operators();
        test_default_and_sparse_observations();
        test_period_and_float_input();
        test_zero_policies();
        test_invalid_period();
        return 0;
    }
    catch (const std::exception &error)
    {
        std::cerr << error.what() << '\n';
        return 1;
    }
}
