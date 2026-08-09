#include <hgraph/lib/std/std_operators.h>
#include <hgraph/lib/testing/check_output.h>
#include <hgraph/lib/testing/eval_node.h>

#include <catch2/catch_test_macros.hpp>

namespace
{
    using namespace hgraph;
    using namespace hgraph::testing;

    struct AddInts
    {
        static constexpr auto name = "feedback_add_ints";

        static void eval(In<"lhs", TS<Int>> lhs,
                         In<"rhs", TS<Int>> rhs,
                         Out<TS<Int>> out)
        {
            out.set(lhs.value() + rhs.value());
        }
    };

    struct IndependentFeedbackGraph
    {
        static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> left, Port<TS<Int>> right)
        {
            auto left_feedback  = stdlib::feedback<TS<Int>>(w, Int{0});
            auto right_feedback = stdlib::feedback<TS<Int>>(w, Int{0});

            left_feedback(left);
            right_feedback(right);

            return wire<AddInts>(w, left_feedback(), right_feedback());
        }
    };

    struct NoDefaultFeedbackGraph
    {
        static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> value)
        {
            auto feedback = stdlib::feedback<TS<Int>>(w);
            feedback(value);
            return feedback();
        }
    };

    struct FibStep
    {
        static constexpr auto name = "feedback_fib_step";

        static void start(State<Int> remaining) { remaining.set(Int{0}); }

        static void eval(In<"count", TS<Int>> count,
                         In<"a", TS<Int>> a,
                         In<"b", TS<Int>> b,
                         State<Int> remaining,
                         Out<TS<Int>> out)
        {
            Int steps_left = remaining.get();
            if (count.modified()) { steps_left = count.value(); }

            out.set(a.value() + b.value());

            if (steps_left > Int{0}) { --steps_left; }
            remaining.set(steps_left);
            if (steps_left == Int{0})
            {
                a.make_passive();
                b.make_passive();
            }
        }
    };

    struct FeedbackFibonacciGraph
    {
        static Port<TS<Int>> compose(Wiring &w)
        {
            auto a = stdlib::feedback<TS<Int>>(w, Int{0});
            auto b = stdlib::feedback<TS<Int>>(w, Int{1});

            auto count = wire<stdlib::const_, TS<Int>>(w, Int{5});
            auto c = wire<FibStep>(w, count, a(), b());
            a(b());
            b(c);
            return c;
        }
    };
}  // namespace

TEST_CASE("stdlib feedback creates independent source nodes for identical schemas")
{
    CHECK_OUTPUT(hgraph::testing::eval_node<IndependentFeedbackGraph>(
                     hgraph::testing::values<hgraph::Int>(7),
                     hgraph::testing::values<hgraph::Int>(11)),
                 hgraph::testing::values<hgraph::Int>(0, 18));
}

TEST_CASE("stdlib feedback without a default is initially silent")
{
    CHECK_OUTPUT(hgraph::testing::eval_node<NoDefaultFeedbackGraph>(
                     hgraph::testing::values<hgraph::Int>(7, 11)),
                 hgraph::testing::values<hgraph::Int>(hgraph::testing::none, 7, 11));
}

TEST_CASE("stdlib feedback supports canonical Fibonacci-style wiring")
{
    hgraph::stdlib::register_standard_operators();

    CHECK_OUTPUT(hgraph::testing::eval_node<FeedbackFibonacciGraph>(),
                 hgraph::testing::values<hgraph::Int>(1, 2, 3, 5, 8));
}

namespace
{
    struct PassiveFeedbackAccumulator
    {
        [[maybe_unused]] static constexpr auto name = "passive_feedback_accumulator";

        static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> ts)
        {
            auto fb    = stdlib::feedback<TS<Int>>(w, Int{0});
            auto total = wire<stdlib::add_>(w, ts, passive(fb())).as<TS<Int>>();
            fb(total);
            return total;
        }
    };
}  // namespace

TEST_CASE("feedback: passive consumption lets the loop quiesce naturally")
{
    stdlib::register_standard_operators();
    // No end-time bound: the adder only fires on live ticks because the
    // feedback read is passive, so the simulation ends when the inputs do.
    CHECK_OUTPUT(hgraph::testing::eval_node<PassiveFeedbackAccumulator>(
                     hgraph::testing::values<Int>(1, 2, 3)),
                 hgraph::testing::values<Int>(1, 3, 6));
}
