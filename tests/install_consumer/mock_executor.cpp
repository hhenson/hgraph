#include <hgraph/lib/testing/mock_runtime.h>
#include <hgraph/types/graph_wiring.h>

#include <utility>

int main()
{
    hgraph::Wiring wiring;
    hgraph::testing::MockGraphExecutor executor{std::move(wiring).finish()};
    if (!executor.has_value()) { return 1; }

    const auto clock = executor.view().evaluation_clock();
    if (!clock.valid() || clock.evaluation_time() != hgraph::MIN_ST) { return 2; }
    const auto next = hgraph::MIN_ST + hgraph::MIN_TD;
    executor.set_evaluation_time(next);
    return clock.evaluation_time() == next ? 0 : 3;
}
