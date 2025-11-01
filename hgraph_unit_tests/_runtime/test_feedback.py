from hgraph import compute_node, TS, graph, feedback
from hgraph.test import eval_node

import pytest
pytestmark = pytest.mark.smoke


def test_feedback():

    @compute_node(active=("target",))
    def trade_delta(target: TS[float], prev_position: TS[float]) -> TS[float]:
        return target.value - prev_position.value

    @compute_node(active=("traded",))
    def update_position(traded: TS[float], prev_position: TS[float]) -> TS[float]:
        return traded.value + prev_position.value

    @graph
    def trade(signal: TS[float], aum: float) -> TS[float]:
        # Allocate a new feedback instance
        position_feedback = feedback(TS[float], 0.0)  # Optionally set the initial state
        delta = trade_delta(signal * aum, position_feedback())  # Use feedback value
        position = update_position(delta, position_feedback())  # And again for good measure

        # Bind the value to feedback
        position_feedback(position)

        return position

    assert eval_node(trade, [0.75, 0.8, 0.5, 0.6], 100.0) == [75.0, 80.0, 50.0, 60.0]


# Tests to do
# Failure / forget to bind raises exception
# With default, without default
# Failure to use?
