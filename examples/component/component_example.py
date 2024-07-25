from frozendict import frozendict as fd

from hgraph import (
    component,
    TSD,
    TS,
    mul_,
    map_,
    GraphConfiguration,
    EvaluationMode,
    evaluate_graph,
    graph,
    debug_print,
    MIN_ST,
    MIN_TD,
    replay_from_memory,
    set_replay_values,
    SimpleArrayReplaySource,
)


@component
def compute_signal(returns: TSD[str, TS[float]], factors: TSD[str, TS[float]]) -> TSD[str, TS[float]]:
    # Start with a very simple idea
    return map_(mul_, returns, factors)


@graph
def main():
    returns = replay_from_memory("returns", TSD[str, TS[float]])
    factors = replay_from_memory("factors", TSD[str, TS[float]])
    signal = compute_signal(returns, factors)
    debug_print("signal", signal)


def run_record():
    set_replay_values("returns", SimpleArrayReplaySource([fd(a=0.25, b=1.2), fd(a=0.27, b=1.3)]))
    set_replay_values("factors", SimpleArrayReplaySource([fd(a=0.1, b=0.2), fd(a=0.12, b=0.18)]))
    config = GraphConfiguration(run_mode=EvaluationMode.SIMULATION)
    evaluate_graph(main, config)


def run_simulation():
    set_replay_values("returns", SimpleArrayReplaySource([fd(a=0.25, b=1.2), fd(a=0.27, b=1.3)]))
    set_replay_values("factors", SimpleArrayReplaySource([fd(a=0.1, b=0.2), fd(a=0.12, b=0.18)]))
    config = GraphConfiguration(run_mode=EvaluationMode.SIMULATION)
    evaluate_graph(main, config)


def run_replay():
    set_replay_values("returns", SimpleArrayReplaySource([]))
    set_replay_values("factors", SimpleArrayReplaySource([]))
    # This should replay using the recorded data, ignoring any incoming data until the recorded data is processed.
    # Once we have replayed the data the graph returns to RECORD mode and will process any new data received.
    config = GraphConfiguration(run_mode=EvaluationMode.SIMULATION)
    evaluate_graph(main, config)


def run_compare():
    # In this mode, we run the component itself to see if the component will reproduce itself.
    config = GraphConfiguration(run_mode=EvaluationMode.SIMULATION)
    evaluate_graph(compute_signal, config)


def run_recover():
    set_replay_values("returns", SimpleArrayReplaySource([fd(a=0.26, b=1.4), fd(a=0.23, b=1.35)], MIN_ST + MIN_TD * 3))
    set_replay_values("factors", SimpleArrayReplaySource([fd(a=0.11, b=0.19), fd(a=0.09, b=0.21)], MIN_ST + MIN_TD * 3))
    # This should identify that there is recorded state, reload the last known state and then continue from that point on.
    config = GraphConfiguration(run_mode=EvaluationMode.SIMULATION)
    evaluate_graph(main, config)


if __name__ == "__main__":
    run_simulation()
    # run_record()
    # run_replay()
    # run_compare()
    # run_recover()
