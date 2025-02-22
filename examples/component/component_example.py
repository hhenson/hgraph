from frozendict import frozendict as fd

from hgraph import (
    EvaluationMode,
    GlobalState,
    GraphConfiguration,
    IN_MEMORY,
    MIN_ST,
    MIN_TD,
    RECORDABLE_STATE,
    RecordReplayContext,
    RecordReplayEnum,
    SIGNAL,
    SimpleArrayReplaySource,
    TS,
    TSD,
    TimeSeriesSchema,
    component,
    compute_node,
    debug_print,
    evaluate_graph,
    get_recorded_value,
    graph,
    map_,
    mul_,
    set_record_replay_model,
    set_replay_values,
    switch_,
    to_window,
)
from hgraph._impl._operators._record_replay_in_memory import replay_from_memory


class CountState(TimeSeriesSchema):
    current_count: TS[int]


@compute_node
def count_(signal: SIGNAL, _state: RECORDABLE_STATE[CountState] = None) -> TS[int]:
    _state.current_count.value += 1
    return _state.current_count.value


@count_.start
def count_start(_state: RECORDABLE_STATE[CountState]):
    if not _state.current_count.valid:
        _state.current_count.value = 0


@component
def compute_signal(returns: TSD[str, TS[float]], factors: TSD[str, TS[float]]) -> TSD[str, TS[float]]:
    # Start with a very simple idea
    count = count_(returns, __recordable_id__="count_")
    sub_count = switch_(
        count < 4,
        {
            True: lambda c: count_(c, __recordable_id__="count_"),
            False: lambda c: count_(c, __recordable_id__="count_"),
        },
        count,
    )
    window = map_(lambda x: to_window(x, period=3), returns)
    debug_print("window", window)
    debug_print("Count", count)
    debug_print("Sub Count", sub_count)
    return map_(mul_, returns, factors)


@graph
def main_graph():
    returns = replay_from_memory("returns", TSD[str, TS[float]])
    factors = replay_from_memory("factors", TSD[str, TS[float]])
    signal = compute_signal(returns, factors)
    debug_print("signal", signal)


def run_record():
    with RecordReplayContext(mode=RecordReplayEnum.RECORD):
        set_replay_values("returns", SimpleArrayReplaySource([fd(a=0.25, b=1.2), fd(a=0.27, b=1.3)]))
        set_replay_values("factors", SimpleArrayReplaySource([fd(a=0.1, b=0.2), fd(a=0.12, b=0.18)]))
        config = GraphConfiguration(run_mode=EvaluationMode.SIMULATION)
        evaluate_graph(main_graph, config)


def run_simulation():
    set_replay_values("returns", SimpleArrayReplaySource([fd(a=0.25, b=1.2), fd(a=0.27, b=1.3)]))
    set_replay_values("factors", SimpleArrayReplaySource([fd(a=0.1, b=0.2), fd(a=0.12, b=0.18)]))
    config = GraphConfiguration(run_mode=EvaluationMode.SIMULATION)
    evaluate_graph(main_graph, config)


def run_replay():
    with RecordReplayContext(mode=RecordReplayEnum.REPLAY):
        set_replay_values("returns", SimpleArrayReplaySource([]))
        set_replay_values("factors", SimpleArrayReplaySource([]))
        # This should replay using the recorded data, ignoring any incoming data until the recorded data is processed.
        # Once we have replayed the data the graph returns to RECORD mode and will process any new data received.
        config = GraphConfiguration(run_mode=EvaluationMode.SIMULATION)
        evaluate_graph(main_graph, config)


def run_replay_output():
    with RecordReplayContext(mode=RecordReplayEnum.REPLAY_OUTPUT):
        set_replay_values("returns", SimpleArrayReplaySource([]))
        set_replay_values("factors", SimpleArrayReplaySource([]))
        # This should replay using the recorded data, ignoring any incoming data until the recorded data is processed.
        # Once we have replayed the data the graph returns to RECORD mode and will process any new data received.
        config = GraphConfiguration(run_mode=EvaluationMode.SIMULATION)
        evaluate_graph(main_graph, config)


def run_compare():
    with RecordReplayContext(mode=RecordReplayEnum.COMPARE):
        # In this mode, we run the component itself to see if the component will reproduce itself.
        config = GraphConfiguration(run_mode=EvaluationMode.SIMULATION)
        evaluate_graph(main_graph, config)


def run_recover():
    with RecordReplayContext(mode=RecordReplayEnum.RECOVER):
        set_replay_values(
            "returns", SimpleArrayReplaySource([fd(a=0.26, b=1.4), fd(a=0.23, b=1.35)], MIN_ST + MIN_TD * 3)
        )
        set_replay_values(
            "factors", SimpleArrayReplaySource([fd(a=0.11, b=0.19), fd(a=0.09, b=0.21)], MIN_ST + MIN_TD * 3)
        )
        # This should identify that there is recorded state,
        # reload the last known state and then continue from that point on.
        config = GraphConfiguration(run_mode=EvaluationMode.SIMULATION, start_time=MIN_ST + MIN_TD * 2)
        evaluate_graph(main_graph, config)


def main():
    with GlobalState() as gs:
        # set_record_replay_model("DataFrame")
        set_record_replay_model(IN_MEMORY)
        # run_simulation()
        print("\nRun Record\n")
        run_record()
        returns = get_recorded_value("returns", "compute_signal")
        factors = get_recorded_value("factors", "compute_signal")
        out = get_recorded_value("__out__", "compute_signal")

        print("\nRun Replay\n")
        run_replay()
        print(gs.keys())

        print("\nRun Replay Output\n")
        run_replay_output()

        print("\nRun Compare\n")
        run_compare()

        print("\nRun Recover\n")
        run_recover()
        # ...


if __name__ == "__main__":
    main()
