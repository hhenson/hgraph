from datetime import timedelta
from hgraph import (
    MIN_ST,
    TS,
    EvaluationMode,
    GlobalState,
    GraphConfiguration,
    evaluate_graph,
    get_recorded_value,
    graph,
    record,
    register_adaptor,
)
from hgraph.adaptors.run_graph_on_thread import publish_output, run_graph_on_thread, run_graph_on_thread_impl


def test_run_graph_on_thread():
    @graph
    def sig(a: TS[int], b: TS[int]) -> TS[int]:
        return a + b

    @graph
    def simulation(a: TS[int], b: TS[int]):
        c = sig(a, b)
        publish_output(c)

    @graph
    def main():
        register_adaptor(None, run_graph_on_thread_impl)

        record(
            run_graph_on_thread[TS[int]](
                fn=simulation,
                global_state={"eh": 0},
                params=dict(
                    a=1,
                    b=2,
                    start_time=MIN_ST,
                    end_time=MIN_ST + timedelta(seconds=10),
                ),
            )
        )

    with GlobalState():
        evaluate_graph(
            main, config=GraphConfiguration(run_mode=EvaluationMode.REAL_TIME, end_time=timedelta(seconds=1))
        )
        value = get_recorded_value()
        assert value[0][1]["out"] == 3
