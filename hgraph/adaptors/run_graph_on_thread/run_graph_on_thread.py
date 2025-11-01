from datetime import datetime, time
import logging
from threading import Thread
import traceback
from typing import Callable, Generic, Type
from hgraph import (
    AUTO_RESOLVE,
    DEFAULT,
    MAX_DT,
    MIN_DT,
    OUT,
    TIME_SERIES_TYPE,
    TS,
    TSB,
    TSD,
    EvaluationMode,
    GlobalState,
    GraphConfiguration,
    TimeSeriesSchema,
    evaluate_graph,
    graph,
    map_,
    push_queue,
    service_adaptor,
    service_adaptor_impl,
    sink_node,
)

logger = logging.getLogger(__name__)


class RunGraphOutput(TimeSeriesSchema, Generic[OUT]):
    out: OUT
    started: TS[bool]
    finished: TS[bool]
    status: TS[str]


@sink_node
def publish_output(ts: TIME_SERIES_TYPE):
    GlobalState.instance().output_queue(ts.delta_value)


@service_adaptor
def run_graph_on_thread(
    fn: TS[Callable],
    global_state: TS[dict[str, object]],
    params: TS[dict[str, object]],
    out_: Type[OUT] = DEFAULT[OUT],
    path: str = "thread_graph_runner",
) -> TSB[RunGraphOutput[OUT]]:
    pass


@service_adaptor_impl(interfaces=run_graph_on_thread)
def run_graph_on_thread_impl(
    fn: TSD[int, TS[Callable]],
    global_state: TSD[int, TS[dict[str, object]]],
    params: TSD[int, TS[dict[str, object]]],
    out_: Type[OUT] = AUTO_RESOLVE,
    path: str = "thread_graph_runner",
) -> TSD[int, TSB[RunGraphOutput[OUT]]]:

    GlobalState.init_multithreading()
    path = f"{path}[{out_}]"

    @push_queue(TSD[int, TSB[RunGraphOutput[out_]]])
    def receive(sender: Callable, path: str):
        GlobalState.instance()[path] = sender

    @sink_node
    def started(x: TS[bool] = True):
        GlobalState.instance().started_queue(True)

    def run(fn, global_state, params, output_queue):
        try:
            gs = GlobalState(
                **global_state,
                output_queue=lambda x: output_queue({"out": x}),
                started_queue=lambda x: output_queue({"started": x}),
                status_queue=lambda x: output_queue({"status": x}),
            )

            with gs:
                start_time = params.get("start_time", datetime.combine(params.get("start_date", MIN_DT.date()), time()))
                end_time = params.get("end_time", datetime.combine(params.get("end_date", MAX_DT.date()), time()))
                params = {k: v for k, v in params.items() if k in fn.signature.args}

                @graph
                def g():
                    fn(**params)
                    started()

                evaluate_graph(
                    g,
                    GraphConfiguration(
                        run_mode=EvaluationMode.SIMULATION,
                        start_time=start_time,
                        end_time=end_time,
                        # trace=True
                    ),
                )
                output_queue({"finished": True})
                output_queue({"status": "OK"})
        except Exception as e:
            logger.exception(e)
            output_queue({"finished": True, "status": "ERROR:" + str(e) + "\n" + traceback.format_exc()})

    @sink_node
    def _run_graph_on_thread(
        i: TS[int], fn: TS[Callable], global_state: TS[dict[str, object]], params: TS[dict[str, object]], path: str
    ):
        output_queue = GlobalState.instance().get(path)
        Thread(
            target=run, args=(fn.value, global_state.value, params.value, lambda x: output_queue({i.value: x}))
        ).start()

    map_(lambda key, f, gs, p: _run_graph_on_thread(key, f, gs, p, path), f=fn, gs=global_state, p=params)
    return receive(path=path)
