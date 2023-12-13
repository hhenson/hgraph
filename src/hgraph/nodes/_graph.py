from hgraph import sink_node, SIGNAL, EvaluationEngineApi


@sink_node
def stop_engine(ts: SIGNAL, msg: str = "Stopping", evaluation_engine_api: EvaluationEngineApi = None):
    """ Stops the engine """
    print(
        f"[{evaluation_engine_api.evaluation_clock.now}][{evaluation_engine_api.evaluation_clock.evaluation_time}] stop_engine: {msg}")
    evaluation_engine_api.request_engine_stop()
