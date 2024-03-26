from hgraph import sink_node, SIGNAL, EvaluationEngineApi

__all__ = ("stop_engine",)


@sink_node
def stop_engine(ts: SIGNAL, msg: str = "Stopping", _engine : EvaluationEngineApi = None):
    """ Stops the engine """
    print(
        f"[{_engine.evaluation_clock.now}][{_engine.evaluation_clock.evaluation_time}] stop_engine: {msg}")
    _engine.request_engine_stop()
