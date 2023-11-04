from hg import sink_node, ExecutionContext, SIGNAL


@sink_node
def stop_engine(ts: SIGNAL, msg: str = "Stopping", execution_context: ExecutionContext = None):
    """ Stops the engine """
    print(f"[{execution_context.wall_clock_time}][{execution_context.current_engine_time}] stop_engine: {msg}")
    execution_context.request_engine_stop()