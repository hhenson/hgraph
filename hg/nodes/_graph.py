from hg import sink_node, ExecutionContext, SIGNAL


@sink_node
def stop_engine(ts: SIGNAL, execution_context: ExecutionContext):
    """ Stops the engine """
    execution_context.stop_engine()