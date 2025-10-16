from hgraph._impl._runtime._node import BaseNodeImpl

__all__ = ["PythonContextStubSourceNode"]

class PythonContextStubSourceNode(BaseNodeImpl):
    def do_eval(self):
        """The service must be available by now, so we can retrieve the output reference."""
        from hgraph._runtime._global_state import GlobalState

        path = f'context-{self.owning_graph_id[:self.scalars["depth"]]}-{self.scalars["path"]}'
        shared = GlobalState.instance().get(path)

        from hgraph import TimeSeriesOutput

        from hgraph import TimeSeriesReferenceInput
        from hgraph import TimeSeriesReferenceOutput
        value = None
        output: TimeSeriesOutput = None
        if shared is None:
            raise RuntimeError(f"Missing shared output for path: {path}")
        elif isinstance(shared, TimeSeriesReferenceOutput):
            output = shared
            value = shared.value
        elif isinstance(shared, TimeSeriesReferenceInput):
            if shared.has_peer:  # it is a reference with a peer so its value might update
                output = shared.output
            value = shared.value
        else:
            raise RuntimeError(f"Unexpected shared output type: {type(shared)}")

        if output:
            output.subscribe(self)
            if self.subscribed_output is not None and self.subscribed_output is not output:
                self.subscribed_output.unsubscribe(self)
            self.subscribed_output = output

        # NOTE: The output needs to be a reference value output so we can set the value and continue!
        self.output.value = value  # might be none

    def do_start(self):
        """Make sure we get notified to serve the reference"""
        self.subscribed_output = None
        self.notify()

    def do_stop(self):
        if self.subscribed_output is not None:
            self.subscribed_output.unsubscribe(self)
