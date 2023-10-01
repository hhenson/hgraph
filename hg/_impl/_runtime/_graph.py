from dataclasses import dataclass

from hg._runtime._lifecycle import start_guard, stop_guard
from hg._runtime import Graph


@dataclass
class GraphImpl(Graph):
    """
    Provide a reference implementation of the Graph.
    """

    is_started: bool = False

    def initialise(self):
        ...

    @start_guard
    def start(self):
        for node in self.nodes:
            node.start()

    @stop_guard
    def stop(self):
        for node in self.nodes:
            node.stop()

    def dispose(self):
        ...

