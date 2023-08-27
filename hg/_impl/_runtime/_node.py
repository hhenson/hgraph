from dataclasses import dataclass
from typing import Optional, Mapping

from hg._runtime import Node, NodeSignature


@dataclass
class NodeImpl(Node):

    signature: NodeSignature


    @property
    def signature(self) -> NodeSignature:
        pass

    @property
    def input(self) -> Optional["TimeSeriesBundleInput"]:
        pass

    @property
    def inputs(self) -> Optional[Mapping[str, "TimeSeriesInput"]]:
        pass

    @property
    def output(self) -> Optional["TimeSeries"]:
        pass

    @property
    def outputs(self) -> Optional[Mapping[str, "TimeSeriesOutput"]]:
        pass