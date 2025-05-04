from dataclasses import dataclass, field
from typing import Callable, Any, Sequence

from hgraph._builder._output_builder import OutputBuilder
from hgraph._types._scalar_types import SCALAR
from hgraph._types._time_series_types import TimeSeriesOutput

__all__ = ("FeatureOutputExtension", "FeatureOutputRequestTracker")


@dataclass
class FeatureOutputRequestTracker:
    output: TimeSeriesOutput
    requesters: set = field(default_factory=set)


@dataclass
class FeatureOutputExtension:
    owning_output: TimeSeriesOutput
    output_builder: OutputBuilder
    value_getter: Callable[[TimeSeriesOutput, TimeSeriesOutput, SCALAR], None]
    initial_value_getter: Callable[[TimeSeriesOutput, TimeSeriesOutput, SCALAR], None] | None = None
    _outputs: dict[SCALAR, FeatureOutputRequestTracker] = field(default_factory=dict)

    def create_or_increment(self, key: SCALAR, requester: object) -> TimeSeriesOutput:
        tracker = self._outputs.get(key, None)
        if tracker is None:
            # Features are bound to the node, but not the output they are associated to.
            tracker = FeatureOutputRequestTracker(
                output=self.output_builder.make_instance(owning_node=self.owning_output.owning_node)
            )
            self._outputs[key] = tracker
            (
                self.value_getter(self.owning_output, tracker.output, key)
                if self.initial_value_getter is None
                else self.initial_value_getter(self.owning_output, tracker.output, key)
            )
        tracker.requesters.add(requester)
        return tracker.output

    def update(self, key: SCALAR):
        tracker = self._outputs.get(key, None)
        if tracker is not None:
            self.value_getter(self.owning_output, tracker.output, key)

    def update_all(self, keys: Sequence[SCALAR]):
        if self._outputs:
            for key in keys:  # This may be better if we do a pre-intersection check
                self.update(key)

    def release(self, key: SCALAR, requester: object):
        tracker = self._outputs.get(key, None)
        if tracker is not None:
            tracker.requesters.remove(requester)
            if not tracker.requesters:
                del self._outputs[key]
