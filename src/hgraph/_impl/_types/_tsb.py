from dataclasses import dataclass
from datetime import datetime
from typing import Any, Mapping, Generic, TYPE_CHECKING, cast

from hgraph._runtime._constants import MIN_DT
from hgraph._impl._types._input import PythonBoundTimeSeriesInput
from hgraph._impl._types._output import PythonTimeSeriesOutput
from hgraph._types._time_series_types import TimeSeriesOutput, TimeSeriesInput
from hgraph._types._tsb_type import TimeSeriesBundleInput, TS_SCHEMA, TimeSeriesBundleOutput

if TYPE_CHECKING:
    from hgraph._runtime._node import Node

__all__ = ("PythonTimeSeriesBundleOutput", "PythonTimeSeriesBundleInput")


# With Bundles there are two implementation types, namely bound and un-bound.
# Bound bundles are those that are bound to a specific output, and un-bound bundles are those that are not.
# A bound bundle has a peer output of the same shape that this bundle can map directly to. A bound bundle
# has a higher performance characteristic, as it does not need to loop over all the inputs to determine
# things such as active, modified, etc.


@dataclass
class PythonTimeSeriesBundleOutput(PythonTimeSeriesOutput, TimeSeriesBundleOutput[TS_SCHEMA], Generic[TS_SCHEMA]):

    def __init__(self, schema: TS_SCHEMA, *args, **kwargs):
        Generic.__init__(self)
        TimeSeriesBundleOutput.__init__(self, schema)
        PythonTimeSeriesOutput.__init__(self, *args, **kwargs)

    @property
    def value(self):
        if s := self.__schema__.scalar_type():
            return s(**{k: ts.value for k, ts in self.items()})
        else:
            return {k: ts.value for k, ts in self.items() if ts.valid}

    @value.setter
    def value(self, v: Mapping[str, Any] | None):
        if v is None:
            self.invalidate()
        else:
            if type(v) is self.__schema__.scalar_type():
                for k in self.__schema__._schema_keys():
                    if (i := getattr(v, k, None)) is not None:
                        cast(TimeSeriesOutput, self[k]).value = i
            else:
                for k, v_ in v.items():
                    if v_ is not None:
                        cast(TimeSeriesOutput, self[k]).value = v_

    def clear(self):
        for v in self.values():
            v.clear()

    def invalidate(self):
        if self.valid:
            for v in self.values():
                v.invalidate()
        self.mark_invalid()

    @property
    def delta_value(self):
        return {k: ts.delta_value for k, ts in self.items() if ts.modified and ts.valid}

    def can_apply_result(self, result: Mapping[str, Any] | None) -> bool:
        if result is None:
            return True
        else:
            if type(result) is self.__schema__.scalar_type():
                return self.modified
            else:
                for k, v_ in result.items():
                    if v_ is not None:
                        if not cast(TimeSeriesOutput, self[k]).can_apply_result(v_):
                            return False
        return True

    def apply_result(self, result: Mapping[str, Any] | None):
        if result is None:
            return
        self.value = result

    def copy_from_output(self, output: TimeSeriesOutput):
        if not isinstance(output, PythonTimeSeriesBundleOutput):
            raise TypeError(f"Expected {PythonTimeSeriesBundleOutput}, got {type(output)}")
        # TODO: Put in some validation that the signatures are compatible?
        for k, v in output.items():
            self[k].copy_from_output(v)

    def copy_from_input(self, input: TimeSeriesInput):
        if not isinstance(input, TimeSeriesBundleInput):
            raise TypeError(f"Expected {TimeSeriesBundleInput}, got {type(input)}")
        for k, v in input.items():
            self[k].copy_from_input(v)

    def mark_invalid(self):
        if self.valid:
            super().mark_invalid()
            for v in self._ts_value.values():
                v.mark_invalid()

    @property
    def all_valid(self) -> bool:
        return all(ts.valid for ts in self._ts_value.values())


class PythonTimeSeriesBundleInput(PythonBoundTimeSeriesInput, TimeSeriesBundleInput[TS_SCHEMA], Generic[TS_SCHEMA]):
    """
    The bound TSB has a corresponding peer output that it is bound to.
    This means most all methods can be delegated to the output. This is slightly more efficient than the unbound version.
    """

    def __init__(self, schema: TS_SCHEMA, owning_node: "Node" = None, parent_input: "TimeSeriesInput" = None):
        Generic.__init__(self)
        TimeSeriesBundleInput.__init__(self, schema)
        PythonBoundTimeSeriesInput.__init__(self, _owning_node=owning_node, _parent_input=parent_input)

    def set_subscribe_method(self, subscribe_input: bool):
        super().set_subscribe_method(subscribe_input)
        for ts in self.values():
            ts.set_subscribe_method(subscribe_input)

    @property
    def all_valid(self) -> bool:
        return all(ts.valid for ts in self.values())

    @property
    def bound(self) -> bool:
        return super().bound or any(ts.bound for ts in self.values())

    def do_bind_output(self, output: TimeSeriesOutput) -> bool:
        output: PythonTimeSeriesBundleOutput
        peer = True
        for k, ts in self.items():
            peer &= ts.bind_output(output[k])

        super().do_bind_output(output if peer else None)
        return peer

    def do_un_bind_output(self):
        for ts in self.values():
            ts.un_bind_output()
        if self.has_peer:
            super().do_un_bind_output()

    @property
    def value(self) -> Any:
        if self.has_peer:
            return super().value
        else:
            if s := self.__schema__.scalar_type():
                v = {k: ts.value for k, ts in self.items() if ts.valid or getattr(s, k, None) is None}
                return s(**v)
            else:
                return {k: ts.value for k, ts in self.items() if ts.valid}

    @property
    def delta_value(self) -> Mapping[str, Any]:
        if self.has_peer:
            return super().delta_value
        else:
            return {k: ts.delta_value for k, ts in self.items() if ts.modified and ts.valid}

    @property
    def active(self) -> bool:
        """
        For UnBound TS, if any of the elements are active we report the input as active,
        Note, that make active / make passive will make ALL instances active / passive.
        Thus, just because the input returns True for active, it does not mean that make_active is a no-op.
        """
        if self.has_peer:
            return super().active
        else:
            return any(ts.active for ts in self.values())

    def make_active(self):
        if self.has_peer:
            super().make_active()
        else:
            for ts in self.values():
                ts.make_active()

    def make_passive(self):
        if self.has_peer:
            super().make_passive()
        else:
            for ts in self.values():
                ts.make_passive()

    @property
    def modified(self) -> bool:
        if self.has_peer:
            return super().modified
        else:
            return any(ts.modified for ts in self.values())

    @property
    def valid(self) -> bool:
        if self.has_peer:
            return super().valid
        else:
            return any(ts.valid for ts in self.values())

    @property
    def last_modified_time(self) -> datetime:
        if self.has_peer:
            return super().last_modified_time
        else:
            return max((ts.last_modified_time for ts in self.values()), default=MIN_DT)
