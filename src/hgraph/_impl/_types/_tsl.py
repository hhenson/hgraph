from datetime import datetime
from typing import Generic, Optional, Any, Union

from frozendict import frozendict

from hgraph._impl._types._input import PythonBoundTimeSeriesInput
from hgraph._impl._types._output import PythonTimeSeriesOutput
from hgraph._types._scalar_types import SIZE
from hgraph._types._time_series_types import TimeSeriesOutput, TimeSeriesInput, TIME_SERIES_TYPE
from hgraph._types._tsl_type import TimeSeriesListInput, TimeSeriesListOutput

__all__ = ("PythonTimeSeriesListOutput", "PythonTimeSeriesListInput")


class PythonTimeSeriesListOutput(
    PythonTimeSeriesOutput, TimeSeriesListOutput[TIME_SERIES_TYPE, SIZE], Generic[TIME_SERIES_TYPE, SIZE]
):

    def __init__(self, __type__: TIME_SERIES_TYPE, __size__: SIZE, *args, **kwargs):
        Generic.__init__(self)
        TimeSeriesListOutput.__init__(self, __type__, __size__)
        PythonTimeSeriesOutput.__init__(self, *args, **kwargs)

    @property
    def value(self) -> Optional[tuple]:
        return tuple(ts.value if ts.valid else None for ts in self._ts_values)

    @value.setter
    def value(self, v: tuple | frozendict | dict | list):
        if v is None:
            self.invalidate()
        else:
            if isinstance(v, (dict, frozendict)):
                for k, v_ in v.items():
                    if v_ is not None:
                        self[k].value = v_
            elif isinstance(v, (tuple, list)):
                if len(v) != len(self._ts_values):
                    raise ValueError(f"Expected {len(self._ts_values)} elements, got {len(v)}")
                for ts, v_ in zip(self._ts_values, v):
                    if v_ is not None:
                        ts.value = v_

    @property
    def delta_value(self) -> Optional[dict[int, Any]]:
        return {i: ts.delta_value for i, ts in enumerate(self._ts_values) if ts.modified}

    def can_apply_result(self, result: Any) -> bool:
        if result is None:
            return True
        elif isinstance(result, (dict, frozendict)):
            for k, v_ in result.items():
                if v_ is not None:
                    if not self[k].can_apply_result(v_):
                        return False
        elif isinstance(result, (tuple, list)):
            if len(result) != len(self._ts_values):
                raise ValueError(f"Expected {len(self._ts_values)} elements, got {len(result)}")
            for ts, v_ in zip(self._ts_values, result):
                if v_ is not None:
                    if not ts.can_apply_result(v_):
                        return False
        return True

    def apply_result(self, result: Any):
        if result is None:
            return
        self.value = result

    def copy_from_output(self, output: "TimeSeriesOutput"):
        for ts_self, ts_output in zip(self.values(), output.values()):
            ts_self.copy_from_output(ts_output)

    def copy_from_input(self, input: "TimeSeriesInput"):
        for ts_self, ts_input in zip(self.values(), input.values()):
            ts_self.copy_from_input(ts_input)

    def clear(self):
        for v in self.values():
            v.clear()

    def invalidate(self):
        for v in self.values():
            v.invalidate()

    def mark_invalid(self):
        if self.valid:
            for v in self.values():
                v.mark_invalid()
            super().mark_invalid()

    @property
    def all_valid(self) -> bool:
        return all(ts.valid for ts in self.values())


class PythonTimeSeriesListInput(
    PythonBoundTimeSeriesInput, TimeSeriesListInput[TIME_SERIES_TYPE, SIZE], Generic[TIME_SERIES_TYPE, SIZE]
):

    def __init__(
        self,
        __type__: TIME_SERIES_TYPE,
        __size__: SIZE,
        _parent_or_node: Union["Node", "TimeSeriesInput"] = None,
    ):
        Generic.__init__(self)
        TimeSeriesListInput.__init__(self, __type__, __size__)
        PythonBoundTimeSeriesInput.__init__(self, _parent_or_node=_parent_or_node)

    # TODO: With the introduction of REF inputs, we need to revisit the definition of has_peer.

    @property
    def bound(self) -> bool:
        return super().bound or any(ts.bound for ts in self.values())

    def do_bind_output(self, output: TimeSeriesOutput):
        output: PythonTimeSeriesListOutput
        peer = True
        for ts_input, ts_output in zip(
            self.values(), output.values() if output is not None else [None] * len(self.values())
        ):
            peer &= ts_input.bind_output(ts_output)

        super().do_bind_output(output if peer else None)
        return peer

    def do_un_bind_output(self, unbind_refs: bool = False):
        for ts_input in self.values():
            ts_input.un_bind_output(unbind_refs=unbind_refs)
        if self.has_peer:
            super().do_un_bind_output(unbind_refs=unbind_refs)

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
    def value(self):
        if self.has_peer:
            return super().value
        else:
            return tuple(ts.value if ts.valid else None for ts in self.values())

    @property
    def delta_value(self):
        if self.has_peer and not self._sampled:
            return super().delta_value
        else:
            return {k: ts.delta_value for k, ts in self.modified_items()}

    @property
    def modified(self) -> bool:
        if self.has_peer:
            return super().modified or self._sampled
        else:
            return any(ts.modified for ts in self.values())

    @property
    def valid(self) -> bool:
        if self.has_peer:
            return super().valid
        else:
            return any(ts.valid for ts in self.values())

    @property
    def all_valid(self) -> bool:
        return all(ts.valid for ts in self.values())

    @property
    def last_modified_time(self) -> datetime:
        if self.has_peer:
            return super().last_modified_time
        else:
            return max(ts.last_modified_time for ts in self.values())
