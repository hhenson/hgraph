from collections import defaultdict
from dataclasses import dataclass, field
from typing import Generic, Iterable, Any, Set, Optional, cast

from hgraph._types._time_series_types import TimeSeriesOutput
from hgraph._impl._types._input import PythonBoundTimeSeriesInput
from hgraph._impl._types._output import PythonTimeSeriesOutput
from hgraph._types._scalar_types import SCALAR
from hgraph._types._tss_type import SetDelta, TimeSeriesSetOutput, TimeSeriesSetInput

__all__ = ("PythonSetDelta", "PythonTimeSeriesSetOutput", "PythonTimeSeriesSetInput", "Removed")


@dataclass(frozen=True, eq=False)
class PythonSetDelta(SetDelta[SCALAR], Generic[SCALAR]):
    added: frozenset[SCALAR]
    removed: frozenset[SCALAR]

    @property
    def added_elements(self) -> Iterable[SCALAR]:
        return self.added or set()

    @property
    def removed_elements(self) -> Iterable[SCALAR]:
        return self.removed or set()

    def __eq__(self, other):
        if type(self) == type(other):
            return (self.added, self.removed) == (other.added, other.removed)
        if isinstance(other, (set, frozenset, list, tuple)):
            # Check the number of added and removed are the same, if not then they are not equal
            if len(self.added) + len(self.removed) != len(other):
                return False
            return all(i in self.added if type(i) is not Removed else i in self.removed for i in other)
        return NotImplemented


@dataclass(frozen=True)
class Removed(Generic[SCALAR]):
    item: SCALAR

    def __hash__(self):
        return hash(self.item)

    def __eq__(self, other):
        return self.item == other.item if type(other) is Removed else other


class Extensions:
    pass


@dataclass
class PythonTimeSeriesSetOutput(PythonTimeSeriesOutput, TimeSeriesSetOutput[SCALAR], Generic[SCALAR]):
    _tp: type | None = None
    _value: set[SCALAR] = field(default_factory=set)
    _added: set[SCALAR] | None = None
    _removed: set[SCALAR] | None = None
    _extensions: dict[SCALAR, Extensions] = field(default_factory=lambda: defaultdict(Extensions))

    def __post_init__(self):
        from hgraph._impl._builder._ts_builder import PythonTimeSeriesBuilderFactory
        from hgraph import TS
        from hgraph import HgTimeSeriesTypeMetaData
        self._bool_ts_builder = PythonTimeSeriesBuilderFactory.instance().make_output_builder(
            HgTimeSeriesTypeMetaData.parse(TS[bool]))

    def invalidate(self):
        self.clear()

    @property
    def value(self) -> Set[SCALAR]:
        return self._value

    @value.setter
    def value(self, v: Set[SCALAR] | SetDelta[SCALAR] | None):
        if v is None:
            self.invalidate()
            return
        if isinstance(v, SetDelta):
            self._added = {e for e in v.added_elements if e not in self._value}
            self._removed = {e for e in v.removed_elements if e in self._value}
            if self._removed.intersection(self._added):
                raise ValueError("Cannot remove and add the same element")
            self._value.update(self._added)
            self._value.difference_update(self._removed)
        else:
            # Assume that the result is a set, and then we are adding all the elements that are not marked Removed
            self._added = {r for r in v if type(r) is not Removed and r not in self._value}
            self._removed = {r.item for r in v if type(r) is Removed and r.item in self._value} \
                if len(self._added) != len(v) else set()
            self._value.update(self._added)
            self._value.difference_update(self._removed)
        self._post_modify()

    def _post_modify(self):
        if self._added or self._removed or not self.valid:
            self.mark_modified()
            if self._extensions:
                for notify in self._added & self._extensions.keys():
                    # Perhaps the default dict is not a good idea?
                    if (ts := vars(self._extensions[notify]).get('ts_contains', None)) is not None:
                        ts.value = True
                for notify in self._removed & self._extensions.keys():
                    if (ts := vars(self._extensions[notify]).get('ts_contains', None)) is not None:
                        ts.value = False

    @property
    def delta_value(self) -> SetDelta[SCALAR]:
        return PythonSetDelta(self._added, self._removed)

    def add(self, element: SCALAR, extensions=None):
        if extensions is not None:
            vars(self._extensions[element]).update(extensions if type(extensions) is dict else vars(extensions))
        if element not in self._value:
            if self._added is not None:
                self._added.add(element)
            else:
                self._added = {element}

            self._value.add(element)

            if (ex := self._extensions.get(element, None)) is not None:
                if (ts := getattr(ex, "ts_contains", None)) is not None:
                    ts.value = True

            self.mark_modified()

    def remove(self, element: SCALAR):
        if element in self._value:
            if self._removed is not None:
                self._removed.add(element)
            else:
                self._removed = {element}

            self._value.remove(element)

            if (ex := self._extensions.get(element, None)) is not None:
                if (ts := getattr(ex, "ts_contains", None)) is not None:
                    ts.value = False

            self.mark_modified()

    def clear(self):
        self._removed = self._value
        self._value = set()
        for element in self._removed:
            if (ex := self._extensions.get(element, None)) is not None:
                if (ts := getattr(ex, "ts_contains", None)) is not None:
                    ts.value = False

        self.mark_modified()

    def apply_result(self, result: Any):
        if result is None:
            return
        self.value = result

    def mark_modified(self):
        super().mark_modified()
        self.owning_graph.evaluation_engine_api.add_after_evaluation_notification(self._reset)

    def _reset(self):
        self._added = None
        self._removed = None

    def ts_contains(self, item: SCALAR):
        if (ts := vars(self._extensions[item]).get('ts_contains', None)) is None:
            from hgraph import TimeSeriesValueOutput
            ts: TimeSeriesValueOutput = self._bool_ts_builder.make_instance(self.owning_node, self)
            ts.value = item in self._value
            self._extensions[item].ts_contains = ts
        return ts

    def copy_from_output(self, output: "TimeSeriesOutput"):
        self._added = frozenset(output.value.difference(self._value))
        self._removed = frozenset()
        if self._added:
            self._value.update(self._added)
            self.mark_modified()

    def copy_from_input(self, input: "TimeSeriesInput"):
        self._added = frozenset(input.value.difference(self._value))
        self._removed = frozenset()
        if self._added:
            self._value.update(self._added)
            self.mark_modified()

    def __contains__(self, item: SCALAR) -> bool:
        return item in self._value

    def values(self) -> Set[SCALAR]:
        return self._value

    def added(self) -> Iterable[SCALAR]:
        return self._added or set()

    def was_added(self, item: SCALAR) -> bool:
        return item in self._added

    def removed(self) -> Iterable[SCALAR]:
        return self._removed or set()

    def was_removed(self, item: SCALAR) -> bool:
        return item in self._removed


@dataclass
class PythonTimeSeriesSetInput(PythonBoundTimeSeriesInput, TimeSeriesSetInput[SCALAR], Generic[SCALAR]):
    # output: TimeSeriesSetOutput[SCALAR] = None # TODO: Not sure how to do this in a dataclass? output is a property
    _prev_output: Optional[TimeSeriesSetOutput[SCALAR]] = None

    def do_bind_output(self, output: TimeSeriesOutput) -> bool:
        if self._output is not None:
            self._prev_output = self.output
            self.owning_graph.evaluation_engine_api.add_after_evaluation_notification(self._reset_prev)

        return super().do_bind_output(output)

    def do_un_bind_output(self):
        self._prev_output = self.output
        if self._prev_output is not None:
            self.owning_graph.evaluation_engine_api.add_after_evaluation_notification(self._reset_prev)
        super().do_un_bind_output()

    def _reset_prev(self):
        self._prev_output = None

    def __contains__(self, item: SCALAR) -> bool:
        return self.output.__contains__(item)

    @property
    def delta_value(self):
        return PythonSetDelta(self.added(), self.removed())

    def values(self) -> Iterable[SCALAR]:
        return self.output.values()

    def added(self) -> Iterable[SCALAR]:
        return self.output.added() if self._prev_output is None \
            else cast(set, self.output.added()) | (cast(set, self.values()) - cast(set, self._prev_output.values()))

    def was_added(self, item: SCALAR) -> bool:
        return self.output.was_added(item) and (self._prev_output is None or item not in self._prev_output.values())

    def removed(self) -> Iterable[SCALAR]:
        return self.output.removed() if self._prev_output is None \
            else (cast(set, self._prev_output.values()) - cast(set, self.values()))

    def was_removed(self, item: SCALAR) -> bool:
        return self.output.was_removed(item) and (
                self._prev_output is None or item in self._prev_output.values() and item not in self.values())
