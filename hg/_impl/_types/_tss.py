from collections import defaultdict
from dataclasses import dataclass, field
from typing import Generic, Iterable, Any, Set, Optional, cast

from hg import TimeSeriesOutput
from hg._impl._types._input import PythonBoundTimeSeriesInput
from hg._impl._types._output import PythonTimeSeriesOutput
from hg._types._scalar_types import SCALAR
from hg._types._tss_type import SetDelta, TimeSeriesSetOutput, TimeSeriesSetInput

__all__ = ("PythonSetDelta", "PythonTimeSeriesSetOutput", "PythonTimeSeriesSetInput")


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
    _tp: type = None
    _value: set[SCALAR] = field(default_factory=set)
    _added: frozenset[SCALAR] | None = None
    _removed: frozenset[SCALAR] | None = None
    _extensions: dict[SCALAR, Extensions] = field(default_factory=lambda: defaultdict(Extensions))

    def __post_init__(self):
        from hg._impl._builder._ts_builder import PythonTimeSeriesBuilderFactory
        from hg import TS
        from hg import HgTimeSeriesTypeMetaData
        self._bool_ts_builder = PythonTimeSeriesBuilderFactory.instance().make_output_builder(
            HgTimeSeriesTypeMetaData.parse(TS[bool]))

    @property
    def value(self) -> Set[SCALAR]:
        return self._value

    @property
    def delta_value(self) -> SetDelta[SCALAR]:
        return PythonSetDelta(self._added, self._removed)

    def apply_result(self, result: Any):
        if result is None:
            return
        if isinstance(result, SetDelta):
            self._added = frozenset(e for e in result.added_elements if e not in self._value)
            self._removed = frozenset(e for e in result.removed_elements if e in self._value)
            if self._removed.intersection(self._added):
                raise ValueError("Cannot remove and add the same element")
            self._value.update(self._added)
            self._value.difference_update(self._removed)
        else:
            # Assume that the result is a set, and then we are adding all the elements that are not marked Removed
            self._added = frozenset(r for r in result if type(r) is not Removed)
            self._removed = frozenset(r.item for r in result if type(r) is Removed) \
                if len(self._added) != len(result) else frozenset()
            self._value.update(self._added)
            self._value.difference_update(self._removed)
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

    def mark_modified(self):
        super().mark_modified()
        self.owning_graph.context.add_after_evaluation_notification(self._reset)

    def _reset(self):
        self._added = None
        self._removed = None

    def ts_contains(self, item: SCALAR):
        if (ts := vars(self._extensions[item]).get('ts_contains', None)) is None:
            from hg import TimeSeriesValueOutput
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
            self.owning_graph.context.add_after_evaluation_notification(self._reset_prev)

        return super().do_bind_output(output)

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
