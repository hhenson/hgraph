from dataclasses import dataclass, field
from datetime import datetime
from typing import Generic, Iterable, Any, Set, Optional

from hgraph import MIN_DT
from hgraph._impl._types._feature_extension import FeatureOutputExtension
from hgraph._impl._types._input import PythonBoundTimeSeriesInput
from hgraph._impl._types._output import PythonTimeSeriesOutput
from hgraph._types._scalar_types import SCALAR
from hgraph._types._time_series_types import TimeSeriesOutput
from hgraph._types._ts_type import TS
from hgraph._types._tss_type import SetDelta, TimeSeriesSetOutput, TimeSeriesSetInput, set_delta

__all__ = ("PythonSetDelta", "PythonTimeSeriesSetOutput", "PythonTimeSeriesSetInput", "Removed")


class PythonSetDelta(SetDelta[SCALAR], Generic[SCALAR]):

    def __init__(self, added: frozenset[SCALAR], removed: frozenset[SCALAR]):
        self._added: frozenset[SCALAR] = added
        self._removed: frozenset[SCALAR] = removed

    @property
    def added(self) -> Iterable[SCALAR]:
        return self._added or set()

    @property
    def removed(self) -> Iterable[SCALAR]:
        return self._removed or set()

    def __eq__(self, other):
        if isinstance(other, SetDelta):
            return (self.added, self.removed) == (other.added, other.removed)
        if isinstance(other, (set, frozenset, list, tuple)):
            # Check the number of added and removed are the same, if not then they are not equal
            if len(self.added) + len(self.removed) != len(other):
                return False
            return all(i in self.added if type(i) is not Removed else i.item in self.removed for i in other)
        return NotImplemented

    def __hash__(self):
        return hash((self.added, self.removed))

    def __add__(self, other: "PythonSetDelta[SCALAR]") -> "PythonSetDelta[SCALAR]":
        if not isinstance(other, SetDelta):
            raise TypeError(f"Cannot add {type(self)} to {type(other)}")

        added = (self.added - other.removed) | other.added
        removed = (other.removed - self.added) | (self.removed - other.added)
        return PythonSetDelta(added=added, removed=removed)

    def __str__(self):
        return f"SetDelta(added={self.added}, removed={self.removed})"

    def __repr__(self):
        return f"PythonSetDelta(added={self.added}, removed={self.removed})"


@dataclass(frozen=True)
class Removed(Generic[SCALAR]):
    item: SCALAR

    def __hash__(self):
        return hash(self.item)

    def __eq__(self, other):
        return self.item == other.item if type(other) is Removed else other


@dataclass
class PythonTimeSeriesSetOutput(PythonTimeSeriesOutput, TimeSeriesSetOutput[SCALAR], Generic[SCALAR]):
    _tp: type | None = None
    _value: set[SCALAR] = field(default_factory=set)
    _added: set[SCALAR] | None = None
    _removed: set[SCALAR] | None = None
    _contains_ref_outputs: FeatureOutputExtension = None
    _is_empty_ref_output: TimeSeriesOutput = None

    def __post_init__(self):
        from hgraph._impl._builder._ts_builder import PythonTimeSeriesBuilderFactory
        from hgraph import TS
        from hgraph import HgTimeSeriesTypeMetaData

        factory = PythonTimeSeriesBuilderFactory.instance()
        bool_ts_builder = factory.make_output_builder(HgTimeSeriesTypeMetaData.parse_type(TS[bool]))
        self._contains_ref_outputs = FeatureOutputExtension(
            self, bool_ts_builder, (lambda output, result_output, key: result_output.apply_result(key in output.value))
        )
        # Use owning output as the empty state will only occur if this output is going change anyhow and it
        # Deals with state not being fully ready on construction when creating a TSD key_set.
        self._is_empty_ref_output = bool_ts_builder.make_instance(owning_output=self)

    def invalidate(self):
        self.clear()
        self._last_modified_time = MIN_DT

    @property
    def value(self) -> Set[SCALAR]:
        return self._value

    @value.setter
    def value(self, v: Set[SCALAR] | SetDelta[SCALAR] | None):
        if v is None:
            self.invalidate()
            return
        if isinstance(v, SetDelta):
            self._added = {e for e in v.added if e not in self._value}
            self._removed = {e for e in v.removed if e in self._value}
            if self._removed.intersection(self._added):
                raise ValueError("Cannot remove and add the same element")
            self._value.update(self._added)
            self._value.difference_update(self._removed)
        elif isinstance(v, frozenset):
            # Lets make the value be v, will create an appropriate delta
            old_value = self._value
            self._value = v
            self._added = frozenset(v - old_value)
            self._removed = frozenset(old_value - v)
        else:
            # Assume that the result is a set, and then we are adding all the elements that are not marked Removed
            self._added = {r for r in v if type(r) is not Removed and r not in self._value}
            self._removed = (
                {r.item for r in v if type(r) is Removed and r.item in self._value}
                if len(self._added) != len(v)
                else set()
            )
            if self._removed.intersection(self._added):
                raise ValueError("Cannot remove and add the same element")
            self._value.update(self._added)
            self._value.difference_update(self._removed)
        self._post_modify()

    def _post_modify(self):
        if self._added or self._removed or not self.valid:
            self.mark_modified()
            if self._added and self._is_empty_ref_output.value:
                self._is_empty_ref_output.value = False
            elif self._removed and not self._value:
                self._is_empty_ref_output.value = True
            self._contains_ref_outputs.update_all(self._added)
            self._contains_ref_outputs.update_all(self._removed)

    @property
    def delta_value(self) -> SetDelta[SCALAR]:
        return set_delta(self._added, self._removed, self._tp)

    def add(self, element: SCALAR):
        if element not in self._value:
            if not self._value:
                self._is_empty_ref_output.value = False

            if self._added is not None:
                self._added.add(element)
            else:
                self._added = {element}

            if self._removed is not None:
                self._removed.discard(element)

            self._value.add(element)
            self._contains_ref_outputs.update(element)

            self.mark_modified()

    def remove(self, element: SCALAR):
        if element in self._value:
            was_added = False
            if self._added is not None:
                if element in self._added:
                    self._added.discard(element)
                    was_added = True

            if not was_added:
                if self._removed is not None:
                    self._removed.add(element)
                else:
                    self._removed = {element}

            self._value.remove(element)
            self._contains_ref_outputs.update(element)

            if not self._value:
                self._is_empty_ref_output.value = True

            self.mark_modified()

    def clear(self):
        self._removed = self._value - (self._added or set())
        self._added = None
        self._contains_ref_outputs.update_all(self._value)
        self._value = set()
        self._is_empty_ref_output.value = True
        self.mark_modified()

    def can_apply_result(self, result: Any) -> bool:
        return not self.modified

    def apply_result(self, result: Any):
        if result is None:
            return
        self.value = result

    def mark_modified(self, modified_time: datetime = None):
        super().mark_modified(modified_time)
        if node := self.owning_node:
            node.graph.evaluation_engine_api.add_after_evaluation_notification(self._reset)

    def _reset(self):
        self._added = None
        self._removed = None

    def get_contains_output(self, item: SCALAR, requester: Any) -> TS[bool]:
        return self._contains_ref_outputs.create_or_increment(item, requester)

    def release_contains_output(self, item: SCALAR, requester: Any):
        self._contains_ref_outputs.release(item, requester)

    def is_empty_output(self) -> TS[bool]:
        if not self._is_empty_ref_output.valid:
            # Initialise the output.
            self._is_empty_ref_output.value = bool(not self._value)
        return self._is_empty_ref_output

    def copy_from_output(self, output: "TimeSeriesOutput"):
        self._added = frozenset(output.value.difference(self._value))
        self._removed = frozenset(self._value - output.value)
        if self._added or self._removed:
            self._value = set(output.value)
            self.mark_modified()

    def copy_from_input(self, input: "TimeSeriesInput"):
        self._added = frozenset(input.value.difference(self._value))
        self._removed = frozenset(self._value - input.value)
        if self._added or self._removed:
            self._value = set(input.value)
            self.mark_modified()

    def __contains__(self, item: SCALAR) -> bool:
        return item in self._value

    def __len__(self):
        return len(self._value)

    def values(self) -> Set[SCALAR]:
        return self._value

    def added(self) -> Iterable[SCALAR]:
        return self._added or set()

    def was_added(self, item: SCALAR) -> bool:
        return item in self._added if self._added is not None else False

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

    def do_un_bind_output(self, unbind_refs: bool = False):
        self._prev_output = self.output
        if self._prev_output is not None:
            self.owning_graph.evaluation_engine_api.add_after_evaluation_notification(self._reset_prev)
        super().do_un_bind_output(unbind_refs=unbind_refs)

    def _reset_prev(self):
        self._prev_output = None

    def __contains__(self, item: SCALAR) -> bool:
        return self.output.__contains__(item) if self.output is not None else False

    def __len__(self):
        return self.output.__len__() if self.output is not None else 0

    @property
    def modified(self) -> bool:
        return (self._output is not None and self._output.modified) or self._sampled

    @property
    def delta_value(self):
        return PythonSetDelta(self.added(), self.removed())

    def values(self) -> Iterable[SCALAR]:
        return frozenset(self.output.values()) if self.bound else frozenset()

    def added(self) -> Iterable[SCALAR]:
        if self._prev_output is not None:
            # Get the old values + anything that was removed in this engine cycle (as that is what would have been here)
            # Then remove any items that were only added in this cycle (as they would not have been there)
            # So added must be the new_values less the original old values
            return self.values() - (
                (self._prev_output.values() | self._prev_output.removed()) - self._prev_output.added()
            )
        elif self.output is not None:
            return self.values() if self._sampled else self.output.added()
        else:
            return set()

    def was_added(self, item: SCALAR) -> bool:
        if self._prev_output is not None:
            self.output.was_added(item) and item not in self._prev_output.values()
        elif self._sampled:
            return item in self.output.value()
        else:
            self.output.was_added(item)

    def removed(self) -> Iterable[SCALAR]:
        if self._prev_output is not None:
            return (
                (self._prev_output.values() | self._prev_output.removed()) - self._prev_output.added()
            ) - self.values()
        elif self._sampled:
            return set()
        elif self.output is not None:
            return self.output.removed()
        else:
            return set()

    def was_removed(self, item: SCALAR) -> bool:
        if self._prev_output is not None:
            return item in self._prev_output.values() and item not in self.values()
        elif self._sampled:
            return False
        else:
            return self.output.was_removed(item)
