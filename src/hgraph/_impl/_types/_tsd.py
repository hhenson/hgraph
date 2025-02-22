from dataclasses import dataclass
from datetime import datetime
from itertools import chain
from typing import Generic, Any, Iterable, Tuple, TYPE_CHECKING, cast

from frozendict import frozendict

from hgraph._impl._types._feature_extension import FeatureOutputExtension
from hgraph._impl._types._input import PythonBoundTimeSeriesInput
from hgraph._impl._types._output import PythonTimeSeriesOutput
from hgraph._runtime._constants import MIN_DT
from hgraph._types._ref_type import TimeSeriesReferenceOutput, TimeSeriesReference
from hgraph._types._scalar_types import SCALAR
from hgraph._types._time_series_types import K, V
from hgraph._types._tsd_type import TimeSeriesDictOutput, TimeSeriesDictInput, REMOVE_IF_EXISTS, REMOVE
from hgraph._types._tss_type import TimeSeriesSetInput
from hgraph._types._tss_type import TimeSeriesSetOutput

if TYPE_CHECKING:
    from hgraph._types._time_series_types import TimeSeriesOutput, TimeSeriesInput

__all__ = ("PythonTimeSeriesDictOutput", "PythonTimeSeriesDictInput", "TSDKeyObserver")


class TSDKeyObserver:
    """
    Used to track additions and removals of parent keys.
    Since the TSD is dynamic, the inputs associated to an output needs to be updated when a key is added or removed.
    in order to correctly manage it's internal state.
    """

    def on_key_added(self, key: K):
        """Called when a key is added"""

    def on_key_removed(self, key: K):
        """Called when a key is removed"""


class PythonTimeSeriesDictOutput(PythonTimeSeriesOutput, TimeSeriesDictOutput[K, V], Generic[K, V]):

    def __init__(
        self,
        __key_set__,
        __key_tp__,
        __value_tp__,
        __value_output_builder__,
        __value_reference_builder__,
        *args,
        **kwargs,
    ):
        Generic.__init__(self)
        __key_set__: TimeSeriesSetOutput
        __key_set__._owning_node = kwargs["_owning_node"]
        __key_set__._parent_output = self
        TimeSeriesDictOutput.__init__(self, __key_set__, __key_tp__, __value_tp__)
        super().__init__(*args, **kwargs)
        self._key_observers: list[TSDKeyObserver] = []
        from hgraph._builder._ts_builder import TSOutputBuilder

        self._ts_builder: TSOutputBuilder = __value_output_builder__
        self._ts_ref_builder: TSOutputBuilder = __value_reference_builder__
        self._removed_items: dict[K, V] = {}
        self._added_keys: set[str] = set()
        self._ref_ts_feature: FeatureOutputExtension = FeatureOutputExtension(
            self, self._ts_ref_builder, lambda output, key: TimeSeriesReference.make(output.get(key))
        )
        self._ts_values_to_keys: dict[int, K] = {}
        self._modified_items: list[Tuple[K, V]] = []

    def add_key_observer(self, observer: TSDKeyObserver):
        self._key_observers.append(observer)

    def remove_key_observer(self, observer: TSDKeyObserver):
        self._key_observers.remove(observer)

    @property
    def value(self) -> frozendict:
        return frozendict({k: v.value for k, v in self.items() if v.valid})

    @property
    def delta_value(self):
        return frozendict(
            chain(
                ((k, v.delta_value) for k, v in self.items() if v.modified and v.valid), ((k, REMOVE) for k in self.removed_keys())
            )
        )

    @value.setter
    def value(self, v: frozendict | dict | Iterable[tuple[K, SCALAR]] | None):
        if v is None:
            self.invalidate()
            return
        if not self.valid and not v:
            self.key_set.mark_modified()  # Even if we tick an empty set, we still need to mark this as modified
        # Expect a mapping of some sort or an iterable of k, v pairs
        for k, v_ in v.items() if isinstance(v, (dict, frozendict)) else v:
            if v_ is None:
                continue
            if v_ is REMOVE or v_ is REMOVE_IF_EXISTS:  # Supporting numpy arrays has its costs (==)
                if v_ is REMOVE_IF_EXISTS and k not in self._ts_values:  # is check should be faster than contains check
                    continue
                del self[k]
            else:
                self.get_or_create(k).value = v_
        if self._removed_items or self._added_keys:
            self.owning_graph.evaluation_engine_api.add_after_evaluation_notification(self._clear_key_changes)

    def __delitem__(self, k):
        if k not in self._ts_values:
            raise KeyError(f"TSD[{self.__key_tp__}, {self.__value_tp__}] Key {k} does not exist")

        was_added = self.key_set.was_added(k)
        cast(TimeSeriesSetOutput, self.key_set).remove(k)

        for observer in self._key_observers:
            observer.on_key_removed(k)

        item = self._ts_values.pop(k)
        item.clear()
        if not was_added:
            self._removed_items[k] = item
        self._ts_values_to_keys.pop(id(item))
        self._ref_ts_feature.update(k)
        try:
            self._modified_items.remove((k, item))
        except:
            pass

    def mark_child_modified(self, child: "TimeSeriesOutput", modified_time: datetime):
        if self._last_modified_time < modified_time:
            # _last_modified_time is set in mark_modified later
            self._modified_items = []

        if child is not self._key_set:
            self._modified_items.append((self._ts_values_to_keys[id(child)], child))

        super().mark_child_modified(child, modified_time)

    def get_ref(self, key: K, reference: Any) -> TimeSeriesReferenceOutput:
        return self._ref_ts_feature.create_or_increment(key, reference)

    def release_ref(self, key: K, requester: Any) -> None:
        return self._ref_ts_feature.release(key, requester)

    def pop(self, key: K) -> V:
        v = None
        if key in self._ts_values:
            v = self._ts_values[key]
            del self[key]
        return v

    def key_from_value(self, value: V) -> K:
        return self._ts_values_to_keys.get(id(value))

    def clear(self):
        for observer in self._key_observers:
            for k in self._removed_items:
                observer.on_key_removed(k)

        self.key_set.clear()
        for v in self._ts_values.values():
            v.clear()
        self._removed_items = self._ts_values
        self._ts_values_to_keys.clear()
        self._ref_ts_feature.update_all(self._removed_items.keys())
        self._modified_items.clear()

    def invalidate(self):
        for v in self.values():
            v.invalidate()
        self.mark_invalid()

    def can_apply_result(self, result: Any) -> bool:
        if result is None:
            return True
        if not result:
            return True
        # Expect a mapping of some sort or an iterable of k, v pairs
        for k, v_ in result.items() if isinstance(result, (dict, frozendict)) else result:
            if v_ is None:
                continue
            if v_ is REMOVE or v_ is REMOVE_IF_EXISTS:  # Supporting numpy arrays has its costs (==)
                if v_ is REMOVE_IF_EXISTS and k not in self._ts_values:  # is check should be faster than contains check
                    continue
                if self[k].modified:
                    return False
            else:
                if (
                    k in self._removed_items
                ):  # Unlike TSS, TSD should not allow re-adding a removed key in the same tick
                    return False
                if (v := self.get(k)) and not v.can_apply_result(v_):
                    return False
        return True

    def apply_result(self, result: Any):
        if result is None:
            return
        try:
            self.value = result
        except TypeError:
            raise TypeError(f"Cannot apply result {result} of type {result.__class__} to {self}")

    def _create(self, key: K):
        cast(TimeSeriesSetOutput, self.key_set).add(key)
        item = self._ts_builder.make_instance(owning_output=self)
        self._ts_values[key] = item
        self._ts_values_to_keys[id(item)] = key
        self._ref_ts_feature.update(key)
        for observer in self._key_observers:
            observer.on_key_added(key)

    def _clear_key_changes(self):
        self._removed_items = {}
        self._added_keys = set()

    def copy_from_output(self, output: "TimeSeriesOutput"):
        output: PythonTimeSeriesDictOutput
        for k in self.key_set.value - output.key_set.value:
            del self[k]
        for k, v in output.items():
            self.get_or_create(k).copy_from_output(v)

    def copy_from_input(self, input: "TimeSeriesInput"):
        input: PythonTimeSeriesDictInput
        for k in self.key_set.value - input.key_set.value:
            del self[k]
        for k, v in input.items():
            self.get_or_create(k).copy_from_input(v)

    def added_keys(self) -> Iterable[K]:
        return self._added_keys

    def added_values(self) -> Iterable[V]:
        return (self[k] for k in self._added_keys)

    def added_items(self) -> Iterable[Tuple[K, V]]:
        return ((k, self[k]) for k in self._added_keys)

    def modified_keys(self) -> Iterable[K]:
        return (k for k, v in self._modified_items) if self.modified else ()

    def modified_values(self) -> Iterable[V]:
        return (v for k, v in self._modified_items) if self.modified else ()

    def modified_items(self) -> Iterable[Tuple[K, V]]:
        return self._modified_items if self.modified else ()

    def removed_keys(self) -> Iterable[K]:
        return self._removed_items.keys()

    def removed_values(self) -> Iterable[V]:
        return self._removed_items.values()

    def removed_items(self) -> Iterable[Tuple[K, V]]:
        return self._removed_items.items()


@dataclass
class PythonTimeSeriesDictInput(PythonBoundTimeSeriesInput, TimeSeriesDictInput[K, V], TSDKeyObserver, Generic[K, V]):
    _prev_output: TimeSeriesDictOutput | None = None

    def __init__(self, __key_set__, __key_tp__, __value_tp__, *args, **kwargs):
        Generic.__init__(self)
        __key_set__: TimeSeriesOutput
        __key_set__._owning_node = kwargs["_owning_node"]
        __key_set__._parent_input = self
        TimeSeriesDictInput.__init__(self, __key_set__, __key_tp__, __value_tp__)
        PythonBoundTimeSeriesInput.__init__(self, *args, **kwargs)
        from hgraph._impl._builder._ts_builder import PythonTimeSeriesBuilderFactory
        from hgraph._builder._ts_builder import TSInputBuilder

        self._ts_builder: TSInputBuilder = PythonTimeSeriesBuilderFactory.instance().make_input_builder(__value_tp__)
        self._removed_items: dict[K, V] = {}
        self._has_peer: bool = False
        self._ts_values_to_keys: dict[int, K] = {}
        self._last_notified_time = MIN_DT
        self._modified_items = []

    @property
    def has_peer(self) -> bool:
        return self._has_peer

    def do_bind_output(self, output: "TimeSeriesOutput"):
        output: PythonTimeSeriesDictOutput
        key_set: "TimeSeriesSetInput" = self.key_set

        if output.__value_tp__ != self.__value_tp__ and (
            output.__value_tp__.has_references or self.__value_tp__.has_references
        ):
            # TODO: there might be a corner case where the above check is not sufficient, like a bundle on both sides
            #  that contains REFs but there are other items that are of different but compatible non-REF types.
            #  It would be very esoteric and I cannot think of an example so will leave the check as is
            peer = False
            key_set.set_subscribe_method(subscribe_input=True)
        else:
            peer = True
            key_set.set_subscribe_method(subscribe_input=self._subscribe_input)

        self._has_peer = peer

        key_set.bind_output(output.key_set)

        if self.owning_node.is_started and self.output:
            self.output.remove_key_observer(self)
            self._prev_output = self._output
            self.owning_graph.evaluation_engine_api.add_after_evaluation_notification(self._reset_prev)

        super().do_bind_output(output)

        if self._ts_values:
            self.owning_graph.evaluation_engine_api.add_after_evaluation_notification(self._clear_key_changes)

        for key in key_set.values():
            self.on_key_added(key)

        for key in key_set.removed():
            self.on_key_removed(key)

        output.add_key_observer(self)
        return peer

    def do_un_bind_output(self):
        key_set: "TimeSeriesSetInput" = self.key_set
        key_set.un_bind_output()
        if self._ts_values:
            self._removed_items = {k: (v, v.valid) for k, v in self._ts_values.items()}
            self._ts_values = {}
            self.owning_graph.evaluation_engine_api.add_after_evaluation_notification(self._clear_key_changes)

            to_keep = {}
            for k, (v, was_valid) in self._removed_items.items():
                if v.parent_input is not self:
                    # Check for transplanted items, these do not get removed, but can be un-bound
                    v.un_bind_output()
                    self._ts_values[k] = v
                else:
                    to_keep[k] = (v, was_valid)
            self._removed_items = to_keep

        self.output.remove_key_observer(self)
        super().do_un_bind_output()

    def make_active(self):
        if self.has_peer:
            super().make_active()
        else:
            self._active = True
            self.key_set.make_active()
            for v in self._ts_values.values():
                v.make_active()

    def make_passive(self):
        if self.has_peer:
            super().make_passive()
        else:
            self._active = False
            self.key_set.make_passive()
            for v in self._ts_values.values():
                v.make_passive()

    def _create(self, key: K):
        item = self._ts_builder.make_instance(owning_input=self)
        item.set_subscribe_method(subscribe_input=not self.has_peer)
        self._ts_values[key] = item
        self._ts_values_to_keys[id(item)] = key

    def notify_parent(self, child: "TimeSeriesInput", modified_time: datetime):
        if self._last_notified_time < modified_time:
            self._last_notified_time = modified_time
            self._modified_items = []

        if child is not self._key_set:
            self._modified_items.append((self._ts_values_to_keys[id(child)], child))

        super().notify_parent(self, modified_time)

    def _reset_prev(self):
        self._prev_output = None

    def on_key_added(self, key: K):
        v = self.get_or_create(key)
        if not self.has_peer and self.active:
            v.make_active()
        v.bind_output(self.output[key])

    def on_key_removed(self, key: K):
        if not self._removed_items:
            self.owning_graph.evaluation_engine_api.add_after_evaluation_notification(self._clear_key_changes)
        value: TimeSeriesInput = self._ts_values.pop(key, None)
        if value is None:
            return
        was_valid = value.valid
        if value.parent_input is self:
            if value.active:
                value.make_passive()
            self._removed_items[key] = (value, was_valid)
            try:
                self._modified_items.remove((key, value))
            except:
                pass
            if not self.has_peer:
                value.un_bind_output()
        else:
            self._ts_values[key] = value
            value.un_bind_output()

    def _clear_key_changes(self):
        self._removed_items = {}

    @property
    def modified(self) -> bool:
        if self.has_peer:
            return super().modified
        elif self.active:
            et = self.owning_graph.evaluation_clock.evaluation_time
            return self._last_notified_time == et or self.key_set.modified or self._sample_time == et
        else:
            return self.key_set.modified or any(v.modified for v in self._ts_values.values())

    @property
    def last_modified_time(self) -> datetime:
        if self.has_peer:
            return super().last_modified_time
        elif self.active:
            return max(self._last_notified_time, self.key_set.last_modified_time, self._sample_time)
        else:
            return max(self.key_set.last_modified_time,
                       max((v.last_modified_time for v in self._ts_values.values()), default=MIN_DT))

    @property
    def value(self):
        return frozendict((k, v.value) for k, v in self.items())

    @property
    def delta_value(self):
        return frozendict(
            chain(
                ((k, v.delta_value) for k, v in self.modified_items() if v.valid),
                ((k, REMOVE if self._removed_items.get(k, (None, False))[1] else REMOVE_IF_EXISTS) for k, v in self.removed_items())
            )
        )

    def key_from_value(self, value: V) -> K:
        return self._ts_values_to_keys[id(value)]

    def added_keys(self) -> Iterable[K]:
        return self.key_set.added()

    def added_values(self) -> Iterable[V]:
        return (self[k] for k in self.added_keys())

    def added_items(self) -> Iterable[Tuple[K, V]]:
        return ((k, self[k]) for k in self.added_keys())

    def modified_keys(self) -> Iterable[K]:
        if self._sampled:
            return (k for k, v in self.items() if v.valid)
        elif self.has_peer:
            return self._output.modified_keys()
        elif self.active:
            if self._last_notified_time == self.owning_graph.evaluation_clock.evaluation_time:
                return (k for k, v in self._modified_items)
            else:
                return ()
        else:
            return (k for k, v in self.items() if v.modified)

    def modified_values(self) -> Iterable[V]:
        if self._sampled:
            return (v for k, v in self.items() if v.valid)
        elif self.has_peer:
            return self._output.modified_values()
        elif self.active:
            if self._last_notified_time == self.owning_graph.evaluation_clock.evaluation_time:
                return (v for k, v in self._modified_items)
            else:
                return ()
        else:
            return (v for k, v in self.items() if v.modified)

    def modified_items(self) -> Iterable[Tuple[K, V]]:
        if self._sampled:
            return (i for i in self.items() if i[1].valid)
        elif self.has_peer:
            return ((k, self._ts_values[k]) for k in self._output.modified_keys() if k in self._ts_values)
        elif self.active:
            if self._last_notified_time == self.owning_graph.evaluation_clock.evaluation_time:
                return self._modified_items
            else:
                return ()
        else:
            return ((k, v) for k, v in self.items() if v.modified)

    def removed_keys(self) -> Iterable[K]:
        return self.key_set.removed()

    def removed_values(self) -> Iterable[V]:
        for key in self.removed_keys():
            if (v := self._removed_items.get(key)) is None:
                v = self._ts_values.get(key), True
            yield v[0]

    def removed_items(self) -> Iterable[Tuple[K, V]]:
        for key in self.removed_keys():
            if (v := self._removed_items.get(key)) is None:
                v = self._ts_values.get(key), True
            yield (key, v[0])
