from dataclasses import dataclass
from itertools import chain
from typing import Generic, Any, Iterable, Tuple, TYPE_CHECKING

from frozendict import frozendict

from hg._impl._types._input import PythonBoundTimeSeriesInput
from hg._impl._types._output import PythonTimeSeriesOutput
from hg._types._time_series_types import K, V
from hg._types._tsd_type import TimeSeriesDictOutput, TimeSeriesDictInput, REMOVE_IF_EXISTS, REMOVE

if TYPE_CHECKING:
    from hg._types._time_series_types import TimeSeriesOutput, TimeSeriesInput

__all__ = ("PythonTimeSeriesDictOutput", "PythonTimeSeriesDictInput", "TSDKeyObserver")


class TSDKeyObserver:
    """
    Used to track additions and removals of parent keys.
    Since the TSD is dynamic, the inputs associated to an output needs to be updated when a key is added or removed.
    in order to correctly manage it's internal state.
    """

    def on_key_added(self, key: K):
        pass

    def on_key_removed(self, key: K):
        pass


class PythonTimeSeriesDictOutput(PythonTimeSeriesOutput, TimeSeriesDictOutput[K, V], Generic[K, V]):

    def __init__(self, __key_set__, __key_tp__, __value_tp__, *args, **kwargs):
        Generic.__init__(self)
        __key_set__: TimeSeriesOutput
        __key_set__._owning_node = kwargs['_owning_node']
        __key_set__._parent_output = self
        TimeSeriesDictOutput.__init__(self, __key_set__, __key_tp__, __value_tp__)
        super().__init__(*args, **kwargs)
        self._key_observers: list[TSDKeyObserver] = []
        from hg._impl._builder._ts_builder import PythonTimeSeriesBuilderFactory
        from hg._builder._ts_builder import TSOutputBuilder
        self._ts_builder: TSOutputBuilder = PythonTimeSeriesBuilderFactory.instance().make_output_builder(__value_tp__)
        self._removed_items: dict[K, V] = {}
        self._added_keys: set[str] = set()

    def add_key_observer(self, observer: TSDKeyObserver):
        self._key_observers.append(observer)

    def remove_key_observer(self, observer: TSDKeyObserver):
        self._key_observers.remove(observer)

    @property
    def value(self) -> frozendict:
        return frozendict({k: v.value for k, v in self.items() if v.valid})

    @property
    def delta_value(self):
        return frozendict(chain(
            ((k, v.delta_value) for k, v in self.items() if v.modified),
            ((k, REMOVE) for k in self.removed_keys())))

    def apply_result(self, result: Any):
        if result is None:
            return
        # Expect a mapping of some sort or an iterable of k, v pairs
        for k, v in result.items() if isinstance(result, (dict, frozendict)) else result:
            if v in (REMOVE, REMOVE_IF_EXISTS):
                if k not in self._ts_values:
                    if v is REMOVE:
                        raise KeyError(f"TSD[{self.__key_tp__}, {self.__value_tp__}] Key {k} does not exist")
                    else:
                        continue
                self._removed_items[k] = self._ts_values.pop(k)
                self.key_set.remove(k)
                for observer in self._key_observers:
                    observer.on_key_removed(k)
                continue
            elif k not in self._ts_values:
                self.key_set.add(k)
                self._ts_values[k] = self._ts_builder.make_instance(owning_output=self)
                for observer in self._key_observers:
                    observer.on_key_added(k)
            self[k].apply_result(v)
        if self._removed_items or self._added_keys:
            self.owning_graph.context.add_after_evaluation_notification(self._clear_key_changes)

    def _clear_key_changes(self):
        self._removed_items = {}
        self._added_keys = set()

    def copy_from_output(self, output: "TimeSeriesOutput"):
        output: PythonTimeSeriesDictOutput
        for k, v in output.items():
            self[k].copy_from_output(v)

    def copy_from_input(self, input: "TimeSeriesInput"):
        input: PythonTimeSeriesDictInput
        for k, v in input.items():
            self[k].copy_from_input(v)

    def added_keys(self) -> Iterable[K]:
        return self._added_keys

    def added_values(self) -> Iterable[V]:
        return (self[k] for k in self._added_keys)

    def added_items(self) -> Iterable[Tuple[K, V]]:
        return ((k, self[k]) for k in self._added_keys)

    def removed_keys(self) -> Iterable[K]:
        return self._removed_items.keys()

    def removed_values(self) -> Iterable[V]:
        return self._removed_items.values()

    def removed_items(self) -> Iterable[Tuple[K, V]]:
        return self._removed_items.items()


@dataclass
class PythonTimeSeriesDictInput(PythonBoundTimeSeriesInput, TimeSeriesDictInput[K, V], TSDKeyObserver, Generic[K, V]):

    _prev_output: TimeSeriesDictOutput = None

    def __init__(self, __key_set__, __key_tp__, __value_tp__, *args, **kwargs):
        Generic.__init__(self)
        __key_set__: TimeSeriesOutput
        __key_set__._owning_node = kwargs['_owning_node']
        __key_set__._parent_input = self
        TimeSeriesDictInput.__init__(self, __key_set__, __key_tp__, __value_tp__)
        PythonBoundTimeSeriesInput.__init__(self, *args, **kwargs)
        from hg._impl._builder._ts_builder import PythonTimeSeriesBuilderFactory
        from hg._builder._ts_builder import TSInputBuilder
        self._ts_builder: TSInputBuilder = PythonTimeSeriesBuilderFactory.instance().make_input_builder(__value_tp__)
        self._removed_items: dict[K, V] = {}

    def do_bind_output(self, output: "TimeSeriesOutput"):
        output: PythonTimeSeriesDictOutput
        key_set: "TimeSeriesSetInput" = self.key_set
        key_set.bind_output(output.key_set)

        if output.__value_tp__ != self.__value_tp__ and (output.__value_tp__.has_references or self.__value_tp__.has_references):
            # TODO: there might be a corner case where the above check is not sufficient, like a bundle on both sides
            #  that contains REFs but there are other items that are of different but compatible non-REF types.
            #  It would be very esoteric and I cannot think of an example so will leave the check as is
            peer = False
        else:
            peer = True

        if self.owning_node.is_started and self.output:
            self.output.remove_key_observer(self)
            self._prev_output = self._output
            self.owning_graph.context.add_after_evaluation_notification(self._reset_prev)

        super().do_bind_output(output)

        if self._ts_values:
            self._removed_items = self._ts_values
            self._ts_values = {}
            self.owning_graph.context.add_after_evaluation_notification(self._clear_key_changes)

        for key in key_set.values():
            self.on_key_added(key)

        output.add_key_observer(self)
        return peer

    def _reset_prev(self):
        self._prev_output = None

    def on_key_added(self, key: K):
        self._ts_values[key] = (v := self._ts_builder.make_instance(owning_input=self))
        v.bind_output(self.output[key])
        if not self.has_peer and self.active:
            v.make_active()

    def on_key_removed(self, key: K):
        if not self._removed_items:
            self.owning_graph.context.add_after_evaluation_notification(self._clear_key_changes)
        value = self._ts_values.pop(key)
        if value.active:
            value.make_passive()
        self._removed_items[key] = value

    def _clear_key_changes(self):
        self._removed_items = {}

    @property
    def value(self):
        return frozendict((k, v.value) for k, v in self.items())

    @property
    def delta_value(self):
        return frozendict(chain(
            ((k, v.delta_value) for k, v in self.items() if v.modified),
            ((k, REMOVE) for k in self.removed_keys())))

    def __contains__(self, item):
        return item in self._ts_values

    def added_keys(self) -> Iterable[K]:
        return self.key_set.added()

    def added_values(self) -> Iterable[V]:
        return (self[k] for k in self.added_keys())

    def added_items(self) -> Iterable[Tuple[K, V]]:
        return ((k, self[k]) for k in self.added_keys())

    def removed_keys(self) -> Iterable[K]:
        return self.key_set.removed()

    def removed_values(self) -> Iterable[V]:
        return (self._removed_items[key] for key in self.removed_keys())

    def removed_items(self) -> Iterable[Tuple[K, V]]:
        return ((key, self._removed_items[key]) for key in self.removed_keys())


