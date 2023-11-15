from typing import Generic, Any, Iterable, Tuple

from frozendict import frozendict

from hg._impl._types._input import PythonBoundTimeSeriesInput
from hg._impl._types._output import PythonTimeSeriesOutput
from hg._types._time_series_types import K, V
from hg._types._tsd_type import TimeSeriesDictOutput, TimeSeriesDictInput, REMOVE_KEY_IF_EXISTS, REMOVE_KEY


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

    def __init__(self, __key_tp__, __value_tp__, *args, **kwargs):
        Generic.__init__(self)
        TimeSeriesDictOutput.__init__(self, __key_tp__, __value_tp__)
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
        return frozendict({k: v.delta_value for k, v in self.items() if v.modified})

    def apply_result(self, result: Any):
        if result is None:
            return
        # Expect a mapping of some sort or an iterable of k, v pairs
        for k, v in result.items() if isinstance(result, (dict, frozendict)) else result:
            if v in (REMOVE_KEY, REMOVE_KEY_IF_EXISTS):
                if k not in self._removed_items:
                    if v is REMOVE_KEY:
                        raise KeyError(f"TSD[{self.__key_tp__}, {self.__value_tp__}] Key {k} does not exist")
                    else:
                        continue
                self._removed_items[k] = self._ts_values.pop(k)
                for observer in self._key_observers:
                    observer.on_key_removed(k)
                continue
            elif k not in self._ts_values:
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


class PythonTimeSeriesDictInput(PythonBoundTimeSeriesInput, TimeSeriesDictInput[K, V], TSDKeyObserver, Generic[K, V]):

    # At the moment the only supported inptut type is a bound input. When we start to support time-series references
    # this will need to change
    # Also no clever lazy mapping of input wrappers. That can come later.

    def __init__(self, __key_tp__, __value_tp__, *args, **kwargs):
        Generic.__init__(self)
        TimeSeriesDictInput.__init__(self, __key_tp__, __value_tp__)
        PythonBoundTimeSeriesInput.__init__(self, *args, **kwargs)
        from hg._impl._builder._ts_builder import PythonTimeSeriesBuilderFactory
        from hg._builder._ts_builder import TSInputBuilder
        self._ts_builder: TSInputBuilder = PythonTimeSeriesBuilderFactory.instance().make_input_builder(__value_tp__)
        self._removed_items: dict[K, V] = {}

    def bind_output(self, output: "TimeSeriesOutput"):
        super().bind_output(output)
        output: PythonTimeSeriesDictOutput
        output.add_key_observer(self)

    def on_key_added(self, key: K):
        self._ts_values[key] = (v := self._ts_builder.make_instance(owning_input=self))
        v.bind_output(self.output[key])

    def on_key_removed(self, key: K):
        if not self._removed_items:
            self.owning_graph.context.add_after_evaluation_notification(self._clear_key_changes)
        self._removed_items[key] = self._ts_values.pop(key)

    def _clear_key_changes(self):
        self._removed_items = {}

    def added_keys(self) -> Iterable[K]:
        output: PythonTimeSeriesDictOutput = self.output
        return output.added_keys()

    def added_values(self) -> Iterable[V]:
        return (self[k] for k in self.added_keys())

    def added_items(self) -> Iterable[Tuple[K, V]]:
        return ((k, self[k]) for k in self.added_keys())

    def removed_keys(self) -> Iterable[K]:
        return self._removed_items.keys()

    def removed_values(self) -> Iterable[V]:
        return self._removed_items.values()

    def removed_items(self) -> Iterable[Tuple[K, V]]:
        return self._removed_items.items()


