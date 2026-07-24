"""GlobalState/GlobalContext and record/replay configuration.

``_push_runtime_global_state``/``_pop_runtime_global_state`` are called from
C++ (py_nodes: the GlobalState injectable) — keep the names stable."""
import threading

import _hgraph

from ._sentinels import REMOVE, Removed, _SetDelta

_global_state_local = threading.local()
_GLOBAL_MISSING = object()
_GRAPH_LOGGER_KEY = "__hgraph_graph_logger__"
_GRAPH_LOGGER_FORMATTER_KEY = "__hgraph_graph_logger_formatter__"
_RECORDER_API_KEY = "__recorder_api__"
_RECORDER_LABEL_KEY = "__recorder_api__label__"


def utc_now():
    """Return the current UTC time as a naive datetime (hgraph parity:
    naive tzinfo-free datetimes throughout)."""
    from datetime import datetime, timezone

    return datetime.now(timezone.utc).replace(tzinfo=None)


def get_recorded_value(key="out", recordable_id=None):
    """hgraph's in-memory recording accessor.

    With a ``recordable_id`` this returns the timestamped recording
    ``[(engine_time, value), ...]`` under the ``:memory:<id>.<key>`` scheme
    (upstream parity). Without one it first tries upstream's default id
    (``nodes.record``) and then falls back to the plain ``key`` — NB the
    plain-key form is this runtime's cycle-aligned harness recording (bare
    values, no timestamps); record with ``recordable_id=`` for the
    timestamped shape."""
    state = GlobalState.instance()
    if recordable_id is not None:
        return state[f":memory:{recordable_id}.{key}"]
    default_key = f":memory:nodes.record.{key}"
    if default_key in state:
        return state[default_key]
    return state[key]


def set_recorder_api(recorder):
    """Store the process-specific recorder API in the active graph state."""
    GlobalState.instance()[_RECORDER_API_KEY] = recorder


def get_recorder_api():
    """Return the recorder API selected for the active graph state."""
    return GlobalState.instance()[_RECORDER_API_KEY]


def set_recording_label(label):
    GlobalState.instance()[_RECORDER_LABEL_KEY] = label


def get_recording_label():
    return GlobalState.instance()[_RECORDER_LABEL_KEY]


def _friendly_recording_delta(delta):
    if isinstance(delta, dict) and set(delta) in (
            {"removed", "modified"}, {"removed", "modified", "removed_strict"}):
        compact = dict(delta["modified"])
        compact.update((key, REMOVE) for key in delta["removed"])
        compact.update((key, REMOVE) for key in delta.get("removed_strict", ()))
        return compact
    if isinstance(delta, dict) and set(delta) == {"added", "removed"}:
        return _SetDelta(
            tuple(delta["added"]) + tuple(Removed(value) for value in delta["removed"]))
    return delta


class _MemoryRecording(list):
    """Python compatibility view over a typed C++ in-memory recording."""

    def __init__(self, owner, key, entries):
        self._owner = owner
        self._key = key
        super().__init__((when, _friendly_recording_delta(delta))
                         for when, delta in entries)

    def __setitem__(self, index, value):
        if isinstance(index, slice):
            raise TypeError("slice replacement is not supported for memory recordings")
        if index < 0:
            index += len(self)
        if index < 0 or index >= len(self):
            raise IndexError("memory recording assignment index out of range")
        when, delta = value
        self._owner._impl._set_memory_recording_entry(
            self._key, index, when, delta)
        super().__setitem__(index, value)


class GlobalState:
    """Python seed/result owner for the C++ graph GlobalState copy lifecycle."""

    def __init__(self, **values):
        self._impl = _hgraph._GlobalState()
        self._compat_context = None
        for key, value in values.items():
            self[key] = value

    def __len__(self):
        return len(self._impl)

    def __contains__(self, key):
        return key in self._impl

    def __getitem__(self, key):
        value = self._impl[key]
        return _MemoryRecording(self, key, value) if key.startswith(":memory:") else value

    def __setitem__(self, key, value):
        self._impl[key] = value

    def __delitem__(self, key):
        del self._impl[key]

    def get(self, key, default=None):
        if key not in self:
            return default
        return self[key]

    def keys(self):
        return self._impl.keys()

    def __iter__(self):
        return iter(self.keys())

    def __bool__(self):
        return bool(len(self))

    def setdefault(self, key, default=None):
        if key in self:
            return self[key]
        self[key] = default
        return default

    def pop(self, key, default=_GLOBAL_MISSING):
        if key in self:
            value = self[key]
            del self[key]
            return value
        if default is _GLOBAL_MISSING:
            raise KeyError(key)
        return default

    @staticmethod
    def has_instance():
        return getattr(_global_state_local, "state", None) is not None

    @staticmethod
    def instance():
        runtime_state = getattr(_global_state_local, "runtime_state", None)
        if runtime_state is not None:
            return runtime_state
        state = getattr(_global_state_local, "state", None)
        if state is None:
            state = GlobalState()
            _global_state_local.state = state
        return state

    def __enter__(self):
        if self._compat_context is not None:
            raise RuntimeError("GlobalState is already active")
        self._compat_context = GlobalContext(self)
        return self._compat_context.__enter__()

    def __exit__(self, exc_type, exc_value, traceback):
        context, self._compat_context = self._compat_context, None
        return context.__exit__(exc_type, exc_value, traceback)


class GlobalContext:
    """Select one Python GlobalState seed for wiring and result copy-back."""

    def __init__(self, state=None):
        self.state = state if state is not None else GlobalState.instance()
        if not isinstance(self.state, GlobalState):
            raise TypeError("GlobalContext state must be a GlobalState")
        self._previous = None
        self._entered = False

    def __enter__(self):
        if self._entered or getattr(_global_state_local, "context_active", False):
            raise RuntimeError("GlobalContext does not support nested activation")
        self._previous = getattr(_global_state_local, "state", None)
        _global_state_local.state = self.state
        _global_state_local.context_active = True
        self._entered = True
        return self.state

    def __exit__(self, exc_type, exc_value, traceback):
        if self._entered:
            _global_state_local.context_active = False
            if self._previous is None:
                del _global_state_local.state
            else:
                _global_state_local.state = self._previous
            self._previous = None
            self._entered = False
        return False


def _push_runtime_global_state(state):
    if getattr(_global_state_local, "runtime_state", None) is not None:
        raise RuntimeError("a runtime GlobalState is already active on this thread")
    _global_state_local.runtime_state = state


def _pop_runtime_global_state():
    if getattr(_global_state_local, "runtime_state", None) is not None:
        del _global_state_local.runtime_state


def set_record_replay_config(model):
    _hgraph._set_record_replay_config(GlobalState.instance()._impl, model)
    # python-readable mirror (model-gated python overloads read it in their
    # requires= predicates; the C++ config has no python getter)
    GlobalState.instance()["__record_replay_model__"] = model


def set_as_of(value):
    _hgraph._set_as_of(GlobalState.instance()._impl, value)
    # python-readable mirror (the data-frame record/replay model reads it
    # at wiring/replay time; the C++ config has no python getter)
    GlobalState.instance()["__as_of__"] = value


def set_time_zone_provider():
    """Install the configured immutable C++ TZDB provider for this graph seed."""
    _hgraph._set_time_zone_provider(GlobalState.instance()._impl)


def set_table_schema_date_key(key):
    _hgraph._set_table_schema_date_key(GlobalState.instance()._impl, key)


def set_table_schema_as_of_key(key):
    _hgraph._set_table_schema_as_of_key(GlobalState.instance()._impl, key)


def evaluate_const(name, args=(), kwargs=None, output_type=None):
    return _hgraph._evaluate_const(GlobalState.instance()._impl, name, args, kwargs or {}, output_type)

class _RecordReplayModes:
    NONE = _hgraph.MODE_NONE
    RECORD = _hgraph.MODE_RECORD
    REPLAY = _hgraph.MODE_REPLAY
    COMPARE = _hgraph.MODE_COMPARE
    REPLAY_OUTPUT = _hgraph.MODE_REPLAY_OUTPUT
    RESET = _hgraph.MODE_RESET
    RECOVER = _hgraph.MODE_RECOVER


RecordReplayEnum = _RecordReplayModes


class record_replay_scope:
    """Context manager: pushes a record/replay mode scope for the wiring
    within (the C++ RAII scope)."""

    def __init__(self, mode, recordable_id=""):
        self._mode, self._id = mode, recordable_id
        self._scope = None

    def __enter__(self):
        self._scope = _hgraph.record_replay_scope(self._mode, self._id)
        return self

    def __exit__(self, *exc):
        del self._scope
        self._scope = None
        return False

class RecordReplayContext(record_replay_scope):
    """hgraph parity: the upstream name for the mode scope context manager
    (``with RecordReplayContext(mode=RecordReplayEnum.RECORD): ...``)."""

    def __init__(self, mode=None, recordable_id=""):
        super().__init__(mode if mode is not None else _RecordReplayModes.NONE, recordable_id)


def set_record_replay_model(model):
    """hgraph parity alias for set_record_replay_config."""
    set_record_replay_config(model)

def comparison_summary(fq_key):
    """(compared, mismatches) from a Compare run's ``fq.__compare__``."""
    return _hgraph._comparison_summary(GlobalState.instance()._impl, fq_key)
