"""
State Tracer for Time-Series Types

This module instruments the Python time-series implementation to trace
all state transitions. Used for documenting state models and verifying
test coverage.
"""
from collections import defaultdict
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum, auto
from typing import Any, Optional
import functools
import threading

__all__ = [
    "StateTracer",
    "StateTransition",
    "TransitionType",
    "get_tracer",
    "enable_tracing",
    "disable_tracing",
    "trace_context",
]


class TransitionType(Enum):
    """Types of state transitions that can occur."""
    # Output transitions
    OUTPUT_CREATED = auto()
    OUTPUT_VALUE_SET = auto()
    OUTPUT_MARK_MODIFIED = auto()
    OUTPUT_MARK_INVALID = auto()
    OUTPUT_CHILD_MODIFIED = auto()
    OUTPUT_SUBSCRIBER_ADDED = auto()
    OUTPUT_SUBSCRIBER_REMOVED = auto()
    OUTPUT_NOTIFY = auto()

    # Input transitions
    INPUT_CREATED = auto()
    INPUT_BIND_OUTPUT = auto()
    INPUT_UNBIND_OUTPUT = auto()
    INPUT_MAKE_ACTIVE = auto()
    INPUT_MAKE_PASSIVE = auto()
    INPUT_NOTIFY = auto()
    INPUT_SAMPLED = auto()

    # Container transitions (TSL, TSB, TSD)
    CONTAINER_CHILD_ADDED = auto()
    CONTAINER_CHILD_REMOVED = auto()
    CONTAINER_CLEARED = auto()

    # Set-specific transitions (TSS)
    SET_ELEMENT_ADDED = auto()
    SET_ELEMENT_REMOVED = auto()
    SET_DELTA_RESET = auto()

    # Dict-specific transitions (TSD)
    DICT_KEY_ADDED = auto()
    DICT_KEY_REMOVED = auto()
    DICT_VALUE_MODIFIED = auto()

    # Window-specific transitions (TSW)
    WINDOW_VALUE_PUSHED = auto()
    WINDOW_VALUE_POPPED = auto()
    WINDOW_MIN_SIZE_MET = auto()

    # Reference transitions (REF)
    REF_VALUE_SET = auto()
    REF_OBSERVER_ADDED = auto()
    REF_OBSERVER_REMOVED = auto()
    REF_BIND_INPUT = auto()


@dataclass
class StateTransition:
    """A single state transition event."""
    timestamp: datetime
    transition_type: TransitionType
    ts_type: str  # TS, TSS, TSL, TSB, TSD, TSW, REF
    class_name: str
    instance_id: int
    method_name: str
    before_state: dict
    after_state: dict
    args: tuple = field(default_factory=tuple)
    kwargs: dict = field(default_factory=dict)
    test_name: Optional[str] = None

    def state_change(self) -> dict:
        """Return dict of changed state properties."""
        changes = {}
        all_keys = set(self.before_state.keys()) | set(self.after_state.keys())
        for key in all_keys:
            before = self.before_state.get(key)
            after = self.after_state.get(key)
            if before != after:
                changes[key] = {"before": before, "after": after}
        return changes


class StateTracer:
    """Traces state transitions in time-series types."""

    _instance: Optional["StateTracer"] = None
    _lock = threading.Lock()

    def __init__(self):
        self.transitions: list[StateTransition] = []
        self.enabled = False
        self.current_test: Optional[str] = None
        self._original_methods: dict = {}
        self._transition_counts: dict[TransitionType, int] = defaultdict(int)
        self._type_transitions: dict[str, list[StateTransition]] = defaultdict(list)

    @classmethod
    def instance(cls) -> "StateTracer":
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = cls()
        return cls._instance

    def enable(self):
        """Enable tracing."""
        if not self.enabled:
            self.enabled = True
            self._install_hooks()

    def disable(self):
        """Disable tracing."""
        if self.enabled:
            self.enabled = False
            self._remove_hooks()

    def clear(self):
        """Clear all recorded transitions."""
        self.transitions.clear()
        self._transition_counts.clear()
        self._type_transitions.clear()

    def set_test(self, test_name: str):
        """Set current test name for transitions."""
        self.current_test = test_name

    def record(self, transition: StateTransition):
        """Record a state transition."""
        if self.enabled:
            transition.test_name = self.current_test
            self.transitions.append(transition)
            self._transition_counts[transition.transition_type] += 1
            self._type_transitions[transition.ts_type].append(transition)

    def get_state_snapshot(self, obj) -> dict:
        """Get a snapshot of an object's state."""
        state = {}

        # Common properties
        if hasattr(obj, 'valid'):
            try:
                state['valid'] = obj.valid
            except:
                state['valid'] = '<error>'

        if hasattr(obj, 'modified'):
            try:
                state['modified'] = obj.modified
            except:
                state['modified'] = '<error>'

        if hasattr(obj, 'all_valid'):
            try:
                state['all_valid'] = obj.all_valid
            except:
                state['all_valid'] = '<error>'

        if hasattr(obj, '_last_modified_time'):
            state['last_modified_time'] = str(obj._last_modified_time)

        # Output-specific
        if hasattr(obj, '_value'):
            state['value'] = repr(obj._value)[:100]

        if hasattr(obj, '_subscribers'):
            state['subscriber_count'] = len(obj._subscribers)

        # Input-specific
        if hasattr(obj, '_output'):
            state['bound'] = obj._output is not None

        if hasattr(obj, '_active'):
            state['active'] = obj._active

        if hasattr(obj, '_sample_time'):
            state['sample_time'] = str(obj._sample_time)

        if hasattr(obj, 'has_peer'):
            try:
                state['has_peer'] = obj.has_peer
            except:
                state['has_peer'] = '<error>'

        # Set-specific
        if hasattr(obj, '_added'):
            state['added'] = repr(obj._added)[:50] if obj._added else 'None'
        if hasattr(obj, '_removed'):
            state['removed'] = repr(obj._removed)[:50] if obj._removed else 'None'

        # Container-specific
        if hasattr(obj, '__len__'):
            try:
                state['length'] = len(obj)
            except:
                pass

        return state

    def _get_ts_type(self, obj) -> str:
        """Determine the time-series type from class name."""
        class_name = type(obj).__name__
        if 'Value' in class_name:
            return 'TS'
        elif 'Set' in class_name:
            return 'TSS'
        elif 'List' in class_name:
            return 'TSL'
        elif 'Bundle' in class_name:
            return 'TSB'
        elif 'Dict' in class_name:
            return 'TSD'
        elif 'Window' in class_name:
            return 'TSW'
        elif 'Reference' in class_name:
            return 'REF'
        elif 'Signal' in class_name:
            return 'SIGNAL'
        return 'UNKNOWN'

    def _install_hooks(self):
        """Install tracing hooks on time-series classes."""
        from hgraph._impl._types._output import PythonTimeSeriesOutput
        from hgraph._impl._types._input import PythonBoundTimeSeriesInput
        from hgraph._impl._types._ts import PythonTimeSeriesValueOutput
        from hgraph._impl._types._tss import PythonTimeSeriesSetOutput

        # Hook mark_modified on base output
        self._hook_method(
            PythonTimeSeriesOutput, 'mark_modified',
            TransitionType.OUTPUT_MARK_MODIFIED
        )

        # Hook mark_invalid on base output
        self._hook_method(
            PythonTimeSeriesOutput, 'mark_invalid',
            TransitionType.OUTPUT_MARK_INVALID
        )

        # Hook subscribe/unsubscribe
        self._hook_method(
            PythonTimeSeriesOutput, 'subscribe',
            TransitionType.OUTPUT_SUBSCRIBER_ADDED
        )
        self._hook_method(
            PythonTimeSeriesOutput, 'unsubscribe',
            TransitionType.OUTPUT_SUBSCRIBER_REMOVED
        )

        # Hook _notify
        self._hook_method(
            PythonTimeSeriesOutput, '_notify',
            TransitionType.OUTPUT_NOTIFY
        )

        # Hook value setter on value output
        self._hook_property_setter(
            PythonTimeSeriesValueOutput, 'value',
            TransitionType.OUTPUT_VALUE_SET
        )

        # Hook input methods
        self._hook_method(
            PythonBoundTimeSeriesInput, 'bind_output',
            TransitionType.INPUT_BIND_OUTPUT
        )
        self._hook_method(
            PythonBoundTimeSeriesInput, 'un_bind_output',
            TransitionType.INPUT_UNBIND_OUTPUT
        )
        self._hook_method(
            PythonBoundTimeSeriesInput, 'make_active',
            TransitionType.INPUT_MAKE_ACTIVE
        )
        self._hook_method(
            PythonBoundTimeSeriesInput, 'make_passive',
            TransitionType.INPUT_MAKE_PASSIVE
        )
        self._hook_method(
            PythonBoundTimeSeriesInput, 'notify',
            TransitionType.INPUT_NOTIFY
        )

    def _remove_hooks(self):
        """Remove all installed hooks."""
        for (cls, method_name), original in self._original_methods.items():
            if method_name.startswith('_prop_'):
                # Property setter
                prop_name = method_name[6:]
                prop = getattr(cls, prop_name)
                setattr(cls, prop_name, property(prop.fget, original, prop.fdel))
            else:
                setattr(cls, method_name, original)
        self._original_methods.clear()

    def _hook_method(self, cls, method_name: str, transition_type: TransitionType):
        """Hook a method to trace calls."""
        original = getattr(cls, method_name)
        self._original_methods[(cls, method_name)] = original
        tracer = self

        @functools.wraps(original)
        def traced(self_obj, *args, **kwargs):
            if not tracer.enabled:
                return original(self_obj, *args, **kwargs)

            before = tracer.get_state_snapshot(self_obj)
            try:
                result = original(self_obj, *args, **kwargs)
            finally:
                after = tracer.get_state_snapshot(self_obj)

            transition = StateTransition(
                timestamp=datetime.now(),
                transition_type=transition_type,
                ts_type=tracer._get_ts_type(self_obj),
                class_name=type(self_obj).__name__,
                instance_id=id(self_obj),
                method_name=method_name,
                before_state=before,
                after_state=after,
                args=args,
                kwargs=kwargs,
            )
            tracer.record(transition)
            return result

        setattr(cls, method_name, traced)

    def _hook_property_setter(self, cls, prop_name: str, transition_type: TransitionType):
        """Hook a property setter to trace value changes."""
        prop = getattr(cls, prop_name)
        original_setter = prop.fset
        self._original_methods[(cls, f'_prop_{prop_name}')] = original_setter
        tracer = self

        @functools.wraps(original_setter)
        def traced_setter(self_obj, value):
            if not tracer.enabled:
                return original_setter(self_obj, value)

            before = tracer.get_state_snapshot(self_obj)
            try:
                result = original_setter(self_obj, value)
            finally:
                after = tracer.get_state_snapshot(self_obj)

            transition = StateTransition(
                timestamp=datetime.now(),
                transition_type=transition_type,
                ts_type=tracer._get_ts_type(self_obj),
                class_name=type(self_obj).__name__,
                instance_id=id(self_obj),
                method_name=f'{prop_name}.setter',
                before_state=before,
                after_state=after,
                args=(value,),
            )
            tracer.record(transition)
            return result

        setattr(cls, prop_name, property(prop.fget, traced_setter, prop.fdel))

    def summary(self) -> dict:
        """Generate a summary of all traced transitions."""
        return {
            'total_transitions': len(self.transitions),
            'by_type': dict(self._transition_counts),
            'by_ts_type': {k: len(v) for k, v in self._type_transitions.items()},
            'unique_tests': len(set(t.test_name for t in self.transitions if t.test_name)),
        }

    def state_flow_for_type(self, ts_type: str) -> list[dict]:
        """Get all state flows for a specific time-series type."""
        flows = []
        for t in self._type_transitions.get(ts_type, []):
            flows.append({
                'transition': t.transition_type.name,
                'method': t.method_name,
                'changes': t.state_change(),
                'test': t.test_name,
            })
        return flows

    def unique_transitions(self) -> dict[str, set[str]]:
        """Get unique transition types observed for each ts_type."""
        result = defaultdict(set)
        for t in self.transitions:
            key = f"{t.method_name}:{','.join(sorted(t.state_change().keys()))}"
            result[t.ts_type].add(key)
        return dict(result)


def get_tracer() -> StateTracer:
    """Get the global tracer instance."""
    return StateTracer.instance()


def enable_tracing():
    """Enable state tracing."""
    get_tracer().enable()


def disable_tracing():
    """Disable state tracing."""
    get_tracer().disable()


@contextmanager
def trace_context(test_name: str = None):
    """Context manager for tracing a test."""
    tracer = get_tracer()
    tracer.enable()
    if test_name:
        tracer.set_test(test_name)
    try:
        yield tracer
    finally:
        tracer.set_test(None)
        tracer.disable()
