"""Interactive / notebook graph sessions.

Design record: ``docs/source/developer_guide/notebook.rst``. A session keeps a
persistent ``Wiring`` on the bridge's ambient wiring stack so ordinary public
API calls (``const(...)``, operator sugar, ...) wire into it cell by cell;
``port.eval()`` / ``port.plot()`` evaluate the graph **as it currently
stands** via the C++ snapshot-run (``Wiring.run(snapshot=True)``) and read the
port's recorded values back.

This is a clean design, not a reproduction of upstream ``hgraph.notebook``:
one session object (no module-global context seeding), one record sink per
port (reused across evals), sparse absolute-time recording, and
``hgraph.reflection`` for display dispatch.

Usage::

    from hgraph.notebook import session
    nb = session()          # open (or reset) the ambient session

    c = const(42)           # cells wire with the ordinary public API
    c.eval()                # -> EvalResult [(engine_time, 42)]
    (c + 1).eval()          # graph keeps growing; each eval re-runs it

    nb.reset()              # discard and start over
    nb.close()              # tear down
"""

from __future__ import annotations

import _hgraph

from ._wiring import _core
from ._wiring._core import WiringPort, wire

__all__ = ("EvalResult", "NotebookSession", "current_session", "session")

_RECORD_STATE_PREFIX = ":memory:nodes.record."

_SESSION: NotebookSession | None = None


class EvalResult(list):
    """``[(engine_time, value)]`` for one evaluated port, notebook-friendly."""

    def _repr_html_(self):
        rows = "".join(
            f"<tr><td>{when}</td><td>{value!r}</td></tr>" for when, value in self
        )
        return (
            "<table><thead><tr><th>time</th><th>value</th></tr></thead>"
            f"<tbody>{rows}</tbody></table>"
        )

    @property
    def values(self):
        """Just the values, in tick order."""
        return [value for _, value in self]


class NotebookSession:
    """A persistent ambient wiring session evaluated via snapshot-run."""

    def __init__(self, start_time=None, end_time=None):
        self._start_time = start_time
        self._end_time = end_time
        self._wiring = None
        self._state = None
        # id(port) -> (port, record key); the strong port ref pins the id.
        self._recorded = {}
        self._counter = 0
        self._open()

    # -- lifecycle ---------------------------------------------------------

    def _open(self):
        # A seeded GlobalState selects the SPARSE absolute-time recorder (the
        # IN_MEMORY record/replay model): gap-tolerant and returning true
        # engine times, which interactive inspection and plotting want.
        self._state = _hgraph._GlobalState()
        self._wiring = _hgraph.Wiring(self._state)
        _core._wiring_stack.append(self._wiring)
        self._recorded = {}
        self._counter = 0

    def _teardown(self):
        if self._wiring is not None:
            try:
                _core._wiring_stack.remove(self._wiring)
            except ValueError:
                pass  # already unstacked (e.g. a consuming run elsewhere)
        self._wiring = None
        self._state = None
        self._recorded = {}

    def reset(self):
        """Discard the session's graph and start an empty one."""
        self._teardown()
        self._open()

    def close(self):
        """Tear the session down (the ambient wiring stops accepting nodes)."""
        global _SESSION
        self._teardown()
        if _SESSION is self:
            _SESSION = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, exc_tb):
        self.close()
        return False

    # -- evaluation --------------------------------------------------------

    def run(self):
        """Evaluate the whole graph as it currently stands (simulation)."""
        self._require_open()
        # Each eval re-runs the grown graph from the configured start time,
        # and the wiring seeds from the LIVE session state (the 2026-07-27
        # GlobalState lifecycle) — so the prior run's recordings, copied back
        # into the session state, must not re-seed and accumulate.
        for _, key in self._recorded.values():
            state_key = _RECORD_STATE_PREFIX + key
            if state_key in self._state:
                del self._state[state_key]
        return self._wiring.run(
            start_time=self._start_time, end_time=self._end_time, snapshot=True
        )

    def eval(self, port) -> EvalResult:
        """Record ``port``, evaluate the current graph, return its ticks.

        The record sink is wired once per port and reused by later evals
        (each eval re-runs the whole graph from the configured start time).
        """
        self._require_open()
        if not isinstance(port, WiringPort):
            raise TypeError(f"eval expects a wired port, got {port!r}")
        entry = self._recorded.get(id(port))
        if entry is None:
            key = f"Eval_{self._counter}"
            self._counter += 1
            wire("record", port, key)
            self._recorded[id(port)] = (port, key)
        else:
            key = entry[1]
        self.run()
        # The run copies the runtime state back into the session's seeded
        # state; the sparse recording is [(engine_time, delta)] under the
        # record operator's state key.
        return EvalResult(self._state[_RECORD_STATE_PREFIX + key])

    def plot(self, port, title=None, ylabel=None):
        """Evaluate ``port`` and step-plot it (``TS`` scalar or ``TSB``)."""
        from matplotlib import pyplot as plt

        from .reflection import fields, is_bundle, is_ts

        result = self.eval(port)
        output_type = port.output_type
        times = [when for when, _ in result]
        if is_ts(output_type):
            plt.step(times, [value for _, value in result], where="post")
        elif is_bundle(output_type):
            for name in fields(output_type):
                plt.step(
                    times,
                    [value.get(name) for _, value in result],
                    where="post",
                    label=name,
                )
            plt.legend()
        else:
            raise TypeError(f"cannot plot a port of type {output_type!r}")
        plt.xlabel("time")
        if ylabel:
            plt.ylabel(ylabel)
        if title:
            plt.title(title)
        return result

    def _require_open(self):
        if self._wiring is None:
            raise RuntimeError("this notebook session is closed - call session() to open one")


def session(start_time=None, end_time=None) -> NotebookSession:
    """Open the ambient notebook session (resetting any existing one)."""
    global _SESSION
    if _SESSION is not None:
        _SESSION.close()
    _SESSION = NotebookSession(start_time=start_time, end_time=end_time)
    return _SESSION


def current_session() -> NotebookSession:
    """The active session; raises with guidance when none is open."""
    if _SESSION is None:
        raise RuntimeError(
            "no notebook session is active - run "
            "`from hgraph.notebook import session; session()` first"
        )
    return _SESSION


def _port_eval(self) -> EvalResult:
    """Evaluate the graph as it stands and return this port's ticks."""
    return current_session().eval(self)


def _port_plot(self, title=None, ylabel=None):
    """Evaluate and step-plot this port (requires matplotlib)."""
    return current_session().plot(self, title=title, ylabel=ylabel)


# Port sugar, installed ONCE at module import (a documented extension point on
# the bridge's own WiringPort - not per-session monkey-patching). The methods
# raise a helpful error when no session is active.
WiringPort.eval = _port_eval
WiringPort.plot = _port_plot
