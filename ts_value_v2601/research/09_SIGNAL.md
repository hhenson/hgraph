# SIGNAL Behavior

Wrapper API surface:
- `TS_Signal` wrapper exposes only base `TimeSeriesInput` methods.

---

## Python behavior (current)

`PythonTimeSeriesSignal` is a bound input with special semantics:

Properties:
- `value` is always `True`.
- `delta_value` is always `True`.

Collection behavior:
- `__getitem__(index)`:
  - Lazily creates child signal inputs when used as a free-standing bundle/list element.
  - Child signals inherit active/passive state.
- `bound`:
  - True if it has a bound output, or if any child signals exist.
- `valid`, `modified`, `last_modified_time`:
  - If child items exist, these are derived from children.
  - Otherwise use base bound input semantics.

---

## C++ proxy behavior (current)

- `PyTimeSeriesSignalInput` is a thin wrapper over `PyTimeSeriesInput`.
- No special behavior beyond base input view.

---

## Behavioral examples

### Example: signal in a bundle
- `signal = TS_Signal()`
- `signal[0]` creates a child signal.
- `signal.modified` becomes `True` if any child ticks.


---

## Tick-by-tick traces (per method)

### value
| Tick / Action | value | delta_value |
| --- | --- | --- |
| t0..tN: any tick | `True` | `True` |

### __getitem__
| Tick / Action | effect |
| --- | --- |
| t1: access index `0` | creates child signal |
| t2: child exists | inherits active state; included in modified/valid aggregation |

### modified / valid
| Tick / Action | modified | valid |
| --- | --- | --- |
| t1: no children | base input behavior | base input behavior |
| t2: with children | aggregate child state | aggregate child state |
