# Python persistence examples

These examples progress from explicit recording to component-level recovery:

- [`record_and_replay.py`](record_and_replay.py) records a scalar time series,
  inspects its Arrow frame, and replays it.
- [`keyed_recording.py`](keyed_recording.py) records and replays a keyed `TSD`
  stream while showing its partition column.
- [`component_modes.py`](component_modes.py) applies record, replay, compare,
  and recover modes to a reusable component without changing the component.

Run any example after installing `hgraph` and `hgraph-persistence`:

```sh
python extensions/persistence/python/examples/record_and_replay.py
```

One `GlobalState` encloses the related recording and replay runs because the
selected frame store is run configuration shared by those runs. The examples
select `hgraph_persistence.FRAME_BACKEND`, rather than the deprecated
`DataFrame` compatibility name.
