# Python persistence examples

These examples progress from explicit recording to component-level recovery:

- [`record_and_replay.py`](record_and_replay.py) records a scalar time series,
  inspects its Arrow frame, and replays it.
- [`keyed_recording.py`](keyed_recording.py) records and replays a keyed `TSD`
  stream while showing its partition column.
- [`component_modes.py`](component_modes.py) applies record, replay, compare,
  and recover modes to a reusable component without changing the component.
- [`completed_days.py`](completed_days.py) commits a complete component image
  after a successful bounded day and restores its hidden state in a new
  process for the following day.

Run any example after installing `hgraph` and `hgraph-persistence`:

```sh
python extensions/persistence/python/examples/record_and_replay.py
```

One `GlobalState` encloses the related recording and replay runs because the
selected frame store is run configuration shared by those runs. The examples
select `hgraph_persistence.FRAME_BACKEND`, rather than the deprecated
`DataFrame` compatibility name.

For completed-day recovery, use an empty directory and run:

```sh
python extensions/persistence/python/examples/completed_days.py /tmp/strategy-checkpoints 1
python extensions/persistence/python/examples/completed_days.py /tmp/strategy-checkpoints 2
```

The first process emits `1, 3`; the second emits `6, 10`. Each day uses an
explicit immutable key and explicitly names its predecessor. Retrying an
already completed day is rejected; a computation or stop failure leaves no
new day image. This API is separate from the older input-seeding `RECOVER`
mode and does not require selecting a record/replay backend.

The component must satisfy the native checkpoint eligibility contract. Keep
external sources and effects outside its boundary, use `RECORDABLE_STATE` for
semantic node state, and change `revision` when the strategy code changes.
No automatic code migration or source-offset transaction is provided.
