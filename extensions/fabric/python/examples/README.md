# Python Fabric examples

These examples progress from a one-frame local publisher to subscription
policies and a typed multi-input derived dataset:

- [`publish_once.py`](publish_once.py) is a complete runnable local example.
- [`subscription_modes.py`](subscription_modes.py) shows separate Live, Replay,
  and Snapshot consumers.
- [`derived_dataset.py`](derived_dataset.py) joins two Fabric inputs and
  publishes a derived dataset with automatic or explicit lineage.

Run the basic publisher from the repository root after installing the hgraph,
persistence, and Fabric wheels:

```sh
python extensions/fabric/python/examples/publish_once.py
```

The local memory service is scoped to one graph execution. It is useful for
testing publication and wiring, but it does not provide data to a later process
or graph run. Reusable component graphs therefore do not register a service;
the outer host wrapper does that once. A production native host supplies
persistent object/Frame stores and Kafka while reusing the same Python
components.

Subscription modes are alternatives for a given data id in one root graph:

- `LIVE` follows new accepted revisions and normally runs in real time.
- `REPLAY` walks durable revisions over the executor's start/end interval.
- `SNAPSHOT` emits one consistent image at a required `as_of` cutoff.

Fabric publishes complete atomic Frames. Typed `Frame[Row]` views are useful
inside an application graph; convert the final value to `TS[Frame]` at the
Fabric publication boundary. The Arrow schema is retained in the stored Frame.

