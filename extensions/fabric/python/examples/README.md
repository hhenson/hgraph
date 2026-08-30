# Python Fabric examples

These examples progress from a one-frame local publisher to historical reads,
run-selected subscriptions, and a typed multi-input derived dataset:

- [`publish_once.py`](publish_once.py) is a complete runnable local example.
- [`load_data.py`](load_data.py) performs a standalone latest or point-in-time
  read against an explicit configuration.
- [`subscription_modes.py`](subscription_modes.py) shows that the same
  subscription graph runs Live in real time and Replay in simulation.
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

Subscription behavior belongs to the graph run, not each data id:

- real-time execution follows new accepted revisions;
- simulation walks durable revisions over the executor's start/end interval;
- a narrow simulation interval is the graph-coordinated equivalent of a
  one-point replay.

For a simple historical read that needs no graph coordination,
`load_data(config, data_id)` returns the latest version. Passing `as_of`
instead returns the newest version at or before that cutoff. Neither form
resolves dependency consistency.

Fabric publishes complete atomic Frames. Typed `Frame[Row]` views are useful
inside an application graph; convert the final value to `TS[Frame]` at the
Fabric publication boundary. The Arrow schema is retained in the stored Frame.
