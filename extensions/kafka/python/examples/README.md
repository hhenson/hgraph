# Python Kafka examples

These examples cover the three normal Kafka graph shapes:

- [`consume_and_commit.py`](consume_and_commit.py) decodes a record and only
  exposes its paired cursor to the commit service after processing succeeds.
- [`publish_with_delivery.py`](publish_with_delivery.py) publishes one record
  with ordered headers and observes its asynchronous broker delivery report.
- [`bounded_replay.py`](bounded_replay.py) runs a deterministic, timestamp-
  ordered recovery over the graph's simulation interval.

They use a Kafka broker at `localhost:9092` by default. Each script accepts
`--bootstrap-servers`; the consumer and replay examples also accept topic and
group arguments. For example:

```sh
python extensions/kafka/python/examples/consume_and_commit.py \
  --bootstrap-servers localhost:9092 --topic orders
```

The outer `app` graph registers the service configuration. Reusable graph
components only use the path-bound subscribe, publish, commit, event, and
delivery-report edges.

The bounded replay deliberately uses `EvaluationMode.SIMULATION`, a record-
timestamp recovery clock, and a finite stop position. Publishing and commits
are real-time operations and are rejected in simulation.
