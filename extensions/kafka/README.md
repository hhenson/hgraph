# hgraph-kafka

C++-first Kafka services for hgraph, implementing the contract in hgraph RFC
0015. The extension uses librdkafka's C API and exposes the same service model
to native C++ and Python graphs.

One path-bound, multi-interface `service_impl` owns the Kafka clients for a
configuration. Subscriptions, publish requests, explicit commits, and events
all bind to that service instance. Graph output reaches Kafka through the
service's sink inputs; records, delivery reports, and events re-enter the root
graph through bounded push sources. Kafka clients and worker threads are
created on graph start and stopped with the graph.

The public record and configuration shapes are hgraph compound scalars. Kafka
headers preserve order, duplicates, null values, and empty byte strings.

## Native C++

The installed package exports `hgraph::kafka`:

```cpp
#include <hgraph/kafka/service.h>
#include <hgraph/kafka/value_builders.h>
#include <hgraph/lib/std/operators/conversion.h>
#include <hgraph/lib/std/operators/registration.h>

using namespace hgraph;
using namespace hgraph::kafka;

struct KafkaGraph {
    static constexpr auto name = "kafka_graph";

    static void compose(Wiring &w) {
        const auto path = service::path("primary");
        register_service(
            w, path,
            service_config().bootstrap_servers({Str{"localhost:9092"}}).build());

        auto key = wire<stdlib::const_, TS<KafkaSubscriptionKey>>(
            w, subscription_key()
                   .topics({Str{"orders"}})
                   .group_id(Str{"orders-worker"})
                   .build());
        auto subscription = subscribe(w, path, key);

        auto record = wire<stdlib::const_, TS<KafkaProduceRecord>>(
            w, make_produce_record(Bytes{"ready"}));
        auto delivery = publish(
            w, path, publish_request(w, Str{"status"}, record));

        auto cursor = wire<stdlib::getattr_, TS<KafkaCursor>>(
            w, subscription, Str{"cursor"});
        commit(w, path, cursor);
        auto event = events(w, path);
    }
};
```

`KafkaSubscriptionOutput` provides the record and its matching next-offset
cursor on the same graph tick, plus subscription state. A cursor is accepted
only while its subscription identity, assignment generation, and partition
remain live. Commits are monotonic per assigned partition.

## Python

The Python authoring surface lowers to the same native service:

```python
import hgraph as hg
import hgraph_kafka as kafka

@hg.graph
def app():
    kafka.register_kafka_service(
        kafka.KafkaServiceConfig.from_bootstrap_servers(
            ["localhost:9092"], client_id="orders-worker"
        ),
        path="primary",
    )
    key = kafka.KafkaSubscriptionKey(
        topics=("orders",),
        group_id="orders-worker",
        start_position=kafka.KafkaStartPosition.committed(),
    )
    subscription = kafka.kafka_subscribe(
        hg.const(key, tp=hg.TS[kafka.KafkaSubscriptionKey]),
        path="primary",
    )
    kafka.kafka_commit(subscription["cursor"], path="primary")
```

The core `hgraph` wheel owns a guarded compatibility shim at the released
`hgraph.adaptors.kafka` import path. Existing `message_publisher`,
`message_subscriber`, `KafkaMessage`, and `register_kafka_adaptor` imports
continue to work when `hgraph-kafka` is installed. The extension wheel installs
only `hgraph_kafka`; it never contributes files to the core `hgraph` package.

## Recovery and simulation

Subscriptions support explicit topic, pattern, or partition selection; group
or independent assignment; earliest, latest, committed, timestamp, explicit,
and graph-start positions; snapshot, timestamp, and explicit stop boundaries;
key filters; deterministic timestamp/topic/partition/offset replay; and
explicit or graph-delivery commits.

Simulation is intentionally limited to bounded, record-time recovery. The
consumer preloads the finite replay and schedules records at deterministic
graph times. Publish, commit, unbounded asynchronous input, and
`OnGraphDelivery` commit mode are rejected in simulation rather than silently
changing their semantics.

## Build and test

This is a first-party extension in the hgraph monorepo. It remains a separate
CMake package and Python distribution: the top-level core package does not
link librdkafka or install these modules.

For an in-tree native development build from the repository root:

```sh
cmake -S . -B build-kafka \
  -DHGRAPH_BUILD_KAFKA_EXTENSION=ON \
  -DBUILD_TESTING=ON
cmake --build build-kafka --parallel
ctest --test-dir build-kafka --output-on-failure
```

The extension can still be configured independently against an installed
hgraph SDK:

```sh
cmake -S . -B build -DCMAKE_PREFIX_PATH=/path/to/hgraph/install
cmake --build build --parallel
ctest --test-dir build --output-on-failure
```

Build its separately deployable ABI3 wheel from the repository root after
making the matching hgraph SDK discoverable through `CMAKE_PREFIX_PATH`:

```sh
CMAKE_PREFIX_PATH=/path/to/hgraph/sdk \
  uv build --wheel --package hgraph-kafka --python 3.12
```

The deterministic suite uses librdkafka's mock cluster and the extension fake
transport. To include a real broker round trip, provide a clean topic:

```sh
HGRAPH_KAFKA_INTEGRATION_BOOTSTRAP=localhost:9092 \
HGRAPH_KAFKA_INTEGRATION_TOPIC=hgraph-kafka-integration \
ctest --test-dir build --output-on-failure
```

Wheel builds require the SDK installed by a stable-ABI hgraph wheel. The
extension rejects an SDK that links `Python::Python`, because that would pin
the nominal ABI3 module to the build interpreter.
