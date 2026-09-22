# Checkpoint benchmarks

## Keyed endpoint against a hand-written record

```sh
cmake-build-cpp/extensions/persistence/tests/hgraph_persistence_tests '[checkpoint-benchmark]'
```

Records one `TSD[int, TS[float]]` endpoint of 1,000, 10,000 and 100,000 keys
through the generic path and through a hand-written record of the same keys,
values and modification times as three packed arrays, recovered through the
ordinary mutation API. Each timing is the median of 7 repetitions in
microseconds. The hand-written form is the bar a generic image has to
approach, not a correctness reference: it loses slot identity, free-stack
order, the published plane and child clocks.

- `capture_us` builds the owned in-memory image; `restore_us` validates and
  quietly imports one into fresh storage.
- `encode_store_us` encodes and publishes to a memory store; `read_decode_us`
  reads one back and decodes it, including the checksum.
- `encoded_bytes` is read back from a durable local object.

A Release build on macOS (Apple Clang, arm64), 100,000 keys. They are
observations, not performance guarantees:

| | Image v1 | Image v2 (RFC 0039) | Hand-written |
| --- | ---: | ---: | ---: |
| Encoded bytes | 26,315,709 | 1,931,274 | 2,400,008 |
| Bytes per key | 263 | 19.3 | 24 |
| Capture (ms) | 7.1 | 5.1 | |
| Encode and store (ms) | 306 | 2.6 | 2.2 (record) |
| Read and decode (ms) | 144 | 7.4 | |
| Restore (ms) | 14.6 | 8.1 | 8.6 (recover) |

## Completed window checkpoint benchmark

Build the native persistence tests, then explicitly select the hidden benchmark:

```sh
cmake-build-cpp/extensions/persistence/tests/hgraph_persistence_tests '[checkpoint-window-benchmark]'
```

It prints JSON rows for 16, 256, and 4096 live integer samples, with count-window
capacity equal to the live count and with capacity 100000. Each timing is the
median of 11 repetitions in microseconds. Run a release build for representative
performance measurements; compare results on the same machine and build type.

- `encoded_bytes` is the exact whole-component binary envelope size, including
  the window schema once, one typed value list, and one timestamp per sample.
- `capture_us` captures and destroys an owned window image.
- `cold_load_us` creates fresh endpoint storage, validates and loads the image,
  then destroys the endpoint. It includes the existing ring allocation policy.
- `encode_validate_memory_store_us` encodes, decodes and checks lossless recovery,
  then publishes the immutable object in memory. It excludes filesystem latency.
- `memory_read_decode_us` reads and decodes a previously stored memory object.

Schema registry caches are warm. Local durable storage is used only outside the
timed operations to inspect the encoded size. No historical input events are
replayed during loading. The benchmark has no timing assertions and is excluded
from the ordinary correctness suite.

A Release build on macOS (Apple Clang, arm64) produced these measurements for
image format version 2. They are observations, not performance guarantees.
Version 1 encoded the same windows in 747, 3,787 and 56,805 bytes:

| Live samples | Count period | Encoded bytes | Capture (µs) | Cold load (µs) |
| ---: | ---: | ---: | ---: | ---: |
| 16 | 16 | 342 | 0.250 | 0.334 |
| 256 | 256 | 2,511 | 1.708 | 4.375 |
| 4,096 | 4,096 | 37,074 | 21.834 | 63.667 |
| 16 | 100,000 | 358 | 0.333 | 0.541 |
| 256 | 100,000 | 2,521 | 1.500 | 4.791 |
| 4,096 | 100,000 | 37,082 | 21.917 | 69.125 |

The period appears once in the schema; it does not determine the number of
serialized samples. The larger capacity changes the image by only 6–12 bytes
in this experiment. Capture, loading, and image size track retained samples.
