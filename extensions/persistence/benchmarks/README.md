# Completed window checkpoint benchmark

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
image format version 1. They are observations, not performance guarantees:

| Live samples | Count period | Encoded bytes | Capture (µs) | Cold load (µs) |
| ---: | ---: | ---: | ---: | ---: |
| 16 | 16 | 747 | 0.292 | 0.458 |
| 256 | 256 | 3,787 | 1.500 | 4.625 |
| 4,096 | 4,096 | 56,805 | 21.667 | 73.125 |
| 16 | 100,000 | 759 | 0.333 | 0.541 |
| 256 | 100,000 | 3,795 | 1.500 | 4.792 |
| 4,096 | 100,000 | 56,811 | 21.334 | 74.000 |

The period appears once in the schema; it does not determine the number of
serialized samples. The larger capacity changes the image by only 6–12 bytes
in this experiment. Capture, loading, and image size track retained samples.
