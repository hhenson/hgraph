# Task 03: Python Bindings for TSTypeRegistry

## Objective

Expose TSMeta and TSTypeRegistry to Python via nanobind.

## File to Create

`cpp/src/cpp/api/python/py_ts_type_registry.cpp`

## Implementation

### TSKind Enum Binding

```cpp
static void register_ts_kind(nb::module_& m) {
    nb::enum_<TSKind>(m, "TSKind", "Categories of time-series types")
        .value("TSValue", TSKind::TSValue, "TS[T] - scalar time-series")
        .value("TSS", TSKind::TSS, "TSS[T] - time-series set")
        .value("TSD", TSKind::TSD, "TSD[K, V] - time-series dict")
        .value("TSL", TSKind::TSL, "TSL[TS, Size] - time-series list")
        .value("TSW", TSKind::TSW, "TSW[T, size, min_size] - time-series window")
        .value("TSB", TSKind::TSB, "TSB[Schema] - time-series bundle")
        .value("REF", TSKind::REF, "REF[TS] - reference to time-series")
        .value("SIGNAL", TSKind::SIGNAL, "SIGNAL - presence/absence marker");
}
```

### TSBFieldInfo Binding

```cpp
static void register_tsb_field_info(nb::module_& m) {
    nb::class_<TSBFieldInfo>(m, "TSBFieldInfo",
        "Metadata for a single field in a TSB schema")
        .def_prop_ro("name", [](const TSBFieldInfo& self) {
            return self.name ? std::string(self.name) : std::string();
        }, "Get the field name")
        .def_ro("index", &TSBFieldInfo::index, "Get the field index (0-based)")
        .def_prop_ro("ts_type", [](const TSBFieldInfo& self) {
            return self.ts_type;
        }, nb::rv_policy::reference, "Get the field's time-series schema");
}
```

### TSMeta Binding

```cpp
static void register_ts_meta(nb::module_& m) {
    nb::class_<TSMeta>(m, "TSMeta", "Time-series type metadata")
        .def_ro("kind", &TSMeta::kind, "Time-series category")

        // Value/key types
        .def_prop_ro("value_type", [](const TSMeta& self) {
            return self.value_type;
        }, nb::rv_policy::reference, "Value type (for TS, TSS, TSW)")
        .def_prop_ro("key_type", [](const TSMeta& self) {
            return self.key_type;
        }, nb::rv_policy::reference, "Key type (for TSD)")
        .def_prop_ro("element_ts", [](const TSMeta& self) {
            return self.element_ts;
        }, nb::rv_policy::reference, "Element TS schema (for TSD value, TSL, REF)")

        // Size
        .def_ro("fixed_size", &TSMeta::fixed_size, "Fixed size (for TSL, 0=dynamic)")

        // Window properties
        .def_ro("is_duration_based", &TSMeta::is_duration_based,
            "True if duration-based window (for TSW)")
        .def_prop_ro("period", [](const TSMeta& self) -> size_t {
            return self.is_duration_based ? 0 : self.window.tick.period;
        }, "Tick period (for tick-based TSW)")
        .def_prop_ro("min_period", [](const TSMeta& self) -> size_t {
            return self.is_duration_based ? 0 : self.window.tick.min_period;
        }, "Minimum tick period (for tick-based TSW)")
        .def_prop_ro("time_range", [](const TSMeta& self) {
            return self.is_duration_based ? self.window.duration.time_range
                                          : engine_time_delta_t{0};
        }, "Time range (for duration-based TSW)")
        .def_prop_ro("min_time_range", [](const TSMeta& self) {
            return self.is_duration_based ? self.window.duration.min_time_range
                                          : engine_time_delta_t{0};
        }, "Minimum time range (for duration-based TSW)")

        // Bundle properties
        .def_ro("field_count", &TSMeta::field_count, "Number of fields (for TSB)")
        .def_prop_ro("bundle_name", [](const TSMeta& self) {
            return self.bundle_name ? std::string(self.bundle_name) : std::string();
        }, "Bundle schema name (for TSB)")
        .def_prop_ro("fields", [](const TSMeta& self) {
            std::vector<const TSBFieldInfo*> result;
            if (self.fields && self.field_count > 0) {
                result.reserve(self.field_count);
                for (size_t i = 0; i < self.field_count; ++i) {
                    result.push_back(&self.fields[i]);
                }
            }
            return result;
        }, nb::rv_policy::reference_internal, "Field metadata (for TSB)")
        .def_prop_ro("python_type", [](const TSMeta& self) {
            return self.python_type;
        }, "Python type for reconstruction (for TSB)")

        // Helper methods
        .def("is_collection", &TSMeta::is_collection,
            "True if TSS, TSD, TSL, or TSB")
        .def("is_scalar_ts", &TSMeta::is_scalar_ts,
            "True if TS, TSW, or SIGNAL");
}
```

### TSTypeRegistry Binding

```cpp
static void register_ts_type_registry(nb::module_& m) {
    using namespace nb::literals;

    nb::class_<TSTypeRegistry>(m, "TSTypeRegistry",
        "Registry for time-series type schemas (singleton)")

        .def_static("instance", &TSTypeRegistry::instance,
            nb::rv_policy::reference,
            "Get the singleton instance")

        .def("ts", &TSTypeRegistry::ts,
            nb::rv_policy::reference,
            "value_type"_a,
            "Create TS[T] schema for scalar time-series")

        .def("tss", &TSTypeRegistry::tss,
            nb::rv_policy::reference,
            "element_type"_a,
            "Create TSS[T] schema for time-series set")

        .def("tsd", &TSTypeRegistry::tsd,
            nb::rv_policy::reference,
            "key_type"_a, "value_ts"_a,
            "Create TSD[K, V] schema for time-series dict")

        .def("tsl", &TSTypeRegistry::tsl,
            nb::rv_policy::reference,
            "element_ts"_a, "fixed_size"_a = 0,
            "Create TSL[TS, Size] schema for time-series list")

        .def("tsw", &TSTypeRegistry::tsw,
            nb::rv_policy::reference,
            "value_type"_a, "period"_a, "min_period"_a = 0,
            "Create TSW[T] schema for tick-based window")

        .def("tsw_duration", &TSTypeRegistry::tsw_duration,
            nb::rv_policy::reference,
            "value_type"_a, "time_range"_a,
            "min_time_range"_a = engine_time_delta_t{0},
            "Create TSW[T] schema for duration-based window")

        .def("tsb", &TSTypeRegistry::tsb,
            nb::rv_policy::reference,
            "fields"_a, "name"_a, "python_type"_a = nb::none(),
            "Create TSB[Schema] schema for time-series bundle")

        .def("ref", &TSTypeRegistry::ref,
            nb::rv_policy::reference,
            "referenced_ts"_a,
            "Create REF[TS] schema for time-series reference")

        .def("signal", &TSTypeRegistry::signal,
            nb::rv_policy::reference,
            "Get the SIGNAL schema singleton");
}
```

### Module Registration

```cpp
// In cpp/src/cpp/api/python/py_ts_type_registry.cpp

void ts_type_registry_register_with_nanobind(nb::module_& m) {
    register_ts_kind(m);
    register_tsb_field_info(m);
    register_ts_meta(m);
    register_ts_type_registry(m);
}
```

## Integration Point

Add registration call in `cpp/src/cpp/python/_hgraph_types.cpp`:

```cpp
// Add include at top
#include <hgraph/api/python/py_ts_type_registry.h>

// In export_types function, add after value_register_with_nanobind(m):
void export_types(nb::module_ &m) {
    using namespace hgraph;

    // Value type system (must come before time series types that use them)
    value_register_with_nanobind(m);

    // TSTypeRegistry (add here - after value, before time series types)
    ts_type_registry_register_with_nanobind(m);

    // ... rest of registrations ...
}
```

## Header File

Create `cpp/include/hgraph/api/python/py_ts_type_registry.h`:

```cpp
#pragma once

#include <nanobind/nanobind.h>

namespace nb = nanobind;

namespace hgraph {
void ts_type_registry_register_with_nanobind(nb::module_& m);
}
```

## Duration Conversion

For `tsw_duration()`, nanobind's chrono header converts Python `datetime.timedelta` to `std::chrono::duration`. Ensure `#include <hgraph/python/chrono.h>` is included.

## Testing Approach

Python tests:
```python
from hgraph._hgraph import TSTypeRegistry, TSKind
from hgraph._hgraph.value import get_scalar_type_meta

def test_ts_creation():
    reg = TSTypeRegistry.instance()
    int_meta = get_scalar_type_meta(int)
    ts_int = reg.ts(int_meta)

    assert ts_int.kind == TSKind.TSValue
    assert ts_int.value_type is int_meta

def test_deduplication():
    reg = TSTypeRegistry.instance()
    int_meta = get_scalar_type_meta(int)
    ts1 = reg.ts(int_meta)
    ts2 = reg.ts(int_meta)

    assert ts1 is ts2  # Same pointer
```
