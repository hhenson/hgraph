# Access Patterns Design

## Overview

This document defines the patterns for reading, writing, and iterating over time-series values.

## Reading

### TSInput Reading

```cpp
// Check if input ticked this cycle
if (input.modified()) {
    // Access the value
    TSView view = input.value();
    int val = view.value().as<int>();  // value() returns View, as<T>() extracts typed value
}
```

### TSOutput Reading

```cpp
// Access own output (less common)
TSView view = output.output();
```

### Nested Reading (TSB)

```cpp
TSBView bundle = input.value();

// By name
if (bundle.field("price").modified()) {
    double price = bundle.field("price").value().as<double>();
}

// By index
TSView first_field = bundle.field(0);
```

### Collection Reading (TSL, TSD, TSS)

```cpp
// TSL
TSLView list = input.value();
for (size_t i = 0; i < list.size(); ++i) {
    TSView elem = list.at(i);
}

// TSD
TSDView dict = input.value();
for (auto key : dict.keys()) {
    TSView val = dict.at(key);
}

// TSS
TSSView set = input.value();
for (auto elem : set.values()) {
    // ...
}
```

## Writing

### TSOutput Writing

```cpp
// Set scalar value
output.set_value(42);

// Set from View
output.set_value(some_view);

// Mark modified without changing value
output.mark_modified();
```

### Nested Writing (TSB)

```cpp
TSBOutput bundle = ...;

// Write single field
bundle.field("price").set_value(100.0);

// Write multiple fields atomically (peered bundles)
bundle.set_value({
    {"price", 100.0},
    {"quantity", 50}
});
```

### Collection Writing

```cpp
// TSL
TSLOutput list = ...;
list.append(value_from(42));
list.at(0).set_value(value_from(99));

// TSD
TSDOutput dict = ...;
dict.set_item(key_view, value);
dict.remove(key_view);

// TSS
TSSOutput set = ...;
set.add(value_from(42));
set.remove(value_from(10));
```

## Iteration

### TSL Iteration

```cpp
TSLView list = input.value();

// Index-based
for (size_t i = 0; i < list.size(); ++i) {
    TSView elem = list.at(i);
}

// Range-based values
for (TSView elem : list.values()) {
    // ...
}

// With modification check
for (TSView elem : list.modified_values()) {
    // Only elements that ticked
}

// Items with indices
for (auto [idx, ts] : list.modified_items()) {
    // idx is View of the index, ts is TSView
}
```

### TSD Iteration

```cpp
TSDView dict = input.value();

// All keys
for (auto key : dict.keys()) {
    TSView val = dict.at(key);
}

// Only modified entries
for (auto key : dict.modified_keys()) {
    TSView val = dict.at(key);
}

// Added keys this tick
for (auto key : dict.added_keys()) {
    // Newly added entries
}

// Removed keys this tick
for (auto key : dict.removed_keys()) {
    // Entries removed this tick
}

// Items iteration
for (auto [key, ts] : dict.modified_items()) {
    // key is View, ts is TSView
}
```

### TSS Iteration

```cpp
TSSView set = input.value();

// All values
for (auto val : set.values()) {
    // ...
}

// Delta iteration
for (auto val : set.added()) {
    // Newly added this tick
}
for (auto val : set.removed()) {
    // Removed this tick
}

// Check specific element
bool was_added = set.was_added(elem_view);
bool was_removed = set.was_removed(elem_view);
```

## Python Bindings

### Pythonic Access

```python
# TSB field access
price = input.value.price  # __getattr__
price = input.value["price"]  # __getitem__

# TSL access
first = input.value[0]
for elem in input.value:
    pass

# TSD access
val = input.value["key"]
for key, val in input.value.items():
    pass

# TSS access
if item in input.value:
    pass
for item in input.value:
    pass
```

### Type Conversion

```python
# Automatic conversion to Python types
val: int = input.value.value()
val: list = input.value.to_list()
val: dict = input.value.to_dict()
```

### Python Wrapper Architecture

**Challenge**: C++ TSOutput/TSInput are owned by Node as value members (`std::optional<TSOutput>`), not shared_ptr. Python wrappers need to prevent dangling pointers.

**Solution**: View-based wrappers that hold `shared_ptr<Node>` for lifetime management.

```cpp
// Base wrapper - holds node lifetime and view
class PyTimeSeriesOutput {
protected:
    std::shared_ptr<Node> node_;     // Prevents Node destruction
    TSOutput* output_;                // Raw pointer (valid while node_ alive)

public:
    PyTimeSeriesOutput(std::shared_ptr<Node> node, TSOutput* output)
        : node_(std::move(node)), output_(output) {}

    // State queries
    bool modified() const { return output_->modified(current_time()); }
    bool valid() const { return output_->valid(); }

    // Value access (returns Python wrapper, not raw view)
    nb::object value() const;
};

class PyTimeSeriesInput {
protected:
    std::shared_ptr<Node> node_;
    TSInput* input_;

public:
    // Similar pattern
};
```

### Kind-Specific Wrappers

Each TS kind has specialized Python wrappers:

```cpp
// Scalar wrapper
class PyTimeSeriesValueOutput : public PyTimeSeriesOutput {
public:
    nb::object value() const {
        return output_->view(current_time()).to_python();
    }

    void set_value(nb::object val) {
        output_->set_value(Value::from_python(val, meta()), current_time());
    }
};

// Bundle wrapper with __getattr__
class PyTimeSeriesBundleOutput : public PyTimeSeriesOutput {
public:
    nb::object __getattr__(std::string_view name) const {
        // Return wrapper for field
        TSView field_view = output_->view(current_time()).field(name);
        return meta()->field_meta(name)->as_output_api(field_view, node_);
    }
};

// List wrapper with __getitem__ and __iter__
class PyTimeSeriesListOutput : public PyTimeSeriesOutput {
public:
    nb::object __getitem__(size_t index) const {
        TSView elem_view = output_->view(current_time()).at(index);
        return elem_meta()->as_output_api(elem_view, node_);
    }

    size_t __len__() const {
        return output_->view(current_time()).size();
    }
};
```

### TSMeta as Python Wrapper Factory

TSMeta's `as_output_api()` / `as_input_api()` methods create the correct Python wrapper:

```cpp
class TSValueMeta : public TSMeta {
public:
    nb::object as_output_api(TSView view, std::shared_ptr<Node> node) const override {
        return nb::cast(PyTimeSeriesValueOutput(node, view.output()));
    }
};

class TSBTypeMeta : public TSMeta {
public:
    nb::object as_output_api(TSView view, std::shared_ptr<Node> node) const override {
        return nb::cast(PyTimeSeriesBundleOutput(node, view.output(), this));
    }
};
```

### Python Value Caching

For frequently accessed values, cache the Python conversion:

```cpp
class PyTimeSeriesValueOutput : public PyTimeSeriesOutput {
    mutable std::optional<nb::object> cached_value_;
    mutable engine_time_t cached_time_ = MIN_TIME;

public:
    nb::object value() const {
        engine_time_t mod_time = output_->last_modified_time();
        if (cached_time_ < mod_time) {
            cached_value_ = output_->view(current_time()).to_python();
            cached_time_ = mod_time;
        }
        return *cached_value_;
    }
};
```

### from_python / to_python

Value/View provides conversion methods:

```cpp
class Value {
public:
    // Convert Python object to Value
    static Value from_python(nb::object obj, const TypeMeta* meta);

    // Convert Value to Python object
    nb::object to_python() const;
};

class View {
public:
    // Convert View to Python (may share or copy data)
    nb::object to_python() const;

    // Update View from Python object
    void from_python(nb::object obj);
};
```

## Subscription Control

### Active/Passive

```cpp
// Deactivate input (stops receiving updates)
input.set_active(false);

// Reactivate
input.set_active(true);

// Per-field control (TSB)
input.set_active("expensive_field", false);
```

### Subscription Propagation

When a TSInput is deactivated:
1. Its active_value_ flag is set to false
2. The linked TSOutput removes it from observers
3. Graph scheduler stops scheduling for this input

## Open Questions

- TODO: Batched updates for performance?
- TODO: Transactional semantics for writes?
- TODO: Copy-on-write for large values?

## References

- User Guide: `06_ACCESS_PATTERNS.md`
- Research: Various TS-specific documents
