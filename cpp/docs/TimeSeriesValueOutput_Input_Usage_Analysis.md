# TimeSeriesValueOutput/Input Usage Analysis

## Public API Methods Used by External Code

### TimeSeriesValueOutput<T> Methods

**From TimeSeriesOutput base (virtual overrides):**
- `py_value()` - Returns Python object representation of value (or None if invalid)
- `py_delta_value()` - Returns delta value (same as py_value for scalar)
- `py_set_value(nb::object)` - Sets value from Python object
- `apply_result(nb::object)` - Applies node computation result
- `mark_invalid()` - Invalidates the time series (sets value to {})
- `copy_from_output(const TimeSeriesOutput&)` - Copies from another output
- `copy_from_input(const TimeSeriesInput&)` - Copies from input
- `is_same_type(const TimeSeriesType*)` - Type checking

**TimeSeriesValueOutput-specific methods:**
- `value() const -> const T&` - Direct access to typed value
- `set_value(const T&)` - Set value by const reference
- `set_value(T&&)` - Set value by move
- `reset_value()` - Resets value to default {}

### TimeSeriesValueInput<T> Methods

**TimeSeriesValueInput-specific methods:**
- `value() const -> const T&` - Access to the typed value (delegates to value_output().value())
- `value_output() -> TimeSeriesValueOutput<T>&` - Get the bound output
- `value_output() const -> const TimeSeriesValueOutput<T>&` - Get the bound output (const)
- `is_same_type(const TimeSeriesType*)` - Type checking

## Usage Patterns in Codebase

### 1. TimeSeriesSetOutput - Uses TimeSeriesValueOutput<bool>
**Location:** `tss.h`, `tss.cpp`

**Pattern:** Creates `TimeSeriesValueOutput<bool>` instances for:
- `_is_empty_ref_output` - Tracks whether the set is empty
- `_contains_ref_outputs` - Reference outputs for contains() queries

**Methods called:**
- Constructor via `TimeSeriesValueOutputBuilder<bool>().make_instance()`
- `set_value(bool)` - To update contains/empty status
- `valid()` - Check if the output has been set

```cpp
// Creating the output
_is_empty_ref_output = dynamic_cast_ref<TimeSeriesValueOutput<bool>>(
    TimeSeriesValueOutputBuilder<bool>().make_instance(this)
);

// Using it
if (!_is_empty_ref_output->valid()) {
    _is_empty_ref_output->set_value(empty());
}

// For contains queries
reinterpret_cast<TimeSeriesValueOutput<bool>&>(ref).set_value(
    ts.contains(key)
);
```

### 2. SwitchNode - Uses TimeSeriesValueInput<K>
**Location:** `switch_node.h`, `switch_node.cpp`

**Pattern:** Uses a `TimeSeriesValueInput<K>*` pointer to track the switch key

**Methods called:**
- `valid()` - Check if key is available
- `modified()` - Check if key changed
- `value()` - Get the current key value

```cpp
if (!key_ts->valid()) {
    return; // No key
}

if (key_ts->modified()) {
    if (keys_equal(key_ts->value(), active_key_.value())) {
        // ...
    }
    active_key_ = key_ts->value();
}
```

### 3. Node Error Handling - Uses py_set_value
**Location:** `node.cpp`

**Pattern:** Sets error outputs with exception information

**Methods called:**
- `py_set_value(nb::object)` - Sets error object or string

```cpp
error_output()->py_set_value(nb::cast(error_ptr));
error_output()->py_set_value(nb::str(e.to_string().c_str()));
error_output()->py_set_value(nb::none());
```

## Key Observations

### Most Commonly Used Methods:

**TimeSeriesValueOutput:**
1. `set_value(const T&)` - Direct typed value setting (most common)
2. `value() const` - Direct typed value access
3. `py_set_value(nb::object)` - Python interop
4. `valid()` - Inherited from base, checks if valid
5. `mark_invalid()` - Resets to invalid state

**TimeSeriesValueInput:**
1. `value() const` - Gets the typed value
2. `valid()` - Inherited, checks if bound output is valid
3. `modified()` - Inherited, checks if value changed
4. `value_output()` - Gets the underlying output (used internally)

### Access Patterns:

1. **Direct typed access** is preferred over Python object access when working in C++
2. **Casting patterns** are used frequently:
   - `dynamic_cast_ref<TimeSeriesValueOutput<T>>()` for safe downcasting
   - `reinterpret_cast<TimeSeriesValueOutput<T>&>()` for performance-critical paths
3. **Builder pattern** is used to create instances via `TimeSeriesValueOutputBuilder<T>`

### Type Safety:

- Template instantiation is explicit at the bottom of `ts.cpp` for common types:
  - bool, int64_t, double, engine_date_t, engine_time_t, engine_time_delta_t, nb::object
- `is_same_type()` provides runtime type checking
- Dynamic casting is used extensively for type conversion

## Methods NOT Used Externally:

- `reset_value()` - Only used internally within TimeSeriesValueOutput
- `copy_from_output()` / `copy_from_input()` - Specialized copy operations, not commonly used
- `py_delta_value()` - Mostly for Python API compatibility
- `apply_result()` - Used by node execution framework, not by user code

## Summary

### Critical Public Interface:

The external interface is quite clean and focused. Most usage relies on:

**For Output:**
- Direct typed methods: `value()`, `set_value(T)`
- State checking: `valid()`
- Python interop: `py_set_value(nb::object)`

**For Input:**
- Direct typed access: `value()`
- State checking: `valid()`, `modified()`
- Internal access: `value_output()`

### Design Patterns:

1. **Type-safe access** - Templates ensure compile-time type safety
2. **Lazy evaluation** - Values only computed when accessed
3. **Builder pattern** - Instances created via specialized builders
4. **Mixed access** - Both C++ typed access and Python object access supported
5. **Inheritance-based** - Inherits common behavior from Base classes

### Files Using These Types:

- `tss.h/cpp` - TimeSeriesSet uses bool outputs for empty/contains
- `switch_node.h/cpp` - SwitchNode uses typed input for key
- `node.cpp` - Error handling uses py_set_value
- `ts.cpp` - Internal implementation and template instantiations

---

**Generated:** 2025-11-08
**Last Updated:** 2025-11-08
