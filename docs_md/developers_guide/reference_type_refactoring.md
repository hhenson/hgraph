# Reference Type Refactoring Proposal

**Status:** Proposed  
**Created:** 2025-11-10  
**Author:** AI Assistant (based on architecture review)

## Problem Statement

### Current Issues

The current `REF[...]` implementation loses type structure information, forcing complex dual-mode logic:

1. **Type Information Loss**: `REF[TSL[TS[int], Size[2]]]` → Single `TimeSeriesReferenceInput` class
2. **Dual-Mode Complexity**: Must support both peered/non-peered dynamically
3. **Optional Structure Fields**: `_items` optional vector used as fallback for structured types
4. **No Compile-Time Type Checking**: C++ can't validate bindings at compile time
5. **Complex State Logic**: `modified()`, `valid()`, `bind_output()` handle multiple modes

### Example of Current Problem

```cpp
// Current: All references look the same
TimeSeriesReferenceInput ref_to_ts;      // REF[TS[int]]
TimeSeriesReferenceInput ref_to_tsl;     // REF[TSL[TS[int], Size[3]]]
TimeSeriesReferenceInput ref_to_tsb;     // REF[TSB[MySchema]]

// Can't type-check at compile time!
// Runtime checks needed everywhere
if (ref_to_tsl._items.has_value()) { /* is it structured? */ }
if (ref_to_tsl.has_output()) { /* is it peered? */ }
```

---

## Proposed Solution: Typed Reference Hierarchy

Create a **hierarchy of specialized reference types** mirroring the time-series type hierarchy, preserving structure information and enabling compile-time type checking.

### Architecture Overview

```
BaseTimeSeriesReferenceInput (abstract)
├── TimeSeriesValueReferenceInput     (REF[TS[T]])
├── TimeSeriesListReferenceInput      (REF[TSL[T, SIZE]])
├── TimeSeriesBundleReferenceInput    (REF[TSB[Schema]])
├── TimeSeriesDictReferenceInput      (REF[TSD[K, V]])
└── TimeSeriesSetReferenceInput       (REF[TSS[T]])
```

Same for outputs:
```
BaseTimeSeriesReferenceOutput (abstract)
├── TimeSeriesValueReferenceOutput
├── TimeSeriesListReferenceOutput
├── TimeSeriesBundleReferenceOutput
├── TimeSeriesDictReferenceOutput
└── TimeSeriesSetReferenceOutput
```

---

## Detailed Design

### 1. Base Classes (C++)

**File:** `cpp/include/hgraph/types/ref_base.h`

```cpp
struct BaseTimeSeriesReferenceInput : BaseTimeSeriesInput {
    using BaseTimeSeriesInput::BaseTimeSeriesInput;
    
    // Common reference operations
    virtual void clone_binding(const BaseTimeSeriesReferenceInput::ptr &other) = 0;
    
    [[nodiscard]] bool is_reference() const final { return true; }
    [[nodiscard]] bool has_reference() const final { return true; }
    
    // Subclasses must implement value retrieval
    [[nodiscard]] virtual TimeSeriesReference::ptr value() const = 0;
};

struct BaseTimeSeriesReferenceOutput : BaseTimeSeriesOutput {
    using BaseTimeSeriesOutput::BaseTimeSeriesOutput;
    
    [[nodiscard]] bool is_reference() const final { return true; }
    [[nodiscard]] bool has_reference() const final { return true; }
    
    virtual const TimeSeriesReference::ptr &value() const = 0;
    virtual void set_value(TimeSeriesReference::ptr value) = 0;
};
```

---

### 2. Specialized Value Reference (Simple TS Types)

**File:** `cpp/include/hgraph/types/ref_value.h`

```cpp
// For REF[TS[int]], REF[TS[str]], etc.
struct TimeSeriesValueReferenceInput : BaseTimeSeriesReferenceInput {
    using BaseTimeSeriesReferenceInput::BaseTimeSeriesReferenceInput;
    
    // Only supports peered binding to ReferenceOutput
    // OR wrapping a single output
    bool bind_output(time_series_output_ptr output_) override;
    void un_bind_output(bool unbind_refs) override;
    
    TimeSeriesReference::ptr value() const override;
    
    void clone_binding(const BaseTimeSeriesReferenceInput::ptr &other) override;
    
    // Simplified logic - no _items
private:
    mutable TimeSeriesReference::ptr _cached_value;
};

struct TimeSeriesValueReferenceOutput : BaseTimeSeriesReferenceOutput {
    using BaseTimeSeriesReferenceOutput::BaseTimeSeriesReferenceOutput;
    
    const TimeSeriesReference::ptr &value() const override { return _value; }
    void set_value(TimeSeriesReference::ptr value) override;
    
    void observe_reference(TimeSeriesInput::ptr input_);
    void stop_observing_reference(TimeSeriesInput::ptr input_);
    
private:
    TimeSeriesReference::ptr _value;
    std::unordered_set<TimeSeriesInput::ptr> _reference_observers;
};
```

**Implementation:** `cpp/src/cpp/types/ref_value.cpp`

```cpp
bool TimeSeriesValueReferenceInput::bind_output(time_series_output_ptr output_) {
    if (auto ref_output = dynamic_cast<TimeSeriesValueReferenceOutput*>(output_.get())) {
        // Peered binding - standard case
        reset_value();
        return BaseTimeSeriesInput::do_bind_output(output_);
    }
    
    // Non-peered: Wrap any output as a reference
    _cached_value = TimeSeriesReference::make(std::move(output_));
    output().reset();
    
    if (owning_node()->is_started()) {
        set_sample_time(owning_graph()->evaluation_clock()->evaluation_time());
        notify(sample_time());
    } else {
        owning_node()->add_start_input(this);
    }
    
    return false;  // Not a peer
}

TimeSeriesReference::ptr TimeSeriesValueReferenceInput::value() const {
    if (has_output()) {
        return static_cast<TimeSeriesValueReferenceOutput*>(output().get())->value();
    }
    return _cached_value;
}
```

---

### 3. Specialized List Reference

**File:** `cpp/include/hgraph/types/ref_list.h`

```cpp
// For REF[TSL[TS[int], Size[3]]]
struct TimeSeriesListReferenceInput : BaseTimeSeriesReferenceInput, IndexedTimeSeriesInput {
    TimeSeriesListReferenceInput(node_ptr owning_node, size_t size);
    
    // Can be peered to TimeSeriesListReferenceOutput
    // OR can have _items bound to individual outputs
    bool bind_output(time_series_output_ptr output_) override;
    void un_bind_output(bool unbind_refs) override;
    
    TimeSeriesReference::ptr value() const override;
    
    void clone_binding(const BaseTimeSeriesReferenceInput::ptr &other) override;
    
    // IndexedTimeSeriesInput interface
    TimeSeriesInput *get_input(size_t index) override;
    size_t size() const override { return _size; }
    const std::vector<time_series_input_ptr> &values() const override;
    
    [[nodiscard]] bool modified() const override;
    [[nodiscard]] bool valid() const override;
    [[nodiscard]] bool all_valid() const override;
    [[nodiscard]] engine_time_t last_modified_time() const override;
    
    void make_active() override;
    void make_passive() override;
    
protected:
    void notify_parent(TimeSeriesInput *child, engine_time_t modified_time) override;
    
private:
    const size_t _size;
    // Pre-allocated, not optional!
    std::vector<BaseTimeSeriesReferenceInput::ptr> _items;
    mutable TimeSeriesReference::ptr _cached_value;
    
    bool any_item_modified() const;
};

struct TimeSeriesListReferenceOutput : BaseTimeSeriesReferenceOutput {
    TimeSeriesListReferenceOutput(node_ptr owning_node, size_t size);
    
    const TimeSeriesReference::ptr &value() const override { return _value; }
    void set_value(TimeSeriesReference::ptr value) override;
    
    void observe_reference(TimeSeriesInput::ptr input_);
    void stop_observing_reference(TimeSeriesInput::ptr input_);
    
private:
    const size_t _size;
    TimeSeriesReference::ptr _value;
    std::unordered_set<TimeSeriesInput::ptr> _reference_observers;
};
```

**Implementation:** `cpp/src/cpp/types/ref_list.cpp`

```cpp
bool TimeSeriesListReferenceInput::bind_output(time_series_output_ptr output_) {
    // Type-safe check!
    if (auto ref_list_output = dynamic_cast<TimeSeriesListReferenceOutput*>(output_.get())) {
        // Peered binding - standard case
        reset_value();
        return BaseTimeSeriesInput::do_bind_output(output_);
    }
    
    // Non-peered: Must be an IndexedTimeSeriesOutput with matching size
    auto indexed_output = dynamic_cast<IndexedTimeSeriesOutput*>(output_.get());
    if (!indexed_output) {
        throw std::runtime_error(
            "Cannot bind REF[TSL] to non-list output. "
            "Expected TimeSeriesListReferenceOutput or IndexedTimeSeriesOutput"
        );
    }
    
    if (indexed_output->size() != _size) {
        throw std::runtime_error(fmt::format(
            "Size mismatch: REF[TSL] has size {} but output has size {}",
            _size, indexed_output->size()
        ));
    }
    
    // Bind each child reference to corresponding output element
    for (size_t i = 0; i < _size; ++i) {
        _items[i]->bind_output(indexed_output->get_output(i));
    }
    
    // Notify if started
    if (owning_node()->is_started()) {
        set_sample_time(owning_graph()->evaluation_clock()->evaluation_time());
        if (active()) { notify(sample_time()); }
    }
    
    return false;  // Not a peer binding
}

TimeSeriesReference::ptr TimeSeriesListReferenceInput::value() const {
    if (has_output()) {
        // Peered: get from output
        return static_cast<TimeSeriesListReferenceOutput*>(output().get())->value();
    }
    
    // Non-peered: construct from items
    if (!_cached_value || any_item_modified()) {
        std::vector<TimeSeriesReference::ptr> item_refs;
        item_refs.reserve(_size);
        for (const auto &item : _items) {
            item_refs.push_back(item->value());
        }
        _cached_value = TimeSeriesReference::make(std::move(item_refs));
    }
    return _cached_value;
}

bool TimeSeriesListReferenceInput::modified() const {
    if (sampled()) return true;
    if (has_output()) return output()->modified();
    // Non-peered: any child modified
    return std::any_of(_items.begin(), _items.end(), 
        [](const auto &item) { return item->modified(); });
}

bool TimeSeriesListReferenceInput::valid() const {
    if (has_output()) return output()->valid();
    // Non-peered: any child valid
    return std::any_of(_items.begin(), _items.end(),
        [](const auto &item) { return item->valid(); });
}

bool TimeSeriesListReferenceInput::all_valid() const {
    if (has_output()) return output()->valid();
    // Non-peered: all children valid
    return std::all_of(_items.begin(), _items.end(),
        [](const auto &item) { return item->all_valid(); });
}
```

---

### 4. Specialized Bundle Reference

**File:** `cpp/include/hgraph/types/ref_bundle.h`

```cpp
// For REF[TSB[MySchema]]
struct TimeSeriesBundleReferenceInput : BaseTimeSeriesReferenceInput {
    TimeSeriesBundleReferenceInput(
        node_ptr owning_node, 
        TimeSeriesSchema::ptr schema,
        std::unordered_map<std::string, InputBuilder::ptr> field_builders
    );
    
    bool bind_output(time_series_output_ptr output_) override;
    void un_bind_output(bool unbind_refs) override;
    
    TimeSeriesReference::ptr value() const override;
    
    void clone_binding(const BaseTimeSeriesReferenceInput::ptr &other) override;
    
    // Bundle interface
    TimeSeriesInput *get_item(const std::string &key);
    const std::unordered_map<std::string, BaseTimeSeriesReferenceInput::ptr> &items() const;
    
    [[nodiscard]] bool modified() const override;
    [[nodiscard]] bool valid() const override;
    [[nodiscard]] bool all_valid() const override;
    
    void make_active() override;
    void make_passive() override;
    
protected:
    void notify_parent(TimeSeriesInput *child, engine_time_t modified_time) override;
    
private:
    TimeSeriesSchema::ptr _schema;
    // Pre-created items with proper types!
    std::unordered_map<std::string, BaseTimeSeriesReferenceInput::ptr> _items;
    mutable TimeSeriesReference::ptr _cached_value;
};

struct TimeSeriesBundleReferenceOutput : BaseTimeSeriesReferenceOutput {
    TimeSeriesBundleReferenceOutput(node_ptr owning_node, TimeSeriesSchema::ptr schema);
    
    const TimeSeriesReference::ptr &value() const override { return _value; }
    void set_value(TimeSeriesReference::ptr value) override;
    
private:
    TimeSeriesSchema::ptr _schema;
    TimeSeriesReference::ptr _value;
    std::unordered_set<TimeSeriesInput::ptr> _reference_observers;
};
```

---

### 5. Specialized Dict Reference

**File:** `cpp/include/hgraph/types/ref_dict.h`

```cpp
// For REF[TSD[str, TS[int]]]
template<typename K>
struct TimeSeriesDictReferenceInput : BaseTimeSeriesReferenceInput {
    TimeSeriesDictReferenceInput(
        node_ptr owning_node,
        InputBuilder::ptr value_ref_builder  // Builder for REF[TS[int]]
    );
    
    bool bind_output(time_series_output_ptr output_) override;
    void un_bind_output(bool unbind_refs) override;
    
    TimeSeriesReference::ptr value() const override;
    
    void clone_binding(const BaseTimeSeriesReferenceInput::ptr &other) override;
    
    // Dict interface - creates items on demand
    TimeSeriesInput *get_input(const K &key);
    
    [[nodiscard]] bool modified() const override;
    [[nodiscard]] bool valid() const override;
    [[nodiscard]] bool all_valid() const override;
    
private:
    InputBuilder::ptr _value_ref_builder;
    std::unordered_map<K, BaseTimeSeriesReferenceInput::ptr> _items;
    mutable TimeSeriesReference::ptr _cached_value;
};

// Specialized for common key types
using TimeSeriesDictReferenceInput_int = TimeSeriesDictReferenceInput<int>;
using TimeSeriesDictReferenceInput_str = TimeSeriesDictReferenceInput<std::string>;
```

---

## Builder System Refactoring

### 6. Builder Hierarchy

**File:** `cpp/include/hgraph/builders/time_series_types/time_series_ref_builders.h`

```cpp
struct TimeSeriesRefInputBuilder : InputBuilder {
    using InputBuilder::InputBuilder;
    bool has_reference() const override { return true; }
};

struct TimeSeriesRefOutputBuilder : OutputBuilder {
    using OutputBuilder::OutputBuilder;
    bool has_reference() const override { return true; }
};

// ===== Value Reference Builders =====

struct TimeSeriesValueRefInputBuilder : TimeSeriesRefInputBuilder {
    time_series_input_ptr make_instance(node_ptr owning_node) const override;
    time_series_input_ptr make_instance(time_series_input_ptr owning_input) const override;
    
    static void register_with_nanobind(nb::module_ &m);
};

struct TimeSeriesValueRefOutputBuilder : TimeSeriesRefOutputBuilder {
    time_series_output_ptr make_instance(node_ptr owning_node) const override;
    time_series_output_ptr make_instance(time_series_output_ptr owning_output) const override;
    
    static void register_with_nanobind(nb::module_ &m);
};

// ===== List Reference Builders =====

struct TimeSeriesListRefInputBuilder : TimeSeriesRefInputBuilder {
    InputBuilder::ptr value_builder;  // Builder for child elements
    size_t size;
    
    TimeSeriesListRefInputBuilder(InputBuilder::ptr value_builder, size_t size)
        : value_builder(std::move(value_builder)), size(size) {}
    
    time_series_input_ptr make_instance(node_ptr owning_node) const override;
    time_series_input_ptr make_instance(time_series_input_ptr owning_input) const override;
    
    static void register_with_nanobind(nb::module_ &m);
};

struct TimeSeriesListRefOutputBuilder : TimeSeriesRefOutputBuilder {
    size_t size;
    
    explicit TimeSeriesListRefOutputBuilder(size_t size) : size(size) {}
    
    time_series_output_ptr make_instance(node_ptr owning_node) const override;
    time_series_output_ptr make_instance(time_series_output_ptr owning_output) const override;
    
    static void register_with_nanobind(nb::module_ &m);
};

// ===== Bundle Reference Builders =====

struct TimeSeriesBundleRefInputBuilder : TimeSeriesRefInputBuilder {
    TimeSeriesSchema::ptr schema;
    std::unordered_map<std::string, InputBuilder::ptr> field_builders;
    
    TimeSeriesBundleRefInputBuilder(
        TimeSeriesSchema::ptr schema,
        std::unordered_map<std::string, InputBuilder::ptr> field_builders
    ) : schema(std::move(schema)), field_builders(std::move(field_builders)) {}
    
    time_series_input_ptr make_instance(node_ptr owning_node) const override;
    time_series_input_ptr make_instance(time_series_input_ptr owning_input) const override;
    
    static void register_with_nanobind(nb::module_ &m);
};

struct TimeSeriesBundleRefOutputBuilder : TimeSeriesRefOutputBuilder {
    TimeSeriesSchema::ptr schema;
    
    explicit TimeSeriesBundleRefOutputBuilder(TimeSeriesSchema::ptr schema)
        : schema(std::move(schema)) {}
    
    time_series_output_ptr make_instance(node_ptr owning_node) const override;
    time_series_output_ptr make_instance(time_series_output_ptr owning_output) const override;
    
    static void register_with_nanobind(nb::module_ &m);
};

// ===== Dict Reference Builders (Templates) =====

template<typename K>
struct TimeSeriesDictRefInputBuilder : TimeSeriesRefInputBuilder {
    InputBuilder::ptr value_ref_builder;
    
    explicit TimeSeriesDictRefInputBuilder(InputBuilder::ptr value_ref_builder)
        : value_ref_builder(std::move(value_ref_builder)) {}
    
    time_series_input_ptr make_instance(node_ptr owning_node) const override;
    time_series_input_ptr make_instance(time_series_input_ptr owning_input) const override;
};

// Explicit instantiations for common types
using TimeSeriesDictRefInputBuilder_int = TimeSeriesDictRefInputBuilder<int>;
using TimeSeriesDictRefInputBuilder_str = TimeSeriesDictRefInputBuilder<std::string>;
```

**Implementation Example:** `cpp/src/cpp/builders/time_series_types/time_series_list_ref_input_builder.cpp`

```cpp
time_series_input_ptr TimeSeriesListRefInputBuilder::make_instance(node_ptr owning_node) const {
    auto ref = new TimeSeriesListReferenceInput(owning_node, size);
    
    // Pre-create children with correct type using the value_builder
    for (size_t i = 0; i < size; ++i) {
        auto child = value_builder->make_instance(time_series_input_ptr(ref));
        ref->_items[i] = nb::cast<BaseTimeSeriesReferenceInput::ptr>(child);
    }
    
    return time_series_input_ptr{static_cast<TimeSeriesInput*>(ref)};
}

void TimeSeriesListRefInputBuilder::register_with_nanobind(nb::module_ &m) {
    nb::class_<TimeSeriesListRefInputBuilder, TimeSeriesRefInputBuilder>(m, "InputBuilder_TSL_Ref")
        .def(nb::init<InputBuilder::ptr, size_t>(), "value_builder"_a, "size"_a);
}
```

---

## Factory Integration

### 7. Python Builder Factory Changes

**File:** `hgraph/_impl/_builder/_ts_builder.py`

```python
class PythonTimeSeriesBuilderFactory(TimeSeriesBuilderFactory):
    
    def make_input_builder(self, value_tp: HgTimeSeriesTypeMetaData) -> TSInputBuilder:
        if isinstance(value_tp, HgREFTypeMetaData):
            # Inspect what's being referenced
            return self._make_ref_input_builder(value_tp)
        
        # ... rest of factory logic
    
    def _make_ref_input_builder(self, ref_tp: HgREFTypeMetaData) -> TSInputBuilder:
        """Create specialized reference input builder based on referenced type"""
        referenced_tp = ref_tp.value_tp
        
        if isinstance(referenced_tp, HgTSTypeMetaData):
            # Simple value reference: REF[TS[int]]
            return PythonREFValueInputBuilder(value_tp=referenced_tp.value_scalar_tp)
        
        elif isinstance(referenced_tp, HgTSLTypeMetaData):
            # List reference: REF[TSL[TS[int], Size[3]]]
            child_ref_tp = HgREFTypeMetaData(referenced_tp.value_tp)
            child_builder = self._make_ref_input_builder(child_ref_tp)
            return PythonREFListInputBuilder(
                value_builder=child_builder,
                size_tp=referenced_tp.size_tp
            )
        
        elif isinstance(referenced_tp, HgTSBTypeMetaData):
            # Bundle reference: REF[TSB[MySchema]]
            field_builders = {}
            for key, field_tp in referenced_tp.bundle_schema_tp.meta_data_schema.items():
                field_ref_tp = HgREFTypeMetaData(field_tp)
                field_builders[key] = self._make_ref_input_builder(field_ref_tp)
            return PythonREFBundleInputBuilder(
                schema=referenced_tp.bundle_schema_tp.py_type,
                field_builders=frozendict(field_builders)
            )
        
        elif isinstance(referenced_tp, HgTSDTypeMetaData):
            # Dict reference: REF[TSD[str, TS[int]]]
            value_ref_tp = HgREFTypeMetaData(referenced_tp.value_tp)
            value_builder = self._make_ref_input_builder(value_ref_tp)
            return PythonREFDictInputBuilder(
                key_tp=referenced_tp.key_tp,
                value_builder=value_builder
            )
        
        elif isinstance(referenced_tp, HgTSSTypeMetaData):
            # Set reference: REF[TSS[int]]
            return PythonREFSetInputBuilder(value_tp=referenced_tp.value_scalar_tp)
        
        else:
            raise TypeError(f"Unsupported reference type: {referenced_tp}")
    
    def _make_ref_output_builder(self, ref_tp: HgREFTypeMetaData) -> TSOutputBuilder:
        """Create specialized reference output builder based on referenced type"""
        referenced_tp = ref_tp.value_tp
        
        if isinstance(referenced_tp, HgTSTypeMetaData):
            return PythonREFValueOutputBuilder(value_tp=referenced_tp.value_scalar_tp)
        
        elif isinstance(referenced_tp, HgTSLTypeMetaData):
            return PythonREFListOutputBuilder(size_tp=referenced_tp.size_tp)
        
        elif isinstance(referenced_tp, HgTSBTypeMetaData):
            return PythonREFBundleOutputBuilder(
                schema=referenced_tp.bundle_schema_tp.py_type
            )
        
        elif isinstance(referenced_tp, HgTSDTypeMetaData):
            return PythonREFDictOutputBuilder(key_tp=referenced_tp.key_tp)
        
        elif isinstance(referenced_tp, HgTSSTypeMetaData):
            return PythonREFSetOutputBuilder(value_tp=referenced_tp.value_scalar_tp)
        
        else:
            raise TypeError(f"Unsupported reference type: {referenced_tp}")
```

### 8. C++ Builder Factory

**File:** `hgraph/_use_cpp_runtime.py`

```python
class HgCppFactory(hgraph.TimeSeriesBuilderFactory):
    
    def make_input_builder(self, value_tp):
        if isinstance(value_tp, hgraph.HgREFTypeMetaData):
            return self._make_ref_input_builder(value_tp)
        # ... other types
    
    def make_output_builder(self, value_tp):
        if isinstance(value_tp, hgraph.HgREFTypeMetaData):
            return self._make_ref_output_builder(value_tp)
        # ... other types
    
    def _make_ref_input_builder(self, ref_tp):
        """Create specialized C++ reference input builder"""
        referenced_tp = ref_tp.value_tp
        
        if isinstance(referenced_tp, hgraph.HgTSTypeMetaData):
            # Simple value reference
            return _hgraph.InputBuilder_TS_Ref_Value()
        
        elif isinstance(referenced_tp, hgraph.HgTSLTypeMetaData):
            # List reference - recursively create child builder
            child_ref_tp = hgraph.HgREFTypeMetaData(referenced_tp.value_tp)
            child_builder = self._make_ref_input_builder(child_ref_tp)
            return _hgraph.InputBuilder_TSL_Ref(
                child_builder,
                referenced_tp.size_tp.py_type.SIZE
            )
        
        elif isinstance(referenced_tp, hgraph.HgTSBTypeMetaData):
            # Bundle reference - create builder for each field
            field_builders = {}
            for key, field_tp in referenced_tp.bundle_schema_tp.meta_data_schema.items():
                field_ref_tp = hgraph.HgREFTypeMetaData(field_tp)
                field_builders[key] = self._make_ref_input_builder(field_ref_tp)
            
            return _hgraph.InputBuilder_TSB_Ref(
                _hgraph.TimeSeriesSchema(
                    keys=tuple(referenced_tp.bundle_schema_tp.meta_data_schema.keys()),
                    scalar_type=referenced_tp.bundle_schema_tp.py_type.scalar_type()
                ),
                field_builders
            )
        
        elif isinstance(referenced_tp, hgraph.HgTSDTypeMetaData):
            # Dict reference - need value builder
            value_ref_tp = hgraph.HgREFTypeMetaData(referenced_tp.value_tp)
            value_builder = self._make_ref_input_builder(value_ref_tp)
            return _tsd_ref_input_builder_for_tp(referenced_tp.key_tp)(value_builder)
        
        elif isinstance(referenced_tp, hgraph.HgTSSTypeMetaData):
            # Set reference
            return _tss_ref_input_builder_type_for(referenced_tp.value_scalar_tp)()
        
        else:
            raise NotImplementedError(f"Reference to {type(referenced_tp).__name__}")
    
    def _make_ref_output_builder(self, ref_tp):
        """Create specialized C++ reference output builder"""
        referenced_tp = ref_tp.value_tp
        
        if isinstance(referenced_tp, hgraph.HgTSTypeMetaData):
            return _hgraph.OutputBuilder_TS_Ref_Value()
        
        elif isinstance(referenced_tp, hgraph.HgTSLTypeMetaData):
            return _hgraph.OutputBuilder_TSL_Ref(referenced_tp.size_tp.py_type.SIZE)
        
        elif isinstance(referenced_tp, hgraph.HgTSBTypeMetaData):
            return _hgraph.OutputBuilder_TSB_Ref(
                _hgraph.TimeSeriesSchema(
                    keys=tuple(referenced_tp.bundle_schema_tp.meta_data_schema.keys()),
                    scalar_type=referenced_tp.bundle_schema_tp.py_type.scalar_type()
                )
            )
        
        elif isinstance(referenced_tp, hgraph.HgTSDTypeMetaData):
            return _tsd_ref_output_builder_for_tp(referenced_tp.key_tp)()
        
        elif isinstance(referenced_tp, hgraph.HgTSSTypeMetaData):
            return _tss_ref_output_builder_for_tp(referenced_tp.value_scalar_tp)()
        
        else:
            raise NotImplementedError(f"Reference to {type(referenced_tp).__name__}")

# Helper functions for templated builders
def _tsd_ref_input_builder_for_tp(key_scalar_tp):
    return {
        bool: _hgraph.InputBuilder_TSD_Ref_bool,
        int: _hgraph.InputBuilder_TSD_Ref_int,
        float: _hgraph.InputBuilder_TSD_Ref_float,
        date: _hgraph.InputBuilder_TSD_Ref_date,
        datetime: _hgraph.InputBuilder_TSD_Ref_date_time,
        timedelta: _hgraph.InputBuilder_TSD_Ref_time_delta,
    }.get(key_scalar_tp.py_type, _hgraph.InputBuilder_TSD_Ref_object)
```

---

## Migration Strategy

### Phase 1: Add Specialized Types (Non-Breaking)

**Goal:** Implement new types alongside existing implementation

**Tasks:**
- [ ] Create new header files:
  - [ ] `cpp/include/hgraph/types/ref_base.h`
  - [ ] `cpp/include/hgraph/types/ref_value.h`
  - [ ] `cpp/include/hgraph/types/ref_list.h`
  - [ ] `cpp/include/hgraph/types/ref_bundle.h`
  - [ ] `cpp/include/hgraph/types/ref_dict.h`
  - [ ] `cpp/include/hgraph/types/ref_set.h`

- [ ] Implement specialized C++ types
  - [ ] Base classes with common interface
  - [ ] `TimeSeriesValueReferenceInput/Output`
  - [ ] `TimeSeriesListReferenceInput/Output`
  - [ ] `TimeSeriesBundleReferenceInput/Output`
  - [ ] `TimeSeriesDictReferenceInput/Output` (templated)
  - [ ] `TimeSeriesSetReferenceInput/Output`

- [ ] Create builder hierarchy
  - [ ] `cpp/include/hgraph/builders/time_series_types/time_series_ref_builders.h`
  - [ ] Implement specialized builders
  - [ ] Register with nanobind

- [ ] Add Python specialized builders
  - [ ] `PythonREFValueInputBuilder/OutputBuilder`
  - [ ] `PythonREFListInputBuilder/OutputBuilder`
  - [ ] `PythonREFBundleInputBuilder/OutputBuilder`
  - [ ] `PythonREFDictInputBuilder/OutputBuilder`
  - [ ] `PythonREFSetInputBuilder/OutputBuilder`

### Phase 2: Update Factory & Tests

**Goal:** Wire up new types through factory system

**Tasks:**
- [ ] Update `PythonTimeSeriesBuilderFactory`:
  - [ ] Add `_make_ref_input_builder()` method
  - [ ] Add `_make_ref_output_builder()` method
  - [ ] Update `make_input_builder()` to use specialized ref builders
  - [ ] Update `make_output_builder()` to use specialized ref builders

- [ ] Update `HgCppFactory` in `_use_cpp_runtime.py`:
  - [ ] Implement `_make_ref_input_builder()`
  - [ ] Implement `_make_ref_output_builder()`
  - [ ] Add helper functions for templated types

- [ ] Add comprehensive tests:
  - [ ] Test peered bindings for each type
  - [ ] Test non-peered bindings for each type
  - [ ] Test type checking and error messages
  - [ ] Test size/schema validation
  - [ ] Test nested references (REF[TSL[REF[TS[int]]]])
  - [ ] Test clone_binding for each type
  - [ ] Test modified/valid/all_valid logic
  - [ ] Test active/passive propagation

### Phase 3: Deprecate & Clean Up

**Goal:** Remove old implementation

**Tasks:**
- [ ] Mark old `TimeSeriesReferenceInput/Output` as deprecated
  - [ ] Add deprecation warnings
  - [ ] Update documentation

- [ ] Migrate internal uses:
  - [ ] Switch node implementations to use base class
  - [ ] Update special nodes (switch, map, reduce) if needed
  - [ ] Verify all tests pass with new implementation

- [ ] Remove old implementation:
  - [ ] Delete or merge old `TimeSeriesReferenceInput`
  - [ ] Could make it an alias to `TimeSeriesValueReferenceInput`
  - [ ] Clean up unused code

- [ ] Update documentation:
  - [ ] Document new hierarchy
  - [ ] Update examples
  - [ ] Add migration guide

---

## Benefits Summary

### ✅ Type Safety

```cpp
// Compile-time type checking!
auto list_ref = static_cast<TimeSeriesListReferenceInput*>(input);

if (list_ref->size() != expected_size) {
    throw std::runtime_error("Size mismatch");
}

// Can type-check bindings
auto output = dynamic_cast<TimeSeriesListReferenceOutput*>(output_.get());
if (!output) {
    throw std::runtime_error("Type mismatch: expected list reference output");
}
```

### ✅ Simplified Logic

- **No Optional Fields:** `_items` is always present for structured types
- **No Mode Checking:** Each type knows its structure
- **Clear Semantics:** Each class has single responsibility
- **Simpler Methods:** No branching on "is it peered or not?" in every method

**Before:**
```cpp
bool modified() const {
    if (_sampled) return true;
    if (_output) return _output->modified();
    if (_items.has_value()) {
        return std::any_of(_items->begin(), _items->end(), ...);
    }
    return false;
}
```

**After (Value):**
```cpp
bool modified() const {
    if (sampled()) return true;
    if (has_output()) return output()->modified();
    return false;  // Simpler!
}
```

**After (List):**
```cpp
bool modified() const {
    if (sampled()) return true;
    if (has_output()) return output()->modified();
    return std::any_of(_items.begin(), _items.end(), ...);  // No has_value check!
}
```

### ✅ Better Error Messages

```
Error: Cannot bind REF[TSL[TS[int], Size[3]]] to TS[int] output.
Expected: TimeSeriesListReferenceOutput or TSL[TS[int], Size[3]] output.

Error: Size mismatch when binding REF[TSL].
Expected size: 3
Actual size: 5

Error: Cannot bind REF[TSB[MySchema]] to TSB[OtherSchema].
Schema mismatch: missing field 'price'
```

### ✅ Performance

- **Pre-allocated Children:** TSL and TSB children created at construction, not lazily
- **No Runtime Type Checks:** Compiler knows the type
- **Simpler Logic:** Less branching in hot paths
- **Cache-Friendly:** Fixed-size structures, better memory layout

### ✅ Maintainability

- **Mirrors TS Hierarchy:** Easy to understand structure
- **Single Responsibility:** Each class does one thing well
- **Easier to Extend:** Add new reference types by following pattern
- **Better Documentation:** Clear what each type supports

---

## Open Questions & Decisions Needed

### 1. REF[TSD] Pre-allocation

**Question:** Should `TimeSeriesDictReferenceInput` pre-allocate anything?

**Options:**
- **A:** Create items on-demand (current proposal)
  - Pro: Handles dynamic keys naturally
  - Pro: No wasted memory
  - Con: Lazy creation adds complexity
  
- **B:** Pre-allocate known keys for schema-based TSD
  - Pro: More consistent with TSB approach
  - Con: Not all TSDs have schemas
  - Con: Doesn't help with truly dynamic keys

**Recommendation:** Stick with on-demand creation, but store the `value_ref_builder` for type checking.

### 2. REF[TSW] Support

**Question:** How to handle `REF[TSW[...]]`?

**Options:**
- **A:** Treat like value references (single entity)
  - Pro: Windows are typically used as complete values
  - Con: Loses structure information
  
- **B:** Create specialized `TimeSeriesWindowReferenceInput`
  - Pro: Consistent with other types
  - Con: Not clear what non-peered binding means for windows
  
- **C:** Disallow REF[TSW]
  - Pro: Simplifies design
  - Con: May be useful in some patterns

**Recommendation:** Treat like value references initially. Revisit if use cases emerge.

### 3. Backward Compatibility

**Question:** How to maintain Python API compatibility?

**Approach:**
- Keep `TimeSeriesReference.make()` factory working
- Factory internally produces correct specialized type
- Python code sees correct specialized class
- Old code that doesn't care about type still works

**Example:**
```python
# Still works
ref = TimeSeriesReference.make(output)

# But type is now specialized
assert isinstance(ref, BoundTimeSeriesReference)

# In C++, this is now TimeSeriesValueReferenceInput internally
ref_input.bind_output(output)
```

### 4. TimeSeriesReference Holder Specialization

**Question:** Should we specialize `TimeSeriesReference` holder types too?

**Current:**
- `BoundTimeSeriesReference` holds any output
- `UnBoundTimeSeriesReference` holds list of references

**Possible:**
- `BoundValueTimeSeriesReference`
- `BoundListTimeSeriesReference` (with size)
- `BoundBundleTimeSeriesReference` (with schema)

**Options:**
- **A:** Keep current holders (simpler)
- **B:** Specialize holders (more validation)

**Recommendation:** Start with current holders. They work across all types. Specialize later if validation benefits are needed.

### 5. Nested References

**Question:** How to handle `REF[TSL[REF[TS[int]], Size[3]]]`?

**Analysis:**
- Outer REF → `TimeSeriesListReferenceInput`
- Children are → `TimeSeriesValueReferenceInput` (also references!)
- Factory recursively creates reference builders

**This should work naturally with the proposed design!**

```python
# Factory call for REF[TSL[REF[TS[int]], Size[3]]]
referenced_tp = TSL[REF[TS[int]], Size[3]]
child_ref_tp = HgREFTypeMetaData(REF[TS[int]])  # This is already a REF!
child_builder = self._make_ref_input_builder(child_ref_tp)  # Recursion handles it
return PythonREFListInputBuilder(child_builder, size=3)
```

### 6. Python Implementation

**Question:** Should Python implementation also get specialized classes?

**Options:**
- **A:** Keep Python using current single class (easier)
- **B:** Create Python specialized classes to mirror C++

**Recommendation:** Start with option A. Python is reference implementation and flexibility is useful for debugging. Once C++ is stable, can consider mirroring if benefits are clear.

---

## Testing Strategy

### Unit Tests

```python
# Test REF[TS[int]] - Value Reference
def test_ref_value_peered_binding():
    """Test peered binding of value reference"""
    # Create REF output and input, bind them, verify behavior

def test_ref_value_nonpeered_binding():
    """Test wrapping regular output in value reference"""
    # Bind REF input to TS output, verify it wraps

# Test REF[TSL[TS[int], Size[3]]] - List Reference
def test_ref_list_peered_binding():
    """Test peered binding of list reference"""

def test_ref_list_nonpeered_binding():
    """Test binding list reference to individual outputs"""

def test_ref_list_size_validation():
    """Test size mismatch errors"""

# Test REF[TSB[MySchema]] - Bundle Reference
def test_ref_bundle_peered_binding():
    """Test peered binding of bundle reference"""

def test_ref_bundle_nonpeered_binding():
    """Test binding bundle reference to structured output"""

def test_ref_bundle_schema_validation():
    """Test schema mismatch errors"""

# Test REF[TSD[str, TS[int]]] - Dict Reference
def test_ref_dict_peered_binding():
    """Test peered binding of dict reference"""

def test_ref_dict_nonpeered_binding():
    """Test binding dict reference with dynamic keys"""

# Test Nested References
def test_nested_ref_list_of_refs():
    """Test REF[TSL[REF[TS[int]], Size[2]]]"""

# Test State Management
def test_ref_modified_tracking():
    """Test modified() for each reference type"""

def test_ref_validity_checking():
    """Test valid() and all_valid() for each reference type"""

def test_ref_active_passive():
    """Test active/passive state propagation"""

# Test Clone Binding
def test_ref_clone_binding():
    """Test clone_binding for each reference type"""
```

### Integration Tests

```python
def test_ref_in_map_node():
    """Test references work in map nodes"""

def test_ref_in_switch_node():
    """Test references work in switch nodes"""

def test_ref_in_reduce_node():
    """Test references work in reduce nodes"""

def test_ref_service_integration():
    """Test references in service implementations"""
```

---

## Implementation Checklist

### Core Implementation
- [ ] Define base classes in `ref_base.h`
- [ ] Implement `TimeSeriesValueReferenceInput/Output`
- [ ] Implement `TimeSeriesListReferenceInput/Output`
- [ ] Implement `TimeSeriesBundleReferenceInput/Output`
- [ ] Implement `TimeSeriesDictReferenceInput/Output` (templated)
- [ ] Implement `TimeSeriesSetReferenceInput/Output`

### Builders
- [ ] Create builder hierarchy in `time_series_ref_builders.h`
- [ ] Implement value reference builders
- [ ] Implement list reference builders
- [ ] Implement bundle reference builders
- [ ] Implement dict reference builders (templated)
- [ ] Implement set reference builders
- [ ] Register all builders with nanobind

### Factory Integration
- [ ] Update `PythonTimeSeriesBuilderFactory`
- [ ] Update `HgCppFactory` in `_use_cpp_runtime.py`
- [ ] Add helper functions for templated builders

### Python Builders (Optional)
- [ ] Create specialized Python builder classes
- [ ] Or keep using existing single class

### Testing
- [ ] Unit tests for each reference type
- [ ] Peered binding tests
- [ ] Non-peered binding tests
- [ ] Validation and error message tests
- [ ] State management tests
- [ ] Integration tests with special nodes
- [ ] Performance benchmarks

### Documentation
- [ ] Update architecture docs
- [ ] Document migration guide
- [ ] Update API reference
- [ ] Add examples for each reference type

### Cleanup
- [ ] Deprecate old implementation
- [ ] Migrate internal uses
- [ ] Remove old code
- [ ] Update CHANGELOG

---

## Related Files

### Current Implementation
- `cpp/include/hgraph/types/ref.h`
- `cpp/src/cpp/types/ref.cpp`
- `hgraph/_types/_ref_type.py`
- `hgraph/_impl/_types/_ref.py`
- `cpp/include/hgraph/builders/time_series_types/time_series_ref_input_builder.h`
- `cpp/include/hgraph/builders/time_series_types/time_series_ref_output_builder.h`

### New Files to Create
- `cpp/include/hgraph/types/ref_base.h`
- `cpp/include/hgraph/types/ref_value.h`
- `cpp/include/hgraph/types/ref_list.h`
- `cpp/include/hgraph/types/ref_bundle.h`
- `cpp/include/hgraph/types/ref_dict.h`
- `cpp/include/hgraph/types/ref_set.h`
- `cpp/src/cpp/types/ref_value.cpp`
- `cpp/src/cpp/types/ref_list.cpp`
- `cpp/src/cpp/types/ref_bundle.cpp`
- `cpp/src/cpp/types/ref_dict.cpp`
- `cpp/src/cpp/types/ref_set.cpp`
- `cpp/include/hgraph/builders/time_series_types/time_series_ref_builders.h`
- `cpp/src/cpp/builders/time_series_types/time_series_ref_builders.cpp`

### Files to Update
- `hgraph/_impl/_builder/_ts_builder.py`
- `hgraph/_use_cpp_runtime.py`
- `hgraph/_types/_ref_meta_data.py` (possibly)

---

## Notes

- This refactoring maintains backward compatibility at the Python API level
- The factory pattern allows transparent switching between implementations
- Type information is preserved throughout the system
- Validation can happen at construction time rather than runtime
- The design mirrors the existing time-series type hierarchy for consistency

