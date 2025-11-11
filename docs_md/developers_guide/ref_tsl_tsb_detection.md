# REF[TSL] and REF[TSB] Detection at Builder Selection

**Status:** Implementation Plan  
**Created:** 2025-11-10  
**Goal:** Distinguish between `REF[TSL]`/`REF[TSB]` and other references at builder creation time

---

## Problem

Currently, all `REF[...]` types use the same builder regardless of what they reference:

```python
# Python Factory (line 424)
HgREFTypeMetaData: lambda: PythonREFInputBuilder(value_tp=cast(HgREFTypeMetaData, value_tp).value_tp),

# C++ Factory (line 190)
hgraph.HgREFTypeMetaData: lambda: _hgraph.InputBuilder_TS_Ref(),
```

This loses structure information, making it impossible to:
- Pre-allocate children for `REF[TSL]` with known size
- Type-check bindings at construction time
- Provide better error messages
- Optimize for the structured case

---

## Solution: Builder Selection Based on Referenced Type

### Step 1: Inspect the Referenced Type

When we encounter `HgREFTypeMetaData`, inspect `value_tp.value_tp` to determine what's being referenced:

```python
if isinstance(value_tp, HgREFTypeMetaData):
    referenced_tp = value_tp.value_tp
    
    if isinstance(referenced_tp, HgTSLTypeMetaData):
        # Create TSL-specific reference builder
        return create_tsl_ref_builder(referenced_tp)
    
    elif isinstance(referenced_tp, HgTSBTypeMetaData):
        # Create TSB-specific reference builder
        return create_tsb_ref_builder(referenced_tp)
    
    else:
        # Create generic reference builder (current behavior)
        return create_generic_ref_builder(referenced_tp)
```

---

## Implementation

### Phase 1: Python Factory Changes

**File:** `hgraph/_impl/_builder/_ts_builder.py`

#### Current Code (lines 424, 440):

```python
HgREFTypeMetaData: lambda: PythonREFInputBuilder(value_tp=cast(HgREFTypeMetaData, value_tp).value_tp),
```

#### New Code:

```python
def make_input_builder(self, value_tp: HgTimeSeriesTypeMetaData) -> TSInputBuilder:
    # Handle REF specially to inspect what's referenced
    if isinstance(value_tp, HgREFTypeMetaData):
        return self._make_ref_input_builder(value_tp)
    
    return {
        HgTSTypeMetaData: lambda: PythonTSInputBuilder(...),
        # ... other types ...
        HgCONTEXTTypeMetaData: lambda: self.make_input_builder(value_tp.ts_type),
    }.get(type(value_tp), lambda: _throw(value_tp))()

def _make_ref_input_builder(self, ref_tp: HgREFTypeMetaData) -> TSInputBuilder:
    """Create appropriate reference builder based on what's being referenced"""
    referenced_tp = ref_tp.value_tp
    
    if isinstance(referenced_tp, HgTSLTypeMetaData):
        # REF[TSL[...]]
        # Need to create builder for child references
        child_ref_tp = HgREFTypeMetaData(referenced_tp.value_tp)
        child_builder = self._make_ref_input_builder(child_ref_tp)
        
        return PythonTSLREFInputBuilder(
            value_builder=child_builder,
            size_tp=referenced_tp.size_tp
        )
    
    elif isinstance(referenced_tp, HgTSBTypeMetaData):
        # REF[TSB[...]]
        # Create builders for each field
        field_builders = {}
        for key, field_tp in referenced_tp.bundle_schema_tp.meta_data_schema.items():
            field_ref_tp = HgREFTypeMetaData(field_tp)
            field_builders[key] = self._make_ref_input_builder(field_ref_tp)
        
        return PythonTSBREFInputBuilder(
            schema=referenced_tp.bundle_schema_tp.py_type,
            field_builders=frozendict(field_builders)
        )
    
    else:
        # All other types: TS, TSD, TSS, TSW, etc.
        return PythonREFInputBuilder(value_tp=referenced_tp)

def _make_ref_output_builder(self, ref_tp: HgREFTypeMetaData) -> TSOutputBuilder:
    """Create appropriate reference output builder"""
    referenced_tp = ref_tp.value_tp
    
    if isinstance(referenced_tp, HgTSLTypeMetaData):
        return PythonTSLREFOutputBuilder(size_tp=referenced_tp.size_tp)
    
    elif isinstance(referenced_tp, HgTSBTypeMetaData):
        return PythonTSBREFOutputBuilder(schema=referenced_tp.bundle_schema_tp.py_type)
    
    else:
        return PythonREFOutputBuilder(value_tp=referenced_tp)
```

#### Same for `make_output_builder`:

```python
def make_output_builder(self, value_tp: HgTimeSeriesTypeMetaData) -> TSOutputBuilder:
    if isinstance(value_tp, HgREFTypeMetaData):
        return self._make_ref_output_builder(value_tp)
    
    return {
        # ... other types ...
    }.get(type(value_tp), lambda: _throw(value_tp))()
```

---

### Phase 2: New Builder Classes (Python)

**File:** `hgraph/_impl/_builder/_ts_builder.py`

Add new builder classes that understand TSL/TSB structure:

```python
@dataclass(frozen=True)
class PythonTSLREFInputBuilder(PythonInputBuilder, REFInputBuilder):
    """Builder for REF[TSL[...]] - pre-creates child reference inputs"""
    value_builder: TSInputBuilder  # Builder for child references
    size_tp: "HgScalarTypeMetaData"
    
    def make_instance(self, owning_node=None, owning_input=None):
        from hgraph._impl._types._ref import PythonTimeSeriesListReferenceInput
        from hgraph import Size
        
        size = cast(Size, self.size_tp.py_type).SIZE
        
        ref = PythonTimeSeriesListReferenceInput(
            _parent_or_node=owning_input if owning_input is not None else owning_node,
            _size=size
        )
        
        # Pre-create children with proper types!
        ref._items = [
            self.value_builder.make_instance(owning_input=ref)
            for _ in range(size)
        ]
        
        return ref
    
    def release_instance(self, item):
        super().release_instance(item)
        if item._items:
            for child in item._items:
                self.value_builder.release_instance(child)


@dataclass(frozen=True)
class PythonTSBREFInputBuilder(PythonInputBuilder, REFInputBuilder):
    """Builder for REF[TSB[...]] - pre-creates field reference inputs"""
    schema: "TimeSeriesSchema"
    field_builders: Mapping[str, TSInputBuilder]
    
    def make_instance(self, owning_node=None, owning_input=None):
        from hgraph._impl._types._ref import PythonTimeSeriesBundleReferenceInput
        
        ref = PythonTimeSeriesBundleReferenceInput(
            _parent_or_node=owning_input if owning_input is not None else owning_node,
            _schema=self.schema
        )
        
        # Pre-create fields with proper types!
        ref._items = {
            key: builder.make_instance(owning_input=ref)
            for key, builder in self.field_builders.items()
        }
        
        return ref
    
    def release_instance(self, item):
        super().release_instance(item)
        if item._items:
            for key, builder in self.field_builders.items():
                builder.release_instance(item._items[key])


@dataclass(frozen=True)
class PythonTSLREFOutputBuilder(PythonOutputBuilder, REFOutputBuilder):
    """Builder for REF[TSL[...]] output"""
    size_tp: "HgScalarTypeMetaData"
    
    def make_instance(self, owning_node: Node = None, owning_output: TimeSeriesOutput = None):
        from hgraph._impl._types._ref import PythonTimeSeriesListReferenceOutput
        from hgraph import Size
        
        size = cast(Size, self.size_tp.py_type).SIZE
        
        return PythonTimeSeriesListReferenceOutput(
            _parent_or_node=owning_output if owning_output is not None else owning_node,
            _size=size
        )
    
    def release_instance(self, item):
        super().release_instance(item)


@dataclass(frozen=True)
class PythonTSBREFOutputBuilder(PythonOutputBuilder, REFOutputBuilder):
    """Builder for REF[TSB[...]] output"""
    schema: "TimeSeriesSchema"
    
    def make_instance(self, owning_node: Node = None, owning_output: TimeSeriesOutput = None):
        from hgraph._impl._types._ref import PythonTimeSeriesBundleReferenceOutput
        
        return PythonTimeSeriesBundleReferenceOutput(
            _parent_or_node=owning_output if owning_output is not None else owning_node,
            _schema=self.schema
        )
    
    def release_instance(self, item):
        super().release_instance(item)
```

---

### Phase 3: New Reference Input/Output Classes (Python)

**File:** `hgraph/_impl/_types/_ref.py`

Add specialized classes that know about their structure:

```python
@dataclass
class PythonTimeSeriesListReferenceInput(PythonBoundTimeSeriesInput, TimeSeriesReferenceInput):
    """Reference input for TSL - knows its size and has pre-allocated items"""
    
    _size: int = 0
    _value: typing.Optional[TimeSeriesReference] = None
    _items: list[TimeSeriesReferenceInput] = field(default_factory=list)
    
    @property
    def bound(self) -> bool:
        # Always considered bound since items are pre-created
        return super().bound or bool(self._items)
    
    def bind_output(self, output: TimeSeriesOutput) -> bool:
        peer = self.do_bind_output(output)
        
        if self.owning_node.is_started and self._output and self._output.valid:
            self._sample_time = self.owning_graph.evaluation_clock.evaluation_time
            if self.active:
                self.notify(self._sample_time)
        
        return peer
    
    def do_bind_output(self, output: TimeSeriesOutput) -> bool:
        if isinstance(output, PythonTimeSeriesListReferenceOutput):
            # Peered binding to another REF[TSL] output
            self._value = None
            return super().do_bind_output(output)
        
        # Non-peered: bind to TSL or other indexed output
        if hasattr(output, '__len__') and len(output) == self._size:
            # Bind each child to corresponding output element
            for i, child in enumerate(self._items):
                child.bind_output(output[i])
            
            self._output = None
            if self.owning_node.is_started:
                self._sample_time = self.owning_graph.evaluation_clock.evaluation_time
                self.notify(self._sample_time)
            else:
                self.owning_node.start_inputs.append(self)
            
            return False
        else:
            raise TypeError(
                f"Cannot bind REF[TSL] of size {self._size} to output of type {type(output).__name__}. "
                f"Expected PythonTimeSeriesListReferenceOutput or indexable output with size {self._size}."
            )
    
    @property
    def value(self):
        if self._output is not None:
            return super().value
        elif self._value:
            return self._value
        elif self._items:
            # Build from child references
            self._value = TimeSeriesReference.make(from_items=[i.value for i in self._items])
            return self._value
        else:
            return None
    
    @property
    def modified(self) -> bool:
        if self._sampled:
            return True
        elif self._output is not None:
            return self.output.modified
        elif self._items:
            return any(i.modified for i in self._items)
        else:
            return False
    
    @property
    def valid(self) -> bool:
        if self._output is not None:
            return self._output.valid
        return self._value is not None or (self._items and any(i.valid for i in self._items))
    
    @property
    def all_valid(self) -> bool:
        if self._output is not None:
            return self._output.valid
        return (self._items and all(i.all_valid for i in self._items)) or self._value is not None
    
    def make_active(self):
        if self._output is not None:
            super().make_active()
        else:
            self._active = True
        
        # Propagate to children
        if self._items:
            for item in self._items:
                item.make_active()
        
        if self.valid:
            self._sample_time = self.owning_graph.evaluation_clock.evaluation_time
            self.notify(self.last_modified_time)
    
    def make_passive(self):
        if self._output is not None:
            super().make_passive()
        else:
            self._active = False
        
        # Propagate to children
        if self._items:
            for item in self._items:
                item.make_passive()
    
    def notify_parent(self, child: "TimeSeriesInput", modified_time: datetime):
        # Child modified, clear cached value
        self._value = None
        self._sample_time = modified_time
        if self.active:
            super().notify_parent(self, modified_time)
    
    def __getitem__(self, item):
        if not self._items:
            raise IndexError("REF[TSL] items not initialized")
        return self._items[item]
    
    def __len__(self):
        return self._size


@dataclass
class PythonTimeSeriesBundleReferenceInput(PythonBoundTimeSeriesInput, TimeSeriesReferenceInput):
    """Reference input for TSB - knows its schema and has pre-allocated fields"""
    
    _schema: "TimeSeriesSchema" = None
    _value: typing.Optional[TimeSeriesReference] = None
    _items: dict[str, TimeSeriesReferenceInput] = field(default_factory=dict)
    
    @property
    def bound(self) -> bool:
        return super().bound or bool(self._items)
    
    def bind_output(self, output: TimeSeriesOutput) -> bool:
        peer = self.do_bind_output(output)
        
        if self.owning_node.is_started and self._output and self._output.valid:
            self._sample_time = self.owning_graph.evaluation_clock.evaluation_time
            if self.active:
                self.notify(self._sample_time)
        
        return peer
    
    def do_bind_output(self, output: TimeSeriesOutput) -> bool:
        if isinstance(output, PythonTimeSeriesBundleReferenceOutput):
            # Peered binding
            self._value = None
            return super().do_bind_output(output)
        
        # Non-peered: bind to TSB with matching schema
        if hasattr(output, '_ts_values') and isinstance(output._ts_values, dict):
            # Check schema compatibility
            if set(output._ts_values.keys()) != set(self._items.keys()):
                raise TypeError(
                    f"Cannot bind REF[TSB] with schema {set(self._items.keys())} "
                    f"to output with fields {set(output._ts_values.keys())}"
                )
            
            # Bind each field
            for key, child in self._items.items():
                child.bind_output(output._ts_values[key])
            
            self._output = None
            if self.owning_node.is_started:
                self._sample_time = self.owning_graph.evaluation_clock.evaluation_time
                self.notify(self._sample_time)
            else:
                self.owning_node.start_inputs.append(self)
            
            return False
        else:
            raise TypeError(
                f"Cannot bind REF[TSB] to output of type {type(output).__name__}. "
                f"Expected PythonTimeSeriesBundleReferenceOutput or TSB output."
            )
    
    @property
    def value(self):
        if self._output is not None:
            return super().value
        elif self._value:
            return self._value
        elif self._items:
            self._value = TimeSeriesReference.make(from_items=[self._items[k].value for k in sorted(self._items.keys())])
            return self._value
        else:
            return None
    
    @property
    def modified(self) -> bool:
        if self._sampled:
            return True
        elif self._output is not None:
            return self.output.modified
        elif self._items:
            return any(i.modified for i in self._items.values())
        else:
            return False
    
    @property
    def valid(self) -> bool:
        if self._output is not None:
            return self._output.valid
        return self._value is not None or (self._items and any(i.valid for i in self._items.values()))
    
    @property
    def all_valid(self) -> bool:
        if self._output is not None:
            return self._output.valid
        return (self._items and all(i.all_valid for i in self._items.values())) or self._value is not None
    
    def make_active(self):
        if self._output is not None:
            super().make_active()
        else:
            self._active = True
        
        if self._items:
            for item in self._items.values():
                item.make_active()
        
        if self.valid:
            self._sample_time = self.owning_graph.evaluation_clock.evaluation_time
            self.notify(self.last_modified_time)
    
    def make_passive(self):
        if self._output is not None:
            super().make_passive()
        else:
            self._active = False
        
        if self._items:
            for item in self._items.values():
                item.make_passive()
    
    def notify_parent(self, child: "TimeSeriesInput", modified_time: datetime):
        self._value = None
        self._sample_time = modified_time
        if self.active:
            super().notify_parent(self, modified_time)
    
    def __getattr__(self, item):
        if item.startswith('_'):
            return super().__getattribute__(item)
        if not self._items:
            raise AttributeError(f"REF[TSB] items not initialized")
        return self._items[item]


@dataclass
class PythonTimeSeriesListReferenceOutput(PythonTimeSeriesOutput, TimeSeriesReferenceOutput):
    """Reference output for TSL"""
    
    _size: int = 0
    _value: typing.Optional[TimeSeriesReference] = None
    _reference_observers: dict[int, TimeSeriesInput] = field(default_factory=dict)
    
    @property
    def value(self) -> TimeSeriesReference:
        return self._value
    
    @property
    def delta_value(self) -> TimeSeriesReference:
        return self._value
    
    @value.setter
    def value(self, v: TimeSeriesReference):
        if v is None:
            self.invalidate()
            return
        if not isinstance(v, TimeSeriesReference):
            raise TypeError(f"Expected TimeSeriesReference, got {type(v)}")
        
        # Validate it's appropriate for a list
        if isinstance(v, UnBoundTimeSeriesReference):
            if len(v.items) != self._size:
                raise ValueError(f"Reference has {len(v.items)} items but TSL size is {self._size}")
        
        self._value = v
        self.mark_modified()
        for observer in self._reference_observers.values():
            self._value.bind_input(observer)
    
    def observe_reference(self, input_: TimeSeriesInput):
        self._reference_observers[id(input_)] = input_
    
    def stop_observing_reference(self, input_: TimeSeriesInput):
        self._reference_observers.pop(id(input_), None)
    
    def clear(self):
        self.value = EmptyTimeSeriesReference()
    
    def invalidate(self):
        self._value = None
        self.mark_invalid()


@dataclass
class PythonTimeSeriesBundleReferenceOutput(PythonTimeSeriesOutput, TimeSeriesReferenceOutput):
    """Reference output for TSB"""
    
    _schema: "TimeSeriesSchema" = None
    _value: typing.Optional[TimeSeriesReference] = None
    _reference_observers: dict[int, TimeSeriesInput] = field(default_factory=dict)
    
    @property
    def value(self) -> TimeSeriesReference:
        return self._value
    
    @property
    def delta_value(self) -> TimeSeriesReference:
        return self._value
    
    @value.setter
    def value(self, v: TimeSeriesReference):
        if v is None:
            self.invalidate()
            return
        if not isinstance(v, TimeSeriesReference):
            raise TypeError(f"Expected TimeSeriesReference, got {type(v)}")
        
        self._value = v
        self.mark_modified()
        for observer in self._reference_observers.values():
            self._value.bind_input(observer)
    
    def observe_reference(self, input_: TimeSeriesInput):
        self._reference_observers[id(input_)] = input_
    
    def stop_observing_reference(self, input_: TimeSeriesInput):
        self._reference_observers.pop(id(input_), None)
    
    def clear(self):
        self.value = EmptyTimeSeriesReference()
    
    def invalidate(self):
        self._value = None
        self.mark_invalid()
```

---

### Phase 4: C++ Factory Changes

**File:** `hgraph/_use_cpp_runtime.py`

Similar pattern to Python - inspect the referenced type:

```python
def make_input_builder(self, value_tp):
    if isinstance(value_tp, hgraph.HgREFTypeMetaData):
        return self._make_ref_input_builder(value_tp)
    
    return {
        hgraph.HgSignalMetaData: lambda: _hgraph.InputBuilder_TS_Signal(),
        # ... other types ...
    }.get(type(value_tp), lambda: hgraph._impl._builder._ts_builder._throw(value_tp))()

def _make_ref_input_builder(self, ref_tp):
    """Create specialized C++ reference input builder"""
    referenced_tp = ref_tp.value_tp
    
    if isinstance(referenced_tp, hgraph.HgTSLTypeMetaData):
        # REF[TSL[...]] - need child builder
        child_ref_tp = hgraph.HgREFTypeMetaData(referenced_tp.value_tp)
        child_builder = self._make_ref_input_builder(child_ref_tp)
        return _hgraph.InputBuilder_TSL_Ref(
            child_builder,
            referenced_tp.size_tp.py_type.SIZE
        )
    
    elif isinstance(referenced_tp, hgraph.HgTSBTypeMetaData):
        # REF[TSB[...]] - need field builders
        field_builders = []
        for field_tp in referenced_tp.bundle_schema_tp.meta_data_schema.values():
            field_ref_tp = hgraph.HgREFTypeMetaData(field_tp)
            field_builders.append(self._make_ref_input_builder(field_ref_tp))
        
        return _hgraph.InputBuilder_TSB_Ref(
            _hgraph.TimeSeriesSchema(
                keys=tuple(referenced_tp.bundle_schema_tp.meta_data_schema.keys()),
                scalar_type=referenced_tp.bundle_schema_tp.py_type.scalar_type()
                    if referenced_tp.bundle_schema_tp.py_type.scalar_type() else None
            ),
            field_builders
        )
    
    else:
        # All other types - use current generic builder
        return _hgraph.InputBuilder_TS_Ref()

def _make_ref_output_builder(self, ref_tp):
    """Create specialized C++ reference output builder"""
    referenced_tp = ref_tp.value_tp
    
    if isinstance(referenced_tp, hgraph.HgTSLTypeMetaData):
        return _hgraph.OutputBuilder_TSL_Ref(referenced_tp.size_tp.py_type.SIZE)
    
    elif isinstance(referenced_tp, hgraph.HgTSBTypeMetaData):
        return _hgraph.OutputBuilder_TSB_Ref(
            _hgraph.TimeSeriesSchema(
                keys=tuple(referenced_tp.bundle_schema_tp.meta_data_schema.keys()),
                scalar_type=referenced_tp.bundle_schema_tp.py_type.scalar_type()
                    if referenced_tp.bundle_schema_tp.py_type.scalar_type() else None
            )
        )
    
    else:
        return _hgraph.OutputBuilder_TS_Ref()
```

---

### Phase 5: C++ Builders (Minimal Stubs)

**File:** `cpp/include/hgraph/builders/time_series_types/time_series_ref_builders.h`

Create new builder classes that will be implemented later:

```cpp
// List Reference Builders
struct HGRAPH_EXPORT TimeSeriesListRefInputBuilder : InputBuilder {
    InputBuilder::ptr value_builder;
    size_t size;
    
    TimeSeriesListRefInputBuilder(InputBuilder::ptr value_builder, size_t size)
        : value_builder(std::move(value_builder)), size(size) {}
    
    time_series_input_ptr make_instance(node_ptr owning_node) const override;
    time_series_input_ptr make_instance(time_series_input_ptr owning_input) const override;
    
    bool has_reference() const override { return true; }
    
    static void register_with_nanobind(nb::module_ &m);
};

struct HGRAPH_EXPORT TimeSeriesListRefOutputBuilder : OutputBuilder {
    size_t size;
    
    explicit TimeSeriesListRefOutputBuilder(size_t size) : size(size) {}
    
    time_series_output_ptr make_instance(node_ptr owning_node) const override;
    time_series_output_ptr make_instance(time_series_output_ptr owning_output) const override;
    
    bool has_reference() const override { return true; }
    
    static void register_with_nanobind(nb::module_ &m);
};

// Bundle Reference Builders
struct HGRAPH_EXPORT TimeSeriesBundleRefInputBuilder : InputBuilder {
    TimeSeriesSchema::ptr schema;
    std::unordered_map<std::string, InputBuilder::ptr> field_builders;
    
    TimeSeriesBundleRefInputBuilder(
        TimeSeriesSchema::ptr schema,
        std::unordered_map<std::string, InputBuilder::ptr> field_builders
    ) : schema(std::move(schema)), field_builders(std::move(field_builders)) {}
    
    time_series_input_ptr make_instance(node_ptr owning_node) const override;
    time_series_input_ptr make_instance(time_series_input_ptr owning_input) const override;
    
    bool has_reference() const override { return true; }
    
    static void register_with_nanobind(nb::module_ &m);
};

struct HGRAPH_EXPORT TimeSeriesBundleRefOutputBuilder : OutputBuilder {
    TimeSeriesSchema::ptr schema;
    
    explicit TimeSeriesBundleRefOutputBuilder(TimeSeriesSchema::ptr schema)
        : schema(std::move(schema)) {}
    
    time_series_output_ptr make_instance(node_ptr owning_node) const override;
    time_series_output_ptr make_instance(time_series_output_ptr owning_output) const override;
    
    bool has_reference() const override { return true; }
    
    static void register_with_nanobind(nb::module_ &m);
};
```

**For now, these can delegate to existing builders but with the structure information available!**

---

## Testing Strategy

### Test 1: Detection Works

```python
def test_ref_tsl_builder_detection():
    """Verify that REF[TSL] creates specialized builder"""
    from hgraph import HgREFTypeMetaData, HgTSLTypeMetaData, HgTSTypeMetaData, HgScalarTypeMetaData
    from hgraph._impl._builder._ts_builder import PythonTimeSeriesBuilderFactory
    
    factory = PythonTimeSeriesBuilderFactory()
    
    # REF[TSL[TS[int], Size[3]]]
    ref_tp = HgREFTypeMetaData(
        HgTSLTypeMetaData(
            HgTSTypeMetaData(HgScalarTypeMetaData.parse_type(int)),
            Size[3]
        )
    )
    
    builder = factory.make_input_builder(ref_tp)
    
    # Should be specialized builder
    assert isinstance(builder, PythonTSLREFInputBuilder)
    assert builder._size == 3

def test_ref_tsb_builder_detection():
    """Verify that REF[TSB] creates specialized builder"""
    # Similar test for TSB

def test_ref_ts_builder_unchanged():
    """Verify that REF[TS] still uses generic builder"""
    # Should still use PythonREFInputBuilder
```

### Test 2: Pre-allocated Children

```python
def test_ref_tsl_children_preallocated():
    """Verify that REF[TSL] input has children pre-created"""
    from hgraph import graph, TS, TSL, REF, Size
    
    @graph
    def test_graph(ref_in: REF[TSL[TS[int], Size[3]]]):
        pass
    
    # Wire up and check
    instance = test_graph.wire_graph(...)
    ref_input = instance.inputs['ref_in']
    
    # Should have _items pre-allocated
    assert hasattr(ref_input, '_items')
    assert len(ref_input._items) == 3
    assert all(isinstance(item, TimeSeriesReferenceInput) for item in ref_input._items)
```

### Test 3: Type Checking

```python
def test_ref_tsl_size_mismatch_error():
    """Verify size mismatch gives good error"""
    # Try to bind REF[TSL[TS[int], Size[3]]] to TSL of size 5
    # Should get clear error message
```

---

## Implementation Checklist

### Minimal Implementation (Just Detection)
- [ ] Update `PythonTimeSeriesBuilderFactory.make_input_builder()` to call `_make_ref_input_builder()`
- [ ] Add `_make_ref_input_builder()` method with type inspection
- [ ] Update `PythonTimeSeriesBuilderFactory.make_output_builder()` to call `_make_ref_output_builder()`
- [ ] Add `_make_ref_output_builder()` method with type inspection
- [ ] For now, can return existing builders but log what was detected

### Python Implementation
- [ ] Create `PythonTSLREFInputBuilder` class
- [ ] Create `PythonTSBREFInputBuilder` class
- [ ] Create `PythonTSLREFOutputBuilder` class
- [ ] Create `PythonTSBREFOutputBuilder` class
- [ ] Create `PythonTimeSeriesListReferenceInput` class
- [ ] Create `PythonTimeSeriesBundleReferenceInput` class
- [ ] Create `PythonTimeSeriesListReferenceOutput` class
- [ ] Create `PythonTimeSeriesBundleReferenceOutput` class

### C++ Factory
- [ ] Update `HgCppFactory.make_input_builder()` to call `_make_ref_input_builder()`
- [ ] Add `_make_ref_input_builder()` method
- [ ] Update `HgCppFactory.make_output_builder()` to call `_make_ref_output_builder()`
- [ ] Add `_make_ref_output_builder()` method

### C++ Builders (Stubs Initially)
- [ ] Create `TimeSeriesListRefInputBuilder` header
- [ ] Create `TimeSeriesBundleRefInputBuilder` header
- [ ] Create `TimeSeriesListRefOutputBuilder` header
- [ ] Create `TimeSeriesBundleRefOutputBuilder` header
- [ ] Implement stubs that delegate to existing builders
- [ ] Register with nanobind

### Testing
- [ ] Test builder detection for REF[TSL]
- [ ] Test builder detection for REF[TSB]
- [ ] Test that REF[TS] still works
- [ ] Test pre-allocated children
- [ ] Test type checking and error messages

---

## Benefits of This Approach

### 1. Non-Breaking
- Existing REF[TS], REF[TSD], etc. continue to use same builders
- Only TSL/TSB get specialized treatment
- Python code doesn't change

### 2. Incremental
- Can implement detection first, then specialized classes
- C++ can start with stubs that delegate to existing code
- Each piece can be tested independently

### 3. Clear Signal
- At builder creation time, we know the structure
- Can make different decisions based on type
- Foundation for full specialization later

### 4. Better Errors
- Can validate size/schema at binding time
- Type mismatch errors are clearer
- Helps users understand what went wrong

---

## Next Steps After Detection

Once detection is working:

1. **Implement specialized Python classes** with pre-allocated children
2. **Add validation** in `bind_output()` methods
3. **Implement C++ specialized builders** that create proper types
4. **Add C++ specialized input/output classes** (can reuse most of current code)
5. **Extend to TSD** if needed (probably not - dynamic keys)

This focused approach gives us immediate benefits (detection, better errors) without requiring the full hierarchy refactoring!

