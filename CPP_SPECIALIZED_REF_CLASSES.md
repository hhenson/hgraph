# C++ Specialized Reference Classes - Implementation Complete

## Summary

Created C++ parallel implementations for all specialized reference types, matching the Python structure.

## Files Created

### 1. Header File (Declarations)
**`cpp/include/hgraph/builders/time_series_types/specialized_ref_builders.h`**
- 12 specialized builder class declarations (6 input + 6 output)
- All inherit from `InputBuilder` or `OutputBuilder`
- Mirror Python builder structure

### 2. Implementation File
**`cpp/src/cpp/builders/time_series_types/specialized_ref_builders.cpp`**
- Complete implementations for all 12 builders
- `make_instance()` methods create appropriate specialized types
- `register_with_nanobind()` methods for Python binding

## Files Modified

### 1. Type Declarations
**`cpp/include/hgraph/types/ref.h`**
- Added 6 specialized reference input classes
- Added 6 specialized reference output classes
- All inherit from `TimeSeriesReferenceInput` or `TimeSeriesReferenceOutput`

### 2. Type Implementations
**`cpp/src/cpp/types/ref.cpp`**
- Added `register_with_nanobind()` implementations for all 12 specialized classes
- Simple registrations using nanobind class binding

### 3. Type Registration
**`cpp/src/cpp/python/_hgraph_types.cpp`**
- Added registrations for all 12 specialized reference classes
- Called after generic reference registrations

### 4. Builder Registration - Input
**`cpp/src/cpp/builders/input_builder.cpp`**
- Included specialized_ref_builders.h
- Added registration calls for 6 specialized input builders

### 5. Builder Registration - Output
**`cpp/src/cpp/builders/output_builder.cpp`**
- Included specialized_ref_builders.h
- Added registration calls for 6 specialized output builders

### 6. Build Configuration
**`cpp/src/cpp/CMakeLists.txt`**
- Added specialized_ref_builders.cpp to build sources

### 7. Python Factory
**`hgraph/_use_cpp_runtime.py`**
- Updated `_make_ref_input_builder` with dictionary dispatch for all 6 types
- Updated `_make_ref_output_builder` with dictionary dispatch for all 6 types
- Uses specialized C++ builders: `InputBuilder_TS_Value_Ref`, `InputBuilder_TSL_Ref`, etc.

## C++ Class Hierarchy

### Input Classes
```
TimeSeriesReferenceInput (generic)
├── TimeSeriesValueReferenceInput     (REF[TS])
├── TimeSeriesListReferenceInput      (REF[TSL])
├── TimeSeriesBundleReferenceInput    (REF[TSB])
├── TimeSeriesDictReferenceInput      (REF[TSD])
├── TimeSeriesSetReferenceInput       (REF[TSS])
└── TimeSeriesWindowReferenceInput    (REF[TSW])
```

### Output Classes
```
TimeSeriesReferenceOutput (generic)
├── TimeSeriesValueReferenceOutput    (REF[TS])
├── TimeSeriesListReferenceOutput     (REF[TSL])
├── TimeSeriesBundleReferenceOutput   (REF[TSB])
├── TimeSeriesDictReferenceOutput     (REF[TSD])
├── TimeSeriesSetReferenceOutput      (REF[TSS])
└── TimeSeriesWindowReferenceOutput   (REF[TSW])
```

## Builder Classes

### Input Builders
- `TimeSeriesValueRefInputBuilder`    → Creates `TimeSeriesValueReferenceInput`
- `TimeSeriesListRefInputBuilder`     → Creates `TimeSeriesListReferenceInput`
- `TimeSeriesBundleRefInputBuilder`   → Creates `TimeSeriesBundleReferenceInput`
- `TimeSeriesDictRefInputBuilder`     → Creates `TimeSeriesDictReferenceInput`
- `TimeSeriesSetRefInputBuilder`      → Creates `TimeSeriesSetReferenceInput`
- `TimeSeriesWindowRefInputBuilder`   → Creates `TimeSeriesWindowReferenceInput`

### Output Builders
- `TimeSeriesValueRefOutputBuilder`   → Creates `TimeSeriesValueReferenceOutput`
- `TimeSeriesListRefOutputBuilder`    → Creates `TimeSeriesListReferenceOutput`
- `TimeSeriesBundleRefOutputBuilder`  → Creates `TimeSeriesBundleReferenceOutput`
- `TimeSeriesDictRefOutputBuilder`    → Creates `TimeSeriesDictReferenceOutput`
- `TimeSeriesSetRefOutputBuilder`     → Creates `TimeSeriesSetReferenceOutput`
- `TimeSeriesWindowRefOutputBuilder`  → Creates `TimeSeriesWindowReferenceOutput`

## Nanobind Registration

All classes are exposed to Python with appropriate names:
- Input types: `TimeSeriesValueReferenceInput`, etc.
- Output types: `TimeSeriesValueReferenceOutput`, etc.
- Input builders: `InputBuilder_TS_Value_Ref`, `InputBuilder_TSL_Ref`, etc.
- Output builders: `OutputBuilder_TS_Value_Ref`, `OutputBuilder_TSL_Ref`, etc.

## Python Factory Integration

Updated `HgCppFactory` in `_use_cpp_runtime.py`:

```python
def _make_ref_input_builder(self, ref_tp):
    referenced_tp = ref_tp.value_tp
    return {
        HgTSTypeMetaData: lambda: _hgraph.InputBuilder_TS_Value_Ref(),
        HgTSLTypeMetaData: lambda: _hgraph.InputBuilder_TSL_Ref(),
        HgTSBTypeMetaData: lambda: _hgraph.InputBuilder_TSB_Ref(),
        HgTSDTypeMetaData: lambda: _hgraph.InputBuilder_TSD_Ref(),
        HgTSSTypeMetaData: lambda: _hgraph.InputBuilder_TSS_Ref(),
        HgTSWTypeMetaData: lambda: _hgraph.InputBuilder_TSW_Ref(),
    }.get(type(referenced_tp), lambda: _hgraph.InputBuilder_TS_Ref())()
```

(Same pattern for output builders)

## Implementation Details

### Marker Classes
All specialized classes are currently simple marker classes that inherit all behavior from generic:
- Use `using TimeSeriesReferenceInput::TimeSeriesReferenceInput;` for constructor inheritance
- Only override `register_with_nanobind()` for type exposure
- Ready for future type-specific enhancements

### TimeSeriesListReferenceInput Enhancement
Added `get_input(size_t index)` override:
- Currently delegates to parent
- Placeholder for future batch creation logic

## Next Steps

1. **Build**: Run cmake build to compile new C++ code
2. **Test**: Run Python tests with `HGRAPH_USE_CPP=1`
3. **Enhance**: Add batch creation logic for TSL if needed
4. **Optimize**: Add type-specific optimizations as needed

## Lines Added

- **ref.h**: ~85 lines (12 class declarations)
- **ref.cpp**: ~90 lines (12 registrations)
- **specialized_ref_builders.h**: ~135 lines (12 builder declarations)
- **specialized_ref_builders.cpp**: ~190 lines (12 builder implementations)
- **Other files**: ~30 lines (includes, registrations, factory updates)

**Total**: ~530 lines of C++ code matching Python structure

## Status

✅ C++ code structure complete and matches Python
✅ All files created and modified
✅ Build configuration updated
⏳ Needs compilation and testing with HGRAPH_USE_CPP=1

