# Requirements Document

## Introduction

The Value/TSValue System Implementation provides a modern C++ architecture for hgraph's time-series data structures that separates data representation from temporal semantics. This system replaces the current one-to-one Python port with a type-erased, schema-driven approach optimized for buffer-friendly operations while maintaining full compatibility with existing hgraph functionality and Python interoperability.

## Glossary

- **Value**: Type-erased data container with schema-driven operations (TypeMeta/type_ops)
- **TSValue**: Time-series overlay providing hierarchical modification tracking and observability
- **TypeMeta**: Schema describing data type, structure, size, alignment, and operations for Value storage
- **TSMeta**: Schema describing time-series structure and temporal semantics for TSValue overlay
- **type_ops**: Operations vtable providing construct, destruct, copy, equals, hash, and Python interop
- **ts_ops**: Time-series operations vtable providing modification tracking, delta computation, and observer notification
- **View/TSView**: Non-owning, lightweight accessor into Value/TSValue with navigation capabilities
- **TSLink**: Symbolic link mechanism enabling partial peering and dynamic routing between time-series
- **DeltaValue**: Patch-like representation expressing incremental changes to collection data
- **Container_Hooks**: Integration points for set/map operations (insert, swap, erase) to maintain overlay alignment
- **Schema_Evolution**: Versioning and migration support for TypeMeta and TSMeta changes over time

## Requirements

### Requirement 1: Type-Erased Value System

**User Story:** As a C++ developer working with hgraph time-series, I want a type-erased Value system with schema-driven operations, so that I can work with heterogeneous data structures efficiently while maintaining type safety through runtime schema validation.

#### Acceptance Criteria

1. THE Value system SHALL provide type-erased storage for all scalar types (bool, int64_t, double, engine_time_t, engine_date_t, engine_time_delta_t, nb::object)
2. THE Value system SHALL support composite types (Bundle, Tuple, List, Set, Map) with arbitrary nesting
3. THE TypeMeta schema SHALL describe data layout, field structure, and operations for each type
4. THE type_ops vtable SHALL provide construct, destruct, copy, move, equals, hash, to_string, to_python, and from_python operations
5. THE Value system SHALL use contiguous memory layout where possible for cache-friendly access
6. THE Value system SHALL integrate with Python objects through nb::object type and conversion operations

### Requirement 2: Time-Series Overlay Architecture

**User Story:** As a system architect, I want TSValue overlays that track modification state and observers separately from data storage, so that I can optimize buffer operations while preserving time-series semantics and hierarchical change propagation.

#### Acceptance Criteria

1. THE TSValue overlay SHALL track modification timestamps, validity state, and observer subscriptions independently of Value data
2. THE TSValue overlay SHALL support hierarchical change propagation where child modifications update parent timestamps
3. THE TSValue overlay SHALL maintain observer notification mechanisms for linked inputs
4. THE ts_ops vtable SHALL provide modified(), valid(), all_valid(), last_modified_time(), and delta computation operations
5. THE TSValue system SHALL support all existing time-series types (TS, TSB, TSL, TSD, TSS, REF, SIGNAL)
6. THE TSValue overlay SHALL enable zero-copy bridging with underlying Value data

### Requirement 3: View-Based Access Patterns

**User Story:** As a node developer, I want lightweight View and TSView objects that provide type-safe access to data without copying, so that I can navigate complex data structures efficiently and maintain clear ownership semantics.

#### Acceptance Criteria

1. THE View system SHALL provide non-owning access to Value data with path tracking and owner references
2. THE View system SHALL support kind-specific views (BundleView, ListView, SetView, MapView) with appropriate APIs
3. THE TSView system SHALL provide time-bound views that answer modified(), valid(), and delta queries for specific engine times
4. THE View system SHALL support navigation patterns (at(), field(), contains()) without data copying
5. THE View system SHALL maintain type safety through runtime schema validation
6. THE View system SHALL provide efficient iteration over collections using ViewRange and ViewPairRange concepts

### Requirement 4: TSLink and Dynamic Binding

**User Story:** As a graph runtime developer, I want TSLink symbolic linking that supports partial peering and dynamic routing, so that I can implement complex binding patterns including REF-based dynamic routing and mixed local/linked hierarchies.

#### Acceptance Criteria

1. THE TSLink system SHALL support symbolic links between time-series inputs and outputs
2. THE TSLink system SHALL enable partial peering where some children are linked and others are local
3. THE TSLink system SHALL support dynamic rebinding for REF types with proper sample-time semantics
4. THE TSLink system SHALL handle active/passive subscription modes for observer notifications
5. THE TSLink system SHALL maintain binding state and provide unbinding capabilities
6. THE TSLink system SHALL integrate with the existing graph runtime notification mechanisms

### Requirement 5: Delta Computation and Change Tracking

**User Story:** As a reactive system developer, I want comprehensive delta computation that tracks what changed, how it changed, and when it changed, so that I can build efficient incremental processing algorithms.

#### Acceptance Criteria

1. THE DeltaValue system SHALL represent incremental changes for all collection types (List, Set, Map)
2. THE DeltaValue system SHALL support added, removed, and updated element tracking
3. THE TSValue system SHALL compute deltas from overlay state and container differences
4. THE delta system SHALL provide both C++ access (added(), removed(), modified_items()) and Python conversion (delta_to_python())
5. THE delta system SHALL handle hierarchical change propagation where child changes update parent deltas
6. THE delta system SHALL support delta application (apply_delta()) for both Value and TSValue layers

### Requirement 6: Container Hook Integration

**User Story:** As a performance engineer, I want container hook integration that maintains overlay alignment during set/map operations, so that modification tracking remains accurate during swap-with-last and other container optimizations.

#### Acceptance Criteria

1. THE container hook system SHALL integrate with set and map operations (insert, swap, erase)
2. THE container hook system SHALL maintain overlay array alignment with backing store indices
3. THE container hook system SHALL support swap-with-last optimization while preserving modification tracking
4. THE container hook system SHALL provide zero-cost integration when hooks are not needed
5. THE container hook system SHALL handle both Value-layer and TSValue-layer container operations
6. THE container hook system SHALL maintain consistency between data storage and overlay state

### Requirement 7: Python Integration and Interoperability

**User Story:** As an hgraph user, I want seamless Python integration that preserves existing APIs while leveraging the new C++ implementation, so that I can adopt the new system without changing application code.

#### Acceptance Criteria

1. THE system SHALL support all existing Python time-series APIs (TS, TSB, TSD, TSL, TSS, TSW, REF, SIGNAL)
2. THE system SHALL provide transparent conversion between Python objects and Value/TSValue representations
3. THE system SHALL support Python CompoundScalar and TimeSeriesSchema integration
4. THE system SHALL maintain compatibility with existing node signatures and graph construction
5. THE system SHALL provide Python access to delta information through delta_to_python() methods
6. THE system SHALL support both Python and C++ runtime implementations with feature parity

### Requirement 8: Schema Registration and Type System

**User Story:** As a library developer, I want extensible schema registration that supports both built-in and custom types, so that I can define new data structures while maintaining type safety and operation consistency.

#### Acceptance Criteria

1. THE TypeRegistry SHALL support registration of atomic, bundle, tuple, list, set, and map types
2. THE TypeRegistry SHALL provide both template-based and name-based registration APIs
3. THE TypeRegistry SHALL support Python type binding for CompoundScalar and TimeSeriesSchema classes
4. THE TSRegistry SHALL support registration of all time-series types with appropriate ts_ops
5. THE schema system SHALL support both static template definitions and runtime builder patterns
6. THE schema system SHALL provide schema introspection and validation capabilities