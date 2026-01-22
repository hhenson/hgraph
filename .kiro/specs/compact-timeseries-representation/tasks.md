# Implementation Plan: Compact Time-Series Representation

## Overview

This implementation plan breaks down the Value/TSValue system into discrete coding tasks that build incrementally from core type system foundations through complete time-series functionality. The approach prioritizes establishing the type-erased Value layer first, then adding the TSValue temporal overlay, and finally integrating with Python and the existing hgraph runtime.

## Tasks

- [ ] 1. Establish core type system infrastructure
  - [ ] 1.1 Implement TypeMeta schema system
    - Create TypeMeta class with kind, name, size, and operations
    - Implement field access for bundles (field_name, field_type, field_offset)
    - Implement container access for lists/sets/maps (element_type, key_type, value_type)
    - _Requirements: 1.3, 8.6_
  
  - [ ]* 1.2 Write property test for TypeMeta schema accuracy
    - **Property 2: Schema-Data Alignment**
    - **Validates: Requirements 1.3, 1.5**
  
  - [ ] 1.3 Implement TypeRegistry with registration and lookup
    - Create singleton TypeRegistry with name and type caches
    - Implement register_type() in three forms (template, template+ops, name+ops)
    - Implement get() methods for name-based and template-based lookup
    - _Requirements: 8.1, 8.2_
  
  - [ ]* 1.4 Write property test for schema registration consistency
    - **Property 15: Schema Registration Consistency**
    - **Validates: Requirements 8.1, 8.2, 8.4**

- [ ] 2. Implement type_ops architecture and atomic types
  - [ ] 2.1 Design and implement type_ops vtable structure
    - Create type_ops struct with common operations (construct, destroy, copy, move, equals, hash, to_string, to_python, from_python)
    - Implement tagged union for kind-specific operations (atomic_ops, bundle_ops, list_ops, set_ops, map_ops)
    - Create operation implementations for all atomic types (bool, int64_t, double, engine_time_t, engine_date_t, engine_time_delta_t, nb::object)
    - _Requirements: 1.1, 1.4_
  
  - [ ]* 2.2 Write property test for atomic type operations
    - **Property 1: Type System Round-Trip Consistency**
    - **Validates: Requirements 1.1, 1.2, 1.3, 1.4, 1.6**
  
  - [ ] 2.3 Implement iterator concepts (ViewRange and ViewPairRange)
    - Create ViewRange for single-value iteration
    - Create ViewPairRange for key-value pair iteration
    - Implement iterator protocols with proper C++ iterator semantics
    - _Requirements: 3.6_

- [ ] 3. Implement Value storage and View access system
  - [ ] 3.1 Create Value class with type-erased storage
    - Implement Value constructor with schema and optional Python object
    - Implement memory management with proper RAII for type-erased data
    - Add copy, move, and assignment operations using type_ops
    - _Requirements: 1.2, 1.5_
  
  - [ ] 3.2 Implement View system with path tracking
    - Create View class with owner reference and path tracking
    - Implement navigation methods (at() by index and name)
    - Add type-safe extraction methods (as<T>(), data<T>())
    - Implement Path and PathElement classes for navigation tracking
    - _Requirements: 3.1, 3.4, 3.5_
  
  - [ ]* 3.3 Write property test for view navigation consistency
    - **Property 6: View Navigation Consistency**
    - **Validates: Requirements 3.1, 3.4, 3.5**
  
  - [ ] 3.4 Implement specialized view types (BundleView, ListView, SetView, MapView)
    - Create kind-specific view classes with appropriate APIs
    - Implement field access for BundleView
    - Implement indexed access and modification for ListView
    - Implement membership and modification for SetView and MapView
    - _Requirements: 3.2_

- [ ] 4. Checkpoint - Ensure Value layer tests pass
  - Ensure all tests pass, ask the user if questions arise.

- [ ] 5. Implement composite type operations
  - [ ] 5.1 Implement bundle operations (bundle_ops)
    - Create field access operations (field_at_index, field_at_name)
    - Implement field iteration (items() returning ViewPairRange)
    - Add field count and name lookup operations
    - _Requirements: 1.2_
  
  - [ ] 5.2 Implement list operations (list_ops)
    - Create indexed access operations (at, append, clear, size)
    - Implement iteration operations (values, items)
    - Add bounds checking and dynamic resizing for dynamic lists
    - _Requirements: 1.2_
  
  - [ ] 5.3 Implement set and map operations (set_ops, map_ops)
    - Create membership operations (contains, add, remove for sets)
    - Create key-value operations (at, set_item, remove for maps)
    - Implement iteration operations (keys, values, items)
    - Add size and clear operations for both types
    - _Requirements: 1.2_
  
  - [ ]* 5.4 Write property test for composite type round-trip
    - **Property 1: Type System Round-Trip Consistency** (composite types)
    - **Validates: Requirements 1.1, 1.2, 1.3, 1.4, 1.6**

- [ ] 6. Implement DeltaValue system for change tracking
  - [ ] 6.1 Create DeltaValue and DeltaView classes
    - Implement DeltaValue with schema-specific delta storage
    - Create DeltaView for non-owning access to delta data
    - Add delta application methods (apply_to for Value)
    - _Requirements: 5.1, 5.2_
  
  - [ ] 6.2 Implement collection-specific delta operations
    - Create SetDeltaValue with added/removed tracking
    - Create MapDeltaValue with added/updated/removed tracking
    - Create ListDeltaValue with index-based update tracking
    - _Requirements: 5.1, 5.2_
  
  - [ ]* 6.3 Write property test for delta representation completeness
    - **Property 11: Delta Representation Completeness**
    - **Validates: Requirements 5.1, 5.2, 5.3, 5.6**

- [ ] 7. Implement TSMeta and TSRegistry for time-series schemas
  - [ ] 7.1 Create TSMeta class for time-series schema
    - Implement TSMeta with TSKind and reference to TypeMeta
    - Add field access for TSB (field_ts_meta by name and index)
    - Add container access for TSL/TSD/TSS (element_ts_meta, key_type, value_ts_meta)
    - _Requirements: 2.4, 2.5_
  
  - [ ] 7.2 Implement TSRegistry for time-series type registration
    - Create TSRegistry singleton with registration and lookup
    - Support registration of all time-series types (TS, TSB, TSL, TSD, TSS, REF, SIGNAL)
    - Add Python type binding support for CompoundScalar and TimeSeriesSchema
    - _Requirements: 8.4, 8.3_

- [ ] 8. Implement ts_ops architecture for time-series operations
  - [ ] 8.1 Design and implement ts_ops vtable structure
    - Create ts_ops struct with common operations (construct, destroy, copy, modified, valid, all_valid, last_modified_time, set_modified_time)
    - Add value access operations (value, set_value, apply_delta)
    - Add Python interop operations (to_python, delta_to_python, delta)
    - Implement tagged union for kind-specific operations
    - _Requirements: 2.4_
  
  - [ ] 8.2 Implement time-series specific operations for each kind
    - Create ts_scalar_ops (minimal - inherits common operations)
    - Create tsb_ops with field access and iteration
    - Create tsl_ops with element access and iteration
    - Create tsd_ops with key-value access and key set operations
    - Create tss_ops with membership and delta operations (added, removed)
    - Create ref_ops and signal_ops
    - _Requirements: 2.5_

- [ ] 9. Implement TSValue storage with overlay architecture
  - [ ] 9.1 Create TSValue class with dual-layer storage
    - Implement TSValue with Value storage and separate overlay data
    - Add timestamp tracking and observer list management
    - Implement modification time propagation for hierarchical structures
    - _Requirements: 2.1, 2.2_
  
  - [ ]* 9.2 Write property test for TSValue overlay independence
    - **Property 3: TSValue Overlay Independence**
    - **Validates: Requirements 2.1, 2.6**
  
  - [ ] 9.3 Implement observer notification system
    - Create observer subscription/unsubscription mechanisms
    - Implement notification dispatch to active observers
    - Add support for active/passive observer modes
    - _Requirements: 2.3_
  
  - [ ]* 9.4 Write property test for observer notification correctness
    - **Property 5: Observer Notification Correctness**
    - **Validates: Requirements 2.3, 4.4, 4.6**

- [ ] 10. Implement TSView system with time-bound access
  - [ ] 10.1 Create TSView class with time binding
    - Implement TSView with bound engine time and owner reference
    - Add state query methods (modified, valid, all_valid at bound time)
    - Implement value access and navigation (field, operator[])
    - _Requirements: 3.3_
  
  - [ ]* 10.2 Write property test for time-bound view correctness
    - **Property 7: Time-Bound View Correctness**
    - **Validates: Requirements 3.3**
  
  - [ ] 10.3 Implement delta access through TSView
    - Add delta() method returning DeltaView for bound time
    - Implement delta_to_python() for Python access
    - Add iteration methods for modified elements (modified_values, modified_items)
    - _Requirements: 5.4, 5.5_

- [ ] 11. Checkpoint - Ensure TSValue layer tests pass
  - Ensure all tests pass, ask the user if questions arise.

- [ ] 12. Implement TSLink system for dynamic binding
  - [ ] 12.1 Create TSLink class for symbolic linking
    - Implement TSLink with binding state management (Unbound, Bound, Active, Passive)
    - Add bind/unbind operations with target TSValue
    - Implement value access delegation to target
    - _Requirements: 4.1, 4.5_
  
  - [ ]* 12.2 Write property test for TSLink binding consistency
    - **Property 8: TSLink Binding Consistency**
    - **Validates: Requirements 4.1, 4.5**
  
  - [ ] 12.3 Implement partial peering support
    - Add support for mixed local/linked hierarchies in composite time-series
    - Implement independent peering state for each child element
    - _Requirements: 4.2_
  
  - [ ]* 12.4 Write property test for partial peering correctness
    - **Property 9: Partial Peering Correctness**
    - **Validates: Requirements 4.2**

- [ ] 13. Implement REF type with dynamic rebinding
  - [ ] 13.1 Create TimeSeriesReference class
    - Implement TimeSeriesReference as value type for REF
    - Add target access and validity checking
    - Implement equality and hashing for container use
    - _Requirements: 4.3_
  
  - [ ] 13.2 Implement REF binding semantics
    - Add REF → TS binding with dynamic link creation
    - Add TS → REF binding with reference creation
    - Implement rebinding with proper sample-time semantics
    - _Requirements: 4.3_
  
  - [ ]* 13.3 Write property test for REF dynamic rebinding
    - **Property 10: REF Dynamic Rebinding**
    - **Validates: Requirements 4.3**

- [ ] 14. Implement container hooks for overlay alignment
  - [ ] 14.1 Create ContainerHooks system
    - Implement hook registration for insert, swap, and erase operations
    - Add hook invocation during container operations
    - Support zero-cost integration when hooks are disabled
    - _Requirements: 6.1, 6.4_
  
  - [ ] 14.2 Integrate hooks with set and map implementations
    - Modify set operations to call hooks during insert/erase
    - Implement swap-with-last optimization with hook support
    - Ensure overlay arrays stay aligned with backing store
    - _Requirements: 6.2, 6.3_
  
  - [ ]* 14.3 Write property test for container hook alignment
    - **Property 12: Container Hook Alignment**
    - **Validates: Requirements 6.1, 6.2, 6.3, 6.6**

- [ ] 15. Implement Python integration layer
  - [ ] 15.1 Add Python object conversion support
    - Implement to_python() and from_python() for all Value types
    - Add support for CompoundScalar and TimeSeriesSchema binding
    - Implement transparent conversion between Python and C++ representations
    - _Requirements: 7.2, 7.3_
  
  - [ ]* 15.2 Write property test for Python interop round-trip
    - **Property 13: Python Interop Round-Trip**
    - **Validates: Requirements 7.2, 7.5**
  
  - [ ] 15.3 Implement Python time-series API compatibility
    - Create Python wrappers for all time-series types (TS, TSB, TSL, TSD, TSS, TSW, REF, SIGNAL)
    - Ensure API compatibility with existing Python implementations
    - Add delta_to_python() methods for all time-series types
    - _Requirements: 7.1, 7.4, 7.5_
  
  - [ ]* 15.4 Write property test for Python API compatibility
    - **Property 14: Python API Compatibility**
    - **Validates: Requirements 7.1, 7.4, 7.6**

- [ ] 16. Implement hierarchical change propagation
  - [ ] 16.1 Add change propagation logic to composite time-series
    - Implement upward timestamp propagation when children are modified
    - Add delta computation that includes child changes in parent deltas
    - Ensure all_valid() correctly checks all descendants
    - _Requirements: 2.2, 5.5_
  
  - [ ]* 16.2 Write property test for hierarchical change propagation
    - **Property 4: Hierarchical Change Propagation**
    - **Validates: Requirements 2.2, 5.5**

- [ ] 17. Integration and runtime wiring
  - [ ] 17.1 Integrate with existing hgraph runtime
    - Connect TSValue system with graph node evaluation
    - Implement compatibility with existing node signatures
    - Add feature flag support for gradual rollout
    - _Requirements: 7.4, 4.6_
  
  - [ ] 17.2 Add schema introspection and validation
    - Implement schema validation methods for TypeMeta and TSMeta
    - Add introspection capabilities for runtime schema discovery
    - Create validation utilities for schema compliance checking
    - _Requirements: 8.6_
  
  - [ ]* 17.3 Write property test for schema introspection accuracy
    - **Property 16: Schema Introspection Accuracy**
    - **Validates: Requirements 8.6**

- [ ] 18. Final checkpoint - Ensure all tests pass
  - Ensure all tests pass, ask the user if questions arise.

## Notes

- Tasks marked with `*` are optional property-based tests that can be skipped for faster MVP
- Each task references specific requirements for traceability
- Checkpoints ensure incremental validation throughout development
- Property tests validate universal correctness properties across all inputs
- Unit tests validate specific examples and edge cases
- The implementation builds incrementally from Value layer through TSValue layer to full integration