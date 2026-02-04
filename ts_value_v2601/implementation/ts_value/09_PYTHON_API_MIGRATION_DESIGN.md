# Revised Python API Wrapper Migration Design Document

## TSView-Based Python Wrapper Architecture

**Version**: 2.1 (Corrected)
**Date**: 2026-02-03
**Status**: Implementation In Progress

> **IMPORTANT CORRECTION**: This document describes **replacing** `_impl` with `view_` in existing wrappers, NOT creating new wrapper classes or maintaining dual storage permanently.

---

## 1. Executive Summary

This document describes the migration strategy for Python API wrappers in hgraph from the old `TimeSeriesInput/TimeSeriesOutput` class hierarchy to the new TSView-based architecture. The goal is to preserve the exact Python API while leveraging the new, more efficient TSView infrastructure.

**Key Revisions in v2.0:**
- Fixed TSInputView constructor signature to match design docs
- Clarified TSInputView navigation logic for link-following
- Added complete REF alternative link establishment logic with nested conversion
- Added all missing Python API method mappings
- Added observer management connection details
- Added lifecycle sequence diagrams
- Added edge case handling for circular refs, partial binding, TSW alternatives

---

## 2. Architecture Mapping

### 2.1 Old Architecture Overview

The current implementation uses a class hierarchy:

```
TimeSeriesType (abstract base)
├── TimeSeriesInput
│   ├── TimeSeriesBundleInput
│   ├── TimeSeriesListInput
│   ├── TimeSeriesDictInputImpl
│   ├── TimeSeriesSetInput
│   ├── TimeSeriesWindowInput
│   └── TimeSeriesReferenceInput (and specializations)
└── TimeSeriesOutput
    ├── TimeSeriesBundleOutput
    ├── TimeSeriesListOutput
    ├── TimeSeriesDictOutputImpl
    ├── TimeSeriesSetOutput
    ├── TimeSeriesWindowOutput
    └── TimeSeriesReferenceOutput (and specializations)
```

Python wrappers hold `ApiPtr<TimeSeriesType>` (aliased shared_ptr) to the C++ implementation:

```cpp
struct PyTimeSeriesType {
    ApiPtr<TimeSeriesType> _impl;  // shared_ptr with control block
};
```

### 2.2 New Architecture Overview

The new architecture introduces:

```
TSValue (owns data: value_, time_, observer_, delta_value_, link_)
    │
TSView (ViewData + current_time)
    │
├── TSOutputView (wraps TSView + TSOutput* context)
└── TSInputView (wraps TSView + TSInput* context)
```

TSOutput and TSInput are stored in Node as value members, not heap-allocated via shared_ptr.

### 2.3 Class Mapping Table

| Old Class | New Backing | Notes |
|-----------|-------------|-------|
| PyTimeSeriesType | TSView | Base queries (modified, valid, value) |
| PyTimeSeriesOutput | TSOutputView | Mutation + observer management |
| PyTimeSeriesInput | TSInputView | Binding + subscription control |
| PyTimeSeriesBundleOutput | TSBView via TSOutputView | Field navigation |
| PyTimeSeriesBundleInput | TSBView via TSInputView | Field navigation |
| PyTimeSeriesListOutput | TSLView via TSOutputView | Index navigation |
| PyTimeSeriesListInput | TSLView via TSInputView | Index navigation |
| PyTimeSeriesDictOutput | TSDView via TSOutputView | Key/delta tracking |
| PyTimeSeriesDictInput | TSDView via TSInputView | Key/delta tracking |
| PyTimeSeriesSetOutput | TSSView via TSOutputView | Set delta tracking |
| PyTimeSeriesSetInput | TSSView via TSInputView | Set delta tracking |
| PyTimeSeriesWindowOutput | TSWView via TSOutputView | Window semantics |
| PyTimeSeriesWindowInput | TSWView via TSInputView | Window semantics |
| PyTimeSeriesReferenceOutput | TSView (REF kind) | REF value handling |
| PyTimeSeriesReferenceInput | TSView (REF kind) + REFLink | Dereferencing |

### 2.4 Lifetime Management Changes

**Old Approach (being removed):**
```cpp
// ApiPtr holds shared_ptr to heap-allocated TimeSeriesType
ApiPtr<TimeSeriesOutput> _impl;
// Control block manages lifetime
```

**Current State (transitional):**
```cpp
// Both old and new storage present during migration
struct PyTimeSeriesOutput {
    api_ptr _impl;                           // LEGACY - TO BE REMOVED
    std::optional<TSOutputView> output_view_; // NEW - REPLACEMENT
};
```

**Target State (end goal):**
```cpp
// TSOutput/TSInput are value members in Node
struct Node {
    std::vector<TSOutput> outputs_;
    std::vector<TSInput> inputs_;
};

// Python wrappers hold only view-based storage:
struct PyTimeSeriesOutput {
    TSOutputView output_view_;    // View into output (includes node lifetime)
};
```

---

## 3. TSInputView Navigation and Link Following

### 3.1 Constructor Signature Alignment

**Per design/05_TSOUTPUT_TSINPUT.md, TSInputView constructor:**

```cpp
// CORRECT signature from ts_input_view.h:
TSInputView(TSView ts_view, TSInput* input) noexcept
    : ts_view_(std::move(ts_view))
    , input_(input)
    , bound_output_(nullptr) {}
```

The `bound_output_` member is set via `set_bound_output()` during binding, not at construction.

### 3.2 Navigation Logic for Link-Following

**Key Insight**: TSInputView navigation must handle two cases:
1. **Unbound position**: Navigate within local TSValue structure
2. **Bound position**: Follow link to navigate within target's structure

```cpp
TSInputView TSInputView::field(const std::string& name) const {
    // Check if this position is bound (linked)
    if (ts_view_.is_bound()) {
        // Navigate within the LINKED TARGET's structure
        // The link's ViewData points to target, so navigation follows target
        TSView child_view = ts_view_.field(name);  // Navigates target
        return TSInputView(child_view, input_);
    } else {
        // Navigate within local structure
        TSView child_view = ts_view_.field(name);
        return TSInputView(child_view, input_);
    }
}

TSInputView TSInputView::operator[](size_t index) const {
    // Same logic - check bound state, then navigate
    TSView child_view = ts_view_[index];
    return TSInputView(child_view, input_);
}
```

**Important**: When TSView::is_bound() returns true, the TSView's ViewData already points to the target's data (via LinkTarget). Navigation via `ts_view_.field()` or `ts_view_[index]` automatically follows the link because ViewData contains target pointers.

### 3.3 Link Navigation Sequence Diagram

```
[User calls] input_view.field("price")
      │
      ▼
[Check ts_view_.is_bound()]
      │
      ├── false (unbound)
      │       │
      │       ▼
      │   [Navigate local TSValue]
      │   ts_view_.field("price")
      │       │
      │       ▼
      │   [Return TSInputView with local child view]
      │
      └── true (bound)
              │
              ▼
          [ViewData points to linked target]
              │
              ▼
          [ts_view_.field("price") navigates target structure]
              │
              ▼
          [Return TSInputView with target child view]
```

### 3.4 Active State Hierarchy for TSB

For un-peered bundles, active state is per-field:

```cpp
// TSInput::active_ mirrors the TS schema structure
// For TSB[a: TS[int], b: TS[float]]:
// active_ is a fixed_list[bool, 2] - one per field

bool TSInputView::active() const {
    // Navigate active_ to this position
    value::View active_view = input_->active_view();
    // Use same path indices to access active state at this position
    for (size_t idx : ts_view_.short_path().indices()) {
        active_view = active_view[idx];
    }
    return active_view.as<bool>();
}
```

### 3.5 Link Organization in TSInput.value_

For an un-peered TSB input:

```
TSInput.value_ for TSB[a: TS[int], b: TS[float]]:
├── value_ (compound structure):
│   ├── field_a_link: LinkTarget {is_linked, value_data, time_data, ...}
│   └── field_b_link: LinkTarget {is_linked, value_data, time_data, ...}
└── active_ (mirrors structure):
    ├── field_a_active: bool
    └── field_b_active: bool

Navigation to get field link:
1. TSInputView::field("a")
2. Calls ts_view_.field("a") which computes:
   - offset = meta.field_offset("a")
   - child_data = base_data + offset
   - Returns new ViewData pointing to field_a_link
3. child_view.is_bound() checks if field_a_link.is_linked
```

---

## 4. Node Integration

### 4.1 Storage Model

TSInput and TSOutput are stored directly in Node as vectors:

```cpp
class Node {
    std::vector<TSInput> inputs_;
    std::vector<TSOutput> outputs_;
    // ... other members
};
```

This ownership model means:
- Node destruction destroys all its TSInput/TSOutput instances
- No reference counting overhead for time-series access
- Views must capture Node lifetime to prevent dangling access

### 4.2 Obtaining Views from Node

Python wrappers obtain views by:

1. Capturing `shared_ptr<Node>` for lifetime management
2. Calling `input.view(current_time)` or `output.view(current_time)`
3. The view captures the current engine time for modification checks

```cpp
// Factory function example
nb::object wrap_output(std::shared_ptr<Node> node, size_t port_index) {
    TSOutput& output = node->outputs_[port_index];
    engine_time_t current_time = node->graph()->current_time();
    TSOutputView view = output.view(current_time);

    // Create wrapper based on TSMeta kind
    return create_wrapper_for_kind(node, view);
}
```

### 4.3 Binding and Subscription Management

**Binding (TSInput to TSOutput):**
```cpp
void TSInputView::bind(TSOutputView& output) {
    // 1. Populate link data in TSView's ViewData
    // The link_data in ViewData points to LinkTarget in TSValue::link_
    LinkTarget* link = static_cast<LinkTarget*>(ts_view_.view_data().link_data);
    link->is_linked = true;
    link->value_data = output.view_data().value_data;
    link->time_data = output.view_data().time_data;
    link->observer_data = output.view_data().observer_data;
    link->delta_data = output.view_data().delta_data;
    link->link_data = output.view_data().link_data;
    link->ops = output.view_data().ops;
    link->meta = output.view_data().meta;

    // 2. Store output reference for subscription management
    bound_output_ = output.output();

    // 3. Subscribe if active
    if (active()) {
        output.subscribe(input_);
    }
}
```

**Subscription:**
```cpp
void TSInputView::make_active() {
    // Update active state
    value::View active_view = get_active_at_position();
    active_view.set(true);

    // Subscribe to bound output if any
    if (bound_output_ != nullptr) {
        TSOutputView output_view = bound_output_->view(current_time());
        output_view.subscribe(input_);
    }
}
```

---

## 5. Observer Management Connection

### 5.1 Observer Storage in TSValue

TSValue has an `observer_` Value that stores ObserverList structures:

```cpp
// TSValue members
value::Value<> observer_;   // Schema from generate_observer_schema(meta_)

// ObserverList structure
class ObserverList {
    std::vector<Notifiable*> observers_;
public:
    void add(Notifiable* obs);
    void remove(Notifiable* obs);
    void notify_all(engine_time_t et);
};
```

### 5.2 Observer Schema Generation

```cpp
// For TS[int]: observer_ is ObserverList
// For TSB[a: TS[int], b: TS[float]]: observer_ is tuple<ObserverList, ObserverList>
// For TSL[TS[int], SIZE[3]]: observer_ is fixed_list<ObserverList, 3>

const TypeMeta* generate_observer_schema(const TSMeta* ts_meta) {
    switch (ts_meta->kind()) {
        case TSKind::TS:
        case TSKind::TSS:
        case TSKind::TSW:
        case TSKind::REF:
        case TSKind::SIGNAL:
            return TypeMeta::observer_list();

        case TSKind::TSB: {
            std::vector<const TypeMeta*> field_schemas;
            for (size_t i = 0; i < ts_meta->field_count(); ++i) {
                field_schemas.push_back(
                    generate_observer_schema(ts_meta->field_meta(i)));
            }
            return TypeMeta::tuple(field_schemas);
        }

        case TSKind::TSL: {
            const TypeMeta* elem_schema =
                generate_observer_schema(ts_meta->element_meta());
            return TypeMeta::fixed_list(elem_schema, ts_meta->size());
        }

        case TSKind::TSD:
            // Dict has container-level observers + per-slot observers
            return TypeMeta::dict_observer(
                ts_meta->key_type(),
                generate_observer_schema(ts_meta->element_meta()));
    }
}
```

### 5.3 Subscribe/Unsubscribe Flow

```cpp
void TSOutputView::subscribe(Notifiable* observer) {
    // Get observer data from ViewData
    void* obs_data = ts_view_.view_data().observer_data;

    // For scalar types, observer_data is ObserverList*
    // For containers, it's a tuple with container-level + per-child observers
    if (ts_meta()->kind() == TSKind::TS ||
        ts_meta()->kind() == TSKind::TSS ||
        ts_meta()->kind() == TSKind::TSW ||
        ts_meta()->kind() == TSKind::REF ||
        ts_meta()->kind() == TSKind::SIGNAL) {
        // Atomic types - single ObserverList
        ObserverList* obs_list = static_cast<ObserverList*>(obs_data);
        obs_list->add(observer);
    } else {
        // Container types - tuple with [container_observers, children...]
        auto* tuple = static_cast<ObserverTuple*>(obs_data);
        tuple->container_observers.add(observer);
    }
}

void TSOutputView::unsubscribe(Notifiable* observer) {
    // Same logic, but call remove() instead of add()
    // ...
}
```

### 5.4 Notification Trigger (on set_value)

```cpp
void TSView::set_value(const value::View& src) {
    // 1. Copy value data
    ts_meta()->value_type()->ops->copy_assign(
        view_data_.value_data, src.data(), ts_meta()->value_type());

    // 2. Update modification time
    engine_time_t* time_ptr = static_cast<engine_time_t*>(view_data_.time_data);
    *time_ptr = current_time_;

    // 3. Notify observers
    notify_observers();
}

void TSView::notify_observers() {
    void* obs_data = view_data_.observer_data;
    if (obs_data == nullptr) return;

    // Get appropriate ObserverList and notify
    if (ts_meta()->kind() == TSKind::TS /* ... */) {
        ObserverList* obs_list = static_cast<ObserverList*>(obs_data);
        obs_list->notify_all(current_time_);
    } else {
        auto* tuple = static_cast<ObserverTuple*>(obs_data);
        tuple->container_observers.notify_all(current_time_);
    }
}
```

### 5.5 Notification Propagation Rules

When a child field is modified:
1. Child's observers are notified
2. Parent's observers are NOT automatically notified
3. If parent notification is needed, the parent must subscribe to all children

Rationale: Allows fine-grained subscription without redundant notifications.

### 5.6 Observer Notification Lifecycle Diagram

```
[Output.set_value(v)]
      │
      ▼
[Copy value to value_data]
      │
      ▼
[Update time_data = current_time]
      │
      ▼
[Get ObserverList from observer_data]
      │
      ▼
[For each observer in list]
      │
      ▼
[observer->notify(current_time)]
      │
      ▼
[TSInput::notify() called]
      │
      ▼
[Schedule owning_node for execution]
```

---

## 6. REF Alternative Link Establishment

### 6.1 Position-by-Position Logic Table

| Native Type | Target Type | Alternative Contains | Subscription |
|-------------|-------------|---------------------|--------------|
| TS[T] | TS[T] | Link -> native | Via Link |
| REF[TS[T]] | REF[TS[T]] | Link -> native | Via Link |
| REF[TS[T]] | TS[T] | REFLink | REF source + current target |
| REF[X] | Y (where X != Y) | REFLink + nested conversion | REF source + recursive |
| TS[T] | REF[TS[T]] | TSReference value | None (value is static) |
| TSD/TSL | TSD/TSL | Per-element links | Native structure changes |
| TSB | TSB | Per-field links | Per-field as above |
| TSW | TSW | Per-element links | Window structure native |

### 6.2 Nested REF Conversion (REF[TSD[K,TS[V]]] -> TSD[K,REF[TS[V]]])

This is the complex case where a REF to a container needs element-level wrapping:

```cpp
void TSOutput::establish_links_recursive(
    TSValue& alt,
    TSView alt_view,
    TSView native_view,
    const TSMeta* target_meta,
    const TSMeta* native_meta
) {
    // Case: REF on native side with nested conversion needed
    if (native_meta->kind() == TSKind::REF) {
        const TSMeta* deref_meta = native_meta->referenced_meta();

        if (target_meta == deref_meta) {
            // Simple REF[X] -> X: Create REFLink for direct dereference
            REFLink* ref_link = create_ref_link(alt_view, native_view, current_time_);
            ref_links_.push_back(std::unique_ptr<REFLink>(ref_link));
        } else {
            // Complex: REF[TSD[K,TS[V]]] -> TSD[K,REF[TS[V]]]
            //
            // Strategy:
            // 1. Create REFLink to dereference outer REF
            // 2. REFLink stores a callback for nested conversion
            // 3. When REF changes, rebind triggers nested conversion on new target

            REFLink* ref_link = create_ref_link_with_conversion(
                alt_view, native_view, target_meta, deref_meta);
            ref_link->set_conversion_callback([this, target_meta, deref_meta](
                TSView linked_target_view) {
                // linked_target_view is the dereferenced TSD[K,TS[V]]
                // Now establish element-level TS -> REF wrapping
                establish_element_wrapping(
                    linked_target_view, target_meta, deref_meta);
            });
            ref_links_.push_back(std::unique_ptr<REFLink>(ref_link));
        }
        return;
    }

    // Case: TS -> REF (wrapping)
    if (native_meta->kind() == TSKind::TS && target_meta->kind() == TSKind::REF) {
        // Create TSReference pointing to native's path
        TSReference ref = TSReference::peered(native_view.short_path());
        alt_view.set_value(value::View::from(ref));
        return;
    }

    // Case: Composite types - recurse
    if (target_meta->kind() == TSKind::TSB && native_meta->kind() == TSKind::TSB) {
        for (size_t i = 0; i < target_meta->field_count(); ++i) {
            establish_links_recursive(
                alt,
                alt_view.field(i),
                native_view.field(i),
                target_meta->field_meta(i),
                native_meta->field_meta(i)
            );
        }
        return;
    }

    // Case: Same type - direct link
    if (target_meta == native_meta) {
        alt_view.bind(native_view);
        return;
    }

    // ... handle TSD/TSL similarly
}
```

### 6.3 REFLink with Conversion Callback

```cpp
class REFLink : public Notifiable {
    // ... existing members ...

    std::function<void(TSView)> conversion_callback_;

public:
    void set_conversion_callback(std::function<void(TSView)> cb) {
        conversion_callback_ = std::move(cb);
    }

    void notify(engine_time_t et) override {
        // REF source changed - rebind to new target
        rebind_target(et);

        // If we have a conversion callback, invoke it on new target
        if (conversion_callback_ && target_.valid()) {
            TSView target_view = target_view(et);
            conversion_callback_(target_view);
        }
    }
};
```

### 6.4 Nested Conversion Ownership

When `REF[TSD[K,TS[V]]]` is converted to `TSD[K,REF[TS[V]]]`:

1. **Alternative creation**: The requesting TSOutput creates the alternative
2. **REFLink ownership**: The requesting TSOutput owns the REFLink in `ref_links_`
3. **Target alternative**: When REF resolves to a target output:
   - Target output may need to create its own alternative `TSD[K,REF[TS[V]]]`
   - Target alternative is owned by target output
   - REFLink holds pointer to target alternative, not ownership
4. **Rebind on REF change**: REFLink detects REF change -> unbinds old -> requests new target alternative -> binds

```cpp
REFLink* create_ref_link_with_conversion(
    TSView alt_view,
    TSView native_view,
    const TSMeta* target_meta,
    const TSMeta* deref_meta
) {
    REFLink* ref_link = new REFLink();
    ref_link->bind_to_ref(native_view);

    ref_link->set_conversion([target_meta](TSView dereferenced_source) {
        // This runs on the LINKED output
        TSOutput* linked_output = dereferenced_source.output();
        // Request or create alternative on the linked output
        // The linked output owns this alternative
        TSOutputView alt_view = linked_output->view(current_time, target_meta);
        return alt_view.ts_view();
    });

    return ref_link;
}
```

---

## 7. Complete Python API Method Mappings

### 7.1 PyTimeSeriesType (Base)

| Method | Old Implementation | New Implementation |
|--------|-------------------|-------------------|
| `owning_node()` | `_impl->owning_node()` | `view_.short_path().node()` wrapped |
| `owning_graph()` | `_impl->owning_graph()` | `node_->graph()` |
| `value` | `_impl->py_value()` | `view_.to_python()` |
| `delta_value` | `_impl->py_delta_value()` | `view_.delta_to_python()` |
| `last_modified_time` | `_impl->last_modified_time()` | `view_.last_modified_time()` |
| `modified` | `_impl->modified()` | `view_.modified()` |
| `valid` | `_impl->valid()` | `view_.valid()` |
| `all_valid` | `_impl->all_valid()` | `view_.all_valid()` |
| `is_reference` | `_impl->is_reference()` | `view_.ts_meta()->kind() == TSKind::REF` |
| `has_parent_or_node` | `_impl->has_parent_or_node()` | `!view_.short_path().indices().empty() \|\| node_ != nullptr` |
| `has_owning_node` | `_impl->has_owning_node()` | `node_ != nullptr` |

### 7.2 PyTimeSeriesOutput

| Method | Old Implementation | New Implementation |
|--------|-------------------|-------------------|
| `parent_output()` | `impl()->parent_output()` | Navigate via short_path parent index |
| `has_parent_output()` | `impl()->has_parent_output()` | `!view_.short_path().indices().empty()` |
| `apply_result(value)` | `impl()->apply_result(value)` | `if (!value.is_none()) output_view_.from_python(value)` |
| `set_value(value)` | `impl()->py_set_value(value)` | `if (value.is_none()) output_view_.invalidate(); else output_view_.from_python(value)` |
| `copy_from_output(out)` | `impl()->copy_from_output(*)` | `output_view_.set_value(out.output_view_.value())` |
| `copy_from_input(in)` | `impl()->copy_from_input(*)` | `output_view_.set_value(in.input_view_.value())` |
| `clear()` | `impl()->clear()` | `output_view_.invalidate(); clear_delta()` |
| `invalidate()` | `impl()->invalidate()` | `output_view_.invalidate()` |
| `can_apply_result(value)` | `impl()->can_apply_result(value)` | See below |

**can_apply_result Implementation:**
```cpp
bool PyTimeSeriesOutput::can_apply_result(nb::object value) {
    if (value.is_none()) return true;  // None always applicable

    // Check if value can be converted to the expected type
    const TypeMeta* value_meta = output_view_.ts_meta()->value_type();
    try {
        // Attempt conversion without actually setting
        value::Value<> test_val(value_meta);
        value_meta->ops->from_python(test_val.data(), value, value_meta);
        return true;
    } catch (...) {
        return false;
    }
}
```

### 7.3 PyTimeSeriesInput

| Method | Old Implementation | New Implementation |
|--------|-------------------|-------------------|
| `parent_input()` | `impl()->parent_input()` | Navigate via short_path parent index |
| `has_parent_input()` | `impl()->has_parent_input()` | `!view_.short_path().indices().empty()` |
| `active()` | `impl()->active()` | `input_view_.active()` |
| `make_active()` | `impl()->make_active()` | `input_view_.make_active()` |
| `make_passive()` | `impl()->make_passive()` | `input_view_.make_passive()` |
| `bound()` | `impl()->bound()` | `input_view_.is_bound()` |
| `has_peer()` | `impl()->has_peer()` | `input_view_.ts_meta()->is_peered()` |
| `output()` | `wrap_output(impl()->output())` | `wrap_output(input_view_.bound_output())` |
| `has_output()` | `impl()->has_output()` | `input_view_.bound_output() != nullptr` |
| `bind_output(out)` | `impl()->bind_output(unwrap_output(o))` | `input_view_.bind(out.output_view_); return true` |
| `un_bind_output(unbind_refs)` | `impl()->un_bind_output(unbind_refs)` | `input_view_.unbind(); if (unbind_refs) unbind_nested_refs()` |
| `reference_output()` | `wrap_output(impl()->reference_output())` | For REF types, get the REFLink target |
| `get_input(index)` | `wrap_input(impl()->get_input(index))` | `wrap_input_view(input_view_[index])` |

### 7.4 PyTimeSeriesBundle

| Method | New Implementation Strategy |
|--------|---------------------------|
| `__getitem__(key)` | `ts_view.field(key)` if string, `ts_view[key]` if int, wrap result |
| `__getattr__(name)` | Same as `__getitem__` with string key |
| `__iter__()` | `nb::iter(keys())` - iterate over field names |
| `__contains__(key)` | Check field name exists in TSMeta.field_names() |
| `keys()` | Return field names from TSMeta |
| `values()` | Return wrapped views for all fields |
| `items()` | Return (key, wrapped_view) pairs |
| `valid_keys()` | Filter fields by `field_view.valid()` |
| `valid_values()` | Filter values by validity |
| `valid_items()` | Return (key, wrapped_view) for valid fields |
| `modified_keys()` | Filter fields by `field_view.modified()` |
| `modified_values()` | Filter values by modification |
| `modified_items()` | Return (key, wrapped_view) for modified fields |
| `key_from_value(ts)` | Find field name by matching short_path |
| `__len__()` | `ts_meta()->field_count()` |
| `empty` | `field_count() == 0` |
| `schema` | Return bundle's TimeSeriesSchema |

### 7.5 PyTimeSeriesList

| Method | New Implementation Strategy |
|--------|---------------------------|
| `__getitem__(index)` | `ts_view[index]`, wrap result |
| `__iter__()` | Iterate 0 to size(), yield wrapped elements |
| `__len__()` | `ts_view.size()` |
| `keys()` | Return range [0, size) as list |
| `values()` | Return wrapped views for all elements |
| `items()` | Return (index, wrapped_view) pairs |
| `valid_keys()` | Filter indices by `element.valid()` |
| `valid_values()` | Filter elements by validity |
| `valid_items()` | Return (index, wrapped_view) for valid elements |
| `modified_keys()` | Filter indices by `element.modified()` |
| `modified_values()` | Filter elements by modification |
| `modified_items()` | Return (index, wrapped_view) for modified elements |
| `empty` | `size() == 0` |

### 7.6 PyTimeSeriesDict

| Method | New Implementation Strategy |
|--------|---------------------------|
| `__getitem__(key)` | `as_dict().at(key)`, wrap result |
| `__setitem__(key, value)` | `as_dict().get_or_create(key).from_python(value)` |
| `__delitem__(key)` | `as_dict().remove(key)` |
| `__contains__(key)` | `as_dict().contains(key)` |
| `__iter__()` | `nb::iter(keys())` |
| `__len__()` | `as_dict().size()` |
| `get(key, default)` | If contains, return wrapped; else return default |
| `get_or_create(key)` | `as_dict().get_or_create(key)`, wrap |
| `create(key)` | `as_dict().create(key)` |
| `pop(key, default)` | Get value, remove key, return value or default |
| `keys()` | Iterate slots, return keys as Python list |
| `values()` | Iterate slots, wrap values |
| `items()` | Iterate slots, return (key, wrapped_value) |
| `key_set` | Access underlying KeySet time-series wrapper |
| `added_keys()` | `delta().added_slots()`, map to keys |
| `added_values()` | `delta().added_slots()`, wrap values |
| `added_items()` | `delta().added_slots()`, return pairs |
| `has_added` | `!delta().added_slots().empty()` |
| `was_added(key)` | Check key's slot in added_slots |
| `removed_keys()` | `delta().removed_slots()`, map to keys |
| `removed_values()` | `delta().removed_slots()`, wrap removed values |
| `removed_items()` | `delta().removed_slots()`, return pairs |
| `has_removed` | `!delta().removed_slots().empty()` |
| `was_removed(key)` | Check key's slot in removed_slots |
| `modified_keys()` | Filter keys by slot's `modified()` |
| `modified_values()` | Filter values by modification |
| `modified_items()` | Filter pairs by modification |
| `was_modified(key)` | Check slot's `modified()` |
| `valid_keys()` | Filter keys by slot's `valid()` |
| `valid_values()` | Filter values by validity |
| `valid_items()` | Filter pairs by validity |
| `key_from_value(ts)` | Find key by matching ts's short_path |
| `get_ref(key, requester)` | Create TSReference for key, track requester |
| `release_ref(key, requester)` | Release reference, remove tracking |
| `on_key_added(key)` | (Input only) Handle key addition notification |
| `on_key_removed(key)` | (Input only) Handle key removal notification |

### 7.7 PyTimeSeriesSet

| Method | New Implementation Strategy |
|--------|---------------------------|
| `__contains__(item)` | `as_set().contains(item)` |
| `__len__()` | `as_set().size()` |
| `__iter__()` | Iterate over `as_set().elements()` |
| `empty` | `size() == 0` |
| `values()` | Return Python set from all elements |
| `added()` | `delta().added_slots()`, map to elements |
| `removed()` | `delta().removed_slots()`, map to elements |
| `was_added(item)` | Check element's slot in added_slots |
| `was_removed(item)` | Check element's slot in removed_slots |
| `add(item)` | (Output only) `as_set().add(element)` |
| `remove(item)` | (Output only) `as_set().remove(element)` |
| `get_contains_output(item)` | Create boolean output tracking membership |
| `release_contains_output(item)` | Release membership tracking output |
| `is_empty_output()` | Create boolean output tracking emptiness |

### 7.8 PyTimeSeriesWindow

| Method | New Implementation Strategy |
|--------|---------------------------|
| `value_times()` | `as_window().times()`, convert to Python list |
| `first_modified_time` | `as_window().first_modified_time()` |
| `size` | `as_window().size()` (current element count) |
| `min_size` | Schema property (minimum window size requirement) |
| `has_removed_value` | `as_window().has_removed_value()` |
| `removed_value` | `as_window().removed_value().to_python()` |
| `__len__()` | `as_window().size()` |

### 7.9 PyTimeSeriesReference

| Method | New Implementation Strategy |
|--------|---------------------------|
| `value` | Return TSReference/FQReference Python object |
| `set_value(ref)` | (Output only) Set TSReference value |
| `items()` | For non-peered, return list of child references |
| `__getitem__(index)` | Access item by index (for non-peered) |
| `has_output` | Whether the reference points to a valid output |
| `is_empty` | Check if EMPTY reference |
| `is_bound` | Check if reference is resolved |
| `output` | Get the referenced output (if peered) |
| `bind_input(input)` | Bind an input to observe this reference |

---

## 8. Edge Cases

### 8.1 Circular References in REF Chains

**Scenario**: `REF[A]` where A eventually references back to itself.

**Policy Decision**: Circular references are **REJECTED** at bind time.

**Detection Strategy**: Track alternative creation in progress.

```cpp
class TSOutput {
    std::unordered_set<const TSMeta*> in_progress_alternatives_;

    TSValue& get_or_create_alternative(const TSMeta* schema) {
        if (in_progress_alternatives_.contains(schema)) {
            throw std::runtime_error("Circular reference detected");
        }

        struct Guard {
            std::unordered_set<const TSMeta*>& set;
            const TSMeta* meta;
            ~Guard() { set.erase(meta); }
        } guard{in_progress_alternatives_, schema};
        in_progress_alternatives_.insert(schema);

        // ... create alternative
    }
};
```

**Runtime Resolution**: For REFLink chains, track visited nodes during resolution.

```cpp
class REFLink {
    TSView resolve_target(engine_time_t current_time,
                         std::unordered_set<void*>& visited) {
        // Check for circular traversal
        if (!visited.insert(this).second) {
            // Already visited - circular reference
            return TSView();  // Invalid view
        }

        // Read TSReference from source
        TSReference ref = ref_source_view_data_.value().as<TSReference>();

        // Resolve the reference
        TSView target = resolve_reference(ref, current_time);

        // If target is also a REFLink, continue resolution
        if (target.ts_meta()->kind() == TSKind::REF) {
            // Recursively resolve through REFLinks
            // ...
        }

        return target;
    }
};
```

### 8.2 Alternative Invalidation on Schema Change

**Scenario**: Schema metadata changes (e.g., TSMeta is regenerated or modified).

**Policy Decision**: Alternatives are keyed by TSMeta pointer. Schema changes create new TSMeta instances, which naturally invalidate old alternatives.

**Lifetime Policy**: Alternatives remain alive while any TSInput is bound to them. When no inputs are bound, alternatives may be lazily collected.

```cpp
class TSOutput {
    // Reference counting for alternatives
    std::unordered_map<const TSMeta*, std::pair<TSValue, size_t>> alternatives_;

    TSOutputView view(engine_time_t current_time, const TSMeta* schema) {
        if (schema == ts_meta()) {
            return TSOutputView(native_value_.ts_view(current_time), this);
        }

        auto it = alternatives_.find(schema);
        if (it != alternatives_.end()) {
            return TSOutputView(it->second.first.ts_view(current_time), this);
        }

        // Create with ref count = 0, incremented when bound
        auto& [alt, count] = alternatives_.emplace(
            schema, std::make_pair(TSValue(*schema), 0)).first->second;
        establish_alternative_links(alt, schema);
        return TSOutputView(alt.ts_view(current_time), this);
    }

    void increment_alternative_ref(const TSMeta* schema) {
        auto it = alternatives_.find(schema);
        if (it != alternatives_.end()) {
            it->second.second++;
        }
    }

    void decrement_alternative_ref(const TSMeta* schema) {
        auto it = alternatives_.find(schema);
        if (it != alternatives_.end()) {
            if (--it->second.second == 0) {
                // Optional: lazy cleanup
                // alternatives_.erase(it);
            }
        }
    }
};
```

### 8.3 Partial TSB Binding (Some Fields Bound, Some Not)

**Scenario**: Un-peered bundle where some fields are bound, others are not.

**Key Design Points**:
1. Each field has its own LinkTarget in TSValue::link_
2. Binding is per-field, not all-or-nothing
3. `valid()` on parent returns true only if ALL fields are valid

```cpp
// Un-peered TSB binding example
TSInput[TSB[a: TS[int], b: TS[float]]]

value_ structure:
  |-- field_a: LinkTarget {is_linked=false}
  +-- field_b: LinkTarget {is_linked=false}

After partial binding:
  input_view.field("a").bind(output1_view);

  |-- field_a: LinkTarget {is_linked=true, value_data=output1.value_, ...}
  +-- field_b: LinkTarget {is_linked=false}

Validity semantics:
  input_view.valid()            -> false (field_b not bound)
  input_view.all_valid()        -> false (field_b not bound)
  input_view.field("a").valid() -> output1 valid state
  input_view.field("b").valid() -> false (not bound)
```

**Active State for Partial Binding**:

```cpp
void make_bound_fields_active(TSInputView& bundle_input) {
    for (const auto& field_name : bundle_input.ts_meta()->field_names()) {
        TSInputView field_view = bundle_input.field(field_name);
        if (field_view.is_bound()) {
            field_view.make_active();
        }
    }
}
```

### 8.4 TSW Alternatives (How Do They Work?)

**Scenario**: TSW (time-series window) with alternative view requests.

**Policy**: TSW can participate in alternatives, primarily for element type conversion.

**Key Points**:
1. TSW stores timestamped values in a cyclic buffer
2. Alternatives for TSW are primarily for the element type, not window structure
3. Window structure (cyclic buffer) is always native

```cpp
// TSW[TS[int]] -> alternative TSW[TS[float]]
// The window structure stays the same, but element access converts

class TSWView {
    size_t capacity() const;
    size_t size() const;

    TSView at(size_t index) const {
        // Get element view from cyclic buffer
        // If alternative, element view uses alternative's element schema
        return element_view_at(index);
    }

    engine_time_t time_at(size_t index) const;
};

// Alternative creation for TSW
TSValue& get_or_create_tsw_alternative(const TSMeta* target_schema) {
    // Create alternative with same window structure but different element schema
    TSValue alt(target_schema);

    // Establish per-element links for conversion
    establish_window_element_links(alt, target_schema);
    return alternatives_.emplace(target_schema, std::move(alt)).first->second;
}
```

**TSW Removed Value Handling**:

```cpp
nb::object PyTimeSeriesWindowOutput::removed_value() const {
    TSWView window = output_view_.ts_view().as_window();
    if (!window.has_removed_value()) {
        return nb::none();
    }

    value::View removed = window.removed_value();
    const TypeMeta* elem_meta = window.ts_meta()->element_type();
    return elem_meta->ops->to_python(removed.data(), elem_meta);
}
```

---

## 9. Lifecycle Sequence Diagrams

### 9.1 Alternative Creation Lifecycle

```
[Consumer requests view with different schema]
      |
      v
[TSOutput::view(current_time, target_schema)]
      |
      v
[Check: target_schema == native_schema?]
      |
      +-- YES -> Return native TSOutputView
      |
      +-- NO -> [get_or_create_alternative(target_schema)]
                    |
                    v
              [Check alternatives_ map]
                    |
                    +-- FOUND -> Return existing alternative view
                    |
                    +-- NOT FOUND
                          |
                          v
                    [Create new TSValue with target_schema]
                          |
                          v
                    [establish_links_recursive()]
                          |
                          +-- Same type -> Create Link
                          +-- REF[X]->X -> Create REFLink
                          +-- X->REF[X] -> Create TSReference
                          +-- Container -> Recurse per element
                          |
                          v
                    [Store in alternatives_ map]
                          |
                          v
                    [Return alternative TSOutputView]
```

### 9.2 Binding Lifecycle

```
[Python: input.bind_output(output)]
            |
            v
[PyTimeSeriesInput::bind_output(output_)]
            |
            v
[Unwrap output_ to get PyTimeSeriesOutput]
            |
            v
[input_view_.bind(output.output_view_)]
            |
            +-----------------------------+
            |                             |
            v                             v
[Get LinkTarget from input's link_data]  [Get ViewData from output]
            |                             |
            v                             v
[Copy output's ViewData to LinkTarget]   [Set bound_output_ pointer]
            |                             |
            +-----------------------------+
                          |
                          v
              [Check if input is active]
                          |
              +-----------+-----------+
              |                       |
         active=true             active=false
              |                       |
              v                       v
[output.subscribe(input_)]       [Done - no subscription]
              |
              v
[Add input_ to output's ObserverList]
              |
              v
           [Done]
```

### 9.3 Value Modification and Notification Lifecycle

```
[Python: output.value = 42]
            |
            v
[PyTimeSeriesOutput::set_value(42)]
            |
            v
[output_view_.from_python(42)]
            |
            v
[TSView::from_python(42)]
            |
            +---------------------------+
            |                           |
            v                           v
[Convert Python to C++]          [Update time_data]
            |                           |
            v                           v
[Copy to value_data]            [*time_ptr = current_time_]
            |                           |
            +---------------------------+
                          |
                          v
              [notify_observers()]
                          |
                          v
            [Get ObserverList from observer_data]
                          |
                          v
            [For each Notifiable* in list]
                          |
                          v
              [notifiable->notify(current_time)]
                          |
                          v
              [TSInput::notify() called]
                          |
                          v
            [owning_node_->schedule()]
                          |
                          v
            [Node added to scheduler queue]
```

### 9.4 REFLink Rebind Lifecycle

```
[REF source modified - TSReference changed]
            |
            v
[notify_observers() on REF source]
            |
            v
[REFLink::notify(current_time)]
            |
            v
[rebind_target(current_time)]
            |
            +-----------------------------+
            |                             |
            v                             v
[Unsubscribe from old target]    [Read new TSReference]
            |                             |
            v                             v
[old_target.unsubscribe(this)]  [Resolve TSReference to path]
            |                             |
            +-----------------------------+
                          |
                          v
            [Find new target output]
                          |
                          v
            [Populate target_ LinkTarget]
                          |
                          v
            [Subscribe to new target if needed]
                          |
                          v
            [Invoke conversion_callback_ if set]
                          |
                          v
            [Update last_rebind_time_]
```

---

## 10. Wrapper Factory Changes

### 10.1 New Factory Pattern

The wrapper factory needs to create appropriate wrappers based on TSMeta kind:

```cpp
nb::object create_wrapper_for_kind(
    std::shared_ptr<Node> node,
    TSOutputView view
) {
    switch (view.ts_meta()->kind()) {
        case TSKind::TS:
            return nb::cast(PyTimeSeriesValueOutput(node, view));
        case TSKind::TSB:
            return nb::cast(PyTimeSeriesBundleOutput(node, view));
        case TSKind::TSL:
            return nb::cast(PyTimeSeriesListOutput(node, view));
        case TSKind::TSD:
            return nb::cast(PyTimeSeriesDictOutput(node, view));
        case TSKind::TSS:
            return nb::cast(PyTimeSeriesSetOutput(node, view));
        case TSKind::TSW:
            if (view.ts_meta()->is_fixed_window()) {
                return nb::cast(PyTimeSeriesFixedWindowOutput(node, view));
            } else {
                return nb::cast(PyTimeSeriesTimeWindowOutput(node, view));
            }
        case TSKind::REF:
            return nb::cast(PyTimeSeriesReferenceOutput(node, view));
        case TSKind::SIGNAL:
            return nb::cast(PyTimeSeriesSignalOutput(node, view));
        default:
            throw std::runtime_error("Unknown TSKind");
    }
}
```

---

## 11. Implementation Strategy (CORRECTED)

> **CORRECTION**: We are NOT creating new wrapper classes. We are replacing `_impl` with `view_` in the EXISTING wrappers.

### 11.1 Phase 1: Add View-Based Infrastructure ✅ COMPLETE

Add view-based storage and methods to existing wrappers:
- Add `view_` / `output_view_` / `input_view_` members
- Add view-based constructors
- Implement view-based code paths in all methods
- Transitionally use `has_view()` checks to support both paths

### 11.2 Phase 2: Update Call Sites 🚧 IN PROGRESS

Update all code that creates wrappers:
- Wrapper factory functions
- Node output/input creation
- Navigation methods (get_item, field, etc.)

### 11.3 Phase 3: Remove Legacy Code ⏳ PENDING

Once all call sites use views:
- Remove `_impl` member from PyTimeSeriesType
- Remove legacy constructors
- Remove `has_view()` checks and legacy branches
- Simplify method implementations to direct view access

### 11.4 Phase 4: Cleanup ⏳ PENDING

Final cleanup:
- Remove ApiPtr usage for time-series types
- Remove old TimeSeriesType C++ class hierarchy (if fully replaced)
- Simplify wrapper_factory.cpp

---

## 12. Resolved Questions

### 12.1 Thread Safety

**Question**: Is alternatives_ access thread-safe?

**Answer**: Not needed. Per CLAUDE.md, hgraph uses single-threaded graph execution. All alternative creation and access happens within a single thread.

### 12.2 Circular References

**Question**: How to handle circular references?

**Answer**: Rejected at bind time. See Section 8.1 for detection mechanism.

### 12.3 Alternative Lifetime

**Question**: When can alternatives be destroyed?

**Answer**: Reference-counted per bound TSInput. Alternatives remain while any input is bound. See Section 8.2 for implementation.

---

## 13. Critical Files for Implementation

1. **`cpp/include/hgraph/types/time_series/ts_input_view.h`** - TSInputView class with correct navigation and link-following logic

2. **`cpp/include/hgraph/types/time_series/ts_output.h`** - TSOutput with alternative management and REFLink integration

3. **`cpp/include/hgraph/types/time_series/ref_link.h`** - REFLink with conversion callback for nested REF conversions

4. **`cpp/include/hgraph/api/python/py_time_series.h`** - Core Python wrapper base classes with new backing

5. **`cpp/src/cpp/api/python/py_tsd.cpp`** - Most complex wrapper implementation, reference for delta tracking pattern
