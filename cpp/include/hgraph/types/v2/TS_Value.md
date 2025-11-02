# Time Series Values

**Type-erased time series value containers for HGraph**

Time series values represent the state of nodes in HGraph's reactive graph system. They provide zero-copy sharing
between outputs (producers) and inputs (consumers), enabling efficient event propagation and subscription management.

---

## Table of Contents

1. [Overview](#overview)
2. [Architecture](#architecture)
3. [TimeSeriesValueOutput - Event Producer](#timeseriesvalueoutput---event-producer)
4. [TimeSeriesValueInput - Event Consumer](#timeseriesvalueinput---event-consumer)
5. [Binding and Subscription](#binding-and-subscription)
6. [Active State Management](#active-state-management)
7. [Use Cases](#use-cases)
8. [API Reference](#api-reference)

---

## Overview

HGraph uses two primary types to represent time series values in the C++ runtime:

- **`TimeSeriesValueOutput`**: Produces events and holds the authoritative state (one per node output)
- **`TimeSeriesValueInput`**: Consumes events from a bound output via zero-copy sharing (many per output)

Both types use **type erasure** via `AnyValue<>` to eliminate template proliferation while maintaining type safety
through visitor patterns.

### Key Design Principles

1. **Zero-copy sharing**: Inputs and outputs share the same `TimeSeriesValueImpl` via `shared_ptr`
2. **Single source of truth**: All state lives in the shared impl, not in wrappers
3. **Type erasure**: `AnyValue<>` eliminates per-type template instantiation
4. **Subscriber pattern**: Inputs subscribe to outputs to receive change notifications
5. **Active state tracking**: Inputs track whether they're actively consuming values

### State Management

```
┌─────────────────────┐
│ TimeSeriesValue     │
│      Output         │
│  (Event Producer)   │
└──────────┬──────────┘
           │ shares
           ▼
  ┌────────────────────┐         ┌─────────────────────┐
  │ TimeSeriesValueImpl│◄────────┤ TimeSeriesValue     │
  │                    │ shares  │      Input          │
  │  • AnyValue value  │         │  (Event Consumer)   │
  │  • TsEventAny evt  │◄────────┤                     │
  │  • subscribers     │ shares  └─────────────────────┘
  └────────────────────┘
```

---

## Architecture

### Memory Layout

**TimeSeriesValueOutput**:
```
┌────────────────────────────────────────────────────┐
│ shared_ptr<TimeSeriesValueImpl> _impl              │
│   • Points to shared impl (8 bytes)                │
├────────────────────────────────────────────────────┤
│ Notifiable* _parent                                │
│   • Parent node for time queries (8 bytes)         │
└────────────────────────────────────────────────────┘
Total: 16 bytes (stack-allocated wrapper)
```

**TimeSeriesValueInput**:
```
┌────────────────────────────────────────────────────┐
│ shared_ptr<TimeSeriesValueImpl> _impl              │
│   • Points to shared impl (8 bytes)                │
├────────────────────────────────────────────────────┤
│ Notifiable* _parent                                │
│   • Parent node for notification (8 bytes)         │
└────────────────────────────────────────────────────┘
Total: 16 bytes (stack-allocated wrapper)
```

**Shared TimeSeriesValueImpl** (SimplePeeredImpl):
```
┌────────────────────────────────────────────────────┐
│ AnyValue<> _value                  (~40 bytes)     │
│   • Current value (type-erased)                    │
├────────────────────────────────────────────────────┤
│ TsEventAny _last_event             (~56 bytes)     │
│   • Most recent event with timestamp               │
├────────────────────────────────────────────────────┤
│ std::unordered_set<Notifiable*>   (24+ bytes)     │
│   • Subscriber set for notifications               │
└────────────────────────────────────────────────────┘
Total: ~120 bytes (heap-allocated, shared)
```

### Implementation Variants

The system uses two impl variants via virtual dispatch:

| Implementation      | Used By        | State Tracking        | Purpose                         |
|---------------------|----------------|------------------------|----------------------------------|
| **NonBoundImpl**    | Unbound inputs | `bool _active`        | Default before binding           |
| **SimplePeeredImpl**| Outputs/inputs | Subscriber set        | Shared state after binding       |

---

## TimeSeriesValueOutput - Event Producer

Represents the output of a graph node that produces time series values.

### Construction

```cpp
struct MyNode : Notifiable, CurrentTimeProvider {
    engine_time_t _current_time{min_start_time()};

    void notify(engine_time_t et) override { /* ... */ }
    engine_time_t current_engine_time() const override { return _current_time; }
};

MyNode parent;
TimeSeriesValueOutput output(&parent);
```

**Requirements:**
- Parent must implement both `Notifiable` and `CurrentTimeProvider` traits
- Parent pointer cannot be null (throws `std::runtime_error`)
- Automatically creates a `SimplePeeredImpl` for sharing

### Setting Values

```cpp
// Set with AnyValue (copy)
AnyValue<> val;
val.emplace<int>(42);
output.set_value(val);

// Set with AnyValue (move)
AnyValue<> val2;
val2.emplace<std::string>("hello");
output.set_value(std::move(val2));

// Invalidate (mark as invalid)
output.invalidate();
```

**Behavior:**
- `set_value()` creates a `TsEventKind::Modify` event
- Event is applied to shared impl
- All bound inputs are notified via `notify_subscribers()`
- Timestamp comes from `parent->current_engine_time()`

### Querying State

```cpp
// Current value (type-erased)
const AnyValue<>& val = output.value();

// State queries
bool was_modified = output.modified();     // Modified at current time?
bool is_valid = output.valid();            // Has valid value?
engine_time_t t = output.last_modified_time(); // When last changed

// Delta value (event at current time)
TsEventAny event = output.delta_value();
```

### Thread Safety

**Not thread-safe** - external synchronization required if shared between threads.

---

## TimeSeriesValueInput - Event Consumer

Represents an input to a graph node that consumes time series values from an output.

### Construction

```cpp
MyNode parent;
TimeSeriesValueInput input(&parent);
```

**Initial State:**
- Not bound to any output
- Uses `NonBoundImpl` (returns defaults)
- Active state tracked locally as boolean

### Binding to Output

```cpp
TimeSeriesValueOutput output(&parent);
input.bind_output(&output);
```

**Binding Behavior:**
1. Captures current active state
2. Marks passive on old impl (if active)
3. Switches to output's impl (zero-copy sharing)
4. Restores active state on new impl (if was active)
5. Now shares exact same state as output

**Zero-Copy Sharing:**
```cpp
output.set_value(some_value);

// Input sees same value object (no copy)
assert(&output.value() == &input.value());
```

### Rebinding

Inputs can be rebound to different outputs:

```cpp
TimeSeriesValueOutput output1(&parent);
TimeSeriesValueOutput output2(&parent);
TimeSeriesValueInput input(&parent);

input.bind_output(&output1);  // Bind to output1
input.mark_active();           // Subscribe to output1

input.bind_output(&output2);  // Rebind to output2
// Active state preserved: now subscribed to output2
// No longer subscribed to output1
```

### Querying State

```cpp
// Read value (same as output)
const AnyValue<>& val = input.value();

// State queries
bool was_modified = input.modified();
bool is_valid = input.valid();
engine_time_t t = input.last_modified_time();

// Check if active
bool is_active = input.active();
```

### Notification Flow

When bound output changes:

```
Output.set_value()
    ↓
SimplePeeredImpl.apply_event()
    ↓
SimplePeeredImpl.notify_subscribers()
    ↓
[for each subscriber]
    ↓
Input.notify(time)
    ↓
ParentNode.notify(time)  ← Schedules parent node
```

---

## Binding and Subscription

### Binding Lifecycle

```cpp
// 1. Create unbound input
TimeSeriesValueInput input(&parent);
assert(!input.valid());  // NonBoundImpl returns false

// 2. Bind to output
TimeSeriesValueOutput output(&parent);
input.bind_output(&output);
// Now shares SimplePeeredImpl with output

// 3. Output sets value
AnyValue<> val;
val.emplace<int>(100);
output.set_value(val);

// 4. Input sees same value
assert(input.valid());
assert(*input.value().get_if<int>() == 100);
```

### Multiple Inputs, One Output

```cpp
TimeSeriesValueOutput output(&parent);
TimeSeriesValueInput input1(&parent);
TimeSeriesValueInput input2(&parent);
TimeSeriesValueInput input3(&parent);

// All bind to same output
input1.bind_output(&output);
input2.bind_output(&output);
input3.bind_output(&output);

// All share same impl
assert(input1.get_impl() == input2.get_impl());
assert(input2.get_impl() == output.get_impl());

// One write, all see change
output.set_value(make_any(42));
assert(*input1.value().get_if<int>() == 42);
assert(*input2.value().get_if<int>() == 42);
assert(*input3.value().get_if<int>() == 42);
```

---

## Active State Management

Inputs track whether they're **active** (subscribed to receive notifications).

### Active State API

```cpp
TimeSeriesValueInput input(&parent);
input.bind_output(&output);

// Initially not active
assert(!input.active());

// Mark active (subscribe)
input.mark_active();
assert(input.active());

// Now receives notifications when output changes
output.set_value(make_any(10));  // Input's parent.notify() called

// Mark passive (unsubscribe)
input.mark_passive();
assert(!input.active());

// No longer receives notifications
output.set_value(make_any(20));  // Input's parent.notify() NOT called
```

### Active State Persistence

Active state is preserved across rebinding:

```cpp
input.bind_output(&output1);
input.mark_active();
assert(input.active());

// Rebind to different output
input.bind_output(&output2);

// Active state preserved
assert(input.active());

// Subscription moved:
// - Removed from output1's subscribers
// - Added to output2's subscribers
```

### Subscriber Set

Internally, `SimplePeeredImpl` maintains a subscriber set:

```cpp
// When input marks active:
impl->mark_active(input_ptr);
// → subscribers.insert(input_ptr)

// When input marks passive:
impl->mark_passive(input_ptr);
// → subscribers.erase(input_ptr)

// When checking active:
bool is_active = impl->active(input_ptr);
// → return subscribers.contains(input_ptr)
```

---

## Use Cases

### 1. Basic Producer-Consumer

```cpp
struct ProducerNode : Notifiable, CurrentTimeProvider {
    TimeSeriesValueOutput output{this};
    engine_time_t _time{min_start_time()};

    void produce_value(int v) {
        _time += std::chrono::seconds(1);
        AnyValue<> val;
        val.emplace<int>(v);
        output.set_value(val);
    }

    void notify(engine_time_t et) override {}
    engine_time_t current_engine_time() const override { return _time; }
};

struct ConsumerNode : Notifiable, CurrentTimeProvider {
    TimeSeriesValueInput input{this};
    engine_time_t _time{min_start_time()};

    void process() {
        if (input.modified() && input.valid()) {
            int value = *input.value().get_if<int>();
            std::cout << "Consumed: " << value << "\n";
        }
    }

    void notify(engine_time_t et) override { process(); }
    engine_time_t current_engine_time() const override { return _time; }
};

// Wire up
ProducerNode producer;
ConsumerNode consumer;
consumer.input.bind_output(&producer.output);
consumer.input.mark_active();

// Produce values
producer.produce_value(42);   // Consumer notified
producer.produce_value(100);  // Consumer notified
```

### 2. Fan-Out (One Producer, Many Consumers)

```cpp
TimeSeriesValueOutput sensor(&parent);
TimeSeriesValueInput display(&display_node);
TimeSeriesValueInput logger(&logger_node);
TimeSeriesValueInput analyzer(&analyzer_node);

// All consume same sensor output
display.bind_output(&sensor);
logger.bind_output(&sensor);
analyzer.bind_output(&sensor);

// All mark active
display.mark_active();
logger.mark_active();
analyzer.mark_active();

// One update notifies all
sensor.set_value(make_any(98.6));
// → display_node.notify() called
// → logger_node.notify() called
// → analyzer_node.notify() called
```

### 3. Dynamic Rebinding (Switch Input Source)

```cpp
TimeSeriesValueOutput primary_source(&parent);
TimeSeriesValueOutput backup_source(&parent);
TimeSeriesValueInput input(&consumer);

// Initially bind to primary
input.bind_output(&primary_source);
input.mark_active();

// Switch to backup on failure
if (primary_failed) {
    input.bind_output(&backup_source);
    // Still active, now consuming from backup
}
```

### 4. Conditional Subscription

```cpp
TimeSeriesValueInput input(&parent);
input.bind_output(&output);

// Subscribe only when interested
if (user_is_watching) {
    input.mark_active();
    // Now receives updates
}

// Unsubscribe to save computation
if (!user_is_watching) {
    input.mark_passive();
    // No longer receives updates
}
```

### 5. Type-Safe Value Access

```cpp
TimeSeriesValueOutput output(&parent);
output.set_value(make_any<std::string>("hello"));

TimeSeriesValueInput input(&parent);
input.bind_output(&output);

// Type-safe access via visitor
input.value().visit_as<std::string>([](const std::string& s) {
    std::cout << "String value: " << s << "\n";
});

// Or direct access with type check
if (auto* ptr = input.value().get_if<std::string>()) {
    std::cout << "String value: " << *ptr << "\n";
}
```

### 6. Event Inspection

```cpp
TimeSeriesValueInput input(&parent);
input.bind_output(&output);
input.mark_active();

if (input.modified()) {
    TsEventAny event = input.delta_value();

    switch (event.kind) {
        case TsEventKind::Modify:
            event.visit_value_as<int>([](int v) {
                std::cout << "Value changed to: " << v << "\n";
            });
            break;
        case TsEventKind::Invalidate:
            std::cout << "Value invalidated\n";
            break;
        default:
            break;
    }
}
```

---

## API Reference

### TimeSeriesValueOutput

#### Construction

| Method | Signature | Description |
|--------|-----------|-------------|
| Constructor | `explicit TimeSeriesValueOutput(ParentNode* parent)` | Create output with parent node (throws if null) |

#### Value Modification

| Method | Signature | Description |
|--------|-----------|-------------|
| `set_value` | `void set_value(const AnyValue<>& v)` | Set value (copy) and notify subscribers |
| `set_value` | `void set_value(AnyValue<>&& v)` | Set value (move) and notify subscribers |
| `invalidate` | `void invalidate()` | Mark value as invalid and notify subscribers |

#### Value Access

| Method | Signature | Description |
|--------|-----------|-------------|
| `value` | `const AnyValue<>& value() const` | Get current value (type-erased) |
| `valid` | `bool valid() const` | Check if value is valid |
| `modified` | `bool modified() const` | Check if modified at current time |
| `last_modified_time` | `engine_time_t last_modified_time() const` | Get timestamp of last modification |
| `delta_value` | `TsEventAny delta_value() const` | Get event at current time |

#### Internal

| Method | Signature | Description |
|--------|-----------|-------------|
| `get_impl` | `shared_ptr<TimeSeriesValueImpl> get_impl() const` | Get shared impl (for binding) |
| `current_time` | `engine_time_t current_time() const` | Get current time from parent |

### TimeSeriesValueInput

#### Construction

| Method | Signature | Description |
|--------|-----------|-------------|
| Constructor | `explicit TimeSeriesValueInput(ParentNode* parent)` | Create input with parent node |

#### Binding

| Method | Signature | Description |
|--------|-----------|-------------|
| `bind_output` | `void bind_output(TimeSeriesValueOutput* output)` | Bind to output (share impl, preserve active state) |

#### Active State

| Method | Signature | Description |
|--------|-----------|-------------|
| `active` | `bool active() const` | Check if input is active (subscribed) |
| `mark_active` | `void mark_active()` | Subscribe to receive notifications |
| `mark_passive` | `void mark_passive()` | Unsubscribe from notifications |

#### Value Access

| Method | Signature | Description |
|--------|-----------|-------------|
| `value` | `const AnyValue<>& value() const` | Get current value (type-erased) |
| `valid` | `bool valid() const` | Check if value is valid |
| `modified` | `bool modified() const` | Check if modified at current time |
| `last_modified_time` | `engine_time_t last_modified_time() const` | Get timestamp of last modification |
| `delta_value` | `TsEventAny delta_value() const` | Get event at current time |

#### Internal

| Method | Signature | Description |
|--------|-----------|-------------|
| `notify` | `void notify(engine_time_t t)` | Called by impl when value changes (delegates to parent) |
| `current_time` | `engine_time_t current_time() const` | Get current time from parent |

### ParentNode Concept

```cpp
template <typename T>
concept ParentNode =
    std::derived_from<T, Notifiable> &&
    std::derived_from<T, CurrentTimeProvider>;
```

Parent nodes must implement:

| Trait | Method | Description |
|-------|--------|-------------|
| `Notifiable` | `void notify(engine_time_t et)` | Receive notification to schedule |
| `CurrentTimeProvider` | `engine_time_t current_engine_time() const` | Provide current graph time |

---

## Best Practices

1. **Always bind before marking active** - Inputs should be bound to an output before subscribing:
   ```cpp
   input.bind_output(&output);  // Bind first
   input.mark_active();         // Then subscribe
   ```

2. **Use mark_passive when done** - Unsubscribe to prevent unnecessary notifications:
   ```cpp
   input.mark_passive();  // Stop receiving notifications
   ```

3. **Check valid() before accessing value** - Avoid reading invalid values:
   ```cpp
   if (input.valid()) {
       auto value = input.value();
   }
   ```

4. **Use visitors for type safety** - Prefer visitors over direct pointer access:
   ```cpp
   // Good
   input.value().visit_as<int>([](int v) { process(v); });

   // Less safe
   int* ptr = input.value().get_if<int>();
   if (ptr) process(*ptr);
   ```

5. **Preserve active state on rebind** - Binding automatically preserves subscription state (handled internally)

6. **One output, many inputs** - Outputs can have multiple bound inputs (fan-out pattern)

7. **Parent nodes must be valid** - Ensure parent pointers remain valid for the lifetime of the time series value

---

## Implementation Notes

- **Zero-copy**: Inputs and outputs share the same `AnyValue<>` instance (no copying)
- **Reference counting**: `shared_ptr` manages impl lifetime automatically
- **Virtual dispatch**: One virtual call per operation (minimal overhead)
- **Small buffer optimization**: `AnyValue<>` stores small values inline (no heap allocation)
- **Subscriber set overhead**: `std::unordered_set<Notifiable*>` - typically 24 bytes + 8 bytes per subscriber
- **Thread safety**: Not thread-safe - requires external synchronization
- **Exception safety**: Constructors throw on null parent, otherwise noexcept operations

---

## See Also

- [TS_Events.md](TS_Events.md) - Event types and visitor patterns
- [AnyValue.md](AnyValue.md) - Type erasure implementation details
- [HGraph Documentation](https://docs.hgraph.io) - Time series concepts and graph semantics
