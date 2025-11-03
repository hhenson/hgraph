# Time Series Values

**Type-erased time series value containers for HGraph**

Time series values represent the state of nodes in HGraph's reactive graph system. They provide zero-copy sharing
between outputs (producers) and inputs (consumers), enabling efficient event propagation and subscription management.

---

## Table of Contents

1. [Overview](#overview)
2. [Architecture](#architecture)
3. [TSOutput - Event Producer](#tsoutput---event-producer)
4. [TSInput - Event Consumer](#tsinput---event-consumer)
5. [Binding and Subscription](#binding-and-subscription)
6. [Active State Management](#active-state-management)
7. [Type Validation](#type-validation)
8. [Use Cases](#use-cases)
9. [API Reference](#api-reference)

---

## Overview

HGraph uses two primary types to represent time series values in the C++ runtime:

- **`TSOutput`**: Produces events and holds the authoritative state (one per node output)
- **`TSInput`**: Consumes events from a bound output via zero-copy sharing (many per output)

Both types use **type erasure** via `AnyValue<>` to eliminate template proliferation while maintaining type safety
through visitor patterns.

### Key Design Principles

1. **Zero-copy sharing**: Inputs and outputs share the same `TSValue` via `shared_ptr`
2. **Single source of truth**: All state lives in the shared impl, not in wrappers
3. **Type erasure**: `AnyValue<>` eliminates per-type template instantiation
4. **Type safety**: Runtime validation ensures type consistency at bind and event application time
5. **Subscriber pattern**: Inputs subscribe to outputs to receive change notifications
6. **Active state tracking**: Inputs track whether they're actively consuming values

### State Management

```
┌─────────────────────┐
│     TSOutput        │
│  (Event Producer)   │
└──────────┬──────────┘
           │ shares
           ▼
  ┌────────────────────┐         ┌─────────────────────┐
  │      TSValue       │◄────────┤      TSInput        │
  │                    │ shares  │  (Event Consumer)   │
  │  • AnyValue value  │         │                     │
  │  • TsEventAny evt  │◄────────┤                     │
  │  • subscribers     │ shares  └─────────────────────┘
  └────────────────────┘
```

---

## Architecture

### Memory Layout

**TSOutput**:

```
┌────────────────────────────────────────────────────┐
│ shared_ptr<TSValue> _impl              │
│   • Points to shared impl (8 bytes)                │
├────────────────────────────────────────────────────┤
│ Notifiable* _parent                                │
│   • Parent node for time queries (8 bytes)         │
└────────────────────────────────────────────────────┘
Total: 16 bytes (stack-allocated wrapper)
```

**TSInput**:

```
┌────────────────────────────────────────────────────┐
│ shared_ptr<TSValue> _impl              │
│   • Points to shared impl (8 bytes)                │
├────────────────────────────────────────────────────┤
│ Notifiable* _parent                                │
│   • Parent node for notification (8 bytes)         │
└────────────────────────────────────────────────────┘
Total: 16 bytes (stack-allocated wrapper)
```

**Shared TSValue** (SimplePeeredImpl):

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

| Implementation       | Used By        | State Tracking | Purpose                    |
|----------------------|----------------|----------------|----------------------------|
| **NonBoundImpl**     | Unbound inputs | `bool _active` | Default before binding     |
| **SimplePeeredImpl** | Outputs/inputs | Subscriber set | Shared state after binding |

### File Organization

The implementation is split across multiple files for separation of concerns:

| File                | Contents                                                                                                        | Purpose                               |
|---------------------|-----------------------------------------------------------------------------------------------------------------|---------------------------------------|
| **ts_value.h**      | `TSValue` (pure virtual base class)<br>`TSOutput` wrapper class<br>`TSInput` wrapper class<br>Factory functions | Public API and interface definitions  |
| **ts_value_impl.h** | `NonBoundImpl`<br>`SimplePeeredImpl`                                                                            | Concrete implementations of `TSValue` |
| **ts_value.cpp**    | Constructor and method implementations                                                                          | Non-inline implementation details     |

**Key Design Decisions**:

- Constructors are non-template and take `Notifiable*` directly, with factory functions providing template convenience.
  This allows implementation to be moved to `.cpp` file, reducing compile times and header dependencies.
- Both `TSOutput` and `TSInput` are **move-only types** (copying is deleted, moving is defaulted). This prevents
  accidental copies and ensures clear ownership semantics.

---

## TSOutput - Event Producer

Represents the output of a graph node that produces time series values.

### Construction

```cpp
struct MyNode : Notifiable, CurrentTimeProvider {
    engine_time_t _current_time{min_start_time()};

    void notify(engine_time_t et) override { /* ... */ }
    engine_time_t current_engine_time() const override { return _current_time; }
};

MyNode parent;

// Direct constructor (requires cast and explicit typeid)
TSOutput output(static_cast<Notifiable*>(&parent), typeid(int));

// Factory function (template convenience - recommended)
auto output2 = make_ts_output<MyNode, std::string>(&parent);

// Factory with explicit typeid
auto output3 = make_ts_output<MyNode, double>(&parent, typeid(double));
```

**Requirements:**

- Parent must implement both `Notifiable` and `CurrentTimeProvider` traits
- Parent pointer cannot be null (throws `std::runtime_error`)
- Must specify value type via `typeid(T)` for type validation
- Automatically creates a `SimplePeeredImpl` for sharing
- Factory functions handle `ParentNode` concept checking and casting

**Semantics:**

- `TSOutput` is a **move-only type** (cannot be copied, only moved)
- This ensures clear ownership and prevents accidental sharing of output state

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
- **Type validation**: Event value type must match output's declared type (throws if mismatch)
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

## TSInput - Event Consumer

Represents an input to a graph node that consumes time series values from an output.

### Construction

```cpp
MyNode parent;

// Direct constructor (requires cast and explicit typeid)
TSInput input(static_cast<Notifiable*>(&parent), typeid(int));

// Factory function (template convenience - recommended)
auto input2 = make_ts_input<MyNode, std::string>(&parent);
```

**Initial State:**

- Not bound to any output
- Uses `NonBoundImpl` (returns defaults)
- Active state tracked locally as boolean
- Type stored for validation at bind time

**Semantics:**

- `TSInput` is a **move-only type** (cannot be copied, only moved)
- This ensures clear ownership and prevents accidental sharing of input state

### Binding to Output

```cpp
// Using direct constructors
TSOutput output(static_cast<Notifiable*>(&parent), typeid(int));
TSInput input(static_cast<Notifiable*>(&parent), typeid(int));
input.bind_output(&output);

// Using factory functions (recommended)
auto output2 = make_ts_output<MyNode, int>(&parent);
auto input2 = make_ts_input<MyNode, int>(&parent);
input2.bind_output(&output2);
```

**Binding Behavior:**

1. **Type validation**: Input and output types must match (throws if mismatch)
2. Captures current active state
3. Marks passive on old impl (if active)
4. Switches to output's impl (zero-copy sharing)
5. Restores active state on new impl (if was active)
6. Now shares exact same state as output

**Zero-Copy Sharing:**

```cpp
output.set_value(some_value);

// Input sees same value object (no copy)
assert(&output.value() == &input.value());
```

### Rebinding

Inputs can be rebound to different outputs:

```cpp
TSOutput output1(&parent, typeid(int));
TSOutput output2(&parent, typeid(int));
TSInput input(&parent, typeid(int));

input.bind_output(&output1);  // Bind to output1
input.mark_active();           // Subscribe to output1

input.bind_output(&output2);  // Rebind to output2
// Active state preserved: now subscribed to output2
// No longer subscribed to output1
// Type validation ensures output2 is also int
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
TSOutput.set_value()
    ↓
SimplePeeredImpl.apply_event()
    ↓
SimplePeeredImpl.notify_subscribers()
    ↓
[for each subscriber]
    ↓
TSInput.notify(time)
    ↓
ParentNode.notify(time)  ← Schedules parent node
```

---

## Binding and Subscription

### Binding Lifecycle

```cpp
// 1. Create unbound input
TSInput input(&parent, typeid(int));
assert(!input.valid());  // NonBoundImpl returns false

// 2. Bind to output
TSOutput output(&parent, typeid(int));
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
TSOutput output(&parent, typeid(int));
TSInput input1(&parent, typeid(int));
TSInput input2(&parent, typeid(int));
TSInput input3(&parent, typeid(int));

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
TSInput input(&parent, typeid(int));
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

## Type Validation

The time series value system provides runtime type validation to ensure type safety despite using type erasure.

### Type Declaration

Every input and output must declare its value type at construction:

```cpp
// Output declares it produces int values
TSOutput output(&parent, typeid(int));

// Input declares it consumes int values
TSInput input(&parent, typeid(int));
```

The type information is stored internally as `TypeId` (a wrapper around `std::type_info*`) and is used for validation at
two critical points.

### Validation Point 1: Binding

When an input binds to an output, their types must match:

```cpp
TSOutput int_output(&parent, typeid(int));
TSInput string_input(&parent, typeid(std::string));

// This will throw std::runtime_error with descriptive message
try {
    string_input.bind_output(&int_output);
} catch (const std::runtime_error& e) {
    // Error message: "Type mismatch in bind_output: input expects
    //                 std::string but output provides int"
}
```

**Validation Logic:**

```cpp
if (input_type != output_type) {
    throw std::runtime_error(
        "Type mismatch in bind_output: input expects " +
        input_type.name() + " but output provides " + output_type.name()
    );
}
```

### Validation Point 2: Event Application

When an event is applied to an output, the event's value type must match:

```cpp
TSOutput int_output(&parent, typeid(int));

AnyValue<> val;
val.emplace<std::string>("wrong type");

// This will throw std::runtime_error
try {
    int_output.set_value(val);
} catch (const std::runtime_error& e) {
    // Error message: "Type mismatch in apply_event: expected
    //                 int but got std::string"
}
```

**Validation Logic** (in `SimplePeeredImpl::apply_event()`):

```cpp
if (event.kind == Modify || event.kind == Recover) {
    if (event.value.type() != expected_type) {
        throw std::runtime_error(
            "Type mismatch in apply_event: expected " +
            expected_type.name() + " but got " + event.value.type().name()
        );
    }
}
```

### Type Safety Guarantees

1. **Compile-time**: Type erasure via `AnyValue<>` eliminates template proliferation
2. **Runtime**: Type validation catches mismatches at bind and set operations
3. **Clear errors**: Exception messages show expected vs actual types using RTTI names
4. **No silent failures**: Invalid operations always throw, never silently corrupt data

### Example: Type-Safe Pipeline

```cpp
struct Pipeline {
    MockParentNode parent;

    // Type-checked chain: int → string → double
    TSOutput sensor{&parent, typeid(int)};
    TSInput formatter_in{&parent, typeid(int)};
    TSOutput formatter_out{&parent, typeid(std::string)};
    TSInput logger_in{&parent, typeid(std::string)};

    void wire() {
        formatter_in.bind_output(&sensor);      // ✓ int → int
        logger_in.bind_output(&formatter_out);  // ✓ string → string

        // This would fail at runtime:
        // logger_in.bind_output(&sensor);      // ✗ int → string mismatch
    }
};
```

### Performance Considerations

- **Type storage**: Single `TypeId` per impl (~8 bytes)
- **Validation overhead**: One type comparison at bind and per event application
- **Comparison cost**: Pointer comparison via `std::type_info::operator==`
- **No impact on value access**: Type validation happens only at modification points

---

## Use Cases

### 1. Basic Producer-Consumer

```cpp
struct ProducerNode : Notifiable, CurrentTimeProvider {
    TSOutput output{static_cast<Notifiable*>(this), typeid(int)};
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
    TSInput input{static_cast<Notifiable*>(this), typeid(int)};
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
auto sensor = make_ts_output<ParentNode, double>(&parent);
auto display = make_ts_input<ParentNode, double>(&display_node);
auto logger = make_ts_input<ParentNode, double>(&logger_node);
auto analyzer = make_ts_input<ParentNode, double>(&analyzer_node);

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
auto primary_source = make_ts_output<ParentNode, int>(&parent);
auto backup_source = make_ts_output<ParentNode, int>(&parent);
auto input = make_ts_input<ParentNode, int>(&consumer);

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
auto input = make_ts_input<ParentNode, int>(&parent);
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
TSOutput output(&parent, typeid(std::string));
output.set_value(make_any<std::string>("hello"));

TSInput input(&parent, typeid(std::string));
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
TSInput input(&parent, typeid(int));
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

### TSOutput

#### Construction

| Method      | Signature                                                                                              | Description                                                       |
|-------------|--------------------------------------------------------------------------------------------------------|-------------------------------------------------------------------|
| Constructor | `TSOutput(Notifiable* parent, const std::type_info& value_type)`                                       | Create output with parent and value type (implementation in .cpp) |
| Factory     | `template<ParentNode P, typename T>`<br>`make_ts_output(P* parent, const std::type_info& = typeid(T))` | Template convenience factory (handles cast and concept check)     |

#### Value Modification

| Method       | Signature                             | Description                                             |
|--------------|---------------------------------------|---------------------------------------------------------|
| `set_value`  | `void set_value(const AnyValue<>& v)` | Set value (copy), validate type, and notify subscribers |
| `set_value`  | `void set_value(AnyValue<>&& v)`      | Set value (move), validate type, and notify subscribers |
| `invalidate` | `void invalidate()`                   | Mark value as invalid and notify subscribers            |

#### Value Access

| Method               | Signature                                  | Description                        |
|----------------------|--------------------------------------------|------------------------------------|
| `value`              | `const AnyValue<>& value() const`          | Get current value (type-erased)    |
| `valid`              | `bool valid() const`                       | Check if value is valid            |
| `modified`           | `bool modified() const`                    | Check if modified at current time  |
| `last_modified_time` | `engine_time_t last_modified_time() const` | Get timestamp of last modification |
| `delta_value`        | `TsEventAny delta_value() const`           | Get event at current time          |

#### Internal

| Method         | Signature                              | Description                   |
|----------------|----------------------------------------|-------------------------------|
| `get_impl`     | `shared_ptr<TSValue> get_impl() const` | Get shared impl (for binding) |
| `current_time` | `engine_time_t current_time() const`   | Get current time from parent  |

### TSInput

#### Construction

| Method      | Signature                                                                                             | Description                                                      |
|-------------|-------------------------------------------------------------------------------------------------------|------------------------------------------------------------------|
| Constructor | `TSInput(Notifiable* parent, const std::type_info& value_type)`                                       | Create input with parent and value type (implementation in .cpp) |
| Factory     | `template<ParentNode P, typename T>`<br>`make_ts_input(P* parent, const std::type_info& = typeid(T))` | Template convenience factory (handles cast and concept check)    |

#### Binding

| Method        | Signature                            | Description                                                                                 |
|---------------|--------------------------------------|---------------------------------------------------------------------------------------------|
| `bind_output` | `void bind_output(TSOutput* output)` | Bind to output (validate types, share impl, preserve active state - implementation in .cpp) |

#### Active State

| Method         | Signature             | Description                           |
|----------------|-----------------------|---------------------------------------|
| `active`       | `bool active() const` | Check if input is active (subscribed) |
| `mark_active`  | `void mark_active()`  | Subscribe to receive notifications    |
| `mark_passive` | `void mark_passive()` | Unsubscribe from notifications        |

#### Value Access

| Method               | Signature                                  | Description                        |
|----------------------|--------------------------------------------|------------------------------------|
| `value`              | `const AnyValue<>& value() const`          | Get current value (type-erased)    |
| `valid`              | `bool valid() const`                       | Check if value is valid            |
| `modified`           | `bool modified() const`                    | Check if modified at current time  |
| `last_modified_time` | `engine_time_t last_modified_time() const` | Get timestamp of last modification |
| `delta_value`        | `TsEventAny delta_value() const`           | Get event at current time          |

#### Internal

| Method         | Signature                            | Description                                             |
|----------------|--------------------------------------|---------------------------------------------------------|
| `notify`       | `void notify(engine_time_t t)`       | Called by impl when value changes (delegates to parent) |
| `current_time` | `engine_time_t current_time() const` | Get current time from parent                            |

### ParentNode Concept

```cpp
template <typename T>
concept ParentNode =
    std::derived_from<T, Notifiable> &&
    std::derived_from<T, CurrentTimeProvider>;
```

Parent nodes must implement:

| Trait                 | Method                                      | Description                      |
|-----------------------|---------------------------------------------|----------------------------------|
| `Notifiable`          | `void notify(engine_time_t et)`             | Receive notification to schedule |
| `CurrentTimeProvider` | `engine_time_t current_engine_time() const` | Provide current graph time       |

---

## Best Practices

1. **Always specify value types** - Declare types at construction for runtime validation:
   ```cpp
   TSOutput output(&parent, typeid(int));
   TSInput input(&parent, typeid(int));
   ```

2. **Always bind before marking active** - Inputs should be bound to an output before subscribing:
   ```cpp
   input.bind_output(&output);  // Bind first (validates types)
   input.mark_active();         // Then subscribe
   ```

3. **Use mark_passive when done** - Unsubscribe to prevent unnecessary notifications:
   ```cpp
   input.mark_passive();  // Stop receiving notifications
   ```

4. **Check valid() before accessing value** - Avoid reading invalid values:
   ```cpp
   if (input.valid()) {
       auto value = input.value();
   }
   ```

5. **Use visitors for type safety** - Prefer visitors over direct pointer access:
   ```cpp
   // Good
   input.value().visit_as<int>([](int v) { process(v); });

   // Less safe
   int* ptr = input.value().get_if<int>();
   if (ptr) process(*ptr);
   ```

6. **Match types across pipeline** - Ensure connected inputs/outputs have matching types to avoid runtime errors

7. **Preserve active state on rebind** - Binding automatically preserves subscription state (handled internally)

8. **One output, many inputs** - Outputs can have multiple bound inputs (fan-out pattern)

9. **Parent nodes must be valid** - Ensure parent pointers remain valid for the lifetime of the time series value

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
