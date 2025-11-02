# AnyValue: Type-Erased Value Container

## Overview

`AnyValue` is a type-erased container that can hold any copyable type with Small Buffer Optimization (SBO). It provides
a type-safe interface for storing and accessing heterogeneous values without dynamic polymorphism.

**Key Features:**

- **Small Buffer Optimization**: Small types stored inline, avoiding heap allocation
- **Type Safety**: Runtime type checking with `std::type_info`
- **Reference Semantics**: Support for borrowed references with automatic materialization
- **Visitor Pattern**: Type-safe and generic visitation mechanisms
- **Value Semantics**: Proper copy/move/equality/comparison operations

---

## Architecture

### Memory Layout

```cpp
class AnyValue {
    const VTable* vtable_;              // 8 bytes - dispatch table
    bool using_heap_;                   // 1 byte - storage location flag
    alignas(Align) unsigned char storage_[SBO];  // 16 bytes (default) - inline buffer
};
```

**Size**: 32 bytes total (with default SBO=16, Align=16)

### Storage Strategy

Values are stored either **inline** (SBO) or on the **heap**:

```
┌─────────────────────────────────────────┐
│ Small Value (≤16 bytes, align ≤16)     │
├─────────────────────────────────────────┤
│  vtable_: → VTable<T>                   │
│  using_heap_: false                     │
│  storage_: [actual T object]            │
└─────────────────────────────────────────┘

┌─────────────────────────────────────────┐
│ Large Value (>16 bytes or align >16)    │
├─────────────────────────────────────────┤
│  vtable_: → VTable<T>                   │
│  using_heap_: true                      │
│  storage_: [pointer to heap T*]         │
└─────────────────────────────────────────┘
```

**Decision Logic:**

```cpp
if (sizeof(T) <= SBO && alignof(T) <= Align) {
    // Inline: construct directly in storage_
    new (storage_) T(args...);
} else {
    // Heap: allocate and store pointer
    T* p = new T(args...);
    memcpy(storage_, &p, sizeof(T*));
}
```

---

## Basic Usage

### Construction and Emplacement

```cpp
#include <hgraph/types/v2/any_value.h>

// Empty container
AnyValue<> empty;
assert(!empty.has_value());

// Emplace values
AnyValue<> v;
v.emplace<int>(42);
v.emplace<std::string>("hello");
v.emplace<double>(3.14);
```

### Type-Safe Access

```cpp
AnyValue<> v;
v.emplace<int>(42);

// Type-safe pointer access
if (int* p = v.get_if<int>()) {
    *p = 100;  // Modify value
    std::cout << *p << "\n";  // 100
}

// Wrong type returns nullptr
if (double* p = v.get_if<double>()) {
    // Not executed - type mismatch
}
```

### Copy and Move

```cpp
AnyValue<> v1;
v1.emplace<std::string>("hello");

// Deep copy
AnyValue<> v2 = v1;
assert(v2.get_if<std::string>());
assert(*v2.get_if<std::string>() == "hello");

// Move (transfers ownership for heap-allocated, copies inline)
AnyValue<> v3 = std::move(v1);
assert(v3.has_value());
```

---

## Reference Semantics

`AnyValue` supports **borrowed references** to external objects. References are non-owning and remain valid when copied
or moved.

### Creating References

```cpp
int x = 42;
AnyValue<> ref;
ref.emplace_ref(x);  // Borrow x

// Access referent
assert(*ref.get_if<int>() == 42);

// Mutation of referent is visible
x = 100;
assert(*ref.get_if<int>() == 100);
```

### Reference Materialization

When copied or moved, references are **materialized** into owned values:

```cpp
std::string s = "original";
AnyValue<> ref;
ref.emplace_ref(s);

// Copy materializes
AnyValue<> owned = ref;
assert(!owned.is_reference());  // Now owned
assert(*owned.get_if<std::string>() == "original");

// Changes to referent don't affect materialized copy
s = "modified";
assert(*ref.get_if<std::string>() == "modified");
assert(*owned.get_if<std::string>() == "original");
```

### Explicit Materialization

```cpp
std::string s = "data";
AnyValue<> ref;
ref.emplace_ref(s);

ref.ensure_owned();  // Convert to owned value in-place
assert(!ref.is_reference());

s = "changed";
// ref is no longer affected
assert(*ref.get_if<std::string>() == "data");
```

---

## Storage Introspection

Query how values are stored for debugging and optimization:

```cpp
AnyValue<> v;

// Empty
assert(v.storage_size() == 0);
assert(!v.is_inline());
assert(!v.is_heap_allocated());

// Small value (inline/SBO)
v.emplace<int>(42);
assert(v.is_inline());
assert(v.storage_size() == 16);  // SBO buffer size

// Large value (heap)
v.emplace<std::array<char, 100>>();
assert(v.is_heap_allocated());
assert(v.storage_size() == sizeof(void*));  // Pointer size

// References
int x = 42;
v.emplace_ref(x);
assert(v.is_heap_allocated());  // Uses "heap" flag
assert(v.is_reference());
```

**Use Cases:**

- **Debugging**: Verify expected storage optimization
- **Profiling**: Track inline vs heap allocation rates
- **Tuning**: Guide SBO buffer size decisions

---

## Visitor Pattern

The visitor pattern provides type-safe and generic ways to handle contained values.

### Type-Safe Visitation: `visit_as<T>()`

When you know the possible type, use `visit_as<T>()`:

```cpp
AnyValue<> v;
v.emplace<int64_t>(42);

// Const visitation
int64_t result = 0;
bool visited = v.visit_as<int64_t>([&result](int64_t val) {
    result = val * 2;
});
assert(visited);      // true - type matched
assert(result == 84);

// Type mismatch returns false
visited = v.visit_as<double>([](double) {
    // Not called
});
assert(!visited);  // false - no match
```

### Mutable Visitation

```cpp
AnyValue<> v;
v.emplace<int64_t>(42);

// Modify value through visitor
v.visit_as<int64_t>([](int64_t& val) {
    val = 100;
});

assert(*v.get_if<int64_t>() == 100);
```

### Multi-Type Dispatch Pattern

```cpp
AnyValue<> v = /* ... */;

// Try multiple types
if (v.visit_as<int64_t>([](int64_t i) {
    std::cout << "Integer: " << i << "\n";
})) {
    // Handled as int64_t
} else if (v.visit_as<double>([](double d) {
    std::cout << "Double: " << d << "\n";
})) {
    // Handled as double
} else if (v.visit_as<std::string>([](const std::string& s) {
    std::cout << "String: " << s << "\n";
})) {
    // Handled as string
} else {
    std::cout << "Unknown type\n";
}
```

### Generic Introspection: `visit_untyped()`

For generic algorithms, debugging, or serialization:

```cpp
AnyValue<> v;
v.emplace<std::string>("hello");

// Generic visitation with type_info
v.visit_untyped([](const void* ptr, const std::type_info& ti) {
    std::cout << "Type: " << ti.name() << "\n";

    // Dynamic dispatch based on type_info
    if (ti == typeid(int64_t)) {
        auto val = *static_cast<const int64_t*>(ptr);
        std::cout << "int64_t: " << val << "\n";
    } else if (ti == typeid(std::string)) {
        auto& val = *static_cast<const std::string*>(ptr);
        std::cout << "string: " << val << "\n";
    } else {
        std::cout << "Unknown type\n";
    }
});
```

### Visitor Pattern Use Cases

#### 1. Generic Serialization

```cpp
void serialize(const AnyValue<>& v, std::ostream& out) {
    v.visit_untyped([&](const void* ptr, const std::type_info& ti) {
        if (ti == typeid(int64_t)) {
            out << *static_cast<const int64_t*>(ptr);
        } else if (ti == typeid(double)) {
            out << *static_cast<const double*>(ptr);
        } else if (ti == typeid(std::string)) {
            out << '"' << *static_cast<const std::string*>(ptr) << '"';
        }
        // ... more types
    });
}
```

#### 2. Type-Safe Processing Pipeline

```cpp
class ValueProcessor {
public:
    void process(const AnyValue<>& v) {
        // Try each handler in order
        if (v.visit_as<int64_t>([this](int64_t i) { handle_integer(i); }))
            return;
        if (v.visit_as<double>([this](double d) { handle_floating(d); }))
            return;
        if (v.visit_as<std::string>([this](const std::string& s) { handle_string(s); }))
            return;

        handle_unknown();
    }

private:
    void handle_integer(int64_t) { /* ... */ }
    void handle_floating(double) { /* ... */ }
    void handle_string(const std::string&) { /* ... */ }
    void handle_unknown() { /* ... */ }
};
```

#### 3. Debug Introspection

```cpp
void debug_print(const AnyValue<>& v) {
    if (!v.has_value()) {
        std::cout << "Empty\n";
        return;
    }

    std::cout << "Storage: "
              << (v.is_inline() ? "inline" : "heap") << ", "
              << v.storage_size() << " bytes, "
              << (v.is_reference() ? "ref" : "owned") << "\n";

    std::cout << "Value: ";
    v.visit_untyped([](const void* ptr, const std::type_info& ti) {
        std::cout << "type=" << ti.name();
        // Could print value based on type
    });
    std::cout << "\n";
}
```

#### 4. Accumulator Pattern

```cpp
double sum_numeric(const std::vector<AnyValue<>>& values) {
    double sum = 0.0;

    for (const auto& v : values) {
        v.visit_as<int64_t>([&sum](int64_t i) { sum += i; });
        v.visit_as<double>([&sum](double d) { sum += d; });
        v.visit_as<float>([&sum](float f) { sum += f; });
    }

    return sum;
}
```

---

## Comparison Operations

### Equality

Values are equal if they have the same type and equal values. Equality is implemented as a friend operator with vtable
dispatch.

```cpp
AnyValue<> v1, v2;
v1.emplace<int>(42);
v2.emplace<int>(42);
assert(v1 == v2);

v2.emplace<int>(43);
assert(v1 != v2);

// Different types are never equal
v2.emplace<double>(42.0);
assert(v1 != v2);

// Empty values are equal
AnyValue<> e1, e2;
assert(e1 == e2);
```

### Less-Than Ordering

Ordering is supported when the contained type supports `operator<`:

```cpp
AnyValue<> v1, v2;
v1.emplace<int>(10);
v2.emplace<int>(20);
assert(v1 < v2);

v1.emplace<std::string>("abc");
v2.emplace<std::string>("xyz");
assert(v1 < v2);
```

**Constraints:**

- Both values must have the same type (throws `std::runtime_error` otherwise)
- Both values must be non-empty (throws `std::runtime_error` otherwise)
- Type must support `operator<` (throws `std::runtime_error` otherwise)

```cpp
AnyValue<> v1, v2;
v1.emplace<int>(10);
v2.emplace<double>(20.0);

try {
    bool result = v1 < v2;  // Type mismatch
} catch (std::runtime_error& e) {
    // "AnyValue: operator< type mismatch"
}
```

---

## Hashing

`AnyValue` is hashable when the contained type is hashable. A std::hash specialization is provided for use in STL
containers.

```cpp
AnyValue<> v;
v.emplace<int64_t>(42);

std::size_t h1 = v.hash_code();
std::size_t h2 = std::hash<int64_t>{}(42);
assert(h1 == h2);

// Can be used in unordered containers
std::unordered_set<AnyValue<>> set;
set.insert(v);

// std::hash specialization provided
std::hash<AnyValue<>>{}(v);
```

**Hash Behavior:**

- Empty values hash to 0
- Uses `std::hash<T>` if available for type `T`
- Falls back to combined type-id and pointer hash using golden ratio constant (2^64 / phi)

---

## String Representation

The `to_string()` function provides a string representation for logging and debugging:

```cpp
AnyValue<> v;
v.emplace<int64_t>(42);
std::string s = to_string(v);  // "42"

AnyValue<> empty;
std::string s2 = to_string(empty);  // "<empty>"
```

---

## Complete Example

```cpp
#include <hgraph/types/v2/any_value.h>
#include <iostream>
#include <vector>

// Generic container processing
class ValueContainer {
    std::vector<AnyValue<>> values_;

public:
    template<typename T>
    void add(T&& value) {
        AnyValue<> v;
        v.emplace<std::decay_t<T>>(std::forward<T>(value));
        values_.push_back(std::move(v));
    }

    void print_all() const {
        for (const auto& v : values_) {
            std::cout << "Value: ";

            // Type-safe visitation
            if (v.visit_as<int64_t>([](int64_t i) {
                std::cout << "int64_t(" << i << ")";
            })) {}
            else if (v.visit_as<double>([](double d) {
                std::cout << "double(" << d << ")";
            })) {}
            else if (v.visit_as<std::string>([](const std::string& s) {
                std::cout << "string(\"" << s << "\")";
            })) {}
            else {
                // Generic fallback
                v.visit_untyped([](const void*, const std::type_info& ti) {
                    std::cout << "unknown(" << ti.name() << ")";
                });
            }

            std::cout << " ["
                      << (v.is_inline() ? "inline" : "heap")
                      << ", " << v.storage_size() << " bytes]\n";
        }
    }

    double sum_numeric() const {
        double sum = 0.0;
        for (const auto& v : values_) {
            v.visit_as<int64_t>([&](int64_t i) { sum += i; });
            v.visit_as<double>([&](double d) { sum += d; });
        }
        return sum;
    }
};

int main() {
    ValueContainer container;

    container.add(42);
    container.add(3.14);
    container.add(std::string("hello"));
    container.add(100L);

    container.print_all();
    std::cout << "Sum of numeric values: " << container.sum_numeric() << "\n";

    return 0;
}
```

**Output:**

```
Value: int64_t(42) [inline, 16 bytes]
Value: double(3.14) [inline, 16 bytes]
Value: string("hello") [inline, 16 bytes]
Value: int64_t(100) [inline, 16 bytes]
Sum of numeric values: 145.14
```

---

## API Reference

### Core Methods

| Method                | Description                                        |
|-----------------------|----------------------------------------------------|
| `has_value()`         | Check if container holds a value                   |
| `type()`              | Get TypeId of contained value                      |
| `reset()`             | Clear the contained value                          |
| `emplace<T>(args...)` | Construct value of type T in-place                 |
| `emplace_ref<T>(ref)` | Store borrowed reference to external object        |
| `get_if<T>()`         | Get pointer to value if type matches, else nullptr |
| `ensure_owned()`      | Convert reference to owned value in-place          |

### Storage Introspection

| Method                | Description                                       |
|-----------------------|---------------------------------------------------|
| `storage_size()`      | Returns bytes used (0, pointer size, or SBO size) |
| `is_inline()`         | True if using Small Buffer Optimization           |
| `is_heap_allocated()` | True if heap-allocated                            |
| `is_reference()`      | True if holds borrowed reference                  |

### Visitor Pattern

| Method                   | Description                                        |
|--------------------------|----------------------------------------------------|
| `visit_as<T>(visitor)`   | Type-safe visitation, returns true if type matches |
| `visit_untyped(visitor)` | Generic visitation with type_info                  |

### Comparison and Hashing

| Method/Operator             | Description                                                   |
|-----------------------------|---------------------------------------------------------------|
| `operator==` / `operator!=` | Equality comparison (type + value)                            |
| `operator<`                 | Less-than ordering (throws if type mismatch or not supported) |
| `hash_code()`               | Get hash value of contained value                             |
| `std::hash<AnyValue<>>`     | STL hash specialization                                       |

### Utility

| Function                | Description                       |
|-------------------------|-----------------------------------|
| `to_string(AnyValue<>)` | String representation for logging |

---

## See Also

- `ts_event.h` - Time series event types using `AnyValue`
- `nanobind::object` - Python object wrapper (same size as SBO buffer)
- `std::any` - Standard library type-erased container (no SBO)
- `std::variant` - Type-safe union (compile-time type list)
