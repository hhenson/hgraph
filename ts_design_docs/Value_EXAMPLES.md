# Value Type System - Examples

**Version**: 2.0
**Date**: 2025-12-28
**Related**: [Value_DESIGN.md](Value_DESIGN.md) | [Value_USER_GUIDE.md](Value_USER_GUIDE.md)

---

## Table of Contents

1. [Policy-Based Python Caching](#1-policy-based-python-caching)
2. [CRTP Mixin Patterns](#2-crtp-mixin-patterns)
3. [TSValue Implementation](#3-tsvalue-implementation)
4. [Custom Extensions](#4-custom-extensions)
5. [Thread-Safe Patterns](#5-thread-safe-patterns)

---

## 1. Policy-Based Python Caching

The simplest way to add Python object caching using the policy template parameter.

### 1.1 Basic Usage

```cpp
#include <hgraph/value/value.h>
#include <nanobind/nanobind.h>

namespace nb = nanobind;
using namespace hgraph::value;

// Default - no caching, no overhead
// Note: Use large integers (>256) to avoid Python's small integer cache
Value<> v1(123456789);
nb::object py1 = v1.to_python();  // Direct conversion
nb::object py2 = v1.to_python();  // Another conversion
assert(!py1.is(py2));  // Different Python objects

// With caching - same API
Value<WithPythonCache> v2(123456789);
nb::object py3 = v2.to_python();  // Converts and caches
nb::object py4 = v2.to_python();  // Returns cached
assert(py3.is(py4));  // Same Python object!

// Using type alias
CachedValue v3(123456789);  // Same as Value<WithPythonCache>
```

### 1.2 Automatic Cache Invalidation

```cpp
CachedValue v(123456789);

nb::object py1 = v.to_python();  // Cached

// Get mutable view - automatically invalidates cache
ValueView view = v.view();
view.as<int64_t>() = 987654321;

nb::object py2 = v.to_python();  // Re-converts (cache was invalidated)
assert(!py1.is(py2));  // Different objects
assert(nb::cast<int64_t>(py2) == 987654321);
```

### 1.3 Cache Behavior with from_python

```cpp
CachedValue v(0);

nb::object py_value = nb::int_(123456789);
v.from_python(py_value);

// Cache is populated with the source object
nb::object py = v.to_python();
assert(py.is(py_value));  // Same object - no reconversion needed!
```

### 1.4 Size Comparison

```cpp
#include <type_traits>

// Verify zero overhead when not using caching
static_assert(sizeof(Value<NoCache>) ==
              sizeof(ValueStorage) + sizeof(TypeMeta*));

// With cache, adds only the optional<nb::object>
static_assert(sizeof(Value<WithPythonCache>) ==
              sizeof(ValueStorage) + sizeof(TypeMeta*) + sizeof(std::optional<nb::object>));
```

---

## 2. CRTP Mixin Patterns

For multiple extensions or custom behavior, use CRTP mixin chaining.

### 2.1 Core Building Blocks

```cpp
namespace hgraph::value {

// Core value data
class ValueCore {
public:
    void* data();
    const void* data() const;
    const TypeMeta* schema() const;
protected:
    ValueStorage _storage;
    const TypeMeta* _schema{nullptr};
};

// Base operations via CRTP
template<typename Derived>
class ValueOps : public ValueCore {
public:
    nb::object to_python() const {
        return static_cast<const Derived*>(this)->impl_to_python();
    }

    void from_python(const nb::object& src) {
        static_cast<Derived*>(this)->impl_from_python(src);
    }

protected:
    nb::object base_to_python() const {
        return _schema->ops->to_python(data(), _schema);
    }

    void base_from_python(const nb::object& src) {
        _schema->ops->from_python(data(), src, _schema);
    }
};

} // namespace hgraph::value
```

### 2.2 WithCache Mixin

```cpp
namespace hgraph::value {

template<typename Base>
class WithCache : public Base {
public:
    using Base::Base;

protected:
    nb::object impl_to_python() const {
        if (_cache) return *_cache;
        _cache = this->base_to_python();
        return *_cache;
    }

    void impl_from_python(const nb::object& src) {
        invalidate();
        this->base_from_python(src);
        _cache = src;  // Cache the source
    }

public:
    void invalidate() { _cache = std::nullopt; }
    bool has_cache() const { return _cache.has_value(); }

private:
    mutable std::optional<nb::object> _cache;
};

} // namespace hgraph::value
```

### 2.3 WithModTracking Mixin

```cpp
namespace hgraph::value {

template<typename Base>
class WithModTracking : public Base {
public:
    using Base::Base;

    void impl_from_python(const nb::object& src) {
        Base::impl_from_python(src);
        notify_modified();
    }

    void on_modified(std::function<void()> callback) {
        _callbacks.push_back(std::move(callback));
    }

    void clear_callbacks() {
        _callbacks.clear();
    }

private:
    void notify_modified() {
        for (auto& cb : _callbacks) cb();
    }

    std::vector<std::function<void()>> _callbacks;
};

} // namespace hgraph::value
```

### 2.4 Composing Mixins

```cpp
// Read right-to-left: ValueOps -> WithCache -> WithModTracking
using TrackedCachedValue = WithModTracking<WithCache<ValueOps<TrackedCachedValue>>>;

// Note: Use large integers (>256) to avoid Python's small integer cache
TrackedCachedValue v(123456789);

// Register callback
int modification_count = 0;
v.on_modified([&]{ ++modification_count; });

// Use caching
nb::object py1 = v.to_python();  // Converts + caches
nb::object py2 = v.to_python();  // Returns cached
assert(py1.is(py2));

// Modify triggers callback
v.from_python(nb::int_(987654321));
assert(modification_count == 1);

// Cache was invalidated
nb::object py3 = v.to_python();  // Re-converts
assert(!py1.is(py3));
```

---

## 3. TSValue Implementation

A complete TSValue type with caching and modification tracking.

### 3.1 Full Implementation

```cpp
namespace hgraph::value {

// Forward declare the final type
class TSValue;

// Define the mixin chain
using TSValueBase = WithModTracking<WithCache<ValueOps<TSValue>>>;

class TSValue : public TSValueBase {
public:
    using TSValueBase::TSValueBase;

    // Construct from schema
    explicit TSValue(const TypeMeta* schema)
        : TSValueBase() {
        this->_schema = schema;
        this->_storage = ValueStorage(schema);
    }

    // Construct from scalar value
    template<typename T>
    explicit TSValue(const T& val)
        : TSValueBase() {
        this->_schema = scalar_type_meta<T>();
        this->_storage = ValueStorage(val);
    }

    // Get mutable view - invalidates cache
    [[nodiscard]] ValueView view() {
        invalidate();  // From WithCache
        return ValueView(data(), schema());
    }

    // Get const view
    [[nodiscard]] ConstValueView const_view() const {
        return ConstValueView(data(), schema());
    }

    // Mark as modified externally
    void mark_modified() {
        invalidate();
        // Note: doesn't call callbacks - that's for from_python
    }
};

} // namespace hgraph::value
```

### 3.2 TSValue Usage

```cpp
TSValue price(100.0);

// Register observers
std::vector<double> history;
price.on_modified([&]{
    history.push_back(price.const_view().as<double>());
});

// Updates trigger callbacks
price.from_python(nb::float_(105.5));
price.from_python(nb::float_(103.2));
price.from_python(nb::float_(108.0));

// History: [105.5, 103.2, 108.0]
assert(history.size() == 3);

// Caching still works
nb::object py1 = price.to_python();
nb::object py2 = price.to_python();
assert(py1.is(py2));  // Cached
```

---

## 4. Custom Extensions

### 4.1 Validation Mixin

```cpp
template<typename Base>
class WithValidation : public Base {
public:
    using Base::Base;

    void impl_from_python(const nb::object& src) {
        // Pre-validation
        if (src.is_none()) {
            throw std::runtime_error("Cannot convert None to Value");
        }

        // Type-specific validation
        if (this->schema()->kind == TypeKind::Scalar) {
            validate_scalar(src, this->schema());
        }

        Base::impl_from_python(src);
    }

private:
    static void validate_scalar(const nb::object& src, const TypeMeta* schema) {
        // Example: ensure numeric values are in range
        if (schema == scalar_type_meta<int64_t>()) {
            int64_t val = nb::cast<int64_t>(src);
            if (val < 0) {
                throw std::runtime_error("Negative values not allowed");
            }
        }
    }
};

// Usage
using ValidatedValue = WithValidation<ValueOps<ValidatedValue>>;

ValidatedValue v(0);
try {
    v.from_python(nb::none());  // Throws: "Cannot convert None"
} catch (...) {}

try {
    v.from_python(nb::int_(-5));  // Throws: "Negative values not allowed"
} catch (...) {}
```

### 4.2 Logging Mixin

```cpp
template<typename Base>
class WithLogging : public Base {
public:
    using Base::Base;

protected:
    nb::object impl_to_python() const {
        auto result = Base::impl_to_python();
        std::cout << "[LOG] to_python: " << nb::str(result).c_str() << "\n";
        return result;
    }

    void impl_from_python(const nb::object& src) {
        std::cout << "[LOG] from_python: " << nb::str(src).c_str() << "\n";
        Base::impl_from_python(src);
    }
};

// Usage: logging + caching
using LoggedCachedValue = WithLogging<WithCache<ValueOps<LoggedCachedValue>>>;

LoggedCachedValue v(123456789);
v.to_python();  // Logs: "[LOG] to_python: 123456789"
v.to_python();  // No log (cached)

v.from_python(nb::int_(987654321));  // Logs: "[LOG] from_python: 987654321"
v.to_python();  // Logs: "[LOG] to_python: 987654321"
```

---

## 5. Thread-Safe Patterns

### 5.1 Thread-Safe Cache

```cpp
template<typename Base>
class ThreadSafeCache : public Base {
public:
    using Base::Base;

protected:
    nb::object impl_to_python() const {
        std::lock_guard<std::mutex> lock(_mutex);
        if (_cache) return *_cache;
        _cache = this->base_to_python();
        return *_cache;
    }

    void impl_from_python(const nb::object& src) {
        std::lock_guard<std::mutex> lock(_mutex);
        _cache = std::nullopt;
        this->base_from_python(src);
        _cache = src;
    }

public:
    void invalidate() {
        std::lock_guard<std::mutex> lock(_mutex);
        _cache = std::nullopt;
    }

private:
    mutable std::mutex _mutex;
    mutable std::optional<nb::object> _cache;
};

// Usage
using ThreadSafeValue = ThreadSafeCache<ValueOps<ThreadSafeValue>>;
```

### 5.2 Atomic Modification Tracking

```cpp
template<typename Base>
class AtomicModTracking : public Base {
public:
    using Base::Base;

    void impl_from_python(const nb::object& src) {
        Base::impl_from_python(src);
        _version.fetch_add(1, std::memory_order_release);
    }

    uint64_t version() const {
        return _version.load(std::memory_order_acquire);
    }

private:
    std::atomic<uint64_t> _version{0};
};
```

---

## Summary: Pattern Selection Guide

| Need | Use | Example |
|------|-----|---------|
| Just Python caching | Policy | `Value<WithPythonCache>` |
| Caching + callbacks | CRTP | `WithModTracking<WithCache<...>>` |
| Custom validation | CRTP | `WithValidation<...>` |
| Logging | CRTP | `WithLogging<...>` |
| Thread safety | CRTP | `ThreadSafeCache<...>` |
| No extensions | Either | `Value<>` or `PlainValue` |

**Key Principles:**

1. **Policy-based** for built-in single-concern extensions (e.g., `WithPythonCache`)
2. **CRTP mixins** for custom behavior or multiple extensions
3. **Both have zero runtime overhead** - compile-time dispatch
4. **Same API** regardless of extensions used

---

**End of Examples**
