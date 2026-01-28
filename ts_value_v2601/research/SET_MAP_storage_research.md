# Set/Map Storage Research

**Created:** 2025-01-21
**Purpose:** Evaluate storage approaches for Sets and Maps against best-of-class designs

---

## Objectives

1. **Extensibility**: Set→Map with view-based casting; horizontal extension for time/observer with minimal overhead
2. **Efficient Iteration**: Fast traversal of live elements
3. **Memory Stability**: Pointers to keys/values remain valid; erased items accessible for one engine cycle
4. **DataFrame Conversion**: Low-cost conversion to columnar formats (e.g., Apache Arrow)

---

## Current Design Summary

From the existing design documents:

### TypeMeta (01_SCHEMA.md)
- Sets/Maps use `sizeof(container_header)` statically
- Elements allocated separately and managed by vtable operations
- Separate `set_ops` and `map_ops` in the `type_ops` union

### TSD (03_TIME_SERIES.md, 07_DELTA.md)
- Uses `MapStorage` with a key set
- Slot-indexed parallel arrays for `time_` and `observer_`
- Time schema: `List[engine_time_t]` parallel by slot index
- Observer schema: `List[ObserverList]` parallel by slot index

### TSS (07_DELTA.md)
- Uses `TrackedSetStorage` with separate `_value`, `_added`, `_removed` PlainValues
- Delta tracking via add/remove cancellation within same cycle

### Delta Tracking (07_DELTA.md lines 265-296)
- Death time markers on keys: `death_time == MIN_DT` means alive
- Erased items added to end of free list
- Reverse traversal of free list finds this tick's removals
- Dead keys preserved until slot reused (can still access key data for delta)

---

## Research Findings

### Memory Stability Approaches

#### 1. Slot Maps with Generational Indices

**Sources:**
- [Slot Map C++ Proposal (P0661R0)](https://www.open-std.org/jtc1/sc22/wg21/docs/papers/2017/p0661r0.pdf)
- [Game Engine handle_map](https://www.gamedev.net/tutorials/programming/general-and-gameplay-programming/game-engine-containers-handle_map-r4495/)
- [SergeyMakeev/SlotMap](https://github.com/SergeyMakeev/SlotMap)

**Characteristics:**
- Slots stored linearly in memory for fast iteration
- Constant time allocation and deallocation
- Zero overhead lookups
- Generation counter invalidates keys when slot reused
- Stable references to elements (pointers never move)

**Key Pattern:**
```cpp
struct SlotHandle {
    uint32_t index;      // Slot position
    uint32_t generation; // Validity check
};

// Validation: handle.generation == slots[handle.index].generation
```

#### 2. Node-Based Hash Maps

**Source:** [Swiss Tables Blog](https://abseil.io/blog/20180927-swisstables)

- `absl::node_hash_map`: Stable pointers via node indirection
- Slower than flat hash maps due to extra indirection
- Standard `std::unordered_map` mandates stable addressing (linked-list buckets)

**Trade-off:** Stability costs performance due to pointer chasing.

#### 3. Flat Hash Maps (Swiss Table, Robin Hood)

**Sources:**
- [Swiss Tables Blog](https://abseil.io/blog/20180927-swisstables)
- [Comprehensive C++ Hashmap Benchmarks 2022](https://martin.ankerl.com/2022/08/27/hashmap-bench-01/)
- [Robin Hood Hashing](https://github.com/martinus/robin-hood-hashing)

**Characteristics:**
- Fastest lookup and iteration (cache-friendly)
- **No pointer stability** - elements move on rehash
- Swiss Table uses SSE instructions for 16-way parallel probe
- Robin Hood reduces variance in probe lengths

**Conclusion:** Flat hash maps are fast but incompatible with our stability requirement.

### Iteration Patterns

#### Dense Array Pattern

**Source:** [Game Engine handle_map](https://www.gamedev.net/tutorials/programming/general-and-gameplay-programming/game-engine-containers-handle_map-r4495/)

- Maintain a dense array of live slot indices
- Iteration is O(live_count) not O(capacity)
- O(1) lookup via handle contains sparse set location
- Trade-off: Requires maintenance on add/remove

#### plf::colony

**Source:** [plf::colony](https://plflib.org/colony.htm)

- Unordered container with pointer/iterator validity
- Fast iteration/insertion/erasure
- More complex than slot map pattern

### DataFrame Conversion

#### Struct of Arrays (SoA) Layout

**Sources:**
- [Apache Arrow Row to Columnar Conversion](https://arrow.apache.org/docs/cpp/examples/row_columnar_conversion.html)
- [Sparrow: Modern C++ Arrow Implementation](https://johan-mabille.medium.com/sparrow-1f23817f6696)

**Key Insight:** Arrow data structures are "structs of arrays" composed of flat arrays. SoA layout enables zero-copy buffer wrapping.

**Array of Structs (AoS) - Current implicit design:**
```cpp
struct SlotEntry { key_data, generation, death_time };
std::vector<SlotEntry> slots_;  // Requires gather for Arrow
```

**Struct of Arrays (SoA) - Recommended:**
```cpp
std::vector<std::byte> keys_;           // Flat key array
std::vector<uint32_t> generations_;      // Parallel
std::vector<engine_time_t> death_times_; // Parallel
std::vector<std::byte> values_;          // Parallel (for maps)

// Arrow conversion: arrow::Buffer::Wrap(keys_.data(), keys_.size())
```

---

## Analysis Against Objectives

### Objective 1: Extensibility (Set→Map, Horizontal Extension)

**Current Gap:**
- `set_ops` and `map_ops` are separate vtable unions
- Set and Map are different `TypeKind` values
- View-casting between them is awkward

**Recommended Approach: Unified Slot-Based Storage**

A Set is conceptually a Map with no values (`value_meta_ == nullptr`). Unifying the storage enables:
- `SetView` from any `SlotStorage`
- `MapView` only when `value_meta_ != nullptr`
- Zero-cost cast: `MapView → SetView` (just ignore values)

**Horizontal Extension:**

Time and observer arrays are already parallel by slot index (03_TIME_SERIES.md). Making this explicit:

```cpp
struct TSSlotStorage : SlotStorage {
    Value time_;       // List[engine_time_t] - parallel by slot
    Value observer_;   // List[ObserverList] - parallel by slot
};
```

This matches the existing design but makes the slot-parallel nature explicit.

### Objective 2: Efficient Iteration

**Current:** `ViewRange` iterators yielding `View` per element. Good abstraction.

**Enhancements:**

| Pattern | Current | Recommended |
|---------|---------|-------------|
| Dense iteration | Contiguous slots | Add `live_count_` to skip validity checks |
| Sparse iteration | Implicit | Add dense/sparse iterator variants |
| SIMD iteration | Not addressed | Align slots to cache lines (64 bytes) |

**Dense Array Optimization:**

```cpp
struct SlotStorage {
    std::vector<uint32_t> dense_;  // Live slot indices only
    size_t live_count_;

    // Iteration: for (uint32_t slot : dense_) { ... }
    // O(live_count) not O(capacity)
};
```

### Objective 3: Memory Stability

**Current:** Death time markers preserve erased items. This is excellent.

**Enhancement: Generational Handles**

```cpp
struct SlotEntry {
    alignas(8) std::byte key_data_[KEY_SIZE];
    uint32_t generation_;        // Incremented on reuse
    engine_time_t death_time_;   // MIN_DT = alive
};

struct SlotHandle {
    uint32_t slot_index_;
    uint32_t generation_;
};
```

**Stability Guarantees:**
1. Pointer to key/value: Valid until slot reused (generation change)
2. Death time marker: Keeps data readable for one cycle
3. No rehashing movement: Slots never move, only index table rehashes

### Objective 4: DataFrame Conversion

**Current Gap:** Not explicitly addressed.

**Recommended:** SoA layout for direct Arrow buffer wrapping.

```cpp
// Arrow conversion path
arrow::Result<std::shared_ptr<arrow::Table>> to_arrow(const SlotStorage& s) {
    // Keys and values are already flat contiguous arrays
    auto key_buffer = arrow::Buffer::Wrap(s.keys_.data(), s.live_count_ * s.key_size_);
    auto val_buffer = arrow::Buffer::Wrap(s.values_.data(), s.live_count_ * s.val_size_);

    // Build validity bitmap from death_times (live slots)
    // ... create arrow::Table from buffers
}
```

---

## Recommended Storage Design

```cpp
// Unified slot-based storage supporting Set, Map, and TS extensions
struct SlotStorage {
    // === Core identity ===
    const TypeMeta* key_meta_;
    const TypeMeta* value_meta_;  // nullptr for Set

    // === SoA layout for Arrow-friendliness ===
    std::vector<std::byte> keys_;            // [key_size * capacity]
    std::vector<std::byte> values_;          // [value_size * capacity] or empty for Set
    std::vector<uint32_t> generations_;      // [capacity] - for handle validation
    std::vector<engine_time_t> death_times_; // [capacity] - MIN_DT = alive

    // === Index structure (Swiss Table or Robin Hood) ===
    std::vector<int32_t> index_;             // hash → slot (-1 = empty, -2 = tombstone)
    std::vector<uint8_t> ctrl_;              // Swiss table control bytes (optional)

    // === Dense tracking for iteration ===
    std::vector<uint32_t> dense_;            // Live slot indices only
    size_t live_count_;

    // === Free list for slot reuse ===
    std::vector<uint32_t> free_list_;        // LIFO - recent removals at end

    // === Accessors ===
    void* key_at(size_t slot) {
        return keys_.data() + slot * key_meta_->size();
    }
    void* value_at(size_t slot) {
        return values_.data() + slot * value_meta_->size();
    }
    bool is_alive(size_t slot) const {
        return death_times_[slot] == MIN_ENGINE_TIME;
    }
    bool is_set() const {
        return value_meta_ == nullptr;
    }
};
```

### View Layer

```cpp
// SetView works with any SlotStorage (ignores values)
class SetView {
    SlotStorage* storage_;
public:
    // Iteration over keys only
    ViewRange values() const;
    bool contains(View key) const;
    size_t size() const { return storage_->live_count_; }
};

// MapView requires value_meta_ != nullptr
class MapView {
    SlotStorage* storage_;
public:
    SetView as_set() const { return SetView{storage_}; }  // Zero-cost cast
    ViewRange keys() const;
    ViewPairRange items() const;
    View at(View key) const;
};
```

### TS Extension Layer

```cpp
// Time-series extension adds parallel time/observer arrays
struct TSSlotStorage {
    SlotStorage base_;

    // Parallel by slot index (same as existing design)
    Value time_;       // List[engine_time_t]
    Value observer_;   // List[ObserverList]

    engine_time_t slot_time(size_t slot) const {
        return time_.view().at(slot).as<engine_time_t>();
    }

    ObserverList& slot_observers(size_t slot) {
        return observer_.view().at(slot).as<ObserverList>();
    }
};
```

### Delta Tracking

Existing design (death time markers + free list reverse scan) works well with this structure:

```cpp
// Find keys removed this tick
auto get_removed_this_tick(const SlotStorage* storage, engine_time_t current_time) {
    std::vector<size_t> removed;
    for (auto it = storage->free_list_.rbegin(); it != storage->free_list_.rend(); ++it) {
        if (storage->death_times_[*it] == current_time) {
            removed.push_back(*it);
        } else {
            break;  // Earlier removals - stop
        }
    }
    return removed;
}
```

---

## Comparison Summary

| Aspect | Current Design | Revised Design | Benefit |
|--------|---------------|----------------|---------|
| **Set→Map cast** | Different TypeKind | Composition (Map HAS-A Set) | `as_set()` returns reference |
| **TSD→TSS cast** | Separate structures | Composition (TSD HAS-A Map HAS-A Set) | Shared KeySet, toll-free cast |
| **Time/Observer** | Parallel Value arrays | SlotObserver extensions | Same semantics, decoupled ownership |
| **Memory stability** | Death time markers | Generations (0 = dead, >0 = alive) | Single mechanism for liveness + stale detection |
| **Delta tracking** | Death time + free list scan | DeltaTracker observer | ts_ops concern only; no tombstoning in core |
| **DataFrame** | AoS implicit | SoA explicit per array | Direct `arrow::Buffer::Wrap()` |
| **Hash implementation** | Hand-coded Robin Hood | `ankerl::unordered_dense` | Proven, optimized, maintained |
| **Layer separation** | Monolithic | KeySet → type_ops → ts_ops | Clear responsibilities, testable |

---

## Open Design Questions

### 1. Hash Index Library Choice

**Decision:** Use `ankerl::unordered_dense` (already in codebase per 01_SCHEMA.md).

**Rationale:**
- Well-benchmarked, production-quality
- Handles hash collisions correctly
- No need to hand-code Robin Hood or Swiss Table
- Can swap implementation later if needed

### 2. Generation Width

**32-bit generation:** ~4 billion slot reuses before wrap-around.

**Consideration:** If a slot is reused once per millisecond, wrap occurs after ~49 days. For most applications this is sufficient. If concerned:
- Use 64-bit generation (doubles SlotHandle size)
- Or implement wrap-around detection

**Recommendation:** 32-bit is sufficient for typical use cases.

### 3. Delta Tracker: Copy vs Reference Removed Keys

**Options:**
- **Copy removed keys**: DeltaTracker copies key data on erase (small overhead, clean)
- **Reference with generation**: DeltaTracker stores (slot, generation) and reads from KeySet

**Trade-offs:**
- Copy: Simpler, no lifetime issues, small memory overhead
- Reference: Zero-copy, but must ensure slot not reused before tick ends

**Recommendation:** Copy for safety; revisit if memory becomes a concern.

### 4. Capacity Growth Strategy

**Decision:** Delegate to `ankerl::unordered_dense` for index growth.

For parallel arrays (values_, times_, etc.), use standard `std::vector` growth (typically 1.5x or 2x).

### 5. Observer Notification Order

**Question:** When multiple observers are registered, does order matter?

**Current design:** Vector of observers, notified in registration order.

**Consideration:** DeltaTracker should see erase BEFORE slot data is modified. Current design handles this since observers are notified before generation change.

---

## Revised Design: Protocol-Based Extension Architecture

### Design Principles

| Principle | Rationale |
|-----------|-----------|
| **Separation of concerns** | KeySet manages membership; extensions manage associated data |
| **Composition over inheritance** | Map HAS-A Set; TSD HAS-A TSS |
| **Protocol-based extension** | Handlers register for events, no tight coupling |
| **External memory ownership** | Extensions allocate their own arrays → toll-free Arrow/numpy |
| **Prefer standard implementations** | Use proven libraries (ankerl, abseil) over hand-coded algorithms |
| **Generation-based liveness** | Single mechanism for slot validity and stale reference detection |

### Layer Separation

```
┌─────────────────────────────────────────────────────────────┐
│                      ts_ops layer                           │
│  (TSS, TSD - time-series semantics, delta, observers)       │
├─────────────────────────────────────────────────────────────┤
│                     type_ops layer                          │
│  (Set, Map - value semantics, contains, iteration)          │
├─────────────────────────────────────────────────────────────┤
│                    KeySet (core)                            │
│  (slot management, hash index, membership, generation)      │
└─────────────────────────────────────────────────────────────┘
```

---

## Core Layer: KeySet

KeySet is the minimal foundation - it manages **membership only** (no values, no timestamps).

### Design Decisions

1. **No death_time in core**: Delta tracking is an extension concern, not core
2. **Generation for liveness**: `generation[slot] == 0` means dead; increment on reuse
3. **Standard hash implementation**: Use `ankerl::unordered_dense` for indexing
4. **Slot stability**: Keys never move; index table handles hash collisions

```cpp
// Core membership storage with slot-based stability
class KeySet {
public:
    // === Identity ===
    const TypeMeta* key_meta_;

    // === Key storage (SoA for Arrow compatibility) ===
    std::vector<std::byte> keys_;        // [key_size * capacity] - stable addresses
    std::vector<uint32_t> generations_;  // [capacity] - 0 = dead, >0 = alive (increments on reuse)

    // === Hash index (use standard implementation) ===
    // Maps hash(key) → slot_index; we delegate to ankerl::unordered_dense
    ankerl::unordered_dense::map<size_t, size_t> hash_to_slot_;

    // === Slot management ===
    std::vector<size_t> free_list_;      // Available slots for reuse
    size_t live_count_;                  // Number of alive slots
    size_t capacity_;                    // Total allocated slots

    // === Extension protocol ===
    std::vector<SlotObserver*> observers_;

    // === Core operations ===

    // Add key, returns (slot_index, was_new)
    std::pair<size_t, bool> insert(const void* key_data);

    // Remove key, returns true if existed
    bool erase(const void* key_data);

    // Find key, returns slot or nullopt
    std::optional<size_t> find(const void* key_data) const;

    // Check if slot is alive
    bool is_alive(size_t slot) const {
        return generations_[slot] > 0;
    }

    // Access key at slot (caller must verify alive)
    void* key_at(size_t slot) {
        return keys_.data() + slot * key_meta_->size();
    }

    // Validate a handle
    bool is_valid_handle(size_t slot, uint32_t gen) const {
        return slot < capacity_ && generations_[slot] == gen && gen > 0;
    }
};
```

### Slot Observer Protocol

Observers receive minimal events - just slot lifecycle:

```cpp
// Observer interface for slot events
struct SlotObserver {
    virtual ~SlotObserver() = default;

    // Capacity changed - resize parallel arrays
    virtual void on_capacity(size_t old_cap, size_t new_cap) = 0;

    // Slot allocated for new key
    virtual void on_insert(size_t slot) = 0;

    // Slot deallocated (key removed)
    virtual void on_erase(size_t slot) = 0;

    // All slots cleared
    virtual void on_clear() = 0;
};
```

### KeySet Implementation Notes

```cpp
std::pair<size_t, bool> KeySet::insert(const void* key_data) {
    size_t hash = key_meta_->ops().hash(key_data);

    // Check existing
    auto it = hash_to_slot_.find(hash);
    if (it != hash_to_slot_.end() && is_alive(it->second)) {
        if (key_meta_->ops().equals(key_at(it->second), key_data)) {
            return {it->second, false};  // Already exists
        }
        // Hash collision with different key - need collision handling
        // (ankerl handles this internally)
    }

    // Allocate slot
    size_t slot = allocate_slot();

    // Copy key data
    key_meta_->ops().copy(key_at(slot), key_data);

    // Update index
    hash_to_slot_[hash] = slot;

    // Notify observers
    for (auto* obs : observers_) obs->on_insert(slot);

    ++live_count_;
    return {slot, true};
}

bool KeySet::erase(const void* key_data) {
    auto slot_opt = find(key_data);
    if (!slot_opt) return false;

    size_t slot = *slot_opt;

    // Notify observers BEFORE marking dead (they may need to read data)
    for (auto* obs : observers_) obs->on_erase(slot);

    // Mark dead (generation stays same until reuse)
    generations_[slot] = 0;

    // Remove from index
    size_t hash = key_meta_->ops().hash(key_data);
    hash_to_slot_.erase(hash);

    // Add to free list
    free_list_.push_back(slot);

    --live_count_;
    return true;
}

size_t KeySet::allocate_slot() {
    if (!free_list_.empty()) {
        size_t slot = free_list_.back();
        free_list_.pop_back();
        generations_[slot]++;  // Increment generation on reuse
        return slot;
    }

    // Expand if needed
    if (capacity_ == keys_.size() / key_meta_->size()) {
        size_t old_cap = capacity_;
        size_t new_cap = std::max(size_t{8}, capacity_ * 2);
        keys_.resize(new_cap * key_meta_->size());
        generations_.resize(new_cap, 0);
        capacity_ = new_cap;

        for (auto* obs : observers_) obs->on_capacity(old_cap, new_cap);
    }

    size_t slot = capacity_++;
    generations_[slot] = 1;  // First generation
    return slot;
}
```

---

## type_ops Layer: Set and Map

### SetStorage (Thin wrapper over KeySet)

Set IS-A KeySet conceptually, but we wrap for API clarity:

```cpp
class SetStorage {
    KeySet keys_;

public:
    explicit SetStorage(const TypeMeta* element_meta)
        : keys_{element_meta} {}

    // === Set operations (map to type_ops::set_ops) ===
    bool contains(const void* elem) const { return keys_.find(elem).has_value(); }
    bool add(const void* elem) { return keys_.insert(elem).second; }
    bool remove(const void* elem) { return keys_.erase(elem); }
    size_t size() const { return keys_.live_count_; }
    void clear() { keys_.clear(); }

    // === Iteration ===
    // Yields Views over alive slots
    ViewRange values() const;

    // === Access to underlying KeySet ===
    KeySet& key_set() { return keys_; }
    const KeySet& key_set() const { return keys_; }

    // === Toll-free Arrow access ===
    const std::byte* data() const { return keys_.keys_.data(); }
    size_t capacity() const { return keys_.capacity_; }
};
```

### MapStorage (Composes SetStorage + ValueArray)

Map HAS-A Set plus a parallel value array:

```cpp
// Parallel value array that observes KeySet
class ValueArray : public SlotObserver {
    const TypeMeta* value_meta_;
    std::vector<std::byte> values_;

public:
    explicit ValueArray(const TypeMeta* value_meta)
        : value_meta_(value_meta) {}

    void on_capacity(size_t, size_t new_cap) override {
        values_.resize(new_cap * value_meta_->size());
    }

    void on_insert(size_t slot) override {
        value_meta_->ops().construct(value_at(slot));
    }

    void on_erase(size_t slot) override {
        value_meta_->ops().destroy(value_at(slot));
    }

    void on_clear() override {
        for (size_t i = 0; i < capacity(); ++i) {
            if (/* slot alive */) value_meta_->ops().destroy(value_at(i));
        }
    }

    void* value_at(size_t slot) {
        return values_.data() + slot * value_meta_->size();
    }

    size_t capacity() const { return values_.size() / value_meta_->size(); }

    // === Toll-free Arrow access ===
    std::byte* data() { return values_.data(); }
};

// Map composes Set + ValueArray
class MapStorage {
    SetStorage set_;
    ValueArray values_;

public:
    MapStorage(const TypeMeta* key_meta, const TypeMeta* value_meta)
        : set_(key_meta)
        , values_(value_meta)
    {
        set_.key_set().observers_.push_back(&values_);
    }

    // === Map operations (map to type_ops::map_ops) ===
    bool contains(const void* key) const { return set_.contains(key); }
    size_t size() const { return set_.size(); }

    void* at(const void* key) {
        auto slot = set_.key_set().find(key);
        if (!slot) throw std::out_of_range("key not found");
        return values_.value_at(*slot);
    }

    void* set_item(const void* key, const void* value) {
        auto [slot, was_new] = set_.key_set().insert(key);
        values_.value_meta_->ops().copy(values_.value_at(slot), value);
        return values_.value_at(slot);
    }

    bool remove(const void* key) {
        return set_.remove(key);
    }

    // === View as Set (toll-free) ===
    const SetStorage& as_set() const { return set_; }

    // === Iteration ===
    ViewRange keys() const { return set_.values(); }
    ViewPairRange items() const;  // (key, value) pairs

    // === Toll-free Arrow access ===
    const std::byte* key_data() const { return set_.data(); }
    std::byte* value_data() { return values_.data(); }
};
```

---

## ts_ops Layer: TSS and TSD

### Delta Tracking Extension

Delta tracking is a **ts_ops concern**, not core storage:

```cpp
// Tracks add/remove events per tick for delta queries
class DeltaTracker : public SlotObserver {
    std::vector<size_t> added_;
    std::vector<size_t> removed_;

    // For removed keys, we need to preserve data until tick ends
    // Store copies of removed keys (small overhead, clean separation)
    std::vector<std::byte> removed_keys_;
    const TypeMeta* key_meta_;

public:
    explicit DeltaTracker(const TypeMeta* key_meta)
        : key_meta_(key_meta) {}

    void on_capacity(size_t, size_t) override {}

    void on_insert(size_t slot) override {
        // If was removed this tick, cancel out
        auto it = std::find(removed_.begin(), removed_.end(), slot);
        if (it != removed_.end()) {
            removed_.erase(it);
            // Also remove from removed_keys_ (complex, skip for clarity)
        } else {
            added_.push_back(slot);
        }
    }

    void on_erase(size_t slot) override {
        // If was added this tick, cancel out
        auto it = std::find(added_.begin(), added_.end(), slot);
        if (it != added_.end()) {
            added_.erase(it);
        } else {
            removed_.push_back(slot);
            // Copy removed key data for delta access
            // (key still readable until slot reused, but we copy for safety)
        }
    }

    void on_clear() override {
        // All existing become removed (unless added this tick)
        added_.clear();
        // removed_ = all previously alive slots
    }

    // === Tick lifecycle ===
    void begin_tick() {
        added_.clear();
        removed_.clear();
        removed_keys_.clear();
    }

    // === Delta access ===
    const std::vector<size_t>& added() const { return added_; }
    const std::vector<size_t>& removed() const { return removed_; }
    bool was_added(size_t slot) const {
        return std::find(added_.begin(), added_.end(), slot) != added_.end();
    }
    bool was_removed(size_t slot) const {
        return std::find(removed_.begin(), removed_.end(), slot) != removed_.end();
    }
};
```

### Time Tracking Extension

```cpp
// Parallel timestamp array for modification tracking
class TimeArray : public SlotObserver {
    std::vector<engine_time_t> times_;

public:
    void on_capacity(size_t, size_t new_cap) override {
        times_.resize(new_cap, MIN_ENGINE_TIME);
    }

    void on_insert(size_t slot) override {
        times_[slot] = MIN_ENGINE_TIME;  // Invalid until set
    }

    void on_erase(size_t slot) override {
        // Keep time (may be queried for delta)
    }

    void on_clear() override {
        std::fill(times_.begin(), times_.end(), MIN_ENGINE_TIME);
    }

    engine_time_t at(size_t slot) const { return times_[slot]; }
    void set(size_t slot, engine_time_t t) { times_[slot] = t; }

    bool modified(size_t slot, engine_time_t current) const {
        return times_[slot] >= current;
    }

    bool valid(size_t slot) const {
        return times_[slot] != MIN_ENGINE_TIME;
    }

    // === Toll-free numpy access ===
    engine_time_t* data() { return times_.data(); }
};
```

### Observer List Extension

```cpp
// Parallel observer lists for TS notifications
class ObserverArray : public SlotObserver {
    std::vector<ObserverList> observers_;

public:
    void on_capacity(size_t, size_t new_cap) override {
        observers_.resize(new_cap);
    }

    void on_insert(size_t slot) override {
        observers_[slot].clear();
    }

    void on_erase(size_t slot) override {
        observers_[slot].notify_removed();
        observers_[slot].clear();
    }

    void on_clear() override {
        for (auto& obs : observers_) {
            obs.notify_removed();
            obs.clear();
        }
    }

    ObserverList& at(size_t slot) { return observers_[slot]; }
};
```

### TSSStorage (Composes SetStorage + TS extensions)

```cpp
class TSSStorage {
    SetStorage set_;
    TimeArray times_;
    ObserverArray observers_;
    DeltaTracker delta_;

public:
    explicit TSSStorage(const TypeMeta* element_meta)
        : set_(element_meta)
        , delta_(element_meta)
    {
        auto& ks = set_.key_set();
        ks.observers_.push_back(&times_);
        ks.observers_.push_back(&observers_);
        ks.observers_.push_back(&delta_);
    }

    // === Set operations (delegate to set_) ===
    bool contains(const void* elem) const { return set_.contains(elem); }
    size_t size() const { return set_.size(); }

    bool add(const void* elem, engine_time_t tick_time) {
        auto [slot, was_new] = set_.key_set().insert(elem);
        if (was_new || !times_.valid(slot)) {
            times_.set(slot, tick_time);
        }
        return was_new;
    }

    bool remove(const void* elem) {
        return set_.remove(elem);
    }

    // === Tick lifecycle ===
    void begin_tick() { delta_.begin_tick(); }

    // === Delta access (ts_ops::tss_ops) ===
    ViewRange added() const;   // Iterate added slots as Views
    ViewRange removed() const; // Iterate removed slots as Views
    bool was_added(const void* elem) const;
    bool was_removed(const void* elem) const;

    // === Time access ===
    bool modified(size_t slot, engine_time_t current) const {
        return times_.modified(slot, current);
    }

    // === View as Set ===
    const SetStorage& as_set() const { return set_; }
};
```

### TSDStorage (Composes MapStorage + TS extensions)

```cpp
class TSDStorage {
    MapStorage map_;
    TimeArray times_;
    ObserverArray observers_;
    DeltaTracker delta_;

public:
    TSDStorage(const TypeMeta* key_meta, const TypeMeta* value_meta)
        : map_(key_meta, value_meta)
        , delta_(key_meta)
    {
        auto& ks = map_.as_set().key_set();
        ks.observers_.push_back(&times_);
        ks.observers_.push_back(&observers_);
        ks.observers_.push_back(&delta_);
    }

    // === Map operations (delegate to map_) ===
    bool contains(const void* key) const { return map_.contains(key); }
    size_t size() const { return map_.size(); }
    void* at(const void* key) { return map_.at(key); }
    void* set_item(const void* key, const void* value, engine_time_t tick_time);
    bool remove(const void* key) { return map_.remove(key); }

    // === Tick lifecycle ===
    void begin_tick() { delta_.begin_tick(); }

    // === Delta access (ts_ops::tsd_ops) ===
    ViewRange added_keys() const;
    ViewRange removed_keys() const;
    ViewRange modified_keys(engine_time_t current) const;

    // === View as TSS (toll-free) ===
    // Returns key_set with time/observer/delta extensions
    const SetStorage& key_set() const { return map_.as_set(); }

    // === View as Map (toll-free) ===
    const MapStorage& as_map() const { return map_; }
};
```

---

## Composition Hierarchy

```
┌─────────────────────────────────────────────────────────────────┐
│                         TSDStorage                              │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │                      MapStorage                          │   │
│  │  ┌─────────────────────────────┐  ┌──────────────────┐  │   │
│  │  │        SetStorage           │  │   ValueArray     │  │   │
│  │  │  ┌───────────────────────┐  │  │   (values_[])    │  │   │
│  │  │  │       KeySet          │  │  └──────────────────┘  │   │
│  │  │  │  keys_[], generations_│  │                        │   │
│  │  │  │  hash_to_slot_        │  │                        │   │
│  │  │  └───────────────────────┘  │                        │   │
│  │  └─────────────────────────────┘                        │   │
│  └─────────────────────────────────────────────────────────┘   │
│  ┌──────────────┐ ┌───────────────┐ ┌────────────────┐         │
│  │  TimeArray   │ │ ObserverArray │ │  DeltaTracker  │         │
│  │  (times_[])  │ │ (observers_[])│ │ (added_/removed│         │
│  └──────────────┘ └───────────────┘ └────────────────┘         │
└─────────────────────────────────────────────────────────────────┘

TSSStorage = SetStorage + TimeArray + ObserverArray + DeltaTracker
TSDStorage = MapStorage + TimeArray + ObserverArray + DeltaTracker
         (MapStorage = SetStorage + ValueArray)
```

---

## Integration with type_ops / ts_ops

### type_ops (Value layer)

```cpp
struct set_ops {
    bool (*contains)(const void* storage, View elem);
    size_t (*size)(const void* storage);
    void (*add)(void* storage, View elem);
    bool (*remove)(void* storage, View elem);
    void (*clear)(void* storage);
    ViewRange (*values)(const void* storage);
};

struct map_ops {
    bool (*contains)(const void* storage, View key);
    size_t (*size)(const void* storage);
    View (*at)(void* storage, View key);
    void (*set_item)(void* storage, View key, View val);
    bool (*remove)(void* storage, View key);
    void (*clear)(void* storage);
    ViewRange (*keys)(const void* storage);
    ViewPairRange (*items)(const void* storage);
};
```

### ts_ops (Time-series layer)

```cpp
struct tss_ops {
    // Inherits set semantics via value_schema
    ViewRange (*added)(const void* storage);
    ViewRange (*removed)(const void* storage);
    bool (*was_added)(const void* storage, View elem);
    bool (*was_removed)(const void* storage, View elem);
};

struct tsd_ops {
    // Inherits map semantics via value_schema
    ViewRange (*added_keys)(const void* storage);
    ViewRange (*removed_keys)(const void* storage);
    ViewRange (*modified_keys)(const void* storage);
    // TSSView (*key_set)(const void* storage);  // View as TSS
};
```

---

## Toll-Free Arrow/NumPy Conversion

Each array is contiguous and independently owned:

```cpp
// Zero-copy Arrow conversion
auto to_arrow_table(const TSDStorage& tsd) {
    auto& ks = tsd.as_map().as_set().key_set();

    // Build validity bitmap from generations (0 = dead)
    auto validity = arrow::Buffer::FromVector(
        ks.generations_ | std::views::transform([](uint32_t g) { return g > 0; })
    );

    // Wrap key array
    auto keys = arrow::Buffer::Wrap(ks.keys_.data(), ks.keys_.size());

    // Wrap value array
    auto values = arrow::Buffer::Wrap(tsd.as_map().value_data(), ...);

    // Wrap time array
    auto times = arrow::Buffer::Wrap(tsd.times_.data(), ...);

    return arrow::Table::Make(schema, {keys, values, times}, validity);
}

// Zero-copy numpy view
auto times_as_numpy(const TSSStorage& tss) {
    return nb::ndarray<engine_time_t>(
        tss.times_.data(),
        {tss.times_.capacity()},
        nb::handle()
    );
}
```

---

## Standard Library Usage

| Component | Implementation | Rationale |
|-----------|---------------|-----------|
| Hash index | `ankerl::unordered_dense::map` | Fast, well-tested, handles collisions |
| Dynamic arrays | `std::vector` | Standard, predictable growth |
| Optional | `std::optional` | Standard null handling |
| Algorithms | `std::find`, `std::ranges` | Standard, optimized |
| Memory | `std::byte` | Type-safe raw storage |

**Explicit non-goals:**
- No hand-coded Robin Hood or Swiss Table
- No custom allocators (initially)
- No SIMD intrinsics (rely on compiler vectorization)

---

## Implementation Phases (Revised)

### Phase 1: KeySet Core
- KeySet with SoA key layout (`keys_[]`, `generations_[]`)
- Use `ankerl::unordered_dense::map` for hash indexing
- Slot allocation/deallocation with generation tracking
- SlotObserver protocol infrastructure
- Unit tests for membership operations

### Phase 2: type_ops Layer (Set/Map)
- SetStorage wrapping KeySet
- ValueArray as SlotObserver
- MapStorage composing SetStorage + ValueArray
- Integration with type_ops vtable
- Implement `set_ops` and `map_ops` functions
- Unit tests for Set/Map operations

### Phase 3: ts_ops Layer (TSS/TSD)
- TimeArray for modification tracking
- ObserverArray for notification lists
- DeltaTracker for add/remove tracking (no tombstoning needed)
- TSSStorage composing SetStorage + TS extensions
- TSDStorage composing MapStorage + TS extensions
- Integration with ts_ops vtable
- Unit tests for delta semantics

### Phase 4: View Integration
- SlotHandle for stable references with generation validation
- SetView/MapView using composed storage
- TSSView/TSDView with time-aware access
- Integration with existing ViewRange/ViewPairRange

### Phase 5: Arrow/NumPy Integration
- Zero-copy `arrow::Buffer::Wrap()` for each array
- Validity bitmap from `generations_[]` (> 0 = valid)
- Type-specific Arrow array builders
- numpy array views via nanobind

---

## Summary: Protocol-Based Layered Architecture

### Core Insight

**Separation of concerns across layers:**
1. **KeySet (core)**: Membership only - slots, indexing, generations
2. **type_ops (Set/Map)**: Value semantics - composition adds values to sets
3. **ts_ops (TSS/TSD)**: Time-series semantics - composition adds time/observers/delta

Each layer owns its own memory; upper layers observe lower layers via protocol.

### Composition Model

```
┌─────────────────────────────────────────────────────────────────┐
│                         TSDStorage                              │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │                      MapStorage                          │   │
│  │  ┌─────────────────────────────┐  ┌──────────────────┐  │   │
│  │  │        SetStorage           │  │   ValueArray     │  │   │
│  │  │  ┌───────────────────────┐  │  │   (values_[])    │  │   │
│  │  │  │       KeySet          │  │  └──────────────────┘  │   │
│  │  │  │  keys_[], generations_│  │                        │   │
│  │  │  │  ankerl::hash_map     │  │                        │   │
│  │  │  └───────────────────────┘  │                        │   │
│  │  └─────────────────────────────┘                        │   │
│  └─────────────────────────────────────────────────────────┘   │
│  ┌──────────────┐ ┌───────────────┐ ┌────────────────┐         │
│  │  TimeArray   │ │ ObserverArray │ │  DeltaTracker  │         │
│  │  (times_[])  │ │ (observers_[])│ │ (added_/removed│         │
│  └──────────────┘ └───────────────┘ └────────────────┘         │
└─────────────────────────────────────────────────────────────────┘

Set  = SetStorage(KeySet)
Map  = MapStorage(SetStorage + ValueArray)
TSS  = TSSStorage(SetStorage + TimeArray + ObserverArray + DeltaTracker)
TSD  = TSDStorage(MapStorage + TimeArray + ObserverArray + DeltaTracker)
```

### Key Properties

| Property | How Achieved |
|----------|--------------|
| **Set→Map casting** | `MapStorage.as_set()` returns contained `SetStorage&` |
| **Map→Set casting** | `MapStorage.as_set()` (same - values ignored) |
| **TSS→Set casting** | `TSSStorage.as_set()` returns contained `SetStorage&` |
| **TSD→Map casting** | `TSDStorage.as_map()` returns contained `MapStorage&` |
| **TSD→TSS casting** | `TSDStorage.key_set()` (conceptual - keys with TS extensions) |
| **Toll-free Arrow** | Each array is contiguous → `arrow::Buffer::Wrap()` |
| **Memory stability** | Slots never move; `generation > 0` = alive |
| **Stale ref detection** | Handle stores (slot, generation); validate on access |
| **Delta tracking** | DeltaTracker observes KeySet; ts_ops concern only |
| **Standard algorithms** | `ankerl::unordered_dense` for indexing; `std::vector` for arrays |

### Observer Protocol Summary

| Event | Trigger | Observer Response |
|-------|---------|-------------------|
| `on_capacity(old, new)` | KeySet resize | Resize parallel arrays |
| `on_insert(slot)` | New key added | Initialize slot data |
| `on_erase(slot)` | Key removed | Cleanup (may copy data for delta) |
| `on_clear()` | Bulk clear | Reset all slot data |

### Layer Responsibilities

| Layer | Owns | Observes | Provides |
|-------|------|----------|----------|
| **KeySet** | keys_, generations_, hash index | — | Membership, slot stability |
| **SetStorage** | — (wraps KeySet) | — | set_ops interface |
| **ValueArray** | values_[] | KeySet | Parallel value storage |
| **MapStorage** | — (composes Set + Values) | — | map_ops interface |
| **TimeArray** | times_[] | KeySet | Modification timestamps |
| **ObserverArray** | observers_[] | KeySet | Notification lists |
| **DeltaTracker** | added_[], removed_[] | KeySet | Per-tick deltas |
| **TSSStorage** | — (composes Set + TS extensions) | — | tss_ops interface |
| **TSDStorage** | — (composes Map + TS extensions) | — | tsd_ops interface |

---

## References

### Slot Maps and Generational Indices
- [Slot Map C++ Proposal (P0661R0)](https://www.open-std.org/jtc1/sc22/wg21/docs/papers/2017/p0661r0.pdf)
- [Game Engine handle_map Tutorial](https://www.gamedev.net/tutorials/programming/general-and-gameplay-programming/game-engine-containers-handle_map-r4495/)
- [SergeyMakeev/SlotMap](https://github.com/SergeyMakeev/SlotMap)
- [twiggler/slotmap](https://github.com/twiggler/slotmap)

### Hash Map Implementations
- [Swiss Tables Blog (Abseil)](https://abseil.io/blog/20180927-swisstables)
- [Comprehensive C++ Hashmap Benchmarks 2022](https://martin.ankerl.com/2022/08/27/hashmap-bench-01/)
- [Robin Hood Hashing](https://github.com/martinus/robin-hood-hashing)
- [Tessil/robin-map](https://github.com/Tessil/robin-map)
- [greg7mdp/parallel-hashmap](https://github.com/greg7mdp/parallel-hashmap)

### Columnar Storage and Arrow
- [Apache Arrow Row to Columnar Conversion](https://arrow.apache.org/docs/cpp/examples/row_columnar_conversion.html)
- [Sparrow: Modern C++ Arrow Implementation](https://johan-mabille.medium.com/sparrow-1f23817f6696)
- [Arrow Streaming Columnar (Wes McKinney)](https://wesmckinney.com/blog/arrow-streaming-columnar/)

### Related Design Documents
- `01_SCHEMA.md` - TypeMeta and type_ops structure
- `03_TIME_SERIES.md` - TSMeta and parallel time/observer arrays
- `07_DELTA.md` - Delta tracking and death time markers
