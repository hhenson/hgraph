#pragma once

/**
 * @file queue_ops.h
 * @brief TypeOps implementation for Queue type.
 *
 * Queue is a FIFO data structure with optional max capacity.
 * - When unbounded (max_capacity = 0), grows dynamically
 * - When bounded and full, oldest element is evicted (like cyclic buffer)
 * - Supports push_back() and pop_front()
 */

#include <hgraph/types/value/type_meta.h>
#include <hgraph/types/value/value_view.h>

#include <nanobind/nanobind.h>
#include <nanobind/stl/string.h>

#include <cstring>
#include <deque>
#include <memory>
#include <stdexcept>
#include <string>

namespace nb = nanobind;

namespace hgraph::value {

// ============================================================================
// Queue Storage
// ============================================================================

/**
 * @brief Storage structure for queue using std::deque for FIFO ordering.
 *
 * Uses a slot-based design:
 * - `order`: std::deque holding slot indices in FIFO order
 * - `data`: contiguous byte storage for element data (slot pool)
 * - `free_slots`: recycled slot indices for reuse
 *
 * This provides O(1) push_back/pop_front via std::deque while maintaining
 * cache-friendly contiguous element storage.
 */
struct QueueStorage {
    std::deque<size_t> order;          // Slot indices in FIFO order (front = oldest)
    std::vector<std::byte> data;       // Element data pool (slots)
    std::vector<size_t> free_slots;    // Recycled slot indices
    size_t max_capacity{0};            // Max capacity (0 = unbounded)
    size_t slot_count{0};              // Total slots allocated in data

    QueueStorage() = default;

    // Allow move but not copy
    QueueStorage(QueueStorage&&) noexcept = default;
    QueueStorage& operator=(QueueStorage&&) noexcept = default;
    QueueStorage(const QueueStorage&) = delete;
    QueueStorage& operator=(const QueueStorage&) = delete;

    /// Get pointer to element at given slot index
    [[nodiscard]] void* slot_ptr(size_t slot_idx, size_t elem_size) {
        return data.data() + slot_idx * elem_size;
    }
    [[nodiscard]] const void* slot_ptr(size_t slot_idx, size_t elem_size) const {
        return data.data() + slot_idx * elem_size;
    }

    /// Current number of elements in queue
    [[nodiscard]] size_t size() const { return order.size(); }

    /// Check if queue has max capacity limit
    [[nodiscard]] bool is_bounded() const { return max_capacity > 0; }

    /// Check if bounded queue is full
    [[nodiscard]] bool is_full() const { return is_bounded() && size() >= max_capacity; }
};

// ============================================================================
// Queue Operations
// ============================================================================

/**
 * @brief Operations for Queue types (FIFO with optional max capacity).
 *
 * Key behaviors:
 * - Optional max capacity set at type creation (stored in schema->fixed_size)
 * - max_capacity = 0 means unbounded (can grow)
 * - push_back adds to tail, pop_front removes from head
 * - When bounded and full, push_back evicts the oldest element
 * - Logical index 0 = front (oldest), index size-1 = back (newest)
 *
 * Implementation uses slot-based storage:
 * - std::deque<size_t> maintains FIFO order of slot indices
 * - std::vector<std::byte> provides contiguous element storage
 * - Free slots are recycled to avoid unbounded memory growth
 */
struct QueueOps {

    // ========== Helper Functions ==========

    static size_t get_element_size(const TypeMeta* schema) {
        return schema->element_type ? schema->element_type->size : 0;
    }

    /// Get element pointer by logical index (0 = front, size-1 = back)
    static void* get_element_ptr(void* obj, size_t logical_index, const TypeMeta* schema) {
        auto* storage = static_cast<QueueStorage*>(obj);
        size_t elem_size = get_element_size(schema);
        size_t slot_idx = storage->order[logical_index];
        return storage->slot_ptr(slot_idx, elem_size);
    }

    static const void* get_element_ptr_const(const void* obj, size_t logical_index, const TypeMeta* schema) {
        auto* storage = static_cast<const QueueStorage*>(obj);
        size_t elem_size = get_element_size(schema);
        size_t slot_idx = storage->order[logical_index];
        return storage->slot_ptr(slot_idx, elem_size);
    }

    // ========== Slot Management ==========

    /**
     * @brief Allocate a new slot, reusing from free list if available.
     * Returns the slot index. Constructs the element at that slot.
     */
    static size_t allocate_slot(QueueStorage* storage, const TypeMeta* elem_type) {
        size_t elem_size = elem_type ? elem_type->size : 0;
        size_t slot_idx;

        if (!storage->free_slots.empty()) {
            // Reuse a free slot
            slot_idx = storage->free_slots.back();
            storage->free_slots.pop_back();
        } else {
            // Allocate new slot at end
            slot_idx = storage->slot_count;
            storage->slot_count++;

            // Expand data buffer
            size_t new_byte_size = storage->slot_count * elem_size;
            storage->data.resize(new_byte_size);

            // Construct element at new slot
            void* elem_ptr = storage->slot_ptr(slot_idx, elem_size);
            if (elem_type && elem_type->ops && elem_type->ops->construct) {
                elem_type->ops->construct(elem_ptr, elem_type);
            }
        }

        return slot_idx;
    }

    /**
     * @brief Free a slot, destructing its element and adding to free list.
     */
    static void free_slot(QueueStorage* storage, size_t slot_idx, const TypeMeta* elem_type) {
        size_t elem_size = elem_type ? elem_type->size : 0;

        // Destruct element at slot
        void* elem_ptr = storage->slot_ptr(slot_idx, elem_size);
        if (elem_type && elem_type->ops && elem_type->ops->destruct) {
            elem_type->ops->destruct(elem_ptr, elem_type);
        }

        // Re-construct for reuse (keeps slot in valid state)
        if (elem_type && elem_type->ops && elem_type->ops->construct) {
            elem_type->ops->construct(elem_ptr, elem_type);
        }

        // Add to free list
        storage->free_slots.push_back(slot_idx);
    }

    /**
     * @brief Destruct all active elements and clear the queue.
     */
    static void destruct_all_slots(QueueStorage* storage, const TypeMeta* elem_type) {
        size_t elem_size = elem_type ? elem_type->size : 0;

        // Destruct all allocated slots (active + free)
        for (size_t i = 0; i < storage->slot_count; ++i) {
            void* elem_ptr = storage->slot_ptr(i, elem_size);
            if (elem_type && elem_type->ops && elem_type->ops->destruct) {
                elem_type->ops->destruct(elem_ptr, elem_type);
            }
        }
    }

    // ========== Core Operations ==========

    static void construct(void* dst, const TypeMeta* schema) {
        auto* storage = new (dst) QueueStorage();
        storage->max_capacity = schema->fixed_size;  // 0 = unbounded
        // order, data, free_slots start empty - slots allocated on demand
    }

    static void destruct(void* obj, const TypeMeta* schema) {
        auto* storage = static_cast<QueueStorage*>(obj);
        destruct_all_slots(storage, schema->element_type);
        storage->~QueueStorage();  // Deque/vector destructors free memory
    }

    static void copy_assign(void* dst, const void* src, const TypeMeta* schema) {
        auto* dst_storage = static_cast<QueueStorage*>(dst);
        auto* src_storage = static_cast<const QueueStorage*>(src);
        const TypeMeta* elem_type = schema->element_type;
        size_t elem_size = elem_type ? elem_type->size : 0;

        // Clear destination
        for (size_t slot_idx : dst_storage->order) {
            free_slot(dst_storage, slot_idx, elem_type);
        }
        dst_storage->order.clear();

        // Copy elements from source
        for (size_t i = 0; i < src_storage->size(); ++i) {
            size_t new_slot = allocate_slot(dst_storage, elem_type);
            void* dst_elem = dst_storage->slot_ptr(new_slot, elem_size);
            const void* src_elem = get_element_ptr_const(src, i, schema);
            if (elem_type && elem_type->ops && elem_type->ops->copy_assign) {
                elem_type->ops->copy_assign(dst_elem, src_elem, elem_type);
            }
            dst_storage->order.push_back(new_slot);
        }

        dst_storage->max_capacity = src_storage->max_capacity;
    }

    static void move_assign(void* dst, void* src, const TypeMeta* schema) {
        auto* dst_storage = static_cast<QueueStorage*>(dst);
        auto* src_storage = static_cast<QueueStorage*>(src);
        const TypeMeta* elem_type = schema->element_type;

        // Destruct all dst slots
        destruct_all_slots(dst_storage, elem_type);

        // Move ownership from src to dst
        dst_storage->order = std::move(src_storage->order);
        dst_storage->data = std::move(src_storage->data);
        dst_storage->free_slots = std::move(src_storage->free_slots);
        dst_storage->max_capacity = src_storage->max_capacity;
        dst_storage->slot_count = src_storage->slot_count;

        // Reset source
        src_storage->slot_count = 0;
    }

    static void move_construct(void* dst, void* src, const TypeMeta* /*schema*/) {
        auto* src_storage = static_cast<QueueStorage*>(src);
        // Placement new with move: take ownership of all storage
        auto* dst_storage = new (dst) QueueStorage();
        dst_storage->order = std::move(src_storage->order);
        dst_storage->data = std::move(src_storage->data);
        dst_storage->free_slots = std::move(src_storage->free_slots);
        dst_storage->max_capacity = src_storage->max_capacity;
        dst_storage->slot_count = src_storage->slot_count;

        // Reset source
        src_storage->slot_count = 0;
    }

    static bool equals(const void* a, const void* b, const TypeMeta* schema) {
        auto* storage_a = static_cast<const QueueStorage*>(a);
        auto* storage_b = static_cast<const QueueStorage*>(b);
        const TypeMeta* elem_type = schema->element_type;

        // Must have same number of elements
        if (storage_a->size() != storage_b->size()) return false;

        // Compare elements in logical order
        for (size_t i = 0; i < storage_a->size(); ++i) {
            const void* elem_a = get_element_ptr_const(a, i, schema);
            const void* elem_b = get_element_ptr_const(b, i, schema);
            if (elem_type && elem_type->ops && elem_type->ops->equals) {
                if (!elem_type->ops->equals(elem_a, elem_b, elem_type)) {
                    return false;
                }
            }
        }
        return true;
    }

    static std::string to_string(const void* obj, const TypeMeta* schema) {
        auto* storage = static_cast<const QueueStorage*>(obj);
        const TypeMeta* elem_type = schema->element_type;
        std::string result = "Queue[";

        for (size_t i = 0; i < storage->size(); ++i) {
            if (i > 0) result += ", ";
            const void* elem_ptr = get_element_ptr_const(obj, i, schema);
            if (elem_type && elem_type->ops && elem_type->ops->to_string) {
                result += elem_type->ops->to_string(elem_ptr, elem_type);
            } else {
                result += "<null>";
            }
        }
        result += "]";
        return result;
    }

    // ========== Python Interop ==========

    static nb::object to_python(const void* obj, const TypeMeta* schema) {
        auto* storage = static_cast<const QueueStorage*>(obj);
        const TypeMeta* elem_type = schema->element_type;
        nb::list result;

        for (size_t i = 0; i < storage->size(); ++i) {
            const void* elem_ptr = get_element_ptr_const(obj, i, schema);
            if (elem_type && elem_type->ops && elem_type->ops->to_python) {
                result.append(elem_type->ops->to_python(elem_ptr, elem_type));
            } else {
                result.append(nb::none());
            }
        }
        return result;
    }

    static void from_python(void* dst, const nb::object& src, const TypeMeta* schema) {
        if (!nb::isinstance<nb::list>(src) && !nb::isinstance<nb::tuple>(src)) {
            throw std::runtime_error("Queue.from_python expects a list or tuple");
        }

        auto* storage = static_cast<QueueStorage*>(dst);
        const TypeMeta* elem_type = schema->element_type;
        nb::sequence seq = nb::cast<nb::sequence>(src);
        size_t src_len = nb::len(seq);
        size_t elem_size = elem_type ? elem_type->size : 0;

        // Clear the queue first
        for (size_t slot_idx : storage->order) {
            free_slot(storage, slot_idx, elem_type);
        }
        storage->order.clear();

        // Determine how many to add
        size_t copy_count = src_len;
        if (storage->max_capacity > 0 && copy_count > storage->max_capacity) {
            copy_count = storage->max_capacity;
        }

        // Push elements
        for (size_t i = 0; i < copy_count; ++i) {
            size_t slot_idx = allocate_slot(storage, elem_type);
            void* elem_ptr = storage->slot_ptr(slot_idx, elem_size);
            if (elem_type && elem_type->ops && elem_type->ops->from_python) {
                elem_type->ops->from_python(elem_ptr, seq[i], elem_type);
            }
            storage->order.push_back(slot_idx);
        }
    }

    // ========== Hashable Operations ==========

    static size_t hash(const void* obj, const TypeMeta* schema) {
        auto* storage = static_cast<const QueueStorage*>(obj);
        const TypeMeta* elem_type = schema->element_type;
        size_t result = 0;

        for (size_t i = 0; i < storage->size(); ++i) {
            const void* elem_ptr = get_element_ptr_const(obj, i, schema);
            if (elem_type && elem_type->ops && elem_type->ops->hash) {
                size_t elem_hash = elem_type->ops->hash(elem_ptr, elem_type);
                result ^= elem_hash + 0x9e3779b9 + (result << 6) + (result >> 2);
            }
        }
        return result;
    }

    // ========== Iterable Operations ==========

    static size_t size(const void* obj, const TypeMeta* /*schema*/) {
        auto* storage = static_cast<const QueueStorage*>(obj);
        return storage->size();
    }

    // ========== Indexable Operations ==========

    static const void* get_at(const void* obj, size_t index, const TypeMeta* schema) {
        auto* storage = static_cast<const QueueStorage*>(obj);
        if (index >= storage->size()) {
            throw std::out_of_range("Queue index out of range");
        }
        return get_element_ptr_const(obj, index, schema);
    }

    static void set_at(void* obj, size_t index, const void* value, const TypeMeta* schema) {
        auto* storage = static_cast<QueueStorage*>(obj);
        if (index >= storage->size()) {
            throw std::out_of_range("Queue index out of range");
        }
        void* elem_ptr = get_element_ptr(obj, index, schema);
        const TypeMeta* elem_type = schema->element_type;
        if (elem_type && elem_type->ops && elem_type->ops->copy_assign) {
            elem_type->ops->copy_assign(elem_ptr, value, elem_type);
        }
    }

    // ========== Queue-Specific Operations ==========

    /**
     * @brief Push a value to the back of the queue.
     *
     * If unbounded, allocates new slot as needed.
     * If bounded and full, evicts the oldest element (front) first.
     */
    static void push_back(void* obj, const void* value, const TypeMeta* schema) {
        auto* storage = static_cast<QueueStorage*>(obj);
        const TypeMeta* elem_type = schema->element_type;
        size_t elem_size = elem_type ? elem_type->size : 0;

        // If bounded and full, evict oldest first
        if (storage->is_full()) {
            // Free the front slot and remove from order
            size_t front_slot = storage->order.front();
            free_slot(storage, front_slot, elem_type);
            storage->order.pop_front();
        }

        // Allocate new slot and copy value
        size_t new_slot = allocate_slot(storage, elem_type);
        void* elem_ptr = storage->slot_ptr(new_slot, elem_size);
        if (elem_type && elem_type->ops && elem_type->ops->copy_assign) {
            elem_type->ops->copy_assign(elem_ptr, value, elem_type);
        }
        storage->order.push_back(new_slot);
    }

    /**
     * @brief Remove the front element from the queue.
     *
     * @throws std::out_of_range if the queue is empty
     */
    static void pop_front(void* obj, const TypeMeta* schema) {
        auto* storage = static_cast<QueueStorage*>(obj);
        const TypeMeta* elem_type = schema->element_type;

        if (storage->order.empty()) {
            throw std::out_of_range("pop_front on empty Queue");
        }

        // Free the front slot and remove from order
        size_t front_slot = storage->order.front();
        free_slot(storage, front_slot, elem_type);
        storage->order.pop_front();
    }

    /**
     * @brief Clear all elements from the queue.
     */
    static void clear(void* obj, const TypeMeta* schema) {
        auto* storage = static_cast<QueueStorage*>(obj);
        const TypeMeta* elem_type = schema->element_type;

        // Free all slots
        for (size_t slot_idx : storage->order) {
            free_slot(storage, slot_idx, elem_type);
        }
        storage->order.clear();
    }

    /**
     * @brief Get the max capacity (0 = unbounded).
     */
    static size_t max_capacity(const void* obj, const TypeMeta* /*schema*/) {
        auto* storage = static_cast<const QueueStorage*>(obj);
        return storage->max_capacity;
    }

    /// Get the operations vtable for queues
    static const TypeOps* ops() {
        static const TypeOps queue_ops = {
            &construct,
            &destruct,
            &copy_assign,
            &move_assign,
            &move_construct,
            &equals,
            &to_string,
            &to_python,
            &from_python,
            &hash,
            nullptr,   // less_than (queues not ordered)
            &size,
            &get_at,
            &set_at,
            nullptr,   // get_field (not a bundle)
            nullptr,   // set_field (not a bundle)
            nullptr,   // contains (not a set)
            nullptr,   // insert (not a set)
            nullptr,   // erase (not a set)
            nullptr,   // map_get (not a map)
            nullptr,   // map_set (not a map)
            nullptr,   // resize
            &clear,
        };
        return &queue_ops;
    }
};

} // namespace hgraph::value
