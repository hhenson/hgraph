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
#include <memory>
#include <stdexcept>
#include <string>

namespace nb = nanobind;

namespace hgraph::value {

// ============================================================================
// Queue Storage
// ============================================================================

/**
 * @brief Storage structure for queue.
 *
 * Uses a circular buffer internally. When bounded, behaves like CyclicBuffer
 * when full. When unbounded, grows dynamically.
 */
struct QueueStorage {
    void* data{nullptr};           // Element buffer (circular)
    size_t allocated_capacity{0};  // Currently allocated buffer size
    size_t max_capacity{0};        // Max capacity (0 = unbounded)
    size_t size{0};                // Current element count
    size_t head{0};                // Index of front element

    QueueStorage() = default;

    QueueStorage(const QueueStorage&) = delete;
    QueueStorage& operator=(const QueueStorage&) = delete;
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
 */
struct QueueOps {
    // Initial capacity for unbounded queues
    static constexpr size_t INITIAL_CAPACITY = 4;
    static constexpr size_t GROWTH_FACTOR = 2;

    // ========== Helper Functions ==========

    static size_t get_element_size(const TypeMeta* schema) {
        return schema->element_type ? schema->element_type->size : 0;
    }

    /// Convert logical index to physical storage index
    static size_t to_physical_index(const QueueStorage* storage, size_t logical_index) {
        return (storage->head + logical_index) % storage->allocated_capacity;
    }

    static void* get_element_ptr(void* obj, size_t logical_index, const TypeMeta* schema) {
        auto* storage = static_cast<QueueStorage*>(obj);
        size_t elem_size = get_element_size(schema);
        size_t physical = to_physical_index(storage, logical_index);
        return static_cast<char*>(storage->data) + physical * elem_size;
    }

    static const void* get_element_ptr_const(const void* obj, size_t logical_index, const TypeMeta* schema) {
        auto* storage = static_cast<const QueueStorage*>(obj);
        size_t elem_size = get_element_size(schema);
        size_t physical = to_physical_index(storage, logical_index);
        return static_cast<const char*>(storage->data) + physical * elem_size;
    }

    // ========== Memory Management ==========

    /**
     * @brief Allocate buffer and construct elements.
     */
    static void allocate_buffer(QueueStorage* storage, size_t capacity, const TypeMeta* elem_type) {
        size_t elem_size = elem_type ? elem_type->size : 0;
        if (capacity > 0 && elem_size > 0) {
            storage->data = std::malloc(capacity * elem_size);
            if (!storage->data) {
                throw std::bad_alloc();
            }
            storage->allocated_capacity = capacity;

            // Construct all elements in the buffer
            for (size_t i = 0; i < capacity; ++i) {
                void* elem_ptr = static_cast<char*>(storage->data) + i * elem_size;
                if (elem_type->ops && elem_type->ops->construct) {
                    elem_type->ops->construct(elem_ptr, elem_type);
                }
            }
        }
    }

    /**
     * @brief Free buffer and destruct elements.
     */
    static void free_buffer(QueueStorage* storage, const TypeMeta* elem_type) {
        if (storage->data && elem_type) {
            size_t elem_size = elem_type->size;
            for (size_t i = 0; i < storage->allocated_capacity; ++i) {
                void* elem_ptr = static_cast<char*>(storage->data) + i * elem_size;
                if (elem_type->ops && elem_type->ops->destruct) {
                    elem_type->ops->destruct(elem_ptr, elem_type);
                }
            }
            std::free(storage->data);
        }
        storage->data = nullptr;
        storage->allocated_capacity = 0;
    }

    /**
     * @brief Grow the buffer to accommodate more elements.
     */
    static void grow_buffer(QueueStorage* storage, const TypeMeta* schema) {
        const TypeMeta* elem_type = schema->element_type;
        size_t elem_size = elem_type ? elem_type->size : 0;
        if (elem_size == 0) return;

        size_t new_capacity = storage->allocated_capacity == 0
            ? INITIAL_CAPACITY
            : storage->allocated_capacity * GROWTH_FACTOR;

        // If bounded, don't grow beyond max_capacity
        if (storage->max_capacity > 0 && new_capacity > storage->max_capacity) {
            new_capacity = storage->max_capacity;
        }

        // Allocate new buffer
        void* new_data = std::malloc(new_capacity * elem_size);
        if (!new_data) {
            throw std::bad_alloc();
        }

        // Construct all elements in new buffer
        for (size_t i = 0; i < new_capacity; ++i) {
            void* elem_ptr = static_cast<char*>(new_data) + i * elem_size;
            if (elem_type->ops && elem_type->ops->construct) {
                elem_type->ops->construct(elem_ptr, elem_type);
            }
        }

        // Copy existing elements in logical order to new buffer (linearized)
        for (size_t i = 0; i < storage->size; ++i) {
            const void* src = get_element_ptr_const(storage, i, schema);
            void* dst = static_cast<char*>(new_data) + i * elem_size;
            if (elem_type->ops && elem_type->ops->copy_assign) {
                elem_type->ops->copy_assign(dst, src, elem_type);
            }
        }

        // Free old buffer
        free_buffer(storage, elem_type);

        // Update storage
        storage->data = new_data;
        storage->allocated_capacity = new_capacity;
        storage->head = 0;  // Linearized, head is now at 0
    }

    // ========== Core Operations ==========

    static void construct(void* dst, const TypeMeta* schema) {
        auto* storage = new (dst) QueueStorage();
        storage->max_capacity = schema->fixed_size;  // 0 = unbounded
        storage->size = 0;
        storage->head = 0;

        // For bounded queues, pre-allocate to max capacity
        // For unbounded, start with initial capacity
        const TypeMeta* elem_type = schema->element_type;
        if (storage->max_capacity > 0) {
            allocate_buffer(storage, storage->max_capacity, elem_type);
        } else {
            allocate_buffer(storage, INITIAL_CAPACITY, elem_type);
        }
    }

    static void destruct(void* obj, const TypeMeta* schema) {
        auto* storage = static_cast<QueueStorage*>(obj);
        free_buffer(storage, schema->element_type);
        storage->~QueueStorage();
    }

    static void copy_assign(void* dst, const void* src, const TypeMeta* schema) {
        auto* dst_storage = static_cast<QueueStorage*>(dst);
        auto* src_storage = static_cast<const QueueStorage*>(src);
        const TypeMeta* elem_type = schema->element_type;
        size_t elem_size = elem_type ? elem_type->size : 0;

        // Ensure dst has enough capacity
        if (dst_storage->allocated_capacity < src_storage->size) {
            free_buffer(dst_storage, elem_type);
            allocate_buffer(dst_storage, src_storage->allocated_capacity, elem_type);
        }

        // Copy elements in logical order (linearized in dst)
        for (size_t i = 0; i < src_storage->size; ++i) {
            const void* src_elem = get_element_ptr_const(src, i, schema);
            void* dst_elem = static_cast<char*>(dst_storage->data) + i * elem_size;
            if (elem_type && elem_type->ops && elem_type->ops->copy_assign) {
                elem_type->ops->copy_assign(dst_elem, src_elem, elem_type);
            }
        }

        dst_storage->size = src_storage->size;
        dst_storage->head = 0;  // Linearized
        dst_storage->max_capacity = src_storage->max_capacity;
    }

    static void move_assign(void* dst, void* src, const TypeMeta* schema) {
        auto* dst_storage = static_cast<QueueStorage*>(dst);
        auto* src_storage = static_cast<QueueStorage*>(src);
        const TypeMeta* elem_type = schema->element_type;

        // Free dst buffer
        free_buffer(dst_storage, elem_type);

        // Move ownership from src to dst
        dst_storage->data = src_storage->data;
        dst_storage->allocated_capacity = src_storage->allocated_capacity;
        dst_storage->max_capacity = src_storage->max_capacity;
        dst_storage->size = src_storage->size;
        dst_storage->head = src_storage->head;

        // Reset source
        src_storage->data = nullptr;
        src_storage->allocated_capacity = 0;
        src_storage->size = 0;
        src_storage->head = 0;
    }

    static bool equals(const void* a, const void* b, const TypeMeta* schema) {
        auto* storage_a = static_cast<const QueueStorage*>(a);
        auto* storage_b = static_cast<const QueueStorage*>(b);
        const TypeMeta* elem_type = schema->element_type;

        // Must have same number of elements
        if (storage_a->size != storage_b->size) return false;

        // Compare elements in logical order
        for (size_t i = 0; i < storage_a->size; ++i) {
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

        for (size_t i = 0; i < storage->size; ++i) {
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

        for (size_t i = 0; i < storage->size; ++i) {
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

        // Clear the queue first
        storage->size = 0;
        storage->head = 0;

        // Determine how many to add
        size_t copy_count = src_len;
        if (storage->max_capacity > 0 && copy_count > storage->max_capacity) {
            copy_count = storage->max_capacity;
        }

        // Ensure we have enough capacity
        if (storage->allocated_capacity < copy_count) {
            free_buffer(storage, elem_type);
            allocate_buffer(storage, copy_count, elem_type);
        }

        // Push elements
        size_t elem_size = elem_type ? elem_type->size : 0;
        for (size_t i = 0; i < copy_count; ++i) {
            void* elem_ptr = static_cast<char*>(storage->data) + i * elem_size;
            if (elem_type && elem_type->ops && elem_type->ops->from_python) {
                elem_type->ops->from_python(elem_ptr, seq[i], elem_type);
            }
            storage->size++;
        }
    }

    // ========== Hashable Operations ==========

    static size_t hash(const void* obj, const TypeMeta* schema) {
        auto* storage = static_cast<const QueueStorage*>(obj);
        const TypeMeta* elem_type = schema->element_type;
        size_t result = 0;

        for (size_t i = 0; i < storage->size; ++i) {
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
        return storage->size;
    }

    // ========== Indexable Operations ==========

    static const void* get_at(const void* obj, size_t index, const TypeMeta* schema) {
        auto* storage = static_cast<const QueueStorage*>(obj);
        if (index >= storage->size) {
            throw std::out_of_range("Queue index out of range");
        }
        return get_element_ptr_const(obj, index, schema);
    }

    static void set_at(void* obj, size_t index, const void* value, const TypeMeta* schema) {
        auto* storage = static_cast<QueueStorage*>(obj);
        if (index >= storage->size) {
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
     * If unbounded and buffer is full, grows the buffer.
     * If bounded and full, evicts the oldest element (front).
     */
    static void push_back(void* obj, const void* value, const TypeMeta* schema) {
        auto* storage = static_cast<QueueStorage*>(obj);
        const TypeMeta* elem_type = schema->element_type;
        size_t elem_size = elem_type ? elem_type->size : 0;

        // Check if we need to grow or evict
        if (storage->size >= storage->allocated_capacity) {
            if (storage->max_capacity == 0) {
                // Unbounded: grow the buffer
                grow_buffer(storage, schema);
            } else {
                // Bounded and full: evict oldest (advance head, overwrite)
                // The size stays the same, we just move head and overwrite
                size_t tail = to_physical_index(storage, storage->size - 1);
                size_t new_tail = (tail + 1) % storage->allocated_capacity;
                void* elem_ptr = static_cast<char*>(storage->data) + new_tail * elem_size;
                if (elem_type && elem_type->ops && elem_type->ops->copy_assign) {
                    elem_type->ops->copy_assign(elem_ptr, value, elem_type);
                }
                storage->head = (storage->head + 1) % storage->allocated_capacity;
                return;
            }
        }

        // Normal push: add at tail
        size_t tail = to_physical_index(storage, storage->size);
        void* elem_ptr = static_cast<char*>(storage->data) + tail * elem_size;
        if (elem_type && elem_type->ops && elem_type->ops->copy_assign) {
            elem_type->ops->copy_assign(elem_ptr, value, elem_type);
        }
        storage->size++;
    }

    /**
     * @brief Remove the front element from the queue.
     *
     * @throws std::out_of_range if the queue is empty
     */
    static void pop_front(void* obj, const TypeMeta* schema) {
        auto* storage = static_cast<QueueStorage*>(obj);

        if (storage->size == 0) {
            throw std::out_of_range("pop_front on empty Queue");
        }

        // Advance head, decrease size
        storage->head = (storage->head + 1) % storage->allocated_capacity;
        storage->size--;
    }

    /**
     * @brief Clear all elements from the queue.
     */
    static void clear(void* obj, const TypeMeta* /*schema*/) {
        auto* storage = static_cast<QueueStorage*>(obj);
        storage->size = 0;
        storage->head = 0;
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
