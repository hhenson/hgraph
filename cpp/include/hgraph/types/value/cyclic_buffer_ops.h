#pragma once

/**
 * @file cyclic_buffer_ops.h
 * @brief TypeOps implementation for CyclicBuffer type.
 *
 * CyclicBuffer is a fixed-size circular buffer that re-centers on read.
 * When full, the oldest element is overwritten. Logical index 0 always
 * refers to the oldest element in the buffer.
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
// CyclicBuffer Storage
// ============================================================================

/**
 * @brief Storage structure for cyclic buffer.
 *
 * Elements are stored in a pre-allocated buffer that wraps around.
 * The `head` index points to the oldest element (logical index 0).
 * Physical layout may differ from logical order due to wrapping.
 */
struct CyclicBufferStorage {
    void* data{nullptr};       // Pre-allocated circular buffer
    size_t capacity{0};        // Fixed buffer size (set at type creation)
    size_t size{0};            // Current element count (0..capacity)
    size_t head{0};            // Index of oldest element (rotation pointer)

    CyclicBufferStorage() = default;

    CyclicBufferStorage(const CyclicBufferStorage&) = delete;
    CyclicBufferStorage& operator=(const CyclicBufferStorage&) = delete;
};

// ============================================================================
// CyclicBuffer Operations
// ============================================================================

/**
 * @brief Operations for CyclicBuffer types (fixed-size circular buffers).
 *
 * Key behaviors:
 * - Fixed capacity set at type creation (stored in schema->fixed_size)
 * - When full, push_back evicts the oldest element
 * - Logical index 0 = oldest element, index size-1 = newest element
 * - Physical storage uses circular indexing: physical = (head + logical) % capacity
 */
struct CyclicBufferOps {
    // ========== Helper Functions ==========

    static size_t get_element_size(const TypeMeta* schema) {
        return schema->element_type ? schema->element_type->size : 0;
    }

    /// Convert logical index to physical storage index
    static size_t to_physical_index(const CyclicBufferStorage* storage, size_t logical_index) {
        return (storage->head + logical_index) % storage->capacity;
    }

    static void* get_element_ptr(void* obj, size_t logical_index, const TypeMeta* schema) {
        auto* storage = static_cast<CyclicBufferStorage*>(obj);
        size_t elem_size = get_element_size(schema);
        size_t physical = to_physical_index(storage, logical_index);
        return static_cast<char*>(storage->data) + physical * elem_size;
    }

    static const void* get_element_ptr_const(const void* obj, size_t logical_index, const TypeMeta* schema) {
        auto* storage = static_cast<const CyclicBufferStorage*>(obj);
        size_t elem_size = get_element_size(schema);
        size_t physical = to_physical_index(storage, logical_index);
        return static_cast<const char*>(storage->data) + physical * elem_size;
    }

    // ========== Core Operations ==========

    static void construct(void* dst, const TypeMeta* schema) {
        // Initialize storage structure
        auto* storage = new (dst) CyclicBufferStorage();
        storage->capacity = schema->fixed_size;
        storage->size = 0;
        storage->head = 0;

        // Allocate buffer for capacity elements
        const TypeMeta* elem_type = schema->element_type;
        size_t elem_size = elem_type ? elem_type->size : 0;

        if (storage->capacity > 0 && elem_size > 0) {
            storage->data = std::malloc(storage->capacity * elem_size);
            if (!storage->data) {
                throw std::bad_alloc();
            }

            // Construct all elements in the buffer (they may be overwritten later)
            for (size_t i = 0; i < storage->capacity; ++i) {
                void* elem_ptr = static_cast<char*>(storage->data) + i * elem_size;
                if (elem_type->ops && elem_type->ops->construct) {
                    elem_type->ops->construct(elem_ptr, elem_type);
                }
            }
        }
    }

    static void destruct(void* obj, const TypeMeta* schema) {
        auto* storage = static_cast<CyclicBufferStorage*>(obj);
        const TypeMeta* elem_type = schema->element_type;
        size_t elem_size = elem_type ? elem_type->size : 0;

        // Destruct all elements in the buffer
        if (storage->data && elem_type) {
            for (size_t i = 0; i < storage->capacity; ++i) {
                void* elem_ptr = static_cast<char*>(storage->data) + i * elem_size;
                if (elem_type->ops && elem_type->ops->destruct) {
                    elem_type->ops->destruct(elem_ptr, elem_type);
                }
            }
            std::free(storage->data);
        }
        storage->~CyclicBufferStorage();
    }

    static void copy_assign(void* dst, const void* src, const TypeMeta* schema) {
        auto* dst_storage = static_cast<CyclicBufferStorage*>(dst);
        auto* src_storage = static_cast<const CyclicBufferStorage*>(src);
        const TypeMeta* elem_type = schema->element_type;
        size_t elem_size = elem_type ? elem_type->size : 0;

        // Copy metadata
        dst_storage->size = src_storage->size;
        dst_storage->head = src_storage->head;

        // Copy all elements (physical layout)
        if (elem_type && elem_type->ops && elem_type->ops->copy_assign) {
            for (size_t i = 0; i < src_storage->capacity; ++i) {
                void* dst_elem = static_cast<char*>(dst_storage->data) + i * elem_size;
                const void* src_elem = static_cast<const char*>(src_storage->data) + i * elem_size;
                elem_type->ops->copy_assign(dst_elem, src_elem, elem_type);
            }
        }
    }

    static void move_assign(void* dst, void* src, const TypeMeta* schema) {
        auto* dst_storage = static_cast<CyclicBufferStorage*>(dst);
        auto* src_storage = static_cast<CyclicBufferStorage*>(src);
        const TypeMeta* elem_type = schema->element_type;
        size_t elem_size = elem_type ? elem_type->size : 0;

        // Destruct dst elements
        if (dst_storage->data && elem_type) {
            for (size_t i = 0; i < dst_storage->capacity; ++i) {
                void* elem_ptr = static_cast<char*>(dst_storage->data) + i * elem_size;
                if (elem_type->ops && elem_type->ops->destruct) {
                    elem_type->ops->destruct(elem_ptr, elem_type);
                }
            }
            std::free(dst_storage->data);
        }

        // Move ownership from src to dst
        dst_storage->data = src_storage->data;
        dst_storage->capacity = src_storage->capacity;
        dst_storage->size = src_storage->size;
        dst_storage->head = src_storage->head;

        // Reset source
        src_storage->data = nullptr;
        src_storage->capacity = 0;
        src_storage->size = 0;
        src_storage->head = 0;
    }

    static void move_construct(void* dst, void* src, const TypeMeta* /*schema*/) {
        auto* src_storage = static_cast<CyclicBufferStorage*>(src);
        // Placement new with move: transfer data ownership directly
        auto* dst_storage = new (dst) CyclicBufferStorage();
        dst_storage->data = src_storage->data;
        dst_storage->capacity = src_storage->capacity;
        dst_storage->size = src_storage->size;
        dst_storage->head = src_storage->head;

        // Reset source
        src_storage->data = nullptr;
        src_storage->capacity = 0;
        src_storage->size = 0;
        src_storage->head = 0;
    }

    static bool equals(const void* a, const void* b, const TypeMeta* schema) {
        auto* storage_a = static_cast<const CyclicBufferStorage*>(a);
        auto* storage_b = static_cast<const CyclicBufferStorage*>(b);
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
        auto* storage = static_cast<const CyclicBufferStorage*>(obj);
        const TypeMeta* elem_type = schema->element_type;
        std::string result = "CyclicBuffer[";

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
        auto* storage = static_cast<const CyclicBufferStorage*>(obj);
        const TypeMeta* elem_type = schema->element_type;
        nb::list result;

        // Return elements in logical order (re-centered)
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
            throw std::runtime_error("CyclicBuffer.from_python expects a list or tuple");
        }

        auto* storage = static_cast<CyclicBufferStorage*>(dst);
        const TypeMeta* elem_type = schema->element_type;
        nb::sequence seq = nb::cast<nb::sequence>(src);
        size_t src_len = nb::len(seq);

        // Clear the buffer first
        storage->size = 0;
        storage->head = 0;

        // Push elements up to capacity
        size_t copy_count = std::min(src_len, storage->capacity);
        for (size_t i = 0; i < copy_count; ++i) {
            // Use push_back logic to add elements
            size_t insert_pos = storage->size;
            storage->size++;

            size_t elem_size = elem_type ? elem_type->size : 0;
            size_t physical = to_physical_index(storage, insert_pos);
            void* elem_ptr = static_cast<char*>(storage->data) + physical * elem_size;

            if (elem_type && elem_type->ops && elem_type->ops->from_python) {
                elem_type->ops->from_python(elem_ptr, seq[i], elem_type);
            }
        }
    }

    // ========== Hashable Operations ==========

    static size_t hash(const void* obj, const TypeMeta* schema) {
        auto* storage = static_cast<const CyclicBufferStorage*>(obj);
        const TypeMeta* elem_type = schema->element_type;
        size_t result = 0;

        // Hash elements in logical order
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
        auto* storage = static_cast<const CyclicBufferStorage*>(obj);
        return storage->size;
    }

    // ========== Indexable Operations ==========

    static const void* get_at(const void* obj, size_t index, const TypeMeta* schema) {
        auto* storage = static_cast<const CyclicBufferStorage*>(obj);
        if (index >= storage->size) {
            throw std::out_of_range("CyclicBuffer index out of range");
        }
        return get_element_ptr_const(obj, index, schema);
    }

    static void set_at(void* obj, size_t index, const void* value, const TypeMeta* schema) {
        auto* storage = static_cast<CyclicBufferStorage*>(obj);
        if (index >= storage->size) {
            throw std::out_of_range("CyclicBuffer index out of range");
        }
        void* elem_ptr = get_element_ptr(obj, index, schema);
        const TypeMeta* elem_type = schema->element_type;
        if (elem_type && elem_type->ops && elem_type->ops->copy_assign) {
            elem_type->ops->copy_assign(elem_ptr, value, elem_type);
        }
    }

    // ========== CyclicBuffer-Specific Operations ==========

    /**
     * @brief Push a value to the back of the cyclic buffer.
     *
     * If the buffer is not full, increments size and adds at the end.
     * If the buffer is full, overwrites the oldest element (at head)
     * and advances the head pointer.
     */
    static void push_back(void* obj, const void* value, const TypeMeta* schema) {
        auto* storage = static_cast<CyclicBufferStorage*>(obj);
        const TypeMeta* elem_type = schema->element_type;
        size_t elem_size = elem_type ? elem_type->size : 0;

        if (storage->size < storage->capacity) {
            // Buffer not full: add at end
            size_t physical = to_physical_index(storage, storage->size);
            void* elem_ptr = static_cast<char*>(storage->data) + physical * elem_size;
            if (elem_type && elem_type->ops && elem_type->ops->copy_assign) {
                elem_type->ops->copy_assign(elem_ptr, value, elem_type);
            }
            storage->size++;
        } else {
            // Buffer full: overwrite oldest (at head), advance head
            void* elem_ptr = static_cast<char*>(storage->data) + storage->head * elem_size;
            if (elem_type && elem_type->ops && elem_type->ops->copy_assign) {
                elem_type->ops->copy_assign(elem_ptr, value, elem_type);
            }
            storage->head = (storage->head + 1) % storage->capacity;
        }
    }

    /**
     * @brief Clear all elements from the cyclic buffer.
     */
    static void clear(void* obj, const TypeMeta* schema) {
        auto* storage = static_cast<CyclicBufferStorage*>(obj);

        // Reset to empty state (elements remain constructed but are "unused")
        storage->size = 0;
        storage->head = 0;
    }

    /**
     * @brief Get the capacity of the cyclic buffer.
     */
    static size_t capacity(const void* obj, const TypeMeta* /*schema*/) {
        auto* storage = static_cast<const CyclicBufferStorage*>(obj);
        return storage->capacity;
    }

    /**
     * @brief Check if the cyclic buffer is full.
     */
    static bool full(const void* obj, const TypeMeta* /*schema*/) {
        auto* storage = static_cast<const CyclicBufferStorage*>(obj);
        return storage->size == storage->capacity;
    }

    /// Get the operations vtable for cyclic buffers
    static const TypeOps* ops() {
        static const TypeOps cyclic_buffer_ops = {
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
            nullptr,   // less_than (cyclic buffers not ordered)
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
            nullptr,   // resize (fixed capacity)
            &clear,
        };
        return &cyclic_buffer_ops;
    }
};

} // namespace hgraph::value
